use crate::database;
use crate::discovery;
use crate::models::{NetworkInterface, NetworkMetric};
use anyhow::{Context, Result};
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_postgres::Client;
use uuid::Uuid;
use rand::random;

// Configuration constants for buffer management
const MAX_BUFFER_AGE_MINUTES: i64 = 10;  // Maximum age of buffered metrics
const MAX_METRICS_PER_INTERFACE: usize = 100;  // Maximum metrics to buffer per interface
const MAX_BUFFER_SIZE: usize = 1000;  // Total maximum buffer size across all interfaces
const INTERFACE_DETECTION_THRESHOLD: usize = 5;  // Minimum metrics to trigger forced detection

// Define a struct for the metric message
#[derive(Debug, Clone)]
pub struct MetricMessage {
    pub metrics: Vec<NetworkMetric>,
}

// Structure to track interface changes
#[derive(Debug)]
struct InterfaceTracker {
    interfaces: HashMap<String, NetworkInterface>,
    last_scan: chrono::DateTime<Utc>,
}

impl InterfaceTracker {
    fn new(interfaces: HashMap<String, NetworkInterface>) -> Self {
        InterfaceTracker {
            interfaces,
            last_scan: Utc::now(),
        }
    }

    fn update(&mut self, new_interfaces: HashMap<String, NetworkInterface>) -> (Vec<String>, Vec<String>) {
        let current_names: HashSet<String> = self.interfaces.keys().cloned().collect();
        let new_names: HashSet<String> = new_interfaces.keys().cloned().collect();

        // Find added and removed interfaces
        let added: Vec<String> = new_names.difference(&current_names).cloned().collect();
        let removed: Vec<String> = current_names.difference(&new_names).cloned().collect();

        // Update the tracker with new interfaces
        self.interfaces = new_interfaces;
        self.last_scan = Utc::now();

        (added, removed)
    }

    fn get(&self, interface_name: &str) -> Option<&NetworkInterface> {
        self.interfaces.get(interface_name)
    }

    fn get_all(&self) -> &HashMap<String, NetworkInterface> {
        &self.interfaces
    }
}

pub async fn collect_metrics(
    client: Arc<Client>,
    max_retries: usize,
    hostname: &str,
) -> Result<()> {
    println!("Starting metrics collection for host {}", hostname);

    let host_id = database::ensure_host_exists(Arc::clone(&client), hostname, max_retries).await?;

    // Start with an empty interface tracker - no initial discovery
    let mut interface_tracker = InterfaceTracker::new(HashMap::new());

    // Create a buffer for metrics from unknown interfaces
    let mut unknown_metrics: HashMap<String, Vec<NetworkMetric>> = HashMap::new();

    // Start the continuous metrics collection
    let mut metrics_rx = start_continuous_metrics_collection().await?;

    // Create a channel for periodic interface rediscovery
    let (rediscover_tx, mut rediscover_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Trigger an immediate but non-blocking rediscovery to find interfaces in background
    tokio::spawn(async move {
        if let Err(e) = rediscover_tx.send(()).await {
            eprintln!("Failed to send rediscovery signal: {}", e);
        }
    });

    // Create the status interval BEFORE the loop
    let mut status_interval = tokio::time::interval(Duration::from_secs(30));

    // Track period metrics only
    let mut period_metrics = 0;
    let mut last_status_time = Utc::now();

    loop {
        tokio::select! {
            Some(message) = metrics_rx.recv() => {
                match process_metrics_batch(
                    Arc::clone(&client),
                    &interface_tracker,
                    message.metrics,
                    max_retries,
                    &mut unknown_metrics
                ).await {
                    Ok(count) => {
                        period_metrics += count;
                    },
                    Err(e) => eprintln!("Error storing metrics: {}", e),
                }
            },

            _ = status_interval.tick() => {
                let now = Utc::now();
                let seconds = (now - last_status_time).num_seconds().max(1); // Avoid division by zero
                let rate = period_metrics as f64 / seconds as f64;

                if period_metrics != 0 {
                    println!("[{}] Status: {} metrics collected in the last {} seconds (rate: {:.1} metrics/sec)",
                             now.format("%H:%M:%S"),
                             period_metrics,
                             seconds,
                             rate);
                    last_status_time = now;
                    period_metrics = 0;
                }

            },
            Some(_) = rediscover_rx.recv() => {
                let zones = discovery::discover_zones(Arc::clone(&client), host_id, max_retries).await?;
                match rediscover_interfaces(
                    Arc::clone(&client),
                    host_id,
                    &zones,
                    &mut interface_tracker,
                    max_retries,
                    &mut unknown_metrics
                ).await {
                    Ok((new_count, processed_count)) => {
                        if new_count > 0 || processed_count > 0 {
                            println!("Discovered {} new interfaces and processed {} buffered metrics",
                                    new_count, processed_count);
                        }
                    },
                    Err(e) => eprintln!("Error during interface rediscovery: {}", e),
                }
            },
            else => break,
        }
    }

    Ok(())
}

async fn rediscover_interfaces(
    client: Arc<Client>,
    host_id: Uuid,
    zones: &HashMap<String, Uuid>,
    interface_tracker: &mut InterfaceTracker,
    max_retries: usize,
    unknown_metrics: &mut HashMap<String, Vec<NetworkMetric>>,
) -> Result<(usize, usize)> {
    // Discover current interfaces with reduced verbosity
    let current_interfaces = discovery::discover_interfaces(
        Arc::clone(&client),
        host_id,
        zones,
        max_retries,
        false  // Less verbose during rediscovery
    ).await?;

    // Update tracker and get changes
    let (added, removed) = interface_tracker.update(current_interfaces);

    // Log changes if any
    if !added.is_empty() {
        println!("Discovered {} new interfaces:", added.len());
        for name in &added {
            let interface = interface_tracker.get(name).unwrap();
            let parent_info = match &interface.parent_interface {
                Some(parent) => format!(", parent: {}", parent),
                None => String::new(),
            };

            println!("  - {} (type: {}{})",
                    name,
                    interface.interface_type,
                    parent_info);
        }
    }

    if !removed.is_empty() {
        println!("Removed {} interfaces:", removed.len());
        for name in &removed {
            println!("  - {}", name);
        }
    }

    // Count how many new interfaces discovered and metrics processed
    let mut processed_count = 0;

    // Process buffered metrics for newly discovered interfaces
    for name in &added {
        if let Some(metrics) = unknown_metrics.remove(name) {
            let metrics_len = metrics.len();
            println!("Processing {} buffered metrics for newly discovered interface {}",
                     metrics_len, name);

            // Store the processed metrics count for this interface
            let mut interface_processed = 0;

            // Process each buffered metric
            for metric in metrics {
                let interface_id = interface_tracker.get(name).unwrap().interface_id;

                if let Ok(_) = store_metric(
                    Arc::clone(&client),
                    interface_id,
                    &metric,
                    max_retries
                ).await {
                    processed_count += 1;
                    interface_processed += 1;
                }
            }

            println!("Processed {}/{} buffered metrics for interface {}",
                    interface_processed, metrics_len, name);
        }
    }

    // Force detection for interfaces with enough buffered metrics
    let interfaces_to_force = unknown_metrics.iter()
        .filter(|(_, metrics)| metrics.len() >= INTERFACE_DETECTION_THRESHOLD)
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();

    for interface_name in interfaces_to_force {
        if interface_tracker.get(&interface_name).is_none() {
            println!("Forcing detection for unknown interface with {} buffered metrics: {}",
                    unknown_metrics.get(&interface_name).map(|m| m.len()).unwrap_or(0),
                    interface_name);

            // Try to detect this specific interface explicitly by querying dladm
            if let Ok(interface) = force_interface_detection(
                Arc::clone(&client),
                host_id,
                &interface_name,
                max_retries
            ).await {
                // Update the tracker with the new interface
                let mut updated_interfaces = interface_tracker.get_all().clone();
                updated_interfaces.insert(interface_name.clone(), interface);
                let (added, _) = interface_tracker.update(updated_interfaces);

                if !added.is_empty() {
                    // Process buffered metrics for this interface
                    if let Some(metrics) = unknown_metrics.remove(&interface_name) {
                        // Store the processed metrics count for this interface
                        let mut interface_processed = 0;
                        let metrics_len = metrics.len();

                        for metric in metrics {
                            let interface_id = interface_tracker.get(&interface_name).unwrap().interface_id;

                            if let Ok(_) = store_metric(
                                Arc::clone(&client),
                                interface_id,
                                &metric,
                                max_retries
                            ).await {
                                processed_count += 1;
                                interface_processed += 1;
                            }
                        }

                        println!("Processed {}/{} buffered metrics for newly detected interface {}",
                                 interface_processed, metrics_len, interface_name);
                    }
                }
            } else {
                println!("Failed to force detection for interface {}, metrics will remain buffered",
                        interface_name);
            }
        }
    }

    // Clean up buffer to prevent memory leaks:

    // 1. Age-based cleanup - remove metrics older than MAX_BUFFER_AGE_MINUTES
    let cutoff_time = Utc::now() - chrono::Duration::minutes(MAX_BUFFER_AGE_MINUTES);

    for (interface_name, metrics) in unknown_metrics.iter_mut() {
        let original_len = metrics.len();
        metrics.retain(|m| m.timestamp > cutoff_time);
        let removed = original_len - metrics.len();

        if removed > 0 {
            println!("Dropped {} old buffered metrics for unknown interface {}",
                    removed, interface_name);
        }

        // 2. Size-based cleanup - limit metrics per interface
        if metrics.len() > MAX_METRICS_PER_INTERFACE {
            // Sort by timestamp (newest first) and keep only the most recent MAX_METRICS_PER_INTERFACE
            metrics.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
            let truncated = metrics.len() - MAX_METRICS_PER_INTERFACE;
            metrics.truncate(MAX_METRICS_PER_INTERFACE);

            println!("Truncated {} excess buffered metrics for interface {}",
                    truncated, interface_name);
        }
    }

    // 3. Remove empty entries
    unknown_metrics.retain(|_, metrics| !metrics.is_empty());

    // 4. Global buffer size limit - if we exceed MAX_BUFFER_SIZE, drop oldest metrics
    let total_buffered = unknown_metrics.values().map(|v| v.len()).sum::<usize>();
    if total_buffered > MAX_BUFFER_SIZE {
        println!("Buffer size ({}) exceeds maximum ({}), trimming oldest metrics",
                total_buffered, MAX_BUFFER_SIZE);

        // Flatten all metrics into one vector with interface name
        let mut all_metrics = Vec::new();
        for (interface, metrics) in unknown_metrics.iter() {
            for metric in metrics {
                all_metrics.push((interface.clone(), metric.clone(), metric.timestamp));
            }
        }

        // Sort by timestamp (oldest first)
        all_metrics.sort_by(|a, b| a.2.cmp(&b.2));

        // Determine how many to remove
        let to_remove = total_buffered - MAX_BUFFER_SIZE;
        let metrics_to_remove = all_metrics.iter().take(to_remove).collect::<Vec<_>>();

        // Remove the oldest metrics
        for (interface, metric, _) in metrics_to_remove {
            if let Some(metrics) = unknown_metrics.get_mut(interface) {
                metrics.retain(|m| m.timestamp != metric.timestamp);
            }
        }

        println!("Removed {} oldest metrics from buffer", to_remove);

        // Clean up empty entries again
        unknown_metrics.retain(|_, metrics| !metrics.is_empty());
    }

    Ok((added.len(), processed_count))
}

// Function to force detection of a specific interface by querying dladm directly
async fn force_interface_detection(
    client: Arc<Client>,
    host_id: Uuid,
    interface_name: &str,
    max_retries: usize,
) -> Result<NetworkInterface> {
    println!("Attempting to force detection of interface: {}", interface_name);

    // Try to get accurate interface type by querying dladm directly for this specific interface
    let output = Command::new("/usr/sbin/dladm")
        .args(&["show-link", "-p", "-o", "link,class", interface_name])
        .output()
        .context(format!("Failed to run dladm show-link for {}", interface_name))?;

    let link_output = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in dladm output")?;

    // Default values
    let mut interface_type = "dev".to_string(); // Default type
    let mut parent_interface = None;

    // Parse dladm output to get actual interface type
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 && fields[0] == interface_name {
            interface_type = fields[1].to_string();
            break;
        }
    }

    // If it's a VNIC, get the parent interface
    if interface_type == "vnic" {
        let vnic_output = Command::new("/usr/sbin/dladm")
            .args(&["show-vnic", "-p", "-o", "link,over", interface_name])
            .output()
            .context(format!("Failed to run dladm show-vnic for {}", interface_name))?;

        let vnic_text = String::from_utf8(vnic_output.stdout)
            .context("Invalid UTF-8 in dladm vnic output")?;

        for line in vnic_text.lines() {
            let fields: Vec<&str> = line.split(':').collect();
            if fields.len() >= 2 && fields[0] == interface_name {
                parent_interface = Some(fields[1].to_string());
                break;
            }
        }
    }

    // If dladm didn't find the interface, fall back to guessing based on naming conventions
    if link_output.trim().is_empty() {
        println!("Interface {} not found in dladm, using heuristics to determine type", interface_name);

        interface_type = if interface_name.starts_with("igb") ||
                          interface_name.starts_with("e1000g") ||
                          interface_name.starts_with("bge") ||
                          interface_name.starts_with("ixgbe") {
            "phys".to_string()  // Physical device
        } else if interface_name.ends_with("stub") ||
                  interface_name.contains("stub") {
            "etherstub".to_string()
        } else if interface_name.ends_with("0") &&
                  !interface_name.contains("gw") &&
                  !interface_name.starts_with("igb") {
            "bridge".to_string()  // Likely a bridge (ends with 0)
        } else if interface_name.contains("overlay") {
            "overlay".to_string()
        } else if interface_name.contains("gw") {
            "vnic".to_string()  // Gateway VNICs typically end with gw
        } else {
            "dev".to_string()  // Default to generic device if we can't determine
        };
    }

    // Create the interface record
    let interface_id = discovery::ensure_interface_exists(
        Arc::clone(&client),
        host_id,
        None, // Default to global zone
        interface_name.to_string(),
        interface_type.clone(),
        parent_interface.clone(),
        max_retries,
    ).await?;

    let interface = NetworkInterface {
        interface_id,
        host_id,
        zone_id: None,
        interface_name: interface_name.to_string(),
        interface_type,
        parent_interface,
    };

    println!("Created interface record for interface: {} (type: {}{})",
             interface_name,
             interface.interface_type,
             match &interface.parent_interface {
                 Some(parent) => format!(", parent: {}", parent),
                 None => String::new(),
             });

    Ok(interface)
}

async fn process_metrics_batch(
    client: Arc<Client>,
    interface_tracker: &InterfaceTracker,
    metrics: Vec<NetworkMetric>,
    max_retries: usize,
    unknown_metrics: &mut HashMap<String, Vec<NetworkMetric>>,
) -> Result<usize> {
    let mut stored_count = 0;
    let mut unknown_count = 0;

    for metric in metrics {
        if let Some(interface) = interface_tracker.get(&metric.interface_name) {
            // Use the helper function to store the metric
            if let Ok(_) = store_metric(
                Arc::clone(&client),
                interface.interface_id,
                &metric,
                max_retries
            ).await {
                stored_count += 1;
            }
        } else {
            // Store the metric for this unknown interface
            unknown_metrics
                .entry(metric.interface_name.clone())
                .or_insert_with(Vec::new)
                .push(metric);

            unknown_count += 1;
        }
    }

    if unknown_count > 0 {
        println!("Buffered {} metrics for {} unknown interfaces",
                 unknown_count,
                 unknown_metrics.keys().collect::<std::collections::HashSet<_>>().len());
    }

    Ok(stored_count)
}

// Helper function to store a single metric
async fn store_metric(
    client: Arc<Client>,
    interface_id: Uuid,
    metric: &NetworkMetric,
    max_retries: usize
) -> Result<()> {
    execute_with_retry(move || {
        let client = Arc::clone(&client);
        let timestamp = metric.timestamp;
        let input_bytes = metric.input_bytes;
        let input_packets = metric.input_packets;
        let output_bytes = metric.output_bytes;
        let output_packets = metric.output_packets;

        Box::pin(async move {
            client
                .execute(
                    "INSERT INTO netmetrics (
                    interface_id, timestamp,
                    input_bytes, input_packets, output_bytes, output_packets,
                    collection_method
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    &[
                        &interface_id,
                        &timestamp,
                        &input_bytes,
                        &input_packets,
                        &output_bytes,
                        &output_packets,
                        &"dlstat",
                    ],
                )
                .await
                .context("Failed to insert metrics")
        })
    }, max_retries)
    .await?;

    Ok(())
}

// Start a continuous metrics collection in the background
pub async fn start_continuous_metrics_collection() -> Result<mpsc::Receiver<MetricMessage>> {
    // Create a channel for sending metrics from the background thread
    let (tx, rx) = mpsc::channel::<MetricMessage>(100);

    // Spawn a dedicated thread for the blocking I/O operations
    thread::spawn(move || {
        let result = continuous_dlstat_collection(tx);
        if let Err(e) = result {
            eprintln!("Metrics collection thread error: {}", e);
        }
    });

    Ok(rx)
}

// Function to run in the background thread that continuously reads from dlstat
fn continuous_dlstat_collection(tx: mpsc::Sender<MetricMessage>) -> Result<()> {
    // Start dlstat with interval mode
    let mut child = Command::new("/usr/sbin/dlstat")
        .args(&["-i", "1"])  // 1 second interval
        .stdout(Stdio::piped())
        .spawn()
        .context("Failed to run dlstat -i command")?;

    let stdout = child.stdout.take().unwrap();
    let reader = BufReader::new(stdout);

    let mut line_count = 0;
    let mut section_count = 0;
    let mut current_metrics = Vec::new();
    let mut is_collecting = false;

    // Read lines continuously
    for line in reader.lines() {
        let line = line.context("Failed to read line from dlstat")?;

        // Check for header line which indicates a new section of data
        if line.contains("LINK") && line.contains("IPKTS") {
            // If we were collecting a section and have metrics, send them
            if is_collecting && !current_metrics.is_empty() {
                // Create a message with the collected metrics
                let message = MetricMessage {
                    metrics: current_metrics,
                };

                // Try to send the metrics to the channel
                if let Err(e) = tx.blocking_send(message) {
                    eprintln!("Failed to send metrics: {}", e);
                    break; // Receiver dropped, time to exit
                }

                // Reset for next batch
                current_metrics = Vec::new();
            }

            section_count += 1;
            line_count = 0;

            // Skip the first section (cumulative stats since boot)
            // Only start collecting from the second section onwards
            is_collecting = section_count > 1;

            continue;
        }

        line_count += 1;

        // Skip header lines and don't process the first section
        if line_count <= 1 || !is_collecting {
            continue;
        }

        // Parse the line
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() >= 5 {
            let interface_name = fields[0].to_string();

            // Parse the values - handle units (K, M, G) if present
            let input_packets = parse_metric_value(fields[1])?;
            let input_bytes = parse_metric_value(fields[2])?;
            let output_packets = parse_metric_value(fields[3])?;
            let output_bytes = parse_metric_value(fields[4])?;

            current_metrics.push(NetworkMetric {
                interface_name,
                input_bytes,
                input_packets,
                output_bytes,
                output_packets,
                timestamp: Utc::now(),
            });
        }
    }

    // If we get here, the process terminated
    println!("dlstat process terminated");
    Ok(())
}

// Helper function to parse values with K, M, G suffixes
fn parse_metric_value(value_str: &str) -> Result<i64> {
    let mut value_str = value_str.to_string();
    let mut multiplier = 1;

    // Check for suffixes
    if value_str.ends_with('K') {
        multiplier = 1_000;
        value_str.pop();
    } else if value_str.ends_with('M') {
        multiplier = 1_000_000;
        value_str.pop();
    } else if value_str.ends_with('G') {
        multiplier = 1_000_000_000;
        value_str.pop();
    }

    // Parse the numeric part
    match value_str.parse::<f64>() {
        Ok(value) => Ok((value * multiplier as f64) as i64),
        Err(_) => Err(anyhow::anyhow!("Failed to parse value: {}", value_str))
    }
}

// Helper function for database operations with retries
pub async fn execute_with_retry<F, T, E>(f: F, max_retries: usize) -> Result<T>
where
    F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>> + Send + Sync,
    E: std::fmt::Display + Into<anyhow::Error>,
{
    let mut retries = 0;
    let mut delay = Duration::from_millis(100);

    loop {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                retries += 1;
                if retries >= max_retries {
                    return Err(e.into());
                }

                // Log the error and retry
                eprintln!("Database operation failed (retry {}/{}): {}", retries, max_retries, e);
                tokio::time::sleep(delay).await;

                // Exponential backoff with jitter
                delay = Duration::from_millis(
                    (delay.as_millis() as f64 * 1.5) as u64 +
                    random::<u64>() % 100
                );
            }
        }
    }
}
