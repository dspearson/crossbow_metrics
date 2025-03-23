use crate::database;
use crate::discovery;
use crate::discovery::ensure_interface_exists;
use crate::models::{NetworkInterface, NetworkMetric};
use anyhow::{Context, Result};
use chrono::Utc;
use std::collections::HashMap;
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

pub async fn collect_metrics(
    client: Arc<Client>,
    interval_secs: u64,
    max_retries: usize,
    hostname: &str,
) -> Result<()> {
    println!("Starting metrics collection for host {}", hostname);

    // Buffer for metrics from unknown interfaces
    let mut unknown_metrics: HashMap<String, Vec<NetworkMetric>> = HashMap::new();

    // Discover initial interfaces
    let host_id = database::ensure_host_exists(Arc::clone(&client), hostname, max_retries).await?;
    let zones = discovery::discover_zones(Arc::clone(&client), host_id, max_retries).await?;
    let mut interface_map = discovery::discover_interfaces(Arc::clone(&client), host_id, &zones, max_retries).await?;

    println!("Initially discovered {} interfaces", interface_map.len());

    // Start the continuous metrics collection
    let mut metrics_rx = start_continuous_metrics_collection().await?;

    // Create a channel for periodic interface rediscovery
    let (rediscover_tx, mut rediscover_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Spawn a task for periodic interface rediscovery
    let rediscover_interval = std::cmp::max(interval_secs * 10, 60);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(rediscover_interval));
        loop {
            interval.tick().await;
            if let Err(e) = rediscover_tx.send(()).await {
                eprintln!("Failed to send rediscovery signal: {}", e);
                break;
            }
        }
    });

    // Process metrics as they arrive and handle interface rediscovery
    let mut total_metrics = 0;
    let mut status_interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            Some(message) = metrics_rx.recv() => {
                match process_metrics_batch(
                    Arc::clone(&client),
                    &interface_map,
                    message.metrics,
                    max_retries,
                    &mut unknown_metrics
                ).await {
                    Ok(count) => {
                        total_metrics += count;
                    },
                    Err(e) => eprintln!("Error storing metrics: {}", e),
                }
            }

            _ = status_interval.tick() => {
                println!("[{}] Status: Total metrics collected: {}",
                         Utc::now().format("%H:%M:%S"),
                         total_metrics);
            },

            Some(_) = rediscover_rx.recv() => {
                println!("Performing periodic interface rediscovery...");
                match rediscover_interfaces(
                    Arc::clone(&client),
                    host_id,
                    &zones,
                    &mut interface_map,
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
            }

            else => break,
        }
    }

    Ok(())
}

// Enhanced rediscover function with better buffer management
async fn rediscover_interfaces(
    client: Arc<Client>,
    host_id: Uuid,
    zones: &HashMap<String, Uuid>,
    interface_map: &mut HashMap<String, NetworkInterface>,
    max_retries: usize,
    unknown_metrics: &mut HashMap<String, Vec<NetworkMetric>>,
) -> Result<(usize, usize)> {
    // Discover all current interfaces
    let current_interfaces = discovery::discover_interfaces(
        Arc::clone(&client),
        host_id,
        zones,
        max_retries
    ).await?;

    // Count how many new interfaces we discover
    let mut new_count = 0;
    let mut processed_count = 0;

    // Add any new interfaces to our map and process buffered metrics
    for (name, interface) in current_interfaces {
        let is_new = !interface_map.contains_key(&name);

        if is_new {
            println!("Discovered new interface: {}", name);
            interface_map.insert(name.clone(), interface);
            new_count += 1;

            // Check if we have buffered metrics for this interface
            if let Some(metrics) = unknown_metrics.remove(&name) {
                println!("Processing {} buffered metrics for newly discovered interface {}",
                        metrics.len(), name);

                // Process each buffered metric
                for metric in metrics {
                    let client = Arc::clone(&client);
                    let interface_id = interface_map.get(&name).unwrap().interface_id;
                    let timestamp = metric.timestamp;
                    let input_bytes = metric.input_bytes;
                    let input_packets = metric.input_packets;
                    let output_bytes = metric.output_bytes;
                    let output_packets = metric.output_packets;

                    if let Ok(_) = execute_with_retry(move || {
                        let client = Arc::clone(&client);
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
                                .context("Failed to insert buffered metrics")
                        })
                    }, max_retries)
                    .await {
                        processed_count += 1;
                    }
                }
            }
        }
    }

    // Force detection for interfaces with enough buffered metrics
    let interfaces_to_force = unknown_metrics.iter()
        .filter(|(_, metrics)| metrics.len() >= INTERFACE_DETECTION_THRESHOLD)
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();

    for interface_name in interfaces_to_force {
        if !interface_map.contains_key(&interface_name) {
            println!("Forcing detection for unknown interface with {} buffered metrics: {}",
                    unknown_metrics.get(&interface_name).map(|m| m.len()).unwrap_or(0),
                    interface_name);

            // Try to detect this specific interface explicitly
            if let Ok(interface) = force_interface_detection(
                Arc::clone(&client),
                host_id,
                &interface_name,
                max_retries
            ).await {
                interface_map.insert(interface_name.clone(), interface);
                new_count += 1;

                // Process buffered metrics for this interface
                if let Some(metrics) = unknown_metrics.remove(&interface_name) {
                    for metric in metrics {
                        let client = Arc::clone(&client);
                        let interface_id = interface_map.get(&interface_name).unwrap().interface_id;
                        let timestamp = metric.timestamp;
                        let input_bytes = metric.input_bytes;
                        let input_packets = metric.input_packets;
                        let output_bytes = metric.output_bytes;
                        let output_packets = metric.output_packets;

                        if let Ok(_) = execute_with_retry(move || {
                            let client = Arc::clone(&client);
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
                                    .context("Failed to insert buffered metrics")
                            })
                        }, max_retries)
                        .await {
                            processed_count += 1;
                        }
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

    Ok((new_count, processed_count))
}

// Function to force detection of a specific interface
async fn force_interface_detection(
    client: Arc<Client>,
    host_id: Uuid,
    interface_name: &str,
    max_retries: usize,
) -> Result<NetworkInterface> {
    println!("Attempting to force detection of interface: {}", interface_name);

    // Try to determine interface type based on naming conventions
    let interface_type = if interface_name.starts_with("igb") ||
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

    // Create the interface record
    let interface_id = ensure_interface_exists(
        Arc::clone(&client),
        host_id,
        None, // Default to global zone
        interface_name.to_string(),
        interface_type.clone(), // Clone here before moving into the struct
        None, // No parent information
        max_retries,
    ).await?;

    let interface = NetworkInterface {
        interface_id,
        host_id,
        zone_id: None,
        interface_name: interface_name.to_string(),
        interface_type: interface_type.clone(), // Clone here to avoid move
        parent_interface: None,
    };

    println!("Created interface record for unknown interface: {} (type: {})",
             interface_name, interface_type);

    Ok(interface)
}

async fn process_metrics_batch(
    client: Arc<Client>,
    interface_map: &HashMap<String, NetworkInterface>,
    metrics: Vec<NetworkMetric>,
    max_retries: usize,
    unknown_metrics: &mut HashMap<String, Vec<NetworkMetric>>,
) -> Result<usize> {
    let mut stored_count = 0;
    let mut unknown_count = 0;

    for metric in metrics {
        if let Some(interface) = interface_map.get(&metric.interface_name) {
            let client = Arc::clone(&client);
            let interface_id = interface.interface_id;
            let timestamp = metric.timestamp;
            let input_bytes = metric.input_bytes;
            let input_packets = metric.input_packets;
            let output_bytes = metric.output_bytes;
            let output_packets = metric.output_packets;

            // We could store additional metadata like interface_type with each metric
            // if that would be useful for querying later

            execute_with_retry(move || {
                let client = Arc::clone(&client);
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

            stored_count += 1;
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

            if section_count <= 2 {
                println!("Section {}: {} metrics collection",
                        section_count,
                        if is_collecting { "Starting" } else { "Skipping" });
            }

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
