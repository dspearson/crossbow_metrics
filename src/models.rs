use crate::database;
use crate::discovery;
use anyhow::{Context, Result};
use chrono::Utc;
use log::{debug, error, info, trace, warn};
use macready::buffer::{BufferConfig, MetricsBuffer};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_postgres::Client;
use uuid::Uuid;

// Configuration constants for buffer management
const MAX_BUFFER_AGE_MINUTES: i64 = 10; // Maximum age of buffered metrics
const MAX_METRICS_PER_INTERFACE: usize = 100; // Maximum metrics to buffer per interface
const MAX_BUFFER_SIZE: usize = 1000; // Total maximum buffer size across all interfaces
const INTERFACE_DETECTION_THRESHOLD: usize = 5; // Minimum metrics to trigger forced detection

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

    fn update(
        &mut self,
        new_interfaces: HashMap<String, NetworkInterface>,
    ) -> (Vec<String>, Vec<String>) {
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
    verbose: bool,
) -> Result<()> {
    let host_id = database::ensure_host_exists(Arc::clone(&client), hostname, max_retries).await?;

    // Start with an empty interface tracker - no initial discovery
    let mut interface_tracker = InterfaceTracker::new(HashMap::new());

    // Create a buffer configuration based on our constants
    let buffer_config = BufferConfig {
        max_age_minutes: MAX_BUFFER_AGE_MINUTES,
        max_per_entity: MAX_METRICS_PER_INTERFACE,
        max_total: MAX_BUFFER_SIZE,
        detection_threshold: INTERFACE_DETECTION_THRESHOLD,
    };

    // Create a buffer for metrics from unknown interfaces using macready's implementation
    let unknown_metrics_buffer = Arc::new(MetricsBuffer::<NetworkMetric>::with_config(buffer_config));

    // Create a HashMap wrapper around the buffer for backward compatibility
    let mut unknown_metrics = HashMap::new();

    // Start the continuous metrics collection
    info!("Starting continuous metrics collection");
    let mut metrics_rx = start_continuous_metrics_collection().await?;

    // Create channels for communication
    let (rediscover_tx, mut rediscover_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Trigger an immediate but non-blocking rediscovery to find interfaces in background
    trigger_initial_discovery(rediscover_tx.clone()).await;

    // Setup status tracking
    let mut status_tracker = setup_status_tracking();

    info!("Entering main collection loop");
    loop {
        tokio::select! {
            Some(message) = metrics_rx.recv() => {
                match process_metrics_batch(
                    Arc::clone(&client),
                    &interface_tracker,
                    message.metrics,
                    max_retries,
                    &mut unknown_metrics,
                    &unknown_metrics_buffer,
                ).await {
                    Ok(count) => {
                        status_tracker.period_metrics += count;
                        trace!("Processed {} metrics in this batch", count);
                    },
                    Err(e) => error!("Error storing metrics: {}", e),
                }
            },

            _ = status_tracker.interval.tick() => {
                update_and_log_status(&mut status_tracker);
            },

            Some(_) = rediscover_rx.recv() => {
                handle_interface_rediscovery(
                    Arc::clone(&client),
                    host_id,
                    &mut interface_tracker,
                    max_retries,
                    &mut unknown_metrics,
                    &unknown_metrics_buffer,
                    verbose
                ).await?;
            },

            else => break,
        }
    }

    Ok(())
}

async fn trigger_initial_discovery(rediscover_tx: mpsc::Sender<()>) {
    debug!("Triggering initial interface discovery");
    tokio::spawn(async move {
        if let Err(e) = rediscover_tx.send(()).await {
            error!("Failed to send rediscovery signal: {}", e);
        }
    });
}

struct StatusTracker {
    interval: tokio::time::Interval,
    period_metrics: usize,
    last_status_time: chrono::DateTime<Utc>,
}

fn setup_status_tracking() -> StatusTracker {
    StatusTracker {
        interval: tokio::time::interval(Duration::from_secs(30)),
        period_metrics: 0,
        last_status_time: Utc::now(),
    }
}

fn update_and_log_status(tracker: &mut StatusTracker) {
    let now = Utc::now();
    let seconds = (now - tracker.last_status_time).num_seconds().max(1); // Avoid division by zero
    let rate = tracker.period_metrics as f64 / seconds as f64;

    if tracker.period_metrics != 0 {
        info!(
            "Status: {} metrics collected in the last {} seconds (rate: {:.1} metrics/sec)",
            tracker.period_metrics, seconds, rate
        );
        tracker.last_status_time = now;
        tracker.period_metrics = 0;
    } else {
        debug!("No metrics collected in the last {} seconds", seconds);
    }
}

async fn handle_interface_rediscovery(
    client: Arc<Client>,
    host_id: Uuid,
    interface_tracker: &mut InterfaceTracker,
    max_retries: usize,
    unknown_metrics: &mut HashMap<String, Vec<NetworkMetric>>,
    unknown_metrics_buffer: &Arc<MetricsBuffer<NetworkMetric>>,
    verbose: bool,
) -> Result<()> {
    debug!("Starting interface rediscovery process");
    let zones = discovery::discover_zones(Arc::clone(&client), host_id, max_retries).await?;

    match rediscover_interfaces(
        Arc::clone(&client),
        host_id,
        &zones,
        interface_tracker,
        max_retries,
        unknown_metrics,
        unknown_metrics_buffer,
        verbose,
    )
    .await
    {
        Ok((new_count, processed_count)) => {
            if new_count > 0 || processed_count > 0 {
                info!(
                    "Discovered {} new interfaces and processed {} buffered metrics",
                    new_count, processed_count
                );
            } else {
                debug!("No new interfaces discovered during rediscovery");
            }
            Ok(())
        }
        Err(e) => {
            error!("Error during interface rediscovery: {}", e);
            Err(e)
        }
    }
}

async fn rediscover_interfaces(
    client: Arc<Client>,
    host_id: Uuid,
    zones: &HashMap<String, Uuid>,
    interface_tracker: &mut InterfaceTracker,
    max_retries: usize,
    unknown_metrics: &mut HashMap<String, Vec<NetworkMetric>>,
    unknown_metrics_buffer: &Arc<MetricsBuffer<NetworkMetric>>,
    verbose: bool,
) -> Result<(usize, usize)> {
    // Discover current interfaces
    debug!("Discovering current interfaces");
    let current_interfaces =
        discovery::discover_interfaces(Arc::clone(&client), host_id, zones, max_retries, verbose)
            .await?;

    // Update tracker and get changes
    let (added, removed) = interface_tracker.update(current_interfaces);

    // Log any interface changes
    log_interface_changes(&added, &removed, interface_tracker, zones, verbose);

    // Process buffered metrics for newly discovered interfaces
    let mut processed_count = process_buffered_metrics_for_new_interfaces(
        &client,
        &added,
        interface_tracker,
        unknown_metrics,
        unknown_metrics_buffer,
        max_retries,
    )
    .await?;

    // Handle interfaces with enough buffered metrics to force detection
    processed_count += handle_force_detection_interfaces(
        client,
        host_id,
        interface_tracker,
        unknown_metrics,
        unknown_metrics_buffer,
        max_retries,
    )
    .await?;

    // Clean up the buffer to prevent memory leaks - use macready's cleanup method
    match unknown_metrics_buffer.cleanup() {
        Ok(removed) => {
            if removed > 0 {
                debug!("Cleaned up {} old buffered metrics", removed);
            }
        },
        Err(e) => {
            warn!("Error cleaning up buffer: {}", e);
        }
    }

    Ok((added.len(), processed_count))
}

fn log_interface_changes(
    added: &[String],
    removed: &[String],
    interface_tracker: &InterfaceTracker,
    zones: &HashMap<String, Uuid>,
    verbose: bool,
) {
    if !added.is_empty() || !removed.is_empty() {
        info!("Interface changes: +{} -{}", added.len(), removed.len());

        if verbose {
            for name in added {
                if let Some(interface) = interface_tracker.get(name) {
                    let parent_info = match &interface.parent_interface {
                        Some(parent) => format!(", parent: {}", parent),
                        None => String::new(),
                    };

                    let zone_info = match &interface.zone_id {
                        Some(zone_id) => {
                            let unknown = "unknown".to_string();
                            let zone_name = zones
                                .iter()
                                .find_map(
                                    |(name, id)| if id == zone_id { Some(name) } else { None },
                                )
                                .unwrap_or(&unknown);
                            format!(", zone: {}", zone_name)
                        }
                        None => ", global zone".to_string(),
                    };

                    debug!(
                        "New interface: {} (type: {}{}{})",
                        name, interface.interface_type, parent_info, zone_info
                    );
                }
            }

            for name in removed {
                debug!("Removed interface: {}", name);
            }
        }
    }
}

async fn process_buffered_metrics_for_new_interfaces(
    client: &Arc<Client>,
    added: &[String],
    interface_tracker: &InterfaceTracker,
    unknown_metrics: &mut HashMap<String, Vec<NetworkMetric>>,
    unknown_metrics_buffer: &Arc<MetricsBuffer<NetworkMetric>>,
    max_retries: usize,
) -> Result<usize> {
    let mut processed_count = 0;

    for name in added {
        // First try getting metrics from the HashMap for backward compatibility
        if let Some(metrics) = unknown_metrics.remove(name) {
            let metrics_len = metrics.len();
            info!(
                "Processing {} buffered metrics from HashMap for newly discovered interface {}",
                metrics_len, name
            );

            // Store the processed metrics count for this interface
            let mut interface_processed = 0;

            // Process each buffered metric
            for metric in metrics {
                if let Some(interface) = interface_tracker.get(name) {
                    let interface_id = interface.interface_id;

                    if let Ok(_) =
                        store_metric(Arc::clone(client), interface_id, &metric, max_retries).await
                    {
                        processed_count += 1;
                        interface_processed += 1;
                    }
                }
            }

            debug!(
                "Processed {}/{} buffered metrics for interface {}",
                interface_processed, metrics_len, name
            );
        }

        // Then try getting metrics from macready's buffer
        match unknown_metrics_buffer.take_for_entity(name) {
            Ok(metrics) => {
                let metrics_len = metrics.len();
                if metrics_len > 0 {
                    info!(
                        "Processing {} buffered metrics from MetricsBuffer for newly discovered interface {}",
                        metrics_len, name
                    );

                    // Store the processed metrics count for this interface
                    let mut interface_processed = 0;

                    // Process each buffered metric
                    for metric in metrics {
                        if let Some(interface) = interface_tracker.get(name) {
                            let interface_id = interface.interface_id;

                            if let Ok(_) =
                                store_metric(Arc::clone(client), interface_id, &metric, max_retries).await
                            {
                                processed_count += 1;
                                interface_processed += 1;
                            }
                        }
                    }

                    debug!(
                        "Processed {}/{} buffered metrics from MetricsBuffer for interface {}",
                        interface_processed, metrics_len, name
                    );
                }
            },
            Err(e) => {
                warn!("Error taking metrics from buffer for {}: {}", name, e);
            }
        }
    }

    Ok(processed_count)
}

async fn handle_force_detection_interfaces(
    client: Arc<Client>,
    host_id: Uuid,
    interface_tracker: &mut InterfaceTracker,
    unknown_metrics: &mut HashMap<String, Vec<NetworkMetric>>,
    unknown_metrics_buffer: &Arc<MetricsBuffer<NetworkMetric>>,
    max_retries: usize,
) -> Result<usize> {
    let mut processed_count = 0;

    // First, check the HashMap candidates
    let interfaces_to_force = unknown_metrics
        .iter()
        .filter(|(_, metrics)| metrics.len() >= INTERFACE_DETECTION_THRESHOLD)
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();

    // Then, check the MetricsBuffer candidates
    let buffer_candidates = match unknown_metrics_buffer.get_autodetect_candidates() {
        Ok(candidates) => candidates,
        Err(e) => {
            warn!("Error getting candidates from buffer: {}", e);
            vec![]
        }
    };

    // Combine the candidates for force detection
    let all_candidates: HashSet<String> = interfaces_to_force
        .into_iter()
        .chain(buffer_candidates.into_iter())
        .collect();

    // Process all candidates
    for interface_name in all_candidates {
        if interface_tracker.get(&interface_name).is_none() {
            // Check if we have metrics in the HashMap
            let hash_metrics_count = unknown_metrics
                .get(&interface_name)
                .map(|m| m.len())
                .unwrap_or(0);

            // Check if we have metrics in the MetricsBuffer
            let buffer_metrics_count = match unknown_metrics_buffer.count_for_entity(&interface_name) {
                Ok(count) => count,
                Err(_) => 0,
            };

            let total_metrics = hash_metrics_count + buffer_metrics_count;

            info!(
                "Forcing detection for unknown interface with {} buffered metrics: {}",
                total_metrics, interface_name
            );

            // Try to detect this specific interface explicitly
            if let Ok(interface) = discovery::force_interface_detection(
                Arc::clone(&client),
                host_id,
                &interface_name,
                max_retries,
            )
            .await
            {
                // Update the tracker with the new interface
                let mut updated_interfaces = interface_tracker.get_all().clone();
                updated_interfaces.insert(interface_name.clone(), interface);
                let (added, _) = interface_tracker.update(updated_interfaces);

                if !added.is_empty() {
                    // Process buffered metrics for this interface from HashMap
                    if let Some(metrics) = unknown_metrics.remove(&interface_name) {
                        // Store the processed metrics count for this interface
                        let mut interface_processed = 0;
                        let metrics_len = metrics.len();

                        for metric in metrics {
                            if let Some(interface) = interface_tracker.get(&interface_name) {
                                let interface_id = interface.interface_id;

                                if let Ok(_) = store_metric(
                                    Arc::clone(&client),
                                    interface_id,
                                    &metric,
                                    max_retries,
                                )
                                .await
                                {
                                    processed_count += 1;
                                    interface_processed += 1;
                                }
                            }
                        }

                        debug!(
                            "Processed {}/{} buffered metrics from HashMap for newly detected interface {}",
                            interface_processed, metrics_len, interface_name
                        );
                    }

                    // Process buffered metrics for this interface from MetricsBuffer
                    match unknown_metrics_buffer.take_for_entity(&interface_name) {
                        Ok(metrics) => {
                            let metrics_len = metrics.len();
                            if metrics_len > 0 {
                                // Store the processed metrics count for this interface
                                let mut interface_processed = 0;

                                for metric in metrics {
                                    if let Some(interface) = interface_tracker.get(&interface_name) {
                                        let interface_id = interface.interface_id;

                                        if let Ok(_) = store_metric(
                                            Arc::clone(&client),
                                            interface_id,
                                            &metric,
                                            max_retries,
                                        )
                                        .await
                                        {
                                            processed_count += 1;
                                            interface_processed += 1;
                                        }
                                    }
                                }

                                debug!(
                                    "Processed {}/{} buffered metrics from MetricsBuffer for newly detected interface {}",
                                    interface_processed, metrics_len, interface_name
                                );
                            }
                        },
                        Err(e) => {
                            warn!("Error taking metrics from buffer for {}: {}", interface_name, e);
                        }
                    }
                }
            } else {
                debug!(
                    "Failed to force detection for interface {}, metrics will remain buffered",
                    interface_name
                );
            }
        }
    }

    Ok(processed_count)
}

async fn process_metrics_batch(
    client: Arc<Client>,
    interface_tracker: &InterfaceTracker,
    metrics: Vec<NetworkMetric>,
    max_retries: usize,
    unknown_metrics: &mut HashMap<String, Vec<NetworkMetric>>,
    unknown_metrics_buffer: &Arc<MetricsBuffer<NetworkMetric>>,
) -> Result<usize> {
    let mut stored_count = 0;
    let mut unknown_count = 0;

    trace!("Processing batch of {} metrics", metrics.len());

    for metric in metrics {
        if let Some(interface) = interface_tracker.get(&metric.interface_name) {
            // Use the helper function to store the metric
            if let Ok(_) = store_metric(
                Arc::clone(&client),
                interface.interface_id,
                &metric,
                max_retries,
            )
            .await
            {
                stored_count += 1;
            }
        } else {
            // Store the metric in the macready buffer
            match unknown_metrics_buffer.add(metric.clone()) {
                Ok(_) => {
                    // Successfully added to the macready buffer
                },
                Err(e) => {
                    // If there's an error with the macready buffer, fall back to HashMap
                    warn!("Error adding to MetricsBuffer: {}, using HashMap fallback", e);
                    unknown_metrics
                        .entry(metric.interface_name.clone())
                        .or_insert_with(Vec::new)
                        .push(metric.clone());
                }
            }

            // For backward compatibility, also store in the HashMap
            unknown_metrics
                .entry(metric.interface_name.clone())
                .or_insert_with(Vec::new)
                .push(metric);

            unknown_count += 1;
        }
    }

    // Only log at debug level about buffered metrics
    if unknown_count > 0 {
        trace!(
            "Buffered {} metrics for {} unknown interfaces",
            unknown_count,
            unknown_metrics
                .keys()
                .collect::<std::collections::HashSet<_>>()
                .len()
        );
    }

    Ok(stored_count)
}

// Helper function to store a single metric
async fn store_metric(
    client: Arc<Client>,
    interface_id: Uuid,
    metric: &NetworkMetric,
    max_retries: usize,
) -> Result<()> {
    // Use our existing execute_with_retry function, not macready's
    database::execute_with_retry(
        || {
            let client = Arc::clone(&client);
            let timestamp = metric.timestamp;
            let input_bytes = metric.input_bytes;
            let input_packets = metric.input_packets;
            let output_bytes = metric.output_bytes;
            let output_packets = metric.output_packets;
            let interface_id = interface_id;

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
        },
        max_retries,
    )
    .await?;

    Ok(())
}

// Start a continuous metrics collection in the background
pub async fn start_continuous_metrics_collection() -> Result<mpsc::Receiver<MetricMessage>> {
    // Create a channel for sending metrics from the background thread
    let (tx, rx) = mpsc::channel::<MetricMessage>(100);

    // Spawn a dedicated thread for the blocking I/O operations
    info!("Starting metrics collection thread");
    thread::spawn(move || {
        let result = continuous_dlstat_collection(tx);
        if let Err(e) = result {
            error!("Metrics collection thread error: {}", e);
        }
    });

    Ok(rx)
}

// Function to run in the background thread that continuously reads from dlstat
fn continuous_dlstat_collection(tx: mpsc::Sender<MetricMessage>) -> Result<()> {
    // Start dlstat with interval mode
    debug!("Starting dlstat process with interval mode");
    let mut child = Command::new("/usr/sbin/dlstat")
        .args(&["-i", "1"]) // 1 second interval
        .stdout(Stdio::piped())
        .spawn()
        .context("Failed to run dlstat -i command")?;

    let stdout = child.stdout.take().unwrap();
    let reader = BufReader::new(stdout);

    let mut line_count = 0;
    let mut section_count = 0;
    let mut current_metrics = Vec::new();
    let mut is_collecting = false;

    info!("dlstat process started, reading data...");

    // Read lines continuously
    for line in reader.lines() {
        let line = line.context("Failed to read line from dlstat")?;

        // Check for header line which indicates a new section of data
        if line.contains("LINK") && line.contains("IPKTS") {
            // Process completed section if we have metrics
            if is_collecting && !current_metrics.is_empty() {
                send_metrics_batch(&tx, &current_metrics)?;
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

        // Parse the line and add to current batch
        if let Some(metric) = parse_dlstat_line(&line)? {
            current_metrics.push(metric);
        }
    }

    // If we get here, the process terminated
    warn!("dlstat process terminated");
    Ok(())
}

fn send_metrics_batch(tx: &mpsc::Sender<MetricMessage>, metrics: &[NetworkMetric]) -> Result<()> {
    // Create a message with the collected metrics
    let message = MetricMessage {
        metrics: metrics.to_vec(),
    };

    trace!("Sending {} metrics from dlstat", message.metrics.len());

    // Try to send the metrics to the channel
    tx.blocking_send(message)
        .map_err(|e| anyhow::anyhow!("Failed to send metrics: {}", e))?;

    Ok(())
}

fn parse_dlstat_line(line: &str) -> Result<Option<NetworkMetric>> {
    // Parse the line
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() >= 5 {
        let interface_name = fields[0].to_string();

        // Parse the values - handle units (K, M, G) if present
        let input_packets = parse_metric_value(fields[1])?;
        let input_bytes = parse_metric_value(fields[2])?;
        let output_packets = parse_metric_value(fields[3])?;
        let output_bytes = parse_metric_value(fields[4])?;

        return Ok(Some(NetworkMetric {
            interface_name,
            input_bytes,
            input_packets,
            output_bytes,
            output_packets,
            timestamp: Utc::now(),
        }));
    }

    Ok(None)
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
        Err(_) => Err(anyhow::anyhow!("Failed to parse value: {}", value_str)),
    }
}

// Implement macready's MetricPoint trait for NetworkMetric
impl macready::collector::MetricPoint for NetworkMetric {
    // We need to specify this as just NetworkInterface - not the fully qualified path
    type EntityType = NetworkInterface;

    fn entity_id(&self) -> Option<&<Self::EntityType as macready::entity::Entity>::Id> {
        None // We don't have the entity ID in the metric yet - it gets resolved later
    }

    fn entity_name(&self) -> &str {
        &self.interface_name
    }

    fn timestamp(&self) -> chrono::DateTime<Utc> {
        self.timestamp
    }

    fn values(&self) -> std::collections::HashMap<String, i64> {
        let mut values = std::collections::HashMap::new();
        values.insert("input_bytes".to_string(), self.input_bytes);
        values.insert("input_packets".to_string(), self.input_packets);
        values.insert("output_bytes".to_string(), self.output_bytes);
        values.insert("output_packets".to_string(), self.output_packets);
        values
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "interface_name": self.interface_name,
            "interface_id": null, // We don't have the ID yet
            "input_bytes": self.input_bytes,
            "input_packets": self.input_packets,
            "output_bytes": self.output_bytes,
            "output_packets": self.output_packets,
            "timestamp": self.timestamp.to_rfc3339(),
        })
    }

    fn collection_method(&self) -> &str {
        "dlstat"
    }
}
