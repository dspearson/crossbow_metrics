use crate::database::execute_with_retry;
use crate::models::{NetworkInterface, NetworkMetric};
use anyhow::{Context, Result};
use chrono::Utc;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use tokio::sync::mpsc;
use tokio_postgres::Client;
use uuid::Uuid;

// Define a struct for the metric message
#[derive(Debug, Clone)]
pub struct MetricMessage {
    pub metrics: Vec<NetworkMetric>,
}

pub async fn collect_metrics(
    client: Arc<Client>,
    interface_map: &HashMap<String, NetworkInterface>,
    _interval_secs: u64, // No longer needed since dlstat controls the interval
    max_retries: usize,
    hostname: &str,
) -> Result<()> {
    println!("Starting continuous metrics collection for host {}", hostname);

    // Print information about the interfaces we're monitoring
    let host_ids: std::collections::HashSet<Uuid> = interface_map
        .values()
        .map(|interface| interface.host_id)
        .collect();

    println!("Monitoring interfaces across {} hosts", host_ids.len());
    for host_id in &host_ids {
        println!("Host ID: {}", host_id);
    }

    // Start the continuous metrics collection
    let mut metrics_rx = start_continuous_metrics_collection().await?;

    // Process metrics as they arrive
    while let Some(message) = metrics_rx.recv().await {
        match process_metrics_batch(
            Arc::clone(&client),
            interface_map,
            message.metrics,
            max_retries
        ).await {
            Ok(count) => println!("Successfully stored {} metrics", count),
            Err(e) => eprintln!("Error storing metrics: {}", e),
        }
    }

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

            // Skip the first section (initial counts)
            is_collecting = section_count > 1;
            continue;
        }

        line_count += 1;

        // Skip header lines
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
    Ok(())
}

// Function to process a batch of metrics
async fn process_metrics_batch(
    client: Arc<Client>,
    interface_map: &HashMap<String, NetworkInterface>,
    metrics: Vec<NetworkMetric>,
    max_retries: usize,
) -> Result<usize> {
    let mut stored_count = 0;

    for metric in metrics {
        if let Some(interface) = interface_map.get(&metric.interface_name) {
            let client = Arc::clone(&client);
            let interface_id = interface.interface_id;
            let host_id = interface.host_id;
            let timestamp = metric.timestamp;
            let input_bytes = metric.input_bytes;
            let input_packets = metric.input_packets;
            let output_bytes = metric.output_bytes;
            let output_packets = metric.output_packets;

            // Enhanced logging with all interface information
            let zone_info = match &interface.zone_id {
                Some(id) => format!("zone: {}", id),
                None => "global zone".to_string(),
            };

            let parent_info = match &interface.parent_interface {
                Some(parent) => format!("parent: {}", parent),
                None => "no parent".to_string(),
            };

            println!("Collecting metrics for host {} interface {} (type: {}, {}, {}) - in: {}b/{}p, out: {}b/{}p",
                host_id,
                interface.interface_name,
                interface.interface_type,
                zone_info,
                parent_info,
                input_bytes,
                input_packets,
                output_bytes,
                output_packets
            );

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
            println!("Unknown interface: {}", metric.interface_name);
        }
    }

    Ok(stored_count)
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
