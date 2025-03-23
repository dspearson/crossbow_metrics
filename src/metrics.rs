use crate::database::execute_with_retry;
use crate::models::{NetworkInterface, NetworkMetric};
use anyhow::{Context, Result};
use chrono::Utc;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tokio_postgres::Client;
use uuid::Uuid;

pub async fn collect_metrics(
    client: Arc<Client>,
    interface_map: &HashMap<String, NetworkInterface>,
    interval_secs: u64,
    max_retries: usize,
    hostname: &str,  // Add hostname parameter
) -> Result<()> {
    println!("Starting metrics collection for host {} with interval: {} seconds",
             hostname, interval_secs);

    // Create a interval timer
    let mut interval = time::interval(Duration::from_secs(interval_secs));

    // Get a set of unique host_ids from our interfaces for reporting
    let host_ids: std::collections::HashSet<Uuid> = interface_map
        .values()
        .map(|interface| interface.host_id)
        .collect();

    println!("Monitoring interfaces across {} hosts", host_ids.len());
    for host_id in &host_ids {
        println!("Host ID: {}", host_id);
    }

    loop {
        interval.tick().await;

        // Collect metrics using dlstat
        match collect_dlstat_metrics(Arc::clone(&client), interface_map, max_retries).await {
            Ok(_) => println!("Successfully collected metrics"),
            Err(e) => eprintln!("Error collecting metrics: {}", e),
        }
    }
}

async fn collect_dlstat_metrics(
    client: Arc<Client>,
    interface_map: &HashMap<String, NetworkInterface>,
    max_retries: usize,
) -> Result<()> {
    // First try to use dlstat -i to get interval metrics if available
    let metrics = match collect_from_dlstat_interval().await {
        Ok(m) => m,
        Err(e) => {
            println!("Interval dlstat failed ({}), falling back to regular dlstat", e);
            collect_from_dlstat().await?
        }
    };

    // Store metrics in database with retry logic
    for metric in metrics {
        if let Some(interface) = interface_map.get(&metric.interface_name) {
            let client = Arc::clone(&client);
            let interface_id = interface.interface_id;
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

            println!("Collecting metrics for {} (type: {}, {}, {}) - in: {}b/{}p, out: {}b/{}p",
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
        } else {
            println!("Unknown interface: {}", metric.interface_name);
        }
    }

    Ok(())
}

// Try to collect metrics using dlstat with interval option
async fn collect_from_dlstat_interval() -> Result<Vec<NetworkMetric>> {
    let mut metrics = Vec::new();

    // Run dlstat with a 1-second interval to get rate metrics
    let mut child = Command::new("/usr/sbin/dlstat")
        .args(&["-i", "1", "1"]) // 1 second interval, 1 report
        .stdout(Stdio::piped())
        .spawn()
        .context("Failed to run dlstat -i command")?;

    let stdout = child.stdout.take().unwrap();
    let reader = BufReader::new(stdout);
    let mut line_count = 0;

    // Wait for the command to complete - no await as this is a std::process::Child
    let status = child.wait().context("Failed to wait for dlstat process")?;
    if !status.success() {
        return Err(anyhow::anyhow!("dlstat -i command failed with status: {}", status));
    }

    // Parse the output
    for line in reader.lines() {
        let line = line.context("Failed to read line from dlstat")?;
        line_count += 1;

        // Skip header lines (first two lines)
        if line_count <= 2 {
            continue;
        }

        // Parse the line
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() >= 5 {
            let interface_name = fields[0].to_string();
            let input_bytes: i64 = fields[1].parse().context("Failed to parse input bytes")?;
            let input_packets: i64 = fields[2].parse().context("Failed to parse input packets")?;
            let output_bytes: i64 = fields[3].parse().context("Failed to parse output bytes")?;
            let output_packets: i64 = fields[4].parse().context("Failed to parse output packets")?;

            metrics.push(NetworkMetric {
                interface_name,
                input_bytes,
                input_packets,
                output_bytes,
                output_packets,
                timestamp: Utc::now(),
            });
        }
    }

    Ok(metrics)
}

// Fallback to regular dlstat
async fn collect_from_dlstat() -> Result<Vec<NetworkMetric>> {
    let mut metrics = Vec::new();

    // Run regular dlstat
    let output = Command::new("/usr/sbin/dlstat")
        .stdout(Stdio::piped())
        .spawn()
        .context("Failed to run dlstat command")?;

    let stdout = output.stdout.unwrap();
    let reader = BufReader::new(stdout);
    let mut line_count = 0;

    for line in reader.lines() {
        let line = line.context("Failed to read line from dlstat")?;
        line_count += 1;

        // Skip header lines (first two lines)
        if line_count <= 2 {
            continue;
        }

        // Parse the line
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() >= 5 {
            let interface_name = fields[0].to_string();
            let input_bytes: i64 = fields[1].parse().context("Failed to parse input bytes")?;
            let input_packets: i64 = fields[2].parse().context("Failed to parse input packets")?;
            let output_bytes: i64 = fields[3].parse().context("Failed to parse output bytes")?;
            let output_packets: i64 = fields[4].parse().context("Failed to parse output packets")?;

            metrics.push(NetworkMetric {
                interface_name,
                input_bytes,
                input_packets,
                output_bytes,
                output_packets,
                timestamp: Utc::now(),
            });
        }
    }

    Ok(metrics)
}
