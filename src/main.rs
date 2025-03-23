use anyhow::{Context, Result, Error};
use chrono::{DateTime, Utc};
use clap::Parser;
use config::{Config, ConfigError, File};
use rand::random;
use serde::Deserialize;
use std::collections::HashMap;
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::pin::Pin;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to config file
    #[arg(long, default_value = "config.toml")]
    config: String,

    /// Hostname to use for metrics collection
    #[arg(long)]
    hostname: Option<String>,

    /// Collection interval in seconds (overrides config file if specified)
    #[arg(short, long)]
    interval: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct DatabaseConfig {
    username: String,
    password: String,
    hosts: Vec<String>,
    port: u16,
    database: String,
    sslmode: String,
}

#[derive(Debug, Deserialize)]
struct AppConfig {
    database: DatabaseConfig,
    interval: Option<u64>,
    max_retries: Option<usize>,
}

impl AppConfig {
    fn load(config_path: &str) -> Result<Self, ConfigError> {
        let config = Config::builder()
            .add_source(File::with_name(config_path))
            .build()?;

        config.try_deserialize()
    }

    fn get_connection_string(&self) -> String {
        // Join multiple hosts with commas
        let hosts_with_ports: Vec<String> = self.database.hosts
            .iter()
            .map(|host| format!("{}:{}", host, self.database.port))
            .collect();

        let hosts = hosts_with_ports.join(",");

        format!(
            "postgresql://{}:{}@{}/{}?sslmode={}",
            self.database.username,
            self.database.password,
            hosts,
            self.database.database,
            self.database.sslmode
        )
    }
}

#[derive(Debug)]
struct NetworkInterface {
    interface_id: Uuid,
    #[allow(dead_code)]
    host_id: Uuid,
    #[allow(dead_code)]
    zone_id: Option<Uuid>,
    #[allow(dead_code)]
    interface_name: String,
    #[allow(dead_code)]
    interface_type: String,
    #[allow(dead_code)]
    parent_interface: Option<String>,
}

#[derive(Debug)]
struct NetworkMetric {
    interface_name: String,
    input_bytes: i64,
    input_packets: i64,
    output_bytes: i64,
    output_packets: i64,
    timestamp: DateTime<Utc>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Load configuration
    let config = AppConfig::load(&args.config)
        .context(format!("Failed to load config from {}", args.config))?;

    // Get database connection string (credentials not visible in ps)
    let db_url = config.get_connection_string();

    // Get interval from CLI args or config file
    let interval = args.interval.unwrap_or_else(|| config.interval.unwrap_or(60));

    // Get max retries
    let max_retries = config.max_retries.unwrap_or(5);

    // Get system hostname if not provided
    let hostname = match args.hostname {
        Some(h) => h,
        None => {
            let output = Command::new("hostname")
                .output()
                .context("Failed to run hostname command")?;
            String::from_utf8(output.stdout)
                .context("Invalid UTF-8 in hostname output")?
                .trim()
                .to_string()
        }
    };

    println!("Collecting network metrics for host: {}", hostname);
    println!("Collection interval: {} seconds", interval);
    println!("Database hosts: {}", config.database.hosts.join(", "));

    // Connect to the database
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls)
        .await
        .context("Failed to connect to database")?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Database connection error: {}", e);
        }
    });

    // Wrap client in Arc for thread-safe sharing
    let client = Arc::new(client);

    // Ensure host exists
    let host_id = ensure_host_exists(Arc::clone(&client), &hostname, max_retries).await?;

    // Discover zones
    let zones = discover_zones(Arc::clone(&client), host_id, max_retries).await?;

    // Discover interfaces and build a mapping
    let interface_map = discover_interfaces(Arc::clone(&client), host_id, &zones, max_retries).await?;

    // Start the metrics collection loop
    collect_metrics(client, &interface_map, interval, max_retries).await?;

    Ok(())
}

async fn execute_with_retry<F, T>(f: F, max_retries: usize) -> Result<T>
where
    F: Fn() -> Pin<Box<dyn Future<Output = Result<T>> + Send>> + Send + Sync,
{
    let mut retries = 0;
    let mut delay = Duration::from_millis(100);

    loop {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                retries += 1;
                if retries >= max_retries {
                    return Err(e);
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

async fn ensure_host_exists(client: Arc<Client>, hostname: &str, max_retries: usize) -> Result<Uuid> {
    // Check if host exists
    let hostname = hostname.to_string(); // Clone to avoid reference issues

    let host_id = execute_with_retry(move || {
        let client = Arc::clone(&client);
        let hostname = hostname.clone();

        Box::pin(async move {
            let row = client
                .query_opt(
                    "SELECT host_id FROM hosts WHERE hostname = $1",
                    &[&hostname],
                )
                .await
                .context("Failed to query host")?;

            let host_id = match row {
                Some(row) => {
                    let host_id: Uuid = row.get(0);
                    println!("Found existing host record: {}", host_id);
                    host_id
                }
                None => {
                    // Create new host
                    let host_id = Uuid::new_v4();
                    client
                        .execute(
                            "INSERT INTO hosts (host_id, hostname, created_at) VALUES ($1, $2, CURRENT_TIMESTAMP)",
                            &[&host_id, &hostname],
                        )
                        .await
                        .context("Failed to insert host")?;
                    println!("Created new host record: {}", host_id);
                    host_id
                }
            };

            Ok::<_, Error>(host_id)
        })
    }, max_retries)
    .await?;

    Ok(host_id)
}

async fn discover_zones(client: Arc<Client>, host_id: Uuid, max_retries: usize) -> Result<HashMap<String, Uuid>> {
    let mut zones = HashMap::new();

    // Get zone list from system
    let output = Command::new("/usr/sbin/zoneadm")
        .arg("list")
        .arg("-p")
        .output()
        .context("Failed to run zoneadm command")?;

    let zone_output = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in zoneadm output")?;

    for line in zone_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let zone_name = fields[1].to_string();
            let zone_status = if fields.len() >= 3 {
                Some(fields[2].to_string())
            } else {
                None
            };

            // Check if zone exists in database with retry logic
            let zone_client = Arc::clone(&client);
            let zone_host_id = host_id;
            let zone_name_clone = zone_name.clone();
            let zone_status_clone = zone_status.clone();

            let zone_id = execute_with_retry(move || {
                let client = Arc::clone(&zone_client);
                let host_id = zone_host_id;
                let zone_name = zone_name_clone.clone();
                let zone_status = zone_status_clone.clone();

                Box::pin(async move {
                    let row = client
                        .query_opt(
                            "SELECT zone_id FROM zones WHERE host_id = $1 AND zone_name = $2",
                            &[&host_id, &zone_name],
                        )
                        .await
                        .context("Failed to query zone")?;

                    let zone_id = match row {
                        Some(row) => {
                            let id: Uuid = row.get(0);
                            // Update zone status if available
                            if let Some(status) = &zone_status {
                                client
                                    .execute(
                                        "UPDATE zones SET zone_status = $1 WHERE zone_id = $2",
                                        &[status, &id],
                                    )
                                    .await
                                    .context("Failed to update zone status")?;
                            }
                            id
                        }
                        None => {
                            // Create new zone
                            let id = Uuid::new_v4();
                            client
                                .execute(
                                    "INSERT INTO zones (zone_id, host_id, zone_name, zone_status, created_at)
                                     VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)",
                                    &[&id, &host_id, &zone_name, &zone_status],
                                )
                                .await
                                .context("Failed to insert zone")?;
                            println!("Created new zone record: {} - {}", zone_name, id);
                            id
                        }
                    };

                    Ok::<_, Error>(zone_id)
                })
            }, max_retries)
            .await?;

            zones.insert(zone_name, zone_id);
        }
    }

    println!("Discovered {} zones", zones.len());
    Ok(zones)
}

async fn discover_interfaces(
    client: Arc<Client>,
    host_id: Uuid,
    _zones: &HashMap<String, Uuid>,
    max_retries: usize,
) -> Result<HashMap<String, NetworkInterface>> {
    let mut interfaces = HashMap::new();

    // Get physical interfaces using dladm
    let output = Command::new("/usr/sbin/dladm")
        .args(&["show-phys", "-p", "-o", "link,class,state,mtu"])
        .output()
        .context("Failed to run dladm show-phys command")?;

    let phys_output = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in dladm output")?;

    // Process physical interfaces
    for line in phys_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 3 {
            let interface_name = fields[0].to_string();
            let interface_type = fields[1].to_string();

            // Store interface in database and get UUID
            let interface_id = ensure_interface_exists(
                Arc::clone(&client),
                host_id,
                None, // No zone for physical interfaces
                interface_name.clone(),
                interface_type.clone(),
                None, // No parent for physical interfaces
                max_retries,
            ).await?;

            interfaces.insert(interface_name.clone(), NetworkInterface {
                interface_id,
                host_id,
                zone_id: None,
                interface_name,
                interface_type,
                parent_interface: None,
            });
        }
    }

    // Get virtual interfaces (VNICs)
    let output = Command::new("/usr/sbin/dladm")
        .args(&["show-vnic", "-p", "-o", "link,over"])
        .output()
        .context("Failed to run dladm show-vnic command")?;

    let vnic_output = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in dladm vnic output")?;

    // Process VNICs
    for line in vnic_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].to_string();
            let parent_interface = fields[1].to_string();

            // Attempt to determine which zone this VNIC belongs to
            let zone_id = None;

            // Store interface in database and get UUID
            let interface_id = ensure_interface_exists(
                Arc::clone(&client),
                host_id,
                zone_id,
                interface_name.clone(),
                "vnic".to_string(),
                Some(parent_interface.clone()),
                max_retries,
            ).await?;

            interfaces.insert(interface_name.clone(), NetworkInterface {
                interface_id,
                host_id,
                zone_id,
                interface_name,
                interface_type: "vnic".to_string(),
                parent_interface: Some(parent_interface),
            });
        }
    }

    // Check for etherstubs as well
    let output = Command::new("/usr/sbin/dladm")
        .args(&["show-etherstub", "-p"])
        .output()
        .context("Failed to run dladm show-etherstub command")?;

    let etherstub_output = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in dladm etherstub output")?;

    // Process etherstubs
    for line in etherstub_output.lines() {
        let etherstub_name = line.trim().to_string();
        if !etherstub_name.is_empty() {
            // Store interface in database and get UUID
            let interface_id = ensure_interface_exists(
                Arc::clone(&client),
                host_id,
                None,
                etherstub_name.clone(),
                "etherstub".to_string(),
                None,
                max_retries,
            ).await?;

            interfaces.insert(etherstub_name.clone(), NetworkInterface {
                interface_id,
                host_id,
                zone_id: None,
                interface_name: etherstub_name,
                interface_type: "etherstub".to_string(),
                parent_interface: None,
            });
        }
    }

    println!("Discovered {} interfaces", interfaces.len());
    Ok(interfaces)
}

async fn ensure_interface_exists(
    client: Arc<Client>,
    host_id: Uuid,
    zone_id: Option<Uuid>,
    interface_name: String,
    interface_type: String,
    parent_interface: Option<String>,
    max_retries: usize,
) -> Result<Uuid> {
    // Execute with retry logic
    let interface_id = execute_with_retry(move || {
        let client = Arc::clone(&client);
        let host_id = host_id;
        let zone_id = zone_id;
        let interface_name = interface_name.clone();
        let interface_type = interface_type.clone();
        let parent_interface = parent_interface.clone();

        Box::pin(async move {
            let row = client
                .query_opt(
                    "SELECT interface_id FROM interfaces
                     WHERE host_id = $1 AND interface_name = $2 AND (zone_id = $3 OR (zone_id IS NULL AND $3 IS NULL))",
                    &[&host_id, &interface_name, &zone_id],
                )
                .await
                .context("Failed to query interface")?;

            let interface_id = match row {
                Some(row) => {
                    let interface_id: Uuid = row.get(0);
                    // Update interface information
                    client
                        .execute(
                            "UPDATE interfaces SET
                             interface_type = $1,
                             parent_interface = $2,
                             is_active = true
                             WHERE interface_id = $3",
                            &[&interface_type, &parent_interface, &interface_id],
                        )
                        .await
                        .context("Failed to update interface")?;
                    interface_id
                }
                None => {
                    // Create new interface
                    let interface_id = Uuid::new_v4();
                    client
                        .execute(
                            "INSERT INTO interfaces (
                             interface_id, host_id, zone_id, interface_name,
                             interface_type, parent_interface, is_active, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, true, CURRENT_TIMESTAMP)",
                            &[&interface_id, &host_id, &zone_id, &interface_name, &interface_type, &parent_interface],
                        )
                        .await
                        .context("Failed to insert interface")?;
                    println!("Created new interface record: {} - {}", interface_name, interface_id);
                    interface_id
                }
            };

            Ok::<_, Error>(interface_id)
        })
    }, max_retries)
    .await?;

    Ok(interface_id)
}

async fn collect_metrics(
    client: Arc<Client>,
    interface_map: &HashMap<String, NetworkInterface>,
    interval_secs: u64,
    max_retries: usize,
) -> Result<()> {
    println!("Starting metrics collection with interval: {} seconds", interval_secs);

    // Create a interval timer
    let mut interval = time::interval(Duration::from_secs(interval_secs));

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
