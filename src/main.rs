mod config;
mod database;
mod discovery;
mod metrics;
mod models;

use anyhow::{Context, Result};
use clap::Parser;
use config::{AppConfig, Args};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Load configuration
    let config = AppConfig::load(&args.config)
        .context(format!("Failed to load config from {}", args.config))?;

    // Get database connection string
    let db_url = config.get_connection_string();

    // Get interval from CLI args or config file
    let interval = args.interval.unwrap_or_else(|| config.interval.unwrap_or(60));

    // Get max retries
    let max_retries = config.max_retries.unwrap_or(5);

    // Get system hostname if not provided
    let hostname = match args.hostname {
        Some(h) => h,
        None => config::get_hostname()?,
    };

    println!("Collecting network metrics for host: {}", hostname);
    println!("Collection interval: {} seconds", interval);
    println!("Database hosts: {}", config.database.hosts.join(", "));

    // Connect to the database with TLS support
    let client = database::establish_connection(&db_url, &config.database.sslmode).await?;

    // Ensure host exists in the database before proceeding
    let _ = database::ensure_host_exists(Arc::clone(&client), &hostname, max_retries).await?;

    // Start the metrics collection loop
    metrics::collect_metrics(client, interval, max_retries, &hostname).await?;

    Ok(())
}
