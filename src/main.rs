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

    // Get max retries
    let max_retries = config.max_retries.unwrap_or(5);

    // Get system hostname if not provided
    let hostname = match args.hostname {
        Some(h) => h,
        None => config::get_hostname()?,
    };

    println!("Database hosts: {}", config.database.hosts.join(", "));

    // Connect to the database with TLS support
    println!("Establishing database connection...");
    let client = match database::establish_connection(&db_url, &config.database.sslmode).await {
        Ok(client) => {
            println!("Successfully connected to database");
            client
        },
        Err(e) => {
            eprintln!("Critical error: Failed to establish database connection: {}", e);
            eprintln!("Aborting startup since database connection is required");
            return Err(e);
        }
    };

    // Start a background task to periodically check database connection health
    let _health_check_handle = database::start_connection_health_check(Arc::clone(&client));

    // Start the metrics collection loop
    println!("Starting metrics collection for host {}", hostname);
    metrics::collect_metrics(client, max_retries, &hostname).await?;

    Ok(())
}
