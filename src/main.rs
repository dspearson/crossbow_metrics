mod config;
mod database;
mod discovery;
mod metrics;
mod models;

use anyhow::{Context, Result};
use clap::Parser;
use config::{AppConfig, Args};

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
    let client = database::establish_connection(&db_url, &config.database.sslmode).await?;

    // Start the metrics collection loop
    metrics::collect_metrics(client, max_retries, &hostname).await?;

    Ok(())
}
