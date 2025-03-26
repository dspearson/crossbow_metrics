// src/main.rs - add macready dependency
mod config;
mod database;
mod discovery;
mod metrics;
mod models;
mod errors;
mod database_adapter;

use anyhow::{Context, Result};
use clap::Parser;
use config::{AppConfig, Args, LogLevel};
use env_logger::{Builder, Env};
use log::{debug, error, info};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI arguments
    let args = Args::parse();

    // Initialise logging
    initialise_logging(&args);

    // Load configuration
    let config = load_configuration(&args)?;

    // Get hostname
    let hostname = determine_hostname(&args)?;

    // Display some basic info
    info!("Database hosts: {}", config.database.hosts.join(", "));
    debug!("Database connection URL format: postgresql://username:***@hosts/database");

    // Connect to the database
    let client = connect_to_database(&config).await?;

    // Start a background task to periodically check database connection health
    let _health_check_handle = database::start_connection_health_check(Arc::clone(&client));

    // Get max retries
    let max_retries = config.max_retries.unwrap_or(5);

    // Start the metrics collection loop
    info!("Starting metrics collection for host {}", hostname);
    metrics::collect_metrics(client, max_retries, &hostname, args.verbose).await?;

    Ok(())
}

fn initialise_logging(args: &Args) {
    let level = if args.quiet {
        LogLevel::Error.to_filter()
    } else {
        args.log_level.to_filter()
    };

    // Create a default environment and override the log level
    let env = Env::default().default_filter_or(level.to_string());

    // Initialise with custom format
    Builder::from_env(env)
        .format(|buf, record| {
            use chrono::Local;
            use std::io::Write;

            let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
            writeln!(
                buf,
                "{} {} [{}] {}",
                timestamp,
                record.level(),
                record.target(),
                record.args()
            )
        })
        .init();

    // Log startup information at debug level instead of info
    debug!("Logging initialized at level: {}", level);
}

fn load_configuration(args: &Args) -> Result<AppConfig> {
    AppConfig::load(&args.config).context(format!("Failed to load config from {}", args.config))
}

fn determine_hostname(args: &Args) -> Result<String> {
    match &args.hostname {
        Some(h) => Ok(h.clone()),
        None => config::get_hostname(),
    }
}

async fn connect_to_database(config: &AppConfig) -> Result<Arc<tokio_postgres::Client>> {
    // Get database connection string
    let db_url = config.get_connection_string();

    // Connect to the database with TLS support
    info!("Establishing database connection...");
    match database_adapter::establish_connection(&db_url, &config.database.sslmode).await {
        Ok(client) => {
            info!("Successfully connected to database");
            Ok(client)
        }
        Err(e) => {
            error!(
                "Critical error: Failed to establish database connection: {}",
                e
            );
            error!("Aborting startup since database connection is required");
            Err(e)
        }
    }
}
