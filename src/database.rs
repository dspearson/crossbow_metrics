use crate::errors;
use anyhow::{Context, Result};
use log::{debug, error, info, trace, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::Client;

// Re-export macready's retry function for use in other modules
pub use macready::retry::{execute_with_retry, RetryConfig};
use macready::connection::health::HealthStatus;

pub async fn establish_connection(db_url: &str, sslmode: &str) -> Result<Arc<Client>> {
    // We're using the database_adapter as an intermediary to convert the connection string
    crate::database_adapter::establish_connection(db_url, sslmode).await
}

// Keep this function for compatibility with main.rs
pub async fn start_connection_health_check(client: Arc<Client>) -> tokio::task::JoinHandle<()> {
    info!("Starting periodic database connection health checks");
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;
            match validate_connection(Arc::clone(&client)).await {
                Ok(_) => {
                    trace!("Database connection health check passed");
                }
                Err(e) => {
                    warn!("Database connection health check failed: {}", e);
                }
            }
        }
    })
}

pub async fn validate_connection(client: Arc<Client>) -> Result<()> {
    debug!("Validating database connection...");

    // Set a short timeout for the validation query
    let timeout = Duration::from_secs(10);

    // Run a simple query with timeout
    match tokio::time::timeout(timeout, client.query_one("SELECT 1", &[])).await {
        Ok(Ok(_)) => {
            debug!("Database connection is valid");
            Ok(())
        }
        Ok(Err(e)) => {
            error!("Database validation failed: {}", e);
            Err(anyhow::anyhow!("Database validation failed: {}", e))
        }
        Err(_) => {
            error!(
                "Database validation timed out after {} seconds",
                timeout.as_secs()
            );
            Err(anyhow::anyhow!(
                "Database validation timed out after {} seconds",
                timeout.as_secs()
            ))
        }
    }
}

pub async fn ensure_host_exists(
    client: Arc<Client>,
    hostname: &str,
    max_retries: usize,
) -> Result<uuid::Uuid> {
    // Check if host exists
    let hostname = hostname.to_string(); // Clone to avoid reference issues

    // Use the existing execute_with_retry function with anyhow Result
    let retry_config = RetryConfig {
        max_attempts: max_retries,
        initial_delay_ms: 100,
        backoff_factor: 1.5,
        max_delay_ms: 30_000,
        jitter: true,
    };

    errors::to_anyhow_result(
        execute_with_retry(
            || {
                let client = Arc::clone(&client);
                let hostname = hostname.clone();

                Box::pin(async move {
                    errors::to_macready_result(find_or_create_host(&client, &hostname).await)
                })
            },
            retry_config,
            "ensure_host_exists",
        )
        .await
    )
}

async fn find_or_create_host(client: &Client, hostname: &str) -> Result<uuid::Uuid> {
    let row = client
        .query_opt(
            "SELECT host_id FROM hosts WHERE hostname = $1",
            &[&hostname],
        )
        .await
        .context("Failed to query host")?;

    match row {
        Some(row) => {
            let host_id: uuid::Uuid = row.get(0);
            debug!("Found existing host record: {}", host_id);
            Ok(host_id)
        }
        None => create_new_host(client, hostname).await,
    }
}

async fn create_new_host(client: &Client, hostname: &str) -> Result<uuid::Uuid> {
    // Create new host
    let host_id = uuid::Uuid::new_v4();
    client
        .execute(
            "INSERT INTO hosts (host_id, hostname, created_at) VALUES ($1, $2, CURRENT_TIMESTAMP)",
            &[&host_id, &hostname],
        )
        .await
        .context("Failed to insert host")?;
    info!("Created new host record: {}", host_id);
    Ok(host_id)
}
