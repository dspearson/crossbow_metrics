use anyhow::{Context, Error, Result};
use log::{debug, error, info, trace, warn};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rand::random;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tokio_postgres::Client;
use uuid::Uuid;

pub async fn establish_connection(db_url: &str, sslmode: &str) -> Result<Arc<Client>> {
    if sslmode == "disable" {
        debug!("Connecting to database without TLS");
        // Connect without TLS
        let (client, connection) = tokio_postgres::connect(db_url, tokio_postgres::NoTls)
            .await
            .context("Failed to connect to database without TLS")?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Database connection error: {}", e);
            }
        });

        // Wrap the client in an Arc
        let client_arc = Arc::new(client);

        // Validate the connection before returning
        validate_connection(Arc::clone(&client_arc)).await?;

        return Ok(client_arc);
    }

    debug!("Connecting to database with TLS (sslmode={})", sslmode);
    // Set up TLS
    let tls_builder = TlsConnector::builder();  // Removed 'mut' as it's not needed

    // If you're using self-signed certificates during development,
    // uncomment the following line:
    // let tls_builder = tls_builder.danger_accept_invalid_certs(true);

    let tls_connector = tls_builder
        .build()
        .context("Failed to build TLS connector")?;

    let tls = MakeTlsConnector::new(tls_connector);

    // Connect with TLS
    let (client, connection) = tokio_postgres::connect(db_url, tls)
        .await
        .context("Failed to connect to database with TLS")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Database connection error: {}", e);
        }
    });

    // Wrap the client in an Arc
    let client_arc = Arc::new(client);

    // Validate the connection before returning
    validate_connection(Arc::clone(&client_arc)).await?;

    Ok(client_arc)
}

// New function to validate a database connection
pub async fn validate_connection(client: Arc<Client>) -> Result<()> {
    debug!("Validating database connection...");

    // Set a short timeout for the validation query
    let timeout = Duration::from_secs(10);

    // Run a simple query with timeout
    match time::timeout(timeout, client.query_one("SELECT 1", &[])).await {
        Ok(Ok(_)) => {
            debug!("Database connection is valid");
            Ok(())
        },
        Ok(Err(e)) => {
            error!("Database validation failed: {}", e);
            Err(anyhow::anyhow!("Database validation failed: {}", e))
        },
        Err(_) => {
            error!("Database validation timed out after {} seconds", timeout.as_secs());
            Err(anyhow::anyhow!("Database validation timed out after {} seconds", timeout.as_secs()))
        }
    }
}

// New function to periodically check connection health
pub async fn start_connection_health_check(client: Arc<Client>) -> tokio::task::JoinHandle<()> {
    info!("Starting periodic database connection health checks");
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;
            match validate_connection(Arc::clone(&client)).await {
                Ok(_) => {
                    trace!("Database connection health check passed");
                },
                Err(e) => {
                    warn!("Database connection health check failed: {}", e);
                    // You could potentially set a flag or send a message to a channel here
                    // to signal to the main process that the connection is unhealthy
                }
            }
        }
    })
}

pub async fn execute_with_retry<F, T>(f: F, max_retries: usize) -> Result<T>
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
                warn!("Database operation failed (retry {}/{}): {}", retries, max_retries, e);
                time::sleep(delay).await;

                // Exponential backoff with jitter
                delay = Duration::from_millis(
                    (delay.as_millis() as f64 * 1.5) as u64 +
                    random::<u64>() % 100
                );
            }
        }
    }
}

pub async fn ensure_host_exists(client: Arc<Client>, hostname: &str, max_retries: usize) -> Result<Uuid> {
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
                    debug!("Found existing host record: {}", host_id);
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
                    info!("Created new host record: {}", host_id);
                    host_id
                }
            };

            Ok::<_, Error>(host_id)
        })
    }, max_retries)
    .await?;

    Ok(host_id)
}
