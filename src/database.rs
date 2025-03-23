use anyhow::{Context, Error, Result};
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
        // Connect without TLS
        let (client, connection) = tokio_postgres::connect(db_url, tokio_postgres::NoTls)
            .await
            .context("Failed to connect to database without TLS")?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Database connection error: {}", e);
            }
        });

        return Ok(Arc::new(client));
    }

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
            eprintln!("Database connection error: {}", e);
        }
    });

    Ok(Arc::new(client))
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
                eprintln!("Database operation failed (retry {}/{}): {}", retries, max_retries, e);
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
