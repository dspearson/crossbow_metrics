// src/database_adapter.rs
use anyhow::Result;
use log::{debug, info};
use std::sync::Arc;
use tokio_postgres::Client;

/// Establish a connection to the database - will be deprecated as we fully migrate to macready
pub async fn establish_connection(
    db_url: &str,
    sslmode: &str,
) -> Result<Arc<Client>> {
    info!("Establishing database connection using legacy adapter");
    debug!("Connection will use SSL mode: {}", sslmode);

    let (client, connection) = tokio_postgres::connect(db_url, tokio_postgres::NoTls)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to database: {}", e))?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Test a simple query to verify the connection works
    client.execute("SELECT 1", &[])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to execute test query: {}", e))?;

    info!("Successfully connected to database");

    Ok(Arc::new(client))
}
