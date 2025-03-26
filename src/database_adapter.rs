// src/database_adapter.rs
use anyhow::Result;
use log::{debug, info};
use macready::config::{DatabaseConfig, SslMode};
use macready::connection::health::{HealthCheck, HealthStatus};
use macready::connection::postgres::PostgresProvider;
use std::sync::Arc;
use tokio_postgres::Client;

/// Establish a connection to the database using macready
pub async fn establish_connection(
    db_url: &str,
    sslmode: &str,
) -> Result<Arc<Client>> {
    // Parse the connection URL to get the components
    let (username, password, hosts, database) = parse_db_url(db_url)?;

    // Convert the connection parameters to macready's format
    let ssl_mode = match sslmode.to_lowercase().as_str() {
        "disable" => SslMode::Disable,
        "allow" => SslMode::Allow,
        "prefer" => SslMode::Prefer,
        "require" => SslMode::Require,
        "verify-ca" => SslMode::VerifyCa,
        "verify-full" => SslMode::VerifyFull,
        _ => SslMode::Disable,
    };

    debug!("Connecting to database with SSL mode: {}", sslmode);

    let db_config = DatabaseConfig {
        host: hosts.first().unwrap_or(&"localhost".to_string()).clone(),
        port: 5432,
        name: database,
        username,
        password,
        ssl_mode,
        ca_cert: None,
        client_cert: None,
        client_key: None,
    };

    // For the initial port, we'll use tokio-postgres directly
    // because it matches the existing API better
    info!("Establishing database connection to {}:{}/{}", db_config.host, db_config.port, db_config.name);

    let (client, connection) = tokio_postgres::connect(db_url, tokio_postgres::NoTls)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to database: {}", e))?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create a health check for this connection
    let health_status = Arc::new(HealthStatus::new("database_connection"));

    // Test a simple query to verify the connection works
    client.execute("SELECT 1", &[])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to execute test query: {}", e))?;

    info!("Successfully connected to database");

    Ok(Arc::new(client))
}

/// Parse a database URL into its components
fn parse_db_url(url: &str) -> Result<(String, String, Vec<String>, String)> {
    // Simple parsing, would need to be more robust in production
    let url = url.strip_prefix("postgresql://").ok_or_else(|| {
        anyhow::anyhow!("Invalid PostgreSQL URL format")
    })?;

    let auth_and_rest: Vec<&str> = url.split('@').collect();
    if auth_and_rest.len() != 2 {
        return Err(anyhow::anyhow!("Invalid URL format"));
    }

    let auth: Vec<&str> = auth_and_rest[0].split(':').collect();
    if auth.len() != 2 {
        return Err(anyhow::anyhow!("Invalid authentication format"));
    }

    let username = auth[0].to_string();
    let password = auth[1].to_string();

    let host_and_db: Vec<&str> = auth_and_rest[1].split('/').collect();
    if host_and_db.len() != 2 {
        return Err(anyhow::anyhow!("Invalid host/database format"));
    }

    // Handle port in host part
    let host_parts: Vec<&str> = host_and_db[0].split(':').collect();
    let host = host_parts[0].to_string();

    let hosts = vec![host];
    let database = host_and_db[1].to_string();

    Ok((username, password, hosts, database))
}
