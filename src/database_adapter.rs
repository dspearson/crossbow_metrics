// src/database_adapter.rs
use anyhow::Result;
use macready::config::{DatabaseConfig, SslMode};
use macready::connection::health::HealthStatus;
use macready::connection::postgres::PostgresProvider;
use std::sync::Arc;
use tokio_postgres::Client;

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

    // For now, let's fall back to the original connection method
    // because of the type compatibility issues
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

    // For a more integrated approach, we'd wrap this client with health checks
    // But for now, we'll keep it simple

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

    let hosts = vec![host_and_db[0].to_string()];
    let database = host_and_db[1].to_string();

    Ok((username, password, hosts, database))
}
