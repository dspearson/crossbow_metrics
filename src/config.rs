use anyhow::{Context, Result};
use clap::Parser;
use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::process::Command;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Path to config file
    #[arg(long, default_value = "config.toml")]
    pub config: String,

    /// Hostname to use for metrics collection
    #[arg(long)]
    pub hostname: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub username: String,
    pub password: String,
    pub hosts: Vec<String>,
    pub port: u16,
    pub database: String,
    pub sslmode: String,
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub max_retries: Option<usize>,
}

impl AppConfig {
    pub fn load(config_path: &str) -> Result<Self, ConfigError> {
        let config = Config::builder()
            .add_source(File::with_name(config_path))
            .build()?;

        config.try_deserialize()
    }

    pub fn get_connection_string(&self) -> String {
        // Join multiple hosts with commas
        let hosts_with_ports: Vec<String> = self.database.hosts
            .iter()
            .map(|host| format!("{}:{}", host, self.database.port))
            .collect();

        let hosts = hosts_with_ports.join(",");

        // The sslmode will be handled separately in the code
        format!(
            "postgresql://{}:{}@{}/{}",
            self.database.username,
            self.database.password,
            hosts,
            self.database.database
        )
    }
}

pub fn get_hostname() -> Result<String> {
    let output = Command::new("hostname")
        .output()
        .context("Failed to run hostname command")?;

    String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in hostname output")
        .map(|s| s.trim().to_string())
}
