use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::process::Command;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum LogLevel {
    /// Only show errors
    Error,
    /// Show errors and warnings
    Warn,
    /// Show errors, warnings, and info (default)
    Info,
    /// Show errors, warnings, info, and debug messages
    Debug,
    /// Show all messages including trace
    Trace,
}

impl FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "error" => Ok(LogLevel::Error),
            "warn" => Ok(LogLevel::Warn),
            "info" => Ok(LogLevel::Info),
            "debug" => Ok(LogLevel::Debug),
            "trace" => Ok(LogLevel::Trace),
            _ => Err(format!("Unknown log level: {}", s)),
        }
    }
}

impl LogLevel {
    pub fn to_filter(&self) -> log::LevelFilter {
        match self {
            LogLevel::Error => log::LevelFilter::Error,
            LogLevel::Warn => log::LevelFilter::Warn,
            LogLevel::Info => log::LevelFilter::Info,
            LogLevel::Debug => log::LevelFilter::Debug,
            LogLevel::Trace => log::LevelFilter::Trace,
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Path to config file
    #[arg(long, default_value = "config.toml")]
    pub config: String,

    /// Hostname to use for metrics collection
    #[arg(long)]
    pub hostname: Option<String>,

    /// Verbosity level for logging
    #[arg(short, long, default_value = "info")]
    pub log_level: LogLevel,

    /// Quiet mode (errors only, overrides log-level)
    #[arg(short, long)]
    pub quiet: bool,

    /// Show additional detail
    #[arg(short, long)]
    pub verbose: bool,
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
    #[allow(dead_code)]
    pub log_level: Option<String>,
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
        let hosts_with_ports: Vec<String> = self
            .database
            .hosts
            .iter()
            .map(|host| format!("{}:{}", host, self.database.port))
            .collect();

        let hosts = hosts_with_ports.join(",");

        // The sslmode will be handled separately in the code
        format!(
            "postgresql://{}:{}@{}/{}",
            self.database.username, self.database.password, hosts, self.database.database
        )
    }

    #[allow(dead_code)]
    pub fn get_log_level(&self) -> LogLevel {
        match &self.log_level {
            Some(level) => <LogLevel as FromStr>::from_str(level).unwrap_or(LogLevel::Info),
            None => LogLevel::Info,
        }
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
