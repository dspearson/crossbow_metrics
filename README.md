# Crossbow Metrics

A network metrics collector for OmniOS.

## Overview

Crossbow Metrics is a Rust-based utility that collects network interface metrics on OmniOS systems. It monitors network interfaces (including VNICs, physical interfaces, and etherstubs), automatically discovers zones and their associated interfaces, and stores all metrics data in a PostgreSQL-compatible database (developed against Oxide's OSS release).

This is a one-person project intended for personal infrastructure monitoring, so be aware that while it works for me, it's not production-hardened software, nor is there any paid labour behind it.

## Features

- Dynamic network interface discovery
- Zone-aware interface mapping
- Automatic monitoring of newly discovered interfaces
- Buffering of metrics for unknown interfaces until they're discovered
- MAC address tracking and history
- Built-in connection retry mechanism with exponential backoff
- PostgreSQL/CockroachDB backend storage

## Installation

### Prerequisites

- Rust toolchain (edition 2024)
- OmniOS CE or similar Illumos-based system
- Access to a PostgreSQL or CockroachDB database
- Administrative access (to run network commands)

### Building

```bash
cargo build --release
```

The binary will be available at `target/release/crossbow_metrics`.

### Database Setup

Before running the application, you need to set up the database tables. The SQL schema can be found in `resources/metrics.sql`. Run this script against your PostgreSQL/CockroachDB instance:

```bash
psql -U yourusername -d yourdb -f resources/metrics.sql
```

Or for CockroachDB:

```bash
cockroach sql --url 'postgresql://user@hostname:26257/metrics?sslmode=verify-full' < resources/metrics.sql
```

## Configuration

Create a `config.toml` file with the following structure:

```toml
[database]
username = "your_db_username"
password = "your_db_password"
hosts = ["db.example.com"]  # Can specify multiple hosts with commas
port = 26257  # Standard CockroachDB port, use 5432 for PostgreSQL
database = "metrics"
sslmode = "require"  # Options: disable, require, verify-ca, verify-full

# Optional settings
max_retries = 5  # Number of times to retry database operations before giving up
log_level = "info"  # Options: error, warn, info, debug, trace
```

## Usage

Run the application with:

```bash
./crossbow_metrics --config /path/to/config.toml
```

### Command-line Options

```
Options:
  --config <CONFIG>      Path to config file [default: config.toml]
  --hostname <HOSTNAME>  Hostname to use for metrics collection (optional, auto-detected if not specified)
  -l, --log-level <LOG_LEVEL>
                         Verbosity level for logging [default: info]
  -q, --quiet            Quiet mode (errors only, overrides log-level)
  -v, --verbose          Show additional detail
  -h, --help             Print help
  -V, --version          Print version
```

## How It Works

Crossbow Metrics works by:

1. Establishing a connection to the database
2. Detecting the host's zones (including global zone)
3. Discovering network interfaces and mapping them to zones
4. Starting a background thread to continuously collect metrics using `dlstat`
5. Storing metrics in the database, buffering metrics for unknown interfaces
6. Periodically re-scanning for new interfaces
7. Tracking MAC address changes and maintaining history

## Known Limitations

- **Resource Consumption**: The application may consume non-trivial amounts of memory when buffering metrics for many unknown interfaces - generally speaking, this should never happen.
- **Error Handling**: While there's basic error handling and retry logic, some edge cases might not be handled gracefully.
- **Database Dependency**: The application requires a functioning database connection to operate; it will not cache metrics locally if the database is unreachable.
- **Limited Testing**: The codebase has been tested on a limited set of OmniOS environments, and there is a lack of written test cases.

## Future Improvements?

- [ ] Add proper unit and integration tests
- [ ] Implement local caching of metrics when database is unavailable
- [ ] Create dashboard templates for common visualisation platforms
- [ ] Add support for CPU and memory metrics collection
- [ ] Add proper documentation on all functions and modules
- [ ] Implement configurable collection intervals
- [ ] Add support for historical data pruning/aggregation
- [ ] Create proper IPS packages for installation
- [ ] Add redundant interface detection methods for greater reliability
- [ ] Support for more detailed interface statistics where available

## Troubleshooting

If you encounter issues:

1. Increase the log level (`--log-level debug` or `--log-level trace`)
2. Check that the user running the application has sufficient privileges to execute the required system commands (`dlstat`, `dladm`, `zonecfg`, etc.)
3. Verify database connectivity - this is done early in execution, but later failure modes have not been experimented with
4. Ensure your firewall rules allow connections to the database
