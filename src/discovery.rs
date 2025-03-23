use crate::database::execute_with_retry;
use crate::models::NetworkInterface;
use crate::process_utils;
use nix::sys::signal::{self, Signal};
use anyhow::{Context, Error, Result};
use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::Client;
use uuid::Uuid;

// Maximum time for a command to run before considering it hung
const CMD_TIMEOUT_SECS: u64 = 15;

// Function to get MAC address and MTU for an interface
async fn get_interface_details(interface_name: &str) -> Result<(Option<String>, Option<i64>)> {
    // Get MAC address using multiple methods
    let mac_address = get_mac_address(interface_name).await?;

    // Get MTU using dladm
    let mtu = get_interface_mtu(interface_name).await?;

    Ok((mac_address, mtu))
}

async fn get_mac_address(interface_name: &str) -> Result<Option<String>> {
    // Try VNIC method first
    if let Some(mac) = try_get_mac_vnic(interface_name).await? {
        return Ok(Some(mac));
    }

    // Try physical method next
    if let Some(mac) = try_get_mac_phys(interface_name).await? {
        return Ok(Some(mac));
    }

    // Try link method last
    if let Some(mac) = try_get_mac_link(interface_name).await? {
        return Ok(Some(mac));
    }

    debug!("Could not determine MAC address for {}", interface_name);
    Ok(None)
}

async fn try_get_mac_vnic(interface_name: &str) -> Result<Option<String>> {
    trace!(
        "Attempting to get MAC address for {} via dladm show-vnic",
        interface_name
    );
    
    let interface_name_clone = interface_name.to_string();
    let vnic_output = tokio::task::spawn_blocking(move || {
        execute_command_with_timeout(
            "/usr/sbin/dladm",
            &["show-vnic", "-p", "-o", "macaddress", &interface_name_clone]
        )
    }).await??;

    if let Some(output) = vnic_output {
        let mac_text = output.trim().to_string();
        if !mac_text.is_empty() {
            let formatted_mac = format_mac_address(&mac_text)?;
            if !formatted_mac.is_empty() {
                debug!(
                    "Found MAC address for {} with VNIC method: {}",
                    interface_name, &formatted_mac
                );
                return Ok(Some(formatted_mac));
            }
        }
    }

    Ok(None)
}

async fn try_get_mac_phys(interface_name: &str) -> Result<Option<String>> {
    trace!(
        "Attempting to get MAC address for {} via dladm show-phys",
        interface_name
    );

    let interface_name_clone = interface_name.to_string();
    let mac_output = tokio::task::spawn_blocking(move || {
        execute_command_with_timeout(
            "/usr/sbin/dladm",
            &["show-phys", "-p", "-o", "macaddress", &interface_name_clone],
        )
    })
    .await??;

    if let Some(output) = mac_output {
        let mac_text = output.trim().to_string();
        if !mac_text.is_empty() {
            let formatted_mac = format_mac_address(&mac_text)?;
            if !formatted_mac.is_empty() {
                debug!(
                    "Found MAC address for {}: {}",
                    interface_name, &formatted_mac
                );
                return Ok(Some(formatted_mac));
            }
        }
    }

    Ok(None)
}

async fn try_get_mac_link(interface_name: &str) -> Result<Option<String>> {
    trace!(
        "Attempting to get MAC address for {} via dladm show-link",
        interface_name
    );

    let interface_name_clone = interface_name.to_string();
    let alt_mac_output = tokio::task::spawn_blocking(move || {
        execute_command_with_timeout(
            "/usr/sbin/dladm",
            &["show-link", "-p", "-o", "macaddress", &interface_name_clone],
        )
    })
    .await??;

    if let Some(output) = alt_mac_output {
        let mac_text = output.trim().to_string();
        if !mac_text.is_empty() {
            let formatted_mac = format_mac_address(&mac_text)?;
            if !formatted_mac.is_empty() {
                debug!(
                    "Found MAC address for {} with alternative method: {}",
                    interface_name, &formatted_mac
                );
                return Ok(Some(formatted_mac));
            }
        }
    }

    Ok(None)
}

async fn get_interface_mtu(interface_name: &str) -> Result<Option<i64>> {
    trace!("Attempting to get MTU for {}", interface_name);

    let interface_name_clone = interface_name.to_string();
    let mtu_output = tokio::task::spawn_blocking(move || {
        execute_command_with_timeout(
            "/usr/sbin/dladm",
            &["show-link", "-p", "-o", "mtu", &interface_name_clone],
        )
    })
    .await??;

    if let Some(output) = mtu_output {
        let mtu_text = output.trim().to_string();
        if !mtu_text.is_empty() {
            if let Ok(mtu_value) = mtu_text.parse::<i64>() {
                if mtu_value > 0 {
                    debug!("Found MTU for {}: {}", interface_name, mtu_value);
                    return Ok(Some(mtu_value));
                }
            }
        }
    }

    debug!("Could not determine MTU for {}", interface_name);
    Ok(None)
}

async fn update_existing_interface(
    client: &Arc<Client>,
    interface_id: Uuid,
    interface_name: &str,
    interface_type: &str,
    parent_interface: &Option<String>,
    mac_address: &Option<String>,
    mtu: Option<i64>,
    zone_id: Option<Uuid>,
    max_retries: usize,
) -> Result<()> {
    // Check current MAC address to see if it changed
    let current_mac = get_current_mac_address(client, interface_id, max_retries).await?;

    // Check if MAC address has changed
    let mac_changed = is_mac_address_changed(&current_mac, mac_address);

    if mac_changed {
        handle_mac_address_change(client, interface_id, &current_mac, mac_address, max_retries)
            .await?;
    }

    // Update interface details
    update_interface_record(
        client,
        interface_id,
        interface_type,
        parent_interface,
        mac_address,
        mtu,
        max_retries,
    )
    .await?;

    // Format zone information
    let zone_desc = match &zone_id {
        Some(id) => format!("zone ID: {}", id),
        None => "global zone".to_string(),
    };

    // Format MAC address for logging
    let mac_display = match mac_address {
        Some(mac) => format!("MAC: {}", mac),
        None => "no MAC".to_string(),
    };

    // Format MTU for logging
    let mtu_display = match mtu {
        Some(mtu_value) => format!("MTU: {}", mtu_value),
        None => "default MTU".to_string(),
    };

    if mac_changed {
        // Format the previous MAC for better logging
        let old_mac_display = match &current_mac {
            Some(mac) => format!("{}", mac),
            None => "none".to_string(),
        };

        // Format the new MAC
        let new_mac_display = match mac_address {
            Some(mac) => format!("{}", mac),
            None => "none".to_string(),
        };

        info!(
            "Updated interface record with MAC change: {} - {} ({}, MAC: {} → {}, {})",
            interface_name, interface_id, zone_desc, old_mac_display, new_mac_display, mtu_display
        );
    } else {
        trace!(
            "Updated existing interface record: {} - {} ({}, {}, {})",
            interface_name, interface_id, zone_desc, mac_display, mtu_display
        );
    }

    Ok(())
}

fn log_detailed_interface_info(
    interfaces: &HashMap<String, NetworkInterface>,
    zones: &HashMap<String, Uuid>,
) {
    debug!("Interface details:");
    for (name, interface) in interfaces {
        // Format zone information
        let zone_info = match &interface.zone_id {
            Some(id) => {
                let zone_name = zones
                    .iter()
                    .find(|&(_, zone_id)| *zone_id == *id)
                    .map(|(name, _)| name.clone())
                    .unwrap_or_else(|| "unknown".to_string());
                format!("zone: {}", zone_name)
            }
            None => "global zone".to_string(),
        };

        // Format parent information
        let parent_display = match &interface.parent_interface {
            Some(parent) => format!("parent: {}", parent),
            None => "no parent".to_string(),
        };

        debug!(
            "Interface: {} (type: {}, {}, {})",
            name, interface.interface_type, zone_info, parent_display
        );
    }
}

// Execute a command with a timeout to prevent hanging
fn execute_command_with_timeout(command: &str, args: &[&str]) -> Result<Option<String>> {
    // Create the command
    let mut cmd = Command::new(command);
    cmd.args(args);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    
    debug!("Executing command: {} {}", command, args.join(" "));
    
    // Start the command
    let mut child = cmd.spawn().context(format!("Failed to execute command: {}", command))?;
    
    // Set up a timeout for the command
    let timeout = std::time::Duration::from_secs(CMD_TIMEOUT_SECS);
    
    // Use a separate thread to wait for the command to complete
    let child_id = child.id();
    let timeout_handle = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        
        while start.elapsed() < timeout {
            // Check if the process has exited
            match child.try_wait() {
                Ok(Some(status)) => {
                    // Process has exited
                    if status.success() {
                        // Read stdout if successful
                        if let Some(stdout) = child.stdout.take() {
                            let mut reader = BufReader::new(stdout);
                            let mut output = String::new();
                            if reader.read_to_string(&mut output).is_ok() {
                                return Ok(Some(output));
                            }
                        }
                        // If we couldn't read stdout but exit code was 0, return empty string
                        return Ok(Some(String::new()));
                    } else {
                        // Process failed
                        let mut stderr = String::new();
                        if let Some(stderr_handle) = child.stderr.take() {
                            let mut reader = BufReader::new(stderr_handle);
                            let _ = reader.read_to_string(&mut stderr);
                        }
                        return Err(anyhow::anyhow!(
                            "Command failed with status {}: {}",
                            status, stderr.trim()
                        ));
                    }
                },
                Ok(None) => {
                    // Process still running, sleep a bit and try again
                    std::thread::sleep(std::time::Duration::from_millis(100));
                },
                Err(e) => {
                    // Error checking process status
                    return Err(anyhow::anyhow!("Error checking process status: {}", e));
                }
            }
        }
        
        // If we get here, the process has timed out
        warn!("Command timed out after {} seconds: {} {}", 
              timeout.as_secs(), command, args.join(" "));
        
        // Try to kill the process
        match nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(child_id as i32),
            nix::sys::signal::Signal::SIGTERM
        ) {
            Ok(_) => debug!("Successfully sent SIGTERM to process {}", child_id),
            Err(e) => warn!("Failed to terminate process {}: {}", child_id, e),
        }
        
        // Give it a moment to terminate
        std::thread::sleep(std::time::Duration::from_millis(500));
        
        // If still alive, force kill
        if child.try_wait().is_ok() {
            match nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(child_id as i32),
                nix::sys::signal::Signal::SIGKILL
            ) {
                Ok(_) => debug!("Successfully sent SIGKILL to process {}", child_id),
                Err(e) => warn!("Failed to kill process {}: {}", child_id, e),
            }
        }
        
        Ok(None) // Return None to indicate timeout
    });
    
    timeout_handle.join().unwrap_or_else(|_| {
        warn!("Timeout thread panicked for command: {} {}", command, args.join(" "));
        Ok(None)
    })
}

// Helper function to properly format MAC addresses from OmniOS output
fn format_mac_address(raw_mac: &str) -> Result<String> {
    // For debugging
    trace!("Raw MAC address string: {:?}", raw_mac);

    // Handle empty strings
    if raw_mac.trim().is_empty() {
        return Ok(String::new());
    }

    // Handle the specific OmniOS format with escaped colons (e.g., "2\:8\:20\:7b\:40\:5d")
    if raw_mac.contains("\\:") {
        // Simply remove the escape characters
        let formatted = raw_mac.replace("\\:", ":");
        debug!(
            "Converted escaped MAC format: {:?} -> {:?}",
            raw_mac, formatted
        );
        return Ok(formatted);
    }

    // Remove any backslashes, quotes, or other unwanted characters
    let cleaned = raw_mac
        .replace("\\", "")
        .replace("\"", "")
        .replace("'", "")
        .trim()
        .to_string();

    if cleaned.is_empty() {
        return Ok(String::new());
    }

    // Check if it's a colon-separated MAC or some other format
    if cleaned.contains(':') {
        // Already in standard format, return as is
        return Ok(cleaned);
    }

    // Handle different formats of MAC addresses
    match get_standardized_mac_format(&cleaned) {
        Some(formatted) => Ok(formatted),
        None => {
            // If we can't recognize the format, log a warning and return the cleaned string anyway
            warn!("Unrecognized MAC address format: {}", raw_mac);
            Ok(cleaned)
        }
    }
}

fn get_standardized_mac_format(mac: &str) -> Option<String> {
    // Format 1: 12 hex characters without separators (e.g., "0013214B5C6F")
    if mac.len() == 12 && mac.chars().all(|c| c.is_digit(16)) {
        let mut formatted = String::with_capacity(17);
        for (i, c) in mac.chars().enumerate() {
            if i > 0 && i % 2 == 0 {
                formatted.push(':');
            }
            formatted.push(c);
        }
        return Some(formatted);
    }

    // Format 2: Dash separated (e.g., "00-13-21-4B-5C-6F")
    if mac.contains('-') {
        return Some(mac.replace('-', ":"));
    }

    // Format 3: Space separated
    if mac.contains(' ') {
        return Some(mac.replace(' ', ":"));
    }

    // Format 4: Dot separated
    if mac.contains('.') {
        return Some(mac.replace('.', ":"));
    }

    None
}

pub async fn discover_zones(
    client: Arc<Client>,
    host_id: Uuid,
    max_retries: usize,
) -> Result<HashMap<String, Uuid>> {
    let mut zones = HashMap::new();

    // Get zone list from system
    debug!("Retrieving zone list from system");
    let zone_output = get_zone_list().await?;

    for line in zone_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let zone_name = fields[1].to_string();
            let zone_status = if fields.len() >= 3 {
                Some(fields[2].to_string())
            } else {
                None
            };

            trace!("Processing zone: {} (status: {:?})", zone_name, zone_status);

            // Get or create the zone in the database
            let zone_id = ensure_zone_exists(
                Arc::clone(&client),
                host_id,
                &zone_name,
                zone_status,
                max_retries,
            )
            .await?;

            zones.insert(zone_name, zone_id);
        }
    }

    info!("Discovered {} zones", zones.len());
    trace!("Zone details: {:#?}", zones);
    Ok(zones)
}

async fn get_zone_list() -> Result<String> {
    let output = tokio::task::spawn_blocking(|| {
        execute_command_with_timeout("/usr/sbin/zoneadm", &["list", "-p"])
    })
    .await??;

    match output {
        Some(output) => Ok(output),
        None => Err(anyhow::anyhow!(
            "Failed to get zone list - command timed out or was terminated"
        )),
    }
}

async fn ensure_zone_exists(
    client: Arc<Client>,
    host_id: Uuid,
    zone_name: &str,
    zone_status: Option<String>,
    max_retries: usize,
) -> Result<Uuid> {
    // Check if zone exists in database with retry logic
    let zone_client = Arc::clone(&client);
    let zone_host_id = host_id;
    let zone_name_clone = zone_name.to_string();
    let zone_status_clone = zone_status.clone();

    execute_with_retry(move || {
        let client = Arc::clone(&zone_client);
        let host_id = zone_host_id;
        let zone_name = zone_name_clone.clone();
        let zone_status = zone_status_clone.clone();

        Box::pin(async move {
            let row = client
                .query_opt(
                    "SELECT zone_id FROM zones WHERE host_id = $1 AND zone_name = $2",
                    &[&host_id, &zone_name],
                )
                .await
                .context("Failed to query zone")?;

            let zone_id = match row {
                Some(row) => {
                    let id: Uuid = row.get(0);
                    // Update zone status if available
                    if let Some(status) = &zone_status {
                        client
                            .execute(
                                "UPDATE zones SET zone_status = $1 WHERE zone_id = $2",
                                &[status, &id],
                            )
                            .await
                            .context("Failed to update zone status")?;
                    }
                    debug!("Found existing zone record: {} - {}", zone_name, id);
                    id
                }
                None => {
                    // Create new zone
                    let id = Uuid::new_v4();
                    client
                        .execute(
                            "INSERT INTO zones (zone_id, host_id, zone_name, zone_status, created_at)
                             VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)",
                            &[&id, &host_id, &zone_name, &zone_status],
                        )
                        .await
                        .context("Failed to insert zone")?;
                    info!("Created new zone record: {} - {}", zone_name, id);
                    id
                }
            };

            Ok::<_, Error>(zone_id)
        })
    }, max_retries)
    .await
}

pub async fn discover_interfaces(
    client: Arc<Client>,
    host_id: Uuid,
    zones: &HashMap<String, Uuid>,
    max_retries: usize,
    verbose: bool,
) -> Result<HashMap<String, NetworkInterface>> {
    let mut interfaces: HashMap<String, NetworkInterface> = HashMap::new();

    // Create a mapping between interfaces and zones
    let zone_interface_map = build_zone_interface_map(zones, verbose).await?;

    // Get interface information from the system
    let (link_output, vnic_parents) = get_interface_information().await?;

    if verbose {
        debug_interface_list(&link_output);
    }

    // Process interfaces by type to ensure parent interfaces are created first
    process_physical_interfaces(&mut interfaces, &client, host_id, &link_output, max_retries)
        .await?;

    process_etherstub_interfaces(&mut interfaces, &client, host_id, &link_output, max_retries)
        .await?;

    process_vnic_interfaces(
        &mut interfaces,
        &client,
        host_id,
        &zone_interface_map,
        &link_output,
        &vnic_parents,
        zones,
        max_retries,
        verbose,
    )
    .await?;

    debug!("Discovered {} interfaces", interfaces.len());

    if verbose {
        log_detailed_interface_info(&interfaces, zones);
    }

    Ok(interfaces)
}

async fn get_interface_information() -> Result<(String, HashMap<String, String>)> {
    // Get all datalinks (including physical, etherstub, vnic)
    debug!("Retrieving datalink information from system");

    let link_output = tokio::task::spawn_blocking(|| {
        execute_command_with_timeout("/usr/sbin/dladm", &["show-link", "-p", "-o", "link,class"])
    })
    .await??;

    let link_output = match link_output {
        Some(output) => output,
        None => {
            return Err(anyhow::anyhow!(
                "Failed to get interface information - command timed out or was terminated"
            ));
        }
    };

    // Get VNIC parent relationships
    debug!("Retrieving VNIC parent relationships");
    let vnic_output = tokio::task::spawn_blocking(|| {
        execute_command_with_timeout("/usr/sbin/dladm", &["show-vnic", "-p", "-o", "link,over"])
    })
    .await??;

    let vnic_output = match vnic_output {
        Some(output) => output,
        None => {
            return Err(anyhow::anyhow!(
                "Failed to get VNIC information - command timed out or was terminated"
            ));
        }
    };

    // Build a mapping of VNICs to their parent interfaces
    let mut vnic_parents = HashMap::new();
    for line in vnic_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].to_string();
            let parent_interface = fields[1].to_string();
            vnic_parents.insert(interface_name, parent_interface);
        }
    }

    Ok((link_output, vnic_parents))
}

fn debug_interface_list(link_output: &str) {
    debug!("Discovered interfaces:");
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0];
            let interface_type = fields[1];
            trace!(
                "Found interface: {} (type: {})",
                interface_name, interface_type
            );
        }
    }
}

async fn process_physical_interfaces(
    interfaces: &mut HashMap<String, NetworkInterface>,
    client: &Arc<Client>,
    host_id: Uuid,
    link_output: &str,
    max_retries: usize,
) -> Result<()> {
    debug!("Processing physical interfaces");
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].to_string();
            let interface_type = fields[1].to_string();

            if interface_type == "phys" {
                // It's a physical interface
                trace!("Processing physical interface: {}", interface_name);
                let interface_id = ensure_interface_exists(
                    Arc::clone(client),
                    host_id,
                    None, // Physical interfaces are in global zone
                    interface_name.clone(),
                    interface_type.clone(),
                    None, // No parent for physical interfaces
                    max_retries,
                )
                .await?;

                interfaces.insert(
                    interface_name.clone(),
                    NetworkInterface {
                        interface_id,
                        host_id,
                        zone_id: None,
                        interface_name,
                        interface_type,
                        parent_interface: None,
                    },
                );
            }
        }
    }

    Ok(())
}

async fn process_etherstub_interfaces(
    interfaces: &mut HashMap<String, NetworkInterface>,
    client: &Arc<Client>,
    host_id: Uuid,
    link_output: &str,
    max_retries: usize,
) -> Result<()> {
    debug!("Processing etherstub interfaces");
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].to_string();
            let interface_type = fields[1].to_string();

            if interface_type == "etherstub" {
                // It's an etherstub
                trace!("Processing etherstub interface: {}", interface_name);
                let interface_id = ensure_interface_exists(
                    Arc::clone(client),
                    host_id,
                    None, // Etherstubs are in global zone
                    interface_name.clone(),
                    interface_type.clone(),
                    None, // No parent for etherstubs
                    max_retries,
                )
                .await?;

                interfaces.insert(
                    interface_name.clone(),
                    NetworkInterface {
                        interface_id,
                        host_id,
                        zone_id: None,
                        interface_name,
                        interface_type,
                        parent_interface: None,
                    },
                );
            }
        }
    }

    Ok(())
}

async fn process_vnic_interfaces(
    interfaces: &mut HashMap<String, NetworkInterface>,
    client: &Arc<Client>,
    host_id: Uuid,
    zone_interface_map: &HashMap<String, Uuid>,
    link_output: &str,
    vnic_parents: &HashMap<String, String>,
    zones: &HashMap<String, Uuid>,
    max_retries: usize,
    verbose: bool,
) -> Result<()> {
    debug!("Processing VNIC interfaces");
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].to_string();
            let interface_type = fields[1].to_string();

            if interface_type == "vnic" {
                // It's a VNIC - get its parent
                let parent_interface = vnic_parents.get(&interface_name).cloned();
                trace!(
                    "Processing VNIC interface: {} (parent: {:?})",
                    interface_name, parent_interface
                );

                // Get the zone_id for this interface (default to None for global zone)
                let zone_id = zone_interface_map.get(&interface_name).cloned();

                if verbose && zone_id.is_some() {
                    // Using a let binding to create a longer-lived value
                    let unknown = "unknown".to_string();
                    let zone_name = zones
                        .iter()
                        .find_map(|(name, id)| {
                            if *id == zone_id.unwrap() {
                                Some(name)
                            } else {
                                None
                            }
                        })
                        .unwrap_or(&unknown);
                    debug!("Interface {} belongs to zone {}", interface_name, zone_name);
                }

                let interface_id = ensure_interface_exists(
                    Arc::clone(client),
                    host_id,
                    zone_id,
                    interface_name.clone(),
                    interface_type.clone(),
                    parent_interface.clone(),
                    max_retries,
                )
                .await?;

                interfaces.insert(
                    interface_name.clone(),
                    NetworkInterface {
                        interface_id,
                        host_id,
                        zone_id,
                        interface_name,
                        interface_type,
                        parent_interface,
                    },
                );
            }
        }
    }

    Ok(())
}

// Function to build the mapping between interface names and zone IDs
async fn build_zone_interface_map(
    zones: &HashMap<String, Uuid>,
    verbose: bool,
) -> Result<HashMap<String, Uuid>> {
    info!("Building zone-interface map with {} zones", zones.len());

    // Try zonecfg method first
    let mut zone_interface_map = build_map_using_zonecfg(zones, verbose).await?;

    // If zonecfg method didn't work, try dladm show-link -Z
    if zone_interface_map.is_empty() {
        zone_interface_map = build_map_using_dladm(zones, verbose).await?;
    }

    // Display the mapping we've built
    log_zone_interface_mapping(&zone_interface_map, zones);

    Ok(zone_interface_map)
}

async fn build_map_using_zonecfg(
    zones: &HashMap<String, Uuid>,
    verbose: bool,
) -> Result<HashMap<String, Uuid>> {
    let mut interface_map = HashMap::new();

    // Check non-global zones using zonecfg info net
    for (zone_name, zone_uuid) in zones {
        // Skip global zone
        if zone_name == "global" {
            continue;
        }

        debug!("Checking zone {} for net resources", zone_name);

        // Get network interfaces defined in the zone
        let zone_name_clone = zone_name.clone();
        let zonecfg_output = tokio::task::spawn_blocking(move || {
            execute_command_with_timeout(
                "/usr/sbin/zonecfg",
                &["-z", &zone_name_clone, "info", "net"],
            )
        })
        .await??;

        let zonecfg_output = match zonecfg_output {
            Some(output) => output,
            None => {
                warn!(
                    "Timeout retrieving network configuration for zone {}",
                    zone_name
                );
                continue;
            }
        };

        if verbose {
            trace!("zonecfg output for zone {}:", zone_name);
            for line in zonecfg_output.lines() {
                trace!("  {}", line);
            }
        }

        // Parse the output to find the physical interface name
        for line in zonecfg_output.lines() {
            let line = line.trim();

            if line.starts_with("physical:") {
                // Extract interface name after "physical:"
                if let Some(iface_name) = line.split(':').nth(1) {
                    let interface_name = iface_name.trim().to_string();

                    info!("Found interface {} in zone {}", interface_name, zone_name);
                    interface_map.insert(interface_name, *zone_uuid);
                }
            }
        }
    }

    Ok(interface_map)
}

async fn build_map_using_dladm(
    zones: &HashMap<String, Uuid>,
    verbose: bool,
) -> Result<HashMap<String, Uuid>> {
    let mut zone_interface_map = HashMap::new();

    info!("No mappings found with zonecfg, trying dladm show-link -Z");

    let dladm_output = tokio::task::spawn_blocking(|| {
        execute_command_with_timeout(
            "/usr/sbin/dladm",
            &["show-link", "-Z", "-p", "-o", "link,zone"],
        )
    })
    .await??;

    let link_text = match dladm_output {
        Some(output) => output,
        None => {
            warn!("Timeout retrieving zone-link mappings with dladm");
            return Ok(zone_interface_map);
        }
    };

    if verbose {
        trace!("dladm show-link -Z output:");
        for line in link_text.lines() {
            trace!("  {}", line);
        }
    }

    for line in link_text.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].trim();
            let zone_name = fields[1].trim();

            if !zone_name.is_empty() && zone_name != "global" {
                if let Some(zone_uuid) = zones.get(zone_name) {
                    info!(
                        "Associating interface {} with zone {}",
                        interface_name, zone_name
                    );
                    zone_interface_map.insert(interface_name.to_string(), *zone_uuid);
                }
            }
        }
    }

    Ok(zone_interface_map)
}

fn log_zone_interface_mapping(
    zone_interface_map: &HashMap<String, Uuid>,
    zones: &HashMap<String, Uuid>,
) {
    if zone_interface_map.is_empty() {
        warn!("WARNING: No zone-interface mappings found!");
    } else {
        info!(
            "Built zone-interface map with {} entries",
            zone_interface_map.len()
        );

        for (interface, zone_uuid) in zone_interface_map {
            // Find zone name for the UUID
            // Create a longer-lived string to avoid the borrowing issue
            let unknown = "unknown".to_string();

            let zone_name = zones
                .iter()
                .find_map(|(name, id)| if *id == *zone_uuid { Some(name) } else { None })
                .unwrap_or(&unknown);

            debug!(
                "Interface {} → Zone {} ({})",
                interface, zone_name, zone_uuid
            );
        }
    }
}

/// Update an interface and track MAC address changes if detected
pub async fn ensure_interface_exists(
    client: Arc<Client>,
    host_id: Uuid,
    zone_id: Option<Uuid>,
    interface_name: String,
    interface_type: String,
    parent_interface: Option<String>,
    max_retries: usize,
) -> Result<Uuid> {
    // Get MAC address and MTU for this interface
    let (mac_address, mtu) = get_interface_details(&interface_name)
        .await
        .unwrap_or((None, None));

    debug!(
        "Found details for {}: MAC: {:?}, MTU: {:?}",
        interface_name, mac_address, mtu
    );

    // First check if interface exists
    let interface_id =
        get_interface_id(&client, host_id, &interface_name, zone_id, max_retries).await?;

    // Check if this is a new interface or existing one
    let is_new = is_new_interface(&client, interface_id, max_retries).await?;

    if is_new {
        create_new_interface(
            &client,
            interface_id,
            host_id,
            zone_id,
            &interface_name,
            &interface_type,
            &parent_interface,
            &mac_address,
            mtu,
            max_retries,
        )
        .await?;
    } else {
        update_existing_interface(
            &client,
            interface_id,
            &interface_name,
            &interface_type,
            &parent_interface,
            &mac_address,
            mtu,
            zone_id,
            max_retries,
        )
        .await?;
    }

    Ok(interface_id)
}

async fn get_interface_id(
    client: &Arc<Client>,
    host_id: Uuid,
    interface_name: &str,
    zone_id: Option<Uuid>,
    max_retries: usize,
) -> Result<Uuid> {
    let client_clone = Arc::clone(client);
    let interface_name_clone = interface_name.to_string();
    let host_id_clone = host_id;
    let zone_id_clone = zone_id;

    execute_with_retry(move || {
        let client = Arc::clone(&client_clone);
        let interface_name = interface_name_clone.clone();
        let host_id = host_id_clone;
        let zone_id = zone_id_clone;

        Box::pin(async move {
            let row = client
                .query_opt(
                    "SELECT interface_id FROM interfaces
                     WHERE host_id = $1 AND interface_name = $2 AND (zone_id = $3 OR (zone_id IS NULL AND $3 IS NULL))",
                    &[&host_id, &interface_name, &zone_id],
                )
                .await
                .context("Failed to query interface")?;

            let interface_id = match row {
                Some(row) => row.get::<_, Uuid>(0),
                None => Uuid::new_v4()  // Generate a new ID but don't create the record yet
            };

            Ok::<_, Error>(interface_id)
        })
    }, max_retries)
    .await
}
async fn is_new_interface(
    client: &Arc<Client>,
    interface_id: Uuid,
    max_retries: usize,
) -> Result<bool> {
    let client_clone = Arc::clone(client);
    let interface_id_clone = interface_id;

    execute_with_retry(
        move || {
            let client = Arc::clone(&client_clone);
            let interface_id = interface_id_clone;

            Box::pin(async move {
                let row = client
                    .query_opt(
                        "SELECT 1 FROM interfaces WHERE interface_id = $1",
                        &[&interface_id],
                    )
                    .await
                    .context("Failed to check if interface exists")?;

                Ok::<_, Error>(row.is_none())
            })
        },
        max_retries,
    )
    .await
}
