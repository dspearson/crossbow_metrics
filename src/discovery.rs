use crate::database::execute_with_retry;
use crate::models::NetworkInterface;
use anyhow::{Context, Error, Result};
use log::{debug, info, trace, warn};
use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;
use tokio_postgres::Client;
use uuid::Uuid;

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
    let vnic_output = Command::new("/usr/sbin/dladm")
        .args(&["show-vnic", "-p", "-o", "macaddress", interface_name])
        .output();

    if let Ok(output) = vnic_output {
        if output.status.success() {
            let mac_text = String::from_utf8_lossy(&output.stdout)
                .to_string()
                .trim()
                .to_string();
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
        } else {
            trace!("dladm show-vnic command failed or returned no output");
        }
    }

    Ok(None)
}

async fn try_get_mac_phys(interface_name: &str) -> Result<Option<String>> {
    trace!(
        "Attempting to get MAC address for {} via dladm show-phys",
        interface_name
    );
    let mac_output = Command::new("/usr/sbin/dladm")
        .args(&["show-phys", "-p", "-o", "macaddress", interface_name])
        .output();

    if let Ok(output) = mac_output {
        if output.status.success() {
            let mac_text = String::from_utf8_lossy(&output.stdout)
                .to_string()
                .trim()
                .to_string();
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
        } else {
            trace!("dladm show-phys command failed or returned no output");
        }
    }

    Ok(None)
}

async fn try_get_mac_link(interface_name: &str) -> Result<Option<String>> {
    trace!(
        "Attempting to get MAC address for {} via dladm show-link",
        interface_name
    );
    let alt_mac_output = Command::new("/usr/sbin/dladm")
        .args(&["show-link", "-p", "-o", "macaddress", interface_name])
        .output();

    if let Ok(output) = alt_mac_output {
        if output.status.success() {
            let mac_text = String::from_utf8_lossy(&output.stdout)
                .to_string()
                .trim()
                .to_string();
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
        } else {
            trace!("dladm show-link command failed or returned no output");
        }
    }

    Ok(None)
}

async fn get_interface_mtu(interface_name: &str) -> Result<Option<i64>> {
    trace!("Attempting to get MTU for {}", interface_name);
    let mtu_output = Command::new("/usr/sbin/dladm")
        .args(&["show-link", "-p", "-o", "mtu", interface_name])
        .output();

    if let Ok(output) = mtu_output {
        if output.status.success() {
            let mtu_text = String::from_utf8_lossy(&output.stdout)
                .to_string()
                .trim()
                .to_string();
            if !mtu_text.is_empty() {
                if let Ok(mtu_value) = mtu_text.parse::<i64>() {
                    if mtu_value > 0 {
                        debug!("Found MTU for {}: {}", interface_name, mtu_value);
                        return Ok(Some(mtu_value));
                    }
                }
            }
        } else {
            trace!("dladm show-link command for MTU failed or returned no output");
        }
    }

    debug!("Could not determine MTU for {}", interface_name);
    Ok(None)
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
    let output = Command::new("/usr/sbin/zoneadm")
        .arg("list")
        .arg("-p")
        .output()
        .context("Failed to run zoneadm command")?;

    String::from_utf8(output.stdout).context("Invalid UTF-8 in zoneadm output")
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
    let output = Command::new("/usr/sbin/dladm")
        .args(&["show-link", "-p", "-o", "link,class"])
        .output()
        .context("Failed to run dladm show-link command")?;

    let link_output = String::from_utf8(output.stdout).context("Invalid UTF-8 in dladm output")?;

    // Get VNIC parent relationships
    debug!("Retrieving VNIC parent relationships");
    let vnic_output = Command::new("/usr/sbin/dladm")
        .args(&["show-vnic", "-p", "-o", "link,over"])
        .output()
        .context("Failed to run dladm show-vnic command")?;

    let vnic_output =
        String::from_utf8(vnic_output.stdout).context("Invalid UTF-8 in dladm vnic output")?;

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
        let zonecfg_output = Command::new("/usr/sbin/zonecfg")
            .args(&["-z", zone_name, "info", "net"])
            .output();

        if let Ok(output) = zonecfg_output {
            let zonecfg_text = String::from_utf8_lossy(&output.stdout).to_string();

            if verbose {
                trace!("zonecfg output for zone {}:", zone_name);
                for line in zonecfg_text.lines() {
                    trace!("  {}", line);
                }
            }

            // Parse the output to find the physical interface name
            for line in zonecfg_text.lines() {
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
        } else if let Err(e) = &zonecfg_output {
            warn!("Error running zonecfg for {}: {}", zone_name, e);
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

    let dladm_output = Command::new("/usr/sbin/dladm")
        .args(&["show-link", "-Z", "-p", "-o", "link,zone"])
        .output();

    if let Ok(output) = dladm_output {
        let link_text = String::from_utf8_lossy(&output.stdout).to_string();

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

async fn create_new_interface(
    client: &Arc<Client>,
    interface_id: Uuid,
    host_id: Uuid,
    zone_id: Option<Uuid>,
    interface_name: &str,
    interface_type: &str,
    parent_interface: &Option<String>,
    mac_address: &Option<String>,
    mtu: Option<i64>,
    max_retries: usize,
) -> Result<()> {
    // Create new interface
    let client_clone = Arc::clone(client);
    let interface_id_clone = interface_id;
    let host_id_clone = host_id;
    let zone_id_clone = zone_id;
    let interface_name_clone = interface_name.to_string();
    let interface_type_clone = interface_type.to_string();
    let parent_interface_clone = parent_interface.clone();
    let mac_address_clone = mac_address.clone();
    let mtu_clone = mtu;

    execute_with_retry(
        move || {
            let client = Arc::clone(&client_clone);
            let interface_id = interface_id_clone;
            let host_id = host_id_clone;
            let zone_id = zone_id_clone;
            let interface_name = interface_name_clone.clone();
            let interface_type = interface_type_clone.clone();
            let parent_interface = parent_interface_clone.clone();
            let mac_address = mac_address_clone.clone();
            let mtu = mtu_clone;

            Box::pin(async move {
                client
                    .execute(
                        "INSERT INTO interfaces (
                 interface_id, host_id, zone_id, interface_name,
                 interface_type, parent_interface, mac_address, mtu,
                 is_active, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true, CURRENT_TIMESTAMP)",
                        &[
                            &interface_id,
                            &host_id,
                            &zone_id,
                            &interface_name,
                            &interface_type,
                            &parent_interface,
                            &mac_address,
                            &mtu,
                        ],
                    )
                    .await
                    .context("Failed to insert interface")
            })
        },
        max_retries,
    )
    .await?;

    // If MAC address is provided, create initial history record
    if let Some(mac) = mac_address {
        create_initial_mac_history(client, interface_id, mac, max_retries).await?;
    }

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

    info!(
        "Created new interface record: {} - {} ({}, {}, {})",
        interface_name, interface_id, zone_desc, mac_display, mtu_display
    );

    Ok(())
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

pub async fn force_interface_detection(
    client: Arc<Client>,
    host_id: Uuid,
    interface_name: &str,
    max_retries: usize,
) -> Result<NetworkInterface> {
    debug!(
        "Attempting to force detection of interface: {}",
        interface_name
    );

    let (interface_type, parent_interface) =
        determine_interface_type_and_parent(interface_name).await?;

    // Create the interface record
    let interface_id = ensure_interface_exists(
        Arc::clone(&client),
        host_id,
        None, // Default to global zone
        interface_name.to_string(),
        interface_type.clone(),
        parent_interface.clone(),
        max_retries,
    )
    .await?;

    let interface = NetworkInterface {
        interface_id,
        host_id,
        zone_id: None,
        interface_name: interface_name.to_string(),
        interface_type,
        parent_interface,
    };

    // Format parent information for logging
    let parent_display = match &interface.parent_interface {
        Some(parent) => format!(", parent: {}", parent),
        None => String::new(),
    };

    info!(
        "Created interface record for interface: {} (type: {}{})",
        interface_name, interface.interface_type, parent_display
    );

    Ok(interface)
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

async fn handle_mac_address_change(
    client: &Arc<Client>,
    interface_id: Uuid,
    current_mac: &Option<String>,
    new_mac: &Option<String>,
    max_retries: usize,
) -> Result<()> {
    // Format MAC addresses for logging
    let old_mac_display = match current_mac {
        Some(mac) => mac.clone(),
        None => "none".to_string(),
    };

    let new_mac_display = match new_mac {
        Some(mac) => mac.clone(),
        None => "none".to_string(),
    };

    debug!(
        "MAC address change detected for interface {}: {} → {}",
        interface_id, old_mac_display, new_mac_display
    );

    // 1. Close previous MAC history record if exists
    if let Some(mac) = current_mac {
        close_previous_mac_history(client, interface_id, mac, max_retries).await?;
    }

    // 2. Create new MAC history record
    if let Some(new_mac) = new_mac {
        create_new_mac_history(client, interface_id, new_mac, max_retries).await?;
    }

    Ok(())
}

async fn create_initial_mac_history(
    client: &Arc<Client>,
    interface_id: Uuid,
    mac: &str,
    max_retries: usize,
) -> Result<()> {
    let client_clone = Arc::clone(client);
    let interface_id_clone = interface_id;
    let mac_clone = mac.to_string();

    execute_with_retry(
        move || {
            let client = Arc::clone(&client_clone);
            let interface_id = interface_id_clone;
            let mac = mac_clone.clone();

            Box::pin(async move {
                client
                    .execute(
                        "INSERT INTO mac_address_history
                 (interface_id, mac_address, effective_from, change_reason)
                 VALUES ($1, $2, CURRENT_TIMESTAMP, $3)",
                        &[&interface_id, &mac, &"Initial discovery"],
                    )
                    .await
                    .context("Failed to insert initial MAC history record")
            })
        },
        max_retries,
    )
    .await?;

    Ok(())
}

async fn get_current_mac_address(
    client: &Arc<Client>,
    interface_id: Uuid,
    max_retries: usize,
) -> Result<Option<String>> {
    let client_clone = Arc::clone(client);
    let interface_id_clone = interface_id;

    execute_with_retry(
        move || {
            let client = Arc::clone(&client_clone);
            let interface_id = interface_id_clone;

            Box::pin(async move {
                let row = client
                    .query_opt(
                        "SELECT mac_address FROM interfaces WHERE interface_id = $1",
                        &[&interface_id],
                    )
                    .await
                    .context("Failed to query current MAC address")?;

                // Fix: Properly handle NULL values in the database column
                let current_mac: Option<Option<String>> = row.map(|r| r.get(0));
                // Fix: Extract the inner Option correctly
                let result = current_mac.flatten();

                Ok::<_, Error>(result)
            })
        },
        max_retries,
    )
    .await
}

fn is_mac_address_changed(current_mac: &Option<String>, new_mac: &Option<String>) -> bool {
    match (current_mac, new_mac) {
        (Some(current), Some(new)) => current != new,
        (None, Some(_)) => true,
        _ => false,
    }
}

async fn close_previous_mac_history(
    client: &Arc<Client>,
    interface_id: Uuid,
    mac: &str,
    max_retries: usize,
) -> Result<()> {
    let client_clone = Arc::clone(client);
    let interface_id_clone = interface_id;
    let mac_clone = mac.to_string();

    execute_with_retry(
        move || {
            let client = Arc::clone(&client_clone);
            let interface_id = interface_id_clone;
            let mac = mac_clone.clone();

            Box::pin(async move {
                client
                    .execute(
                        "UPDATE mac_address_history
                 SET effective_to = CURRENT_TIMESTAMP
                 WHERE interface_id = $1 AND mac_address = $2 AND effective_to IS NULL",
                        &[&interface_id, &mac],
                    )
                    .await
                    .context("Failed to close previous MAC history record")
            })
        },
        max_retries,
    )
    .await?;

    Ok(())
}

async fn create_new_mac_history(
    client: &Arc<Client>,
    interface_id: Uuid,
    new_mac: &str,
    max_retries: usize,
) -> Result<()> {
    let client_clone = Arc::clone(client);
    let interface_id_clone = interface_id;
    let new_mac_clone = new_mac.to_string();

    execute_with_retry(
        move || {
            let client = Arc::clone(&client_clone);
            let interface_id = interface_id_clone;
            let new_mac = new_mac_clone.clone();

            Box::pin(async move {
                client
                    .execute(
                        "INSERT INTO mac_address_history
                 (interface_id, mac_address, effective_from, change_reason)
                 VALUES ($1, $2, CURRENT_TIMESTAMP, $3)",
                        &[&interface_id, &new_mac, &"Discovery update"],
                    )
                    .await
                    .context("Failed to insert new MAC history record")
            })
        },
        max_retries,
    )
    .await?;

    Ok(())
}

async fn update_interface_record(
    client: &Arc<Client>,
    interface_id: Uuid,
    interface_type: &str,
    parent_interface: &Option<String>,
    mac_address: &Option<String>,
    mtu: Option<i64>,
    max_retries: usize,
) -> Result<()> {
    let client_clone = Arc::clone(client);
    let interface_id_clone = interface_id;
    let interface_type_clone = interface_type.to_string();
    let parent_interface_clone = parent_interface.clone();
    let mac_address_clone = mac_address.clone();
    let mtu_clone = mtu;

    execute_with_retry(
        move || {
            let client = Arc::clone(&client_clone);
            let interface_id = interface_id_clone;
            let interface_type = interface_type_clone.clone();
            let parent_interface = parent_interface_clone.clone();
            let mac_address = mac_address_clone.clone();
            let mtu = mtu_clone;

            Box::pin(async move {
                client
                    .execute(
                        "UPDATE interfaces SET
                 interface_type = $1,
                 parent_interface = $2,
                 mac_address = $3,
                 mtu = $4,
                 is_active = true
                 WHERE interface_id = $5",
                        &[
                            &interface_type,
                            &parent_interface,
                            &mac_address,
                            &mtu,
                            &interface_id,
                        ],
                    )
                    .await
                    .context("Failed to update interface")
            })
        },
        max_retries,
    )
    .await?;

    Ok(())
}

pub async fn determine_interface_type_and_parent(
    interface_name: &str,
) -> Result<(String, Option<String>)> {
    // Try to get accurate interface type by querying dladm directly for this specific interface
    let output = Command::new("/usr/sbin/dladm")
        .args(&["show-link", "-p", "-o", "link,class", interface_name])
        .output()
        .context(format!(
            "Failed to run dladm show-link for {}",
            interface_name
        ))?;

    let link_output = String::from_utf8(output.stdout).context("Invalid UTF-8 in dladm output")?;

    // Default values
    let mut interface_type = "dev".to_string(); // Default type
    let mut parent_interface = None;

    // Parse dladm output to get actual interface type
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 && fields[0] == interface_name {
            interface_type = fields[1].to_string();
            break;
        }
    }

    // If it's a VNIC, get the parent interface
    if interface_type == "vnic" {
        parent_interface = get_vnic_parent(interface_name).await?;
    }

    // If dladm didn't find the interface, fall back to guessing based on naming conventions
    if link_output.trim().is_empty() {
        debug!(
            "Interface {} not found in dladm, using heuristics to determine type",
            interface_name
        );
        interface_type = guess_interface_type(interface_name);
    }

    Ok((interface_type, parent_interface))
}

pub async fn get_vnic_parent(interface_name: &str) -> Result<Option<String>> {
    let vnic_output = Command::new("/usr/sbin/dladm")
        .args(&["show-vnic", "-p", "-o", "link,over", interface_name])
        .output()
        .context(format!(
            "Failed to run dladm show-vnic for {}",
            interface_name
        ))?;

    let vnic_text =
        String::from_utf8(vnic_output.stdout).context("Invalid UTF-8 in dladm vnic output")?;

    for line in vnic_text.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 && fields[0] == interface_name {
            return Ok(Some(fields[1].to_string()));
        }
    }

    Ok(None)
}

fn guess_interface_type(interface_name: &str) -> String {
    if interface_name.starts_with("igb")
        || interface_name.starts_with("e1000g")
        || interface_name.starts_with("bge")
        || interface_name.starts_with("ixgbe")
    {
        "phys".to_string() // Physical device
    } else if interface_name.ends_with("stub") || interface_name.contains("stub") {
        "etherstub".to_string()
    } else if interface_name.ends_with("0")
        && !interface_name.contains("gw")
        && !interface_name.starts_with("igb")
    {
        "bridge".to_string() // Likely a bridge (ends with 0)
    } else if interface_name.contains("overlay") {
        "overlay".to_string()
    } else if interface_name.contains("gw") {
        "vnic".to_string() // Gateway VNICs typically end with gw
    } else {
        "dev".to_string() // Default to generic device if we can't determine
    }
}
