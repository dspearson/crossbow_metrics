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
    // Default to None for both values in case commands fail
    let mut mac_address = None;
    let mut mtu = None;

    // Try to get MAC address using dladm show-phys first
    trace!("Attempting to get MAC address for {} via dladm show-phys", interface_name);
    let mac_output = Command::new("/usr/sbin/dladm")
        .args(&["show-phys", "-p", "-o", "link,address", interface_name])
        .output();

    if let Ok(output) = mac_output {
        let mac_text = String::from_utf8_lossy(&output.stdout).to_string();
        for line in mac_text.lines() {
            let fields: Vec<&str> = line.split(':').collect();
            if fields.len() >= 2 && fields[0] == interface_name {
                // Found a MAC address, format it nicely
                let raw_mac = fields[1].trim();
                if !raw_mac.is_empty() && raw_mac != "0" {
                    mac_address = Some(raw_mac.to_string());
                    debug!("Found MAC address for {}: {}", interface_name, raw_mac);
                }
                break;
            }
        }
    } else {
        trace!("First MAC address attempt failed, will try alternative method");
    }

    // Try alternative method if first one didn't work (using show-link instead)
    if mac_address.is_none() {
        trace!("Attempting to get MAC address for {} via dladm show-link", interface_name);
        let alt_mac_output = Command::new("/usr/sbin/dladm")
            .args(&["show-link", "-p", "-o", "link,address", interface_name])
            .output();

        if let Ok(output) = alt_mac_output {
            let mac_text = String::from_utf8_lossy(&output.stdout).to_string();
            for line in mac_text.lines() {
                let fields: Vec<&str> = line.split(':').collect();
                if fields.len() >= 2 && fields[0] == interface_name {
                    let raw_mac = fields[1].trim();
                    if !raw_mac.is_empty() && raw_mac != "0" {
                        mac_address = Some(raw_mac.to_string());
                        debug!("Found MAC address for {} with alternative method: {}", interface_name, raw_mac);
                    }
                    break;
                }
            }
        }
    }

    // For VNICs, third attempt via show-vnic if needed
    if mac_address.is_none() {
        trace!("Attempting to get MAC address for {} via dladm show-vnic", interface_name);
        let vnic_output = Command::new("/usr/sbin/dladm")
            .args(&["show-vnic", "-p", "-o", "link,macaddress", interface_name])
            .output();

        if let Ok(output) = vnic_output {
            let mac_text = String::from_utf8_lossy(&output.stdout).to_string();
            for line in mac_text.lines() {
                let fields: Vec<&str> = line.split(':').collect();
                if fields.len() >= 2 && fields[0] == interface_name {
                    let raw_mac = fields[1].trim();
                    if !raw_mac.is_empty() && raw_mac != "0" {
                        mac_address = Some(raw_mac.to_string());
                        debug!("Found MAC address for {} with VNIC method: {}", interface_name, raw_mac);
                    }
                    break;
                }
            }
        }
    }

    if mac_address.is_none() {
        debug!("Could not determine MAC address for {}", interface_name);
    }

    // Try to get MTU using dladm show-link
    trace!("Attempting to get MTU for {}", interface_name);
    let mtu_output = Command::new("/usr/sbin/dladm")
        .args(&["show-link", "-p", "-o", "link,mtu", interface_name])
        .output();

    if let Ok(output) = mtu_output {
        let mtu_text = String::from_utf8_lossy(&output.stdout).to_string();
        for line in mtu_text.lines() {
            let fields: Vec<&str> = line.split(':').collect();
            if fields.len() >= 2 && fields[0] == interface_name {
                // Try to parse the MTU value
                if let Ok(mtu_value) = fields[1].trim().parse::<i64>() {
                    if mtu_value > 0 {
                        mtu = Some(mtu_value);
                        debug!("Found MTU for {}: {}", interface_name, mtu_value);
                    }
                }
                break;
            }
        }
    }

    if mtu.is_none() {
        debug!("Could not determine MTU for {}", interface_name);
    }

    Ok((mac_address, mtu))
}

pub async fn discover_zones(client: Arc<Client>, host_id: Uuid, max_retries: usize) -> Result<HashMap<String, Uuid>> {
    let mut zones = HashMap::new();

    // Get zone list from system
    debug!("Retrieving zone list from system");
    let output = Command::new("/usr/sbin/zoneadm")
        .arg("list")
        .arg("-p")
        .output()
        .context("Failed to run zoneadm command")?;

    let zone_output = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in zoneadm output")?;

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

            // Check if zone exists in database with retry logic
            let zone_client = Arc::clone(&client);
            let zone_host_id = host_id;
            let zone_name_clone = zone_name.clone();
            let zone_status_clone = zone_status.clone();

            let zone_id = execute_with_retry(move || {
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
            .await?;

            zones.insert(zone_name, zone_id);
        }
    }

    info!("Discovered {} zones", zones.len());
    trace!("Zone details: {:#?}", zones);
    Ok(zones)
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

    // Get all datalinks (including physical, etherstub, vnic)
    debug!("Retrieving datalink information from system");
    let output = Command::new("/usr/sbin/dladm")
        .args(&["show-link", "-p", "-o", "link,class"])
        .output()
        .context("Failed to run dladm show-link command")?;

    let link_output = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in dladm output")?;

    if verbose {
        debug!("Discovered interfaces:");
    }

    // Process all links
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].to_string();
            let interface_type = fields[1].to_string();

            if verbose {
                trace!("Found interface: {} (type: {})", interface_name, interface_type);
            }
        }
    }

    // Get VNIC parent relationships
    debug!("Retrieving VNIC parent relationships");
    let vnic_output = Command::new("/usr/sbin/dladm")
        .args(&["show-vnic", "-p", "-o", "link,over"])
        .output()
        .context("Failed to run dladm show-vnic command")?;

    let vnic_output = String::from_utf8(vnic_output.stdout)
        .context("Invalid UTF-8 in dladm vnic output")?;

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

    // First process all physical interfaces
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
                    Arc::clone(&client),
                    host_id,
                    None, // Physical interfaces are in global zone
                    interface_name.clone(),
                    interface_type.clone(),
                    None, // No parent for physical interfaces
                    max_retries,
                ).await?;

                interfaces.insert(interface_name.clone(), NetworkInterface {
                    interface_id,
                    host_id,
                    zone_id: None,
                    interface_name,
                    interface_type,
                    parent_interface: None,
                });
            }
        }
    }

    // Next process all etherstubs
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
                    Arc::clone(&client),
                    host_id,
                    None, // Etherstubs are in global zone
                    interface_name.clone(),
                    interface_type.clone(),
                    None, // No parent for etherstubs
                    max_retries,
                ).await?;

                interfaces.insert(interface_name.clone(), NetworkInterface {
                    interface_id,
                    host_id,
                    zone_id: None,
                    interface_name,
                    interface_type,
                    parent_interface: None,
                });
            }
        }
    }

    // Finally process all VNICs
    debug!("Processing VNIC interfaces");
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].to_string();
            let interface_type = fields[1].to_string();

            if interface_type == "vnic" {
                // It's a VNIC - get its parent
                let parent_interface = vnic_parents.get(&interface_name).cloned();
                trace!("Processing VNIC interface: {} (parent: {:?})", interface_name, parent_interface);

                // Get the zone_id for this interface (default to None for global zone)
                let zone_id = zone_interface_map.get(&interface_name).cloned();

                if verbose && zone_id.is_some() {
                    // Using a let binding to create a longer-lived value
                    let unknown = "unknown".to_string();
                    let zone_name = zones.iter()
                        .find_map(|(name, id)| if *id == zone_id.unwrap() { Some(name) } else { None })
                        .unwrap_or(&unknown);
                    debug!("Interface {} belongs to zone {}", interface_name, zone_name);
                }

                let interface_id = ensure_interface_exists(
                    Arc::clone(&client),
                    host_id,
                    zone_id,
                    interface_name.clone(),
                    interface_type.clone(),
                    parent_interface.clone(),
                    max_retries,
                ).await?;

                interfaces.insert(interface_name.clone(), NetworkInterface {
                    interface_id,
                    host_id,
                    zone_id,
                    interface_name,
                    interface_type,
                    parent_interface,
                });
            }
        }
    }

    debug!("Discovered {} interfaces", interfaces.len());

    if verbose {
        debug!("Interface details:");
        for (name, interface) in &interfaces {
            let zone_info = match &interface.zone_id {
                Some(id) => {
                    let zone_name = zones.iter()
                                         .find(|&(_, zone_id)| *zone_id == *id)
                                         .map(|(name, _)| name.clone())
                                         .unwrap_or_else(|| "unknown".to_string());
                    format!("zone: {}", zone_name)
                },
                None => "global zone".to_string(),
            };

            debug!("Interface: {} (type: {}, {}, parent: {:?})",
                   name,
                   interface.interface_type,
                   zone_info,
                   interface.parent_interface.as_deref().unwrap_or("none")
            );
        }
    }

    Ok(interfaces)
}

// Function to build the mapping between interface names and zone IDs
async fn build_zone_interface_map(
    zones: &HashMap<String, Uuid>,
    verbose: bool,
) -> Result<HashMap<String, Uuid>> {
    let mut zone_interface_map = HashMap::new();

    info!("Building zone-interface map with {} zones", zones.len());

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
            // Format should be like:
            // net:
            //     address not specified
            //     allowed-address not specified
            //     defrouter not specified
            //     global-nic: switch0
            //     mac-addr: 2:8:20:55:0:5d
            //     physical: http0
            //     vlan-id not specified

            for line in zonecfg_text.lines() {
                let line = line.trim();

                if line.starts_with("physical:") {
                    // Extract interface name after "physical:"
                    if let Some(iface_name) = line.split(':').nth(1) {
                        let interface_name = iface_name.trim().to_string();

                        info!("Found interface {} in zone {}", interface_name, zone_name);
                        zone_interface_map.insert(interface_name, *zone_uuid);
                    }
                }
            }
        } else if let Err(e) = &zonecfg_output {
            warn!("Error running zonecfg for {}: {}", zone_name, e);
        }
    }

    // If we still don't have any mappings, try with dladm show-link -Z
    if zone_interface_map.is_empty() {
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
                            info!("Associating interface {} with zone {}", interface_name, zone_name);
                            zone_interface_map.insert(interface_name.to_string(), *zone_uuid);
                        }
                    }
                }
            }
        }
    }

    // Display the mapping we've built
    if zone_interface_map.is_empty() {
        warn!("WARNING: No zone-interface mappings found!");
    } else {
        info!("Built zone-interface map with {} entries", zone_interface_map.len());

        for (interface, zone_uuid) in &zone_interface_map {
            // Find zone name for the UUID
            // Create a longer-lived string to avoid the borrowing issue
            let unknown = "unknown".to_string();

            let zone_name = zones.iter()
                .find_map(|(name, id)| if *id == *zone_uuid { Some(name) } else { None })
                .unwrap_or(&unknown);

            debug!("Interface {} → Zone {} ({})", interface, zone_name, zone_uuid);
        }
    }

    Ok(zone_interface_map)
}

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
    let (mac_address, mtu) = get_interface_details(&interface_name).await
        .unwrap_or((None, None));

    // Execute with retry logic
    let interface_id = execute_with_retry(move || {
        let client = Arc::clone(&client);
        let host_id = host_id;
        let zone_id = zone_id;
        let interface_name = interface_name.clone();
        let interface_type = interface_type.clone();
        let parent_interface = parent_interface.clone();
        let mac_address = mac_address.clone();
        let mtu = mtu;

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
                Some(row) => {
                    let interface_id: Uuid = row.get(0);
                    // Update interface information
                    client
                        .execute(
                            "UPDATE interfaces SET
                             interface_type = $1,
                             parent_interface = $2,
                             mac_address = $3,
                             mtu = $4,
                             is_active = true
                             WHERE interface_id = $5",
                            &[&interface_type, &parent_interface, &mac_address, &mtu, &interface_id],
                        )
                        .await
                        .context("Failed to update interface")?;

                    let zone_desc = match &zone_id {
                        Some(id) => format!("zone ID: {}", id),
                        None => "global zone".to_string(),
                    };

                    trace!("Updated existing interface record: {} - {} ({}, MAC: {:?}, MTU: {:?})",
                          interface_name, interface_id, zone_desc, mac_address, mtu);
                    interface_id
                }
                None => {
                    // Create new interface
                    let interface_id = Uuid::new_v4();
                    client
                        .execute(
                            "INSERT INTO interfaces (
                             interface_id, host_id, zone_id, interface_name,
                             interface_type, parent_interface, mac_address, mtu,
                             is_active, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true, CURRENT_TIMESTAMP)",
                            &[&interface_id, &host_id, &zone_id, &interface_name, &interface_type,
                              &parent_interface, &mac_address, &mtu],
                        )
                        .await
                        .context("Failed to insert interface")?;

                    let zone_desc = match &zone_id {
                        Some(id) => format!("zone ID: {}", id),
                        None => "global zone".to_string(),
                    };

                    info!("Created new interface record: {} - {} ({}, MAC: {:?}, MTU: {:?})",
                         interface_name, interface_id, zone_desc, mac_address, mtu);
                    interface_id
                }
            };

            Ok::<_, Error>(interface_id)
        })
    }, max_retries)
    .await?;

    Ok(interface_id)
}
