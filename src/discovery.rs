use crate::database::execute_with_retry;
use crate::models::NetworkInterface;
use anyhow::{Context, Error, Result};
use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;
use tokio_postgres::Client;
use uuid::Uuid;

pub async fn discover_zones(client: Arc<Client>, host_id: Uuid, max_retries: usize) -> Result<HashMap<String, Uuid>> {
    let mut zones = HashMap::new();

    // Get zone list from system
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
                            println!("Created new zone record: {} - {}", zone_name, id);
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

    println!("Discovered {} zones", zones.len());
    Ok(zones)
}

pub async fn discover_interfaces(
    client: Arc<Client>,
    host_id: Uuid,
    zones: &HashMap<String, Uuid>,
    max_retries: usize,
    verbose: bool,
) -> Result<HashMap<String, NetworkInterface>> {
    let mut interfaces = HashMap::new();

    // Create a mapping between interfaces and zones
    let zone_interface_map = build_zone_interface_map(zones, verbose).await?;

    // Get all datalinks (including physical, etherstub, vnic)
    let output = Command::new("/usr/sbin/dladm")
        .args(&["show-link", "-p", "-o", "link,class"])
        .output()
        .context("Failed to run dladm show-link command")?;

    let link_output = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in dladm output")?;

    if verbose {
        println!("Discovered interfaces:");
    }

    // Process all links
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].to_string();
            let interface_type = fields[1].to_string();

            if verbose {
                println!("Found interface: {} (type: {})", interface_name, interface_type);
            }
        }
    }

    // Get VNIC parent relationships
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
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].to_string();
            let interface_type = fields[1].to_string();

            if interface_type == "phys" {
                // It's a physical interface
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
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].to_string();
            let interface_type = fields[1].to_string();

            if interface_type == "etherstub" {
                // It's an etherstub
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
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].to_string();
            let interface_type = fields[1].to_string();

            if interface_type == "vnic" {
                // It's a VNIC - get its parent
                let parent_interface = vnic_parents.get(&interface_name).cloned();

                // Get the zone_id for this interface (default to None for global zone)
                let zone_id = zone_interface_map.get(&interface_name).cloned();

                if verbose && zone_id.is_some() {
                    // Using a let binding to create a longer-lived value
                    let unknown = "unknown".to_string();
                    let zone_name = zones.iter()
                        .find_map(|(name, id)| if *id == zone_id.unwrap() { Some(name) } else { None })
                        .unwrap_or(&unknown);
                    println!("Interface {} belongs to zone {}", interface_name, zone_name);
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

    if verbose {
        println!("Discovered {} interfaces", interfaces.len());
        for (name, interface) in &interfaces {
            let zone_info = match interface.zone_id {
                Some(id) => {
                    let zone_name = zones.iter()
                                         .find(|&(_, zone_id)| *zone_id == id)
                                         .map(|(name, _)| name.clone())
                                         .unwrap_or_else(|| "unknown".to_string());
                    format!("zone: {}", zone_name)
                },
                None => "global zone".to_string(),
            };

            println!("Interface: {} (type: {}, {}, parent: {:?})",
                     name,
                     interface.interface_type,
                     zone_info,
                     interface.parent_interface.as_deref().unwrap_or("none")
            );
        }
    }

    Ok(interfaces)
}

// This is a new function to build the mapping between interface names and zone IDs
async fn build_zone_interface_map(
    zones: &HashMap<String, Uuid>,
    _verbose: bool,
) -> Result<HashMap<String, Uuid>> {
    let mut zone_interface_map = HashMap::new();

    println!("Building zone-interface map with {} zones", zones.len());

    // First, find out which zones are running
    let output = Command::new("/usr/sbin/zoneadm")
        .args(&["list", "-p"])
        .output()
        .context("Failed to run zoneadm list command")?;

    let zone_output = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in zoneadm output")?;

    // Debug: Show raw zoneadm output
    println!("Raw zoneadm output:");
    for line in zone_output.lines() {
        println!("  {}", line);
    }

    // Process each running zone (except global) to get its associated VNICs
    for line in zone_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 3 {
            let _zone_id_str = fields[0].to_string(); // Numeric zone ID (not used)
            let zone_name = fields[1].to_string();
            let zone_status = fields[2].to_string();

            println!("Processing zone: {} (status: {})", zone_name, zone_status);

            // Skip if zone is not running or is global
            if zone_status != "running" || zone_name == "global" {
                println!("  Skipping zone (not running or global zone)");
                continue;
            }

            // Get zone UUID from our map
            if let Some(zone_uuid) = zones.get(&zone_name) {
                println!("  Found zone UUID: {}", zone_uuid);

                // Get VNICs for this specific zone
                let vnic_cmd = format!("/usr/sbin/dladm show-vnic -p -z {} -o link", zone_name);
                println!("  Running command: {}", vnic_cmd);

                let vnic_output = Command::new("/usr/sbin/dladm")
                    .args(&["show-vnic", "-p", "-z", &zone_name, "-o", "link"])
                    .output()
                    .context(format!("Failed to run dladm show-vnic for zone {}", zone_name))?;

                let vnic_text = String::from_utf8(vnic_output.stdout)
                    .context("Invalid UTF-8 in dladm vnic output")?;

                // Debug: Show raw dladm output
                println!("  Raw dladm show-vnic output for zone {}:", zone_name);
                for line in vnic_text.lines() {
                    println!("    {}", line);
                }

                // Map each VNIC to this zone's UUID
                for vnic_line in vnic_text.lines() {
                    let vnic_name = vnic_line.trim();
                    if !vnic_name.is_empty() {
                        println!("  Associating VNIC {} with zone {}", vnic_name, zone_name);
                        zone_interface_map.insert(vnic_name.to_string(), *zone_uuid);
                    }
                }

                // If we didn't find any VNICs, show stderr in case of error
                if vnic_text.trim().is_empty() {
                    let stderr = String::from_utf8_lossy(&vnic_output.stderr);
                    println!("  No VNICs found. stderr: {}", stderr);
                }
            } else {
                println!("  Zone UUID not found for zone: {}", zone_name);
            }
        }
    }

    // If we couldn't get zone info with dladm, try another approach with zonecfg
    if zone_interface_map.is_empty() {
        println!("No interfaces found with dladm, trying zonecfg approach");

        for (zone_name, zone_uuid) in zones {
            // Skip global zone
            if zone_name == "global" {
                continue;
            }

            println!("Trying zonecfg for zone: {}", zone_name);

            // Use zonecfg to get interface info - assuming zone has anet resources configured
            let zonecfg_cmd = format!("/usr/sbin/zonecfg -z {} info anet", zone_name);
            println!("  Running command: {}", zonecfg_cmd);

            let zonecfg_output = Command::new("/usr/sbin/zonecfg")
                .args(&["-z", zone_name, "info", "anet"])
                .output();

            // Only process if command succeeded
            if let Ok(output) = zonecfg_output {
                let anet_text = String::from_utf8_lossy(&output.stdout);

                // Debug: Show raw zonecfg output
                println!("  Raw zonecfg output for zone {}:", zone_name);
                for line in anet_text.lines() {
                    println!("    {}", line);
                }

                // Parse the output to extract interface names
                // Format is typically: name: <vnic_name>
                for line in anet_text.lines() {
                    if line.contains("lower-link:") {
                        // This isn't the interface name, just the parent
                        continue;
                    }

                    if line.contains("name:") {
                        if let Some(vnic_name) = line.split(':').nth(1) {
                            let vnic_name = vnic_name.trim();
                            println!("  From zonecfg: Associating VNIC {} with zone {}", vnic_name, zone_name);
                            zone_interface_map.insert(vnic_name.to_string(), *zone_uuid);
                        }
                    }
                }
            } else if let Err(e) = &zonecfg_output {
                println!("  Error running zonecfg for {}: {}", zone_name, e);
            }
        }
    }

    println!("Zone-interface map built with {} entries", zone_interface_map.len());

    // Display the entire map for debugging
    if !zone_interface_map.is_empty() {
        println!("Zone-interface map contents:");
        for (interface, zone_id) in &zone_interface_map {
            println!("  Interface {} → Zone ID {}", interface, zone_id);
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
    // Execute with retry logic
    let interface_id = execute_with_retry(move || {
        let client = Arc::clone(&client);
        let host_id = host_id;
        let zone_id = zone_id;
        let interface_name = interface_name.clone();
        let interface_type = interface_type.clone();
        let parent_interface = parent_interface.clone();

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
                             is_active = true
                             WHERE interface_id = $3",
                            &[&interface_type, &parent_interface, &interface_id],
                        )
                        .await
                        .context("Failed to update interface")?;
                    interface_id
                }
                None => {
                    // Create new interface
                    let interface_id = Uuid::new_v4();
                    client
                        .execute(
                            "INSERT INTO interfaces (
                             interface_id, host_id, zone_id, interface_name,
                             interface_type, parent_interface, is_active, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, true, CURRENT_TIMESTAMP)",
                            &[&interface_id, &host_id, &zone_id, &interface_name, &interface_type, &parent_interface],
                        )
                        .await
                        .context("Failed to insert interface")?;
                    println!("Created new interface record: {} - {}", interface_name, interface_id);
                    interface_id
                }
            };

            Ok::<_, Error>(interface_id)
        })
    }, max_retries)
    .await?;

    Ok(interface_id)
}
