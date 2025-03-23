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
) -> Result<HashMap<String, NetworkInterface>> {
    let mut interfaces = HashMap::new();
    let mut zone_interface_map = HashMap::new();

    // First, get all interfaces that might appear in dlstat
    // We'll use dladm show-link for this, which shows all datalinks
    let output = Command::new("/usr/sbin/dladm")
        .args(&["show-link", "-p", "-o", "link,class"])
        .output()
        .context("Failed to run dladm show-link command")?;

    let link_output = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in dladm output")?;

    // Create a mapping of all interfaces and their types
    let mut interface_types = HashMap::new();
    for line in link_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 2 {
            let interface_name = fields[0].to_string();
            let interface_type = fields[1].to_string();
            interface_types.insert(interface_name, interface_type);
        }
    }

    // Now get VNIC information (to establish parent relationships)
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

    // For each zone, find what interfaces are assigned to it
    // We skip the global zone because it's handled differently
    for (zone_name, zone_id) in zones {
        if zone_name == "global" {
            continue;
        }

        // Use zonecfg to get all net resources for the zone
        let zonecfg_output = Command::new("/usr/sbin/zonecfg")
            .args(&["-z", zone_name, "info", "net"])
            .output()
            .context(format!("Failed to run zonecfg for zone {}", zone_name))?;

        let zonecfg_output = String::from_utf8(zonecfg_output.stdout)
            .context("Invalid UTF-8 in zonecfg output")?;


        let mut current_interface: Option<String> = None;
        let mut current_parent: Option<String> = None;

        for line in zonecfg_output.lines() {
            let line = line.trim();

            if line.starts_with("net:") {
                // If we have collected a complete interface entry, store it
                if let (Some(interface), Some(parent)) = (&current_interface, &current_parent) {
                    println!("Mapped interface {} (parent: {}) to zone {}",
                          interface, parent, zone_name);
                    vnic_parents.insert(interface.clone(), parent.clone());
                }

                // Reset for the next interface
                current_interface = None;
                current_parent = None;
            } else if line.starts_with("physical:") {
                if let Some(parts) = line.split_once(':') {
                    let interface_name = parts.1.trim().to_string();
                    current_interface = Some(interface_name.clone());

                    // Map this interface to this zone
                    zone_interface_map.insert(interface_name, *zone_id);
                }
            } else if line.starts_with("global-nic:") {
                if let Some(parts) = line.split_once(':') {
                    let parent_name = parts.1.trim().to_string();
                    current_parent = Some(parent_name);
                }
            }
        }

        // Handle the last interface in the output if there is one
        if let (Some(interface), Some(parent)) = (&current_interface, &current_parent) {
            println!("Mapped interface {} (parent: {}) to zone {}",
                  interface, parent, zone_name);
            vnic_parents.insert(interface.clone(), parent.clone());
        }
    }

    // Now process all interfaces, using the zone mapping we've built
    // First check dlstat directly to get all interfaces it knows about
    let dlstat_output = Command::new("/usr/sbin/dlstat")
        .output()
        .context("Failed to run dlstat command")?;

    let dlstat_output = String::from_utf8(dlstat_output.stdout)
        .context("Invalid UTF-8 in dlstat output")?;

    let mut line_count = 0;
    let mut all_dlstat_interfaces = Vec::new();

    for line in dlstat_output.lines() {
        line_count += 1;

        // Skip header lines
        if line_count <= 2 {
            continue;
        }

        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() >= 5 {
            let interface_name = fields[0].to_string();
            all_dlstat_interfaces.push(interface_name);
        }
    }

    // Finally, register all interfaces with their correct zone and type information
    for interface_name in all_dlstat_interfaces {
        let interface_type = interface_types.get(&interface_name)
            .cloned()
            .unwrap_or_else(|| {
                // Make an educated guess based on naming conventions
                if interface_name.starts_with("igb") ||
                   interface_name.starts_with("e1000g") ||
                   interface_name.starts_with("bge") ||
                   interface_name.starts_with("ixgbe") {
                    "physical".to_string()
                } else {
                    "unknown".to_string()
                }
            });

        let parent_interface = vnic_parents.get(&interface_name).cloned();

        // Get the zone_id for this interface
        let zone_id = zone_interface_map.get(&interface_name).cloned();

        // Store interface in database and get UUID
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

    println!("Discovered {} interfaces", interfaces.len());
    for (name, interface) in &interfaces {
        let zone_info = match interface.zone_id {
            Some(id) => {
                let zone_name = zones.iter()
                                     .find(|&(_, zone_id)| *zone_id == id)  // Use explicit reference pattern
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
            interface.parent_interface
        );
    }

    Ok(interfaces)
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
