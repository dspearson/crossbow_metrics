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

    // First, get physical interfaces
    let output = Command::new("/usr/sbin/dladm")
        .args(&["show-phys", "-p", "-o", "link,class,state,mtu"])
        .output()
        .context("Failed to run dladm show-phys command")?;

    let phys_output = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in dladm output")?;

    // Process physical interfaces
    for line in phys_output.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 3 {
            let interface_name = fields[0].to_string();
            let interface_type = fields[1].to_string();

            // Store interface in database and get UUID
            let interface_id = ensure_interface_exists(
                Arc::clone(&client),
                host_id,
                None, // No zone for physical interfaces
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

    // Get virtual interfaces (VNICs)
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

    // Now get zone information - for each zone, check which VNICs are assigned to it
    for (zone_name, zone_id) in zones {
        // Skip global zone as we'll assign unmatched VNICs to it later
        if zone_name == "global" {
            continue;
        }

        // Get network interfaces for this zone
        let zone_nics = Command::new("/usr/sbin/zonecfg")
            .args(&["-z", zone_name, "info", "net"])
            .output()
            .context(format!("Failed to run zonecfg for zone {}", zone_name))?;

        let zone_nics_output = String::from_utf8(zone_nics.stdout)
            .context("Invalid UTF-8 in zonecfg output")?;

        // Parse zonecfg output to find zone network interfaces
        let mut current_vnic = None;
        for line in zone_nics_output.lines() {
            let line = line.trim();

            if line.starts_with("net:") {
                current_vnic = None;
            } else if line.starts_with("physical:") {
                // Extract the interface name from "physical: vnic_name"
                if let Some(vnic_name) = line.strip_prefix("physical:") {
                    current_vnic = Some(vnic_name.trim().to_string());
                }
            }

            // When we have identified a VNIC for this zone
            if let Some(vnic_name) = &current_vnic {
                let parent_interface = vnic_parents.get(vnic_name).cloned();

                // Store interface in database and get UUID
                let interface_id = ensure_interface_exists(
                    Arc::clone(&client),
                    host_id,
                    Some(*zone_id),
                    vnic_name.clone(),
                    "vnic".to_string(),
                    parent_interface.clone(),
                    max_retries,
                ).await?;

                interfaces.insert(vnic_name.clone(), NetworkInterface {
                    interface_id,
                    host_id,
                    zone_id: Some(*zone_id),
                    interface_name: vnic_name.clone(),
                    interface_type: "vnic".to_string(),
                    parent_interface,
                });

                // Remove from vnic_parents so we don't process it twice
                vnic_parents.remove(vnic_name);
            }
        }
    }

    // Process remaining VNICs (those not assigned to a specific zone)
    for (vnic_name, parent_interface) in vnic_parents {
        // Store interface in database and get UUID
        let interface_id = ensure_interface_exists(
            Arc::clone(&client),
            host_id,
            None, // No specific zone - belongs to global zone
            vnic_name.clone(),
            "vnic".to_string(),
            Some(parent_interface.clone()),
            max_retries,
        ).await?;

        interfaces.insert(vnic_name.clone(), NetworkInterface {
            interface_id,
            host_id,
            zone_id: None,
            interface_name: vnic_name,
            interface_type: "vnic".to_string(),
            parent_interface: Some(parent_interface),
        });
    }

    // Check for etherstubs as well
    let output = Command::new("/usr/sbin/dladm")
        .args(&["show-etherstub", "-p"])
        .output()
        .context("Failed to run dladm show-etherstub command")?;

    let etherstub_output = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in dladm etherstub output")?;

    // Process etherstubs
    for line in etherstub_output.lines() {
        let etherstub_name = line.trim().to_string();
        if !etherstub_name.is_empty() {
            // Store interface in database and get UUID
            let interface_id = ensure_interface_exists(
                Arc::clone(&client),
                host_id,
                None,
                etherstub_name.clone(),
                "etherstub".to_string(),
                None,
                max_retries,
            ).await?;

            interfaces.insert(etherstub_name.clone(), NetworkInterface {
                interface_id,
                host_id,
                zone_id: None,
                interface_name: etherstub_name,
                interface_type: "etherstub".to_string(),
                parent_interface: None,
            });
        }
    }

    println!("Discovered {} interfaces", interfaces.len());
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
