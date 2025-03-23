use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug)]
pub struct NetworkInterface {
    pub interface_id: Uuid,
    pub host_id: Uuid,
    pub zone_id: Option<Uuid>,
    pub interface_name: String,
    pub interface_type: String,
    pub parent_interface: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NetworkMetric {
    pub interface_name: String,
    pub input_bytes: i64,
    pub input_packets: i64,
    pub output_bytes: i64,
    pub output_packets: i64,
    pub timestamp: DateTime<Utc>,
}
