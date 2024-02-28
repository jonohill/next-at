use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AtEntity<Attributes> {
    pub id: String,
    pub attributes: Attributes,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AtEntityList<Attributes> {
    pub data: Vec<AtEntity<Attributes>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Stop {
    // TODO what is this
    pub location_type: u32,
    pub parent_station: Option<String>,
    pub platform_code: Option<String>,
    pub stop_code: String,
    pub stop_id: String,
    pub stop_lat: f64,
    pub stop_lon: f64,
    pub stop_name: String,
}
