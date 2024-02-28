use chrono::NaiveDateTime;
use geo::Point;
use sea_orm::FromQueryResult;
use serde::{Deserialize, Serialize};

use crate::{geo::get_bounding_box, gtfs::structure::GtfsStop};

const SEARCH_DISTANCE_METRES: f64 = 1000.0;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GtfsStopIndex {
    pub id: i64,
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

impl From<GtfsStop> for GtfsStopIndex {
    fn from(stop: GtfsStop) -> Self {
        let id = stop.id.expect("Stop must have an id");
        let lng = stop.stop_lon.expect("Stop must have a longitude");
        let lat = stop.stop_lat.expect("Stop must have a latitude");

        let bounding_box = get_bounding_box(Point::new(lng, lat), SEARCH_DISTANCE_METRES);
        let min = bounding_box.min();
        let max = bounding_box.max();

        GtfsStopIndex {
            id,
            min_lat: min.y,
            max_lat: max.y,
            min_lon: min.x,
            max_lon: max.x,
        }
    }
}

/// Mapped from database
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RawStopArrival {
    pub trip_id: String,
    pub stop_sequence: u32,
    pub route_short_name: String,
    pub stop_headsign: String,
    pub trip_start_time: String,
    pub arrival_time: String,
}

#[derive(Debug, Serialize, Clone)]
pub enum Freshness {
    Realtime,
    Scheduled,
}

#[derive(Debug, Serialize, Clone)]
pub enum ArrivalStatus {
    Cancelled,
    TravellingTo,
    Departed,
}

fn to_iso_datetime<S>(time: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&time.format("%Y-%m-%dT%H:%M:%S").to_string())
}
