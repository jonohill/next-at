use super::error::RtResult;
use super::utils::find_trip_run;
use crate::db::util::OptionMapSet;
use crate::entity::prelude::*;
use crate::entity::vehicle;
use crate::gtfs::structure::realtime::FeedEntity;
use sea_orm::prelude::*;
use sea_orm::IntoActiveModel;
use sea_orm::{ConnectionTrait, Set};

pub async fn process_vehicle(tx: &impl ConnectionTrait, entity: FeedEntity) -> RtResult<()> {
    let vehicle = entity.vehicle.expect("Expected vehicle to be set");

    let timestamp = vehicle.timestamp.unwrap_or_else(chrono::Utc::now);

    let (lat, lng, bearing, speed) = match vehicle.position {
        Some(p) => (
            Some(p.latitude as f64),
            Some(p.longitude as f64),
            match p.bearing {
                Some(b) => Some(b.into_inner()? as f64),
                None => None,
            },
            Some(p.speed.map(|s| s as f64)).flatten(),
        ),
        None => (None, None, None, None),
    };

    let trip = vehicle.trip;

    let vehicle = vehicle
        .vehicle
        .ok_or_else(|| crate::gtfs::realtime::Error::InvalidData("No vehicle data".to_string()))?;
    let vehicle_id = vehicle
        .id
        .ok_or_else(|| crate::gtfs::realtime::Error::InvalidData("No vehicle id".to_string()))?;

    let mut db_vehicle = Vehicle::find()
        .filter(vehicle::Column::VehicleId.eq(vehicle_id.clone()))
        .one(tx)
        .await?
        .map(|v| v.into_active_model())
        .unwrap_or_else(|| vehicle::ActiveModel {
            vehicle_id: Set(vehicle_id.clone()),
            ..Default::default()
        });

    db_vehicle.label = vehicle.label.map_set();
    db_vehicle.license_plate = vehicle.license_plate.map_set();
    db_vehicle.latitude = lat.map_set();
    db_vehicle.longitude = lng.map_set();
    db_vehicle.bearing = bearing.map_set();
    db_vehicle.speed = speed.map_set();
    db_vehicle.timestamp = Set(timestamp.timestamp_millis());

    db_vehicle.save(tx).await?;

    // And update the trip if the vehicle is on one
    if let Some(trip) = trip {
        let mut trip_run = find_trip_run(tx, trip).await?.into_active_model();
        trip_run.vehicle_id = Set(Some(vehicle_id));
        trip_run.save(tx).await?;
    }

    Ok(())
}
