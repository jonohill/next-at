use crate::db::util::OptionMapSet;
use chrono::Duration;
use chrono::Utc;
use itertools::Itertools;
use sea_orm::sea_query::all;
use sea_orm::sea_query::OnConflict;
use sea_orm::ActiveModelTrait;
use sea_orm::ConnectionTrait;
use sea_orm::QueryOrder;
use sea_orm::Set;
use sea_orm::TryIntoModel;

use super::error::Error;
use super::error::RtResult;
use crate::db::links::TripAgency;
use crate::db::util::col;
use crate::entity::gtfs_stop_times;
use crate::entity::prelude::*;
use crate::entity::stop_time_index;
use crate::entity::trip_run;
use crate::entity::vehicle;
use crate::gtfs::realtime::FeedEntity;
use crate::gtfs::structure::realtime::trip_descriptor::ScheduleRelationship;
use crate::gtfs::structure::realtime::TripDescriptor;
use crate::gtfs::structure::realtime::VehicleDescriptor;
use crate::gtfs::utils::GtfsDateTimeParser;
use sea_orm::ColumnTrait;
use sea_orm::EntityTrait;
use sea_orm::IntoActiveModel;
use sea_orm::QueryFilter;

use super::utils::find_trip_run;

async fn duplicate_trip_run(
    db: &impl ConnectionTrait,
    trip_descriptor: &TripDescriptor,
) -> RtResult<trip_run::Model> {
    let start_date = trip_descriptor
        .start_date
        .as_ref()
        .ok_or_else(|| Error::InvalidData("Need start_date to duplicate trip".to_string()))?
        .clone();
    let start_time = trip_descriptor
        .start_time
        .as_ref()
        .ok_or_else(|| Error::InvalidData("Need start_time to duplicate trip".to_string()))?
        .clone();

    let mut date_parser = GtfsDateTimeParser::new();

    let trip_id = trip_descriptor
        .trip_id
        .as_ref()
        .ok_or_else(|| Error::InvalidData("Need trip_id to duplicate trip".to_string()))?
        .clone();

    let (trip, Some(agency)) = GtfsTrips::find()
        .find_also_linked(TripAgency)
        .filter(trip_run::Column::TripId.eq(&trip_id))
        .one(db)
        .await?
        .ok_or_else(|| Error::NotFound(format!("Trip not found: {}", &trip_id)))?
    else {
        return Err(Error::NotFound(format!(
            "Agency not found for trip: {}",
            &trip_id
        )));
    };
    let tz = agency.agency_timezone.unwrap_or_else(|| "UTC".to_string());
    let new_date = date_parser.parse_date(&start_date)?;
    let new_date_time = date_parser.parse_time(&new_date, &start_time, &tz)?;

    // check up front if trip run already exists
    // to avoid bothering with both trip run and the stop times
    let existing_trip_run = TripRun::find()
        .filter(all![
            trip_run::Column::TripId.eq(&trip_id),
            trip_run::Column::StartTimestamp.eq(new_date_time.timestamp_millis())
        ])
        .one(db)
        .await?;

    if let Some(existing_trip_run) = existing_trip_run {
        return Ok(existing_trip_run);
    }

    let new_trip_run = trip_run::ActiveModel {
        trip_id: Set(trip.trip_id.clone()),
        route_id: Set(trip.route_id.clone()),
        direction_id: Set(trip.direction_id),
        start_date: Set(start_date),
        start_timestamp: Set(new_date_time.timestamp_millis()),
        schedule_relationship: Set(ScheduleRelationship::Duplicated as i32),
        ..Default::default()
    };

    let trip_run = TripRun::insert(new_trip_run)
        .on_conflict(
            // ignore duplicate trip already created
            OnConflict::columns([trip_run::Column::TripId, trip_run::Column::StartTimestamp])
                .do_nothing()
                .to_owned(),
        )
        .exec_with_returning(db)
        .await?;

    // Copy over the stop times
    // assumption: gap between stop times is the same as the original trip
    let stop_times = GtfsStopTimes::find()
        .filter(gtfs_stop_times::Column::TripId.eq(&trip_id))
        .order_by_asc(gtfs_stop_times::Column::StopSequence)
        .all(db)
        .await?;

    if stop_times.is_empty() {
        return Err(Error::NotFound(format!(
            "No stop times found for trip: {}",
            &trip_id
        )));
    }

    let original_first_time =
        date_parser.parse_time(&new_date, &stop_times[0].departure_time, &tz)?;

    for stop_time in stop_times {
        let original_date_time =
            date_parser.parse_time(&new_date, &stop_time.departure_time, &tz)?;
        let stop_delta =
            original_date_time.timestamp_millis() - original_first_time.timestamp_millis();

        let arrival_date_time = new_date_time + Duration::milliseconds(stop_delta);

        stop_time_index::ActiveModel {
            stop_id: Set(stop_time.stop_id),
            stop_sequence: Set(stop_time.stop_sequence),
            trip_id: Set(trip_id.clone()),
            trip_run_id: Set(trip_run.id),
            arrival_timestamp: Set(arrival_date_time.timestamp_millis()),
            ..Default::default()
        }
        .insert(db)
        .await?;
    }

    Ok(trip_run)
}

pub async fn process_trip_update(db: &impl ConnectionTrait, entity: FeedEntity) -> RtResult<()> {
    let trip_update = entity.trip_update.expect("Expected trip_update to be set");

    let sr = trip_update.trip.schedule_relationship;
    let mut trip_run = match sr {
        Some(ScheduleRelationship::Scheduled | ScheduleRelationship::Canceled) | Some(ScheduleRelationship::Deleted) => {
            let mut trip_run = find_trip_run(db, trip_update.trip)
                .await?
                .into_active_model();
            trip_run.schedule_relationship = Set(sr.unwrap() as i32);
            trip_run.save(db).await?
        }
        Some(ScheduleRelationship::Duplicated) => duplicate_trip_run(db, &trip_update.trip)
            .await?
            .into_active_model(),
        _ => {
            log::info!(
                "Got unimplemented trip schedule relationship: {:?}",
                trip_update
            );
            return Ok(());
        }
    };

    trip_run = match trip_update.vehicle {
        Some(VehicleDescriptor {
            id: Some(vehicle_id),
            label: vehicle_label,
            ..
        }) => {
            let vehicle_model = vehicle::ActiveModel {
                vehicle_id: Set(vehicle_id.clone()),
                label: vehicle_label.map_set(),
                timestamp: Set(Utc::now().timestamp_millis()),
                ..Default::default()
            };

            // ensure vehicle actually exists for FK
            log::debug!("Inserting vehicle: {:?}", vehicle_model);
            Vehicle::insert(vehicle_model)
                .on_conflict(
                    OnConflict::column(vehicle::Column::VehicleId)
                        .do_nothing()
                        .to_owned(),
                )
                .exec_without_returning(db)
                .await?;

            trip_run.vehicle_id = Set(Some(vehicle_id));
            trip_run.save(db).await?
        }
        _ => trip_run,
    };

    let trip_run = trip_run.try_into_model()?;

    let stop_times = StopTimeIndex::find()
        .filter(stop_time_index::Column::TripRunId.eq(trip_run.id))
        .order_by_asc(stop_time_index::Column::StopSequence)
        .all(db)
        .await?;

    if let Some(stop_time_updates) = trip_update.stop_time_update {
        // per spec they're supposed to be sorted anyway
        let updates = stop_time_updates
            .into_iter()
            .sorted_by_key(|u| u.stop_sequence)
            .collect::<Vec<_>>();

        for update in updates {
            let stop_time = match (update.stop_sequence, update.stop_id) {
                (Some(seq), _) => stop_times
                    .iter()
                    .find(|st| st.stop_sequence == seq as i32)
                    .ok_or_else(|| Error::NotFound(format!("Stop time not found: {}", seq)))?,
                (_, Some(stop_id)) => stop_times
                    .iter()
                    .find(|st| st.stop_id == stop_id)
                    .ok_or_else(|| Error::NotFound(format!("Stop time not found: {}", stop_id)))?,
                _ => {
                    return Err(Error::InvalidData(
                        "Need stop_sequence or stop_id".to_string(),
                    ))
                }
            };

            if let Some(arrival) = update.arrival {
                let delay = arrival
                    .delay
                    .map(|d| d as i64)
                    .or_else(|| arrival.time.map(|t| t - stop_time.arrival_timestamp));
                if let Some(delay) = delay {
                    // Update this and subsequent stop time arrivals

                    StopTimeIndex::update_many()
                        .col_expr(
                            stop_time_index::Column::UpdatedArrivalTimestamp,
                            col(stop_time_index::Column::ArrivalTimestamp).add(delay * 1000),
                        )
                        .filter(all![
                            stop_time_index::Column::TripRunId.eq(trip_run.id), // gte not gt!
                            stop_time_index::Column::StopSequence.gte(stop_time.stop_sequence)
                        ])
                        .exec(db)
                        .await?;
                }
            }

            if let Some(departure) = update.departure {
                let delay = departure
                    .delay
                    .map(|d| d as i64)
                    .or_else(|| departure.time.map(|t| t - stop_time.departure_timestamp));
                if let Some(delay) = delay {
                    // Update subsequent stop arrivals based on previous departure delay

                    StopTimeIndex::update_many()
                        .col_expr(
                            stop_time_index::Column::UpdatedArrivalTimestamp,
                            col(stop_time_index::Column::ArrivalTimestamp).add(delay * 1000),
                        )
                        .filter(all![
                            stop_time_index::Column::TripRunId.eq(trip_run.id), // gt not gte!
                            stop_time_index::Column::StopSequence.gt(stop_time.stop_sequence)
                        ])
                        .exec(db)
                        .await?;
                }
            }
        }
    }

    Ok(())
}
