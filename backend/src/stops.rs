use crate::entity::prelude::*;
use crate::{
    db::{
        error::DbResult,
        util::{col, pow},
    },
    entity::{gtfs_routes, gtfs_stop_times, gtfs_stops, gtfs_trips, stop_index, stop_time_index},
    error::NextAtResult,
    ContextData,
};
use chrono::{Duration, Utc};
use sea_orm::sea_query::all;
use sea_orm::{ColumnTrait, EntityTrait, JoinType, QueryFilter, QueryOrder, QuerySelect};
use sea_orm::{FromQueryResult, RelationTrait};
use serde::{Deserialize, Serialize};
use std::ops::Add;

/// Stop as returned in the API
/// as opposed to a [`gtfs_structures::Stop`]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Stop {
    pub id: String,
    pub code: String,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, FromQueryResult)]
pub struct StopRoute {
    pub id: String,
    pub short_name: String,
    pub long_name: String,
    pub r#type: i32,
    pub color: String,
    pub text_color: String,
}

#[derive(Debug, Serialize, Clone, FromQueryResult)]
pub struct StopArrival {
    pub trip_id: String,
    pub stop_sequence: u32,
    pub route_short_name: String,
    pub stop_headsign: String,
    // pub freshness: Freshness,
    // pub status: ArrivalStatus,
    // pub trip_start_time: i64,
    pub arrival_timestamp: i64,
    // pub occupancy: Option<u8>,
}

pub async fn get_closest_stops(
    ctx: &ContextData,
    lat: f64,
    lon: f64,
    limit: u64,
) -> DbResult<Vec<Stop>> {
    use gtfs_stops as s;
    use gtfs_stops::Entity as GtfsStop;
    use stop_index as si;

    let gtfs_stops = GtfsStop::find()
        .join(JoinType::InnerJoin, gtfs_stops::Relation::StopIndex.def())
        .filter(si::Column::MinLat.lte(lat))
        .filter(si::Column::MaxLat.gte(lat))
        .filter(si::Column::MinLon.lte(lon))
        .filter(si::Column::MaxLon.gte(lon))
        .order_by_asc(
            pow(col(s::Column::StopLat).sub(lat), 2).add(pow(col(s::Column::StopLon).sub(lon), 2)),
        )
        .limit(limit)
        .all(&ctx.db)
        .await?;

    let stops = gtfs_stops
        .into_iter()
        .map(|s| Stop {
            id: s.stop_id.clone(),
            code: s.stop_code.unwrap_or(s.stop_id),
            name: s.stop_name,
        })
        .collect();
    Ok(stops)
}

pub async fn get_stop_arrivals(ctx: &ContextData, stop_id: &str) -> NextAtResult<Vec<StopArrival>> {
    use gtfs_routes as r;
    use gtfs_stop_times as st;
    use gtfs_trips as t;
    use stop_time_index as sti;

    let now = Utc::now().timestamp_millis();
    let tomorrow = Utc::now().add(Duration::days(1)).timestamp_millis();

    let arrivals = StopTimeIndex::find()
        .filter(all![
            sti::Column::StopId.eq(stop_id),
            sti::Column::ArrivalTimestamp.gte(now),
            sti::Column::ArrivalTimestamp.lt(tomorrow),
        ])
        .join(JoinType::InnerJoin, sti::Relation::GtfsStopTimes.def())
        .join(JoinType::InnerJoin, st::Relation::GtfsTrips.def())
        .join(JoinType::InnerJoin, t::Relation::GtfsRoutes.def())
        .order_by_asc(sti::Column::ArrivalTimestamp)
        .select_only()
        .columns([
            sti::Column::TripId,
            sti::Column::StopSequence,
            sti::Column::ArrivalTimestamp,
        ])
        .column(r::Column::RouteShortName)
        .column(st::Column::StopHeadsign)
        .limit(50)
        .into_model::<StopArrival>()
        .all(&ctx.db)
        .await?;

    Ok(arrivals)
}

pub async fn get_stop_routes(ctx: &ContextData, stop_id: &str) -> DbResult<Vec<StopRoute>> {
    use gtfs_routes as r;

    let stop_routes = gtfs_stop_times::Entity::find()
        .distinct()
        .select_only()
        .column_as(r::Column::RouteId, "id")
        .column_as(r::Column::RouteShortName, "short_name")
        .column_as(r::Column::RouteLongName, "long_name")
        .column_as(r::Column::RouteType, "type")
        .column_as(r::Column::RouteColor, "color")
        .column_as(r::Column::RouteTextColor, "text_color")
        .join(
            JoinType::InnerJoin,
            gtfs_stop_times::Relation::GtfsTrips.def(),
        )
        .join(JoinType::InnerJoin, gtfs_trips::Relation::GtfsRoutes.def())
        .filter(gtfs_stop_times::Column::StopId.eq(stop_id))
        .into_model::<StopRoute>()
        .all(&ctx.db)
        .await?;

    Ok(stop_routes)
}

#[cfg(test)]
mod test {

    use chrono::Local;

    use crate::test_utils::ctx;

    use super::*;

    #[tokio::test]
    async fn test_closest_stops() {
        let ctx = ctx().await;

        let stops = get_closest_stops(&ctx, -36.8485, 174.7633, 5)
            .await
            .unwrap();
        println!("Closest stops: {:?}", stops);
    }

    #[tokio::test]
    async fn test_stop_arrivals() {
        let now = Local::now().naive_local();
        // println!("Now: {}", now);
        let ctx = ctx().await;
        get_stop_arrivals(&ctx, "4018-7ef4a7b7").await.unwrap();
    }
}
