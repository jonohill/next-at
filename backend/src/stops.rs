use crate::entity::{prelude::*, trip_run};
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
use itertools::Itertools;
use migration::{Expr, Func};
use sea_orm::sea_query::all;
use sea_orm::{ColumnTrait, EntityTrait, JoinType, QueryFilter, QueryOrder, QuerySelect};
use sea_orm::{FromQueryResult, RelationTrait};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    pub route_id: String,
    pub route_short_name: String,
    pub route_long_name: String,
    pub route_type: i32,
    pub route_color: String,
    pub route_text_color: String,
}

#[derive(Debug, Serialize, Clone, FromQueryResult)]
pub struct StopArrival {
    pub trip_id: String,
    #[serde(skip_serializing)]
    pub route_id: String,
    pub stop_sequence: u32,
    #[serde(skip_serializing)]
    pub stop_headsign: String,
    pub trip_headsign: String,
    pub start_timestamp: i64,
    pub arrival_timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_arrival_timestamp: Option<i64>,
}

#[derive(Serialize)]
pub struct RouteTrip {
    pub route_id: String,
    pub route_short_name: String,
    pub route_long_name: String,
    pub route_type: i32,
    pub route_color: String,
    pub route_text_color: String,
    pub stop_headsign: String,
}

#[derive(Serialize)]
pub struct StopRouteTripArrival {
    pub route_trip: RouteTrip,
    pub arrivals: Vec<StopArrival>,
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

pub async fn get_stop_arrivals(ctx: &ContextData, stop_id: &str) -> NextAtResult<Vec<StopRouteTripArrival>> {
    use gtfs_routes as r;
    use gtfs_stop_times as st;
    use gtfs_trips as t;
    use stop_time_index as sti;
    use trip_run as tr;

    let now = Utc::now().timestamp_millis();
    let tomorrow = Utc::now().add(Duration::days(1)).timestamp_millis();

    let ts_col = || Expr::expr(Func::coalesce([col(sti::Column::UpdatedArrivalTimestamp).into(), col(sti::Column::ArrivalTimestamp).into()]));

    let arrivals = StopTimeIndex::find()
        .filter(all![
            sti::Column::StopId.eq(stop_id),
            ts_col().gte(now),
            ts_col().lt(tomorrow),
        ])
        .join(JoinType::InnerJoin, sti::Relation::TripRun.def())
        .join(JoinType::InnerJoin, sti::Relation::GtfsStopTimes.def())
        .join(JoinType::InnerJoin, st::Relation::GtfsTrips.def())
        .join(JoinType::InnerJoin, t::Relation::GtfsRoutes.def())
        .order_by_asc(ts_col())
        .select_only()
        .columns([
            sti::Column::TripId,
            sti::Column::StopSequence,
            sti::Column::ArrivalTimestamp,
            sti::Column::UpdatedArrivalTimestamp,
        ])
        .column(tr::Column::StartTimestamp)
        .column(st::Column::StopHeadsign)
        .column(r::Column::RouteId)
        .limit(50)
        .into_model::<StopArrival>()
        .all(&ctx.db)
        .await?;

    let routes = get_stop_routes(ctx, stop_id).await?;
    
    let mut stop_arrivals = HashMap::<(String, String), StopRouteTripArrival>::new();

    for arrival in arrivals {

        if let Some(route) = routes.iter().find(|r| r.route_id == arrival.route_id) {
            let item = stop_arrivals
                .entry((arrival.route_id.clone(), arrival.stop_headsign.clone()))
                .or_insert(StopRouteTripArrival {
                    route_trip: RouteTrip {
                        route_id: route.route_id.clone(),
                        route_short_name: route.route_short_name.clone(),
                        route_long_name: route.route_long_name.clone(),
                        route_type: route.route_type,
                        route_color: route.route_color.clone(),
                        route_text_color: route.route_text_color.clone(),
                        stop_headsign: arrival.stop_headsign.clone(),
                    },
                    arrivals: vec![],
                });
    
            item.arrivals.push(arrival);
        }

    }

    let stop_arrivals = stop_arrivals.into_values()
        .filter(|v| !v.arrivals.is_empty())
        .sorted_by_key(|a| a.arrivals[0].arrival_timestamp)
        .collect::<Vec<_>>();
    Ok(stop_arrivals)
}

pub async fn get_stop_routes(ctx: &ContextData, stop_id: &str) -> DbResult<Vec<StopRoute>> {
    
    use stop_time_index::Column as sti;
    use gtfs_routes::Column as r;
    
    let week_hence = Utc::now().add(Duration::weeks(1)).timestamp_millis();
    
    let stop_routes = StopTimeIndex::find()
        .filter(all![
            sti::ArrivalTimestamp.lte(week_hence),
            sti::StopId.eq(stop_id),
        ])
        .join(JoinType::InnerJoin, stop_time_index::Relation::TripRun.def())
        .join(JoinType::InnerJoin, trip_run::Relation::GtfsRoutes.def())
        .group_by(r::RouteId)
        .order_by_desc(r::RouteId.count())
        .select_only()
        .columns([
            r::RouteId,
            r::RouteShortName,
            r::RouteLongName,
            r::RouteType,
            r::RouteColor,
            r::RouteTextColor,
        ])
        .into_model::<StopRoute>()
        .all(&ctx.db)
        .await?;

    Ok(stop_routes)
}

#[cfg(test)]
mod test {

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
        // println!("Now: {}", now);
        let ctx = ctx().await;
        get_stop_arrivals(&ctx, "4018-7ef4a7b7").await.unwrap();
    }
}
