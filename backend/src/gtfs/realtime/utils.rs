use super::error::{Error, RtResult};
use crate::db::util::col;
use crate::entity::{gtfs_agency, gtfs_routes, trip_run};
use crate::gtfs::utils::GtfsDateTimeParser;
use crate::{entity::gtfs_trips, gtfs::structure::realtime::TripDescriptor};
use chrono::TimeZone;
use chrono::Utc;
use chrono_tz::Tz;
use sea_orm::sea_query::Func;
use sea_orm::sea_query::SimpleExpr;
use sea_orm::ColumnTrait;
use sea_orm::QueryFilter;
use sea_orm::QueryOrder;
use sea_orm::QuerySelect;
use sea_orm::RelationTrait;
use sea_orm::SelectColumns;
use sea_orm::{ConnectionTrait, EntityTrait, JoinType};

pub async fn find_trip_run(
    tx: &impl ConnectionTrait,
    trip_descriptor: TripDescriptor,
) -> RtResult<trip_run::Model> {
    use gtfs_routes::Entity as Route;
    use gtfs_trips::Entity as Trip;
    use trip_run::Entity as TripRun;

    let mut date_parser = GtfsDateTimeParser::new();

    let trip = if let Some(trip_id) = trip_descriptor.trip_id.clone() {
        Trip::find()
            .filter(gtfs_trips::Column::TripId.eq(trip_id.clone()))
            .one(tx)
            .await?
    } else {
        None
    };

    let route_id = trip_descriptor
        .route_id
        .clone()
        .or_else(|| trip.as_ref().map(|t| t.route_id.clone()))
        .ok_or_else(|| {
            Error::InvalidData("TripDescriptor should have route_id or trip_id".to_string())
        })?;

    // We now have the route, either via the trip or directly
    // which means we can get the timezone
    let (timezone,): (String,) = Route::find()
        .join(JoinType::InnerJoin, gtfs_routes::Relation::GtfsAgency.def())
        .filter(gtfs_routes::Column::RouteId.eq(route_id.clone()))
        .select_only()
        .select_column(gtfs_agency::Column::AgencyTimezone)
        .into_tuple()
        .one(tx)
        .await?
        .ok_or_else(|| Error::NotFound(format!("Route not found: {}", route_id.clone())))?;

    let tz: Tz = timezone
        .parse()
        .map_err(|e| Error::InvalidData(format!("Invalid timezone: {}", e)))?;

    // Gather as much as we can about when the trip is
    let mut trip_time = Utc::now().with_timezone(&tz);
    if let Some(start_date) = trip_descriptor.start_date.clone() {
        let start_date = date_parser.parse_date(&start_date)?;
        // set the date portion
        trip_time = tz
            .from_local_datetime(&start_date.and_time(trip_time.time()))
            .single()
            .ok_or_else(|| Error::InvalidData("Invalid date".to_string()))?;
    }
    if let Some(start_time) = trip_descriptor.start_time {
        let start_time = date_parser.parse_time(&trip_time.date_naive(), &start_time, &timezone)?;
        trip_time = start_time;
    }

    // We now have all we can about the trip
    let mut trip_run_query = TripRun::find()
        // closest to time
        .order_by_asc(SimpleExpr::from(Func::abs(
            col(trip_run::Column::StartTimestamp).sub(trip_time.timestamp_millis()),
        )))
        .to_owned();
    if let Some(trip_id) = trip_descriptor.trip_id {
        trip_run_query = trip_run_query.filter(trip_run::Column::TripId.eq(trip_id));
    }
    if let Some(route_id) = trip_descriptor.route_id.clone() {
        trip_run_query = trip_run_query.filter(trip_run::Column::RouteId.eq(route_id));
    }
    if let Some(direction_id) = trip_descriptor.direction_id {
        trip_run_query =
            trip_run_query.filter(trip_run::Column::DirectionId.eq(direction_id as i32));
    }
    if let Some(start_date) = trip_descriptor.start_date {
        trip_run_query = trip_run_query.filter(trip_run::Column::StartDate.eq(start_date));
    }

    let trip_run = trip_run_query
        .one(tx)
        .await?
        .ok_or_else(|| Error::NotFound("No trip runs found".to_string()))?;

    Ok(trip_run)
}
