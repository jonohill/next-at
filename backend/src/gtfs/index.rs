use std::{collections::HashMap, ops::Sub, time::Instant};

use crate::entity::prelude::*;
use crate::{
    db::{
        self,
        util::{null, SeaRusqliteAdapter},
    },
    entity::*,
    geo::get_bounding_box,
    gtfs::utils::GtfsDateTimeParser,
};
use chrono::{Datelike, NaiveDate, Utc, Weekday};
use geo::Point;
use migration::raw::RawSql;
use migration::{Sql000003StopTimeIndexTable, Sql000004StopTimeIndexIndexes};
use rusqlite::params;
use sea_orm::sea_query::any;
use sea_orm::sea_query::OnConflict;
use sea_orm::{sea_query::all, QueryOrder};
use sea_orm::{
    sea_query::{IntoCondition, Query, UnionType},
    ColumnTrait, EntityTrait, QueryFilter, QuerySelect, QueryTrait, RelationTrait, Select,
};

use super::utils::DateError;

const SEARCH_DISTANCE_METRES: f64 = 1000.0;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Database error: {0}")]
    DatabaseOrm(#[from] sea_orm::error::DbErr),

    #[error("Database error: {0}")]
    DatabaseSqlx(#[from] rusqlite::Error),

    #[error("Error parsing date: {0}")]
    Date(#[from] DateError),

    #[error("{0}")]
    Other(String),
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Prepare a query which gets all the stop times for a date
fn prepare_stop_times_for_date(dt: &NaiveDate) -> Select<gtfs_stop_times::Entity> {
    use gtfs_stop_times::Entity as StopTime;
    use sea_orm::JoinType::*;

    // aliases
    use gtfs_calendar as c;
    use gtfs_calendar_dates as cd;
    use gtfs_routes as r;
    use gtfs_stop_times as st;
    use gtfs_trips as t;

    let calendar_day_col = match dt.weekday() {
        Weekday::Mon => gtfs_calendar::Column::Monday,
        Weekday::Tue => gtfs_calendar::Column::Tuesday,
        Weekday::Wed => gtfs_calendar::Column::Wednesday,
        Weekday::Thu => gtfs_calendar::Column::Thursday,
        Weekday::Fri => gtfs_calendar::Column::Friday,
        Weekday::Sat => gtfs_calendar::Column::Saturday,
        Weekday::Sun => gtfs_calendar::Column::Sunday,
    };
    let gtfs_date: i32 = dt.format("%Y%m%d").to_string().parse().unwrap();

    StopTime::find()
        .join(LeftJoin, st::Relation::GtfsTrips.def())
        .join(LeftJoin, t::Relation::GtfsRoutes.def())
        .join(LeftJoin, r::Relation::GtfsAgency.def())
        .join(LeftJoin, t::Relation::Service.def())
        .join(LeftJoin, service::Relation::GtfsCalendar.def())
        .join(
            LeftJoin,
            service::Relation::GtfsCalendarDates
                .def()
                // Custom condition - join to the row for this date
                .on_condition(move |_, _| cd::Column::Date.eq(gtfs_date).into_condition()),
        )
        .filter(any![
            // Regular services
            all![
                c::Column::StartDate.lte(gtfs_date),
                c::Column::EndDate.gte(gtfs_date),
                calendar_day_col.eq(1),
                // except those cancelled for this date
                any![
                    cd::Column::ExceptionType.is_null(),
                    cd::Column::ExceptionType.ne(2) // cancelled
                ]
            ],
            // Added services
            cd::Column::ExceptionType.eq(1)
        ])
}

fn prepare_last_calendar_date() -> Select<gtfs_calendar_dates::Entity> {
    let mut calendar_dates = gtfs_calendar_dates::Entity::find()
        .select_only()
        .column(gtfs_calendar_dates::Column::Date);

    // union only seems to be possible with seaquery directly
    // (this adds to the underlying query)
    sea_orm::QuerySelect::query(&mut calendar_dates)
        .union(
            UnionType::All,
            Query::select()
                .column(gtfs_calendar::Column::EndDate)
                .from(gtfs_calendar::Entity)
                .to_owned(),
        )
        .order_by(gtfs_calendar_dates::Column::Date, sea_orm::Order::Desc)
        .limit(1);

    calendar_dates
}

struct CountLogger {
    count: usize,
    last_time: Instant,
    name: String,
}

impl CountLogger {
    fn new(name: &str) -> Self {
        Self {
            count: 0,
            last_time: Instant::now(),
            name: name.to_string(),
        }
    }

    fn log(&mut self) {
        self.count += 1;
        if self.count % 100_000 == 0 {
            log::info!(
                "Inserted {} {} ({} ms)",
                self.count,
                self.name,
                self.last_time.elapsed().as_millis()
            );
            self.last_time = Instant::now();
        }
    }
}

fn do_build_stop_time_index() -> Result<()> {
    const MAX_DAYS: i32 = 60;

    let mut db = db::util::open_rusqlite()?;

    // for speed
    db.pragma_update(None, "foreign_keys", "OFF")?;

    let mut gtfs_date_time = GtfsDateTimeParser::new();

    let last_date_i: i32 = prepare_last_calendar_date()
        .into_query()
        .prepare(&db)?
        .query_row(|r| r.get(0))?;
    let last_date = gtfs_date_time.parse_date(&last_date_i.to_string())?;

    let start_date = Utc::now()
        .sub(chrono::Duration::days(1))
        .naive_local()
        .date();

    // tx rolled back on drop if not committed
    let tx = db.transaction()?;
    {
        Query::delete()
            .from_table(TripRun)
            .prepare(&tx)?
            .execute()?;

        // Stupid hack that works - drop the table (faster than deleting rows)
        // And recreate it without indexes (yet) to make inserts faster
        log::info!("Deleting existing stop index");
        tx.execute_batch(&Sql000003StopTimeIndexTable::down_sql().unwrap())?;
        tx.execute_batch(&Sql000003StopTimeIndexTable::up_sql())?;

        // Prepare trip run insert
        let mut insert_into_trip_run = Query::insert()
            .into_table(TripRun)
            .columns([
                trip_run::Column::TripId,
                trip_run::Column::RouteId,
                trip_run::Column::DirectionId,
                trip_run::Column::StartDate,
                trip_run::Column::StartTimestamp,
            ])
            .values_panic(vec![null(); 5]) // placeholders
            .returning_col(trip_run::Column::Id)
            .prepare(&tx)?
            .into_inner();

        // Prepare stop time index insert
        let mut insert_into_index = Query::insert()
            .into_table(stop_time_index::Entity)
            .columns([
                stop_time_index::Column::StopId,
                stop_time_index::Column::StopSequence,
                stop_time_index::Column::TripId,
                stop_time_index::Column::TripRunId,
                stop_time_index::Column::ArrivalTimestamp,
                stop_time_index::Column::DepartureTimestamp,
            ])
            .values_panic(vec![null(); 6]) // placeholders
            .prepare(&tx)?
            .into_inner();

        // we keep track of the number of stop times in each 10 minute period of the day
        // so that we can find the ideal maintenance window
        let mut period_counts = (0..144).map(|i| (i, 0)).collect::<HashMap<_, _>>();

        let mut day_count = 0;
        let mut date = start_date;

        while date <= last_date && day_count < MAX_DAYS {
            day_count += 1;

            log::info!("Building stop index for {}", date);

            let mut day_data_query = prepare_stop_times_for_date(&date)
                .select_only()
                .columns([
                    gtfs_stop_times::Column::StopId,
                    gtfs_stop_times::Column::StopSequence,
                    gtfs_stop_times::Column::TripId,
                    gtfs_stop_times::Column::ArrivalTime,
                    gtfs_stop_times::Column::DepartureTime,
                ])
                .column(gtfs_agency::Column::AgencyTimezone)
                .columns([gtfs_trips::Column::RouteId, gtfs_trips::Column::DirectionId])
                // So that we get the trip start time before the rest of the stops
                // which lets us create a trip run to correlate with the rest of the stops
                .order_by_asc(gtfs_stop_times::Column::TripId)
                .order_by_asc(gtfs_stop_times::Column::StopSequence)
                .into_query()
                .prepare(&tx)?;

            let mut count = CountLogger::new("stop times");

            let mut trip_run_id: Option<i64> = None;

            let mut day_data = day_data_query.query()?;

            while let Some(r) = day_data.next()? {
                let stop_id: String = r.get(0)?;
                let stop_sequence: i32 = r.get(1)?;
                let trip_id: String = r.get(2)?;
                let arrival_time: String = r.get(3)?;
                let departure_time: String = r.get(4)?;
                let agency_timezone: String = r.get(5)?;
                let route_id: String = r.get(6)?;
                let direction_id: i32 = r.get(7)?;

                let arrival_time =
                    gtfs_date_time.parse_time(&date, &arrival_time, &agency_timezone)?;
                let departure_time =
                    gtfs_date_time.parse_time(&date, &departure_time, &agency_timezone)?;

                if stop_sequence == 1 {
                    // The trip run starts at the departure from the first stop
                    let id: i64 = insert_into_trip_run.query_row(
                        params![
                            trip_id,
                            route_id,
                            direction_id,
                            date.format("%Y%m%d").to_string(), // gtfs format
                            departure_time.timestamp_millis(),
                        ],
                        |r| r.get(0),
                    )?;
                    trip_run_id = Some(id);
                }

                let trip_run_id =
                    trip_run_id.ok_or_else(|| Error::Other("No trip run id".to_string()))?;

                let arrival_time_millis = arrival_time.timestamp_millis();
                let departure_time_millis = departure_time.timestamp_millis();
                let period = arrival_time_millis % 86400000 / 600000;
                period_counts
                    .entry(period)
                    .and_modify(|c| *c += 1)
                    .or_insert(1);

                // Prepared query
                // Check order is the same as declared in insert_into_index
                insert_into_index.execute(params![
                    stop_id,
                    stop_sequence,
                    trip_id,
                    trip_run_id,
                    arrival_time_millis,
                    departure_time_millis,
                ])?;

                count.log();
            }

            date = date.succ_opt().unwrap();
        }

        // find ideal maintenance time
        // we just choose a time slot with the least stop times
        let min_period = period_counts
            .iter()
            .min_by_key(|(_, &count)| count)
            .expect("No periods. Not initialised?");

        Query::insert()
            .into_table(maintenance_time::Entity)
            .columns([
                maintenance_time::Column::Id,
                maintenance_time::Column::MinuteOfDay,
            ])
            .values_panic([1.into(), (min_period.0 * 10).into()])
            .on_conflict(
                OnConflict::column(maintenance_time::Column::Id)
                    .update_column(maintenance_time::Column::MinuteOfDay)
                    .to_owned(),
            )
            .prepare(&tx)?
            .execute()?;

        log::info!("Re-creating indexes");
        tx.execute_batch(&Sql000004StopTimeIndexIndexes::up_sql())?;
    }
    log::info!("Committing transaction");
    tx.commit()?;

    Ok(())
}

pub async fn build_stop_time_index() -> Result<()> {
    // Uses rusqlite directly in a background thread
    // This is much faster than going through the orm async layers
    tokio::task::spawn_blocking(do_build_stop_time_index)
        .await
        .unwrap() // spawn result
}

fn do_build_stop_index() -> Result<()> {
    let mut db = db::util::open_rusqlite()?;

    // for speed, and to allow deleting out of order records
    db.pragma_update(None, "foreign_keys", "OFF")?;

    let tx = db.transaction()?;
    {
        Query::delete()
            .from_table(StopIndex)
            .prepare(&tx)?
            .execute()?;

        let mut insert_index = Query::insert()
            .into_table(StopIndex)
            .columns([
                stop_index::Column::StopId,
                stop_index::Column::MinLat,
                stop_index::Column::MaxLat,
                stop_index::Column::MinLon,
                stop_index::Column::MaxLon,
            ])
            .values_panic([null(), null(), null(), null(), null()]) // placeholders
            .prepare(&tx)?
            .into_inner();

        let mut stops_query = GtfsStops::find()
            .select_only()
            .columns([
                gtfs_stops::Column::StopId,
                gtfs_stops::Column::StopLat,
                gtfs_stops::Column::StopLon,
            ])
            .into_query()
            .prepare(&tx)?;
        let mut stops_rows = stops_query.query()?;

        while let Some(stop) = stops_rows.next()? {
            let stop_id: String = stop.get(0)?;
            let lat = stop.get(1)?;
            let lon = stop.get(2)?;

            let bounding_box = get_bounding_box(Point::new(lon, lat), SEARCH_DISTANCE_METRES);
            let min = bounding_box.min();
            let max = bounding_box.max();

            insert_index.execute(params![stop_id, min.y, max.y, min.x, max.x,])?;
        }
    }
    tx.commit()?;

    Ok(())
}

pub async fn build_stop_index() -> Result<()> {
    tokio::task::spawn_blocking(do_build_stop_index)
        .await
        .unwrap() // spawn result
}

#[cfg(test)]
mod tests {
    use chrono::Local;
    use sea_orm::{DbBackend, QueryTrait};

    use super::*;

    #[test]
    #[ignore]
    fn test_prepare_stop_times_for_date() {
        let date = Local::now().naive_local().date();
        let query = prepare_stop_times_for_date(&date);
        let query2 = query
            .select_only()
            .columns([
                gtfs_stop_times::Column::StopId,
                gtfs_stop_times::Column::StopSequence,
                gtfs_stop_times::Column::TripId,
                gtfs_stop_times::Column::ArrivalTime,
            ])
            .column(gtfs_agency::Column::AgencyTimezone);
        println!("{}", query2.build(DbBackend::Sqlite));
        todo!()
    }
}
