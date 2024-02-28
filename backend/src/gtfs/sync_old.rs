use std::collections::HashMap;

use futures_util::{pin_mut, StreamExt};
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize};

use crate::db;
use crate::db::util::*;
use crate::gtfs::reader::{get_gtfs_zip_from_url, read_gtfs_from_zip, GtfsError, GtfsItem};

const AT_GTFS_ZIP_URL: &str = "https://gtfs.at.govt.nz/gtfs.zip";

// Sqlite max number of parameters is 32766
// Max insert columns is 15 (Stop) = 32766 / 15 = 2184
const BATCH_SIZE: usize = 2_000;

#[derive(thiserror::Error, Debug)]
pub enum GtfsSyncError {
    #[error("GTFS error: {0}")]
    Gtfs(#[from] GtfsError),

    #[error("Database error: {0}")]
    DbClient(#[from] anyhow::Error), // libsql uses anyhow::Error, kind of annoying for a library

    #[error(transparent)]
    Db(#[from] db::error::DbError),

    #[error("Insert error: {0}")]
    Insert(String),

    #[error("Import record was updated elsewhere. Two imports running?")]
    Conflict,

    #[error("Error deserialising from DB: {0}")]
    DbParse(#[from] serde_json::Error),

    #[error("{0}")]
    Other(String),
}

type GtfsSyncResult<T> = Result<T, GtfsSyncError>;

const FILE_NAMES: [&str; 9] = [
    "agency.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "feed_info.txt",
    "routes.txt",
    "shapes.txt",
    "stops.txt",
    "stop_times.txt",
    "trips.txt",
];

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ImportFile {
    record_count: i64,
    start_time: String,
    end_time: Option<String>,
}

impl Default for ImportFile {
    fn default() -> Self {
        Self {
            record_count: 0,
            start_time: sql_now(),
            end_time: None,
        }
    }
}

fn bool_as_int<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<u32>::deserialize(deserializer)?
        .map(|i| i != 0);
    Ok(s)
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[enum_def]
struct Import {
    id: i64,
    #[serde(deserialize_with = "bool_as_int")]
    active: Option<bool>,
    url: Option<String>,
    file_last_modified: Option<String>,
    start_time: String,
    end_time: Option<String>,
    last_updated: String,
    files: String, // stored as json
}

enum DbOrTx<'a> {
    Db(&'a libsql_client::Client),
    Tx(&'a libsql_client::Transaction<'a>),
}

use DbOrTx::*;

use super::structure::{GtfsCalendarDateIden, GtfsFeedInfoIden, GtfsRouteIden, GtfsTripIden};

#[derive(Debug, Default)]
struct ImportState {
    import: Import,
    files: HashMap<String, ImportFile>,
}

impl ImportState {

    async fn get(db: &libsql_client::Client) -> GtfsSyncResult<Option<ImportState>> {
        let select = Query::select()
            .column(Asterisk)
            .from(ImportIden::Table)
            .order_by(ImportIden::StartTime, Order::Desc)
            .limit(1)
            .statement();
        let state = db
            .execute(select)
            .await?
            .first::<Import>()
            .transpose()?
            .map(|import| {
                let files = serde_json::from_str(&import.files)?;
                Ok::<_, GtfsSyncError>(ImportState { import, files })
            })
            .transpose()?;
        Ok(state)
    }

    async fn new(db: &libsql_client::Client) -> GtfsSyncResult<ImportState> {
        use ImportIden::*;

        let files = FILE_NAMES
            .iter()
            .map(|file_name| (file_name.to_string(), ImportFile::default()))
            .collect::<HashMap<_, _>>();

        // New import
        let insert = Query::insert()
            .into_table(ImportIden::Table)
            .columns([Files])
            .values([serde_json::to_string(&files)?.into()])?
            .returning_all()
            .statement();
        let import = db
            .execute(insert)
            .await?
            .first::<Import>()
            .transpose()?
            .ok_or_else(|| {
                GtfsSyncError::Insert("Expected import record to be returned".to_string())
            })?;

        let state = ImportState { import, files };

        Ok(state)
    }

    fn prepare_save(&mut self) -> GtfsSyncResult<Statement> {
        let og_updated = self.import.last_updated.clone();
        self.import.last_updated = sql_now();
        self.import.files = serde_json::to_string(&self.files)?;

        let update = Query::update()
            .table(ImportIden::Table)
            .values(self.import.db_values()?)
            .and_where(col(ImportIden::Id).eq(self.import.id))
            .and_where(col(ImportIden::LastUpdated).eq(og_updated))
            .statement();

        Ok(update)
    }

    async fn save(&mut self, db_or_tx: DbOrTx<'_>) -> GtfsSyncResult<()> {
        let q = self.prepare_save()?;

        let result = match db_or_tx {
            Db(db) => db.execute(q).await?,
            Tx(tx) => tx.execute(q).await?,
        };

        if result.rows_affected != 1 {
            return Err(GtfsSyncError::Conflict);
        }

        Ok(())
    }

    async fn finish(&mut self, db: &libsql_client::Client) -> GtfsSyncResult<()> {
        self.import.end_time = Some(sql_now());
        self.import.active = None;

        self.save(Db(db)).await?;

        Ok(())
    }

    async fn delete(self, db: &libsql_client::Client) -> GtfsSyncResult<()> {
        let delete = Query::delete()
            .from_table(ImportIden::Table)
            .and_where(col(ImportIden::Id).eq(self.import.id))
            .statement();

        db.execute(delete).await?;

        Ok(())
    }

    fn set_file_pos(&mut self, file_name: &str, pos: i64) {
        let file = self.files.get_mut(file_name).expect("Expected file to be in state");
        file.record_count = file.record_count.max(pos);
    }

    fn finish_file(&mut self, file_name: &str) {
        let file = self.files.get_mut(file_name).expect("Expected file to be in state");
        file.end_time = Some(sql_now());
    }

}

pub fn filename_for_item(item: &GtfsItem) -> String {
    match item {
        GtfsItem::Agency(_) => "agency.txt",
        GtfsItem::Calendar(_) => "calendar.txt",
        GtfsItem::CalendarDate(_) => "calendar_dates.txt",
        GtfsItem::FeedInfo(_) => "feed_info.txt",
        GtfsItem::Route(_) => "routes.txt",
        GtfsItem::Shape(_) => "shapes.txt",
        GtfsItem::Stop(_) => "stops.txt",
        GtfsItem::StopTime(_) => "stop_times.txt",
        GtfsItem::Trip(_) => "trips.txt",
    }
    .to_string()
}

fn table_for_item(item: &GtfsItem) -> String {
    match item {
        GtfsItem::Agency(_) => GtfsAgencyIden::Table.to_string(),
        GtfsItem::Calendar(_) => GtfsCalendarIden::Table.to_string(),
        GtfsItem::CalendarDate(_) => GtfsCalendarDateIden::Table.to_string(),
        GtfsItem::FeedInfo(_) => GtfsFeedInfoIden::Table.to_string(),
        GtfsItem::Route(_) => GtfsRouteIden::Table.to_string(),
        GtfsItem::Shape(_) => GtfsShapeIden::Table.to_string(),
        GtfsItem::Stop(_) => GtfsStopIden::Table.to_string(),
        GtfsItem::StopTime(_) => GtfsStopTimeIden::Table.to_string(),
        GtfsItem::Trip(_) => GtfsTripIden::Table.to_string(),
    }
}

fn conflict_keys_for_item(item: &GtfsItem) -> Vec<Alias> {
    match item {
        GtfsItem::Agency(_) => vec![GtfsAgencyIden::AgencyId.to_string()],
        GtfsItem::Calendar(_) => vec![GtfsCalendarIden::ServiceId.to_string()],
        GtfsItem::CalendarDate(_) => vec![GtfsCalendarDateIden::ServiceId.to_string(), GtfsCalendarDateIden::Date.to_string()],
        GtfsItem::FeedInfo(_) => vec![],
        GtfsItem::Route(_) => vec![GtfsRouteIden::RouteId.to_string()],
        GtfsItem::Shape(_) => vec![GtfsShapeIden::ShapeId.to_string(), GtfsShapeIden::ShapePtSequence.to_string()],
        GtfsItem::Stop(_) => vec![GtfsStopIden::StopId.to_string()],
        GtfsItem::StopTime(_) => vec![GtfsStopTimeIden::TripId.to_string(), GtfsStopTimeIden::StopSequence.to_string()],
        GtfsItem::Trip(_) => vec![GtfsTripIden::TripId.to_string()],
    }.into_iter().map(Alias::new).collect()
}

pub struct GtfsSync<'a> {
    db: &'a libsql_client::Client,
    insert: InsertStatement,
    last_item: Option<GtfsItem>,
    state: ImportState,
    stops: Vec<GtfsStop>,
}

impl<'a> GtfsSync<'a> {

    async fn new(db: &'a libsql_client::Client) -> GtfsSyncResult<Self> {
        let sync = Self {
            db,
            insert: Query::insert(),
            last_item: None,
            state: ImportState::default(),
            stops: vec![],
        };
        Ok(sync)
    }

    /// Finish preparing the insert and commit the batch
    async fn commit_batch(&mut self) -> GtfsSyncResult<()> {
        if let Some(last_item) = self.last_item.take() {

            let tx = self.db.transaction().await?;

            let tx_result = {

                let conflict_keys = conflict_keys_for_item(&last_item);
                if !conflict_keys.is_empty() {
                    let mut update_columns = last_item.db_columns()?;
                    update_columns.push(Alias::new("import_id"));

                    self.insert
                        .on_conflict(
                            OnConflict::columns(conflict_keys)
                                .update_columns(update_columns)
                                .to_owned(),
                        );
                }

                if let GtfsItem::Stop(_) = &last_item {
                    // so we can get the db id for each item
                    self.insert.returning(Query::returning().columns([
                        GtfsStopIden::Id,
                        GtfsStopIden::StopId,
                    ]));
                }
            
                let insert_result = tx.execute(self.insert.statement()).await?;
                
                if let GtfsItem::Stop(_) = &last_item {
                    // And update the indexes 
                    let id_map: HashMap<_, _> = insert_result
                        .rows
                        .iter()
                        .map(|row| {
                            let id: i64 = row.try_column("id").expect("Expected id to be returned");
                            let stop_id: &str = row.try_column("stop_id").expect("Expected stop_id to be returned");
                            (stop_id.to_string(), id)
                        })
                        .collect();

                    // no upserts are possible for rtree!
                    let db_ids = id_map.values().copied();
                    let delete = Query::delete()
                        .from_table(GtfsStopIndexIden::Table)
                        .and_where(col(GtfsStopIndexIden::Id).is_in(db_ids))
                        .statement();
                    tx.execute(delete).await?;

                    let columns = [
                        GtfsStopIndexIden::Id,
                        GtfsStopIndexIden::MinLat,
                        GtfsStopIndexIden::MaxLat,
                        GtfsStopIndexIden::MinLon,
                        GtfsStopIndexIden::MaxLon,
                    ]
                    .iter()
                    .map(|i| Alias::new(i.to_string()))
                    .collect_vec();
                    let mut index_insert = Query::insert()
                        .into_table(GtfsStopIndexIden::Table)
                        .columns(columns.clone())
                        .to_owned();

                    for stop in &self.stops {
                        if let GtfsStop { stop_lat: Some(_), stop_lon: Some(_), .. } = stop {
                            let stop_id = stop.stop_id.clone();
                            let db_id = id_map.get(&stop_id).expect("Expected stop_id to be in map");
                            let stop = GtfsStop {
                                id: Some(*db_id),
                                ..stop.clone()
                            };
                            let stop_index = GtfsStopIndex::from(stop);
                            
                            index_insert.values(stop_index.db_values_for_columns(&columns)?)?;
                        }
                    }

                    tx.execute(index_insert.statement()).await?;

                }
                
                self.state.save(Tx(&tx)).await?;
                
                Ok(())
            };

            if tx_result.is_ok() {
                tx.commit().await?;
            } else {
                log::error!("Error in batch, rolling back: {:?}", tx_result);
                tx.rollback().await?;
            }

            tx_result
        } else {
            log::debug!("empty batch");
            Ok(())
        }
    }

    async fn do_sync(mut self) -> GtfsSyncResult<usize> {
        log::debug!("Syncing GTFS data...");
    
        let state = ImportState::get(self.db).await?;
    
        let resuming = state
            .as_ref()
            .is_some_and(|state| state.import.end_time.is_none());
    
        // If there was a previous import and we're wanting to start a new one
        // then we only bother if the server has newer data for us
        let file_last_modified = state.as_ref().and_then(|state| {
            if resuming {
                None
            } else {
                state.import.file_last_modified.clone()
            }
        });
    
        self.state = match state {
            Some(state) => {
                if resuming {
                    state
                } else {
                    if Some(true) == state.import.active {
                        return Err(GtfsSyncError::Other(
                            "Inconsistent state - active and end_time both set".to_string(),
                        ));
                    }
                    ImportState::new(self.db).await?
                }
            },
            None => ImportState::new(self.db).await?,
        };

        log::info!("Downloading zip...");

        let get_items_result = get_gtfs_zip_from_url(AT_GTFS_ZIP_URL, file_last_modified).await?;
    
        let (last_modified, zip_reader) = match get_items_result {
            Some(zip_reader) => zip_reader,
            None => {
                log::info!("No new GTFS data");
    
                // This will occur if the last modified check meant that no new data was downloaded
                // so we cancel this import completely
                self.state.delete(self.db).await?;
    
                return Ok(0);
            }
        };

        log::debug!("Zip last modified: {:?}", last_modified);

        // If the last modified is different to what we have for an in progress import
        // then we should abandon that one and start fresh
        if resuming && self.state.import.file_last_modified != last_modified {
            log::info!("GTFS data changed on server since last import, starting fresh");
            self.state.finish(self.db).await?;
            
            self.state = ImportState::new(self.db).await?;
        }

        // Check which files we (still) need to import and progress so far
        let import_files = self.state
            .files
            .iter()
            .filter_map(|(file_name, file)| {
                if file.end_time.is_none() {
                    Some((file_name.clone(), file.record_count as u64))
                } else {
                    None
                }
            })
            .collect();
    
        log::info!("Importing files: {:?}", import_files);
    
        self.state.import.file_last_modified = last_modified;
        self.state.save(Db(self.db)).await?;

        let gtfs_items = read_gtfs_from_zip(zip_reader, import_files);

        let mut table_columns = None;
        let mut table = None;
        let mut insert_count = 0;
    
        pin_mut!(gtfs_items);
        while let Some(item_and_pos) = gtfs_items.next().await {
            let (item, pos) = item_and_pos?;
    
            let item_table = table_for_item(&item);
    
            // If we start a new table or reach the limit then it's time to commit the batch
            if table != Some(item_table.clone()) || insert_count >= BATCH_SIZE {
                if table != Some(item_table.clone()) {
                    if let Some(last_item) = &self.last_item {
                        // Started a new table, which means we should close the state for the old one
                        self.state.finish_file(&filename_for_item(last_item));
                    }
                }

                self.commit_batch().await?;

                // and start next batch

                let mut columns = item.db_columns()?;
                table_columns = Some(columns.clone());
                table = Some(item_table.clone());
                
                // and the import_id - assumes all gtfs tables have this!
                columns.push(Alias::new("import_id"));
                
                insert_count = 0;
                self.insert = Query::insert()
                    .into_table(Alias::new(item_table))
                    .columns(columns)
                    .to_owned();
                self.last_item = None;
                self.stops = vec![];
            }

            let table_columns = table_columns.as_ref().expect("columns should be set");
            log::trace!("Columns: {:?}", table_columns);

            let mut values = item.db_values_for_columns(table_columns)?;
            values.push(self.state.import.id.into());
            log::trace!("Values: {:?}", values);

            self.insert.values(values)?;
            insert_count += 1;
            self.state.set_file_pos(&filename_for_item(&item), pos as i64);
            
            if let GtfsItem::Stop(stop) = &item {
                // Stops will have their index item updated too
                self.stops.push(stop.clone());
            }
            
            self.last_item = Some(item);
        }

        // And the final batch
        self.commit_batch().await?;
    
        // Success! We can complete this import
        let import_id = self.state.import.id;
        let item_count = self.state.files.values().map(|file| file.record_count as usize).sum();
        self.state.finish(self.db).await?;

        log::info!("Imported {} GTFS items", item_count);
    
        // and cleanup any old items
        let cleanup_queries = [
            "gtfs_agency",
            "gtfs_calendar",
            "gtfs_calendar_date",
            "gtfs_route",
            "gtfs_shape",
            "gtfs_stop",
            "gtfs_stop_time",
            "gtfs_trip",
        ];
        
        for table_name in &cleanup_queries {
            log::debug!("Cleaning up {}", table_name);
            let delete = Query::delete()
                .from_table(Alias::new(table_name.to_string()))
                .and_where(col(Alias::new("import_id")).lt(import_id))
                .statement();
            self.db.execute(delete).await?;
        }
    
        // for stop index, remove any not in stop table
        self.db.execute(
            Query::delete()
                .from_table(GtfsStopIndexIden::Table)
                .and_where(
                    col(GtfsStopIndexIden::Id).not_in_subquery(
                        Query::select()
                            .column(GtfsStopIden::Id)
                            .from(GtfsStopIden::Table)
                            .to_owned(),
                    ),
                )
                .statement(),
        )
        .await?;
    
        Ok(item_count)
    }
    
    pub async fn sync(db: &'a libsql_client::Client) -> GtfsSyncResult<usize> {
        let sync = Self::new(db).await?;
        sync.do_sync().await
    }

}


#[cfg(test)]
mod test {

    use crate::{gtfs::sync::GtfsSync, test_utils::*};

    #[tokio::test]
    #[ignore]
    async fn test_sync_gtfs_data() {
        let db = db().await;
        let count = GtfsSync::sync(&db).await.unwrap();

        assert!(count > 0);
    }
}
