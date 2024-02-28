use crate::entity::prelude::*;
use crate::{db::util::SeaRusqliteAdapter, entity::service};
use async_zip::{base::read::mem::ZipFileReader, error::ZipError};
use itertools::Itertools;
use rusqlite::vtab::csvtab;
use sea_orm::sea_query::UnionType;
use sea_orm::{
    sea_query::{self, Alias, Expr, OnConflict, Query, SqliteQueryBuilder},
    ActiveModelTrait, DatabaseConnection, EntityName, EntityTrait, Iden, IntoActiveModel, Iterable,
    QueryOrder, Set,
};
use tempfile::TempDir;
use tokio::{
    fs::File,
    io::{self, AsyncWriteExt},
    task,
};
use tokio_util::compat::FuturesAsyncReadCompatExt;

use crate::{
    db::util::open_rusqlite,
    entity::{
        gtfs_agency, gtfs_calendar, gtfs_calendar_dates, gtfs_feed_info, gtfs_routes, gtfs_shapes,
        gtfs_stop_times, gtfs_stops, gtfs_trips, import, prelude::Import,
    },
};

#[derive(thiserror::Error, Debug)]
pub enum GtfsSyncError {
    #[error("Request error: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("Zip error: {0}")]
    BadZipFile(#[from] async_zip::error::ZipError),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Database error: {0}")]
    DbError(#[from] sea_orm::DbErr),

    #[error("Database query error: {0}")]
    DbPrepareError(#[from] sea_query::error::Error),

    #[error("CSV Import error: {0}")]
    CsvImportError(#[from] rusqlite::Error),

    #[error("Index build error: {0}")]
    IndexBuildError(#[from] crate::gtfs::index::Error),
}

pub type GtfsSyncResult<T> = Result<T, GtfsSyncError>;

const AT_GTFS_ZIP_URL: &str = "https://gtfs.at.govt.nz/gtfs.zip";

// Order is important!
const FILE_NAMES: [&str; 9] = [
    "feed_info.txt",
    "agency.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "routes.txt",
    "trips.txt",
    "shapes.txt",
    "stops.txt",
    "stop_times.txt",
];

trait ImportEx {
    async fn get_last_import(db: &DatabaseConnection) -> GtfsSyncResult<Option<import::Model>>;
}

impl ImportEx for Import {
    async fn get_last_import(db: &DatabaseConnection) -> GtfsSyncResult<Option<import::Model>> {
        use import::Column::*;

        let found = Import::find().order_by_desc(Timestamp).one(db).await?;
        Ok(found)
    }
}

pub async fn get_gtfs_files_from_zip(
    url: &str,
    if_modified_since: Option<String>,
) -> GtfsSyncResult<Option<(Option<String>, TempDir)>> {
    let resp = reqwest::get(url).await?.error_for_status()?;

    let last_modified = resp
        .headers()
        .get("last-modified")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string());

    // Check the last modified header here - Azure block storage seems to ignore If-Modified-Since/If-None-Match!
    if if_modified_since == last_modified {
        return Ok(None);
    }

    // I'd prefer to stream the response into the zip reader
    // however it appears, at least for the tested zips, that they are are compressed
    // in a way that requires the dictionary (end of the file) to be read
    // If memory usage becomes an issue, consider reading ranges from the server

    let bytes = resp.bytes().await?;

    let zip_reader = ZipFileReader::new(bytes.into()).await?;

    let tmp_dir = TempDir::new()?;

    for i in 0..usize::MAX {
        let entry = match zip_reader.reader_with_entry(i).await {
            Ok(entry) => entry,
            Err(ZipError::EntryIndexOutOfBounds) => break,
            Err(e) => return Err(e.into()),
        };

        let filename = entry.entry().filename().as_str()?;

        if !FILE_NAMES.contains(&filename) {
            continue;
        }

        let path = tmp_dir.path().join(filename);
        let mut reader = entry.compat();

        let mut file = File::create(path).await?;
        io::copy(&mut reader, &mut file).await?;
        file.flush().await?;
    }

    Ok(Some((last_modified, tmp_dir)))
}

macro_rules! insert_from_csv {
    ($import_id:expr, $mod:ident, $id_cols:expr) => {
        {
            use $mod::*;

            let table_name = Entity::default().table_name().to_string();
            let csv_table_name = format!("{}_{}", table_name, $import_id);

            // Same as the table without the id/import_id
            let csv_columns = Column::iter()
                .filter(|c| !["id", "import_id"].contains(&c.to_string().as_str()))
                .collect_vec();

            // import_id at the end so we can select in the same order
            let all_columns = csv_columns.clone().into_iter()
                .chain([Column::ImportId]);

            let unique_cols = $id_cols
                .iter()
                .map(|c| Alias::new(c.to_string())).collect_vec();

            let csv_data = Query::select()
                .columns(csv_columns.clone())
                .expr_as(Expr::value($import_id), Alias::new("import_id")) // must come after cols
                .from(Alias::new(csv_table_name.clone()))
                // sqlite docs on select/insert upserts:
                // to avoid a parsing ambiguity, the SELECT statement should always contain a WHERE clause,
                // even if that clause is simply "WHERE true"
                .and_where(Expr::cust("true"))
                .to_owned();

            let mut insert = Query::insert()
                .into_table(Entity)
                .columns(all_columns.clone())
                .select_from(csv_data)?
                .to_owned();

            if ! unique_cols.is_empty() {
                insert
                    .on_conflict(
                        OnConflict::columns(unique_cols)
                            .update_columns(all_columns)
                            .to_owned()
                    );
            }

            let insert_sql = insert.to_string(SqliteQueryBuilder);

            // and cleanup old records
            let cleanup_sql = Query::delete()
                .from_table(Entity)
                .and_where(Expr::col(Column::ImportId).lt($import_id))
                .to_string(SqliteQueryBuilder);

            // The last statement is the insert so the count is correct
            let sql = [
                cleanup_sql,
                insert_sql
            ].join(";\n");

            (csv_table_name, sql)
        }
    };
}

struct SyncState {
    import_id: i64,
    file_dir: TempDir,
}

fn import_csvs(state: &SyncState) -> GtfsSyncResult<u64> {
    let SyncState {
        import_id,
        file_dir,
    } = state;

    // Rusqlite is used directly for its csv import functionality
    let db = open_rusqlite()?;
    csvtab::load_module(&db)?;

    let dir_path = file_dir.path();

    let mut insert_count = 0;

    for filename in &FILE_NAMES {
        let path = dir_path.join(filename).to_str().unwrap().to_string(); // only if somehow invalid utf-8

        let i_id = *import_id;
        let (csv_table_name, update_from_csv) = match *filename {
            "feed_info.txt" => insert_from_csv!(i_id, gtfs_feed_info, [] as [String; 0]),
            "agency.txt" => insert_from_csv!(i_id, gtfs_agency, ["agency_id"]),
            "calendar.txt" => insert_from_csv!(i_id, gtfs_calendar, ["service_id"]),
            "calendar_dates.txt" => {
                insert_from_csv!(i_id, gtfs_calendar_dates, ["service_id", "date"])
            }
            "routes.txt" => insert_from_csv!(i_id, gtfs_routes, ["route_id"]),
            "trips.txt" => insert_from_csv!(i_id, gtfs_trips, ["trip_id"]),
            "shapes.txt" => insert_from_csv!(i_id, gtfs_shapes, ["shape_id", "shape_pt_sequence"]),
            "stops.txt" => insert_from_csv!(i_id, gtfs_stops, ["stop_id"]),
            "stop_times.txt" => {
                insert_from_csv!(i_id, gtfs_stop_times, ["trip_id", "stop_sequence"])
            }
            other => panic!("FILE_NAMES out of sync with insert code: {}", other),
        };

        let statement = format!(
            "
            BEGIN;
            CREATE VIRTUAL TABLE temp.{csv_table_name} USING csv(filename='{path}', header=yes);
            {update_from_csv};
            COMMIT;
        "
        );

        log::trace!("{}", statement);

        db.execute_batch(&statement)?;
        insert_count += db.changes();
    }

    Ok(insert_count)
}

pub struct Sync<'a> {
    db: &'a DatabaseConnection,
    // state: SyncState,
}

impl<'a> Sync<'a> {
    async fn do_sync(&self) -> GtfsSyncResult<u64> {
        log::debug!("Syncing GTFS data...");

        let last_import = Import::get_last_import(self.db).await?;

        let prev_last_modified = last_import.and_then(|i| i.file_last_modified);

        let (last_modified, tmp_dir) =
            match get_gtfs_files_from_zip(AT_GTFS_ZIP_URL, prev_last_modified).await? {
                Some((last_modified, tmp_dir)) => (last_modified, tmp_dir),
                None => {
                    log::debug!("No new GTFS data available");
                    return Ok(0);
                }
            };

        log::debug!("GTFS files extracted to {:?}", tmp_dir.path());

        let new_import = import::ActiveModel {
            ..Default::default()
        }
        .insert(self.db)
        .await?;

        let record_count = task::spawn_blocking(move || {
            import_csvs(&SyncState {
                import_id: new_import.id,
                file_dir: tmp_dir,
            })
        })
        .await
        .unwrap()?; // unwrap spawn error

        log::debug!("Finished GTFS static data import");

        // And build service table
        // This is not in the spec, but it provides a way to have FKs between all the tables
        let db = open_rusqlite()?;

        let mut date_services = Query::select()
            .distinct()
            .column(gtfs_calendar_dates::Column::ServiceId)
            .from(gtfs_calendar_dates::Entity)
            .to_owned();
        let regular_services = Query::select()
            .distinct()
            .column(gtfs_calendar::Column::ServiceId)
            .from(gtfs_calendar::Entity)
            .to_owned();
        let all_services = date_services
            .union(UnionType::Distinct, regular_services)
            .to_owned();

        Query::insert()
            .into_table(Service)
            .columns([service::Column::ServiceId])
            .select_from(all_services)?
            .prepare(&db)?
            .execute()?;

        // success
        let mut this_import = new_import.into_active_model();
        this_import.file_last_modified = Set(last_modified);
        this_import.save(self.db).await?;

        // build_stop_index(self.db).await?;

        Ok(record_count)
    }

    pub async fn sync(db: &'a DatabaseConnection) -> GtfsSyncResult<u64> {
        Self {
            db,
            // state: SyncState {
            //     import_id: 0,
            //     file_dir: TempDir::new()?,
            // },
        }
        .do_sync()
        .await
    }
}
