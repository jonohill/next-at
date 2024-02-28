use chrono::Utc;
use sea_orm::{DatabaseConnection, EntityTrait};
use tokio::time::sleep;

use crate::db::util::open_seaorm;
use crate::entity::prelude::*;
use crate::gtfs::sync::Sync;
use crate::gtfs::{index, realtime};
use sea_orm::DbErr;
use sea_orm::TransactionTrait;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Database error: {0}")]
    Database(#[from] DbErr),

    #[error("Realtime error: {0}")]
    Realtime(#[from] crate::gtfs::realtime::Error),

    #[error("Indexing error: {0}")]
    Indexing(#[from] crate::gtfs::index::Error),

    #[error("Sync error: {0}")]
    Sync(#[from] crate::gtfs::sync::GtfsSyncError),
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub async fn sync_and_index(db: &DatabaseConnection) -> Result<()> {
    log::info!("Checking for new data");

    if Sync::sync(db).await? > 0 {
        // Indexes only need to be rebuilt if there is new data
        index::build_stop_index().await?;
        index::build_stop_time_index().await?;
    }

    Ok(())
}

/// Runs forever, doing maintenance at the maintenance window time
pub async fn keep_maintained() -> Result<()> {
    let db = open_seaorm().await;

    loop {
        let maintenance_time = MaintenanceTime::find_by_id(1)
            .one(&db)
            .await?
            .expect("No maintenance time. Not synced and indexed?")
            .minute_of_day as i64;

        let current_minute = Utc::now().timestamp() % 86400 / 60;
        let wait_time = if current_minute < maintenance_time {
            maintenance_time - current_minute
        } else {
            1440 /* minutes in a day */ - current_minute + maintenance_time
        };

        log::info!("Waiting {} minutes for maintenance window", wait_time);
        sleep(tokio::time::Duration::from_secs(wait_time as u64 * 60)).await;

        log::info!("Starting maintenance");

        // update static data
        // this also deletes all the old data
        sync_and_index(&db).await?;

        let tx = db.begin().await?;
        realtime::cleanup(&tx).await?;
        tx.commit().await?;

        log::info!("Maintenance done");
    }
}
