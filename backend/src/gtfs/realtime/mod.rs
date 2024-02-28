mod alert;
mod error;
mod trip_update;
mod utils;
mod vehicle;
use crate::gtfs::realtime::vehicle::process_vehicle;
use std::time::Duration;

use chrono::{TimeZone, Utc};
pub use error::Error;
use sea_orm::{DatabaseTransaction, TransactionTrait};
use tokio::time::sleep;

use crate::{
    gtfs::realtime::alert::process_alert, gtfs::realtime::trip_update::process_trip_update,
    ContextData,
};

use self::error::RtResult;

use super::structure::realtime::FeedEntity;

async fn process_shape(_tx: &DatabaseTransaction, entity: FeedEntity) -> RtResult<()> {
    log::info!("Got a shape, but this is not implemented: {:?}", entity);
    Ok(())
}

pub async fn monitor_firehose(ctx: &ContextData) -> RtResult<()> {
    log::info!("Firehose monitor is running");

    let last_update_time = Utc.timestamp_opt(0, 0).unwrap();

    loop {
        let updates = match ctx.at_client.get_realtime_feed().await {
            Ok(updates) => updates,
            Err(e) => {
                log::error!("Error getting realtime feed: {}", e);
                sleep(Duration::from_secs(30)).await;
                continue;
            }
        };

        if updates.header.timestamp <= Some(last_update_time) {
            log::debug!("No new updates");
            sleep(Duration::from_secs(15)).await;
            continue;
        }

        let count = updates.entity.len();

        log::debug!("Start processing updates");

        let tx = ctx.db.begin().await?;
        {
            for entity in updates.entity {

                let result: RtResult<()> = {
                    if entity.alert.is_some() {
                        process_alert(&tx, entity.clone()).await
                    } else if entity.trip_update.is_some() {
                        process_trip_update(&tx, entity.clone()).await
                    } else if entity.vehicle.is_some() {
                        process_vehicle(&tx, entity.clone()).await
                    } else if entity.shape.is_some() {
                        process_shape(&tx, entity.clone()).await
                    } else {
                        Ok(())
                    }
                };

                match result {
                    Ok(()) => {}
                    Err(e) => {
                        log::error!("Error processing entity: {}", e);
                        continue;
                    }
                };
            }
        }
        tx.commit().await?;

        log::debug!("End processing - {} updates", count);

        // TODO delay heuristic?

        sleep(Duration::from_secs(31)).await;
    }
}

pub async fn cleanup(db: &DatabaseTransaction) -> RtResult<()> {
    alert::cleanup_alerts(db).await?;
    // TODO other types
    Ok(())
}
