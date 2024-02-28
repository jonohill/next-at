use std::ops::Add;

use crate::entity::alert_active_period;
use crate::entity::{alert, alert_informed_entity};
use crate::gtfs::realtime::utils::find_trip_run;
use crate::gtfs::structure::realtime::FeedEntity;
use chrono::Utc;
use sea_orm::ActiveValue::NotSet;
use sea_orm::QueryTrait;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect, Set,
    TransactionTrait,
};

use super::error::RtResult;

pub async fn process_alert(tx: &DatabaseTransaction, entity: FeedEntity) -> RtResult<()> {
    let alert = entity.alert.expect("Expected alert to be set");

    use alert::*;

    let sp = tx.begin().await?;

    alert_informed_entity::Entity::delete_many()
        .filter(alert_informed_entity::Column::AlertId.eq(entity.id.clone()))
        .exec(tx)
        .await?;

    alert::Entity::delete_many()
        .filter(Column::AlertId.eq(entity.id.clone()))
        .exec(tx)
        .await?;

    alert::ActiveModel {
        id: NotSet,
        alert_id: Set(Some(entity.id.clone())),
        cause: Set(alert.cause.map(|c| c as i32)),
        effect: Set(alert.effect.map(|e| e as i32)),
        header_text: Set(alert.header_text.map(|t| t.get("en")).flatten()),
        description_text: Set(alert.description_text.map(|t| t.get("en")).flatten()),
        timestamp: Set(Some(Utc::now().timestamp_millis())),
    }
    .insert(tx)
    .await?;

    if let Some(entities) = alert.informed_entity {
        for informed in entities {
            let mut trip_run = None;
            if let Some(trip) = informed.trip {
                trip_run = Some(find_trip_run(tx, trip).await?);
            }

            alert_informed_entity::ActiveModel {
                id: NotSet,
                alert_id: Set(Some(entity.id.clone())),
                agency_id: Set(informed.agency_id),
                route_id: Set(informed.route_id),
                route_type: Set(informed.route_type.map(|rt| rt as i32)),
                stop_id: Set(informed.stop_id),
                direction_id: Set(informed.direction_id.map(|d| d as i32)),
                trip_run_id: Set(trip_run.map(|tr| tr.id)),
            }
            .insert(tx)
            .await?;
        }
    }

    if let Some(active_periods) = alert.active_period {
        for active_period in active_periods {
            let start = active_period.start.unwrap_or(0);
            // technically until the end of time if set but we'll assume tomorrow
            let end = active_period.end.unwrap_or_else(|| {
                Utc::now().add(chrono::Duration::days(1)).timestamp_millis() as u64
            });

            alert_active_period::ActiveModel {
                id: NotSet,
                alert_id: Set(entity.id.clone()),
                start_timestamp: Set(start as i64),
                end_timestamp: Set(end as i64),
            }
            .insert(tx)
            .await?;
        }
    }

    sp.commit().await?;

    Ok(())
}

pub async fn cleanup_alerts(tx: &DatabaseTransaction) -> RtResult<()> {
    let sp = tx.begin().await?;

    alert_active_period::Entity::delete_many()
        .filter(alert_active_period::Column::EndTimestamp.lt(Utc::now().timestamp_millis() as i64))
        .exec(tx)
        .await?;

    alert::Entity::delete_many()
        .filter(
            alert::Column::AlertId.not_in_subquery(
                alert_active_period::Entity::find()
                    .select_only()
                    .column(alert_active_period::Column::AlertId)
                    .into_query(),
            ),
        )
        .exec(tx)
        .await?;

    sp.commit().await?;

    Ok(())
}
