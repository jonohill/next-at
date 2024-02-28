pub use sea_orm_migration::prelude::*;

pub mod raw;

use crate::raw::*;

pub struct Migrator;

sql_up!("000001_gtfs_tables");
sql_up!("000002_realtime");
sql_up_down!("000003_stop_time_index_table");
sql_up!("000004_stop_time_index_indexes");

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Sql000001GtfsTables::boxed(),
            Sql000002Realtime::boxed(),
            Sql000003StopTimeIndexTable::boxed(),
            Sql000004StopTimeIndexIndexes::boxed(),
        ]
    }
}
