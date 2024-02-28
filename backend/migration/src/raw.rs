use sea_orm_migration::{async_trait, sea_orm::ConnectionTrait, DbErr, MigrationName, MigrationTrait, SchemaManager};

pub trait RawSql: Send + Sync {
    fn up_sql() -> String;
    fn down_sql() -> Option<String>;
}

#[macro_export]
macro_rules! sql_migration {
    ($sql_name:expr, $sql_up:expr, $sql_down:expr) => {
        paste::paste! {
            pub struct [<Sql $sql_name:camel>];

            impl RawSql for [<Sql $sql_name:camel>] {
                fn up_sql() -> String {
                    $sql_up
                }

                fn down_sql() -> Option<String> {
                    $sql_down
                }
            }

            impl [<Sql $sql_name:camel>] {
                fn boxed() -> Box<dyn MigrationTrait> {
                    Box::new(
                        RawMigration::new($sql_name, [<Sql $sql_name:camel>])
                    )
                }
            }
        }
    };
}

#[macro_export]
macro_rules! sql_up {
    ($name:expr) => {
        sql_migration!(
            $name,
            include_str!(concat!("sql/", $name, ".sql")).to_string(),
            None
        );
    };
}

#[macro_export]
macro_rules! sql_up_down {
    ($name:expr) => {
        sql_migration!(
            $name,
            include_str!(concat!("sql/", $name, "/up.sql")).to_string(),
            Some(include_str!(concat!("sql/", $name, "/down.sql")).to_string())
        );
    };
}

pub struct RawMigration<T: RawSql> {
    name: String,
    _sql: T,
}

impl<T: RawSql> RawMigration<T> {
    pub fn new(name: &str, sql: T) -> Self {
        Self {
            name: name.to_string(),
            _sql: sql,
        }
    }
}

impl<T: RawSql> MigrationName for RawMigration<T> {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<T: RawSql> MigrationTrait for RawMigration<T> {

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        db.execute_unprepared(&T::up_sql()).await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if let Some(down_sql) = T::down_sql() {
            let db = manager.get_connection();

            db.execute_unprepared(&down_sql).await?;
        } else {
            return Err(DbErr::Migration("Down migration not implemented".to_string()));
        }

        Ok(())
    }
    
}

impl <T: RawSql> RawSql for RawMigration<T> {
    fn up_sql() -> String {
        T::up_sql()
    }

    fn down_sql() -> Option<String> {
        T::down_sql()
    }
}
