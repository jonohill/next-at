use std::{env, ops::Deref};

use rusqlite::{params_from_iter, ParamsFromIter};
use sea_orm::{
    sea_query::{sea_value_to_json_value, QueryStatementWriter, SqliteQueryBuilder},
    DatabaseConnection, SqlxSqliteConnector,
};
use sea_orm::{
    sea_query::{Expr, IntoColumnRef, Nullable, SimpleExpr},
    ActiveValue,
};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
    SqlitePool,
};

pub async fn open_seaorm() -> DatabaseConnection {
    let db_path = env::var("DATABASE_PATH").expect("DATABASE_PATH must be set");

    // Create via sqlx so we can customise the options
    let options = SqliteConnectOptions::new()
        .filename(db_path.clone())
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal) // with WAL, worst that could happen is a rollback of last tx
        .pragma("cache_size", "-1000000"); // 1GB memory cache

    let pool = SqlitePool::connect_with(options).await.unwrap();

    SqlxSqliteConnector::from_sqlx_sqlite_pool(pool)
}

pub fn open_rusqlite() -> Result<rusqlite::Connection, rusqlite::Error> {
    let db_path = env::var("DATABASE_PATH").expect("DATABASE_PATH must be set");

    let conn = rusqlite::Connection::open(db_path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "cache_size", "-1000000")?;
    // rusqlite is used for bulk imports, disabling FKs is faster for this
    conn.pragma_update(None, "foreign_keys", "OFF")?;

    Ok(conn)
}

pub trait SeaRusqliteAdapter {
    /// Prepares a sea query for use with rusqlite
    fn prepare<'conn>(
        &self,
        db: &'conn rusqlite::Connection,
    ) -> Result<SeaRusqlitePrepared<'conn>, rusqlite::Error>;
}

impl<Q: QueryStatementWriter> SeaRusqliteAdapter for Q {
    fn prepare<'conn>(
        &self,
        db: &'conn rusqlite::Connection,
    ) -> Result<SeaRusqlitePrepared<'conn>, rusqlite::Error> {
        let (sql, values) = self.build(SqliteQueryBuilder);

        log::debug!("Prepared SQL: {}", sql);
        log::debug!("Prepared values: {:?}", values);

        let json_values: Vec<_> = values
            .into_iter()
            .map(|v| sea_value_to_json_value(&v))
            .collect();
        let params = params_from_iter(json_values);
        log::debug!("Prepared params: {:?}", params);

        Ok(SeaRusqlitePrepared {
            statement: db.prepare_cached(&sql)?,
            params,
        })
    }
}

pub struct SeaRusqlitePrepared<'conn> {
    pub statement: rusqlite::CachedStatement<'conn>,
    pub params: ParamsFromIter<Vec<serde_json::Value>>,
}

impl<'conn> Deref for SeaRusqlitePrepared<'conn> {
    type Target = rusqlite::CachedStatement<'conn>;

    fn deref(&self) -> &Self::Target {
        &self.statement
    }
}

impl<'conn> SeaRusqlitePrepared<'conn> {
    /// Consumes the wrapped prepared statement and returns the inner rusqlite statement
    pub fn into_inner(self) -> rusqlite::CachedStatement<'conn> {
        self.statement
    }

    pub fn execute(&mut self) -> Result<usize, rusqlite::Error> {
        self.statement.execute(self.params.clone())
    }

    pub fn query(&mut self) -> Result<rusqlite::Rows<'_>, rusqlite::Error> {
        self.statement.query(self.params.clone())
    }

    pub fn query_row<T, F>(&mut self, f: F) -> Result<T, rusqlite::Error>
    where
        F: FnOnce(&rusqlite::Row) -> Result<T, rusqlite::Error>,
    {
        self.statement.query_row(self.params.clone(), f)
    }

    // pub fn query_map<F, T>(&mut self, f: F) -> Result<rusqlite::MappedRows<'_, F>, rusqlite::Error>
    // where
    //     F: FnMut(&rusqlite::Row) -> Result<T, rusqlite::Error>,
    // {
    //     self.statement.query_map(self.params.clone(), f)
    // }
}

pub fn null() -> SimpleExpr {
    sea_orm::Value::Int(None).into()
}

pub fn pow<T>(x: T, v: usize) -> SimpleExpr
where
    T: Into<SimpleExpr>,
{
    if v == 0 {
        return Expr::val(1).into();
    }

    let base: SimpleExpr = x.into();
    let mut result = base.clone();

    for _ in 1..v {
        result = result.mul(base.clone());
    }

    result
}

pub fn col<T>(n: T) -> Expr
where
    T: IntoColumnRef,
{
    Expr::col(n)
}

pub trait OptionMapSet<T: Into<sea_orm::Value> + Nullable> {
    fn map_set(self) -> ActiveValue<Option<T>>;
}

impl<T: Into<sea_orm::Value> + Nullable> OptionMapSet<T> for Option<T> {
    fn map_set(self) -> ActiveValue<Option<T>> {
        match self {
            Some(v) => ActiveValue::Set(Some(v)),
            None => ActiveValue::NotSet,
        }
    }
}
