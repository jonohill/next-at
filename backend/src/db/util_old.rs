use itertools::Itertools;
use libsql_client::{Statement, Value};
use log::log_enabled;
use sea_query::{
    Alias, Expr, Iden, InsertStatement, IntoColumnRef, IntoIden, NullAlias, QueryStatementWriter, SelectStatement, SimpleExpr, SqliteQueryBuilder
};
use serde::{de::DeserializeOwned, Serialize};

use crate::gtfs::structure::GtfsStopIden;

use super::error::{DbError, DbResult};

fn libsql_value_to_json_value(value: Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Blob { value } => {
            serde_json::Value::Array(value.into_iter().map(|v| v.into()).collect())
        }
        Value::Float { value } => {
            serde_json::Value::Number(serde_json::Number::from_f64(value).unwrap())
        }
        Value::Integer { value } => serde_json::Value::Number(value.into()),
        Value::Text { value } => serde_json::Value::String(value),
    }
}

fn try_json_value_to_sea_value(value: serde_json::Value) -> DbResult<sea_query::Value> {
    if matches!(value, serde_json::Value::Object(_)) {
        // nested structures are treated as json
        return Ok(value.to_string().into());
    }

    let value: sea_query::Value = match value {
        serde_json::Value::Null => sea_query::Value::Int(None),
        serde_json::Value::Bool(v) => v.into(),
        serde_json::Value::Number(v) => {
            if let Some(i) = v.as_u64() {
                i.into()
            } else if let Some(i) = v.as_i64() {
                i.into()
            } else if let Some(f) = v.as_f64() {
                f.into()
            } else {
                // maybe unreachable?
                return Err(DbError::Prepare(
                    "Number is outside supported range".to_string(),
                ));
            }
        }
        serde_json::Value::String(v) => v.into(),
        serde_json::Value::Array(a) => {
            // array of numbers is assumed to be a blob
            a.iter()
                .map(|v| match v.as_u64() {
                    Some(u) => Ok(u as u8),
                    None => Err(DbError::Prepare(
                        "Array contains non-integer value".to_string(),
                    )),
                })
                .collect::<Result<Vec<u8>, DbError>>()
                .map(|v| v.into())?
        }
        serde_json::Value::Object(_) => unreachable!("already tested"),
    };
    Ok(value)
}

pub fn from_db_row<T: DeserializeOwned>(row: &libsql_client::Row) -> DbResult<T> {
    let object: serde_json::Value = row
        .value_map
        .iter()
        .map(|(k, v)| {
            log::trace!("{}: {:?}", k, v);
            let json_value = libsql_value_to_json_value(v.clone());
            Ok((k.to_string(), json_value))
        })
        .collect::<DbResult<serde_json::Map<String, serde_json::Value>>>()?
        .into();

    serde_json::from_value(object)
        .map_err(|e| DbError::Result(format!("Error deserialising from DB: {}", e)))
}

pub trait DbEntity: Serialize + DeserializeOwned + Clone {
    fn from_db_row(row: &libsql_client::Row) -> DbResult<Self> {
        from_db_row(row)
    }

    fn db_values(&self) -> DbResult<Vec<(Alias, sea_query::SimpleExpr)>> {
        serde_json::to_value(self)?
            .as_object()
            .ok_or_else(|| DbError::Prepare("Only structs are supported".to_string()))?
            .iter()
            .map(|(k, v)| {
                try_json_value_to_sea_value(v.clone())
                    .map(|db_value| (Alias::new(k.to_string()), db_value.into()))
            })
            .collect()
    }

    fn db_values_for_columns(&self, columns: &[Alias]) -> DbResult<Vec<sea_query::SimpleExpr>> {
        let values = self.db_values()?;

        let col_values = columns.iter().map(|col| {
            values
                .iter()
                .find(|(k, _)| k.to_string() == col.to_string())
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| sea_query::Value::Int(None).into())
        }).collect_vec();

        Ok(col_values)
    }

    fn db_columns(&self) -> DbResult<Vec<Alias>> {
        let cols = serde_json::to_value(self)
            .unwrap()
            .as_object()
            .ok_or_else(|| DbError::Prepare("Only structs are supported".to_string()))?
            .keys()
            .map(|k| Alias::new(k.to_string()))
            .collect();
        Ok(cols)
    }
}

impl<T> DbEntity for T where T: Serialize + DeserializeOwned + Clone {}

pub trait DbTable {
    fn db_table(&self) -> impl IntoIden;
}

pub fn to_sql_date(date: &chrono::NaiveDateTime) -> String {
    date.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn sql_now() -> String {
    to_sql_date(&chrono::Utc::now().naive_local())
}

pub trait ResultSetEx {
    fn iter<T>(&self) -> impl Iterator<Item = DbResult<T>>
    where
        T: DeserializeOwned;

    fn first<T>(&self) -> Option<DbResult<T>>
    where
        T: DeserializeOwned,
    {
        self.iter().next()
    }
}

impl ResultSetEx for libsql_client::ResultSet {
    fn iter<T>(&self) -> impl Iterator<Item = DbResult<T>>
    where
        T: DeserializeOwned,
    {
        self.rows.iter().map(|row| from_db_row(row))
    }
}

/// Short hand for sea_query::Expr::col
pub fn col<T>(n: T) -> Expr
where
    T: IntoColumnRef,
{
    Expr::col(n)
}

fn map_sea_value_to_libsql_value(value: sea_query::Value) -> Value {
    use sea_query::Value::*;

    let value = match value {
        Bool(v) => v.map(|v| (v as u8).into()),
        TinyInt(v) => v.map(|v| v.into()),
        SmallInt(v) => v.map(|v| v.into()),
        Int(v) => v.map(|v| v.into()),
        BigInt(v) => v.map(|v| v.into()),
        TinyUnsigned(v) => v.map(|v| v.into()),
        SmallUnsigned(v) => v.map(|v| v.into()),
        Unsigned(v) => v.map(|v| v.into()),
        BigUnsigned(v) => v.map(|v| (v as i64).into()),
        Float(v) => v.map(|v| v.into()),
        Double(v) => v.map(|v| v.into()),
        String(v) => v.map(|v| (*v).into()),
        Char(v) => v.map(|v| v.to_string().into()),
        Bytes(v) => v.map(|v| (*v).into()),
    };

    value.unwrap_or(Value::Null)
}

pub trait LibSqlStatement {
    fn statement(&self) -> Statement;
}

impl<T: QueryStatementWriter> LibSqlStatement for T {
    fn statement(&self) -> Statement {
        let (sql, values) = self.build(SqliteQueryBuilder);

        if log_enabled!(log::Level::Trace) {
            log::trace!("{}", self.to_string(SqliteQueryBuilder));
        }

        let values = values
            .into_iter()
            .map(map_sea_value_to_libsql_value)
            .collect_vec();
        Statement::with_args(sql, &values)
    }
}

pub trait DbEntityInsert<T: DbEntity> {
    fn column_values(&mut self, item: &T) -> DbResult<&mut Self>;

    /// Insert the item, but also add additional values to the insert statement
    fn column_values_with<I>(&mut self, item: &T, additional_values: I) -> DbResult<&mut Self>
    where
        I: IntoIterator<Item = (Alias, SimpleExpr)>;
}

impl<T: DbEntity> DbEntityInsert<T> for InsertStatement {
    fn column_values(&mut self, item: &T) -> DbResult<&mut Self>
    where
        T: DbEntity,
    {
        let (cols, values): (Vec<_>, Vec<_>) = item.db_values()?.into_iter().unzip();
        let insert = self.columns(cols).values(values)?;
        Ok(insert)
    }

    fn column_values_with<I>(&mut self, item: &T, additional_values: I) -> DbResult<&mut Self>
    where
        T: DbEntity,
        I: IntoIterator<Item = (Alias, SimpleExpr)>,
    {
        let additional_values = additional_values.into_iter().collect_vec();

        let all_values = item
            .db_values()?
            .into_iter()
            .filter(|(k, _)| {
                !additional_values
                    .iter()
                    .any(|(k2, _)| k.to_string() == k2.to_string())
            })
            .chain(additional_values.clone());

        let (cols, values): (Vec<_>, Vec<_>) = all_values.into_iter().unzip();
        let insert = self.columns(cols).values(values)?;
        Ok(insert)
    }
}

pub trait ColumnsAs {
    fn columns_as<S, T, I>(&mut self, cols: I) -> &mut Self
    where
        S: ToString,
        T: IntoColumnRef,
        I: IntoIterator<Item = (T, S)>;
        
    fn col_as<S, T>(&mut self, col: T, alias: S) -> &mut Self
    where
        S: ToString,
        T: IntoColumnRef,
    {
        self.columns_as(std::iter::once((col, alias)))
    }
}

impl ColumnsAs for SelectStatement {
    fn columns_as<S, T, I>(&mut self, cols: I) -> &mut Self
    where
        S: ToString,
        T: IntoColumnRef,
        I: IntoIterator<Item = (T, S)>,
    {
        for (col, alias) in cols {
            self.expr_as(Expr::col(col), Alias::new(alias.to_string()));
        }
        self
    }

}


#[cfg(test)]
mod test {
    use sea_query::{enum_def, Asterisk, Query};
    use serde_json::json;

    use super::{col, LibSqlStatement};

    #[enum_def]
    #[allow(dead_code)]
    struct SomeItem {
        id: i64,
    }

    #[test]
    fn test_libsql_seaquery() {
        let get = Query::select()
            .column(Asterisk)
            .from(SomeItemIden::Table)
            .and_where(col(SomeItemIden::Id).eq(1))
            .statement();

        // statement is opaque but produces a json string
        let get_value: serde_json::Value = serde_json::from_str(&get.to_string()).unwrap();
        assert_eq!(
            get_value,
            json!({
                "sql": r#"SELECT * FROM "some_item" WHERE "id" = ?"#,
                "args": ["1"]
            })
        );
    }
}
