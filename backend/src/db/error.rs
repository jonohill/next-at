#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error("{0}")]
    Prepare(String),

    #[error("{0}")]
    Query(#[from] sea_orm::DbErr),
}

impl From<serde_json::Error> for DbError {
    fn from(e: serde_json::Error) -> Self {
        DbError::Prepare(format!("{}", e))
    }
}

pub type DbResult<T> = Result<T, DbError>;
