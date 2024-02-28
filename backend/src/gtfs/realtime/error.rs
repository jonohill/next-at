use std::num::ParseFloatError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("AT error: {0}")]
    At(#[from] crate::at::error::AtError),

    // TODO can't work out how to have the proper tx error, which uses a generic
    #[error("Database Transaction error: {0}")]
    Unknown(String),

    #[error("Database error: {0}")]
    Db(#[from] sea_orm::DbErr),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Invalid date: {0}")]
    DateFormat(#[from] crate::gtfs::utils::DateError),

    #[error("Parse Error: {0}")]
    Parse(#[from] ParseFloatError),
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, e)
    }
}

pub type RtResult<T> = Result<T, Error>;
