use std::num::ParseIntError;

use actix_web::{HttpResponse, ResponseError};
use reqwest::StatusCode;
use serde_json::json;

use crate::gtfs;
use crate::{at::error::AtError, db::error::DbError, gtfs::sync::GtfsSyncError};

#[allow(dead_code)]
#[derive(thiserror::Error, Debug)]
pub enum NextAtError {
    #[error("AT error: {0}")]
    At(#[from] AtError),

    #[error("Database error: {0}")]
    Db(#[from] DbError),

    #[error("Database error: {0}")]
    Database(#[from] sea_orm::DbErr),

    #[error("Data format error: {0}")]
    DataFormat(String),

    #[error("GTFS sync error: {0}")]
    GtfsSync(#[from] GtfsSyncError),

    #[error("GTFS index error: {0}")]
    GtfsIndex(#[from] gtfs::index::Error),

    #[error(transparent)]
    Request(#[from] reqwest::Error),

    #[error("Error response: {0} {1}")]
    Response(u16, String),
}

impl From<ParseIntError> for NextAtError {
    fn from(value: ParseIntError) -> Self {
        NextAtError::DataFormat(value.to_string())
    }
}

impl ResponseError for NextAtError {
    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        match self {
            NextAtError::Response(_, message) => {
                HttpResponse::build(self.status_code()).json(json!({ "error": message }))
            }
            other => {
                log::error!("{}", other);
                actix_web::HttpResponse::InternalServerError().finish()
            }
        }
    }

    fn status_code(&self) -> reqwest::StatusCode {
        match self {
            NextAtError::Response(status, _) => {
                StatusCode::from_u16(*status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            }
            _ => reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

pub type NextAtResult<T> = Result<T, NextAtError>;
