#[derive(thiserror::Error, Debug)]
pub enum AtError {
    #[error("Init error: {0}")]
    Init(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Deserialize error: {0}")]
    Deserialize(#[from] serde_json::Error),
}

pub type AtResult<T> = Result<T, AtError>;
