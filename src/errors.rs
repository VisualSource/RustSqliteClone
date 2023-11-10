use crate::engine::error::Error as EngineError;
use crate::sql::error::Error as SqlError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Database engine error: {0}")]
    Engine(#[from] EngineError),
    #[error("Sql Error: {0}")]
    Sql(#[from] SqlError),
    #[error("Io Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Utf8 convertion error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("Invaild Argument")]
    Argument,
    #[error("Unexpexted Error: {0}")]
    Unexpexted(&'static str),
    #[error("Serde Error: {0}")]
    Serde(#[from] serde_json::Error),
}
