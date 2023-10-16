use std::num::ParseIntError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Failed to parse arguments")]
    Argument,

    #[error("Failed to parse int")]
    ParseInt(#[from] ParseIntError),

    #[error("{0}")]
    Execution(String),

    #[error("Invaild systax: {0}")]
    SystaxError(&'static str),

    #[error("Systax Error: {0}")]
    TokenizerError(String),
    #[error("Failed to convert to required type")]
    ConvertionError(#[from] std::convert::Infallible),
}

pub type DBError<T> = Result<T, DatabaseError>;
