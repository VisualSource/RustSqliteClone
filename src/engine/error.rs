use std::num::ParseIntError;

use thiserror::Error;
#[derive(Debug, Error)]
pub enum Error {
    #[error("Offset Overflow")]
    OffsetOverflow,
    #[error("Array recieved is large then maxium allowd size.")]
    TryFromSlice,
    #[error("Unexpected Error")]
    Unexpected,
    #[error("Failed to encode item: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("Failed to decode item: {0}")]
    Decode(#[from] bincode::error::DecodeError),
    #[error("IOError: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unexpected Error: {0}")]
    UnexpectedWithReason(&'static str),
    #[error("Not Found")]
    NotFound,
    #[error("Transform Erorr: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Validation Error: {0}")]
    Validate(String),

    #[error("Parse Int Error: {0}")]
    ParseInt(#[from] ParseIntError),
    #[error("Failed to lock")]
    Lock,
}
