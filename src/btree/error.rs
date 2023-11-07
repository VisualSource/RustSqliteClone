use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unexpected Error")]
    UnexpectedError,
    #[error("UTF-8 Error")]
    UTF8Error,
    #[error("Key Overflow Error")]
    KeyOverflowError,

    #[error("Value Overflow Error")]
    ValueOverflowError,

    #[error("Unexpected Error: Array recieved is larger than the maximum allowed size of: {0}.")]
    TryFromSliceError(&'static str),

    #[error("IoError")]
    IoError(#[from] std::io::Error),

    #[error("Key Not Found")]
    KeyNotFound,
}
