use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    UnknownChar(String),
    #[error("Systax Error: {0}")]
    Systax(&'static str),
}
