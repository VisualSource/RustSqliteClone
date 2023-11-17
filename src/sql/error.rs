use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    UnknownChar(String),
    #[error("{0}")]
    Systax(&'static str),
}
