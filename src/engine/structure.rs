use serde::{Deserialize, Serialize};

use super::{error::Error, page_layout::PTR_SIZE};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Offset(pub usize);

impl TryFrom<[u8; PTR_SIZE]> for Offset {
    type Error = Error;
    fn try_from(value: [u8; PTR_SIZE]) -> Result<Self, Self::Error> {
        Ok(Offset(usize::from_be_bytes(value)))
    }
}

pub struct Usize(pub usize);

impl TryFrom<&[u8]> for Usize {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() > PTR_SIZE {
            return Err(Error::TryFromSlice);
        }

        let mut truncated_arr = [0u8; PTR_SIZE];
        for (i, item) in value.iter().enumerate() {
            truncated_arr[i] = *item;
        }

        Ok(Usize(usize::from_be_bytes(truncated_arr)))
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, PartialOrd, Ord)]
pub enum Value {
    String(String),
    U64(u64),
    Null,
}
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq, PartialOrd, Ord)]
pub struct Record(pub Vec<Value>);
