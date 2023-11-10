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
    UInt(usize),
    Null,
}

impl Value {
    pub fn print_type(value: u8) -> &'static str {
        match value {
            0x00 => "string",
            0x01 => "uint",
            0x02 => "u64",
            _ => "null",
        }
    }

    pub fn from_string(value: &String) -> u8 {
        match value.as_str() {
            "string" => 0x00,
            "uint" => 0x01,
            "u64" => 0x02,
            _ => 0x03, // null
        }
    }

    pub fn is_type(&self, value: u8, nullable: bool) -> bool {
        match &self {
            Self::String(_) => value == 0x00,
            Self::UInt(_) => value == 0x01,
            Self::U64(_) => value == 0x02,
            Self::Null => value == 0x03 || nullable,
        }
    }
}

impl From<&Value> for u8 {
    fn from(value: &Value) -> Self {
        match value {
            Value::String(_) => 0x00,
            Value::UInt(_) => 0x01,
            Value::U64(_) => 0x02,
            Value::Null => 0x03,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq, PartialOrd, Ord)]
pub struct Record(pub Vec<Value>);

impl Record {
    pub fn as_json(&self) -> Result<String, Error> {
        serde_json::to_string(self).map_err(|x| Error::Serde(x))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get_key(&self, idx: usize) -> Result<Value, Error> {
        self.0
            .get(idx)
            .ok_or_else(|| Error::UnexpectedWithReason("Failed to get key"))
            .cloned()
    }
}
