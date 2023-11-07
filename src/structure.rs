use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    UInt(usize),
    UInt64(u64),
    String(String),
    Null,
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Value::Null => write!(f, "Null"),
            Value::String(value) => write!(f, "{}", value),
            Value::UInt(i) => write!(f, "{}", i),
            Value::UInt64(i) => write!(f, "{}", i),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Eq)]
pub struct Record(pub Vec<Value>);

impl Ord for Record {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Record {
    pub fn get_with(&self, idxs: &Vec<usize>) -> Vec<Value> {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(idx, value)| {
                if idxs.contains(&idx) {
                    return Some(value.to_owned());
                }

                None
            })
            .collect::<Vec<Value>>()
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub enum DataType {
    UINT,
    UINT64,
    String,
    NULL,
}

impl DataType {
    pub fn get_default(&self) -> Value {
        match &self {
            Self::NULL => Value::Null,
            Self::String => Value::String(String::default()),
            Self::UINT => Value::UInt(0),
            Self::UINT64 => Value::UInt64(0),
        }
    }

    fn match_str(value: String) -> DataType {
        match value.as_str() {
            "uint" => Self::UINT,
            "uint64" => Self::UINT64,
            "string" => Self::String,
            _ => Self::NULL,
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let result = match self {
            Self::NULL => "null",
            Self::UINT => "uint",
            Self::String => "string",
            Self::UINT64 => "uint64",
        };

        write!(f, "{}", result)
    }
}

impl From<String> for DataType {
    fn from(value: String) -> Self {
        DataType::match_str(value.to_lowercase())
    }
}
impl From<&String> for DataType {
    fn from(value: &String) -> Self {
        DataType::match_str(value.to_lowercase())
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct Col {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Table {
    pub table: String,
    pub cols: Vec<Col>,
}
impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "==== {} ====\n{}",
            self.table,
            self.cols
                .iter()
                .map(|x| format!("{}: {}", x.name, x.data_type))
                .collect::<Vec<String>>()
                .join("|")
        )
    }
}
