use serde::{Deserialize, Serialize};

use crate::engine::structure::Value;

use self::interperter::ColumnData;

pub mod error;
pub mod interperter;
pub mod tokenizer;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
pub enum Ordering {
    Asc,
    Desc,
}

impl Default for Ordering {
    fn default() -> Self {
        Self::Asc
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub nullable: bool,
    pub data_type: u8,
    pub autoincrement: bool,
    pub ordering: Ordering,
    pub default_value: Option<String>,
}

impl ColumnDef {
    pub fn new(
        name: String,
        nullable: bool,
        data_type: u8,
        autoincrement: bool,
        ordering: Ordering,
        default_value: Option<String>,
    ) -> Self {
        Self {
            name,
            nullable,
            data_type,
            autoincrement,
            ordering,
            default_value,
        }
    }

    pub fn get_default_value(&self) -> Value {
        Value::Null
    }
}

#[derive(Debug, PartialEq)]
pub enum Statement {
    /// insert into {TABLE} {COLLUMN-NAME?(,)} VALUES (expr?(,))
    Insert {
        cols: Vec<String>,
        data: Vec<ColumnData>,
        table: String,
    },
    Select {
        table: String,
        columns: Vec<String>,
    },
    Create {
        primary_key: usize,
        table: String,
        cols: Vec<ColumnDef>,
    },
}
