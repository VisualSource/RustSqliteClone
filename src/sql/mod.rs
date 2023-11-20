use serde::{Deserialize, Serialize};

use crate::engine::structure::Value;

use self::interperter::ColumnData;

pub mod error;
pub mod interperter;
pub mod tokenizer;

#[macro_export]
macro_rules! sql {
    ($query:tt) => {
        $crate::sql::_query($query)
    };
}

#[doc(hidden)]
pub fn _query<T: Into<String>>(value: T) -> Vec<tokenizer::Token> {
    let input = value.into();
    tokenizer::tokenizer(&input).expect("Failed to parse query")
}

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

#[derive(Debug, PartialEq)]
pub enum Condition {
    E(String, String),
    GT(String, String),
    LT(String, String),
    GTE(String, String),
    LTE(String, String),
    NE(String, String),
    NOT,
    BETWEEN(String, String, String),
    LIKE(String, String),
    AND,
    OR,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub nullable: bool,
    pub data_type: u8,
    pub unique: bool,
    pub autoincrement: bool,
    pub ordering: Ordering,
    pub default_value: Option<String>,
}

impl ColumnDef {
    pub fn new(
        name: String,
        nullable: bool,
        unique: bool,
        data_type: u8,
        autoincrement: bool,
        ordering: Ordering,
        default_value: Option<String>,
    ) -> Self {
        Self {
            unique,
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
        target: Option<Vec<Condition>>,
    },
    Create {
        primary_key: usize,
        table: String,
        cols: Vec<ColumnDef>,
    },
    Delete {
        table: String,
        target: Vec<Condition>,
    },
    Update {
        table: String,
        columns: Vec<(String, ColumnData)>,
        target: Option<Vec<Condition>>,
    },
    DropTable {
        table: String,
    },
}
