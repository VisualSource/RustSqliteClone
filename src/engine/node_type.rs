use std::fmt::Display;

use crate::sql::ColumnDef;

use super::{
    error::Error,
    structure::{Offset, Record, Value},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Schema {
    pub name: String,
    pub primary_key: usize,
    // (Column Name, Data Type, Nullable)
    pub columns: Vec<ColumnDef>,
    child_offset: Option<usize>,
}

impl Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "==== {} ====", self.name)?;
        for (idx, col) in self.columns.iter().enumerate() {
            writeln!(
                f,
                "{} {}{}{}{}{}",
                col.name,
                Value::print_type(col.data_type),
                if idx == self.primary_key {
                    " PRIMARY KEY"
                } else {
                    ""
                },
                if col.nullable { "?" } else { "" },
                if col.unique && idx != self.primary_key {
                    " UNIQUE"
                } else {
                    ""
                },
                if col.autoincrement {
                    " AUTOINCREMENT"
                } else {
                    ""
                }
            )?;
        }
        write!(f, "")
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            name: String::default(),
            primary_key: 0,
            columns: vec![],
            child_offset: Some(256),
        }
    }
}

impl Schema {
    pub fn new(
        name: String,
        primary_key_index: usize,
        columns: Vec<ColumnDef>,
        child_offset: Option<usize>,
    ) -> Self {
        Self {
            name,
            primary_key: primary_key_index,
            columns: columns,
            child_offset,
        }
    }
    pub fn get_child_offset(&self) -> Option<Offset> {
        if let Some(offset) = self.child_offset {
            return Some(Offset(offset));
        }
        None
    }

    pub fn get_indexs_from_names(&self, values: &Vec<String>) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if values.contains(&col.name) {
                    return Some(idx);
                }

                None
            })
            .collect::<Vec<usize>>()
    }
    pub fn validate_record(&self, record: &Record) -> Result<(), Error> {
        let schema_len = self.len();

        if record.len() != schema_len {
            return Err(Error::UnexpectedWithReason("Column does not match schema"));
        }

        for x in 0..schema_len {
            let item = record.0.get(x).ok_or_else(|| Error::Unexpected)?;
            let col = self.columns.get(x).ok_or_else(|| Error::Unexpected)?;

            if !item.is_type(col.data_type, col.nullable) {
                return Err(Error::Validate(format!(
                    "value for column \"{}\" is not of type {}",
                    col.name,
                    Value::print_type(col.data_type)
                )));
            }
        }

        Ok(())
    }

    pub fn get_column_idx_by_name(&self, column: &String) -> Option<usize> {
        self.columns.iter().position(|x| &x.name == column)
    }
    pub fn len(&self) -> usize {
        self.columns.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeType {
    Schema(Schema),
    Internal(Vec<Offset>, Vec<Value>),
    Leaf(Vec<Record>),
    Unexpected,
}

impl From<u8> for NodeType {
    fn from(value: u8) -> Self {
        match value {
            0x01 => NodeType::Internal(Vec::<Offset>::new(), Vec::<Value>::new()),
            0x02 => NodeType::Leaf(Vec::<Record>::new()),
            0x03 => NodeType::Schema(Schema::default()),
            _ => NodeType::Unexpected,
        }
    }
}

impl From<&NodeType> for u8 {
    fn from(value: &NodeType) -> Self {
        match value {
            NodeType::Internal(_, _) => 0x01,
            NodeType::Leaf(_) => 0x02,
            NodeType::Schema(_) => 0x03,
            NodeType::Unexpected => 0x04,
        }
    }
}
