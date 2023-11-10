use super::{
    error::Error,
    structure::{Offset, Record, Value},
};
use serde::{Deserialize, Serialize};

type ColumnDefintion = (String, u8, bool);
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Schema {
    pub name: String,
    pub primary_key: usize,
    // (Column Name, Data Type, Nullable)
    pub columns: Vec<ColumnDefintion>,
    child_offset: Option<usize>,
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
        columns: Vec<ColumnDefintion>,
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
    pub fn validate_record(&self, record: Record) -> Result<(), Error> {
        let schema_len = self.column_len();

        if record.len() != schema_len {
            return Err(Error::UnexpectedWithReason("Column does not match schema"));
        }

        for x in 0..schema_len {
            let item = record.0.get(x).ok_or_else(|| Error::Unexpected)?;
            let col = self.columns.get(x).ok_or_else(|| Error::Unexpected)?;

            if !item.is_type(col.1, col.2) {
                return Err(Error::Validate(format!(
                    "value for column \"{}\" is not of type {}",
                    col.0,
                    Value::print_type(col.1)
                )));
            }
        }

        Ok(())
    }

    pub fn get_column_by_name(&self, column: String) -> Option<&ColumnDefintion> {
        self.columns.iter().find(|x| x.0 == column)
    }
    pub fn column_len(&self) -> usize {
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
