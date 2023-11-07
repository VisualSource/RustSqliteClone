use super::structure::{Offset, Record, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeType {
    Schema(Vec<Offset>, Vec<Value>),
    Leaf(Vec<Record>),
    Unexpected,
}

impl From<u8> for NodeType {
    fn from(value: u8) -> Self {
        match value {
            0x01 => NodeType::Schema(Vec::<Offset>::new(), Vec::<Value>::new()),
            0x02 => NodeType::Leaf(Vec::<Record>::new()),
            _ => NodeType::Unexpected,
        }
    }
}

impl From<&NodeType> for u8 {
    fn from(value: &NodeType) -> Self {
        match value {
            NodeType::Schema(_, _) => 0x01,
            NodeType::Leaf(_) => 0x02,
            NodeType::Unexpected => 0x03,
        }
    }
}
