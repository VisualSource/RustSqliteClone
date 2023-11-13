use std::fmt::Display;

use crate::sql::interperter::ColumnData;

use super::{error::Error, node_type::Schema, page_layout::PTR_SIZE};
use serde::{Deserialize, Serialize, Serializer};

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

#[derive(Debug, PartialEq, Eq, Deserialize, Clone, PartialOrd, Ord)]
pub enum Value {
    String(String),
    U64(u64),
    UInt(usize),
    Null,
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            match &self {
                Value::String(v) => serializer.serialize_str(v),
                Value::U64(v) => serializer.serialize_u64(*v),
                Value::UInt(v) => serializer.serialize_u64(*v as u64),
                _ => serializer.serialize_none(),
            }
        } else {
            match &self {
                Value::String(v) => serializer.serialize_newtype_variant("Value", 0, "String", v),
                Value::U64(v) => serializer.serialize_newtype_variant("Value", 1, "U64", v),
                Value::UInt(v) => serializer.serialize_newtype_variant("Value", 2, "Unit", v),
                Value::Null => serializer.serialize_unit_variant("Value", 3, "Null"),
            }
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Value::Null => write!(f, "{}", "null"),
            Value::String(v) => write!(f, "{}", v),
            Value::U64(v) => write!(f, "{}", v),
            Value::UInt(v) => write!(f, "{}", v),
        }
    }
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

    pub fn get_default(value: u8) -> Value {
        match value {
            0x00 => Value::String(String::default()),
            0x01 => Value::UInt(0),
            0x02 => Value::U64(0),
            _ => Value::Null,
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

    pub fn parse(value: &String, data_type: u8) -> Result<Self, Error> {
        let result = match data_type {
            0x00 => Value::String(value.to_owned()),
            0x01 => Value::UInt(value.parse::<usize>()?),
            0x02 => Value::U64(value.parse::<u64>()?),
            _ => Value::Null,
        };

        Ok(result)
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
    pub fn create_from(
        cols: &Vec<String>,
        col_data: &Vec<ColumnData>,
        schema: &Schema,
    ) -> Result<Self, Error> {
        let mut data = vec![];

        let has_specified_columns = cols.len() == 0;

        for idx in 0..schema.len() {
            let column = schema
                .columns
                .get(idx)
                .ok_or_else(|| Error::UnexpectedWithReason(""))?;

            if has_specified_columns {
                // is the current column specified
                if cols.contains(&column.name) {
                    let insert_idx = cols
                        .iter()
                        .position(|x| x == &column.name)
                        .ok_or_else(|| Error::Unexpected)?;

                    // column was specified, get value and parse
                    let column_data = match col_data
                        .get(insert_idx)
                        .ok_or_else(|| Error::UnexpectedWithReason("Failed to get column data"))?
                    {
                        ColumnData::Null => Value::Null,
                        ColumnData::Value(data) => Value::parse(data, column.data_type)?,
                    };

                    data.push(column_data);
                    continue;
                }

                // column was not specified

                // use default value if there
                if let Some(value) = &column.default_value {
                    let output = Value::parse(value, column.data_type)?;

                    data.push(output);
                    continue;
                }

                // if nullable use that
                if column.nullable {
                    data.push(Value::Null);
                    continue;
                }

                // column connot be null and does not have a default value, so return error
                return Err(Error::Validate(format!(
                    "Column '{}' was not set!",
                    column.name
                )));
            }

            let column_data = match col_data.get(idx).ok_or_else(|| {
                Error::Validate(format!("No data was set for column '{}'.", column.name))
            })? {
                ColumnData::Null => Value::Null,
                ColumnData::Value(v) => Value::parse(v, column.data_type)?,
            };

            data.push(column_data);
        }

        let record = Record(data);

        schema.validate_record(&record)?;

        Ok(record)
    }

    pub fn select_only(&self, idxs: &Vec<usize>) -> Record {
        let mut data = vec![];

        for x in 0..self.len() {
            if idxs.contains(&x) {
                data.push(self.0[x].clone())
            }
        }

        Record(data)
    }

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

#[cfg(test)]
mod tests {
    use super::Record;
    use super::Value;

    #[test]
    fn test_record() {
        let record = Record(vec![
            Value::Null,
            Value::String("Hello".into()),
            Value::U64(30),
            Value::UInt(32),
        ]);
        let json = serde_json::to_string(&record).expect("Error");

        let config = bincode::config::standard();
        let bin = bincode::serde::encode_to_vec(record, config).expect("Failed to encoded");

        println!("{} {:?}", json, bin);

        let a: (Record, usize) =
            bincode::serde::decode_from_slice(bin.as_slice(), config).expect("Failed to decoded");

        println!("{:?}", a);
    }

    #[test]
    fn test_seralize() {
        let json_null = serde_json::to_string(&Value::Null).expect("Error");
        let config = bincode::config::standard();
        let bin_null =
            bincode::serde::encode_to_vec(Value::Null, config).expect("Faiiled to bincode");

        println!(
            "{} | {:?} {}",
            json_null,
            bin_null.as_slice(),
            bin_null.len()
        );
    }

    #[test]
    fn test_deseralize() {
        let config = bincode::config::standard();

        let null_bin = vec![3];

        let bin_null: (Value, usize) =
            bincode::serde::decode_from_slice(null_bin.as_slice(), config)
                .expect("Failed to convert");

        assert_eq!(Value::Null, bin_null.0);
    }
}
