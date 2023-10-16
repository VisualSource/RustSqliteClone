use super::Statement;
use crate::errors::{DBError, DatabaseError};
use crate::structure::{DataType, Record, Table, Value};
use std::collections::BTreeMap;

// id | username | email

pub fn execute_statement(
    statement: &Statement,
    db: &mut BTreeMap<String, (Table, Vec<Record>)>,
) -> DBError<Option<Vec<Vec<Value>>>> {
    match statement {
        Statement::Insert { cols, data, table } => {
            let (table_info, rows) = db.get_mut(table).ok_or_else(|| {
                DatabaseError::Execution(format!("There is no table with the name '{}'.", table))
            })?;

            if cols.len() == 0 {
                let mut row: Vec<Value> = Vec::with_capacity(table_info.cols.len());

                if table_info.cols.len() != data.len() {
                    return Err(DatabaseError::Execution(format!(
                        "The number of colums does not match the number of data given. ({}) ({})",
                        table_info.cols.len(),
                        data.len()
                    )));
                }

                for (idx, col) in table_info.cols.iter().enumerate() {
                    if let Some(data_value) = data.get(idx) {
                        match col.data_type {
                            DataType::UINT => {
                                let value = data_value.parse::<usize>()?;
                                row.push(Value::UInt(value));
                            }
                            DataType::UINT64 => {
                                let value = data_value.parse::<u64>()?;
                                row.push(Value::UInt64(value));
                            }
                            DataType::String => row.push(Value::String(data_value.to_owned())),
                            DataType::NULL => row.push(Value::Null),
                        }
                    } else {
                        return Err(DatabaseError::Execution(format!(
                            "Missing data row at {}",
                            idx
                        )));
                    }
                }

                rows.push(Record(row));

                return Ok(None);
            }

            if cols.len() != data.len() {
                return Err(DatabaseError::Execution(format!(
                    "The number of colums does not match the number of data given. ({}) ({})",
                    cols.len(),
                    data.len()
                )));
            }

            let mut row: Vec<Value> = Vec::with_capacity(table_info.cols.len());

            for col in &table_info.cols {
                if !cols.contains(&col.name) {
                    row.push(col.data_type.get_default());
                }

                let idx = cols
                    .iter()
                    .enumerate()
                    .find_map(|(idx, val)| if val == &col.name { Some(idx) } else { None })
                    .ok_or_else(|| {
                        DatabaseError::Execution(format!("No column with name '{}'.", col.name))
                    })?;

                if let Some(data_value) = data.get(idx) {
                    match col.data_type {
                        DataType::UINT => {
                            let value = data_value.parse::<usize>()?;
                            row.push(Value::UInt(value));
                        }
                        DataType::UINT64 => {
                            let value = data_value.parse::<u64>()?;
                            row.push(Value::UInt64(value));
                        }
                        DataType::String => row.push(Value::String(data_value.to_owned())),
                        DataType::NULL => row.push(Value::Null),
                    }
                } else {
                    return Err(DatabaseError::Execution(format!(
                        "Missing data row at {}",
                        idx
                    )));
                }
            }

            rows.push(Record(row));

            Ok(None)
        }
        Statement::Create { table, cols } => {
            db.insert(
                table.to_owned(),
                (
                    Table {
                        table: table.to_owned(),
                        cols: cols.to_vec(),
                    },
                    Vec::default(),
                ),
            );

            return Ok(None);
        }
        Statement::Select { table, columns } => {
            let (table_info, rows) = db.get(table).ok_or_else(|| {
                DatabaseError::Execution(format!("There is no table with the name '{}'.", table))
            })?;

            if rows.is_empty() {
                return Ok(Some(vec![]));
            }

            let cols = if columns.is_empty() {
                (0..table_info.cols.len())
                    .map(|x| x)
                    .collect::<Vec<usize>>()
            } else {
                table_info
                    .cols
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, col)| {
                        if columns.contains(&col.name) {
                            return Some(idx);
                        }
                        None
                    })
                    .collect::<Vec<usize>>()
            };

            Ok(Some(
                rows.iter()
                    .map(|x| x.get_with(&cols))
                    .collect::<Vec<Vec<Value>>>(),
            ))
        }
    }
}
