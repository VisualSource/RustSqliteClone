use crate::engine::structure::Record;
use crate::engine::{btree::BTreeBuilder, node_type::Schema};
use crate::errors::Error;
use crate::sql::Statement;
use std::fs;
use std::path::PathBuf;

pub fn execute_statement(statement: &Statement) -> Result<Option<Vec<Record>>, Error> {
    match statement {
        Statement::Insert { cols, data, table } => {
            let path = format!("./db/{}/table", table.to_lowercase().replace(" ", "_"));
            let mut db = BTreeBuilder::new()
                .b_parameter(10)
                .path(PathBuf::from(path))
                .build()?;

            db.set_file_cursor(256);

            let schema = db.get_table()?;

            let value = Record::create_from(cols, data, &schema)?;

            db.insert(value)?;

            Ok(None)
        }
        Statement::Select { table, columns } => {
            let path = format!("./db/{}/table", table.to_lowercase().replace(" ", "_"));
            let mut db = BTreeBuilder::new()
                .b_parameter(10)
                .path(PathBuf::from(path))
                .build()?;

            db.set_file_cursor(256);

            let results = db.select(columns, None)?;

            Ok(Some(results))
        }
        Statement::Create {
            table,
            cols,
            primary_key,
        } => {
            let table_name = table.to_lowercase().replace(" ", "_");

            let path = PathBuf::from(format!("./db/{}", table_name));

            if !path.exists() {
                fs::create_dir_all(&path)?;
            }

            let file_name = path.join("/table");

            if file_name.is_file() {
                return Err(Error::Unexpexted("Table already exists"));
            }

            let mut db = BTreeBuilder::new().b_parameter(10).path(path).build()?;

            let schema = Schema::new(
                table.to_owned(),
                primary_key.to_owned(),
                cols.to_owned(),
                None,
            );

            db.create_table(schema)?;

            Ok(None)
        }
        Statement::Delete { table, target } => todo!(),
        Statement::Update {
            table,
            columns,
            data,
            target,
        } => todo!(),
        Statement::DropTable { table } => {
            let table_name = table.to_lowercase().replace(" ", "_");
            let path = PathBuf::from(format!("./db/{}", table_name));

            if path.exists() {
                fs::remove_dir_all(path)?;
            }

            Ok(None)
        }
    }
}
