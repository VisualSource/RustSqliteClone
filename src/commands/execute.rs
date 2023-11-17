use crate::engine::structure::Record;
use crate::engine::{btree::BTreeBuilder, node_type::Schema};
use crate::errors::Error;
use crate::sql::Statement;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub struct LockTable {
    locks: std::collections::HashMap<String, RwLock<()>>,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            locks: std::collections::HashMap::new(),
        }
    }

    pub fn get_lock(&self, table: &String) -> Result<(&RwLock<()>, PathBuf), Error> {
        let table_name = table.to_lowercase().replace(" ", "_");
        let table_path = PathBuf::from(format!("./db/{}/table", table_name));

        if !table_path.exists() {
            return Err(Error::Unexpexted("Table does not exists"));
        }

        match self.locks.get(table) {
            Some(lock) => Ok((lock, table_path)),
            None => Err(Error::Unexpexted("Table does not exists.")),
        }
    }

    pub fn add_lock(&mut self, table: String) -> Result<(), Error> {
        let table_name = table.to_lowercase().replace(" ", "_");
        let table_path = PathBuf::from(format!("./db/{}/table", table_name));

        let key_exists = self.locks.contains_key(&table_name);
        let file_exists = table_path.exists();

        if file_exists && key_exists {
            return Ok(());
        }

        if !key_exists {
            self.locks.insert(table.clone(), RwLock::new(()));
        }

        if !file_exists {
            fs::create_dir_all(&table_path)?;
        }

        Ok(())
    }

    pub fn remove_lock(&mut self, table: String) -> Result<(), Error> {
        let table_name = table.to_lowercase().replace(" ", "_");
        let table_path = PathBuf::from(format!("./db/{}/table", table_name));

        let key_exists = self.locks.contains_key(&table_name);
        let file_exists = table_path.exists();

        if key_exists {
            self.locks.remove(&table_name);
        }

        if file_exists {
            fs::remove_dir_all(
                table_path
                    .parent()
                    .ok_or_else(|| Error::Unexpexted("Failed to get path parent"))?,
            )?;
        }

        Ok(())
    }
}

pub type AccessLockTable = Arc<RwLock<LockTable>>;

pub fn execute_statement(
    statement: &Statement,
    lock_table: AccessLockTable,
) -> Result<Option<Vec<Record>>, Error> {
    match statement {
        Statement::Insert { cols, data, table } => {
            if let Ok((lock, table_path)) = lock_table
                .read()
                .map_err(|_| Error::Unexpexted("Failed to lock"))?
                .get_lock(table)
            {
                let mut db = BTreeBuilder::new()
                    .b_parameter(10)
                    .cursor_offset(256)
                    .path(PathBuf::from(table_path))
                    .build()?;

                if let Ok(_) = lock
                    .write()
                    .map_err(|_| Error::Unexpexted("Failed to lock"))
                {
                    let schema = db.get_table()?;

                    let value = Record::create_from(cols, data, &schema)?;

                    db.insert(value)?;
                }

                return Ok(None);
            }

            Err(Error::Unexpexted("Failed to lock table"))
        }
        Statement::Select { table, columns } => {
            if let Ok((lock, table_path)) = lock_table
                .read()
                .map_err(|_| Error::Unexpexted("Failed to lock"))?
                .get_lock(table)
            {
                let mut db = BTreeBuilder::new()
                    .b_parameter(10)
                    .cursor_offset(256)
                    .path(PathBuf::from(table_path))
                    .build()?;

                if let Ok(_) = lock.read() {
                    let results = db.select(columns, None)?;
                    return Ok(Some(results));
                }
            }

            Err(Error::Unexpexted("Failed to lock table"))
        }
        Statement::Create {
            table,
            cols,
            primary_key,
        } => {
            if let Ok(mut handler) = lock_table.write() {
                handler.add_lock(table.clone())?;

                if let Ok((lock, table_path)) = handler.get_lock(table) {
                    let mut db = BTreeBuilder::new()
                        .b_parameter(10)
                        .path(table_path)
                        .build()?;

                    if let Ok(_) = lock.write() {
                        let schema = Schema::new(
                            table.to_owned(),
                            primary_key.to_owned(),
                            cols.to_owned(),
                            None,
                        );

                        db.create_table(schema)?;
                    }
                }
            }

            Err(Error::Unexpexted("Failed to create table."))
        }
        Statement::Delete { table, target } => todo!(),
        Statement::Update {
            table,
            columns,
            data,
            target,
        } => todo!(),
        Statement::DropTable { table } => {
            lock_table
                .write()
                .map_err(|_| Error::Unexpexted("Failed to lock"))?
                .remove_lock(table.to_owned())?;

            Err(Error::Unexpexted("Failed to lock table."))
        }
    }
}
