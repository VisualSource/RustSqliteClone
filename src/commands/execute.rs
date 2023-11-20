use crate::engine::structure::Record;
use crate::engine::{btree::BTreeBuilder, node_type::Schema};
use crate::errors::Error;
use crate::sql::Statement;
use log::info;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub struct LockTable {
    locks: std::collections::HashMap<String, RwLock<()>>,
}

impl LockTable {
    pub fn new(locks: HashMap<String, RwLock<()>>) -> Self {
        Self { locks }
    }

    pub fn get_lock(&self, table: &String) -> Result<(&RwLock<()>, PathBuf), Error> {
        let table_name = table.to_lowercase().replace(" ", "_");
        let table_path = PathBuf::from(format!("./db/{}/table", table_name));

        if !table_path.exists() {
            return Err(Error::Unexpexted("Table does not exists"));
        }

        match self.locks.get(table) {
            Some(lock) => Ok((lock, table_path)),
            None => Err(Error::Unexpexted("No table was found.")),
        }
    }

    pub fn add_lock(&mut self, table: String) -> Result<(&RwLock<()>, PathBuf), Error> {
        let table_name = table.to_lowercase().replace(" ", "_");
        let table_path = PathBuf::from(format!("./db/{}/table", table_name));

        let key_exists = self.locks.contains_key(&table_name);
        let file_exists = table_path.exists();

        if !key_exists {
            self.locks.insert(table.clone(), RwLock::new(()));
        }

        if !file_exists {
            fs::create_dir_all(
                &table_path
                    .parent()
                    .ok_or_else(|| Error::Unexpexted("Failed to get parent path."))?,
            )?;
        }

        match self.locks.get(&table_name) {
            Some(lock) => Ok((lock, table_path)),
            None => Err(Error::Unexpexted("Table does not exists.")),
        }
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
            let table_lock = lock_table.read().map_err(|e| Error::Lock(e.to_string()))?;

            let (lock, table_path) = table_lock.get_lock(table)?;

            let mut db = BTreeBuilder::new()
                .b_parameter(10)
                .cursor_offset(256)
                .path(PathBuf::from(table_path))
                .build()?;

            if let Ok(_) = lock.write() {
                let schema = db.get_table()?;

                let value = Record::create_from(cols, data, &schema)?;

                db.insert(value)?;

                return Ok(None);
            }

            Err(Error::Lock("Failed to lock.".into()))
        }
        Statement::Select {
            table,
            columns,
            target,
        } => {
            let table_lock = lock_table.read().map_err(|e| Error::Lock(e.to_string()))?;

            let (lock, table_path) = table_lock.get_lock(table)?;

            let mut db = BTreeBuilder::new()
                .b_parameter(10)
                .cursor_offset(256)
                .path(PathBuf::from(table_path))
                .build()?;

            if let Ok(_) = lock.read() {
                let results = db.select(columns, None)?;
                return Ok(Some(results));
            }

            Ok(None)
        }
        Statement::Create {
            table,
            cols,
            primary_key,
        } => {
            if let Ok(mut handler) = lock_table.write() {
                if let Ok((lock, table_path)) = handler.add_lock(table.to_string()) {
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

                        return Ok(None);
                    }
                }
            }

            Err(Error::Unexpexted("Failed to create table."))
        }
        Statement::Delete { table, target } => {
            let table_lock = lock_table.read().map_err(|e| Error::Lock(e.to_string()))?;

            let (lock, table_path) = table_lock.get_lock(table)?;

            let mut db = BTreeBuilder::new()
                .b_parameter(10)
                .cursor_offset(256)
                .path(PathBuf::from(table_path))
                .build()?;

            if let Ok(_) = lock.write() {
                db.delete(Some(target))?;
            }

            Ok(None)
        }
        Statement::Update {
            table,
            columns,
            target,
        } => {
            let table_lock = lock_table.read().map_err(|e| Error::Lock(e.to_string()))?;

            let (lock, table_path) = table_lock.get_lock(table)?;

            let mut db = BTreeBuilder::new()
                .b_parameter(10)
                .cursor_offset(256)
                .path(PathBuf::from(table_path))
                .build()?;

            if let Ok(_) = lock.write() {
                db.update(columns, target)?;
            }

            Ok(None)
        }
        Statement::DropTable { table } => {
            if let Ok(mut lock) = lock_table.write() {
                lock.remove_lock(table.to_owned())?;
                info!("Dropped Table '{}'", table);
                return Ok(None);
            }

            Err(Error::Unexpexted("Failed to lock."))
        }
    }
}
