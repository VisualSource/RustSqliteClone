use std::{collections::HashMap, path::Path, sync::RwLock};

use log::info;

use crate::{engine::btree::BTreeBuilder, errors::Error};

use super::execute::LockTable;

pub fn get_table_locks(file_dir: &'static str) -> Result<LockTable, Error> {
    let dirs = Path::new(file_dir).read_dir()?;

    let mut table: HashMap<String, RwLock<()>> = HashMap::new();

    for dir in dirs {
        let path = dir?.path();
        if !path.is_dir() {
            continue;
        }

        let item = path
            .iter()
            .last()
            .ok_or_else(|| Error::Unexpexted("Failed to get last el"))?;

        table.insert(item.to_string_lossy().to_string(), RwLock::new(()));
    }

    Ok(LockTable::new(table))
}

pub fn run_meta_command(buffer: &String) -> Result<(), Error> {
    // remove \r
    let input: String = buffer.trim().chars().filter(|x| !x.is_control()).collect();

    match input.as_str() {
        ".exit" => std::process::exit(0),
        ".show tables" => {
            let dirs = Path::new("./db").read_dir()?;

            for dir in dirs.into_iter() {
                let path = dir?.path().join("table");

                let mut db = BTreeBuilder::new().b_parameter(10).path(path).build()?;

                let table = db.get_table()?;

                info!("{}", table);
            }
        }
        _ => {
            println!("Unknown Command: {}", input.escape_debug());
        }
    }

    Ok(())
}
