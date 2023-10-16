use crate::{Record, Table};
use std::collections::BTreeMap;

use crate::errors::DBError;

pub fn run_meta_command(
    buffer: &String,
    db: &mut BTreeMap<String, (Table, Vec<Record>)>,
) -> DBError<()> {
    // remove \r
    let input: String = buffer.trim().chars().filter(|x| !x.is_control()).collect();

    match input.as_str() {
        ".exit" => std::process::exit(0),
        ".show tables" => {
            if db.is_empty() {
                println!("No tables found");
            } else {
                db.iter().for_each(|(_key, (info, _))| println!("{}", info));
            }
        }
        _ => {
            println!("Unknown Command: {}", input.escape_debug());
        }
    }

    Ok(())
}
