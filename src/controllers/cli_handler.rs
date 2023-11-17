use crate::{
    commands::{
        execute::{execute_statement, AccessLockTable, LockTable},
        meta, prepare,
    },
    errors::Error,
};
use std::{
    collections::HashMap,
    io::{stdin, stdout, Write},
    sync::{Arc, RwLock},
};

fn run_request(value: &String, mut lock_table: AccessLockTable) -> Result<(), Error> {
    let statement = prepare::prepare_statement(&value)?;

    let result = execute_statement(&statement, lock_table)?;

    if let Some(v) = result {
        for x in v {
            for a in x.0 {
                print!("{} | ", a);
            }
            println!();
        }
    }

    Ok(())
}

pub fn handle_cli() -> Result<(), Error> {
    let mut input = String::new();

    let lock_table: AccessLockTable = Arc::new(RwLock::new(LockTable::new()));

    loop {
        input.clear();
        print!("> ");
        stdout().flush()?;

        stdin().read_line(&mut input)?;

        if input.starts_with(".") {
            if let Err(err) = meta::run_meta_command(&input) {
                eprintln!("{}", err);
            }
            continue;
        }

        let lock = lock_table.clone();

        if let Err(e) = run_request(&input, lock) {
            eprintln!("{}", e);
        }
    }
}
