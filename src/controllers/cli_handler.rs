use crate::{
    commands::{
        execute::{execute_statement, AccessLockTable},
        meta::{self, get_table_locks},
        prepare,
    },
    errors::Error,
};
use log::{Level, Metadata, Record};
use std::{
    io::{stdin, stdout, Write},
    sync::{Arc, RwLock},
};

struct CliLogger;

impl log::Log for CliLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{}", record.args())
        }
    }

    fn flush(&self) {
        stdout().flush().expect("Failed to flush output!")
    }
}

fn run_request(value: &String, lock_table: AccessLockTable) -> Result<(), Error> {
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

static LOGGER: CliLogger = CliLogger;

pub fn handle_cli() -> Result<(), Error> {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(log::LevelFilter::Info))
        .map_err(|_| Error::Logger("Failed to set logger."))?;

    let mut input = String::new();

    let lock_table: AccessLockTable = Arc::new(RwLock::new(get_table_locks("./db")?));

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
