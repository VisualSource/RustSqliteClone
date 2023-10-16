use std::{env, ffi::OsString};

use crate::errors::{DBError, DatabaseError};

#[derive(Debug)]
pub struct Config {
    pub tcp: bool,
    pub address: OsString,
    pub port: OsString,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tcp: true,
            address: OsString::from("127.0.0.1"),
            port: OsString::from("80"),
        }
    }
}

pub fn parse_args() -> DBError<Config> {
    let mut config = Config::default();
    let mut args = env::args_os();

    while let Some(arg) = args.next() {
        match arg.as_os_str().to_str().expect("Failed to parse argument") {
            "--host" => {
                let value = args.next().ok_or_else(|| DatabaseError::Argument)?;
                config.address = value;
            }
            "--port" => {
                let value = args.next().ok_or_else(|| DatabaseError::Argument)?;
                config.port = value;
            }
            "--repl" => {
                config.tcp = false;
            }
            _ => {}
        }
    }

    Ok(config)
}
