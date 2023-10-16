mod commands;
mod errors;
mod sql;
mod structure;
mod tcp;

use commands::{args, execute, meta, prepare};
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use structure::{Record, Table};

pub type DB = Mutex<BTreeMap<String, (Table, Vec<Record>)>>;

fn main() -> io::Result<()> {
    let config = args::parse_args()
        .map_err(|x| io::Error::new(io::ErrorKind::InvalidInput, x.to_string()))?;

    let db = Arc::new(Mutex::new(BTreeMap::<String, (Table, Vec<Record>)>::new()));

    if config.tcp {
        let listener = std::net::TcpListener::bind(format!(
            "{}:{}",
            config.address.to_str().expect("Failed to parse address"),
            config.port.to_str().expect("Failed to parse port")
        ))
        .expect("Failed to create tcp bind.");

        println!(
            "Running on {}:{}",
            config.address.to_str().expect("Err"),
            config.port.to_str().expect("Err")
        );

        for s in listener.incoming() {
            let stream = s.expect("Failed to get tcp stream");

            let db_ref = db.clone();

            std::thread::spawn(move || tcp::handle_stream(stream, db_ref));
        }

        return Ok(());
    }

    let mut input_buf = String::new();
    loop {
        input_buf.clear();
        print!("> ");
        io::stdout().flush().expect("Failed to flush.");
        match io::stdin().read_line(&mut input_buf) {
            Ok(_) => {
                if input_buf.starts_with(".") {
                    if let Ok(mut lock) = db.lock() {
                        if let Err(err) = meta::run_meta_command(&input_buf, &mut lock) {
                            eprintln!("{}", err);
                        }
                    } else {
                        eprintln!("Failed to lock db");
                    }
                    continue;
                }
                match prepare::prepare_statement(&input_buf) {
                    Ok(statement) => {
                        if let Ok(mut lck) = db.lock() {
                            match execute::execute_statement(&statement, &mut lck) {
                                Ok(result) => match result {
                                    Some(value) => {
                                        for x in value {
                                            for y in x {
                                                print!(" {} |", y);
                                            }
                                            println!();
                                        }
                                    }
                                    None => println!("Ok"),
                                },
                                Err(err) => eprintln!("{}", err),
                            }
                        } else {
                            eprintln!("Failed to lock db");
                        }
                    }
                    Err(err) => eprintln!("{}", err),
                }
            }
            Err(err) => eprintln!("{}", err),
        }
    }
}
