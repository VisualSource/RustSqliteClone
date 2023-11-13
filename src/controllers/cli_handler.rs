use crate::{
    commands::{execute, meta, prepare},
    errors::Error,
};
use std::io::{stdin, stdout, Write};

fn run_request(value: &String) -> Result<(), Error> {
    let statement = prepare::prepare_statement(&value)?;

    let result = execute::execute_statement(&statement)?;

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

        if let Err(e) = run_request(&input) {
            eprintln!("{}", e);
        }
    }
}
