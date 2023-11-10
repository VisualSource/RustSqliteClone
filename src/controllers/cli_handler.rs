use crate::{
    commands::{execute, meta, prepare},
    errors::Error,
};
use std::io::{stdin, stdout, Write};

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

        let statement = prepare::prepare_statement(&input)?;

        match execute::execute_statement(&statement) {
            Ok(result) => match result {
                Some(v) => {
                    println!("{:?}", v);
                }
                None => println!(""),
            },
            Err(err) => eprintln!("{}", err),
        }
    }
}
