mod commands;
mod controllers;
mod engine;
mod errors;
#[macro_use]
mod sql;

use commands::args;
use controllers::{cli_handler::handle_cli, tcp_handler::hanlde_tcp};
use std::io;

fn main() -> io::Result<()> {
    let config = args::parse_args()
        .map_err(|x| io::Error::new(io::ErrorKind::InvalidInput, x.to_string()))?;

    // read table route defs

    if config.tcp {
        return hanlde_tcp(
            config.address.to_str().expect("Failed to convert cow"),
            config.port.to_str().expect("Failed to convert cow"),
        )
        .map_err(|x| {
            eprintln!("{}", x);
            io::Error::new(io::ErrorKind::Other, x.to_string())
        });
    }

    handle_cli().map_err(|x| {
        eprintln!("{}", x);
        io::Error::new(io::ErrorKind::Other, x.to_string())
    })
}
