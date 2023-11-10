use crate::errors::Error;

pub fn run_meta_command(buffer: &String) -> Result<(), Error> {
    // remove \r
    let input: String = buffer.trim().chars().filter(|x| !x.is_control()).collect();

    match input.as_str() {
        ".exit" => std::process::exit(0),
        ".show tables" => {}
        _ => {
            println!("Unknown Command: {}", input.escape_debug());
        }
    }

    Ok(())
}
