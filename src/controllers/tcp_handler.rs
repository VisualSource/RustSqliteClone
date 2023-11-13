use crate::commands::execute::execute_statement;
use crate::commands::prepare;
use crate::engine::structure::Record;
use crate::errors::Error;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

fn create_response(value: String, status: &'static str, content_type: &'static str) -> String {
    format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n{}",
        status,
        content_type,
        value.len(),
        value
    )
}

fn handle_request(value: &String, lock: Arc<Mutex<i32>>) -> Result<Option<String>, Error> {
    let statement = prepare::prepare_statement(&value)?;

    let result = { execute_statement(&statement)? };

    match result {
        Some(data) => {
            let content = serde_json::to_string::<Vec<Record>>(&data)?;
            Ok(Some(content))
        }
        None => Ok(None),
    }
}

fn handle_stream(mut stream: TcpStream, lock: Arc<Mutex<i32>>) -> Result<(), Error> {
    let mut buffer = [0; 1024];
    stream.read(&mut buffer)?;

    if !buffer.starts_with(b"POST / HTTP/1.1\r\n") {
        stream
            .write_all("HTTP/1.1 400 BAD REQUEST\r\n\r\n".as_bytes())
            .expect("Faied to write response");
        stream.flush().expect("Failed to flush");
        return Ok(());
    }

    let request = std::str::from_utf8(&buffer)?;

    let mut headers = vec![];
    let mut body_start = false;
    let mut body = "";
    request.split("\r\n").for_each(|x| {
        if body_start {
            body = x;
        } else {
            if x.is_empty() {
                body_start = true;
            } else {
                headers.push(x);
            }
        }
    });

    let request_body = body.to_string();

    let res = match handle_request(&request_body, lock) {
        Ok(result) => match result {
            Some(v) => create_response(v, "200", "application/json"),
            None => create_response("[]".to_string(), "200", "application/json"),
        },
        Err(err) => create_response(
            format!("{{\"error\":\"{}\"}}", err.to_string()),
            "400",
            "application/json",
        ),
    };

    stream.write_all(res.as_bytes())?;
    stream.flush()?;

    Ok(())
}

pub fn hanlde_tcp<T>(address: T, port: T) -> Result<(), Error>
where
    T: std::string::ToString,
{
    let config_address = address.to_string();
    let config_port = port.to_string();

    let listener = TcpListener::bind(format!("{}:{}", config_address, config_port))?;

    println!("Running on {}:{}", config_address, config_port);

    let exec_lock = Arc::new(Mutex::new(0));

    for s in listener.incoming() {
        let stream = s.expect("Failed to get tcp stream");

        let lock = exec_lock.clone();

        std::thread::spawn(move || handle_stream(stream, lock));
    }

    Ok(())
}
