use crate::commands::{execute, prepare};
use crate::structure::Value;
use crate::DB;
use std::{io::Read, io::Write, net::TcpStream, sync, vec};

fn create_response(value: String, status: &'static str, content_type: &'static str) -> String {
    format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n{}",
        status,
        content_type,
        value.len(),
        value
    )
}

fn run(db: sync::Arc<DB>, request: &String) -> String {
    match prepare::prepare_statement(&request) {
        Ok(statement) => match db.lock() {
            Ok(mut lock) => match execute::execute_statement(&statement, &mut lock) {
                Ok(result) => match result {
                    Some(v) => {
                        let json = v
                            .iter()
                            .map(|x| {
                                let row = x
                                    .iter()
                                    .map(|i| match i {
                                        Value::Null => "null".into(),
                                        Value::String(p) => format!("\"{}\"", p),
                                        Value::UInt(p) => format!("{}", p),
                                        Value::UInt64(p) => format!("{}", p),
                                    })
                                    .collect::<Vec<String>>()
                                    .join(",");
                                format!("[{}]", row)
                            })
                            .collect::<Vec<String>>()
                            .join(",");

                        create_response(format!("[{}]", json), "200 Ok", "application/json")
                    }
                    None => {
                        create_response("{\"status\":\"Ok\"}".into(), "200 Ok", "application/json")
                    }
                },
                Err(err) => create_response(err.to_string(), "400 Bad Request", "plain/text"),
            },
            Err(_err) => create_response(
                "Failed to get database".to_string(),
                "500 Internal Server Error",
                "plain/text",
            ),
        },
        Err(err) => create_response(err.to_string(), "400 Bad Request", "plain/text"),
    }
}

pub fn handle_stream(mut stream: TcpStream, db: sync::Arc<DB>) {
    // read first 1024 bytes
    let mut buffer = [0; 1024];
    stream.read(&mut buffer).expect("Failed to read");

    if !buffer.starts_with(b"POST / HTTP/1.1\r\n") {
        stream
            .write_all("HTTP/1.1 400 BAD REQUEST\r\n\r\n".as_bytes())
            .expect("Faied to write response");
        stream.flush().expect("Failed to flush");
        return;
    }

    let request = std::str::from_utf8(&buffer).expect("Failed to convert");

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

    let res = run(db, &body.to_string());

    stream
        .write_all(res.as_bytes())
        .expect("Faied to write response");
    stream.flush().expect("Failed to flush");
}
