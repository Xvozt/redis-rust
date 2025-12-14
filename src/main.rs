#![allow(unused_imports)]
use std::{
    fmt::format,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use codecrafters_redis::{ParseResult, RespParser, RespValue};
use tokio::task::spawn_blocking;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| {
                    handle_connection(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream) -> () {
    println!("accepted new connection");

    let mut parser = RespParser::new();

    let mut buffer = [0; 512];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                println!("connection closed");
                break;
            }
            Ok(n) => {
                parser.feed(&buffer[..n]);

                loop {
                    match parser.parse() {
                        ParseResult::Complete(value, consumed) => {
                            let response = handle_command(&value);
                            if let Err(e) = stream.write_all(response.as_bytes()) {
                                println!("failed to write: {}", e);
                                return;
                            }

                            parser.consume(consumed);
                        }
                        ParseResult::Incomplete => {
                            break;
                        }
                        ParseResult::Error(e) => {
                            let _ = stream.write_all(e.as_bytes());
                            return;
                        }
                    }
                }
            }
            Err(e) => {
                println!("Error reading stream: {}", e);
                break;
            }
        }
    }
}

fn handle_command(value: &RespValue) -> String {
    match value {
        RespValue::Array(Some(elements)) if !elements.is_empty() => {
            let command = match &elements[0] {
                RespValue::BulkString(Some(cmd)) => String::from_utf8_lossy(cmd).to_uppercase(),
                RespValue::SimpleString(cmd) => cmd.to_uppercase(),
                _ => return "-ERR Invalid command format\r\n".to_string(),
            };
            match command.as_str() {
                "PING" => "+PONG\r\n".to_string(),
                "ECHO" => {
                    if elements.len() < 2 {
                        return "-ERR wrong number of arguments for 'echo' command\r\n".to_string();
                    }
                    match &elements[1] {
                        RespValue::BulkString(Some(msg)) => {
                            return format!(
                                "${}\r\n{}\r\n",
                                msg.len(),
                                String::from_utf8_lossy(&msg)
                            )
                        }
                        RespValue::SimpleString(msg) => {
                            return format!("${}\r\n{}\r\n", msg.len(), msg)
                        }
                        _ => "-ERR invalid argument type\r\n".to_string(),
                    }
                }
                _ => format!("-ERR unknown command: '{}'\r\n", command),
            }
        }
        _ => "-ERR Invalid command format \r\n".to_string(),
    }
}
