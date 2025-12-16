#![allow(unused_imports)]
use std::{
    collections::HashMap,
    fmt::format,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

use codecrafters_redis::{ParseResult, RespParser, RespValue};
use tokio::task::spawn_blocking;

type Storage = Arc<Mutex<HashMap<String, Vec<u8>>>>;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let storage: Storage = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let storage_clone = Arc::clone(&storage);
                thread::spawn(|| {
                    handle_connection(stream, storage_clone);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream, storage: Storage) -> () {
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
                            let response = handle_command(&value, &storage);
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

fn handle_command(value: &RespValue, storage: &Storage) -> String {
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
                "SET" => {
                    if elements.len() < 3 {
                        return "-ERR wrong number of arguments for 'SET' commang\r\n".to_string();
                    }
                    let key = match &elements[1] {
                        RespValue::BulkString(Some(k)) => String::from_utf8_lossy(k).to_string(),
                        RespValue::SimpleString(s) => s.clone(),
                        _ => return "-ERR Invalid key type".to_string(),
                    };

                    let value = match &elements[2] {
                        RespValue::BulkString(Some(v)) => v.clone(),
                        RespValue::SimpleString(v) => v.as_bytes().to_vec(),
                        _ => return "-ERR Invalid value type\r\n".to_string(),
                    };

                    let mut store = storage.lock().unwrap();
                    store.insert(key, value);

                    "+OK\r\n".to_string()
                }
                "GET" => {
                    if elements.len() < 2 {
                        return "-ERR wrong number of arguments for 'GET' command\r\n".to_string();
                    }

                    let key = match &elements[1] {
                        RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                        RespValue::SimpleString(s) => s.clone(),
                        _ => return "-ERR Invalid key type\r\n".to_string(),
                    };

                    let store = storage.lock().unwrap();
                    match store.get(&key) {
                        Some(v) => format!("${}\r\n{}\r\n", v.len(), String::from_utf8_lossy(v)),
                        None => "$-1\r\n".to_string(),
                    }
                }
                _ => format!("-ERR unknown command: '{}'\r\n", command),
            }
        }
        _ => "-ERR Invalid command format \r\n".to_string(),
    }
}
