use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use crate::{
    command::handle_command,
    parser::{ParseResult, RespParser},
    storage::Storage,
};

pub struct RedisServer {
    addr: String,
    storage: Storage,
}

impl RedisServer {
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            storage: Storage::new(),
        }
    }

    pub fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.addr)?;
        println!("Redis server listening on {}", self.addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let storage_clone = self.storage.clone();
                    thread::spawn(move || {
                        handle_connection(stream, storage_clone);
                    });
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }

        Ok(())
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
