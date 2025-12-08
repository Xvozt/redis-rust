#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                let mut buffer = [0; 512];

                loop {
                    match stream.read(&mut buffer) {
                        Ok(0) => {
                            println!("connection closed");
                            break;
                        }
                        Ok(n) => {
                            let received = String::from_utf8(Vec::from(&buffer[..n])).unwrap();
                            println!("received: {}", received);

                            if let Err(e) = stream.write_all("+PONG\r\n".as_bytes()) {
                                println!("failed to write: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            println!("Error reading stream: {}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
