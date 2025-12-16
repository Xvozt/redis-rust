#![allow(unused_imports)]
use std::{
    collections::HashMap,
    fmt::format,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

use codecrafters_redis::RedisServer;
use codecrafters_redis::Storage;
use codecrafters_redis::{handle_command, server};
use codecrafters_redis::{ParseResult, RespParser, RespValue};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    let server = RedisServer::new("127.0.0.1:6379");
    if let Err(e) = server.run() {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}
