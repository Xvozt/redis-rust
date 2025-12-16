use crate::RespValue;
use crate::Storage;

pub fn handle_command(value: &RespValue, storage: &Storage) -> String {
    match value {
        RespValue::Array(Some(elements)) if !elements.is_empty() => {
            let command = extract_command_name(&elements[0]);

            match command.as_str() {
                "PING" => handle_ping(elements),
                "ECHO" => handle_echo(elements),
                "SET" => {
                    // TODO: i stopped here
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

                    storage.set(key, value);

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

                    match storage.get(&key) {
                        Some(v) => format!("${}\r\n{}\r\n", v.len(), String::from_utf8_lossy(&v)),
                        None => "$-1\r\n".to_string(),
                    }
                }
                _ => format!("-ERR unknown command: '{}'\r\n", command),
            }
        }
        _ => "-ERR Invalid command format \r\n".to_string(),
    }
}

fn handle_ping(_elements: &[RespValue]) -> String {
    "+PONG\r\n".to_string()
}

fn handle_echo(elements: &[RespValue]) -> String {
    if elements.len() < 2 {
        return "-ERR wrong number of arguments for 'echo' command\r\n".to_string();
    }
    match &elements[1] {
        RespValue::BulkString(Some(msg)) => {
            return format!("${}\r\n{}\r\n", msg.len(), String::from_utf8_lossy(&msg))
        }
        RespValue::SimpleString(msg) => return format!("${}\r\n{}\r\n", msg.len(), msg),
        _ => "-ERR invalid argument type\r\n".to_string(),
    }
}

fn extract_command_name(value: &RespValue) -> String {
    match value {
        RespValue::BulkString(Some(cmd)) => String::from_utf8_lossy(cmd).to_uppercase(),
        RespValue::SimpleString(cmd) => cmd.to_uppercase(),
        _ => String::new(),
    }
}
