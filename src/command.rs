use crate::RespValue;
use crate::Storage;

pub fn handle_command(value: &RespValue, storage: &Storage) -> String {
    match value {
        RespValue::Array(Some(elements)) if !elements.is_empty() => {
            let command = extract_command_name(&elements[0]);

            match command.as_str() {
                "PING" => handle_ping(elements),
                "ECHO" => handle_echo(elements),
                "SET" => handle_set(elements, storage),
                "GET" => handle_get(elements, storage),
                "RPUSH" => handle_rpush(elements, storage),
                "LRANGE" => handle_lrange(elements, storage),
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

fn handle_set(elements: &[RespValue], storage: &Storage) -> String {
    if elements.len() < 3 {
        return "-ERR wrong number of arguments for 'SET' command\r\n".to_string();
    }

    let key = match &elements[1] {
        RespValue::BulkString(Some(k)) => String::from_utf8_lossy(k).to_string(),
        RespValue::SimpleString(s) => s.clone(),
        _ => return "-ERR Invalid key type\r\n".to_string(),
    };

    let value = match &elements[2] {
        RespValue::BulkString(Some(v)) => v.clone(),
        RespValue::SimpleString(v) => v.as_bytes().to_vec(),
        _ => return "-ERR Invalid value type\r\n".to_string(),
    };

    let mut i = 3;
    let mut expiration: Option<(u64, bool)> = None;

    while i < elements.len() {
        let option = extract_command_name(&elements[i]);

        match option.as_str() {
            "EX" => {
                if i + 1 >= elements.len() {
                    return "-ERR syntax error\r\n".to_string();
                }

                let seconds = match extract_integer_from_resp_value(&elements[i + 1]) {
                    Some(s) if s > 0 => s as u64,
                    _ => return "-ERR invalid expire time in 'SET' command\r\n".to_string(),
                };
                expiration = Some((seconds, false));
                i += 2;
            }
            "PX" => {
                if i + 1 >= elements.len() {
                    return "-ERR syntax error\r\n".to_string();
                }

                let milliseconds = match extract_integer_from_resp_value(&elements[i + 1]) {
                    Some(s) if s > 0 => s as u64,
                    _ => return "-ERR invalid expire time in 'SET' command\r\n".to_string(),
                };
                expiration = Some((milliseconds, true));
                i += 2;
            }
            _ => {
                return format!(
                    "-ERR syntax error, unexpected option '{}'. Only 'EX' or 'PX' are allowed",
                    option
                );
            }
        }
    }

    match expiration {
        Some((seconds, false)) => storage.set_ex(key, value, seconds),
        Some((milliseconds, true)) => storage.set_px(key, value, milliseconds),
        _ => storage.set(key, value),
    }

    "+OK\r\n".to_string()
}

fn handle_get(elements: &[RespValue], storage: &Storage) -> String {
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

fn handle_rpush(elements: &[RespValue], storage: &Storage) -> String {
    if elements.len() < 3 {
        return "-ERR wrong number of arguments for 'RPUSH' command\r\n".to_string();
    };

    let key = match &elements[1] {
        RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
        RespValue::SimpleString(s) => s.clone(),
        _ => return "-ERR Invalid key type\r\n".to_string(),
    };
    let values: Result<Vec<Vec<u8>>, String> = elements[2..]
        .iter()
        .map(|value| match value {
            RespValue::BulkString(Some(s)) => Ok(s.clone()),
            RespValue::SimpleString(s) => Ok(s.as_bytes().to_vec()),
            _ => Err("-ERR Invalid key type\r\n".to_string()),
        })
        .collect();

    let values = match values {
        Ok(vals) => vals,
        Err(e) => return e,
    };

    match storage.rpush(key, values) {
        Ok(len) => format!(":{}\r\n", len),
        Err(msg) => format!("-{}\r\n", msg),
    }
}

fn extract_command_name(value: &RespValue) -> String {
    match value {
        RespValue::BulkString(Some(cmd)) => String::from_utf8_lossy(cmd).to_uppercase(),
        RespValue::SimpleString(cmd) => cmd.to_uppercase(),
        _ => String::new(),
    }
}

fn extract_integer_from_resp_value(value: &RespValue) -> Option<i64> {
    match value {
        RespValue::Integer(i) => Some(*i),
        RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).parse::<i64>().ok(),
        RespValue::SimpleString(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn handle_lrange(elements: &[RespValue], storage: &Storage) -> String {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread::sleep, time::Duration};

    #[test]
    fn test_ping_command_returns_pong() {
        let storage = Storage::new();

        let cmd = RespValue::Array(Some(vec![RespValue::BulkString(Some(b"PING".to_vec()))]));
        assert_eq!(handle_command(&cmd, &storage), "+PONG\r\n")
    }
    #[test]
    fn test_ping_command_handles_case_insensibly() {
        let storage = Storage::new();

        let cmd = RespValue::Array(Some(vec![RespValue::BulkString(Some(b"Ping".to_vec()))]));
        assert_eq!(handle_command(&cmd, &storage), "+PONG\r\n")
    }

    #[test]
    fn test_echo_command_works_with_bulk_string() {
        let storage = Storage::new();

        let cmd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"ECHO".to_vec())),
            RespValue::BulkString(Some(b"Hello".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd, &storage), "$5\r\nHello\r\n")
    }

    #[test]
    fn test_echo_command_works_with_simple_string() {
        let storage = Storage::new();

        let cmd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"ECHO".to_vec())),
            RespValue::SimpleString("Simple_hello".to_string()),
        ]));
        assert_eq!(handle_command(&cmd, &storage), "$12\r\nSimple_hello\r\n")
    }

    #[test]
    fn test_echo_command_returns_error_for_invalid_number_of_arguments() {
        let storage = Storage::new();

        let cmd = RespValue::Array(Some(vec![RespValue::BulkString(Some(b"ECHO".to_vec()))]));
        assert_eq!(
            handle_command(&cmd, &storage),
            "-ERR wrong number of arguments for 'echo' command\r\n"
        )
    }

    #[test]
    fn test_set_command_works() {
        let storage = Storage::new();

        let cmd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd, &storage), "+OK\r\n")
    }

    #[test]
    fn test_set_command_is_idempotent() {
        let storage = Storage::new();

        let cmd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));

        handle_command(&cmd, &storage);
        assert_eq!(handle_command(&cmd, &storage), "+OK\r\n")
    }

    #[test]
    fn test_set_command_changes_value_if_called_twice_with_different_values() {
        let storage = Storage::new();

        let cmd1 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));

        let cmd2 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value-new".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd1, &storage), "+OK\r\n");
        assert_eq!(handle_command(&cmd2, &storage), "+OK\r\n");

        let cmd_get = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"GET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_get, &storage), "$9\r\nvalue-new\r\n");
    }

    #[test]
    fn test_get_command_works_for_existing_key() {
        let storage = Storage::new();

        let cmd_set = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        handle_command(&cmd_set, &storage);
        let cmd_get = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"GET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_get, &storage), "$5\r\nvalue\r\n")
    }

    #[test]
    fn test_get_command_returns_null_bulk_string_if_key_doesnt_exist() {
        let storage = Storage::new();

        let cmd_get = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"GET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_get, &storage), "$-1\r\n")
    }

    #[test]
    fn test_set_command_with_expiration_in_seconds() {
        let storage = Storage::new();

        let cmd_set = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
            RespValue::BulkString(Some(b"EX".to_vec())),
            RespValue::BulkString(Some(b"1".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_set, &storage), "+OK\r\n");

        let cmd_get = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"GET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_get, &storage), "$5\r\nvalue\r\n");

        sleep(Duration::from_millis(1100));

        assert_eq!(handle_command(&cmd_get, &storage), "$-1\r\n")
    }

    #[test]
    fn test_rpush_command_works() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"\"element_one\"".to_vec())),
            RespValue::BulkString(Some(b"\"element_two\"".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_rpush, &storage), ":2\r\n")
    }

    #[test]
    fn test_rpush_command_appends_to_existing_list() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"\"element_one\"".to_vec())),
            RespValue::BulkString(Some(b"\"element_two\"".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_rpush, &storage), ":2\r\n");

        let cmd_rpush_second = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"\"element_three\"".to_vec())),
            RespValue::BulkString(Some(b"\"element_four\"".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_rpush_second, &storage), ":4\r\n");
    }

    #[test]
    fn test_rpush_command_returns_error_on_wrong_number_of_arguments() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
        ]));
        assert_eq!(
            handle_command(&cmd_rpush, &storage),
            "-ERR wrong number of arguments for 'RPUSH' command\r\n"
        )
    }

    #[test]
    fn test_rpush_command_doesnt_work_on_keys() {
        let storage = Storage::new();

        let cmd_set = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_set, &storage), "+OK\r\n");

        let cmd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"\"element_one\"".to_vec())),
            RespValue::BulkString(Some(b"\"element_two\"".to_vec())),
        ]));
        assert_eq!(
            handle_command(&cmd, &storage),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        )
    }

    #[test]
    fn test_lrange_command_doesnt_work_on_keys() {
        let storage = Storage::new();

        let cmd_set = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_set, &storage), "+OK\r\n");

        let cmd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LRANGE".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::Integer(0),
            RespValue::Integer(1),
        ]));
        assert_eq!(
            handle_command(&cmd, &storage),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        )
    }
}
