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
                "LPUSH" => handle_lpush(elements, storage),
                "LRANGE" => handle_lrange(elements, storage),
                "LLEN" => handle_llen(elements, storage),
                "LPOP" => handle_lpop(elements, storage),
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

fn handle_get(elements: &[RespValue], storage: &Storage) -> String {
    if elements.len() < 2 {
        return "-ERR wrong number of arguments for 'GET' command\r\n".to_string();
    }

    let key = extract_key(&elements[1]);

    match storage.get(&key) {
        Some(v) => format!("${}\r\n{}\r\n", v.len(), String::from_utf8_lossy(&v)),
        None => "$-1\r\n".to_string(),
    }
}

fn handle_set(elements: &[RespValue], storage: &Storage) -> String {
    if elements.len() < 3 {
        return "-ERR wrong number of arguments for 'SET' command\r\n".to_string();
    }

    let key = extract_key(&elements[1]);

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

fn handle_list_push<F>(elements: &[RespValue], push_fn: F) -> String
where
    F: FnOnce(String, Vec<Vec<u8>>) -> Result<usize, String>,
{
    if elements.len() < 3 {
        return "-ERR wrong number of arguments for command\r\n".to_string();
    };

    let key = extract_key(&elements[1]);

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

    match push_fn(key, values) {
        Ok(len) => format!(":{}\r\n", len),
        Err(msg) => format!("-{}\r\n", msg),
    }
}

fn handle_rpush(elements: &[RespValue], storage: &Storage) -> String {
    handle_list_push(elements, |k, v| storage.rpush(k, v))
}

fn handle_lpush(elements: &[RespValue], storage: &Storage) -> String {
    handle_list_push(elements, |k, v| storage.lpush(k, v))
}

fn handle_lrange(elements: &[RespValue], storage: &Storage) -> String {
    if elements.len() != 4 {
        return "-ERR wrong number of arguments for command\r\n".to_string();
    };

    let key = extract_key(&elements[1]);

    let start = match extract_integer_from_resp_value(&elements[2]) {
        Some(i) => i as isize,
        None => return "value is not an integer or out of range\r\n".to_string(),
    };

    let end = match extract_integer_from_resp_value(&elements[3]) {
        Some(i) => i as isize,
        None => return "value is not an integer or out of range\r\n".to_string(),
    };

    match storage.lrange(&key, start, end) {
        Ok(items) => format_array(items),
        Err(e) => format!("-{}\r\n", e),
    }
}

fn handle_llen(elements: &[RespValue], storage: &Storage) -> String {
    if elements.len() != 2 {
        return "-ERR wrong number of arguments for command\r\n".to_string();
    }

    let key = extract_key(&elements[1]);

    match storage.llen(&key) {
        Ok(len) => format!(":{}\r\n", len),
        Err(e) => format!("-{}\r\n", e),
    }
}

fn handle_lpop(elements: &[RespValue], storage: &Storage) -> String {
    match elements.len() {
        2 => {
            let key = extract_key(&elements[1]);

            match storage.lpop(&key) {
                Ok(Some(v)) => format!("${}\r\n{}\r\n", v.len(), String::from_utf8_lossy(&v)),
                Ok(None) => "$-1\r\n".to_string(),
                Err(e) => format!("-{}\r\n", e),
            }
        }

        3 => {
            let key = extract_key(&elements[1]);
            let count = match extract_integer_from_resp_value(&elements[2]) {
                Some(i) => i as isize,
                None => return "value is out of range, must be positive\r\n".to_string(),
            };

            match storage.lpop_multiple(&key, count as usize) {
                Ok(Some(items)) => format_array(items),
                Ok(None) => "$-1\r\n".to_string(),
                Err(e) => format!("-{}\r\n", e),
            }
        }
        _ => return "-ERR wrong number of arguments for command\r\n".to_string(),
    }
}

fn extract_command_name(value: &RespValue) -> String {
    match value {
        RespValue::BulkString(Some(cmd)) => String::from_utf8_lossy(cmd).to_uppercase(),
        RespValue::SimpleString(cmd) => cmd.to_uppercase(),
        _ => String::new(),
    }
}

fn extract_key(key_candidate: &RespValue) -> String {
    match key_candidate {
        RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
        RespValue::SimpleString(s) => s.clone(),
        _ => return "-ERR Invalid key type\r\n".to_string(),
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

fn format_array(items: Vec<Vec<u8>>) -> String {
    if items.is_empty() {
        return "*0\r\n".to_string();
    }
    let elements: Vec<String> = items
        .iter()
        .map(|item| format!("${}\r\n{}\r\n", item.len(), String::from_utf8_lossy(item)))
        .collect();

    format!("*{}\r\n{}", items.len(), elements.join(""))
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
            "-ERR wrong number of arguments for command\r\n"
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
    fn test_lpush_command_works() {
        let storage = Storage::new();

        let cmd_lpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_lpush, &storage), ":2\r\n")
    }

    #[test]
    fn test_lrange_after_lpush_returns_in_reverse_order() {
        let storage = Storage::new();

        let cmd_lpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
            RespValue::BulkString(Some(b"c".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_lpush, &storage), ":3\r\n");

        let cmd_lrange = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LRANGE".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::Integer(0),
            RespValue::Integer(-1),
        ]));

        assert_eq!(
            handle_command(&cmd_lrange, &storage),
            "*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"
        );
    }

    #[test]
    fn test_lpush_command_appends_to_existing_list() {
        let storage = Storage::new();

        let cmd_lpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_lpush, &storage), ":2\r\n");

        let cmd_lpush_second = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"c".to_vec())),
            RespValue::BulkString(Some(b"d".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_lpush_second, &storage), ":4\r\n");
    }

    #[test]
    fn test_lpush_command_returns_error_on_wrong_number_of_arguments() {
        let storage = Storage::new();

        let cmd_lpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
        ]));
        assert_eq!(
            handle_command(&cmd_lpush, &storage),
            "-ERR wrong number of arguments for command\r\n"
        )
    }

    #[test]
    fn test_llen_command_returns_len_for_list() {
        let storage = Storage::new();

        let cmd_lpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"c".to_vec())),
            RespValue::BulkString(Some(b"d".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_lpush, &storage), ":2\r\n");

        let cmd_llen = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LLEN".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_llen, &storage), ":2\r\n")
    }

    #[test]
    fn test_llen_command_returns_zero_for_nonexisting_list() {
        let storage = Storage::new();

        let cmd_llen = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LLEN".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_llen, &storage), ":0\r\n")
    }

    #[test]
    fn test_llen_command_returns_error_on_wrong_number_of_arguments() {
        let storage = Storage::new();

        let cmd_llen = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LLEN".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"c".to_vec())),
        ]));

        assert_eq!(
            handle_command(&cmd_llen, &storage),
            "-ERR wrong number of arguments for command\r\n"
        )
    }

    #[test]
    fn test_lpush_command_doesnt_work_on_keys() {
        let storage = Storage::new();

        let cmd_set = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_set, &storage), "+OK\r\n");

        let cmd_lpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPUSH".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"\"element_one\"".to_vec())),
            RespValue::BulkString(Some(b"\"element_two\"".to_vec())),
        ]));
        assert_eq!(
            handle_command(&cmd_lpush, &storage),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        )
    }

    #[test]
    fn test_lrange_command_works() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"\"element_one\"".to_vec())),
            RespValue::BulkString(Some(b"\"element_two\"".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_rpush, &storage), ":2\r\n");

        let cmd_lrange = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LRANGE".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::Integer(0),
            RespValue::Integer(1),
        ]));

        assert_eq!(
            handle_command(&cmd_lrange, &storage),
            "*2\r\n$13\r\n\"element_one\"\r\n$13\r\n\"element_two\"\r\n"
        );
    }

    #[test]
    fn test_lrange_command_return_empty_array_if_list_doenst_exist() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"\"element_one\"".to_vec())),
            RespValue::BulkString(Some(b"\"element_two\"".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_rpush, &storage), ":2\r\n");

        let cmd_lrange = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LRANGE".to_vec())),
            RespValue::BulkString(Some(b"not_existed_list".to_vec())),
            RespValue::Integer(0),
            RespValue::Integer(1),
        ]));

        assert_eq!(handle_command(&cmd_lrange, &storage), "*0\r\n");
    }

    #[test]
    fn test_lrange_command_return_empty_array_if_start_is_bigger_than_end() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"\"element_one\"".to_vec())),
            RespValue::BulkString(Some(b"\"element_two\"".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_rpush, &storage), ":2\r\n");

        let cmd_lrange = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LRANGE".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::Integer(3),
            RespValue::Integer(1),
        ]));

        assert_eq!(handle_command(&cmd_lrange, &storage), "*0\r\n");
    }

    #[test]
    fn test_lrange_command_return_empty_array_if_start_is_bigger_than_len() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"\"element_one\"".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_rpush, &storage), ":1\r\n");

        let cmd_lrange = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LRANGE".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::Integer(2),
            RespValue::Integer(3),
        ]));

        assert_eq!(handle_command(&cmd_lrange, &storage), "*0\r\n");
    }

    #[test]
    fn test_lrange_command_len_become_end_if_end_is_bigger() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"\"element_one\"".to_vec())),
            RespValue::BulkString(Some(b"\"element_two\"".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_rpush, &storage), ":2\r\n");

        let cmd_lrange = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LRANGE".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::Integer(0),
            RespValue::Integer(5),
        ]));

        assert_eq!(
            handle_command(&cmd_lrange, &storage),
            "*2\r\n$13\r\n\"element_one\"\r\n$13\r\n\"element_two\"\r\n"
        );
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

    #[test]
    fn test_lrange_command_with_missing_arguments() {
        let storage = Storage::new();
        let cmd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LRANGE".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
        ]));
        assert_eq!(
            handle_command(&cmd, &storage),
            "-ERR wrong number of arguments for command\r\n"
        );
    }

    #[test]
    fn test_lpop_command_returns_popped_element() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_rpush, &storage), ":2\r\n");

        let cmd_lpop = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPOP".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_lpop, &storage), "$1\r\na\r\n");
    }

    #[test]
    fn test_lpop_command_returns_number_of_elements_if_called_with_count() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_rpush, &storage), ":2\r\n");

        let cmd_lpop_multiple = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPOP".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"2".to_vec())),
        ]));

        assert_eq!(
            handle_command(&cmd_lpop_multiple, &storage),
            "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
        );
    }

    #[test]
    fn test_lpop_command_returns_all_elements_if_count_more_than_length_of_array() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_rpush, &storage), ":2\r\n");

        let cmd_lpop_multiple = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPOP".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"3".to_vec())),
        ]));

        assert_eq!(
            handle_command(&cmd_lpop_multiple, &storage),
            "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
        );
    }

    #[test]
    fn test_lpop_command_returns_empty_array_if_count_is_zero() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_rpush, &storage), ":2\r\n");

        let cmd_lpop_multiple = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPOP".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"0".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_lpop_multiple, &storage), "*0\r\n");
    }

    #[test]
    fn test_lpop_command_returns_null_string_for_non_existing() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"a".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_rpush, &storage), ":1\r\n");

        let cmd_lpop = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPOP".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_lpop, &storage), "$1\r\na\r\n");

        let cmd_lpop = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPOP".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_lpop, &storage), "$-1\r\n");
    }

    #[test]
    fn test_lpop_command_returns_error_on_wrong_number_of_arguments() {
        let storage = Storage::new();

        let cmd_lpop = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPOP".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"5".to_vec())),
            RespValue::BulkString(Some(b"6".to_vec())),
        ]));

        assert_eq!(
            handle_command(&cmd_lpop, &storage),
            "-ERR wrong number of arguments for command\r\n"
        )
    }

    #[test]
    fn test_lpop_command_doesnt_work_on_keys() {
        let storage = Storage::new();

        let cmd_set = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_set, &storage), "+OK\r\n");

        let cmd_lpop = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LPOP".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
        ]));
        assert_eq!(
            handle_command(&cmd_lpop, &storage),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        )
    }
}
