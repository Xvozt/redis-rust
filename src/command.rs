use std::collections::HashMap;

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
                "BLPOP" => handle_blpop(elements, storage),
                "TYPE" => handle_type(elements, storage),
                "XADD" => handle_xadd(elements, storage),
                "XRANGE" => handle_xrange(elements, storage),
                "XREAD" => handle_xread(elements, storage),
                _ => format!("-ERR unknown command: '{}'\r\n", command),
            }
        }
        _ => "-ERR Invalid command format \r\n".to_string(),
    }
}

fn handle_xread(elements: &[RespValue], storage: &Storage) -> String {
    if elements.len() != 4 {
        return "-ERR wrong number of arguments for command\r\n".to_string();
    }

    if extract_command_name(&elements[1]) != "STREAMS" {
        return "-ERR syntax error\r\n".to_string();
    }

    let stream_name = extract_key(&elements[2]);

    let id = match &elements[3] {
        RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
        RespValue::SimpleString(s) => s.clone(),
        _ => return "-ERR Invalid stream ID specified as stream command argument\r\n".to_string(),
    };

    match storage.xread(&stream_name, &id) {
        Ok(v) => format_xread(&stream_name, v),
        Err(e) => {
            format!("-{}\r\n", e)
        }
    }
}

fn handle_xrange(elements: &[RespValue], storage: &Storage) -> String {
    if elements.len() != 4 {
        return "-ERR wrong number of arguments for command\r\n".to_string();
    }

    let stream_name = extract_key(&elements[1]);
    let start = match &elements[2] {
        RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
        RespValue::SimpleString(s) => s.clone(),
        _ => return "-ERR Invalid stream ID specified as stream command argument\r\n".to_string(),
    };

    let end = match &elements[3] {
        RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
        RespValue::SimpleString(s) => s.clone(),
        _ => return "-ERR Invalid stream ID specified as stream command argument\r\n".to_string(),
    };

    match storage.xrange(&stream_name, &start, &end) {
        Ok(v) => format_xrange(v),
        Err(e) => {
            format!("-{}\r\n", e)
        }
    }
}

fn handle_xadd(elements: &[RespValue], storage: &Storage) -> String {
    if elements.len() < 5 || ((elements.len() - 3) % 2 != 0) {
        return "-ERR wrong number of arguments for command\r\n".to_string();
    }

    let stream_name = extract_key(&elements[1]);

    let id = match &elements[2] {
        RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
        RespValue::SimpleString(s) => s.clone(),
        _ => return "-ERR Invalid stream ID specified as stream command argument\r\n".to_string(),
    };

    let mut values: HashMap<String, Vec<u8>> = HashMap::with_capacity(elements[3..].len());

    for pair in elements[3..].windows(2).step_by(2) {
        let key = match &pair[0] {
            RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
            RespValue::SimpleString(s) => s.clone(),
            _ => return "-ERR Invalid key type\r\n".to_string(),
        };

        let value = match &pair[1] {
            RespValue::BulkString(Some(s)) => s.to_owned(),
            RespValue::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return "-ERR Invalid key type\r\n".to_string(),
        };

        values.insert(key, value);
    }

    match storage.xadd(stream_name, &id, values) {
        Ok(s) => format!("${}\r\n{}\r\n", s.len(), s),
        Err(e) => format!("-{}\r\n", e),
    }
}

fn handle_type(elements: &[RespValue], storage: &Storage) -> String {
    if elements.len() != 2 {
        return "-ERR wrong number of arguments for 'GET' command\r\n".to_string();
    }

    let key = extract_key(&elements[1]);
    let key_type = storage.get_type(&key);
    format!("+{}\r\n", key_type.to_string())
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

fn handle_blpop(elements: &[RespValue], storage: &Storage) -> String {
    if elements.len() < 3 {
        return "-ERR wrong number of arguments for command\r\n".to_string();
    }
    let keys_args = &elements[..elements.len() - 1];
    let timeout_arg = &elements[elements.len() - 1];

    let keys: Vec<String> = keys_args.iter().map(|arg| extract_key(arg)).collect();
    let timeout = extract_timeout(timeout_arg);

    let timeout: f64 = match timeout {
        Some(t) => t as f64,
        None => return "-ERR timeout must be a number\r\n".to_string(),
    };

    match storage.blpop(keys, timeout) {
        Ok(Some((key, value))) => {
            format!(
                "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                String::from_utf8_lossy(&value)
            )
        }
        Ok(None) => "*-1\r\n".to_string(),
        Err(e) => format!("-{}\r\n", e),
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

fn extract_timeout(value: &RespValue) -> Option<f64> {
    match value {
        RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).parse::<f64>().ok(),
        RespValue::SimpleString(s) => s.parse::<f64>().ok(),
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

fn format_xread(stream_name: &str, items: Vec<Vec<Vec<u8>>>) -> String {
    if items.is_empty() {
        return "*0\r\n".to_string();
    }

    let mut out = String::new();
    out.push_str(&format!("*1\r\n",)); // only works for 1 stream xread
    out.push_str(&format!("*2\r\n",));
    out.push_str(&format!("${}\r\n{}\r\n", stream_name.len(), &stream_name));
    out.push_str(&format_xrange(items));
    out
}

fn format_xrange(items: Vec<Vec<Vec<u8>>>) -> String {
    if items.is_empty() {
        return "*0\r\n".to_string();
    }

    let mut out = String::new();
    out.push_str(&format!("*{}\r\n", items.len()));
    for item in items {
        if item.is_empty() {
            out.push_str("*0\r\n");
            continue;
        }
        let id = &item[0];
        let fields = &item[1..];

        out.push_str("*2\r\n");

        out.push_str(&format!(
            "${}\r\n{}\r\n",
            id.len(),
            String::from_utf8_lossy(id)
        ));

        out.push_str(&format!("*{}\r\n", fields.len()));

        for f in fields {
            out.push_str(&format!(
                "${}\r\n{}\r\n",
                f.len(),
                String::from_utf8_lossy(f)
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        thread::sleep,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

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

    #[test]
    fn test_blpop_command_returns_element_immediately() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
        ]));

        handle_command(&cmd_rpush, &storage);

        let cmd_blpop = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"BLPOP".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"0".to_vec())),
        ]));

        assert_eq!(
            handle_command(&cmd_blpop, &storage),
            "*2\r\n$4\r\nlist\r\n$1\r\na\r\n"
        )
    }

    #[test]
    fn test_blpop_command_returns_element_after_push() {
        let storage = Storage::new();

        let cmd_blpop = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"BLPOP".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"5".to_vec())),
        ]));

        let storage_clone = storage.clone();
        let blpop_thread = std::thread::spawn(move || handle_command(&cmd_blpop, &storage_clone));

        std::thread::sleep(std::time::Duration::from_millis(100));

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"hello".to_vec())),
        ]));

        let rpush_result = handle_command(&cmd_rpush, &storage);
        assert_eq!(rpush_result, ":1\r\n");

        let blpop_result = blpop_thread.join().unwrap();
        assert_eq!(blpop_result, "*2\r\n$4\r\nlist\r\n$5\r\nhello\r\n");

        let cmd_lrange = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LRANGE".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::Integer(0),
            RespValue::Integer(-1),
        ]));

        let lrange_result = handle_command(&cmd_lrange, &storage);
        assert_eq!(lrange_result, "*0\r\n");
    }

    #[test]
    fn test_blpop_command_returns_null_array_if_timeout_expires_early_that_element_pushed() {
        let storage = Storage::new();

        let cmd_blpop = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"BLPOP".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"0.01".to_vec())),
        ]));

        let storage_clone = storage.clone();
        let blpop_thread = std::thread::spawn(move || handle_command(&cmd_blpop, &storage_clone));

        std::thread::sleep(std::time::Duration::from_millis(50));

        let blpop_result = blpop_thread.join().unwrap();
        assert_eq!(blpop_result, "*-1\r\n");

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"hello".to_vec())),
        ]));

        let rpush_result = handle_command(&cmd_rpush, &storage);
        assert_eq!(rpush_result, ":1\r\n");

        let cmd_lrange = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"LRANGE".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::Integer(0),
            RespValue::Integer(-1),
        ]));

        let lrange_result = handle_command(&cmd_lrange, &storage);
        assert_eq!(lrange_result, "*1\r\n$5\r\nhello\r\n");
    }

    #[test]
    fn test_blpop_command_returns_null_array_for_non_existing_list() {
        let storage = Storage::new();

        let cmd_blpop = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"BLPOP".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"1".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_blpop, &storage), "*-1\r\n")
    }

    #[test]
    fn test_blpop_command_returns_error_on_wrong_number_of_arguments() {
        let storage = Storage::new();

        let cmd_blpop = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"BLPOP".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
        ]));

        assert_eq!(
            handle_command(&cmd_blpop, &storage),
            "-ERR wrong number of arguments for command\r\n"
        )
    }

    #[test]
    fn test_type_command_returns_type_for_string() {
        let storage = Storage::new();

        let cmd_set = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"foo".to_vec())),
        ]));

        let cmd_type = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"TYPE".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_set, &storage), "+OK\r\n");

        assert_eq!(handle_command(&cmd_type, &storage), "+string\r\n")
    }

    #[test]
    fn test_type_command_returns_type_for_list() {
        let storage = Storage::new();

        let cmd_rpush = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"RPUSH".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
            RespValue::BulkString(Some(b"\"element_one\"".to_vec())),
            RespValue::BulkString(Some(b"\"element_two\"".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_rpush, &storage), ":2\r\n");

        let cmd_type = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"TYPE".to_vec())),
            RespValue::BulkString(Some(b"list".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_type, &storage), "+list\r\n")
    }

    #[test]
    fn test_type_command_returns_none_for_non_existing_key() {
        let storage = Storage::new();

        let cmd_type = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"TYPE".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_type, &storage), "+none\r\n");
    }

    #[test]
    fn test_xadd_command_works_with_specified_id() {
        let storage = Storage::new();

        let cmd_xadd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_xadd, &storage), "$3\r\n0-1\r\n")
    }

    #[test]
    fn test_xadd_command_works_with_generated_id() {
        let storage = Storage::new();

        let cmd_xadd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"*".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));

        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let result = handle_command(&cmd_xadd, &storage);

        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let mut lines = result.split("\r\n");
        let len_line = lines.next().unwrap();
        let id = lines.next().unwrap();
        assert_eq!(lines.next(), Some(""));

        assert!(len_line.starts_with('$'));
        let len = len_line[1..].parse::<usize>().unwrap();
        assert_eq!(len, id.len());

        let mut parts = id.split('-');
        let ms = parts.next().unwrap().parse::<u128>().unwrap();
        let _seq = parts.next().unwrap().parse::<u64>().unwrap();
        assert!(parts.next().is_none());

        assert!(ms >= before && ms <= after);
    }

    #[test]
    fn test_xadd_command_works_with_more_than_one_pair_of_values() {
        let storage = Storage::new();

        let cmd_xadd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
            RespValue::BulkString(Some(b"field2".to_vec())),
            RespValue::BulkString(Some(b"value2".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_xadd, &storage), "$3\r\n0-1\r\n")
    }

    #[test]
    fn test_xadd_command_returns_error_for_wrong_number_of_arguments() {
        let storage = Storage::new();

        let cmd_xadd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
            RespValue::BulkString(Some(b"field2".to_vec())),
        ]));
        assert_eq!(
            handle_command(&cmd_xadd, &storage),
            "-ERR wrong number of arguments for command\r\n"
        )
    }

    #[test]
    fn test_xadd_command_returns_error_for_invalid_provided_id() {
        let storage = Storage::new();

        let cmd_xadd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"-1-1".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        assert_eq!(
            handle_command(&cmd_xadd, &storage),
            "-ERR Invalid stream ID specified as stream command argument\r\n"
        );

        let cmd_xadd2 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"0-0".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        assert_eq!(
            handle_command(&cmd_xadd2, &storage),
            "-ERR The ID specified in XADD must be greater than 0-0\r\n"
        );

        let cmd_xadd3 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"f-f".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        assert_eq!(
            handle_command(&cmd_xadd3, &storage),
            "-ERR Invalid stream ID specified as stream command argument\r\n"
        )
    }

    #[test]
    fn test_xadd_command_returns_error_if_provided_id_is_smaller_than_existing() {
        let storage = Storage::new();

        let cmd_xadd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"1-2".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_xadd, &storage), "$3\r\n1-2\r\n");

        let cmd_xadd2 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
            RespValue::BulkString(Some(b"field2".to_vec())),
            RespValue::BulkString(Some(b"value2".to_vec())),
        ]));
        assert_eq!(
            handle_command(&cmd_xadd2, &storage),
            "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
        )
    }

    #[test]
    fn test_type_command_works_on_stream() {
        let storage = Storage::new();

        let cmd_xadd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_xadd, &storage), "$3\r\n0-1\r\n");

        let cmd_type = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"TYPE".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_type, &storage), "+stream\r\n")
    }

    #[test]
    fn test_xadd_command_doesnt_work_on_strings() {
        let storage = Storage::new();

        let cmd_set = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_set, &storage), "+OK\r\n");

        let cmd_xadd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"*".to_vec())),
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
        ]));
        assert_eq!(
            handle_command(&cmd_xadd, &storage),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        )
    }

    #[test]
    fn test_xread_command_returns_entries_after_id() {
        let storage = Storage::new();

        let cmd_xadd1 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_xadd1, &storage), "$3\r\n0-1\r\n");

        let cmd_xadd2 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"0-2".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_xadd2, &storage), "$3\r\n0-2\r\n");

        let cmd_xread = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XREAD".to_vec())),
            RespValue::BulkString(Some(b"STREAMS".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
        ]));

        let expected = concat!(
            "*1\r\n",
            "*2\r\n",
            "$8\r\nmystream\r\n",
            "*1\r\n",
            "*2\r\n",
            "$3\r\n0-2\r\n",
            "*2\r\n",
            "$5\r\nfield\r\n",
            "$5\r\nvalue\r\n"
        );

        assert_eq!(handle_command(&cmd_xread, &storage), expected);
    }

    #[test]
    fn test_xread_command_returns_empty_array_when_no_new_entries() {
        let storage = Storage::new();

        let cmd_xadd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_xadd, &storage), "$3\r\n0-1\r\n");

        let cmd_xread = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XREAD".to_vec())),
            RespValue::BulkString(Some(b"STREAMS".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
        ]));

        assert_eq!(handle_command(&cmd_xread, &storage), "*0\r\n");
    }

    #[test]
    fn test_xread_command_returns_error_on_wrong_number_of_arguments() {
        let storage = Storage::new();

        let cmd_xread = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XREAD".to_vec())),
            RespValue::BulkString(Some(b"STREAMS".to_vec())),
            RespValue::BulkString(Some(b"mystream".to_vec())),
        ]));

        assert_eq!(
            handle_command(&cmd_xread, &storage),
            "-ERR wrong number of arguments for command\r\n"
        );
    }

    #[test]
    fn test_xread_command_returns_entries_for_two_streams() {
        let storage = Storage::new();

        let cmd_xadd_stream1 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"stream1".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_xadd_stream1, &storage), "$3\r\n0-1\r\n");

        let cmd_xadd_stream2 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"stream2".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value2".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_xadd_stream2, &storage), "$3\r\n0-1\r\n");

        let cmd_xread = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XREAD".to_vec())),
            RespValue::BulkString(Some(b"STREAMS".to_vec())),
            RespValue::BulkString(Some(b"stream1".to_vec())),
            RespValue::BulkString(Some(b"stream2".to_vec())),
            RespValue::BulkString(Some(b"0-0".to_vec())),
            RespValue::BulkString(Some(b"0-0".to_vec())),
        ]));

        let expected = concat!(
            "*2\r\n",
            "*2\r\n",
            "$7\r\nstream1\r\n",
            "*1\r\n",
            "*2\r\n",
            "$3\r\n0-1\r\n",
            "*2\r\n",
            "$5\r\nfield\r\n",
            "$5\r\nvalue\r\n",
            "*2\r\n",
            "$7\r\nstream2\r\n",
            "*1\r\n",
            "*2\r\n",
            "$3\r\n0-1\r\n",
            "*2\r\n",
            "$5\r\nfield\r\n",
            "$6\r\nvalue2\r\n"
        );

        assert_eq!(handle_command(&cmd_xread, &storage), expected);
    }

    #[test]
    fn test_xread_command_omits_streams_with_no_new_entries() {
        let storage = Storage::new();

        let cmd_xadd_stream1 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"stream1".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_xadd_stream1, &storage), "$3\r\n0-1\r\n");

        let cmd_xadd_stream2 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XADD".to_vec())),
            RespValue::BulkString(Some(b"stream2".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
            RespValue::BulkString(Some(b"field".to_vec())),
            RespValue::BulkString(Some(b"value2".to_vec())),
        ]));
        assert_eq!(handle_command(&cmd_xadd_stream2, &storage), "$3\r\n0-1\r\n");

        let cmd_xread = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"XREAD".to_vec())),
            RespValue::BulkString(Some(b"STREAMS".to_vec())),
            RespValue::BulkString(Some(b"stream1".to_vec())),
            RespValue::BulkString(Some(b"stream2".to_vec())),
            RespValue::BulkString(Some(b"0-0".to_vec())),
            RespValue::BulkString(Some(b"0-1".to_vec())),
        ]));

        let expected = concat!(
            "*1\r\n",
            "*2\r\n",
            "$7\r\nstream1\r\n",
            "*1\r\n",
            "*2\r\n",
            "$3\r\n0-1\r\n",
            "*2\r\n",
            "$5\r\nfield\r\n",
            "$5\r\nvalue\r\n"
        );

        assert_eq!(handle_command(&cmd_xread, &storage), expected);
    }
}
