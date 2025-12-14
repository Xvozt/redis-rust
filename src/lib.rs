#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

#[derive(Debug, PartialEq)]
pub enum ParseResult {
    Complete(RespValue, usize),
    Incomplete,
    Error(String),
}

pub struct RespParser {
    byte_buffer: Vec<u8>,
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            byte_buffer: Vec::default(),
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.byte_buffer.extend_from_slice(data);
    }

    pub fn parse(&self) -> ParseResult {
        self.parse_value(0)
    }

    pub fn consume(&mut self, n: usize) {
        self.byte_buffer.drain(..n);
    }

    pub fn has_data(&self) -> bool {
        !self.byte_buffer.is_empty()
    }

    pub fn clear(&mut self) {
        self.byte_buffer.clear();
    }

    fn parse_value(&self, pos: usize) -> ParseResult {
        if !self.has_bytes(pos, 1) {
            return ParseResult::Incomplete;
        }
        match self.byte_buffer[pos] {
            b'+' => self.parse_simle_string(pos),
            b'-' => self.parse_error(pos),
            b':' => self.parse_integer(pos),
            b'$' => self.parse_bulk_string(pos),
            b'*' => self.parse_array(pos),
            byte => ParseResult::Error(format!("Unsupported type prefix: '{}'", byte as char)),
        }
    }

    fn parse_simle_string(&self, pos: usize) -> ParseResult {
        if !self.has_bytes(pos, 1) {
            return ParseResult::Incomplete;
        }
        let crlf_pos = match self.find_crlf(pos + 1) {
            Some(p) => p,
            None => return ParseResult::Incomplete,
        };

        let content = match self.get_slice(pos + 1, crlf_pos) {
            Some(bytes) => bytes,
            None => return ParseResult::Error("Invalid slice range".to_string()),
        };

        let simple_string = match String::from_utf8(content.to_vec()) {
            Ok(s) => s,
            Err(_) => return ParseResult::Error("Invalid UTF-8".to_string()),
        };

        let consumed_bytes = crlf_pos + 2 - pos;
        ParseResult::Complete(RespValue::SimpleString(simple_string), consumed_bytes)
    }

    fn parse_integer(&self, pos: usize) -> ParseResult {
        if !self.has_bytes(pos, 1) {
            return ParseResult::Incomplete;
        }

        let crlf_pos = match self.find_crlf(pos + 1) {
            Some(p) => p,
            None => return ParseResult::Incomplete,
        };

        let content = match self.get_slice(pos + 1, crlf_pos) {
            Some(bytes) => bytes,
            None => return ParseResult::Error("Invalid slice range".to_string()),
        };

        let consumed_bytes = crlf_pos + 2 - pos;
        if let Some(i) = Self::parse_i64(content) {
            ParseResult::Complete(RespValue::Integer(i), consumed_bytes)
        } else {
            ParseResult::Error(format!(
                "Invalid integer format: '{}'",
                String::from_utf8_lossy(content)
            ))
        }
    }

    fn parse_bulk_string(&self, pos: usize) -> ParseResult {
        if !self.has_bytes(pos, 1) {
            return ParseResult::Incomplete;
        }

        let crlf_pos = match self.find_crlf(pos + 1) {
            Some(p) => p,
            None => return ParseResult::Incomplete,
        };

        let length_bytes = match self.get_slice(pos + 1, crlf_pos) {
            Some(bytes) => bytes,
            None => return ParseResult::Error("Invalid slice range".to_string()),
        };

        let length = match Self::parse_i64(length_bytes) {
            Some(l) => l,
            None => return ParseResult::Error("Invalid bulk string".to_string()),
        };

        match length {
            -1 => {
                let consumed = crlf_pos + 2 - pos;
                ParseResult::Complete(RespValue::BulkString(None), consumed)
            }
            n if n < -1 => ParseResult::Error(format!("Invalid bulk string length: {}", n)),

            n => {
                let len = n as usize;
                let content_start = crlf_pos + 2;
                let content_end = content_start + len;
                if !self.has_bytes(content_end, 2) {
                    return ParseResult::Incomplete;
                }

                let content = match self.get_slice(content_start, content_end) {
                    Some(bytes) => bytes,
                    None => return ParseResult::Error("Invalid slice range".to_string()),
                };

                // 2 bytes checked, so unwrap is safe
                let trailing_crlf = self.get_slice(content_end, content_end + 2).unwrap();

                if trailing_crlf != b"\r\n" {
                    return ParseResult::Error("Missing trailing CRLF".to_string());
                }

                let consumed_bytes = content_end + 2 - pos;
                return ParseResult::Complete(
                    RespValue::BulkString(Some(content.to_vec())),
                    consumed_bytes,
                );
            }
        }
    }

    fn parse_array(&self, pos: usize) -> ParseResult {
        if !self.has_bytes(pos, 1) {
            return ParseResult::Incomplete;
        }
        let crlf_pos = match self.find_crlf(pos + 1) {
            Some(p) => p,
            None => return ParseResult::Incomplete,
        };

        let count_bytes = match self.get_slice(pos + 1, crlf_pos) {
            Some(bytes) => bytes,
            None => return ParseResult::Error("Invalid slice range".to_string()),
        };

        let count = match Self::parse_i64(count_bytes) {
            Some(l) => l,
            None => return ParseResult::Error("Invalid bulk string".to_string()),
        };

        let element_count = match count {
            -1 => {
                let consumed = crlf_pos + 2 - pos;
                return ParseResult::Complete(RespValue::Array(None), consumed);
            }
            n if n < -1 => return ParseResult::Error(format!("Invalid array length: {}", n)),
            0 => {
                let consumed = crlf_pos + 2 - pos;
                return ParseResult::Complete(RespValue::Array(Some(vec![])), consumed);
            }
            n => n as usize,
        };

        let mut elements = Vec::with_capacity(element_count);
        let mut current_pos = crlf_pos + 2;

        for _ in 0..element_count {
            match self.parse_value(current_pos) {
                ParseResult::Complete(value, consumed) => {
                    elements.push(value);
                    current_pos += consumed;
                }
                ParseResult::Incomplete => return ParseResult::Incomplete,
                ParseResult::Error(e) => return ParseResult::Error(e),
            }
        }

        let total_consumed = current_pos - pos;
        ParseResult::Complete(RespValue::Array(Some(elements)), total_consumed)
    }

    fn parse_error(&self, pos: usize) -> ParseResult {
        if !self.has_bytes(pos, 1) {
            return ParseResult::Incomplete;
        }

        let crlf_pos = match self.find_crlf(pos + 1) {
            Some(p) => p,
            None => return ParseResult::Incomplete,
        };

        let content = match self.get_slice(pos + 1, crlf_pos) {
            Some(bytes) => bytes,
            None => return ParseResult::Error("Invalid slice range".to_string()),
        };

        let error_string = match String::from_utf8(content.to_vec()) {
            Ok(s) => s,
            Err(_) => return ParseResult::Error("Invalid UTF-8".to_string()),
        };

        let consumed_bytes = crlf_pos + 2 - pos;

        ParseResult::Complete(RespValue::Error(error_string), consumed_bytes)
    }

    fn find_crlf(&self, pos: usize) -> Option<usize> {
        self.byte_buffer[pos..]
            .windows(2)
            .enumerate()
            .find(|(_i, window)| window[0] == b'\r' && window[1] == b'\n')
            .map(|(relative_index, _window)| pos + relative_index)
    }

    fn has_bytes(&self, pos: usize, n: usize) -> bool {
        pos + n <= self.byte_buffer.len()
    }

    fn get_slice(&self, start: usize, end: usize) -> Option<&[u8]> {
        if start <= end && end <= self.byte_buffer.len() {
            Some(&self.byte_buffer[start..end])
        } else {
            None
        }
    }

    fn parse_i64(bytes: &[u8]) -> Option<i64> {
        let s = std::str::from_utf8(bytes).ok()?;
        s.parse::<i64>().ok()
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn test_find_crlf() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK\r\nSOMETHING");
        assert_eq!(parser.find_crlf(0), Some(3));
        assert_eq!(parser.find_crlf(4), None);
    }

    #[test]
    fn test_has_bytes() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK\r\nSOMETHING");
        assert_eq!(parser.has_bytes(2, 5), true);
        assert_eq!(parser.has_bytes(10, 5), false);
    }

    #[test]
    fn test_has_get_slice() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK\r\nSOMETHING");
        assert_eq!(parser.get_slice(1, 3), Some(&b"OK"[..]));
        assert_eq!(parser.get_slice(0, 5), Some(&b"+OK\r\n"[..]));
        assert_eq!(parser.get_slice(1, 100), None)
    }

    #[test]
    fn test_parse_i64() {
        let bytes = b"125";
        let bytes2 = b"abc";
        assert_eq!(RespParser::parse_i64(bytes), Some(125));
        assert_eq!(RespParser::parse_i64(bytes2), None);
    }

    #[test]
    fn test_parse_simple_string() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK\r\n");
        match parser.parse() {
            ParseResult::Complete(RespValue::SimpleString(s), consumed) => {
                assert_eq!(s, "OK");
                assert_eq!(consumed, 5)
            }
            other => panic!("Expected Complete(SimpleString), got {:?}", other),
        }
    }

    #[test]
    fn test_parse_error() {
        let mut parser = RespParser::new();
        parser.feed(b"-ERR unknown command 'asdf'\r\n");
        match parser.parse() {
            ParseResult::Complete(RespValue::Error(s), consumed) => {
                assert_eq!(s, "ERR unknown command 'asdf'");
                assert_eq!(consumed, 29)
            }
            other => panic!("Expected Complete(Error), got {:?}", other),
        }
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$5\r\nhello\r\n");
        match parser.parse() {
            ParseResult::Complete(RespValue::BulkString(Some(bytes)), consumed) => {
                assert_eq!(bytes, b"hello");
                assert_eq!(consumed, 11);
            }
            other => panic!("Expected Complete(BulkString(Some(...))), got {:?}", other),
        }
    }

    #[test]
    fn test_parse_empty_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$0\r\n\r\n");
        match parser.parse() {
            ParseResult::Complete(RespValue::BulkString(Some(bytes)), consumed) => {
                assert_eq!(bytes, b"");
                assert_eq!(consumed, 6)
            }
            other => panic!("Expected Complete(BulkString), got {:?}", other),
        }
    }

    #[test]
    fn test_parse_array_with_2_bulk_strings() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
        match parser.parse() {
            ParseResult::Complete(RespValue::Array(Some(elements)), consumed) => {
                assert_eq!(elements.len(), 2);
                match &elements[0] {
                    RespValue::BulkString(Some(bytes)) => assert_eq!(bytes, b"hello"),
                    _ => panic!("Expected BulkString"),
                };
                match &elements[1] {
                    RespValue::BulkString(Some(bytes)) => assert_eq!(bytes, b"world"),
                    _ => panic!("Expected BulkString"),
                }
                assert_eq!(consumed, 26)
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_empty_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*0\r\n");
        match parser.parse() {
            ParseResult::Complete(RespValue::Array(Some(elements)), consumed) => {
                assert_eq!(elements.len(), 0);
                assert_eq!(consumed, 4)
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_null_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*-1\r\n");
        match parser.parse() {
            ParseResult::Complete(RespValue::Array(None), consumed) => {
                assert_eq!(consumed, 5)
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_integer_positive() {
        let mut parser = RespParser::new();
        parser.feed(b":1000\r\n");
        match parser.parse() {
            ParseResult::Complete(RespValue::Integer(i), consumed) => {
                assert_eq!(i, 1000);
                assert_eq!(consumed, 7)
            }
            other => panic!("Expected Comelete(Integer), got {:?}", other),
        }
    }

    #[test]
    fn test_parse_integer_negative() {
        let mut parser = RespParser::new();
        parser.feed(b":-35\r\n");
        match parser.parse() {
            ParseResult::Complete(RespValue::Integer(i), consumed) => {
                assert_eq!(i, -35);
                assert_eq!(consumed, 6)
            }
            other => panic!("Expected Comelete(Integer), got {:?}", other),
        }
    }
    #[test]
    fn test_parse_integer_zero() {
        let mut parser = RespParser::new();
        parser.feed(b":0\r\n");
        match parser.parse() {
            ParseResult::Complete(RespValue::Integer(i), consumed) => {
                assert_eq!(i, 0);
                assert_eq!(consumed, 4)
            }
            other => panic!("Expected Comelete(Integer), got {:?}", other),
        }
    }
    #[test]
    fn test_parse_non_integer_returns_error() {
        let mut parser = RespParser::new();
        parser.feed(b":abc\r\n");
        match parser.parse() {
            ParseResult::Error(msg) => {
                assert!(msg.contains("Invalid integer"))
            }
            other => panic!("Expected Error, got {:?}", other),
        }
    }
}
