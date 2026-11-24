use crate::{Error, MAX_DEPTH, Result, Value, check_integer, float};
use serde_json::{Map, Number};
use std::str::FromStr;

enum Frame {
    Array(Vec<Value>),
    Object(Map<String, Value>, String),
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
    ingestion: bool,
}

impl Parser<'_> {
    fn whitespace(&mut self) {
        while self
            .input
            .as_bytes()
            .get(self.pos)
            .is_some_and(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.pos += 1;
        }
    }

    fn consume(&mut self, byte: u8) -> bool {
        if self.input.as_bytes().get(self.pos) == Some(&byte) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn error(&self, message: &str, pos: usize) -> Error {
        let prefix = &self.input[..pos];
        let line = prefix.bytes().filter(|b| *b == b'\n').count() + 1;
        let column = prefix.rsplit('\n').next().unwrap_or("").chars().count() + 1;
        let character = prefix.chars().count();
        Error::new(format!(
            "{message}: line {line} column {column} (char {character})"
        ))
    }
    fn expect(&mut self, byte: u8) -> Result<()> {
        self.whitespace();
        if !self.consume(byte) {
            return Err(self.error(&format!("Expecting '{}' delimiter", byte as char), self.pos));
        }
        Ok(())
    }

    fn string(&mut self) -> Result<String> {
        self.whitespace();
        let start = self.pos;
        if !self.consume(b'"') {
            return Err(self.error(
                "Expecting property name enclosed in double quotes",
                self.pos,
            ));
        }
        while let Some(&byte) = self.input.as_bytes().get(self.pos) {
            let pos = self.pos;
            self.pos += 1;
            match byte {
                b'"' => return Ok(serde_json::from_str(&self.input[start..self.pos])?),
                0..=31 => return Err(self.error("Invalid control character at", pos)),
                b'\\' => {
                    let Some(&escape) = self.input.as_bytes().get(self.pos) else {
                        break;
                    };
                    if escape == b'u' {
                        let digits = self.input.as_bytes().get(self.pos + 1..self.pos + 5);
                        if !digits.is_some_and(|s| s.iter().all(u8::is_ascii_hexdigit)) {
                            return Err(self.error("Invalid \\uXXXX escape", self.pos));
                        }
                        self.pos += 5;
                    } else if b"\"\\/bfnrt".contains(&escape) {
                        self.pos += 1;
                    } else {
                        return Err(self.error("Invalid \\escape", pos));
                    }
                }
                _ => {}
            }
        }
        Err(self.error("Unterminated string starting at", start))
    }

    fn scalar(&mut self) -> Result<Value> {
        if self.input.as_bytes().get(self.pos) == Some(&b'"') {
            return Ok(Value::String(self.string()?));
        }
        for (text, value) in [
            ("null", Value::Null),
            ("true", Value::Bool(true)),
            ("false", Value::Bool(false)),
        ] {
            if self.input[self.pos..].starts_with(text) {
                self.pos += text.len();
                return Ok(value);
            }
        }
        if self.ingestion {
            for constant in ["NaN", "Infinity", "-Infinity"] {
                if self.input[self.pos..].starts_with(constant) {
                    self.pos += constant.len();
                    // Value has no NaN/infinity variant. Retain a nonfinite
                    // numeric lexeme so field gates run before canonical
                    // encoding rejects it. It cannot be signed or emitted.
                    return Ok(Value::Number(Number::from_str("1e9999")?));
                }
            }
        }
        let start = self.pos;
        self.consume(b'-');
        if !self.consume(b'0') {
            if !self
                .input
                .as_bytes()
                .get(self.pos)
                .is_some_and(|b| matches!(b, b'1'..=b'9'))
            {
                self.pos = start;
                return Err(self.error("Expecting value", start));
            }
            self.pos += 1;
            while self
                .input
                .as_bytes()
                .get(self.pos)
                .is_some_and(u8::is_ascii_digit)
            {
                self.pos += 1;
            }
        }
        if self.input.as_bytes().get(self.pos) == Some(&b'.')
            && self
                .input
                .as_bytes()
                .get(self.pos + 1)
                .is_some_and(u8::is_ascii_digit)
        {
            self.pos += 2;
            while self
                .input
                .as_bytes()
                .get(self.pos)
                .is_some_and(u8::is_ascii_digit)
            {
                self.pos += 1;
            }
        }
        if self
            .input
            .as_bytes()
            .get(self.pos)
            .is_some_and(|b| matches!(b, b'e' | b'E'))
        {
            let exponent = self.pos;
            self.pos += 1;
            if !self.consume(b'+') {
                self.consume(b'-');
            }
            if !self
                .input
                .as_bytes()
                .get(self.pos)
                .is_some_and(u8::is_ascii_digit)
            {
                self.pos = exponent;
            } else {
                while self
                    .input
                    .as_bytes()
                    .get(self.pos)
                    .is_some_and(u8::is_ascii_digit)
                {
                    self.pos += 1;
                }
            }
        }
        let text = &self.input[start..self.pos];
        let number = Number::from_str(text)?;
        if text.contains(['.', 'e', 'E']) {
            let f: f64 = text.parse().map_err(|_| Error::new("Invalid float"))?;
            return if self.ingestion && !f.is_finite() {
                Ok(Value::Number(number))
            } else {
                float(f)
            };
        }
        if self.ingestion && text.trim_start_matches('-').len() > crate::MAX_INTEGER_DIGITS {
            return Err(Error::new(format!(
                "Exceeds the limit (4300 digits) for integer string conversion: value has {} digits; use sys.set_int_max_str_digits() to increase the limit",
                text.trim_start_matches('-').len()
            )));
        }
        check_integer(text)?;
        Ok(Value::Number(number))
    }
}

/// Parse in one scan with an explicit stack, at most MAX_DEPTH containers deep.
/// Every value is validated before duplicate-key replacement (last value wins).
/// Strings alone use serde's decoder, so its private number marker stays data.
/// Unpaired surrogates and nonfinite floats cannot produce canonical UTF-8 bytes
/// and are rejected even if a later duplicate key would overwrite them.
pub fn parse(input: &str) -> Result<Value> {
    parse_mode(input, false)
}
fn parse_mode(input: &str, ingestion: bool) -> Result<Value> {
    let mut parser = Parser {
        input,
        pos: 0,
        ingestion,
    };
    if input.starts_with('\u{feff}') {
        return Err(parser.error("Unexpected UTF-8 BOM (decode using utf-8-sig)", 0));
    }
    let mut stack = Vec::new();
    let mut completed = None;
    loop {
        let value = match completed.take() {
            Some(value) => value,
            None => {
                parser.whitespace();
                let next = input.as_bytes().get(parser.pos);
                if matches!(next, Some(b'[' | b'{')) && stack.len() >= MAX_DEPTH {
                    return Err(Error::new("JSON nesting exceeds 512 containers"));
                }
                if parser.consume(b'[') {
                    parser.whitespace();
                    if parser.consume(b']') {
                        Value::Array(Vec::new())
                    } else {
                        stack.push(Frame::Array(Vec::new()));
                        continue;
                    }
                } else if parser.consume(b'{') {
                    parser.whitespace();
                    if parser.consume(b'}') {
                        Value::Object(Map::new())
                    } else {
                        let key = parser.string()?;
                        parser.expect(b':')?;
                        stack.push(Frame::Object(Map::new(), key));
                        continue;
                    }
                } else {
                    parser.scalar()?
                }
            }
        };
        match stack.last_mut() {
            Some(Frame::Array(items)) => {
                items.push(value);
                parser.whitespace();
                if parser.consume(b']') {
                    let Some(Frame::Array(items)) = stack.pop() else {
                        unreachable!()
                    };
                    completed = Some(Value::Array(items));
                } else {
                    parser.expect(b',')?;
                }
            }
            Some(Frame::Object(fields, key)) => {
                fields.insert(std::mem::take(key), value);
                parser.whitespace();
                if parser.consume(b'}') {
                    let Some(Frame::Object(fields, _)) = stack.pop() else {
                        unreachable!()
                    };
                    completed = Some(Value::Object(fields));
                } else {
                    parser.expect(b',')?;
                    *key = parser.string()?;
                    parser.expect(b':')?;
                }
            }
            None => {
                parser.whitespace();
                if parser.pos != input.len() {
                    return Err(parser.error("Extra data", parser.pos));
                }
                return Ok(value);
            }
        }
    }
}

/// Decode an asset as Python's Path.read_text + json.loads do. Syntax errors
/// use Unicode character positions; text-mode universal newlines are applied.
/// Canonical parse remains strict about nonfinite numbers.
pub fn decode_input(raw: &[u8]) -> Result<Value> {
    let text = std::str::from_utf8(raw).map_err(|e| {
        let start = e.valid_up_to();
        let end = start + e.error_len().unwrap_or(raw.len() - start);
        let location = if end == start + 1 {
            format!("byte 0x{:02x} in position {start}", raw[start])
        } else {
            format!("bytes in position {start}-{}", end - 1)
        };
        let reason = if e.error_len().is_none() {
            "unexpected end of data"
        } else if matches!(raw[start], 0xc2..=0xf4) {
            "invalid continuation byte"
        } else {
            "invalid start byte"
        };
        Error::new(format!("'utf-8' codec can't decode {location}: {reason}"))
    })?;
    let text = text.replace("\r\n", "\n").replace('\r', "\n");
    parse_mode(&text, true)
}
