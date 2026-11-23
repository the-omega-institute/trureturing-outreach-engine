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

    fn expect(&mut self, byte: u8) -> Result<()> {
        self.whitespace();
        if !self.consume(byte) {
            return Err(Error::new(format!(
                "Expected {} at byte {}",
                byte as char, self.pos
            )));
        }
        Ok(())
    }

    fn string(&mut self) -> Result<String> {
        self.whitespace();
        let start = self.pos;
        self.expect(b'"')?;
        while let Some(&byte) = self.input.as_bytes().get(self.pos) {
            self.pos += 1;
            match byte {
                b'"' => return Ok(serde_json::from_str(&self.input[start..self.pos])?),
                b'\\' => self.pos += 1,
                _ => {}
            }
        }
        Err(Error::new("Unterminated JSON string"))
    }

    fn scalar(&mut self) -> Result<Value> {
        if self.input.as_bytes().get(self.pos) == Some(&b'"') {
            return Ok(Value::String(self.string()?));
        }
        let start = self.pos;
        while self
            .input
            .as_bytes()
            .get(self.pos)
            .is_some_and(|b| !matches!(b, b' ' | b'\n' | b'\r' | b'\t' | b',' | b']' | b'}'))
        {
            self.pos += 1;
        }
        let text = &self.input[start..self.pos];
        Ok(match text {
            "null" => Value::Null,
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => {
                let number = Number::from_str(text)?;
                if text.contains(['.', 'e', 'E']) {
                    return float(text.parse().map_err(|_| Error::new("Invalid float"))?);
                }
                check_integer(text)?;
                Value::Number(number)
            }
        })
    }
}

/// Parse in one scan with an explicit stack, at most MAX_DEPTH containers deep.
/// Every value is validated before duplicate-key replacement (last value wins).
/// Strings alone use serde's decoder, so its private number marker stays data.
/// Unpaired surrogates and nonfinite floats cannot produce canonical UTF-8 bytes
/// and are rejected even if a later duplicate key would overwrite them.
pub fn parse(input: &str) -> Result<Value> {
    let mut parser = Parser { input, pos: 0 };
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
                    return Err(Error::new("Trailing JSON data"));
                }
                return Ok(value);
            }
        }
    }
}
