//! Byte contracts shared with the Python engine; no I/O or ledger writes.
//!
//! Use [`parse`] for incoming JSON, then [`canonical`] for the UTF-8 encoding
//! used by digests and signatures. In particular, integers never pass through
//! a float, and floats are rounded to binary64 before Python-style formatting.

mod identity;
mod parser;
mod serializer;
mod signing;
mod workflow;

pub use identity::*;
pub use parser::parse;
pub use serde_json::Value;
pub use serializer::{SerializeOptions, canonical, dumps};
pub use signing::*;
pub use workflow::*;

use serde_json::Map;
use sha2::{Digest, Sha256};
use std::fmt;

#[derive(Debug)]
pub struct Error(String);

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Maximum number of nested arrays/objects accepted by parse and dumps.
pub const MAX_DEPTH: usize = 512;
/// CPython's default int_max_str_digits limit (the sign does not count).
pub const MAX_INTEGER_DIGITS: usize = 4300;

pub(crate) fn check_integer(text: &str) -> Result<()> {
    if text.trim_start_matches('-').len() > MAX_INTEGER_DIGITS {
        return Err(Error::new("Integer exceeds 4300 decimal digits"));
    }
    Ok(())
}

/// Construct a binary64 JSON value, rejecting NaN and infinities before
/// serde_json can silently turn them into null. Use this for Rust floats.
pub fn float(number: f64) -> Result<Value> {
    serde_json::Number::from_f64(number)
        .map(Value::Number)
        .ok_or_else(|| Error::new("Out of range float values are not JSON compliant"))
}

/// Shortest round-trip binary64 digits, with Python's fixed/scientific
/// boundary (-4 <= decimal exponent < 16), signed two-digit exponents,
/// integral .0, and signed zero. Rejects NaN and both infinities.
pub fn python_float(number: f64) -> Result<String> {
    if !number.is_finite() {
        return Err(Error::new(
            "Out of range float values are not JSON compliant",
        ));
    }
    if number == 0.0 {
        return Ok(if number.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        }
        .into());
    }
    let mut buffer = ryu::Buffer::new();
    let repr = buffer.format_finite(number.abs());
    let (mantissa, exponent) = match repr.split_once('e') {
        Some((m, e)) => (m, e.parse::<i32>().expect("Ryū exponent")),
        None => (repr, 0),
    };
    let decimal = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let leading = digits.len() - digits.trim_start_matches('0').len();
    let power = exponent + decimal - leading as i32 - 1;
    let digits = digits.trim_start_matches('0').trim_end_matches('0');
    let mut output = if number.is_sign_negative() { "-" } else { "" }.to_owned();
    if !(-4..16).contains(&power) {
        output.push_str(&digits[..1]);
        if digits.len() > 1 {
            output.push('.');
            output.push_str(&digits[1..]);
        }
        output.push_str(&format!("e{power:+03}"));
    } else if power < 0 {
        output.push_str("0.");
        output.extend(std::iter::repeat_n('0', (-power - 1) as usize));
        output.push_str(digits);
    } else {
        let point = power as usize + 1;
        if point >= digits.len() {
            output.push_str(digits);
            output.extend(std::iter::repeat_n('0', point - digits.len()));
            output.push_str(".0");
        } else {
            output.push_str(&digits[..point]);
            output.push('.');
            output.push_str(&digits[point..]);
        }
    }
    Ok(output)
}

pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Also the Python request hash, result hash, event ID and packet hash.
pub fn digest(value: &Value) -> Result<String> {
    Ok(sha256(canonical(value)?.as_bytes()))
}

pub(crate) fn field<'a>(value: &'a Value, key: &str) -> Result<&'a Value> {
    value
        .get(key)
        .ok_or_else(|| Error::new(format!("Missing field: {key}")))
}

pub(crate) fn object(value: &Value) -> Result<&Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| Error::new("Expected object"))
}
