use crate::{Result, Value, parse};
use serde::{Deserialize, Deserializer, de::Error};
use std::ops::Deref;

/// Validated JSON. Incoming values can only be deserialized through the bounded
/// canon parser, including arbitrary integers, binary64 rounding and key order.
/// There is deliberately no mutable dereference or conversion from serde Value.
#[derive(Clone, Debug, PartialEq)]
pub struct CanonValue(Value);

impl CanonValue {
    pub fn parse(input: &str) -> Result<Self> {
        parse(input).map(Self)
    }
    pub fn into_value(self) -> Value {
        self.0
    }
    pub fn value(&self) -> &Value {
        &self.0
    }
}

impl Deref for CanonValue {
    type Target = Value;
    fn deref(&self) -> &Value {
        &self.0
    }
}

impl<'de> Deserialize<'de> for CanonValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
        Self::parse(raw.get()).map_err(D::Error::custom)
    }
}

fn integer(value: &Value) -> Option<String> {
    match value {
        Value::Bool(b) => Some(if *b { "1" } else { "0" }.into()),
        Value::Number(n) => {
            let s = n.as_str();
            if s.contains(['.', 'e', 'E']) {
                let f = s.parse::<f64>().ok()?;
                // Fixed precision zero emits the exact integral binary64 value;
                // converting the integer to f64 would lose comparison precision.
                (f.is_finite() && f.fract() == 0.0).then(|| {
                    if f == 0.0 {
                        "0".into()
                    } else {
                        format!("{f:.0}")
                    }
                })
            } else {
                Some(if s == "-0" { "0" } else { s }.into())
            }
        }
        _ => None,
    }
}

/// Python JSON-domain equality, including recursive numeric/bool equality and
/// exact comparisons between arbitrary integers and integral binary64 values.
pub fn python_eq(left: &Value, right: &Value) -> bool {
    let mut pending = vec![(left, right)];
    while let Some((a, b)) = pending.pop() {
        match (a, b) {
            (Value::Bool(_) | Value::Number(_), Value::Bool(_) | Value::Number(_)) => {
                match (integer(a), integer(b)) {
                    (Some(a), Some(b)) if a == b => {}
                    (None, None) => {
                        if a.as_f64() != b.as_f64() {
                            return false;
                        }
                    }
                    _ => return false,
                }
            }
            (Value::Array(a), Value::Array(b)) if a.len() == b.len() => {
                pending.extend(a.iter().zip(b))
            }
            (Value::Object(a), Value::Object(b)) if a.len() == b.len() => {
                for (key, value) in a {
                    let Some(other) = b.get(key) else {
                        return false;
                    };
                    pending.push((value, other));
                }
            }
            _ if a == b => {}
            _ => return false,
        }
    }
    true
}
