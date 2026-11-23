//! Pure read-only operator views. Adapters supply data, configuration and time.
mod digest;
mod exceptions;
mod lanes;
mod registry;
mod reviews;
mod status;
mod text;
pub use digest::review_digest;
pub use exceptions::{exceptions, render_exceptions};
use outreach_canon::{Error, Result, Value};
pub use reviews::reviews;
pub use status::{StatusInput, status};
pub use text::{casefold, py_str, truth};

pub fn object<const N: usize>(fields: [(&str, Value); N]) -> Value {
    Value::Object(fields.into_iter().map(|(k, v)| (k.into(), v)).collect())
}
pub fn array(value: &Value) -> &[Value] {
    value.as_array().map(Vec::as_slice).unwrap_or(&[])
}
pub fn s(value: &Value) -> &str {
    value.as_str().unwrap_or("")
}
pub fn fallback<'a>(values: &[&'a Value]) -> &'a Value {
    values
        .iter()
        .copied()
        .find(|v| truth(v))
        .unwrap_or(&Value::Null)
}
pub fn string(value: impl Into<String>) -> Value {
    Value::String(value.into())
}
pub fn regex(pattern: &str) -> Result<regex::Regex> {
    regex::Regex::new(pattern).map_err(|e| Error::new(format!("Unsupported registry regex: {e}")))
}
/// Filesystem-free environment. Table values and workflow hashes are supplied
/// by the CLI; empty tables are explicit fixtures, never silent I/O fallbacks.
pub struct Environment {
    pub config: Value,
    pub tables: Value,
    pub workflows: Value,
}
