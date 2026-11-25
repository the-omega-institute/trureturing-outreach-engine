//! Pure read-only operator views. Adapters supply data, configuration and time.
pub mod builders;
mod digest;
pub mod envelopes;
mod exceptions;
pub mod gates;
pub mod ingest;
mod lanes;
pub mod mathesis_delivery;
pub mod persistence;
pub mod publication;
mod registry;
pub mod results;
mod reviews;
pub mod sent_events;
mod status;
mod text;
pub mod transitions;
pub mod validation;
pub use digest::review_digest;
pub use exceptions::{exceptions, render_exceptions};
pub use lanes::validate as validate_lanes;
use outreach_canon::{Error, Result, Value};
pub use reviews::{reviewable, reviews};
pub use status::{StatusInput, status};
pub const OWNER_QUESTION_AGE: f64 = 2.0 * 86400.0;
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
pub struct Environment<'a> {
    pub config: Value,
    pub tables: Value,
    pub workflow: &'a dyn Fn(&str) -> Result<Value>,
}
