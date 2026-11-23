use crate::{
    Error, Result, SerializeOptions, Value, canonical, digest, dumps, field, python_float, sha256,
};
use serde_json::json;

/// Exact control.input_identity receipt key. GitHub asset IDs are integers or
/// strings; missing IDs, null and the other JSON scalars follow Python str().
pub fn input_identity(asset: &Value) -> Result<String> {
    let id = match asset.get("id") {
        None => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) => "None".into(),
        Some(Value::Bool(b)) => if *b { "True" } else { "False" }.into(),
        Some(Value::Number(n)) => {
            if n.as_str().contains(['.', 'e', 'E']) {
                python_float(
                    n.as_str()
                        .parse()
                        .map_err(|_| Error::new("Invalid asset ID"))?,
                )?
            } else {
                canonical(&Value::Number(n.clone()))?
            }
        }
        _ => return Err(Error::new("Asset ID must be a JSON scalar")),
    };
    canonical(&json!([
        id,
        field(asset, "name")?,
        asset.get("digest").unwrap_or(&Value::Null)
    ]))
}

pub fn workflow_digest(workflow: &Value, review: &Value) -> Result<String> {
    digest(&json!({"workflow": workflow, "review": review}))
}

pub fn attempt_key(item: &str, version: &Value, content_hash: &str) -> Result<String> {
    digest(&json!([item, version, content_hash]))
}

pub fn registry_identity(prefix: &str, text: &str) -> String {
    format!("{prefix}-{}", &sha256(text.as_bytes())[..16])
}

/// registry.record_event / sent_events.record_page: default spaced separators,
/// sorted keys, raw Unicode, and Python's default allow_nan (finite domain).
pub fn event_identity(event: &Value) -> Result<String> {
    Ok(registry_identity(
        "event",
        &dumps(
            event,
            SerializeOptions {
                ensure_ascii: false,
                sort_keys: true,
                ..SerializeOptions::default()
            },
        )?,
    ))
}

pub fn request_id(kind: &str, identity: &Value) -> Result<String> {
    Ok(format!("{kind}-{}", &digest(identity)?[..32]))
}

pub fn staged_asset_name(prefix: &str, value: &Value) -> Result<String> {
    Ok(format!("{prefix}-{}.json", digest(value)?))
}

pub fn artifact_key(repository: Option<&str>, artifact: &Value) -> Result<String> {
    digest(&json!([
        repository,
        field(artifact, "release_tag")?,
        field(artifact, "name")?,
        field(artifact, "bytes")?,
        field(artifact, "sha256")?
    ]))
}

pub fn source_identity(row: &Value) -> Result<String> {
    let upstream = field(row, "upstream")?;
    let mut declarations = field(upstream, "declaration_gids")?
        .as_array()
        .ok_or_else(|| Error::new("Expected declaration list"))?
        .iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| Error::new("Expected declaration string"))
        })
        .collect::<Result<Vec<_>>>()?;
    declarations.sort();
    digest(
        &json!({"id": field(row, "id")?, "repository": field(upstream, "repository")?,
        "kind": field(upstream, "resolution_kind")?, "declarations": declarations}),
    )
}

pub fn delivery_tokens(packet: &Value) -> Result<Vec<String>> {
    fn items<'a>(value: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
        field(value, key)?
            .as_array()
            .ok_or_else(|| Error::new("Expected array"))
    }
    match field(packet, "kind")?.as_str() {
        Some("reply") => Ok(vec![digest(&json!([
            "reply",
            field(packet, "thread_id")?,
            field(packet, "latest_inbound_message_id")?
        ]))?]),
        Some("firstcontact") => {
            let mut tokens = Vec::new();
            for recipient in items(packet, "to")?.iter().chain(items(packet, "cc")?) {
                for problem in items(packet, "problem_ids")? {
                    tokens.push(digest(&json!(["firstcontact", recipient, problem]))?);
                }
            }
            Ok(tokens)
        }
        _ => items(packet, "covered_days")?
            .iter()
            .map(|day| digest(&json!(["daily", field(packet, "account")?, day])))
            .collect(),
    }
}
