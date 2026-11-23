use crate::{Result, Value, digest, field, parse};
use serde_json::Map;

pub const KINDS: &[&str] = &[
    "firstcontact",
    "reply",
    "research",
    "shortnote",
    "daily",
    "mathesis",
    "submission",
];

/// Mirror control.workflow using already-resolved file text. read returns None
/// only for a missing file. Permit a missing voice guide only for the historical
/// source-only pin case (revision != None and policy_revision == None).
/// Repository/overlay selection and git I/O belong to a future adapter.
pub fn workflow_inputs(
    kind: &str,
    allow_historical_missing_voice: bool,
    mut read: impl FnMut(&str) -> Result<Option<String>>,
) -> Result<Value> {
    let mut paths = match kind {
        "reply" => vec![
            "replies/workflow.json",
            "replies/review.md",
            "firstcontact/review.md",
        ],
        "daily" => vec![
            "daily/workflow.json",
            "daily/DESIGN-notes.md",
            "firstcontact/review.md",
        ],
        "firstcontact" => vec!["firstcontact/workflow.json", "firstcontact/review.md"],
        "submission" => vec![
            "submission/workflow.json",
            "submission/review.md",
            "research/review.md",
            "submission/author-policy.json",
        ],
        _ => vec!["research/workflow.json", "research/review.md"],
    };
    if kind != "daily" {
        paths.push("runtime.py");
    }
    let mut inputs = Map::new();
    for path in paths {
        let text = read(path)?
            .ok_or_else(|| crate::Error::new(format!("Missing workflow file: {path}")))?;
        inputs.insert(path.into(), Value::String(text));
    }
    if matches!(kind, "reply" | "firstcontact") {
        let path = "replies/voice.md";
        match read(path)? {
            Some(text) => {
                inputs.insert(path.into(), Value::String(text));
            }
            None if allow_historical_missing_voice => {}
            None => return Err(crate::Error::new("Missing workflow file: replies/voice.md")),
        }
    }
    let automation = parse(
        &read("automation.json")?.ok_or_else(|| crate::Error::new("Missing automation.json"))?,
    )?;
    let mut selected = Map::new();
    for key in ["schema_version", "routing_contract", "continuation"] {
        selected.insert(key.into(), field(&automation, key)?.clone());
    }
    if matches!(kind, "reply" | "firstcontact") {
        selected.insert("mail".into(), field(&automation, "mail")?.clone());
    }
    let keys: &[&str] = match kind {
        "daily" => &["daily"],
        "research" | "shortnote" | "mathesis" => &["coverage", "public_updates"],
        _ => &[],
    };
    if !keys.is_empty() {
        let publication = field(&automation, "publication")?;
        let mut subset = Map::new();
        for key in keys {
            subset.insert((*key).into(), field(publication, key)?.clone());
        }
        selected.insert("publication".into(), Value::Object(subset));
    }
    inputs.insert("automation.json".into(), Value::Object(selected));
    Ok(Value::Object(inputs))
}

pub fn workflow(
    kind: &str,
    allow_historical_missing_voice: bool,
    read: impl FnMut(&str) -> Result<Option<String>>,
) -> Result<String> {
    digest(&workflow_inputs(
        kind,
        allow_historical_missing_voice,
        read,
    )?)
}
