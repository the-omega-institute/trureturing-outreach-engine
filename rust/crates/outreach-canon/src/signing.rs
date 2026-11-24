use crate::{Error, Result, Value, canonical, field, object};
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;
use subtle::ConstantTimeEq;

/// The cryptographic primitive shared by all Python envelope families.
/// Key bytes are the UTF-8 encoding of the Python secret string.
pub fn hmac_sha256(value: &Value, key: &[u8]) -> Result<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(canonical(value)?.as_bytes());
    Ok(mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

/// Sign every field of an unsigned object, refusing accidental double signing.
pub fn sign_envelope(unsigned: &Value, key: &[u8]) -> Result<Value> {
    sign(unsigned, key, false)
}

pub fn sign_mathesis_envelope(unsigned: &Value, key: &[u8]) -> Result<Value> {
    sign(unsigned, key, true)
}

fn sign(unsigned: &Value, key: &[u8], mathesis: bool) -> Result<Value> {
    let mut signed = object(unsigned)?.clone();
    if signed.contains_key("signature") {
        return Err(Error::new("Envelope is already signed"));
    }
    let signature = if mathesis {
        mathesis_signature(unsigned, key)?
    } else {
        hmac_sha256(unsigned, key)?
    };
    signed.insert("signature".into(), Value::String(signature));
    Ok(Value::Object(signed))
}

/// Mathesis uses an additional purpose wrapper; plain envelope HMACs are invalid.
pub fn mathesis_signature(unsigned: &Value, key: &[u8]) -> Result<String> {
    if key.is_empty() {
        return Err(Error::new("The local operator signing key is required"));
    }
    hmac_sha256(
        &json!({"purpose": "mathesis-delivery", "envelope": unsigned}),
        key,
    )
}

/// Verify the exact lowercase hex string in constant time for equal-length
/// signatures. No case folding, hex normalization, or field allowlist projection.
/// This checks cryptography only; use verify_typed_envelope for the field gate.
pub fn verify_envelope(signed: &Value, key: &[u8]) -> Result<bool> {
    verify(signed, key, false)
}

pub fn verify_mathesis_envelope(signed: &Value, key: &[u8]) -> Result<bool> {
    verify(signed, key, true)
}

fn verify(signed: &Value, key: &[u8], mathesis: bool) -> Result<bool> {
    if key.is_empty() {
        return Ok(false);
    }
    let mut unsigned = object(signed)?.clone();
    let Some(Value::String(signature)) = unsigned.shift_remove("signature") else {
        return Ok(false);
    };
    let unsigned = Value::Object(unsigned);
    let expected = if mathesis {
        mathesis_signature(&unsigned, key)?
    } else {
        hmac_sha256(&unsigned, key)?
    };
    Ok(bool::from(expected.as_bytes().ct_eq(signature.as_bytes())))
}

#[derive(Clone, Copy, Debug)]
pub enum EnvelopeKind {
    Review,
    Decision,
    PoolAdopt,
    FailedRequeue,
    DraftRevision,
    SubmissionReview,
    MathesisDelivery,
}

/// The admission field sets and signature domain from tasks.py/control.py/
/// mathesis_delivery.py. Ledger bindings, permissions and semantic checks still
/// belong to the coordinator; passing this function never authorizes an effect.
pub fn verify_typed_envelope(signed: &Value, key: &[u8], kind: EnvelopeKind) -> Result<bool> {
    if key.is_empty() {
        return Ok(false);
    }
    let fields = object(signed)?;
    let required: &[&str] = match kind {
        EnvelopeKind::Review => &[
            "binding",
            "reviewer",
            "verdict",
            "reason",
            "evidence_ref",
            "signature",
        ],
        EnvelopeKind::Decision => &[
            "schema_version",
            "task_id",
            "result_hash",
            "answers",
            "evidence_ref",
            "reviewer",
            "signature",
        ],
        EnvelopeKind::PoolAdopt => &[
            "schema_version",
            "sandbox_id",
            "workspace_id",
            "group",
            "reason",
            "signature",
        ],
        EnvelopeKind::FailedRequeue => &[
            "schema_version",
            "task_id",
            "request_hash",
            "failed_revision",
            "reason",
            "signature",
        ],
        EnvelopeKind::DraftRevision => &[
            "schema_version",
            "task_id",
            "old_packet_hash",
            "new_packet_hash",
            "reason",
            "packet",
            "signature",
        ],
        EnvelopeKind::SubmissionReview => &[
            "schema_version",
            "paper_id",
            "task_id",
            "result_hash",
            "signature",
        ],
        EnvelopeKind::MathesisDelivery => &[
            "schema_version",
            "task_id",
            "result_hash",
            "workflow_hash",
            "decision_ref",
            "metadata",
            "source",
            "reviewer",
            "signature",
        ],
    };
    let mut expected = required.to_vec();
    match kind {
        EnvelopeKind::Review => {
            for name in ["rebind_workflow_hash", "edited_body"] {
                if fields.contains_key(name) {
                    expected.push(name);
                }
            }
            if validate_rebind(signed).is_err() {
                return Ok(false);
            }
        }
        EnvelopeKind::DraftRevision if fields.contains_key("reviewer") => expected.push("reviewer"),
        EnvelopeKind::MathesisDelivery
            if signed.pointer("/metadata/schema").and_then(Value::as_str)
                == Some("mathesis.pre-reviewed-delivery.v1") =>
        {
            expected.push("pdf")
        }
        _ => {}
    }
    if fields.len() != expected.len() || expected.iter().any(|k| !fields.contains_key(*k)) {
        return Ok(false);
    }
    // These two recovery paths and submission-review check schema before HMAC.
    if matches!(
        kind,
        EnvelopeKind::PoolAdopt | EnvelopeKind::FailedRequeue | EnvelopeKind::SubmissionReview
    ) && !python_one(field(signed, "schema_version")?)
    {
        return Ok(false);
    }
    verify(signed, key, matches!(kind, EnvelopeKind::MathesisDelivery))
}

fn python_one(value: &Value) -> bool {
    // Python == 1 accepts True and 1.0 here (the reference does not use type()).
    value == &Value::Bool(true)
        || value
            .as_number()
            .is_some_and(|n| n.as_str().parse::<f64>() == Ok(1.0))
}

fn validate_rebind(value: &Value) -> Result<()> {
    if let Some(hash) = value.get("rebind_workflow_hash") {
        if value.get("verdict").and_then(Value::as_str) != Some("PASS") {
            return Err(Error::new("Only PASS may rebind a workflow"));
        }
        if !hash.as_str().is_some_and(|s| {
            s.len() == 64
                && s.bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        }) {
            return Err(Error::new("Invalid workflow rebind digest"));
        }
    }
    Ok(())
}

/// Pure equivalent of control.sign_review. The caller supplies the workflow
/// hash resolved for kind; this crate does not read repositories or policies.
pub fn sign_review(
    envelope: &Value,
    key: &[u8],
    kind: Option<&str>,
    current_hash: Option<&str>,
) -> Result<Value> {
    let mut unsigned = object(envelope)?.clone();
    if unsigned.contains_key("signature") {
        return Err(Error::new("Envelope is already signed"));
    }
    if let Some(body) = unsigned.get("edited_body") {
        if envelope.get("verdict").and_then(Value::as_str) != Some("PASS") {
            return Err(Error::new("Only PASS may include edited_body"));
        }
        if kind.is_some_and(|k| !["reply", "firstcontact"].contains(&k)) {
            return Err(Error::new(
                "edited_body is only allowed for reply and firstcontact",
            ));
        }
        if !body.as_str().is_some_and(|s| {
            s.chars()
                .any(|c| !c.is_whitespace() && !('\u{1c}'..='\u{1f}').contains(&c))
        }) {
            return Err(Error::new("edited_body must be a nonempty string"));
        }
    }
    let known_kind = kind.is_some_and(|k| crate::KINDS.contains(&k));
    let current = if known_kind {
        Some(current_hash.ok_or_else(|| Error::new("Supply the current workflow hash for kind"))?)
    } else {
        None
    };
    if let Some(current) = current
        && envelope.get("verdict").and_then(Value::as_str) == Some("PASS")
        && !unsigned.contains_key("rebind_workflow_hash")
        && field(field(envelope, "binding")?, "workflow_hash")?.as_str() != Some(current)
    {
        unsigned.insert("rebind_workflow_hash".into(), Value::String(current.into()));
    }
    let unsigned = Value::Object(unsigned);
    validate_rebind(&unsigned)?;
    if let Some(hash) = unsigned.get("rebind_workflow_hash") {
        if !known_kind {
            return Err(Error::new(
                "Workflow rebind requires --kind from the bound task request",
            ));
        }
        if hash.as_str() != current {
            return Err(Error::new(
                "Workflow rebind must match the current workflow for the task kind",
            ));
        }
    }
    sign_envelope(&unsigned, key)
}
