//! Lossless typed envelopes and offline asset construction. No I/O or effects.
use crate::{validation::*, *};
use outreach_canon::{CanonValue, canonical, digest, sign_envelope, verify_envelope};
use outreach_model::{Binding, TaskKind, Verdict};

#[derive(Clone, Debug)]
pub struct ReviewEnvelope {
    pub binding: Binding,
    pub verdict: Verdict,
    pub edited_body: Option<String>,
    pub rebind_workflow_hash: Option<String>,
    pub raw: CanonValue,
}
#[derive(Clone, Debug)]
pub enum DecisionAction {
    Answers(CanonValue),
    Close { reason: String },
}
#[derive(Clone, Debug)]
pub struct DecisionEnvelope {
    pub task_id: String,
    pub action: DecisionAction,
    pub raw: CanonValue,
}
#[derive(Clone, Debug)]
pub struct DraftRevisionEnvelope {
    pub task_id: String,
    pub packet: CanonValue,
    pub raw: CanonValue,
}
#[derive(Clone, Debug)]
pub struct RequestEnvelope {
    pub kind: TaskKind,
    pub raw: CanonValue,
}
fn canon(v: &Value) -> Result<CanonValue> {
    CanonValue::parse(&canonical(v)?)
}
fn key(key: &[u8]) -> Result<()> {
    require(
        !key.is_empty(),
        "Reviewer verification key is not configured",
    )
}
fn signature(v: &Value, key: &[u8], message: &str) -> Result<()> {
    require(verify_envelope(v, key)?, message)
}
pub fn validate_review(v: &Value, secret: &[u8]) -> Result<()> {
    key(secret)?;
    require(
        keys(
            v,
            &[
                "binding",
                "reviewer",
                "verdict",
                "reason",
                "evidence_ref",
                "signature",
            ],
            &["rebind_workflow_hash", "edited_body"],
        ),
        "Invalid signed review",
    )?;
    rebind(v)?;
    signature(v, secret, "Invalid signed review: review signature failed")?;
    let independent = independent_reviewer(&v["reviewer"])?;
    if independent {
        hashable(&v["verdict"])?;
    }
    require(
        independent && ["PASS", "BOUNCE", "HOLD"].contains(&s(&v["verdict"])),
        "Invalid reviewer/verdict",
    )?;
    require(
        nonempty(&v["reason"]) && nonempty(&v["evidence_ref"]),
        "Review needs reasoning and evidence",
    )
}
impl ReviewEnvelope {
    pub fn parse(raw: &str, secret: &[u8]) -> Result<Self> {
        let raw = CanonValue::parse(raw)?;
        validate_review(&raw, secret)?;
        Ok(Self {
            binding: Binding::parse(&canonical(&raw["binding"])?)?,
            verdict: s(&raw["verdict"]).parse()?,
            edited_body: raw
                .get("edited_body")
                .and_then(Value::as_str)
                .map(String::from),
            rebind_workflow_hash: raw
                .get("rebind_workflow_hash")
                .and_then(Value::as_str)
                .map(String::from),
            raw,
        })
    }
}
pub fn validate_decision(v: &Value, secret: &[u8]) -> Result<()> {
    key(secret)?;
    let base = [
        "schema_version",
        "task_id",
        "result_hash",
        "evidence_ref",
        "reviewer",
        "signature",
    ];
    require(
        keys(v, &[base.as_slice(), &["answers"]].concat(), &[])
            || keys(v, &[base.as_slice(), &["action", "reason"]].concat(), &[]),
        "Invalid user decision envelope",
    )?;
    if v.get("action").is_some() {
        require(
            v["action"] == "close" && nonempty(&v["reason"]),
            "Question closure needs a reason",
        )?;
    }
    signature_type(v, false, "Decision signature failed")?;
    signature(v, secret, "Decision signature failed")?;
    require(
        one(&v["schema_version"]) && v["reviewer"] == "local-claude-loop",
        "Only the local operator records user decisions",
    )?;
    require(
        nonempty(&v["evidence_ref"]),
        "Preserve the evidence source for the decision",
    )
}
impl DecisionEnvelope {
    pub fn parse(raw: &str, secret: &[u8]) -> Result<Self> {
        let raw = CanonValue::parse(raw)?;
        validate_decision(&raw, secret)?;
        let action = if raw.get("action").is_some() {
            DecisionAction::Close {
                reason: s(&raw["reason"]).into(),
            }
        } else {
            DecisionAction::Answers(canon(&raw["answers"])?)
        };
        Ok(Self {
            task_id: s(&raw["task_id"]).into(),
            action,
            raw,
        })
    }
}
pub fn validate_draft_revision(v: &Value, secret: &[u8]) -> Result<()> {
    key(secret)?;
    require(
        keys(
            v,
            &[
                "schema_version",
                "task_id",
                "old_packet_hash",
                "new_packet_hash",
                "reason",
                "packet",
                "signature",
            ],
            &["reviewer"],
        ),
        "Invalid draft revision envelope",
    )?;
    signature_type(v, false, "Draft revision signature failed")?;
    signature(v, secret, "Draft revision signature failed")?;
    require(
        one(&v["schema_version"]) && nonempty(&v["task_id"]),
        "Invalid draft revision identity",
    )?;
    if v.get("reviewer").is_some() {
        require(
            v["reviewer"] == "local-claude-loop",
            "Only the local operator stages draft revisions",
        )?;
    }
    require(hex(&v["old_packet_hash"], 64), "Invalid old packet hash")?;
    require(hex(&v["new_packet_hash"], 64), "Invalid new packet hash")?;
    require(nonempty(&v["reason"]), "Draft revision needs a reason")?;
    packet(&v["packet"])?;
    require(
        v["new_packet_hash"] == digest(&v["packet"])?,
        "New packet hash does not match the staged packet",
    )
}
impl DraftRevisionEnvelope {
    pub fn parse(raw: &str, secret: &[u8]) -> Result<Self> {
        let raw = CanonValue::parse(raw)?;
        validate_draft_revision(&raw, secret)?;
        Ok(Self {
            task_id: s(&raw["task_id"]).into(),
            packet: canon(&raw["packet"])?,
            raw,
        })
    }
}
impl RequestEnvelope {
    pub fn parse(raw: &str) -> Result<Self> {
        let raw = CanonValue::parse(raw)?;
        request(&raw)?;
        Ok(Self {
            kind: s(&raw["kind"]).parse()?,
            raw,
        })
    }
}
#[derive(Clone, Debug)]
pub struct StagedAsset {
    pub name: String,
    pub bytes: Vec<u8>,
}
pub fn stage(v: &Value, prefix: &str) -> Result<StagedAsset> {
    require(
        [
            "request",
            "review",
            "decision",
            "draft-revision",
            "submission-review",
            "mathesis",
            "local-result",
            "prompt-requeue",
            "failed-requeue",
            "pool-adopt",
            "formalization-backfill",
            "prior-work-backfill",
        ]
        .contains(&prefix),
        "Unknown input family",
    )?;
    Ok(StagedAsset {
        name: format!("{prefix}-{}.json", digest(v)?),
        bytes: canonical(v)?.into_bytes(),
    })
}
pub fn build_decision(unsigned: &Value, secret: &[u8]) -> Result<Value> {
    let v = sign_envelope(unsigned, secret)?;
    validate_decision(&v, secret)?;
    Ok(v)
}
pub fn build_draft_revision(
    row: &Value,
    new_packet: &Value,
    reason: &str,
    secret: &[u8],
) -> Result<Value> {
    require(
        row["state"] == "owner-draft",
        "Draft revision requires an owner-draft task",
    )?;
    let old = if truth(&row["data"]["packet_override"]) {
        &row["data"]["packet_override"]
    } else {
        &row["data"]["result"]["packet"]
    };
    let receipt = crate::fallback(&[&row["data"]["draft_receipt"]]);
    require(
        old.is_object() && (receipt.is_object() || !truth(receipt)),
        "Owner draft has no reviewed packet or receipt",
    )?;
    require(
        receipt["content_hash"].is_string() && receipt["content_hash"] == digest(old)?,
        "Owner draft receipt does not match its reviewed packet",
    )?;
    require(
        nonempty(&receipt["remote_id"]),
        "Owner draft receipt has no remote ID",
    )?;
    require(
        !reason.trim_matches(crate::text::whitespace).is_empty(),
        "Draft revision needs a reason",
    )?;
    packet(new_packet)?;
    require(
        old.as_object().is_some_and(|o| {
            o.iter().all(|(k, v)| {
                ["body", "attachments"].contains(&k.as_str())
                    || outreach_canon::python_eq(v, &new_packet[k])
            })
        }) && old.as_object().map(|o| o.len()) == new_packet.as_object().map(|o| o.len()),
        "Draft revision may change only body and attachments",
    )?;
    sign_envelope(
        &object([
            ("schema_version", 1.into()),
            ("task_id", row["id"].clone()),
            ("old_packet_hash", receipt["content_hash"].clone()),
            ("new_packet_hash", string(digest(new_packet)?)),
            ("reason", string(reason)),
            ("packet", new_packet.clone()),
        ]),
        secret,
    )
}
/// Build a request from explicitly resolved pins and selection. Filesystem and
/// historical revision resolution belong to the caller, never the pure core.
pub fn build_request(
    kind: &str,
    identity: &Value,
    owner: &str,
    inputs: Value,
    binding: &Value,
) -> Result<Value> {
    let mut v = object([
        ("schema_version", 1.into()),
        ("id", string(format!("{kind}-{}", &digest(identity)?[..32]))),
        ("kind", string(kind)),
        ("owner", string(owner)),
        ("inputs", inputs),
    ]);
    require(binding.is_object(), "Missing workflow binding")?;
    for (k, value) in binding.as_object().into_iter().flatten() {
        v[k] = value.clone();
    }
    request(&v)?;
    Ok(v)
}
pub fn build_submission_review(row: &Value, paper: &Value, secret: &[u8]) -> Result<Value> {
    sign_envelope(
        &object([
            ("schema_version", 1.into()),
            ("paper_id", paper.clone()),
            ("task_id", row["id"].clone()),
            ("result_hash", string(digest(&row["data"]["result"])?)),
        ]),
        secret,
    )
}
pub fn mathesis_reviewed(row: &Value) -> Result<()> {
    let result = &row["data"]["result"];
    let review = &row["data"]["review"];
    require(
        row["kind"] == "mathesis"
            && result["outcome"] == "completed"
            && review["verdict"] == "PASS"
            && review["binding"]["result_hash"] == digest(result)?
            && review["binding"]["workflow_hash"] == row["request"]["workflow_hash"],
        "Mathesis delivery requires the exact independently reviewed result",
    )
}
pub fn validate_mathesis(v: &Value, secret: &[u8]) -> Result<()> {
    validate_mathesis_mode(v, secret, false)
}
pub(crate) fn validate_mathesis_mode(v: &Value, secret: &[u8], test: bool) -> Result<()> {
    let collection = v["metadata"]["schema"] == "mathesis.pre-reviewed-delivery.v1";
    require(
        keys(
            v,
            &[
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
            if collection { &["pdf"] } else { &[] },
        ) && (!collection || v.get("pdf").is_some()),
        "Invalid Mathesis delivery envelope",
    )?;
    require(
        !secret.is_empty(),
        "The local operator signing key is required",
    )?;
    signature_type(v, false, "Mathesis delivery signature failed")?;
    require(
        outreach_canon::verify_mathesis_envelope(v, secret)?,
        "Mathesis delivery signature failed",
    )?;
    require(
        one(&v["schema_version"])
            && independent_reviewer(&v["reviewer"])?
            && nonempty(&v["decision_ref"]),
        "Preserve the actual author/processing decision reference",
    )?;
    require(
        v["task_id"].is_string() && regex(r"^[A-Za-z0-9_.-]{1,120}$")?.is_match(s(&v["task_id"])),
        "Invalid Mathesis task identity",
    )?;
    let a = &v["source"];
    let m = &v["metadata"];
    require(
        keys(a, &["release_tag", "name", "sha256", "bytes", "role"], &[])
            && a["release_tag"] == format!("outreach-task-{}", s(&v["task_id"]))
            && a["name"].is_string()
            && regex(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")?.is_match(s(&a["name"]))
            && hex(&a["sha256"], 64)
            && ["source-archive", "note-tex", "manuscript-tex"].contains(&s(&a["role"]))
            && integer(&a["bytes"])
            && a["bytes"]
                .as_u64()
                .is_some_and(|n| n > 0 && n <= 30 * 1024 * 1024),
        "Select a preserved scientific source from this reviewed task",
    )?;
    if collection {
        require(
            !test,
            "Collection cannot use the private synthetic manuscript test",
        )?;
        collection_metadata(m)?;
        let pdf = &v["pdf"];
        require(
            keys(
                pdf,
                &["release_tag", "name", "sha256", "bytes", "role"],
                &[],
            ) && pdf["release_tag"] == a["release_tag"]
                && regex(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,195}\.pdf$")?.is_match(s(&pdf["name"]))
                && pdf["role"] == "note-pdf"
                && integer(&pdf["bytes"])
                && pdf["bytes"]
                    .as_u64()
                    .is_some_and(|n| n > 0 && n <= 30 * 1024 * 1024)
                && m["review"]["pdf_sha256"] == pdf["sha256"]
                && a["role"] == "source-archive"
                && s(&a["name"]).ends_with(".zip")
                && m["review"]["source_sha256"] == a["sha256"]
                && m["producer"] == "trureturing-outreach",
            "Invalid reviewed collection files",
        )?;
        return Ok(());
    }
    require(
        m.is_object()
            && m["schema"] == "mathesis.manuscript-delivery.v1"
            && truth(&m["work_id"])
            && truth(&m["version"])
            && m["paper"].is_object(),
        "Missing Mathesis source identity",
    )?;
    let p = &m["paper"];
    require(
        truth(&p["processing_consent"])
            && truth(&p["ai_disclosure"])
            && truth(&p["authors"])
            && ["publication", "review_only"].contains(&s(&p["purpose"])),
        "Record actual author information, processing consent and disclosure",
    )?;
    if test {
        require(
            m["producer"] == "outreach-infrastructure-test"
                && m["work_id"] == "test-runtime-20261031"
                && m["version"] == "1"
                && p["purpose"] == "review_only"
                && a["sha256"]
                    == "1a07d345c39df2081ca4796666afb1f2a5628388c3e3e675bffce6d4813c17c2"
                && p["make_public_after_acceptance"] == false
                && p["open_to_contributors"] == false,
            "The acceptance entry only reuses the existing private synthetic manuscript",
        )
    } else {
        require(
            m["producer"] == "trureturing-outreach",
            "Invalid production producer",
        )
    }
}
pub fn collection_metadata(v: &Value) -> Result<()> {
    require(
        keys(
            v,
            &[
                "schema",
                "producer",
                "work_id",
                "version",
                "previous_version",
                "authorize_publication",
                "paper",
                "review",
            ],
            &[],
        ) && v["schema"] == "mathesis.pre-reviewed-delivery.v1"
            && v["authorize_publication"] == true,
        "Invalid pre-reviewed publication envelope",
    )?;
    require(
        ["producer", "work_id"].iter().all(|k| {
            v[k].is_string() && regex(r"^[a-z0-9-]{1,160}$").is_ok_and(|r| r.is_match(s(&v[k])))
        }),
        "Invalid collection slug",
    )?;
    let n = v["version"].as_u64();
    let p = &v["previous_version"];
    require(
        integer(&v["version"])
            && n.is_some_and(|n| n > 0 && n <= u32::MAX as u64)
            && (p.is_null()
                || integer(p)
                    && p.as_u64()
                        .is_some_and(|p| p > 0 && n.is_some_and(|n| p < n))),
        "Collection version must advance its predecessor",
    )?;
    let paper = &v["paper"];
    let r = &v["review"];
    require(
        truth(&paper["title"])
            && truth(&paper["abstract"])
            && paper["authors"].is_array()
            && truth(&paper["authors"])
            && paper["license"] == "CC-BY-4.0"
            && array(&paper["authors"])
                .iter()
                .all(|a| truth(&a["name"]) && !truth(&a["person"])),
        "Supply ordered collection authors, title, abstract and license",
    )?;
    require(
        r["verdict"] == "PASS"
            && r["record"].is_object()
            && truth(&r["record"])
            && hex(&r["pdf_sha256"], 64)
            && hex(&r["source_sha256"], 64),
        "Collection requires an exact file-bound PASS",
    )?;
    require(
        canonical(v)?.len() <= 128 * 1024,
        "Collection metadata exceeds 128 KiB",
    )
}
