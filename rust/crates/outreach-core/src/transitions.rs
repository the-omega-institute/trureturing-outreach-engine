//! Pure task transitions. The caller applies the returned changes atomically.
use crate::{envelopes::*, validation::*, *};
use outreach_canon::{canonical, digest, python_eq, sha256};
use outreach_model::Binding;
use std::collections::BTreeSet;

pub struct Context<'a> {
    pub rows: &'a [Value],
    pub events: &'a [Value],
    pub attempts: &'a [Value],
    pub environment: &'a Environment<'a>,
    pub now: f64,
}
#[derive(Clone, Debug)]
pub struct Change {
    pub row: Value,
    pub event_data: Value,
    pub preserved: Option<(Value, Vec<u8>)>,
}
pub fn agent_hold(row: &Value) -> bool {
    row["state"] == "held"
        && row["data"]["result"].is_object()
        && row["data"]["result"]["outcome"] == "hold"
}
pub fn review_current(row: &Value, hash: &Value) -> Result<bool> {
    let r = &row["data"]["review"];
    let b = &r["binding"];
    Ok(r["verdict"] == "PASS"
        && b["task_id"] == row["id"]
        && b["workflow_hash"] == row["request"]["workflow_hash"]
        && b["result_hash"] == digest(&row["data"]["result"])?
        && crate::reviews::matches_workflow(row, hash))
}
pub fn eligible(row: &Value) -> bool {
    let r = &row["data"]["result"];
    row["kind"] == "shortnote"
        && row["request"]["inputs"]["note_round"] != "draft"
        && r["value_assessment"]["verdict"] != "trivial"
        && ((r.get("formalization_gaps").is_some() && r.get("value_assessment").is_some())
            || row["data"]["review"]["verdict"] == "PASS")
}
pub fn passed(row: &Value) -> Result<bool> {
    Ok(eligible(row)
        && row["data"]["result"]["outcome"] == "completed"
        && review_current(row, &row["request"]["workflow_hash"])?)
}
fn owner_rebindable(row: &Value, hash: &Value) -> Result<bool> {
    Ok(row["state"] == "owner-draft"
        && !truth(&row["data"]["sent_receipt"])
        && !truth(&row["data"]["stale_draft_conflict"])
        && review_current(row, &row["request"]["workflow_hash"])?
        && !crate::reviews::matches_workflow(row, hash))
}
fn mail_rebindable(row: &Value, hash: &Value, ctx: &Context<'_>) -> Result<bool> {
    Ok(row["state"] == "awaiting-mailbox"
        && ["reply", "firstcontact"].contains(&s(&row["kind"]))
        && ["sent_receipt", "draft_receipt", "stale_draft_conflict"]
            .iter()
            .all(|k| !truth(&row["data"][k]))
        && review_current(row, &row["request"]["workflow_hash"])?
        && !crate::reviews::matches_workflow(row, hash)
        && !ctx.attempts.iter().any(|a| a["item"] == row["id"]))
}
pub fn review_binding(row: &Value, ctx: &Context<'_>) -> Result<Value> {
    let mut owner = false;
    let mut mail = false;
    if ["owner-draft", "awaiting-mailbox"].contains(&s(&row["state"])) {
        let hash = (ctx.environment.workflow)(s(&row["kind"]))?;
        owner = owner_rebindable(row, &hash)?;
        mail = mail_rebindable(row, &hash, ctx)?;
    }
    let parked = agent_hold(row)
        && row["data"]["review"]["verdict"] == "HOLD"
        && row["data"]["review"]["binding"]["result_hash"] == digest(&row["data"]["result"])?;
    require(
        reviewable(row)? || parked || owner || mail,
        "Task is not awaiting review, an agent-held result or unsent workflow-stale mail",
    )?;
    let mut revision = row["revision"]
        .as_i64()
        .ok_or_else(|| Error::new("Invalid revision"))?;
    let mut expected = revision;
    let r = &row["data"]["review"];
    let marker = if truth(r) {
        Some(object([(
            "review",
            if truth(&r["signed_review_hash"]) {
                r["signed_review_hash"].clone()
            } else {
                string(digest(r)?)
            },
        )]))
    } else {
        None
    };
    let mut events: Vec<_> = ctx
        .events
        .iter()
        .filter(|e| e["id"] == row["id"] && e["revision"].as_i64().is_some_and(|n| n <= revision))
        .collect();
    events.sort_by_key(|e| std::cmp::Reverse(e["revision"].as_i64().unwrap_or(0)));
    for e in events {
        let n = e["revision"].as_i64().unwrap_or(0);
        let allowed = if mail {
            ["awaiting-mailbox", "approved"].contains(&s(&e["state"]))
        } else if owner {
            ["owner-draft", "approved"].contains(&s(&e["state"]))
        } else {
            e["state"] == row["state"]
        };
        if n != expected || !allowed {
            break;
        }
        revision = n;
        expected = n
            .checked_sub(1)
            .ok_or_else(|| Error::new("Ledger integer out of range"))?;
        if marker.as_ref().is_some_and(|m| {
            let raw = e
                .get("_replay_raw_data")
                .and_then(Value::as_str)
                .map(String::from)
                .or_else(|| canonical(&e["data"]).ok());
            canonical(m).ok() == raw
        }) {
            break;
        }
    }
    Ok(object([
        ("task_id", row["id"].clone()),
        ("task_revision", revision.into()),
        ("result_hash", string(digest(&row["data"]["result"])?)),
        ("workflow_hash", row["request"]["workflow_hash"].clone()),
    ]))
}
/// Refuse extra/missing binding fields as Python dictionary equality does.
pub fn binding_equal(a: &Value, b: &Value) -> bool {
    if !keys(
        a,
        &["task_id", "task_revision", "result_hash", "workflow_hash"],
        &[],
    ) || !keys(
        b,
        &["task_id", "task_revision", "result_hash", "workflow_hash"],
        &[],
    ) {
        return false;
    }
    match (
        canonical(a).and_then(|s| Binding::parse(&s)),
        canonical(b).and_then(|s| Binding::parse(&s)),
    ) {
        (Ok(a), Ok(b)) => a.python_eq(&b),
        _ => false,
    }
}
pub fn source_gate(request: &Value, result: &Value, rows: &[Value]) -> Result<()> {
    if !["firstcontact", "reply", "mathesis"].contains(&s(&request["kind"])) {
        return Ok(());
    }
    let inputs = &request["inputs"];
    let mut links = Vec::new();
    for v in [inputs.get("note_sources"), result.get("note_sources")]
        .into_iter()
        .flatten()
    {
        sources(v)?;
        for link in array(v) {
            links.push((link["task_id"].clone(), link["result_hash"].clone(), true));
        }
    }
    if truth(&inputs["note"]["task_id"]) {
        links.push((
            inputs["note"]["task_id"].clone(),
            string(digest(&inputs["note"])?),
            false,
        ));
    }
    if truth(&inputs["pipeline"]["note_task_id"]) {
        links.push((
            inputs["pipeline"]["note_task_id"].clone(),
            inputs["pipeline"]["note_result_hash"].clone(),
            false,
        ));
    }
    for (id, hash, explicit) in links {
        let parent = rows
            .iter()
            .find(|r| r["id"] == id)
            .ok_or_else(|| Error::new("Unknown task"))?;
        let r = &parent["data"]["result"];
        require(
            eligible(parent)
                && ["approved", "handoff-ready", "verified"].contains(&s(&parent["state"]))
                && parent["data"]["review"]["verdict"] == "PASS"
                && r["outcome"] == "completed"
                && hash == digest(r)?,
            "Source short note needs an exact PASS and a non-trivial final round",
        )?;
        if explicit
            || (r.get("formalization_gaps").is_some() && r.get("value_assessment").is_some())
        {
            require(
                review_current(parent, &parent["request"]["workflow_hash"])?,
                "Source short note PASS no longer binds the exact result",
            )?;
        }
    }
    Ok(())
}
fn change(row: &Value, state: &str, data: Value, marker: Value, now: f64) -> Result<Change> {
    let mut row = row.clone();
    row["state"] = string(state);
    row["revision"] = increment(
        row["revision"]
            .as_i64()
            .ok_or_else(|| Error::new("Invalid revision"))?,
    )?
    .into();
    row["data"] = data;
    row["updated"] = outreach_canon::float(now)?;
    Ok(Change {
        row,
        event_data: marker,
        preserved: None,
    })
}
pub fn verdict(row: &Value, envelope: &Value, secret: &[u8], ctx: &Context<'_>) -> Result<Change> {
    validate_review(envelope, secret)?;
    review_edit(envelope, Some(s(&row["kind"])))?;
    if envelope.get("edited_body").is_some() {
        require(
            row["state"] == "awaiting-review",
            "edited_body requires a task awaiting review",
        )?;
    }
    if envelope["verdict"] == "PASS" {
        source_gate(&row["request"], &row["data"]["result"], ctx.rows)?;
        if row["kind"] == "shortnote" && row["data"]["result"]["outcome"] == "completed" {
            require(
                row["data"]["result"]["value_assessment"]["verdict"] != "trivial",
                "A trivial short note cannot PASS",
            )?;
            require(
                eligible(row),
                "Short note requires a closed final round before PASS",
            )?;
            if row["data"]["review"]["verdict"] != "PASS" {
                value_assessment(&row["data"]["result"], true)?;
            }
        }
    }
    if row["state"] == "awaiting-mailbox" {
        require(
            mail_rebindable(row, &(ctx.environment.workflow)(s(&row["kind"]))?, ctx)?,
            "Review targets an obsolete result or workflow, or mail has a delivery attempt or receipt",
        )?;
    }
    if row["state"] == "owner-draft" {
        require(
            envelope["verdict"] == "PASS" && envelope.get("rebind_workflow_hash").is_some(),
            "Owner drafts accept only PASS workflow rebinds",
        )?;
        require(
            !truth(&row["data"]["sent_receipt"]),
            "Cannot rebind a sent owner draft",
        )?;
        require(
            !truth(&row["data"]["stale_draft_conflict"]),
            "Cannot rebind an owner draft with a stale draft conflict",
        )?;
    }
    let rebind = envelope.get("rebind_workflow_hash").is_some()
        && [
            "approved",
            "awaiting-mailbox",
            "owner-draft",
            "handoff-ready",
            "submission-ready",
        ]
        .contains(&s(&row["state"]))
        && row["data"]["review"]["verdict"] == "PASS";
    if rebind {
        let mut binding = row["data"]["review"]["binding"].clone();
        if ["owner-draft", "awaiting-mailbox"].contains(&s(&row["state"]))
            && !binding_equal(&binding, &envelope["binding"])
        {
            require(
                envelope["rebind_workflow_hash"] == (ctx.environment.workflow)(s(&row["kind"]))?,
                "Workflow rebind must match the current workflow for the task kind",
            )?;
            binding = review_binding(row, ctx)?;
        }
        require(
            binding_equal(&binding, &envelope["binding"])
                && envelope["binding"]["result_hash"] == digest(&row["data"]["result"])?
                && row["request"]["workflow_hash"] == envelope["binding"]["workflow_hash"],
            "Review targets an obsolete result or workflow",
        )?;
    } else {
        require(
            binding_equal(&review_binding(row, ctx)?, &envelope["binding"]),
            "Review targets an obsolete result or workflow",
        )?;
    }
    require(
        !agent_hold(row) || ["BOUNCE", "HOLD"].contains(&s(&envelope["verdict"])),
        "An agent-held result accepts only BOUNCE or HOLD, never PASS",
    )?;
    let event = digest(envelope)?;
    let mut data = row["data"].clone();
    data["review"] = envelope.clone();
    let mut preserved = None;
    if let Some(body) = envelope.get("edited_body") {
        let (result, artifact, raw) = crate::results::edited(row, s(body), ctx.environment)?;
        let old = &row["data"]["result"];
        let hash = s(&envelope["binding"]["result_hash"]);
        data["result"] = result.clone();
        data["reviewer_edit"] = object([
            ("from_result_hash", string(hash)),
            (
                "body_sha256_before",
                string(sha256(s(&old["packet"]["body"]).as_bytes())),
            ),
            (
                "body_sha256_after",
                string(sha256(s(&result["packet"]["body"]).as_bytes())),
            ),
            ("reviewer", envelope["reviewer"].clone()),
            ("at", outreach_canon::float(ctx.now)?),
        ]);
        if !data["review_history"].is_object() {
            data["review_history"] = object([]);
        }
        data["review_history"][hash] = object([
            ("body", old["packet"]["body"].clone()),
            (
                "artifacts",
                Value::Array(
                    array(&old["artifacts"])
                        .iter()
                        .filter(|a| a["role"] == "mail-packet")
                        .cloned()
                        .collect(),
                ),
            ),
        ]);
        let mut review = envelope.clone();
        let o = review
            .as_object_mut()
            .ok_or_else(|| Error::new("Invalid signed review"))?;
        o.shift_remove("signature");
        o.shift_remove("edited_body");
        review["binding"]["result_hash"] = string(digest(&result)?);
        review["binding"]["task_revision"] =
            increment(row["revision"].as_i64().unwrap_or(0))?.into();
        review["signed_review_hash"] = string(&event);
        data["review"] = review;
        preserved = Some((artifact, raw));
    }
    if row["kind"] == "shortnote"
        && envelope["verdict"] == "PASS"
        && !rebind
        && row["data"]["result"]["outcome"] == "completed"
    {
        let requests = pending_requests(&row["data"]["result"])?;
        if truth(&requests) {
            data["formalization_requests"] = requests;
        }
    }
    let state = if rebind {
        s(&row["state"])
    } else {
        match s(&envelope["verdict"]) {
            "PASS" => "approved",
            "BOUNCE" => "bounced",
            _ => "held",
        }
    };
    let mut c = change(
        row,
        state,
        data,
        object([("review", string(event))]),
        ctx.now,
    )?;
    c.preserved = preserved;
    Ok(c)
}
pub fn decision(row: &Value, envelope: &Value, secret: &[u8], now: f64) -> Result<Change> {
    validate_decision(envelope, secret)?;
    require(
        row["state"] == "awaiting-user",
        "Task is not awaiting a user decision",
    )?;
    require(
        envelope["result_hash"] == digest(&row["data"]["result"])?,
        "Decision targets an obsolete result",
    )?;
    let questions = &row["data"]["result"]["routing"]["questions"];
    let mut data = row["data"].clone();
    let state = if envelope.get("action").is_some() {
        require(
            ["reply", "research"].contains(&s(&row["kind"])) && truth(questions),
            "Question closure requires open reply or research questions",
        )?;
        data["user_question_closed"] = object([
            ("reason", string("question closed")),
            (
                "evidence",
                object([
                    (
                        "reason",
                        string(s(&envelope["reason"]).trim_matches(crate::text::whitespace)),
                    ),
                    ("evidence_ref", envelope["evidence_ref"].clone()),
                ]),
            ),
        ]);
        "question-closed"
    } else {
        let answers = &envelope["answers"];
        require(
            answers.as_object().is_some_and(|o| {
                !o.is_empty()
                    && o.values().all(nonempty)
                    && o.keys().map(String::as_str).collect::<BTreeSet<_>>()
                        == array(questions).iter().map(|q| s(&q["id"])).collect()
            }),
            "Answer every current question with the actual user response",
        )?;
        data["user_decision"] = envelope.clone();
        "decision-recorded"
    };
    change(
        row,
        state,
        data,
        object([("decision", string(digest(envelope)?))]),
        now,
    )
}
pub fn draft_revision(row: &Value, v: &Value, secret: &[u8], now: f64) -> Result<Change> {
    validate_draft_revision(v, secret)?;
    require(
        row["state"] == "owner-draft",
        "Draft revision requires an owner draft",
    )?;
    let mut data = row["data"].clone();
    let current = if truth(&data["packet_override"]) {
        &data["packet_override"]
    } else {
        &data["result"]["packet"]
    };
    let receipt = &data["draft_receipt"];
    require(
        current.is_object() && (receipt.is_object() || !truth(receipt)),
        "Owner draft has no reviewed packet or receipt",
    )?;
    require(
        nonempty(&receipt["remote_id"]),
        "Owner draft receipt has no remote ID",
    )?;
    require(
        receipt["content_hash"] == v["old_packet_hash"],
        "Draft revision targets an obsolete draft receipt",
    )?;
    require(
        v["old_packet_hash"] == digest(current)?,
        "Draft revision targets an obsolete packet",
    )?;
    require(
        v["packet"]["kind"] == current["kind"],
        "Draft revision cannot change kind",
    )?;
    require(
        current.as_object().into_iter().flatten().all(|(k, value)| {
            ["body", "attachments"].contains(&k.as_str()) || python_eq(value, &v["packet"][k])
        }),
        "Draft revision may change only body and attachments",
    )?;
    let event = digest(v)?;
    let mut pending = array(&data["draft_revision_requests"]).to_vec();
    if !pending.iter().any(|i| i["event_id"] == event) {
        let mut i = v.clone();
        i["event_id"] = string(&event);
        pending.push(i);
    }
    data["draft_revision_requests"] = Value::Array(pending);
    change(
        row,
        s(&row["state"]),
        data,
        object([("draft_revision", string(event))]),
        now,
    )
}
