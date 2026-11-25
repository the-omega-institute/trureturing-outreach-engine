use crate::*;
mod affairs;
mod board;
use affairs::affairs;
use board::submissions;
use outreach_canon::{SerializeOptions, canonical, digest, dumps};
use std::collections::{BTreeMap, BTreeSet};
const CHECKS: [&str; 6] = [
    "send-registry",
    "owner-draft",
    "publication",
    "submissions",
    "affairs",
    "failed",
];
const RECOVERY: [&str; 6] = [
    "requeue",
    "recovery",
    "requeue_id",
    "recovery_task_id",
    "continuation_id",
    "successor",
];
fn line(value: &str) -> String {
    let compact = text::compact(value, usize::MAX);
    if compact.chars().count() > 180 {
        format!("{}…", compact.chars().take(179).collect::<String>())
    } else {
        compact
    }
}
fn error_type(value: &Value) -> Result<String> {
    if value.is_object() {
        let empty = string("");
        let name = if truth(&value["type"]) {
            &value["type"]
        } else if truth(&value["code"]) {
            &value["code"]
        } else {
            &empty
        };
        return Ok(
            if regex(r"^[A-Za-z][A-Za-z0-9_.]{0,49}$")?.is_match(&py_str(name)) {
                py_str(name)
            } else {
                "dict".into()
            },
        );
    }
    if let Some(m) = regex(r"\b[A-Za-z][A-Za-z0-9]*(?:Error|Exception)\b")?.find(&py_str(value)) {
        return Ok(m.as_str().into());
    }
    Ok(if !truth(value) {
        "none"
    } else {
        match value {
            Value::String(_) => "str",
            Value::Array(_) => "list",
            Value::Bool(_) => "bool",
            Value::Number(n) => {
                if n.as_str().contains(['.', 'e', 'E']) {
                    "float"
                } else {
                    "int"
                }
            }
            _ => "NoneType",
        }
    }
    .into())
}
pub(super) fn add(
    items: &mut Vec<Value>,
    check: &str,
    key: &str,
    id: &Value,
    reason: impl AsRef<str>,
    evidence: impl Into<String>,
) {
    items.push(object([
        ("check", string(check)),
        (key, id.clone()),
        ("reason", string(line(reason.as_ref()))),
        ("evidence", string(evidence)),
    ]));
}
fn snapshot(mut work: Value) -> Result<Vec<Value>> {
    if !work.is_object() {
        return Err(Error::new("Work must be an object"));
    }
    let mut rows = Value::Object(Default::default());
    for section in [
        "tasks",
        "affairs_tasks",
        "follow_up",
        "reviews",
        "user_questions",
        "owner_drafts",
        "short_note_deliveries",
        "mathesis_deliveries_needed",
        "registry_updates",
        "parked_holds",
    ] {
        let section_items = work
            .as_object_mut()
            .unwrap()
            .shift_remove(section)
            .unwrap_or(Value::Array(vec![]));
        let Value::Array(section_items) = section_items else {
            return Err(Error::new("Work section must be a list"));
        };
        for mut item in section_items {
            if !item.is_object() {
                return Err(Error::new("Work entry must be an object"));
            }
            let id = fallback(&[&item["id"], &item["task_id"], &item["binding"]["task_id"]]);
            if !truth(id) {
                continue;
            }
            let key = py_str(id);
            if rows.get(&key).is_none() {
                rows[&key] = object([
                    ("id", id.clone()),
                    ("data", object([])),
                    (
                        "request",
                        object([("inputs", object([])), ("workflow_hash", string(""))]),
                    ),
                    ("kind", string("")),
                    ("state", string("")),
                ]);
            }
            let row = &mut rows[&key];
            for key in ["request", "kind", "state", "owner", "updated", "revision"] {
                if !item[key].is_null() {
                    row[key] = item.as_object_mut().unwrap().shift_remove(key).unwrap();
                }
            }
            if let Some(data) = item
                .as_object_mut()
                .unwrap()
                .shift_remove("data")
                .filter(truth)
            {
                let Value::Object(data) = data else {
                    return Err(Error::new("Task data must be an object"));
                };
                row["data"].as_object_mut().unwrap().extend(data);
            }
            for key in [
                "result",
                "review",
                "draft_receipt",
                "draft_intent_expired",
                "sent_receipt",
                "last_error",
                "reason",
                "packet",
                "packet_override",
            ]
            .into_iter()
            .chain(RECOVERY)
            {
                if !item[key].is_null() {
                    row["data"][key] = item.as_object_mut().unwrap().shift_remove(key).unwrap();
                }
            }
            if !truth(&row["kind"]) {
                row["kind"] = row["request"]["kind"].clone();
            }
        }
    }
    for (section, field) in [
        ("publication_deliveries", "publication_loop"),
        ("mathesis_deliveries", "mathesis_delivery"),
    ] {
        let section_items = work
            .as_object_mut()
            .unwrap()
            .shift_remove(section)
            .unwrap_or(Value::Array(vec![]));
        let Value::Array(section_items) = section_items else {
            return Err(Error::new("Work section must be a list"));
        };
        for mut item in section_items {
            if !item.is_object() {
                return Err(Error::new("Work entry must be an object"));
            }
            let task_id = item
                .as_object_mut()
                .unwrap()
                .shift_remove("task_id")
                .ok_or_else(|| Error::new("Missing delivery task_id"))?;
            let key = py_str(&task_id);
            if rows.get(&key).is_none() {
                rows[&key] = object([
                    ("id", task_id),
                    ("kind", string("mathesis")),
                    ("state", item.get("state").cloned().unwrap_or(string(""))),
                    (
                        "request",
                        object([("inputs", object([])), ("workflow_hash", string(""))]),
                    ),
                    ("data", object([])),
                ]);
            }
            rows[&key]["data"][field] = Value::Object(
                std::mem::take(item.as_object_mut().unwrap())
                    .into_iter()
                    .filter(|(k, _)| !["task_id", "state"].contains(&k.as_str()))
                    .collect(),
            );
        }
    }
    Ok(rows
        .as_object_mut()
        .unwrap()
        .into_iter()
        .map(|(_, v)| v.take())
        .filter(|r| r["state"] != "superseded")
        .collect())
}
fn lineage<'a>(row: &'a Value, rows: &'a [Value]) -> std::result::Result<Vec<&'a Value>, String> {
    let mut row = row;
    let mut result = Vec::new();
    let mut seen = BTreeSet::new();
    loop {
        if !seen.insert(py_str(&row["id"])) {
            return Err("Publication source cycle".into());
        }
        result.push(row);
        let inputs = &row["request"]["inputs"];
        let mut parent = fallback(&[
            &inputs["pipeline"]["note_task_id"],
            &inputs["automation_parent"]["task_id"],
        ]);
        if !truth(parent) && row["kind"] == "mathesis" && array(&inputs["note_sources"]).len() == 1
        {
            parent = &inputs["note_sources"][0]["task_id"];
        }
        if !truth(parent) {
            return Ok(result);
        }
        row = rows
            .iter()
            .find(|r| r["id"] == *parent)
            .ok_or_else(|| format!("Source task absent from snapshot: {}", py_str(parent)))?;
    }
}
fn identities(row: &Value) -> BTreeSet<String> {
    let inputs = &row["request"]["inputs"];
    let loop_ = &row["data"]["publication_loop"];
    [
        &row["id"],
        &inputs["result_id"],
        &inputs["pipeline"]["result_id"],
        &inputs["result"]["id"],
        &loop_["note_id"],
    ]
    .into_iter()
    .chain(array(&loop_["problem_ids"]))
    .filter(|v| !v.is_null())
    .map(|v| canonical(v).unwrap())
    .collect()
}
fn excluded(row: &Value, rows: &[Value], decisions: &Value) -> std::result::Result<bool, String> {
    let parents = lineage(row, rows)?;
    let selected: BTreeSet<_> = parents.iter().flat_map(|r| identities(r)).collect();
    for hold in array(&decisions["holds"]) {
        let targets: BTreeSet<_> = ["legacy_id", "task_id", "note_id", "id"]
            .iter()
            .map(|k| &hold[*k])
            .chain(array(&hold["problem_ids"]))
            .filter(|v| !v.is_null())
            .map(|v| canonical(v).unwrap())
            .collect();
        if !selected.is_disjoint(&targets)
            && ["HOLD", "JOINT-WORK"].contains(&s(&hold["decision"]).to_uppercase().as_str())
        {
            return Ok(true);
        }
    }
    for parent in parents.into_iter().chain(rows.iter().filter(|r| {
        ["shortnote", "mathesis"].contains(&s(&r["kind"])) && !identities(r).is_disjoint(&selected)
    })) {
        let data = &parent["data"];
        if data["review"]["verdict"] == "HOLD"
            || [&parent["request"]["inputs"], data, &data["result"]]
                .iter()
                .any(|v| {
                    v["joint_work"] == true
                        || ["joint", "joint-work"].contains(&s(&v["authorship"]))
                })
        {
            return Ok(true);
        }
    }
    Ok(false)
}
fn reviewed(row: &Value) -> Result<bool> {
    let data = &row["data"];
    let result = &data["result"];
    let review = &data["review"];
    let eligible = row["kind"] == "mathesis"
        || (row["kind"] == "shortnote"
            && row["request"]["inputs"]["note_round"] != "draft"
            && result["value_assessment"]["verdict"] != "trivial"
            && ((result.get("formalization_gaps").is_some()
                && result.get("value_assessment").is_some())
                || review["verdict"] == "PASS"));
    Ok([
        "approved",
        "awaiting-mailbox",
        "owner-draft",
        "handoff-ready",
        "verified",
        "no-reply",
        "round-complete",
        "submission-ready",
    ]
    .contains(&s(&row["state"]))
        && eligible
        && result["outcome"] == "completed"
        && review["verdict"] == "PASS"
        && validation::independent_reviewer(&review["reviewer"])?
        && review["binding"]["result_hash"] == digest(result)?
        && review["binding"]["workflow_hash"] == row["request"]["workflow_hash"])
}
pub fn exceptions(work: Value, env: &Environment) -> Result<Value> {
    let rows = snapshot(work)?;
    for row in &rows {
        if !row["request"]["inputs"].is_object() {
            return Err(Error::new("Missing request inputs object"));
        }
    }
    let persons = registry::people(&env.tables)?;
    let mut current = BTreeMap::new();
    let mut items = Vec::new();
    for row in &rows {
        let data = &row["data"];
        let id = &row["id"];
        let result = &data["result"];
        let review = &data["review"];
        let receipt = &data["sent_receipt"];
        if ["reply", "firstcontact"].contains(&s(&row["kind"])) && truth(receipt) {
            let packet = fallback(&[&data["packet_override"], &data["packet"], &result["packet"]]);
            let mut addresses: BTreeSet<_> = array(&packet["to"])
                .iter()
                .chain(array(&packet["cc"]))
                .map(registry::packet_address)
                .collect::<Result<_>>()?;
            if addresses.is_empty() {
                addresses.extend(
                    array(&row["request"]["inputs"]["recipient_emails"])
                        .iter()
                        .map(py_str),
                );
            }
            let mut needles = Vec::new();
            for key in [
                "remote_id",
                "evidence_ref",
                "internetMessageId",
                "message_id",
            ] {
                if truth(&receipt[key]) {
                    let v = py_str(&receipt[key]);
                    needles.push(if key == "evidence_ref" {
                        regex(r"^graph:(?:sent|inbox):")?
                            .replace(&v, "")
                            .into_owned()
                    } else {
                        v
                    });
                }
            }
            let mut missing = BTreeSet::new();
            for address in &addresses {
                let person = persons.iter().rev().find(|p| {
                    array(&p["emails"])
                        .iter()
                        .any(|a| s(a) == casefold(address))
                });
                let records = person.map(|p| array(&p["records"])).unwrap_or(&[]);
                let mut recorded = false;
                for event in records.iter().flat_map(|r| array(&r["hub"]["events"])) {
                    if event["kind"] != "sent" {
                        continue;
                    }
                    if event["task_id"] == *id {
                        recorded = true;
                        break;
                    }
                    let original = event.get("evidence").cloned().unwrap_or(string(""));
                    let mut evidence = original.clone();
                    for _ in 0..3 {
                        if let Some(raw) = evidence.as_str()
                            && let Ok(decoded) = outreach_canon::parse(raw)
                            && decoded != evidence
                        {
                            evidence = decoded;
                        } else {
                            break;
                        }
                    }
                    if evidence["task_id"] == *id {
                        recorded = true;
                        break;
                    }
                    let evidence_text = if original.is_string() {
                        py_str(&original)
                    } else {
                        dumps(
                            &evidence,
                            SerializeOptions {
                                ensure_ascii: false,
                                sort_keys: true,
                                ..Default::default()
                            },
                        )?
                    };
                    if needles
                        .iter()
                        .any(|n| !n.is_empty() && evidence_text.contains(n))
                    {
                        recorded = true;
                        break;
                    }
                }
                if !recorded {
                    if records.is_empty() {
                        missing.insert(format!("unregistered:{address}"));
                    } else {
                        missing.extend(records.iter().map(|r| py_str(&r["id"])));
                    }
                }
            }
            if !missing.is_empty() || addresses.is_empty() {
                add(
                    &mut items,
                    "send-registry",
                    "task_id",
                    id,
                    format!(
                        "Verified send receipt lacks table-1 outgoing event{}",
                        if addresses.is_empty() {
                            "; recipient identity unavailable"
                        } else {
                            ""
                        }
                    ),
                    format!(
                        "data.sent_receipt; tables/contacts.json:{}",
                        missing.into_iter().collect::<Vec<_>>().join(",")
                    ),
                );
            }
        }
        if row["state"] == "owner-draft" && ["reply", "firstcontact"].contains(&s(&row["kind"])) {
            let mut reasons = Vec::new();
            if !truth(fallback(&[&data["draft_receipt"], receipt])) {
                reasons.push("No draft_receipt or sent_receipt");
            }
            if truth(&data["draft_intent_expired"]) && truth(&data["last_error"]) {
                reasons.push("Expired draft intent mailbox reconciliation failed");
            }
            if review["verdict"] != "PASS"
                || review["binding"]["result_hash"]
                    != if truth(result) {
                        digest(result)?
                    } else {
                        digest(&object([]))?
                    }
                || review["binding"]["workflow_hash"] != row["request"]["workflow_hash"]
            {
                reasons.push("review no longer binds exact result");
            }
            let kind = s(&row["kind"]);
            if !current.contains_key(kind) {
                current.insert(kind, (env.workflow)(kind)?);
            }
            if !reviews::matches_workflow(row, &current[kind]) {
                reasons.push("workflow review is stale");
            }
            if !reasons.is_empty() {
                add(
                    &mut items,
                    "owner-draft",
                    "task_id",
                    id,
                    format!(
                        "{}; last_error={}",
                        reasons.join("; "),
                        error_type(if truth(&data["last_error"]) {
                            &data["last_error"]
                        } else {
                            &data["reason"]
                        })?
                    ),
                    "data.review/draft_receipt/sent_receipt",
                );
            }
        }
    }
    let mut delegated = BTreeSet::new();
    for row in &rows {
        if row["kind"] == "mathesis"
            && row["state"] != "failed"
            && let Ok(parents) = lineage(row, &rows)
        {
            delegated.extend(
                parents
                    .into_iter()
                    .skip(1)
                    .filter(|r| r["kind"] == "shortnote")
                    .map(|r| py_str(&r["id"])),
            );
        }
    }
    for row in &rows {
        let id = &row["id"];
        let data = &row["data"];
        let loop_ = &data["publication_loop"];
        let phase = &loop_["phase"];
        if ["shortnote", "mathesis"].contains(&s(&row["kind"])) && reviewed(row)? {
            if truth(phase) && phase != "queued" {
                let mut reason = format!(
                    "publication_loop {}: {}",
                    py_str(phase),
                    if truth(&loop_["reason"]) {
                        py_str(&loop_["reason"])
                    } else {
                        "awaiting publication queue".into()
                    }
                );
                if truth(&loop_["last_error"]) {
                    reason += &format!("; last_error={}", error_type(&loop_["last_error"])?);
                }
                add(
                    &mut items,
                    "publication",
                    "task_id",
                    id,
                    reason,
                    "data.publication_loop",
                );
            } else if !delegated.contains(&py_str(id))
                && (phase != "queued"
                    || (truth(&loop_["result_hash"])
                        && loop_["result_hash"] != digest(&data["result"])?))
            {
                let (excluded, reason) = match excluded(row, &rows, &env.tables["decisions"]) {
                    Ok(excluded) => (
                        excluded,
                        if truth(&env.tables["decisions"]["short_note_publication"]["enabled"]) {
                            "Exact PASS has no current queued publication_loop".into()
                        } else {
                            "Exact PASS awaits an active standing publication decision".into()
                        },
                    ),
                    Err(reason) => (false, reason),
                };
                if !excluded {
                    add(
                        &mut items,
                        "publication",
                        "task_id",
                        id,
                        reason,
                        "data.review/publication_loop; tables/decisions.json",
                    );
                }
            }
        }
        let delivery = &data["mathesis_delivery"];
        let receipt = &delivery["receipt"];
        if ["pending", "conflict"].contains(&s(&delivery["phase"])) {
            add(
                &mut items,
                "publication",
                "task_id",
                id,
                format!(
                    "mathesis_delivery {}; last_error={}",
                    py_str(&delivery["phase"]),
                    error_type(&delivery["last_error"])?
                ),
                "data.mathesis_delivery",
            );
        }
        if delivery["phase"] == "delivered"
            && (receipt["status"] == "draft"
                || !outreach_canon::python_eq(
                    &receipt["expected_claims_revision"],
                    &receipt["confirmed_claims_revision"],
                ))
        {
            add(
                &mut items,
                "publication",
                "task_id",
                id,
                format!(
                    "needs-claims-confirmation: delivered receipt; status={}",
                    receipt
                        .get("status")
                        .map(py_str)
                        .unwrap_or("unknown".into())
                ),
                "data.mathesis_delivery.receipt",
            );
        }
    }
    submissions(&rows, env, &mut items)?;
    affairs(&rows, env, &persons, &mut items)?;
    let research = regex(
        r"(?i)\b(?:new|further|additional|more)\s+(?:mathematical\s+)?research\b|\bnew\s+(?:idea|argument|proof|approach)\b|\bresearch\s+(?:needed|required)\b",
    )?;
    let mut children: BTreeMap<&str, Vec<&Value>> = BTreeMap::new();
    for row in &rows {
        let parent = s(&row["request"]["inputs"]["automation_parent"]["task_id"]);
        if !parent.is_empty() && row["state"] != "failed" {
            children.entry(parent).or_default().push(row);
        }
    }
    for row in &rows {
        let data = &row["data"];
        let children = children
            .get(s(&row["id"]))
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if row["state"] == "held"
            && ["reply", "research"].contains(&s(&row["kind"]))
            && research.is_match(&py_str(fallback(&[
                &data["review"]["reason"],
                &data["reason"],
                &data["last_error"],
                &data["result"]["report"],
            ])))
            && !children.iter().any(|r| r["kind"] == "research")
        {
            add(
                &mut items,
                "affairs",
                "task_id",
                &row["id"],
                "HOLD needs new research; no child research task",
                "data.review/reason; request.inputs.automation_parent",
            );
        }
        if row["state"] == "failed"
            && !RECOVERY.iter().any(|k| truth(&data[*k]))
            && children.is_empty()
        {
            add(
                &mut items,
                "failed",
                "task_id",
                &row["id"],
                format!(
                    "Failed task has no requeue/recovery exit; last_error={}",
                    error_type(if truth(&data["last_error"]) {
                        &data["last_error"]
                    } else {
                        &data["reason"]
                    })?
                ),
                "data.reason/last_error/continuation_id",
            );
        }
    }
    items.sort_by_key(|i| {
        (
            CHECKS.iter().position(|c| i["check"] == *c).unwrap(),
            py_str(fallback(&[&i["task_id"], &i["paper_id"], &i["affair_id"]])),
            py_str(&i["reason"]),
        )
    });
    let counts = Value::Object(
        CHECKS
            .into_iter()
            .map(|c| {
                (
                    c.into(),
                    Value::from(items.iter().filter(|i| i["check"] == c).count()),
                )
            })
            .collect(),
    );
    Ok(object([("counts", counts), ("items", Value::Array(items))]))
}
pub fn render_exceptions(value: &Value, text: bool) -> Result<String> {
    if !text {
        return Ok(dumps(
            value,
            SerializeOptions {
                separators: Some((",", ":")),
                ensure_ascii: false,
                ..Default::default()
            },
        )? + "\n");
    }
    let counts = value["counts"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| format!("{k}={}", py_str(v)))
        .collect::<Vec<_>>()
        .join(" ");
    let mut output = format!("counts: {counts}\n");
    for i in array(&value["items"]) {
        output += &format!(
            "{} {}: {} [{}]\n",
            py_str(&i["check"]),
            py_str(fallback(&[&i["task_id"], &i["paper_id"], &i["affair_id"]])),
            py_str(&i["reason"]),
            py_str(&i["evidence"])
        );
    }
    Ok(output)
}
