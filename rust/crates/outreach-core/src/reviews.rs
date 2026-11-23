use crate::*;
use outreach_canon::{canonical, digest};

pub fn matches_workflow(row: &Value, hash: &Value) -> bool {
    let request = &row["request"]["workflow_hash"];
    let review = &row["data"]["review"];
    let matches = |h: &Value| {
        h == request || (review["verdict"] == "PASS" && h == &review["rebind_workflow_hash"])
    };
    matches(hash)
        || (row["data"]["workflow_binding"]["workflow_hash"] == *hash
            && matches(&row["data"]["workflow_binding"]["legacy_hash"]))
}
pub fn reviewable(row: &Value) -> Result<bool> {
    let data = &row["data"];
    let result = &data["result"];
    if row["state"] == "held" && result.is_object() && result["outcome"] == "hold" {
        return Ok(!(data["review"].is_object()
            && data["review"]["verdict"] == "HOLD"
            && data["review"]["binding"]["result_hash"] == digest(result)?));
    }
    if row["state"] != "awaiting-review" {
        return Ok(false);
    }
    if row["kind"] == "submission" && result["outcome"] == "completed" {
        return Ok(truth(&data["submission_review_requested"]));
    }
    if row["kind"] != "shortnote" {
        return Ok(true);
    }
    Ok(row["request"]["inputs"]["note_round"] != "draft"
        && (result["outcome"] != "completed"
            || (result["value_assessment"]["verdict"] != "trivial"
                && result.get("formalization_gaps").is_some()
                && result.get("value_assessment").is_some())))
}
pub fn reviews(rows: &[Value], events: &[Value]) -> Result<Value> {
    let mut output = Vec::new();
    for row in rows {
        if !reviewable(row)? {
            continue;
        }
        let mut revision = row["revision"]
            .as_i64()
            .ok_or_else(|| Error::new("Invalid revision"))?;
        let mut expected = revision;
        let review = &row["data"]["review"];
        let review_event = if truth(review) {
            Some(canonical(&object([(
                "review",
                string(if truth(&review["signed_review_hash"]) {
                    py_str(&review["signed_review_hash"])
                } else {
                    digest(review)?
                }),
            )]))?)
        } else {
            None
        };
        let mut history: Vec<_> = events
            .iter()
            .filter(|e| {
                e["id"] == row["id"] && e["revision"].as_i64().is_some_and(|r| r <= revision)
            })
            .collect();
        history.sort_by_key(|e| std::cmp::Reverse(e["revision"].as_i64().unwrap()));
        for event in history {
            let number = event["revision"].as_i64().unwrap();
            if number != expected || event["state"] != row["state"] {
                break;
            }
            revision = number;
            expected = number - 1;
            if review_event.as_deref() == event["raw_data"].as_str() && review_event.is_some() {
                break;
            }
        }
        let mut item = object([
            (
                "binding",
                object([
                    ("task_id", row["id"].clone()),
                    ("task_revision", revision.into()),
                    ("result_hash", string(digest(&row["data"]["result"])?)),
                    ("workflow_hash", row["request"]["workflow_hash"].clone()),
                ]),
            ),
            ("request", row["request"].clone()),
            ("result", row["data"]["result"].clone()),
        ]);
        for (source, target) in [
            ("workflow_binding", "workflow_binding"),
            ("submission_review_requested", "submission_review_requested"),
            ("thread_inputs", "resolved_inputs"),
            ("mailbox_history", "mailbox_history"),
        ] {
            if truth(&row["data"][source]) {
                item[target] = if source == "submission_review_requested" {
                    true.into()
                } else {
                    row["data"][source].clone()
                };
            }
        }
        output.push(item);
    }
    Ok(Value::Array(output))
}
