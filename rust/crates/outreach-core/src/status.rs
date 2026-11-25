use crate::*;
pub struct StatusInput {
    pub rows: Vec<Value>,
    pub counts: Value,
    pub counts_by_kind: Value,
    pub sources: Vec<Value>,
    pub source_notes: i64,
    pub derived_tasks: i64,
    pub baseline: Value,
    pub rejected: Value,
    pub connector_errors: Value,
    pub pool_operator_checks: Value,
    pub task_cleanup_checks: Vec<Value>,
    pub old_questions: i64,
}
pub fn status(input: StatusInput, env: &Environment) -> Result<Value> {
    if env.config.get("paused").is_none() {
        return Err(Error::new("Missing paused"));
    }
    let mut waits = Value::Object(Default::default());
    let mut waiting = 0;
    if !env.tables["contacts"]["contacts"].is_array() {
        return Err(Error::new("Missing contacts list"));
    }
    for source in &input.sources {
        if !truth(&source["baseline"]) && !truth(&source["task_id"]) {
            waiting += 1;
        }
        if !truth(&source["task_id"]) {
            continue;
        }
        if source["state"].is_null() {
            return Err(Error::new("Unknown task"));
        }
        if !["approved", "handoff-ready", "verified"].contains(&s(&source["state"])) {
            continue;
        }
        let contacts: Vec<_> = array(&env.tables["contacts"]["contacts"])
            .iter()
            .filter(|p| array(&p["problem_ids"]).contains(&source["result_id"]))
            .collect();
        let reasons = if contacts.is_empty() {
            vec!["No verified author contact registered for this result".to_owned()]
        } else {
            let mut reasons = Vec::new();
            let persons = registry::people(&env.tables)?;
            for p in contacts {
                let found =
                    registry::preflight(s(&p["email"]), s(&p["name"]), &env.tables, &persons)?;
                if !found.is_empty() {
                    reasons.push(found.join(", "));
                }
            }
            reasons
        };
        for reason in reasons {
            let n = waits[&reason].as_i64().unwrap_or(0);
            waits[reason] = (n + 1).into();
        }
    }
    let wait_count: i64 = waits
        .as_object()
        .unwrap()
        .values()
        .map(|v| v.as_i64().unwrap())
        .sum();
    let feedback = if env.config["mailbox_adapter"] == "microsoft-graph" {
        format!(
            "microsoft-graph configured; {}",
            if truth(&env.config["paused"]) {
                "production paused"
            } else {
                "per-message live receipts required"
            }
        )
    } else {
        "unconfigured; requires actual sent/thread receipts".into()
    };
    let mut admission = Vec::new();
    let mut rejected_count = 0;
    if let Some(entries) = input.rejected.as_object() {
        for (sha, entry) in entries {
            let mut item = object([("digest", string(sha))]);
            item.as_object_mut().unwrap().extend(
                entry
                    .as_object()
                    .ok_or_else(|| Error::new("Invalid rejection"))?
                    .clone(),
            );
            admission.push(item);
            if entry["attempts"].as_f64().unwrap_or(0.0) >= 3.0 {
                rejected_count += 1;
            }
        }
    }
    Ok(object([
        ("schema_version", 1.into()),
        ("paused", env.config["paused"].clone()),
        ("counts", input.counts.clone()),
        ("counts_by_kind", input.counts_by_kind),
        ("lanes", lanes::summary(&input.rows, env)?),
        (
            "pipeline",
            object([
                ("baseline", input.baseline),
                ("observed_results", input.sources.len().into()),
                ("new_results_waiting", waiting.into()),
                ("source_notes", input.source_notes.into()),
                ("derived_tasks", input.derived_tasks.into()),
                ("outreach_wait_count", wait_count.into()),
                ("outreach_waits_by_reason", waits),
                ("mailbox_feedback", string(feedback)),
            ]),
        ),
        (
            "open_owner_questions_over_2_days",
            input.old_questions.into(),
        ),
        (
            "errors",
            object([
                (
                    "failed",
                    input.counts.get("failed").cloned().unwrap_or(0.into()),
                ),
                (
                    "held",
                    input.counts.get("held").cloned().unwrap_or(0.into()),
                ),
                ("ingest_rejected", rejected_count.into()),
            ]),
        ),
        ("admission_errors", Value::Array(admission)),
        ("connector_errors", input.connector_errors),
        ("sandbox_pool_operator_checks", input.pool_operator_checks),
        (
            "task_cleanup_operator_checks",
            Value::Array(input.task_cleanup_checks),
        ),
    ]))
}
