use crate::*;
use std::collections::BTreeSet;
pub fn review_digest(work: &Value, env: &Environment) -> Result<Value> {
    let persons = registry::digest_people(&env.tables)?;
    let mut items = Vec::new();
    let points = regex(r"(?:^|\s)(\d+)\.\s")?;
    let feedback_check = regex(r"^feedback \d+$")?;
    if truth(&work["reviews"]) && !work["reviews"].is_array() {
        return Err(Error::new("Reviews must be a list"));
    }
    for item in array(&work["reviews"]) {
        if !item.is_object() {
            return Err(Error::new("Review entry must be an object"));
        }
        for value in [
            &item["request"],
            &item["result"],
            &item["binding"],
            &item["request"]["inputs"],
        ] {
            if truth(value) && !value.is_object() {
                return Err(Error::new(
                    "Review request/result/binding/inputs must be objects",
                ));
            }
        }
        let request = &item["request"];
        let result = &item["result"];
        let inputs = &request["inputs"];
        let binding = &item["binding"];
        let kind = request
            .get("kind")
            .or_else(|| item.get("kind"))
            .cloned()
            .unwrap_or(string("unknown"));
        if kind.is_array() || kind.is_object() {
            return Err(Error::new("Review kind must be a scalar"));
        }
        if kind == "submission"
            && result["outcome"] == "completed"
            && !truth(&item["submission_review_requested"])
        {
            continue;
        }
        let mut labels = Vec::new();
        let mut values = vec![&request["owner"]];
        values.extend(array(&inputs["owner_ids"]));
        for value in values {
            if !truth(value) {
                continue;
            }
            let person = persons
                .iter()
                .rev()
                .find(|p| p["id"] == *value)
                .or_else(|| {
                    if s(value).contains('@') {
                        persons.iter().rev().find(|p| {
                            array(&p["emails"])
                                .iter()
                                .any(|a| s(a) == casefold(s(value)))
                        })
                    } else {
                        None
                    }
                });
            labels.push(
                person
                    .map(|p| py_str(&p["name"]))
                    .unwrap_or_else(|| py_str(value)),
            );
        }
        let result_id = if inputs["result"].is_object() {
            &inputs["result"]["id"]
        } else {
            &inputs["result_id"]
        };
        let result_id = fallback(&[result_id, &result["id"], &result["result_id"]]);
        if truth(result_id) {
            labels.push(py_str(result_id));
        }
        let mut seen = BTreeSet::new();
        labels.retain(|v| seen.insert(v.clone()));
        let raw_report = result.get("report");
        if !truth(&result["headline"])
            && !truth(&result["owner_brief"]["summary_zh"])
            && raw_report.is_some_and(|v| !v.is_string())
        {
            return Err(Error::new(
                "Report must be a string when used as the headline",
            ));
        }
        let report = s(&result["report"]);
        let first = report.split_once(". ").map(|(a, _)| a).unwrap_or(report);
        let headline = fallback(&[&result["headline"], &result["owner_brief"]["summary_zh"]]);
        if truth(headline) && !headline.is_string() {
            return Err(Error::new("Headline must be a string"));
        }
        let headline = text::compact(if truth(headline) { s(headline) } else { first }, 200);
        let mut flags = Vec::new();
        let mut feedback = "feedback —".to_owned();
        if let Some(reason) = inputs["review_feedback"]["reason"].as_str() {
            let points: BTreeSet<_> = points
                .captures_iter(reason)
                .map(|c| c[1].to_owned())
                .collect();
            let passed: BTreeSet<_> = array(&result["self_audit"])
                .iter()
                .filter(|a| a["status"] == "pass" && feedback_check.is_match(s(&a["check"])))
                .map(|a| s(&a["check"]).split_whitespace().nth(1).unwrap().to_owned())
                .collect();
            let resolved = points.intersection(&passed).count();
            let total = points.len();
            feedback = format!("feedback {resolved}/{total}");
            if resolved < total {
                flags.push(format!("feedback unresolved: {}/{total}", total - resolved));
            }
        }
        let empty_report = string("");
        if result["outcome"] == "hold" || item["state"] == "held" {
            let reason = if truth(&item["reason"]) {
                &item["reason"]
            } else {
                result.get("report").unwrap_or(&empty_report)
            };
            flags.push(format!(
                "agent hold: {}",
                text::compact(&py_str(reason), 80)
            ));
        }
        if array(&result["self_audit"])
            .iter()
            .any(|a| a["status"] == "fail")
        {
            flags.push("self_audit fail".into());
        }
        let task = fallback(&[&binding["task_id"], &request["id"], &item["task_id"]]);
        if let Some(covered) = array(&work["note_coverage_overlap"])
            .iter()
            .rev()
            .find(|r| r["task_id"] == *task)
            && truth(&covered["covering_note_task_id"])
        {
            flags.push(format!(
                "covered by PASSed note {}",
                py_str(&covered["covering_note_task_id"])
            ));
        }
        if result["routing"]["mail"] == "owner" {
            flags.push("routing owner".into());
        }
        let current = (env.workflow)(s(&kind)).unwrap_or(Value::Null);
        let changed = truth(&current)
            && truth(&request["workflow_hash"])
            && !reviews::matches_workflow(
                &object([
                    ("request", request.clone()),
                    (
                        "data",
                        object([("workflow_binding", item["workflow_binding"].clone())]),
                    ),
                ]),
                &current,
            );
        if changed {
            flags.push("workflow changed: rebind or rerun".into());
        }
        let mut rounds = Vec::new();
        for key in ["note_round", "submission_round"] {
            if !inputs[key].is_null() {
                rounds.push(format!("{key}={}", py_str(&inputs[key])));
            }
        }
        let attachments = if result["packet"]["attachments"].is_array() {
            array(&result["packet"]["attachments"]).len()
        } else {
            array(&result["artifacts"]).len()
        };
        let output = object([
            (
                "task_id",
                if truth(task) {
                    task.clone()
                } else {
                    string("")
                },
            ),
            ("kind", kind),
            (
                "counterpart",
                string(if labels.is_empty() {
                    "—".into()
                } else {
                    labels.join(", ")
                }),
            ),
            (
                "round",
                string(if rounds.is_empty() {
                    "—".into()
                } else {
                    rounds.join(", ")
                }),
            ),
            (
                "outcome",
                result.get("outcome").cloned().unwrap_or(string("—")),
            ),
            ("headline", string(headline)),
            ("feedback", string(feedback)),
            (
                "risk_flags",
                Value::Array(flags.into_iter().map(string).collect()),
            ),
            ("attachments", attachments.into()),
            ("workflow_changed", changed.into()),
            (
                "revision",
                binding
                    .get("task_revision")
                    .or_else(|| item.get("revision"))
                    .cloned()
                    .unwrap_or(string("—")),
            ),
        ]);
        items.push(output);
    }
    items.sort_by_key(|i| {
        (
            std::cmp::Reverse(array(&i["risk_flags"]).len()),
            s(&i["task_id"]).to_owned(),
        )
    });
    let mut lines = Vec::new();
    for i in &items {
        let flags = array(&i["risk_flags"])
            .iter()
            .map(py_str)
            .collect::<Vec<_>>()
            .join("; ");
        lines.push(string(format!(
            "{} | {} | {} | {} | {} | {} | risks: {} | attachments: {} | revision: {}",
            py_str(&i["task_id"]),
            py_str(&i["kind"]),
            py_str(&i["counterpart"]),
            py_str(&i["round"]),
            py_str(&i["outcome"]),
            py_str(&i["feedback"]),
            if flags.is_empty() { "none" } else { &flags },
            py_str(&i["attachments"]),
            py_str(&i["revision"])
        )));
        lines.push(string(format!("  ↳ {}", s(&i["headline"]))));
    }
    Ok(object([
        ("items", Value::Array(items)),
        ("lines", Value::Array(lines)),
    ]))
}
