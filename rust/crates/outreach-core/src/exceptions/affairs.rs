use super::*;
fn active_action(a: &Value) -> bool {
    truth(a)
        && (!a.is_object()
            || !(truth(&a["done"])
                || truth(&a["fulfilled"])
                || truth(&a["superseded_by"])
                || ["done", "completed", "cancelled", "closed"].contains(&s(&a["status"]))))
}
fn describe(a: &Value) -> String {
    if a.is_object() {
        let value = fallback(&[
            &a["summary"],
            &a["quote"],
            &a["what"],
            &a["text"],
            &a["obligation"],
            &a["reason"],
        ]);
        if truth(value) {
            py_str(value)
        } else {
            String::new()
        }
    } else {
        py_str(a)
    }
}
pub(super) fn affairs(
    rows: &[Value],
    env: &Environment,
    persons: &[Value],
    items: &mut Vec<Value>,
) -> Result<()> {
    let terminal = [
        "superseded",
        "answered-by-mail",
        "question-closed",
        "verified",
        "no-reply",
        "owner-discarded",
        "handoff-ready",
        "round-complete",
        "submission-ready",
    ];
    let live: Vec<_> = rows
        .iter()
        .filter(|r| !terminal.contains(&s(&r["state"])))
        .collect();
    let threads = env.tables["threads"]["lines"]
        .as_array()
        .ok_or_else(|| Error::new("Missing threads lines list"))?;
    let papers = env.tables["submissions"]["papers"]
        .as_array()
        .ok_or_else(|| Error::new("Missing submissions papers list"))?;
    for row in threads.iter().chain(papers) {
        outreach_model::required_str(row, "id")?;
    }
    let needs_owner = !threads.is_empty()
        || !papers.is_empty()
        || live
            .iter()
            .any(|r| ["awaiting-user", "owner-draft"].contains(&s(&r["state"])));
    let empty_labels = vec![string("")];
    let owner_labels = if needs_owner {
        env.config["identity"]["owner_labels"]
            .as_array()
            .ok_or_else(|| Error::new("Missing owner_labels list"))?
    } else {
        &empty_labels
    };
    let owner = owner_labels
        .first()
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("Missing owner label"))?;
    let sources: BTreeMap<_, _> = array(&env.tables["threads"]["lines"])
        .iter()
        .chain(array(&env.tables["submissions"]["papers"]))
        .map(|r| (s(&r["id"]), r))
        .collect();
    let closed = ["closed", "declined", "stopped", "cancelled"];
    for thread in threads {
        if thread.get("stage").is_some_and(|v| !v.is_string()) {
            return Err(Error::new("Thread stage must be a string"));
        }
        if closed.contains(&s(&thread["stage"]).to_lowercase().as_str())
            || (!["us", owner].contains(&s(&thread["ball"]))
                && !truth(&thread["our_open_promises"])
                && !truth(&thread["their_open_requests"]))
        {
            continue;
        }
        // Match Python's lazy thread_keys call: with no live task, an affair
        // still produces its missing-action finding even if correspondence
        // metadata contains a malformed address.
        let mut addresses = BTreeSet::new();
        if !live.is_empty() {
            addresses = std::iter::once(&thread["correspondent"])
                .chain(array(&thread["co_correspondents"]))
                .filter(|p| truth(&p["email"]))
                .map(|p| registry::email(&p["email"]))
                .collect::<Result<_>>()?;
            addresses.extend(
                array(&thread["mail_match"]["emails"])
                    .iter()
                    .map(registry::email)
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        let mut ids: BTreeSet<_> = std::iter::once(&thread["id"])
            .chain(array(&thread["owner_ids"]))
            .map(py_str)
            .collect();
        for p in persons {
            if array(&p["emails"]).iter().any(|a| addresses.contains(s(a))) {
                addresses.extend(array(&p["emails"]).iter().map(py_str));
                ids.insert(py_str(&p["id"]));
                ids.extend(array(&p["records"]).iter().map(|r| py_str(&r["id"])));
            }
        }
        let matched = live
            .iter()
            .map(|r| -> Result<bool> {
                if array(&thread["job_ids"]).contains(&r["id"])
                    || r["id"] == thread["running_job_id"]
                {
                    return Ok(true);
                }
                let inputs = &r["request"]["inputs"];
                let recorded = &inputs["thread_record"];
                let direct = fallback(&[&inputs["thread_ref"]["thread_id"], &recorded["id"]]);
                if truth(direct) {
                    return Ok(*direct == thread["id"]);
                }
                let id_match = [&r["owner"], &r["request"]["owner"]]
                    .into_iter()
                    .chain(array(&inputs["owner_ids"]))
                    .any(|v| ids.contains(&py_str(v)));
                let mut task_addresses = BTreeSet::new();
                for value in [
                    &inputs["mailbox_observation"]["email"],
                    &recorded["correspondent"]["email"],
                ]
                .into_iter()
                .chain(array(&inputs["recipient_emails"]))
                {
                    if truth(value) {
                        task_addresses.insert(registry::email(value)?);
                    }
                }
                if s(&r["request"]["owner"]).contains('@') {
                    task_addresses.insert(registry::email(&r["request"]["owner"])?);
                }
                Ok(id_match || task_addresses.iter().any(|value| addresses.contains(value)))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .any(|matched| matched);
        // Inventory validates/matches eligible threads before exceptions keeps
        // only affairs owned by 'us', including threads with open obligations.
        let ball = s(&thread["ball"]);
        if ["us", "them", owner].contains(&ball) && ball != "us" {
            continue;
        }
        if truth(&thread["job_ids"]) || truth(&thread["running_job_id"]) || matched {
            continue;
        }
        let table =
            if s(&thread["stage"]).to_lowercase().replace('_', "-") == "independent-submission" {
                "submissions"
            } else {
                "threads"
            };
        affair_issue(sources[s(&thread["id"])], table, items);
    }
    for p in array(&env.tables["submissions"]["papers"]) {
        if ["submitted", "accepted", "published"].contains(&s(&p["stage"]))
            || closed.contains(&s(&p["stage"]))
            || owner_labels.contains(&p["next_step_owner"])
        {
            continue;
        }
        if live.iter().any(|t| {
            t["request"]["inputs"]["paper"]["id"] == p["id"]
                || t["request"]["inputs"]["submission_source"]["paper_id"] == p["id"]
        }) {
            continue;
        }
        affair_issue(sources[s(&p["id"])], "submissions", items);
    }
    Ok(())
}
fn affair_issue(source: &Value, table: &str, items: &mut Vec<Value>) {
    let action = truth(fallback(&[&source["next_action"], &source["next_step"]]))
        || array(&source["us_actions"])
            .iter()
            .any(|a| active_action(a) && !describe(a).is_empty());
    let waiting = [
        "waiting_for",
        "waiting_condition",
        "waiting",
        "wait",
        "next_check",
    ]
    .iter()
    .any(|k| truth(&source[*k]));
    if !action && !waiting {
        add(
            items,
            "affairs",
            "affair_id",
            &source["id"],
            "Our affair has no next action, linked execution or explicit wait",
            format!("tables/{table}.json:{}", py_str(&source["id"])),
        );
    }
}
