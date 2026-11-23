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
        py_str(fallback(&[
            &a["summary"],
            &a["quote"],
            &a["what"],
            &a["text"],
            &a["obligation"],
            &a["reason"],
            &string(""),
        ]))
    } else {
        py_str(a)
    }
}
pub(super) fn affairs(
    rows: &[Value],
    env: &Environment,
    persons: &[Value],
    items: &mut Vec<Value>,
) {
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
    let owner_labels = array(&env.config["identity"]["owner_labels"]);
    let owner = owner_labels.first().map(s).unwrap_or("");
    let closed = ["closed", "declined", "stopped", "cancelled"];
    for thread in array(&env.tables["threads"]["lines"]) {
        if closed.contains(&s(&thread["stage"]).to_lowercase().as_str())
            || (!["us", owner].contains(&s(&thread["ball"]))
                && !truth(&thread["our_open_promises"])
                && !truth(&thread["their_open_requests"]))
        {
            continue;
        }
        // Only affairs whose owner is exactly 'us' pass the inventory gate.
        let ball = s(&thread["ball"]);
        if ["us", "them", owner].contains(&ball) && ball != "us" {
            continue;
        }
        let mut addresses: BTreeSet<_> = std::iter::once(&thread["correspondent"])
            .chain(array(&thread["co_correspondents"]))
            .map(|p| registry::address(&p["email"]))
            .filter(|s| !s.is_empty())
            .collect();
        addresses.extend(
            array(&thread["mail_match"]["emails"])
                .iter()
                .map(registry::address),
        );
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
        let matched = live.iter().any(|r| {
            if array(&thread["job_ids"]).contains(&r["id"]) || r["id"] == thread["running_job_id"] {
                return true;
            }
            let inputs = &r["request"]["inputs"];
            let recorded = &inputs["thread_record"];
            let direct = fallback(&[&inputs["thread_ref"]["thread_id"], &recorded["id"]]);
            if truth(direct) {
                return *direct == thread["id"];
            }
            if [&r["owner"], &r["request"]["owner"]]
                .into_iter()
                .chain(array(&inputs["owner_ids"]))
                .any(|v| ids.contains(&py_str(v)))
            {
                return true;
            }
            [
                &inputs["mailbox_observation"]["email"],
                &recorded["correspondent"]["email"],
                &r["request"]["owner"],
            ]
            .into_iter()
            .chain(array(&inputs["recipient_emails"]))
            .any(|v| s(v).contains('@') && addresses.contains(&registry::address(v)))
        });
        if truth(&thread["job_ids"]) || truth(&thread["running_job_id"]) || matched {
            continue;
        }
        affair_issue(thread, "threads", items);
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
        affair_issue(p, "submissions", items);
    }
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
