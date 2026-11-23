use super::*;
fn paper_summary(p: &Value) -> Value {
    object([
        ("id", p["id"].clone()),
        ("title", p["title"].clone()),
        ("authors", p["authors"].clone()),
        (
            "version_date",
            fallback(&[&p["version_date"], &p["version"]["date"]]).clone(),
        ),
        (
            "target_journal",
            if p["target_journal"].is_string() {
                p["target_journal"].clone()
            } else {
                p["target_journal"]["name"].clone()
            },
        ),
        (
            "workspace",
            fallback(&[&p["workspace"], &p["local_package"]]).clone(),
        ),
        ("stage", p["stage"].clone()),
        (
            "blocker",
            string(if truth(&p["blocker"]) {
                py_str(&p["blocker"])
            } else {
                array(&p["blocking_items"])
                    .iter()
                    .map(|b| py_str(&b["reason"]))
                    .collect::<Vec<_>>()
                    .join("; ")
            }),
        ),
    ])
}
pub(super) fn submissions(rows: &[Value], env: &Environment, items: &mut Vec<Value>) -> Result<()> {
    let papers = array(&env.tables["submissions"]["papers"]);
    let mut matched = BTreeSet::new();
    let title = |v: &Value| text::compact(&casefold(if truth(v) { s(v) } else { "" }), usize::MAX);
    for task in rows.iter().filter(|r| r["kind"] == "submission") {
        let mut inputs = task["request"]["inputs"].clone();
        if let Some(fields) = task["data"]["thread_inputs"].as_object() {
            inputs.as_object_mut().unwrap().extend(fields.clone());
        }
        let pinned = &inputs["paper"];
        let link = &inputs["submission_source"];
        let candidates: Vec<_> = papers
            .iter()
            .filter(|p| title(&p["title"]) == title(&pinned["title"]))
            .collect();
        let paper = papers
            .iter()
            .find(|p| p["id"] == pinned["id"])
            .or_else(|| papers.iter().find(|p| p["id"] == link["paper_id"]))
            .or_else(|| (candidates.len() == 1).then(|| candidates[0]));
        let (paper_id, reason) = if let Some(paper) = paper {
            matched.insert(py_str(&paper["id"]));
            let mut reasons = Vec::new();
            if [&pinned["id"], &link["paper_id"]]
                .iter()
                .any(|v| truth(v) && **v != paper["id"])
            {
                reasons.push("paper id mismatch");
            }
            if truth(&pinned["title"]) && title(&pinned["title"]) != title(&paper["title"]) {
                reasons.push("title mismatch");
            }
            let current = paper_summary(paper);
            if (truth(link)
                && link["identity"] != digest(&current)?
                && link["identity"]
                    != digest(&object([
                        ("id", current["id"].clone()),
                        ("authors", current["authors"].clone()),
                        ("workspace", current["workspace"].clone()),
                    ]))?)
                || ["authors", "workspace"].iter().any(|key| {
                    pinned.get(*key).is_some()
                        && !outreach_canon::python_eq(&pinned[*key], &current[*key])
                })
            {
                reasons.push("source identity mismatch");
            }
            let version = fallback(&[&pinned["version_date"], &pinned["version"]["date"]]);
            if truth(version) && *version != current["version_date"] {
                reasons.push("version mismatch");
            }
            if truth(&pinned["stage"]) && pinned["stage"] != current["stage"] {
                reasons.push("stage mismatch");
            }
            (paper["id"].clone(), reasons.join("; "))
        } else {
            (
                fallback(&[&pinned["id"], &link["paper_id"], &string("?")]).clone(),
                "Submission task paper is not on the board".into(),
            )
        };
        if !reason.is_empty() {
            add(
                items,
                "submissions",
                "task_id",
                &task["id"],
                reason,
                format!(
                    "tables/submissions.json:{}; request.inputs.paper/submission_source",
                    py_str(&paper_id)
                ),
            );
        }
    }
    for paper in papers {
        if !matched.contains(&py_str(&paper["id"])) {
            add(
                items,
                "submissions",
                "paper_id",
                &paper["id"],
                "Board paper has no submission task",
                format!("tables/submissions.json:{}", py_str(&paper["id"])),
            );
        }
    }
    Ok(())
}
