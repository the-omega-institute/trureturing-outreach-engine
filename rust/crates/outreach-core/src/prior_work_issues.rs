//! Prior-work issue audit, exact evidence readback and original-result recovery.
use crate::formalization_requests::{self as f, GitHubIssues, IssueIntent, check, take};
use crate::mathesis_delivery::{DeliveryPorts, Persisted, SourceFile, StageError, StageResult};
use crate::prior_work_evidence as e;
use crate::{array, fallback, ingest::Replay, object, s, string, truth};
use outreach_canon::{Value, digest};
use std::collections::{BTreeMap, BTreeSet};
const LANE: &str = "prior-work-issues";
const EVIDENCE: &str = "prior-work-evidence";
const ISSUE: &str = "prior-work-issue:";
const CHECK: &str = "prior-work-check:";
const BACKFILL: &str = "prior-work-backfill:";
const REQUIRED: &str = "Legacy prior-work evidence requires a signed backfill selection";
const READBACK: &str = "Reviewed evidence artifact requires readback";
pub trait Evidence {
    fn download(&mut self, row: &Value, name: &str) -> StageResult<SourceFile>;
}
fn reviewed(row: &Value) -> StageResult<bool> {
    Ok(
        ["approved", "handoff-ready", "verified"].contains(&s(&row["state"]))
            && crate::transitions::passed(row)?,
    )
}
fn pinned(row: &Value) -> &Value {
    fallback(&[
        &row["request"]["inputs"]["result"],
        &row["request"]["inputs"]["pipeline"]["result"],
    ])
}
pub fn result_id(row: &Value, check: &Value) -> Value {
    let i = &row["request"]["inputs"];
    let id = fallback(&[
        &i["pipeline"]["result_id"],
        &i["result_id"],
        &pinned(row)["id"],
        &check["result_id"],
    ]);
    if crate::regex(r"^[a-zA-Z0-9_.-]{1,200}$").is_ok_and(|r| r.is_match(s(id))) {
        id.clone()
    } else {
        Value::Null
    }
}
fn reference(row: &Value, rid: Value) -> StageResult<Value> {
    Ok(object([
        ("task_id", row["id"].clone()),
        ("result_hash", string(digest(&row["data"]["result"])?)),
        ("result_id", rid),
        ("key", Value::Null),
    ]))
}
fn context(row: &Value, check_value: &Value, tables: &Value) -> StageResult<Value> {
    let i = &row["request"]["inputs"];
    let p = &i["pipeline"];
    let pinned = pinned(row);
    let rid = result_id(row, check_value);
    check(truth(&rid), "Missing or invalid upstream result ID")?;
    check(
        !truth(&check_value["result_id"]) || check_value["result_id"] == rid,
        "Prior-work check refers to a different result",
    )?;
    let current = array(&tables["results"]["results"])
        .iter()
        .find(|v| v["id"] == rid)
        .cloned()
        .unwrap_or(Value::Null);
    let sources = [
        &pinned["upstream"],
        pinned,
        &p["upstream"],
        p,
        &i["upstream"],
        i,
        &current["upstream"],
        &current,
        check_value,
    ];
    let first = |names: &[&str], parse: &dyn Fn(&Value) -> Value| -> Value {
        for source in sources {
            for name in names {
                let v = parse(&source[name]);
                if truth(&v) {
                    return v;
                }
            }
        }
        string("")
    };
    fn line(v: &Value) -> Value {
        string(e::line(v))
    }
    let public = |v: &Value| string(e::public_url(v, s(&tables["data_repo"])));
    fn dated(v: &Value) -> Value {
        if e::date_bounds(v).is_some() {
            line(v)
        } else {
            string("")
        }
    }
    let lean = |v: &Value| -> Value {
        let url = e::public_url(v, s(&tables["data_repo"]));
        if crate::regex(r"^https://github\.com/[^/]+/[^/]+/blob/[^/]+/[^?#]+\.lean(?:#.*)?$")
            .is_ok_and(|r| r.is_match(&url))
        {
            string(url)
        } else {
            string("")
        }
    };
    fn declarations(v: &Value) -> Value {
        let values = if v.is_array() {
            array(v).to_vec()
        } else {
            vec![v.clone()]
        };
        Value::Array(
            values
                .iter()
                .map(e::line)
                .filter(|v| !v.is_empty())
                .map(string)
                .collect(),
        )
    }
    Ok(object([
        ("result_id", rid),
        ("statement", first(&["posed_statement", "statement"], &line)),
        (
            "posing_locator",
            first(&["posing_locator", "source_locator"], &line),
        ),
        ("posing_url", first(&["posing_url", "source_url"], &public)),
        (
            "resolution_date",
            first(&["resolution_date", "resolved_at"], &dated),
        ),
        ("lean_url", first(&["lean_url", "evidence_url"], &lean)),
        (
            "declarations",
            first(&["declaration_gids", "declaration_gid"], &declarations),
        ),
    ]))
}
fn reason(mut item: Value, text: &str) -> Value {
    item["reason"] = string(text);
    item
}
fn assess(row: &Value, check_value: &Value, tables: &Value) -> StageResult<Option<Value>> {
    let mut item = reference(row, result_id(row, check_value))?;
    match context(row, check_value, tables) {
        Ok(c) => {
            for (k, v) in c.as_object().into_iter().flatten() {
                item[k] = v.clone();
            }
        }
        Err(error) => return Ok(Some(reason(item, s(&error.summary["reason"])))),
    }
    if ["statement", "posing_locator"]
        .into_iter()
        .any(|field| e::private(s(&item[field])))
    {
        return Ok(Some(reason(
            item,
            "Posed statement or posing locator contains private-source wording",
        )));
    }
    let public: Vec<_> = array(&check_value["items"])
        .iter()
        .filter(|v| !truth(&v["private"]))
        .collect();
    if public.is_empty() {
        return Ok(Some(reason(
            item,
            if truth(&check_value["items"]) {
                "Only private prior-work evidence; needs a public source"
            } else {
                "Prior-exists evidence has no usable public citation"
            },
        )));
    }
    let Some(own) = e::date_bounds(&item["resolution_date"]) else {
        return Ok(Some(reason(item, "Missing trureturing resolution date")));
    };
    let mut earlier = vec![];
    let mut overlaps = false;
    for citation in &public {
        let Some(released) = e::date_bounds(&citation["date"]) else {
            continue;
        };
        if released.1 < own.0 {
            earlier.push((*citation).clone());
        } else if released.0 <= own.1 && (released.0 != released.1 || own.0 != own.1) {
            overlaps = true;
        }
    }
    if earlier.is_empty() {
        let text = if overlaps {
            Some("A public citation date range overlaps the trureturing resolution date")
        } else if public.iter().any(|c| c["date"] == "unrecognized date") {
            Some("A prior-work release date could not be read")
        } else if public.iter().any(|c| !truth(&c["date"])) {
            Some("A public prior-work citation is missing its release date")
        } else {
            None
        };
        return Ok(text.map(|text| reason(item, text)));
    }
    earlier.sort_by_key(|v| (e::date_bounds(&v["date"]), s(&v["url"]).to_owned()));
    let urls: BTreeSet<_> = earlier.iter().map(|v| s(&v["url"])).collect();
    item["key"] = string(digest(&Value::Array(vec![
        item["result_id"].clone(),
        Value::Array(urls.into_iter().map(string).collect()),
    ]))?);
    item["earlier"] = Value::Array(earlier);
    if !(truth(&item["statement"])
        && truth(&item["posing_locator"])
        && (truth(&item["lean_url"]) || truth(&item["declarations"])))
    {
        item["reason"] =
            string("Missing posed statement, posing locator or trureturing declaration");
    }
    Ok(Some(item))
}
fn marker(id: &Value) -> StageResult<String> {
    Ok(format!(
        "<!-- outreach-prior-work-result:{} -->",
        digest(id)?
    ))
}
pub fn payload(item: &Value) -> StageResult<Value> {
    let mut locator = s(&item["posing_locator"]).replace('|', " / ");
    if locator.chars().count() > 120 {
        locator = take(&locator, 119).trim_end().to_owned() + "…";
    }
    let mut statement = s(&item["statement"]).replace('|', " / ");
    let available = 256 - format!("[Prior work]  | {locator}").chars().count();
    if statement.chars().count() > available {
        statement = take(&statement, available - 1).trim_end().to_owned() + "…";
    }
    let mut posing = s(&item["posing_locator"]).to_owned();
    if truth(&item["posing_url"]) {
        posing += &format!("\n\n{}", s(&item["posing_url"]));
    }
    let mut own = vec![format!("Result ID: `{}`", s(&item["result_id"]))];
    if truth(&item["resolution_date"]) {
        own.push(format!("Resolution date: {}", s(&item["resolution_date"])));
    }
    own.extend(
        array(&item["declarations"])
            .iter()
            .map(|v| format!("`{}`", s(v))),
    );
    if truth(&item["lean_url"]) {
        own.push(s(&item["lean_url"]).into());
    }
    let mut citations = vec![];
    for c in array(&item["earlier"]) {
        if truth(&c["private"]) {
            continue;
        }
        let locator = if truth(&c["locator"]) {
            format!(" · {}", s(&c["locator"]))
        } else {
            String::new()
        };
        let text = format!(
            "- {} · {}{locator}. {}\n  {}",
            s(&c["authors"]),
            s(&c["date"]),
            s(&c["proves"]),
            s(&c["url"])
        );
        if !citations.contains(&text) {
            citations.push(text);
        }
    }
    check(
        !citations.is_empty(),
        "A prior-work issue requires an earlier public citation",
    )?;
    let title = format!("[Prior work] {statement} | {locator}");
    let body = format!(
        "## Posed statement\n\n{}\n\n## Posing locator\n\n{posing}\n\n## trureturing resolution\n\n{}\n\n## Prior-work evidence\n\n{}\n\n## Requested upstream update\n\nCredit the earliest verified work in the trureturing records and Atlas. Mark the trureturing proof as an alternative or independent resolution, as appropriate. Audit the upstream prior-work search to establish why it missed these sources.\n\n{}\n\n<!-- outreach-prior-work:{} -->",
        s(&item["statement"]),
        own.join("\n\n"),
        citations.join("\n"),
        marker(&item["result_id"])?,
        s(&item["key"])
    );
    fn mentions(v: &str) -> String {
        let chars: Vec<_> = v.chars().collect();
        let mut out = String::new();
        for (i, c) in chars.iter().enumerate() {
            out.push(*c);
            if *c == '@'
                && chars
                    .get(i + 1)
                    .is_some_and(|c| c.is_alphanumeric() || *c == '_')
                && (i == 0
                    || !(chars[i - 1].is_alphanumeric()
                        || "_/ .".replace(' ', "").contains(chars[i - 1])))
            {
                out.push('\u{200b}');
            }
        }
        out
    }
    Ok(object([
        ("title", string(mentions(&title))),
        ("body", string(mentions(&body))),
    ]))
}
fn state(r: &Replay<'_>, prefix: &str) -> BTreeMap<String, Value> {
    r.rows("pipeline_state")
        .iter()
        .filter_map(|v| {
            s(&v["key"])
                .strip_prefix(prefix)
                .map(|k| (k.to_owned(), v["payload"].clone()))
        })
        .collect()
}
fn artifacts(row: &Value) -> Vec<Value> {
    array(&row["data"]["result"]["artifacts"])
        .iter()
        .filter(|v| {
            let name = s(&v["name"]);
            let lower = name.to_lowercase();
            v["role"] == "evidence"
                && crate::regex(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")
                    .is_ok_and(|r| r.is_match(name))
                && [".md", ".txt"].iter().any(|v| lower.ends_with(v))
                && !["private", "correspondence", "mail"]
                    .iter()
                    .any(|v| lower.contains(v))
        })
        .cloned()
        .collect()
}
fn progress(row: &Value, checks: &BTreeMap<String, Value>) -> StageResult<Value> {
    let value = checks.get(s(&row["id"])).cloned().unwrap_or(Value::Null);
    Ok(
        if value["version"] == 2 && value["result_hash"] == digest(&row["data"]["result"])? {
            value
        } else {
            object([])
        },
    )
}
fn checked(
    row: &Value,
    checks: &BTreeMap<String, Value>,
    backfill: &BTreeMap<String, Value>,
    data_repo: &str,
) -> StageResult<Value> {
    if let Some(value) = row["data"]["result"].get("prior_work_check") {
        return Ok(object([("check", e::project(value, data_repo)?)]));
    }
    if artifacts(row).is_empty() {
        return Ok(object([("check", Value::Null)]));
    }
    if backfill.get(s(&row["id"])) != Some(&string(digest(&row["data"]["result"])?)) {
        return Ok(object([("reason", string(REQUIRED))]));
    }
    let saved = progress(row, checks)?;
    if saved.get("check").is_some() || saved.get("reason").is_some() {
        Ok(saved)
    } else {
        Ok(object([("reason", string(READBACK))]))
    }
}
fn publication_reason(row: &Value, rows: &[Value], tables: &Value, rid: &Value) -> Option<String> {
    let decisions = &tables["decisions"];
    match crate::publication::issue_exclusion(
        row,
        rows,
        decisions,
        &decisions["short_note_publication"],
        rid,
    ) {
        Err(_) => return Some("Publication eligibility could not be verified".into()),
        Ok(Some(text)) => return Some(format!("Excluded from public filing: {text}")),
        _ => {}
    }
    let i = &row["request"]["inputs"];
    let p = &i["pipeline"];
    let pinned = pinned(row);
    let current = array(&tables["results"]["results"])
        .iter()
        .find(|v| v["id"] == *rid)
        .cloned()
        .unwrap_or(Value::Null);
    let excluded = crate::regex(r"(?i)(?:^|[^a-z0-9])(?:sair|cpp)(?:$|[^a-z0-9])").expect("regex");
    if excluded.is_match(s(rid))
        || [
            i,
            p,
            pinned,
            &pinned["upstream"],
            &current,
            &current["upstream"],
        ]
        .iter()
        .any(|source| {
            ["track", "project", "project_id", "repository"]
                .iter()
                .any(|k| excluded.is_match(&e::line(&source[k])))
        })
    {
        Some("Excluded from public filing: SAIR/CPP work".into())
    } else {
        None
    }
}
fn state_key(item: &Value) -> String {
    if truth(&item["result_id"]) {
        s(&item["result_id"]).into()
    } else {
        format!("task:{}", s(&item["task_id"]))
    }
}
fn candidates(
    rows: &[Value],
    checks: &BTreeMap<String, Value>,
    tables: &Value,
    existing: &BTreeMap<String, Value>,
    backfill: &BTreeMap<String, Value>,
) -> StageResult<Vec<Value>> {
    let mut selected: Vec<Value> = vec![];
    for row in rows {
        if !reviewed(row)? {
            continue;
        }
        let inspected = checked(row, checks, backfill, s(&tables["data_repo"]))?;
        let check_value = &inspected["check"];
        if check_value["verdict"] != "prior-exists" && !truth(&inspected["reason"]) {
            continue;
        }
        let rid = result_id(row, check_value);
        if existing.get(s(&rid)).is_some_and(|v| v["state"] == "filed") {
            continue;
        }
        let reason = publication_reason(row, rows, tables, &rid)
            .or_else(|| inspected["reason"].as_str().map(str::to_owned));
        let item = if let Some(text) = reason {
            Some(self::reason(reference(row, rid)?, &text))
        } else {
            assess(row, check_value, tables)?
        };
        let Some(item) = item else { continue };
        let key = state_key(&item);
        if let Some(old) = selected.iter_mut().find(|v| state_key(v) == key) {
            if old.get("reason").is_some() && item.get("reason").is_none() {
                *old = item;
            }
        } else {
            selected.push(item);
        }
    }
    Ok(selected)
}
fn read_failure(error: &StageError) -> String {
    let kind = s(&error.summary["type"]);
    if ["GateError", "UnparseableEvidence"].contains(&kind) {
        s(fallback(&[
            &error.summary["reason"],
            &error.summary["message"],
        ]))
        .into()
    } else if let Some(status) = error.status() {
        format!("Prior-work evidence download failed (HTTP {status})")
    } else if kind == "RuntimeError" {
        let message = s(&error.summary["message"]);
        let status = crate::regex(r"\bHTTP [45]\d\d\b")
            .ok()
            .and_then(|r| r.find(message).map(|m| m.as_str().to_owned()));
        format!(
            "Prior-work evidence download failed{}",
            status.map_or(String::new(), |s| format!(" ({s})"))
        )
    } else {
        format!("Prior-work evidence could not be read ({kind})")
    }
}
pub fn advance(
    r: &mut Replay<'_>,
    client: &mut impl GitHubIssues,
    evidence: &mut impl Evidence,
    ports: &mut impl DeliveryPorts,
) -> StageResult<()> {
    if truth(&r.environment.config["paused"]) {
        return Ok(());
    }
    let rows = r.rows("tasks").to_vec();
    let backfill = state(r, BACKFILL);
    let mut checks = state(r, CHECK);
    let existing = state(r, ISSUE);
    let tables = r.environment.tables.clone();
    let mut legacy = vec![];
    for row in &rows {
        let id = s(&row["id"]);
        if !reviewed(row)?
            || row["data"]["result"].get("prior_work_check").is_some()
            || backfill.get(id) != Some(&string(digest(&row["data"]["result"])?))
            || artifacts(row).is_empty()
        {
            continue;
        }
        let rid = result_id(row, &Value::Null);
        if existing.get(s(&rid)).is_none_or(|v| v["state"] != "filed")
            && publication_reason(row, &rows, &tables, &rid).is_none()
        {
            let binding = string(digest(&row["data"]["result"])?);
            if progress(row, &checks)?.get("check").is_none() && f::due(r, EVIDENCE, id, &binding)?
            {
                legacy.push(row.clone());
            }
        }
    }
    legacy.truncate(8);
    let previous = ports.previous(EVIDENCE);
    legacy.sort_by_key(|row| {
        previous
            .iter()
            .position(|key| *key == s(&row["id"]))
            .unwrap_or(previous.len())
    });
    for row in legacy {
        let id = s(&row["id"]);
        if !ports.allow(EVIDENCE, id) {
            continue;
        }
        let binding = string(digest(&row["data"]["result"])?);
        let saved = progress(&row, &checks)?;
        let index = saved["next_file"].as_u64().unwrap_or(0) as usize;
        let files = artifacts(&row);
        let artifact = files
            .get(index)
            .ok_or_else(|| outreach_canon::Error::new("Invalid evidence index"))?;
        let outcome = evidence
            .download(&row, s(&artifact["name"]))
            .and_then(|file| e::parse_file(&file.bytes, artifact, s(&tables["data_repo"])));
        let mut progress = match outcome {
            Ok(value) if !value.is_null() || index + 1 == files.len() => object([("check", value)]),
            Ok(_) => object([("next_file", (index + 1).into())]),
            Err(error) if error.summary["type"] == "UnparseableEvidence" => object([
                ("check", Value::Null),
                ("reason", string(read_failure(&error))),
            ]),
            Err(error) if f::catchable(&error) || error.summary["type"] == "RuntimeError" => {
                let failures = saved["failures"].as_i64().unwrap_or(0) + 1;
                f::defer(
                    r,
                    EVIDENCE,
                    id,
                    &binding,
                    (1800 * 2_i64.pow((failures - 1).min(6) as u32)).min(86400),
                )?;
                let reason = string(read_failure(&error));
                object([
                    ("next_file", index.into()),
                    ("failures", failures.into()),
                    ("last_error", reason.clone()),
                    ("reason", reason),
                ])
            }
            Err(error) => return Err(error),
        };
        progress["version"] = 2.into();
        progress["result_hash"] = binding;
        checks.insert(id.into(), progress.clone());
        r.set_state(&format!("{CHECK}{id}"), progress)?;
        ports.save(&r.tables)?;
    }
    let mut pending = vec![];
    for item in candidates(&rows, &checks, &tables, &existing, &backfill)? {
        let key = state_key(&item);
        let mut entry = object([
            ("task_id", item["task_id"].clone()),
            ("result_id", item["result_id"].clone()),
            ("key", item["key"].clone()),
            ("result_hash", item["result_hash"].clone()),
            ("state", string("pending")),
        ]);
        if let Some(reason) = item.get("reason") {
            entry["reason"] = reason.clone();
            if existing.get(&key) != Some(&entry) {
                pending.push((item, entry));
            }
        } else if f::due(r, LANE, &key, &entry)? {
            pending.push((item, entry));
        }
    }
    let mut issues = None;
    let mut connector = if client.available() {
        None
    } else {
        Some("Coordinator GitHub connection unavailable")
    };
    pending.truncate(8);
    let previous = ports.previous(LANE);
    pending.sort_by_key(|(_, entry)| {
        previous
            .iter()
            .position(|key| *key == state_key(entry))
            .unwrap_or(previous.len())
    });
    for (item, mut entry) in pending {
        let key = state_key(&entry);
        if !ports.allow(LANE, &key) {
            continue;
        }
        let rid = entry["result_id"].clone();
        if existing.get(&key) != Some(&entry) {
            r.set_state(&format!("{ISSUE}{key}"), entry.clone())?;
            let orphan = format!("task:{}", s(&entry["task_id"]));
            if truth(&rid) && existing.contains_key(&orphan) {
                r.clear_state(&format!("{ISSUE}{orphan}"))?;
            }
            ports.save(&r.tables)?;
        }
        if entry.get("reason").is_some() {
            continue;
        }
        let outcome = (|| -> StageResult<Option<String>> {
            check(connector.is_none(), connector.unwrap_or(""))?;
            if issues.is_none() {
                issues = f::inventory(client, ports, LANE, false)?;
                if issues.is_none() {
                    return Ok(None);
                }
            }
            let marker = marker(&rid)?;
            let legacy = format!("<!-- outreach-prior-work:{} -->", s(&item["key"]));
            let mut receipt = issues
                .as_ref()
                .into_iter()
                .flatten()
                .find(|v| s(&v["body"]).contains(&marker) || s(&v["body"]).contains(&legacy))
                .cloned();
            if receipt.is_none() {
                f::defer(r, LANE, s(&rid), &entry, 1800)?;
                // Python checkpoints before constructing the payload. Keep that
                // ordering, including a payload gate failure after a saved intent.
                ports.save_forced(&r.tables)?;
                let payload = payload(&item)?;
                let permit = Persisted::saved(
                    IssueIntent {
                        payload: payload.clone(),
                    },
                    ports,
                    &r.tables,
                )?;
                let mut value = client.create(permit)?;
                check(value.is_object(), "Invalid GitHub issue receipt")?;
                receipt = Some(value.clone());
                for (k, v) in payload.as_object().into_iter().flatten() {
                    value[k] = v.clone();
                }
                if let Some(issues) = issues.as_mut() {
                    issues.push(value);
                }
            }
            Ok(Some(f::receipt(&receipt.unwrap_or(Value::Null))?))
        })();
        match outcome {
            Ok(None) => return Ok(()),
            Ok(Some(url)) => {
                entry["state"] = string("filed");
                entry["issue_url"] = string(url);
            }
            Err(error) if f::catchable(&error) => {
                f::defer(r, LANE, s(&rid), &entry, 1800)?;
                entry["last_error"] = string(f::error_name(&error, connector));
                connector = connector.or(Some("GitHub filing unavailable this tick"));
                issues = None;
            }
            Err(error) => return Err(error),
        }
        r.set_state(&format!("{ISSUE}{}", s(&rid)), entry)?;
        ports.save(&r.tables)?;
    }
    Ok(())
}

/// Offline preview uses the same candidate projection as the coordinator. Local
/// evidence is optional and cannot silently authorize a legacy backfill.
pub fn plan(
    snapshot: &Value,
    tables: &Value,
    local: Option<&BTreeMap<String, Vec<u8>>>,
) -> StageResult<Value> {
    let rows = if let Some(rows) = snapshot.get("tasks") {
        array(rows).to_vec()
    } else {
        array(&snapshot["affairs_tasks"])
            .iter()
            .map(|v| {
                let mut row = v.clone();
                row["data"] = object([
                    ("result", v["result"].clone()),
                    ("review", v.get("review").cloned().unwrap_or(object([]))),
                ]);
                row
            })
            .collect()
    };
    let mut checks: BTreeMap<_, _> = snapshot["prior_work_checks"]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let backfill: BTreeMap<_, _> = snapshot["prior_work_backfill"]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let mut preview = backfill.clone();
    let existing: BTreeMap<_, _> = array(&snapshot["prior_work_issues"])
        .iter()
        .map(|v| (state_key(v), v.clone()))
        .collect();
    if let Some(local) = local {
        for row in &rows {
            if !reviewed(row)?
                || ![REQUIRED, READBACK].contains(&s(&checked(
                    row,
                    &checks,
                    &backfill,
                    s(&tables["data_repo"]),
                )?["reason"]))
            {
                continue;
            }
            let mut inspected = object([("check", Value::Null)]);
            let mut missing = false;
            for artifact in artifacts(row) {
                let path = format!("{}/{}", s(&row["id"]), s(&artifact["name"]));
                let Some(raw) = local.get(&path) else {
                    missing = true;
                    break;
                };
                match e::parse_file(raw, &artifact, s(&tables["data_repo"])) {
                    Ok(value) if !value.is_null() => {
                        inspected = object([("check", value)]);
                        break;
                    }
                    Ok(_) => {}
                    Err(error) if f::catchable(&error) => {
                        inspected = object([("reason", string(read_failure(&error)))]);
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
            if !missing {
                let binding = string(digest(&row["data"]["result"])?);
                preview.insert(s(&row["id"]).into(), binding.clone());
                inspected["version"] = 2.into();
                inspected["result_hash"] = binding;
                checks.insert(s(&row["id"]).into(), inspected);
            }
        }
    }
    let mut notes = vec![];
    let mut unread = vec![];
    for item in candidates(&rows, &checks, tables, &existing, &preview)? {
        let mut visible = crate::mailbox::fields(
            &item,
            &["task_id", "result_hash", "result_id", "key", "reason"],
        );
        if item.get("reason").is_none() {
            visible["issue"] = payload(&item)?;
            if backfill.get(s(&item["task_id"])) != Some(&item["result_hash"])
                && preview.get(s(&item["task_id"])) == Some(&item["result_hash"])
            {
                visible["requires_backfill"] = true.into();
            }
        }
        if [REQUIRED, READBACK].contains(&s(&item["reason"])) {
            unread.push(visible);
        } else {
            notes.push(visible);
        }
    }
    Ok(object([
        (
            "count",
            notes
                .iter()
                .filter(|v| v.get("issue").is_some())
                .count()
                .into(),
        ),
        ("notes", Value::Array(notes)),
        ("needs_evidence", Value::Array(unread)),
    ]))
}
