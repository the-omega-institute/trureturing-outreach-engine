//! Read-only source, ownership, continuation and sent-revision gates.
use crate::{validation::*, *};
use outreach_canon::{digest, python_eq};
use std::collections::{BTreeMap, BTreeSet};
fn get<'a>(rows: &'a [Value], id: &Value) -> Result<&'a Value> {
    rows.iter()
        .find(|r| r["id"] == *id)
        .ok_or_else(|| Error::new("Unknown task"))
}
pub fn execution(row: &Value, result: &Value, rows: &[Value], env: &Environment<'_>) -> Result<()> {
    require(
        crate::reviews::matches_workflow(row, &(env.workflow)(s(&row["kind"]))?),
        "Workflow changed; wait for coordinator re-pin",
    )?;
    pipeline(&row["request"], rows, env)?;
    crate::transitions::source_gate(&row["request"], result, rows)?;
    parent(&row["request"], rows, env)?;
    paper(&row["request"], env)?;
    owner(&row["request"], env)?;
    crate::results::firstcontact_gate(&row["request"], env)?;
    thread(&row["request"], row["data"].get("thread_inputs"), env)
}
pub fn pipeline(request: &Value, rows: &[Value], env: &Environment<'_>) -> Result<()> {
    let i = &request["inputs"];
    let link = &i["pipeline"];
    if link.is_null() {
        return Ok(());
    }
    require(link.is_object(), "Pipeline provenance must be an object")?;
    let n = link["scenario"].as_i64();
    require(
        integer(&link["scenario"]) && n.is_some_and(|n| [2, 3, 4, 5].contains(&n)),
        "Unknown pipeline scenario",
    )?;
    if n == Some(5) {
        return require(
            request["kind"] == "daily",
            "A daily source window requires a daily task",
        );
    }
    require(
        ["result_id", "source_identity"]
            .iter()
            .all(|k| link[k].is_string() && truth(&link[k])),
        "Missing pipeline source identity",
    )?;
    let source = array(&env.tables["results"]["results"])
        .iter()
        .find(|r| r["id"] == link["result_id"]);
    require(
        source.is_some_and(|r| r["upstream_status"] == "resolved-at-current-snapshot"),
        "Upstream result is no longer resolved at the current snapshot",
    )?;
    let source = source.ok_or_else(|| Error::new("Missing source"))?;
    let mut declarations = array(&source["upstream"]["declaration_gids"]).to_vec();
    declarations.sort_by(|a, b| s(a).cmp(s(b)));
    require(
        link["source_identity"]
            == digest(&object([
                ("id", source["id"].clone()),
                ("repository", source["upstream"]["repository"].clone()),
                ("kind", source["upstream"]["resolution_kind"].clone()),
                ("declarations", Value::Array(declarations)),
            ]))?,
        "Upstream result identity changed; review the new source",
    )?;
    if n == Some(3) {
        return require(
            request["kind"] == "shortnote",
            "An upstream result first requires a short note",
        );
    }
    require(
        ["note_task_id", "note_result_hash"]
            .iter()
            .all(|k| link[k].is_string() && truth(&link[k])),
        "Missing reviewed note parent",
    )?;
    require(
        request["kind"]
            == if n == Some(2) {
                "firstcontact"
            } else {
                "mathesis"
            },
        "Wrong downstream task kind",
    )?;
    let p = get(rows, &link["note_task_id"])?;
    require(
        p["kind"] == "shortnote"
            && ["approved", "handoff-ready", "verified"].contains(&s(&p["state"])),
        "The source note is not approved",
    )?;
    require(
        crate::transitions::eligible(p),
        "The source note needs a non-trivial final round",
    )?;
    require(
        p["data"]["review"]["verdict"] == "PASS",
        "The source note needs independent review",
    )?;
    require(
        link["note_result_hash"] == digest(&p["data"]["result"])?,
        "The approved note version changed",
    )?;
    let pl = &p["request"]["inputs"]["pipeline"];
    require(
        python_eq(&pl["scenario"], &3.into())
            && pl["result_id"] == link["result_id"]
            && pl["source_identity"] == link["source_identity"],
        "Wrong upstream parent",
    )
}
pub fn parent(request: &Value, rows: &[Value], env: &Environment<'_>) -> Result<()> {
    let mut request = request;
    let mut seen = BTreeSet::new();
    while !request["inputs"]["automation_parent"].is_null() {
        require(
            seen.insert(s(&request["id"]).to_owned()),
            "Continuation parent cycle",
        )?;
        let link = &request["inputs"]["automation_parent"];
        require(
            keys(link, &["task_id", "result_hash", "purpose"], &[]),
            "Invalid continuation parent",
        )?;
        let p = get(rows, &link["task_id"])?;
        require(
            link["result_hash"] == digest(&p["data"]["result"])?,
            "Continuation source version changed",
        )?;
        let reviewed = [
            "approved",
            "handoff-ready",
            "verified",
            "awaiting-mailbox",
            "owner-draft",
            "no-reply",
            "round-complete",
            "submission-ready",
        ]
        .contains(&s(&p["state"]));
        match s(&link["purpose"]) {
            "merge-sent-revision" => {
                let links = &request["inputs"]["note_sources"];
                require(
                    request["kind"] == "shortnote"
                        && array(links).len() >= 2
                        && array(links).get(1).is_some_and(|l| {
                            l["task_id"] == p["id"] && l["result_hash"] == link["result_hash"]
                        }),
                    "Sent note merge needs exact source note and reply bindings",
                )?;
                for (n, l) in array(links).iter().enumerate() {
                    let source = get(rows, &l["task_id"])?;
                    require(
                        keys(l, &["task_id", "result_hash"], &[])
                            && l["result_hash"] == digest(&source["data"]["result"])?,
                        "Sent note merge source version changed",
                    )?;
                    require(
                        (n == 0 && source["kind"] == "shortnote")
                            || (n > 0
                                && source["kind"] == "reply"
                                && source["state"] == "verified"
                                && truth(&source["data"]["sent_receipt"])),
                        "Sent note merge requires a short-note base and verified replies",
                    )?;
                }
                return Ok(());
            }
            "independent-submission" => require(
                request["kind"] == "submission"
                    && reviewed
                    && p["data"]["review"]["verdict"] == "PASS",
                "Independent classification needs the exact Claude PASS",
            )?,
            "submission-round" => {
                require(
                    request["kind"] == "submission"
                        && p["kind"] == "submission"
                        && ((p["state"] == "round-complete"
                            && p["data"]["auto_advanced"] == true
                            && p["data"]["result"]["outcome"] == "completed")
                            || (reviewed && p["data"]["review"]["verdict"] == "PASS")),
                    "Submission round needs an auto-completed parent or the exact Claude PASS",
                )?;
                let rounds = crate::builders::submission_rounds(env);
                let round = p["request"]["inputs"]
                    .get("submission_round")
                    .and_then(Value::as_str)
                    .unwrap_or("improve");
                require(
                    rounds.iter().any(|r| r == round),
                    "Unknown submission round",
                )?;
                let next = rounds
                    .iter()
                    .position(|r| r == round)
                    .and_then(|n| rounds.get(n + 1));
                require(
                    next.is_some_and(|r| {
                        request["inputs"]
                            .get("submission_round")
                            .cloned()
                            .unwrap_or(string("improve"))
                            == *r
                    }),
                    "Submission continuation must execute exactly the next round",
                )?;
            }
            "shortnote-uplift" | "shortnote-closure" | "shortnote-value-uplift" => {
                require(
                    request["kind"] == "shortnote"
                        && p["kind"] == "shortnote"
                        && p["state"] == "round-complete",
                    "Shortnote uplift needs the completed previous round",
                )?;
                let r = &p["data"]["result"];
                let purpose = if r["outcome"] != "completed" {
                    ""
                } else if p["request"]["inputs"]["note_round"] == "draft" {
                    "shortnote-uplift"
                } else if r["value_assessment"]["verdict"] == "trivial" {
                    "shortnote-value-uplift"
                } else if r.get("formalization_gaps").is_none()
                    || r.get("value_assessment").is_none()
                {
                    "shortnote-closure"
                } else {
                    ""
                };
                require(
                    link["purpose"] == purpose,
                    "Shortnote uplift parent does not match the continuation",
                )?;
                require(
                    request["inputs"]["note_round"] == "uplift",
                    "Shortnote continuation must execute the uplift round",
                )?;
                let count = p["request"]["inputs"]["note_value_uplifts"]
                    .as_i64()
                    .unwrap_or(0)
                    + i64::from(purpose == "shortnote-value-uplift");
                require(
                    request["inputs"]
                        .get("note_value_uplifts")
                        .unwrap_or(&0.into())
                        == count,
                    "Shortnote continuation must preserve the value uplift count",
                )?;
                require(
                    python_eq(&request["inputs"]["previous_result"], &p["data"]["result"]),
                    "Shortnote uplift must carry the exact previous result",
                )?;
            }
            _ => require(
                ["revision", "user-answer", "guard-continuation"].contains(&s(&link["purpose"]))
                    && p["data"]["continuation_id"] == request["id"]
                    && p["state"] == "revising",
                "Revision is not the registered continuation",
            )?,
        }
        request = &p["request"];
    }
    Ok(())
}
pub fn owner(request: &Value, env: &Environment<'_>) -> Result<()> {
    if env.tables["decisions"].is_null() {
        return Ok(());
    }
    let mut owners = array(&request["inputs"]["owner_ids"]).to_vec();
    owners.push(request["owner"].clone());
    for p in crate::registry::people(&env.tables)? {
        if owners.contains(&p["id"])
            || array(&p["records"])
                .iter()
                .any(|r| owners.contains(&r["id"]))
        {
            require(
                !controlled(&p, env),
                "Owner-controlled line: register incoming facts only; preserve user work",
            )?;
        }
    }
    Ok(())
}
fn controlled(person: &Value, env: &Environment<'_>) -> bool {
    array(&env.tables["decisions"]["owner_controlled_threads"])
        .iter()
        .any(|v| casefold(s(v)) == casefold(s(&person["name"])))
        || array(&person["records"])
            .iter()
            .any(|r| truth(&r["owner_controlled"]) || truth(&r["hub"]["owner_controlled"]))
}
pub fn thread(request: &Value, resolved: Option<&Value>, env: &Environment<'_>) -> Result<()> {
    let snapshot = resolved.unwrap_or(&request["inputs"]);
    let mut threads = vec![&snapshot["thread"]];
    let contexts = array(&snapshot["prior_context"]);
    for c in contexts {
        if c.get("thread").is_some() {
            threads.push(&c["thread"]);
        }
    }
    let mut participants = BTreeSet::new();
    for m in threads
        .into_iter()
        .flat_map(array)
        .chain(contexts.iter().filter(|c| c.get("thread").is_none()))
    {
        for v in std::iter::once(&m["from"])
            .chain(array(&m["toRecipients"]))
            .chain(array(&m["ccRecipients"]))
        {
            let a = crate::registry::packet_address(v)?;
            if !a.is_empty() {
                participants.insert(a);
            }
        }
    }
    let persons = if !participants.is_empty() || !request["inputs"]["thread_record"].is_null() {
        crate::registry::people(&env.tables)?
    } else {
        Vec::new()
    };
    for a in participants {
        require(
            !crate::registry::preflight(&a, "", &env.tables, &persons)?
                .contains(&"table-3-refusal-or-stop-boundary".into()),
            format!("Current conversation has an active table-3 refusal: {a}"),
        )?;
        require(
            !persons
                .iter()
                .any(|p| array(&p["emails"]).iter().any(|e| e == &a) && controlled(p, env)),
            "Owner-controlled participant in thread: register incoming facts only; preserve user work",
        )?;
    }
    let record = &request["inputs"]["thread_record"];
    if !record.is_null() {
        let current = array(&env.tables["threads"]["lines"])
            .iter()
            .find(|r| r["id"] == record["id"])
            .ok_or_else(|| {
                Error::new(format!("Unknown historical thread: {}", s(&record["id"])))
            })?;
        for r in [record, current] {
            require(
                !["closed", "declined", "stopped", "cancelled"]
                    .contains(&s(&r["stage"]).to_lowercase().as_str()),
                "Historical thread is closed or declined",
            )?;
            for p in std::iter::once(&r["correspondent"]).chain(array(&r["co_correspondents"])) {
                if truth(&p["email"]) {
                    require(
                        !crate::registry::preflight(
                            s(&p["email"]),
                            s(&p["name"]),
                            &env.tables,
                            &persons,
                        )?
                        .contains(&"table-3-refusal-or-stop-boundary".into()),
                        format!(
                            "Historical thread blocked by active table-3 refusal: {}",
                            s(&p["email"])
                        ),
                    )?;
                }
            }
            for a in array(&r["mail_match"]["emails"]) {
                require(
                    !crate::registry::preflight(s(a), "", &env.tables, &persons)?
                        .contains(&"table-3-refusal-or-stop-boundary".into()),
                    format!(
                        "Historical thread blocked by active table-3 refusal: {}",
                        s(a)
                    ),
                )?;
            }
        }
    }
    owner(request, env)
}
pub fn paper(request: &Value, env: &Environment<'_>) -> Result<()> {
    let link = &request["inputs"]["submission_source"];
    if link.is_null() {
        return Ok(());
    }
    require(
        request["kind"] == "submission" && keys(link, &["paper_id", "identity"], &[]),
        "Invalid submission board source",
    )?;
    let row = array(&env.tables["submissions"]["papers"])
        .iter()
        .find(|r| r["id"] == link["paper_id"]);
    require(
        row.is_some_and(|r| r["stage"] != "submitted"),
        "Paper removed or already submitted",
    )?;
    let row = row.ok_or_else(|| Error::new("Missing paper"))?;
    let summary = crate::builders::paper_summary(row);
    require(
        link["identity"] == digest(&summary)?
            || link["identity"]
                == digest(&object([
                    ("id", row["id"].clone()),
                    ("authors", row["authors"].clone()),
                    ("workspace", summary["workspace"].clone()),
                ]))?,
        "Submission board changed; select the current package",
    )
}
fn field_ids(values: &[&Value]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for v in values {
        for k in ["result_id", "problem_id", "id", "result_ids", "problem_ids"] {
            let v = &v[k];
            if let Some(s) = v.as_str() {
                if !s.is_empty() {
                    out.insert(s.into());
                }
            } else {
                out.extend(
                    array(v)
                        .iter()
                        .filter_map(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .map(String::from),
                );
            }
        }
    }
    out
}
fn problem_ids(row: &Value) -> BTreeSet<String> {
    let i = &row["request"]["inputs"];
    let d = &row["data"];
    let delivery = &d["mathesis_delivery"];
    let m = &fallback(&[&delivery["collection_envelope"], &delivery["envelope"]])["metadata"];
    let mut ids = field_ids(&[
        i,
        &i["pipeline"],
        &i["result"],
        &d["publication_loop"],
        &m["origin"],
        &m["review"]["record"],
    ]);
    if let Some(id) = s(&row["owner"]).strip_prefix("result-") {
        ids.insert(id.into());
    }
    ids
}
fn note_files(row: &Value) -> Vec<Value> {
    array(&row["data"]["result"]["artifacts"])
        .iter()
        .filter(|a| ["note-pdf", "note-tex", "source-archive"].contains(&s(&a["role"])))
        .cloned()
        .collect()
}
fn source_rows<'a>(row: &Value, rows: &'a [Value]) -> Result<Vec<&'a Value>> {
    let i = &row["request"]["inputs"];
    let mut links = array(&i["note_sources"]).to_vec();
    links.extend_from_slice(array(&row["data"]["result"]["note_sources"]));
    if truth(&i["pipeline"]["note_task_id"]) {
        links.push(object([
            ("task_id", i["pipeline"]["note_task_id"].clone()),
            ("result_hash", i["pipeline"]["note_result_hash"].clone()),
        ]));
    }
    if truth(&i["automation_parent"]["result_hash"]) {
        links.push(i["automation_parent"].clone());
    }
    let mut out = Vec::new();
    for l in links {
        if let Some(p) = rows
            .iter()
            .find(|r| r["id"] == l["task_id"] && r["kind"] == "shortnote")
            && l["result_hash"] == digest(&p["data"]["result"])?
        {
            out.push(p);
        }
    }
    Ok(out)
}
fn attachment_role(name: &str) -> Option<&'static str> {
    let name = name.to_lowercase();
    let note = regex(r"(?i)^note(?:[._ -]|[0-9])").is_ok_and(|r| r.is_match(&name));
    if note {
        if name.ends_with(".pdf") {
            Some("note-pdf")
        } else if name.ends_with(".tex") {
            Some("note-tex")
        } else if name.ends_with(".zip") {
            Some("source-archive")
        } else {
            None
        }
    } else if regex(r"(?i)^source-v[1-9][0-9]*\.zip$").is_ok_and(|r| r.is_match(&name)) {
        Some("source-archive")
    } else {
        None
    }
}
fn base64(raw: &str) -> Vec<u8> {
    let mut bits = 0u32;
    let mut count = 0;
    let mut out = Vec::new();
    for c in raw.bytes() {
        if c == b'=' {
            break;
        }
        if let Some(v) = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
            .iter()
            .position(|b| *b == c)
        {
            bits = (bits << 6) | (v as u32);
            count += 6;
            if count >= 8 {
                count -= 8;
                out.push((bits >> count) as u8);
            }
        }
    }
    out
}
pub fn stamp(v: &Value) -> Option<f64> {
    let micros = instant_micros(v)?;
    let sign = if micros < 0 { "-" } else { "" };
    let n = micros.unsigned_abs();
    format!("{sign}{}.{:06}", n / 1_000_000, n % 1_000_000)
        .parse()
        .ok()
}
/// Python mailbox.instant compares datetimes at microsecond precision. Keep
/// integer UTC coordinates for reply supersession; timestamp() is a separate
/// float projection used by the sent-note index.
pub fn instant_micros(v: &Value) -> Option<i64> {
    let raw = v.as_str()?.replace('Z', "+00:00");
    let re =
        regex(r"^(\d{4}-\d{2}-\d{2}|\d{8}|\d{4}-W\d{2}(?:-\d)?|\d{4}W\d{2}\d?)(?s:.)(.+)$").ok()?;
    let c = re.captures(&raw)?;
    let day = iso_day(&c[1])?;
    let time = &c[2];
    let index = time.find(['+', '-'])?;
    let (clock, zone) = time.split_at(index);
    let (h, m, sec, micro) = iso_time(clock)?;
    if h > 23 || m > 59 || sec > 59 {
        return None;
    }
    let (zh, zm, zs, zf) = iso_time(&zone[1..])?;
    let mut offset = (zh * 3600 + zm * 60 + zs) * 1_000_000;
    // CPython treats an all-zero whole offset as UTC, even with a fraction.
    if offset != 0 {
        offset += zf;
    }
    if offset >= 86_400_000_000 {
        return None;
    }
    if zone.starts_with('-') {
        offset = -offset;
    }
    Some(((day - 719_163) * 86400 + h * 3600 + m * 60 + sec) * 1_000_000 + micro - offset)
}
fn iso_time(raw: &str) -> Option<(i64, i64, i64, i64)> {
    let (whole, fraction) = raw
        .split_once(['.', ','])
        .map_or((raw, None), |(w, f)| (w, Some(f)));
    let mut micros = 0;
    if let Some(f) = fraction {
        if f.is_empty() || !f.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let n = f.len().min(6);
        micros = f[..n].parse::<i64>().ok()? * 10_i64.pow((6 - n) as u32);
    }
    let parts: Vec<_> = if whole.contains(':') {
        whole.split(':').collect()
    } else {
        if ![2, 4, 6].contains(&whole.len()) || !whole.is_ascii() {
            return None;
        }
        (0..whole.len())
            .step_by(2)
            .map(|i| &whole[i..i + 2])
            .collect()
    };
    if parts.is_empty()
        || parts.len() > 3
        || parts
            .iter()
            .any(|p| p.len() != 2 || !p.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    Some((
        parts[0].parse().ok()?,
        parts.get(1).map_or(Some(0), |p| p.parse().ok())?,
        parts.get(2).map_or(Some(0), |p| p.parse().ok())?,
        micros,
    ))
}
pub fn note_revisions(row: &Value, rows: &[Value], tables: &Value, metadata: &Value) -> Result<()> {
    if !["mathesis", "shortnote"].contains(&s(&row["kind"])) {
        return Ok(());
    }
    let mut ids = BTreeMap::<String, BTreeSet<String>>::new();
    let mut shaids = BTreeMap::<String, BTreeSet<String>>::new();
    let mut files = BTreeMap::<String, Vec<Value>>::new();
    let mut heads = BTreeMap::<String, &Value>::new();
    for r in rows
        .iter()
        .filter(|r| ["shortnote", "mathesis"].contains(&s(&r["kind"])))
    {
        ids.insert(s(&r["id"]).into(), problem_ids(r));
    }
    for r in rows.iter().filter(|r| r["kind"] == "shortnote") {
        let name = s(&r["id"]);
        let coverage = array(&tables["pipeline_state"]["rows"])
            .iter()
            .find(|p| p["key"] == format!("note-coverage:{name}"));
        if let Some(c) = coverage
            && c["payload"]["result_hash"] == digest(&r["data"]["result"])?
        {
            ids.entry(name.into()).or_default().extend(
                array(&c["payload"]["problem_ids"])
                    .iter()
                    .map(|v| s(v).into()),
            );
        }
        let f = note_files(r);
        for a in &f {
            shaids
                .entry(s(&a["sha256"]).into())
                .or_default()
                .extend(ids.get(name).into_iter().flatten().cloned());
        }
        if ["handoff-ready", "verified", "round-complete"].contains(&s(&r["state"]))
            && r["data"]["result"]["outcome"] == "completed"
            && !f.is_empty()
        {
            for id in ids.get(name).into_iter().flatten() {
                heads.insert(id.clone(), r);
            }
        }
        files.insert(name.into(), f);
    }
    let mut sent = BTreeMap::<String, Vec<(f64, String, BTreeSet<String>)>>::new();
    let mut replies = BTreeMap::new();
    for r in rows
        .iter()
        .filter(|r| r["kind"] == "reply" && r["state"] == "verified")
    {
        let d = &r["data"];
        let receipt = &d["sent_receipt"];
        let packet = fallback(&[&d["packet_override"], &d["result"]["packet"]]);
        if !truth(&receipt["remote_id"])
            || !truth(&receipt["evidence_ref"])
            || !truth(&packet["attachments"])
            || receipt["content_hash"] != digest(packet)?
        {
            continue;
        }
        let Some(at) = stamp(&receipt["at"]) else {
            continue;
        };
        let mut result_ids = problem_ids(r);
        result_ids.extend(
            array(&packet["problem_ids"])
                .iter()
                .map(|v| s(v).to_owned()),
        );
        let mut selected = array(&r["request"]["inputs"]["selected_inputs"]).to_vec();
        selected.extend_from_slice(array(&d["thread_inputs"]["selected_inputs"]));
        let mut known: BTreeMap<String, Value> = selected
            .iter()
            .filter(|a| truth(&a["sha256"]) && truth(&a["release_tag"]))
            .map(|a| (s(&a["sha256"]).into(), a.clone()))
            .collect();
        for a in note_files(r) {
            known.insert(s(&a["sha256"]).into(), a);
        }
        let mut source_ids = BTreeMap::<String, BTreeSet<String>>::new();
        for p in source_rows(r, rows)? {
            for a in files.get(s(&p["id"])).into_iter().flatten() {
                known.entry(s(&a["sha256"]).into()).or_insert(a.clone());
                source_ids
                    .entry(s(&a["sha256"]).into())
                    .or_default()
                    .extend(ids.get(s(&p["id"])).into_iter().flatten().cloned());
            }
        }
        for a in &selected {
            if ["note-pdf", "note-tex", "source-archive"].contains(&s(&a["role"]))
                || attachment_role(s(&a["name"])).is_some()
            {
                result_ids.extend(
                    ids.get(s(&a["release_tag"]).trim_start_matches("outreach-task-"))
                        .into_iter()
                        .flatten()
                        .cloned(),
                );
                result_ids.extend(shaids.get(s(&a["sha256"])).into_iter().flatten().cloned());
            }
        }
        let mut attached = BTreeMap::new();
        for a in array(&packet["attachments"]) {
            let raw = base64(s(&a["content_b64"]));
            let hash = outreach_canon::sha256(&raw);
            let reference = known.get(&hash).cloned().unwrap_or(object([]));
            let role = if ["note-pdf", "note-tex", "source-archive"]
                .contains(&s(&reference["role"]))
            {
                Some(s(&reference["role"]))
            } else {
                attachment_role(s(&a["name"])).or_else(|| attachment_role(s(&reference["name"])))
            };
            let Some(role) = role else { continue };
            if known.contains_key(&hash) {
                source_ids
                    .entry(hash.clone())
                    .or_default()
                    .extend(shaids.get(&hash).into_iter().flatten().cloned());
            }
            let mut v = reference.clone();
            v["role"] = string(role);
            v["sha256"] = string(&hash);
            v["bytes"] = raw.len().into();
            attached.insert(hash, v);
        }
        if attached.is_empty() {
            continue;
        }
        replies.insert(s(&r["id"]).to_owned(), r);
        for sha in attached.keys() {
            result_ids.extend(source_ids.get(sha).into_iter().flatten().cloned());
        }
        for id in result_ids {
            let hashes = attached
                .keys()
                .filter(|sha| {
                    source_ids
                        .get(*sha)
                        .is_none_or(|ids| ids.is_empty() || ids.contains(&id))
                })
                .cloned()
                .collect::<BTreeSet<_>>();
            if !hashes.is_empty() {
                sent.entry(id)
                    .or_default()
                    .push((at, s(&r["id"]).into(), hashes));
            }
        }
        files.insert(s(&r["id"]).into(), attached.into_values().collect());
    }
    for entries in sent.values_mut() {
        entries.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    }
    let incorporated = |row: &Value| -> Result<BTreeSet<String>> {
        let mut hashes = BTreeSet::new();
        let mut seen = BTreeSet::new();
        let mut pending = vec![row];
        while let Some(r) = pending.pop() {
            if !seen.insert(s(&r["id"]).to_owned()) {
                continue;
            }
            hashes.extend(note_files(r).iter().map(|a| s(&a["sha256"]).to_owned()));
            if r["kind"] == "shortnote" {
                hashes.extend(
                    array(&r["request"]["inputs"]["selected_inputs"])
                        .iter()
                        .filter(|a| truth(&a["sha256"]) && truth(&a["release_tag"]))
                        .map(|a| s(&a["sha256"]).to_owned()),
                );
                for link in array(&r["request"]["inputs"]["note_sources"]) {
                    if let Some(reply) = replies.get(s(&link["task_id"]))
                        && keys(link, &["task_id", "result_hash"], &[])
                        && link["result_hash"] == digest(&reply["data"]["result"])?
                    {
                        hashes.extend(
                            files
                                .get(s(&reply["id"]))
                                .into_iter()
                                .flatten()
                                .map(|a| s(&a["sha256"]).into()),
                        );
                    }
                }
            }
            pending.extend(source_rows(r, rows)?);
        }
        Ok(hashes)
    };
    let missing = |id: &str, r: &Value| -> Result<Vec<String>> {
        let mut hashes = incorporated(r)?;
        let mut out = Vec::new();
        for (_, task, attachments) in sent.get(id).into_iter().flatten() {
            if !hashes.is_superset(attachments) {
                out.push(task.clone());
                hashes.extend(attachments.iter().cloned());
            }
        }
        Ok(out)
    };
    let mut result_ids = problem_ids(row);
    result_ids.extend(field_ids(&[
        &metadata["origin"],
        &metadata["review"]["record"],
    ]));
    for p in source_rows(row, rows)? {
        result_ids.extend(ids.get(s(&p["id"])).into_iter().flatten().cloned());
    }
    for id in result_ids {
        if !sent.contains_key(&id) {
            continue;
        }
        if let Some(head) = heads.get(&id) {
            require(
                missing(&id, head)?.is_empty()
                    && (!truth(&head["request"]["inputs"]["sent_revision_result_id"])
                        || ["handoff-ready", "verified"].contains(&s(&head["state"]))),
                format!("Sent note revision for {id} requires a handoff-ready merged short note"),
            )?;
        }
        if let Some(task) = missing(&id, row)?.first() {
            return Err(Error::new(format!(
                "Stale note for {id} omits the verified sent revision from {task}"
            )));
        }
    }
    Ok(())
}
