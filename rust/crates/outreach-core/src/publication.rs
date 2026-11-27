//! Standing-decision publication. Scientific package validation is supplied by a
//! short-note service; this core owns eligibility, retries, duplicate selection,
//! persisted write barriers and signed Mathesis ingestion.
use crate::mathesis_delivery::{DeliveryPorts, Persisted, SourceFile, StageError, StageResult};
use crate::{array, casefold, fallback, gates, ingest::Replay, object, s, string, truth};
use outreach_canon::{SerializeOptions, Value, digest, dumps, parse, sha256};
use std::collections::{BTreeMap, BTreeSet};

pub struct Package {
    pub metadata: Value,
    pub files: BTreeMap<String, SourceFile>,
    pub source: String,
    pub has_lean: bool,
}
pub struct Registration {
    pub metadata: Value,
}
pub struct ReleaseIntent {
    pub tag: String,
    pub title: String,
}
pub struct UploadIntent {
    pub tag: String,
    pub file: SourceFile,
}
pub trait GitHubReleases {
    fn register_note(&mut self, intent: Persisted<Registration>) -> StageResult<Value>;
    fn release(&mut self, tag: &str) -> StageResult<Value>;
    fn create(&mut self, intent: Persisted<ReleaseIntent>) -> StageResult<()>;
    fn upload(&mut self, intent: Persisted<UploadIntent>) -> StageResult<()>;
    fn download(&mut self, tag: &str, name: &str) -> StageResult<SourceFile>;
}
pub trait ShortNoteService {
    /// Validate exact reviewed identities, bytes, metadata, PDF and ZIP safety;
    /// prepare the canonical immutable package without repacking its source.
    fn package(&mut self, row: &Value) -> StageResult<Package>;
    /// Replay only: inject a recorded failure at a pure operation boundary.
    /// Production implementations return Ok; outputs never drive core decisions.
    fn boundary(&mut self, method: &str, args: Vec<Value>) -> StageResult<()>;
}
#[derive(Default)]
pub struct Options {
    pub previous: Vec<String>,
}
fn check(ok: bool, message: &str) -> StageResult<()> {
    Ok(crate::validation::require(ok, message)?)
}
fn value_error(message: &str) -> StageError {
    StageError {
        summary: object([("type", string("ValueError")), ("reason", string(message))]),
    }
}
fn encoded(value: &Value) -> StageResult<Vec<u8>> {
    Ok((dumps(
        value,
        SerializeOptions {
            indent: Some("  "),
            sort_keys: true,
            ensure_ascii: false,
            ..Default::default()
        },
    )? + "\n")
        .into_bytes())
}
fn lineage(row: &Value, rows: &[Value]) -> StageResult<Vec<Value>> {
    let mut result = vec![];
    let mut current = row;
    let mut seen = BTreeSet::new();
    loop {
        if !seen.insert(s(&current["id"])) {
            return Err(value_error("Publication source cycle"));
        }
        result.push(current.clone());
        let inputs = &current["request"]["inputs"];
        let mut parent = fallback(&[
            &inputs["pipeline"]["note_task_id"],
            &inputs["automation_parent"]["task_id"],
        ]);
        if !truth(parent)
            && current["kind"] == "mathesis"
            && array(&inputs["note_sources"]).len() == 1
        {
            parent = &inputs["note_sources"][0]["task_id"];
        }
        if !truth(parent) {
            return Ok(result);
        }
        current = rows
            .iter()
            .find(|r| r["id"] == *parent)
            .ok_or_else(|| value_error("Unknown task"))?;
    }
}
fn identities(row: &Value, note: &Value) -> BTreeSet<String> {
    let i = &row["request"]["inputs"];
    let d = &row["data"]["publication_loop"];
    [
        &row["id"],
        &i["result_id"],
        &i["pipeline"]["result_id"],
        &i["result"]["id"],
        &d["note_id"],
        &note["id"],
    ]
    .into_iter()
    .chain(array(&d["problem_ids"]))
    .chain(array(&note["problem_ids"]))
    .filter(|v| !v.is_null())
    .map(|v| s(v).to_owned())
    .collect()
}
fn name(raw: &str) -> String {
    let value = raw.split_once(',').map_or_else(
        || raw.to_owned(),
        |(family, given)| format!("{given} {family}"),
    );
    casefold(&value.split_whitespace().collect::<Vec<_>>().join(" "))
}
pub(crate) fn issue_exclusion(
    row: &Value,
    rows: &[Value],
    table: &Value,
    decision: &Value,
    rid: &Value,
) -> StageResult<Option<&'static str>> {
    Ok(exclusion_inner(row, rows, table, decision, &Value::Null, Some(rid))?.0)
}
pub(crate) fn exclusion(
    row: &Value,
    rows: &[Value],
    table: &Value,
    decision: &Value,
    note: &Value,
) -> StageResult<(Option<&'static str>, Value)> {
    exclusion_inner(row, rows, table, decision, note, None)
}
fn exclusion_inner(
    row: &Value,
    rows: &[Value],
    table: &Value,
    decision: &Value,
    note: &Value,
    result_id: Option<&Value>,
) -> StageResult<(Option<&'static str>, Value)> {
    let parents = lineage(row, rows)?;
    let mut selected = identities(row, note);
    for p in &parents {
        selected.extend(identities(p, &Value::Null));
    }
    if let Some(rid) = result_id.filter(|v| truth(v)) {
        selected.insert(s(rid).to_owned());
    }
    let related: Vec<_> = rows
        .iter()
        .filter(|r| {
            ["shortnote", "mathesis"].contains(&s(&r["kind"]))
                && !identities(r, &Value::Null).is_disjoint(&selected)
        })
        .cloned()
        .collect();
    if result_id.is_some_and(truth) {
        selected.extend(related.iter().map(|r| s(&r["id"]).to_owned()));
    }
    let mut reason = None;
    for hold in array(&table["holds"]) {
        let ids: BTreeSet<_> = ["legacy_id", "task_id", "note_id", "id"]
            .into_iter()
            .map(|k| &hold[k])
            .chain(array(&hold["problem_ids"]))
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
        if !selected.is_disjoint(&ids)
            && ["HOLD", "JOINT-WORK"].contains(&s(&hold["decision"]).to_uppercase().as_str())
        {
            reason = Some("held in tables/decisions.json");
            break;
        }
    }
    if reason.is_none() {
        for parent in parents.iter().chain(&related) {
            if parent["data"]["review"]["verdict"] == "HOLD" {
                reason = Some("task or source note is held");
                break;
            }
            if [
                &parent["request"]["inputs"],
                &parent["data"],
                &parent["data"]["result"],
            ]
            .iter()
            .any(|o| {
                o["joint_work"] == true || ["joint", "joint-work"].contains(&s(&o["authorship"]))
            }) {
                reason = Some("joint work needs its own publication decision");
                break;
            }
        }
    }
    if reason.is_none() && !note.is_null() {
        let allowed: BTreeSet<_> = array(&decision["independent_authors"])
            .iter()
            .map(|a| name(s(a)))
            .collect();
        if array(&note["creators"])
            .iter()
            .any(|a| !allowed.contains(&name(s(&a["name"]))))
        {
            reason = Some("joint work needs its own publication decision");
        }
    }
    fn facts(r: &Value) -> Value {
        Value::Array(vec![
            r["id"].clone(),
            r["data"]["review"]["verdict"].clone(),
            Value::Array(
                [&r["request"]["inputs"], &r["data"], &r["data"]["result"]]
                    .iter()
                    .map(|o| Value::Array(vec![o["joint_work"].clone(), o["authorship"].clone()]))
                    .collect(),
            ),
        ])
    }
    let note_ids = if note.is_null() {
        Value::Null
    } else {
        object([
            ("id", note["id"].clone()),
            ("problem_ids", note["problem_ids"].clone()),
        ])
    };
    let mut canonical_ids = selected
        .iter()
        .map(|id| outreach_canon::canonical(&string(id)))
        .collect::<outreach_canon::Result<Vec<_>>>()?;
    canonical_ids.sort();
    let hash = digest(&object([
        (
            "selected",
            Value::Array(canonical_ids.into_iter().map(string).collect()),
        ),
        ("parents", Value::Array(parents.iter().map(facts).collect())),
        ("related", Value::Array(related.iter().map(facts).collect())),
        (
            "holds",
            table.get("holds").cloned().unwrap_or(Value::Array(vec![])),
        ),
        ("decision", decision.clone()),
        ("note", note_ids.clone()),
    ]))?;
    let mut inputs = object([("exclusion_inputs", string(hash))]);
    if !note.is_null() {
        inputs["exclusion_note"] = note_ids;
    }
    Ok((reason, inputs))
}
fn reviewed(row: &Value) -> bool {
    let result = &row["data"]["result"];
    let review = &row["data"]["review"];
    [
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
        && (row["kind"] == "mathesis" || crate::transitions::eligible(row))
        && result["outcome"] == "completed"
        && review["verdict"] == "PASS"
        && ["local-claude-loop", "hosted-cma-review"].contains(&s(&review["reviewer"]))
        && digest(result).is_ok_and(|d| review["binding"]["result_hash"] == d)
        && review["binding"]["workflow_hash"] == row["request"]["workflow_hash"]
}
fn record(
    replay: &mut Replay<'_>,
    port: &mut impl DeliveryPorts,
    service: &mut impl ShortNoteService,
    id: &Value,
    state: Value,
    force: bool,
) -> StageResult<()> {
    service.boundary("record", vec![id.clone(), state["phase"].clone()])?;
    let row = replay.task(id)?;
    let mut data = row["data"].clone();
    data["publication_loop"] = state;
    replay.update(&row, s(&row["state"]), data)?;
    port.save_with_force(&replay.tables, force)?;
    Ok(())
}
fn stage(
    package: &Package,
    gh: &mut impl GitHubReleases,
    port: &mut impl DeliveryPorts,
    tables: &Value,
) -> StageResult<()> {
    let tag = format!("short-note-{}", s(&package.metadata["id"]));
    let mut release = gh.release(&tag)?;
    if release.is_null() {
        gh.create(Persisted::saved(
            ReleaseIntent {
                tag: tag.clone(),
                title: s(&package.metadata["title"]).into(),
            },
            port,
            tables,
        )?)?;
        release = gh.release(&tag)?;
    }
    let revision = &package.metadata["revision"];
    let manifest = format!("note-v{}.json", crate::py_str(revision));
    for filename in [
        format!("note-v{}.pdf", crate::py_str(revision)),
        format!("source-v{}.zip", crate::py_str(revision)),
        manifest,
    ] {
        let file = package
            .files
            .get(&filename)
            .ok_or_else(|| value_error("Missing prepared file"))?;
        if array(&release["assets"])
            .iter()
            .any(|a| a["name"] == filename)
        {
            if gh.download(&tag, &filename)?.bytes != file.bytes {
                return Err(value_error(
                    "Staged versions are immutable; increment revision",
                ));
            }
        } else {
            gh.upload(Persisted::saved(
                UploadIntent {
                    tag: tag.clone(),
                    file: SourceFile {
                        name: filename,
                        bytes: file.bytes.clone(),
                    },
                },
                port,
                tables,
            )?)?;
        }
    }
    Ok(())
}
fn enqueue(
    package: &Package,
    catalog: &Value,
    gh: &mut impl GitHubReleases,
    port: &mut impl DeliveryPorts,
    tables: &Value,
) -> StageResult<String> {
    let metadata = &package.metadata;
    let tag = format!("short-note-{}", s(&metadata["id"]));
    let note = array(&catalog["notes"])
        .iter()
        .find(|n| n["id"] == metadata["id"])
        .ok_or_else(|| value_error("Unknown note"))?;
    if truth(&note["paused"]) {
        return Err(value_error("This note is paused"));
    }
    let request = object([
        ("schema_version", string("short-note-request.v1")),
        ("id", metadata["id"].clone()),
        ("action", string("publish")),
        ("revision", metadata["revision"].clone()),
    ]);
    let bytes = encoded(&request)?;
    let filename = format!("request-{}.json", &sha256(&bytes)[..24]);
    let release = gh.release(&tag)?;
    if release.is_null() {
        return Err(value_error("Stage the note before enqueuing delivery"));
    }
    let revision = crate::py_str(&metadata["revision"]);
    check(
        [
            format!("note-v{revision}.json"),
            format!("note-v{revision}.pdf"),
            format!("source-v{revision}.zip"),
        ]
        .iter()
        .all(|name| array(&release["assets"]).iter().any(|a| a["name"] == *name)),
        "The selected note revision has not finished staging",
    )?;
    let manifest = gh.download(&tag, &format!("note-v{revision}.json"))?;
    let saved = parse(&String::from_utf8_lossy(&manifest.bytes))?;
    check(
        saved["metadata"]["revision"] == metadata["revision"],
        "Staged manifest revision differs from the request",
    )?;
    if array(&release["assets"])
        .iter()
        .any(|a| a["name"] == filename)
    {
        let existing = gh.download(&tag, &filename)?;
        check(
            parse(&String::from_utf8_lossy(&existing.bytes))? == request,
            "Immutable request content differs",
        )?;
    } else {
        gh.upload(Persisted::saved(
            UploadIntent {
                tag: tag.clone(),
                file: SourceFile {
                    name: filename.clone(),
                    bytes,
                },
            },
            port,
            tables,
        )?)?;
    }
    Ok(filename)
}
fn merge(target: &mut Value, input: &Value) {
    if let (Some(a), Some(b)) = (target.as_object_mut(), input.as_object()) {
        for (k, v) in b {
            a.insert(k.clone(), v.clone());
        }
    }
}
fn source_gate(replay: &Replay<'_>, row: &Value) -> StageResult<()> {
    gates::pipeline(&row["request"], replay.rows("tasks"), replay.environment)?;
    crate::transitions::source_gate(
        &row["request"],
        &row["data"]["result"],
        replay.rows("tasks"),
    )?;
    gates::parent(&row["request"], replay.rows("tasks"), replay.environment)?;
    if row["kind"] != "shortnote" {
        for parent in lineage(row, replay.rows("tasks"))?.iter().skip(1) {
            if parent["kind"] == "shortnote" && parent["request"]["inputs"]["note_round"] != "draft"
            {
                check(
                    reviewed(parent),
                    "The source note no longer has an exact PASS",
                )?;
                break;
            }
        }
    }
    Ok(())
}
fn key(note: &Value) -> (String, String) {
    (s(&note["note_id"]).into(), crate::py_str(&note["revision"]))
}
fn selected_key(note: &Value) -> (String, String) {
    (s(&note["id"]).into(), crate::py_str(&note["revision"]))
}
struct SelectionServices<'a, G, P, S> {
    gh: &'a mut G,
    port: &'a mut P,
    service: &'a mut S,
}
fn selection(
    replay: &mut Replay<'_>,
    row: &Value,
    package: &Package,
    decision: &Value,
    services: SelectionServices<'_, impl GitHubReleases, impl DeliveryPorts, impl ShortNoteService>,
    secret: &[u8],
    step: &mut &'static str,
) -> StageResult<()> {
    let SelectionServices { gh, port, service } = services;
    *step = "prior-work-cache";
    let prior = crate::mathesis_delivery::upgrade::evidence(replay, port, row)?;
    let current = replay.task(&row["id"])?;
    let mut data = current["data"].clone();
    data["mathesis_prior_work"] = object([
        ("result_hash", string(digest(&row["data"]["result"])?)),
        ("check", prior.clone()),
        (
            "phase",
            string(if prior.is_null() { "not-run" } else { "ready" }),
        ),
    ]);
    replay.update(&current, s(&current["state"]), data)?;
    port.save(&replay.tables)?;
    *step = "delivery-choices";
    let m = &package.metadata;
    let mut choices = decision.clone();
    choices["source_file"] = string(&package.source);
    let manifest = package
        .files
        .get(&format!("note-v{}.json", crate::py_str(&m["revision"])))
        .ok_or_else(|| value_error("Missing prepared manifest"))?;
    let package_id = sha256(&manifest.bytes);
    let selection_key = format!(
        "publication-selection:{}",
        digest(&Value::Array(vec![m["id"].clone(), m["revision"].clone()]))?
    );
    let previous = replay.state(&selection_key);
    let selected = {
        let mut selected = object([
            ("decision_ref", decision["decision_ref"].clone()),
            ("purpose", decision["purpose"].clone()),
            ("processing_consent", decision["processing_consent"].clone()),
            (
                "make_public_after_acceptance",
                decision["make_public_after_acceptance"].clone(),
            ),
            (
                "ai_disclosure",
                object([
                    ("level", decision["ai_disclosure"]["level"].clone()),
                    (
                        "statement",
                        decision["ai_disclosure"][if package.has_lean {
                            "statement_with_lean"
                        } else {
                            "statement_without_lean"
                        }]
                        .clone(),
                    ),
                ]),
            ),
            ("source_file", string(&package.source)),
        ]);
        if truth(&previous) {
            check(
                previous["package_id"] == package_id
                    && previous["decision_hash"] == digest(decision)?,
                "An existing publication selection is immutable; use a new note revision",
            )?;
            previous["decisions"].clone()
        } else {
            let release = gh.release(&format!("short-note-{}", s(&m["id"])))?;
            if truth(&release) {
                let state = if s(&release["body"]).trim()
                    == "Versioned short-note artifacts and publication receipts."
                {
                    Value::Null
                } else {
                    parse(s(&release["body"]))?
                };
                let entry = &state["versions"][crate::py_str(&m["revision"]).as_str()];
                if truth(&entry["publication"]) {
                    check(
                        entry["package_id"] == package_id,
                        "The published DOI belongs to another scientific package",
                    )?;
                    selected["doi"] = entry["doi"].clone();
                }
            }
            let mut max = None;
            for r in replay.rows("tasks") {
                let d = &r["data"]["mathesis_delivery"];
                let p = &d["envelope"]["metadata"];
                if p["producer"] == "trureturing-outreach"
                    && p["work_id"] == m["id"]
                    && p["paper"]["purpose"] == "publication"
                    && p["version"] != string(crate::py_str(&m["revision"]))
                {
                    check(
                        truth(&d["receipt"]),
                        "Await the earlier Mathesis delivery receipt",
                    )?;
                    max = max.max(d["receipt"]["version"].as_i64());
                }
            }
            if let Some(version) = max {
                selected["previous_version"] = version.into();
            }
            replay.set_state(
                &selection_key,
                object([
                    ("package_id", string(package_id)),
                    ("decision_hash", string(digest(decision)?)),
                    ("decisions", selected.clone()),
                ]),
            )?;
            port.save_forced(&replay.tables)?;
            selected
        }
    };
    merge(&mut choices, &selected);
    *step = "history-choices";
    let mut max = None;
    for r in replay.rows("tasks") {
        let d = &r["data"]["mathesis_delivery"];
        let p = &d.get("collection_envelope").unwrap_or(&d["envelope"])["metadata"];
        if p["schema"] == "mathesis.manuscript-delivery.v1"
            && p["work_id"] == m["id"]
            && p["version"] == string(crate::py_str(&m["revision"]))
            && d["phase"] == "delivered"
        {
            return Err(value_error(
                "This note revision already has a manuscript-deliveries receipt; do not resend",
            ));
        }
        if p["schema"] != "mathesis.pre-reviewed-delivery.v1" || p["work_id"] != m["id"] {
            continue;
        }
        check(
            d["phase"] == "delivered",
            "Await the earlier collected version readback",
        )?;
        check(
            d["receipt"]["version"].as_i64() < m["revision"].as_i64(),
            "Collected revision already selected; retain its exact request",
        )?;
        max = max.max(d["receipt"]["version"].as_i64());
    }
    choices["collection_version"] = m["revision"].clone();
    choices["collection_previous_version"] = max.map(Value::from).unwrap_or(Value::Null);
    *step = "select";
    service.boundary("select", vec![row["id"].clone()])?;
    let current = replay.task(&row["id"])?;
    let prior = if prior.is_null() {
        crate::mathesis_delivery::upgrade::evidence(replay, port, row)?
    } else {
        prior
    };
    let envelope = crate::builders::mathesis(
        &current,
        &object([
            ("kind", string("short_note")),
            ("private_correspondence_included", false.into()),
            ("source_note", m.clone()),
        ]),
        &choices,
        &prior,
        secret,
        replay.environment,
    )?;
    *step = "ingest";
    service.boundary("ingest", vec![row["id"].clone()])?;
    replay.mathesis(&envelope, secret)?;
    port.save_forced(&replay.tables)?;
    Ok(())
}
pub fn advance(
    replay: &mut Replay<'_>,
    gh: &mut impl GitHubReleases,
    service: &mut impl ShortNoteService,
    port: &mut impl DeliveryPorts,
    secret: &[u8],
    options: &Options,
) -> StageResult<()> {
    let config = &replay.environment.config;
    let table = &replay.environment.tables["decisions"];
    let decision = &table["short_note_publication"];
    if truth(&config["paused"])
        || !truth(&config["kinds"]["mathesis"])
        || truth(&config["mathesis"]["test_only"])
        || !truth(&decision["enabled"])
    {
        return Ok(());
    }
    check(
        decision.is_object() && decision["enabled"] == true,
        "No active standing publication decision",
    )?;
    check(
        truth(&decision["decision_ref"])
            && decision["purpose"] == "publication"
            && truth(&decision["processing_consent"])
            && decision["make_public_after_acceptance"] == true,
        "The standing decision must authorize publication and processing",
    )?;
    let policy = &replay.environment.tables["author_policy"];
    check(
        decision["ai_disclosure"]["level"] == "assisted"
            && ["with_lean", "without_lean"].iter().all(|variant| {
                decision["ai_disclosure"][format!("statement_{variant}").as_str()]
                    == policy[format!("locked_statement_{variant}").as_str()]
            }),
        "Use the current locked AI statements verbatim",
    )?;
    if config["mathesis"]["mode"] != "deliver" || secret.is_empty() {
        return Ok(());
    }
    let all = replay.rows("tasks").to_vec();
    let mut sources = BTreeSet::new();
    for r in &all {
        if r["kind"] == "mathesis"
            && !["failed", "superseded"].contains(&s(&r["state"]))
            && let Ok(parents) = lineage(r, &all)
        {
            for p in parents.iter().skip(1).filter(|p| p["kind"] == "shortnote") {
                sources.insert(s(&p["id"]).to_owned());
            }
        }
    }
    let mut queued: BTreeSet<_> = all
        .iter()
        .filter(|r| r["data"]["publication_loop"]["phase"] == "queued")
        .map(|r| key(&r["data"]["publication_loop"]))
        .collect();
    let mut taken: BTreeSet<_> = all
        .iter()
        .filter(|r| r["kind"] == "mathesis" && r["data"]["publication_loop"]["phase"] == "queued")
        .map(|r| key(&r["data"]["publication_loop"]))
        .collect();
    let mut rows: Vec<_> = all
        .iter()
        .filter(|r| {
            ["mathesis", "shortnote"].contains(&s(&r["kind"]))
                && reviewed(r)
                && !sources.contains(s(&r["id"]))
                && gates::note_revisions(r, &all, &replay.tables, &Value::Null).is_ok()
        })
        .cloned()
        .collect();
    let linked = if rows.is_empty() {
        BTreeSet::new()
    } else {
        crate::lanes::linked(&all, replay.environment)?
    };
    rows.sort_by_key(|r| {
        (
            !linked.contains(s(&r["id"])),
            !truth(&r["request"]["inputs"]["owner_ids"]),
            options
                .previous
                .iter()
                .position(|id| r["id"] == *id)
                .unwrap_or(options.previous.len()),
            r["kind"] != "mathesis",
        )
    });
    let dh = digest(decision)?;
    let mut attempts = vec![];
    let mut retries = vec![];
    for row in rows {
        let old = &row["data"]["publication_loop"];
        let rh = digest(&row["data"]["result"])?;
        if old["phase"] == "queued" && old["result_hash"] == rh {
            continue;
        }
        if old["phase"] == "excluded"
            && old["result_hash"] == rh
            && old["decision_hash"] == dh
            && truth(&old["exclusion_inputs"])
            && exclusion(&row, &all, table, decision, &old["exclusion_note"])
                .is_ok_and(|(_, i)| i["exclusion_inputs"] == old["exclusion_inputs"])
        {
            continue;
        }
        let binding = Value::Array(vec![string(&rh), string(&dh)]);
        let recheck = replay.state(&format!("recheck:publication:{}", s(&row["id"])));
        let due = if recheck["binding"] == digest(&binding)? {
            recheck["next_at"].as_f64()
        } else {
            None
        };
        if due.is_some_and(|at| replay.now < at) {
            continue;
        }
        let same = old.get("result_hash").unwrap_or(&string(&rh)) == &rh
            && old.get("decision_hash").unwrap_or(&string(&dh)) == &dh;
        if same && old["phase"] == "pending" && truth(&old["last_error"]) {
            retries.push((
                due.unwrap_or(row["updated"].as_f64().unwrap_or(0.0)),
                row,
                binding,
            ));
        } else {
            attempts.push((row, binding, false));
        }
    }
    retries.sort_by(|a, b| a.0.total_cmp(&b.0));
    attempts.extend(
        retries
            .into_iter()
            .take(
                config["mathesis"]["max_publication_retries_per_tick"]
                    .as_u64()
                    .unwrap_or(3) as usize,
            )
            .map(|(_, r, b)| (r, b, true)),
    );
    let mut remaining = config["mathesis"]["max_publications_per_tick"]
        .as_i64()
        .unwrap_or(2);
    let mut first_failures = 0;
    for (row, binding, retry) in attempts {
        if !port.allow("publication", s(&row["id"])) {
            continue;
        }
        if remaining <= 0 {
            break;
        }
        if !retry && first_failures >= 2 {
            continue;
        }
        let old = &row["data"]["publication_loop"];
        let mut state = object([
            ("phase", string("preparing")),
            ("result_hash", binding[0].clone()),
            ("decision_ref", decision["decision_ref"].clone()),
            ("decision_hash", string(&dh)),
        ]);
        let mut step = "exclusion";
        let mut selected = None;
        let outcome = (|| -> StageResult<bool> {
            let (reason, inputs) =
                exclusion(&row, replay.rows("tasks"), table, decision, &Value::Null)?;
            if let Some(reason) = reason {
                state["phase"] = string("excluded");
                state["reason"] = string(reason);
                merge(&mut state, &inputs);
                record(replay, port, service, &row["id"], state.clone(), false)?;
                return Ok(false);
            }
            step = "source-gate";
            service.boundary("source_gate", vec![row["id"].clone()])?;
            source_gate(replay, &row)?;
            step = "package";
            let package = service.package(&row)?;
            let metadata = &package.metadata;
            if let Err(reason) = gates::note_revisions(
                &row,
                replay.rows("tasks"),
                &replay.tables,
                &metadata["problem_ids"],
            ) {
                state["phase"] = string("pending");
                state["reason"] = string(reason.to_string());
                record(replay, port, service, &row["id"], state.clone(), false)?;
                return Ok(false);
            }
            let key = selected_key(metadata);
            let submitted = queued.contains(&key);
            let envelope = &row["data"]["mathesis_delivery"]["envelope"];
            let delivers = envelope["metadata"]["work_id"] == metadata["id"]
                && outreach_canon::python_eq(
                    &envelope["metadata"]["version"],
                    &metadata["revision"],
                )
                && envelope["result_hash"] == digest(&row["data"]["result"])?;
            if submitted
                && (row["kind"] != "mathesis"
                    || taken.contains(&key)
                    || (truth(&row["data"]["mathesis_delivery"]) && !(truth(old) && delivers)))
            {
                return Ok(false);
            }
            step = "exclusion";
            let (reason, inputs) =
                exclusion(&row, replay.rows("tasks"), table, decision, metadata)?;
            if let Some(reason) = reason {
                state["phase"] = string("excluded");
                state["reason"] = string(reason);
                merge(&mut state, &inputs);
                record(replay, port, service, &row["id"], state.clone(), false)?;
                return Ok(false);
            }
            for (k, v) in [
                ("note_id", &metadata["id"]),
                ("revision", &metadata["revision"]),
                ("problem_ids", &metadata["problem_ids"]),
                ("creators", &metadata["creators"]),
            ] {
                state[k] = v.clone();
            }
            step = "checkpoint";
            let mut checkpoint = if retry { old.clone() } else { object([]) };
            merge(&mut checkpoint, &state);
            if retry {
                checkpoint["phase"] = string("pending");
            }
            record(replay, port, service, &row["id"], checkpoint, true)?;
            if !submitted {
                step = "register";
                let catalog = gh.register_note(Persisted::saved(
                    Registration {
                        metadata: metadata.clone(),
                    },
                    port,
                    &replay.tables,
                )?)?;
                step = "stage";
                stage(&package, gh, port, &replay.tables)?;
                step = "enqueue";
                let result = enqueue(&package, &catalog, gh, port, &replay.tables);
                service.boundary("enqueue", vec![])?;
                state["request"] = string(result?);
                step = "request-readback";
                let request = object([
                    ("schema_version", string("short-note-request.v1")),
                    ("id", metadata["id"].clone()),
                    ("action", string("publish")),
                    ("revision", metadata["revision"].clone()),
                ]);
                let raw = gh.download(
                    &format!("short-note-{}", s(&metadata["id"])),
                    s(&state["request"]),
                )?;
                check(
                    parse(&String::from_utf8_lossy(&raw.bytes))? == request,
                    "Publication request readback differs",
                )?;
            }
            if row["kind"] == "mathesis" && !truth(&row["data"]["mathesis_delivery"]) {
                selection(
                    replay,
                    &row,
                    &package,
                    decision,
                    SelectionServices { gh, port, service },
                    secret,
                    &mut step,
                )?;
            }
            step = "checkpoint";
            replay.clear_state(&format!("recheck:publication:{}", s(&row["id"])))?;
            state["phase"] = string("queued");
            record(replay, port, service, &row["id"], state.clone(), true)?;
            selected = Some(key);
            Ok(true)
        })();
        match outcome {
            Ok(true) => {
                remaining -= 1;
                if let Some(key) = selected {
                    queued.insert(key.clone());
                    if row["kind"] == "mathesis" {
                        taken.insert(key);
                    }
                }
            }
            Ok(false) => {}
            Err(error) if error.summary["type"] == "Stopped" => return Err(error),
            Err(error) => {
                let failures = if retry {
                    old["consecutive_failures"].as_i64().unwrap_or(0) + 1
                } else {
                    first_failures += 1;
                    1
                };
                let delay = [1800.0, 7200.0, 28800.0, 86400.0][(failures.clamp(1, 4) - 1) as usize];
                let next = replay.now + delay;
                replay.set_state(
                    &format!("recheck:publication:{}", s(&row["id"])),
                    object([
                        ("binding", string(digest(&binding)?)),
                        ("checked_at", replay.clock_value.clone()),
                        (
                            "next_at",
                            if let Some(now) = replay.clock_value.as_i64() {
                                (now + delay as i64).into()
                            } else {
                                outreach_canon::float(next)?
                            },
                        ),
                    ]),
                )?;
                state["phase"] = string("pending");
                state["last_error"] = error.summary["type"].clone();
                state["step"] = string(step);
                state["diagnostic"] = error
                    .summary
                    .get("diagnostic")
                    .unwrap_or(&error.summary["type"])
                    .clone();
                state["consecutive_failures"] = failures.into();
                state["next_retry_at"] = if let Some(now) = replay.clock_value.as_i64() {
                    (now + delay as i64).into()
                } else {
                    outreach_canon::float(next)?
                };
                if ["ValueError", "GateError"].contains(&s(&error.summary["type"])) {
                    state["reason"] = error.summary["reason"].clone();
                }
                record(replay, port, service, &row["id"], state, false)?;
            }
        }
    }
    Ok(())
}
