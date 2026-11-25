//! Local scenario adapters; no network, SQLite writer or child process.
mod stages;
use outreach_canon::{Result, Value, canonical, parse};
use outreach_core::{Environment, array, ingest::Replay, mathesis_delivery::*, object, s, string};
use std::{cell::RefCell, io::Read, rc::Rc};
fn bytes(value: &Value) -> Vec<u8> {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let (mut bits, mut count, mut output) = (0_u32, 0, Vec::new());
    for c in s(value).bytes().take_while(|c| *c != b'=') {
        if let Some(n) = alphabet.iter().position(|b| *b == c) {
            bits = (bits << 6) | n as u32;
            count += 6;
            if count >= 8 {
                count -= 8;
                output.push((bits >> count) as u8);
            }
        }
    }
    output
}
fn file(source: &SourceFile) -> Value {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::new();
    for chunk in source.bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for i in 0..4 {
            output.push(if i > chunk.len() {
                '='
            } else {
                alphabet[((n >> (18 - 6 * i)) & 63) as usize] as char
            });
        }
    }
    object([("$file", string(&source.name)), ("base64", string(output))])
}
struct Script {
    entries: Vec<Value>,
    index: usize,
    failure: bool,
    generation: u64,
    observed: Vec<Value>,
    saved_digest: String,
    dependencies: Vec<Value>,
}
impl Script {
    fn depend<I>(&mut self, permit: &Persisted<I>) {
        if permit.snapshot_digest() != self.saved_digest || permit.generation() != self.generation {
            self.failure = true;
        }
        if self
            .entries
            .get(self.index)
            .and_then(|e| e.get("saved_snapshot_digest"))
            .is_some_and(|d| !d.is_null() && d != permit.snapshot_digest())
        {
            self.failure = true;
        }
        self.dependencies.push(object([
            ("generation", permit.generation().into()),
            ("snapshot_digest", string(permit.snapshot_digest())),
        ]));
    }

    fn call(
        &mut self,
        adapter: &str,
        method: &str,
        args: Vec<Value>,
        kwargs: Value,
    ) -> StageResult<Value> {
        let actual = object([
            ("adapter", string(adapter)),
            ("method", string(method)),
            ("args", Value::Array(args)),
            ("kwargs", kwargs),
        ]);
        self.observed.push(actual.clone());
        let entry = self.entries.get(self.index).cloned().unwrap_or(Value::Null);
        self.index += 1;
        if let Some(digest) = entry.get("args_digest")
            && outreach_canon::digest(&actual["args"])? != *digest
        {
            self.failure = true;
        }
        let expected = object([
            ("adapter", entry["adapter"].clone()),
            ("method", entry["method"].clone()),
            (
                "args",
                if entry.get("args_digest").is_some() {
                    actual["args"].clone()
                } else {
                    entry["args"].clone()
                },
            ),
            ("kwargs", entry["kwargs"].clone()),
        ]);
        if canonical(&actual).ok() != canonical(&expected).ok() {
            if std::env::var_os("OUTREACH_SCENARIO_DEBUG").is_some() {
                eprintln!(
                    "actual={} expected={}",
                    canonical(&actual)?,
                    canonical(&expected)?
                );
            }
            self.failure = true;
            return Err(StageError {
                summary: object([("type", string("ScriptMismatch"))]),
            });
        }
        if let Some(error) = entry.get("error") {
            if error.get("summary").is_some() {
                return Err(StageError {
                    summary: error["summary"].clone(),
                });
            }
            let mut summary = object([("type", error["type"].clone())]);
            if error["type"] == "HTTPError" {
                summary["http_status"] = error["status"].clone();
                if let Ok(body) = parse(&String::from_utf8_lossy(&bytes(&error["body"]))) {
                    for key in ["code", "title"] {
                        if let Some(v) = body[key].as_str() {
                            summary[key] = string(
                                v.split_whitespace()
                                    .collect::<Vec<_>>()
                                    .join(" ")
                                    .chars()
                                    .take(160)
                                    .collect::<String>(),
                            );
                        }
                    }
                }
            } else if ["ValueError", "GateError"]
                .iter()
                .any(|k| error["type"] == *k)
            {
                summary["reason"] = error["message"].clone();
            }
            if let Some(diagnostic) = error.get("diagnostic") {
                summary["diagnostic"] = diagnostic.clone();
            }
            return Err(StageError { summary });
        }
        Ok(entry)
    }
}
#[derive(Clone)]
struct Fake(Rc<RefCell<Script>>, String);
impl Fake {
    fn call(&self, method: &str, args: Vec<Value>, kwargs: Value) -> StageResult<Value> {
        Ok(self.0.borrow_mut().call(&self.1, method, args, kwargs)?["result"].clone())
    }
}
impl MathesisClient for Fake {
    fn verify_owner(&mut self) -> StageResult<()> {
        self.call("verify_owner", vec![], object([]))?;
        Ok(())
    }
    fn find(&mut self, m: &Value) -> StageResult<Value> {
        self.call("find", vec![m.clone()], object([]))
    }
    fn status(&mut self, h: &ReceiptHandle) -> StageResult<Value> {
        self.call(
            "status",
            vec![string(&h.submission), h.version.into()],
            object([]),
        )
    }
    fn deliver(&mut self, i: Persisted<ManuscriptIntent>) -> StageResult<Value> {
        self.0.borrow_mut().depend(&i);
        self.call(
            "deliver",
            vec![i.intent().metadata.clone(), file(&i.intent().source)],
            object([]),
        )
    }
    fn deliver_pre_reviewed(&mut self, i: Persisted<CollectionIntent>) -> StageResult<Value> {
        self.0.borrow_mut().depend(&i);
        self.call(
            "deliver_pre_reviewed",
            vec![
                i.intent().metadata.clone(),
                file(&i.intent().pdf),
                file(&i.intent().source),
            ],
            object([]),
        )
    }
    fn withdraw(&mut self, i: Persisted<WithdrawIntent>) -> StageResult<Value> {
        self.0.borrow_mut().depend(&i);
        self.call("withdraw", vec![string(&i.intent().submission)], object([]))
    }
    fn collected(&mut self, work: &str, version: &Value) -> StageResult<Value> {
        self.call("collected", vec![string(work), version.clone()], object([]))
    }
    fn collected_file(&mut self, work: &str, version: &Value, kind: &str) -> StageResult<Vec<u8>> {
        Ok(bytes(
            &self.call(
                "collected_file",
                vec![string(work), version.clone(), string(kind)],
                object([]),
            )?["$bytes"],
        ))
    }
}
struct Ports {
    script: Rc<RefCell<Script>>,
    repo: Value,
    tag: String,
    admissions: Vec<Value>,
    admitted: usize,
    source_cache: std::collections::BTreeMap<String, SourceFile>,
    cache_sources: bool,
}
impl DeliveryServices for Ports {
    fn ledger_tag(&self) -> &str {
        &self.tag
    }
    fn save_forced(&mut self, tables: &Value) -> StageResult<SavedSnapshot> {
        let entry = self.script.borrow_mut().call(
            "ledger",
            "save",
            vec![],
            object([("force", true.into())]),
        )?;
        let mut script = self.script.borrow_mut();
        script.generation = entry["saved_snapshot"]["generation"]
            .as_u64()
            .unwrap_or(script.generation + 1);
        script.saved_digest = entry["saved_snapshot"]["digest"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or(outreach_canon::digest(tables)?);
        Ok(SavedSnapshot {
            generation: script.generation,
            digest: script.saved_digest.clone(),
        })
    }
    fn save(&mut self, _: &Value) -> StageResult<()> {
        self.script
            .borrow_mut()
            .call("ledger", "save", vec![], object([]))?;
        Ok(())
    }
    fn save_with_force(&mut self, _: &Value, force: bool) -> StageResult<()> {
        self.script.borrow_mut().call(
            "ledger",
            "save",
            vec![],
            object([("force", force.into())]),
        )?;
        Ok(())
    }
    fn source(&mut self, d: &Value) -> StageResult<SourceFile> {
        let key = outreach_canon::digest(d)?;
        if self.cache_sources
            && let Some(file) = self.source_cache.get(&key)
        {
            return Ok(SourceFile {
                name: file.name.clone(),
                bytes: file.bytes.clone(),
            });
        }

        let entry = self.script.borrow_mut().call(
            "releases",
            "run",
            vec![
                string("release"),
                string("download"),
                d["release_tag"].clone(),
                string("--repo"),
                self.repo.clone(),
                string("--pattern"),
                d["name"].clone(),
                string("--dir"),
                object([("$directory", true.into())]),
            ],
            object([]),
        )?;
        let source = SourceFile {
            name: s(&d["name"]).into(),
            bytes: bytes(&entry["download"]["base64"]),
        };
        if self.cache_sources {
            self.source_cache.insert(
                key,
                SourceFile {
                    name: source.name.clone(),
                    bytes: source.bytes.clone(),
                },
            );
        }
        Ok(source)
    }
    fn allow(&mut self, lane: &str, key: &str) -> bool {
        if self.admissions.is_empty() {
            return true;
        }
        let expected = self
            .admissions
            .get(self.admitted)
            .cloned()
            .unwrap_or(Value::Null);
        self.admitted += 1;
        if expected["lane"] != lane || expected["key"] != key {
            self.script.borrow_mut().failure = true;
        }
        expected["allowed"] == true
    }
    fn queue_public_link(&mut self, intent: Persisted<LinkIntent>) -> StageResult<Value> {
        self.script.borrow_mut().depend(&intent);
        let envelope = &intent.intent().envelope;
        let receipt = &intent.intent().receipt;
        let revision = s(&envelope["source"]["name"])
            .strip_prefix("source-v")
            .and_then(|s| s.strip_suffix(".zip"))
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| {
                outreach_canon::Error::new(
                    "Select the canonical reviewed note revision for its public reading link",
                )
            })?;
        let work = &envelope["metadata"]["work_id"];
        let request = object([
            ("schema_version", string("short-note-request.v1")),
            ("id", work.clone()),
            ("action", string("link-mathesis")),
            ("revision", revision.into()),
            ("receipt", receipt.clone()),
        ]);
        let encoded = outreach_canon::dumps(
            &request,
            outreach_canon::SerializeOptions {
                indent: Some("  "),
                ensure_ascii: false,
                sort_keys: true,
                ..Default::default()
            },
        )? + "\n";
        let name = format!(
            "request-{}.json",
            &outreach_canon::sha256(encoded.as_bytes())[..24]
        );
        let tag = string(format!("short-note-{}", s(work)));
        let publication = Fake(self.script.clone(), "publication".into());
        let release = publication.call("release", vec![tag.clone()], object([]))?;
        let manifest = publication.call(
            "download",
            vec![
                tag.clone(),
                string(format!("note-v{revision}.json")),
                object([("$directory", true.into())]),
            ],
            object([]),
        )?;
        let package = parse(&String::from_utf8_lossy(&bytes(&manifest["base64"])))?;
        if package["metadata"]["revision"] != revision {
            return Err(outreach_canon::Error::new(
                "Staged manifest revision differs from the request",
            )
            .into());
        }
        if array(&release["assets"]).iter().any(|a| a["name"] == name) {
            let previous = publication.call(
                "download",
                vec![
                    tag.clone(),
                    string(&name),
                    object([("$directory", true.into())]),
                ],
                object([]),
            )?;
            if parse(&String::from_utf8_lossy(&bytes(&previous["base64"])))? != request {
                return Err(outreach_canon::Error::new("Immutable request content differs").into());
            }
        } else {
            publication.call(
                "upload",
                vec![
                    tag.clone(),
                    file(&SourceFile {
                        name: name.clone(),
                        bytes: encoded.into_bytes(),
                    }),
                ],
                object([]),
            )?;
        }
        let readback = publication.call(
            "download",
            vec![tag, string(&name), object([("$directory", true.into())])],
            object([]),
        )?;
        if parse(&String::from_utf8_lossy(&bytes(&readback["base64"])))? != request {
            return Err(outreach_canon::Error::new(
                "Mathesis reading-link request readback differs",
            )
            .into());
        }
        Ok(object([
            ("phase", string("queued")),
            ("request", string(name)),
            ("revision", revision.into()),
        ]))
    }
}
fn run(case: &Value, previous: &mut Option<Value>) -> Result<bool> {
    let mut tables = object([]);
    if let Some(files) = case["initial_files"].as_object() {
        for (path, data) in files {
            if let Some(name) = path
                .strip_prefix("tables/")
                .and_then(|p| p.strip_suffix(".json"))
            {
                tables[name] = parse(&String::from_utf8_lossy(&bytes(data)))?;
            }
            if path.ends_with("/author-policy.json") {
                tables["author_policy"] = parse(&String::from_utf8_lossy(&bytes(data)))?;
            }
        }
    }
    let workflow = |_: &str| Ok(case["workflow"].clone());
    let mut config = case["config"].clone();
    if config.get("identity").is_none() {
        config["identity"] = case["installation"]["identity"].clone();
    }
    let env = Environment {
        config,
        tables,
        workflow: &workflow,
    };
    let resource = |_: &Value| Ok(vec![]);
    let mut initial_rows = case
        .get("initial_rows")
        .cloned()
        .or_else(|| previous.clone())
        .unwrap_or(Value::Null);
    if let Some(expected) = case.get("base_digests") {
        let mut logical = initial_rows.clone();
        sort_tables(&mut logical);
        let mut actual = object([]);
        for (name, table) in logical.as_object().into_iter().flatten() {
            actual[name] = string(outreach_canon::digest(table)?);
        }
        if actual != *expected {
            return Ok(false);
        }
    }
    apply_boundary_delta(&mut initial_rows, &case["initial_delta"])?;
    if let Some(expected) = case.get("initial_digests") {
        let mut actual = object([]);
        let mut logical = initial_rows.clone();
        sort_tables(&mut logical);
        for (name, table) in logical.as_object().into_iter().flatten() {
            actual[name] = string(outreach_canon::digest(table)?);
        }
        if actual != *expected {
            return Ok(false);
        }
    }
    let mut replay = Replay::new(
        initial_rows,
        0,
        case["now"].as_f64().unwrap_or(0.0),
        &env,
        &resource,
    );
    replay.clock_value = case["now"].clone();
    replay.stage_update_times = array(&case["update_times"])
        .iter()
        .map(|v| (v[0].as_f64().unwrap_or(0.0), v[1].as_f64().unwrap_or(0.0)))
        .collect();
    let script = Rc::new(RefCell::new(Script {
        entries: array(&case["script"]).to_vec(),
        index: 0,
        failure: false,
        generation: 0,
        observed: vec![],
        saved_digest: String::new(),
        dependencies: vec![],
    }));
    let mut client = Fake(script.clone(), "mathesis".into());
    let mut ports = Ledger::new(Ports {
        script: script.clone(),
        repo: case["adapter_attributes"]["releases"]["repo"].clone(),
        tag: s(&case["adapter_attributes"]["releases"]["tag"]).into(),
        admissions: array(&case["admissions"]).to_vec(),
        admitted: 0,
        source_cache: Default::default(),
        cache_sources: case["stage"] == "publication",
    });
    let mut options = StageOptions {
        attempted: case["options"]
            .get("attempted")
            .filter(|v| !v.is_null())
            .map(|v| array(v).iter().map(|v| s(v).to_owned()).collect()),
        again: case["options"]["again"] == true,
        previous: array(&case["options"]["previous"])
            .iter()
            .map(|v| s(v).to_owned())
            .collect(),
    };
    let mut files = case["initial_files"].clone();
    let outcome = if case["stage"] == "mathesis" {
        advance_with_options(
            &mut replay,
            &mut client,
            &mut ports,
            case["review_key"].as_str().unwrap_or("secret").as_bytes(),
            &mut options,
        )
    } else if case["stage"] == "publication" {
        outreach_core::publication::advance(
            &mut replay,
            &mut Fake(script.clone(), "publication".into()),
            &mut stages::Notes {
                fake: Fake(script.clone(), "shortnote".into()),
            },
            &mut ports,
            case["review_key"].as_str().unwrap_or("secret").as_bytes(),
            &outreach_core::publication::Options {
                previous: options.previous.clone(),
            },
        )
    } else {
        let mut contacts = env.tables["contacts"].clone();
        let mut store = stages::Store {
            fake: Fake(script.clone(), "registry".into()),
            files: files.clone(),
        };
        let stage = if case["options"]["advance"] == true {
            outreach_core::sent_events::advance
        } else {
            outreach_core::sent_events::reconcile
        };
        let result = stage(
            &mut replay,
            &mut contacts,
            &mut stages::Mailbox {
                fake: Fake(script.clone(), "mailbox".into()),
                settings: case["adapter_attributes"]["mailbox"]["settings"].clone(),
            },
            &mut store,
            &mut ports,
            &s(&case["datetime"]).replace("+00:00", "Z"),
            &outreach_core::sent_events::Options {
                max_pages: case["options"]["max_pages"].as_u64().map(|n| n as usize),
                absent: case["options"]["mailbox"] == false,
            },
        );
        files = store.files;
        result.map(|_| ())
    };
    let error_equal = match outcome {
        Ok(()) => case["error"].is_null(),
        Err(e) => e.summary["type"] == case["error"]["type"],
    };
    let script = script.borrow();
    let mut final_tables = replay.tables.clone();
    sort_tables(&mut final_tables);
    let rows_equal = if case.get("expected_final_rows").is_some() {
        canonical(&final_tables)? == canonical(&case["expected_final_rows"])?
    } else {
        let mut digests = object([]);
        for (name, table) in final_tables.as_object().into_iter().flatten() {
            digests[name] = string(outreach_canon::digest(table)?);
        }
        digests == case["expected_final_digests"]
    };
    let files_equal = case.get("expected_final_rows").is_none()
        || canonical(&files)? == canonical(&case["expected_final_files"])?;
    let write_methods = [
        "deliver",
        "deliver_pre_reviewed",
        "withdraw",
        "upload",
        "create",
        "register_note",
        "save_state",
        "delete_asset",
        "commit_tables",
    ];
    let writes: Vec<_> = script
        .observed
        .iter()
        .filter(|e| write_methods.contains(&s(&e["method"])))
        .map(|e| {
            object([
                ("adapter", e["adapter"].clone()),
                ("method", e["method"].clone()),
                ("args", e["args"].clone()),
                ("kwargs", e["kwargs"].clone()),
            ])
        })
        .collect();
    let writes_equal = case.get("expected_writes").is_none()
        || canonical(&Value::Array(writes))? == canonical(&case["expected_writes"])?;
    let equal = error_equal
        && writes_equal
        && !script.failure
        && script.index == script.entries.len()
        && rows_equal
        && files_equal
        && replay.stage_update_times.is_empty()
        && ports.services().admitted == ports.services().admissions.len();
    if !equal {
        eprintln!(
            "scenario-failed {} script={}/{} mismatch={} rows={}",
            s(&case["id"]),
            script.index,
            script.entries.len(),
            script.failure,
            rows_equal
        );
    }
    if equal {
        *previous = Some(replay.tables);
    }
    Ok(equal)
}
fn apply_boundary_delta(tables: &mut Value, delta: &Value) -> Result<()> {
    for (name, patch) in delta.as_object().into_iter().flatten() {
        if patch["drop"] == true {
            if let Some(tables) = tables.as_object_mut() {
                tables.shift_remove(name);
            }
            continue;
        }
        if tables.get(name).is_none() {
            tables[name] = object([
                ("columns", patch["columns"].clone()),
                ("primary_key", patch["primary_key"].clone()),
                ("rows", Value::Array(vec![])),
            ]);
        }
        let keys = array(&patch["primary_key"]);
        let key = |row: &Value| {
            outreach_canon::canonical(&Value::Array(
                keys.iter().map(|k| row[s(k)].clone()).collect(),
            ))
        };
        let rows = tables[name]["rows"]
            .as_array_mut()
            .ok_or_else(|| outreach_canon::Error::new("Missing boundary rows"))?;
        for deleted in array(&patch["deleted"]) {
            let mut retained = Vec::new();
            for row in rows.drain(..) {
                if key(&row)? != s(deleted) {
                    retained.push(row);
                }
            }
            *rows = retained;
        }
        for change in array(&patch["changes"]) {
            if key(&change["row"])? != s(&change["key"]) {
                return Err(outreach_canon::Error::new("Boundary key mismatch"));
            }
            let mut found = None;
            for (index, row) in rows.iter().enumerate() {
                if key(row)? == s(&change["key"]) {
                    found = Some(index);
                    break;
                }
            }
            if let Some(index) = found {
                if change["append"] == true {
                    rows.remove(index);
                    rows.push(change["row"].clone());
                } else {
                    rows[index] = change["row"].clone();
                }
            } else {
                rows.push(change["row"].clone());
            }
        }
    }
    Ok(())
}
fn sort_tables(final_tables: &mut Value) {
    if let Some(tables) = final_tables.as_object_mut() {
        for table in tables.values_mut() {
            let keys = array(&table["primary_key"]).to_vec();
            if let Some(rows) = table["rows"].as_array_mut() {
                rows.sort_by(|a, b| {
                    for key in &keys {
                        let k = s(key);
                        let cmp = if let (Some(a), Some(b)) = (a[k].as_i64(), b[k].as_i64()) {
                            a.cmp(&b)
                        } else {
                            s(&a[k]).cmp(s(&b[k]))
                        };
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }
    }
}
fn main() -> Result<()> {
    let mut input = Vec::new();
    std::io::stdin()
        .read_to_end(&mut input)
        .map_err(|e| outreach_canon::Error::new(e.to_string()))?;
    let cases = parse(&String::from_utf8_lossy(&input))?;
    let (mut equal, mut different, mut skipped) = (0, 0, 0);
    let mut previous = None;
    for case in array(&cases) {
        if !["mathesis", "publication", "sent"].contains(&s(&case["stage"])) {
            skipped += 1;
        } else if run(case, &mut previous)? {
            equal += 1;
        } else {
            different += 1;
        }
    }
    println!(
        "{}",
        canonical(&object([
            ("equal", equal.into()),
            ("different", different.into()),
            ("skipped", skipped.into())
        ]))?
    );
    if different > 0 {
        std::process::exit(1);
    }
    Ok(())
}
