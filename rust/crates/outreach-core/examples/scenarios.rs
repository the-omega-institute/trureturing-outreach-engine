//! Local scenario adapters; no network, SQLite writer or child process.
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
}
impl Script {
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
    fn deliver(&mut self, i: &Persisted<ManuscriptIntent>) -> StageResult<Value> {
        self.call(
            "deliver",
            vec![i.intent().metadata.clone(), file(&i.intent().source)],
            object([]),
        )
    }
    fn deliver_pre_reviewed(&mut self, i: &Persisted<CollectionIntent>) -> StageResult<Value> {
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
    fn withdraw(&mut self, i: &Persisted<WithdrawIntent>) -> StageResult<Value> {
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
}
impl DeliveryPorts for Ports {
    fn ledger_tag(&self) -> &str {
        &self.tag
    }
    fn save_forced(&mut self, _: &Value) -> StageResult<u64> {
        self.script.borrow_mut().call(
            "ledger",
            "save",
            vec![],
            object([("force", true.into())]),
        )?;
        let mut script = self.script.borrow_mut();
        script.generation += 1;
        Ok(script.generation)
    }
    fn save(&mut self, _: &Value) -> StageResult<()> {
        self.script
            .borrow_mut()
            .call("ledger", "save", vec![], object([]))?;
        Ok(())
    }
    fn source(&mut self, d: &Value) -> StageResult<SourceFile> {
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
        Ok(SourceFile {
            name: s(&d["name"]).into(),
            bytes: bytes(&entry["download"]["base64"]),
        })
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
    fn queue_public_link(&mut self, intent: &Persisted<LinkIntent>) -> StageResult<Value> {
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
fn run(case: &Value) -> Result<bool> {
    let mut tables = object([]);
    if let Some(files) = case["initial_files"].as_object() {
        for (path, data) in files {
            if let Some(name) = path
                .strip_prefix("tables/")
                .and_then(|p| p.strip_suffix(".json"))
            {
                tables[name] = parse(&String::from_utf8_lossy(&bytes(data)))?;
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
    let mut replay = Replay::new(
        case["initial_rows"].clone(),
        0,
        case["now"].as_f64().unwrap_or(0.0),
        &env,
        &resource,
    );
    replay.stage_update_times = array(&case["update_times"])
        .iter()
        .map(|v| (v[0].as_f64().unwrap_or(0.0), v[1].as_f64().unwrap_or(0.0)))
        .collect();
    let script = Rc::new(RefCell::new(Script {
        entries: array(&case["script"]).to_vec(),
        index: 0,
        failure: false,
        generation: 0,
    }));
    let mut client = Fake(script.clone(), "mathesis".into());
    let mut ports = Ports {
        script: script.clone(),
        repo: case["adapter_attributes"]["releases"]["repo"].clone(),
        tag: s(&case["adapter_attributes"]["releases"]["tag"]).into(),
        admissions: array(&case["admissions"]).to_vec(),
        admitted: 0,
    };
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
    let error_equal = match advance_with_options(
        &mut replay,
        &mut client,
        &mut ports,
        case["review_key"].as_str().unwrap_or("secret").as_bytes(),
        &mut options,
    ) {
        Ok(()) => case["error"].is_null(),
        Err(e) => e.summary["type"] == case["error"]["type"],
    };
    let script = script.borrow();
    let mut final_tables = replay.tables.clone();
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
    let rows_equal = canonical(&final_tables)? == canonical(&case["expected_final_rows"])?;
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
        .entries
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
        && replay.stage_update_times.is_empty()
        && ports.admitted == ports.admissions.len();
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
    Ok(equal)
}
fn main() -> Result<()> {
    let mut input = Vec::new();
    std::io::stdin()
        .read_to_end(&mut input)
        .map_err(|e| outreach_canon::Error::new(e.to_string()))?;
    let cases = parse(&String::from_utf8_lossy(&input))?;
    let (mut equal, mut different, mut skipped) = (0, 0, 0);
    for case in array(&cases) {
        if case["stage"] != "mathesis" {
            skipped += 1;
        } else if run(case)? {
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
