//! Local file adapters for signing/building and in-memory ingestion replay.
use super::*;
use outreach_canon::{canonical, sha256, sign_envelope, sign_review};
use outreach_core::{
    envelopes,
    ingest::{InputAsset, Replay, logical_dump},
    string, validation,
};

pub fn handles(args: &[String]) -> bool {
    args.first().is_some_and(|s| {
        [
            "sign-review",
            "review",
            "decision",
            "draft-revision",
            "stage-draft-revision",
            "enqueue",
            "replay-ingest",
            "stage-inbound",
            "stage-thread",
            "stage-submission",
            "stage-mathesis",
            "request-submission-review",
        ]
        .contains(&s.as_str())
    })
}
fn required(args: &mut Vec<String>, key: &str) -> Result<String> {
    take(args, key)?.ok_or_else(|| format!("Missing {key}").into())
}
fn flag(args: &mut Vec<String>, key: &str) -> bool {
    if let Some(i) = args.iter().position(|s| s == key) {
        args.remove(i);
        true
    } else {
        false
    }
}
fn engine(args: &mut Vec<String>) -> Result<PathBuf> {
    let path = take(args, "--engine-root")?.or_else(|| env::var("OUTREACH_ENGINE_ROOT").ok());
    match path {
        Some(p) => Ok(PathBuf::from(p).canonicalize()?),
        None => env::current_dir()?
            .ancestors()
            .find(|p| p.join("managed/control.py").is_file())
            .map(Path::to_path_buf)
            .ok_or_else(|| "Cannot locate engine tree; use --engine-root".into()),
    }
}
fn secret(args: &mut Vec<String>) -> Result<Vec<u8>> {
    let explicit = take(args, "--key-file")?;
    if explicit.is_none()
        && let Ok(s) = env::var("OUTREACH_REVIEW_KEY")
        && !s.is_empty()
    {
        return Ok(s.into_bytes());
    }
    let path = explicit
        .or_else(|| env::var("OUTREACH_REVIEW_KEY_FILE").ok())
        .map(PathBuf::from)
        .or_else(|| {
            env::var("HOME")
                .ok()
                .map(|p| PathBuf::from(p).join(".config/trureturing-outreach/review.key"))
        });
    if let Some(path) = path
        && path.is_file()
    {
        let text = fs::read_to_string(path)?;
        let s =
            text.trim_matches(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c));
        if !s.is_empty() {
            return Ok(s.as_bytes().to_vec());
        }
    }
    Err("Configure the local reviewer-only key file or OUTREACH_REVIEW_KEY".into())
}
fn workflow_value(kind: &str, root: &Path, engine: &Path) -> outreach_canon::Result<Value> {
    workflow(kind, false, |path| {
        let overlay = root.join("policy/managed").join(path);
        let path = if !path.ends_with(".py") && overlay.is_file() {
            overlay
        } else {
            engine.join("managed").join(path)
        };
        match fs::read_to_string(path) {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(outreach_canon::Error::new(e.to_string())),
        }
    })
    .map(Value::String)
}
fn publish_file(value: &Value, prefix: &str, output: &Path) -> Result<()> {
    let asset = envelopes::stage(value, prefix)?;
    let path = if output.is_dir() {
        output.join(&asset.name)
    } else {
        output.to_path_buf()
    };
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, &asset.bytes)?;
    println!(
        "{}",
        canonical(&object([
            ("asset", string(asset.name)),
            ("sha256", string(sha256(&asset.bytes)))
        ]))?
    );
    Ok(())
}
fn emit(value: &Value, output: Option<&Path>, pretty: bool) -> Result<()> {
    let text = if pretty {
        format!(
            "{}\n",
            dumps(
                value,
                SerializeOptions {
                    indent: Some("  "),
                    ensure_ascii: false,
                    ..Default::default()
                }
            )?
        )
    } else {
        canonical(value)?
    };
    if let Some(path) = output {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, text)?;
    } else {
        print!("{text}");
    }
    Ok(())
}
pub fn run(mut args: Vec<String>) -> Result<()> {
    let command = args.remove(0);
    let engine = engine(&mut args)?;
    let root = PathBuf::from(
        take(&mut args, "--root")?
            .or_else(|| env::var("OUTREACH_DATA_ROOT").ok())
            .unwrap_or_else(|| engine.to_string_lossy().into_owned()),
    );
    let output = take(&mut args, "--output")?.map(PathBuf::from);
    if command == "replay-ingest" {
        return replay(args, &root, &engine, output.as_deref());
    }
    let key = if [
        "enqueue",
        "stage-inbound",
        "stage-thread",
        "stage-submission",
    ]
    .contains(&command.as_str())
    {
        Vec::new()
    } else {
        secret(&mut args)?
    };
    match command.as_str() {
        "sign-review" | "review" | "decision" | "draft-revision" | "enqueue" => {
            let kind = take(&mut args, "--kind")?;
            let snapshot = take(&mut args, "--snapshot")?;
            if command == "sign-review" && snapshot.is_some() {
                return Err("sign-review does not read a snapshot".into());
            }
            if let Some(k) = &kind {
                validation::require(
                    outreach_canon::KINDS.contains(&k.as_str()),
                    "Unknown task kind",
                )?;
            }
            if args.len() != 1 {
                return Err("Expected one envelope/request path".into());
            }
            let input = json(Path::new(&args[0]))?;
            let value = match command.as_str() {
                "enqueue" => {
                    validation::request(&input)?;
                    let binding = pinned_binding(
                        s(&input["kind"]),
                        s(&input["source_revision"]),
                        input.get("policy_revision").and_then(Value::as_str),
                        &root,
                        &engine,
                    )?;
                    validation::require(
                        input["workflow_hash"] == binding["workflow_hash"],
                        "Workflow hash does not match the pinned source revision; regenerate the request",
                    )?;
                    let (tables, config) = environment_files(&root, &engine)?;
                    let wf = |kind: &str| workflow_value(kind, &root, &engine);
                    outreach_core::results::firstcontact_gate(
                        &input,
                        &Environment {
                            tables,
                            config,
                            workflow: &wf,
                        },
                    )?;
                    input
                }
                "sign-review" | "review" => {
                    if command == "review" && input.get("signature").is_some() {
                        envelopes::validate_review(&input, &key)?;
                        input
                    } else {
                        let hash = kind
                            .as_ref()
                            .map(|k| workflow_value(k, &root, &engine))
                            .transpose()?;
                        let signed = sign_review(
                            &input,
                            &key,
                            kind.as_deref(),
                            hash.as_ref().and_then(Value::as_str),
                        )?;
                        if input.get("rebind_workflow_hash").is_none()
                            && signed.get("rebind_workflow_hash").is_some()
                        {
                            eprintln!(
                                "sign-review: added rebind_workflow_hash for the current {} workflow",
                                kind.as_deref().unwrap_or("")
                            );
                        }
                        signed
                    }
                }
                "decision" => {
                    if input.get("signature").is_some() {
                        envelopes::validate_decision(&input, &key)?;
                        input
                    } else {
                        envelopes::build_decision(&input, &key)?
                    }
                }
                _ => {
                    let value = if input.get("signature").is_some() {
                        input
                    } else {
                        sign_envelope(&input, &key)?
                    };
                    envelopes::validate_draft_revision(&value, &key)?;
                    value
                }
            };
            if let Some(snapshot) = snapshot {
                let ledger = Ledger::open(Path::new(&snapshot))?;
                let (tables, config) = environment_files(&root, &engine)?;
                let wf = |kind: &str| workflow_value(kind, &root, &engine);
                let environment = Environment {
                    tables,
                    config,
                    workflow: &wf,
                };
                let resource = |_: &Value| {
                    Err(outreach_canon::Error::new(
                        "Recorded artifact is unavailable",
                    ))
                };
                let mut replay = Replay {
                    tables: ledger.replay_snapshot()?,
                    generation: 0,
                    now: 0.0,
                    saved_version: None,
                    environment: &environment,
                    resource: &resource,
                };
                replay.restore()?;
                let prefix = if command == "enqueue" {
                    "request-".into()
                } else {
                    format!("{command}-")
                };
                replay.apply(&prefix, &value, &key).map_err(|e| {
                    Error::Argument(if command == "review" {
                        format!("Review would be rejected by the coordinator: {e}")
                    } else {
                        e.to_string()
                    })
                })?;
            }
            if command == "sign-review" {
                emit(&value, output.as_deref(), true)
            } else {
                publish_file(
                    &value,
                    if command == "enqueue" {
                        "request"
                    } else {
                        &command
                    },
                    output
                        .as_deref()
                        .ok_or("Offline builders require --output PATH or DIRECTORY")?,
                )
            }
        }
        "stage-draft-revision" | "request-submission-review" => {
            let snapshot = required(&mut args, "--snapshot")?;
            let ledger = Ledger::open(Path::new(&snapshot))?;
            let rows = ledger
                .tasks()?
                .into_iter()
                .map(Task::into_value)
                .collect::<outreach_canon::Result<Vec<_>>>()?;
            let (v, prefix) = if command == "stage-draft-revision" {
                let id = required(&mut args, "--task")?;
                let packet = required(&mut args, "--packet")?;
                let reason = required(&mut args, "--reason")?;
                let row = rows.iter().find(|r| r["id"] == id).ok_or("Unknown task")?;
                (
                    envelopes::build_draft_revision(
                        row,
                        &json(Path::new(&packet))?,
                        &reason,
                        &key,
                    )?,
                    "draft-revision",
                )
            } else {
                let paper = required(&mut args, "--paper")?;
                let row = rows
                    .iter()
                    .rev()
                    .find(|r| {
                        r["kind"] == "submission"
                            && r["state"] == "submission-ready"
                            && r["request"]["inputs"]["paper"]["id"] == paper
                            && r["data"]["awaiting_operator_review"] == true
                            && r["data"]["result"]["outcome"] == "completed"
                            && r["request"]["inputs"]
                                .get("submission_round")
                                .is_none_or(|v| v == "finalize")
                    })
                    .ok_or("No unreviewed submission-ready finalize task for paper")?;
                (
                    envelopes::build_submission_review(row, &string(paper), &key)?,
                    "submission-review",
                )
            };
            if !args.is_empty() {
                return Err("Unexpected arguments".into());
            }
            publish_file(
                &v,
                prefix,
                output
                    .as_deref()
                    .ok_or("Offline builders require --output PATH or DIRECTORY")?,
            )
        }
        "stage-inbound" | "stage-thread" | "stage-submission" => {
            let (env_tables, config) = environment_files(&root, &engine)?;
            let wf = |kind: &str| workflow_value(kind, &root, &engine);
            let environment = Environment {
                config,
                tables: env_tables,
                workflow: &wf,
            };
            let revision = take(&mut args, "--revision")?
                .unwrap_or(git_text(&engine, &["rev-parse", "HEAD"])?);
            let policy = match take(&mut args, "--policy-revision")? {
                Some(policy) => Some(policy),
                None if root.canonicalize()? != engine.canonicalize()? => {
                    validation::require(
                        root.join(".git").exists(),
                        "Task creation requires a separate Git checkout for private data",
                    )?;
                    Some(git_text(&root, &["rev-parse", "HEAD"])?)
                }
                None => None,
            };
            let kind = if command == "stage-thread" {
                take(&mut args, "--kind")?.unwrap_or_else(|| "reply".into())
            } else if command == "stage-inbound" {
                "reply".into()
            } else {
                "submission".into()
            };
            let binding = pinned_binding(&kind, &revision, policy.as_deref(), &root, &engine)?;
            let value = if command == "stage-inbound" {
                let message = required(&mut args, "--message-id")?;
                let owner = take(&mut args, "--owner")?;
                let instructions = take(&mut args, "--instructions")?.unwrap_or_default();
                outreach_core::builders::inbound(
                    &message,
                    owner.as_deref(),
                    &instructions,
                    &binding,
                    &environment,
                )?
            } else if command == "stage-thread" {
                let id = required(&mut args, "--thread")?;
                let instructions = take(&mut args, "--instructions")?.unwrap_or_default();
                outreach_core::builders::thread(&id, &kind, &instructions, &binding, &environment)?
            } else {
                let id = required(&mut args, "--paper")?;
                let round = take(&mut args, "--round")?.unwrap_or_else(|| "improve".into());
                let paper = array(&environment.tables["submissions"]["papers"])
                    .iter()
                    .find(|r| r["id"] == id)
                    .ok_or("Unknown independent paper")?;
                let summary = outreach_core::builders::paper_summary(paper);
                let common = git_text(
                    &root,
                    &["rev-parse", "--path-format=absolute", "--git-common-dir"],
                )?;
                let canonical = Path::new(&common)
                    .parent()
                    .ok_or("Missing checkout parent")?
                    .canonicalize()?;
                let workspace = canonical
                    .join(s(&summary["workspace"]))
                    .canonicalize()
                    .map_err(|_| {
                        Error::Argument(
                            "Finish the person-owned package migration before staging it".into(),
                        )
                    })?;
                let mail = canonical
                    .parent()
                    .ok_or("Missing mail parent")?
                    .join("mail")
                    .canonicalize()?;
                validation::require(
                    workspace.is_dir()
                        && workspace.starts_with(&mail)
                        && workspace != mail
                        && !workspace
                            .strip_prefix(&mail)
                            .map_err(|_| "Invalid package")?
                            .components()
                            .any(|p| p.as_os_str() == "outreach-maintenance"),
                    "Finish the person-owned package migration before staging it",
                )?;
                let mut selected = Vec::new();
                // --file is intentionally repeatable, unlike scalar options.
                while let Some(index) = args.iter().position(|s| s == "--file") {
                    args.remove(index);
                    if index >= args.len() {
                        return Err("Missing --file".into());
                    }
                    let relative = args.remove(index);
                    let path = workspace.join(&relative).canonicalize().map_err(|_| {
                        Error::Argument(
                            "Select a scientific file inside this person-owned package".into(),
                        )
                    })?;
                    let rel = path
                        .strip_prefix(&workspace)
                        .map_err(|_| "Invalid selected input")?;
                    validation::require(
                        path.is_file()
                            && !rel
                                .components()
                                .any(|p| p.as_os_str().to_string_lossy().starts_with('.')),
                        "Select a scientific file inside this person-owned package",
                    )?;
                    selected.push(outreach_core::builders::SelectedFile {
                        relative: rel.to_string_lossy().into_owned(),
                        name: path
                            .file_name()
                            .ok_or("Missing input filename")?
                            .to_string_lossy()
                            .into_owned(),
                        raw: fs::read(path)?,
                    });
                }
                let name = workspace
                    .strip_prefix(&mail)
                    .map_err(|_| "Invalid workspace")?
                    .components()
                    .next()
                    .ok_or("Missing workspace name")?
                    .as_os_str()
                    .to_string_lossy();
                let selection = outreach_core::builders::submission(
                    paper,
                    &selected,
                    &name,
                    &round,
                    &binding,
                    &environment,
                )?;
                let destination = output
                    .as_deref()
                    .ok_or("Offline builders require --output DIRECTORY")?;
                validation::require(
                    destination.is_dir(),
                    "stage-submission requires an existing output directory",
                )?;
                for (artifact, raw) in selection.resources {
                    let dir = destination
                        .join("resources")
                        .join(s(&artifact["release_tag"]));
                    fs::create_dir_all(&dir)?;
                    fs::write(dir.join(s(&artifact["name"])), raw)?;
                }
                selection.request
            };
            if !args.is_empty() {
                return Err("Unexpected stage arguments".into());
            }
            publish_file(
                &value,
                "request",
                output
                    .as_deref()
                    .ok_or("Offline builders require --output PATH or DIRECTORY")?,
            )
        }
        "stage-mathesis" => {
            let snapshot = required(&mut args, "--snapshot")?;
            let id = required(&mut args, "--task")?;
            let directory = PathBuf::from(required(&mut args, "--directory")?);
            let decisions = json(Path::new(&required(&mut args, "--decisions")?))?;
            let ledger = Ledger::open(Path::new(&snapshot))?;
            let rows = ledger
                .tasks()?
                .into_iter()
                .map(Task::into_value)
                .collect::<outreach_canon::Result<Vec<_>>>()?;
            let row = rows.iter().find(|r| r["id"] == id).ok_or("Unknown task")?;
            let (tables, config) = environment_files(&root, &engine)?;
            let wf = |kind: &str| workflow_value(kind, &root, &engine);
            let environment = Environment {
                config,
                tables,
                workflow: &wf,
            };
            outreach_core::gates::note_revisions(
                row,
                &rows,
                &ledger.replay_snapshot()?,
                &Value::Null,
            )?;
            outreach_core::transitions::source_gate(
                &row["request"],
                &row["data"]["result"],
                &rows,
            )?;
            envelopes::mathesis_reviewed(row)?;
            let raw = fs::read(safe_child(&directory, "handoff.json")?)?;
            let manifest =
                CanonValue::parse(std::str::from_utf8(&raw).map_err(|_| "Handoff is not UTF-8")?)?
                    .into_value();
            let collection = decisions["purpose"] == "publication"
                && (manifest["kind"] == "short_note"
                    || outreach_core::truth(&row["request"]["inputs"]["pipeline"]["note_task_id"]));
            validation::require(
                array(&row["data"]["result"]["artifacts"]).iter().any(|a| {
                    a["name"] == "handoff.json"
                        && a["role"] == "handoff"
                        && a["bytes"].as_u64() == Some(raw.len() as u64)
                        && a["sha256"] == sha256(&raw)
                }),
                if collection {
                    "The handoff manifest differs from the exact PASS"
                } else {
                    "The scientific handoff manifest was not independently reviewed"
                },
            )?;
            if !collection {
                validation::require(
                    manifest["schema_version"] == "outreach-mathesis-handoff.v1"
                        && manifest["private_correspondence_included"] == false,
                    "Select a scientific-only Outreach handoff",
                )?;
                validation::require(
                    outreach_core::truth(&decisions["decision_ref"])
                        && outreach_core::truth(&decisions["processing_consent"])
                        && outreach_core::truth(&decisions["ai_disclosure"]),
                    "Record actual author choices, processing consent and disclosure before delivery",
                )?;
                validation::require(
                    ["publication", "review_only"].contains(&s(&decisions["purpose"])),
                    "Choose the actual submission purpose",
                )?;
                for (name, hash) in manifest["files"].as_object().into_iter().flatten() {
                    validation::require(
                        Path::new(name)
                            .file_name()
                            .is_some_and(|n| n == name.as_str()),
                        "Handoff source differs from its preserved revision",
                    )?;
                    let raw = fs::read(safe_child(&directory, name)?)?;
                    validation::require(
                        hash == &sha256(&raw),
                        "Handoff source differs from its preserved revision",
                    )?;
                }
                let source = s(&decisions["source_file"]);
                validation::require(
                    manifest["files"].get(source).is_some()
                        && [".zip", ".tar.gz", ".tex", ".pdf"]
                            .iter()
                            .any(|suffix| source.ends_with(suffix)),
                    "Select the existing scientific source archive or manuscript",
                )?;
                let selected_hash = sha256(&fs::read(safe_child(&directory, source)?)?);
                validation::require(
                    array(&row["data"]["result"]["artifacts"])
                        .iter()
                        .any(|a| a["name"] == source && a["sha256"] == selected_hash),
                    "The selected source is not an exact reviewed artifact",
                )?;
            }
            let prior = outreach_core::builders::prior_work(row, &|artifact| {
                let path = safe_child(&directory, s(&artifact["name"]))
                    .map_err(|e| outreach_canon::Error::new(e.to_string()))?;
                fs::read(path).map_err(|e| outreach_canon::Error::new(e.to_string()))
            })?;
            let value = outreach_core::builders::mathesis(
                row,
                &manifest,
                &decisions,
                &prior,
                &key,
                &environment,
            )?;
            outreach_core::gates::note_revisions(
                row,
                &rows,
                &ledger.replay_snapshot()?,
                &value["metadata"],
            )?;
            if !args.is_empty() {
                return Err("Unexpected stage arguments".into());
            }
            publish_file(
                &value,
                "mathesis",
                output
                    .as_deref()
                    .ok_or("Offline builders require --output PATH or DIRECTORY")?,
            )
        }
        _ => Err("Unknown offline command".into()),
    }
}
fn git_text(root: &Path, args: &[&str]) -> Result<String> {
    let out = std::process::Command::new("git")
        .args(args)
        .current_dir(root)
        .output()?;
    if !out.status.success() {
        return Err("Local Git revision is unavailable".into());
    }
    Ok(String::from_utf8(out.stdout)
        .map_err(|_| "Git source is not UTF-8")?
        .trim()
        .into())
}
fn pinned_binding(
    kind: &str,
    revision: &str,
    policy: Option<&str>,
    root: &Path,
    engine: &Path,
) -> Result<Value> {
    validation::require(
        validation::hex(&string(revision), 40),
        "Pin the workflow source commit",
    )?;
    if let Some(policy) = policy {
        validation::require(
            validation::hex(&string(policy), 40),
            "Pin the policy commit",
        )?;
    }
    let source = if policy.is_none() { root } else { engine };
    let hash = workflow(kind, policy.is_none(), |path| {
        let (repo, pin, path) = if !path.ends_with(".py")
            && let Some(policy) = policy
        {
            (root, policy, format!("policy/managed/{path}"))
        } else {
            (source, revision, format!("managed/{path}"))
        };
        let object = format!("{pin}:{path}");
        let mut result = std::process::Command::new("git")
            .args(["show", &object])
            .current_dir(repo)
            .output()
            .map_err(|e| outreach_canon::Error::new(e.to_string()))?;
        if !result.status.success() && policy.is_some() && !path.ends_with(".py") {
            let object = format!(
                "{revision}:managed/{}",
                path.trim_start_matches("policy/managed/")
            );
            result = std::process::Command::new("git")
                .args(["show", &object])
                .current_dir(engine)
                .output()
                .map_err(|e| outreach_canon::Error::new(e.to_string()))?;
        }
        if result.status.success() {
            Ok(Some(
                String::from_utf8(result.stdout)
                    .map_err(|e| outreach_canon::Error::new(e.to_string()))?,
            ))
        } else {
            Ok(None)
        }
    })?;
    let mut v = object([
        ("source_revision", string(revision)),
        ("workflow_hash", string(hash)),
    ]);
    if let Some(policy) = policy {
        v["policy_revision"] = string(policy);
    }
    Ok(v)
}
fn environment_files(root: &Path, engine: &Path) -> Result<(Value, Value)> {
    let mut config = optional_json(&root.join("config.json"))?;
    if config.is_null() {
        config = json(&engine.join("managed/config.example.json"))?;
    }
    let mut tables = object([]);
    for name in [
        "contacts",
        "refusals",
        "decisions",
        "threads",
        "submissions",
        "results",
    ] {
        tables[name] = optional_json(&root.join("tables").join(format!("{name}.json")))?;
    }
    for (name, path) in [
        ("firstcontact_workflow", "firstcontact/workflow.json"),
        ("submission_workflow", "submission/workflow.json"),
    ] {
        let overlay = root.join("policy/managed").join(path);
        tables[name] = json(&if overlay.is_file() {
            overlay
        } else {
            engine.join("managed").join(path)
        })?;
    }
    Ok((tables, config))
}
fn safe_child(root: &Path, name: &str) -> Result<PathBuf> {
    validation::require(
        !name.is_empty()
            && Path::new(name)
                .components()
                .all(|p| matches!(p, std::path::Component::Normal(_))),
        "Unsafe input asset path",
    )?;
    let root = root.canonicalize()?;
    let path = root.join(name).canonicalize()?;
    validation::require(
        path.starts_with(&root),
        "Input escapes local replay directory",
    )?;
    Ok(path)
}
fn replay(mut args: Vec<String>, root: &Path, engine: &Path, output: Option<&Path>) -> Result<()> {
    let snapshot = required(&mut args, "--snapshot")?;
    let inputs = PathBuf::from(required(&mut args, "--inputs")?);
    let generation = take(&mut args, "--generation")?
        .unwrap_or_else(|| "0".into())
        .parse::<i64>()
        .map_err(|_| "Invalid generation")?;
    let now = take(&mut args, "--now")?
        .unwrap_or_else(|| "2000000000".into())
        .parse::<f64>()
        .map_err(|_| "Invalid clock")?;
    validation::require(now.is_finite(), "--now must be finite")?;
    let passes = take(&mut args, "--passes")?
        .unwrap_or_else(|| "1".into())
        .parse::<usize>()
        .map_err(|_| "Invalid passes")?;
    validation::require((1..=1000).contains(&passes), "passes must be 1..1000")?;
    let no_key = flag(&mut args, "--no-key");
    let key = if no_key {
        Vec::new()
    } else {
        secret(&mut args)?
    };
    if !args.is_empty() {
        return Err("Unexpected replay arguments".into());
    }
    let (tables, config) = environment_files(root, engine)?;
    let wf = |kind: &str| workflow_value(kind, root, engine);
    let environment = Environment {
        config,
        tables,
        workflow: &wf,
    };
    let resource = |artifact: &Value| -> outreach_canon::Result<Vec<u8>> {
        let name = s(&artifact["name"]);
        let tag = s(&artifact["release_tag"]);
        let read = (|| -> Result<Vec<u8>> {
            validation::require(
                !tag.contains(['/', '\\']) && !["", ".", ".."].contains(&tag),
                "Unsafe resource release",
            )?;
            let path = safe_child(&inputs.join("resources").join(tag), name)?;
            validation::require(
                path.starts_with(inputs.canonicalize()?),
                "Input escapes local replay directory",
            )?;
            Ok(fs::read(path)?)
        })();
        read.map_err(|_| outreach_canon::Error::new("Recorded artifact is unavailable"))
    };
    let ledger = Ledger::open(Path::new(&snapshot))?;
    let mut replay = Replay {
        tables: ledger.replay_snapshot()?,
        generation,
        now,
        saved_version: None,
        environment: &environment,
        resource: &resource,
    };
    replay.restore()?;
    let manifest = inputs.join("assets.json");
    let metadata = if manifest.is_file() {
        json(&manifest)?
    } else {
        let mut paths = fs::read_dir(&inputs)?
            .map(|e| e.map(|e| e.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        paths.sort();
        Value::Array(
            paths
                .iter()
                .filter(|p| p.is_file() && p.extension().is_some_and(|e| e == "json"))
                .map(|p| {
                    let raw = fs::read(p)?;
                    let name = p.file_name().ok_or("Missing filename")?.to_string_lossy();
                    Ok(object([
                        ("id", string(name.as_ref())),
                        ("name", string(name.as_ref())),
                        ("digest", string(format!("sha256:{}", sha256(&raw)))),
                    ]))
                })
                .collect::<Result<Vec<_>>>()?,
        )
    };
    validation::require(metadata.is_array(), "Asset manifest must be a list")?;
    let mut assets = Vec::new();
    for meta in array(&metadata) {
        let path = safe_child(&inputs, s(&meta["name"]))?;
        assets.push(InputAsset {
            metadata: meta.clone(),
            raw: fs::read(path)?,
        });
    }
    let mut errors = Vec::new();
    for _ in 0..passes {
        errors.push(replay.ingest(&assets, &key)?);
    }
    let value = object([
        ("generation", replay.generation.into()),
        ("errors", Value::Array(errors)),
        ("tables", logical_dump(replay.tables)?),
    ]);
    if let Some(output) = output {
        // create_new also refuses symlinks and existing snapshot/sidecar paths.
        // No replay output option can overwrite an operator's input file.
        use std::io::Write;
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output)?
            .write_all(canonical(&value)?.as_bytes())?;
        Ok(())
    } else {
        emit(&value, None, false)
    }
}
