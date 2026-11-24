use outreach_canon::{CanonValue, SerializeOptions, Value, dumps, workflow};
use outreach_core::{Environment, StatusInput, array, object, s};
use outreach_ledger::Ledger;
use outreach_model::Task;
use std::{
    env, fs,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};
mod offline;
#[derive(Debug)]
enum Error {
    Io(std::io::Error),
    Ledger(outreach_ledger::Error),
    Data(outreach_canon::Error),
    Argument(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::Ledger(e) => e.fmt(f),
            Self::Data(e) => e.fmt(f),
            Self::Argument(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<outreach_ledger::Error> for Error {
    fn from(e: outreach_ledger::Error) -> Self {
        Self::Ledger(e)
    }
}
impl From<outreach_canon::Error> for Error {
    fn from(e: outreach_canon::Error) -> Self {
        Self::Data(e)
    }
}
impl From<String> for Error {
    fn from(e: String) -> Self {
        Self::Argument(e)
    }
}
impl From<&str> for Error {
    fn from(e: &str) -> Self {
        Self::Argument(e.into())
    }
}
type Result<T> = std::result::Result<T, Error>;
const HELP: &str = "Usage: outreach control <COMMAND>\n  Reads: status --compact | reviews | review-digest | exceptions\n  Files: sign-review | review | decision | draft-revision | enqueue\n         stage-draft-revision | stage-thread | stage-inbound | stage-submission\n         stage-mathesis | request-submission-review\n  Replay: replay-ingest --snapshot PATH --inputs DIRECTORY\n  --snapshot PATH          Immutable, closed SQLite snapshot (no downloads)\n  --root PATH              Data root; defaults to OUTREACH_DATA_ROOT or engine root\n  --engine-root PATH       Public engine tree for live workflow inputs\n  --now UNIX_SECONDS       Explicit clock for reproducible reads/replay\n  --work PATH              Work JSON; required for review-digest, optional for exceptions\n  --format json|text       digest defaults to text; exceptions defaults to json\n  --output PATH            Local output file/directory; stage commands require it\n  --overwrite              Allow replacing existing output files; snapshot stays protected\n  --kind KIND              Bound task kind for sign-review; reply/research for stage-thread\n  --key-file PATH          Local signing key (or OUTREACH_REVIEW_KEY/_FILE)\n  Replay options: --generation INTEGER --passes 1..1000 --no-key\n  Stage options match Python; review/stage-draft-revision/Mathesis/submission review require --snapshot.\n  All stage commands produce local files only. Output files must be new unless --overwrite is given.\n";
fn json(path: &Path) -> Result<Value> {
    Ok(CanonValue::parse(&fs::read_to_string(path)?)?.into_value())
}
fn optional_json(path: &Path) -> Result<Value> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(CanonValue::parse(&text)?.into_value()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Value::Null),
        Err(error) => Err(error.into()),
    }
}
fn take(args: &mut Vec<String>, name: &str) -> Result<Option<String>> {
    if let Some(index) = args.iter().position(|a| a == name) {
        args.remove(index);
        if index >= args.len() || args[index].starts_with("--") {
            return Err(format!("Missing value for {name}").into());
        }
        let value = args.remove(index);
        if args.iter().any(|a| a == name) {
            return Err(format!("Duplicate {name}").into());
        }
        Ok(Some(value))
    } else {
        Ok(None)
    }
}
fn run() -> Result<()> {
    let mut args: Vec<_> = env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print!("{HELP}");
        return Ok(());
    }
    if args.is_empty() || args == ["--version"] {
        println!("outreach {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if args.first().map(String::as_str) != Some("control") {
        return Err(HELP.into());
    }
    args.remove(0);
    if offline::handles(&args) {
        return offline::run(args);
    }
    let snapshot = take(&mut args, "--snapshot")?;
    let engine =
        match take(&mut args, "--engine-root")?.or_else(|| env::var("OUTREACH_ENGINE_ROOT").ok()) {
            Some(path) => PathBuf::from(path),
            None => env::current_dir()?
                .ancestors()
                .find(|p| p.join("managed/control.py").is_file())
                .map(Path::to_path_buf)
                .ok_or("Cannot locate engine tree; use --engine-root")?,
        }
        .canonicalize()?;
    let root = PathBuf::from(
        take(&mut args, "--root")?
            .or_else(|| env::var("OUTREACH_DATA_ROOT").ok())
            .unwrap_or_else(|| engine.to_string_lossy().into_owned()),
    )
    .canonicalize()?;
    let config_path = if root == engine
        && env::var("GITHUB_ACTIONS").as_deref() != Ok("true")
        && !root.join("config.json").is_file()
    {
        root.join("private-data/config.json")
    } else {
        root.join("config.json")
    };
    let now = take(&mut args, "--now")?
        .map(|s| s.parse::<f64>())
        .transpose()
        .map_err(|e| Error::Argument(format!("Invalid --now: {e}")))?;
    let work = take(&mut args, "--work")?.map(PathBuf::from);
    let format = take(&mut args, "--format")?.unwrap_or_else(|| {
        if args.first().map(String::as_str) == Some("review-digest") {
            "text".into()
        } else {
            "json".into()
        }
    });
    if !["json", "text"].contains(&format.as_str()) {
        return Err("format must be json or text".into());
    }
    let command = args.first().map(String::as_str).unwrap_or("");
    let valid = match command {
        "status" => args == ["status", "--compact"],
        "reviews" | "review-digest" | "exceptions" => args.len() == 1,
        _ => false,
    };
    if !valid {
        return Err(HELP.into());
    }
    if command == "review-digest" && work.is_none() {
        return Err("review-digest requires --work, as in Python".into());
    }
    if ["status", "reviews"].contains(&command) && (work.is_some() || format != "json") {
        return Err("--work/--format apply only to review-digest and exceptions".into());
    }
    let config = json(&config_path)?;
    outreach_core::validate_lanes(&config)?;
    let workflow_source = |kind: &str| {
        workflow(kind, false, |path| {
            let overlay = root.join("policy/managed").join(path);
            let path = if !path.ends_with(".py") && overlay.is_file() {
                overlay
            } else {
                engine.join("managed").join(path)
            };
            match fs::read_to_string(path) {
                Ok(text) => Ok(Some(text)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(outreach_canon::Error::new(e.to_string())),
            }
        })
        .map(Value::String)
    };
    let mut environment = Environment {
        config,
        tables: object([]),
        workflow: &workflow_source,
    };
    let read_table = |name: &str| json(&root.join("tables").join(format!("{name}.json")));
    match command {
        "exceptions" => {
            for name in [
                "contacts",
                "refusals",
                "decisions",
                "submissions",
                "threads",
            ] {
                environment.tables[name] = read_table(name)?;
            }
        }
        "review-digest" => {
            for name in ["contacts", "refusals"] {
                environment.tables[name] = read_table(name).unwrap_or(Value::Null);
            }
        }
        _ => {}
    }
    let ledger = if command == "review-digest" || (command == "exceptions" && work.is_some()) {
        None
    } else {
        Some(Ledger::open(Path::new(
            snapshot.as_deref().ok_or("--snapshot is required")?,
        ))?)
    };
    let value = match command {
        "status" => {
            let ledger = ledger.as_ref().unwrap();
            let (source_notes, derived_tasks) = ledger.pipeline_counts()?;
            let rows = ledger.lane_rows()?;
            let sources = ledger.pipeline_sources()?;
            environment.tables["contacts"] = read_table("contacts")?;
            let need_correspondence = outreach_core::truth(
                &environment.config["sandbox_pool"]["prioritize_correspondence_notes"],
            ) && rows
                .iter()
                .any(|r| r["kind"] == "shortnote" || r["kind"] == "mathesis");
            let need_preflight = sources.iter().any(|r| {
                ["approved", "handoff-ready", "verified"].contains(&s(&r["state"]))
                    && array(&environment.tables["contacts"]["contacts"])
                        .iter()
                        .any(|p| array(&p["problem_ids"]).contains(&r["result_id"]))
            });
            if need_correspondence || need_preflight {
                environment.tables["refusals"] = read_table("refusals")?;
            }
            if need_preflight {
                // Python's excluded_region treats a missing decision table as
                // no exclusion; malformed present files still fail closed.
                environment.tables["decisions"] =
                    optional_json(&root.join("tables/decisions.json"))?;
            }
            let now = match now {
                Some(n) if n.is_finite() => n,
                Some(_) => return Err("--now must be finite".into()),
                None => wall_clock()?,
            };
            outreach_core::status(
                StatusInput {
                    rows,
                    counts: ledger.counts(false)?,
                    counts_by_kind: ledger.counts(true)?,
                    sources,
                    source_notes,
                    derived_tasks,
                    baseline: ledger.state("baseline")?.unwrap_or(Value::Null),
                    rejected: ledger.state("ingest_rejected")?.unwrap_or(object([])),
                    connector_errors: ledger
                        .state("connector_errors")?
                        .unwrap_or(Value::Array(vec![])),
                    old_questions: ledger
                        .old_owner_questions(now - outreach_core::OWNER_QUESTION_AGE)?,
                },
                &environment,
            )?
        }
        "reviews" => {
            let ledger = ledger.as_ref().unwrap();
            let tasks = ledger.tasks()?;
            let mut reviewable_tasks = Vec::new();
            for task in &tasks {
                if outreach_core::reviewable(&task.clone().into_value()?)? {
                    reviewable_tasks.push(task);
                }
            }
            let events = ledger.review_events(&reviewable_tasks)?;
            outreach_core::reviews(
                tasks
                    .into_iter()
                    .map(Task::into_value)
                    .collect::<outreach_canon::Result<_>>()?,
                &events,
            )?
        }
        "review-digest" => {
            outreach_core::review_digest(&json(work.as_ref().unwrap())?, &environment)?
        }
        "exceptions" => {
            let work = match work {
                Some(path) => json(&path)?,
                None => object([(
                    "tasks",
                    Value::Array(
                        ledger
                            .as_ref()
                            .unwrap()
                            .tasks()?
                            .into_iter()
                            .map(Task::into_value)
                            .collect::<outreach_canon::Result<_>>()?,
                    ),
                )]),
            };
            outreach_core::exceptions(work, &environment)?
        }
        _ => unreachable!(),
    };
    if command == "exceptions" {
        print!(
            "{}",
            outreach_core::render_exceptions(&value, format == "text")?
        );
    } else if command == "review-digest" && format == "text" {
        for line in array(&value["lines"]) {
            println!("{}", s(line));
        }
    } else {
        println!(
            "{}",
            dumps(
                &value,
                SerializeOptions {
                    indent: Some("  "),
                    ensure_ascii: false,
                    ..Default::default()
                }
            )?
        );
    }
    Ok(())
}
// The only wall-clock adapter; pure core receives explicit time.
#[allow(clippy::disallowed_methods)]
fn wall_clock() -> Result<f64> {
    Ok(std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::Argument(e.to_string()))?
        .as_secs_f64())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(2);
    }
}
