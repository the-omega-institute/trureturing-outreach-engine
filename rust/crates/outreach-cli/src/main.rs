use outreach_canon::{CanonValue, SerializeOptions, Value, dumps, workflow};
use outreach_core::{Environment, StatusInput, array, object, s};
use outreach_ledger::Ledger;
use outreach_model::TaskKind;
use std::{
    env, fs,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const HELP: &str = "Usage: outreach control <status --compact|reviews|review-digest|exceptions>\n  --snapshot PATH          Immutable, closed SQLite snapshot (no downloads)\n  --root PATH              Data root; defaults to OUTREACH_DATA_ROOT or engine root\n  --config PATH            Override root/config.json\n  --engine-root PATH       Public engine tree for live workflow inputs\n  --now UNIX_SECONDS       Explicit clock for reproducible question health\n  --work PATH              Work JSON; required for review-digest, optional for exceptions\n  --format json|text       digest defaults to text; exceptions defaults to json\n";
fn json(path: &Path) -> Result<Value> {
    Ok(CanonValue::parse(&fs::read_to_string(path)?)?
        .value()
        .clone())
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
    let snapshot = take(&mut args, "--snapshot")?;
    let engine = PathBuf::from(take(&mut args, "--engine-root")?.unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../..")
            .to_string_lossy()
            .into_owned()
    }));
    let engine = engine.canonicalize()?;
    let root = PathBuf::from(
        take(&mut args, "--root")?
            .or_else(|| env::var("OUTREACH_DATA_ROOT").ok())
            .unwrap_or_else(|| engine.to_string_lossy().into_owned()),
    );
    let root = root.canonicalize()?;
    let config_path = take(&mut args, "--config")?
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            if root == engine && !root.join("config.json").is_file() {
                root.join("private-data/config.json")
            } else {
                root.join("config.json")
            }
        });
    let now = take(&mut args, "--now")?
        .map(|s| s.parse::<f64>())
        .transpose()?;
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
    let mut tables = object([]);
    for name in [
        "contacts",
        "refusals",
        "decisions",
        "submissions",
        "threads",
    ] {
        let path = root.join("tables").join(format!("{name}.json"));
        if path.is_file() {
            tables[name] = json(&path)?;
        } else if command == "exceptions" || (command == "status" && name == "contacts") {
            return Err(format!("Missing table: {}", path.display()).into());
        }
    }
    let mut workflows = object([]);
    if ["exceptions", "review-digest"].contains(&command) {
        for kind in TaskKind::ALL {
            let value = workflow(kind.as_str(), false, |path| {
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
            });
            workflows[kind.as_str()] = match value {
                Ok(hash) => Value::String(hash),
                Err(_) if command == "review-digest" => Value::Null,
                Err(e) => return Err(e.into()),
            };
        }
    }
    let environment = Environment {
        config,
        tables,
        workflows,
    };
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
            let now = match now {
                Some(n) if n.is_finite() => n,
                Some(_) => return Err("--now must be finite".into()),
                None => wall_clock()?,
            };
            outreach_core::status(
                StatusInput {
                    rows: ledger.lane_rows()?,
                    counts: ledger.counts(false)?,
                    counts_by_kind: ledger.counts(true)?,
                    sources: ledger.pipeline_sources()?,
                    source_notes,
                    derived_tasks,
                    baseline: ledger.state("baseline")?.unwrap_or(Value::Null),
                    rejected: ledger.state("ingest_rejected")?.unwrap_or(object([])),
                    connector_errors: ledger
                        .state("connector_errors")?
                        .unwrap_or(Value::Array(vec![])),
                    old_questions: ledger.old_owner_questions(now)?,
                },
                &environment,
            )?
        }
        "reviews" => {
            let ledger = ledger.as_ref().unwrap();
            outreach_core::reviews(&ledger.task_values()?, &ledger.review_events()?)?
        }
        "review-digest" => {
            outreach_core::review_digest(&json(work.as_ref().unwrap())?, &environment)?
        }
        "exceptions" => {
            let work = match work {
                Some(path) => json(&path)?,
                None => object([(
                    "tasks",
                    Value::Array(ledger.as_ref().unwrap().task_values()?),
                )]),
            };
            outreach_core::exceptions(&work, &environment)?
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
        .duration_since(UNIX_EPOCH)?
        .as_secs_f64())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(2);
    }
}
