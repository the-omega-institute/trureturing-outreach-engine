//! Synthetic transaction benchmark, driven/timed by benchmark_ingest.py.
use outreach_canon::{Result, Value, digest, sign_envelope};
use outreach_core::{Environment, ingest::Replay, object, string};

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let legacy = args.first().is_some_and(|s| s == "legacy-copy");
    let count = 2_000;
    let events = 100_000;
    let assets = 100;
    let padding = "Synthetic payload ".repeat(1_800);
    let hash = "a".repeat(64);
    let rows = (0..count)
        .map(|i| {
            object([
                ("id", string(format!("synthetic-{i}"))),
                ("kind", string("research")),
                ("owner", string("synthetic-owner")),
                ("request", object([])),
                ("request_hash", string(&hash)),
                ("state", string("failed")),
                ("revision", 1.into()),
                ("updated", 1.into()),
                ("data", object([("synthetic_payload", string(&padding))])),
            ])
        })
        .collect();
    let history = (0..events)
        .map(|i| {
            object([
                ("seq", i.into()),
                ("id", string(format!("synthetic-{}", i % count))),
                ("state", string("failed")),
                ("revision", 1.into()),
                ("at", 1.into()),
                ("data", object([("reason", string("Synthetic history"))])),
            ])
        })
        .collect();
    let tables = object([
        ("tasks", object([("rows", Value::Array(rows))])),
        ("task_events", object([("rows", Value::Array(history))])),
    ]);
    let workflow = |_: &str| Ok(Value::Null);
    let environment = Environment {
        tables: object([]),
        config: object([]),
        workflow: &workflow,
    };
    let resource = |_: &Value| Ok(vec![]);
    let mut replay = Replay::new(tables, 0, 2.0, &environment, &resource);
    replay.restore()?;
    for i in 0..assets {
        // Emulate the five-table undo copy removed from Replay::apply. The
        // transition and synthetic ledger are identical in the two modes.
        let original = legacy.then(|| {
            [
                "tasks",
                "task_events",
                "control_events",
                "pipeline_state",
                "sqlite_sequence",
            ]
            .map(|name| (name, replay.tables[name].clone()))
        });
        let envelope = sign_envelope(
            &object([
                ("schema_version", 1.into()),
                ("task_id", string(format!("synthetic-{i}"))),
                ("request_hash", string(&hash)),
                ("failed_revision", 1.into()),
                ("reason", string("Synthetic retry")),
            ]),
            b"public synthetic benchmark key",
        )?;
        replay.apply(
            "failed-requeue-",
            &envelope,
            b"public synthetic benchmark key",
        )?;
        drop(original);
    }
    println!("{}", digest(&replay.tables)?);
    Ok(())
}
