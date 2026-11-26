use super::*;
use outreach_core::sandbox_pool::{self as pool, CmaPool, SandboxPool};
impl CmaPool for Fake {
    fn sandbox(&mut self, id: &str) -> StageResult<Value> {
        self.call(
            "request",
            vec![string(format!("/api/v1/sandboxes/{id}"))],
            object([]),
        )
    }
    fn lifecycle(&mut self, permit: Persisted<pool::LifecycleIntent>) -> StageResult<Value> {
        self.0.borrow_mut().depend(&permit);
        let i = permit.intent();
        self.call(
            "request",
            vec![
                string(format!("/api/v1/sandboxes/{}/{}", i.sandbox, i.action)),
                string("POST"),
                Value::Null,
                string(&i.key),
            ],
            object([]),
        )
    }
    fn retire_sandbox(&mut self, permit: Persisted<pool::RetirementIntent>) -> StageResult<Value> {
        self.0.borrow_mut().depend(&permit);
        let i = permit.intent();
        self.call(
            "retire_sandbox",
            vec![
                Value::Null,
                string(&i.sandbox),
                string(&i.key),
                i.previous.clone(),
            ],
            object([
                ("dead_ok", true.into()),
                (
                    "finished",
                    Value::Array(i.finished.iter().map(string).collect()),
                ),
                ("max_agent_deletions", i.max_agent_deletions.into()),
                ("sweep_owned", true.into()),
                (
                    "task_ids",
                    Value::Array(i.task_ids.iter().map(string).collect()),
                ),
                ("now", i.now.clone()),
            ]),
        )
    }
}
pub fn execute(
    case: &Value,
    replay: &mut Replay<'_>,
    ports: &mut impl DeliveryPorts,
    fake: &mut Fake,
) -> StageResult<()> {
    let args = array(&case["arguments"]);
    let kw = &case["keywords"];
    let operation = s(&case["operation"]);
    let mut pool = SandboxPool::restored(case["runtime"].clone());
    if !array(&case["options"]["group_order"]).is_empty() {
        let mut limits = object([]);
        for name in array(&case["options"]["group_order"]) {
            limits[s(name)] = pool.runtime["group_limits"][s(name)].clone();
        }
        pool.runtime["group_limits"] = limits;
    }
    let rows = kw
        .get("rows")
        .filter(|v| !v.is_null())
        .map(|v| array(v).to_vec())
        .unwrap_or_else(|| replay.rows("tasks").to_vec());
    let arg = |i: usize| args.get(i).cloned().unwrap_or(Value::Null);
    let param = |i: usize, key: &str| {
        args.get(i)
            .cloned()
            .or_else(|| kw.get(key).cloned())
            .unwrap_or(Value::Null)
    };
    let mut result = Value::Null;
    match operation {
        "__init__" => {
            pool = SandboxPool::new(
                replay,
                &arg(1),
                &param(2, "kinds"),
                kw.get("correspondence")
                    .cloned()
                    .unwrap_or_else(|| Value::Array(vec![])),
                kw["reserve_review_slots"].as_i64().unwrap_or(0),
                s(&case["options"]["idle_pause_seconds"])
                    .parse::<f64>()
                    .map_err(|_| StageError {
                        summary: object([("type", string("ValueError"))]),
                    })?,
            )?
        }
        "health" => pool.health(replay, fake, ports, &rows)?,
        "idle" => pool.idle(replay, fake, ports, &rows)?,
        "transition" => {
            pool.transition(replay, fake, ports, s(&arg(1)["sandbox_id"]), s(&arg(2)))?
        }
        "slot" => {
            result = pool.slot(
                replay,
                if kw["cma"].is_null() {
                    None
                } else {
                    Some(fake)
                },
                ports,
                &param(0, "row"),
                &rows,
            )?;
        }
        "assigned" => pool.assigned(replay, &arg(0))?,
        "remember" => pool.remember(replay, ports, s(&arg(0)), arg(1), &arg(2))?,
        "failed_entry" => {
            result = pool.failed_entry(
                replay,
                ports,
                s(&arg(0)),
                pool::PoolFailure {
                    workspace: param(1, "workspace_id"),
                    corrupt: param(2, "corrupt"),
                    rotated: param(3, "rotated"),
                    stalled: param(4, "stalled"),
                    group: param(5, "group"),
                },
            )?;
        }
        "record_capacity_failure" | "clear_capacity_failure" => pool.capacity_failure(
            replay,
            &param(0, "row"),
            operation == "clear_capacity_failure",
        )?,
        _ => return Err(outreach_canon::Error::new("Unsupported pool operation").into()),
    }
    if canonical(&pool.runtime)? != canonical(&case["expected_runtime"])?
        || canonical(&result)? != canonical(&case["expected_result"])?
    {
        if std::env::var_os("OUTREACH_SCENARIO_DEBUG").is_some() {
            eprintln!(
                "pool-runtime actual={} expected={} result={} expected_result={}",
                canonical(&pool.runtime)?,
                canonical(&case["expected_runtime"])?,
                canonical(&result)?,
                canonical(&case["expected_result"])?
            );
        }
        return Err(outreach_canon::Error::new("Pool runtime/result differs").into());
    }
    Ok(())
}
