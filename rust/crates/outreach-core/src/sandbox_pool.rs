//! Offline CMA pool. Placement counts are telemetry; admission switches never
//! enter the retirement predicate. CMA owns its bounded stop-before-delete
//! protocol; retirement receives the original checkpointed handle and ownership.
use crate::mathesis_delivery::{DeliveryPorts, StageError, StageResult};
use crate::persistence::Persisted;
use crate::{array, ingest::Replay, object, s, string, truth, validation::require};
use outreach_canon::Value;
use std::collections::BTreeSet;

pub const HEALTHY: [&str; 4] = ["active", "paused", "pausing", "reviving"];
pub const RETAINED_RECHECK_SECONDS: f64 = 21600.;
pub const STALLED_SECONDS: f64 = 7200.;
pub const RETIRE_STALLED_SECONDS: f64 = 43200.;
pub const CAPACITY_BLOCK_SECONDS: f64 = 7200.;
pub const POOL_WAIT_SECONDS: f64 = 1800.;
pub const AGENT_CLEANUP_PER_TICK: i64 = 8;
pub struct PoolFailure {
    pub workspace: Value,
    pub corrupt: Value,
    pub rotated: Value,
    pub stalled: Value,
    pub group: Value,
}
pub struct LifecycleIntent {
    pub sandbox: String,
    pub action: String,
    pub key: String,
}
pub struct RetirementIntent {
    pub sandbox: String,
    pub key: String,
    pub previous: Value,
    pub finished: BTreeSet<String>,
    pub task_ids: BTreeSet<String>,
    pub max_agent_deletions: i64,
    pub now: Value,
}
/// Retirement implementations must stop Agents before deleting them, reconcile
/// accepted deletes by their original IDs, retain foreign work, and return
/// bounded ownership sweeps/operator checks. No live Rust implementation exists.
pub trait CmaPool {
    fn sandbox(&mut self, id: &str) -> StageResult<Value>;
    fn lifecycle(&mut self, intent: Persisted<LifecycleIntent>) -> StageResult<Value>;
    fn retire_sandbox(&mut self, intent: Persisted<RetirementIntent>) -> StageResult<Value>;
}
pub(crate) fn remove(v: &mut Value, key: &str) {
    if let Some(o) = v.as_object_mut() {
        o.shift_remove(key);
    }
}
fn num(v: &Value) -> f64 {
    v.as_f64().unwrap_or(0.)
}
fn count(v: &Value) -> i64 {
    v.as_i64().unwrap_or(0)
}
fn list(v: &Value) -> Vec<Value> {
    array(v).to_vec()
}
fn default(v: &Value, key: &str, fallback: Value) -> Value {
    v.get(key).cloned().unwrap_or(fallback)
}
fn active(v: &Value) -> bool {
    [
        "provisioning",
        "admitting",
        "running",
        "admission-unknown",
        "provision-unknown",
        "credit-wait",
    ]
    .contains(&s(v))
}
fn runs(row: &Value) -> Vec<Value> {
    let mut result = list(&row["data"]["hosted_review_history"]);
    if row["data"]["hosted_review"].is_object() {
        result.insert(0, row["data"]["hosted_review"].clone());
    }
    result
}
fn live(run: &Value) -> bool {
    let c = &run["cleanup"];
    truth(&run["create_key"])
        && !truth(&run["cleanup_complete"])
        && !truth(&c["stopped"])
        && !truth(&c["deleted"])
        && !["stopping", "stopped", "deleting", "deleted"].contains(&s(&c["status"]))
        && run["phase"] != "not_started"
}
pub fn references(data: &Value, entry: &Value) -> bool {
    ["sandbox_id", "pool_sandbox_id", "owned_sandbox_id"]
        .iter()
        .any(|k| data[*k] == entry["sandbox_id"])
        || (truth(&entry["workspace_id"]) && data["workspace_id"] == entry["workspace_id"])
}
pub fn live_reference(row: &Value, entry: &Value) -> bool {
    if runs(row).iter().any(|r| live(r) && references(r, entry)) {
        return true;
    }
    let d = &row["data"];
    let c = &d["cleanup"];
    let retiring = truth(&c["deleted"])
        || truth(&c["stopped"])
        || ["stopping", "stopped", "deleting", "deleted"].contains(&s(&c["status"]))
        || (truth(&c["agent_id"]) && !truth(&c["errors"]));
    references(d, entry) && (active(&row["state"]) || (truth(&d["agent_id"]) && !retiring))
}
pub fn placements(rows: &[Value], entry: &Value) -> i64 {
    let mut count = 0;
    for row in rows {
        let mut groups: Vec<BTreeSet<(String, String)>> = vec![];
        let mut matched = false;
        let mut pending = vec![row["data"].clone()];
        pending.extend(runs(row));
        while let Some(d) = pending.pop() {
            if !d.is_object() {
                continue;
            }
            pending.extend(list(&d["execution_history"]));
            pending.extend(list(&d["sandbox_failure_history"]));
            if !references(&d, entry) {
                continue;
            }
            matched = true;
            let mut ids: BTreeSet<_> = ["agent_id", "create_key", "operation_id"]
                .iter()
                .filter(|k| truth(&d[**k]))
                .map(|k| ((*k).into(), s(&d[*k]).into()))
                .collect();
            if truth(&d["cleanup"]["agent_id"]) {
                ids.insert(("agent_id".into(), s(&d["cleanup"]["agent_id"]).into()));
            }
            if ids.is_empty() {
                continue;
            }
            while let Some(i) = groups.iter().position(|g| !g.is_disjoint(&ids)) {
                ids.extend(groups.remove(i));
            }
            groups.push(ids);
        }
        count += if groups.is_empty() {
            i64::from(matched)
        } else {
            groups.len() as i64
        };
    }
    count
}
fn ledger_agents(rows: &[Value]) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for r in rows {
        let d = &r["data"];
        let mut history = vec![d.clone()];
        for k in [
            "execution_history",
            "sandbox_failure_history",
            "admission_history",
        ] {
            history.extend(list(&d[k]));
        }
        history.push(d["credit_recovery"].clone());
        history.extend(runs(r));
        for d in history {
            for a in [&d["agent_id"], &d["cleanup"]["agent_id"]] {
                if truth(a) {
                    found.insert(s(a).into());
                }
            }
        }
    }
    found
}
/// The serialized state is an offline restart boundary, never a live ledger.
pub struct SandboxPool {
    pub runtime: Value,
}
impl SandboxPool {
    pub fn restored(runtime: Value) -> Self {
        Self { runtime }
    }
    pub fn new(
        replay: &mut Replay<'_>,
        config: &Value,
        kinds: &Value,
        correspondence: Value,
        reserve: i64,
        idle: f64,
    ) -> StageResult<Self> {
        let max = count(&config["max_sandboxes"]);
        let agents = count(&config["agents_per_sandbox"]);
        require(max > 0, "sandbox_pool.max_sandboxes must be positive")?;
        require(
            agents > 0 && agents <= 16,
            "sandbox_pool.agents_per_sandbox must be between 1 and 16",
        )?;
        for k in ["rotate_after_agents", "quota_blocked_max_agents_placed"] {
            require(
                count(&default(config, k, 1.into())) > 0,
                format!("sandbox_pool.{k} must be a positive integer"),
            )?;
        }
        require(
            idle.is_finite() && idle >= 0.,
            "Invalid pool idle pause seconds",
        )?;
        let (limits, kg) = group_config(config, kinds)?;
        let mut pool = Self {
            runtime: object([
                ("correspondence", correspondence),
                ("max_sandboxes", max.into()),
                ("agents_per_sandbox", agents.into()),
                (
                    "reserve_review_slots",
                    reserve.min((agents - 1).max(0)).into(),
                ),
                ("group_limits", limits),
                ("kind_groups", kg),
                ("idle_seconds", outreach_canon::float(idle)?),
                ("entries", Value::Array(list(&replay.state("sandbox_pool")))),
                (
                    "failed",
                    Value::Array(list(&replay.state("sandbox_pool_cleanup"))),
                ),
            ]),
        };
        for key in ["entries", "failed"] {
            for e in pool.runtime[key].as_array_mut().into_iter().flatten() {
                if e.get("agents_placed").is_none() {
                    e["agents_placed"] = placements(replay.rows("tasks"), e).into();
                }
                remove(e, "draining");
            }
        }
        if truth(&pool.runtime["group_limits"]) {
            pool.migrate(replay)?;
        }
        pool.write(replay)?;
        for (k, v) in [
            ("available", Value::Array(vec![])),
            ("observed", object([])),
            ("attempted", Value::Array(vec![])),
            ("errors", Value::Array(vec![])),
            ("agent_cleanup_remaining", 8.into()),
        ] {
            pool.runtime[k] = v;
        }
        Ok(pool)
    }
    fn entries(&self) -> &[Value] {
        array(&self.runtime["entries"])
    }
    fn failed(&self) -> &[Value] {
        array(&self.runtime["failed"])
    }
    fn grouped(&self) -> bool {
        truth(&self.runtime["group_limits"])
    }
    fn write(&self, r: &mut Replay<'_>) -> StageResult<()> {
        r.set_state("sandbox_pool", self.runtime["entries"].clone())?;
        r.set_state("sandbox_pool_cleanup", self.runtime["failed"].clone())?;
        Ok(())
    }
    fn entry(&self, id: &str) -> Value {
        self.entries()
            .iter()
            .chain(self.failed())
            .find(|e| e["sandbox_id"] == id)
            .cloned()
            .unwrap_or(Value::Null)
    }
    fn put(&mut self, e: Value) {
        for key in ["entries", "failed"] {
            if let Some(a) = self.runtime[key].as_array_mut()
                && let Some(i) = a.iter().position(|x| x["sandbox_id"] == e["sandbox_id"])
            {
                a[i] = e;
                return;
            }
        }
    }
    fn in_pool(&self, id: &str) -> bool {
        self.entries().iter().any(|e| e["sandbox_id"] == id)
    }
    fn in_failed(&self, id: &str) -> bool {
        self.failed().iter().any(|e| e["sandbox_id"] == id)
    }
    pub fn group(&self, row: &Value) -> Value {
        if !self.grouped() || row.is_null() {
            return Value::Null;
        }
        if row["state"] == "queued" {
            let kind = if array(&self.runtime["correspondence"]).contains(&row["id"])
                || self.runtime["correspondence"].get(s(&row["id"])).is_some()
            {
                "reply"
            } else {
                s(&row["kind"])
            };
            self.runtime["kind_groups"][kind].clone()
        } else if truth(&row["data"]["pool_group"]) {
            row["data"]["pool_group"].clone()
        } else {
            self.runtime["kind_groups"][s(&row["kind"])].clone()
        }
    }
    fn migrate(&mut self, r: &mut Replay<'_>) -> StageResult<()> {
        let limits = self.runtime["group_limits"].clone();
        let names: Vec<_> = limits
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, _)| k.clone())
            .collect();
        let mut untagged: Vec<_> = self
            .entries()
            .iter()
            .filter(|e| !truth(&e["group"]))
            .cloned()
            .collect();
        untagged.sort_by_key(|e| (count(&e["agents_placed"]), s(&e["sandbox_id"]).to_owned()));
        let counts: Vec<_> = names
            .iter()
            .map(|n| {
                self.entries()
                    .iter()
                    .filter(|e| e["group"] == n.as_str())
                    .count() as i64
            })
            .collect();
        let mut destinations: Vec<_> = names
            .iter()
            .zip(&counts)
            .filter(|(_, c)| **c == 0)
            .map(|(n, _)| n.clone())
            .collect();
        for (n, c) in names.iter().zip(&counts) {
            for _ in 0..(count(&limits[n]) - (*c).max(1)).max(0) {
                destinations.push(n.clone());
            }
        }
        for (e, n) in untagged.iter_mut().zip(&destinations) {
            e["group"] = string(n);
            self.put(e.clone());
        }
        let mut remaining: Vec<_> = untagged.into_iter().skip(destinations.len()).collect();
        remaining.extend(self.failed().to_vec());
        for mut e in remaining {
            if !truth(&e["group"]) {
                let group = r
                    .rows("tasks")
                    .iter()
                    .find(|row| row["id"] == e["created_by_task"])
                    .map(|row| self.group(row))
                    .filter(truth)
                    .unwrap_or_else(|| string(&names[0]));
                e["group"] = group;
                self.put(e);
            }
        }
        let capacity = r.state("sandbox_pool_capacity");
        if capacity.get("capacity_blocked_at").is_some() {
            let mut groups = object([]);
            for n in names {
                groups[&n] = capacity["capacity_blocked_at"].clone();
            }
            r.set_state("sandbox_pool_capacity", object([("groups", groups)]))?;
        }
        Ok(())
    }
    pub fn creation_waiting(&self, row: &Value, now: f64) -> bool {
        active(&row["state"])
            && truth(&row["data"]["pool_create"])
            && !truth(&row["data"]["workspace_id"])
            && now
                - num(&default(
                    &row["data"],
                    "pool_create_started_at",
                    row["updated"].clone(),
                ))
                < POOL_WAIT_SECONDS
    }
    pub fn creation_stalled(&self, row: &Value, now: f64) -> bool {
        active(&row["state"])
            && truth(&row["data"]["pool_create"])
            && !truth(&row["data"]["workspace_id"])
            && now
                - num(&default(
                    &row["data"],
                    "pool_create_started_at",
                    row["updated"].clone(),
                ))
                >= POOL_WAIT_SECONDS
    }
    pub fn capacity_blocked(&self, r: &Replay<'_>, rows: &[Value], group: &Value) -> bool {
        let capacity = r.state("sandbox_pool_capacity");
        let at = if self.grouped() {
            &capacity["groups"][s(group)]
        } else {
            &capacity["capacity_blocked_at"]
        };
        let elapsed = r.now - num(at);
        (!at.is_null() && (0. ..CAPACITY_BLOCK_SECONDS).contains(&elapsed))
            || rows.iter().any(|row| {
                self.group(row) == *group
                    && (self.creation_stalled(row, r.now)
                        || (self.creation_waiting(row, r.now)
                            && count(&row["data"]["capacity_retries"]) > 0))
            })
    }
    pub fn capacity_failure(
        &self,
        r: &mut Replay<'_>,
        row: &Value,
        clear: bool,
    ) -> StageResult<()> {
        let mut cap = r.state("sandbox_pool_capacity");
        if !cap.is_object() {
            cap = object([]);
        }
        if !self.grouped() {
            cap = if clear {
                object([])
            } else {
                object([("capacity_blocked_at", r.clock_value.clone())])
            };
        } else {
            let group = self.group(row);
            if group.is_null() {
                return Ok(());
            }
            if !cap["groups"].is_object() {
                cap["groups"] = object([]);
            }
            if clear {
                if cap["groups"].get(s(&group)).is_none() {
                    return Ok(());
                }
                remove(&mut cap["groups"], s(&group));
            } else {
                cap["groups"][s(&group)] = r.clock_value.clone();
            }
        }
        r.set_state("sandbox_pool_capacity", cap)?;
        Ok(())
    }
    fn discard_available(&mut self, id: &str) {
        if let Some(a) = self.runtime["available"].as_array_mut() {
            a.retain(|v| *v != id);
        }
        remove(&mut self.runtime["observed"], id);
    }
    fn available(&mut self, id: &str) {
        let a = self.runtime["available"]
            .as_array_mut()
            .expect("pool available set");
        if !a.iter().any(|v| v == id) {
            a.push(string(id));
            a.sort_by(|a, b| s(a).cmp(s(b)));
        }
    }
    pub fn failed_entry(
        &mut self,
        r: &mut Replay<'_>,
        ports: &mut impl DeliveryPorts,
        id: &str,
        failure: PoolFailure,
    ) -> StageResult<Value> {
        let PoolFailure {
            workspace,
            corrupt,
            rotated,
            stalled,
            group,
        } = failure;
        let mut e = self.entry(id);
        if e.is_null() {
            e = object([("sandbox_id", string(id)), ("workspace_id", workspace)]);
        }
        e["state"] = string(if truth(&rotated) {
            "retiring"
        } else {
            "failed"
        });
        if self.grouped() && !truth(&e["group"]) {
            e["group"] = if truth(&group) {
                group
            } else {
                string(
                    self.runtime["group_limits"]
                        .as_object()
                        .expect("group map")
                        .keys()
                        .next()
                        .expect("group"),
                )
            };
        }
        for (k, v) in [
            ("corrupt", corrupt),
            ("rotated", rotated),
            ("stalled", stalled),
        ] {
            if truth(&v) {
                e[k] = v;
            }
        }
        self.runtime["entries"]
            .as_array_mut()
            .expect("entries")
            .retain(|x| x["sandbox_id"] != id);
        self.discard_available(id);
        r.set_state("sandbox_pool", self.runtime["entries"].clone())?;
        if let Some(i) = self.failed().iter().position(|x| x["sandbox_id"] == id) {
            self.runtime["failed"][i] = e.clone();
        } else {
            self.runtime["failed"]
                .as_array_mut()
                .expect("failed")
                .push(e.clone());
        }
        r.set_state("sandbox_pool_cleanup", self.runtime["failed"].clone())?;
        ports.save(&r.tables)?;
        Ok(e)
    }
    fn observe_live(&mut self, r: &mut Replay<'_>, rows: &[Value]) -> StageResult<()> {
        for e in self.runtime["entries"].as_array_mut().into_iter().flatten() {
            if rows.iter().any(|row| live_reference(row, e)) || e.get("idle_since").is_none() {
                e["idle_since"] = r.clock_value.clone();
            }
        }
        r.set_state("sandbox_pool", self.runtime["entries"].clone())?;
        Ok(())
    }
    fn last_healthy(&self, e: &Value) -> bool {
        if !self.grouped() {
            return false;
        }
        for peer in self.entries() {
            if peer["sandbox_id"] == e["sandbox_id"] {
                return true;
            }
            if peer["group"] == e["group"]
                && HEALTHY.contains(&s(&peer["state"]))
                && HEALTHY.contains(&s(
                    &self.runtime["observed"][s(&peer["sandbox_id"])]["status"],
                ))
            {
                return false;
            }
        }
        true
    }
    fn busy(&self, r: &Replay<'_>, rows: &[Value], group: &Value) -> StageResult<bool> {
        for row in rows.iter().filter(|row| self.group(row) == *group) {
            if row["state"] == "queued" || active(&row["state"]) {
                return Ok(true);
            }
            let d = &row["data"];
            let effect = [
                "sent_receipt",
                "draft_receipt",
                "draft_intent",
                "delivery_attempt",
                "mathesis_delivery",
                "publication_receipt",
                "handoff_receipt",
                "submission_receipt",
                "stale_draft_conflict",
            ]
            .iter()
            .any(|k| truth(&d[*k]))
                || d["publication_loop"]["phase"] == "queued"
                || truth(&d["publication_loop"]["request"])
                || r.rows("attempts").iter().any(|a| a["item"] == row["id"]);
            if crate::reviewable(row)? && !effect {
                return Ok(true);
            }
            if !effect
                && [
                    "approved",
                    "awaiting-mailbox",
                    "owner-draft",
                    "handoff-ready",
                    "submission-ready",
                ]
                .contains(&s(&row["state"]))
                && crate::transitions::review_current(row, &row["request"]["workflow_hash"])?
                && !crate::reviews::matches_workflow(
                    row,
                    &(r.environment.workflow)(s(&row["kind"]))?,
                )
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn rotate_empty(
        &mut self,
        r: &mut Replay<'_>,
        ports: &mut impl DeliveryPorts,
        id: &str,
        rows: &[Value],
    ) -> StageResult<bool> {
        let mut e = self.entry(id);
        if rows.iter().any(|row| live_reference(row, &e)) {
            return Ok(false);
        }
        let group = if self.grouped() {
            e["group"].clone()
        } else {
            Value::Null
        };
        if !group.is_null() && self.runtime["group_limits"].get(s(&group)).is_none() {
            self.failed_entry(
                r,
                ports,
                id,
                PoolFailure {
                    workspace: e["workspace_id"].clone(),
                    corrupt: Value::Null,
                    rotated: string("group-removed"),
                    stalled: Value::Null,
                    group: Value::Null,
                },
            )?;
            return Ok(true);
        }
        if self.last_healthy(&e) {
            return Ok(false);
        }
        if self.busy(r, rows, &group)? {
            e["idle_since"] = r.clock_value.clone();
            self.put(e);
            return Ok(false);
        }
        if e["state"] == "active" && !truth(&e["intent"]) && self.capacity_blocked(r, rows, &group)
        {
            return Ok(false);
        }
        let reason = match s(&e["state"]) {
            "paused" => "paused",
            "active" | "pausing" => "idle",
            _ => return Ok(false),
        };
        if r.now - num(&e["idle_since"]) < num(&self.runtime["idle_seconds"]) {
            return Ok(false);
        }
        self.failed_entry(
            r,
            ports,
            id,
            PoolFailure {
                workspace: e["workspace_id"].clone(),
                corrupt: Value::Null,
                rotated: string(reason),
                stalled: Value::Null,
                group: Value::Null,
            },
        )?;
        Ok(true)
    }
    fn error(&mut self, r: &mut Replay<'_>, id: &str, e: &StageError) -> StageResult<()> {
        self.runtime["errors"]
            .as_array_mut()
            .expect("errors")
            .push(object([
                ("sandbox_id", string(id)),
                (
                    "error",
                    string(format!(
                        "{}: {}",
                        s(&e.summary["type"]),
                        error_text(e).chars().take(300).collect::<String>()
                    )),
                ),
            ]));
        r.set_state("sandbox_pool_errors", self.runtime["errors"].clone())?;
        Ok(())
    }
    fn cleanup_entry(
        &mut self,
        r: &mut Replay<'_>,
        cma: &mut impl CmaPool,
        ports: &mut impl DeliveryPorts,
        id: &str,
        status: &Value,
        rows: &[Value],
    ) -> StageResult<()> {
        let mut e = self.entry(id);
        let c = &e["cleanup"];
        let unresolved = array(&c["agent_cleanup"])
            .iter()
            .any(|a| !truth(&a["deleted"]));
        let dead = (status == "retired" && unresolved)
            || status == "failed"
            || (truth(&e["corrupt"]) && status == "active")
            || ((truth(&e["stalled"]) || truth(&e["rotated"])) && status != "retired")
            || (self.in_failed(id)
                && ["failed", "retiring"].contains(&s(&e["state"]))
                && status != "retired");
        if dead && !rows.iter().any(|row| live_reference(row, &e)) {
            if !ports.allow("pool-health", id) {
                return Ok(());
            }
            if e.get("cleanup_intent").is_none() {
                e["cleanup_intent"] =
                    object([("key", string(format!("outreach-pool-retire-{id}")))]);
            }
            self.put(e.clone());
            r.set_state("sandbox_pool_cleanup", self.runtime["failed"].clone())?;
            let budget = count(&self.runtime["agent_cleanup_remaining"]);
            let key = format!(
                "{}{}",
                s(&e["cleanup_intent"]["key"]),
                if truth(&e["operator_check_acknowledged"]) {
                    format!("-ack-{}", s(&e["operator_check_acknowledged"]))
                } else {
                    String::new()
                }
            );
            let intent = RetirementIntent {
                sandbox: id.into(),
                key,
                previous: if truth(&e["cleanup"]) {
                    e["cleanup"].clone()
                } else {
                    object([])
                },
                finished: ledger_agents(rows),
                task_ids: rows.iter().map(|r| s(&r["id"]).into()).collect(),
                max_agent_deletions: budget,
                now: r.clock_value.clone(),
            };
            e["cleanup"] = cma.retire_sandbox(Persisted::save(intent, ports, &r.tables)?)?;
            if truth(&e["cleanup"]["sandbox_transport_error"]) {
                let detail = default(
                    &e["cleanup"]["sandbox_transport_error"],
                    "error",
                    string("transport failure"),
                );
                self.runtime["errors"]
                    .as_array_mut()
                    .expect("errors")
                    .push(object([
                        ("sandbox_id", string(id)),
                        (
                            "error",
                            string(s(&detail).chars().take(300).collect::<String>()),
                        ),
                    ]));
                r.set_state("sandbox_pool_errors", self.runtime["errors"].clone())?;
            }
            let processed = e["cleanup"]
                .get("_agent_deletions")
                .map(count)
                .unwrap_or_else(|| budget.min(array(&e["cleanup"]["agent_cleanup"]).len() as i64));
            remove(&mut e["cleanup"], "_agent_deletions");
            self.runtime["agent_cleanup_remaining"] = (budget - processed).max(0).into();
            if e["cleanup"].get("agent_sweep_version").is_none() {
                e["cleanup"]["agent_sweep_version"] = 1.into();
            }
            e["cleanup_checked_at"] = r.clock_value.clone();
            remove(&mut e, "cleanup_error");
            let mut checks = list(&r.state("sandbox_pool_operator_checks"));
            checks.retain(|x| x["sandbox_id"] != id);
            if truth(&e["cleanup"]["operator_check"]) {
                e["operator_check"] = e["cleanup"]["operator_check"].clone();
                checks.push(object([
                    ("sandbox_id", string(id)),
                    ("check", e["operator_check"].clone()),
                ]));
            } else {
                remove(&mut e, "operator_check");
            }
            r.set_state("sandbox_pool_operator_checks", Value::Array(checks))?;
            self.put(e);
        } else if !["failed", "retiring"].contains(&s(status))
            && !dead
            && !truth(&c["operator_check"])
        {
            r.set_state(&format!("sandbox_retired_{id}"), e)?;
            self.runtime["failed"]
                .as_array_mut()
                .expect("failed")
                .retain(|e| e["sandbox_id"] != id);
        }
        Ok(())
    }
    pub fn health(
        &mut self,
        r: &mut Replay<'_>,
        cma: &mut impl CmaPool,
        ports: &mut impl DeliveryPorts,
        rows: &[Value],
    ) -> StageResult<()> {
        self.runtime["available"] = Value::Array(vec![]);
        self.runtime["observed"] = object([]);
        self.runtime["errors"] = Value::Array(vec![]);
        self.runtime["agent_cleanup_remaining"] = 8.into();
        self.observe_live(r, rows)?;
        let mut ids: Vec<String> = vec![];
        for e in self.entries().iter().chain(self.failed()) {
            let id = s(&e["sandbox_id"]).to_owned();
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        for id in ids {
            if !ports.allow("pool-health", &id) {
                continue;
            }
            let e = self.entry(&id);
            let c = &e["cleanup"];
            let recent = r.now - num(&e["cleanup_checked_at"]) < RETAINED_RECHECK_SECONDS;
            if self.in_failed(&id)
                && recent
                && ((truth(&e["operator_check"]) || truth(&c["operator_check"]))
                    || (truth(&c["reason"])
                        && !truth(&c["sandbox_retired"])
                        && ![
                            "owned_agents_pending",
                            "operator_check",
                            "sandbox_delete_requested",
                            "agent_inventory_pending",
                        ]
                        .iter()
                        .any(|k| truth(&c[*k]))
                        && c["agent_sweep_version"] == 1
                        && !truth(&e["cleanup_error"])))
            {
                continue;
            }
            let mut status = Value::Null;
            let result = (|| -> StageResult<()> {
                let sandbox = cma.sandbox(&id)?;
                status = sandbox["status"].clone();
                if ["retired", "retiring", "failed"].contains(&s(&status))
                    || (truth(&sandbox["lifecycle_interruption"])
                        && !["paused", "pausing", "reviving"].contains(&s(&status)))
                {
                    if status == "failed" {
                        self.failed_entry(
                            r,
                            ports,
                            &id,
                            PoolFailure {
                                workspace: e["workspace_id"].clone(),
                                corrupt: Value::Null,
                                rotated: Value::Null,
                                stalled: Value::Null,
                                group: Value::Null,
                            },
                        )?;
                    } else {
                        self.runtime["entries"]
                            .as_array_mut()
                            .expect("entries")
                            .retain(|x| x["sandbox_id"] != id);
                        self.discard_available(&id);
                        r.set_state("sandbox_pool", self.runtime["entries"].clone())?;
                    }
                    r.set_state("sandbox_pool_cleanup", self.runtime["failed"].clone())?;
                    ports.save(&r.tables)?;
                } else if self.in_pool(&id) {
                    self.runtime["observed"][&id] = sandbox;
                    let mut e = self.entry(&id);
                    let mut intent = e["intent"].clone();
                    if (intent["action"] == "pause" && status == "paused")
                        || (intent["action"] == "revive" && status == "active")
                    {
                        remove(&mut e, "intent");
                        intent = object([]);
                    }
                    e["state"] = if truth(&intent) {
                        string(if intent["action"] == "pause" {
                            "pausing"
                        } else {
                            "reviving"
                        })
                    } else {
                        status.clone()
                    };
                    if HEALTHY.contains(&s(&e["state"])) {
                        remove(&mut e, "unhealthy_since");
                    } else if e.get("unhealthy_since").is_none() {
                        e["unhealthy_since"] = r.clock_value.clone();
                    }
                    self.put(e.clone());
                    if !HEALTHY.contains(&s(&e["state"]))
                        && !truth(&intent)
                        && r.now - num(&e["unhealthy_since"]) >= RETIRE_STALLED_SECONDS
                        && !rows.iter().any(|row| live_reference(row, &e))
                    {
                        e["stalled"] = status.clone();
                        self.put(e.clone());
                        self.failed_entry(
                            r,
                            ports,
                            &id,
                            PoolFailure {
                                workspace: e["workspace_id"].clone(),
                                corrupt: Value::Null,
                                rotated: Value::Null,
                                stalled: Value::Null,
                                group: Value::Null,
                            },
                        )?;
                    }
                    if self.in_pool(&id) {
                        self.rotate_empty(r, ports, &id, rows)?;
                    }
                    if self.in_pool(&id) && status == "active" && !truth(&intent) {
                        self.available(&id);
                    }
                }
                if self.in_failed(&id) {
                    self.cleanup_entry(r, cma, ports, &id, &status, rows)?;
                }
                Ok(())
            })();
            if let Err(error) = result {
                if error.is_interruption() {
                    return Err(error);
                }
                self.runtime["available"]
                    .as_array_mut()
                    .expect("available")
                    .retain(|v| *v != id.as_str());
                if self.in_failed(&id) {
                    let mut e = self.entry(&id);
                    if !e["cleanup"].is_object() {
                        e["cleanup"] = object([]);
                    }
                    if status == "retiring" {
                        e["cleanup"]["sandbox_delete_requested"] = true.into();
                    }
                    e["cleanup_error"] = string(format!(
                        "{}: {}",
                        s(&error.summary["type"]),
                        error_text(&error).chars().take(300).collect::<String>()
                    ));
                    self.put(e);
                }
                self.error(r, &id, &error)?;
            }
        }
        self.write(r)?;
        r.set_state("sandbox_pool_errors", self.runtime["errors"].clone())?;
        ports.save(&r.tables)?;
        Ok(())
    }
    pub fn transition(
        &mut self,
        r: &mut Replay<'_>,
        cma: &mut impl CmaPool,
        ports: &mut impl DeliveryPorts,
        id: &str,
        action: &str,
    ) -> StageResult<()> {
        let mut e = self.entry(id);
        let sandbox = self.runtime["observed"][id].clone();
        if array(&self.runtime["attempted"]).iter().any(|v| v == id)
            || sandbox["lifecycle"][action]["allowed"] == false
        {
            return Ok(());
        }
        let mut intent = e["intent"].clone();
        if truth(&intent) && intent["action"] != action {
            return Ok(());
        }
        if action == "pause" && r.rows("tasks").iter().any(|row| live_reference(row, &e)) {
            return Ok(());
        }
        if !ports.allow("pool-health", id) {
            return Ok(());
        }
        self.runtime["attempted"]
            .as_array_mut()
            .expect("attempted")
            .push(string(id));
        self.runtime["attempted"]
            .as_array_mut()
            .expect("attempted")
            .sort_by(|a, b| s(a).cmp(s(b)));
        if !truth(&intent) {
            let n = count(&e["lifecycle_sequence"]) + 1;
            e["lifecycle_sequence"] = n.into();
            intent = object([
                ("action", string(action)),
                ("key", string(format!("outreach-pool-{action}-{id}-{n}"))),
            ]);
            e["intent"] = intent.clone();
        }
        e["state"] = string(if action == "pause" {
            "pausing"
        } else {
            "reviving"
        });
        self.put(e.clone());
        self.runtime["available"]
            .as_array_mut()
            .expect("available")
            .retain(|v| *v != id);
        r.set_state("sandbox_pool", self.runtime["entries"].clone())?;
        let permit = Persisted::save(
            LifecycleIntent {
                sandbox: id.into(),
                action: action.into(),
                key: s(&intent["key"]).into(),
            },
            ports,
            &r.tables,
        )?;
        match cma.lifecycle(permit) {
            Ok(result) => {
                if action == "pause" && result["status"] == "paused" {
                    e["state"] = string("paused");
                    remove(&mut e, "intent");
                }
            }
            Err(error) => {
                if error.is_interruption() {
                    return Err(error);
                }
                if error.status() == Some(409)
                    && [
                        "sandbox_lifecycle_busy",
                        "sandbox_not_active",
                        "sandbox_not_paused",
                    ]
                    .iter()
                    .any(|x| (error_text(&error) + s(&error.summary["body_text"])).contains(x))
                {
                    remove(&mut e, "intent");
                    e["state"] = sandbox["status"].clone();
                }
                self.error(r, id, &error)?;
            }
        }
        self.put(e);
        r.set_state("sandbox_pool", self.runtime["entries"].clone())?;
        ports.save(&r.tables)?;
        Ok(())
    }
    pub fn idle(
        &mut self,
        r: &mut Replay<'_>,
        cma: &mut impl CmaPool,
        ports: &mut impl DeliveryPorts,
        rows: &[Value],
    ) -> StageResult<()> {
        self.observe_live(r, rows)?;
        let ids: Vec<_> = self
            .entries()
            .iter()
            .map(|e| s(&e["sandbox_id"]).to_owned())
            .collect();
        for id in ids {
            let sandbox = self.runtime["observed"][&id].clone();
            if truth(&sandbox)
                && self.rotate_empty(r, ports, &id, rows)?
                && let Err(error) = self.cleanup_entry(r, cma, ports, &id, &sandbox["status"], rows)
            {
                if error.is_interruption() {
                    return Err(error);
                }
                let mut e = self.entry(&id);
                e["cleanup_error"] = string(format!(
                    "{}: {}",
                    s(&error.summary["type"]),
                    error_text(&error).chars().take(300).collect::<String>()
                ));
                self.put(e);
                self.error(r, &id, &error)?;
            }
        }
        self.write(r)?;
        ports.save(&r.tables)?;
        Ok(())
    }
    pub fn admission_limit(&self, e: &Value, rows: &[Value]) -> i64 {
        let occupied = rows
            .iter()
            .filter(|row| runs(row).iter().any(|run| live(run) && references(run, e)))
            .count() as i64;
        let agents = count(&self.runtime["agents_per_sandbox"]);
        agents.min(agents - count(&self.runtime["reserve_review_slots"]) + occupied)
    }
    pub fn slot(
        &mut self,
        r: &mut Replay<'_>,
        cma: Option<&mut impl CmaPool>,
        ports: &mut impl DeliveryPorts,
        row: &Value,
        rows: &[Value],
    ) -> StageResult<Value> {
        let group = self.group(row);
        if self.grouped() && self.runtime["group_limits"].get(s(&group)).is_none() {
            return Ok(Value::Array(vec![false.into(), Value::Null]));
        }
        let entries: Vec<_> = self
            .entries()
            .iter()
            .filter(|e| !self.grouped() || e["group"] == group)
            .cloned()
            .collect();
        let load = |e: &Value| rows.iter().filter(|row| live_reference(row, e)).count() as i64;
        let mut candidates: Vec<_> = entries
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                array(&self.runtime["available"]).contains(&e["sandbox_id"])
                    && load(e) < self.admission_limit(e, rows)
            })
            .collect();
        candidates.sort_by_key(|(i, e)| (load(e), *i));
        if let Some((_, e)) = candidates.first() {
            return Ok(Value::Array(vec![true.into(), (*e).clone()]));
        }
        let pending: Vec<_> = rows
            .iter()
            .filter(|row| self.creation_waiting(row, r.now))
            .collect();
        let waiting = pending.iter().any(|row| self.group(row) == group);
        let unknown = entries.iter().any(|e| {
            self.runtime["observed"].get(s(&e["sandbox_id"])).is_none()
                || (!HEALTHY.contains(&s(&e["state"]))
                    && r.now - num(&default(e, "unhealthy_since", r.clock_value.clone()))
                        < STALLED_SECONDS)
        });
        let pooled: BTreeSet<_> = self
            .entries()
            .iter()
            .chain(self.failed())
            .map(|e| s(&e["sandbox_id"]))
            .collect();
        let legacy: BTreeSet<_> = rows
            .iter()
            .filter(|row| {
                let d = &row["data"];
                truth(&d["owned_sandbox_id"])
                    && !pooled.contains(s(&d["owned_sandbox_id"]))
                    && !truth(&d["pool_sandbox_id"])
                    && !truth(&d["cleanup"]["sandbox_retired"])
                    && (active(&row["state"])
                        || (truth(&d["agent_id"]) && !truth(&d["cleanup"]["deleted"])))
            })
            .map(|row| s(&row["data"]["owned_sandbox_id"]))
            .collect();
        let creating = pending
            .iter()
            .filter(|row| {
                !self
                    .entries()
                    .iter()
                    .any(|e| e["sandbox_id"] == row["data"]["sandbox_id"])
            })
            .count();
        let can_create = !waiting
            && (entries.len() as i64)
                < count(&default(
                    &self.runtime["group_limits"],
                    s(&group),
                    self.runtime["max_sandboxes"].clone(),
                ))
            && !unknown
            && ((self.entries().len() + legacy.len() + creating) as i64)
                < count(&self.runtime["max_sandboxes"]);
        for e in entries {
            let id = s(&e["sandbox_id"]);
            if self.runtime["observed"].get(id).is_some()
                && load(&e) < self.admission_limit(&e, rows)
                && ["paused", "pausing", "reviving"].contains(&s(&e["state"]))
            {
                if let Some(cma) = cma
                    && self.runtime["observed"][id]["status"] == "paused"
                {
                    self.transition(r, cma, ports, id, "revive")?;
                }
                return Ok(Value::Array(vec![false.into(), Value::Null]));
            }
        }
        Ok(Value::Array(vec![can_create.into(), Value::Null]))
    }
    pub fn assigned(&mut self, r: &mut Replay<'_>, entry: &Value) -> StageResult<()> {
        let id = s(&entry["sandbox_id"]);
        let mut e = self.entry(id);
        e["agents_placed"] = (count(&e["agents_placed"]) + 1).into();
        self.put(e);
        r.set_state("sandbox_pool", self.runtime["entries"].clone())?;
        Ok(())
    }
    pub fn remember(
        &mut self,
        r: &mut Replay<'_>,
        ports: &mut impl DeliveryPorts,
        id: &str,
        workspace: Value,
        row: &Value,
    ) -> StageResult<()> {
        let mut entry = object([
            ("sandbox_id", string(id)),
            ("workspace_id", workspace),
            ("created_by_task", row["id"].clone()),
            ("state", string("active")),
            ("idle_since", r.clock_value.clone()),
            ("agents_placed", 1.into()),
        ]);
        if self.grouped() {
            let group = self.group(row);
            entry["group"] = if truth(&group) {
                group
            } else {
                string(
                    self.runtime["group_limits"]
                        .as_object()
                        .expect("groups")
                        .keys()
                        .next()
                        .expect("group"),
                )
            };
        }
        if !self.in_pool(id) {
            self.runtime["entries"]
                .as_array_mut()
                .expect("entries")
                .push(entry);
            r.set_state("sandbox_pool", self.runtime["entries"].clone())?;
            ports.save_forced(&r.tables)?;
        }
        self.available(id);
        self.runtime["observed"][id] = object([("status", string("active"))]);
        Ok(())
    }
}
pub(crate) fn error_text(e: &StageError) -> String {
    s(e.summary
        .get("message")
        .or_else(|| e.summary.get("reason"))
        .unwrap_or(&e.summary["type"]))
    .to_owned()
}
pub fn group_config(config: &Value, kinds: &Value) -> StageResult<(Value, Value)> {
    let priority = default(config, "prioritize_correspondence_notes", false.into());
    require(
        priority.is_boolean(),
        "prioritize_correspondence_notes must be a boolean",
    )?;
    if config.get("groups").is_none() {
        require(
            priority != true,
            "Correspondence note priority requires a Sandbox group for reply",
        )?;
        return Ok((object([]), object([])));
    }
    let groups = &config["groups"];
    require(
        groups.is_array() && !array(groups).is_empty(),
        "sandbox_pool.groups must be a nonempty list",
    )?;
    let mut limits = object([]);
    let mut kg = object([]);
    for g in array(groups) {
        require(g.is_object(), "Each sandbox_pool group must be an object")?;
        let name = s(&g["name"]);
        require(
            g["name"].is_string() && !name.trim().is_empty() && limits.get(name).is_none(),
            "Sandbox group names must be nonempty and unique",
        )?;
        require(
            count(&g["max_sandboxes"]) > 0,
            "Sandbox group max_sandboxes must be positive",
        )?;
        require(
            g["kinds"].is_array() && !array(&g["kinds"]).is_empty(),
            "Sandbox group kinds must be a nonempty list",
        )?;
        limits[name] = g["max_sandboxes"].clone();
        for kind in array(&g["kinds"]) {
            let kind = s(kind);
            require(
                [
                    "firstcontact",
                    "reply",
                    "research",
                    "shortnote",
                    "daily",
                    "mathesis",
                    "submission",
                ]
                .contains(&kind),
                "Sandbox group kinds must be known",
            )?;
            require(
                kg.get(kind).is_none(),
                format!("Task kind belongs to multiple Sandbox groups: {kind}"),
            )?;
            kg[kind] = string(name);
        }
    }
    let mut missing: Vec<_> = kinds
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(k, v)| truth(v) && kg.get(*k).is_none())
        .map(|(k, _)| k.as_str())
        .collect();
    missing.sort();
    require(
        missing.is_empty(),
        format!(
            "Enabled task kinds missing from Sandbox groups: {}",
            missing.join(", ")
        ),
    )?;
    require(
        limits
            .as_object()
            .into_iter()
            .flatten()
            .map(|(_, v)| count(v))
            .sum::<i64>()
            <= count(&config["max_sandboxes"]),
        "Sandbox group limits exceed sandbox_pool.max_sandboxes",
    )?;
    require(
        priority != true || kg.get("reply").is_some(),
        "Correspondence note priority requires a Sandbox group for reply",
    )?;
    Ok((limits, kg))
}
