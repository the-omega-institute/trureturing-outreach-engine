//! Offline ingestion replay. All mutations are to this owned in-memory model.
use crate::{
    envelopes,
    transitions::{self, Change, Context},
    validation::*,
    *,
};
use outreach_canon::{
    canonical, decode_input, digest, input_identity, python_eq, sha256, verify_envelope,
};
use std::collections::BTreeSet;

pub const INPUT_PREFIXES: [&str; 12] = [
    "request-",
    "local-result-",
    "submission-review-",
    "review-",
    "decision-",
    "draft-revision-",
    "mathesis-",
    "prompt-requeue-",
    "failed-requeue-",
    "pool-adopt-",
    "formalization-backfill-",
    "prior-work-backfill-",
];
pub struct InputAsset {
    pub metadata: Value,
    pub raw: Vec<u8>,
}
pub struct Replay<'a> {
    pub tables: Value,
    undo: Option<Vec<Undo>>,
    pub generation: i64,
    pub now: f64,
    pub saved_version: Option<String>,
    pub environment: &'a Environment<'a>,
    /// Read-only artifact port. Implementations must use local recorded bytes.
    pub resource: &'a dyn Fn(&Value) -> Result<Vec<u8>>,
}
enum Undo {
    Append(&'static str, usize),
    Replace(&'static str, usize, Value),
}
impl<'a> Replay<'a> {
    pub fn new(
        tables: Value,
        generation: i64,
        now: f64,
        environment: &'a Environment<'a>,
        resource: &'a dyn Fn(&Value) -> Result<Vec<u8>>,
    ) -> Self {
        Self {
            tables,
            generation,
            now,
            saved_version: None,
            environment,
            resource,
            undo: None,
        }
    }
    fn push_row(&mut self, table: &'static str, value: Value) -> Result<()> {
        let len = self.rows(table).len();
        if let Some(undo) = &mut self.undo {
            undo.push(Undo::Append(table, len));
        }
        self.rows_mut(table)?.push(value);
        Ok(())
    }
    fn replace_row(&mut self, table: &'static str, index: usize, value: Value) -> Result<()> {
        let previous = std::mem::replace(&mut self.rows_mut(table)?[index], value);
        if let Some(undo) = &mut self.undo {
            undo.push(Undo::Replace(table, index, previous));
        }
        Ok(())
    }
    /// Match Tasks.restore's existing migrations on this owned copy only.
    pub fn restore(&mut self) -> Result<()> {
        for (name, columns, pk) in [
            (
                "tasks",
                vec![
                    "id",
                    "request",
                    "request_hash",
                    "owner",
                    "kind",
                    "state",
                    "revision",
                    "data",
                    "updated",
                ],
                vec!["id"],
            ),
            (
                "task_events",
                vec!["seq", "id", "state", "revision", "at", "data"],
                vec!["seq"],
            ),
            ("control_events", vec!["id", "payload"], vec!["id"]),
            (
                "input_asset_receipts",
                vec!["identity", "generation"],
                vec!["identity"],
            ),
            (
                "pipeline_sources",
                vec!["result_id", "identity", "payload", "baseline", "task_id"],
                vec!["result_id"],
            ),
            ("pipeline_state", vec!["key", "payload"], vec!["key"]),
            ("task_migrations", vec!["name", "phase"], vec!["name"]),
            ("sqlite_sequence", vec!["name", "seq"], vec![]),
        ] {
            if self.tables.get(name).is_none() {
                self.tables[name] = object([
                    (
                        "columns",
                        Value::Array(columns.into_iter().map(string).collect()),
                    ),
                    (
                        "primary_key",
                        Value::Array(pk.into_iter().map(string).collect()),
                    ),
                    ("rows", Value::Array(vec![])),
                ]);
            }
        }
        let marker = self
            .rows("task_migrations")
            .iter()
            .position(|r| r["name"] == "compact-task-events-v1");
        if marker.is_none() {
            for e in self.rows_mut("task_events")? {
                e["data"] = event_summary(&e["data"]);
                if let Some(o) = e.as_object_mut() {
                    o.shift_remove("_replay_raw_data");
                }
            }
            self.rows_mut("task_migrations")?.push(object([
                ("name", string("compact-task-events-v1")),
                ("phase", string("done")),
            ]));
        } else if let Some(index) = marker {
            self.rows_mut("task_migrations")?[index]["phase"] = string("done");
        }
        Ok(())
    }
    pub fn rows(&self, table: &str) -> &[Value] {
        array(&self.tables[table]["rows"])
    }
    fn rows_mut(&mut self, table: &str) -> Result<&mut Vec<Value>> {
        self.tables[table]["rows"]
            .as_array_mut()
            .ok_or_else(|| Error::new(format!("Missing replay table: {table}")))
    }
    fn task(&self, id: &Value) -> Result<Value> {
        let row = self
            .rows("tasks")
            .iter()
            .find(|r| r["id"] == *id)
            .cloned()
            .ok_or_else(|| Error::new("Unknown task"))?;
        require(
            row["data"].is_object(),
            "Invalid task data: expected an object",
        )?;
        Ok(row)
    }
    fn seen(&self, event: &str) -> bool {
        self.rows("control_events").iter().any(|r| r["id"] == event)
    }
    fn control(&mut self, v: &Value) -> Result<()> {
        self.push_row(
            "control_events",
            object([("id", string(digest(v)?)), ("payload", v.clone())]),
        )
    }
    fn state(&self, key: &str) -> Value {
        self.rows("pipeline_state")
            .iter()
            .find(|r| r["key"] == key)
            .map(|r| r["payload"].clone())
            .unwrap_or(Value::Null)
    }
    fn set_state(&mut self, key: &str, value: Value) -> Result<()> {
        let value = object([("key", string(key)), ("payload", value)]);
        if let Some(index) = self
            .rows("pipeline_state")
            .iter()
            .position(|r| r["key"] == key)
        {
            self.replace_row("pipeline_state", index, value)
        } else {
            self.push_row("pipeline_state", value)
        }
    }
    fn event(&mut self, row: &Value, data: Value) -> Result<()> {
        let previous = self
            .rows("sqlite_sequence")
            .iter()
            .find(|r| r["name"] == "task_events")
            .and_then(|r| r["seq"].as_i64())
            .unwrap_or(0);
        let seq = increment(
            previous.max(
                self.rows("task_events")
                    .iter()
                    .filter_map(|r| r["seq"].as_i64())
                    .max()
                    .unwrap_or(0),
            ),
        )?;
        let now = outreach_canon::float(self.now)?;
        self.push_row(
            "task_events",
            object([
                ("seq", seq.into()),
                ("id", row["id"].clone()),
                ("state", row["state"].clone()),
                ("revision", row["revision"].clone()),
                ("at", now),
                ("data", data),
            ]),
        )?;
        if self.tables.get("sqlite_sequence").is_some() {
            let value = object([("name", string("task_events")), ("seq", seq.into())]);
            if let Some(index) = self
                .rows("sqlite_sequence")
                .iter()
                .position(|r| r["name"] == "task_events")
            {
                self.replace_row("sqlite_sequence", index, value)?;
            } else {
                self.push_row("sqlite_sequence", value)?;
            }
        }
        Ok(())
    }
    fn apply_change(&mut self, c: Change) -> Result<()> {
        if let Some((a, raw)) = &c.preserved {
            require(sha256(raw) == a["sha256"], "Artifact checksum mismatch")?;
        }
        self.event(&c.row, c.event_data)?;
        let index = self
            .rows("tasks")
            .iter()
            .position(|r| r["id"] == c.row["id"])
            .ok_or_else(|| Error::new("Unknown task"))?;
        self.replace_row("tasks", index, c.row)
    }
    fn update(&mut self, row: &Value, state: &str, data: Value) -> Result<()> {
        let summary = event_summary(&data);
        let mut row = row.clone();
        row["revision"] = increment(
            row["revision"]
                .as_i64()
                .ok_or_else(|| Error::new("Invalid revision"))?,
        )?
        .into();
        row["state"] = string(state);
        row["data"] = data;
        row["updated"] = outreach_canon::float(self.now)?;
        self.apply_change(Change {
            row,
            event_data: summary,
            preserved: None,
        })
    }
    fn context(&self) -> Context<'_> {
        Context {
            rows: self.rows("tasks"),
            events: self.rows("task_events"),
            attempts: self.rows("attempts"),
            environment: self.environment,
            now: self.now,
        }
    }
    pub fn apply(&mut self, family: &str, v: &Value, secret: &[u8]) -> Result<()> {
        // Journal only changed rows and appended lengths. A refused batch
        // restores prior values in reverse order without copying whole tables.
        self.undo = Some(Vec::new());
        let result = self.apply_inner(family, v, secret);
        let undo = self
            .undo
            .take()
            .ok_or_else(|| Error::new("Missing replay transaction"))?;
        if result.is_err() {
            for entry in undo.into_iter().rev() {
                match entry {
                    Undo::Append(table, len) => self.rows_mut(table)?.truncate(len),
                    Undo::Replace(table, index, value) => self.rows_mut(table)?[index] = value,
                }
            }
        }
        result
    }
    fn apply_inner(&mut self, family: &str, v: &Value, secret: &[u8]) -> Result<()> {
        match family {
            "request-" => self.add(v),
            "review-" | "decision-" | "draft-revision-" => {
                match family {
                    "review-" => envelopes::validate_review(v, secret)?,
                    "decision-" => envelopes::validate_decision(v, secret)?,
                    _ => envelopes::validate_draft_revision(v, secret)?,
                };
                let event = digest(v)?;
                let id = if family == "review-" {
                    lookup(&v["binding"], "task_id")?
                } else {
                    &v["task_id"]
                };
                if self.seen(&event) {
                    self.task(id)?;
                    return Ok(());
                }
                let row = self.task(id)?;
                let change = match family {
                    "review-" => transitions::verdict(&row, v, secret, &self.context())?,
                    "decision-" => transitions::decision(&row, v, secret, self.now)?,
                    _ => transitions::draft_revision(&row, v, secret, self.now)?,
                };
                self.apply_change(change)?;
                self.control(v)
            }
            "submission-review-" => self.submission_review(v, secret),
            "prompt-requeue-" => self.prompt_requeue(v),
            "failed-requeue-" => self.failed_requeue(v, secret),
            "pool-adopt-" => self.pool_adopt(v, secret),
            "formalization-backfill-" | "prior-work-backfill-" => self.backfill(family, v, secret),
            "local-result-" => self.local_result(v, secret),
            "mathesis-" => self.mathesis(v, secret),
            _ => Err(Error::new("Unknown input family")),
        }
    }
    fn add(&mut self, v: &Value) -> Result<()> {
        request(v)?;
        crate::results::firstcontact_gate(v, self.environment)?;
        self.uncovered(v)?;
        if let Some(old) = self.rows("tasks").iter().find(|r| r["id"] == v["id"]) {
            return require(
                old["request_hash"] == digest(v)?,
                "Task ID already has different inputs; use a new revision ID",
            );
        }
        let row = object([
            ("id", v["id"].clone()),
            ("request", v.clone()),
            ("request_hash", string(digest(v)?)),
            ("owner", v["owner"].clone()),
            ("kind", v["kind"].clone()),
            ("state", string("queued")),
            ("revision", 1.into()),
            ("data", object([])),
            ("updated", outreach_canon::float(self.now)?),
        ]);
        self.push_row("tasks", row.clone())?;
        self.event(&row, object([]))?;
        self.supersede(&row)
    }
    fn uncovered(&self, v: &Value) -> Result<()> {
        if v["kind"] != "reply" || v["inputs"]["thread_ref"]["inbound"] != true {
            return Ok(());
        }
        if self
            .rows("tasks")
            .iter()
            .any(|r| r["id"] == v["id"] && r["request_hash"] == digest(v).unwrap_or_default())
        {
            return Ok(());
        }
        for id in array(&v["inputs"]["thread_ref"]["internetMessageIds"]) {
            let Some(identity) = message_id(id) else {
                continue;
            };
            for row in self.rows("tasks") {
                if row["kind"] != "reply"
                    || row["id"] == v["id"]
                    || ![
                        "provisioning",
                        "admitting",
                        "running",
                        "admission-unknown",
                        "provision-unknown",
                        "credit-wait",
                        "queued",
                        "awaiting-review",
                        "bounced",
                        "approved",
                        "awaiting-mailbox",
                    ]
                    .contains(&s(&row["state"]))
                {
                    continue;
                }
                let mut ids =
                    array(&row["request"]["inputs"]["thread_ref"]["internetMessageIds"]).to_vec();
                ids.extend([
                    row["request"]["inputs"]["latest_inbound_message_id"].clone(),
                    row["data"]["thread_inputs"]["latest_inbound_message_id"].clone(),
                ]);
                require(
                    !ids.iter().filter_map(message_id).any(|i| i == identity),
                    format!(
                        "Inbound message {identity} is already covered by task {} ({})",
                        s(&row["id"]),
                        s(&row["state"])
                    ),
                )?;
            }
        }
        Ok(())
    }
    fn supersede(&mut self, row: &Value) -> Result<()> {
        if row["kind"] != "reply" {
            return Ok(());
        }
        let mut inherited = Vec::new();
        for old in self.rows("tasks").to_vec() {
            let i = &old["request"]["inputs"];
            let answers = array(&i["user_answers"]);
            let decision_continuation =
                !answers.is_empty() || i["automation_parent"]["purpose"] == "user-answer";
            let eligible = ["owner-draft", "revising"].contains(&s(&old["state"]))
                || (decision_continuation
                    && [
                        "queued",
                        "held",
                        "awaiting-review",
                        "approved",
                        "awaiting-mailbox",
                        "decision-recorded",
                        "bounced",
                    ]
                    .contains(&s(&old["state"])));
            if eligible && !truth(&old["data"]["sent_receipt"]) && newer_reply(row, &old) {
                inherited.extend_from_slice(answers);
                if !truth(&old["data"]["continuation_id"]) {
                    let mut data = old["data"].clone();
                    data["superseded_by"] = row["id"].clone();
                    self.update(&old, "superseded", data)?;
                }
            }
        }
        if !inherited.is_empty() {
            self.carry_answers(row, &inherited)?;
        }
        Ok(())
    }
    fn carry_answers(&mut self, row: &Value, answers: &[Value]) -> Result<()> {
        let i = &row["request"]["inputs"];
        let existing = array(&i["user_answers"]);
        let recorded = array(&row["data"]["carried_user_answers"]);
        let key = |a: &Value| -> Result<String> {
            if a.is_object() && truth(&a["evidence_ref"]) {
                Ok(format!("evidence_ref:{}", canonical(&a["evidence_ref"])?))
            } else {
                Ok(format!("answer:{}", canonical(a)?))
            }
        };
        let mut keys = existing
            .iter()
            .chain(recorded)
            .map(key)
            .collect::<Result<BTreeSet<_>>>()?;
        let mut inherited = Vec::new();
        for a in answers {
            if keys.insert(key(a)?) {
                inherited.push(a.clone());
            }
        }
        if !["queued", "held", "credit-wait"].contains(&s(&row["state"])) || !unstarted(row) {
            if !inherited.is_empty() {
                let mut data = row["data"].clone();
                let mut all = recorded.to_vec();
                all.extend(inherited);
                data["carried_user_answers"] = Value::Array(all);
                self.update(row, s(&row["state"]), data)?;
            }
            return Ok(());
        }
        if inherited.is_empty() && i["user_decision_ever"] == true {
            return Ok(());
        }
        let mut request = row["request"].clone();
        let mut all = existing.to_vec();
        all.extend(inherited);
        request["inputs"]["user_answers"] = Value::Array(all);
        request["inputs"]["user_decision_ever"] = true.into();
        let outcome = if row["kind"] == "reply" {
            "reply"
        } else {
            "research task"
        };
        let instruction = format!(
            "Recorded user answers are decided. Treat those questions as settled; do not ask the owner again, and use the answers to complete the {outcome}."
        );
        let previous = request["inputs"]
            .get("instructions")
            .map(py_str)
            .unwrap_or_default();
        if !previous.contains(&instruction) {
            request["inputs"]["instructions"] = string(
                format!(
                    "{}\n{instruction}",
                    previous.trim_end_matches(crate::text::whitespace)
                )
                .trim_matches(crate::text::whitespace),
            );
        }
        crate::validation::request(&request)?;
        let mut row = row.clone();
        row["request_hash"] = string(digest(&request)?);
        row["request"] = request;
        row["revision"] = increment(row["revision"].as_i64().unwrap_or(0))?.into();
        row["updated"] = outreach_canon::float(self.now)?;
        self.apply_change(Change {
            row,
            event_data: object([("decision_answers_carried", true.into())]),
            preserved: None,
        })
    }
    fn submission_review(&mut self, v: &Value, key: &[u8]) -> Result<()> {
        require(
            !key.is_empty(),
            "Reviewer verification key is not configured",
        )?;
        require(
            keys(
                v,
                &[
                    "schema_version",
                    "paper_id",
                    "task_id",
                    "result_hash",
                    "signature",
                ],
                &[],
            ) && one(&v["schema_version"]),
            "Invalid submission review request",
        )?;
        signature_type(v, false, "Submission review request signature failed")?;
        require(
            verify_envelope(v, key)?,
            "Submission review request signature failed",
        )?;
        if self.seen(&digest(v)?) {
            self.task(&v["task_id"])?;
            return Ok(());
        }
        let row = self
            .rows("tasks")
            .iter()
            .rev()
            .find(|r| {
                r["kind"] == "submission"
                    && r["state"] == "submission-ready"
                    && r["request"]["inputs"]
                        .get("submission_round")
                        .is_none_or(|v| v == "finalize")
                    && r["data"]["result"]["outcome"] == "completed"
                    && r["data"]["awaiting_operator_review"] == true
                    && r["request"]["inputs"]["paper"]["id"] == v["paper_id"]
            })
            .cloned()
            .ok_or_else(|| Error::new("No unreviewed submission-ready finalize task for paper"))?;
        require(
            row["id"] == v["task_id"] && v["result_hash"] == digest(&row["data"]["result"])?,
            "Submission review request targets an obsolete result",
        )?;
        let mut data = row["data"].clone();
        data["awaiting_operator_review"] = false.into();
        data["submission_review_requested"] = true.into();
        self.update(&row, "awaiting-review", data)?;
        self.control(v)
    }
    fn restart(&mut self, row: &Value, v: &Value, prefix: &str) -> Result<()> {
        let mut history = array(&row["data"]["execution_history"]).to_vec();
        history.push(row["data"].clone());
        self.update(
            row,
            "queued",
            object([
                ("execution_history", Value::Array(history)),
                (
                    "execution_restart",
                    string(format!("{prefix}{}", &digest(v)?[..16])),
                ),
            ]),
        )?;
        self.control(v)
    }
    fn prompt_requeue(&mut self, v: &Value) -> Result<()> {
        if self.seen(&digest(v)?) {
            return Ok(());
        }
        let row = self.task(&v["task_id"])?;
        require(
            row["request_hash"] == v["request_hash"],
            "Requeue request hash changed",
        )?;
        let d = &row["data"];
        let r = &d["recovery"];
        let class = r.get("error_class").unwrap_or(&d["error_class"]);
        let exhausted = (["rate-limit", "network", "sandbox-failed"].contains(&s(class))
            && r["status"] == "needs-operator")
            || (!truth(&d["response_id"])
                && py_str(&d["operation_error"])
                    .to_lowercase()
                    .contains("sandbox is not active"));
        require(
            row["state"] == "failed" && (class == "request-too-large" || exhausted),
            "Only failed request-too-large or retry-exhausted transient tasks may be requeued",
        )?;
        self.restart(
            &row,
            v,
            if exhausted {
                "operator-"
            } else {
                "prompt-offload-"
            },
        )
    }
    fn failed_requeue(&mut self, v: &Value, key: &[u8]) -> Result<()> {
        require(
            !key.is_empty(),
            "Recovery verification key is not configured",
        )?;
        require(
            keys(
                v,
                &[
                    "schema_version",
                    "task_id",
                    "request_hash",
                    "failed_revision",
                    "reason",
                    "signature",
                ],
                &[],
            ) && one(&v["schema_version"]),
            "Invalid failed-task recovery request",
        )?;
        signature_type(v, true, "Recovery request signature failed")?;
        require(
            verify_envelope(v, key)?,
            "Recovery request signature failed",
        )?;
        require(nonempty(&v["reason"]), "Recovery request needs a reason")?;
        if self.seen(&digest(v)?) {
            return Ok(());
        }
        let row = self.task(&v["task_id"])?;
        require(
            row["request_hash"] == v["request_hash"],
            "Requeue request hash changed",
        )?;
        require(
            row["state"] == "failed",
            "Only failed tasks may be requeued",
        )?;
        require(
            integer(&v["failed_revision"]) && python_eq(&row["revision"], &v["failed_revision"]),
            "Failed task revision changed",
        )?;
        let effects: Vec<_> = [
            "draft_intent",
            "draft_receipt",
            "sent_receipt",
            "mathesis_delivery",
            "publication_receipt",
        ]
        .into_iter()
        .filter(|k| truth(&row["data"][k]))
        .collect();
        require(
            effects.is_empty(),
            format!(
                "Failed task already has external effects; reconcile them instead: {}",
                effects.join(", ")
            ),
        )?;
        self.restart(&row, v, "operator-")
    }
    fn backfill(&mut self, family: &str, v: &Value, key: &[u8]) -> Result<()> {
        let prior = family == "prior-work-backfill-";
        require(
            keys(
                v,
                if prior {
                    &["schema_version", "purpose", "notes", "signature"]
                } else {
                    &["schema_version", "notes", "signature"]
                },
                &[],
            ) && one(&v["schema_version"])
                && (!prior || v["purpose"] == "prior-work-backfill"),
            if prior {
                "Invalid prior-work backfill"
            } else {
                "Invalid formalization backfill"
            },
        )?;
        sources(&v["notes"])?;
        // The Python signer projects each link before computing the HMAC.
        if !key.is_empty() {
            signature_type(
                v,
                true,
                if prior {
                    "Prior-work backfill signature failed"
                } else {
                    "Formalization backfill signature failed"
                },
            )?;
        }
        let mut normalized = v.clone();
        normalized["schema_version"] = 1.into();
        require(
            verify_envelope(&normalized, key)?,
            if prior {
                "Prior-work backfill signature failed"
            } else {
                "Formalization backfill signature failed"
            },
        )?;
        if self.seen(&digest(v)?) {
            return Ok(());
        }
        for link in array(&v["notes"]) {
            let row = self.task(&link["task_id"])?;
            if !transitions::passed(&row)? || link["result_hash"] != digest(&row["data"]["result"])?
            {
                continue;
            }
            if prior {
                if ["approved", "handoff-ready", "verified"].contains(&s(&row["state"])) {
                    self.set_state(
                        &format!("prior-work-backfill:{}", s(&row["id"])),
                        link["result_hash"].clone(),
                    )?;
                }
            } else if !truth(&row["data"]["formalization_requests"]) {
                let requests = pending_requests(&row["data"]["result"])?;
                if truth(&requests) {
                    let mut data = row["data"].clone();
                    data["formalization_requests"] = requests;
                    self.update(&row, s(&row["state"]), data)?;
                }
            }
        }
        self.control(v)
    }
    fn pool_adopt(&mut self, v: &Value, key: &[u8]) -> Result<()> {
        require(
            !key.is_empty(),
            "Recovery verification key is not configured",
        )?;
        require(
            keys(
                v,
                &[
                    "schema_version",
                    "sandbox_id",
                    "workspace_id",
                    "group",
                    "reason",
                    "signature",
                ],
                &[],
            ) && one(&v["schema_version"]),
            "Invalid pool adoption request",
        )?;
        signature_type(v, true, "Pool adoption signature failed")?;
        require(verify_envelope(v, key)?, "Pool adoption signature failed")?;
        require(
            ["sandbox_id", "workspace_id", "group", "reason"]
                .iter()
                .all(|k| nonempty(&v[k])),
            "Pool adoption needs sandbox, workspace, group and reason",
        )?;
        require(
            s(&v["sandbox_id"]).starts_with("sbx_") && s(&v["workspace_id"]).starts_with("wks_"),
            "Invalid CMA identifiers",
        )?;
        require(
            array(&self.environment.config["sandbox_pool"]["groups"])
                .iter()
                .any(|g| g["name"] == v["group"]),
            format!("Unknown pool group {}", s(&v["group"])),
        )?;
        let event = digest(v)?;
        if self.seen(&event) {
            return Ok(());
        }
        let mut entries = array(&self.state("sandbox_pool")).to_vec();
        require(
            !entries.iter().any(|e| e["sandbox_id"] == v["sandbox_id"]),
            "Sandbox is already in the pool",
        )?;
        let mut cleanup = array(&self.state("sandbox_pool_cleanup")).to_vec();
        let mut entry = object([
            ("sandbox_id", v["sandbox_id"].clone()),
            ("workspace_id", v["workspace_id"].clone()),
            (
                "created_by_task",
                string(format!("operator-adopt-{}", &event[..16])),
            ),
            ("state", string("active")),
            ("idle_since", outreach_canon::float(self.now)?),
            ("agents_placed", 0.into()),
            ("group", v["group"].clone()),
        ]);
        if let Some(index) = cleanup
            .iter()
            .position(|e| e["sandbox_id"] == v["sandbox_id"])
        {
            let prior = &cleanup[index];
            let refusal = if !truth(&prior["rotated"]) || prior["state"] != "retiring" {
                Some("not a rotated Sandbox")
            } else if truth(&prior["corrupt"]) || truth(&prior["stalled"]) {
                Some("corrupt or stalled")
            } else if prior["cleanup"]["sandbox_retired"] != false
                || !truth(&prior["cleanup"]["reason"])
                || truth(&prior["cleanup_error"])
            {
                Some("CMA has not retained it")
            } else if self.rows("tasks").iter().any(|r| live_reference(r, prior)) {
                Some("a live Agent of ours references it")
            } else if !prior["workspace_id"].is_null() && prior["workspace_id"] != v["workspace_id"]
            {
                Some("workspace differs")
            } else {
                None
            };
            require(
                refusal.is_none(),
                format!(
                    "Sandbox is on the cleanup list: {}",
                    refusal.unwrap_or("None")
                ),
            )?;
            entry["agents_placed"] = prior.get("agents_placed").cloned().unwrap_or(0.into());
            entry["readopted"] = object([
                ("rotated", prior["rotated"].clone()),
                ("cleanup_reason", prior["cleanup"]["reason"].clone()),
            ]);
            cleanup.remove(index);
            self.set_state("sandbox_pool_cleanup", Value::Array(cleanup))?;
        }
        entries.push(entry);
        self.set_state("sandbox_pool", Value::Array(entries))?;
        self.control(v)
    }
    fn local_result(&mut self, v: &Value, key: &[u8]) -> Result<()> {
        let fields = [
            "schema_version",
            "task_id",
            "request_hash",
            "task_revision",
            "executor",
            "session_id",
            "result",
            "signature",
            "context",
        ];
        require(
            keys(v, &fields, &[])
                || keys(
                    v,
                    &[fields.as_slice(), &["takeover", "failed_execution"]].concat(),
                    &[],
                ),
            "Invalid local result envelope",
        )?;
        let takeover = truth(&v["takeover"]);
        require(
            if takeover {
                v["takeover"] == true && v["failed_execution"].is_object()
            } else {
                v.get("takeover").is_none() && v.get("failed_execution").is_none()
            },
            "Invalid local takeover binding",
        )?;
        require(
            !key.is_empty(),
            "Local result verification key is not configured",
        )?;
        signature_type(v, true, "Local result signature failed")?;
        require(verify_envelope(v, key)?, "Local result signature failed")?;
        require(
            one(&v["schema_version"])
                && v["executor"] == "local-codex"
                && nonempty(&v["session_id"]),
            "Invalid local executor/session identity",
        )?;
        require(
            v["task_id"].is_string()
                && v["request_hash"].is_string()
                && integer(&v["task_revision"])
                && v["task_revision"].as_i64().is_some_and(|n| n > 0),
            "Invalid local task identity/revision",
        )?;
        if self.seen(&digest(v)?) {
            self.task(&v["task_id"])?;
            return Ok(());
        }
        let row = self.task(&v["task_id"])?;
        require(
            row["request_hash"] == v["request_hash"],
            "Local result request hash is stale",
        )?;
        require(
            takeover || python_eq(&row["revision"], &v["task_revision"]),
            "Local result task revision is stale",
        )?;
        if takeover {
            require(
                row["state"] == "failed"
                    && row["data"]["recovery"]["suggested_exit"] == "local-codex",
                "Local takeover admission rejected: task is no longer eligible",
            )?;
            let mut identity = object([]);
            for k in [
                "agent_id",
                "operation_id",
                "response_id",
                "create_key",
                "command_key",
            ] {
                if let Some(v) = row["data"].get(k) {
                    identity[k] = v.clone();
                }
            }
            require(
                python_eq(&identity, &v["failed_execution"]),
                "Local takeover failed execution identity is stale",
            )?;
            require(
                !truth(&row["data"]["agent_id"])
                    || truth(&row["data"]["cleanup"]["deleted"])
                    || truth(&row["data"]["cleanup"]["stopped"]),
                "Local takeover admission rejected: original CMA Agent may still execute",
            )?;
        } else {
            require(
                ["queued", "credit-wait"].contains(&s(&row["state"])) && unstarted(&row),
                "Local result admission rejected: task already started or has a result",
            )?;
        }
        let context = &v["context"];
        require(
            keys(context, &[], &["thread_inputs", "mailbox_history"]),
            "Invalid local execution context",
        )?;
        require(
            context.as_object().into_iter().flatten().all(|(k, v)| {
                v.is_object() && row["data"].get(k).is_none_or(|old| python_eq(v, old))
            }),
            "Local context cannot replace coordinator evidence",
        )?;
        let mut candidate = row.clone();
        for (k, v) in context.as_object().into_iter().flatten() {
            candidate["data"][k] = v.clone();
        }
        crate::results::validate(&v["result"], &candidate, self.environment)?;
        crate::gates::execution(
            &candidate,
            &v["result"],
            self.rows("tasks"),
            self.environment,
        )?;
        require(
            array(&v["result"]["artifacts"])
                .iter()
                .all(|a| a.get("agent_path").is_none()),
            "Local result needs preserved Release artifacts",
        )?;
        for a in array(&v["result"]["artifacts"]) {
            let raw = (self.resource)(a)?;
            require(
                a["bytes"].as_u64() == Some(raw.len() as u64) && a["sha256"] == sha256(&raw),
                "Artifact readback failed",
            )?;
        }
        let mut data = candidate["data"].clone();
        data["result"] = v["result"].clone();
        data["executor"] = string("local-codex");
        data["local_session_id"] = v["session_id"].clone();
        if takeover {
            data["local_takeover"] = true.into();
            data["failed_execution"] = v["failed_execution"].clone();
        }
        self.update(
            &row,
            if v["result"]["outcome"] == "hold" {
                "held"
            } else {
                "awaiting-review"
            },
            data,
        )?;
        self.control(v)
    }
    fn mathesis(&mut self, v: &Value, key: &[u8]) -> Result<()> {
        envelopes::validate_mathesis(v, key)?;
        let row = self.task(&v["task_id"])?;
        envelopes::mathesis_reviewed(&row)?;
        require(
            v["task_id"] == row["id"]
                && v["result_hash"] == digest(&row["data"]["result"])?
                && v["workflow_hash"] == row["request"]["workflow_hash"]
                && array(&row["data"]["result"]["artifacts"])
                    .iter()
                    .any(|a| python_eq(a, &v["source"])),
            "The delivery package differs from the reviewed task",
        )?;
        crate::gates::note_revisions(&row, self.rows("tasks"), &self.tables, &v["metadata"])?;
        let previous = &row["data"]["mathesis_delivery"];
        if truth(previous) {
            return require(
                python_eq(&previous["envelope"], v),
                "An attempted delivery is immutable; use a new task/version",
            );
        }
        transitions::source_gate(&row["request"], &row["data"]["result"], self.rows("tasks"))?;
        require(
            ["approved", "handoff-ready"].contains(&s(&row["state"])),
            "Mathesis task is not ready for delivery selection",
        )?;
        let mut data = row["data"].clone();
        data["mathesis_delivery"] = object([("phase", string("ready")), ("envelope", v.clone())]);
        self.update(&row, s(&row["state"]), data)
    }
    fn receipt(&mut self, asset: &Value) -> Result<()> {
        let identity = string(input_identity(asset)?);
        let generation = increment(self.generation)?.into();
        if !self
            .rows("input_asset_receipts")
            .iter()
            .any(|r| r["identity"] == identity)
        {
            self.rows_mut("input_asset_receipts")?
                .push(object([("identity", identity), ("generation", generation)]));
        }
        Ok(())
    }
    pub fn ingest(&mut self, assets: &[InputAsset], secret: &[u8]) -> Result<Value> {
        let mut assets: Vec<_> = assets
            .iter()
            .filter(|a| {
                s(&a.metadata["name"]).ends_with(".json")
                    && INPUT_PREFIXES
                        .iter()
                        .any(|p| s(&a.metadata["name"]).starts_with(p))
            })
            .collect();
        let present: BTreeSet<_> = assets
            .iter()
            .map(|a| canonical(&a.metadata["digest"]))
            .collect::<Result<_>>()?;
        let mut rejected = self
            .rows("pipeline_state")
            .iter()
            .find(|r| r["key"] == "ingest_rejected")
            .map(|r| r["payload"].clone())
            .unwrap_or_else(|| object([]));
        let entries = rejected
            .as_object_mut()
            .ok_or_else(|| Error::new("Invalid ingest_rejected: expected an object"))?;
        entries.retain(|sha, _| present.contains(&canonical(&string(sha)).unwrap_or_default()));
        self.rejections(&rejected)?;
        let identities: BTreeSet<_> = assets
            .iter()
            .map(|a| input_identity(&a.metadata))
            .collect::<Result<_>>()?;
        self.rows_mut("input_asset_receipts")?
            .retain(|r| identities.contains(s(&r["identity"])));
        for a in &assets {
            if rejected[s(&a.metadata["digest"])]["attempts"]
                .as_i64()
                .unwrap_or(0)
                >= 3
            {
                self.receipt(&a.metadata)?;
            }
        }
        let known: BTreeSet<_> = self
            .rows("input_asset_receipts")
            .iter()
            .map(|r| s(&r["identity"]).to_owned())
            .collect();
        assets.retain(|a| input_identity(&a.metadata).is_ok_and(|id| !known.contains(&id)));
        assets.sort_by_key(|a| {
            (
                INPUT_PREFIXES
                    .iter()
                    .position(|p| s(&a.metadata["name"]).starts_with(p))
                    .unwrap_or(12),
                s(&a.metadata["name"]),
            )
        });
        let mut failed = BTreeSet::new();
        let mut errors = Vec::new();
        for a in assets {
            let name = s(&a.metadata["name"]);
            let sha = s(&a.metadata["digest"]);
            if failed.contains(sha) {
                continue;
            }
            let family = INPUT_PREFIXES
                .iter()
                .find(|p| name.starts_with(**p))
                .copied()
                .ok_or_else(|| Error::new("Unknown input family"))?;
            let result: Result<bool> = (|| {
                require(
                    sha == format!("sha256:{}", sha256(&a.raw)),
                    "Queue asset mismatch",
                )?;
                let v = decode_input(&a.raw)?;
                if secret.is_empty()
                    && ![
                        "request-",
                        "prompt-requeue-",
                        "failed-requeue-",
                        "pool-adopt-",
                        "local-result-",
                    ]
                    .contains(&family)
                {
                    return Ok(false);
                }
                self.apply(family, &v, secret)?;
                Ok(true)
            })();
            match result {
                Ok(true) => self.receipt(&a.metadata)?,
                Ok(false) => {}
                Err(e) => {
                    let mut error = e.to_string().chars().take(300).collect::<String>();
                    if truth(&a.metadata["digest"]) {
                        failed.insert(sha.to_owned());
                        let attempts = increment(rejected[sha]["attempts"].as_i64().unwrap_or(0))?;
                        rejected[sha] =
                            object([("attempts", attempts.into()), ("error", string(&error))]);
                        self.rejections(&rejected)?;
                        if attempts >= 3 {
                            self.receipt(&a.metadata)?;
                        }
                        if attempts == 3 {
                            error.push_str("; giving up after 3 attempts");
                        }
                    }
                    errors.push(object([("asset", string(name)), ("error", string(error))]));
                }
            }
        }
        // Model the forced save's generation locally. An unchanged replay in
        // this same run does not publish another generation; a fresh run does.
        let version = digest(&object([
            ("events", self.rows("task_events").len().into()),
            ("control_events", self.rows("control_events").len().into()),
            (
                "receipts",
                Value::Array(self.rows("input_asset_receipts").to_vec()),
            ),
            (
                "pipeline_state",
                Value::Array(self.rows("pipeline_state").to_vec()),
            ),
        ]))?;
        if self.saved_version.as_deref() != Some(&version) {
            self.generation = increment(self.generation)?;
            self.saved_version = Some(version);
        }
        Ok(Value::Array(errors))
    }
    fn rejections(&mut self, v: &Value) -> Result<()> {
        if truth(v) {
            self.set_state("ingest_rejected", v.clone())
        } else {
            self.rows_mut("pipeline_state")?
                .retain(|r| r["key"] != "ingest_rejected");
            Ok(())
        }
    }
}
pub fn event_summary(data: &Value) -> Value {
    if (keys(data, &["review"], &[]) && data["review"].is_string())
        || (keys(data, &["decision"], &[]) && data["decision"].is_string())
    {
        return data.clone();
    }
    let mut summary = object([]);
    for k in [
        "reason",
        "last_error",
        "operation_id",
        "agent_id",
        "response_id",
        "create_key",
        "command_key",
        "successor",
        "continuation_id",
    ] {
        if let Some(v) = data[k].as_str() {
            summary[k] = string(v.chars().take(300).collect::<String>());
        }
    }
    for k in [
        "repair_attempts",
        "transport_retries",
        "capacity_retries",
        "credit_retries",
    ] {
        if integer(&data[k]) {
            summary[k] = data[k].clone();
        }
    }
    let verdict = if data["review"].is_object() {
        ["PASS", "BOUNCE", "HOLD"]
            .contains(&s(&data["review"]["verdict"]))
            .then_some(&data["review"]["verdict"])
    } else {
        None
    };
    if let Some(v) = verdict.or_else(|| {
        ["PASS", "BOUNCE", "HOLD"]
            .contains(&s(&data["verdict"]))
            .then_some(&data["verdict"])
    }) {
        summary["verdict"] = v.clone();
    }
    summary
}
pub fn message_id(v: &Value) -> Option<String> {
    v.as_str().filter(|s| s.contains('@')).map(|s| {
        let s = s.trim_matches(crate::text::whitespace);
        if s.starts_with('<') {
            s.into()
        } else {
            format!("<{s}>")
        }
    })
}
pub fn newer_reply(new: &Value, old: &Value) -> bool {
    if new["kind"] != old["kind"] || !["reply", "research"].contains(&s(&old["kind"])) {
        return false;
    }
    let target = |r: &Value| {
        let mut packet = fallback(&[
            &r["data"]["packet_override"],
            &r["data"]["result"]["packet"],
        ])
        .clone();
        if !packet.is_object() {
            packet = object([]);
        }
        for (k, v) in crate::results::effective_inputs(r)
            .as_object()
            .into_iter()
            .flatten()
        {
            packet[k] = v.clone();
        }
        packet
    };
    let latest = target(new);
    let previous = target(old);
    if !truth(&latest["thread_id"])
        || !python_eq(&latest["thread_id"], &previous["thread_id"])
        || !truth(&latest["latest_inbound_message_id"])
        || !truth(&previous["latest_inbound_message_id"])
        || python_eq(
            &latest["latest_inbound_message_id"],
            &previous["latest_inbound_message_id"],
        )
    {
        return false;
    }
    let received = |i: &Value| {
        for m in array(&latest["thread"]).iter().chain(array(&i["thread"])) {
            if python_eq(&m["internetMessageId"], &i["latest_inbound_message_id"])
                && !truth(&m["isDraft"])
                && python_eq(&m["conversationId"], &i["thread_id"])
            {
                return crate::gates::instant_micros(&m["receivedDateTime"]);
            }
        }
        if python_eq(
            &i["mailbox_observation"]["message_id"],
            &i["latest_inbound_message_id"],
        ) {
            crate::gates::instant_micros(&i["mailbox_observation"]["at"])
        } else {
            None
        }
    };
    received(&latest)
        .zip(received(&previous))
        .is_some_and(|(a, b)| a > b)
}
fn live_reference(row: &Value, e: &Value) -> bool {
    let d = &row["data"];
    let c = &d["cleanup"];
    let retiring = truth(&c["deleted"])
        || truth(&c["stopped"])
        || ["stopping", "stopped", "deleting", "deleted"].contains(&s(&c["status"]))
        || (truth(&c["agent_id"]) && !truth(&c["errors"]));
    let references = (truth(&e["sandbox_id"])
        && (d["owned_sandbox_id"] == e["sandbox_id"]
            || d["sandbox_id"] == e["sandbox_id"]
            || d["pool_sandbox_id"] == e["sandbox_id"]))
        || (truth(&e["workspace_id"]) && d["workspace_id"] == e["workspace_id"]);
    references
        && ([
            "provisioning",
            "admitting",
            "running",
            "admission-unknown",
            "provision-unknown",
            "credit-wait",
        ]
        .contains(&s(&row["state"]))
            || (truth(&d["agent_id"]) && !retiring))
}
pub fn unstarted(row: &Value) -> bool {
    let mut keys = vec![
        "agent_id",
        "operation_id",
        "response_id",
        "create_body",
        "create_key",
        "command_key",
        "owned_sandbox_id",
        "response_text",
        "result",
        "review",
    ];
    if !["reply", "research"].contains(&s(&row["kind"]))
        || !truth(&row["data"]["execution_restart"])
    {
        keys.push("prompt");
    }
    !keys.iter().any(|k| row["data"].get(k).is_some())
}
/// Sort rows by declared primary key and replace time values with their global
/// dense rank. Equal times remain equal; every cross-table ordering is checked.
pub fn logical_dump(mut tables: Value) -> Result<Value> {
    if let Some(rows) = tables["task_events"]["rows"].as_array_mut() {
        for r in rows {
            if let Some(o) = r.as_object_mut() {
                o.shift_remove("_replay_raw_data");
            }
        }
    }
    let mut times = Vec::<f64>::new();
    fn visit_times(tables: &mut Value, mut visit: impl FnMut(&mut Value)) {
        for (name, table) in tables.as_object_mut().into_iter().flatten() {
            for row in table["rows"].as_array_mut().into_iter().flatten() {
                // Unknown JSON fields named 'at' are opaque data, not clocks.
                for column in ["at", "updated"] {
                    if let Some(v) = row.get_mut(column) {
                        visit(v);
                    }
                }
                if name == "tasks" {
                    for path in ["/data/reviewer_edit/at", "/data/owner_question_opened_at"] {
                        if let Some(v) = row.pointer_mut(path) {
                            visit(v);
                        }
                    }
                }
                if name == "pipeline_state"
                    && ["sandbox_pool", "sandbox_pool_cleanup"].contains(&s(&row["key"]))
                {
                    for entry in row["payload"].as_array_mut().into_iter().flatten() {
                        for path in ["/idle_since", "/rotated", "/readopted/rotated"] {
                            if let Some(v) = entry.pointer_mut(path) {
                                visit(v);
                            }
                        }
                    }
                }
            }
        }
    }
    visit_times(&mut tables, |v| {
        if let Some(n) = v.as_f64() {
            times.push(n);
        }
    });
    times.sort_by(f64::total_cmp);
    times.dedup();
    visit_times(&mut tables, |v| {
        if let Some(n) = v.as_f64() {
            *v = object([("$time_order", times.partition_point(|t| *t < n).into())]);
        }
    });
    for (_, table) in tables.as_object_mut().into_iter().flatten() {
        let pk: Vec<_> = array(&table["primary_key"])
            .iter()
            .map(|v| s(v).to_owned())
            .collect();
        if let Some(rows) = table["rows"].as_array_mut() {
            rows.sort_by(|a, b| {
                for k in &pk {
                    let order = if let (Some(a), Some(b)) = (a[k].as_i64(), b[k].as_i64()) {
                        a.cmp(&b)
                    } else if let (Some(a), Some(b)) = (a[k].as_str(), b[k].as_str()) {
                        a.cmp(b)
                    } else {
                        canonical(&a[k])
                            .unwrap_or_default()
                            .cmp(&canonical(&b[k]).unwrap_or_default())
                    };
                    if !order.is_eq() {
                        return order;
                    }
                }
                canonical(a)
                    .unwrap_or_default()
                    .cmp(&canonical(b).unwrap_or_default())
            });
        }
    }
    Ok(tables)
}
