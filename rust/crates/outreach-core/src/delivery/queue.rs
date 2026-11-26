//! Transactional queue transitions on the owned replay model only.
use super::*;

pub(crate) fn initialize(r: &mut Replay<'_>) {
    for (name, cols, pk) in [
        (
            "items",
            vec!["id", "state", "version", "kind", "reason"],
            vec!["id"],
        ),
        (
            "versions",
            vec!["item", "version", "hash", "workflow", "packet"],
            vec!["item", "version"],
        ),
        (
            "research",
            vec!["item", "version", "report"],
            vec!["item", "version"],
        ),
        (
            "reviews",
            vec!["item", "version", "envelope"],
            vec!["item", "version"],
        ),
        (
            "policies",
            vec!["kind", "revision", "snapshot"],
            vec!["kind"],
        ),
        (
            "attempts",
            vec![
                "item",
                "key",
                "transport",
                "status",
                "calls",
                "policy_revision",
                "authority",
                "locator",
                "readback",
            ],
            vec!["item"],
        ),
        ("reservations", vec!["token", "item"], vec!["token"]),
        (
            "events",
            vec!["seq", "at", "item", "action", "data"],
            vec!["seq"],
        ),
    ] {
        if r.tables.get(name).is_none() {
            r.tables[name] = object([
                (
                    "columns",
                    Value::Array(cols.into_iter().map(string).collect()),
                ),
                (
                    "primary_key",
                    Value::Array(pk.into_iter().map(string).collect()),
                ),
                ("rows", Value::Array(vec![])),
            ]);
        }
    }
}
fn rows_mut<'a>(r: &'a mut Replay<'_>, name: &str) -> &'a mut Vec<Value> {
    r.tables[name]["rows"].as_array_mut().expect("queue table")
}
fn insert(r: &mut Replay<'_>, table: &str, row: Value) {
    rows_mut(r, table).push(row);
}
fn set(r: &mut Replay<'_>, table: &str, key: &str, id: &Value, row: Value) {
    let rows = rows_mut(r, table);
    if let Some(i) = rows.iter().position(|v| v[key] == *id) {
        rows[i] = row;
    } else {
        rows.push(row);
    }
}
pub(crate) fn attempt(r: &Replay<'_>, id: &Value) -> Value {
    r.rows("attempts")
        .iter()
        .find(|a| a["item"] == *id)
        .cloned()
        .unwrap_or(Value::Null)
}
pub(crate) fn get(r: &Replay<'_>, id: &Value) -> StageResult<Value> {
    let mut row = r
        .rows("items")
        .iter()
        .find(|v| v["id"] == *id)
        .cloned()
        .ok_or_else(|| outreach_canon::Error::new("Unknown item"))?;
    let version = r
        .rows("versions")
        .iter()
        .find(|v| v["item"] == *id && v["version"] == row["version"])
        .ok_or_else(|| outreach_canon::Error::new("Unknown item"))?;
    row["content_hash"] = version["hash"].clone();
    row["workflow_hash"] = version["workflow"].clone();
    row["packet"] = version["packet"].clone();
    Ok(row)
}
fn item_state(r: &mut Replay<'_>, id: &Value, state: &str, reason: Value) {
    let row = rows_mut(r, "items")
        .iter_mut()
        .find(|v| v["id"] == *id)
        .expect("queue item");
    row["state"] = string(state);
    row["reason"] = reason;
}
fn binding(row: &Value) -> Value {
    object([
        ("item_id", row["id"].clone()),
        ("version", row["version"].clone()),
        ("content_hash", row["content_hash"].clone()),
        ("workflow_hash", row["workflow_hash"].clone()),
    ])
}
fn bound(r: &Replay<'_>, id: &Value, workflow: &Value) -> StageResult<Value> {
    let row = get(r, id)?;
    check(row["workflow_hash"] == *workflow, "Workflow policy changed")?;
    check(
        digest(&row["packet"])? == row["content_hash"],
        "Stored packet corruption",
    )?;
    Ok(row)
}
pub(crate) fn events<'a>(
    r: &'a Replay<'_>,
    id: &'a Value,
) -> impl DoubleEndedIterator<Item = &'a Value> {
    r.rows("events").iter().filter(move |e| e["item"] == *id)
}
fn crossed(r: &Replay<'_>, id: &Value) -> bool {
    events(r, id).any(|e| e["action"] == "send-call")
}
fn event(r: &mut Replay<'_>, now: f64, id: &Value, action: &str, data: Value) -> StageResult<()> {
    let now = r.queue_clock_values.pop_front().unwrap_or(now);
    let now = r.queue_event_times.pop_front().unwrap_or(now);
    let seq = r
        .rows("sqlite_sequence")
        .iter()
        .find(|v| v["name"] == "events")
        .and_then(|v| v["seq"].as_i64())
        .unwrap_or(0)
        .max(
            r.rows("events")
                .iter()
                .filter_map(|v| v["seq"].as_i64())
                .max()
                .unwrap_or(0),
        )
        + 1;
    insert(
        r,
        "events",
        object([
            ("seq", seq.into()),
            ("at", outreach_canon::float(now)?),
            ("item", id.clone()),
            ("action", string(action)),
            ("data", data),
        ]),
    );
    set(
        r,
        "sqlite_sequence",
        "name",
        &string("events"),
        object([("name", string("events")), ("seq", seq.into())]),
    );
    Ok(())
}
pub(crate) fn prepare(
    r: &mut Replay<'_>,
    row: &Value,
    packet: &Value,
    workflow: &Value,
    now: f64,
) -> StageResult<()> {
    let id = &row["id"];
    crate::validation::packet(packet)?;
    if !r.rows("items").iter().any(|v| v["id"] == *id) {
        crate::validation::packet(packet)?;
        insert(
            r,
            "items",
            object([
                ("id", id.clone()),
                ("state", string("drafted")),
                ("version", 1.into()),
                ("kind", packet["kind"].clone()),
                ("reason", string("")),
            ]),
        );
        insert(
            r,
            "versions",
            object([
                ("item", id.clone()),
                ("version", 1.into()),
                ("hash", string(digest(packet)?)),
                ("workflow", workflow.clone()),
                ("packet", packet.clone()),
            ]),
        );
        event(r, now, id, "drafted", binding(&get(r, id)?))?;
    } else {
        let current = get(r, id)?;
        check(
            current["content_hash"] == digest(packet)? && current["workflow_hash"] == *workflow,
            "Conflicting intake; create explicit revision",
        )?;
    }
    let current = bound(r, id, workflow)?;
    let b = binding(&current);
    let review = &row["data"]["review"];
    if current["state"] == "drafted" {
        insert(
            r,
            "research",
            object([
                ("item", id.clone()),
                ("version", current["version"].clone()),
                ("report", review["evidence_ref"].clone()),
            ]),
        );
        item_state(r, id, "research-checked", string(""));
        let mut d = b.clone();
        d["report_ref"] = review["evidence_ref"].clone();
        event(r, now, id, "research-checked", d)?;
    }
    if get(r, id)?["state"] == "research-checked" {
        item_state(r, id, "in-review", string(""));
        event(r, now, id, "in-review", b.clone())?;
    }
    if get(r, id)?["state"] == "in-review" {
        let mut envelope = b;
        for (k, v) in [
            ("schema_version", 1.into()),
            ("reviewer", review["reviewer"].clone()),
            ("verdict", string("PASS")),
            ("reason", review["reason"].clone()),
            ("evidence_ref", review["evidence_ref"].clone()),
        ] {
            envelope[k] = v;
        }
        insert(
            r,
            "reviews",
            object([
                ("item", id.clone()),
                ("version", current["version"].clone()),
                ("envelope", envelope.clone()),
            ]),
        );
        item_state(r, id, "approved", review["reason"].clone());
        event(r, now, id, "approved", envelope)?;
    }
    Ok(())
}
pub(crate) fn policy(
    r: &mut Replay<'_>,
    kind: &Value,
    policy: Value,
    now: f64,
) -> StageResult<i64> {
    let fields: BTreeSet<_> = policy
        .as_object()
        .into_iter()
        .flatten()
        .map(|(k, _)| k.as_str())
        .collect();
    check(
        fields
            == ["checked_at", "expires_at", "complete", "paused", "contacts"]
                .into_iter()
                .collect(),
        "Invalid policy fields",
    )?;
    check(
        policy["complete"].is_boolean()
            && policy["paused"].is_boolean()
            && policy["contacts"].is_object(),
        "Invalid policy snapshot",
    )?;
    let now = r.queue_clock_values.pop_front().unwrap_or(now);
    check(
        num(&policy["checked_at"]) <= now && now < num(&policy["expires_at"]),
        "Policy must be fresh",
    )?;
    let revision = r
        .rows("policies")
        .iter()
        .find(|p| p["kind"] == *kind)
        .and_then(|p| p["revision"].as_i64())
        .unwrap_or(0)
        + 1;
    set(
        r,
        "policies",
        "kind",
        kind,
        object([
            ("kind", kind.clone()),
            ("revision", revision.into()),
            ("snapshot", policy.clone()),
        ]),
    );
    event(
        r,
        now,
        &string(format!("policy:{}", s(kind))),
        "policy-updated",
        object([("revision", revision.into()), ("snapshot", policy)]),
    )?;
    Ok(revision)
}
fn outcome(
    r: &mut Replay<'_>,
    now: f64,
    id: &Value,
    state: &str,
    status: &str,
    evidence: String,
) -> StageResult<()> {
    if get(r, id)?["state"] == "verified" {
        return Ok(());
    }
    check(
        ["sending", "send-unknown", "sent"].contains(&s(&get(r, id)?["state"])),
        "Invalid attempt outcome",
    )?;
    item_state(r, id, state, string(&evidence));
    let mut a = attempt(r, id);
    a["status"] = string(status);
    a["locator"] = string(&evidence);
    set(r, "attempts", "item", id, a);
    event(
        r,
        now,
        id,
        state,
        object([("status", string(status)), ("evidence", string(evidence))]),
    )
}
struct Boundary<'a, 'b, P> {
    replay: &'a mut Replay<'b>,
    ports: &'a mut P,
    id: Value,
    key: String,
    now: f64,
}
impl<P: DeliveryPorts> SendBoundary for Boundary<'_, '_, P> {
    fn before_send(&mut self, draft: &str) -> StageResult<()> {
        check(
            get(self.replay, &self.id)?["state"] == "sending",
            "Attempt changed before send",
        )?;
        event(
            self.replay,
            self.now,
            &self.id,
            "send-call",
            object([("key", string(&self.key)), ("draft_id", string(draft))]),
        )?;
        self.ports.save_forced(&self.replay.tables)?;
        Ok(())
    }
}
pub(crate) struct Dispatch<'a> {
    pub row: &'a Value,
    pub workflow: &'a Value,
    pub keys: Vec<String>,
    pub policy: &'a Value,
    pub revision: i64,
    pub now: f64,
}
pub(crate) fn dispatch(
    r: &mut Replay<'_>,
    box_: &mut impl Mailbox,
    ports: &mut impl DeliveryPorts,
    input: Dispatch<'_>,
) -> StageResult<Value> {
    let Dispatch {
        row,
        workflow,
        keys,
        policy,
        revision,
        now,
    } = input;
    let id = &row["id"];
    let current = bound(r, id, workflow)?;
    let packet = current["packet"].clone();
    check(
        ["approved", "not-sent"].contains(&s(&current["state"])),
        "Exact current version is not approved or already attempted",
    )?;
    check(
        r.rows("reviews").iter().any(|v| {
            v["item"] == *id
                && v["version"] == current["version"]
                && v["envelope"]["verdict"] == "PASS"
        }),
        "No exact Claude PASS",
    )?;
    let checked_now = r.queue_clock_values.pop_front().unwrap_or(now);
    check(
        truth(&policy["complete"])
            && !truth(&policy["paused"])
            && num(&policy["checked_at"]) <= checked_now
            && checked_now < num(&policy["expires_at"]),
        "Policy incomplete, stale or paused",
    )?;
    check(
        array(&packet["to"])
            .iter()
            .chain(array(&packet["cc"]))
            .all(|a| policy["contacts"][s(a)] == "clear"),
        "Recipient/account blocked or unknown",
    )?;
    let authority = object([
        ("kind", row["kind"].clone()),
        ("account", packet["sender"].clone()),
        ("expires_at", policy["expires_at"].clone()),
        (
            "evidence_ref",
            string("managed/automation.json:user-authorized-routine-academic-mail"),
        ),
    ]);
    let key = digest(&Value::Array(vec![
        id.clone(),
        current["version"].clone(),
        current["content_hash"].clone(),
    ]))?;
    let old = attempt(r, id);
    let tracked = box_.supports(MailboxFeature::TrackedSend);
    if !old.is_null() {
        check(
            ["rejected", "not-sent"].contains(&s(&old["status"]))
                && old["key"] == key
                && old["transport"] == "microsoft-graph",
            "Uncertain/accepted attempt cannot be repeated or moved",
        )?;
        if old["status"] == "not-sent" {
            check(
                cleaned(r, id, &old["calls"]),
                "Remove the proven unsent attempt draft before retry",
            )?;
        }
        check(
            old["calls"].as_i64().unwrap_or(0) < 3,
            "Submission budget exhausted",
        )?;
        let mut a = old.clone();
        a["status"] = string("sending");
        a["calls"] = (old["calls"].as_i64().unwrap_or(0) + 1).into();
        a["policy_revision"] = revision.into();
        a["authority"] = authority.clone();
        set(r, "attempts", "item", id, a);
    } else {
        let mut tokens: BTreeSet<String> = keys.into_iter().collect();
        if packet["kind"] == "reply" {
            tokens.insert(digest(&Value::Array(vec![
                string("reply"),
                packet["thread_id"].clone(),
                packet["latest_inbound_message_id"].clone(),
            ]))?);
        } else {
            for a in array(&packet["to"]).iter().chain(array(&packet["cc"])) {
                for p in array(&packet["problem_ids"]) {
                    tokens.insert(digest(&Value::Array(vec![
                        string("firstcontact"),
                        a.clone(),
                        p.clone(),
                    ]))?);
                }
            }
        }
        check(
            tokens.iter().all(|t| {
                !r.rows("reservations")
                    .iter()
                    .any(|v| v["token"] == t.as_str())
            }),
            "Duplicate recipient/problem or covered day already reserved",
        )?;
        for t in tokens {
            insert(
                r,
                "reservations",
                object([("token", string(t)), ("item", id.clone())]),
            );
        }
        insert(
            r,
            "attempts",
            object([
                ("item", id.clone()),
                ("key", string(&key)),
                ("transport", string("microsoft-graph")),
                ("status", string("sending")),
                ("calls", 1.into()),
                ("policy_revision", revision.into()),
                ("authority", authority.clone()),
                ("locator", string("")),
                ("readback", string("")),
            ]),
        );
    }
    item_state(r, id, "sending", string(""));
    event(
        r,
        now,
        id,
        "send-intent",
        object([
            ("key", string(&key)),
            ("transport", string("microsoft-graph")),
            ("policy_revision", revision.into()),
            ("authority", authority),
            ("tracks_send_call", tracked.into()),
        ]),
    )?;
    let permit = Persisted::save(
        SendIntent {
            packet,
            key: key.clone(),
        },
        ports,
        &r.tables,
    )?;
    let mut boundary = Boundary {
        replay: r,
        ports,
        id: id.clone(),
        key,
        now,
    };
    match box_.submit(permit, &mut boundary) {
        Ok(receipt) => {
            if s(&receipt["locator"]).trim().is_empty() {
                let pre_send = tracked && !crossed(boundary.replay, id);
                outcome(
                    boundary.replay,
                    now,
                    id,
                    if pre_send { "not-sent" } else { "send-unknown" },
                    if pre_send { "not-sent" } else { "unknown" },
                    "Invalid acceptance; outcome unknown".into(),
                )?;
            } else {
                outcome(
                    boundary.replay,
                    now,
                    id,
                    "sent",
                    "accepted",
                    s(&receipt["locator"]).into(),
                )?;
            }
        }
        Err(e) => {
            if terminal_error(&e) {
                return Err(e);
            }
            let crossed = crossed(boundary.replay, id);
            let typ = s(&e.summary["type"]);
            let (state, status) = if typ == "NotSent" {
                if crossed {
                    ("send-unknown", "unknown")
                } else {
                    ("not-sent", "not-sent")
                }
            } else if typ == "DefiniteRejection" {
                if crossed {
                    ("send-unknown", "unknown")
                } else {
                    ("approved", "rejected")
                }
            } else if tracked && !crossed {
                ("not-sent", "not-sent")
            } else {
                ("send-unknown", "unknown")
            };
            outcome(
                boundary.replay,
                now,
                id,
                state,
                status,
                error_text(&e).chars().take(300).collect(),
            )?;
        }
    }
    get(boundary.replay, id)
}
pub(crate) fn reconcile(
    r: &mut Replay<'_>,
    box_: &mut impl Mailbox,
    ports: &mut impl DeliveryPorts,
    id: &Value,
    now: f64,
) -> StageResult<Value> {
    let row = get(r, id)?;
    if row["state"] == "verified" {
        return Ok(row);
    }
    check(
        ["sending", "send-unknown", "sent", "not-sent"].contains(&s(&row["state"])),
        "No original attempt to reconcile",
    )?;
    let a = attempt(r, id);
    let read = box_.observe(MailboxRead::Lookup {
        packet: row["packet"].clone(),
        key: s(&a["key"]).into(),
    })?;
    if read.is_null() {
        let intent = events(r, id)
            .rfind(|e| e["action"] == "send-intent")
            .cloned()
            .unwrap_or(Value::Null);
        let crossed = crossed(r, id);
        let mut evidence = Value::Null;
        if !crossed && ["sending", "send-unknown"].contains(&s(&row["state"])) {
            if intent["data"]["tracks_send_call"] == true {
                evidence = string(
                    "Draft preparation interrupted before /send; no send-call boundary recorded",
                );
            } else if row["state"] == "send-unknown" && a["locator"] == "GateError" {
                evidence = box_.observe(MailboxRead::LegacyNotSent {
                    packet: row["packet"].clone(),
                    key: s(&a["key"]).into(),
                })?;
            }
        }
        if truth(&evidence) {
            outcome(
                r,
                now,
                id,
                "not-sent",
                "not-sent",
                s(&evidence).chars().take(300).collect(),
            )?;
            ports.save_forced(&r.tables)?;
            return get(r, id);
        }
        if row["state"] == "sending" {
            outcome(
                r,
                now,
                id,
                "send-unknown",
                "unknown",
                "No readback; original outcome unknown".into(),
            )?;
        }
        return get(r, id);
    }
    check(
        read["idempotency_key"] == a["key"]
            && read["content_hash"] == row["content_hash"]
            && !s(&read["remote_id"]).trim().is_empty()
            && !s(&read["evidence_ref"]).trim().is_empty(),
        "Readback does not match approved content",
    )?;
    let mut read = read;
    remove(&mut read, "$type");
    item_state(r, id, "verified", string(""));
    let mut a = a;
    a["status"] = string("verified");
    a["readback"] = read.clone();
    set(r, "attempts", "item", id, a);
    event(r, now, id, "verified", read)?;
    get(r, id)
}
fn cleaned(r: &Replay<'_>, id: &Value, calls: &Value) -> bool {
    events(r, id).any(|e| e["action"] == "draft-cleaned" && e["data"]["calls"] == *calls)
}
pub(crate) fn cleanup(
    r: &mut Replay<'_>,
    box_: &mut impl Mailbox,
    ports: &mut impl DeliveryPorts,
    id: &Value,
    now: f64,
) -> StageResult<()> {
    let row = get(r, id)?;
    let a = attempt(r, id);
    check(
        row["state"] == "not-sent" && a["status"] == "not-sent",
        "Only a proven not-sent attempt permits draft cleanup",
    )?;
    check(
        !crossed(r, id),
        "An attempt that reached send cannot be cleaned up for retry",
    )?;
    if cleaned(r, id, &a["calls"]) {
        return Ok(());
    }
    let evidence = box_.discard_draft(Persisted::save(
        DraftAttempt {
            packet: row["packet"].clone(),
            key: s(&a["key"]).into(),
        },
        ports,
        &r.tables,
    )?)?;
    event(
        r,
        now,
        id,
        "draft-cleaned",
        object([("calls", a["calls"].clone()), ("evidence", evidence)]),
    )?;
    ports.save_forced(&r.tables)?;
    Ok(())
}
