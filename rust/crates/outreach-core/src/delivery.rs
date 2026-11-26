//! Offline reviewed mail delivery. Python is the sole production writer.
//! The mailbox preflight port owns fresh Graph evidence (including the
//! once-per-correspondent Mathesis check); core owns routing and durable intent.
mod drafts;
pub mod mathesis;
mod queue;
use crate::sandbox_pool::{error_text, remove};
use crate::{
    ingest::Replay,
    mathesis_delivery::{DeliveryPorts, StageError, StageResult},
    persistence::Persisted,
    *,
};
use outreach_canon::digest;
use std::collections::BTreeSet;

pub const SEND_GUARD_REASON: &str =
    "Unsent owner draft to the same recipient exists; consolidate before sending";
pub enum MailboxFeature {
    TrackedSend,
    DraftsFor,
    RecipientHistory,
    Message,
    SentItems,
    DraftsFolder,
    DeletedFolder,
    VerifyOwner,
    Matches,
    OwnDraft,
    Thread,
    Find,
    Messages,
    CreateDraft,
    DeleteDraft,
    MoveDraft,
}
impl MailboxFeature {
    pub fn name(&self) -> &'static str {
        match self {
            Self::TrackedSend => "submit_tracked",
            Self::DraftsFor => "drafts_for",
            Self::RecipientHistory => "recipient_history",
            Self::Message => "message",
            Self::SentItems => "sent_items",
            Self::DraftsFolder => "drafts_folder_id",
            Self::DeletedFolder => "deleted_items_folder_id",
            Self::VerifyOwner => "verify_owner",
            Self::Matches => "matches",
            Self::OwnDraft => "own_draft",
            Self::Thread => "thread",
            Self::Find => "find",
            Self::Messages => "messages",
            Self::CreateDraft => "create_draft",
            Self::DeleteDraft => "delete_draft",
            Self::MoveDraft => "move_draft_to_deleted",
        }
    }
}
pub enum MailboxRead {
    Preflight {
        packet: Value,
        request: Value,
        allow_draft: Option<String>,
    },
    Lookup {
        packet: Value,
        key: String,
    },
    LegacyNotSent {
        packet: Value,
        key: String,
    },
    Find {
        folder: String,
        key: String,
    },
    DraftsFor(String),
    RecipientHistory(String),
    Thread(String),
    Message(String),
    SentItems(Option<f64>),
    DraftsFolder,
    DeletedFolder,
    VerifyOwner,
    Matches {
        draft: Value,
        packet: Value,
    },
    OwnDraft {
        draft: Value,
        packet: Value,
        key: String,
    },
    Messages(String),
}
pub struct SendIntent {
    pub packet: Value,
    pub key: String,
}
pub struct DraftAttempt {
    pub packet: Value,
    pub key: String,
}
pub struct DraftIntent {
    pub packet: Value,
    pub key: String,
    pub recovery: bool,
}
pub struct ReplacementIntent {
    pub packet: Value,
    pub key: String,
    pub replacing: String,
}
pub struct DraftId {
    pub remote_id: String,
}
pub trait SendBoundary {
    fn before_send(&mut self, draft: &str) -> StageResult<()>;
}
/// Draft writes and automatic sends have separate capabilities. All writes
/// consume a snapshot-bound permit; reads cannot authorize an automatic retry.
/// ```compile_fail
/// use outreach_core::{delivery::{Mailbox,DraftIntent,SendBoundary},persistence::Persisted};
/// fn owner_cannot_send(box_:&mut impl Mailbox,draft:Persisted<DraftIntent>,boundary:&mut dyn SendBoundary) {
///     box_.submit(draft,boundary);
/// }
/// ```
pub trait Mailbox {
    fn supports(&self, feature: MailboxFeature) -> bool;
    fn attempt_started(&mut self, at: f64);
    fn observe(&mut self, read: MailboxRead) -> StageResult<Value>;
    fn save_draft(&mut self, intent: Persisted<DraftIntent>) -> StageResult<Value>;
    fn submit(
        &mut self,
        intent: Persisted<SendIntent>,
        boundary: &mut dyn SendBoundary,
    ) -> StageResult<Value>;
    fn discard_draft(&mut self, intent: Persisted<DraftAttempt>) -> StageResult<Value>;
    fn create_draft(&mut self, intent: Persisted<ReplacementIntent>) -> StageResult<Value>;
    fn delete_draft(&mut self, intent: Persisted<DraftId>) -> StageResult<Value>;
    fn move_draft(&mut self, intent: Persisted<DraftId>) -> StageResult<Value>;
}
/// Contact registration/rendering stays at its existing data-repository boundary.
/// A successful call returns today's contact table, after checking the supplied
/// fresh mailbox evidence; it never supplies task or queue rows.
pub trait MailRegistry {
    fn register(&mut self, evidence: &Value) -> StageResult<Option<Value>>;
}
#[derive(Default)]
pub struct Options {
    pub absent: bool,
    pub previous: Vec<String>,
    pub queue_now: f64,
}
fn num(v: &Value) -> f64 {
    v.as_f64().unwrap_or(0.)
}
fn check(ok: bool, reason: &str) -> StageResult<()> {
    Ok(validation::require(ok, reason)?)
}
fn packet(row: &Value) -> Value {
    fallback(&[
        &row["data"]["packet_override"],
        &row["data"]["result"]["packet"],
    ])
    .clone()
}
fn addresses(packet: &Value) -> BTreeSet<String> {
    array(&packet["to"])
        .iter()
        .chain(array(&packet["cc"]))
        .map(|v| casefold(s(v).trim()))
        .filter(|s| !s.is_empty())
        .collect()
}
fn address(v: &Value) -> String {
    if v.is_string() {
        casefold(s(v).trim())
    } else {
        casefold(s(fallback(&[&v["emailAddress"]["address"], &v["address"]])).trim())
    }
}
fn attempt_key(m: &Value) -> Value {
    if truth(&m["attempt_key"]) {
        return m["attempt_key"].clone();
    }
    array(&m["internetMessageHeaders"])
        .iter()
        .find(|h| {
            casefold(s(&h["name"])) == "x-outreach-attempt"
                && h["value"].is_string()
                && truth(&h["value"])
        })
        .map(|h| h["value"].clone())
        .unwrap_or(Value::Null)
}
fn binding(row: &Value) -> Value {
    let d = &row["data"];
    object([
        ("task_revision", row["revision"].clone()),
        ("result", d["result"].clone()),
        ("packet", packet(row)),
        ("review", d["review"].clone()),
        ("request_inputs", row["request"]["inputs"].clone()),
        ("thread_inputs", d["thread_inputs"].clone()),
        ("mailbox_history", d["mailbox_history"].clone()),
        (
            "draft_revision_requests",
            d["draft_revision_requests"].clone(),
        ),
        ("draft_revision_intent", d["draft_revision_intent"].clone()),
        (
            "reason",
            fallback(&[&d["last_error"], &d["reason"]]).clone(),
        ),
    ])
}
fn due(r: &Replay<'_>, lane: &str, id: &Value, b: &Value) -> StageResult<bool> {
    let saved = r.state(&format!("recheck:{lane}:{}", s(id)));
    Ok(saved["binding"] != digest(b)? || r.now >= num(&saved["next_at"]))
}
fn defer(r: &mut Replay<'_>, lane: &str, id: &Value, b: &Value, seconds: f64) -> StageResult<()> {
    r.set_state(
        &format!("recheck:{lane}:{}", s(id)),
        object([
            ("binding", string(digest(b)?)),
            ("checked_at", r.clock_value.clone()),
            ("next_at", outreach_canon::float(r.now + seconds)?),
        ]),
    )?;
    Ok(())
}
fn failed(
    r: &mut Replay<'_>,
    ports: &mut impl DeliveryPorts,
    id: &Value,
    e: &StageError,
    terminal: bool,
) -> StageResult<()> {
    let row = r.task(id)?;
    let mut d = row["data"].clone();
    let text = error_text(e);
    let text = if text.is_empty() {
        s(&e.summary["type"]).into()
    } else {
        text
    };
    d["last_error"] = string(text.chars().take(300).collect::<String>());
    if terminal {
        d["hold_reason"] = string(&text);
        d["hold_kind"] = string("delivery-terminal");
    }
    r.update(&row, if terminal { "held" } else { s(&row["state"]) }, d)?;
    let row = r.task(id)?;
    if row["state"] == "owner-draft"
        && !truth(&row["data"]["draft_receipt"])
        && !truth(&row["data"]["sent_receipt"])
    {
        defer(r, "owner-draft", id, &binding(&row), 1800.)?;
    }
    ports.save(&r.tables)
}
fn terminal(
    e: &StageError,
    r: &Replay<'_>,
    box_: &mut impl Mailbox,
    row: &Value,
    p: &Value,
) -> bool {
    if e.summary["type"] != "GateError" {
        return false;
    }
    let d = &row["data"];
    if row["state"] == "owner-draft" {
        if [
            "draft_receipt",
            "draft_intent",
            "draft_revision_intent",
            "stale_draft_conflict",
        ]
        .iter()
        .any(|k| truth(&d[*k]))
            || !queue::attempt(r, &row["id"]).is_null()
        {
            return false;
        }
    } else if row["state"] != "awaiting-mailbox" {
        return false;
    }
    let text = error_text(e);
    if [
        "A newer incoming message requires a new reply/review",
        "Reply-all recipients differ from the latest incoming message",
        "Actual Sent changed or its source identity was not captured",
    ]
    .contains(&text.as_str())
        || text.starts_with("Current conversation has an active table-3 refusal: ")
        || text.starts_with("Mathesis was already introduced to this correspondent")
        || (text.starts_with("A newer incoming message from ")
            && text.ends_with(" in another conversation requires a new reply/review"))
    {
        return true;
    }
    if text == "A server draft already exists; preserve the user draft" {
        let (ids, keys) = own_refs(r, row);
        return box_
            .observe(MailboxRead::Thread(s(&p["thread_id"]).into()))
            .ok()
            .is_some_and(|rows| {
                array(&rows).iter().any(|m| {
                    truth(&m["isDraft"])
                        && truth(&m["id"])
                        && !ids.contains(s(&m["id"]))
                        && !keys.contains(s(&attempt_key(m)))
                })
            });
    }
    false
}
fn own_refs(r: &Replay<'_>, row: &Value) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut ids = BTreeSet::new();
    let mut keys = BTreeSet::new();
    let a = queue::attempt(r, &row["id"]);
    if truth(&a["key"]) {
        keys.insert(s(&a["key"]).into());
    }
    if truth(&row["data"]["draft_receipt"]["remote_id"]) {
        ids.insert(s(&row["data"]["draft_receipt"]["remote_id"]).into());
    }
    for e in queue::events(r, &row["id"]) {
        if e["action"] == "send-call" && truth(&e["data"]["draft_id"]) {
            ids.insert(s(&e["data"]["draft_id"]).into());
        }
    }
    (ids, keys)
}
fn conflicts(
    r: &Replay<'_>,
    box_: &mut impl Mailbox,
    row: &Value,
    p: &Value,
) -> StageResult<Value> {
    let addresses = addresses(p);
    let (ids, keys) = own_refs(r, row);
    let mut seen = BTreeSet::new();
    let mut drafts = BTreeSet::new();
    let mut tasks = BTreeSet::new();
    for recipient in &addresses {
        let found = if box_.supports(MailboxFeature::DraftsFor) {
            box_.observe(MailboxRead::DraftsFor(recipient.clone()))?
        } else if box_.supports(MailboxFeature::RecipientHistory) {
            box_.observe(MailboxRead::RecipientHistory(recipient.clone()))?
        } else {
            Value::Array(vec![])
        };
        for d in array(&found) {
            if d["isDraft"] != true && casefold(s(&d["folder"])) != "drafts" {
                continue;
            }
            let id = s(&d["id"]);
            if id.is_empty()
                || seen.contains(id)
                || ids.contains(id)
                || keys.contains(s(&attempt_key(d)))
            {
                continue;
            }
            let participants: BTreeSet<_> = std::iter::once(&d["from"])
                .chain(array(&d["toRecipients"]))
                .chain(array(&d["ccRecipients"]))
                .map(address)
                .collect();
            if !participants.contains(recipient) {
                continue;
            }
            seen.insert(id.to_owned());
            drafts.insert(id.to_owned());
        }
    }
    for candidate in r.rows("tasks") {
        if candidate["id"] == row["id"]
            || candidate["state"] != "owner-draft"
            || addresses.is_disjoint(&self::addresses(&packet(candidate)))
        {
            continue;
        }
        let saved = &candidate["data"]["draft_receipt"]["remote_id"];
        if !truth(saved) || seen.contains(s(saved)) {
            tasks.insert(s(&candidate["id"]).to_owned());
        }
    }
    Ok(object([
        (
            "task_ids",
            Value::Array(tasks.into_iter().map(string).collect()),
        ),
        (
            "draft_ids",
            Value::Array(drafts.into_iter().map(string).collect()),
        ),
    ]))
}
fn recipient(tables: &Value, a: &str, automatic: bool) -> StageResult<Value> {
    let people = crate::registry::people(tables)?;
    let person = people.iter().find(|p| {
        array(&p["emails"])
            .iter()
            .any(|v| s(v) == casefold(a.trim()))
    });
    check(
        person.is_some(),
        "Mail recipient is not registered in table 1",
    )?;
    let blocked = crate::registry::preflight(a, "", tables, &people)?
        .iter()
        .any(|x| x == "table-3-refusal-or-stop-boundary");
    if automatic {
        check(!blocked, "Recipient has an active table-3 boundary")?;
    }
    Ok(person.map(|p| p["id"].clone()).unwrap_or(Value::Null))
}
fn register(
    registry: &mut impl MailRegistry,
    tables: &mut Value,
    policy: &Value,
) -> StageResult<()> {
    if let Some(contacts) = registry.register(&policy["mail_thread_contacts"])? {
        tables["contacts"] = contacts;
    }
    Ok(())
}
fn expired(box_: &mut impl Mailbox, row: &Value) -> StageResult<()> {
    for intent in array(&row["data"]["draft_intent_expired"]) {
        box_.attempt_started(num(&intent["at"]));
        let found = box_.observe(MailboxRead::Find {
            folder: "drafts".into(),
            key: s(&intent["key"]).into(),
        })?;
        check(
            found.is_null(),
            &format!(
                "Expired draft attempt reappeared; reconcile manually: {}",
                s(fallback(&[&found["id"], &intent["key"]]))
            ),
        )?;
    }
    Ok(())
}
fn override_home(p: &Value, config: &Value) -> Value {
    let domains = config
        .get("home_domains")
        .unwrap_or(&config["mailbox"]["home_domains"]);
    let found: Vec<_> = array(&p["to"])
        .iter()
        .chain(array(&p["cc"]))
        .filter(|a| {
            let domain = casefold(s(a).rsplit('@').next().unwrap_or("").trim_end_matches('.'));
            array(domains).iter().any(|suffix| {
                let suffix = casefold(s(suffix).trim().trim_end_matches('.'));
                domain == suffix || domain.ends_with(&format!(".{suffix}"))
            })
        })
        .cloned()
        .collect();
    if found.is_empty() {
        Value::Null
    } else {
        object([
            ("reason", string("home-institution recipient")),
            ("addresses", Value::Array(found)),
        ])
    }
}
fn route(row: &Value) -> &str {
    let result = &row["data"]["result"];
    let route = &result["routing"];
    if truth(&route["questions"]) {
        "awaiting-user"
    } else if result["outcome"] == "no-reply" || route["mail"] == "none" {
        "no-reply"
    } else if route["mail"] == "owner" || !row["data"]["routing_override"].is_null() {
        "owner-draft"
    } else {
        "awaiting-mailbox"
    }
}

pub fn advance(
    r: &mut Replay<'_>,
    box_: &mut impl Mailbox,
    registry: &mut impl MailRegistry,
    ports: &mut impl DeliveryPorts,
    options: &Options,
) -> StageResult<()> {
    if options.absent {
        return Ok(());
    }
    queue::initialize(r);
    let config = r.environment.config.clone();
    let mut tables = r.environment.tables.clone();
    let group = |row: &Value| {
        if row["state"] == "awaiting-mailbox" {
            1
        } else if truth(&row["data"]["draft_receipt"]) {
            2
        } else {
            0
        }
    };
    let mut rows: Vec<_> = r
        .rows("tasks")
        .iter()
        .filter(|row| {
            ["firstcontact", "reply"].contains(&s(&row["kind"]))
                && ["awaiting-mailbox", "owner-draft"].contains(&s(&row["state"]))
        })
        .cloned()
        .collect();
    rows.sort_by_key(|row| {
        (
            group(row),
            options
                .previous
                .iter()
                .position(|id| row["id"] == id.as_str())
                .unwrap_or(options.previous.len()),
        )
    });
    let mut admitted = vec![];
    for row in rows {
        if group(&row) != 0 || due(r, "owner-draft", &row["id"], &binding(&row))? {
            admitted.push(row);
        }
    }
    for original in admitted {
        let id = &original["id"];
        if !ports.allow("delivery", s(id)) {
            continue;
        }
        let mut row = r.task(id)?;
        let mut data = row["data"].clone();
        if row["state"] == "owner-draft"
            && truth(&data["draft_receipt"])
            && !truth(&config["paused"])
            && truth(&config["kinds"][s(&row["kind"])])
        {
            let b = Value::Array(vec![
                data["draft_receipt"].clone(),
                packet(&row),
                data["draft_revision_requests"].clone(),
                data["draft_revision_intent"].clone(),
            ]);
            let revising =
                truth(&data["draft_revision_requests"]) || truth(&data["draft_revision_intent"]);
            let missing = ["draft-missing-not-sent", "sent-ambiguous"]
                .contains(&s(&data["owner_draft_status"]));
            if !due(r, "owner-sent", id, &b)? && (!revising || missing) {
                continue;
            }
            if !ports.allow("delivery", s(id)) {
                continue;
            }
            let result = (|| -> StageResult<bool> {
                expired(box_, &row)?;
                if truth(&data["draft_intent_expired"]) && truth(&data["last_error"]) {
                    remove(&mut data, "last_error");
                    r.update(&row, s(&row["state"]), data.clone())?;
                    row = r.task(id)?;
                }
                let gone = drafts::reconcile_owner(r, box_, ports, &row, &config)?;
                if gone || !revising {
                    defer(r, "owner-sent", id, &b, 1800.)?;
                    ports.save(&r.tables)?;
                }
                Ok(gone)
            })();
            match result {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) => {
                    if terminal_error(&e) {
                        return Err(e);
                    }
                    failed(r, ports, id, &e, false)?;
                    continue;
                }
            }
        }
        if row["state"] == "owner-draft"
            && (truth(&data["draft_revision_requests"]) || truth(&data["draft_revision_intent"]))
        {
            if !ports.allow("delivery", s(id)) {
                continue;
            }
            match drafts::revision(r, box_, registry, ports, &row, &config, &mut tables) {
                Ok(updated) => {
                    row = updated;
                    data = row["data"].clone();
                }
                Err(e) => {
                    if terminal_error(&e) {
                        return Err(e);
                    }
                    failed(r, ports, id, &e, false)?;
                    continue;
                }
            }
        }
        if !ports.allow("delivery", s(id)) {
            continue;
        }
        let p = packet(&row);
        if !truth(&p) {
            continue;
        }
        let mut checking = false;
        let result = (|| -> StageResult<()> {
            if row["state"] == "owner-draft" && !truth(&data["draft_receipt"]) {
                expired(box_, &row)?;
            }
            let existing = !queue::attempt(r, id).is_null();
            if existing && !truth(&data["draft_receipt"]) {
                let mut current = queue::get(r, id)?;
                let started = queue::events(r, id)
                    .find(|e| e["action"] == "send-intent")
                    .map(|e| num(&e["at"]))
                    .unwrap_or(0.);
                box_.attempt_started(started);
                if ["sending", "send-unknown", "sent", "not-sent"].contains(&s(&current["state"])) {
                    current = queue::reconcile(r, box_, ports, id, options.queue_now)?;
                }
                if current["state"] == "verified" {
                    remove(&mut data, "last_error");
                    data["sent_receipt"] = queue::attempt(r, id)["readback"].clone();
                    r.update(&row, "verified", data.clone())?;
                }
                if current["state"] == "not-sent" {
                    data["last_error"] =
                        string(s(&current["reason"]).chars().take(300).collect::<String>());
                    r.update(&row, s(&row["state"]), data.clone())?;
                    row = r.task(id)?;
                }
                ports.save(&r.tables)?;
                if current["state"] != "not-sent" {
                    return Ok(());
                }
            }
            if truth(&config["paused"]) || !truth(&config["kinds"][s(&row["kind"])]) {
                return Ok(());
            }
            check(
                data["review"]["verdict"] == "PASS",
                "Mail requires exact independent review",
            )?;
            let hash = (r.environment.workflow)(s(&row["kind"]))?;
            check(
                crate::reviews::matches_workflow(&row, &hash),
                "Mail workflow changed; re-review the current version",
            )?;
            check(
                crate::transitions::review_current(&row, &hash)?,
                "Mail review no longer binds the exact result; re-review",
            )?;
            let mut queue_hash = hash.clone();
            if data["workflow_binding"]["workflow_hash"] == hash
                && r.rows("items").iter().any(|v| v["id"] == *id)
                && queue::get(r, id)?["workflow_hash"] == data["workflow_binding"]["legacy_hash"]
            {
                queue_hash = data["workflow_binding"]["legacy_hash"].clone();
            }
            let env = Environment {
                config: config.clone(),
                tables: tables.clone(),
                workflow: r.environment.workflow,
            };
            gates::pipeline(&row["request"], r.rows("tasks"), &env)?;
            gates::parent(&row["request"], r.rows("tasks"), &env)?;
            gates::paper(&row["request"], &env)?;
            transitions::source_gate(&row["request"], &data["result"], r.rows("tasks"))?;
            gates::owner(&row["request"], &env)?;
            checking = true;
            gates::thread(&row["request"], data.get("thread_inputs"), &env)?;
            checking = false;
            let route = route(&row).to_owned();
            check(
                route == s(&row["state"]),
                "Mail ownership or decision route changed",
            )?;
            if route == "owner-draft" && truth(&data["draft_receipt"]) {
                return Ok(());
            }
            let mut expired_intent = Value::Null;
            if route == "owner-draft" && truth(&data["draft_intent"]) {
                let intent = data["draft_intent"].clone();
                box_.attempt_started(num(&intent["at"]));
                ports.save_forced(&r.tables)?;
                let sent = box_.observe(MailboxRead::Lookup {
                    packet: p.clone(),
                    key: s(&intent["key"]).into(),
                })?;
                if !sent.is_null() {
                    check(
                        sent["content_hash"] == digest(&p)?,
                        "Actual Sent differs from reviewed packet",
                    )?;
                    remove(&mut data, "last_error");
                    let mut sent = sent;
                    remove(&mut sent, "$type");
                    data["sent_receipt"] = sent;
                    r.update(&row, "verified", data.clone())?;
                    ports.save(&r.tables)?;
                    return Ok(());
                }
                if r.now - num(&intent["at"]) > 7200.
                    && box_
                        .observe(MailboxRead::Find {
                            folder: "drafts".into(),
                            key: s(&intent["key"]).into(),
                        })?
                        .is_null()
                {
                    expired_intent = intent.clone();
                    expired_intent["confirmed_at"] = r.clock_value.clone();
                }
                if expired_intent.is_null() {
                    let receipt = box_.save_draft(Persisted::saved(
                        DraftIntent {
                            packet: p.clone(),
                            key: s(&intent["key"]).into(),
                            recovery: true,
                        },
                        ports,
                        &r.tables,
                    )?)?;
                    check(
                        receipt["content_hash"] == digest(&p)?,
                        "Saved draft differs from reviewed packet",
                    )?;
                    remove(&mut data, "last_error");
                    data["draft_receipt"] = receipt;
                    r.update(&row, "owner-draft", data.clone())?;
                    ports.save(&r.tables)?;
                    return Ok(());
                }
            }
            if existing {
                check(
                    route == "awaiting-mailbox" || truth(&data["routing_override"]),
                    "Automatic attempt cannot become an owner draft",
                )?;
                queue::cleanup(r, box_, ports, id, options.queue_now)?;
            }
            if route == "owner-draft" {
                if !ports.allow("delivery", s(id)) {
                    return Ok(());
                }
                row = drafts::retire_stale(r, box_, registry, ports, &row, &p, &mut tables)?;
                data = row["data"].clone();
            }
            let mut request = row["request"].clone();
            request["inputs"] = results::effective_inputs(&row);
            checking = true;
            let mut policy = box_.observe(MailboxRead::Preflight {
                packet: p.clone(),
                request,
                allow_draft: None,
            })?;
            checking = false;
            if !ports.allow("delivery", s(id)) {
                return Ok(());
            }
            register(registry, &mut tables, &policy)?;
            remove(&mut policy, "mail_thread_contacts");
            let exception = policy["recipient_exception"].clone();
            remove(&mut policy, "recipient_exception");
            if truth(&exception) {
                data["recipient_exception"] = exception;
                r.update(&row, s(&row["state"]), data.clone())?;
                row = r.task(id)?;
            }
            let mut keys = vec![];
            for a in array(&p["to"]).iter().chain(array(&p["cc"])) {
                let contact = recipient(&tables, s(a), route == "awaiting-mailbox")?;
                if row["kind"] == "firstcontact" {
                    keys.push(digest(&Value::Array(vec![
                        string("firstcontact-scholar"),
                        contact,
                    ]))?);
                }
            }
            remove(&mut data, "last_error");
            remove(&mut data, "stale_draft_conflict");
            if route == "owner-draft" {
                let mut intent = data["draft_intent"].clone();
                let recovery = !intent.is_null() && expired_intent.is_null();
                if intent.is_null() || !expired_intent.is_null() {
                    let key = if !expired_intent.is_null() {
                        digest(&Value::Array(vec![
                            string("owner-draft-retry"),
                            intent["key"].clone(),
                        ]))?
                    } else {
                        digest(&Value::Array(vec![
                            string("owner-draft"),
                            id.clone(),
                            string(digest(&p)?),
                        ]))?
                    };
                    if !expired_intent.is_null() {
                        let mut expired = list(&data["draft_intent_expired"]);
                        expired_intent["replacement_key"] = string(&key);
                        expired.push(expired_intent);
                        data["draft_intent_expired"] = Value::Array(expired);
                    }
                    intent = object([("key", string(key)), ("at", r.clock_value.clone())]);
                    data["draft_intent"] = intent.clone();
                    r.update(&row, "owner-draft", data.clone())?;
                    row = r.task(id)?;
                }
                box_.attempt_started(num(&intent["at"]));
                let receipt = box_.save_draft(Persisted::save(
                    DraftIntent {
                        packet: p.clone(),
                        key: s(&intent["key"]).into(),
                        recovery,
                    },
                    ports,
                    &r.tables,
                )?)?;
                check(
                    receipt["content_hash"] == digest(&p)?,
                    "Saved draft differs from reviewed packet",
                )?;
                data["draft_receipt"] = receipt;
                r.update(&row, "owner-draft", data.clone())?;
                ports.save(&r.tables)?;
                return Ok(());
            }
            let home = override_home(&p, &config);
            if !home.is_null() {
                data["routing_override"] = home;
                r.update(&row, "owner-draft", data.clone())?;
                ports.save(&r.tables)?;
                return Ok(());
            }
            check(
                config["automation"]["automatic_mail"] == true,
                "Automatic mail has not been activated after live acceptance",
            )?;
            let conflicts = conflicts(r, box_, &row, &p)?;
            if truth(&conflicts["task_ids"]) || truth(&conflicts["draft_ids"]) {
                data["reason"] = string(SEND_GUARD_REASON);
                data["delivery_conflicts"] = conflicts.clone();
                data["conflicting_task_ids"] = conflicts["task_ids"].clone();
                data["conflicting_draft_ids"] = conflicts["draft_ids"].clone();
                r.update(&row, "held", data.clone())?;
                ports.save(&r.tables)?;
                return Ok(());
            }
            queue::prepare(r, &row, &p, &queue_hash, options.queue_now)?;
            let revision = queue::policy(r, &row["kind"], policy.clone(), options.queue_now)?;
            if !existing {
                box_.attempt_started(r.now);
            }
            let current = queue::dispatch(
                r,
                box_,
                ports,
                queue::Dispatch {
                    row: &row,
                    workflow: &queue_hash,
                    keys,
                    policy: &policy,
                    revision,
                    now: options.queue_now,
                },
            )?;
            if ["not-sent", "send-unknown"].contains(&s(&current["state"])) {
                data["last_error"] =
                    string(s(&current["reason"]).chars().take(300).collect::<String>());
                r.update(&row, s(&row["state"]), data.clone())?;
                row = r.task(id)?;
            }
            if current["state"] == "not-sent" {
                ports.save(&r.tables)?;
                queue::cleanup(r, box_, ports, id, options.queue_now)?;
            }
            ports.save(&r.tables)?;
            Ok(())
        })();
        if let Err(e) = result {
            if terminal_error(&e) {
                return Err(e);
            }
            let row = r.task(id)?;
            let hold = checking && terminal(&e, r, box_, &row, &p);
            failed(r, ports, id, &e, hold)?;
        }
    }
    Ok(())
}
fn terminal_error(e: &StageError) -> bool {
    e.is_interruption()
}

fn list(v: &Value) -> Vec<Value> {
    array(v).to_vec()
}
