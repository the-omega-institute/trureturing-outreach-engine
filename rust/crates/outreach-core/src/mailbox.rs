//! Offline Inbox intake and inbound hydration. Mailbox reads are explicit and
//! scripted; production Python retains the ledger and Graph adapters.
pub mod attachments;
mod inbound;
mod intake;
mod subjects;
pub mod triage;
use crate::mathesis_delivery::StageResult;
use crate::{array, casefold, object, s, string, truth};
pub use inbound::resolve;
pub use intake::{Options, advance};
use outreach_canon::Value;
use std::collections::{BTreeMap, BTreeSet};
/// Read-only mailbox observations. Inbound matching is the existing Graph
/// adapter's bounded message/header/subject lookup, never a write permission.
pub enum Read {
    VerifyOwner,
    Inbox {
        since: String,
        through: String,
    },
    Thread(String),
    SentSnapshot,
    MatchSent {
        message: Value,
        persons: Vec<Value>,
        datetime: String,
        subjects: Value,
    },
    SentTo(String),
    Locate(Value),
    SentById(String),
    SentSubjects {
        start: i64,
        end: i64,
        incremental: bool,
    },
    Attachment {
        message: Value,
        attachment: Value,
    },
}
pub trait Mailbox {
    fn settings(&self) -> &Value;
    fn supports(&self, name: &str) -> bool;
    fn read(&mut self, read: Read) -> StageResult<Value>;
}
pub(crate) fn address(v: &Value) -> String {
    crate::registry::address(&v["emailAddress"]["address"])
}
pub(crate) fn instant(v: &Value) -> i64 {
    crate::gates::instant_micros(v).unwrap_or(i64::MIN)
}
pub(crate) fn headers(v: &Value) -> BTreeMap<String, String> {
    array(&v["internetMessageHeaders"])
        .iter()
        .filter(|v| truth(&v["name"]))
        .map(|v| (casefold(s(&v["name"])), s(&v["value"]).trim().to_owned()))
        .collect()
}
pub(crate) fn recipients(v: &Value) -> BTreeSet<String> {
    ["toRecipients", "ccRecipients"]
        .into_iter()
        .flat_map(|k| array(&v[k]))
        .map(address)
        .filter(|a| !a.is_empty())
        .collect()
}
pub(crate) fn contacts(rows: &[Value], account: &str) -> BTreeSet<String> {
    rows.iter()
        .flat_map(|m| std::iter::once(address(&m["from"])).chain(recipients(m)))
        .filter(|a| a != account)
        .collect()
}
pub(crate) fn machine(address: &str) -> bool {
    [
        "no-reply",
        "noreply",
        "postmaster",
        "mailer-daemon",
        "mail delivery subsystem",
    ]
    .contains(&casefold(address.trim().rsplit('@').next_back().unwrap_or("")).as_str())
}
pub(crate) fn failure(m: &Value) -> bool {
    let local = address(&m["from"])
        .split('@')
        .next()
        .unwrap_or("")
        .to_owned();
    let ndr = ["postmaster", "mailer-daemon", "mail delivery subsystem"];
    ndr.contains(&local.as_str())||ndr.contains(&casefold(s(&m["from"]["emailAddress"]["name"]).trim()).as_str())
    ||crate::regex(r"(?i)^\s*(?:Undeliverable|Delivery Status Notification|Mail delivery failed|Returned mail|Delivery has failed)\b").is_ok_and(|r|r.is_match(s(&m["subject"])))
    ||["final-recipient","x-failed-recipients"].into_iter().any(|k|headers(m).contains_key(k)||m["dsn_fields"].get(k).is_some())
    ||crate::regex(r"(?im)^\s*(?:Final-Recipient|X-Failed-Recipients)\s*:").is_ok_and(|r|r.is_match(s(&m["body"]["content"])))
}
pub(crate) fn message_key(m: &Value) -> Value {
    crate::ingest::message_id(&m["internetMessageId"])
        .map(string)
        .unwrap_or_else(|| crate::fallback(&[&m["internetMessageId"], &m["id"]]).clone())
        .pipe_if_null(|| {
            Value::Array(vec![
                m["conversationId"].clone(),
                m["receivedDateTime"].clone(),
            ])
        })
}
trait NullFallback {
    fn pipe_if_null(self, f: impl FnOnce() -> Value) -> Value;
}
impl NullFallback for Value {
    fn pipe_if_null(self, f: impl FnOnce() -> Value) -> Value {
        if self.is_null() { f() } else { self }
    }
}
pub(crate) fn fields(m: &Value, keys: &[&str]) -> Value {
    let mut out = object([]);
    for k in keys {
        if let Some(v) = m.get(k) {
            out[*k] = v.clone();
        }
    }
    out
}
pub(crate) const META: [&str; 11] = [
    "id",
    "internetMessageId",
    "conversationId",
    "from",
    "toRecipients",
    "ccRecipients",
    "subject",
    "receivedDateTime",
    "sentDateTime",
    "internetMessageHeaders",
    "dsn_fields",
];
pub(crate) fn merge(a: &mut Value, b: &Value) {
    for (k, v) in b.as_object().into_iter().flatten() {
        a[k] = v.clone();
    }
}
pub(crate) fn coverage(rows: &[Value], finished: bool) -> BTreeMap<String, Vec<Value>> {
    let mut out: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for row in rows {
        let state = s(&row["state"]);
        let done = finished
            && [
                "verified",
                "no-reply",
                "answered-by-mail",
                "question-closed",
                "owner-discarded",
            ]
            .contains(&state);
        if row["kind"] != "reply"
            || (!done
                && [
                    "verified",
                    "failed",
                    "no-reply",
                    "superseded",
                    "answered-by-mail",
                    "question-closed",
                    "handoff-ready",
                    "round-complete",
                    "submission-ready",
                    "revising",
                    "owner-discarded",
                ]
                .contains(&state))
        {
            continue;
        }
        let i = &row["request"]["inputs"];
        let mut ids = vec![
            i["latest_inbound_message_id"].clone(),
            row["data"]["thread_inputs"]["latest_inbound_message_id"].clone(),
        ];
        if done {
            let p = crate::fallback(&[
                &row["data"]["packet_override"],
                &row["data"]["result"]["packet"],
            ]);
            ids.push(p["latest_inbound_message_id"].clone());
        } else {
            ids.extend_from_slice(array(&i["thread_ref"]["internetMessageIds"]));
        }
        let ids: BTreeSet<_> = ids.iter().filter_map(crate::ingest::message_id).collect();
        for id in ids {
            out.entry(id).or_default().push(object([
                ("task_id", row["id"].clone()),
                ("state", row["state"].clone()),
            ]));
        }
    }
    out
}
