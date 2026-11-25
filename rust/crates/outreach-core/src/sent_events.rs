//! Metadata-only Sent Items reconciliation. All observations and table persistence
//! are ports; Python continues to own production writes and Markdown rendering.
use crate::mathesis_delivery::{DeliveryPorts, Persisted, StageResult};
use crate::{
    array, casefold, fallback, gates::instant_micros, ingest::Replay, object, registry, s, string,
    text, truth,
};
use outreach_canon::{SerializeOptions, Value, dumps, parse, sha256};
use std::collections::BTreeSet;

pub struct SentPage {
    pub messages: Vec<Value>,
    pub next_link: Value,
}
pub trait Mailbox {
    fn settings(&self) -> &Value;
    fn verify_owner(&mut self) -> StageResult<String>;
    fn sent_events_page(&mut self, since: &str, until: &str, next: &Value)
    -> StageResult<SentPage>;
}
pub struct ContactIntent {
    pub cursor: Value,
    pub contacts_digest: String,
}
pub trait ContactTables {
    /// Render the supplied contact rows and persist only the scoped table paths.
    /// The adapter must retain the current page on failure; the cursor is saved
    /// only after commit succeeds. Repeated calls repair partial view writes.
    fn render(&mut self, contacts: &Value) -> StageResult<()>;
    fn commit_tables(&mut self, intent: Persisted<ContactIntent>) -> StageResult<()>;
}
fn check(ok: bool, message: &str) -> StageResult<()> {
    Ok(crate::validation::require(ok, message)?)
}
fn json(value: &Value, ascii: bool) -> StageResult<String> {
    Ok(dumps(
        value,
        SerializeOptions {
            ensure_ascii: ascii,
            sort_keys: true,
            ..Default::default()
        },
    )?)
}
fn system(value: &str) -> bool {
    let (local, domain) = value.rsplit_once('@').unwrap_or((value, ""));
    ["arxiv.org", "export.arxiv.org"].contains(&domain)
        || crate::regex(r"^(?:no[-_.]?reply|do[-_.]?not[-_.]?reply|mailer[-_.]?daemon|postmaster|notifications?|system|auto[-_.]?reply|bounces?)(?:[+._-].*)?$")
            .is_ok_and(|r| r.is_match(local))
        || ["no-reply", "noreply", "postmaster", "mailer-daemon", "mail delivery subsystem"].contains(&casefold(local).as_str())
}
fn context(rows: &[Value], message: &Value) -> StageResult<(Value, String)> {
    let matching: Vec<_> = rows
        .iter()
        .filter(|row| {
            ["sent_receipt", "draft_receipt"].iter().any(|key| {
                let r = &row["data"][key];
                (truth(&message["id"]) && r["remote_id"] == message["id"])
                    || r["internetMessageId"] == message["internetMessageId"]
                    || r["evidence_ref"]
                        == format!("graph:sent:{}", s(&message["internetMessageId"]))
            })
        })
        .collect();
    if matching.len() != 1 {
        return Ok((Value::Null, String::new()));
    }
    let row = matching[0];
    let summary = if crate::transitions::review_current(row, &row["request"]["workflow_hash"])? {
        s(fallback(&[
            &row["data"]["result"]["headline"],
            &row["data"]["result"]["owner_brief"]["summary_zh"],
        ]))
    } else {
        ""
    };
    Ok((row["id"].clone(), text::compact(summary, 300)))
}
fn recorded(
    person: &Value,
    contacts: &Value,
    recipient: &str,
    message: &str,
    at: i64,
) -> StageResult<bool> {
    for record in array(&person["records"]) {
        let row = array(&contacts["contacts"])
            .iter()
            .find(|r| r["id"] == record["id"])
            .unwrap_or(record);
        for event in array(&row["hub"]["events"]) {
            if event["kind"] != "sent" {
                continue;
            }
            let source = &event["evidence"];
            let evidence = if let Some(raw) = source.as_str() {
                parse(raw).unwrap_or(Value::Null)
            } else {
                source.clone()
            };
            if evidence.is_object() && evidence.get("recipient").is_some_and(|r| r != recipient) {
                continue;
            }
            let raw = source
                .as_str()
                .map(str::to_owned)
                .unwrap_or(json(source, true)?);
            if raw.contains(message) {
                return Ok(true);
            }
            if evidence["source"] == "sent-events" {
                continue;
            }
            if (!evidence.is_object() || !truth(&evidence["recipient"]))
                && !array(&person["emails"]).iter().any(|e| e == recipient)
            {
                continue;
            }
            if instant_micros(&event["at"]).is_some_and(|t| t.abs_diff(at) <= 600_000_000) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
fn record_page(
    contacts: &mut Value,
    tables: &Value,
    messages: &[Value],
    rows: &[Value],
    ignored: &BTreeSet<String>,
    window: (i64, i64),
    account: &str,
) -> StageResult<usize> {
    let (since, until) = window;
    let mut environment = tables.clone();
    environment["contacts"] = contacts.clone();
    let people = registry::people(&environment)?;
    let mut count = 0;
    for message in messages {
        let Some(id) = message["internetMessageId"]
            .as_str()
            .filter(|v| !v.trim().is_empty())
        else {
            continue;
        };
        if truth(&message["isDraft"]) {
            continue;
        }
        let Some(at) = instant_micros(&message["sentDateTime"]) else {
            continue;
        };
        if at < since || at > until {
            continue;
        }
        let recipients: BTreeSet<_> = ["toRecipients", "ccRecipients", "bccRecipients"]
            .into_iter()
            .flat_map(|key| array(&message[key]))
            .map(registry::packet_address)
            .collect::<outreach_canon::Result<_>>()?;
        let (task, context) = context(rows, message)?;
        let subject = text::compact(
            s(fallback(&[&message["subject"], &string("(no subject)")])),
            usize::MAX,
        );
        let summary = if context.is_empty() {
            subject
        } else {
            format!("{subject} — {context}")
        };
        for recipient in recipients {
            if ignored.contains(&recipient) || system(&recipient) {
                continue;
            }
            let Some(person) = people
                .iter()
                .find(|p| array(&p["emails"]).iter().any(|a| *a == recipient))
            else {
                continue;
            };
            if recorded(person, contacts, &recipient, id, at)? {
                continue;
            }
            let mut evidence = object([
                ("source", string("sent-events")),
                ("internetMessageId", string(id)),
                ("recipient", string(&recipient)),
            ]);
            for key in ["id", "conversationId"] {
                if truth(&message[key]) {
                    evidence[key] = message[key].clone();
                }
            }
            if !account.is_empty() {
                evidence["account"] = string(account);
                evidence["from"] = string(registry::packet_address(&message["from"])?);
            }
            if truth(&task) {
                evidence["task_id"] = task.clone();
            }
            let mut event = object([
                ("kind", string("sent")),
                ("at", message["sentDateTime"].clone()),
                ("summary", string(&summary)),
                ("evidence", string(json(&evidence, false)?)),
            ]);
            event["id"] = string(format!(
                "event-{}",
                &sha256(json(&event, false)?.as_bytes())[..16]
            ));
            let target = contacts["contacts"]
                .as_array_mut()
                .and_then(|rs| {
                    rs.iter_mut()
                        .find(|r| r["id"] == person["records"][0]["id"])
                })
                .ok_or_else(|| outreach_canon::Error::new("Unknown contact"))?;
            if !target["hub"].is_object() {
                target["hub"] = object([]);
            }
            if !target["hub"]["events"].is_array() {
                target["hub"]["events"] = Value::Array(vec![]);
            }
            if let Some(events) = target["hub"]["events"].as_array_mut() {
                events.push(event);
            }
            count += 1;
        }
    }
    Ok(count)
}
#[derive(Default)]
pub struct Options {
    pub max_pages: Option<usize>,
    pub absent: bool,
}
pub fn reconcile(
    replay: &mut Replay<'_>,
    contacts: &mut Value,
    mailbox: &mut impl Mailbox,
    store: &mut impl ContactTables,
    port: &mut impl DeliveryPorts,
    until: &str,
    options: &Options,
) -> StageResult<Value> {
    if options.absent || truth(&replay.environment.config["paused"]) {
        return Ok(object([
            ("status", string("disabled")),
            ("events", 0.into()),
            ("pages", 0.into()),
        ]));
    }
    let account = mailbox.verify_owner()?;
    let settings = mailbox.settings();
    check(
        settings["reconciled"] == true,
        "Mailbox boundary is not reconciled",
    )?;
    let since = s(&settings["live_since"]).to_owned();
    let start = instant_micros(&string(&since));
    let end = instant_micros(&string(until));
    check(
        start.is_some() && end.is_some() && start <= end,
        "Invalid Sent live window",
    )?;
    let mut cursor = replay.state("sent-events");
    let pending = replay.state("sent-events-pending");
    if pending["cursor"]["since"] == since {
        cursor = pending["cursor"].clone();
    } else if cursor["since"] != since || !truth(&cursor["next_link"]) {
        cursor = object([("since", string(&since)), ("until", string(until))]);
    }
    check(
        instant_micros(&cursor["until"]).is_some_and(|t| Some(t) <= end),
        "Sent cursor is in the future",
    )?;
    let mut ignored: BTreeSet<_> = array(&replay.environment.config["mailbox"]["team_addresses"])
        .iter()
        .map(|v| casefold(s(v).trim()))
        .collect();
    ignored.insert(account.clone());
    let mut status = "idle";
    let mut events = 0;
    let mut pages = 0;
    for _ in 0..options.max_pages.unwrap_or(2) {
        if !port.allow("sent-events", "sent-page") {
            status = "deferred";
            break;
        }
        let page = mailbox.sent_events_page(&since, s(&cursor["until"]), &cursor["next_link"])?;
        events += record_page(
            contacts,
            &replay.environment.tables,
            &page.messages,
            replay.rows("tasks"),
            &ignored,
            (
                start.unwrap_or(0),
                instant_micros(&cursor["until"]).unwrap_or(0),
            ),
            &account,
        )?;
        store.render(contacts)?;
        let contacts_digest = outreach_canon::digest(contacts)?;
        replay.set_state(
            "sent-events-pending",
            object([
                ("cursor", cursor.clone()),
                ("contacts_digest", string(&contacts_digest)),
            ]),
        )?;
        port.save_forced(&replay.tables)?;
        store.commit_tables(Persisted::saved(
            ContactIntent {
                cursor: cursor.clone(),
                contacts_digest,
            },
            port,
            &replay.tables,
        )?)?;
        replay.clear_state("sent-events-pending")?;
        cursor["next_link"] = page.next_link.clone();
        replay.set_state("sent-events", cursor.clone())?;
        port.save(&replay.tables)?;
        pages += 1;
        if !truth(&page.next_link) {
            break;
        }
    }
    if events > 0 {
        status = "updated";
    }
    Ok(object([
        ("status", string(status)),
        ("events", events.into()),
        ("pages", pages.into()),
    ]))
}

/// Coordinator entry point: isolate the failure, retain a content-free status.
pub fn advance(
    replay: &mut Replay<'_>,
    contacts: &mut Value,
    mailbox: &mut impl Mailbox,
    store: &mut impl ContactTables,
    port: &mut impl DeliveryPorts,
    until: &str,
    options: &Options,
) -> StageResult<Value> {
    let report = match reconcile(replay, contacts, mailbox, store, port, until, options) {
        Ok(report) => report,
        Err(error) => object([
            ("status", string("error")),
            ("error", error.summary["type"].clone()),
        ]),
    };
    replay.set_state("sent-events-status", report.clone())?;
    Ok(report)
}
