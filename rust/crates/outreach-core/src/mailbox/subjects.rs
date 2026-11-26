use super::*;
use crate::{
    ingest::Replay,
    mathesis_delivery::{DeliveryPorts, StageError},
};
pub(crate) fn stamp(micros: i64) -> String {
    let day = micros.div_euclid(86_400_000_000) + 719163;
    let mut year = (day / 366 + 1).max(1);
    while crate::validation::iso_day(&format!("{:04}-01-01", year + 1)).is_some_and(|d| d <= day) {
        year += 1;
    }
    let mut month = 1;
    while month < 12
        && crate::validation::iso_day(&format!("{year:04}-{:02}-01", month + 1))
            .is_some_and(|d| d <= day)
    {
        month += 1;
    }
    let date =
        day - crate::validation::iso_day(&format!("{year:04}-{month:02}-01")).unwrap_or(day) + 1;
    let seconds = micros.div_euclid(1_000_000).rem_euclid(86400);
    let fraction = micros.rem_euclid(1_000_000);
    format!(
        "{year:04}-{month:02}-{date:02}T{:02}:{:02}:{:02}{}Z",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60,
        if fraction == 0 {
            String::new()
        } else {
            format!(".{fraction:06}")
        }
    )
}
fn compact(m: &Value) -> Value {
    let mut row = object([]);
    for k in [
        "id",
        "internetMessageId",
        "conversationId",
        "subject",
        "sentDateTime",
    ] {
        if let Some(v) = m.get(k)
            && (v.is_null() || v.is_string())
        {
            row[k] = v.clone();
        }
    }
    for k in ["toRecipients", "ccRecipients"] {
        row[k] = Value::Array(
            array(&m[k])
                .iter()
                .map(|v| object([("emailAddress", object([("address", string(address(v)))]))]))
                .collect(),
        );
    }
    row
}
fn refresh(
    r: &mut Replay<'_>,
    box_: &mut impl Mailbox,
    start: i64,
    needed: i64,
    now: i64,
) -> StageResult<(i64, Value)> {
    let saved = r.state("mailbox-sent-subjects");
    let account = crate::registry::address(&box_.settings()["account"]);
    let mut subjects = object([]);
    let mut through = now;
    let mut built = now;
    let mut incremental = false;
    if !saved.is_null() {
        let lower = crate::gates::instant_micros(&saved["from"]);
        let upper = crate::gates::instant_micros(&saved["through"]);
        let old_built = crate::gates::instant_micros(&saved["built_at"]);
        if let (Some(lower), Some(upper), Some(old_built)) = (lower, upper, old_built)
            && saved["account"] == account
            && lower <= start
            && lower <= upper
            && upper <= now.max(needed)
            && old_built <= upper
            && old_built <= now
            && now < old_built + 86400 * 1_000_000
            && saved["subjects"].is_object()
        {
            let mut valid = true;
            for (subject, rows) in saved["subjects"].as_object().into_iter().flatten() {
                if !rows.is_array() {
                    valid = false;
                    break;
                }
                for row in array(rows) {
                    let at = instant(&row["sentDateTime"]);
                    if subject.is_empty()
                        || triage::normalized(s(&row["subject"])) != *subject
                        || at < lower
                        || at > upper
                        || *row != compact(row)
                    {
                        valid = false;
                        break;
                    }
                    if at >= start {
                        if subjects.get(subject).is_none() {
                            subjects[subject] = Value::Array(vec![]);
                        }
                        if let Some(v) = subjects[subject].as_array_mut() {
                            v.push(row.clone());
                        }
                    }
                }
            }
            if valid {
                through = upper;
                built = old_built;
                incremental = true;
            } else {
                subjects = object([]);
            }
        }
    }
    if !incremental {
        through = now.max(needed);
        subjects = box_.read(Read::SentSubjects {
            start,
            end: through,
            incremental: false,
        })?;
    } else if through < needed {
        let fresh_through = now.max(needed);
        let fresh = box_.read(Read::SentSubjects {
            start: through,
            end: fresh_through,
            incremental: true,
        })?;
        for (k, v) in fresh.as_object().into_iter().flatten() {
            let mut rows = array(v).to_vec();
            rows.extend_from_slice(array(&subjects[k]));
            subjects[k] = Value::Array(rows);
        }
        through = fresh_through;
    }
    let payload = object([
        ("account", string(account)),
        ("from", string(stamp(start))),
        ("through", string(stamp(through))),
        ("built_at", string(stamp(built))),
        ("subjects", subjects.clone()),
    ]);
    if saved != payload {
        r.set_state("mailbox-sent-subjects", payload)?;
    }
    Ok((through, subjects))
}
fn registered(sent: &Value, persons: &[Value], account: &str) -> Option<Value> {
    for value in array(&sent["toRecipients"])
        .iter()
        .chain(array(&sent["ccRecipients"]))
    {
        let a = address(value);
        if a.is_empty() || a == account {
            continue;
        }
        if let Some(p) = persons
            .iter()
            .find(|p| array(&p["emails"]).iter().any(|v| v == &a))
        {
            return Some(p.clone());
        }
    }
    None
}
pub(crate) fn requirement(m: &Value) -> Option<&'static str> {
    let subject = s(&m["subject"]);
    let prefix =
        crate::regex(r"(?i)^\s*(?:(?:re|fw|fwd|aw|回复|答复)\s*[:：]\s*)+").expect("regex");
    if !prefix.is_match(subject) {
        return Some(
            "Sent subject matches but the inbound subject lacks a reply or forward prefix",
        );
    }
    let normalized = triage::normalized(subject);
    if normalized.chars().count() < 16
        || crate::regex(r"\b\w+\b")
            .expect("regex")
            .find_iter(&normalized)
            .count()
            < 3
    {
        return Some("Sent subject matches but has fewer than 16 characters or 3 words");
    }
    None
}
pub(crate) fn sent_match(
    m: &Value,
    persons: &[Value],
    account: &str,
    candidates: &[Value],
) -> (Option<Value>, Value) {
    if candidates.is_empty() {
        return (None, Value::Null);
    }
    let received = instant(&m["receivedDateTime"]);
    let previous: Vec<_> = candidates
        .iter()
        .filter(|v| instant(&v["sent"]["sentDateTime"]) <= received)
        .collect();
    let details = |reason: &str| (Some(triage::category("linked-reply", reason)), Value::Null);
    if previous.is_empty() {
        return details("Matching Sent messages postdate the inbound message");
    }
    let registered_owner = |owner: &Value| {
        persons
            .iter()
            .find(|p| p["id"] == *owner || array(&p["records"]).iter().any(|v| v["id"] == *owner))
            .cloned()
    };
    let mut unique = vec![];
    let mut keys = vec![];
    for (i, record) in previous.iter().enumerate() {
        if received - instant(&record["sent"]["sentDateTime"]) > 90 * 86400 * 1_000_000 {
            continue;
        }
        let id = crate::fallback(&[&record["sent"]["internetMessageId"], &record["sent"]["id"]]);
        let key = if truth(id) {
            s(id).to_owned()
        } else {
            format!("index:{i}")
        };
        if let Some(index) = keys.iter().position(|v| *v == key) {
            let old: &Value = unique[index];
            if registered_owner(&record["owner"]).is_some()
                && registered_owner(&old["owner"]).is_none()
            {
                unique[index] = record;
            }
        } else {
            keys.push(key);
            unique.push(*record);
        }
    }
    if unique.len() != 1 {
        return details(if unique.is_empty() {
            "Matching Sent message is outside the 90 days before receipt"
        } else {
            "Sent subject matches multiple Sent messages"
        });
    }
    let record = unique[0];
    let sent = &record["sent"];
    let Some(person) =
        registered_owner(&record["owner"]).or_else(|| registered(sent, persons, account))
    else {
        return details("Sent subject matches but no registered owner was found");
    };
    let mut details = triage::category("linked-reply", "Subject matches one Sent message");
    details["contact_id"] = person["id"].clone();
    if truth(&record["task_id"]) {
        details["task_id"] = record["task_id"].clone();
    }
    if let Some(reason) = requirement(m) {
        details["category_reason"] = string(reason);
        return (Some(details), Value::Null);
    }
    (
        Some(details),
        object([
            ("match", string("subject-within-90-days")),
            ("sent", sent.clone()),
            ("person", person),
            ("task_id", record["task_id"].clone()),
        ]),
    )
}
pub struct Index {
    value: Option<std::result::Result<(i64, i64, Value), Value>>,
}
impl Index {
    pub fn new() -> Self {
        Self { value: None }
    }
    pub fn link(
        &mut self,
        selection: (&Value, &str, &str),
        context: (&[Value], &mut Value, &[Value]),
        r: &mut Replay<'_>,
        box_: &mut impl Mailbox,
        ports: &mut impl DeliveryPorts,
    ) -> StageResult<Value> {
        let (m, datetime, identity) = selection;
        let (persons, subjects, incoming) = context;
        let account = crate::registry::address(&box_.settings()["account"]);
        let h = headers(m);
        for name in ["in-reply-to", "references"] {
            if let Some(text) = h.get(name) {
                for value in crate::regex(r"<[^<>\s]+>|[^\s,<>]+@[^\s,<>]+")?.find_iter(text) {
                    let id = value.as_str();
                    let id = if id.starts_with('<') {
                        id.to_owned()
                    } else {
                        format!("<{}>", id.trim_end_matches(['.', ',', ';']))
                    };
                    let sent = box_.read(Read::SentById(id))?;
                    if sent.is_null() {
                        continue;
                    }
                    if let Some(person) = registered(&sent, persons, &account) {
                        return Ok(object([
                            ("match", string("internet-message-id")),
                            ("sent", sent),
                            ("person", person),
                        ]));
                    }
                }
            }
        }
        if requirement(m).is_some() {
            return Ok(Value::Null);
        }
        let received = instant(&m["receivedDateTime"]);
        let now = instant(&string(datetime));
        if self.value.is_none() {
            if !ports.allow("mailbox-intake", identity) {
                return Err(StageError {
                    summary: object([
                        ("type", string("SentSubjectsDeferred")),
                        ("intake_reason", string("tick budget; retained")),
                        ("message", string("Tick time budget exhausted")),
                    ]),
                });
            }
            let mut dates = vec![received];
            dates.extend(
                incoming
                    .iter()
                    .filter(|m| truth(&m["receivedDateTime"]) && requirement(m).is_none())
                    .map(|m| instant(&m["receivedDateTime"])),
            );
            let start = dates.iter().min().copied().unwrap_or(received) - 90 * 86400 * 1_000_000;
            let needed = dates.iter().max().copied().unwrap_or(received);
            self.value = Some(match refresh(r, box_, start, needed, now) {
                Ok((through, subjects)) => Ok((start, through, subjects)),
                Err(e) => Err(e.summary),
            });
        }
        let (start, through, index) = match self.value.as_ref() {
            Some(Ok(value)) => value,
            Some(Err(summary)) => {
                return Err(StageError {
                    summary: summary.clone(),
                });
            }
            None => return Err(outreach_canon::Error::new("Missing subject index").into()),
        };
        if received > *through || received - 90 * 86400 * 1_000_000 < *start {
            return Err(StageError {
                summary: object([
                    ("type", string("SentSubjectsDeferred")),
                    ("intake_reason", string("thread changed; retained")),
                    (
                        "message",
                        string("Thread changed outside the Sent subject scan window"),
                    ),
                ]),
            });
        }
        let subject = triage::normalized(s(&m["subject"]));
        if subjects.get(&subject).is_none() {
            subjects[&subject] = Value::Array(vec![]);
        }
        if let Some(rows) = subjects[&subject].as_array_mut() {
            rows.extend(
                array(&index[&subject])
                    .iter()
                    .filter(|v| {
                        instant(&v["sentDateTime"]) >= received - 90 * 86400 * 1_000_000
                            && instant(&v["sentDateTime"]) <= received
                    })
                    .map(|v| object([("sent", v.clone())])),
            );
        }
        Ok(sent_match(m, persons, &account, array(&subjects[subject])).1)
    }
}
