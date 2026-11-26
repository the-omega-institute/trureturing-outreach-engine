use super::*;
use crate::{formalization_requests::check, ingest::Replay};
fn record<'a>(r: &'a Replay<'_>, id: &Value) -> StageResult<&'a Value> {
    array(&r.environment.tables["threads"]["lines"])
        .iter()
        .find(|v| v["id"] == *id)
        .ok_or_else(|| {
            outreach_canon::Error::new(format!("Unknown historical thread: {}", s(id))).into()
        })
}
fn participants(record: &Value) -> BTreeSet<String> {
    std::iter::once(&record["correspondent"])
        .chain(array(&record["co_correspondents"]))
        .filter(|v| truth(&v["email"]))
        .map(|v| crate::registry::address(&v["email"]))
        .collect()
}
fn user_owned(record: &Value, r: &Replay<'_>) -> bool {
    record["ball"] == r.environment.config["identity"]["owner_labels"][0]
        || truth(&record["user_decision_ever"])
        || truth(&record["user_decisions"])
}
pub fn resolve(
    row: &Value,
    r: &mut Replay<'_>,
    box_: &mut impl Mailbox,
    inputs: &mut impl attachments::Inputs,
    absent: bool,
) -> StageResult<Value> {
    let i = &row["request"]["inputs"];
    if !["reply", "research"].contains(&s(&row["kind"]))
        || !truth(&i["thread_ref"])
        || truth(&i["thread"])
    {
        return Ok(Value::Null);
    }
    crate::gates::thread(&row["request"], None, r.environment)?;
    check(!absent, "Configured GraphMailbox unavailable")?;
    let account = s(&box_.read(Read::VerifyOwner)?).to_owned();
    check(
        box_.settings()["reconciled"] == true,
        "Mailbox boundary is not reconciled",
    )?;
    let reference = &i["thread_ref"];
    let ids = &reference["internetMessageIds"];
    check(
        ids.is_array() && truth(ids),
        "No known internetMessageId; reconcile latest ROWIDs in the existing thread record",
    )?;
    let mut located = vec![];
    let mut conversations = vec![];
    let mut unresolved = vec![];
    for id in array(ids) {
        let conversation = box_.read(Read::Locate(id.clone()))?;
        if conversation.is_null() {
            unresolved.push(id.clone());
        } else if !conversations.contains(&conversation) {
            conversations.push(conversation.clone());
        }
        located.push((id.clone(), conversation));
    }
    check(
        !conversations.is_empty(),
        "No known incoming internetMessageId resolves to a conversation",
    )?;
    check(
        conversations.len() <= 3,
        "Historical selection exceeds 3 conversations",
    )?;
    let mut threads = vec![];
    for conversation in conversations {
        let messages = box_.read(Read::Thread(s(&conversation).into()))?;
        check(
            truth(&messages)
                && array(&messages)
                    .iter()
                    .all(|m| m["conversationId"] == conversation),
            "Incomplete or mismatched current conversation",
        )?;
        check(
            array(&messages).len() <= 100,
            "Historical conversation exceeds 100 messages",
        )?;
        check(
            located
                .iter()
                .filter(|(_, c)| *c == conversation)
                .all(|(id, _)| {
                    array(&messages)
                        .iter()
                        .any(|m| m["internetMessageId"] == *id)
                }),
            "Current conversation omitted a referenced message",
        )?;
        threads.push((conversation, messages));
    }
    let all: Vec<_> = threads
        .iter()
        .flat_map(|(_, v)| array(v).to_vec())
        .collect();
    if threads.len() == 1 {
        check(
            !all.iter().any(|m| truth(&m["isDraft"])),
            "A server draft already exists; preserve the user draft",
        )?;
    }
    let incoming: Vec<_> = all
        .iter()
        .filter(|m| !truth(&m["isDraft"]) && address(&m["from"]) != account)
        .collect();
    check(
        incoming.iter().any(|m| {
            located
                .iter()
                .any(|(id, c)| *id == m["internetMessageId"] && *c == m["conversationId"])
        }),
        "No known incoming internetMessageId resolves to a conversation",
    )?;
    let latest = incoming
        .iter()
        .rev()
        .max_by_key(|m| instant(&m["receivedDateTime"]))
        .copied()
        .ok_or_else(|| outreach_canon::Error::new("Missing incoming"))?;
    let conversation = latest["conversationId"].clone();
    let mut thread = threads
        .iter()
        .find(|(c, _)| *c == conversation)
        .map(|(_, v)| v.clone())
        .unwrap_or(Value::Null);
    check(
        !array(&thread).iter().any(|m| truth(&m["isDraft"])),
        "A server draft already exists; preserve the user draft",
    )?;
    let sent: Vec<_> = all
        .iter()
        .filter(|m| !truth(&m["isDraft"]) && address(&m["from"]) == account)
        .collect();
    let inbound = truth(&reference["inbound"]);
    if !inbound {
        check(
            !sent.is_empty(),
            "Historical thread needs actual incoming and Sent evidence",
        )?;
    }
    let latest_sent = sent
        .iter()
        .filter(|m| m["conversationId"] == conversation)
        .max_by_key(|m| instant(&m["sentDateTime"]));
    let frozen = if i["thread_record"].is_null() && !inbound {
        record(r, &reference["thread_id"])?
    } else {
        &i["thread_record"]
    };
    if !frozen.is_null() {
        check(
            participants(frozen).contains(&address(&latest["from"])),
            "Latest sender is outside the selected thread correspondents",
        )?;
    }
    let persons = crate::registry::people(&r.environment.tables)?;
    for a in contacts(&all, &account) {
        check(
            !crate::registry::preflight(&a, "", &r.environment.tables, &persons)?
                .contains(&"table-3-refusal-or-stop-boundary".into()),
            &format!("Current conversation has an active table-3 refusal: {a}"),
        )?;
    }
    let actual = contacts(array(&thread), &account);
    let mut owners: Vec<_> = persons
        .iter()
        .filter(|p| array(&p["emails"]).iter().any(|a| actual.contains(s(a))))
        .map(|p| p["id"].clone())
        .collect();
    owners.sort_by(|a, b| s(a).cmp(s(b)));
    if !inbound {
        check(
            owners
                .iter()
                .all(|v| *v == row["request"]["owner"] || array(&i["owner_ids"]).contains(v)),
            "Current conversation adds registered owners; stage a new thread selection",
        )?;
    } else {
        for owner in &owners {
            let mut request = row["request"].clone();
            request["owner"] = owner.clone();
            request["inputs"]["owner_ids"] = Value::Array(vec![]);
            crate::gates::owner(&request, r.environment)?;
        }
    }
    let link = object([
        ("thread_id", conversation.clone()),
        (
            "latest_inbound_message_id",
            latest["internetMessageId"].clone(),
        ),
    ]);
    let mut listings = object([]);
    attachments::preserve(&mut thread, r, &link, box_, inputs, &mut listings, true)?;
    let owned = if inbound {
        false
    } else {
        user_owned(record(r, &reference["thread_id"])?, r)
    };
    let mut resolved = link;
    resolved["thread"] = thread;
    resolved["owner_ids"] = Value::Array(owners);
    resolved["user_decision_ever"] = (truth(&i["user_decision_ever"]) || owned).into();
    resolved["mailbox_observation"] = object([
        ("email", string(address(&latest["from"]))),
        ("at", latest["receivedDateTime"].clone()),
        ("message_id", latest["internetMessageId"].clone()),
        (
            "evidence_ref",
            string(format!("graph:thread:{}", s(&latest["id"]))),
        ),
    ]);
    if let Some(sent) = latest_sent {
        resolved["latest_sent_message_id"] = sent["internetMessageId"].clone();
    }
    let introduced = crate::delivery::mathesis::introduced(&Value::Array(all.clone()), &account);
    resolved["mathesis_introduced"] = if truth(&introduced) {
        introduced
    } else {
        false.into()
    };
    if let Some(evidence) = sent.iter().max_by_key(|m| instant(&m["sentDateTime"])) {
        resolved["sent_evidence"] = object([
            ("thread_id", evidence["conversationId"].clone()),
            ("message_id", evidence["internetMessageId"].clone()),
        ]);
    }
    resolved["unresolved_message_ids"] = Value::Array(unresolved);
    let mut prior = vec![];
    for (c, mut messages) in threads {
        if c == conversation {
            continue;
        }
        attachments::preserve(
            &mut messages,
            r,
            &object([("thread_id", c.clone())]),
            box_,
            inputs,
            &mut listings,
            true,
        )?;
        prior.push(object([
            ("thread_id", c),
            ("read_only", true.into()),
            ("thread", messages),
        ]));
    }
    resolved["prior_context"] = Value::Array(prior);
    Ok(resolved)
}
