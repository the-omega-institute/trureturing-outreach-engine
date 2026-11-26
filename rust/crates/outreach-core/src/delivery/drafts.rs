use super::*;
fn stamp(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| gates::stamp(v))
}
fn saved_at(d: &Value, receipt: &Value) -> Option<f64> {
    if let Some(t) = stamp(fallback(&[&receipt["at"], &receipt["saved_at"]])) {
        return Some(t);
    }
    for revision in array(&d["draft_revisions"]).iter().rev() {
        if revision["new_draft_receipt"]["remote_id"] == receipt["remote_id"] {
            return stamp(&revision["old_draft_deleted"]["at"]);
        }
    }
    stamp(&d["draft_intent"]["at"])
}
fn remote(box_: &mut impl Mailbox, id: &str) -> StageResult<Value> {
    check(!id.trim().is_empty(), "Missing draft remote ID")?;
    match box_.observe(MailboxRead::Message(id.into())) {
        Err(e) if e.status() == Some(404) || e.summary["type"] == "KeyError" => Ok(Value::Null),
        result => result,
    }
}
fn live_draft(box_: &mut impl Mailbox, id: &str, after: bool) -> StageResult<Value> {
    let draft = match remote(box_, id) {
        Err(e) if after && e.status() == Some(403) => Value::Null,
        result => result?,
    };
    if !draft.is_null()
        && box_.supports(MailboxFeature::DraftsFolder)
        && truth(&draft["parentFolderId"])
        && draft["parentFolderId"] != box_.observe(MailboxRead::DraftsFolder)?
    {
        return Ok(Value::Null);
    }
    Ok(draft)
}
fn header_ids(message: &Value) -> BTreeSet<String> {
    let mut headers = std::collections::BTreeMap::new();
    for h in array(&message["internetMessageHeaders"]) {
        headers.insert(casefold(s(&h["name"])), s(&h["value"]));
    }
    let ids = crate::regex(r"<[^<>\s]+>|[^\s,<>]+@[^\s,<>]+").expect("constant regex");
    let mut result = BTreeSet::new();
    for name in ["in-reply-to", "references"] {
        for m in ids.find_iter(headers.get(name).copied().unwrap_or("")) {
            let raw = m.as_str();
            result.insert(if raw.starts_with('<') {
                raw.to_owned()
            } else {
                format!("<{}>", raw.trim_end_matches(['.', ',', ';']))
            });
        }
    }
    result
}
fn replies(draft: &Value, p: &Value) -> bool {
    if draft["conversationId"] == p["thread_id"] {
        return true;
    }
    let values: Vec<_> = array(&draft["internetMessageHeaders"])
        .iter()
        .filter(|h| casefold(s(&h["name"])) == "in-reply-to")
        .map(|h| s(&h["value"]).trim())
        .collect();
    values == vec![s(&p["latest_inbound_message_id"])]
}
fn header_key(draft: &Value) -> Value {
    let values: Vec<_> = array(&draft["internetMessageHeaders"])
        .iter()
        .filter(|h| casefold(s(&h["name"])) == "x-outreach-attempt")
        .collect();
    if values.len() == 1 && values[0]["value"].is_string() && truth(&values[0]["value"]) {
        values[0]["value"].clone()
    } else {
        Value::Null
    }
}
fn verify(
    box_: &mut impl Mailbox,
    draft: &Value,
    p: &Value,
    receipt: &Value,
    thread: bool,
) -> StageResult<Value> {
    let reason = "Owner edited the draft; revision not applied";
    check(draft.is_object() && draft["isDraft"] == true, reason)?;
    check(
        box_.supports(MailboxFeature::VerifyOwner),
        "Mailbox adapter cannot verify the owner; revision not applied",
    )?;
    check(
        casefold(s(&box_.observe(MailboxRead::VerifyOwner)?)) == casefold(s(&p["sender"])),
        reason,
    )?;
    check(
        box_.supports(MailboxFeature::Matches)
            && truth(&box_.observe(MailboxRead::Matches {
                draft: draft.clone(),
                packet: p.clone(),
            })?),
        reason,
    )?;
    if p["kind"] == "reply" && thread {
        check(replies(draft, p), reason)?;
    }
    let key = if truth(&receipt["attempt_key"]) {
        receipt["attempt_key"].clone()
    } else {
        header_key(draft)
    };
    if box_.supports(MailboxFeature::OwnDraft) {
        check(
            truth(&key),
            "Owner draft attempt identity is missing; revision not applied",
        )?;
        check(
            box_.observe(MailboxRead::OwnDraft {
                draft: draft.clone(),
                packet: p.clone(),
                key: s(&key).into(),
            })? != false,
            reason,
        )?;
    }
    Ok(key)
}
fn plain(body: &str, html: bool) -> String {
    let mut body = body.replace("\r\n", "\n");
    if html {
        if let Some(m) = crate::regex(
            r#"(?is)<blockquote\b|<[^>]+(?:class=["'][^"']*gmail_quote|id=["']divrplyfwdmsg)"#,
        )
        .expect("constant regex")
        .find(&body)
        {
            body.truncate(m.start());
        }
        body = crate::regex("(?is)</?(?:div|p|li|tr|table|blockquote)\\b[^>]*>|<br\\b[^>]*>")
            .expect("constant regex")
            .replace_all(&body, "\n")
            .into_owned();
        body = crate::regex("(?s)<[^>]+>")
            .expect("constant regex")
            .replace_all(&body, "")
            .into_owned();
        for (from, to) in [
            ("&nbsp;", " "),
            ("&lt;", "<"),
            ("&gt;", ">"),
            ("&amp;", "&"),
            ("&quot;", "\""),
        ] {
            body = body.replace(from, to);
        }
    }
    let mut lines = vec![];
    let source: Vec<_> = body.lines().collect();
    for (i, line) in source.iter().enumerate() {
        if line.trim().to_lowercase().starts_with("from:")
            && source.iter().skip(i + 1).take(4).any(|next| {
                ["sent:", "date:", "to:", "cc:", "subject:"]
                    .iter()
                    .any(|prefix| next.trim().to_lowercase().starts_with(prefix))
            })
        {
            break;
        }
        if line.trim_start().starts_with('>')
            || line.trim() == "-----Original Message-----"
            || line.trim().starts_with("On ") && line.trim().ends_with("wrote:")
        {
            break;
        }
        lines.push(*line);
    }
    lines
        .join("\n")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
fn sentences(text: &str) -> Vec<String> {
    let mut result = vec![];
    let mut rest = text;
    if rest.to_lowercase().starts_with("dear ")
        && let Some(i) = rest.find(", ")
    {
        result.push(rest[..i + 1].into());
        rest = rest[i + 2..].trim_start();
    }
    let chars: Vec<_> = rest.char_indices().collect();
    let mut start = 0;
    for (n, (i, c)) in chars.iter().enumerate() {
        let end = i + c.len_utf8();
        let next = chars.get(n + 1).map(|(_, c)| *c);
        let cjk = "。！？".contains(*c);
        let latin = ".!?".contains(*c) && next.is_some_and(char::is_whitespace);
        if cjk || latin {
            let sentence = rest[start..end].trim();
            let lower = sentence.to_lowercase();
            if ["mr.", "dr.", "ms.", "prof.", "e.g.", "i.e.", "et al."]
                .iter()
                .any(|x| lower.ends_with(x))
            {
                continue;
            }
            if !sentence.is_empty() {
                result.push(sentence.into());
            }
            start = end;
        }
    }
    if !rest[start..].trim().is_empty() {
        result.push(rest[start..].trim().into());
    }
    result
}
fn edit(p: &Value, m: &Value) -> Value {
    let b = if m["uniqueBody"].is_object() {
        &m["uniqueBody"]
    } else {
        &m["body"]
    };
    let content = if b.is_string() {
        s(b)
    } else if b["content"].is_string() {
        s(&b["content"])
    } else {
        return Value::Null;
    };
    let before = plain(s(&p["body"]), false);
    let after = plain(content, casefold(s(&b["contentType"])) == "html");
    if before == after {
        return Value::Null;
    }
    let a = sentences(&before);
    let b = sentences(&after);
    // SequenceMatcher's longest matching blocks, with earliest ties. Public
    // output retains only changed sentence pairs, capped like draft_status.
    fn blocks(
        a: &[String],
        b: &[String],
        ao: usize,
        bo: usize,
        out: &mut Vec<(usize, usize, usize)>,
    ) {
        let mut best = (0, 0, 0);
        for i in 0..a.len() {
            for j in 0..b.len() {
                let mut n = 0;
                while i + n < a.len() && j + n < b.len() && a[i + n] == b[j + n] {
                    n += 1;
                }
                if n > best.2 {
                    best = (i, j, n);
                }
            }
        }
        let (i, j, n) = best;
        if n == 0 {
            return;
        }
        blocks(&a[..i], &b[..j], ao, bo, out);
        out.push((ao + i, bo + j, n));
        blocks(&a[i + n..], &b[j + n..], ao + i + n, bo + j + n, out);
    }
    let mut matched = vec![];
    blocks(&a, &b, 0, 0, &mut matched);
    matched.push((a.len(), b.len(), 0));
    let (mut ai, mut bi) = (0, 0);
    let mut changed = vec![];
    for (i, j, n) in matched {
        let limit = (i - ai).max(j - bi).min(12 - changed.len());
        for k in 0..limit {
            changed.push(object([
                (
                    "before",
                    string(
                        a.get(ai + k)
                            .filter(|_| ai + k < i)
                            .map(|s| s.chars().take(600).collect::<String>())
                            .unwrap_or_default(),
                    ),
                ),
                (
                    "after",
                    string(
                        b.get(bi + k)
                            .filter(|_| bi + k < j)
                            .map(|s| s.chars().take(600).collect::<String>())
                            .unwrap_or_default(),
                    ),
                ),
            ]));
        }
        ai = i + n;
        bi = j + n;
        if changed.len() == 12 {
            break;
        }
    }
    object([
        ("sent_at", m["sentDateTime"].clone()),
        ("changed", Value::Array(changed)),
    ])
}
pub(super) fn reconcile_owner(
    r: &mut Replay<'_>,
    box_: &mut impl Mailbox,
    ports: &mut impl DeliveryPorts,
    row: &Value,
    config: &Value,
) -> StageResult<bool> {
    let receipt = &row["data"]["draft_receipt"];
    if !truth(&receipt["remote_id"])
        || !box_.supports(MailboxFeature::Message)
        || !box_.supports(MailboxFeature::SentItems)
    {
        return Ok(false);
    }
    let since = saved_at(&row["data"], receipt)
        .or_else(|| stamp(&results::effective_inputs(row)["mailbox_observation"]["at"]));
    let Some(since) = since else {
        return Ok(false);
    };
    let draft = match remote(box_, s(&receipt["remote_id"])) {
        Err(e) if e.status() == Some(403) => Value::Null,
        result => result?,
    };
    let mut discarded = Value::Null;
    if !draft.is_null() && draft["isDraft"] != false {
        let parent = &draft["parentFolderId"];
        if !truth(parent) {
            return Ok(false);
        }
        let has_folder = box_.supports(MailboxFeature::DraftsFolder);
        if has_folder && *parent == box_.observe(MailboxRead::DraftsFolder)? {
            return Ok(false);
        }
        if box_.supports(MailboxFeature::DeletedFolder)
            && *parent == box_.observe(MailboxRead::DeletedFolder)?
        {
            discarded = object([
                ("folder", string("deleteditems")),
                ("parentFolderId", parent.clone()),
                (
                    "lastModifiedDateTime",
                    draft["lastModifiedDateTime"].clone(),
                ),
            ]);
        } else if !has_folder {
            return Ok(false);
        }
    }
    let p = packet(row);
    if !p.is_object() {
        return Ok(true);
    }
    let mut d = row["data"].clone();
    let conversation = fallback(&[&receipt["conversationId"], &p["thread_id"]]).clone();
    let team: BTreeSet<_> = array(&config["mailbox"]["team_addresses"])
        .iter()
        .map(|v| casefold(s(v).trim()))
        .collect();
    let ignored: BTreeSet<_> = array(&p["cc"])
        .iter()
        .map(|v| casefold(s(v).trim()))
        .filter(|a| team.contains(a))
        .collect();
    let expected: BTreeSet<_> = addresses(&p).difference(&ignored).cloned().collect();
    let mut claimed = BTreeSet::new();
    let mut owned = BTreeSet::new();
    let mut coordinator_retired = false;
    for other in r.rows("tasks") {
        if other["id"] == row["id"] {
            continue;
        }
        let od = &other["data"];
        if od["stale_draft_replacement"]["remote_id"] == receipt["remote_id"] {
            coordinator_retired = true;
        }
        if truth(&od["sent_receipt"]["remote_id"]) {
            claimed.insert(s(&od["sent_receipt"]["remote_id"]).to_owned());
        }
        if truth(&od["draft_receipt"]["conversationId"]) {
            owned.insert(s(&od["draft_receipt"]["conversationId"]).to_owned());
        }
        let op = packet(other);
        if truth(&op["thread_id"]) {
            owned.insert(s(&op["thread_id"]).to_owned());
        }
    }
    let sent = match box_.observe(MailboxRead::SentItems(Some(since))) {
        Err(e) if e.summary["type"] == "TypeError" => box_.observe(MailboxRead::SentItems(None))?,
        result => result?,
    };
    let mut matches = vec![];
    let mut weak = vec![];
    for m in array(&sent) {
        if truth(&m["isDraft"])
            || !truth(&m["id"])
            || !truth(&m["internetMessageId"])
            || !m["sentDateTime"].is_string()
        {
            continue;
        }
        let actual: BTreeSet<_> = array(&m["toRecipients"])
            .iter()
            .chain(array(&m["ccRecipients"]))
            .map(address)
            .filter(|a| !ignored.contains(a))
            .collect();
        if expected.is_empty()
            || actual != expected
            || stamp(&m["sentDateTime"]).is_none_or(|t| t <= since)
            || claimed.contains(s(&m["id"]))
        {
            continue;
        }
        if (truth(&conversation) && m["conversationId"] == conversation)
            || (truth(&p["thread_id"]) && m["conversationId"] == p["thread_id"])
            || (truth(&p["latest_inbound_message_id"])
                && header_ids(m).contains(s(&p["latest_inbound_message_id"])))
        {
            if !matches.iter().any(|x: &Value| x["id"] == m["id"]) {
                matches.push(m.clone());
            }
        } else if m["subject"] == p["subject"]
            && !owned.contains(s(&m["conversationId"]))
            && !weak.iter().any(|x: &Value| x["id"] == m["id"])
        {
            weak.push(m.clone());
        }
    }
    if matches.is_empty() {
        matches = weak;
    }
    remove(&mut d, "owner_draft_note");
    let mut state = s(&row["state"]).to_owned();
    if matches.len() == 1 {
        let m = &matches[0];
        let hash = digest(&p)?;
        let key = if truth(&receipt["attempt_key"]) {
            receipt["attempt_key"].clone()
        } else {
            string(digest(&Value::Array(vec![
                string("owner-outlook"),
                row["id"].clone(),
                receipt["remote_id"].clone(),
            ]))?)
        };
        d["sent_receipt"] = object([
            ("idempotency_key", key),
            ("content_hash", string(&hash)),
            ("remote_id", m["id"].clone()),
            (
                "evidence_ref",
                string(format!("graph:sent:{}", s(&m["internetMessageId"]))),
            ),
            ("at", m["sentDateTime"].clone()),
            ("sent_by", string("owner-outlook")),
        ]);
        if !truth(&d["owner_edit"])
            && truth(&conversation)
            && m["conversationId"] == conversation
            && saved_at(&d, receipt).is_some()
            && receipt["content_hash"] == hash
        {
            let edited = edit(&p, m);
            if truth(&edited) {
                d["owner_edit"] = edited;
            }
        }
        remove(&mut d, "owner_draft_status");
        remove(&mut d, "last_error");
        state = "verified".into();
    } else {
        if coordinator_retired {
            return Ok(true);
        }
        let intent = &d["draft_revision_intent"];
        if matches.is_empty()
            && (draft.is_null() || !discarded.is_null())
            && intent["phase"] == "delete-intent"
            && truth(&intent["new_remote_id"])
            && intent["old_remote_id"] == receipt["remote_id"]
        {
            return Ok(false);
        }
        if matches.is_empty() && !discarded.is_null() {
            d["owner_draft_status"] = string("discarded");
            discarded["remote_id"] = receipt["remote_id"].clone();
            discarded["observed_at"] = r.clock_value.clone();
            d["owner_draft_discard"] = discarded;
            remove(&mut d, "last_error");
            state = "owner-discarded".into();
        } else {
            d["owner_draft_status"] = string(if matches.is_empty() {
                "draft-missing-not-sent"
            } else {
                "sent-ambiguous"
            });
            if !matches.is_empty() {
                d["owner_draft_note"] = string(format!(
                    "{} Sent Items messages match the missing owner draft; reconcile manually",
                    matches.len()
                ));
            }
        }
    }
    if d != row["data"] || state != s(&row["state"]) {
        r.update(row, &state, d)?;
        ports.save(&r.tables)?;
    }
    Ok(true)
}
fn revision_error(
    r: &mut Replay<'_>,
    ports: &mut impl DeliveryPorts,
    id: &Value,
    text: &str,
) -> StageResult<Value> {
    let row = r.task(id)?;
    let mut d = row["data"].clone();
    d["last_error"] = string(text.chars().take(300).collect::<String>());
    r.update(&row, s(&row["state"]), d)?;
    ports.save(&r.tables)?;
    Ok(r.task(id)?)
}
fn preflight(
    box_: &mut impl Mailbox,
    registry: &mut impl MailRegistry,
    row: &Value,
    p: &Value,
    tables: &mut Value,
    old: &str,
) -> StageResult<Value> {
    let mut request = row["request"].clone();
    request["inputs"] = results::effective_inputs(row);
    let policy = match box_.observe(MailboxRead::Preflight {
        packet: p.clone(),
        request: request.clone(),
        allow_draft: Some(old.into()),
    }) {
        Err(e)
            if e.summary["type"] == "TypeError"
                && (error_text(&e).contains("allow_draft_id")
                    || error_text(&e).contains("unexpected keyword")) =>
        {
            box_.observe(MailboxRead::Preflight {
                packet: p.clone(),
                request,
                allow_draft: None,
            })?
        }
        result => result?,
    };
    register(registry, tables, &policy)?;
    for a in array(&p["to"]).iter().chain(array(&p["cc"])) {
        recipient(tables, s(a), true)?;
    }
    Ok(policy)
}
fn find(box_: &mut impl Mailbox, key: &str, at: f64) -> StageResult<Value> {
    if box_.supports(MailboxFeature::Find) {
        box_.attempt_started(at);
        let observed = box_.observe(MailboxRead::Find {
            folder: "drafts".into(),
            key: key.into(),
        })?;
        if !observed.is_null() {
            return Ok(observed);
        }
    }
    if box_.supports(MailboxFeature::Messages) {
        let rows = box_.observe(MailboxRead::Messages("drafts".into()))?;
        let matches: Vec<_> = array(&rows)
            .iter()
            .filter(|m| header_key(m) == key)
            .cloned()
            .collect();
        check(
            matches.len() <= 1,
            "Multiple replacement drafts share an attempt; reconcile manually",
        )?;
        return Ok(matches.first().cloned().unwrap_or(Value::Null));
    }
    Ok(Value::Null)
}
pub(super) fn revision(
    r: &mut Replay<'_>,
    box_: &mut impl Mailbox,
    registry: &mut impl MailRegistry,
    ports: &mut impl DeliveryPorts,
    row: &Value,
    config: &Value,
    tables: &mut Value,
) -> StageResult<Value> {
    let id = &row["id"];
    let d = &row["data"];
    let mut pending = list(&d["draft_revision_requests"]);
    let mut intent = d["draft_revision_intent"].clone();
    if intent.is_null() && pending.is_empty()
        || truth(&config["paused"])
        || !truth(&config["kinds"][s(&row["kind"])])
    {
        return Ok(row.clone());
    }
    transitions::source_gate(&row["request"], &d["result"], r.rows("tasks"))?;
    let current_packet = packet(row);
    let receipt = &d["draft_receipt"];
    if !current_packet.is_object() || !receipt.is_object() {
        return Ok(row.clone());
    }
    let event = if intent.is_null() {
        pending[0].clone()
    } else {
        intent.clone()
    };
    let old_hash = event["old_packet_hash"].clone();
    let new_hash = event["new_packet_hash"].clone();
    let new_packet = event["packet"].clone();
    if receipt["content_hash"] != old_hash
        || digest(&current_packet)? != old_hash
        || !new_packet.is_object()
        || digest(&new_packet)? != new_hash
    {
        return revision_error(
            r,
            ports,
            id,
            "Draft revision targets an obsolete draft receipt",
        );
    }
    let old_id = s(fallback(&[&event["old_remote_id"], &receipt["remote_id"]])).to_owned();
    if old_id.is_empty() {
        return revision_error(
            r,
            ports,
            id,
            "Owner draft receipt has no remote ID; revision not applied",
        );
    }
    if intent.is_null() {
        let old = remote(box_, &old_id)?;
        let old_key = match verify(box_, &old, &current_packet, receipt, false) {
            Ok(k) => k,
            Err(_) => {
                return revision_error(
                    r,
                    ports,
                    id,
                    "Owner edited the draft; revision not applied",
                );
            }
        };
        let policy = preflight(box_, registry, row, &new_packet, tables, &old_id)?;
        let mut identity = event.clone();
        remove(&mut identity, "event_id");
        let event_id = if truth(&event["event_id"]) {
            event["event_id"].clone()
        } else {
            string(digest(&identity)?)
        };
        intent = event.clone();
        for (k, v) in [
            ("event_id", event_id.clone()),
            ("old_remote_id", string(&old_id)),
            ("old_attempt_key", old_key),
            (
                "new_key",
                string(digest(&Value::Array(vec![
                    string("owner-draft-revision"),
                    id.clone(),
                    event_id,
                ]))?),
            ),
            ("at", r.clock_value.clone()),
            ("phase", string("intent")),
        ] {
            intent[k] = v;
        }
        if truth(&policy["recipient_exception"]) {
            intent["recipient_exception"] = policy["recipient_exception"].clone();
        }
        let current = r.task(id)?;
        let mut data = current["data"].clone();
        data["draft_revision_intent"] = intent.clone();
        r.update(&current, s(&current["state"]), data)?;
        ports.save_forced(&r.tables)?;
    }
    let key = s(&intent["new_key"]).to_owned();
    let known = truth(&intent["new_remote_id"]);
    let mut new_id = s(&intent["new_remote_id"]).to_owned();
    let mut observed = if known {
        remote(box_, &new_id)?
    } else {
        Value::Null
    };
    if observed.is_null() && !known {
        observed = find(box_, &key, num(&intent["at"]))?;
        if !observed.is_null() {
            new_id = s(&observed["id"]).into();
        }
    }
    if observed.is_null() && known {
        return Err(StageError {
            summary: object([
                ("type", string("ValueError")),
                (
                    "message",
                    string("Replacement draft is not yet readable; retain revision intent"),
                ),
            ]),
        });
    }
    if observed.is_null() {
        let old = remote(box_, &old_id)?;
        let old_receipt = object([
            (
                "attempt_key",
                fallback(&[&intent["old_attempt_key"], &receipt["attempt_key"]]).clone(),
            ),
            ("remote_id", string(&old_id)),
        ]);
        if verify(box_, &old, &current_packet, &old_receipt, false).is_err() {
            return revision_error(r, ports, id, "Owner edited the draft; revision not applied");
        }
        preflight(box_, registry, row, &new_packet, tables, &old_id)?;
        check(
            box_.supports(MailboxFeature::CreateDraft),
            "Mailbox adapter cannot create a replacement draft",
        )?;
        observed = box_.create_draft(Persisted::saved(
            ReplacementIntent {
                packet: new_packet.clone(),
                key: key.clone(),
                replacing: old_id.clone(),
            },
            ports,
            &r.tables,
        )?)?;
        new_id = s(&observed["id"]).into();
        check(!new_id.is_empty(), "Replacement draft has no remote ID")?;
        let current = r.task(id)?;
        intent["new_remote_id"] = string(&new_id);
        intent["phase"] = string("created");
        let mut data = current["data"].clone();
        data["draft_revision_intent"] = intent.clone();
        r.update(&current, s(&current["state"]), data)?;
        ports.save_forced(&r.tables)?;
    } else if observed.get("id").is_some() {
        new_id = s(&observed["id"]).into();
    }
    observed = remote(box_, &new_id)?;
    let new_receipt = object([
        ("attempt_key", string(&key)),
        ("remote_id", string(&new_id)),
    ]);
    if let Err(e) = verify(box_, &observed, &new_packet, &new_receipt, true) {
        return revision_error(r, ports, id, &error_text(&e));
    }
    for a in array(&new_packet["to"])
        .iter()
        .chain(array(&new_packet["cc"]))
    {
        recipient(tables, s(a), true)?;
    }
    let current = r.task(id)?;
    intent["new_remote_id"] = string(&new_id);
    intent["phase"] = string("delete-intent");
    let mut data = current["data"].clone();
    data["draft_revision_intent"] = intent.clone();
    r.update(&current, s(&current["state"]), data)?;
    ports.save_forced(&r.tables)?;
    let old = live_draft(box_, &old_id, false)?;
    if !old.is_null() {
        let old_receipt = object([
            (
                "attempt_key",
                fallback(&[&intent["old_attempt_key"], &receipt["attempt_key"]]).clone(),
            ),
            ("remote_id", string(&old_id)),
        ]);
        if verify(box_, &old, &current_packet, &old_receipt, false).is_err() {
            return revision_error(r, ports, id, "Owner edited the draft; revision not applied");
        }
        box_.delete_draft(Persisted::saved(
            DraftId {
                remote_id: old_id.clone(),
            },
            ports,
            &r.tables,
        )?)?;
    }
    check(
        live_draft(box_, &old_id, true)?.is_null(),
        "Old draft deletion is not yet confirmed",
    )?;
    let receipt = object([
        ("remote_id", string(&new_id)),
        ("attempt_key", string(&key)),
        ("evidence_ref", string(format!("graph:draft:{new_id}"))),
        ("content_hash", new_hash.clone()),
        ("saved_at", observed["createdDateTime"].clone()),
        ("conversationId", observed["conversationId"].clone()),
    ]);
    let evidence = object([
        ("remote_id", string(&old_id)),
        ("verified_absent", true.into()),
        (
            "evidence_ref",
            string(format!("graph:draft-deleted:{old_id}")),
        ),
        ("at", r.clock_value.clone()),
    ]);
    let mut record = object([
        ("reason", event["reason"].clone()),
        ("old_packet_hash", old_hash),
        ("new_packet_hash", new_hash),
        ("new_draft_receipt", receipt.clone()),
        ("old_draft_deleted", evidence),
    ]);
    if truth(&intent["recipient_exception"]) {
        record["recipient_exception"] = intent["recipient_exception"].clone();
    }
    pending.retain(|item| {
        let mut item = item.clone();
        let identity = item["event_id"].clone();
        remove(&mut item, "event_id");
        let identity = if truth(&identity) {
            identity
        } else {
            string(digest(&item).unwrap_or_default())
        };
        identity != intent["event_id"]
    });
    let current = r.task(id)?;
    let mut final_ = current["data"].clone();
    final_["draft_receipt"] = receipt;
    final_["packet_override"] = new_packet;
    let mut revisions = list(&final_["draft_revisions"]);
    revisions.push(record);
    final_["draft_revisions"] = Value::Array(revisions);
    final_["draft_revision_requests"] = Value::Array(pending);
    for k in ["draft_revision_intent", "last_error", "recipient_exception"] {
        remove(&mut final_, k);
    }
    if truth(&intent["recipient_exception"]) {
        final_["recipient_exception"] = intent["recipient_exception"].clone();
    }
    r.update(&current, "owner-draft", final_)?;
    ports.save(&r.tables)?;
    Ok(r.task(id)?)
}
fn verify_stale(
    box_: &mut impl Mailbox,
    draft: &Value,
    old: &Value,
    p: &Value,
) -> StageResult<Value> {
    check(
        draft["conversationId"] == p["thread_id"],
        "Stale draft conversation changed",
    )?;
    let receipt = &old["data"]["draft_receipt"];
    let previous = packet(old);
    check(
        previous.is_object(),
        "Stale draft has no saved packet to compare",
    )?;
    let created = &draft["createdDateTime"];
    let modified = &draft["lastModifiedDateTime"];
    if truth(created) && truth(modified) {
        check(
            stamp(created) == stamp(modified),
            "Stale draft was modified after creation",
        )?;
    } else {
        check(
            receipt["content_hash"] == digest(&previous)?,
            "Stale draft has no unchanged-content evidence",
        )?;
    }
    verify(box_, draft, &previous, receipt, false)?;
    Ok(object([
        ("content_hash", string(digest(&previous)?)),
        ("created_at", created.clone()),
        ("modified_at", modified.clone()),
    ]))
}
pub(super) fn retire_stale(
    r: &mut Replay<'_>,
    box_: &mut impl Mailbox,
    registry: &mut impl MailRegistry,
    ports: &mut impl DeliveryPorts,
    row: &Value,
    p: &Value,
    tables: &mut Value,
) -> StageResult<Value> {
    if p["kind"] != "reply" || !box_.supports(MailboxFeature::Thread) {
        return Ok(row.clone());
    }
    let rows = box_.observe(MailboxRead::Thread(s(&p["thread_id"]).into()))?;
    if !rows.is_array() {
        return Ok(row.clone());
    }
    let drafts: Vec<_> = array(&rows)
        .iter()
        .filter(|m| truth(&m["isDraft"]))
        .collect();
    let pending = &row["data"]["stale_draft_replacement"];
    let (old, mut draft, mut evidence) = if pending["phase"] == "move-intent" {
        (
            r.task(&pending["task_id"])?,
            live_draft(box_, s(&pending["remote_id"]), false)?,
            pending.clone(),
        )
    } else {
        if drafts.len() != 1 {
            return Ok(row.clone());
        }
        let candidates: Vec<_> = r
            .rows("tasks")
            .iter()
            .filter(|other| {
                other["id"] != row["id"]
                    && ["owner-draft", "revising", "superseded"].contains(&s(&other["state"]))
                    && !truth(&other["data"]["sent_receipt"])
                    && other["data"]["draft_receipt"]["remote_id"] == drafts[0]["id"]
                    && ingest::newer_reply(row, other)
            })
            .cloned()
            .collect();
        if candidates.len() != 1 {
            return Ok(row.clone());
        }
        let old = candidates[0].clone();
        let draft = live_draft(box_, s(&drafts[0]["id"]), false)?;
        let evidence = object([
            ("task_id", old["id"].clone()),
            (
                "remote_id",
                old["data"]["draft_receipt"]["remote_id"].clone(),
            ),
            ("destination", string("deleteditems")),
            ("at", r.clock_value.clone()),
            ("phase", string("move-intent")),
        ]);
        (old, draft, evidence)
    };
    check(
        ingest::newer_reply(row, &old) && !truth(&old["data"]["sent_receipt"]),
        "Stale draft no longer belongs to an older unsent reply",
    )?;
    fn verify_or_conflict(
        r: &mut Replay<'_>,
        box_: &mut impl Mailbox,
        ports: &mut impl DeliveryPorts,
        draft: &Value,
        old_and_row: (&Value, &Value),
        p: &Value,
        evidence: &Value,
    ) -> StageResult<Value> {
        let (old, row) = old_and_row;
        match verify_stale(box_, draft, old, p) {
            Ok(v) => Ok(v),
            Err(_) => {
                let current = r.task(&row["id"])?;
                let mut d = current["data"].clone();
                d["stale_draft_conflict"] = object([
                    (
                        "task_ids",
                        Value::Array(vec![old["id"].clone(), row["id"].clone()]),
                    ),
                    ("remote_id", evidence["remote_id"].clone()),
                ]);
                r.update(&current, s(&current["state"]), d)?;
                ports.save(&r.tables)?;
                Err(outreach_canon::Error::new(format!(
                    "Stale user draft must be merged with {}; preserve {}",
                    s(&row["id"]),
                    s(&old["id"])
                ))
                .into())
            }
        }
    }
    if !draft.is_null() {
        evidence["unchanged"] =
            verify_or_conflict(r, box_, ports, &draft, (&old, row), p, &evidence)?;
        preflight(box_, registry, row, p, tables, s(&evidence["remote_id"]))?;
        check(
            box_.supports(MailboxFeature::MoveDraft),
            "Mailbox adapter cannot move the stale draft to Deleted Items",
        )?;
        let mut data = row["data"].clone();
        data["stale_draft_replacement"] = evidence.clone();
        r.update(row, s(&row["state"]), data)?;
        ports.save_forced(&r.tables)?;
        draft = live_draft(box_, s(&evidence["remote_id"]), false)?;
        if !draft.is_null() {
            verify_or_conflict(r, box_, ports, &draft, (&old, row), p, &evidence)?;
            let moved = box_.move_draft(Persisted::saved(
                DraftId {
                    remote_id: s(&evidence["remote_id"]).into(),
                },
                ports,
                &r.tables,
            )?)?;
            evidence["moved_remote_id"] = moved["id"].clone();
        }
    }
    check(
        live_draft(box_, s(&evidence["remote_id"]), true)?.is_null(),
        "Stale draft move is not yet confirmed",
    )?;
    evidence["phase"] = string("moved");
    evidence["verified_absent"] = true.into();
    let old = r.task(&old["id"])?;
    let mut data = old["data"].clone();
    data["superseded_by"] = row["id"].clone();
    data["stale_draft_replacement"] = evidence.clone();
    r.update(&old, "superseded", data)?;
    let current = r.task(&row["id"])?;
    let mut data = current["data"].clone();
    data["stale_draft_replacement"] = evidence;
    r.update(&current, s(&current["state"]), data)?;
    ports.save_forced(&r.tables)?;
    Ok(r.task(&row["id"])?)
}
