use super::*;
use crate::formalization_requests::compact;
use crate::registry::name_key;
pub const ANNOTATIONS: [&str; 7] = [
    "category",
    "category_reason",
    "contact_id",
    "task_id",
    "manuscript_id",
    "joint_manuscript",
    "coauthor_contact_ids",
];
pub fn category(label: &str, reason: &str) -> Value {
    object([
        ("category", string(label)),
        ("category_reason", string(reason)),
    ])
}
fn matches(pattern: &str, text: &str) -> bool {
    crate::regex(pattern).is_ok_and(|r| r.is_match(text))
}
pub fn confirmation(subject: &str) -> bool {
    matches(
        r"(?i)\b(?:confirm|verify)\b|\b(?:authorship|co[- ]authorship)\s+(?:confirmation|verification)\b|\b(?:confirmation|verification)\s+(?:required|request(?:ed)?)\b|\b(?:request|reminder)\b[^\n]{0,80}\b(?:confirmation|verification)\b|请(?:确认|验证)|(?:确认|验证)(?:共同)?作者",
        subject,
    )
}
fn editorial_action(subject: &str) -> bool {
    matches(
        r"(?i)\b(?:decisions?|confirm(?: your)? authorship|co[- ]authorship|requires? action|please edit|incomplete|revisions?|revis(?:e|ed)|reject(?:ed|ion)?|accept(?:ed|ance)?|proofs?)\b",
        subject,
    )
}
pub fn normalized(subject: &str) -> String {
    casefold(&compact(
        &crate::regex(r"(?i)^\s*(?:(?:re|fw|fwd|aw|回复|答复)\s*[:：]\s*)+")
            .expect("regex")
            .replace(subject, ""),
    ))
}
pub fn bulk(sender: &str, headers: &BTreeMap<String, String>) -> Option<String> {
    for label in [
        "list-unsubscribe",
        "list-id",
        "x-auto-response-suppress",
        "feedback-id",
    ] {
        if headers.contains_key(label) {
            return Some(format!("Automation header {label}"));
        }
    }
    if headers
        .get("precedence")
        .is_some_and(|v| ["bulk", "list", "junk"].contains(&casefold(v).as_str()))
    {
        return Some("Bulk precedence header".into());
    }
    if headers
        .get("auto-submitted")
        .is_some_and(|v| casefold(v.split(';').next().unwrap_or("").trim()) != "no")
    {
        return Some("Auto-Submitted header".into());
    }
    let (local, domain) = sender.split_once('@').unwrap_or((sender, ""));
    let automated = [
        "notification",
        "notifications",
        "support",
        "info",
        "documents",
        "billing",
        "newsletter",
        "newsletters",
        "news",
        "alert",
        "alerts",
        "digest",
        "marketing",
        "loyalty",
        "service",
        "mailer",
        "bounce",
        "admin",
        "system",
        "team",
        "hello",
        "notify",
        "announcements",
        "announcement",
    ];
    if local == "intellisys"
        || matches(
            r"(?i)(?:^|[._+-])(?:do[._+-]?not|no|auto)[._+-]?reply|(?:do[._+-]?not|no|auto)[._+-]?reply(?:$|[._+-])",
            local,
        )
        || local.split(['.', '_', '+', '-']).any(|token| {
            automated
                .iter()
                .any(|word| token.starts_with(word) || token.ends_with(word))
        })
    {
        return Some("Automated sender address".into());
    }
    if ["loyalty", "comms", "news", "notifications"]
        .contains(&domain.trim_end_matches('.').split('.').next().unwrap_or(""))
    {
        return Some("Automated sender subdomain".into());
    }
    None
}
fn domain_matches(domain: &str, suffixes: &Value) -> bool {
    array(suffixes)
        .iter()
        .any(|v| domain == s(v) || domain.ends_with(&format!(".{}", s(v))))
}
fn names(text: &str) -> Vec<String> {
    let mut result = crate::regex(r"[^\W_]+")
        .expect("regex")
        .find_iter(&name_key(text))
        .map(|m| m.as_str().to_owned())
        .collect::<Vec<_>>();
    result.sort();
    result
}
fn manuscript_ids(text: &str) -> Vec<String> {
    let mut ids =
        crate::regex(r"(?i)\bmanuscript\s*(?:id|no\.?|number|#)\s*[:#]?\s*([\w]+(?:[./-][\w]+)*)")
            .expect("regex")
            .captures_iter(text)
            .filter(|c| c[1].chars().any(|c| c.is_numeric()))
            .map(|c| c[1].into())
            .collect::<Vec<_>>();
    let r=crate::regex(r"(?i)(?:arxiv:\s*\d{4}\.\d{4,5}(?:v\d+)?|[a-z][a-z0-9]*(?:-[a-z0-9]+)*-\d[a-z0-9]*(?:[.-][a-z0-9]+)*)\b").expect("regex");
    for m in r.find_iter(text) {
        if m.start() == 0
            || !text[..m.start()]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_alphanumeric() || "_.-".contains(c))
        {
            ids.push(m.as_str().replace(' ', ""));
        }
    }
    ids
}
pub struct Triage {
    pub persons: Vec<Value>,
    pub subjects: Value,
    account: String,
    config: Value,
    tables: Value,
    written: BTreeMap<String, Vec<Value>>,
    manuscripts: BTreeMap<String, BTreeSet<String>>,
}
impl Triage {
    pub fn new(
        persons: Vec<Value>,
        account: &str,
        config: &Value,
        tables: &Value,
        rows: &[Value],
        sent: &Value,
    ) -> Self {
        let mut t = Self {
            persons,
            account: account.into(),
            config: config.clone(),
            tables: tables.clone(),
            subjects: object([]),
            written: BTreeMap::new(),
            manuscripts: BTreeMap::new(),
        };
        for person in t.persons.clone() {
            for record in array(&person["records"]) {
                for event in array(&record["hub"]["events"]) {
                    let summary = s(&event["summary"]);
                    let mut ids = manuscript_ids(summary);
                    if truth(&event["manuscript_id"]) {
                        ids.push(s(&event["manuscript_id"]).into());
                    }
                    for id in ids {
                        let entry = t.manuscripts.entry(casefold(&id)).or_default();
                        if truth(&event["joint_manuscript"])
                            || matches(r"(?i)\b(?:joint manuscript|co-?authors?)\b", summary)
                        {
                            entry.insert(s(&person["id"]).into());
                        }
                        for p in array(&event["coauthor_contact_ids"]) {
                            entry.insert(s(p).into());
                        }
                    }
                    if event["kind"] != "sent" {
                        continue;
                    }
                    t.written
                        .entry(s(&person["id"]).into())
                        .or_default()
                        .push(event["at"].clone());
                    let source = if event["evidence"].is_string() {
                        outreach_canon::parse(s(&event["evidence"])).unwrap_or(Value::Null)
                    } else {
                        event["evidence"].clone()
                    };
                    if source["source"] == "sent-events"
                        && source.get("from").is_none_or(|v| *v == account)
                    {
                        let recipient = source.get("recipient").unwrap_or(&person["email"]);
                        t.index(
                            &object([
                                ("subject", string(summary.split(" — ").next().unwrap_or(""))),
                                ("sentDateTime", event["at"].clone()),
                                ("internetMessageId", source["internetMessageId"].clone()),
                                ("conversationId", source["conversationId"].clone()),
                                (
                                    "toRecipients",
                                    Value::Array(vec![object([(
                                        "emailAddress",
                                        object([("address", recipient.clone())]),
                                    )])]),
                                ),
                            ]),
                            source["task_id"].clone(),
                            Value::Null,
                        );
                    }
                }
                for thread in array(&record["hub"]["threads"]) {
                    if truth(&thread["last_out"]["date"]) {
                        t.written
                            .entry(s(&person["id"]).into())
                            .or_default()
                            .push(thread["last_out"]["date"].clone());
                    }
                }
            }
        }
        for m in array(sent) {
            if !truth(&m["isDraft"]) && address(&m["from"]) == account {
                t.index(m, Value::Null, Value::Null);
            }
        }
        for row in rows {
            let d = &row["data"];
            let receipt = &d["sent_receipt"];
            let packet = crate::fallback(&[&d["packet_override"], &d["result"]["packet"]]);
            if !["reply", "firstcontact"].contains(&s(&row["kind"]))
                || !truth(&receipt["at"])
                || !packet.is_object()
            {
                continue;
            }
            let mut m = object([
                ("subject", packet["subject"].clone()),
                ("sentDateTime", receipt["at"].clone()),
                ("internetMessageId", receipt["internetMessageId"].clone()),
            ]);
            if !truth(&m["internetMessageId"])
                && s(&receipt["evidence_ref"]).starts_with("graph:sent:")
            {
                m["internetMessageId"] = string(s(&receipt["evidence_ref"])[11..].to_owned());
            }
            for key in ["to", "cc"] {
                m[format!("{key}Recipients")] = Value::Array(
                    array(&packet[key])
                        .iter()
                        .map(|a| object([("emailAddress", object([("address", a.clone())]))]))
                        .collect(),
                );
            }
            t.index(&m, row["id"].clone(), row["owner"].clone());
        }
        for paper in array(&tables["submissions"]["papers"]) {
            let ids = paper["coauthor_contact_ids"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            let mut authors: BTreeSet<String> = ids.iter().map(|v| s(v).into()).collect();
            for p in &t.persons {
                if array(&paper["authors"])
                    .iter()
                    .any(|a| name_key(s(a)) == name_key(s(&p["name"])))
                {
                    authors.insert(s(&p["id"]).into());
                }
            }
            for field in ["manuscript_id", "id", "subject", "title"] {
                let v = s(&paper[field]);
                let ids = if field == "manuscript_id" {
                    vec![v.into()]
                } else {
                    manuscript_ids(v)
                };
                for id in ids {
                    t.manuscripts
                        .entry(casefold(&id))
                        .or_default()
                        .extend(authors.clone());
                }
            }
        }
        t
    }
    fn index(&mut self, message: &Value, task: Value, owner: Value) {
        if !truth(&message["sentDateTime"]) {
            return;
        }
        let subject = normalized(s(&message["subject"]));
        if !subject.is_empty() {
            if self.subjects.get(&subject).is_none() {
                self.subjects[&subject] = Value::Array(vec![]);
            }
            if let Some(rows) = self.subjects[&subject].as_array_mut() {
                rows.push(object([
                    ("sent", message.clone()),
                    ("task_id", task),
                    ("owner", owner.clone()),
                ]));
            }
        }
        let addresses = recipients(message);
        for p in &self.persons {
            if array(&p["emails"]).iter().any(|a| addresses.contains(s(a))) || p["id"] == owner {
                self.written
                    .entry(s(&p["id"]).into())
                    .or_default()
                    .push(message["sentDateTime"].clone());
            }
        }
    }
    pub fn blocked(
        &self,
        addresses: &BTreeSet<String>,
        owners: &[Value],
        sender_name: &str,
    ) -> bool {
        let mut addresses = addresses.clone();
        for p in &self.persons {
            if owners.contains(&p["id"]) {
                addresses.extend(array(&p["emails"]).iter().map(|v| s(v).to_owned()));
            }
        }
        array(&self.tables["refusals"]["refusals"])
            .iter()
            .filter(|v| truth(&v["active"]))
            .any(|r| {
                array(&r["emails"]).iter().any(|a| addresses.contains(s(a)))
                    || (!sender_name.is_empty() && name_key(sender_name) == name_key(s(&r["name"])))
            })
    }
    pub fn classify(&self, m: &Value, reason: Option<&str>) -> Value {
        let sender = address(&m["from"]);
        let subject = s(&m["subject"]);
        let domain = sender.split('@').nth(1).unwrap_or("").trim_end_matches('.');
        if reason
            .filter(|r| !r.is_empty())
            .unwrap_or(s(&m["intake_reason"]))
            == "owner-controlled"
        {
            return category("owner-controlled", "Owner-controlled conversation");
        }
        if let Some(p) = self
            .persons
            .iter()
            .find(|p| array(&p["emails"]).iter().any(|a| a == &sender) && sender != self.account)
        {
            let mut v = category("known-contact", "Sender address is registered");
            v["contact_id"] = p["id"].clone();
            return v;
        }
        let home = self
            .config
            .get("home_domains")
            .unwrap_or(&self.config["mailbox"]["home_domains"]);
        let home = Value::Array(
            array(home)
                .iter()
                .map(|v| string(casefold(s(v).trim()).trim_end_matches('.')))
                .collect(),
        );
        if domain_matches(domain, &home) {
            return category(
                "institution",
                "Sender is from the mailbox owner’s institution",
            );
        }
        if domain_matches(domain, &self.config["mailbox"]["editorial_domains"]) {
            if !editorial_action(subject) && !confirmation(subject) {
                return category("editorial-info", "Journal or preprint notification");
            }
            let mut v = category(
                "editorial-action",
                "Journal or preprint request or decision",
            );
            let ids = manuscript_ids(subject);
            if let Some(id) = ids
                .iter()
                .find(|id| self.manuscripts.contains_key(&casefold(id)))
                .or(ids.first())
            {
                v["manuscript_id"] = string(id);
                if let Some(coauthors) = self.manuscripts.get(&casefold(id)) {
                    v["joint_manuscript"] = true.into();
                    if !coauthors.is_empty() {
                        v["coauthor_contact_ids"] =
                            Value::Array(coauthors.iter().map(string).collect());
                    }
                }
            }
            return v;
        }
        let services = self.config["mailbox"]
            .get("service_attention_domains")
            .cloned()
            .unwrap_or(Value::Array(vec![string("overleaf.com")]));
        if domain_matches(domain, &services) {
            return category(
                "service-notice",
                "Sender is from a configured collaboration service",
            );
        }
        if let Some(reason) = bulk(&sender, &headers(m)) {
            return category("bulk", &reason);
        }
        if let Some(v) = self.subject_match(m) {
            return v;
        }
        let named = names(s(&m["from"]["emailAddress"]["name"]));
        if named.len() >= 2 {
            let candidates: Vec<_> = self
                .persons
                .iter()
                .filter(|p| {
                    names(s(&p["name"])) == named
                        && self.written.get(s(&p["id"])).is_some_and(|v| {
                            v.iter()
                                .any(|v| instant(v) <= instant(&m["receivedDateTime"]))
                        })
                })
                .collect();
            if candidates.len() == 1 {
                let mut v = category(
                    "likely-contact",
                    "Display name matches one registered prior correspondent",
                );
                v["contact_id"] = candidates[0]["id"].clone();
                return v;
            }
        }
        if !sender.is_empty()
            && (self.account.is_empty()
                || recipients(m).contains(&self.account)
                || headers(m).iter().any(|(k, v)| {
                    ["in-reply-to", "references"].contains(&k.as_str()) && v.contains('@')
                }))
        {
            category("person", "Unmapped sender addressed this mailbox directly")
        } else {
            category(
                "bulk",
                "No direct recipient or registered sender in mailbox metadata",
            )
        }
    }
    fn subject_match(&self, m: &Value) -> Option<Value> {
        super::subjects::sent_match(
            m,
            &self.persons,
            &self.account,
            array(&self.subjects[normalized(s(&m["subject"]))]),
        )
        .0
    }
}
