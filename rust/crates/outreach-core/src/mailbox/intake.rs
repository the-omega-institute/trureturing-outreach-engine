use super::triage::{ANNOTATIONS, Triage, category};
use super::*;
use crate::{
    formalization_requests::{check, remove},
    ingest::Replay,
    mathesis_delivery::DeliveryPorts,
};
use outreach_canon::{Value, canonical, digest};
use unicode_normalization::UnicodeNormalization;
const UNREGISTERED: &str = "unregistered sender, no link to our Sent";
const ATTACHMENTS: &str = "unsupported thread attachments; operator selection needed";
const REFUSAL: &str = "active refusal; retained";
const AGED: &str = "older than 14 days";
const AGE: i64 = 14 * 86400 * 1_000_000;
const RECHECK: f64 = 21600.;
const UNRESOLVABLE: [&str; 3] = [
    "missing conversation identity",
    "missing Internet Message-ID",
    "Inbox message missing from conversation readback; retained",
];
pub struct Options {
    pub absent: bool,
    pub datetime: String,
    pub binding: Value,
    pub previous: Vec<String>,
}
struct Intake {
    now: i64,
    clock: Value,
    live: Option<i64>,
    retained: BTreeSet<String>,
    pending: Vec<(String, Value)>,
    covered: Vec<(String, Value)>,
    no_task: Value,
    holds: BTreeMap<String, Value>,
    notices: Vec<Value>,
    notice_keys: BTreeSet<String>,
    notice_count: i64,
    bounces: Vec<Value>,
    bounce_keys: BTreeSet<String>,
    triage: Triage,
    budget: i64,
    account: String,
    coverage: Option<BTreeMap<String, Vec<Value>>>,
}
fn key(m: &Value) -> String {
    canonical(&message_key(m)).unwrap_or_default()
}
fn insert(rows: &mut Vec<(String, Value)>, key: String, value: Value) {
    if let Some(old) = rows.iter_mut().find(|v| v.0 == key) {
        old.1 = value;
    } else {
        rows.push((key, value));
    }
}
fn number(v: &Value) -> f64 {
    v.as_f64().unwrap_or(0.)
}
impl Intake {
    fn in_window(&self, m: &Value) -> bool {
        let Some(received) = crate::gates::instant_micros(&m["receivedDateTime"]) else {
            return false;
        };
        if UNRESOLVABLE.contains(&s(&m["intake_reason"]))
            && m["expires_at"]
                .as_f64()
                .unwrap_or((received + AGE) as f64 / 1e6)
                < self.now as f64 / 1e6
        {
            return false;
        }
        received <= self.now
            && (self.live.is_none_or(|v| v <= received) || self.retained.contains(&key(m)))
    }
    fn remember(
        &mut self,
        m: &Value,
        reason: &str,
        details: Value,
        handled: bool,
        mut extra: Value,
    ) -> StageResult<()> {
        let k = key(m);
        if self.pending.iter().any(|v| v.0 == k) && !handled {
            return Ok(());
        }
        if UNRESOLVABLE.contains(&reason) {
            let expires = instant(&m["receivedDateTime"]) + AGE;
            if expires < self.now {
                return Ok(());
            }
            let mut v = object([("checked_at", self.clock.clone())]);
            merge(&mut v, &extra);
            v["expires_at"] = outreach_canon::float(expires as f64 / 1e6)?;
            extra = v;
        }
        let mut annotation = if details.get("category").is_some() {
            object([])
        } else {
            self.triage.classify(m, Some(reason))
        };
        if reason == ATTACHMENTS {
            annotation["category_reason"] =
                string("Unsupported thread attachments need operator selection");
        }
        let mut row = fields(m, &META[..10]);
        row["intake_reason"] = string(reason);
        merge(&mut row, &annotation);
        merge(&mut row, &details);
        merge(&mut row, &extra);
        remove(&mut self.no_task, &digest(&message_key(m))?);
        if handled {
            self.pending.retain(|v| v.0 != k);
            insert(&mut self.covered, k, row);
        } else {
            insert(&mut self.pending, k, row);
        }
        Ok(())
    }
    fn no_task(&mut self, m: &Value, reason: &str, evidence: Value) -> StageResult<()> {
        self.pending.retain(|v| v.0 != key(m));
        if instant(&m["receivedDateTime"]) >= self.now - AGE {
            let mut row = object([
                ("reason", string(reason)),
                ("receivedDateTime", m["receivedDateTime"].clone()),
                ("internetMessageId", m["internetMessageId"].clone()),
                ("remote_id", m["id"].clone()),
            ]);
            merge(&mut row, &evidence);
            self.no_task[&digest(&message_key(m))?] = row;
        }
        Ok(())
    }
    fn covered(&mut self, m: &Value, r: &Replay<'_>) -> StageResult<bool> {
        let Some(identity) = crate::ingest::message_id(&m["internetMessageId"]) else {
            return Ok(false);
        };
        let groups = self
            .coverage
            .get_or_insert_with(|| coverage(r.rows("tasks"), true));
        let Some(row) = groups.get(&identity).and_then(|v| v.first()) else {
            return Ok(false);
        };
        let task = row["task_id"].clone();
        let mut details = category(
            "covered",
            &format!("Inbound message is covered by task {}", s(&task)),
        );
        details["task_id"] = task;
        self.remember(m, "covered", details, true, object([]))?;
        Ok(true)
    }
    fn defer(&mut self, m: &Value, reason: &str, details: Value) -> StageResult<()> {
        if let Some(hold) = self.holds.get(s(&m["conversationId"])).cloned() {
            self.remember(m, ATTACHMENTS, object([]), false, hold)
        } else if truth(&m["intake_reason"]) && !m["checked_at"].is_null() {
            self.remember(
                m,
                s(&m["intake_reason"]),
                fields(m, &ANNOTATIONS),
                false,
                object([("checked_at", m["checked_at"].clone())]),
            )
        } else {
            self.remember(m, reason, details, false, object([]))
        }
    }
    fn excluded(&self, m: &Value) -> Option<Value> {
        let headers = headers(m);
        if triage::bulk(&address(&m["from"]), &headers).is_some() {
            let details = self.triage.classify(m, None);
            if [
                "bulk",
                "editorial-info",
                "editorial-action",
                "service-notice",
            ]
            .contains(&s(&details["category"]))
            {
                return Some(details);
            }
        }
        if headers
            .get("auto-submitted")
            .is_some_and(|v| casefold(v.split(';').next().unwrap_or("").trim()) != "no")
        {
            return Some(category("bulk", "Auto-Submitted header"));
        }
        None
    }
    fn notice(&mut self, m: &Value, count: bool) {
        if !self.notice_keys.insert(key(m)) {
            return;
        }
        self.notice_count += i64::from(count);
        let mut v = fields(
            m,
            &[
                "id",
                "internetMessageId",
                "conversationId",
                "from",
                "subject",
                "receivedDateTime",
            ],
        );
        v["intake_reason"] = string("machine_notice");
        self.notices.push(v);
    }
    fn bounce(&mut self, m: &Value) {
        if !self.bounce_keys.insert(key(m)) {
            return;
        }
        let mut v = fields(m, &META);
        let mut dsn = m["dsn_fields"]
            .as_object()
            .map(|_| m["dsn_fields"].clone())
            .unwrap_or(object([]));
        if let Ok(r) = crate::regex(
            r"(?im)^\s*(Final-Recipient|Original-Recipient|X-Failed-Recipients)\s*:\s*([^\n]+)",
        ) {
            for c in r.captures_iter(s(&m["body"]["content"])) {
                dsn[casefold(&c[1])] = string(c[2].trim());
            }
        }
        if truth(&dsn) {
            v["dsn_fields"] = dsn;
        }
        let recipient = bounce_recipient(m, &self.account)
            .map(string)
            .unwrap_or_else(|| {
                crate::fallback(&[&m["original_recipient"], &m["recipient"]]).clone()
            });
        v["intake_reason"] = string("bounce");
        v["reason"] = string("bounce");
        v["recipient"] = recipient.clone();
        v["original_recipient"] = recipient;
        self.bounces.push(v);
    }
    fn enrich(&self, m: &Value, box_: &mut impl Mailbox) -> StageResult<Value> {
        if m.get("body").is_some() || !box_.supports("research_thread") {
            return Ok(m.clone());
        }
        let thread = match box_.read(Read::Thread(s(&m["conversationId"]).into())) {
            Ok(thread) => thread,
            Err(e) if !e.is_interruption() => return Ok(m.clone()),
            Err(e) => return Err(e),
        };
        Ok(array(&thread)
            .iter()
            .find(|v| {
                (truth(&m["internetMessageId"]) && v["internetMessageId"] == m["internetMessageId"])
                    || (truth(&m["id"]) && v["id"] == m["id"])
            })
            .cloned()
            .unwrap_or(m.clone()))
    }
    fn stage(
        &mut self,
        selection: (&Value, &Value, Value, bool),
        r: &mut Replay<'_>,
        box_: &mut impl Mailbox,
        inputs: &mut impl attachments::Inputs,
        listings: &mut Value,
        uploads: &mut Vec<(Value, Value, Value)>,
    ) -> StageResult<()> {
        let (m, latest, mut request, upload) = selection;
        let i = &request["inputs"];
        let identity = object([
            ("thread_id", i["thread_id"].clone()),
            (
                "latest_inbound_message_id",
                i["latest_inbound_message_id"].clone(),
            ),
        ]);
        let result = attachments::preserve(
            &mut request["inputs"]["thread"],
            r,
            &identity,
            box_,
            inputs,
            listings,
            upload,
        );
        match result {
            Ok(true) => {
                r.apply("request-", &request, b"")?;
                self.budget -= 1;
                if self.coverage.is_some() {
                    self.coverage = Some(coverage(r.rows("tasks"), true));
                }
            }
            Ok(false) => uploads.push((m.clone(), latest.clone(), request)),
            Err(error) if error.summary["type"] == "UnsupportedThreadAttachments" => {
                let hold = object([
                    ("checked_at", self.clock.clone()),
                    ("attachment_issues", error.summary["attachments"].clone()),
                ]);
                self.holds
                    .insert(s(&identity["thread_id"]).into(), hold.clone());
                self.remember(m, ATTACHMENTS, object([]), false, hold)?;
            }
            Err(error) if !error.is_interruption() => {
                self.remember(
                    m,
                    "thread attachment preservation failed; retained",
                    object([(
                        "category_reason",
                        string(format!(
                            "Thread attachment preservation failed ({})",
                            s(&error.summary["type"])
                        )),
                    )]),
                    false,
                    object([]),
                )?;
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }
    fn uploads(
        &mut self,
        uploads: &mut Vec<(Value, Value, Value)>,
        r: &mut Replay<'_>,
        box_: &mut impl Mailbox,
        inputs: &mut impl attachments::Inputs,
        listings: &mut Value,
        ports: &mut impl DeliveryPorts,
    ) -> StageResult<()> {
        let pending = std::mem::take(uploads);
        for (m, latest, request) in pending {
            let ready = ports.allow("mailbox-intake", &digest(&message_key(&m))?);
            if self.covered(&latest, r)? {
                continue;
            }
            if !ready {
                self.defer(
                    &m,
                    "tick budget; retained",
                    object([("category_reason", string("Tick time budget exhausted"))]),
                )?;
            } else if self.budget <= 0 {
                self.defer(
                    &m,
                    "bounded; retained",
                    object([(
                        "category_reason",
                        string("New reply task limit reached for this tick"),
                    )]),
                )?;
            } else {
                self.stage(
                    (&m, &latest, request, true),
                    r,
                    box_,
                    inputs,
                    listings,
                    uploads,
                )?;
            }
        }
        Ok(())
    }
}
fn bounce_recipient(m: &Value, account: &str) -> Option<String> {
    let mut headers = m["dsn_fields"]
        .as_object()
        .map(|_| m["dsn_fields"].clone())
        .unwrap_or(object([]));
    for (k, v) in super::headers(m) {
        headers[k] = string(v);
    }
    let mut values = vec![];
    for k in [
        "final-recipient",
        "x-failed-recipients",
        "original-recipient",
    ] {
        if truth(&headers[k]) {
            values.push(s(&headers[k]).to_owned());
        }
    }
    let email = r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}";
    let text = s(&m["body"]["content"]);
    for pattern in [
        format!(
            r"(?im)^\s*(?:Final-Recipient|X-Failed-Recipients|Original-Recipient)\s*:\s*(?:rfc822\s*;\s*)?<?({email})"
        ),
        format!(r"(?i)Your message to\s+<?({email})>?\s+couldn['’]t be delivered"),
        format!(r"(?i)<?({email})>?\s+wasn['’]t found at"),
        format!(r"(?i)<?({email})>?[^\n]{{0,160}}\bUser unknown\b"),
    ] {
        if let Ok(r) = crate::regex(&pattern) {
            values.extend(r.captures_iter(text).map(|c| c[1].to_owned()));
        }
    }
    let re = crate::regex(&format!("(?i){email}")).ok()?;
    for value in values {
        for m_ in re.find_iter(&value) {
            let candidate = m_.as_str().trim_matches('.').to_lowercase();
            if candidate != account.to_lowercase()
                && candidate != address(&m["from"])
                && !machine(&candidate)
            {
                return Some(candidate);
            }
        }
    }
    None
}
fn history(person: &Value) -> bool {
    array(&person["records"]).iter().any(|r| {
        truth(&r["hub"]["threads"])
            || array(&r["hub"]["events"])
                .iter()
                .any(|e| e["kind"] == "sent")
    })
}
fn name_tokens(text: &str) -> Vec<String> {
    let normalized: String = casefold(text)
        .nfkd()
        .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
        .collect();
    let mut words: Vec<_> = crate::regex(r"[^\W\d_]+")
        .expect("regex")
        .find_iter(&normalized)
        .map(|m| m.as_str().to_owned())
        .collect();
    while words
        .first()
        .is_some_and(|v| ["professor", "prof", "dr"].contains(&v.as_str()))
    {
        words.remove(0);
    }
    words
}
fn names_match(left: &str, right: &str) -> bool {
    let (a, b) = (name_tokens(left), name_tokens(right));
    if a.is_empty() || b.is_empty() {
        return false;
    }
    if a == b {
        return true;
    }
    if a.len().min(b.len()) < 2 || a.first() != b.first() || a.last() != b.last() {
        return false;
    }
    fn middle(a: &[String], b: &[String]) -> bool {
        if a.is_empty() || b.is_empty() {
            return a.iter().chain(b).all(|v| v.chars().count() == 1);
        }
        let compatible = a[0] == b[0]
            || (a[0].chars().count().min(b[0].chars().count()) == 1
                && a[0].chars().next() == b[0].chars().next());
        (compatible && middle(&a[1..], &b[1..]))
            || (a.len() > b.len() && a[0].chars().count() == 1 && middle(&a[1..], b))
            || (b.len() > a.len() && b[0].chars().count() == 1 && middle(a, &b[1..]))
    }
    middle(&a[1..a.len() - 1], &b[1..b.len() - 1])
}
fn registrable(email: &str) -> Option<String> {
    let domain = email
        .rsplit_once('@')?
        .1
        .trim_end_matches('.')
        .to_lowercase();
    let labels: Vec<_> = domain.split('.').collect();
    if labels.len() < 2
        || labels.iter().any(|v| {
            !crate::regex(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$").is_ok_and(|r| r.is_match(v))
        })
    {
        return None;
    }
    let rules: BTreeSet<_> = include_str!("../../../../../managed/public_suffix_list.dat")
        .lines()
        .map(str::trim)
        .filter(|v| !v.is_empty() && !v.starts_with("//"))
        .collect();
    let mut length = 1;
    for i in 0..labels.len() {
        let suffix = labels[i..].join(".");
        if rules.contains(format!("!{suffix}").as_str()) {
            length = labels.len() - i - 1;
            break;
        }
        if rules.contains(suffix.as_str()) {
            length = length.max(labels.len() - i);
        }
        if i > 0 && rules.contains(format!("*.{suffix}").as_str()) {
            length = length.max(labels.len() - i + 1);
        }
    }
    (labels.len() > length).then(|| labels[labels.len() - length - 1..].join("."))
}
fn alias(m: &Value, sent: &Value, persons: &[Value], account: &str) -> Option<Value> {
    let mut recipients = recipients(sent);
    recipients.remove(account);
    let candidates: Vec<_> = persons
        .iter()
        .filter(|p| {
            array(&p["emails"])
                .iter()
                .any(|a| recipients.contains(s(a)))
        })
        .collect();
    let named: Vec<_> = candidates
        .iter()
        .filter(|p| names_match(s(&m["from"]["emailAddress"]["name"]), s(&p["name"])))
        .collect();
    if named.len() == 1 {
        return Some((*named[0]).clone());
    }
    if !named.is_empty() {
        return None;
    }
    let domain = registrable(&address(&m["from"]))?;
    let matching: BTreeSet<_> = recipients
        .iter()
        .filter(|a| registrable(a) == Some(domain.clone()))
        .cloned()
        .collect();
    let people: Vec<_> = candidates
        .iter()
        .filter(|p| array(&p["emails"]).iter().any(|a| matching.contains(s(a))))
        .collect();
    if people.len() == 1
        && matching
            .iter()
            .all(|a| array(&people[0]["emails"]).iter().any(|v| v == a))
    {
        Some((*people[0]).clone())
    } else {
        None
    }
}

pub fn advance(
    r: &mut Replay<'_>,
    box_: &mut impl Mailbox,
    inputs: &mut impl attachments::Inputs,
    ports: &mut impl DeliveryPorts,
    options: &Options,
) -> StageResult<()> {
    let cfg = &r.environment.config;
    if options.absent || truth(&cfg["paused"]) || !truth(&cfg["kinds"]["reply"]) {
        return Ok(());
    }
    let account = s(&box_.read(Read::VerifyOwner)?).to_owned();
    check(
        box_.settings()["reconciled"] == true,
        "Mailbox boundary is not reconciled",
    )?;
    let previous = r.state("mailbox");
    let live = box_.settings()["live_since"].clone();
    let replay = truth(&previous["through"]) && truth(&live) && !truth(&previous["intake_audited"]);
    let since = if replay {
        live.clone()
    } else {
        crate::fallback(&[&previous["through"], &live]).clone()
    };
    check(
        truth(&since),
        "Set the verified live Inbox start after reconciling the historical boundary",
    )?;
    let cutoff = options.datetime.replace("+00:00", "Z");
    let now = instant(&string(&cutoff));
    check(instant(&since) <= now, "Mailbox cursor is in the future")?;
    let inbox = box_.read(Read::Inbox {
        since: s(&since).into(),
        through: cutoff.clone(),
    })?;
    let mut incoming = array(&previous["unmapped"]).to_vec();
    incoming.extend_from_slice(array(&inbox));
    let persons = crate::registry::people(&r.environment.tables)?;
    let sent = if box_.supports("sent_snapshot") {
        box_.read(Read::SentSnapshot)?
    } else {
        Value::Array(vec![])
    };
    let triage = Triage::new(
        persons.clone(),
        &account,
        cfg,
        &r.environment.tables,
        r.rows("tasks"),
        &sent,
    );
    let mut no_task = object([]);
    let mut retained: Vec<_> = previous["no_task"]
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(k, v)| {
            crate::gates::instant_micros(&v["receivedDateTime"])
                .filter(|at| *at >= now - AGE && *at <= now)
                .map(|at| (at, k.clone(), v.clone()))
        })
        .collect();
    retained.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    for (_, key, value) in retained {
        no_task[key] = value;
    }
    let mut state = Intake {
        now,
        clock: r.clock_value.clone(),
        live: truth(&live).then(|| instant(&live)),
        retained: array(&previous["unmapped"]).iter().map(key).collect(),
        pending: vec![],
        covered: array(&previous["covered"])
            .iter()
            .map(|v| (key(v), v.clone()))
            .collect(),
        no_task,
        holds: array(&previous["unmapped"])
            .iter()
            .filter(|m| {
                m["intake_reason"] == ATTACHMENTS
                    && !m["checked_at"].is_null()
                    && m.get("attachment_issues").is_some()
            })
            .map(|m| {
                (
                    s(&m["conversationId"]).into(),
                    fields(m, &["checked_at", "attachment_issues"]),
                )
            })
            .collect(),
        notices: vec![],
        notice_keys: BTreeSet::new(),
        notice_count: previous["machine_notice_count"].as_i64().unwrap_or(0),
        bounces: vec![],
        bounce_keys: BTreeSet::new(),
        triage,
        budget: cfg["automation"]["max_new_tasks_per_tick"]
            .as_i64()
            .unwrap_or(4),
        account: account.clone(),
        coverage: None,
    };
    for m in array(&previous["machine_notices"]) {
        state.notice(m, false);
    }
    for m in array(&previous["bounces"]) {
        if failure(m) {
            let message = if truth(&m["original_recipient"]) || truth(&m["recipient"]) {
                m.clone()
            } else {
                state.enrich(m, box_)?
            };
            state.bounce(&message);
        } else {
            state.notice(m, true);
        }
    }
    if !options.previous.is_empty() {
        incoming.sort_by_key(|m| {
            options
                .previous
                .iter()
                .position(|v| digest(&message_key(m)).ok().as_ref() == Some(v))
                .unwrap_or(options.previous.len())
        });
    }
    let registered: BTreeSet<_> = persons
        .iter()
        .flat_map(|p| array(&p["emails"]).iter().map(|v| s(v).to_owned()))
        .collect();
    let mut first = vec![];
    let mut rest = vec![];
    for m in incoming {
        if state.in_window(&m) {
            if registered.contains(&address(&m["from"])) {
                first.push(m);
            } else {
                rest.push(m);
            }
        }
    }
    let boundary = first.len();
    let unregistered = rest.clone();
    first.extend(rest);
    let mut subject_index = super::subjects::Index::new();
    let mut uploads = vec![];
    let mut listings = object([]);
    let mut seen: BTreeMap<String, Value> = BTreeMap::new();
    for (index, m) in first.into_iter().enumerate() {
        if index == boundary {
            state.uploads(&mut uploads, r, box_, inputs, &mut listings, ports)?;
        }
        let k = message_key(&m);
        let identity = digest(&k)?;
        if state.no_task.get(&identity).is_some() || truth(&m["isDraft"]) {
            continue;
        }
        if !ports.allow("mailbox-intake", &identity) {
            state.defer(
                &m,
                "tick budget; retained",
                object([("category_reason", string("Tick time budget exhausted"))]),
            )?;
            continue;
        }
        if failure(&m) {
            let enriched = state.enrich(&m, box_)?;
            state.bounce(&enriched);
            continue;
        }
        if triage::confirmation(s(&m["subject"])) {
            let editorial = state.triage.classify(&m, Some(""));
            if editorial["category"] == "editorial-action" {
                state.remember(&m, "editorial-action", editorial, false, object([]))?;
                continue;
            }
        }
        if machine(&address(&m["from"])) {
            let counted = truth(&previous["through"])
                && !truth(&m["intake_reason"])
                && instant(&m["receivedDateTime"]) <= instant(&previous["through"]);
            state.notice(&m, !counted);
            continue;
        }
        if let Some(details) = state.excluded(&m) {
            state.remember(
                &m,
                if truth(&m["intake_reason"]) {
                    s(&m["intake_reason"])
                } else {
                    "automated or bulk mail"
                },
                details,
                false,
                object([]),
            )?;
            continue;
        }
        if address(&m["from"]) == account {
            state.no_task(&m, "mailbox owner is the sender", object([]))?;
            continue;
        }
        let reason = s(&m["intake_reason"]);
        let checked = number(&m["checked_at"]);
        if UNRESOLVABLE.contains(&reason) && !m["checked_at"].is_null() && r.now - checked < RECHECK
        {
            state.remember(
                &m,
                reason,
                fields(&m, &ANNOTATIONS),
                false,
                object([("checked_at", m["checked_at"].clone())]),
            )?;
            continue;
        }
        let conversation = s(&m["conversationId"]).to_owned();
        if conversation.is_empty() {
            state.remember(
                &m,
                "missing conversation identity",
                object([]),
                false,
                object([]),
            )?;
            continue;
        }
        if let Some(hold) = state.holds.get(&conversation).cloned()
            && r.now - number(&hold["checked_at"]) < RECHECK
        {
            state.remember(&m, ATTACHMENTS, object([]), false, hold)?;
            continue;
        }
        if let Some(selected) = seen.get(&conversation) {
            if !state.pending.iter().any(|v| v.0 == key(&m))
                && k != *selected
                && !state.covered(&m, r)?
            {
                state.no_task(
                    &m,
                    "another incoming message selected for this conversation",
                    object([("selected", selected.clone())]),
                )?;
            }
            continue;
        }
        if state.budget <= 0 {
            state.defer(
                &m,
                "bounded; retained",
                object([(
                    "category_reason",
                    string("New reply task limit reached for this tick"),
                )]),
            )?;
            continue;
        }
        if [UNREGISTERED, REFUSAL, AGED].contains(&reason)
            && !m["checked_at"].is_null()
            && r.now - checked < RECHECK
            && (reason != UNREGISTERED || !registered.contains(&address(&m["from"])))
        {
            if reason == UNREGISTERED
                && instant(&m["receivedDateTime"]) >= now - AGE
                && state.covered(&m, r)?
            {
                continue;
            }
            state.remember(
                &m,
                reason,
                fields(&m, &ANNOTATIONS),
                false,
                object([("checked_at", m["checked_at"].clone())]),
            )?;
            continue;
        }
        let thread = match box_.read(Read::Thread(conversation.clone())) {
            Ok(v) => v,
            Err(e) if e.summary["type"] == "UnsupportedThreadAttachments" => {
                let hold = object([
                    ("checked_at", r.clock_value.clone()),
                    ("attachment_issues", e.summary["attachments"].clone()),
                ]);
                state.holds.insert(conversation.clone(), hold.clone());
                state.remember(&m, ATTACHMENTS, object([]), false, hold)?;
                continue;
            }
            Err(e) => return Err(e),
        };
        state.holds.remove(&conversation);
        let actual: Vec<_> = array(&thread)
            .iter()
            .filter(|v| {
                !truth(&v["isDraft"])
                    && state.in_window(v)
                    && address(&v["from"]) != account
                    && !failure(v)
                    && !machine(&address(&v["from"]))
                    && state.excluded(v).is_none()
            })
            .cloned()
            .collect();
        if !actual.iter().any(|v| message_key(v) == k) {
            state.remember(
                &m,
                "Inbox message missing from conversation readback; retained",
                object([]),
                false,
                object([]),
            )?;
            continue;
        }
        let latest = actual
            .iter()
            .rev()
            .max_by_key(|v| instant(&v["receivedDateTime"]))
            .cloned()
            .unwrap_or(Value::Null);
        let latest_key = message_key(&latest);
        seen.insert(conversation.clone(), latest_key.clone());
        if k != latest_key && !state.covered(&m, r)? {
            state.no_task(
                &m,
                "newer incoming message selected for this conversation",
                object([("selected", latest_key)]),
            )?;
        }
        let sender = address(&latest["from"]);
        if !truth(&latest["internetMessageId"]) {
            state.remember(
                &latest,
                "missing Internet Message-ID",
                object([]),
                false,
                object([]),
            )?;
            continue;
        }
        if state.covered(&latest, r)? {
            continue;
        }
        let mut person = persons
            .iter()
            .find(|p| array(&p["emails"]).iter().any(|a| a == &sender))
            .cloned();
        let sent: Vec<_> = array(&thread)
            .iter()
            .filter(|v| !truth(&v["isDraft"]) && address(&v["from"]) == account)
            .collect();
        let mut linked = Value::Null;
        if person.is_none() && box_.supports("match_inbound_sent") {
            match if box_.supports("native_matching") {
                subject_index.link(
                    (&latest, &options.datetime, &identity),
                    (&persons, &mut state.triage.subjects, &unregistered),
                    r,
                    box_,
                    ports,
                )
            } else {
                box_.read(Read::MatchSent {
                    message: latest.clone(),
                    persons: persons.clone(),
                    datetime: options.datetime.clone(),
                    subjects: state.triage.subjects.clone(),
                })
            } {
                Ok(v) => {
                    linked = v;
                    if !linked.is_null() {
                        person = Some(linked["person"].clone());
                    }
                }
                Err(e) if e.summary["type"] == "SentSubjectsDeferred" => {
                    state.remember(
                        &latest,
                        s(&e.summary["intake_reason"]),
                        object([("category_reason", e.summary["message"].clone())]),
                        false,
                        object([]),
                    )?;
                    continue;
                }
                Err(e) if !e.is_interruption() => {}
                Err(e) => return Err(e),
            }
        }
        let known = person.as_ref().is_some_and(history) && sent.is_empty();
        if person.is_none() || (sent.is_empty() && !known && linked.is_null()) {
            if instant(&latest["receivedDateTime"]) >= now - AGE && state.covered(&latest, r)? {
                continue;
            }
            let details = if person.is_some() {
                object([(
                    "category_reason",
                    string("Registered sender has no current thread Sent mail or registry history"),
                )])
            } else {
                object([])
            };
            state.remember(
                &latest,
                UNREGISTERED,
                details,
                false,
                object([("checked_at", r.clock_value.clone())]),
            )?;
            continue;
        }
        let person = person.unwrap_or(Value::Null);
        let latest_sent = sent
            .iter()
            .rev()
            .max_by_key(|v| instant(&v["sentDateTime"]))
            .copied();
        if let Some(sent) = latest_sent
            && instant(&sent["sentDateTime"]) > instant(&latest["receivedDateTime"])
        {
            state.no_task(
                &latest,
                "answered by a later Sent Items message in this conversation",
                object([("sent", sent["internetMessageId"].clone())]),
            )?;
            continue;
        }
        let mut contacts = contacts(array(&thread), &account);
        let subject_link = linked["match"] == "subject-within-90-days";
        let linked_contacts = if linked.is_null() {
            BTreeSet::new()
        } else {
            recipients(&linked["sent"])
        };
        if subject_link {
            contacts.extend(linked_contacts.clone());
            contacts.remove(&account);
        }
        let mut owners: Vec<_> = persons
            .iter()
            .filter(|p| array(&p["emails"]).iter().any(|a| contacts.contains(s(a))))
            .map(|p| p["id"].clone())
            .collect();
        if !owners.contains(&person["id"]) {
            owners.push(person["id"].clone());
        }
        owners.sort_by(|a, b| s(a).cmp(s(b)));
        owners.dedup();
        let controlled = &r.environment.tables["decisions"]["owner_controlled_threads"];
        let owner_controlled = persons
            .iter()
            .filter(|p| owners.contains(&p["id"]))
            .any(|p| {
                array(controlled)
                    .iter()
                    .any(|v| casefold(s(v)) == casefold(s(&p["name"])))
                    || array(&p["records"]).iter().any(|v| {
                        v["owner_controlled"] == true || v["hub"]["owner_controlled"] == true
                    })
            });
        if owner_controlled {
            for inbound in &actual {
                if latest_sent.is_none_or(|v| {
                    instant(&inbound["receivedDateTime"]) >= instant(&v["sentDateTime"])
                }) {
                    state.remember(
                        inbound,
                        "owner-controlled",
                        object([]),
                        false,
                        object([("checked_at", r.clock_value.clone())]),
                    )?;
                }
            }
            continue;
        }
        let mut details = if !linked.is_null() {
            let mut v = category("linked-reply", "Linked to our Sent message");
            v["contact_id"] = person["id"].clone();
            if truth(&linked["task_id"]) {
                v["task_id"] = linked["task_id"].clone();
            }
            v
        } else {
            object([])
        };
        let mut all_contacts = contacts.clone();
        all_contacts.extend(linked_contacts);
        all_contacts.remove(&account);
        if state.triage.blocked(
            &all_contacts,
            &owners,
            s(&latest["from"]["emailAddress"]["name"]),
        ) {
            details["category_reason"] = string("Active table-3 refusal in the conversation");
            state.remember(
                &latest,
                REFUSAL,
                details,
                false,
                object([("checked_at", r.clock_value.clone())]),
            )?;
            continue;
        }
        if instant(&latest["receivedDateTime"]) < now - AGE {
            details["category_reason"] = string("Inbound message is older than 14 days");
            state.remember(
                &latest,
                AGED,
                details,
                false,
                object([("checked_at", r.clock_value.clone())]),
            )?;
            continue;
        }
        if state.covered(&latest, r)? {
            continue;
        }
        let link = object([
            ("thread_id", string(&conversation)),
            (
                "latest_inbound_message_id",
                latest["internetMessageId"].clone(),
            ),
        ]);
        let mut i = link.clone();
        i["automation_contract"] = 1.into();
        if let Some(sent) = latest_sent {
            i["latest_sent_message_id"] = sent["internetMessageId"].clone();
        }
        i["thread"] = thread;
        i["owner_ids"] = Value::Array(owners);
        i["user_decision_ever"] = false.into();
        let introduced = crate::delivery::mathesis::introduced(&i["thread"], &account);
        i["mathesis_introduced"] = if truth(&introduced) {
            introduced
        } else {
            false.into()
        };
        i["mailbox_observation"] = object([
            ("email", string(&sender)),
            ("at", latest["receivedDateTime"].clone()),
            ("message_id", latest["internetMessageId"].clone()),
            (
                "evidence_ref",
                string(format!("graph:inbox:{}", s(&latest["id"]))),
            ),
        ]);
        let mut instructions = format!(
            "Read the complete current thread. Decide whether any reply is needed. Complete useful research, ask genuine user decisions, preserve user drafts and classify confirmed independent publication for automatic manuscript preparation. Return exact routing. {}",
            include_str!("../reply_owner_rule.txt").trim_end_matches('\n')
        );
        if known {
            let context = if box_.supports("sent_messages_to") {
                match box_.read(Read::SentTo(sender.clone())) {
                    Ok(value) => value,
                    Err(e) if !e.is_interruption() => Value::Null,
                    Err(e) => return Err(e),
                }
            } else {
                Value::Null
            };
            i["prior_context"] = if truth(&context) {
                context
            } else {
                string("registry-only")
            };
            instructions = format!(
                "This is a new conversation from a known correspondent. The current conversation has no Sent item; use the prior context when present and the registry when it is registry-only. {instructions}"
            );
        }
        if !linked.is_null() {
            i["linked_sent_message_id"] = linked["sent"]["internetMessageId"].clone();
            if let Some(alias) = if subject_link {
                None
            } else {
                alias(&latest, &linked["sent"], &persons, &account)
            } {
                i["registry_proposal_hint"] = object([
                    ("email", string(&sender)),
                    ("name", alias["name"].clone()),
                    ("contact_id", alias["id"].clone()),
                    (
                        "evidence_ref",
                        string(format!("graph:inbox:{}", s(&latest["id"]))),
                    ),
                    (
                        "reason",
                        string(
                            "Incoming sender matches a recipient of our linked Sent message; register this alias after review.",
                        ),
                    ),
                ]);
                instructions = format!(
                    "The sender address is an unregistered alias linked to our Sent message. Preserve the reply task for the registered owner and return routing.registry_proposal with this exact alias and evidence so the operator can register it. {instructions}"
                );
            } else {
                instructions = format!(
                    "The unregistered sender is linked to our Sent message, but cannot be uniquely matched to a recipient. Preserve the reply task for the registered owner; do not infer an alias from the task owner. {instructions}"
                );
            }
        }
        i["instructions"] = string(instructions);
        let request =
            crate::envelopes::build_request("reply", &link, s(&person["id"]), i, &options.binding)?;
        if r.rows("tasks").iter().any(|v| v["id"] == request["id"]) {
            let mut details = category("covered", "Inbound message already has an intake task");
            details["task_id"] = request["id"].clone();
            state.remember(&latest, "covered", details, true, object([]))?;
        } else {
            state.stage(
                (&latest, &latest, request, false),
                r,
                box_,
                inputs,
                &mut listings,
                &mut uploads,
            )?;
        }
    }
    state.uploads(&mut uploads, r, box_, inputs, &mut listings, ports)?;
    state
        .notices
        .sort_by(|a, b| s(&a["receivedDateTime"]).cmp(s(&b["receivedDateTime"])));
    let skip = state.notices.len().saturating_sub(20);
    let covered_skip = state.covered.len().saturating_sub(20);
    r.set_state(
        "mailbox",
        object([
            ("through", string(cutoff)),
            ("account", string(account)),
            (
                "unmapped",
                Value::Array(state.pending.into_iter().map(|v| v.1).collect()),
            ),
            ("bounce_count", state.bounces.len().into()),
            ("bounces", Value::Array(state.bounces)),
            ("intake_audited", true.into()),
            ("no_task", state.no_task),
            (
                "covered",
                Value::Array(
                    state
                        .covered
                        .into_iter()
                        .skip(covered_skip)
                        .map(|v| v.1)
                        .collect(),
                ),
            ),
            ("machine_notice_count", state.notice_count.into()),
            (
                "machine_notices",
                Value::Array(state.notices.into_iter().skip(skip).collect()),
            ),
        ]),
    )?;
    ports.save(&r.tables)?;
    Ok(())
}
