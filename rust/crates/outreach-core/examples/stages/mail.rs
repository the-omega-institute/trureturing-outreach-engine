use super::*;
use outreach_core::delivery::{self as mail, MailboxFeature as F, MailboxRead as R};
pub struct Mailbox {
    pub fake: Fake,
    pub features: Value,
}
fn datetime(at: f64) -> Value {
    let micros = (at * 1_000_000.).round() as i64;
    let seconds = micros.div_euclid(1_000_000);
    let fraction = micros.rem_euclid(1_000_000);
    let days = seconds.div_euclid(86400);
    let rem = seconds.rem_euclid(86400);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    y += i64::from(m <= 2);
    object([(
        "$datetime",
        string(format!(
            "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}{}+00:00",
            rem / 3600,
            rem % 3600 / 60,
            rem % 60,
            if fraction == 0 {
                String::new()
            } else {
                format!(".{fraction:06}")
            }
        )),
    )])
}
impl mail::Mailbox for Mailbox {
    fn supports(&self, feature: F) -> bool {
        self.features[feature.name()] == true
    }
    fn attempt_started(&mut self, _at: f64) {}
    fn observe(&mut self, read: R) -> StageResult<Value> {
        let (method, args, kwargs) = match read {
            R::Preflight {
                packet,
                request,
                allow_draft,
            } => (
                "preflight",
                vec![packet, request],
                allow_draft
                    .map(|id| object([("allow_draft_id", string(id))]))
                    .unwrap_or_else(|| object([])),
            ),
            R::Lookup { packet, key } => ("lookup", vec![packet, string(key)], object([])),
            R::LegacyNotSent { packet, key } => {
                ("legacy_not_sent", vec![packet, string(key)], object([]))
            }
            R::Find { folder, key } => ("find", vec![string(folder), string(key)], object([])),
            R::DraftsFor(a) => ("drafts_for", vec![string(a)], object([])),
            R::RecipientHistory(a) => ("recipient_history", vec![string(a)], object([])),
            R::Thread(a) => ("thread", vec![string(a)], object([])),
            R::Message(a) => ("message", vec![string(a)], object([])),
            R::SentItems(at) => (
                "sent_items",
                at.map(|t| vec![datetime(t)]).unwrap_or_default(),
                object([]),
            ),
            R::DraftsFolder => ("drafts_folder_id", vec![], object([])),
            R::DeletedFolder => ("deleted_items_folder_id", vec![], object([])),
            R::VerifyOwner => ("verify_owner", vec![], object([])),
            R::Matches { draft, packet } => ("matches", vec![draft, packet], object([])),
            R::OwnDraft { draft, packet, key } => {
                ("own_draft", vec![draft, packet, string(key)], object([]))
            }
            R::Messages(folder) => ("messages", vec![string(folder)], object([])),
        };
        self.fake.call(method, args, kwargs)
    }
    fn save_draft(&mut self, permit: Persisted<mail::DraftIntent>) -> StageResult<Value> {
        self.fake.0.borrow_mut().depend(&permit);
        let i = permit.intent();
        self.fake.call(
            "save_draft",
            vec![i.packet.clone(), string(&i.key)],
            object([("recovery", i.recovery.into())]),
        )
    }
    fn submit(
        &mut self,
        permit: Persisted<mail::SendIntent>,
        boundary: &mut dyn mail::SendBoundary,
    ) -> StageResult<Value> {
        self.fake.0.borrow_mut().depend(&permit);
        let i = permit.intent();
        let tracked = self.supports(F::TrackedSend);
        let mut args = vec![i.packet.clone(), string(&i.key)];
        if tracked {
            args.push(object([("$callback", true.into())]));
        }
        let outcome = self.fake.call(
            if tracked { "submit_tracked" } else { "submit" },
            args,
            object([]),
        );
        loop {
            let entry = {
                let script = self.fake.0.borrow();
                script
                    .entries
                    .get(script.index)
                    .cloned()
                    .unwrap_or(Value::Null)
            };
            if entry["adapter"] != "boundary" || entry["method"] != "send_call" {
                break;
            }
            self.fake.0.borrow_mut().call(
                "boundary",
                "send_call",
                array(&entry["args"]).to_vec(),
                object([]),
            )?;
            boundary.before_send(s(&entry["args"][0]))?;
        }
        outcome
    }
    fn discard_draft(&mut self, permit: Persisted<mail::DraftAttempt>) -> StageResult<Value> {
        self.fake.0.borrow_mut().depend(&permit);
        let i = permit.intent();
        self.fake.call(
            "discard_draft",
            vec![i.packet.clone(), string(&i.key)],
            object([]),
        )
    }
    fn create_draft(&mut self, permit: Persisted<mail::ReplacementIntent>) -> StageResult<Value> {
        self.fake.0.borrow_mut().depend(&permit);
        let i = permit.intent();
        self.fake.call(
            "create_draft",
            vec![i.packet.clone(), string(&i.key)],
            object([("replacing", string(&i.replacing))]),
        )
    }
    fn delete_draft(&mut self, permit: Persisted<mail::DraftId>) -> StageResult<Value> {
        self.fake.0.borrow_mut().depend(&permit);
        self.fake.call(
            "delete_draft",
            vec![string(&permit.intent().remote_id)],
            object([]),
        )
    }
    fn move_draft(&mut self, permit: Persisted<mail::DraftId>) -> StageResult<Value> {
        self.fake.0.borrow_mut().depend(&permit);
        self.fake.call(
            "move_draft_to_deleted",
            vec![string(&permit.intent().remote_id)],
            object([]),
        )
    }
}
pub struct Registry {
    pub fake: Fake,
    pub files: Value,
}
impl mail::MailRegistry for Registry {
    fn register(&mut self, evidence: &Value) -> StageResult<Option<Value>> {
        let evidence = if evidence.is_null() {
            Value::Array(vec![])
        } else {
            evidence.clone()
        };
        let files = self.fake.call("register", vec![evidence], object([]))?;
        for (k, v) in files.as_object().into_iter().flatten() {
            self.files[k] = v.clone();
        }
        files
            .get("tables/contacts.json")
            .map(|v| parse(&String::from_utf8_lossy(&bytes(v))).map_err(StageError::from))
            .transpose()
    }
}
