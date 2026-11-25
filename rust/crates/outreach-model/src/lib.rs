//! Read models for the Python-owned ledger. Unknown JSON fields stay in canon.
use outreach_canon::{CanonValue, Error, Result, Value, canonical, python_eq};
use std::{fmt, str::FromStr};

macro_rules! wire_enum {
    ($name:ident { $($variant:ident => $wire:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
        pub enum $name { $($variant),+ }
        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
            pub fn as_str(self) -> &'static str { match self { $(Self::$variant => $wire),+ } }
        }
        impl FromStr for $name {
            type Err = Error;
            fn from_str(value: &str) -> Result<Self> {
                match value { $($wire => Ok(Self::$variant),)+ _ => Err(Error::new(format!("Unknown {}: {value}", stringify!($name)))) }
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) }
        }
    }
}
wire_enum!(TaskKind {
    FirstContact => "firstcontact", Reply => "reply", Research => "research",
    ShortNote => "shortnote", Daily => "daily", Mathesis => "mathesis", Submission => "submission"
});
wire_enum!(TaskState {
    Queued => "queued", Provisioning => "provisioning", Admitting => "admitting", Running => "running",
    AdmissionUnknown => "admission-unknown", ProvisionUnknown => "provision-unknown", CreditWait => "credit-wait",
    AwaitingReview => "awaiting-review", Approved => "approved", Bounced => "bounced", Held => "held", Failed => "failed",
    AwaitingMailbox => "awaiting-mailbox", OwnerDraft => "owner-draft", OwnerDiscarded => "owner-discarded",
    HandoffReady => "handoff-ready", Verified => "verified", AwaitingUser => "awaiting-user", AnsweredByMail => "answered-by-mail",
    QuestionClosed => "question-closed", DecisionRecorded => "decision-recorded", Revising => "revising", NoReply => "no-reply",
    RoundComplete => "round-complete", SubmissionReady => "submission-ready", Superseded => "superseded"
});
impl TaskState {
    pub fn active(self) -> bool {
        matches!(
            self,
            Self::Provisioning
                | Self::Admitting
                | Self::Running
                | Self::AdmissionUnknown
                | Self::ProvisionUnknown
                | Self::CreditWait
        )
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct TaskId(pub String);

/// A complete ordered object. Keeping one authoritative object avoids flatten
/// reordering and preserves unknown nested fields and arbitrary integer values.
#[derive(Clone, Debug)]
pub struct TaskData(CanonValue);
impl TaskData {
    pub fn parse(raw: &str) -> Result<Self> {
        let value = CanonValue::parse(raw)?;
        if !value.is_object() {
            return Err(Error::new("Task data must be an object"));
        }
        Ok(Self(value))
    }
    pub fn value(&self) -> &Value {
        self.0.value()
    }
    pub fn into_value(self) -> Value {
        self.0.into_value()
    }
    pub fn canonical(&self) -> Result<String> {
        canonical(&self.0)
    }
    pub fn result(&self) -> Option<&Value> {
        self.0.get("result")
    }
    pub fn review(&self) -> Option<&Value> {
        self.0.get("review")
    }
}

#[derive(Clone, Debug)]
pub struct Request {
    pub id: TaskId,
    pub kind: TaskKind,
    pub raw: CanonValue,
}
impl Request {
    pub fn parse(raw: &str) -> Result<Self> {
        let raw = CanonValue::parse(raw)?;
        Ok(Self {
            id: TaskId(required_str(&raw, "id")?.into()),
            kind: required_str(&raw, "kind")?.parse()?,
            raw,
        })
    }
}
pub fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new(format!("Missing string: {key}")))
}

wire_enum!(Outcome { Completed => "completed", Hold => "hold", NoReply => "no-reply", NeedsDecision => "needs-decision" });
wire_enum!(Verdict { Pass => "PASS", Bounce => "BOUNCE", Hold => "HOLD" });
#[derive(Clone, Debug)]
pub struct TaskResult {
    pub outcome: Outcome,
    pub raw: CanonValue,
}
impl TaskResult {
    pub fn parse(raw: &str) -> Result<Self> {
        let raw = CanonValue::parse(raw)?;
        Ok(Self {
            outcome: required_str(&raw, "outcome")?.parse()?,
            raw,
        })
    }
}
#[derive(Clone, Debug)]
pub struct Review {
    pub verdict: Verdict,
    pub binding: Binding,
    pub raw: CanonValue,
}
impl Review {
    pub fn parse(raw: &str) -> Result<Self> {
        let raw = CanonValue::parse(raw)?;
        let binding = raw
            .get("binding")
            .ok_or_else(|| Error::new("Missing review binding"))?;
        Ok(Self {
            verdict: required_str(&raw, "verdict")?.parse()?,
            binding: Binding::parse(&canonical(binding)?)?,
            raw,
        })
    }
}
#[derive(Clone, Debug)]
pub struct Binding {
    pub task_id: TaskId,
    pub task_revision: CanonValue,
    pub result_hash: String,
    pub workflow_hash: String,
}
impl Binding {
    pub fn parse(raw: &str) -> Result<Self> {
        let value = CanonValue::parse(raw)?;
        let revision = value
            .get("task_revision")
            .ok_or_else(|| Error::new("Missing task_revision"))?;
        Ok(Self {
            task_id: TaskId(required_str(&value, "task_id")?.into()),
            task_revision: CanonValue::parse(&canonical(revision)?)?,
            result_hash: required_str(&value, "result_hash")?.into(),
            workflow_hash: required_str(&value, "workflow_hash")?.into(),
        })
    }
    pub fn python_eq(&self, other: &Self) -> bool {
        self.task_id == other.task_id
            && python_eq(&self.task_revision, &other.task_revision)
            && self.result_hash == other.result_hash
            && self.workflow_hash == other.workflow_hash
    }
}
#[derive(Clone, Debug)]
pub struct Task {
    pub id: TaskId,
    pub request: Request,
    pub request_hash: String,
    pub owner: String,
    pub kind: TaskKind,
    pub state: TaskState,
    pub revision: i64,
    pub data: TaskData,
    pub updated: f64,
}
impl Task {
    pub fn into_value(self) -> Result<Value> {
        Ok(Value::Object(
            [
                ("id", Value::String(self.id.0)),
                ("request", self.request.raw.into_value()),
                ("request_hash", Value::String(self.request_hash)),
                ("owner", Value::String(self.owner)),
                ("kind", Value::String(self.kind.to_string())),
                ("state", Value::String(self.state.to_string())),
                ("revision", self.revision.into()),
                ("data", self.data.into_value()),
                ("updated", outreach_canon::float(self.updated)?),
            ]
            .into_iter()
            .map(|(k, v)| (k.into(), v))
            .collect(),
        ))
    }
}
#[derive(Clone, Debug)]
pub struct TaskEvent {
    pub seq: i64,
    pub id: TaskId,
    pub state: String,
    pub revision: i64,
    pub at: f64,
    pub data: TaskData,
}
#[derive(Clone, Debug)]
pub struct ControlEvent {
    pub id: String,
    pub payload: CanonValue,
}
#[derive(Clone, Debug)]
pub struct InputReceipt {
    pub identity: String,
    pub generation: i64,
}

/// Known keys are classified without guessing the schema of future payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PipelineKey {
    Baseline,
    Mailbox,
    ConnectorErrors,
    OracleCapacity,
    CreditBlock,
    IngestRejected,
    TickDeferred,
    SandboxPool,
    SandboxPoolCleanup,
    UserQuestionCursor,
    MathesisLegacyMigration,
    OwnerDraftObservations,
    SentEvents,
    SentEventsStatus,
    MailboxSentSubjects,
    MailboxAttachments(String),
    NoteCoverage(String),
    Recheck { lane: String, task_id: TaskId },
    Mathesis(String),
    MathesisCollection(String),
    PublicationSelection(String),
    PriorWorkIssue(String),
    PriorWorkCheck(String),
    PriorWorkBackfill(String),
    Other(String),
}
impl From<&str> for PipelineKey {
    fn from(key: &str) -> Self {
        match key {
            "baseline" => Self::Baseline,
            "mailbox" => Self::Mailbox,
            "connector_errors" => Self::ConnectorErrors,
            "oracle_capacity" => Self::OracleCapacity,
            "credit_block" => Self::CreditBlock,
            "ingest_rejected" => Self::IngestRejected,
            "tick_deferred" => Self::TickDeferred,
            "sandbox_pool" => Self::SandboxPool,
            "sandbox_pool_cleanup" => Self::SandboxPoolCleanup,
            "user-question-cursor" => Self::UserQuestionCursor,
            "mathesis-legacy-migration" => Self::MathesisLegacyMigration,
            "owner-draft-observations" => Self::OwnerDraftObservations,
            "sent-events" => Self::SentEvents,
            "sent-events-status" => Self::SentEventsStatus,
            "mailbox-sent-subjects" => Self::MailboxSentSubjects,
            _ => {
                for (prefix, constructor) in [
                    ("note-coverage:", Self::NoteCoverage as fn(String) -> Self),
                    ("mailbox-attachments:", Self::MailboxAttachments),
                    ("mathesis:", Self::Mathesis),
                    ("mathesis-collection:", Self::MathesisCollection),
                    ("publication-selection:", Self::PublicationSelection),
                    ("prior-work-issue:", Self::PriorWorkIssue),
                    ("prior-work-check:", Self::PriorWorkCheck),
                    ("prior-work-backfill:", Self::PriorWorkBackfill),
                ] {
                    if let Some(suffix) = key.strip_prefix(prefix) {
                        return constructor(suffix.into());
                    }
                }
                if let Some(rest) = key.strip_prefix("recheck:")
                    && let Some((lane, id)) = rest.split_once(':')
                {
                    return Self::Recheck {
                        lane: lane.into(),
                        task_id: TaskId(id.into()),
                    };
                }
                Self::Other(key.into())
            }
        }
    }
}
#[derive(Clone, Debug)]
pub struct PipelineEntry {
    pub key: PipelineKey,
    pub payload: CanonValue,
}
