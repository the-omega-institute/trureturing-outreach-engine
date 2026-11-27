//! Scenario-only adapters. Expected rows never provide observations.
use super::super::*;
use outreach_core::{formalization_requests as f, mailbox as m, prior_work_issues as p};
pub struct Mailbox {
    pub fake: Fake,
    pub attributes: Value,
}
fn datetime(micros: i64) -> Value {
    object([("$datetime", string(utc(micros).replace('Z', "+00:00")))])
}
fn utc(micros: i64) -> String {
    let days = micros.div_euclid(86_400_000_000);
    let mut year = 1970 + days.div_euclid(366);
    let day = days + 719163;
    while outreach_core::validation::iso_day(&format!("{:04}-01-01", year + 1))
        .is_some_and(|v| v <= day)
    {
        year += 1;
    }
    let mut month = 1;
    while month < 12
        && outreach_core::validation::iso_day(&format!("{year:04}-{:02}-01", month + 1))
            .is_some_and(|v| v <= day)
    {
        month += 1;
    }
    let d = day
        - outreach_core::validation::iso_day(&format!("{year:04}-{month:02}-01")).unwrap_or(day)
        + 1;
    let sec = micros.div_euclid(1_000_000).rem_euclid(86400);
    format!(
        "{year:04}-{month:02}-{d:02}T{:02}:{:02}:{:02}Z",
        sec / 3600,
        sec / 60 % 60,
        sec % 60
    )
}
impl m::Mailbox for Mailbox {
    fn settings(&self) -> &Value {
        &self.attributes["settings"]
    }
    fn supports(&self, name: &str) -> bool {
        if let Some(v) = self.attributes.get(name) {
            return *v == true;
        }
        self.fake
            .0
            .borrow()
            .entries
            .iter()
            .any(|e| e["adapter"] == "mailbox" && e["method"] == name)
    }
    fn read(&mut self, read: m::Read) -> StageResult<Value> {
        let mut kw = object([]);
        let (method, args) = match read {
            m::Read::VerifyOwner => ("verify_owner", vec![]),
            m::Read::Inbox { since, through } => (
                "messages",
                vec![
                    string("inbox"),
                    object([(
                        "$filter",
                        string(format!(
                            "receivedDateTime ge {since} and receivedDateTime le {through}"
                        )),
                    )]),
                ],
            ),
            m::Read::Thread(c) => ("research_thread", vec![string(c)]),
            m::Read::SentSnapshot => ("sent_snapshot", vec![]),
            m::Read::SentTo(a) => ("sent_messages_to", vec![string(a)]),
            m::Read::Locate(id) => ("conversation_for_message", vec![id]),
            m::Read::Attachment {
                message,
                attachment,
            } => ("attachment_content", vec![message, attachment]),
            m::Read::SentById(id) => ("sent_message_by_internet_id", vec![string(id)]),
            m::Read::SentSubjects {
                start,
                end,
                incremental,
            } => {
                if incremental {
                    kw["incremental"] = true.into();
                }
                ("_read_sent_subjects", vec![datetime(start), datetime(end)])
            }
            m::Read::MatchSent {
                message,
                persons,
                datetime,
                subjects,
            } => {
                kw["now"] = object([("$datetime", string(datetime))]);
                if self.attributes["graph_mailbox"] == true {
                    kw["subjects"] = subjects;
                    kw["sent_index"] = object([("$callback", true.into())]);
                }
                ("match_inbound_sent", vec![message, Value::Array(persons)])
            }
        };
        self.fake.call(method, args, kw)
    }
}
pub struct Inputs {
    pub fake: Fake,
    pub repo: Value,
}
impl m::attachments::Inputs for Inputs {
    fn assets(&mut self, tag: &str) -> StageResult<Value> {
        self.fake.call("task_assets", vec![string(tag)], object([]))
    }
    fn download(&mut self, tag: &str, name: &str) -> StageResult<Vec<u8>> {
        let entry = self.fake.0.borrow_mut().call(
            "releases",
            "run",
            vec![
                string("release"),
                string("download"),
                string(tag),
                string("--repo"),
                self.repo.clone(),
                string("--pattern"),
                string(name),
                string("--dir"),
                object([("$directory", true.into())]),
            ],
            object([]),
        )?;
        Ok(bytes(&entry["download"]["base64"]))
    }
    fn upload(
        &mut self,
        files: Vec<m::attachments::InputFile>,
        tag: &str,
        listings: &mut Value,
    ) -> StageResult<()> {
        let pairs = Value::Array(
            files
                .iter()
                .map(|v| Value::Array(vec![file(&v.file), v.reference.clone()]))
                .collect(),
        );
        let response = self.fake.call(
            "upload_task_batch",
            vec![pairs, string(tag), listings.clone()],
            object([]),
        )?;
        *listings = response["listings"].clone();
        Ok(())
    }
}
pub struct Issues {
    pub fake: Fake,
    pub available: bool,
}
impl f::GitHubIssues for Issues {
    fn available(&self) -> bool {
        self.available
    }
    fn page(&mut self, page: usize) -> StageResult<Value> {
        self.fake.call(
            "request",
            vec![string(format!(
                "{}?state=all&sort=created&direction=asc&per_page=100&page={page}",
                f::ISSUES
            ))],
            object([]),
        )
    }
    fn create(&mut self, permit: Persisted<f::IssueIntent>) -> StageResult<Value> {
        self.fake.0.borrow_mut().depend(&permit);
        self.fake.call(
            "request",
            vec![
                string(f::ISSUES),
                string("POST"),
                permit.intent().payload.clone(),
            ],
            object([]),
        )
    }
}
impl p::Evidence for Fake {
    fn download(&mut self, row: &Value, name: &str) -> StageResult<SourceFile> {
        let source = self.call(
            "download",
            vec![
                row["id"].clone(),
                string(name),
                object([("$directory", true.into())]),
            ],
            object([]),
        )?;
        Ok(SourceFile {
            name: name.into(),
            bytes: bytes(&source["base64"]),
        })
    }
}
