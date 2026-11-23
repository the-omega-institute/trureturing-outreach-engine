//! Immutable snapshot reader. No public SQL/connection escape hatch or writes.
use outreach_canon::{CanonValue, Value};
use outreach_model::*;
use rusqlite::{Connection, OpenFlags};
use std::path::Path;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Sql(rusqlite::Error),
    Data(outreach_canon::Error),
    Snapshot(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::Sql(e) => e.fmt(f),
            Self::Data(e) => e.fmt(f),
            Self::Snapshot(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sql(e)
    }
}
impl From<outreach_canon::Error> for Error {
    fn from(e: outreach_canon::Error) -> Self {
        Self::Data(e)
    }
}
impl From<&str> for Error {
    fn from(e: &str) -> Self {
        Self::Snapshot(e.into())
    }
}
pub type Result<T> = std::result::Result<T, Error>;
pub struct Ledger {
    db: Connection,
}
impl Ledger {
    pub fn open(snapshot: &Path) -> Result<Self> {
        // Absolute, percent-encoded URI: ?, # and non-ASCII path bytes must never
        // change mode/immutable query parameters. Missing files cannot be created.
        let absolute = snapshot.canonicalize()?;
        for suffix in ["-wal", "-journal"] {
            let mut sidecar = absolute.as_os_str().to_os_string();
            sidecar.push(suffix);
            if Path::new(&sidecar).try_exists()? {
                return Err(Error::Snapshot(format!(
                    "Snapshot has a {suffix} side file"
                )));
            }
        }
        let bytes = absolute.as_os_str().as_encoded_bytes();
        let mut uri = String::from("file:");
        for b in bytes {
            if b.is_ascii_alphanumeric() || b"/-._~".contains(b) {
                uri.push(*b as char);
            } else {
                uri.push_str(&format!("%{b:02X}"));
            }
        }
        uri.push_str("?mode=ro&immutable=1");
        let db = Connection::open_with_flags(
            uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        if !db.is_readonly("main")? {
            return Err("Snapshot is not read-only".into());
        }
        Ok(Self { db })
    }
    fn json_rows(&self, sql: &str) -> Result<Vec<Value>> {
        let mut statement = self.db.prepare(sql)?;
        let mut rows = statement.query([])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let raw: String = row.get(0)?;
            result.push(CanonValue::parse(&raw)?.into_value());
        }
        Ok(result)
    }
    pub fn tasks(&self) -> Result<Vec<Task>> {
        let mut statement = self.db.prepare("SELECT id,request,request_hash,owner,kind,state,revision,data,updated FROM tasks ORDER BY rowid")?;
        let mut rows = statement.query([])?;
        let mut tasks = Vec::new();
        while let Some(row) = rows.next()? {
            tasks.push(Task {
                id: TaskId(row.get(0)?),
                request: Request::parse(&row.get::<_, String>(1)?)?,
                request_hash: row.get(2)?,
                owner: row.get(3)?,
                kind: row.get::<_, String>(4)?.parse()?,
                state: row.get::<_, String>(5)?.parse()?,
                revision: row.get(6)?,
                data: TaskData::parse(&row.get::<_, String>(7)?)?,
                updated: row.get(8)?,
            });
        }
        Ok(tasks)
    }
    /// Compare the small review marker inside SQLite; historical packet bodies
    /// never cross the adapter boundary. Historical states are opaque strings.
    pub fn review_events(&self, tasks: &[Task]) -> Result<Vec<Value>> {
        let mut stmt = self.db.prepare(
            "SELECT id,state,revision,data=? FROM task_events WHERE id=? ORDER BY revision DESC",
        )?;
        let mut result = Vec::new();
        for task in tasks {
            let review = &task.data.value()["review"];
            let marker = if review.is_object() && !review.as_object().unwrap().is_empty() {
                let hash = match review["signed_review_hash"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                {
                    Some(hash) => hash.to_owned(),
                    None => outreach_canon::digest(review)?,
                };
                Some(outreach_canon::canonical(&Value::Object(
                    [("review".into(), Value::String(hash))]
                        .into_iter()
                        .collect(),
                ))?)
            } else {
                None
            };
            let mut rows = stmt.query(rusqlite::params![marker, task.id.0])?;
            while let Some(r) = rows.next()? {
                result.push(Value::Object(
                    [
                        ("id".into(), Value::String(r.get(0)?)),
                        ("state".into(), Value::String(r.get(1)?)),
                        ("revision".into(), Value::from(r.get::<_, i64>(2)?)),
                        (
                            "reviewed".into(),
                            Value::Bool(r.get::<_, Option<bool>>(3)?.unwrap_or(false)),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ));
            }
        }
        Ok(result)
    }
    pub fn task_events(&self) -> Result<Vec<TaskEvent>> {
        let mut stmt = self
            .db
            .prepare("SELECT seq,id,state,revision,at,data FROM task_events ORDER BY seq")?;
        let mut rows = stmt.query([])?;
        let mut result = Vec::new();
        while let Some(r) = rows.next()? {
            result.push(TaskEvent {
                seq: r.get(0)?,
                id: TaskId(r.get(1)?),
                state: r.get(2)?,
                revision: r.get(3)?,
                at: r.get(4)?,
                data: TaskData::parse(&r.get::<_, String>(5)?)?,
            });
        }
        Ok(result)
    }
    pub fn control_events(&self) -> Result<Vec<ControlEvent>> {
        let mut stmt = self
            .db
            .prepare("SELECT id,payload FROM control_events ORDER BY rowid")?;
        let mut rows = stmt.query([])?;
        let mut result = Vec::new();
        while let Some(r) = rows.next()? {
            result.push(ControlEvent {
                id: r.get(0)?,
                payload: CanonValue::parse(&r.get::<_, String>(1)?)?,
            });
        }
        Ok(result)
    }
    pub fn input_receipts(&self) -> Result<Vec<InputReceipt>> {
        let mut stmt = self
            .db
            .prepare("SELECT identity,generation FROM input_asset_receipts ORDER BY rowid")?;
        let values = stmt.query_map([], |r| {
            Ok(InputReceipt {
                identity: r.get(0)?,
                generation: r.get(1)?,
            })
        })?;
        Ok(values.collect::<std::result::Result<_, _>>()?)
    }
    pub fn pipeline_state(&self) -> Result<Vec<PipelineEntry>> {
        let mut stmt = self
            .db
            .prepare("SELECT key,payload FROM pipeline_state ORDER BY rowid")?;
        let mut rows = stmt.query([])?;
        let mut result = Vec::new();
        while let Some(r) = rows.next()? {
            result.push(PipelineEntry {
                key: PipelineKey::from(r.get::<_, String>(0)?.as_str()),
                payload: CanonValue::parse(&r.get::<_, String>(1)?)?,
            });
        }
        Ok(result)
    }
    pub fn state(&self, key: &str) -> Result<Option<Value>> {
        let mut stmt = self
            .db
            .prepare("SELECT payload FROM pipeline_state WHERE key=?")?;
        let mut rows = stmt.query([key])?;
        Ok(match rows.next()? {
            Some(r) => Some(CanonValue::parse(&r.get::<_, String>(0)?)?.into_value()),
            None => None,
        })
    }
    pub fn counts(&self, by_kind: bool) -> Result<Value> {
        let mut stmt = self.db.prepare(if by_kind {
            include_str!("counts_kind.sql")
        } else {
            include_str!("counts_state.sql")
        })?;
        let mut rows = stmt.query([])?;
        let mut result = Value::Object(Default::default());
        while let Some(r) = rows.next()? {
            let key: String = r.get(0)?;
            if by_kind {
                key.parse::<TaskKind>()?;
            } else {
                key.parse::<TaskState>()?;
            }
            result[key] = Value::from(r.get::<_, i64>(1)?);
        }
        Ok(result)
    }
    pub fn lane_rows(&self) -> Result<Vec<Value>> {
        let mut stmt = self.db.prepare(include_str!("lane_rows.sql"))?;
        let mut rows = stmt.query([])?;
        let mut result = Vec::new();
        while let Some(r) = rows.next()? {
            result.push(Value::Object(
                [
                    ("id".into(), Value::String(r.get(0)?)),
                    ("owner".into(), Value::String(r.get(1)?)),
                    ("kind".into(), Value::String(r.get(2)?)),
                    ("state".into(), Value::String(r.get(3)?)),
                    (
                        "request".into(),
                        CanonValue::parse(&r.get::<_, String>(4)?)?.into_value(),
                    ),
                    (
                        "data".into(),
                        CanonValue::parse(&r.get::<_, String>(5)?)?.into_value(),
                    ),
                ]
                .into_iter()
                .collect(),
            ));
        }
        Ok(result)
    }
    pub fn pipeline_sources(&self) -> Result<Vec<Value>> {
        self.json_rows("SELECT json_object('result_id',result_id,'baseline',baseline,'task_id',task_id,'state',(SELECT state FROM tasks WHERE id=pipeline_sources.task_id)) FROM pipeline_sources ORDER BY rowid")
    }
    pub fn pipeline_counts(&self) -> Result<(i64, i64)> {
        Ok(self
            .db
            .query_row(include_str!("pipeline_counts.sql"), [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?)
    }
    pub fn old_owner_questions(&self, cutoff: f64) -> Result<i64> {
        let mut stmt = self.db.prepare(include_str!("old_owner_questions.sql"))?;
        let mut rows = stmt.query([true])?;
        let mut count = 0;
        while let Some(row) = rows.next()? {
            if row
                .get::<_, Option<f64>>(1)?
                .is_some_and(|opened| opened < cutoff)
            {
                count += row.get::<_, i64>(2)?;
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use outreach_canon::{canonical, sha256};
    use std::{fs, process::Command};
    struct Temp(std::path::PathBuf);
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn immutable_reader_covers_tables_and_refuses_writes_and_creation() {
        let directory =
            Temp(std::env::temp_dir().join(format!("outreach-ledger-test-{}", std::process::id())));
        fs::create_dir(&directory.0).unwrap();
        let path = directory.0.join("snapshot ? # π.sqlite3");
        let engine = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let script = r#"
import sys
from managed.tasks import Tasks
from managed.queue import canonical
p=Tasks(sys.argv[1], clock=lambda: 1.25)
p.add({'schema_version':1,'id':'synthetic','kind':'reply','owner':'contact-a','source_revision':'a'*40,'workflow_hash':'b'*64,'inputs':{}})
p.update('synthetic',1,'held',{'result':{'outcome':'hold'},'unknown':{'z':10**100,'a':1.0}})
p.db.execute('INSERT INTO control_events VALUES(?,?)',('event',canonical({'future':10**100})))
p.db.execute('INSERT INTO input_asset_receipts VALUES(?,?)',('["asset","synthetic.json",null]',7))
p.db.execute("UPDATE task_events SET state='historical-future-state' WHERE revision=1")
for key in ['baseline','future-key']:
    p.db.execute('INSERT INTO pipeline_state VALUES(?,?)',(key,canonical({'z':1,'a':10**100})))
p.close()
"#;
        assert!(
            Command::new("python3")
                .arg("-c")
                .arg(script)
                .arg(&path)
                .current_dir(engine)
                .env("OUTREACH_DATA_ROOT", &directory.0)
                .env("GITHUB_ACTIONS", "false")
                .status()
                .unwrap()
                .success()
        );
        let before = sha256(&fs::read(&path).unwrap());
        let ledger = Ledger::open(&path).unwrap();
        let tasks = ledger.tasks().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].state, TaskState::Held);
        assert_eq!(
            canonical(&tasks[0].data.value()["unknown"]["a"]).unwrap(),
            "1.0"
        );
        assert_eq!(ledger.task_events().unwrap().len(), 2);
        assert_eq!(
            ledger.task_events().unwrap()[0].state,
            "historical-future-state"
        );
        assert_eq!(ledger.control_events().unwrap()[0].id, "event");
        assert_eq!(ledger.input_receipts().unwrap()[0].generation, 7);
        let states = ledger.pipeline_state().unwrap();
        assert_eq!(states[0].key, PipelineKey::Baseline);
        assert_eq!(states[1].key, PipelineKey::Other("future-key".into()));
        assert_eq!(
            ledger.tasks().unwrap().remove(0).into_value().unwrap()["data"],
            *tasks[0].data.value()
        );
        assert!(ledger.db.execute("DELETE FROM tasks", []).is_err());
        assert!(ledger.db.execute("CREATE TABLE forbidden(x)", []).is_err());
        assert_eq!(ledger.counts(false).unwrap()["held"], 1);
        drop(ledger);
        assert_eq!(sha256(&fs::read(&path).unwrap()), before);
        assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
        for suffix in ["-wal", "-journal"] {
            let sidecar = std::path::PathBuf::from(format!("{}{suffix}", path.display()));
            fs::write(&sidecar, b"synthetic pending bytes").unwrap();
            assert!(matches!(Ledger::open(&path), Err(Error::Snapshot(_))));
            assert_eq!(fs::read(&sidecar).unwrap(), b"synthetic pending bytes");
            assert_eq!(sha256(&fs::read(&path).unwrap()), before);
            fs::remove_file(sidecar).unwrap();
        }
        let absent = directory.0.join("absent.sqlite3");
        assert!(Ledger::open(&absent).is_err());
        assert!(!absent.exists());
    }
}
