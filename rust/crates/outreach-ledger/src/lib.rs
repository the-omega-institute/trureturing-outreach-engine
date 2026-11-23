//! Immutable snapshot reader. No public SQL/connection escape hatch or writes.
use outreach_canon::{CanonValue, Value};
use outreach_model::*;
use rusqlite::{Connection, OpenFlags};
use std::path::Path;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub struct Ledger {
    db: Connection,
}
impl Ledger {
    pub fn open(snapshot: &Path) -> Result<Self> {
        // Absolute, percent-encoded URI: ?, # and non-ASCII path bytes must never
        // change mode/immutable query parameters. Missing files cannot be created.
        let absolute = snapshot.canonicalize()?;
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
            result.push(CanonValue::parse(&raw)?.value().clone());
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
    pub fn task_values(&self) -> Result<Vec<Value>> {
        // Validate discriminants without decoding every large task payload twice.
        self.counts(false)?;
        self.counts(true)?;
        let rows = self.json_rows("SELECT json_object('id',id,'request',json(request),'request_hash',request_hash,'owner',owner,'kind',kind,'state',state,'revision',revision,'data',json(data),'updated',updated) FROM tasks ORDER BY rowid")?;
        for row in &rows {
            required_str(&row["request"], "kind")?.parse::<TaskKind>()?;
            if !row["data"].is_object() {
                return Err("Task data must be an object".into());
            }
        }
        Ok(rows)
    }
    pub fn review_events(&self) -> Result<Vec<Value>> {
        let rows = self.json_rows("SELECT json_object('id',id,'state',state,'revision',revision,'raw_data',data) FROM task_events ORDER BY seq")?;
        for row in &rows {
            required_str(row, "state")?.parse::<TaskState>()?;
        }
        Ok(rows)
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
                state: r.get::<_, String>(2)?.parse()?,
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
            Some(r) => Some(CanonValue::parse(&r.get::<_, String>(0)?)?.value().clone()),
            None => None,
        })
    }
    pub fn counts(&self, by_kind: bool) -> Result<Value> {
        let column = if by_kind { "kind" } else { "state" };
        let mut stmt = self.db.prepare(&format!(
            "SELECT {column},COUNT(*) FROM tasks GROUP BY {column} ORDER BY {column}"
        ))?;
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
        self.json_rows(include_str!("lane_rows.sql"))
    }
    pub fn pipeline_sources(&self) -> Result<Vec<Value>> {
        self.json_rows("SELECT json_object('result_id',result_id,'baseline',baseline,'task_id',task_id,'state',(SELECT state FROM tasks WHERE id=pipeline_sources.task_id)) FROM pipeline_sources ORDER BY rowid")
    }
    pub fn pipeline_counts(&self) -> Result<(i64, i64)> {
        Ok(self.db.query_row("SELECT COALESCE(SUM(kind='shortnote' AND json_extract(request,'$.inputs.pipeline.scenario')=3 AND COALESCE(json_extract(request,'$.inputs.note_round'),'')!='draft'),0), COALESCE(SUM(json_extract(request,'$.inputs.pipeline.scenario') IN (2,4,5)),0) FROM tasks WHERE state!='superseded'", [], |r| Ok((r.get(0)?,r.get(1)?)))?)
    }
    pub fn old_owner_questions(&self, now: f64) -> Result<i64> {
        Ok(self.db.query_row("SELECT COALESCE(SUM(json_array_length(data,'$.result.routing.questions')),0) FROM tasks WHERE state='awaiting-user' AND kind IN ('reply','research') AND json_array_length(data,'$.result.routing.questions')>0 AND COALESCE(json_extract(data,'$.owner_question_opened_at'),(SELECT MIN(e.at) FROM task_events e WHERE e.id=tasks.id AND e.state='awaiting-user'),(SELECT MIN(e.at) FROM task_events e WHERE e.id=tasks.id)) < ?", [now - 2.0 * 86400.0], |r| r.get(0))?)
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
        assert_eq!(ledger.control_events().unwrap()[0].id, "event");
        assert_eq!(ledger.input_receipts().unwrap()[0].generation, 7);
        let states = ledger.pipeline_state().unwrap();
        assert_eq!(states[0].key, PipelineKey::Baseline);
        assert_eq!(states[1].key, PipelineKey::Other("future-key".into()));
        assert_eq!(
            ledger.task_values().unwrap()[0]["data"],
            *tasks[0].data.value()
        );
        assert!(ledger.db.execute("DELETE FROM tasks", []).is_err());
        assert!(ledger.db.execute("CREATE TABLE forbidden(x)", []).is_err());
        assert_eq!(ledger.counts(false).unwrap()["held"], 1);
        drop(ledger);
        assert_eq!(sha256(&fs::read(&path).unwrap()), before);
        assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
        let absent = directory.0.join("absent.sqlite3");
        assert!(Ledger::open(&absent).is_err());
        assert!(!absent.exists());
    }
}
