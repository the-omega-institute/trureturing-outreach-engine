use super::*;
use outreach_core::{publication as p, sent_events as sent};
use std::collections::BTreeMap;
pub struct Notes {
    pub fake: Fake,
}
impl p::ShortNoteService for Notes {
    fn boundary(&mut self, method: &str, args: Vec<Value>) -> StageResult<()> {
        let scripted = self
            .fake
            .0
            .borrow()
            .entries
            .iter()
            .any(|e| e["adapter"] == "boundary");
        if scripted {
            self.fake
                .0
                .borrow_mut()
                .call("boundary", method, args, object([]))?;
        }
        Ok(())
    }
    fn package(&mut self, row: &Value) -> StageResult<p::Package> {
        let response = self.fake.0.borrow_mut().call(
            "shortnote",
            "package",
            vec![
                row["id"].clone(),
                string(outreach_canon::digest(&row["data"]["result"])?),
            ],
            object([]),
        )?["result"]
            .clone();
        let mut files = BTreeMap::new();
        for (name, source) in response["files"].as_object().into_iter().flatten() {
            files.insert(
                name.clone(),
                SourceFile {
                    name: name.clone(),
                    bytes: bytes(&source["base64"]),
                },
            );
        }
        let source = s(&response["source"]).to_owned();
        let metadata = response["metadata"].clone();
        let has_lean = s(&metadata["evidence_url"]).contains(".lean")
            || files
                .get(&source)
                .is_some_and(|f| f.bytes.windows(5).any(|w| w == b".lean"));
        Ok(p::Package {
            metadata,
            files,
            source,
            has_lean,
        })
    }
}
impl p::GitHubReleases for Fake {
    fn register_note(&mut self, intent: Persisted<p::Registration>) -> StageResult<Value> {
        self.0.borrow_mut().depend(&intent);
        self.call(
            "register_note",
            vec![intent.intent().metadata.clone()],
            object([]),
        )
    }
    fn release(&mut self, tag: &str) -> StageResult<Value> {
        self.call("release", vec![string(tag)], object([]))
    }
    fn create(&mut self, intent: Persisted<p::ReleaseIntent>) -> StageResult<()> {
        self.0.borrow_mut().depend(&intent);
        self.call(
            "create",
            vec![string(&intent.intent().tag), string(&intent.intent().title)],
            object([]),
        )?;
        Ok(())
    }
    fn upload(&mut self, intent: Persisted<p::UploadIntent>) -> StageResult<()> {
        self.0.borrow_mut().depend(&intent);
        self.call(
            "upload",
            vec![string(&intent.intent().tag), file(&intent.intent().file)],
            object([]),
        )?;
        Ok(())
    }
    fn download(&mut self, tag: &str, name: &str) -> StageResult<SourceFile> {
        let result = self.call(
            "download",
            vec![
                string(tag),
                string(name),
                object([("$directory", true.into())]),
            ],
            object([]),
        )?;
        Ok(SourceFile {
            name: name.into(),
            bytes: bytes(&result["base64"]),
        })
    }
}
pub struct Mailbox {
    pub fake: Fake,
    pub settings: Value,
}
impl sent::Mailbox for Mailbox {
    fn settings(&self) -> &Value {
        &self.settings
    }
    fn verify_owner(&mut self) -> StageResult<String> {
        Ok(s(&self.fake.call("verify_owner", vec![], object([]))?).into())
    }
    fn sent_events_page(
        &mut self,
        since: &str,
        until: &str,
        next: &Value,
    ) -> StageResult<sent::SentPage> {
        let response = self.fake.call(
            "sent_events_page",
            vec![string(since), string(until), next.clone()],
            object([]),
        )?;
        Ok(sent::SentPage {
            messages: array(&response[0]).to_vec(),
            next_link: response[1].clone(),
        })
    }
}
pub struct Store {
    pub fake: Fake,
    pub files: Value,
}
impl sent::ContactTables for Store {
    fn needs_push(&mut self) -> StageResult<bool> {
        Ok(self.fake.call("needs_push", vec![], object([]))? == true)
    }
    fn render(&mut self, contacts: &Value) -> StageResult<()> {
        let files = self
            .fake
            .call("render", vec![contacts.clone()], object([]))?;
        if let (Some(target), Some(source)) = (self.files.as_object_mut(), files.as_object()) {
            for (k, v) in source {
                target.insert(k.clone(), v.clone());
            }
        }
        Ok(())
    }
    fn commit_tables(&mut self, intent: Persisted<sent::ContactIntent>) -> StageResult<()> {
        self.fake.0.borrow_mut().depend(&intent);
        self.fake.call("commit_tables", vec![], object([]))?;
        Ok(())
    }
}
