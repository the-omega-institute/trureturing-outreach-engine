//! Immutable input assets use content-addressed names and verified readback.
//! This is the existing append-only staging contract, independent of ledger
//! intent persistence; it grants no mail or issue write capability.
use super::*;
use crate::{
    formalization_requests::check,
    ingest::Replay,
    mathesis_delivery::{SourceFile, StageError},
};
use outreach_canon::{digest, sha256};
pub struct InputFile {
    pub file: SourceFile,
    pub reference: Value,
}
pub trait Inputs {
    fn assets(&mut self, tag: &str) -> StageResult<Value>;
    fn download(&mut self, tag: &str, name: &str) -> StageResult<Vec<u8>>;
    fn upload(&mut self, files: Vec<InputFile>, tag: &str, listings: &mut Value)
    -> StageResult<()>;
}
pub fn decode(text: &str) -> StageResult<Vec<u8>> {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = vec![];
    let mut bits = 0_u32;
    let mut count = 0;
    let mut padding = false;
    for c in text.bytes() {
        if c == b'=' {
            padding = true;
            continue;
        }
        check(!padding, "Invalid base64 attachment")?;
        let n = alphabet
            .iter()
            .position(|v| *v == c)
            .ok_or_else(|| outreach_canon::Error::new("Invalid base64 attachment"))?;
        bits = (bits << 6) | n as u32;
        count += 6;
        if count >= 8 {
            count -= 8;
            out.push((bits >> count) as u8);
        }
    }
    check(text.len().is_multiple_of(4), "Invalid base64 attachment")?;
    Ok(out)
}
fn verified(
    reference: &Value,
    inputs: &mut impl Inputs,
    listings: &mut Value,
) -> StageResult<bool> {
    let tag = s(&reference["release_tag"]);
    let name = s(&reference["name"]);
    if listings.get(tag).is_none() {
        listings[tag] = inputs.assets(tag)?;
    }
    let asset = &listings[tag][name];
    if asset.is_null() || asset["size"] != reference["bytes"] {
        return Ok(false);
    }
    let expected = format!("sha256:{}", s(&reference["sha256"]));
    if truth(&asset["digest"]) {
        return Ok(asset["digest"] == expected);
    }
    let raw = inputs.download(tag, name)?;
    if reference["bytes"].as_u64() != Some(raw.len() as u64) || reference["sha256"] != sha256(&raw)
    {
        return Ok(false);
    }
    listings[tag][name]["digest"] = string(expected);
    Ok(true)
}
fn previous(r: &Replay<'_>, conversation: &Value) -> Value {
    let mut index = object([]);
    for row in r.rows("tasks") {
        if row["kind"] != "reply" || row["request"]["inputs"]["thread_id"] != *conversation {
            continue;
        }
        for m in array(&row["request"]["inputs"]["thread"]) {
            for a in array(&m["attachments"]) {
                let v = &a["selected_input"];
                let sha = s(&v["sha256"]);
                if truth(&a["id"])
                    && crate::regex(r"^[a-f0-9]{64}$").is_ok_and(|r| r.is_match(sha))
                    && v["name"] == format!("attachment-{sha}")
                    && crate::regex(r"^outreach-input-[a-f0-9]{32}$")
                        .is_ok_and(|r| r.is_match(s(&v["release_tag"])))
                    && v["bytes"]
                        .as_u64()
                        .is_some_and(|n| n > 0 && n <= 30 * 1024 * 1024)
                {
                    index[s(&a["id"])] = v.clone();
                }
            }
        }
    }
    index
}
pub fn preserve(
    thread: &mut Value,
    r: &mut Replay<'_>,
    identity: &Value,
    box_: &mut impl Mailbox,
    inputs: &mut impl Inputs,
    listings: &mut Value,
    upload: bool,
) -> StageResult<bool> {
    let mut attachments = vec![];
    for (i, m) in array(thread).iter().enumerate() {
        for (j, a) in array(&m["attachments"]).iter().enumerate() {
            attachments.push((i, j, a.clone()));
        }
    }
    if attachments.is_empty() {
        return Ok(true);
    }
    let key = format!("mailbox-attachments:{}", digest(&identity["thread_id"])?);
    let saved = r.state(&key);
    let index = if saved.is_null() {
        previous(r, &identity["thread_id"])
    } else {
        saved.clone()
    };
    let mut refs: Vec<Value> = attachments
        .iter()
        .map(|(_, _, a)| index[s(&a["id"])].clone())
        .collect();
    if !upload && refs.iter().any(|v| !truth(v)) {
        return Ok(false);
    }
    for v in &mut refs {
        if truth(v) && !verified(v, inputs, listings)? {
            *v = Value::Null;
        }
    }
    if refs.iter().any(|v| !truth(v)) {
        if !upload {
            return Ok(false);
        }
        let tag = format!("outreach-input-{}", &digest(identity)?[..32]);
        let mut reused: BTreeMap<String, Value> = refs
            .iter()
            .filter(|v| truth(v))
            .map(|v| (s(&v["sha256"]).into(), v.clone()))
            .collect();
        let mut files = vec![];
        let mut unsupported = vec![];
        for (n, (i, _, a)) in attachments.iter().enumerate() {
            if truth(&refs[n]) {
                continue;
            }
            let encoded = if a.get("contentBytes").is_some() {
                a["contentBytes"].clone()
            } else {
                check(
                    box_.supports("attachment_content"),
                    "Thread attachment content is unavailable",
                )?;
                box_.read(Read::Attachment {
                    message: thread[*i].clone(),
                    attachment: a.clone(),
                })?
            };
            let raw = decode(s(&encoded))?;
            if raw.len() > 30 * 1024 * 1024 {
                unsupported.push(object([
                    ("message_id", thread[*i]["id"].clone()),
                    ("name", a["name"].clone()),
                    ("type", a["@odata.type"].clone()),
                    ("size", raw.len().into()),
                ]));
                continue;
            }
            check(!raw.is_empty(), "Select a supported thread attachment size")?;
            let sha = sha256(&raw);
            if !reused.contains_key(&sha) {
                let mut old = None;
                for v in index.as_object().into_iter().flat_map(|o| o.values()) {
                    if v["sha256"] == sha && verified(v, inputs, listings)? {
                        old = Some(v.clone());
                        break;
                    }
                }
                let reference = old.unwrap_or_else(|| {
                    object([
                        ("release_tag", string(&tag)),
                        ("name", string(format!("attachment-{sha}"))),
                        ("bytes", raw.len().into()),
                        ("sha256", string(&sha)),
                    ])
                });
                if reference["release_tag"] == tag {
                    files.push(InputFile {
                        file: SourceFile {
                            name: s(&reference["name"]).into(),
                            bytes: raw,
                        },
                        reference: reference.clone(),
                    });
                }
                reused.insert(sha.clone(), reference);
            }
            refs[n] = reused.get(&sha).cloned().unwrap_or(Value::Null);
        }
        if !unsupported.is_empty() {
            return Err(StageError {
                summary: object([
                    ("type", string("UnsupportedThreadAttachments")),
                    ("attachments", Value::Array(unsupported)),
                ]),
            });
        }
        if !files.is_empty() {
            let refs: Vec<_> = files.iter().map(|v| v.reference.clone()).collect();
            inputs.upload(files, &tag, listings)?;
            for v in refs {
                check(
                    verified(&v, inputs, listings)?,
                    "Thread attachment input readback differs",
                )?;
            }
        }
    }
    let mut updated = index.clone();
    for ((_, _, a), reference) in attachments.iter().zip(&refs) {
        if truth(&a["id"]) {
            updated[s(&a["id"])] = reference.clone();
        }
    }
    if truth(&updated) && (saved.is_null() || updated != index) {
        r.set_state(&key, updated)?;
    }
    for ((i, j, _), reference) in attachments.into_iter().zip(refs) {
        crate::formalization_requests::remove(&mut thread[i]["attachments"][j], "contentBytes");
        thread[i]["attachments"][j]["selected_input"] = reference;
    }
    Ok(true)
}
