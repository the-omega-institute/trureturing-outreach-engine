//! Pure stage builders. Callers explicitly provide pins and preserved inputs.
use crate::{envelopes::*, validation::*, *};
use outreach_canon::{digest, sha256, sign_mathesis_envelope};
use std::collections::BTreeSet;
const REPLY_OWNER_RULE: &str = include_str!("reply_owner_rule.txt");
pub fn inbound(
    message: &str,
    owner: Option<&str>,
    instructions: &str,
    binding: &Value,
    env: &Environment<'_>,
) -> Result<Value> {
    require(
        !message.trim_matches(crate::text::whitespace).is_empty(),
        "stage-inbound needs an Internet Message-ID",
    )?;
    let message = crate::ingest::message_id(&string(message))
        .ok_or_else(|| Error::new("stage-inbound needs one valid Internet Message-ID"))?;
    require(
        owner.is_none_or(|o| !o.trim_matches(crate::text::whitespace).is_empty()),
        "Invalid inbound owner",
    )?;
    if let Some(o) = owner {
        require(
            crate::registry::people(&env.tables)?
                .iter()
                .any(|p| p["id"] == o),
            "Unknown inbound owner contact",
        )?;
    }
    let owned = owner
        .map(String::from)
        .unwrap_or(format!("inbound-{}", &digest(&string(&message))?[..24]));
    let text = format!(
        "Read the complete current conversation for this staged Inbox message. The operator selected this remaining unmapped message for reply classification. Resolve current identities, refusals, owner routing and whether a reply is needed. {REPLY_OWNER_RULE}{}",
        operator(instructions)
    );
    let inputs = object([
        ("automation_contract", 1.into()),
        (
            "thread_ref",
            object([
                ("internetMessageIds", Value::Array(vec![string(&message)])),
                ("inbound", true.into()),
            ]),
        ),
        (
            "owner_ids",
            Value::Array(owner.map(string).into_iter().collect()),
        ),
        ("user_decision_ever", false.into()),
        ("instructions", string(text)),
    ]);
    let v = build_request(
        "reply",
        &object([
            ("inbound_message_id", string(message)),
            ("owner", string(&owned)),
            ("instructions", string(instructions)),
        ]),
        &owned,
        inputs,
        binding,
    )?;
    crate::gates::thread(&v, None, env)?;
    Ok(v)
}
fn operator(s: &str) -> String {
    if s.is_empty() {
        String::new()
    } else {
        format!("\nOperator instructions: {s}")
    }
}
pub fn thread(
    id: &str,
    kind: &str,
    instructions: &str,
    binding: &Value,
    env: &Environment<'_>,
) -> Result<Value> {
    require(
        ["reply", "research"].contains(&kind),
        "Historical tasks support reply or research",
    )?;
    let record = array(&env.tables["threads"]["lines"])
        .iter()
        .find(|r| r["id"] == id)
        .ok_or_else(|| Error::new(format!("Unknown historical thread: {id}")))?;
    let persons = crate::registry::people(&env.tables)?;
    let mut addresses = BTreeSet::new();
    for person in
        std::iter::once(&record["correspondent"]).chain(array(&record["co_correspondents"]))
    {
        if truth(&person["email"]) {
            addresses.insert(crate::registry::email(&person["email"])?);
        }
    }
    for a in array(&record["mail_match"]["emails"]) {
        addresses.insert(crate::registry::email(a)?);
    }
    let primary = crate::registry::email(&record["correspondent"]["email"])?;
    let person = persons
        .iter()
        .find(|p| array(&p["emails"]).iter().any(|a| a == &primary))
        .ok_or_else(|| Error::new("Register the thread correspondent in table 1 before staging"))?;
    let owners: BTreeSet<_> = persons
        .iter()
        .filter(|p| array(&p["emails"]).iter().any(|a| addresses.contains(s(a))))
        .map(|p| s(&p["id"]))
        .collect();
    let mut reference = object([
        ("thread_id", string(id)),
        ("internetMessageIds", Value::Array(vec![])),
        ("last_in", record["last_in"].clone()),
        ("last_out", record["last_out"].clone()),
    ]);
    let mut known = Vec::new();
    for k in ["last_in", "last_out"] {
        ids(&record[k], &mut known);
        for p in &persons {
            if !array(&p["emails"]).iter().any(|a| addresses.contains(s(a))) {
                continue;
            }
            for r in array(&p["records"]) {
                for receipt in array(&r["hub"]["historical_inbound"]) {
                    if k == "last_in"
                        && !record[k]["ROWID"].is_null()
                        && outreach_canon::python_eq(&receipt["rowid"], &record[k]["ROWID"])
                    {
                        ids(receipt, &mut known);
                    }
                }
            }
        }
    }
    let mut seen = BTreeSet::new();
    known.retain(|v| seen.insert(s(v).to_owned()));
    reference["internetMessageIds"] = Value::Array(known);
    let owner_label = array(&env.config["identity"]["owner_labels"])
        .first()
        .cloned()
        .unwrap_or(Value::Null);
    let inputs = object([
        ("automation_contract", 1.into()),
        ("thread_record", record.clone()),
        ("thread_ref", reference),
        (
            "owner_ids",
            Value::Array(owners.into_iter().map(string).collect()),
        ),
        (
            "user_decision_ever",
            (record["ball"] == owner_label
                || truth(&record["user_decision_ever"])
                || truth(&record["user_decisions"]))
            .into(),
        ),
        (
            "instructions",
            string(format!(
                "Read the complete current conversation, promises, requests and recorded next action. First decide whether any reply is needed. Deliver a checked useful result; preserve user drafts, reply-all, refusals and owner decisions. {REPLY_OWNER_RULE}{}",
                operator(instructions)
            )),
        ),
    ]);
    let v = build_request(
        kind,
        &object([
            ("thread_id", string(id)),
            ("revision", binding["source_revision"].clone()),
            ("inputs", string(digest(&inputs)?)),
        ]),
        s(&person["id"]),
        inputs,
        binding,
    )?;
    crate::gates::thread(&v, None, env)?;
    Ok(v)
}
fn ids(v: &Value, out: &mut Vec<Value>) {
    for k in ["internetMessageId", "message_id", "Message-ID", "messageId"] {
        if let Some(id) = crate::ingest::message_id(&v[k]) {
            out.push(string(id));
        }
    }
}
pub fn paper_summary(row: &Value) -> Value {
    let blocker = if truth(&row["blocker"]) {
        row["blocker"].clone()
    } else {
        string(
            array(&row["blocking_items"])
                .iter()
                .map(|v| s(&v["reason"]))
                .collect::<Vec<_>>()
                .join("; "),
        )
    };
    object([
        ("id", row["id"].clone()),
        ("title", row["title"].clone()),
        ("authors", row["authors"].clone()),
        (
            "version_date",
            fallback(&[&row["version_date"], &row["version"]["date"]]).clone(),
        ),
        (
            "target_journal",
            if row["target_journal"].is_string() {
                row["target_journal"].clone()
            } else {
                row["target_journal"]["name"].clone()
            },
        ),
        (
            "workspace",
            fallback(&[&row["workspace"], &row["local_package"]]).clone(),
        ),
        ("stage", row["stage"].clone()),
        ("blocker", blocker),
    ])
}
pub struct SelectedFile {
    pub relative: String,
    pub name: String,
    pub raw: Vec<u8>,
}
pub struct SubmissionSelection {
    pub request: Value,
    pub resources: Vec<(Value, Vec<u8>)>,
}
pub fn submission_rounds(env: &Environment<'_>) -> Vec<String> {
    let value = &env.tables["submission_workflow"]["rounds"];
    if let Some(o) = value.as_object() {
        o.keys().cloned().collect()
    } else {
        array(value).iter().map(|v| s(v).to_owned()).collect()
    }
}
pub fn submission(
    paper: &Value,
    files: &[SelectedFile],
    workspace_name: &str,
    round: &str,
    binding: &Value,
    env: &Environment<'_>,
) -> Result<SubmissionSelection> {
    require(
        submission_rounds(env).iter().any(|r| r == round),
        "Unknown submission round",
    )?;
    let paper = paper_summary(paper);
    require(
        paper["stage"] != "submitted",
        "Paper is already recorded as submitted",
    )?;
    require(
        !files.is_empty() && files.len() <= 100,
        "Select 1..100 task inputs explicitly",
    )?;
    for f in files {
        require(
            !f.raw.is_empty() && f.raw.len() <= 30 * 1024 * 1024,
            "Selected input exceeds the task asset limit",
        )?;
    }
    require(
        files
            .iter()
            .map(|f| &f.relative)
            .collect::<BTreeSet<_>>()
            .len()
            == files.len(),
        "Duplicate selected input",
    )?;
    let identity = digest(&object([
        ("paper", paper.clone()),
        (
            "files",
            Value::Array(
                files
                    .iter()
                    .map(|f| Value::Array(vec![string(&f.relative), string(sha256(&f.raw))]))
                    .collect(),
            ),
        ),
    ]))?;
    let tag = format!("outreach-input-{}", &identity[..32]);
    let mut artifacts = Vec::new();
    let mut resources = Vec::new();
    for (n, f) in files.iter().enumerate() {
        let hash = sha256(&f.raw);
        let suffix = f
            .name
            .rfind('.')
            .map(|i| &f.name[i..])
            .filter(|s| regex(r"^\.[A-Za-z0-9]{1,10}$").is_ok_and(|r| r.is_match(s)))
            .unwrap_or("");
        let artifact = object([
            ("release_tag", string(&tag)),
            ("name", string(format!("input-{n}-{}{suffix}", &hash[..20]))),
            ("relative_path", string(&f.relative)),
            ("sha256", string(hash)),
            ("bytes", f.raw.len().into()),
        ]);
        resources.push((artifact.clone(), f.raw.clone()));
        artifacts.push(artifact);
    }
    let path = format!("../mail/{workspace_name}");
    let owners: BTreeSet<_> = array(&env.tables["contacts"]["contacts"])
        .iter()
        .filter(|r| {
            array(&r["hub"]["workspace_paths"])
                .iter()
                .any(|v| v == &path)
        })
        .map(|r| s(&r["id"]))
        .collect();
    let link = object([
        ("paper_id", paper["id"].clone()),
        (
            "identity",
            string(digest(&object([
                ("id", paper["id"].clone()),
                ("authors", paper["authors"].clone()),
                ("workspace", paper["workspace"].clone()),
            ]))?),
        ),
    ]);
    let owner = owners
        .first()
        .map(|s| s.to_string())
        .unwrap_or(format!("workspace-{workspace_name}"));
    let inputs = object([
        ("automation_contract", 1.into()),
        ("paper", paper),
        ("submission_source", link.clone()),
        ("submission_round", string(round)),
        ("selected_inputs", Value::Array(artifacts)),
        (
            "owner_ids",
            Value::Array(owners.into_iter().map(string).collect()),
        ),
        (
            "instructions",
            string(format!(
                "Execute the {round} round. Reuse effective existing reviews and exact manuscript versions. Audit literature, proof, journal policy and the saved Oracle reports, repair and re-review gaps, compile and inspect the exact final PDF and source archive. Do not invent author facts or override publication holds. Preparation only."
            )),
        ),
    ]);
    let request = build_request(
        "submission",
        &object([
            ("source", link),
            ("selected_inputs", string(identity)),
            ("submission_round", string(round)),
        ]),
        &owner,
        inputs,
        binding,
    )?;
    Ok(SubmissionSelection { request, resources })
}
pub fn mathesis(
    row: &Value,
    manifest: &Value,
    decisions: &Value,
    prior_work: &Value,
    secret: &[u8],
    env: &Environment<'_>,
) -> Result<Value> {
    mathesis_reviewed(row)?;
    let note = &manifest["source_note"];
    let artifacts = array(&row["data"]["result"]["artifacts"]);
    let source = artifacts
        .iter()
        .find(|a| a["name"] == decisions["source_file"])
        .ok_or_else(|| Error::new("The selected source is not an exact reviewed artifact"))?;
    let collection = decisions["purpose"] == "publication"
        && (manifest["kind"] == "short_note"
            || truth(&row["request"]["inputs"]["pipeline"]["note_task_id"]));
    let mut authors = Vec::new();
    for a in array(&note["creators"]) {
        authors.push(Value::Object(
            a.as_object()
                .into_iter()
                .flatten()
                .filter(|(k, _)| ["name", "affiliation", "orcid"].contains(&k.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ));
    }
    let mut value = object([
        ("schema_version", 1.into()),
        ("task_id", row["id"].clone()),
        ("result_hash", string(digest(&row["data"]["result"])?)),
        ("workflow_hash", row["request"]["workflow_hash"].clone()),
        ("decision_ref", decisions["decision_ref"].clone()),
        ("metadata", Value::Null),
        ("source", source.clone()),
        ("reviewer", string("local-claude-loop")),
    ]);
    if collection {
        value["reviewer"] = row["data"]["review"]["reviewer"].clone();
        require(
            manifest["private_correspondence_included"] == false,
            "Scientific-only handoff required",
        )?;
        require(
            decisions["purpose"] == "publication"
                && truth(&decisions["decision_ref"])
                && decisions["make_public_after_acceptance"] == true,
            "Pre-reviewed collection requires explicit publication authorization",
        )?;
        require(
            s(&note["license"]).to_lowercase() == "cc-by-4.0",
            "Pre-reviewed notes require CC-BY-4.0",
        )?;
        let pdfs: Vec<_> = artifacts
            .iter()
            .filter(|a| a["role"] == "note-pdf" && s(&a["name"]).ends_with(".pdf"))
            .collect();
        require(
            pdfs.len() == 1
                && source["role"] == "source-archive"
                && s(&source["name"]).ends_with(".zip"),
            "Select one exact reviewed PDF and source ZIP",
        )?;
        let pdf = pdfs[0];
        let mut paper = object([
            ("title", note["title"].clone()),
            ("abstract", note["abstract"].clone()),
            ("authors", Value::Array(authors)),
            ("license", string("CC-BY-4.0")),
        ]);
        if let Some(c) =
            regex(r"^https://github\.com/[^/]+/[^/]+/blob/([0-9a-fA-F]{40})/[^?#]+\.lean(?:#.*)?$")?
                .captures(s(&note["evidence_url"]))
        {
            paper["lean"] = object([
                ("url", note["evidence_url"].clone()),
                ("revision", string(&c[1])),
            ]);
        }
        let prior = if prior_work.is_null() {
            object([
                ("verdict", string("not-run")),
                (
                    "reason",
                    string("reviewed before the 2026-11-04 Pro-check policy"),
                ),
            ])
        } else {
            prior_work.clone()
        };
        let record = object([
            ("task_id", row["id"].clone()),
            ("result_hash", value["result_hash"].clone()),
            ("problem_ids", note["problem_ids"].clone()),
            ("signed_pass", row["data"]["review"].clone()),
            ("prior_work_check", prior),
            (
                "publication_authorization",
                object([
                    ("decision_ref", decisions["decision_ref"].clone()),
                    (
                        "direct_display_decision_ref",
                        env.config["identity"]["direct_display_decision_ref"].clone(),
                    ),
                ]),
            ),
        ]);
        let review = object([
            ("verdict", string("PASS")),
            ("pdf_sha256", pdf["sha256"].clone()),
            ("source_sha256", source["sha256"].clone()),
            ("record", record),
        ]);
        value["metadata"] = object([
            ("schema", string("mathesis.pre-reviewed-delivery.v1")),
            ("producer", string("trureturing-outreach")),
            ("work_id", note["id"].clone()),
            (
                "version",
                decisions
                    .get("collection_version")
                    .unwrap_or(&note["revision"])
                    .clone(),
            ),
            (
                "previous_version",
                decisions
                    .get("collection_previous_version")
                    .unwrap_or(&decisions["previous_version"])
                    .clone(),
            ),
            ("authorize_publication", true.into()),
            ("paper", paper),
            ("review", review),
        ]);
        value["pdf"] = pdf.clone();
    } else {
        require(
            manifest["schema_version"] == "outreach-mathesis-handoff.v1"
                && manifest["private_correspondence_included"] == false,
            "Select a scientific-only Outreach handoff",
        )?;
        require(
            truth(&decisions["decision_ref"])
                && truth(&decisions["processing_consent"])
                && truth(&decisions["ai_disclosure"]),
            "Record actual author choices, processing consent and disclosure before delivery",
        )?;
        require(
            ["publication", "review_only"].contains(&s(&decisions["purpose"])),
            "Choose the actual submission purpose",
        )?;
        let mut paper = object([
            ("kind", string("note")),
            ("purpose", decisions["purpose"].clone()),
            ("title", note["title"].clone()),
            ("abstract_text", note["abstract"].clone()),
            ("authors", Value::Array(authors)),
            (
                "processing_consent",
                decisions["processing_consent"].clone(),
            ),
            ("ai_disclosure", decisions["ai_disclosure"].clone()),
            (
                "make_public_after_acceptance",
                if decisions["purpose"] == "review_only" {
                    false.into()
                } else {
                    decisions
                        .get("make_public_after_acceptance")
                        .cloned()
                        .unwrap_or(false.into())
                },
            ),
            ("open_to_contributors", false.into()),
        ]);
        if truth(&decisions["doi"]) {
            paper["doi"] = decisions["doi"].clone();
        }
        value["metadata"] = object([
            ("schema", string("mathesis.manuscript-delivery.v1")),
            ("producer", string("trureturing-outreach")),
            ("work_id", note["id"].clone()),
            ("version", string(py_str(&note["revision"]))),
            ("previous_version", decisions["previous_version"].clone()),
            ("paper", paper),
            (
                "origin",
                object([
                    ("problem_ids", note["problem_ids"].clone()),
                    ("source_url", note["evidence_url"].clone()),
                    ("license", note["license"].clone()),
                    (
                        "evidence",
                        decisions
                            .get("evidence")
                            .cloned()
                            .unwrap_or(Value::Array(vec![])),
                    ),
                ]),
            ),
        ]);
    }
    let value = sign_mathesis_envelope(&value, secret)?;
    validate_mathesis(&value, secret)?;
    Ok(value)
}
pub fn prior_work(row: &Value, resource: &dyn Fn(&Value) -> Result<Vec<u8>>) -> Result<Value> {
    for a in array(&row["data"]["result"]["artifacts"]) {
        if a["role"] != "evidence"
            || ![".md", ".txt"]
                .iter()
                .any(|suffix| s(&a["name"]).ends_with(suffix))
        {
            continue;
        }
        let raw = resource(a)?;
        require(
            a["bytes"].as_u64() == Some(raw.len() as u64) && a["sha256"] == sha256(&raw),
            "Prior-work evidence differs from the exact PASS",
        )?;
        let text = std::str::from_utf8(&raw).map_err(|e| Error::new(e.to_string()))?;
        let heading = regex(r"(?im)^(#{1,6}) +Prior[- ]work check[^\n]*\n")?;
        let Some(c) = heading.captures(text) else {
            continue;
        };
        let depth = c[1].len();
        let start = c.get(0).ok_or_else(|| Error::new("Missing heading"))?.end();
        let tail = &text[start..];
        let end = regex(&format!(r"(?m)^#{{1,{depth}}} +"))?
            .find(tail)
            .map_or(tail.len(), |m| m.start());
        let section = &tail[..end];
        let Some(verdict) =
            regex(r"(?i)\bverdict[*`_ ]*[:=]\s*[*`_ ]*(first-known|prior-exists|unclear)\b")?
                .captures(section)
        else {
            continue;
        };
        let oracle = regex(r"(?i)\bOracle\b")?.is_match(section)
            && regex(r"(?i)\bPro\b|chatgpt-?6-?pro")?.is_match(section)
            && !regex(
                r"(?i)\bno Oracle\b|Oracle[^\n.]{0,50}\bnot (?:run|called|performed|obtained)\b",
            )?
            .is_match(section);
        let mut check = object([
            ("verdict", string(verdict[1].to_lowercase())),
            ("oracle_pro_check", oracle.into()),
        ]);
        if let Some(m)=regex(r"https://chatgpt\.com/c/[A-Za-z0-9-]+")?.find(section) {check["oracle_response_ref"]=string(m.as_str());} else if let Some(c)=regex(r"(?i)(?:response ref(?:erence)?|Oracle (?:response|task)(?: ID)?)\s*[:=]\s*`?([^\n`]+)")?.captures(section) {check["oracle_response_ref"]=string(c[1].trim_matches(crate::text::whitespace));}
        check["task_id"] = row["id"].clone();
        check["result_hash"] = string(digest(&row["data"]["result"])?);
        check["evidence"] = a.clone();
        return Ok(check);
    }
    Ok(Value::Null)
}
