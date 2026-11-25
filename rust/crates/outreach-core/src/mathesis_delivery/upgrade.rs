use super::*;
use crate::{array, fallback};
use outreach_canon::parse;
fn artifact(row: &Value, name: &str) -> StageResult<Value> {
    array(&row["data"]["result"]["artifacts"])
        .iter()
        .find(|a| a["name"] == name)
        .cloned()
        .ok_or_else(|| {
            outreach_canon::Error::new("The package file is not an unambiguous reviewed artifact")
                .into()
        })
}
fn read_json(port: &mut impl DeliveryPorts, row: &Value, name: &str) -> StageResult<Value> {
    let source = checked_artifact(port, row, &artifact(row, name)?)?;
    Ok(parse(
        std::str::from_utf8(&source.bytes)
            .map_err(|e| outreach_canon::Error::new(e.to_string()))?,
    )?)
}
fn prior_section(text: &str) -> Result<Option<Value>> {
    let heading = crate::regex(r"(?im)^(#{1,6}) +Prior[- ]work check[^\n]*\n")?;
    let Some(c) = heading.captures(text) else {
        return Ok(None);
    };
    let start = c.get(0).map_or(0, |m| m.end());
    let rest = &text[start..];
    let next = crate::regex(&format!(r"(?m)^#{{1,{}}} +", c[1].len()))?;
    let section = &rest[..next.find(rest).map_or(rest.len(), |m| m.start())];
    let verdict =
        crate::regex(r"(?i)\bverdict[*`_ ]*[:=]\s*[*`_ ]*(first-known|prior-exists|unclear)\b")?;
    let Some(c) = verdict.captures(section) else {
        return Ok(None);
    };
    let oracle = crate::regex(r"(?i)\bOracle\b")?.is_match(section)
        && crate::regex(r"(?i)\bPro\b|chatgpt-?6-?pro")?.is_match(section)
        && !crate::regex(
            r"(?i)\bno Oracle\b|Oracle[^\n.]{0,50}\bnot (?:run|called|performed|obtained)\b",
        )?
        .is_match(section);
    let mut value = object([
        ("verdict", string(c[1].to_lowercase())),
        ("oracle_pro_check", oracle.into()),
    ]);
    let url = crate::regex(r"https://chatgpt\.com/c/[A-Za-z0-9-]+")?;
    if let Some(m) = url.find(section) {
        value["oracle_response_ref"] = string(m.as_str());
    } else if let Some(c) = crate::regex(
        r"(?i)(?:response ref(?:erence)?|Oracle (?:response|task)(?: ID)?)\s*[:=]\s*`?([^\n`]+)",
    )?
    .captures(section)
    {
        value["oracle_response_ref"] = string(c[1].trim());
    }
    Ok(Some(value))
}
pub(crate) fn evidence(
    replay: &Replay<'_>,
    port: &mut impl DeliveryPorts,
    row: &Value,
) -> StageResult<Value> {
    let mut candidates = vec![row.clone()];
    let mut current = row;
    let mut seen = std::collections::BTreeSet::new();
    while seen.insert(s(&current["id"])) {
        let i = &current["request"]["inputs"];
        let mut parent = fallback(&[
            &i["pipeline"]["note_task_id"],
            &i["automation_parent"]["task_id"],
        ]);
        if !truth(parent) && current["kind"] == "mathesis" && array(&i["note_sources"]).len() == 1 {
            parent = &i["note_sources"][0]["task_id"];
        }
        if !truth(parent) {
            break;
        }
        current = replay
            .rows("tasks")
            .iter()
            .find(|r| r["id"] == *parent)
            .ok_or_else(|| outreach_canon::Error::new("Unknown task"))?;
        if current["kind"] == "shortnote" {
            check(
                crate::transitions::review_current(current, &current["request"]["workflow_hash"])?,
                "Prior-work source no longer has its exact PASS",
            )?;
            candidates.push(current.clone());
            break;
        }
    }
    for candidate in candidates {
        for a in array(&candidate["data"]["result"]["artifacts"])
            .iter()
            .filter(|a| {
                a["role"] == "evidence"
                    && [".md", ".txt"]
                        .iter()
                        .any(|ext| s(&a["name"]).ends_with(ext))
            })
        {
            let source = checked_artifact(port, &candidate, a)?;
            if let Some(mut prior) = prior_section(
                std::str::from_utf8(&source.bytes)
                    .map_err(|e| outreach_canon::Error::new(e.to_string()))?,
            )? {
                prior["task_id"] = candidate["id"].clone();
                prior["result_hash"] = string(digest(&candidate["data"]["result"])?);
                prior["evidence"] = a.clone();
                return Ok(prior);
            }
        }
    }
    Ok(Value::Null)
}
pub(super) fn upgrade(
    replay: &mut Replay<'_>,
    row: &Value,
    port: &mut impl DeliveryPorts,
    secret: &[u8],
) -> StageResult<()> {
    publication_gate(
        replay,
        row,
        &row["data"]["mathesis_delivery"]["envelope"]["metadata"],
    )?;
    gates::pipeline(&row["request"], replay.rows("tasks"), replay.environment)?;
    crate::transitions::source_gate(
        &row["request"],
        &row["data"]["result"],
        replay.rows("tasks"),
    )?;
    gates::parent(&row["request"], replay.rows("tasks"), replay.environment)?;
    let artifacts = array(&row["data"]["result"]["artifacts"]);
    let names: std::collections::BTreeSet<_> = artifacts.iter().map(|a| s(&a["name"])).collect();
    let (metadata, files) = if names.contains("handoff.json") {
        let manifest = read_json(port, row, "handoff.json")?;
        check(
            manifest["schema_version"] == "outreach-mathesis-handoff.v1"
                && manifest["private_correspondence_included"] == false,
            "Select a scientific-only handoff",
        )?;
        (manifest["source_note"].clone(), manifest["files"].clone())
    } else {
        let re = crate::regex(r"^note-v[1-9][0-9]*\.json$")?;
        let manifests: Vec<_> = names.iter().filter(|n| re.is_match(n)).collect();
        if manifests.is_empty() {
            (read_json(port, row, "metadata.json")?, Value::Null)
        } else {
            check(
                manifests.len() == 1,
                "Select exactly one reviewed note revision",
            )?;
            let manifest = read_json(port, row, manifests[0])?;
            check(
                manifest["schema_version"] == "short-note.v1",
                "Invalid reviewed note manifest",
            )?;
            (manifest["metadata"].clone(), manifest["files"].clone())
        }
    };
    let (pdf_name, source_name) = if files.is_null() {
        let pdfs: Vec<_> = artifacts
            .iter()
            .filter(|a| a["role"] == "note-pdf")
            .collect();
        let sources: Vec<_> = artifacts
            .iter()
            .filter(|a| a["role"] == "source-archive" && s(&a["name"]).ends_with(".zip"))
            .collect();
        check(
            pdfs.len() == 1 && sources.len() == 1,
            "Await the reviewed PDF/source ZIP package",
        )?;
        (
            s(&pdfs[0]["name"]).to_owned(),
            s(&sources[0]["name"]).to_owned(),
        )
    } else {
        let pdf = format!("note-v{}.pdf", crate::py_str(&metadata["revision"]));
        let source = format!("source-v{}.zip", crate::py_str(&metadata["revision"]));
        check(
            files
                .as_object()
                .is_some_and(|f| f.len() == 2 && f.contains_key(&pdf) && f.contains_key(&source)),
            "Await the canonical reviewed PDF/source ZIP package",
        )?;
        (pdf, source)
    };
    for name in [&pdf_name, &source_name] {
        let source = checked_artifact(port, row, &artifact(row, name)?)?;
        check(
            files.is_null() || files[name] == sha256(&source.bytes),
            "The handoff package differs",
        )?;
    }
    // The package's scientific bytes remain the exact PASS artifacts; the
    // normalized manifest is used only to build the signed selection.
    let mut note_metadata = object([
        ("work_id", metadata["id"].clone()),
        ("paper", object([("authors", metadata["creators"].clone())])),
        (
            "review",
            object([(
                "record",
                object([("problem_ids", metadata["problem_ids"].clone())]),
            )]),
        ),
    ]);
    note_metadata["producer"] = string("trureturing-outreach");
    publication_gate(replay, row, &note_metadata)?;
    let prior = evidence(replay, port, row)?;
    let status = object([
        ("result_hash", string(digest(&row["data"]["result"])?)),
        ("check", prior.clone()),
        (
            "phase",
            string(if prior.is_null() { "not-run" } else { "ready" }),
        ),
    ]);
    let current = replay.task(&row["id"])?;
    let mut data = current["data"].clone();
    data["mathesis_prior_work"] = status;
    replay.update(&current, s(&current["state"]), data)?;
    port.save(&replay.tables)?;
    let prior = if prior.is_null() {
        evidence(replay, port, row)?
    } else {
        prior
    };
    let decision = &replay.environment.tables["decisions"]["short_note_publication"];
    let mut choices = decision.clone();
    choices["source_file"] = string(source_name);
    choices["collection_version"] = metadata["revision"].clone();
    let mut previous = None;
    for r in replay.rows("tasks") {
        let d = &r["data"]["mathesis_delivery"];
        let m = &d.get("collection_envelope").unwrap_or(&d["envelope"])["metadata"];
        if m["work_id"] != metadata["id"] {
            continue;
        }
        check(
            !(m["schema"] == "mathesis.manuscript-delivery.v1"
                && m["version"] == string(crate::py_str(&metadata["revision"]))
                && d["phase"] == "delivered"),
            "This note revision already has a manuscript-deliveries receipt; do not resend",
        )?;
        if m["schema"] != "mathesis.pre-reviewed-delivery.v1" {
            continue;
        }
        check(
            d["phase"] == "delivered",
            "Await the earlier collected version readback",
        )?;
        check(
            d["receipt"]["version"].as_i64() < metadata["revision"].as_i64(),
            "Collected revision already selected; retain its exact request",
        )?;
        previous = previous.max(d["receipt"]["version"].as_i64());
    }
    choices["collection_previous_version"] = previous.map(Value::from).unwrap_or(Value::Null);
    let manifest = object([
        ("kind", string("short_note")),
        ("private_correspondence_included", false.into()),
        ("source_note", metadata),
    ]);
    let current = replay.task(&row["id"])?;
    let envelope = crate::builders::mathesis(
        &current,
        &manifest,
        &choices,
        &prior,
        secret,
        replay.environment,
    )?;
    let mut delivery = row["data"]["mathesis_delivery"].clone();
    delivery["collection_envelope"] = envelope;
    update(replay, &row["id"], delivery, None)?;
    port.save_forced(&replay.tables)?;
    Ok(())
}
