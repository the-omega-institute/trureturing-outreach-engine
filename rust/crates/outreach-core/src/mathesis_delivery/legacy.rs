use super::*;
use crate::{array, fallback};
use outreach_canon::{Value, sign_mathesis_envelope};

pub(super) fn eligible(row: &Value) -> bool {
    let d = &row["data"]["mathesis_delivery"];
    row["kind"] == "mathesis"
        && d["envelope"]["metadata"]["schema"] == "mathesis.manuscript-delivery.v1"
        && ["delivered", "pending"].iter().any(|p| d["phase"] == *p)
        && truth(
            &fallback(&[
                &d["legacy_migration"]["original_receipt"],
                &d["receipt"],
                &d["legacy_receipt"],
            ])["submission"],
        )
}
fn status(v: &Value) -> Option<String> {
    for k in ["status", "submission_status"] {
        if let Some(s) = v[k].as_str() {
            return Some(s.to_lowercase().replace('-', "_"));
        }
    }
    v.get("submission")
        .filter(|v| v.is_object())
        .and_then(status)
}
fn save(
    replay: &mut Replay<'_>,
    port: &mut impl DeliveryPorts,
    row: &Value,
    delivery: Value,
) -> StageResult<()> {
    update(replay, &row["id"], delivery, None)?;
    port.save_forced(&replay.tables)?;
    Ok(())
}
fn build(replay: &Replay<'_>, row: &Value, secret: &[u8]) -> StageResult<Value> {
    envelopes::mathesis_reviewed(row)?;
    let legacy = &row["data"]["mathesis_delivery"]["envelope"];
    let m = &legacy["metadata"];
    let paper = &m["paper"];
    let origin = &m["origin"];
    check(
        crate::regex(r"^[a-z0-9-]{1,160}$")?.is_match(s(&m["work_id"])),
        "Legacy Mathesis work ID cannot enter the collection",
    )?;
    let version = crate::py_str(&m["version"])
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|v| *v > 0)
        .ok_or_else(|| StageError {
            summary: object([
                ("type", string("ValueError")),
                (
                    "reason",
                    string("Legacy Mathesis version is not a collection revision"),
                ),
            ]),
        })?;
    let authors: Vec<_> = array(&paper["authors"])
        .iter()
        .map(|a| {
            Value::Object(
                a.as_object()
                    .into_iter()
                    .flatten()
                    .filter(|(k, _)| ["name", "affiliation", "orcid"].contains(&k.as_str()))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            )
        })
        .collect();
    let abstract_text = fallback(&[&paper["abstract"], &paper["abstract_text"], &m["abstract"]]);
    check(
        truth(&paper["title"])
            && truth(abstract_text)
            && !authors.is_empty()
            && s(fallback(&[&paper["license"], &origin["license"]])).to_lowercase() == "cc-by-4.0",
        "Legacy Mathesis package lacks collection metadata or CC-BY-4.0",
    )?;
    let artifacts = array(&row["data"]["result"]["artifacts"]);
    let pdfs: Vec<_> = artifacts
        .iter()
        .filter(|a| a["role"] == "note-pdf" && s(&a["name"]).ends_with(".pdf"))
        .collect();
    let sources: Vec<_> = artifacts
        .iter()
        .filter(|a| a["role"] == "source-archive" && s(&a["name"]).ends_with(".zip"))
        .collect();
    let canonical_pdf = pdfs
        .iter()
        .find(|a| a["name"] == format!("note-v{version}.pdf"));
    let canonical_source = sources
        .iter()
        .find(|a| a["name"] == format!("source-v{version}.zip"));
    let (pdf, source) = if let (Some(p), Some(s)) = (canonical_pdf, canonical_source) {
        (*p, *s)
    } else {
        check(
            pdfs.len() == 1 && sources.len() == 1,
            "Legacy Mathesis package lacks one exact reviewed PDF/source pair",
        )?;
        (pdfs[0], sources[0])
    };
    let prior = fallback(&[
        &row["data"]["mathesis_prior_work"]["check"],
        &m["review"]["record"]["prior_work_check"],
    ]);
    let prior = if prior.is_null() {
        object([
            ("verdict", string("not-run")),
            (
                "reason",
                string("reviewed before the 2026-11-04 Pro-check policy"),
            ),
        ])
    } else {
        prior.clone()
    };
    let decision = fallback(&[&legacy["decision_ref"]])
        .as_str()
        .unwrap_or("legacy-manuscript-delivery-migration");
    let previous = replay
        .rows("tasks")
        .iter()
        .filter(|r| {
            r["id"] != row["id"]
                && r["data"]["mathesis_delivery"]["collection_envelope"]["metadata"]["work_id"]
                    == m["work_id"]
                && r["data"]["mathesis_delivery"]["legacy_migration"]["phase"] == "redelivered"
        })
        .filter_map(|r| r["data"]["mathesis_delivery"]["receipt"]["version"].as_i64())
        .max();
    let record = object([
        ("task_id", row["id"].clone()),
        ("result_hash", string(digest(&row["data"]["result"])?)),
        (
            "problem_ids",
            origin
                .get("problem_ids")
                .cloned()
                .unwrap_or(Value::Array(vec![])),
        ),
        ("signed_pass", row["data"]["review"].clone()),
        ("prior_work_check", prior),
        (
            "publication_authorization",
            object([
                ("decision_ref", string(decision)),
                (
                    "direct_display_decision_ref",
                    replay.environment.config["identity"]["direct_display_decision_ref"].clone(),
                ),
            ]),
        ),
    ]);
    let mut collection_paper = object([
        ("title", paper["title"].clone()),
        ("abstract", abstract_text.clone()),
        ("authors", Value::Array(authors)),
        ("license", string("CC-BY-4.0")),
    ]);
    let url = s(fallback(&[&origin["source_url"], &m["evidence_url"]]));
    if let Some(c) = crate::regex(
        r"^https://github\.com/[^/]+/[^/]+/blob/([0-9a-fA-F]{40})/[^?#]+\.lean(?:#.*)?$",
    )?
    .captures(url)
    {
        collection_paper["lean"] = object([("url", string(url)), ("revision", string(&c[1]))]);
    }
    let metadata = object([
        ("schema", string("mathesis.pre-reviewed-delivery.v1")),
        ("producer", string("trureturing-outreach")),
        ("work_id", m["work_id"].clone()),
        ("version", version.into()),
        (
            "previous_version",
            previous.map(Value::from).unwrap_or(Value::Null),
        ),
        ("authorize_publication", true.into()),
        ("paper", collection_paper),
        (
            "review",
            object([
                ("verdict", string("PASS")),
                ("pdf_sha256", pdf["sha256"].clone()),
                ("source_sha256", source["sha256"].clone()),
                ("record", record),
            ]),
        ),
    ]);
    let envelope = sign_mathesis_envelope(
        &object([
            ("schema_version", 1.into()),
            ("task_id", row["id"].clone()),
            ("result_hash", string(digest(&row["data"]["result"])?)),
            ("workflow_hash", row["request"]["workflow_hash"].clone()),
            ("decision_ref", string(decision)),
            ("metadata", metadata),
            ("pdf", pdf.clone()),
            ("source", source.clone()),
            ("reviewer", row["data"]["review"]["reviewer"].clone()),
        ]),
        secret,
    )?;
    envelopes::validate_mathesis(&envelope, secret)?;
    Ok(envelope)
}
fn synthetic_receipt(metadata: &Value) -> Value {
    object([
        ("schema", string("mathesis.pre-reviewed-receipt.v1")),
        ("status", string("collected")),
        ("work_id", metadata["work_id"].clone()),
        ("version", metadata["version"].clone()),
        ("pdf_sha256", metadata["review"]["pdf_sha256"].clone()),
        ("source_sha256", metadata["review"]["source_sha256"].clone()),
    ])
}
pub(super) fn migrate(
    replay: &mut Replay<'_>,
    client: &mut impl MathesisClient,
    port: &mut impl DeliveryPorts,
    secret: &[u8],
) -> StageResult<()> {
    let mut candidates: Vec<_> = replay
        .rows("tasks")
        .iter()
        .filter(|r| {
            eligible(r)
                && revision_hold(replay, r).is_none()
                && !["redelivered", "skipped"]
                    .iter()
                    .any(|p| r["data"]["mathesis_delivery"]["legacy_migration"]["phase"] == *p)
        })
        .cloned()
        .collect();
    candidates.sort_by(|a, b| {
        a["updated"]
            .as_f64()
            .partial_cmp(&b["updated"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut blocked = false;
    candidates.retain(|r| {
        if truth(&r["data"]["mathesis_delivery"]["legacy_migration"]["last_error"]) {
            blocked = true;
            true
        } else {
            !blocked
        }
    });
    let limit = replay.environment.config["mathesis"]["legacy_migration_per_tick"]
        .as_u64()
        .unwrap_or(3) as usize;
    let mut processed = 0;
    for original in candidates.into_iter().take(limit) {
        if let Err(error) = step(replay, &original, client, port, secret) {
            let row = replay.task(&original["id"])?;
            let mut delivery = row["data"]["mathesis_delivery"].clone();
            let mut migration = delivery
                .get("legacy_migration")
                .cloned()
                .unwrap_or(object([("phase", string("pending"))]));
            migration["last_error"] = error.summary;
            if ![
                "withdraw-intent",
                "redelivery-intent",
                "collection-confirmed",
            ]
            .iter()
            .any(|p| migration["phase"] == *p)
            {
                migration["phase"] = string("pending");
            }
            delivery["legacy_migration"] = migration;
            save(replay, port, &row, delivery)?;
        }
        processed += 1;
    }
    let mut summary = object([
        ("pending", 0.into()),
        ("withdrawn", 0.into()),
        ("redelivered", 0.into()),
        ("skipped", 0.into()),
        ("skipped_reasons", object([])),
        ("processed_this_tick", processed.into()),
    ]);
    for row in replay.rows("tasks").iter().filter(|r| eligible(r)) {
        let m = &row["data"]["mathesis_delivery"]["legacy_migration"];
        if !truth(m) {
            continue;
        }
        let phase = if summary.get(s(&m["phase"])).is_some() {
            s(&m["phase"])
        } else {
            "pending"
        };
        summary[phase] = (summary[phase].as_i64().unwrap_or(0) + 1).into();
        if phase == "skipped" {
            let reason = m["reason"].as_str().unwrap_or("unspecified");
            summary["skipped_reasons"][reason] =
                (summary["skipped_reasons"][reason].as_i64().unwrap_or(0) + 1).into();
        }
    }
    replay.set_state("mathesis-legacy-migration", summary)?;
    port.save_forced(&replay.tables)?;
    Ok(())
}
fn step(
    replay: &mut Replay<'_>,
    row: &Value,
    client: &mut impl MathesisClient,
    port: &mut impl DeliveryPorts,
    secret: &[u8],
) -> StageResult<()> {
    let mut delivery = row["data"]["mathesis_delivery"].clone();
    let mut migration = delivery
        .get("legacy_migration")
        .cloned()
        .unwrap_or(object([("phase", string("pending"))]));
    let legacy_receipt = fallback(&[&migration["original_receipt"], &delivery["receipt"]]).clone();
    let handle = ReceiptHandle {
        submission: s(&legacy_receipt["submission"]).into(),
        version: legacy_receipt["version"].as_i64().unwrap_or(0),
    };
    let observed = client.status(&handle)?;
    let mut observed_status = status(&observed);
    if matches!(
        observed_status.as_deref(),
        Some("accepted" | "not_accepted")
    ) {
        migration["phase"] = string("skipped");
        migration["reason"] = string(format!(
            "submission is {}",
            observed_status.unwrap_or_default()
        ));
        migration["original_receipt"] = legacy_receipt;
        delivery["legacy_migration"] = migration;
        return save(replay, port, row, delivery);
    }
    if !matches!(
        observed_status.as_deref(),
        Some("draft" | "in_review" | "withdrawn")
    ) {
        let reason = observed_status
            .as_ref()
            .map_or("Mathesis status missing".into(), |s| {
                format!("unexpected submission status: {s}")
            });
        if observed_status.is_some() {
            migration["phase"] = string("pending");
        }
        migration["last_error"] =
            object([("type", string("ValueError")), ("reason", string(reason))]);
        delivery["legacy_migration"] = migration;
        return save(replay, port, row, delivery);
    }
    if delivery.get("collection_envelope").is_none() {
        match build(replay, row, secret).and_then(|e| {
            gate(replay, row)?;
            Ok(e)
        }) {
            Ok(envelope) => {
                migration["original_receipt"] = legacy_receipt.clone();
                delivery["collection_envelope"] = envelope;
                remove(&mut delivery, "receipt");
                delivery["legacy_migration"] = migration.clone();
                save(replay, port, row, delivery.clone())?;
            }
            Err(error) if error.summary["type"] == "GateError" => {
                migration["phase"] = string("skipped");
                migration["reason"] = error.summary["reason"].clone();
                delivery["legacy_migration"] = migration;
                return save(replay, port, row, delivery);
            }
            Err(error) => return Err(error),
        }
    }
    let envelope = delivery["collection_envelope"].clone();
    let metadata = &envelope["metadata"];
    if let Err(error) = publication_gate(replay, row, metadata) {
        if error.summary["type"] == "GateError" {
            migration["phase"] = string("skipped");
            migration["reason"] = error.summary["reason"].clone();
            delivery["legacy_migration"] = migration;
            return save(replay, port, row, delivery);
        }
        return Err(error);
    }
    let mut receipt = migration["redelivery_receipt"].clone();
    let mut collected = Value::Null;
    if receipt.is_null() && migration["phase"] == "redelivery-intent" {
        match readback(client, metadata) {
            Ok(paper) => {
                collected = paper;
                receipt = synthetic_receipt(metadata);
            }
            Err(error) if error.status() == Some(404) => {}
            Err(error) => {
                migration["last_error"] = error.summary;
                delivery["legacy_migration"] = migration;
                return save(replay, port, row, delivery);
            }
        }
    }
    if receipt.is_null() {
        let pdf = checked_artifact(port, row, &envelope["pdf"])?;
        let source = checked_artifact(port, row, &envelope["source"])?;
        migration["phase"] = string("redelivery-intent");
        migration["request_hash"] = string(digest(&envelope)?);
        delivery["request_hash"] = migration["request_hash"].clone();
        delivery["legacy_migration"] = migration.clone();
        update(replay, &row["id"], delivery.clone(), None)?;
        let intent = Persisted::save(
            CollectionIntent {
                metadata: metadata.clone(),
                pdf,
                source,
            },
            port,
            &replay.tables,
        )?;
        receipt = match client.deliver_pre_reviewed(&intent) {
            Ok(r) => collection_receipt(&r, metadata)?,
            Err(error) if error.status() == Some(409) => {
                collected = readback(client, metadata)?;
                synthetic_receipt(metadata)
            }
            Err(error) => return Err(error),
        };
    }
    if collected.is_null() {
        collected = readback(client, metadata)?;
    }
    migration["phase"] = string("collection-confirmed");
    migration["redelivery_receipt"] = receipt.clone();
    migration["collected_paper"] = collected.clone();
    delivery["legacy_migration"] = migration.clone();
    delivery["receipt"] = receipt.clone();
    save(replay, port, row, delivery.clone())?;
    if matches!(observed_status.as_deref(), Some("draft" | "in_review")) {
        migration["phase"] = string("withdraw-intent");
        remove(&mut migration, "last_error");
        delivery["legacy_migration"] = migration.clone();
        update(replay, &row["id"], delivery.clone(), None)?;
        let intent = Persisted::save(
            WithdrawIntent {
                submission: handle.submission.clone(),
            },
            port,
            &replay.tables,
        )?;
        let result = client.withdraw(&intent).and_then(|response| {
            migration["withdrawal_response"] = response;
            client.status(&handle)
        });
        let observed = match result {
            Ok(v) => v,
            Err(error) => {
                migration["last_error"] = error.summary;
                delivery["legacy_migration"] = migration;
                return save(replay, port, row, delivery);
            }
        };
        observed_status = status(&observed);
        if observed_status.as_deref() != Some("withdrawn") {
            return Err(StageError {
                summary: object([
                    ("type", string("ValueError")),
                    (
                        "reason",
                        string(format!(
                            "withdrawal not confirmed; submission remains {}",
                            observed_status.as_deref().unwrap_or("None")
                        )),
                    ),
                ]),
            });
        }
        migration["withdrawal_status"] = observed;
    }
    migration["phase"] = string("redelivered");
    migration["collected_paper"] = collected.clone();
    remove(&mut migration, "last_error");
    delivery["phase"] = string("delivered");
    delivery["collected_paper"] = collected;
    delivery["legacy_receipt"] = legacy_receipt;
    delivery["legacy_migration"] = migration;
    delivery["receipt"] = receipt;
    update(replay, &row["id"], delivery, Some("verified"))?;
    port.save_forced(&replay.tables)?;
    Ok(())
}
