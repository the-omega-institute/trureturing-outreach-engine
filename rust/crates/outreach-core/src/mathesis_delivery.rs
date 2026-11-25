//! Offline Mathesis delivery state machine. Ports supply all observations.
//! Python retains exclusive production ledger ownership.
mod legacy;
pub(crate) mod upgrade;

use crate::{envelopes, gates, ingest::Replay, object, s, string, truth, validation::require};
use outreach_canon::{Result, Value, digest, python_eq, sha256};

pub use crate::persistence::{Ledger, Persisted, SavedSnapshot};

pub struct SourceFile {
    pub name: String,
    pub bytes: Vec<u8>,
}
pub struct ManuscriptIntent {
    pub metadata: Value,
    pub source: SourceFile,
}
pub struct CollectionIntent {
    pub metadata: Value,
    pub pdf: SourceFile,
    pub source: SourceFile,
}
pub struct LinkIntent {
    pub envelope: Value,
    pub receipt: Value,
}
pub struct WithdrawIntent {
    pub submission: String,
}
pub struct ReceiptHandle {
    pub submission: String,
    pub version: i64,
}

/// Every external write requires a successful forced-save capability.
///
/// ```compile_fail
/// use outreach_core::mathesis_delivery::{MathesisClient, ManuscriptIntent};
/// fn bypass(client: &mut impl MathesisClient, intent: ManuscriptIntent) {
///     client.deliver(intent);
/// }
/// ```
pub trait MathesisClient {
    fn verify_owner(&mut self) -> StageResult<()>;
    fn find(&mut self, metadata: &Value) -> StageResult<Value>;
    fn status(&mut self, handle: &ReceiptHandle) -> StageResult<Value>;
    fn deliver(&mut self, intent: Persisted<ManuscriptIntent>) -> StageResult<Value>;
    fn deliver_pre_reviewed(&mut self, intent: Persisted<CollectionIntent>) -> StageResult<Value>;
    fn withdraw(&mut self, intent: Persisted<WithdrawIntent>) -> StageResult<Value>;
    fn collected(&mut self, work: &str, version: &Value) -> StageResult<Value>;
    fn collected_file(&mut self, work: &str, version: &Value, kind: &str) -> StageResult<Vec<u8>>;
}
pub trait DeliveryServices {
    fn ledger_tag(&self) -> &str;
    /// Success means the intent and reservation are durable, including when the
    /// saver confirms an already committed identical generation.
    fn save_forced(&mut self, tables: &Value) -> StageResult<SavedSnapshot>;
    fn save(&mut self, tables: &Value) -> StageResult<()>;
    fn save_with_force(&mut self, tables: &Value, force: bool) -> StageResult<()> {
        if force {
            self.save_forced(tables)?;
        } else {
            self.save(tables)?;
        }
        Ok(())
    }
    fn source(&mut self, descriptor: &Value) -> StageResult<SourceFile>;
    fn allow(&mut self, lane: &str, key: &str) -> bool;
    fn queue_public_link(&mut self, intent: Persisted<LinkIntent>) -> StageResult<Value>;
}

/// Sealed persistence port. Only the ledger wrapper can acknowledge a save.
/// ```compile_fail
/// use outreach_core::mathesis_delivery::DeliveryPorts;
/// struct Bypass;
/// impl DeliveryPorts for Bypass {}
/// ```
pub trait DeliveryPorts: DeliveryServices + crate::persistence::sealed::Sealed {
    fn saved_snapshot(&self) -> Option<(&SavedSnapshot, &str)>;
}
impl<S: DeliveryServices> DeliveryPorts for Ledger<S> {
    fn saved_snapshot(&self) -> Option<(&SavedSnapshot, &str)> {
        self.saved_snapshot()
    }
}

#[derive(Debug)]
pub struct StageError {
    pub summary: Value,
}
pub type StageResult<T> = std::result::Result<T, StageError>;
impl From<outreach_canon::Error> for StageError {
    fn from(error: outreach_canon::Error) -> Self {
        Self {
            summary: object([
                ("type", string("GateError")),
                ("reason", string(error.to_string())),
            ]),
        }
    }
}
impl StageError {
    pub fn status(&self) -> Option<i64> {
        self.summary["http_status"].as_i64()
    }
}
fn check(condition: bool, message: &str) -> StageResult<()> {
    Ok(require(condition, message)?)
}
fn remove(value: &mut Value, key: &str) {
    if let Some(o) = value.as_object_mut() {
        o.shift_remove(key);
    }
}
fn receipt_matches(receipt: &Value, envelope: &Value) -> StageResult<Value> {
    let metadata = &envelope["metadata"];
    check(
        receipt["schema"] == "mathesis.manuscript-receipt.v1"
            && receipt["submission"]
                .as_str()
                .is_some_and(|s| !s.is_empty())
            && receipt["version"].as_i64().is_some_and(|n| n > 0)
            && ["producer", "work_id"]
                .iter()
                .all(|k| receipt[k] == metadata[k])
            && receipt["upstream_version"] == metadata["version"]
            && receipt["source_sha256"] == envelope["source"]["sha256"],
        "Mathesis did not acknowledge the exact delivered manuscript",
    )?;
    if metadata["paper"]["purpose"] == "review_only" {
        check(
            receipt["publication"].is_null(),
            "A private review unexpectedly has a publication",
        )?;
    }
    Ok(receipt.clone())
}
fn checked_source(port: &mut impl DeliveryPorts, descriptor: &Value) -> StageResult<SourceFile> {
    let source = port.source(descriptor)?;
    check(
        descriptor["bytes"].as_u64() == Some(source.bytes.len() as u64)
            && descriptor["sha256"] == sha256(&source.bytes),
        "Mathesis source readback differs from the reviewed artifact",
    )?;
    Ok(source)
}
fn update(
    replay: &mut Replay<'_>,
    id: &Value,
    delivery: Value,
    state: Option<&str>,
) -> StageResult<()> {
    let row = replay.task(id)?;
    let mut data = row["data"].clone();
    data["mathesis_delivery"] = delivery;
    replay.update(&row, state.unwrap_or(s(&row["state"])), data)?;
    Ok(())
}
fn gate(replay: &Replay<'_>, row: &Value) -> StageResult<()> {
    check(
        truth(&row["data"]["publication_loop"])
            || crate::reviews::matches_workflow(row, &(replay.environment.workflow)("mathesis")?),
        "Mathesis workflow changed; re-review",
    )?;
    envelopes::mathesis_reviewed(row)?;
    let rows = replay.rows("tasks");
    let env = replay.environment;
    gates::note_revisions(row, rows, &replay.tables, &Value::Null)?;
    gates::pipeline(&row["request"], rows, env)?;
    crate::transitions::source_gate(&row["request"], &row["data"]["result"], rows)?;
    gates::parent(&row["request"], rows, env)?;
    gates::owner(&row["request"], env)?;
    Ok(())
}
fn needs_delivery(row: &Value, active: bool) -> bool {
    let d = &row["data"]["mathesis_delivery"];
    if truth(&d["collection_envelope"])
        && truth(&d["legacy_migration"])
        && d["legacy_migration"]["phase"] != "redelivered"
    {
        return false;
    }
    ["ready", "pending", "conflict"]
        .iter()
        .any(|p| d["phase"] == *p)
        || (active
            && d["phase"] == "delivered"
            && d.get("collection_envelope").unwrap_or(&d["envelope"])["metadata"]["schema"]
                == "mathesis.pre-reviewed-delivery.v1"
            && d["public_link"]["phase"] != "queued")
}

#[derive(Default)]
pub struct StageOptions {
    pub attempted: Option<std::collections::BTreeSet<String>>,
    pub again: bool,
    pub previous: Vec<String>,
}
pub fn advance(
    replay: &mut Replay<'_>,
    client: &mut impl MathesisClient,
    port: &mut impl DeliveryPorts,
    secret: &[u8],
) -> StageResult<()> {
    advance_with_options(replay, client, port, secret, &mut StageOptions::default())
}
fn revision_hold(replay: &Replay<'_>, row: &Value) -> Option<String> {
    gates::note_revisions(row, replay.rows("tasks"), &replay.tables, &Value::Null)
        .err()
        .map(|e| e.to_string())
}
pub fn advance_with_options(
    replay: &mut Replay<'_>,
    client: &mut impl MathesisClient,
    port: &mut impl DeliveryPorts,
    secret: &[u8],
    options: &mut StageOptions,
) -> StageResult<()> {
    let config = &replay.environment.config;
    let test = config["mathesis"]["test_only"] == true;
    check(
        !test || port.ledger_tag() == "outreach-mathesis-test-20261031",
        "Synthetic acceptance cannot operate on the production ledger",
    )?;
    let active = !truth(&config["paused"])
        && truth(&config["kinds"]["mathesis"])
        && config["mathesis"]["mode"] == "deliver";
    let slots = config["mathesis"]["deliveries_per_tick"]
        .as_i64()
        .unwrap_or(4)
        .max(0) as usize;
    let slots = slots.saturating_sub(options.attempted.as_ref().map_or(0, |s| s.len()));
    let mut identity_verified = false;
    let legacy = active
        && !test
        && (options.attempted.is_none() || options.again)
        && replay.rows("tasks").iter().any(|r| {
            legacy::eligible(r)
                && revision_hold(replay, r).is_none()
                && !["skipped", "redelivered"]
                    .iter()
                    .any(|p| r["data"]["mathesis_delivery"]["legacy_migration"]["phase"] == *p)
        });
    if legacy {
        client.verify_owner()?;
        identity_verified = true;
        legacy::migrate(replay, client, port, secret)?;
    }
    let mut rows: Vec<_> = replay
        .rows("tasks")
        .iter()
        .filter(|r| {
            r["kind"] == "mathesis"
                && needs_delivery(r, active)
                && !options
                    .attempted
                    .as_ref()
                    .is_some_and(|ids| ids.contains(s(&r["id"])))
                && (revision_hold(replay, r).is_none()
                    || ["pending", "conflict"]
                        .contains(&s(&r["data"]["mathesis_delivery"]["phase"])))
        })
        .cloned()
        .collect();
    let linked = crate::lanes::linked(replay.rows("tasks"), replay.environment)?;
    rows.sort_by(|a, b| {
        (!linked.contains(s(&a["id"])))
            .cmp(&(!linked.contains(s(&b["id"]))))
            .then_with(|| {
                (a["data"]["mathesis_delivery"]["phase"] == "delivered")
                    .cmp(&(b["data"]["mathesis_delivery"]["phase"] == "delivered"))
            })
            .then_with(|| {
                a["updated"]
                    .as_f64()
                    .partial_cmp(&b["updated"].as_f64())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    rows.truncate(slots);
    if let Some(attempted) = &mut options.attempted {
        attempted.extend(rows.iter().map(|r| s(&r["id"]).to_owned()));
    }
    let priority = |r: &Value| {
        options
            .previous
            .iter()
            .position(|p| r["id"] == *p)
            .unwrap_or(options.previous.len())
    };
    rows.sort_by_key(|r| {
        (
            if linked.is_empty() {
                false
            } else {
                !linked.contains(s(&r["id"]))
            },
            priority(r),
        )
    });
    for original in rows {
        if !port.allow("mathesis", s(&original["id"])) {
            continue;
        }
        let id = &original["id"];
        let row = replay.task(id)?;
        let delivery = &row["data"]["mathesis_delivery"];
        if delivery["phase"] == "ready" && (!active || row["state"] != "handoff-ready") {
            continue;
        }
        let outcome = advance_row(
            replay,
            &row,
            client,
            port,
            secret,
            active,
            &mut identity_verified,
        );
        if let Err(error) = outcome {
            let row = replay.task(id)?;
            let mut delivery = row["data"]["mathesis_delivery"].clone();
            delivery["last_error"] = error.summary.clone();
            if error.status() == Some(409) {
                delivery["phase"] = string("conflict");
            }
            update(replay, id, delivery, None)?;
            port.save(&replay.tables)?;
        }
        let latest = replay.task(id)?;
        if latest["revision"] == original["revision"]
            && latest["data"]["mathesis_delivery"]["phase"] == "conflict"
        {
            update(
                replay,
                id,
                latest["data"]["mathesis_delivery"].clone(),
                None,
            )?;
            port.save(&replay.tables)?;
        }
    }
    Ok(())
}
fn advance_row(
    replay: &mut Replay<'_>,
    row: &Value,
    client: &mut impl MathesisClient,
    port: &mut impl DeliveryPorts,
    secret: &[u8],
    active: bool,
    identity_verified: &mut bool,
) -> StageResult<()> {
    let id = &row["id"];
    let mut delivery = row["data"]["mathesis_delivery"].clone();
    let envelope = delivery
        .get("collection_envelope")
        .unwrap_or(&delivery["envelope"])
        .clone();
    let test = replay.environment.config["mathesis"]["test_only"] == true;
    envelopes::validate_mathesis_mode(&envelope, secret, test)?;
    let own_note = truth(&row["request"]["inputs"]["pipeline"]["note_task_id"])
        || truth(&row["data"]["publication_loop"])
        || envelope["metadata"]["schema"] == "mathesis.pre-reviewed-delivery.v1";
    let collection = envelope["metadata"]["schema"] == "mathesis.pre-reviewed-delivery.v1";
    if own_note
        && !test
        && !collection
        && envelope["metadata"]["paper"]["purpose"] == "publication"
        && !truth(&delivery["request_hash"])
        && !truth(&delivery["receipt"])
    {
        upgrade::upgrade(replay, row, port, secret)?;
        let current = replay.task(id)?;
        return advance_row(
            replay,
            &current,
            client,
            port,
            secret,
            active,
            identity_verified,
        );
    }
    if !*identity_verified {
        client.verify_owner()?;
        *identity_verified = true;
    }
    if collection {
        return self::collection(replay, row, &envelope, client, port, active, secret);
    }
    let mut metadata = envelope["metadata"].clone();
    let coordinate = format!(
        "mathesis:{}",
        digest(&Value::Array(
            [
                metadata["producer"].clone(),
                metadata["work_id"].clone(),
                metadata["version"].clone(),
                metadata["paper"]["purpose"].clone()
            ]
            .into()
        ))?
    );
    let request_hash = digest(&object([
        ("metadata", metadata.clone()),
        ("filename", envelope["source"]["name"].clone()),
        ("source", envelope["source"]["sha256"].clone()),
    ]))?;
    let reserved = replay.state(&coordinate);
    check(
        !truth(&reserved) || reserved["request_hash"] == request_hash,
        "Mathesis manuscript/version already has a different delivery",
    )?;
    if truth(&reserved["revision"]) {
        check(
            !truth(&delivery["revision"])
                || python_eq(&delivery["revision"], &reserved["revision"]),
            "Keep the checkpointed Mathesis delivery version",
        )?;
        delivery["revision"] = reserved["revision"].clone();
    }
    merge(&mut metadata, &delivery["revision"]);
    let mut expected = envelope.clone();
    expected["metadata"] = metadata.clone();
    let mut observed = if truth(&delivery["receipt"]) {
        delivery["receipt"].clone()
    } else {
        reserved["receipt"].clone()
    };
    if ["pending", "conflict"]
        .iter()
        .any(|p| delivery["phase"] == *p)
        && !truth(&observed)
    {
        let found = match client.find(&metadata) {
            Err(e) if e.status() == Some(404) => Value::Null,
            other => other?,
        };
        if delivery["phase"] == "conflict" {
            if !(truth(&found)
                && found["schema"] == "mathesis.manuscript-receipt.v1"
                && found["producer"] == metadata["producer"]
                && found["work_id"] == metadata["work_id"]
                && found["current"] == true
                && found["version"].as_i64().is_some_and(|v| v > 0)
                && python_eq(&found["current_version"], &found["version"])
                && found["upstream_version"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty()))
            {
                return Ok(());
            }
            if found["source_sha256"] == envelope["source"]["sha256"] {
                delivery["revision"] = object([("version", found["upstream_version"].clone())]);
                metadata = envelope["metadata"].clone();
                merge(&mut metadata, &delivery["revision"]);
                expected["metadata"] = metadata.clone();
                observed = receipt_matches(&found, &expected)?;
            } else if !own_note
                && !truth(&delivery["revision"])
                && active
                && row["state"] == "handoff-ready"
            {
                let version = found["version"]
                    .as_i64()
                    .ok_or_else(|| outreach_canon::Error::new("Invalid version"))?;
                let next = version
                    .checked_add(1)
                    .ok_or_else(|| outreach_canon::Error::new("Version overflow"))?;
                delivery["revision"] = object([
                    (
                        "version",
                        string(format!("outreach-{}-{next}", &request_hash[..24])),
                    ),
                    ("previous_version", version.into()),
                ]);
                merge(&mut metadata, &delivery["revision"]);
                expected["metadata"] = metadata.clone();
            } else {
                return Ok(());
            }
        } else if truth(&found)
            && found["upstream_version"] == metadata["version"]
            && found["source_sha256"] == envelope["source"]["sha256"]
        {
            observed = receipt_matches(&found, &expected)?;
        }
    }
    let mut reservation = object([
        ("request_hash", string(&request_hash)),
        ("task_id", id.clone()),
    ]);
    if truth(&delivery["revision"]) {
        reservation["revision"] = delivery["revision"].clone();
    }
    if !truth(&observed) {
        if !active || row["state"] != "handoff-ready" || own_note {
            return Ok(());
        }
        gate(replay, row)?;
        envelopes::mathesis_reviewed(row)?;
        check(
            envelope["task_id"] == row["id"]
                && envelope["result_hash"] == digest(&row["data"]["result"])?
                && envelope["workflow_hash"] == row["request"]["workflow_hash"]
                && crate::array(&row["data"]["result"]["artifacts"]).contains(&envelope["source"]),
            "The delivery package differs from the reviewed task",
        )?;
        let source = checked_source(port, &envelope["source"])?;
        if !port.allow("mathesis", s(id)) {
            return Ok(());
        }
        delivery["phase"] = string("pending");
        delivery["request_hash"] = string(&request_hash);
        update(replay, id, delivery.clone(), None)?;
        replay.set_state(&coordinate, reservation.clone())?;
        let intent = Persisted::save(ManuscriptIntent { metadata, source }, port, &replay.tables)?;
        observed = receipt_matches(&client.deliver(intent)?, &expected)?;
    }
    delivery["phase"] = string("pending");
    delivery["receipt"] = receipt_matches(&observed, &expected)?;
    update(replay, id, delivery.clone(), None)?;
    reservation["receipt"] = observed.clone();
    replay.set_state(&coordinate, reservation.clone())?;
    port.save_forced(&replay.tables)?;
    let handle = ReceiptHandle {
        submission: s(&observed["submission"]).into(),
        version: observed["version"].as_i64().unwrap_or(0),
    };
    let persisted = receipt_matches(&client.status(&handle)?, &expected)?;
    check(
        persisted["submission"] == observed["submission"]
            && python_eq(&persisted["version"], &observed["version"]),
        "Mathesis receipt changed its submission identity",
    )?;
    delivery["phase"] = string("delivered");
    delivery["receipt"] = persisted.clone();
    remove(&mut delivery, "last_error");
    update(
        replay,
        id,
        delivery,
        (row["state"] == "handoff-ready").then_some("verified"),
    )?;
    reservation["receipt"] = persisted;
    replay.set_state(&coordinate, reservation)?;
    port.save(&replay.tables)
}
fn merge(target: &mut Value, source: &Value) {
    if let (Some(target), Some(source)) = (target.as_object_mut(), source.as_object()) {
        target.extend(source.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
}
#[allow(clippy::too_many_arguments)]
fn collection(
    replay: &mut Replay<'_>,
    row: &Value,
    envelope: &Value,
    client: &mut impl MathesisClient,
    port: &mut impl DeliveryPorts,
    active: bool,
    secret: &[u8],
) -> StageResult<()> {
    let metadata = &envelope["metadata"];
    envelopes::mathesis_reviewed(row)?;
    check(
        crate::array(&row["data"]["result"]["artifacts"]).contains(&envelope["source"])
            && crate::array(&row["data"]["result"]["artifacts"]).contains(&envelope["pdf"]),
        "Collection files differ from the exact PASS",
    )?;
    let signed = &metadata["review"]["record"]["signed_pass"];
    check(
        python_eq(signed, &row["data"]["review"])
            && outreach_canon::verify_envelope(signed, secret)?,
        "Collection requires the authentic signed PASS for this exact result",
    )?;
    check(
        crate::validation::independent_reviewer(&signed["reviewer"])?,
        "Collection requires independent signed review",
    )?;
    check(
        metadata["review"]["record"]["prior_work_check"].is_object()
            && metadata["review"]["record"]["prior_work_check"]["verdict"].is_string(),
        "Collection requires a Prior-work record",
    )?;
    let mut delivery = row["data"]["mathesis_delivery"].clone();
    let coordinate = format!(
        "mathesis-collection:{}",
        digest(&Value::Array(vec![
            metadata["producer"].clone(),
            metadata["work_id"].clone(),
            metadata["version"].clone()
        ]))?
    );
    let binding = digest(&object([
        ("metadata", metadata.clone()),
        (
            "files",
            object([
                (
                    "pdf",
                    object([
                        ("name", envelope["pdf"]["name"].clone()),
                        ("sha256", envelope["pdf"]["sha256"].clone()),
                    ]),
                ),
                (
                    "source",
                    object([
                        ("name", envelope["source"]["name"].clone()),
                        ("sha256", envelope["source"]["sha256"].clone()),
                    ]),
                ),
            ]),
        ),
    ]))?;
    let reserved = replay.state(&coordinate);
    check(
        !truth(&reserved) || reserved["request_hash"] == binding,
        "Collection version already has another selection",
    )?;
    let mut receipt = if truth(&delivery["receipt"]) {
        delivery["receipt"].clone()
    } else {
        reserved["receipt"].clone()
    };
    if !truth(&receipt) {
        if !active || row["state"] != "handoff-ready" {
            return Ok(());
        }
        gate(replay, row)?;
        publication_gate(replay, row, metadata)?;
        let pdf = checked_artifact(port, row, &envelope["pdf"])?;
        let source = checked_artifact(port, row, &envelope["source"])?;
        if !port.allow("mathesis", s(&row["id"])) {
            return Ok(());
        }
        delivery["phase"] = string("pending");
        delivery["request_hash"] = string(&binding);
        update(replay, &row["id"], delivery.clone(), None)?;
        replay.set_state(
            &coordinate,
            object([
                ("request_hash", string(&binding)),
                ("task_id", row["id"].clone()),
            ]),
        )?;
        let intent = Persisted::save(
            CollectionIntent {
                metadata: metadata.clone(),
                pdf,
                source,
            },
            port,
            &replay.tables,
        )?;
        receipt = client.deliver_pre_reviewed(intent)?;
    }
    collection_receipt(&receipt, metadata)?;
    delivery["phase"] = string("pending");
    delivery["receipt"] = receipt.clone();
    update(replay, &row["id"], delivery.clone(), None)?;
    replay.set_state(
        &coordinate,
        object([
            ("request_hash", string(&binding)),
            ("task_id", row["id"].clone()),
            ("receipt", receipt.clone()),
        ]),
    )?;
    let link = Persisted::save(
        LinkIntent {
            envelope: envelope.clone(),
            receipt: receipt.clone(),
        },
        port,
        &replay.tables,
    )?;
    let paper = readback(client, metadata)?;
    let sent_revision_hold = revision_hold(replay, row);
    if active && sent_revision_hold.is_none() && delivery["public_link"]["phase"] != "queued" {
        delivery["public_link"] = port.queue_public_link(link)?;
    } else if delivery["public_link"]["phase"] != "queued" {
        delivery["public_link"] = object([
            ("phase", string("pending")),
            (
                "reason",
                string(
                    sent_revision_hold
                        .as_deref()
                        .unwrap_or("Resume to queue the public reading link"),
                ),
            ),
        ]);
    }
    delivery["phase"] = string("delivered");
    delivery["collected_paper"] = paper;
    remove(&mut delivery, "last_error");
    update(replay, &row["id"], delivery, Some("verified"))?;
    port.save(&replay.tables)
}

fn collection_receipt(receipt: &Value, metadata: &Value) -> StageResult<Value> {
    check(
        receipt["schema"] == "mathesis.pre-reviewed-receipt.v1"
            && receipt["status"] == "collected"
            && receipt["work_id"] == metadata["work_id"]
            && receipt["version"].as_i64().is_some()
            && receipt["version"] == metadata["version"]
            && ["pdf_sha256", "source_sha256"]
                .iter()
                .all(|k| receipt[k] == metadata["review"][k]),
        "Mathesis receipt differs from the exact reviewed PDF/source version",
    )?;
    Ok(receipt.clone())
}
fn person_name(name: &str) -> String {
    let name = name.split_once(',').map_or_else(
        || name.to_owned(),
        |(family, given)| format!("{given} {family}"),
    );
    crate::casefold(&name.split_whitespace().collect::<Vec<_>>().join(" "))
}
fn readback(client: &mut impl MathesisClient, metadata: &Value) -> StageResult<Value> {
    let paper = client.collected(s(&metadata["work_id"]), &metadata["version"])?;
    check(
        paper["id"] == metadata["work_id"]
            && paper["revision"] == string(crate::py_str(&metadata["version"]))
            && paper["previous_version"]
                == if metadata["previous_version"].is_null() {
                    Value::Null
                } else {
                    string(crate::py_str(&metadata["previous_version"]))
                },
        "Collected paper version differs",
    )?;
    let expected = &metadata["paper"];
    let authors = crate::array(&expected["authors"]);
    let observed = crate::array(&paper["authors"]);
    let authors_match = authors.len() == observed.len()
        && authors.iter().zip(observed).all(|(a, b)| {
            let mut a = a.clone();
            let mut b = b.clone();
            if !b["name"].is_string() {
                return false;
            }
            a["name"] = string(person_name(s(&a["name"])));
            b["name"] = string(person_name(s(&b["name"])));
            python_eq(&a, &b)
        });
    check(
        authors_match
            && expected
                .as_object()
                .into_iter()
                .flatten()
                .filter(|(k, _)| k.as_str() != "authors")
                .all(|(k, v)| python_eq(&paper[k], v)),
        "Collected bibliographic metadata differs from the selected version",
    )?;
    for kind in ["pdf", "source"] {
        let url = format!(
            "/api/v1/collected-papers/{}/files/{kind}?version={}",
            s(&metadata["work_id"]),
            crate::py_str(&metadata["version"])
        );
        check(
            paper[kind]["url"] == url,
            "Collected file must pin the exact version",
        )?;
        check(
            sha256(&client.collected_file(s(&metadata["work_id"]), &metadata["version"], kind)?)
                == metadata["review"][format!("{kind}_sha256")],
            "Collected file hash differs from the exact PASS",
        )?;
    }
    Ok(paper)
}
fn publication_gate(replay: &Replay<'_>, row: &Value, metadata: &Value) -> StageResult<()> {
    use std::collections::BTreeSet;
    let table = &replay.environment.tables["decisions"];
    let decision = &table["short_note_publication"];
    check(
        decision.is_object() && decision["enabled"] == true,
        "No active standing publication decision",
    )?;
    check(
        truth(&decision["decision_ref"])
            && decision["purpose"] == "publication"
            && truth(&decision["processing_consent"])
            && decision["make_public_after_acceptance"] == true,
        "The standing decision must authorize publication and processing",
    )?;
    let mut selected = BTreeSet::new();
    let mut lineage = Vec::new();
    let mut current = row;
    loop {
        if !selected.insert(s(&current["id"]).to_owned()) {
            return Err(outreach_canon::Error::new("Publication source cycle").into());
        }
        lineage.push(current);
        let inputs = &current["request"]["inputs"];
        let mut parent = crate::fallback(&[
            &inputs["pipeline"]["note_task_id"],
            &inputs["automation_parent"]["task_id"],
        ]);
        if !truth(parent)
            && current["kind"] == "mathesis"
            && crate::array(&inputs["note_sources"]).len() == 1
        {
            parent = &inputs["note_sources"][0]["task_id"];
        }
        if !truth(parent) {
            break;
        }
        current = replay
            .rows("tasks")
            .iter()
            .find(|r| r["id"] == *parent)
            .ok_or_else(|| outreach_canon::Error::new("Unknown task"))?;
    }
    let identities = |r: &Value| {
        let i = &r["request"]["inputs"];
        let d = &r["data"]["publication_loop"];
        let mut ids: BTreeSet<String> = [
            &r["id"],
            &i["result_id"],
            &i["pipeline"]["result_id"],
            &i["result"]["id"],
            &d["note_id"],
        ]
        .iter()
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect();
        for list in [&i["result_ids"], &i["problem_ids"], &d["problem_ids"]] {
            ids.extend(
                crate::array(list)
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned),
            );
        }
        ids
    };
    for parent in &lineage {
        selected.extend(identities(parent));
    }
    selected.insert(s(&metadata["work_id"]).into());
    selected.extend(
        crate::array(&metadata["review"]["record"]["problem_ids"])
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned),
    );
    for hold in crate::array(&table["holds"]) {
        let mut targets: BTreeSet<String> = ["legacy_id", "task_id", "note_id", "id"]
            .iter()
            .filter_map(|k| hold[k].as_str().map(str::to_owned))
            .collect();
        targets.extend(
            crate::array(&hold["problem_ids"])
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned),
        );
        check(
            selected.is_disjoint(&targets)
                || !["HOLD", "JOINT-WORK"].contains(&s(&hold["decision"]).to_uppercase().as_str()),
            "Publication is held or joint-excluded",
        )?;
    }
    for related in replay.rows("tasks").iter().filter(|r| {
        lineage.contains(r)
            || (["shortnote", "mathesis"].contains(&s(&r["kind"]))
                && !identities(r).is_disjoint(&selected))
    }) {
        let data = &related["data"];
        let inputs = &related["request"]["inputs"];
        check(
            data["review"]["verdict"] != "HOLD"
                && ![inputs, data, &data["result"]].iter().any(|v| {
                    v["joint_work"] == true
                        || ["joint", "joint-work"].contains(&s(&v["authorship"]))
                }),
            "Publication is held or joint-excluded",
        )?;
    }
    let allowed: BTreeSet<_> = crate::array(&decision["independent_authors"])
        .iter()
        .map(|v| person_name(s(v)))
        .collect();
    check(
        crate::array(&metadata["paper"]["authors"])
            .iter()
            .all(|a| allowed.contains(&person_name(s(&a["name"])))),
        "Publication is held or joint-excluded",
    )
}
fn checked_artifact(
    port: &mut impl DeliveryPorts,
    row: &Value,
    descriptor: &Value,
) -> StageResult<SourceFile> {
    check(
        crate::regex(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")?.is_match(s(&descriptor["name"])),
        "Select a safe reviewed artifact name",
    )?;
    check(
        crate::array(&row["data"]["result"]["artifacts"])
            .iter()
            .filter(|a| a["name"] == descriptor["name"])
            .count()
            == 1,
        "The package file is not an unambiguous reviewed artifact",
    )?;
    check(
        descriptor["release_tag"] == format!("outreach-task-{}", s(&row["id"])),
        "Preserve the task source identity",
    )?;
    check(
        descriptor["bytes"]
            .as_u64()
            .is_some_and(|n| n > 0 && n <= 30 * 1024 * 1024),
        "Invalid scientific artifact size",
    )?;
    let source = port.source(descriptor)?;
    check(
        descriptor["bytes"].as_u64() == Some(source.bytes.len() as u64)
            && descriptor["sha256"] == sha256(&source.bytes),
        "The preserved artifact differs from the exact PASS",
    )?;
    Ok(source)
}
