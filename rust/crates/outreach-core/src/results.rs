//! Result gates needed by local ingestion and signed body replacements.
use crate::{validation::*, *};
use outreach_canon::{canonical, python_eq, sha256};
use std::collections::BTreeSet;

pub fn firstcontact_gate(request: &Value, env: &Environment<'_>) -> Result<()> {
    if request["kind"] != "firstcontact" {
        return Ok(());
    }
    let recipients = &request["inputs"]["recipient_emails"];
    require(
        recipients.is_array() && truth(recipients),
        "First contact requires all intended To/Cc recipient_emails for table 3 preflight",
    )?;
    let normalized: Vec<_> = array(recipients)
        .iter()
        .map(crate::registry::email)
        .collect::<Result<_>>()?;
    require(
        normalized.iter().collect::<BTreeSet<_>>().len() == normalized.len(),
        "Duplicate first-contact recipient",
    )?;
    let people = crate::registry::people(&env.tables)?;
    for (raw, address) in array(recipients).iter().zip(normalized) {
        let reasons = crate::registry::preflight(s(raw), "", &env.tables, &people)?;
        require(
            reasons.is_empty(),
            format!(
                "First-contact index gate refused {}: {}",
                s(raw),
                reasons.join(", ")
            ),
        )?;
        require(
            people
                .iter()
                .any(|p| array(&p["emails"]).iter().any(|a| a == &address)),
            format!(
                "Register the eligible recipient in table 1 before task admission: {}",
                s(raw)
            ),
        )?;
    }
    Ok(())
}
pub fn effective_inputs(row: &Value) -> Value {
    let mut i = row["request"]["inputs"].clone();
    for (k, v) in row["data"]["thread_inputs"]
        .as_object()
        .into_iter()
        .flatten()
    {
        i[k] = v.clone();
    }
    let history = &row["data"]["mailbox_history"];
    if row["kind"] == "firstcontact" && !history.is_null() {
        if history["complete"] == true {
            let mut included = Vec::new();
            let mut excluded = Vec::new();
            for recipient in array(&row["request"]["inputs"]["recipient_emails"]) {
                let normalized = crate::registry::email(recipient).unwrap_or_default();
                let aliases = array(&history["recipient_addresses"][&normalized]);
                let messages: Vec<_> = array(&history["messages"])
                    .iter()
                    .filter(|m| array(&m["addresses"]).iter().any(|a| aliases.contains(a)))
                    .collect();
                let attempts: Vec<_> = array(&history["attempts"])
                    .iter()
                    .filter(|m| array(&m["addresses"]).iter().any(|a| aliases.contains(a)))
                    .collect();
                if !messages.is_empty() || !attempts.is_empty() {
                    let mut seen = BTreeSet::new();
                    let threads = messages
                        .iter()
                        .filter_map(|m| {
                            seen.insert(s(&m["conversationId"]).to_owned())
                                .then_some(m["conversationId"].clone())
                        })
                        .collect();
                    excluded.push(object([("email",recipient.clone()),("reason",string("Existing correspondence/draft or delivery attempt; use the existing thread, preserve owner drafts and reconcile uncertain sends")),("thread_ids",Value::Array(threads)),("attempt_keys",Value::Array(attempts.iter().map(|a|a["key"].clone()).collect()))]));
                } else {
                    included.push(recipient.clone());
                }
            }
            let primary = array(&row["request"]["inputs"]["to"])
                .iter()
                .find(|a| included.contains(a))
                .or_else(|| included.first())
                .cloned();
            i["to"] = Value::Array(primary.clone().into_iter().collect());
            i["cc"] = Value::Array(
                included
                    .iter()
                    .filter(|a| primary.as_ref() != Some(*a))
                    .cloned()
                    .collect(),
            );
            i["recipient_emails"] = Value::Array(included);
            let mut prior = array(&row["request"]["inputs"]["excluded_recipients"]).to_vec();
            prior.extend(excluded);
            i["excluded_recipients"] = Value::Array(prior);
        }
        i["mailbox_history"] = history.clone();
    }
    if truth(&row["request"]["inputs"]["user_decision_ever"]) {
        i["user_decision_ever"] = true.into();
    }
    i
}
fn english(v: &Value) -> bool {
    s(v).chars().any(|c| c.is_ascii_alphabetic())
}
pub fn routing(result: &Value, row: &Value, env: &Environment<'_>) -> Result<()> {
    let r = &result["routing"];
    if r.is_null() {
        return require(
            !truth(&row["request"]["inputs"]["automation_contract"]),
            "This task requires explicit mail/decision/submission routing",
        );
    }
    require(
        keys(
            r,
            &["mail", "questions", "independent_submission"],
            &["registry_proposal"],
        ),
        "Invalid result routing",
    )?;
    let proposal = &r["registry_proposal"];
    let hint = &row["request"]["inputs"]["registry_proposal_hint"];
    if !hint.is_null() {
        require(
            proposal.is_object(),
            "A linked alias needs routing.registry_proposal for operator registration",
        )?;
    }
    if !hint.is_null() || !proposal.is_null() {
        require(
            keys(
                proposal,
                &["email", "contact_id", "evidence_ref"],
                &["name", "reason"],
            ) && ["email", "contact_id", "evidence_ref"]
                .iter()
                .all(|k| nonempty(&proposal[k])),
            "Invalid registry proposal",
        )?;
        if !hint.is_null() {
            require(
                casefold(s(&proposal["email"])) == casefold(s(&hint["email"]))
                    && proposal["contact_id"] == hint["contact_id"]
                    && proposal["evidence_ref"] == hint["evidence_ref"],
                "Registry proposal does not preserve the linked alias evidence",
            )?;
        }
    }
    hashable(&r["mail"])?;
    require(
        ["auto", "owner", "none"].contains(&s(&r["mail"])),
        "Unknown mail route",
    )?;
    require(
        ["firstcontact", "reply"].contains(&s(&row["kind"])) || r["mail"] == "none",
        "A scientific task cannot send mail",
    )?;
    let q = &r["questions"];
    require(q.is_array(), "Questions must be a list")?;
    for q in array(q) {
        require(
            keys(q, &["id", "question", "source_quote", "evidence_ref"], &[])
                && q.as_object().is_some_and(|o| o.values().all(nonempty)),
            "A user question needs its actual quote and source",
        )?;
    }
    require(
        array(q)
            .iter()
            .map(|q| s(&q["id"]))
            .collect::<BTreeSet<_>>()
            .len()
            == array(q).len(),
        "Duplicate user question",
    )?;
    if truth(q) {
        require(
            r["mail"] != "auto",
            "Unresolved user decisions cannot auto-send",
        )?;
        require(
            result["outcome"] == "needs-decision",
            "User questions need the reviewed decision-request outcome",
        )?;
    }
    if result["outcome"] == "needs-decision" {
        require(
            truth(q) && result["packet"].is_null(),
            "A decision request is not a finished letter",
        )?;
    }
    if r["mail"] == "none" {
        require(
            result["packet"].is_null(),
            "No-mail result must not contain a sendable packet",
        )?;
    }
    let paper = &r["independent_submission"];
    if !paper.is_null() {
        require(
            keys(
                paper,
                &[
                    "id",
                    "title",
                    "authors",
                    "target_journal",
                    "source_quote",
                    "message_id",
                    "artifact_names",
                ],
                &[],
            ),
            "Independent-paper proposal needs identity, authors, source and selected inputs",
        )?;
        require(
            [
                "id",
                "title",
                "target_journal",
                "source_quote",
                "message_id",
            ]
            .iter()
            .all(|k| nonempty(&paper[k])),
            "Missing independent-paper provenance",
        )?;
        require(
            regex(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")?.is_match(s(&paper["id"])),
            "Invalid independent-paper ID",
        )?;
        let authors = array(&paper["authors"]);
        require(
            paper["authors"].is_array()
                && !authors.is_empty()
                && authors
                    .iter()
                    .all(|a| array(&env.config["identity"]["team_authors"]).contains(a))
                && authors.iter().map(s).collect::<BTreeSet<_>>().len() == authors.len(),
            "Joint and conditional coauthorship belong to the collaboration track",
        )?;
        let names = &paper["artifact_names"];
        require(
            names.is_array()
                && truth(names)
                && array(names).iter().map(s).collect::<BTreeSet<_>>().len() == array(names).len()
                && array(names)
                    .iter()
                    .all(|n| array(&result["artifacts"]).iter().any(|a| a["name"] == *n)),
            "Select actual preserved manuscript inputs",
        )?;
        require(
            row["kind"] == "reply",
            "Independent classification starts from correspondence",
        )?;
    }
    Ok(())
}
pub fn validate(result: &Value, row: &Value, env: &Environment<'_>) -> Result<()> {
    let fields = [
        "schema_version",
        "task_id",
        "outcome",
        "report",
        "artifacts",
        "packet",
    ];
    let mut optional = vec!["routing", "self_audit", "headline"];
    let kind = s(&row["kind"]);
    if ["shortnote", "research"].contains(&kind) {
        optional.extend(["author_contacts", "author_contacts_reason"]);
    }
    if kind == "shortnote" {
        optional.extend(["formalization_gaps", "value_assessment", "prior_work_check"]);
    }
    if ["firstcontact", "reply", "mathesis"].contains(&kind) {
        optional.push("note_sources");
    }
    if ["firstcontact", "reply", "research"].contains(&kind) {
        optional.push("owner_brief");
    }
    if kind == "firstcontact" {
        optional.push("recipient_exclusions");
    }
    if kind == "reply"
        || (kind == "research" && truth(&effective_inputs(row)["mailbox_observation"]))
    {
        optional.push("registry_patch");
    }
    require(
        result.is_object(),
        "Invalid CMA result contract: expected a JSON object",
    )?;
    let mut unexpected: Vec<_> = result
        .as_object()
        .into_iter()
        .flatten()
        .map(|(k, _)| k.as_str())
        .filter(|k| !fields.contains(k) && !optional.contains(k))
        .collect();
    unexpected.sort();
    let mut missing: Vec<_> = fields
        .iter()
        .copied()
        .filter(|k| result.get(k).is_none())
        .collect();
    missing.sort();
    require(
        unexpected.is_empty() && missing.is_empty(),
        format!(
            "Invalid CMA result contract: unexpected keys {} / missing keys {}",
            py_str(&Value::Array(
                unexpected.iter().map(|k| string(*k)).collect()
            )),
            py_str(&Value::Array(missing.iter().map(|k| string(*k)).collect()))
        ),
    )?;
    require(
        one(&result["schema_version"]) && result["task_id"] == row["id"],
        "Wrong CMA result identity",
    )?;
    hashable(&result["outcome"])?;
    require(
        ["completed", "hold", "no-reply", "needs-decision"].contains(&s(&result["outcome"]))
            && nonempty(&result["report"]),
        "Missing research judgment",
    )?;
    if let Some(links) = result.get("note_sources") {
        sources(links)?;
    }
    if kind == "shortnote" {
        value_assessment(
            result,
            result["outcome"] == "completed"
                && row["request"]["inputs"]["note_round"] != "draft"
                && row["request"]["workflow_hash"] == (env.workflow)("shortnote")?,
        )?;
        if let Some(check) = result.get("prior_work_check") {
            require(
                check.is_object()
                    && ["first-known", "prior-exists", "unclear"].contains(&s(&check["verdict"])),
                "Invalid prior_work_check verdict",
            )?;
            require(
                check["items"].is_array() && array(&check["items"]).iter().all(Value::is_object),
                "prior_work_check needs a list of evidence items",
            )?;
        }
        require(
            result["value_assessment"]["verdict"] != "trivial" || result["outcome"] == "completed",
            "A trivial note must return its completed artifacts and value assessment for uplift",
        )?;
    }
    if let Some(h) = result.get("headline") {
        require(h.is_string(), "headline must be a string")?;
        require(
            nonempty(h)
                && s(h).chars().count() <= 300
                && crate::text::split_lines(s(h)) == vec![s(h)]
                && english(h),
            "headline must be one English line of 1..300 characters",
        )?;
    }
    if let Some(a) = result.get("self_audit") {
        require(a.is_array(), "self_audit must be a list")?;
        for i in array(a) {
            require(
                keys(i, &["check", "status", "note"], &[])
                    && i.as_object().is_some_and(|o| o.values().all(nonempty))
                    && ["pass", "fail", "n/a"].contains(&s(&i["status"])),
                "Invalid self_audit entry",
            )?;
        }
    }
    if let Some(p) = result.get("registry_patch") {
        require(
            keys(
                p,
                &["incoming_summary", "praise", "refusal", "new_contact"],
                &["alias_of"],
            ),
            "Invalid registry_patch shape",
        )?;
        require(
            nonempty(&p["incoming_summary"])
                && !s(&p["incoming_summary"]).contains('\n')
                && english(&p["incoming_summary"])
                && s(&p["incoming_summary"]).chars().count() <= 400,
            "incoming_summary must be one short factual English line",
        )?;
        require(
            p["praise"].is_array(),
            "registry_patch praise must be a list",
        )?;
        for item in array(&p["praise"]) {
            require(
                keys(item, &["quote", "context", "story"], &[])
                    && item.as_object().is_some_and(|o| o.values().all(nonempty)),
                "Invalid registry_patch praise entry",
            )?;
        }
        require(
            p["refusal"].is_null()
                || (keys(&p["refusal"], &["quote", "scope"], &[])
                    && p["refusal"]
                        .as_object()
                        .is_some_and(|o| o.values().all(nonempty))),
            "Invalid registry_patch refusal",
        )?;
        let alias = &p["alias_of"];
        if !alias.is_null() {
            require(
                p["new_contact"].is_null(),
                "registry_patch alias_of and new_contact are mutually exclusive",
            )?;
            require(
                keys(alias, &["contact_id", "evidence_ref", "reason"], &[])
                    && alias.as_object().is_some_and(|o| o.values().all(nonempty)),
                "Invalid registry_patch alias_of",
            )?;
            require(
                s(&alias["reason"]).chars().count() <= 300
                    && crate::text::split_lines(s(&alias["reason"])).len() == 1
                    && english(&alias["reason"]),
                "alias_of reason must be one short factual English sentence",
            )?;
            require(
                array(&env.tables["contacts"]["contacts"])
                    .iter()
                    .any(|c| c["id"] == alias["contact_id"]),
                "registry_patch alias_of contact_id does not exist",
            )?;
            unregistered(
                row,
                env,
                "registry_patch alias_of is only for an unregistered sender",
            )?;
        }
        if !p["new_contact"].is_null() {
            require(
                keys(
                    &p["new_contact"],
                    &[
                        "name",
                        "title",
                        "title_evidence_url",
                        "affiliation",
                        "evidence_url",
                    ],
                    &[],
                ) && ["name", "title_evidence_url", "affiliation", "evidence_url"]
                    .iter()
                    .all(|k| nonempty(&p["new_contact"][k]))
                    && ["Professor", "Dr.", "none"].contains(&s(&p["new_contact"]["title"])),
                "Invalid registry_patch new_contact",
            )?;
            url(&p["new_contact"]["title_evidence_url"])?;
            url(&p["new_contact"]["evidence_url"])?;
            unregistered(
                row,
                env,
                "registry_patch new_contact is only for an unregistered sender",
            )?;
        }
    }
    routing(result, row, env)?;
    if let Some(b) = result.get("owner_brief") {
        require(
            ["reply", "firstcontact", "research"].contains(&kind),
            "owner_brief is only valid for correspondence or research results",
        )?;
        require(
            keys(b, &["summary_zh", "decisions_zh", "cautions_zh"], &[]),
            "Invalid owner_brief shape",
        )?;
        let chinese = |v: &Value| {
            nonempty(v)
                && !s(v).contains('\n')
                && s(v).chars().any(|c| ('\u{3400}'..='\u{9fff}').contains(&c))
        };
        require(
            chinese(&b["summary_zh"])
                && s(&b["summary_zh"])
                    .chars()
                    .filter(|c| "。！？".contains(*c))
                    .count()
                    <= 1,
            "owner_brief.summary_zh must be one Chinese sentence",
        )?;
        for key in ["decisions_zh", "cautions_zh"] {
            require(
                b[key].is_array(),
                format!("owner_brief.{key} must be a list"),
            )?;
            for v in array(&b[key]) {
                require(
                    chinese(v),
                    format!("owner_brief.{key} entries must be Chinese strings"),
                )?;
            }
        }
    }
    if result["routing"]["mail"] == "owner" || truth(&result["routing"]["questions"]) {
        require(
            result.get("owner_brief").is_some(),
            "Owner routing or user questions require owner_brief",
        )?;
    }
    if result.get("author_contacts").is_some() || result.get("author_contacts_reason").is_some() {
        authors(result, &row["request"])?;
    }
    if result["outcome"] == "no-reply" {
        require(
            kind == "reply" && result["routing"]["mail"] == "none" && result["packet"].is_null(),
            "No-reply must be an explicit correspondence judgment",
        )?;
    }
    if result["outcome"] == "needs-decision" {
        require(
            truth(&result["routing"]["questions"]) && result["packet"].is_null(),
            "A decision request must expose its questions for independent review",
        )?;
    }
    require(result["artifacts"].is_array(), "Missing artifact list")?;
    require(
        array(&result["artifacts"]).len() <= 100,
        "Too many task artifacts",
    )?;
    let mut names = BTreeSet::new();
    for a in array(&result["artifacts"]) {
        require(
            keys(
                a,
                &["release_tag", "name", "sha256", "bytes", "role"],
                &["agent_path"],
            ),
            "Invalid artifact reference",
        )?;
        require(
            ["release_tag", "name", "role"]
                .iter()
                .all(|k| nonempty(&a[k])),
            "Invalid artifact identity",
        )?;
        require(
            a["release_tag"] == format!("outreach-task-{}", s(&row["id"])),
            "Artifacts must belong to this task release",
        )?;
        require(
            regex(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")?.is_match(s(&a["name"])),
            "Invalid artifact filename",
        )?;
        require(hex(&a["sha256"], 64), "Invalid artifact checksum")?;
        require(
            integer(&a["bytes"])
                && a["bytes"]
                    .as_number()
                    .is_some_and(|n| !n.as_str().starts_with('-') && n.as_str() != "0"),
            "Invalid artifact size",
        )?;
        names.insert(s(&a["name"]));
        if a.get("agent_path").is_some() {
            require(
                regex(r"^deliverables/[A-Za-z0-9][A-Za-z0-9._/-]{0,499}$")?
                    .is_match(s(&a["agent_path"]))
                    && s(&a["agent_path"])
                        .split('/')
                        .all(|p| !["", ".", ".."].contains(&p))
                    && a["bytes"].as_u64().is_some_and(|n| n <= 30 * 1024 * 1024),
                "Invalid Agent artifact path or size",
            )?;
        }
    }
    require(
        names.len() == array(&result["artifacts"]).len(),
        "Duplicate task artifact name",
    )?;
    if result["packet"].is_object() && result["packet"].get("artifact").is_some() {
        require(
            keys(&result["packet"], &["artifact"], &[]) && result["packet"]["artifact"].is_string(),
            "Invalid mail packet artifact reference",
        )?;
        let matching: Vec<_> = array(&result["artifacts"])
            .iter()
            .filter(|a| a["name"] == result["packet"]["artifact"])
            .collect();
        require(
            matching.len() == 1 && matching[0]["role"] == "mail-packet",
            "Packet must name one mail-packet artifact of this result",
        )?;
        require(
            matching[0]["bytes"]
                .as_u64()
                .is_some_and(|n| n <= 25 * 1024 * 1024),
            "Mail packet artifact exceeds 25 MiB",
        )?;
        return Err(Error::new("Mail packet artifact is not allowed here"));
    }
    let roles: BTreeSet<_> = array(&result["artifacts"])
        .iter()
        .map(|a| s(&a["role"]))
        .collect();
    if kind == "shortnote" && result["outcome"] == "completed" {
        require(
            ["note-tex", "note-pdf"].iter().all(|r| roles.contains(r)),
            "Short note needs source and PDF",
        )?;
    }
    if kind == "submission" && result["outcome"] == "completed" {
        require(
            [
                "manuscript-pdf",
                "manuscript-tex",
                "source-archive",
                "evidence",
            ]
            .iter()
            .all(|r| roles.contains(r)),
            "Submission preparation needs source, PDF, archive and reviews",
        )?;
    }
    if ["firstcontact", "reply", "daily"].contains(&kind)
        && result["outcome"] == "completed"
        && result["routing"]["mail"] != "none"
    {
        let p = &result["packet"];
        require(
            p.is_object() && p["kind"] == kind,
            "Missing exact outgoing packet",
        )?;
        packet(p)?;
        let inputs = effective_inputs(row);
        if kind == "reply" {
            reply_signoff(s(&p["body"]), &env.config)?;
            for k in ["thread_id", "latest_inbound_message_id"] {
                if let Some(v) = inputs.get(k) {
                    require(
                        python_eq(&p[k], v),
                        format!("Reply packet changed its source {k}"),
                    )?;
                }
            }
            require(
                !truth(&inputs["user_decision_ever"]) || truth(&p["user_decision_ever"]),
                "A revision cannot clear user-decision history",
            )?;
        }
        if kind == "firstcontact" {
            if row["data"].get("mailbox_history").is_some() {
                require(
                    row["data"]["mailbox_history"]["complete"] == true,
                    "Fresh first-contact mailbox history is incomplete",
                )?;
            }
            let mut request = row["request"].clone();
            request["inputs"] = inputs.clone();
            firstcontact_gate(&request, env)?;
            let actual: BTreeSet<_> = array(&p["to"])
                .iter()
                .chain(array(&p["cc"]))
                .map(s)
                .collect();
            let intended: BTreeSet<_> = array(&inputs["recipient_emails"])
                .iter()
                .map(crate::registry::email)
                .collect::<Result<_>>()?;
            let entries = result
                .get("recipient_exclusions")
                .filter(|v| truth(v))
                .cloned()
                .unwrap_or(Value::Array(vec![]));
            require(entries.is_array(), "recipient_exclusions must be a list")?;
            let mut dropped = BTreeSet::new();
            for entry in array(&entries) {
                require(
                    keys(entry, &["email", "reason"], &[])
                        && nonempty(&entry["email"])
                        && nonempty(&entry["reason"]),
                    "Invalid recipient_exclusions entry",
                )?;
                let email = crate::registry::email(&entry["email"])?;
                require(
                    intended.contains(&email) && dropped.insert(email),
                    "recipient_exclusions may only remove distinct preflighted recipients",
                )?;
            }
            require(
                dropped.is_empty() || intended.len() > dropped.len(),
                "recipient_exclusions cannot remove every recipient",
            )?;
            require(
                actual == intended.difference(&dropped).map(String::as_str).collect(),
                "First-contact packet To/Cc differ from the preflighted recipients; stage a revised request",
            )?;
            if inputs.get("to").is_some() && dropped.is_empty() {
                require(
                    python_eq(&p["to"], &inputs["to"]) && python_eq(&p["cc"], &inputs["cc"]),
                    "First-contact packet changed the grouped To/Cc order",
                )?;
            } else if !dropped.is_empty() {
                let order: Vec<_> = array(&inputs["recipient_emails"])
                    .iter()
                    .filter(|a| crate::registry::email(a).is_ok_and(|e| !dropped.contains(&e)))
                    .collect();
                let preferred = array(&inputs["to"])
                    .iter()
                    .find(|a| crate::registry::email(a).is_ok_and(|e| !dropped.contains(&e)))
                    .or_else(|| order.first().copied());
                let to: Vec<_> = preferred
                    .into_iter()
                    .map(crate::registry::email)
                    .collect::<Result<_>>()?;
                let cc: Vec<_> = order
                    .into_iter()
                    .filter(|a| preferred != Some(*a))
                    .map(crate::registry::email)
                    .collect::<Result<_>>()?;
                require(
                    array(&p["to"])
                        .iter()
                        .map(crate::registry::email)
                        .collect::<Result<Vec<_>>>()?
                        == to
                        && array(&p["cc"])
                            .iter()
                            .map(crate::registry::email)
                            .collect::<Result<Vec<_>>>()?
                            == cc,
                    "First-contact packet changed the grouped To/Cc order",
                )?;
            }
        }
    }
    Ok(())
}
fn unregistered(row: &Value, env: &Environment<'_>, message: &str) -> Result<()> {
    let inputs = effective_inputs(row);
    let sender = &inputs["mailbox_observation"]["email"];
    if truth(sender) {
        require(
            !crate::registry::people(&env.tables)?.iter().any(|p| {
                array(&p["emails"])
                    .iter()
                    .any(|a| s(a) == casefold(s(sender)))
            }),
            message,
        )?;
    }
    Ok(())
}
fn url(v: &Value) -> Result<()> {
    require(
        v.is_string()
            && !s(v).chars().any(crate::text::whitespace)
            && regex(r"^https?://[^\s/]+(?:/[^\s]*)?$")?.is_match(s(v)),
        "Author evidence needs an HTTP(S) URL",
    )
}
fn authors(result: &Value, request: &Value) -> Result<()> {
    let authors = &result["author_contacts"];
    require(
        authors.is_array(),
        "Short note needs author_contacts proposals",
    )?;
    if !truth(authors) {
        require(
            nonempty(&result["author_contacts_reason"]),
            "Empty author_contacts needs author_contacts_reason",
        )?;
    }
    if let Some(reason) = result.get("author_contacts_reason") {
        require(nonempty(reason), "Invalid author_contacts_reason")?;
    }
    let mut seen = BTreeSet::new();
    let mut seen_addresses = BTreeSet::new();
    for a in array(authors) {
        require(
            keys(
                a,
                &[
                    "full_name",
                    "title",
                    "title_evidence_url",
                    "affiliation",
                    "source_url",
                    "emails",
                    "problem_ids",
                    "in_refusals",
                    "refusals_checked_revision",
                ],
                &["email_omission_reason"],
            ),
            "Invalid author_contacts entry",
        )?;
        require(
            nonempty(&a["full_name"]) && nonempty(&a["affiliation"]),
            "Author needs full name and current affiliation/status",
        )?;
        require(
            ["Professor", "Dr.", "none"].contains(&s(&a["title"])),
            "Invalid verified academic title",
        )?;
        url(&a["title_evidence_url"])?;
        url(&a["source_url"])?;
        let p = &a["problem_ids"];
        require(
            p.is_array() && truth(p) && array(p).iter().all(nonempty),
            "Author links need nonempty problem_ids",
        )?;
        require(
            array(p).iter().map(s).collect::<BTreeSet<_>>().len() == array(p).len(),
            "Duplicate author problem ID",
        )?;
        require(
            a["in_refusals"].is_boolean(),
            "Author needs the pinned refusal check",
        )?;
        require(
            hex(&a["refusals_checked_revision"], 40),
            "Pin the refusal-table revision",
        )?;
        require(
            &a["refusals_checked_revision"]
                == fallback(&[&request["policy_revision"], &request["source_revision"]]),
            "Refusal check must use the pinned task revision",
        )?;
        let rid = &request["inputs"]["result"]["id"];
        if truth(rid) {
            require(
                array(p).contains(rid),
                "Author proposal must cover the task result ID",
            )?;
        }
        require(a["emails"].is_array(), "Author emails must be a list")?;
        if !truth(&a["emails"]) {
            require(
                nonempty(&a["email_omission_reason"]),
                "Unreachable author needs email_omission_reason",
            )?;
        }
        let mut addresses = BTreeSet::new();
        for e in array(&a["emails"]) {
            require(
                keys(e, &["email", "evidence_url"], &[]),
                "Every email needs its own source evidence",
            )?;
            let email = crate::registry::email(&e["email"])?;
            url(&e["evidence_url"])?;
            require(addresses.insert(email), "Duplicate author email")?;
        }
        require(
            seen_addresses.is_disjoint(&addresses),
            "An email cannot identify two author proposals",
        )?;
        seen_addresses.extend(addresses);
        require(
            seen.insert((
                casefold(s(&a["full_name"])),
                array(p).iter().map(s).collect::<BTreeSet<_>>(),
            )),
            "Duplicate author proposal",
        )?;
    }
    Ok(())
}
fn reply_signoff(body: &str, config: &Value) -> Result<()> {
    let lines: Vec<_> = crate::text::split_lines(body)
        .into_iter()
        .map(|l| l.trim_matches(crate::text::whitespace))
        .filter(|l| !l.is_empty())
        .collect();
    let re = regex(r"^(?:(best|kind|warm)( regards| wishes)?,?|regards,?|sincerely,?|cheers,?)$")?;
    let closing = lines.iter().rposition(|l| {
        let ascii_case = l
            .chars()
            .map(|c| match c {
                'İ' | 'ı' => 'i',
                'ſ' => 's',
                'K' => 'k',
                c => c,
            })
            .collect::<String>();
        re.is_match(&crate::text::lower(&ascii_case))
    });
    let tail = &lines[closing.map_or(lines.len().saturating_sub(3), |n| n + 1)..];
    require(
        !tail.iter().any(|l| {
            array(&config["identity"]["reply_affiliation_markers"])
                .iter()
                .any(|m| casefold(l).contains(s(m)))
        }),
        "Reply sign-off must end with the names only; remove the affiliation line",
    )
}
pub fn edited(row: &Value, body: &str, env: &Environment<'_>) -> Result<(Value, Value, Vec<u8>)> {
    let original = &row["data"]["result"];
    require(
        original["outcome"] == "completed"
            && original["packet"].is_object()
            && original["packet"].get("body").is_some(),
        "edited_body requires a completed result with a mail packet",
    )?;
    let body = body.replace("\r\n", "\n").replace('\r', "\n");
    let mut packet = original["packet"].clone();
    packet["body"] = string(&body);
    let raw = canonical(&packet)?.into_bytes();
    require(
        raw.len() <= 25 * 1024 * 1024,
        "Edited mail packet exceeds 25 MiB",
    )?;
    let chars: Vec<_> = body.chars().collect();
    require(
        !chars.iter().enumerate().any(|(i, c)| {
            *c == '—'
                || (*c == '–'
                    && (i == 0
                        || crate::text::whitespace(chars[i - 1])
                        || i + 1 == chars.len()
                        || crate::text::whitespace(chars[i + 1])))
        }),
        "Edited body must not use em or en dashes as sentence punctuation",
    )?;
    if row["kind"] == "firstcontact" {
        firstcontact_body(&body, env)?;
    }
    let sha = sha256(&raw);
    let artifact = object([
        (
            "release_tag",
            string(format!("outreach-task-{}", s(&row["id"]))),
        ),
        ("name", string(format!("reviewed-packet-{sha}.json"))),
        ("role", string("mail-packet")),
        ("bytes", raw.len().into()),
        ("sha256", string(sha)),
    ]);
    let mut artifacts: Vec<_> = array(&original["artifacts"])
        .iter()
        .filter(|a| a["role"] != "mail-packet")
        .cloned()
        .collect();
    artifacts.push(artifact.clone());
    let mut result = original.clone();
    result["packet"] = packet;
    result["artifacts"] = Value::Array(artifacts);
    validate(&result, row, env)?;
    Ok((result, artifact, raw))
}
fn firstcontact_body(body: &str, env: &Environment<'_>) -> Result<()> {
    let policy = &env.tables["firstcontact_workflow"];
    let body = body.trim_matches(crate::text::whitespace);
    let prose = body
        .split_once('\n')
        .map_or("", |(_, p)| p)
        .trim_start_matches(crate::text::whitespace);
    let opening = casefold(
        regex(r"\n[\s\x1c-\x1f]*\n")?
            .split(prose)
            .next()
            .unwrap_or(""),
    );
    let mut terms: Vec<_> = array(&env.config["identity"]["firstcontact_opening_terms"])
        .iter()
        .map(s)
        .collect();
    terms.extend([
        "https://github.com/the-omega-institute/trureturing",
        "https://www.trureturing.org/",
        "ai orchestration",
        "connect",
        "lean",
    ]);
    require(
        terms.iter().all(|t| opening.contains(t)),
        "First-contact opening is missing required content",
    )?;
    let compose = array(&policy["steps"])
        .iter()
        .find(|v| v["id"] == "compose");
    let ending = compose
        .into_iter()
        .flat_map(|v| array(&v["instructions"]))
        .map(s)
        .find(|s| s.starts_with("Ending:"))
        .unwrap_or("");
    let captures =
        regex(r"Closing paragraph 1: (.+) Closing paragraph 2: (.+) No work-plan paragraph")?
            .captures(ending)
            .ok_or_else(|| {
                Error::new("First-contact closing requirements are missing from the workflow")
            })?;
    let url = format!(
        "{}/",
        s(&env.config["mathesis"]["base_url"]).trim_end_matches('/')
    );
    let closing = [
        s(&policy["closing_suffix_exact"]).into(),
        captures[1].replace("[configured Mathesis URL]", &url),
        captures[2].replace("[configured Mathesis URL]", &url),
        s(&policy["signature_exact"]).replace(
            "[configured firstcontact_signature]",
            s(&env.config["identity"]["firstcontact_signature"]),
        ),
    ]
    .join("\n\n");
    require(
        body.ends_with(&closing),
        "First-contact closing must use the required invitation, paragraphs and signature",
    )
}
