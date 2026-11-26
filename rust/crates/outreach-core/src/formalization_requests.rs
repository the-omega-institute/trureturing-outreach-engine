//! Reviewed scientific issue filing. Inventories are read to completion before a
//! snapshot-bound, single-use POST capability is issued. No production adapter.
use crate::mathesis_delivery::{DeliveryPorts, Persisted, StageResult};
use crate::{array, ingest::Replay, object, s, string, transitions, truth, validation};
use outreach_canon::{Value, digest};

pub const ISSUES: &str = "repos/the-omega-institute/trureturing-requests/issues";
pub const LANE: &str = "formalization-requests";
pub struct IssueIntent {
    pub payload: Value,
}
/// A partial inventory cannot authorize creation; an uncertain creation must be
/// reconciled by a fresh inventory and the original hidden marker.
/// ```compile_fail
/// use outreach_core::formalization_requests::{GitHubIssues, IssueIntent};
/// fn bypass(client: &mut impl GitHubIssues, intent: IssueIntent) { client.create(intent); }
/// ```
pub trait GitHubIssues {
    fn available(&self) -> bool;
    fn page(&mut self, page: usize) -> StageResult<Value>;
    fn create(&mut self, intent: Persisted<IssueIntent>) -> StageResult<Value>;
}
pub(crate) fn check(ok: bool, message: &str) -> StageResult<()> {
    Ok(validation::require(ok, message)?)
}
pub(crate) fn compact(text: &str) -> String {
    text.split(crate::text::whitespace)
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}
pub(crate) fn take(text: &str, n: usize) -> String {
    text.chars().take(n).collect()
}
pub(crate) fn remove(v: &mut Value, key: &str) {
    if let Some(o) = v.as_object_mut() {
        o.shift_remove(key);
    }
}
pub(crate) fn due(r: &Replay<'_>, lane: &str, id: &str, binding: &Value) -> StageResult<bool> {
    let saved = r.state(&format!("recheck:{lane}:{id}"));
    Ok(saved["binding"] != digest(binding)? || r.now >= saved["next_at"].as_f64().unwrap_or(0.))
}
pub(crate) fn defer(
    r: &mut Replay<'_>,
    lane: &str,
    id: &str,
    binding: &Value,
    seconds: i64,
) -> StageResult<()> {
    let next = if let Some(now) = r.clock_value.as_i64() {
        (now + seconds).into()
    } else {
        outreach_canon::float(r.now + seconds as f64)?
    };
    r.set_state(
        &format!("recheck:{lane}:{id}"),
        object([
            ("binding", string(digest(binding)?)),
            ("checked_at", r.clock_value.clone()),
            ("next_at", next),
        ]),
    )?;
    Ok(())
}
pub fn payload(item: &Value) -> Value {
    let gap = &item["gap"];
    let mut statement = compact(s(&gap["statement"]));
    let doi = validation::coordinate(&gap["source_doi"], true);
    let mut tag = if doi.is_empty() {
        "No DOI".into()
    } else {
        format!("DOI:{doi}")
    };
    let mut locator = take(&compact(s(&gap["source_locator"])), 100);
    let mut available = 256_usize.saturating_sub(
        format!("[Formalization]  | {tag} | {locator}")
            .chars()
            .count(),
    );
    if available < 25 {
        locator = take(&locator, 30);
        tag = take(&tag, 140);
        available = 256
            - format!("[Formalization]  | {tag} | {locator}")
                .chars()
                .count();
    }
    if statement.chars().count() > available {
        statement = take(&statement, available.saturating_sub(1)) + "…";
    }
    let mut sources = vec![if doi.is_empty() {
        "- No DOI".into()
    } else {
        format!("- DOI: https://doi.org/{doi}")
    }];
    if truth(&gap["source_arxiv"]) {
        sources.push(format!(
            "- arXiv: https://arxiv.org/abs/{}",
            validation::coordinate(&gap["source_arxiv"], false)
        ));
    }
    if truth(&gap["source_url"]) {
        sources.push(format!("- Public source: {}", s(&gap["source_url"])));
    }
    sources.extend([
        format!("- Source location: {}", compact(s(&gap["source_locator"]))),
        format!("- Note location: {}", compact(s(&gap["note_locator"]))),
    ]);
    object([
        (
            "title",
            string(format!("[Formalization] {statement} | {tag} | {locator}")),
        ),
        (
            "body",
            string(format!(
                "## Exact mathematical statement\n\n{}\n\n## Source and exact location\n\n{}\n\n## Why formalize it\n\n{}\n\n<!-- outreach-formalization:{} -->",
                s(&gap["statement"]),
                sources.join("\n"),
                s(&gap["why_it_matters"]),
                s(&item["key"])
            )),
        ),
    ])
}
fn title(text: &str) -> Option<(String, String)> {
    let pattern =
        crate::regex(r"(?i)^\[Formalization\]\s*(.*?)\s*\|\s*(DOI:\s*[^|]+|No DOI)\s*\|").ok()?;
    let captures = pattern.captures(text)?;
    Some((compact(&captures[1]), captures[2].into()))
}
pub fn same_issue(issue: &Value, item: &Value) -> StageResult<bool> {
    let Some((statement, tag)) = title(s(&issue["title"])) else {
        return Ok(false);
    };
    let body = s(&issue["body"]);
    if body.contains(&format!(
        "<!-- outreach-formalization:{} -->",
        s(&item["key"])
    )) {
        return Ok(true);
    }
    let gap = &item["gap"];
    let exact = compact(s(&gap["statement"]));
    let paragraphs = crate::regex(r"\n\s*\n")?;
    if exact != statement && !paragraphs.split(body).any(|p| compact(p) == exact) {
        return Ok(false);
    }
    for (kind, value) in [
        ("doi", validation::coordinate(&gap["source_doi"], true)),
        ("arxiv", validation::coordinate(&gap["source_arxiv"], false)),
        (
            "url",
            s(&gap["source_url"])
                .split('#')
                .next()
                .unwrap_or("")
                .trim_end_matches('/')
                .into(),
        ),
    ] {
        if value.is_empty() {
            continue;
        }
        if kind == "doi" && validation::coordinate(&string(&tag), true) == value {
            return Ok(true);
        }
        let (prefix, forbidden, fold) = match kind {
            "doi" => (r"(?:doi:\s*|doi\.org/)", r"[\w./-]", true),
            "arxiv" => (r"(?:arxiv:\s*|arxiv\.org/(?:abs|pdf)/)", r"[\w/-]", true),
            _ => ("", r"[\w/.-]", false),
        };
        let pattern = format!(
            "{}{}{}",
            if fold { "(?i)" } else { "" },
            prefix,
            regex::escape(&value)
        );
        let forbidden = crate::regex(&format!("^{forbidden}"))?;
        if crate::regex(&pattern)?
            .find_iter(body)
            .any(|m| !forbidden.is_match(&body[m.end()..]))
        {
            return Ok(true);
        }
    }
    Ok(false)
}
pub(crate) fn inventory(
    client: &mut impl GitHubIssues,
    ports: &mut impl DeliveryPorts,
    lane: &str,
    formal: bool,
) -> StageResult<Option<Vec<Value>>> {
    let mut issues = vec![];
    for page in 1..1000 {
        if !ports.allow(lane, "inventory") {
            return Ok(None);
        }
        let batch = client.page(page)?;
        check(
            batch.is_array()
                && array(&batch)
                    .iter()
                    .all(|i| i.is_object() && i.get("title").is_some()),
            "Invalid GitHub issue inventory",
        )?;
        issues.extend(
            array(&batch)
                .iter()
                .filter(|i| {
                    i.get("pull_request").is_none() && (!formal || title(s(&i["title"])).is_some())
                })
                .cloned(),
        );
        if array(&batch).len() < 100 {
            return Ok(Some(issues));
        }
    }
    Err(outreach_canon::Error::new("GitHub issue inventory exceeds the scan limit").into())
}
pub(crate) fn receipt(receipt: &Value) -> StageResult<String> {
    let url = s(&receipt["html_url"]);
    check(
        crate::regex(
            r"^https://github\.com/the-omega-institute/trureturing-requests/issues/[1-9]\d*$",
        )?
        .is_match(url),
        "Missing GitHub issue receipt URL",
    )?;
    Ok(url.into())
}
pub(crate) fn catchable(e: &crate::mathesis_delivery::StageError) -> bool {
    [
        "OSError",
        "TimeoutError",
        "ConnectionError",
        "HTTPError",
        "ValueError",
        "GateError",
        "UnparseableEvidence",
        "FileNotFoundError",
        "PermissionError",
    ]
    .contains(&s(&e.summary["type"]))
}
pub(crate) fn error_name(
    e: &crate::mathesis_delivery::StageError,
    connector: Option<&str>,
) -> String {
    if let Some(status) = e.status() {
        format!("GitHub HTTP {status}")
    } else {
        connector.unwrap_or(s(&e.summary["type"])).into()
    }
}
pub fn advance(
    r: &mut Replay<'_>,
    client: &mut impl GitHubIssues,
    ports: &mut impl DeliveryPorts,
) -> StageResult<()> {
    if truth(&r.environment.config["paused"]) {
        return Ok(());
    }
    let mut candidates = vec![];
    for row in r.rows("tasks") {
        if transitions::passed(row)? {
            for item in array(&row["data"]["formalization_requests"]) {
                if item["state"] == "pending" && due(r, LANE, s(&item["key"]), &item["gap"])? {
                    candidates.push((row.clone(), item.clone()));
                }
            }
        }
    }
    let mut issues = None;
    let mut connector = if client.available() {
        None
    } else {
        Some("Coordinator GitHub connection unavailable")
    };
    candidates.truncate(8);
    let previous = ports.previous(LANE);
    candidates.sort_by_key(|(row, item)| {
        previous
            .iter()
            .position(|key| *key == format!("{}-{}", s(&row["id"]), s(&item["key"])))
            .unwrap_or(previous.len())
    });
    for (original, item) in candidates {
        if !ports.allow(LANE, &format!("{}-{}", s(&original["id"]), s(&item["key"]))) {
            break;
        }
        let row = r.task(&original["id"])?;
        let mut saved = item.clone();
        let outcome = (|| -> StageResult<Option<String>> {
            check(connector.is_none(), connector.unwrap_or(""))?;
            if issues.is_none() {
                match inventory(client, ports, LANE, true) {
                    Ok(None) => return Ok(None),
                    Ok(v) => issues = v,
                    Err(e) => {
                        if catchable(&e) {
                            connector = Some("GitHub issue inventory unavailable");
                        }
                        return Err(e);
                    }
                }
            }
            let mut existing = None;
            for issue in issues.as_ref().into_iter().flatten() {
                if same_issue(issue, &item)? {
                    existing = Some(issue.clone());
                    break;
                }
            }
            if existing.is_none() {
                defer(r, LANE, s(&item["key"]), &item["gap"], 1800)?;
                ports.save_forced(&r.tables)?;
                let payload = payload(&item);
                let permit = Persisted::saved(
                    IssueIntent {
                        payload: payload.clone(),
                    },
                    ports,
                    &r.tables,
                )?;
                let mut value = client.create(permit)?;
                check(value.is_object(), "Invalid GitHub issue receipt")?;
                existing = Some(value.clone());
                for (k, v) in payload.as_object().into_iter().flatten() {
                    value[k] = v.clone();
                }
                if let Some(issues) = issues.as_mut() {
                    issues.push(value);
                }
            }
            Ok(Some(receipt(&existing.unwrap_or(Value::Null))?))
        })();
        match outcome {
            Ok(None) => return Ok(()),
            Ok(Some(url)) => {
                saved["state"] = string("filed");
                saved["issue_url"] = string(url);
                remove(&mut saved, "last_error");
            }
            Err(e) if catchable(&e) => {
                saved["last_error"] = string(error_name(&e, connector));
                defer(r, LANE, s(&item["key"]), &item["gap"], 1800)?;
                issues = None;
                connector = connector.or(Some("GitHub filing unavailable this tick"));
            }
            Err(e) => return Err(e),
        }
        if saved != item {
            let mut data = row["data"].clone();
            data["formalization_requests"] = Value::Array(
                array(&data["formalization_requests"])
                    .iter()
                    .map(|v| {
                        if v["key"] == item["key"] {
                            saved.clone()
                        } else {
                            v.clone()
                        }
                    })
                    .collect(),
            );
            r.update(&row, s(&row["state"]), data)?;
            ports.save(&r.tables)?;
        }
    }
    Ok(())
}
