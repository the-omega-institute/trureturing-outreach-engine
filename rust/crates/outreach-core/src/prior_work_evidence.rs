//! Scientific-only projection of reviewed evidence, with date precision retained.
use crate::formalization_requests::{check, compact};
use crate::mathesis_delivery::{StageError, StageResult};
use crate::{array, fallback, object, s, string, truth};
use outreach_canon::Value;
const CONTEXT: [&str; 6] = [
    "result_id",
    "statement",
    "posing_locator",
    "posing_url",
    "resolution_date",
    "lean_url",
];
fn matches(pattern: &str, text: &str) -> bool {
    crate::regex(pattern).is_ok_and(|r| r.is_match(text))
}
pub fn line(v: &Value) -> String {
    let text = s(v);
    let text = crate::regex(r"(?s)<!--.*?-->")
        .expect("regex")
        .replace_all(text, "");
    compact(
        &crate::regex(r"<?\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b>?")
            .expect("regex")
            .replace_all(&text, ""),
    )
}
pub fn private(text: &str) -> bool {
    matches(
        r"(?i)\b(?:private|confidential)(?:ly)?\b|\be-?mail(?:s|ed)?\b|\bpers(?:onal|\.)\s*(?:communication|comm\b|correspondence)|\b(?:our|their|his|her) correspondence\b|\bcorrespondence (?:with|from) (?:the |its |his |her |their )?(?:posing |original )?(?:authors?|posers?|prof(?:essor)?|dr)\b",
        text,
    ) || crate::regex(r"(?i)\b(?:in|by|via|through) correspondence\b").is_ok_and(|r| {
        r.find_iter(text)
            .any(|m| !text[m.end()..].to_lowercase().starts_with(" with"))
    })
}
fn unverified(text: &str) -> bool {
    matches(
        r"(?i)\b(?:unverified|unread|unchecked|not (?:checked|verified)|partial(?: overlap)?)\b",
        text,
    )
}
pub fn public_url(v: &Value, data_repo: &str) -> String {
    let mut value = s(v)
        .trim()
        .trim_matches(['<', '>'])
        .trim_matches('`')
        .to_owned();
    if let Ok(r) = crate::regex(r"^\[[^\]]+\]\((https?://\S+)\)$")
        && let Some(c) = r.captures(&value)
    {
        value = c[1].into();
    }
    if value
        .chars()
        .any(|c| c.is_whitespace() || "<>\"`".contains(c))
        || matches(r"\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b", &value)
    {
        return String::new();
    }
    let Some((scheme, rest)) = value.split_once("://") else {
        return String::new();
    };
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .to_lowercase();
    if !["http", "https"].contains(&scheme)
        || !host.contains('.')
        || host.contains('@')
        || [
            "chatgpt.com",
            "outlook.office.com",
            "outlook.office365.com",
            "mail.google.com",
        ]
        .contains(&host.as_str())
        || host.ends_with(".slack.com")
        || (host == "github.com"
            && rest
                .to_lowercase()
                .starts_with(&format!("github.com/{}/", data_repo.to_lowercase())))
    {
        String::new()
    } else {
        value
    }
}
pub fn date_bounds(v: &Value) -> Option<(i64, i64)> {
    let raw = line(v);
    let raw = raw.trim();
    let day = |s: &str| crate::validation::iso_day(s);
    if matches(r"^\d{4}$", raw) {
        return Some((day(&format!("{raw}-01-01"))?, day(&format!("{raw}-12-31"))?));
    }
    let (y, m, d) = if matches(r"^\d{4}-\d{2}(?:-\d{2}(?:T.*)?)?$", raw) {
        let year = raw[..4].parse::<i64>().ok()?;
        let month = raw[5..7].parse::<usize>().ok()?;
        let d = if raw.len() >= 10 {
            Some(raw[8..10].parse::<u32>().ok()?)
        } else {
            None
        };
        if raw.len() > 10 && crate::gates::instant_micros(&string(raw)).is_none() {
            return None;
        }
        (year, month, d)
    } else {
        let clean = raw.replace([',', '.'], "");
        let parts: Vec<_> = clean.split_whitespace().collect();
        let names = [
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        ];
        let find = |text: &str| {
            let text = text.to_lowercase();
            names
                .iter()
                .position(|n| *n == text || n[..3] == text || (*n == "september" && text == "sept"))
                .map(|i| i + 1)
        };
        match parts.as_slice() {
            [month, year] => (year.parse().ok()?, find(month)?, None),
            [a, b, year] => {
                let (m, d) = if let Some(m) = find(a) {
                    (m, b.parse().ok()?)
                } else {
                    (find(b)?, a.parse().ok()?)
                };
                (year.parse().ok()?, m, Some(d))
            }
            _ => return None,
        }
    };
    let first = day(&format!("{y:04}-{m:02}-{:02}", d.unwrap_or(1)))?;
    if d.is_some() {
        return Some((first, first));
    }
    let end = (28..=31)
        .rev()
        .find_map(|d| day(&format!("{y:04}-{m:02}-{d:02}")))?;
    Some((first, end))
}
pub fn project(check_value: &Value, data_repo: &str) -> StageResult<Value> {
    check(
        check_value.is_object()
            && ["first-known", "prior-exists", "unclear"].contains(&s(&check_value["verdict"])),
        "Invalid prior_work_check verdict",
    )?;
    check(
        check_value["items"].is_array()
            && array(&check_value["items"]).iter().all(Value::is_object),
        "prior_work_check needs a list of evidence items",
    )?;
    let mut projected = object(CONTEXT.map(|key| (key, string(line(&check_value[key])))));
    projected["verdict"] = check_value["verdict"].clone();
    let mut items = vec![];
    for item in array(&check_value["items"]) {
        let status = line(&item["status"]);
        let authors = if item["authors"].is_array() {
            array(&item["authors"])
                .iter()
                .map(line)
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            line(&item["authors"])
        };
        let authors = line(&string(authors));
        let locator = line(&item["locator"]);
        let proves = line(fallback(&[&item["proves"], &item["what_it_proves"]]));
        if (item
            .get("private")
            .is_some_and(|v| !v.is_null() && !outreach_canon::python_eq(v, &Value::Bool(false))))
            || [
                &authors,
                &locator,
                &proves,
                &status,
                &line(&item["kind"]),
                &line(&item["visibility"]),
                &line(&item["source"]),
            ]
            .into_iter()
            .any(|v| private(v))
        {
            items.push(object([("private", true.into())]));
            continue;
        }
        if item["verified"] == false || item["settles"] == false || unverified(&status) {
            continue;
        }
        let url = public_url(&item["url"], data_repo);
        if url.is_empty()
            || authors.is_empty()
            || proves.is_empty()
            || unverified(&proves)
            || matches(
                r"(?i)\b(?:does not|did not|doesn.t) (?:settle|prove|refute|answer|resolve)\b",
                &proves,
            )
        {
            continue;
        }
        let date = fallback(&[&item["date"], &item["year"]]);
        let dated = if date.is_number() {
            date.to_string()
        } else {
            s(date).to_owned()
        };
        let dated = dated.trim();
        let dated = if ["", "date unknown", "unknown", "undated", "n.d.", "—", "-"]
            .contains(&dated.to_lowercase().as_str())
        {
            String::new()
        } else if date_bounds(&string(dated)).is_some() {
            dated.into()
        } else {
            "unrecognized date".into()
        };
        items.push(object([
            ("url", string(url)),
            ("authors", string(authors)),
            ("date", string(dated)),
            ("locator", string(locator)),
            ("proves", string(proves)),
        ]));
    }
    projected["items"] = Value::Array(items);
    Ok(projected)
}
fn label(raw: &str) -> Option<&'static str> {
    let label = raw
        .trim_matches([' ', '*', '_', '`'])
        .to_lowercase()
        .replace('_', " ");
    Some(match label.as_str() {
        "author" | "authors" => "authors",
        "year" => "year",
        "date" | "year/date" | "release date" => "date",
        "locator" => "locator",
        "url" | "public url" => "url",
        "what it proves" | "what exactly it proves" | "proves" => "proves",
        "kind" => "kind",
        "visibility" => "visibility",
        "status" => "status",
        "source" => "source",
        "result id" => "result_id",
        "statement" | "posed statement" => "statement",
        "posing locator" => "posing_locator",
        "posing url" => "posing_url",
        "resolution date" | "upstream resolution date" | "trureturing resolution date" => {
            "resolution_date"
        }
        "lean url" | "lean link" => "lean_url",
        _ => return None,
    })
}
pub fn parse_text(text: &str, data_repo: &str) -> StageResult<Value> {
    let heading = crate::regex(r"(?im)^(#{1,6}) +Prior[- ]work check[^\n]*\n")?;
    let Some(c) = heading.captures(text) else {
        return Ok(Value::Null);
    };
    let rest = &text[c.get(0).map_or(0, |m| m.end())..];
    let end = crate::regex(&format!(r"(?m)^#{{1,{}}} +", c[1].len()))?
        .find(rest)
        .map_or(rest.len(), |m| m.start());
    let section = &rest[..end];
    let Some(c) =
        crate::regex(r"(?i)\bverdict[*`_ ]*[:=]\s*[*`_ ]*(first-known|prior-exists|unclear)\b")?
            .captures(section)
    else {
        return Ok(Value::Null);
    };
    let mut check_value = object([
        ("verdict", string(c[1].to_lowercase())),
        ("items", Value::Array(vec![])),
    ]);
    if check_value["verdict"] != "prior-exists" {
        return project(&check_value, data_repo);
    }
    let mut item = Value::Null;
    let mut items = vec![];
    let mut columns = vec![];
    for raw in section.lines() {
        let raw = raw.trim();
        if raw.starts_with('|') {
            let cells: Vec<_> = raw.trim_matches('|').split('|').map(str::trim).collect();
            let fields: Vec<_> = cells.iter().map(|v| label(v)).collect();
            if fields.contains(&Some("url")) && fields.contains(&Some("authors")) {
                if truth(&item) {
                    items.push(item);
                }
                item = Value::Null;
                columns = fields;
            } else if cells.len() == columns.len() && !cells.iter().all(|c| matches(r"^[: -]+$", c))
            {
                let mut value = object([]);
                for (key, cell) in columns.iter().zip(cells) {
                    if let Some(key) = key {
                        value[*key] = string(cell);
                    }
                }
                items.push(value);
            }
            continue;
        }
        if matches(
            r"(?i)^(?:#+ +|[-*] +|\d+[.)] +)(?:\*\*)?(?:item\s*\d*\b|verified item|prior (?:item|work)|private (?:report|correspondence)|reported by)",
            raw,
        ) || matches(r"^#{1,6} +", raw)
        {
            if truth(&item) {
                items.push(item);
            }
            item = object([]);
            if private(raw) {
                item["private"] = true.into();
            }
            continue;
        }
        let stripped = crate::regex(r"^(?:[-*] |\d+[.)] )")?.replace(raw, "");
        if let Some(c) = crate::regex(r"^([\w */_`-]+):[ *_`]*(.*)$")?.captures(&stripped)
            && let Some(key) = label(&c[1])
        {
            let value = string(c[2].trim_matches([' ', '*']));
            if CONTEXT.contains(&key) {
                check_value[key] = value;
            } else {
                if item.is_null() || item.get(key).is_some() {
                    return Err(StageError {
                        summary: object([
                            ("type", string("UnparseableEvidence")),
                            (
                                "reason",
                                string("Prior-work evidence requires one heading per entry"),
                            ),
                        ]),
                    });
                }
                if key == "kind" && private(s(&value)) && truth(&item) && !truth(&item["private"]) {
                    return Err(StageError {
                        summary: object([
                            ("type", string("UnparseableEvidence")),
                            (
                                "reason",
                                string("Prior-work evidence requires one heading per entry"),
                            ),
                        ]),
                    });
                }
                item[key] = value;
            }
        }
    }
    if truth(&item) {
        items.push(item);
    }
    check_value["items"] = Value::Array(items);
    project(&check_value, data_repo)
}
pub fn parse_file(raw: &[u8], artifact: &Value, data_repo: &str) -> StageResult<Value> {
    check(
        artifact["bytes"].as_u64() == Some(raw.len() as u64)
            && artifact["sha256"] == outreach_canon::sha256(raw),
        "Prior-work evidence differs from the exact PASS",
    )?;
    let text = std::str::from_utf8(raw).map_err(|_| StageError {
        summary: object([
            ("type", string("UnparseableEvidence")),
            ("reason", string("Prior-work evidence is not valid UTF-8")),
        ]),
    })?;
    parse_text(text, data_repo)
}
