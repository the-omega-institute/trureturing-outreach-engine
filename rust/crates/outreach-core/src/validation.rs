//! Admission gates, in the order used by the live Python writer.
use crate::{array, object, s, string, text::whitespace, *};
use outreach_canon::{canonical, python_eq};
use std::collections::BTreeSet;
use unicode_normalization::UnicodeNormalization;

pub fn require(ok: bool, message: impl Into<String>) -> Result<()> {
    if ok { Ok(()) } else { Err(Error::new(message)) }
}
pub fn increment(n: i64) -> Result<i64> {
    n.checked_add(1)
        .ok_or_else(|| Error::new("Ledger integer out of range"))
}
pub fn nonempty(v: &Value) -> bool {
    v.as_str()
        .is_some_and(|v| !v.trim_matches(whitespace).is_empty())
}
pub fn hex(v: &Value, n: usize) -> bool {
    v.as_str().is_some_and(|s| {
        s.len() == n
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}
pub fn keys(v: &Value, required: &[&str], optional: &[&str]) -> bool {
    v.as_object().is_some_and(|o| {
        required.iter().all(|k| o.contains_key(*k))
            && o.keys()
                .all(|k| required.contains(&k.as_str()) || optional.contains(&k.as_str()))
    })
}
pub fn one(v: &Value) -> bool {
    python_eq(v, &1.into())
}
pub fn integer(v: &Value) -> bool {
    v.as_number()
        .is_some_and(|n| !n.as_str().contains(['.', 'e', 'E']))
}
pub fn hashable(v: &Value) -> Result<()> {
    match v {
        Value::Array(_) => Err(Error::new("unhashable type: 'list'")),
        Value::Object(_) => Err(Error::new("unhashable type: 'dict'")),
        _ => Ok(()),
    }
}
pub fn lookup<'a>(v: &'a Value, k: &str) -> Result<&'a Value> {
    match v {
        Value::Object(o) => o.get(k).ok_or_else(|| Error::new(format!("'{k}'"))),
        Value::Array(_) => Err(Error::new(
            "list indices must be integers or slices, not str",
        )),
        Value::String(_) => Err(Error::new("string indices must be integers, not 'str'")),
        v => {
            let name = match v {
                Value::Null => "NoneType",
                Value::Bool(_) => "bool",
                Value::Number(_) if integer(v) => "int",
                _ => "float",
            };
            Err(Error::new(format!("'{name}' object is not subscriptable")))
        }
    }
}
pub fn signature_type(v: &Value, guard: bool, message: &str) -> Result<()> {
    match &v["signature"] {
        Value::String(s) => require(
            s.is_ascii(),
            "comparing strings with non-ASCII characters is not supported",
        ),
        value if guard => {
            let _ = value;
            Err(Error::new(message))
        }
        value => {
            let name = match value {
                Value::Null => "NoneType",
                Value::Bool(_) => "bool",
                Value::Number(_) if integer(value) => "int",
                Value::Number(_) => "float",
                Value::Array(_) => "list",
                Value::Object(_) => "dict",
                Value::String(_) => "str",
            };
            Err(Error::new(format!(
                "unsupported operand types(s) or combination of types: 'str' and '{name}'"
            )))
        }
    }
}
pub fn sources(v: &Value) -> Result<()> {
    require(v.is_array(), "note_sources must be a list")?;
    for link in array(v) {
        require(
            keys(link, &["task_id", "result_hash"], &[])
                && nonempty(&link["task_id"])
                && hex(&link["result_hash"], 64),
            "Each note source needs a task ID and exact result hash",
        )?;
    }
    Ok(())
}
pub fn request(v: &Value) -> Result<()> {
    require(
        v.is_object() && one(&v["schema_version"]),
        "Unsupported task request",
    )?;
    require(
        keys(
            v,
            &[
                "schema_version",
                "id",
                "kind",
                "owner",
                "source_revision",
                "inputs",
                "workflow_hash",
            ],
            &["policy_revision"],
        ),
        "Unexpected task request fields",
    )?;
    hashable(&v["kind"])?;
    require(
        outreach_canon::KINDS.contains(&s(&v["kind"])),
        "Unknown task kind",
    )?;
    for k in ["id", "owner", "source_revision", "workflow_hash"] {
        require(nonempty(&v[k]), format!("Missing {k}"))?;
    }
    require(
        s(&v["id"]).chars().count() <= 120
            && s(&v["id"])
                .chars()
                .all(|c| crate::text::alphanumeric(c) || "-_.".contains(c)),
        "Unsafe task ID",
    )?;
    let i = &v["inputs"];
    require(i.is_object(), "Task inputs must be an object")?;
    if let Some(sup) = i.get("supersedes_owner_questions") {
        require(
            v["kind"] == "reply"
                && sup.as_object().is_some_and(|o| {
                    !o.is_empty() && o.iter().all(|(k, v)| nonempty(&string(k)) && nonempty(v))
                }),
            "Question supersession requires reply task IDs and operator reasons",
        )?;
    }
    if let Some(origin) = i.get("reply_origin_task_id") {
        require(
            ["shortnote", "mathesis"].contains(&s(&v["kind"])) && nonempty(origin),
            "Reply origin needs a reply task ID on shortnote or Mathesis work",
        )?;
    }
    let owners = i.get("owner_ids").cloned().unwrap_or(Value::Array(vec![]));
    require(
        owners.is_array() && array(&owners).iter().all(nonempty),
        "Invalid additional scholar ownership",
    )?;
    require(
        hex(&v["source_revision"], 40),
        "Pin the workflow source commit",
    )?;
    if let Some(rev) = v.get("policy_revision") {
        require(hex(rev, 40), "Pin the policy commit")?;
    }
    require(hex(&v["workflow_hash"], 64), "Missing workflow digest")?;
    if v["kind"] == "shortnote" {
        hashable(&i["note_round"])?;
        require(
            i["note_round"].is_null() || ["draft", "uplift"].contains(&s(&i["note_round"])),
            "Unknown shortnote round",
        )?;
        let n = i.get("note_value_uplifts").cloned().unwrap_or(0.into());
        require(
            integer(&n) && !s_number(&n).starts_with('-'),
            "Invalid short-note uplift count",
        )?;
    }
    if let Some(links) = i.get("note_sources") {
        sources(links)?;
    }
    canonical(v)?;
    Ok(())
}
fn s_number(v: &Value) -> String {
    v.as_number().map(ToString::to_string).unwrap_or_default()
}
fn strings(v: &Value, label: &str) -> Result<()> {
    require(
        v.is_array() && array(v).iter().all(nonempty),
        format!("Invalid {label}"),
    )?;
    require(
        array(v).iter().map(s).collect::<BTreeSet<_>>().len() == array(v).len(),
        format!("Duplicate {label}"),
    )
}
fn media(v: &Value) -> Result<()> {
    require(v.is_array(), "Media must be a list")?;
    let mut names = BTreeSet::new();
    for m in array(v) {
        require(
            keys(m, &["name", "media_type", "content_b64"], &[]),
            "Media must include exact name, type and bytes",
        )?;
        let name = s(&m["name"]);
        require(
            m["name"].is_string()
                && !["", ".", ".."].contains(&name)
                && !name.contains(['/', '\\', '\r', '\n', '\0']),
            "Invalid media name",
        )?;
        require(
            m["media_type"].is_string() && s(&m["media_type"]).contains('/'),
            "Invalid MIME type",
        )?;
        require(canonical_base64(&m["content_b64"]), "Invalid media bytes")?;
        names.insert(name);
    }
    require(names.len() == array(v).len(), "Duplicate media name")
}
fn canonical_base64(v: &Value) -> bool {
    let Some(s) = v.as_str() else { return false };
    let b = s.as_bytes();
    let padding = b.iter().rev().take_while(|c| **c == b'=').count();
    let val = |c: u8| {
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
            .iter()
            .position(|x| *x == c)
    };
    if !b.len().is_multiple_of(4)
        || padding > 2
        || b[..b.len() - padding].iter().any(|c| val(*c).is_none())
    {
        return false;
    }
    match padding {
        1 => val(b[b.len() - 2]).is_some_and(|v| v % 4 == 0),
        2 => val(b[b.len() - 3]).is_some_and(|v| v % 16 == 0),
        _ => true,
    }
}
fn mailbox(v: &Value) -> bool {
    v.as_str().is_some_and(|s| {
        crate::text::lower(s) == s
            && !s.chars().any(whitespace)
            && regex(r"^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$").is_ok_and(|r| r.is_match(s))
    })
}
pub fn packet(v: &Value) -> Result<()> {
    require(
        v.is_object() && integer(&v["schema_version"]) && one(&v["schema_version"]),
        "Unsupported packet schema",
    )?;
    hashable(&v["kind"])?;
    match s(&v["kind"]) {
        "reply" | "firstcontact" => {
            let mut f = vec![
                "schema_version",
                "kind",
                "sender",
                "to",
                "cc",
                "subject",
                "body",
                "problem_ids",
                "attachments",
            ];
            if v["kind"] == "reply" {
                f.extend([
                    "thread_id",
                    "latest_inbound_message_id",
                    "user_decision_ever",
                ]);
            }
            require(keys(v, &f, &[]), "Unknown/missing letter fields")?;
            for field in ["to", "cc", "problem_ids"] {
                strings(&v[field], field)?;
            }
            require(
                truth(&v["to"]) && (truth(&v["problem_ids"]) || v["kind"] == "reply"),
                "Missing recipient or problem",
            )?;
            if v["kind"] == "reply" {
                require(
                    nonempty(&v["thread_id"])
                        && nonempty(&v["latest_inbound_message_id"])
                        && v["user_decision_ever"].is_boolean(),
                    "Reply requires the exact latest inbound identity and user-decision history",
                )?;
            }
            require(
                std::iter::once(&v["sender"])
                    .chain(array(&v["to"]))
                    .chain(array(&v["cc"]))
                    .all(mailbox),
                "Use explicit normalized mailbox addresses",
            )?;
            let addresses: Vec<_> = array(&v["to"])
                .iter()
                .chain(array(&v["cc"]))
                .map(s)
                .collect();
            require(
                addresses.iter().collect::<BTreeSet<_>>().len() == addresses.len(),
                "To/Cc must not overlap",
            )?;
            require(
                nonempty(&v["subject"]) && !s(&v["subject"]).contains(['\r', '\n', '\0']),
                "Invalid subject",
            )?;
            require(nonempty(&v["body"]), "Missing body")?;
            media(&v["attachments"])?;
        }
        "daily" => {
            require(
                keys(
                    v,
                    &[
                        "schema_version",
                        "kind",
                        "account",
                        "covered_days",
                        "text",
                        "media",
                        "evidence_fingerprint",
                    ],
                    &[],
                ),
                "Unknown/missing daily fields",
            )?;
            require(nonempty(&v["account"]), "Missing account")?;
            strings(&v["covered_days"], "covered_days")?;
            require(truth(&v["covered_days"]), "Missing daily coverage")?;
            let mut days = Vec::new();
            for d in array(&v["covered_days"]) {
                days.push(iso_day(s(d)).ok_or_else(|| Error::new("Invalid covered date"))?);
            }
            require(
                array(&v["covered_days"]).iter().all(|d| {
                    s(d).len() == 10 && s(d).as_bytes()[4] == b'-' && s(d).as_bytes()[7] == b'-'
                }) && days.windows(2).all(|p| p[1] == p[0] + 1),
                "Daily dates must be ordered contiguous ISO dates",
            )?;
            require(nonempty(&v["text"]), "Missing daily text")?;
            require(
                hex(&v["evidence_fingerprint"], 64),
                "Invalid evidence fingerprint",
            )?;
            media(&v["media"])?;
        }
        _ => return Err(Error::new("Unknown workflow kind")),
    }
    canonical(v)?;
    Ok(())
}
pub fn iso_day(s: &str) -> Option<i64> {
    if let Some(c) = regex(r"^(\d{4})-?W(\d{2})(?:-?(\d))?$").ok()?.captures(s) {
        if (s.as_bytes().get(4) == Some(&b'-') && ![8, 10].contains(&s.len()))
            || (s.as_bytes().get(4) == Some(&b'W') && ![7, 8].contains(&s.len()))
        {
            return None;
        }
        let y = c[1].parse::<i64>().ok()?;
        let w = c[2].parse::<i64>().ok()?;
        let d = c
            .get(3)
            .map_or(Some(1), |m| m.as_str().parse::<i64>().ok())?;
        let jan4 = iso_day(&format!("{y:04}-01-04"))?;
        let monday = jan4 - (jan4 - 1) % 7;
        let dec28 = iso_day(&format!("{y:04}-12-28"))?;
        let maxweek = (dec28 - monday) / 7 + 1;
        let ordinal = monday + (w - 1) * 7 + d - 1;
        return ((1..=maxweek).contains(&w)
            && (1..=7).contains(&d)
            && (1..=3_652_059).contains(&ordinal))
        .then_some(ordinal);
    }
    let s = if s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit()) {
        format!("{}-{}-{}", &s[..4], &s[4..6], &s[6..])
    } else {
        s.into()
    };
    let p: Vec<_> = s.split('-').collect();
    if p.len() != 3
        || p[0].len() != 4
        || p[1].len() != 2
        || p[2].len() != 2
        || p.iter().any(|p| !p.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    let y = p[0].parse::<i64>().ok()?;
    let m = p[1].parse::<usize>().ok()?;
    let d = p[2].parse::<i64>().ok()?;
    if !(1..=9999).contains(&y) || !(1..=12).contains(&m) {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let months = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=months[m - 1]).contains(&d) {
        return None;
    }
    Some(
        365 * (y - 1) + (y - 1) / 4 - (y - 1) / 100
            + (y - 1) / 400
            + months[..m - 1].iter().sum::<i64>()
            + d,
    )
}
pub fn review_edit(v: &Value, kind: Option<&str>) -> Result<()> {
    if let Some(body) = v.get("edited_body") {
        require(v["verdict"] == "PASS", "Only PASS may include edited_body")?;
        require(
            kind.is_none_or(|k| ["reply", "firstcontact"].contains(&k)),
            "edited_body is only allowed for reply and firstcontact",
        )?;
        require(nonempty(body), "edited_body must be a nonempty string")?;
    }
    Ok(())
}
pub fn rebind(v: &Value) -> Result<()> {
    if let Some(hash) = v.get("rebind_workflow_hash") {
        require(v["verdict"] == "PASS", "Only PASS may rebind a workflow")?;
        require(hex(hash, 64), "Invalid workflow rebind digest")?;
    }
    Ok(())
}
pub fn value_assessment(result: &Value, required: bool) -> Result<()> {
    if required || result.get("formalization_gaps").is_some() {
        gaps(&result["formalization_gaps"])?;
    }
    if required || result.get("value_assessment").is_some() {
        let v = &result["value_assessment"];
        require(
            keys(v, &["verdict", "reasons"], &[])
                && ["trivial", "incremental", "substantial"].contains(&s(&v["verdict"]))
                && nonempty(&v["reasons"]),
            "Short-note value assessment needs a valid verdict and concrete reasons",
        )?;
    }
    Ok(())
}
pub fn gaps(v: &Value) -> Result<()> {
    require(v.is_array(), "formalization_gaps must be a list")?;
    for g in array(v) {
        require(
            keys(
                g,
                &[
                    "statement",
                    "note_locator",
                    "source_doi",
                    "source_arxiv",
                    "source_url",
                    "source_locator",
                    "why_it_matters",
                ],
                &[],
            ),
            "Invalid formalization gap fields",
        )?;
        require(
            [
                "statement",
                "note_locator",
                "source_locator",
                "why_it_matters",
            ]
            .iter()
            .all(|k| nonempty(&g[k])),
            "Each formalization gap needs the exact statement, both locators and its significance",
        )?;
        require(
            g["source_doi"].is_null()
                || (!coordinate(&g["source_doi"], true).chars().any(whitespace)
                    && regex(r"^10\.\d{4,}/\S+$")?.is_match(&coordinate(&g["source_doi"], true))),
            "Invalid formalization gap DOI",
        )?;
        require(
            g["source_arxiv"].is_null()
                || regex(r"^(?:\d{4}\.\d{4,5}|[a-z][a-z.\-]*/\d{7})(?:v[1-9]\d*)?$")?
                    .is_match(&coordinate(&g["source_arxiv"], false)),
            "Invalid formalization gap arXiv ID",
        )?;
        if !g["source_url"].is_null() {
            let url = s(&g["source_url"]);
            require(
                nonempty(&g["source_url"]) && !url.chars().any(whitespace),
                "Invalid gap source URL",
            )?;
            public_https(url)?;
        }
        require(
            ["source_doi", "source_arxiv", "source_url"]
                .iter()
                .any(|k| !g[k].is_null()),
            "Each formalization gap needs a DOI, arXiv ID or public source URL",
        )?;
    }
    Ok(())
}
fn public_https(url: &str) -> Result<()> {
    let url = url.trim_start_matches(|c: char| c <= '\u{20}');
    let Some((scheme, rest)) = url.split_once("://") else {
        return Err(Error::new("Use a public HTTPS source URL"));
    };
    let netloc = rest.split(['/', '?', '#']).next().unwrap_or("");
    require(
        netloc.contains('[') == netloc.contains(']'),
        "Invalid IPv6 URL",
    )?;
    if let Some((_, tail)) = netloc.split_once('[') {
        let host = tail.split(']').next().unwrap_or("");
        if host.starts_with('v') {
            require(
                regex(r"^v[a-fA-F0-9]+\..+$")?.is_match(host),
                "IPvFuture address is invalid",
            )?;
        } else {
            let invalid = format!(
                "{} does not appear to be an IPv4 or IPv6 address",
                crate::text::repr(&string(host))
            );
            let address = if let Some((address, zone)) = host.split_once('%') {
                require(!zone.is_empty() && !zone.contains('%'), &invalid)?;
                address
            } else {
                host
            };
            require(
                address.parse::<std::net::Ipv4Addr>().is_err(),
                "An IPv4 address cannot be in brackets",
            )?;
            require(address.parse::<std::net::Ipv6Addr>().is_ok(), invalid)?;
        }
    }
    let check = netloc.replace(['@', ':', '#', '?'], "");
    let normalized: String = check.nfkc().collect();
    require(
        normalized == check || !normalized.contains(['/', '?', '#', '@', ':']),
        format!(
            "netloc {} contains invalid characters under NFKC normalization",
            crate::text::repr(&string(netloc))
        ),
    )?;
    let (host, user, password) = if let Some((user, host)) = netloc.rsplit_once('@') {
        let (user, password) = user.split_once(':').unwrap_or((user, ""));
        (host, user, password)
    } else {
        (netloc, "", "")
    };
    let hostname = if let Some(host) = host.strip_prefix('[') {
        host.split(']').next().unwrap_or("")
    } else {
        host.split(':').next().unwrap_or("")
    };
    require(
        scheme.eq_ignore_ascii_case("https")
            && !hostname.is_empty()
            && user.is_empty()
            && password.is_empty(),
        "Use a public HTTPS source URL",
    )
}
pub fn coordinate(v: &Value, doi: bool) -> String {
    let v = s(v).trim_matches(whitespace);
    let pattern = if doi {
        r"(?i)^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)"
    } else {
        r"(?i)^(?:https?://arxiv\.org/(?:abs|pdf)/|arxiv:\s*)"
    };
    let v = regex(pattern).expect("static regex").replace(v, "");
    casefold(if doi {
        &v
    } else {
        v.strip_suffix(".pdf").unwrap_or(&v)
    })
}
pub fn pending_requests(result: &Value) -> Result<Value> {
    let gaps_value = result
        .get("formalization_gaps")
        .cloned()
        .unwrap_or(Value::Array(vec![]));
    gaps(&gaps_value)?;
    let mut seen = BTreeSet::new();
    let mut items = Vec::new();
    for g in array(&gaps_value) {
        let mut coordinates = Vec::new();
        for (kind, value) in [
            ("doi", coordinate(&g["source_doi"], true)),
            ("arxiv", coordinate(&g["source_arxiv"], false)),
            (
                "url",
                s(&g["source_url"])
                    .split('#')
                    .next()
                    .unwrap_or("")
                    .trim_end_matches('/')
                    .into(),
            ),
        ] {
            if !value.is_empty() {
                coordinates.push(Value::Array(vec![string(kind), string(value)]));
            }
        }
        let key = outreach_canon::digest(&Value::Array(vec![
            string(crate::text::compact(s(&g["statement"]), usize::MAX)),
            coordinates
                .first()
                .cloned()
                .ok_or_else(|| Error::new("Missing source coordinate"))?,
        ]))?;
        if seen.insert(key.clone()) {
            items.push(object([
                ("key", string(key)),
                ("state", string("pending")),
                ("gap", g.clone()),
            ]));
        }
    }
    Ok(Value::Array(items))
}
