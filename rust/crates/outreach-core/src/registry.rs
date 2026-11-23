use crate::*;
use std::collections::{BTreeMap, BTreeSet};
use unicode_normalization::UnicodeNormalization;

pub fn name_key(value: &str) -> String {
    text::compact(&casefold(&value.nfkc().collect::<String>()), usize::MAX)
}
pub fn address(v: &Value) -> String {
    casefold(s(v).trim_matches(text::whitespace))
}
/// registry.email: strings, bare addresses and Unicode casefolding.
pub fn email(v: &Value) -> Result<String> {
    let raw = v
        .as_str()
        .ok_or_else(|| Error::new("Use a bare email address"))?;
    let value = casefold(raw.trim_matches(text::whitespace));
    let valid = value.split_once('@').is_some_and(|(local, domain)| {
        !local.is_empty()
            && domain.len() >= 3
            && domain.as_bytes()[1..domain.len() - 1].contains(&b'.')
    }) && value.chars().filter(|c| *c == '@').count() == 1
        && !value
            .chars()
            .any(|c| text::whitespace(c) || "<>".contains(c));
    if !valid {
        return Err(Error::new("Use a bare email address"));
    }
    Ok(value)
}
/// mailbox.address: Graph objects only, strip + lower (not casefold).
pub fn packet_address(v: &Value) -> Result<String> {
    if v.is_string() {
        return email(v);
    }
    if truth(v) && !v.is_object() {
        return Err(Error::new("Mailbox recipient must be an object"));
    }
    let header = &v["emailAddress"];
    if truth(header) && !header.is_object() {
        return Err(Error::new("emailAddress must be an object"));
    }
    let address = &header["address"];
    if !truth(address) {
        return Ok(String::new());
    }
    let address = address
        .as_str()
        .ok_or_else(|| Error::new("Mailbox address must be a string"))?;
    Ok(address.trim_matches(text::whitespace).to_lowercase())
}
pub fn digest_people(tables: &Value) -> Result<Vec<Value>> {
    if let Ok(persons) = people(tables) {
        return Ok(persons);
    }
    let mut persons = Vec::new();
    for row in array(&tables["contacts"]["contacts"]) {
        if !row.is_object() || !truth(&row["email"]) {
            continue;
        }
        outreach_model::required_str(row, "id")?;
        outreach_model::required_str(row, "name")?;
        let address = outreach_model::required_str(row, "email")?;
        let mut emails = BTreeSet::from([address.to_owned()]);
        for alias in array(&row["relationship"]["aliases"]) {
            emails.insert(
                alias
                    .as_str()
                    .ok_or_else(|| Error::new("Alias must be a string"))?
                    .into(),
            );
        }
        let mut person = row.clone();
        person["emails"] = Value::Array(emails.into_iter().map(string).collect());
        persons.push(person);
    }
    Ok(persons)
}
fn root(parents: &mut BTreeMap<String, String>, key: &str) -> String {
    let mut current = key.to_owned();
    let mut path = Vec::new();
    while let Some(next) = parents.get(&current) {
        if next == &current {
            break;
        }
        path.push(current.clone());
        current = next.clone();
    }
    parents
        .entry(current.clone())
        .or_insert_with(|| current.clone());
    for old in path {
        parents.insert(old, current.clone());
    }
    current
}
pub fn people(tables: &Value) -> Result<Vec<Value>> {
    let rows = tables["contacts"]["contacts"]
        .as_array()
        .ok_or_else(|| Error::new("Missing contacts list"))?;
    let refusals = tables["refusals"]["refusals"]
        .as_array()
        .ok_or_else(|| Error::new("Missing refusals list"))?;
    for row in rows {
        for key in ["id", "name", "email"] {
            outreach_model::required_str(row, key)?;
        }
        email(&row["email"])?;
        for key in ["relationship", "salutation", "hub"] {
            if truth(&row[key]) && !row[key].is_object() {
                return Err(Error::new(format!("Contact {key} must be an object")));
            }
        }
    }
    for row in refusals {
        if !row["emails"].is_array() {
            return Err(Error::new("Missing refusal emails list"));
        }
    }
    let mut parents = BTreeMap::new();
    for row in rows {
        let email = email(&row["email"])?;
        root(&mut parents, &email);
        let rel = &row["relationship"];
        let mut aliases = array(&rel["aliases"]).to_vec();
        aliases.extend([rel["alias_of"].clone(), rel["canonical_email"].clone()]);
        if truth(&row["salutation"]["verified"]) {
            aliases.extend_from_slice(array(&row["salutation"]["emails"]));
        }
        for alias in aliases {
            if s(&alias).contains('@') {
                let a = root(&mut parents, &email);
                let b = root(&mut parents, &self::email(&alias)?);
                parents.insert(b, a);
            }
        }
    }
    for refusal in refusals {
        let emails = array(&refusal["emails"]);
        for alias in emails.iter().skip(1) {
            let a = root(&mut parents, s(&emails[0]));
            let b = root(&mut parents, s(alias));
            parents.insert(b, a);
        }
    }
    let mut grouped: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for row in rows {
        grouped
            .entry(root(&mut parents, &address(&row["email"])))
            .or_default()
            .push(row.clone());
    }
    let mut result = Vec::new();
    for (key, mut records) in grouped {
        records.sort_by(|a, b| s(&a["email"]).cmp(s(&b["email"])));
        let preferred = records.iter().find_map(|r| {
            truth(&r["relationship"]["canonical_email"])
                .then_some(&r["relationship"]["canonical_email"])
        });
        let primary = records
            .iter()
            .find(|r| Some(&r["email"]) == preferred)
            .unwrap_or(&records[0]);
        let mut display = s(&primary["name"]);
        if display.contains('@') || display.trim_matches(text::whitespace).is_empty() {
            display = records
                .iter()
                .map(|r| s(&r["name"]))
                .find(|n| !n.contains('@') && !n.trim_matches(text::whitespace).is_empty())
                .unwrap_or(display);
        }
        let keys: Vec<_> = parents.keys().cloned().collect();
        let aliases: Vec<_> = keys
            .into_iter()
            .filter(|a| root(&mut parents, a) == key)
            .map(string)
            .collect();
        let id = records.iter().map(|r| s(&r["id"])).min().unwrap_or("");
        result.push(object([
            ("id", string(id)),
            ("name", string(display)),
            ("email", primary["email"].clone()),
            ("emails", Value::Array(aliases)),
            ("records", Value::Array(records)),
        ]));
    }
    result.sort_by_key(|p| (name_key(s(&p["name"])), s(&p["email"]).to_owned()));
    Ok(result)
}
fn contacted(person: &Value) -> bool {
    for row in array(&person["records"]) {
        let rel = &row["relationship"];
        if (truth(&rel["last_sent"])
            && [
                "last_sent_message_id",
                "last_sent_mail_id",
                "last_sent_receipt",
                "verified_sent_record",
                "verified_sent_message_id",
            ]
            .iter()
            .any(|k| truth(&rel[*k])))
            || (truth(&rel["last_reply"])
                && [
                    "last_reply_message_id",
                    "last_reply_mail_id",
                    "last_reply_receipt",
                ]
                .iter()
                .any(|k| truth(&rel[*k])))
        {
            return true;
        }
        for t in array(&row["hub"]["threads"]) {
            if !array(&person["emails"])
                .contains(t.get("correspondent_email").unwrap_or(&row["email"]))
            {
                continue;
            }
            if ["last_in", "last_out"].iter().any(|k| {
                truth(&t[*k]["date"]) && (truth(&t[*k]["ROWID"]) || truth(&t[*k]["message_id"]))
            }) {
                return true;
            }
        }
        if array(&row["hub"]["historical_inbound"]).iter().any(|r| {
            array(&person["emails"]).contains(&r["correspondent_email"])
                && truth(&r["date"])
                && truth(&r["rowid"])
                && truth(&r["message_id"])
        }) {
            return true;
        }
        if array(&row["hub"]["events"])
            .iter()
            .any(|e| (e["kind"] == "sent" || e["kind"] == "reply") && truth(&e["at"]))
        {
            return true;
        }
    }
    false
}
pub fn preflight(
    email: &str,
    name: &str,
    tables: &Value,
    persons: &[Value],
) -> Result<Vec<String>> {
    let email = self::email(&string(email))?;
    let person = persons
        .iter()
        .find(|p| array(&p["emails"]).iter().any(|a| s(a) == email));
    let aliases: BTreeSet<_> = person
        .map(|p| array(&p["emails"]).iter().map(s).collect())
        .unwrap_or_else(|| BTreeSet::from([email.as_str()]));
    let mut reasons = Vec::new();
    if array(&tables["refusals"]["refusals"]).iter().any(|r| {
        truth(&r["active"])
            && (array(&r["emails"]).iter().any(|e| aliases.contains(s(e)))
                || (!name.is_empty() && name_key(s(&r["name"])) == name_key(name)))
    }) {
        reasons.push("table-3-refusal-or-stop-boundary".into());
    }
    if let Some(p) = person {
        if array(&p["records"])
            .iter()
            .flat_map(|r| array(&r["hub"]["undeliverable_addresses"]))
            .any(|r| address(&r["email"]) == email)
        {
            reasons.push("undeliverable-address".into());
        }
        if contacted(p) {
            reasons.push("already-contacted-use-the-existing-thread".into());
        }
    }
    let rule = &tables["decisions"]["firstcontact_excluded_regions"];
    let mut excluded = array(&rule["domain_suffixes"])
        .iter()
        .any(|v| email.ends_with(s(v)));
    if let Some(p) = person
        && truth(&rule["affiliation_pattern"])
    {
        let pattern = regex(s(&rule["affiliation_pattern"]))?;
        let exempt = if truth(&rule["affiliation_exempt_pattern"]) {
            Some(regex(s(&rule["affiliation_exempt_pattern"]))?)
        } else {
            None
        };
        excluded |= array(&p["records"]).iter().any(|r| {
            r["affiliation"].as_str().is_some_and(|a| {
                pattern.is_match(a) && !exempt.as_ref().is_some_and(|e| e.is_match(a))
            })
        });
    }
    if excluded {
        reasons.push("excluded-region-mainland-china".into());
    }
    Ok(reasons)
}
