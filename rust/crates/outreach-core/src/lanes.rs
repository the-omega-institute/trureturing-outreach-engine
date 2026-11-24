use crate::*;
use outreach_model::{TaskKind, TaskState};
use std::collections::{BTreeMap, BTreeSet};
fn note(row: &Value) -> bool {
    row["kind"] == "shortnote" || row["kind"] == "mathesis"
}
fn ids(values: &[&Value]) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    for v in values {
        for key in ["result_id", "problem_id", "id", "result_ids", "problem_ids"] {
            let value = &v[key];
            if value.is_array() {
                result.extend(
                    array(value)
                        .iter()
                        .filter_map(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned),
                );
            } else if let Some(s) = value.as_str()
                && !s.is_empty()
            {
                result.insert(s.into());
            }
        }
    }
    result
}
fn linked(rows: &[Value], env: &Environment) -> Result<BTreeSet<String>> {
    let mut output = BTreeSet::new();
    if !truth(&env.config["sandbox_pool"]["prioritize_correspondence_notes"])
        || !rows.iter().any(note)
    {
        return Ok(output);
    }
    let persons = registry::people(&env.tables)?;
    let contact = |row: &Value| {
        let email = registry::address(&row["request"]["inputs"]["mailbox_observation"]["email"]);
        persons
            .iter()
            .find(|p| array(&p["emails"]).iter().any(|a| s(a) == email))
            .or_else(|| {
                persons
                    .iter()
                    .find(|p| array(&p["records"]).iter().any(|r| r["id"] == row["owner"]))
            })
            .map(|p| s(&p["id"]).to_owned())
            .unwrap_or_else(|| s(&row["owner"]).into())
    };
    let open: BTreeSet<_> = rows
        .iter()
        .filter(|r| {
            r["kind"] == "reply"
                && (s(&r["state"])
                    .parse::<TaskState>()
                    .is_ok_and(TaskState::active)
                    || [
                        "queued",
                        "awaiting-review",
                        "approved",
                        "awaiting-mailbox",
                        "owner-draft",
                        "awaiting-user",
                        "decision-recorded",
                        "bounced",
                    ]
                    .contains(&s(&r["state"])))
        })
        .map(contact)
        .collect();
    let mut problems = BTreeSet::new();
    for p in &persons {
        if open.contains(s(&p["id"])) {
            for r in array(&p["records"]) {
                problems.extend(
                    array(&r["problem_ids"])
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned),
                );
            }
        }
    }
    let by_id: BTreeMap<_, _> = rows.iter().map(|r| (s(&r["id"]), r)).collect();
    for row in rows.iter().filter(|r| note(r)) {
        let mut pending = vec![row];
        let mut seen = BTreeSet::new();
        let mut found = false;
        while let Some(ancestor) = pending.pop() {
            if !seen.insert(s(&ancestor["id"])) {
                continue;
            }
            if ancestor["kind"] == "reply" {
                found = true;
                break;
            }
            let inputs = &ancestor["request"]["inputs"];
            let data = &ancestor["data"];
            let delivery = &data["mathesis_delivery"];
            let metadata =
                &fallback(&[&delivery["collection_envelope"], &delivery["envelope"]])["metadata"];
            let mut found_ids = ids(&[
                inputs,
                &inputs["pipeline"],
                &inputs["result"],
                &data["publication_loop"],
                &metadata["origin"],
                &metadata["review"]["record"],
            ]);
            if let Some(id) = s(&ancestor["owner"]).strip_prefix("result-") {
                found_ids.insert(id.into());
            }
            if !found_ids.is_disjoint(&problems) {
                found = true;
                break;
            }
            if let Some(r) = by_id.get(s(&inputs["reply_origin_task_id"]))
                && r["kind"] == "reply"
            {
                pending.push(r);
            }
            for (key, field) in [
                ("automation_parent", "task_id"),
                ("pipeline", "note_task_id"),
            ] {
                if let Some(r) = by_id.get(s(&inputs[key][field])) {
                    pending.push(r);
                }
            }
            for link in array(&inputs["note_sources"]) {
                if let Some(r) = by_id.get(s(&link["task_id"]))
                    && r["kind"] == "shortnote"
                {
                    pending.push(r);
                }
            }
        }
        if found {
            output.insert(s(&row["id"]).into());
        }
    }
    Ok(output)
}
pub fn validate(config: &Value) -> Result<()> {
    if !config.is_object() {
        return Err(Error::new("Config must be an object"));
    }
    if let Some(v) = config.get("followup_lane")
        && !v.is_boolean()
    {
        return Err(Error::new("followup_lane must be a boolean"));
    }
    let pool = &config["sandbox_pool"];
    if truth(pool) && !pool.is_object() {
        return Err(Error::new("sandbox_pool must be an object"));
    }
    if pool.get("prioritize_correspondence_notes").is_some() {
        validate_groups(config)?;
    }
    if let Some(v) = config.get("kind_quota") {
        if v != "equal" {
            return Err(Error::new("kind_quota must be equal"));
        }
        if config["max_active"].as_i64().is_none_or(|n| n < 0) {
            return Err(Error::new("max_active must be a nonnegative integer"));
        }
        let kinds = config["kinds"]
            .as_object()
            .ok_or_else(|| Error::new("Missing kinds object"))?;
        for kind in kinds.keys() {
            kind.parse::<TaskKind>()?;
        }
    }
    Ok(())
}
fn validate_groups(config: &Value) -> Result<()> {
    let pool = &config["sandbox_pool"];
    let priority = pool
        .get("prioritize_correspondence_notes")
        .unwrap_or(&Value::Bool(false));
    if !priority.is_boolean() {
        return Err(Error::new(
            "prioritize_correspondence_notes must be a boolean",
        ));
    }
    let Some(groups) = pool.get("groups") else {
        return if truth(priority) {
            Err(Error::new("Correspondence priority requires a reply group"))
        } else {
            Ok(())
        };
    };
    let groups = groups
        .as_array()
        .filter(|a| !a.is_empty())
        .ok_or_else(|| Error::new("Sandbox groups must be a nonempty list"))?;
    let mut names = BTreeSet::new();
    let mut members = BTreeSet::new();
    let mut sum = 0_i64;
    for group in groups {
        let name = group["name"]
            .as_str()
            .filter(|s| !s.trim_matches(text::whitespace).is_empty())
            .ok_or_else(|| Error::new("Invalid Sandbox group name"))?;
        if !names.insert(name) {
            return Err(Error::new("Duplicate Sandbox group name"));
        }
        let limit = group["max_sandboxes"]
            .as_i64()
            .filter(|n| *n > 0)
            .ok_or_else(|| Error::new("Invalid Sandbox group capacity"))?;
        sum = sum
            .checked_add(limit)
            .ok_or_else(|| Error::new("Sandbox capacity overflow"))?;
        for kind in group["kinds"]
            .as_array()
            .filter(|a| !a.is_empty())
            .ok_or_else(|| Error::new("Sandbox group kinds must be a nonempty list"))?
        {
            let kind = kind
                .as_str()
                .ok_or_else(|| Error::new("Invalid group kind"))?;
            kind.parse::<TaskKind>()?;
            if !members.insert(kind) {
                return Err(Error::new("Task kind belongs to multiple Sandbox groups"));
            }
        }
    }
    let kinds = config["kinds"]
        .as_object()
        .ok_or_else(|| Error::new("Missing kinds object"))?;
    if kinds
        .iter()
        .any(|(k, v)| truth(v) && !members.contains(k.as_str()))
    {
        return Err(Error::new("Enabled task kinds missing from Sandbox groups"));
    }
    if pool["max_sandboxes"].as_i64().is_none_or(|n| sum > n) {
        return Err(Error::new("Sandbox group limits exceed max_sandboxes"));
    }
    if truth(priority) && !members.contains("reply") {
        return Err(Error::new("Correspondence priority requires a reply group"));
    }
    Ok(())
}
pub fn summary(rows: &[Value], env: &Environment) -> Result<Value> {
    let config = &env.config;
    let mut result = Value::Object(Default::default());
    if config.get("kind_quota").is_none() {
        return Ok(result);
    }
    validate_groups(config)?;
    let pool = &config["sandbox_pool"];
    let mut limits = BTreeMap::new();
    let mut groups = BTreeMap::new();
    let mut order = Vec::new();
    for group in array(&pool["groups"]) {
        let name = s(&group["name"]);
        let limit = group["max_sandboxes"]
            .as_i64()
            .filter(|n| *n > 0)
            .ok_or_else(|| Error::new("Invalid sandbox group capacity"))?;
        if name.trim().is_empty() || limits.insert(name, limit).is_some() {
            return Err(Error::new("Invalid sandbox group name"));
        }
        for kind in array(&group["kinds"]) {
            let kind = s(kind);
            kind.parse::<TaskKind>()?;
            if groups.insert(kind, name).is_some() {
                return Err(Error::new("Task kind belongs to multiple Sandbox groups"));
            }
        }
    }
    let kinds = config["kinds"]
        .as_object()
        .ok_or_else(|| Error::new("Missing kinds"))?;
    let mut lanes: BTreeMap<Option<&str>, BTreeSet<&str>> = BTreeMap::new();
    for (kind, enabled) in kinds {
        if !truth(enabled) {
            continue;
        }
        kind.parse::<TaskKind>()?;
        if truth(&pool["groups"]) && !groups.contains_key(kind.as_str()) {
            return Err(Error::new("Enabled task kinds missing from Sandbox groups"));
        }
        let group = groups.get(kind.as_str()).copied();
        if !lanes.contains_key(&group) {
            order.push(group);
        }
        let names = lanes.entry(group).or_default();
        names.insert(kind);
        if truth(&config["followup_lane"]) && ["shortnote", "mathesis"].contains(&kind.as_str()) {
            names.insert("followup");
        }
    }
    let priority = truth(&pool["prioritize_correspondence_notes"]);
    if priority
        && kinds
            .iter()
            .any(|(k, v)| ["shortnote", "mathesis"].contains(&k.as_str()) && truth(v))
    {
        let group = groups.get("reply").copied().ok_or_else(|| {
            Error::new("Correspondence note priority requires a Sandbox group for reply")
        })?;
        if !lanes.contains_key(&Some(group)) {
            order.push(Some(group));
        }
        lanes
            .entry(Some(group))
            .or_default()
            .insert("correspondence");
    }
    let correspondence = linked(rows, env)?;
    let mut active: BTreeMap<(Option<&str>, &str), i64> = BTreeMap::new();
    let mut queued = active.clone();
    for row in rows {
        let state: TaskState = s(&row["state"]).parse()?;
        let kind = s(&row["kind"]);
        kind.parse::<TaskKind>()?;
        let is_correspondence = if state == TaskState::Queued {
            correspondence.contains(s(&row["id"]))
        } else {
            row["data"]["pool_lane"] == "correspondence"
        };
        let lane = if is_correspondence {
            "correspondence"
        } else if truth(&config["followup_lane"])
            && note(row)
            && truth(&row["request"]["inputs"]["owner_ids"])
        {
            "followup"
        } else {
            kind
        };
        let group = if state == TaskState::Queued {
            groups
                .get(if correspondence.contains(s(&row["id"])) {
                    "reply"
                } else {
                    kind
                })
                .copied()
        } else {
            row["data"]["pool_group"]
                .as_str()
                .filter(|s| !s.is_empty())
                .or_else(|| groups.get(kind).copied())
        };
        if state.active() {
            *active.entry((group, lane)).or_default() += 1;
        } else if state == TaskState::Queued {
            *queued.entry((group, lane)).or_default() += 1;
        }
    }
    for group in order {
        let names = &lanes[&group];
        let capacity = match group {
            None => config["max_active"].as_i64().unwrap_or(0),
            Some(g) => limits[g]
                .checked_mul(
                    pool["agents_per_sandbox"]
                        .as_i64()
                        .ok_or_else(|| Error::new("Missing agents_per_sandbox integer"))?,
                )
                .ok_or_else(|| Error::new("Sandbox capacity overflow"))?,
        };
        for (index, name) in names.iter().enumerate() {
            let quota = capacity / names.len() as i64
                + i64::from((index as i64) < capacity % names.len() as i64);
            let a = *active.get(&(group, *name)).unwrap_or(&0);
            let q = *queued.get(&(group, *name)).unwrap_or(&0);
            if result.get(*name).is_none() {
                result[*name] = object([
                    ("active", 0.into()),
                    ("queued", 0.into()),
                    ("quota", 0.into()),
                    ("borrowing", 0.into()),
                ]);
            }
            for (key, n) in [
                ("active", a),
                ("queued", q),
                ("quota", quota),
                ("borrowing", (a - quota).max(0)),
            ] {
                let previous = result[*name][key].as_i64().unwrap();
                result[*name][key] = (previous + n).into();
            }
        }
    }
    Ok(result)
}
