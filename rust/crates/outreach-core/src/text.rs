use outreach_canon::Value;
pub fn truth(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_str().parse::<f64>().is_ok_and(|x| x != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}
pub fn whitespace(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}
pub fn compact(s: &str, limit: usize) -> String {
    s.split(whitespace)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(limit)
        .collect()
}
pub fn py_str(v: &Value) -> String {
    match v {
        Value::Null => "None".into(),
        Value::Bool(true) => "True".into(),
        Value::Bool(false) => "False".into(),
        Value::String(s) => s.clone(),
        Value::Number(n) => {
            if n.as_str().contains(['.', 'e', 'E']) {
                outreach_canon::python_float(n.as_f64().unwrap()).unwrap()
            } else {
                n.to_string()
            }
        }
        Value::Array(a) => format!("[{}]", a.iter().map(repr).collect::<Vec<_>>().join(", ")),
        Value::Object(o) => format!(
            "{{{}}}",
            o.iter()
                .map(|(k, v)| format!("{}: {}", repr(&Value::String(k.clone())), repr(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}
fn repr(v: &Value) -> String {
    if let Value::String(s) = v {
        let quote = if s.contains('\'') && !s.contains('"') {
            '"'
        } else {
            '\''
        };
        let mut output = quote.to_string();
        for ch in s.chars() {
            if ch == quote || ch == '\\' {
                output.push('\\');
                output.push(ch);
            } else {
                match ch {
                    '\n' => output.push_str("\\n"),
                    '\r' => output.push_str("\\r"),
                    '\t' => output.push_str("\\t"),
                    c if !printable(c) => {
                        let cp = c as u32;
                        output.push_str(&if cp <= 0xff {
                            format!("\\x{cp:02x}")
                        } else if cp <= 0xffff {
                            format!("\\u{cp:04x}")
                        } else {
                            format!("\\U{cp:08x}")
                        });
                    }
                    c => output.push(c),
                }
            }
        }
        output.push(quote);
        output
    } else {
        py_str(v)
    }
}
pub fn casefold(value: &str) -> String {
    value
        .chars()
        .flat_map(|c| include!("casefold.rs"))
        .collect()
}

fn printable(c: char) -> bool {
    let ranges: &[(u32, u32)] = include!("nonprintable.rs");
    let cp = c as u32;
    let i = ranges.partition_point(|(start, _)| *start <= cp);
    i == 0 || cp > ranges[i - 1].1
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_unicode_category_and_casefold_scalar_matches_python() {
        let vectors = outreach_canon::parse(include_str!("../../../vectors/unicode.json")).unwrap();
        let vectors = &vectors["vectors"][0];
        for r in vectors["ranges"].as_array().unwrap() {
            let start = r["start"].as_u64().unwrap() as u32;
            let end = r["end"].as_u64().unwrap() as u32;
            for cp in start..=end {
                if let Some(ch) = char::from_u32(cp) {
                    assert_eq!(
                        printable(ch),
                        r["printable"].as_bool().unwrap(),
                        "U+{cp:04X}"
                    );
                }
            }
            for (cp, key) in [(start, "first_repr"), (end, "last_repr")] {
                if let Some(ch) = char::from_u32(cp) {
                    assert_eq!(
                        repr(&Value::String(ch.to_string())),
                        r[key].as_str().unwrap(),
                        "U+{cp:04X}"
                    );
                }
            }
        }
        let folds: std::collections::BTreeMap<_, _> = vectors["casefold"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| (v[0].as_u64().unwrap() as u32, v[1].as_str().unwrap()))
            .collect();
        for cp in 0..=0x10ffff {
            if let Some(ch) = char::from_u32(cp) {
                let original = ch.to_string();
                assert_eq!(
                    casefold(&original),
                    folds.get(&cp).copied().unwrap_or(&original),
                    "U+{cp:04X}"
                );
            }
        }
    }
}
