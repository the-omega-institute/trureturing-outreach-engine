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
pub fn split_lines(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut chars = s.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if matches!(
            c,
            '\n' | '\r'
                | '\x0b'
                | '\x0c'
                | '\u{1c}'
                | '\u{1d}'
                | '\u{1e}'
                | '\u{85}'
                | '\u{2028}'
                | '\u{2029}'
        ) {
            out.push(&s[start..i]);
            start = i + c.len_utf8();
            if c == '\r' && chars.peek().is_some_and(|(_, c)| *c == '\n') {
                chars.next();
                start += 1;
            }
        }
    }
    if start < s.len() {
        out.push(&s[start..]);
    }
    out
}
pub fn alphanumeric(c: char) -> bool {
    let ranges: &[(u32, u32)] = include!("alphanumeric.rs");
    let n = c as u32;
    let index = ranges.partition_point(|(start, _)| *start <= n);
    index > 0 && n <= ranges[index - 1].1
}
/// CPython 3.12 lower, including its context-sensitive Greek final sigma.
pub fn lower(s: &str) -> String {
    let chars: Vec<_> = s.chars().collect();
    let mut out = String::new();
    type Ranges = &'static [(u32, u32)];
    let (cased, ignorable): (Ranges, Ranges) = include!("case_properties.rs");
    let has = |ranges: &[(u32, u32)], c: char| {
        let cp = c as u32;
        let i = ranges.partition_point(|(a, _)| *a <= cp);
        i > 0 && cp <= ranges[i - 1].1
    };
    for (i, c) in chars.iter().copied().enumerate() {
        if c == 'Σ' {
            let before = chars[..i]
                .iter()
                .rev()
                .find(|c| !has(ignorable, **c))
                .is_some_and(|c| has(cased, *c));
            let after = chars[i + 1..]
                .iter()
                .find(|c| !has(ignorable, **c))
                .is_some_and(|c| has(cased, *c));
            out.push(if before && !after { 'ς' } else { 'σ' });
        } else {
            let mapped: Vec<char> = include!("lowercase.rs");
            out.extend(mapped);
        }
    }
    out
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
pub fn repr(v: &Value) -> String {
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
                    assert_eq!(
                        alphanumeric(ch),
                        r["category"]
                            .as_str()
                            .is_some_and(|s| s.starts_with(['L', 'N'])),
                        "alphanumeric U+{cp:04X}"
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
    #[test]
    fn python_lower_observes_final_sigma_context() {
        for (input, expected) in [
            ("ΟΣ", "ος"),
            ("ΟΣΑ", "οσα"),
            ("Σ", "σ"),
            ("ΟΣ\u{301}", "ος\u{301}"),
            (".ʰΣ", ".ʰσ"),
            ("aʰΣ", "aʰς"),
            ("İ", "i\u{307}"),
        ] {
            assert_eq!(lower(input), expected);
        }
    }
}
