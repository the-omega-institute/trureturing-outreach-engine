use crate::{Error, MAX_DEPTH, Result, Value, check_integer, python_float};

/// Python json.dumps options for the representable JSON domain. indent is the
/// string repeated per level (Some("  ") corresponds to Python indent=2).
/// None separators selects Python's defaults, including the indent-dependent
/// item separator. sort_keys=false follows insertion order; duplicate keys keep their first
/// position and their last value, matching Python dictionaries.
#[derive(Clone, Copy, Debug)]
pub struct SerializeOptions<'a> {
    pub separators: Option<(&'a str, &'a str)>,
    pub indent: Option<&'a str>,
    pub ensure_ascii: bool,
    pub sort_keys: bool,
    pub allow_nan: bool,
}

impl Default for SerializeOptions<'_> {
    fn default() -> Self {
        Self {
            separators: None,
            indent: None,
            ensure_ascii: true,
            sort_keys: false,
            allow_nan: true,
        }
    }
}

enum Part<'a> {
    Value(&'a Value, usize),
    Text(&'a str),
    Quoted(&'a str),
    Newline(usize),
}

fn quoted(text: &str, ensure_ascii: bool, output: &mut String) -> Result<()> {
    if !ensure_ascii {
        output.push_str(&serde_json::to_string(text)?);
        return Ok(());
    }
    output.push('"');
    for ch in text.chars() {
        match ch {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{8}' => output.push_str("\\b"),
            '\u{c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{20}'..='\u{7e}' => output.push(ch),
            _ => {
                for unit in ch.encode_utf16(&mut [0; 2]) {
                    output.push_str(&format!("\\u{unit:04x}"));
                }
            }
        }
    }
    output.push('"');
    Ok(())
}

/// Bounded, iterative serialization. canonical on values constructed outside
/// parse / float (or composition of supported scalars) is unsupported: serde's
/// From<f64> silently loses nonfinite values by converting them to null.
pub fn dumps(value: &Value, options: SerializeOptions<'_>) -> Result<String> {
    let (item_separator, key_separator) = options
        .separators
        .unwrap_or((if options.indent.is_some() { "," } else { ", " }, ": "));
    let mut output = String::new();
    let mut stack = vec![Part::Value(value, 0)];
    while let Some(part) = stack.pop() {
        match part {
            Part::Text(text) => output.push_str(text),
            Part::Quoted(text) => quoted(text, options.ensure_ascii, &mut output)?,
            Part::Newline(depth) => {
                if let Some(indent) = options.indent {
                    output.push('\n');
                    for _ in 0..depth {
                        output.push_str(indent);
                    }
                }
            }
            Part::Value(value, depth) => {
                if matches!(value, Value::Array(_) | Value::Object(_)) && depth >= MAX_DEPTH {
                    return Err(Error::new("JSON nesting exceeds 512 containers"));
                }
                match value {
                    Value::Null => output.push_str("null"),
                    Value::Bool(b) => output.push_str(if *b { "true" } else { "false" }),
                    Value::Number(number) => {
                        let text = number.as_str();
                        if text.contains(['.', 'e', 'E']) {
                            let number: f64 =
                                text.parse().map_err(|_| Error::new("Invalid float"))?;
                            if options.allow_nan && !number.is_finite() {
                                output.push_str(if number.is_nan() {
                                    "NaN"
                                } else if number.is_sign_negative() {
                                    "-Infinity"
                                } else {
                                    "Infinity"
                                });
                            } else {
                                output.push_str(&python_float(number)?);
                            }
                        } else {
                            check_integer(text)?;
                            output.push_str(if text == "-0" { "0" } else { text });
                        }
                    }
                    Value::String(text) => quoted(text, options.ensure_ascii, &mut output)?,
                    Value::Array(items) => {
                        output.push('[');
                        stack.push(Part::Text("]"));
                        if !items.is_empty() {
                            stack.push(Part::Newline(depth));
                        }
                        for (i, item) in items.iter().enumerate().rev() {
                            stack.push(Part::Value(item, depth + 1));
                            stack.push(Part::Newline(depth + 1));
                            if i != 0 {
                                stack.push(Part::Text(item_separator));
                            }
                        }
                    }
                    Value::Object(fields) => {
                        output.push('{');
                        stack.push(Part::Text("}"));
                        let mut ordered: Vec<_> = fields.iter().collect();
                        if options.sort_keys {
                            // UTF-8 order equals Python Unicode code-point order.
                            ordered.sort_unstable_by_key(|(a, _)| *a);
                        }
                        if !ordered.is_empty() {
                            stack.push(Part::Newline(depth));
                        }
                        for (i, (key, item)) in ordered.into_iter().enumerate().rev() {
                            stack.push(Part::Value(item, depth + 1));
                            stack.push(Part::Text(key_separator));
                            stack.push(Part::Quoted(key));
                            stack.push(Part::Newline(depth + 1));
                            if i != 0 {
                                stack.push(Part::Text(item_separator));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(output)
}

/// Python json.dumps(sort_keys=True, ensure_ascii=False,
/// separators=(',', ':'), allow_nan=False), encoded as UTF-8.
pub fn canonical(value: &Value) -> Result<String> {
    dumps(
        value,
        SerializeOptions {
            separators: Some((",", ":")),
            indent: None,
            ensure_ascii: false,
            sort_keys: true,
            allow_nan: false,
        },
    )
}
