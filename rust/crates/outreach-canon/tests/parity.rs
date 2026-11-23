use outreach_canon::*;
use std::{fs, path::PathBuf};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn vectors(family: &str) -> Value {
    serde_json::from_slice(
        &fs::read(root().join("vectors").join(format!("{family}.json"))).unwrap(),
    )
    .unwrap()
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap()
}

fn kind(name: &str) -> EnvelopeKind {
    match name {
        "review" => EnvelopeKind::Review,
        "decision" => EnvelopeKind::Decision,
        "pool-adopt" => EnvelopeKind::PoolAdopt,
        "failed-requeue" => EnvelopeKind::FailedRequeue,
        "draft-revision" => EnvelopeKind::DraftRevision,
        "submission-review" => EnvelopeKind::SubmissionReview,
        "mathesis-delivery" => EnvelopeKind::MathesisDelivery,
        _ => panic!("Unknown envelope family: {name}"),
    }
}

#[test]
fn canonical_json_and_digests_match_python() {
    for (i, vector) in vectors("canonical")["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let input = parse(text(vector, "input")).unwrap();
        let actual = canonical(&input).unwrap();
        assert_eq!(
            actual.as_bytes(),
            text(vector, "canonical").as_bytes(),
            "canonical vector {i}: {}",
            vector["input"]
        );
        assert_eq!(
            digest(&input).unwrap(),
            text(vector, "digest"),
            "digest vector {i}"
        );
        assert_eq!(
            canonical(&parse(&actual).unwrap()).unwrap(),
            actual,
            "idempotence vector {i}"
        );
    }
}

#[test]
fn binary64_matches_python_including_boundaries() {
    for (i, vector) in vectors("floats")["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let bits = u64::from_str_radix(text(vector, "bits"), 16).unwrap();
        let actual = python_float(f64::from_bits(bits)).unwrap();
        assert_eq!(
            actual,
            text(vector, "canonical"),
            "float vector {i}: bits={bits:016x}"
        );
        assert_eq!(
            canonical(&float(f64::from_bits(bits)).unwrap()).unwrap(),
            actual
        );
        assert_eq!(
            actual.parse::<f64>().unwrap().to_bits(),
            bits,
            "round trip vector {i}"
        );
    }
}

#[test]
fn registry_and_sent_event_identities_match_real_python_builders() {
    for vector in vectors("events")["vectors"].as_array().unwrap() {
        let event = parse(text(vector, "input")).unwrap();
        assert_eq!(event_identity(&event).unwrap(), text(vector, "id"));
        assert_eq!(
            dumps(
                &event,
                SerializeOptions {
                    ensure_ascii: false,
                    sort_keys: true,
                    ..SerializeOptions::default()
                }
            )
            .unwrap(),
            text(vector, "serialized")
        );
    }
}

#[test]
fn explicit_serialization_options_match_python() {
    for (i, vector) in vectors("serialization")["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let value = parse(text(vector, "input")).unwrap();
        let options = &vector["options"];
        let separators = options["separators"]
            .as_array()
            .map(|s| (s[0].as_str().unwrap(), s[1].as_str().unwrap()));
        assert_eq!(
            dumps(
                &value,
                SerializeOptions {
                    separators,
                    indent: options["indent"].as_str(),
                    ensure_ascii: options["ensure_ascii"].as_bool().unwrap(),
                    sort_keys: options["sort_keys"].as_bool().unwrap(),
                    allow_nan: options["allow_nan"].as_bool().unwrap(),
                }
            )
            .unwrap(),
            text(vector, "expected"),
            "serialization vector {i}"
        );
    }
}

#[test]
fn integer_limits_apply_to_parsing_duplicates_and_constructed_values() {
    use std::str::FromStr;
    for sign in ["", "-"] {
        let accepted = format!("{sign}{}", "9".repeat(MAX_INTEGER_DIGITS));
        assert_eq!(canonical(&parse(&accepted).unwrap()).unwrap(), accepted);
        let rejected = format!("{accepted}9");
        assert!(parse(&rejected).is_err());
        assert!(parse(&format!(r#"{{"a":{rejected},"a":1}}"#)).is_err());
        let value = Value::Number(serde_json::Number::from_str(&rejected).unwrap());
        assert!(canonical(&value).is_err());
        let mut asset = serde_json::Map::new();
        asset.insert("id".into(), value);
        asset.insert("name".into(), Value::String("synthetic.json".into()));
        assert!(input_identity(&Value::Object(asset)).is_err());
    }
}

#[test]
fn deep_inputs_and_constructed_values_return_errors_without_recursion() {
    for vector in vectors("depth_limits")["vectors"].as_array().unwrap() {
        assert!(parse(text(vector, "input")).is_err());
    }
    for depth in [MAX_DEPTH, MAX_DEPTH + 1, 20_000] {
        let input = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
        assert_eq!(parse(&input).is_ok(), depth <= MAX_DEPTH);
        let object = format!("{}0{}", r#"{"a":"#.repeat(depth), "}".repeat(depth));
        assert_eq!(parse(&object).is_ok(), depth <= MAX_DEPTH);
        let mut value = Value::Null;
        for _ in 0..depth {
            value = Value::Array(vec![value]);
        }
        assert_eq!(canonical(&value).is_ok(), depth <= MAX_DEPTH);
        // Value itself has recursive Drop; dismantle adversarial test trees
        // iteratively so this test isolates the bounded serializer's behavior.
        while let Value::Array(mut items) = value {
            value = items.pop().unwrap();
        }
    }
    // Oversized/deep values must be validated even when overwritten later.
    let deep = format!("{}0{}", "[".repeat(513), "]".repeat(513));
    assert!(parse(&format!(r#"{{"a":{deep},"a":1}}"#)).is_err());
}

#[test]
fn parser_rejects_malformed_json_at_container_boundaries() {
    for input in [
        "",
        " ",
        "[1,]",
        r#"{"a":1,}"#,
        "[true false]",
        "[nullx]",
        "01",
        "-01",
        "+1",
        "[1:2]",
        r#"{"a" 1}"#,
        r#"{a:1}"#,
        "[",
        "{",
        "0 0",
        "1e",
        "1.",
        "\u{a0}1",
        "\"unterminated\\",
        "\"control\n\"",
        "[NaN]",
        r#"{"a":NaN,"a":1}"#,
    ] {
        assert!(parse(input).is_err(), "accepted {input:?}");
    }
}

#[test]
fn signed_envelopes_and_event_ids_match_python() {
    for (i, vector) in vectors("envelopes")["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let unsigned = parse(text(vector, "unsigned")).unwrap();
        let key = text(vector, "key").as_bytes();
        let signed = if text(vector, "family") == "mathesis-delivery" {
            sign_mathesis_envelope(&unsigned, key).unwrap()
        } else {
            sign_envelope(&unsigned, key).unwrap()
        };
        assert_eq!(
            canonical(&signed).unwrap(),
            text(vector, "signed"),
            "signature vector {i}"
        );
        assert_eq!(
            digest(&signed).unwrap(),
            text(vector, "event_digest"),
            "event vector {i}"
        );
        assert!(
            verify_typed_envelope(&signed, key, kind(text(vector, "family"))).unwrap(),
            "valid envelope {i}"
        );
        assert!(sign_envelope(&signed, key).is_err());
    }
}

#[test]
fn signature_verification_and_field_gates_match_python() {
    for (i, vector) in vectors("verification")["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let signed = parse(text(vector, "signed")).unwrap();
        let key = text(vector, "key").as_bytes();
        let family = text(vector, "family");
        assert_eq!(
            verify_typed_envelope(&signed, key, kind(family)).unwrap(),
            vector["typed_valid"].as_bool().unwrap(),
            "typed gate {i}: {family}"
        );
        let raw = if family == "mathesis-delivery" {
            verify_mathesis_envelope(&signed, key)
        } else {
            verify_envelope(&signed, key)
        };
        assert_eq!(
            raw.unwrap(),
            vector["raw_valid"].as_bool().unwrap(),
            "raw gate {i}: {family}"
        );
    }
}

#[test]
fn sign_review_edit_and_rebind_rules_match_python() {
    for (i, vector) in vectors("reviews")["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let unsigned = parse(text(vector, "unsigned")).unwrap();
        let result = sign_review(
            &unsigned,
            text(vector, "key").as_bytes(),
            vector["kind"].as_str(),
            vector["current_hash"].as_str(),
        );
        if let Some(expected) = vector["signed"].as_str() {
            assert_eq!(
                canonical(&result.unwrap()).unwrap(),
                expected,
                "sign-review vector {i}"
            );
        } else {
            assert_eq!(
                result.unwrap_err().to_string(),
                text(vector, "error"),
                "sign-review rejection {i}"
            );
        }
    }
}

#[test]
fn identities_and_digest_helpers_match_python() {
    for (i, vector) in vectors("identities")["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let args = parse(text(vector, "args")).unwrap();
        let string = |n: usize| args[n].as_str().unwrap();
        let result = match text(vector, "operation") {
            "input_identity" => input_identity(&args).unwrap(),
            "attempt_key" => attempt_key(string(0), &args[1], string(2)).unwrap(),
            "registry_identity" => registry_identity(string(0), string(1)),
            "request_id" => request_id(string(0), &args[1]).unwrap(),
            "staged_asset_name" => staged_asset_name(string(0), &args[1]).unwrap(),
            "artifact_key" => artifact_key(args[0].as_str(), &args[1]).unwrap(),
            "source_identity" => source_identity(&args).unwrap(),
            "workflow_digest" => workflow_digest(&args[0], &args[1]).unwrap(),
            "delivery_tokens" => {
                let actual: Vec<Value> = delivery_tokens(&args)
                    .unwrap()
                    .into_iter()
                    .map(Value::String)
                    .collect();
                assert_eq!(
                    actual,
                    *vector["expected"].as_array().unwrap(),
                    "delivery tokens {i}"
                );
                continue;
            }
            name => panic!("Unknown identity helper: {name}"),
        };
        assert_eq!(
            result,
            text(vector, "expected"),
            "identity vector {i}: {}",
            vector["operation"]
        );
    }
}

#[test]
fn workflow_formula_matches_python() {
    let family = vectors("workflows");
    for (i, vector) in family["vectors"].as_array().unwrap().iter().enumerate() {
        let mut files = family["file_sets"][text(vector, "profile")]
            .as_object()
            .unwrap()
            .clone();
        files.extend(vector["overrides"].as_object().unwrap().clone());
        for path in vector["missing"].as_array().unwrap() {
            files.remove(path.as_str().unwrap());
        }
        let result = workflow(
            text(vector, "kind"),
            vector["historical"].as_bool().unwrap(),
            |path| Ok(files.get(path).map(|v| v.as_str().unwrap().to_owned())),
        );
        if vector["error"].as_bool().unwrap() {
            assert!(result.is_err(), "workflow rejection {i}");
        } else {
            assert_eq!(
                result.unwrap(),
                text(vector, "digest"),
                "workflow vector {i}: {}",
                vector["kind"]
            );
        }
    }
}

#[test]
fn nonfinite_floats_and_unencodable_surrogates_produce_no_bytes() {
    for vector in vectors("rejections")["vectors"].as_array().unwrap() {
        assert!(
            parse(text(vector, "input"))
                .and_then(|v| canonical(&v))
                .is_err(),
            "Python rejects {}",
            vector["input"]
        );
    }
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(python_float(value).is_err());
        assert!(float(value).is_err());
    }
}
