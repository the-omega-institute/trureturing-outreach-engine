//! Development-only streaming differential-test adapter.
use outreach_canon::{Value, canonical, parse};
use outreach_core::{object, string, validation};
use std::io::{self, BufRead};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    for line in io::stdin().lock().lines() {
        let input = parse(&line?)?;
        if input["family"] == "instant" {
            let micros = outreach_core::gates::instant_micros(&input["value"]);
            println!(
                "{}",
                canonical(&object([
                    ("ok", micros.is_some().into()),
                    ("micros", micros.map(Value::from).unwrap_or(Value::Null))
                ]))?
            );
            continue;
        }
        let result = match input["family"].as_str() {
            Some("request") => validation::request(&input["value"]),
            Some("packet") => validation::packet(&input["value"]),
            Some("gaps") => validation::gaps(&input["value"]),
            _ => Err(outreach_canon::Error::new("Unknown fixture family")),
        };
        let v = match result {
            Ok(()) => object([("ok", Value::Bool(true))]),
            Err(e) => object([("ok", Value::Bool(false)), ("error", string(e.to_string()))]),
        };
        println!("{}", canonical(&v)?);
    }
    Ok(())
}
