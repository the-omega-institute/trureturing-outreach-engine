//! Ephemeral live Python inputs arrive on stdin; no repository text is pinned.
use outreach_canon::{Error, Result, parse, workflow};
use std::io::{self, Read};

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let vectors = parse(&input)?;
    let vectors = vectors
        .as_array()
        .ok_or_else(|| Error::new("Expected vectors"))?;
    for vector in vectors {
        let text = |key: &str| -> Result<&str> {
            vector[key]
                .as_str()
                .ok_or_else(|| Error::new(format!("Missing {key}")))
        };
        let kind = text("kind")?;
        let actual = workflow(kind, false, |path| {
            Ok(vector["files"][path].as_str().map(str::to_owned))
        })?;
        if actual != text("digest")? {
            return Err(Error::new(format!("Live workflow hash mismatch: {kind}")).into());
        }
    }
    println!("Live Python workflow hashes: {} matched", vectors.len());
    Ok(())
}
