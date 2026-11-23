# Rust engine, first slice

This workspace implements the Python engine's byte contracts in `outreach-canon`.
`outreach-model` and `outreach-core` are compiling placeholders; `outreach-cli`
builds an `outreach` binary that prints `outreach 0.1.0`. Python remains the only
ledger writer. The Rust workspace performs no coordinator, delivery, publication,
or private-data operations.

The toolchain is pinned to the locally installed stable **Rust 1.95.0**. Dependencies
are centralized and pinned in `Cargo.toml`; `Cargo.lock` is checked in. Production
workspace dependencies follow `canon <- model <- core <- cli`, with core also
allowed to use canon directly for phase-2 signature verification, checked by
`check_architecture.py`. Canon remains a leaf and the graph is acyclic. All
packages forbid unsafe code. Clippy requires time through explicit inputs/ports
and bans `HashMap`/`HashSet`; use ordered collections such as `BTreeMap`/`BTreeSet`
or `IndexMap`/`IndexSet` when those dependencies are introduced.

## Run

From `rust/`:

```sh
cargo fmt --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo install cargo-deny --version 0.20.2 --locked
cargo deny --locked check
python3 check_architecture.py
python3 vectors/generate.py --check
python3 -m unittest discover -s vectors
python3 check_live_workflows.py
cargo run --locked -p outreach-cli
```

Python 3.12 or later and its standard library suffice for the generator and its
test. The checked-in vectors also reproduce under Python 3.15. Rust parity tests
read the checked-in JSON files directly and need no Python subprocess.

The dependency audit allows MIT, Apache-2.0 and BSD-3-Clause licenses and denies
advisories, yanked dependencies, duplicate dependency versions, wildcard versions,
and unknown registries/Git sources. Its only license exclusion is the four local
unpublished workspace packages: this repository has no declared license, and this
slice does not assign one. All third-party dependencies are audited. There are no
advisory exceptions.

## Byte parity

`outreach-canon` provides:

- `parse` and `canonical`: Python's
  `json.dumps(sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)`
  followed by UTF-8 encoding, within the domain and bounds below. Integers retain
  exact precision through CPython's default limit of **4,300 decimal digits**;
  the minus sign does not count. Both parsing and serialization reject larger
  integers, including an oversized value overwritten by a duplicate key.
  Floating-point
  literals round to IEEE binary64; Ryū supplies shortest round-trip digits, then
  the formatter applies Python's notation thresholds, exponent signs/padding,
  `.0` and negative zero. `1e16` becomes `1e+16`; `1e-5` becomes `1e-05`.
- `dumps` with `SerializeOptions`: explicit separators, string indentation,
  `ensure_ascii`, `sort_keys` and `allow_nan`. Default separators are `(', ', ': ')`
  without indentation and `(',', ': ')` with indentation. `Some("  ")` represents
  Python's `indent=2`. Parity covers finite JSON values, Unicode scalar strings
  and stored map order; original insertion order and nonfinite values are not
  represented by the current `Value`. In this supported finite domain,
  `allow_nan=True` and `False` give the same bytes. These options do **not** yet
  establish parity for every engine `json.dumps` call or read-only command output.
- `event_identity`: registry and Sent Items event IDs use sorted keys, raw Unicode,
  Python's default spaced separators and default `allow_nan`, rather than the
  compact canonical format. Vectors come from `registry.record_event` and
  `sent_events.record_page` with synthetic contacts/messages and stubbed I/O.
- `sha256` for raw bytes and `digest` for canonical JSON, which also supplies
  request/result/packet hashes and control event IDs. Helpers reproduce queue
  workflow digests, attempt keys, delivery tokens, registry identities, pipeline
  request IDs/source identities, staged asset names and artifact keys.
- Plain HMAC-SHA256 envelope signing, exact lowercase hex comparison using
  `subtle::ConstantTimeEq`, the Mathesis delivery purpose wrapper, and the exact
  pre-signature field gates for review, decision, pool adoption, failed requeue,
  draft revision, submission review and Mathesis delivery. `sign_review` also
  reproduces edit/rebind validation and automatic workflow rebind, given an
  explicitly resolved current workflow hash. These are cryptographic gates;
  task state, result bindings and permissions still require coordinator checks.
- `input_identity`: the canonical receipt tuple of Python-stringified asset ID,
  name and digest. Missing ID, missing digest, null and all scalar ID types are
  covered; GitHub asset IDs are integers or strings. Non-scalar IDs are rejected.
- `workflow`/`workflow_inputs`: the current scoped `control.workflow(kind)` formula,
  including raw file text, mail voice guide, runtime inclusion, submission policy,
  and selected automation sections. A reader callback supplies already-resolved
  UTF-8 text. Missing voice is allowed only when the caller explicitly identifies
  a historical source-only pin. Data overlay selection, hosted policy requirements,
  commit verification and `git show` remain the future adapter's responsibility.

Use `outreach_canon::parse` for incoming JSON. It avoids serde_json's reserved
arbitrary-precision number marker being interpreted as an ordinary object's value;
such object keys must remain literal data. Duplicate keys follow Python's last
value rule, and object sorting uses Unicode code-point order. Unicode normalization
is never applied. Unpaired surrogates cannot produce Python UTF-8 digest/signature
bytes and are rejected by the Rust parser; Python itself can temporarily hold them
in a `str` returned by `canonical`. NaN, infinities and overflowing floating-point
literals likewise produce no canonical bytes. Finite underflow preserves signed zero.

Both parsing and serialization use explicit stacks and accept at most **512
nested arrays/objects** (the root container counts as one). Deeper input returns
`Err`, including a deep value overwritten by a duplicate key. The walk visits
each token/value once without rescanning subtrees; object insertion/sorting has
the usual ordered-map/sort cost. This fixed bound deliberately diverges from
Python's interpreter-dependent `RecursionError` threshold; it prevents stack
overflow and quadratic subtree scanning.

Use `float(f64) -> Result<Value>` for every Rust-created floating-point value;
NaN and both infinities return `Err`. Incoming float literals also go through this
constructor after binary64 rounding. `serde_json::Value::from(f64::NAN)` loses
the NaN by converting it to `null`. Calling `canonical` on values not produced by
`parse` or the checked constructor is unsupported; engine helpers compose these
values with ordinary strings, booleans and integers. The future `CanonValue`
boundary will enforce this provenance at the type level.

## Golden vectors

`vectors/generate.py` imports the current Python engine. Its random seed and test
key are fixed and public. All vector inputs are synthetic; no current workflow
text or source-file SHA-256 receipts are embedded or pinned. It intercepts real Python
verification paths at `hmac.compare_digest`, before any ledger access or effect,
to record both the field gate and the comparison. Expected signatures, hashes,
review edits/rebinds and workflow hashes come from Python. The non-Mathesis
signature oracle calls the production `control.sign_review` byte signer with
review-specific validators stubbed for deliberately invalid mutation cases; each
family's unmodified verifier independently checks its HMAC and real field gate.
The attempt-key formula exists only inline in `Queue.dispatch`; the generator
calls that entry point on a fake queue and captures the actual intent INSERT,
stopping at its checkpoint before a transport can run. No credential, ledger,
private checkout or external service is used.

| Family | Vectors | Coverage |
| --- | ---: | --- |
| Canonical JSON + SHA-256 | 4,133 | 4,096 seeded random values; 4,300-digit signed integers, depth 512, raw UTF-8 including astral keys, controls, duplicate/reserved keys and rounding cases |
| Binary64 formatting | 15,848 | 10,000 seeded random bit patterns; decimal thresholds and neighbors across the finite exponent range, halfway cases, subnormals and signed zero |
| Signed envelopes + event digests | 168 | 24 synthetic examples in each of seven signature families |
| Signature verification | 2,952 | Real Python field gates; changed fields/keys, extra/missing fields, signature type/case/length and schema edge cases |
| `sign_review` | 216 | Every kind, current/changed workflow, optional body edits, automatic/explicit rebind and refusals |
| Identity helpers | 516 | Receipts, request/registry/source IDs, attempts, assets, artifacts, queue workflow digests and delivery tokens |
| Rejections | 16 | Nonfinite/overflowed floats, unpaired surrogate strings/keys (including raw UTF-8 mixed with escapes), 4,301-digit signed integers and overwritten oversized integers |
| Registry / Sent Items event identity | 48 | Real event builders and default spaced JSON identity bytes |
| Serializer options | 384 | Separators, indentation, ASCII escaping, sorting and allow_nan across the finite representable domain |
| Explicit depth bound | 1 | Python accepts depth 513; Rust deliberately rejects it |
| Workflow formula | 298 | All seven kinds plus Python's fallback; synthetic text, each input changed/missing, automation scoping/reformatting and historical voice omission |

The Python test regenerates every vector in memory and compares the complete file
bytes. Separately, on every CI run, `check_live_workflows.py` calls the real
`control.workflow` against the current public `managed/` tree, captures the actual
file inputs and expected digest, and streams them to the Rust `check_workflows`
example. It checks all seven kinds and the fallback without reading a private
policy overlay or saving source snapshots. Changes to workflow text never require
regenerating committed vectors; changes to the hashing formula require a Rust
adaptation and may require new synthetic cases. CI runs on changes to `rust/`,
`managed/`, or its own workflow. To refresh after an intentional byte-contract change:

```sh
python3 vectors/generate.py
python3 vectors/generate.py --check
cargo test --locked --workspace
```

Review the changed vectors together with the Python change and adapt Rust until
every vector matches. Do not normalize a mismatch away or change Python to fit
Rust. This slice covers the pure portion of design phases 0–1; ledger/adapters,
live recordings, read-only operational commands and cutover belong to later slices.

The isolated Rust workflow pins third-party actions to full commit SHAs, installs
the toolchain named by `rust-toolchain.toml`, and has a timeout and concurrency
group. It uses `cargo test`; the unused nextest configuration has been removed.
It adds no production workflow calls, publication steps, default-runner changes
or repository-wide CI policy changes.

## Known gaps

- Phase 2 needs order-preserving parse: first-occurrence key position, last value
  wins. The current map stores sorted keys. Although indentation formatting is
  covered for stored/sorted order, byte-identical read-only output still needs
  preserved order plus Python-compatible `indent=2` output (`control.py` uses
  `json.dumps(result, ensure_ascii=False, indent=2)`).
- Introduce a `CanonValue` deserialized through `RawValue` + `parse`, and forbid
  `serde_json::from_str` outside canon. This is a phase-2 type boundary, not an
  implemented guarantee of the current public `serde_json::Value` alias.
- Expose a public `python_eq` for binding comparisons (`1 == 1.0 == True`). The
  current private schema-one check does not establish general Python equality.
- Review-key whitespace stripping must match Rust `is_whitespace` plus
  `\x1c`–`\x1f`. Existing edited-body validation already accounts for those
  controls; the future key-loading adapter must do so too.
- For `{"a":NaN,"a":1}`, Python produces `{"a":1}`; Rust rejects at parse.
  The parser validates every encountered value before duplicate replacement.
  Python nonfinite values and lone-surrogate strings (even with
  `ensure_ascii=True`) remain outside this slice's representable value domain.
