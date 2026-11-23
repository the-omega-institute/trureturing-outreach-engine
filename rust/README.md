# Rust engine: byte contracts and read-only snapshots

`outreach-canon` implements Python's byte contracts; `outreach-model` supplies
ordered, lossless read models; `outreach-ledger` opens closed SQLite snapshots
with `mode=ro&immutable=1`. `outreach-core` builds operator views from supplied
values, and `outreach-cli` exposes `outreach control status --compact`, `reviews`,
`review-digest`, and `exceptions`. Python remains the only ledger writer. These
commands perform local reads and never download Releases, initialize a database,
run migrations, stage inputs, or perform external effects.

The toolchain is pinned to the locally installed stable **Rust 1.95.0**. Dependencies
are centralized and pinned in `Cargo.toml`; `Cargo.lock` is checked in. Production
workspace dependencies follow `canon <- model <- core`; ledger depends only on
model and canon, and CLI composes them. `check_architecture.py` checks these
edges and forbids serde JSON decoders outside canon. Canon remains a leaf and
the graph is acyclic. All
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
python3 parity/compare.py
cargo run --locked -p outreach-cli -- --help
```

Python 3.12 or later and its standard library suffice for the generator and its
test. The checked-in vectors also reproduce under Python 3.15. Canon tests read
the checked-in JSON files directly; ledger tests invoke Python to create their
temporary synthetic database through the existing Python writer.

The dependency audit allows MIT, Apache-2.0, BSD-3-Clause and Zlib licenses and denies
advisories, yanked dependencies, duplicate dependency versions, wildcard versions,
and unknown registries/Git sources. Its only license exclusion is the five local
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
  and original insertion order. Duplicate keys keep their first position and
  last value. Nonfinite values remain outside the representable domain. Within
  that finite domain, `allow_nan=True` and `False` give the same bytes. The CLI
  uses Python's `indent=2` / `ensure_ascii=False` options and trailing newline;
  exceptions preserves Python's separate compact JSON/text renderer.
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
| Serializer options | 384 | Separators, indentation, ASCII escaping, sorting and allow_nan with original insertion order |
| Ordered parse | 3 | First-occurrence key position, last value wins, nested duplicates and reserved marker keys |
| Python equality | 1,550 | Bool/int/float equivalence, exact huge-int vs float comparisons, nested containers and key order |
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
Rust. The current slice adds immutable ledger reads and four read-only commands.
Network adapters, live recordings and cutover remain later work.

The isolated Rust workflow pins third-party actions to full commit SHAs, installs
the toolchain named by `rust-toolchain.toml`, and has a timeout and concurrency
group. It uses `cargo test`; the unused nextest configuration has been removed.
It adds no production workflow calls, publication steps, default-runner changes
or repository-wide CI policy changes.

## Snapshot commands and parity

From the repository root (or use `rust/target/debug/outreach` after building):

```sh
rust/target/debug/outreach control status --compact --snapshot /path/snapshot.sqlite3 --root /path/data
rust/target/debug/outreach control reviews --snapshot /path/snapshot.sqlite3 --root /path/data
rust/target/debug/outreach control exceptions --snapshot /path/snapshot.sqlite3 --root /path/data
rust/target/debug/outreach control review-digest --work /path/work.json --root /path/data --format json
```

`--root` defaults to `OUTREACH_DATA_ROOT`, then the engine root. Config comes from
`root/config.json` (or `private-data/config.json` in the engine checkout), with an
explicit `--config` override. Workflow inputs come from the current public
`managed/` tree and the data root's `policy/managed/` overlay; Python source files
always come from the engine. `--engine-root` selects another public source tree.
`--now UNIX_SECONDS` injects the status clock. Use a closed snapshot: immutable
SQLite deliberately ignores changes and WAL contents from a concurrently written
database. The reader has no public connection/SQL escape hatch or write API.

`review-digest` requires `--work` and defaults to **text**, matching Python. It
does not synthesize the much broader `automation.work` inventory. `exceptions`
accepts the same optional work file and defaults to compact **JSON**; without
`--work`, it uses the snapshot's tasks. `reviews` preserves request/result field
order, original row arrival order, review eligibility and event revision binding.
Unknown task kinds/states fail closed. The model includes all 7 live kinds and
26 live states, including owner-discarded and question-closed. Unknown task-data
fields and pipeline payloads stay as canon values; known pipeline keys/prefixes
are classified without imposing future payload schemas. TaskData round-trips the
exact canonical bytes, including arbitrary integers and unknown nested fields.

The former phase-2 canon gaps are implemented here: order-preserving parsing,
Python output separators, `CanonValue` deserialization through `RawValue` plus
the bounded canon parser, and public recursive `python_eq` (`1 == 1.0 == True`,
with exact integer/float comparisons). `CanonValue` exposes no mutable inner
value or conversion from an unchecked serde value. Programmatic output assembly
continues to use `Value`; incoming adapter/model JSON uses canon exclusively.

`python3 parity/compare.py` builds temporary SQLite fixtures with the live
`Tasks` writer and test request helper. It also reuses every synthetic work/table
snapshot exercised by `managed/tests/test_exceptions.py`. Extra scenarios cover
all states/kinds, grouped quotas, correspondence ancestry, pipeline contact gates,
Unicode digest text, workflow rebinding and review cleanup history. It runs the
real Python `_main` dispatcher with only Release restore replaced by an immutable
local reader, then compares stdout bytes against Rust. View builders and output
serialization are not stubbed or frozen. **8 command variants × 61 fixtures =
488 comparisons** currently pass. Every input snapshot is hashed before/after;
sidecar creation is checked. All fixtures and command outputs live only in a
TemporaryDirectory/captured subprocess output, and CI runs the same harness.
Changes in live Python state/kind lists and helper behavior are exercised on each
run. No file under `managed/` is changed.

For an operator's private snapshot, run locally:

```sh
python3 rust/compare_private_snapshot.py /tmp/claude-501/ledger-state-EXAMPLE.sqlite3 --root /path/private-data
# Add --work /path/work.json for the digest and work-file exceptions variants.
```

This script uses the same comparison code. It prints only counts, exit codes,
stdout lengths and mismatch offsets, never private output or diffs; it saves no
results and checks the snapshot bytes/sidecars. Without a work file it compares
four variants (status, reviews, JSON exceptions and text exceptions). It does not
look for, download, or copy private snapshots. Private parity is operator-run and
is not a claimed validation of this public slice.

rusqlite is pinned to 0.41.1 with bundled SQLite and default features disabled;
the unused cache/WebAssembly feature tree is excluded. Ordered serde JSON needs
the permissive Zlib license (foldhash); license/advisory/source/duplicate checks
remain enabled without exceptions. The 512-container / 4,300-integer-digit bounds
from the first slice remain unchanged. Tests also verify that SQLite refuses
writes/DDL, missing snapshots are not created, URI punctuation cannot override
read-only flags, and every requested table can be read.

## Known gaps and phase 2

- Signing and ingest commands remain Python: `sign-review`, `enqueue`, `decision`,
  and `stage-*`, along with the 12-family coordinator ingestion path, forced-save
  intent boundary, reconciliation and all ledger writes. Canon already supplies
  the pure signature contracts; the new read models do not confer write authority.
- Other phase-1 operator inventories (`work`, full `status`, `todo`) and network
  Release restore are not exposed by this CLI. The current work/status builders
  were read for their command contracts; the full automation inventory remains
  Python-owned.
- Review-key whitespace stripping must match Rust `is_whitespace` plus
  `\x1c`–`\x1f`. The future key-loading adapter still needs to apply that rule.
- For `{"a":NaN,"a":1}`, Python produces `{"a":1}`; Rust rejects at parse.
  Every encountered value is validated before duplicate replacement. Nonfinite
  values and lone surrogates remain outside canon's representable domain.
- Python accepts 513 nested arrays; Rust deliberately rejects beyond 512.
  Registry affiliation regexes use Rust's regex syntax; unsupported Python regex
  constructs produce an explicit error rather than silently skipping a gate.
