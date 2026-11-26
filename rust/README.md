# Rust engine: byte contracts, offline signing and ingestion replay

`outreach-canon` implements Python's byte contracts; `outreach-model` supplies
ordered, lossless read models; `outreach-ledger` opens closed SQLite snapshots
with `mode=ro&immutable=1`. `outreach-core` builds operator views from supplied
values, and `outreach-cli` exposes `outreach control status --compact`, `reviews`,
`review-digest`, and `exceptions`. Python remains the only ledger writer. These
commands perform local reads and never download Releases, initialize a database,
run migrations, stage inputs, or perform external effects.

Phase 2 adds local envelope builders and an in-memory replay of 13 supported ingestion
families, including exact operator-signed resets of parked hosted reviews.
The SQLite adapter still has no writer. Python under `managed/` is
unchanged and remains the only production ledger writer. No Rust command uploads
to a Release, edits GitHub, sends mail, or runs a coordinator tick.

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
python3.12 vectors/generate.py --check
python3.12 -m unittest discover -s vectors
python3.12 check_live_workflows.py
python3.12 check_live_sql.py
python3.12 parity/compare.py
python3.12 parity/validators.py
python3.12 parity/signing_ingest.py
python3.12 parity/builders.py
cargo run --locked -p outreach-cli -- --help
```

Use **CPython 3.12** for generators, parity and Python suites; CI pins the same
minor version. Its `unicodedata.unidata_version` is **15.0.0** (not 15.1).
`unicode_vectors.py` records every code point's category/`str.isprintable()` range
and every changed casefold, and generates the Rust tables. Phase 2 also generates
CPython's lowercase, cased and case-ignorable tables and alphanumeric ranges.
Mailbox and packet address lowercase uses those tables, including final sigma.
Rust tests check all
1,112,064 Unicode scalars plus Python `repr` at every category-range boundary.
Full casefold mappings include unchanged scalars implicitly, so newer Rust Unicode
assignments cannot change this contract. Newer Python releases can change these
vectors; the generator rejects a Unicode version mismatch instead of refreshing
silently. Updating this pin requires regenerating and reviewing both tables and
vectors. The separate NFKC registry helper still uses the pinned
`unicode-normalization` crate's Unicode tables. Thread stages
and decision labels that call Rust `to_lowercase`/`to_uppercase` still follow
the Rust standard library's Unicode tables, rather than CPython 3.12's tables.
These operations can differ from Python for characters assigned or changed
after Unicode 15.0, and are outside the exhaustive casefold/nonprintable parity
claim. Pinning them to CPython would require generating and reviewing
upper/NFKC tables from the selected interpreter.

Canon tests read committed JSON directly; ledger tests invoke Python to create
a temporary synthetic database through the existing Python writer. No private
inputs or current source snapshots enter committed vectors.

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
  draft revision, submission review and Mathesis delivery. Decisions include both
  answer maps and explicit `close` actions. `sign_review` also
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
| Unicode text | Exhaustive | All Unicode categories, printable ranges, repr boundaries and all changed casefold mappings from CPython 3.12 |
| Workflow formula | 298 | All seven kinds plus Python's fallback; synthetic text, each input changed/missing, automation scoping/reformatting and historical voice omission |

The Python test regenerates every vector in memory and compares the complete file
bytes. Separately, on every CI run, `check_live_workflows.py` calls the real
`control.workflow` against the current public `managed/` tree, captures the actual
file inputs and expected digest, and streams them to the Rust `check_workflows`
example. It checks all seven kinds and the fallback without reading a private
policy overlay or saving source snapshots. Changes to workflow text never require
regenerating committed vectors; changes to the hashing formula require a Rust
adaptation and may require new synthetic cases. CI runs on changes to `rust/`,
`managed/`, `scripts/short_notes/` (imported by publication), or its own workflow. To refresh after an intentional byte-contract change:

```sh
python3.12 vectors/generate.py
python3.12 vectors/generate.py --check
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

`--root` defaults to `OUTREACH_DATA_ROOT`, then the engine root. Config comes
from `root/config.json`, with `private-data/config.json` used only for a local
engine checkout lacking the root file, matching `location.load_config`. Owner
labels use that same config. Workflow inputs come from the current public
`managed/` tree and the data root's `policy/managed/` overlay; Python source files
always come from the engine. Hashes are read when the matching view needs them:
owner-draft reply/firstcontact rows in exceptions, and each eligible review in the
digest. Unlisted/missing digest kinds use Python's research-source fallback.
Missing digest workflow files suppress that item's change flag; missing required
owner-draft workflow files fail the command, matching Python.

`--engine-root` or `OUTREACH_ENGINE_ROOT` selects the public source tree. The
default is discovered from the current directory's ancestors at runtime; an
installed binary outside a checkout requires an explicit tree. No build-machine
path is compiled into the default. `--now UNIX_SECONDS` injects the status clock.
Use a closed snapshot: `Ledger::open` refuses existing `-wal` or `-journal` files,
since immutable SQLite ignores pending changes in them. This is a deliberate
stricter gate than the offline Python adapter. An existing `-shm` file is left
untouched. The reader has no public connection/SQL escape hatch or write API.

`review-digest` requires `--work` and defaults to **text**, matching Python. It
does not synthesize the much broader `automation.work` inventory. `exceptions`
accepts the same optional work file and defaults to compact **JSON**; without
`--work`, it uses the snapshot's tasks. `reviews` preserves request/result field
order, original row arrival order, review eligibility and event revision binding.
Unknown current task kinds/states fail closed; historical event states remain
opaque strings, as in Python. The model includes all 7 live kinds and
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

`python3.12 parity/compare.py` builds temporary SQLite fixtures with the live
`Tasks` writer and test request helper. It reuses every synthetic work/table
snapshot exercised by `managed/tests/test_exceptions.py`. The original suite
checks **8 command variants × 61 fixtures = 488 successful byte comparisons**.
`parity/review_findings.json` adds named, committed synthetic reproducers from the
independent review (**62 fixtures, 139 comparisons, including 87 expected
failures**), including all five inputs from the earlier supplied generator,
the round-3 `threademail`, `nodecisions` and 200,000-event probes, missing
workflow inputs, registry fallbacks, mailbox lower/casefold distinctions, invalid
configurations, malformed affair addresses, raw review marker values and
historical event states. The harness compares successful
stdout bytes and separately requires both implementations to fail for negative
fixtures; Rust must return a nonzero exit other than 101, with no panic or signal.
It hashes the snapshot and all preexisting side-file contents before and after.
All generated databases and command outputs remain in a TemporaryDirectory or
captured subprocess output and are removed. No file under `managed/` is changed.

`check_live_sql.py` captures SQL emitted by live `Tasks.lane_rows`,
`Tasks.owner_questions`, `Tasks.counts` (both columns), and `pipeline.status`
(both pipeline counts and source selection), then
compares it with the exact frozen read queries used by Rust, ignoring formatting
whitespace. CI runs this alongside the live workflow check. The lane query now
returns the same columns as Python; request/data cross the adapter individually,
without a surrounding SQLite JSON re-serialization.

For an operator's private snapshot, run locally:

```sh
python3.12 rust/compare_private_snapshot.py /path/snapshot.sqlite3 --root /path/private-data
# Add --work /path/work.json for the digest and work-file exceptions variants.
```

This script uses the same comparison code. It prints only counts, exit codes,
stdout lengths and mismatch offsets, never private output or diffs; it saves no
results and checks the snapshot bytes/sidecars. Without a work file it compares
four variants (status, reviews, JSON exceptions and text exceptions). It does not
look for, download, or copy private snapshots. Private parity is operator-run and
is not a claimed validation of this public slice.

rusqlite is pinned to 0.41.1 with **bundled SQLite 3.54.1** and default
features disabled; Python links to its interpreter/platform SQLite, whose version
can differ (this machine's CPython 3.12 also reports 3.54.1). Parity tests use both
engines but do not claim support for every older system SQLite version. The
benchmark prints Python's SQLite version so its measurements can be interpreted.
The unused cache/WebAssembly feature tree is excluded. Ordered serde JSON needs
the permissive Zlib license (foldhash); license/advisory/source/duplicate checks
remain enabled without exceptions. Clippy bans `serde_json::Map::remove` because
it swaps entries with `preserve_order`; use `shift_remove`. Architecture checks
also forbid `str::parse::<Value>()`, including re-exported/renamed `Value`.
The 512-container / 4,300-integer-digit bounds remain unchanged. Reader tests
verify SQLite rejects write SQL and refuses missing snapshots and pending side
files, with all bytes preserved.

## Local performance benchmark

Run `python3.12 benchmark.py` from `rust/`. It builds release, measures the
baseline synthetic ledger (2,000 tasks by default) and an event-heavy ledger
(5,000 tasks, 40 events per task, 200,000 events total, with 100 reviewable
tasks), and measures three independent subprocess runs per command and
implementation, alternating execution order. Time is the median wall time; RSS
is the largest child peak from `wait4`, excluding the parent and fixture builder.
Stdout hashes must agree; time and peak RSS must both be at most 2× Python. Use
`--no-event-case` only when a quick baseline run is needed. This benchmark is
intentionally outside CI, and removes its database and output files on
completion.

On Darwin arm64 / CPython 3.12.13 / SQLite 3.54.1:

| Case / command | Python seconds / peak MiB | Rust release seconds / peak MiB | Rust/Python time / RSS |
| --- | ---: | ---: | ---: |
| baseline / exceptions | 1.108 / 307.1 | 0.433 / 420.3 | 0.39× / 1.37× |
| baseline / reviews | 2.685 / 647.9 | 0.818 / 526.2 | 0.30× / 0.81× |
| 200,000 events / reviews | 1.057 / 53.0 | 0.080 / 32.4 | 0.08× / 0.61× |

The task adapter parses each request/data column once through canon and moves it
into the view. Exceptions coalesces work sections by ownership, then builds its
children map once. Reviews filters by reviewability first, then scans events once
in SQLite sequence order and compares borrowed event text without parsing or
retaining historical packet payloads. Only event summaries for reviewable tasks
cross into core.

## Offline signing, stage files and ingestion replay

`outreach-core::envelopes` supplies lossless typed review, decision (answers or
close), draft-revision and request envelopes. Admission validators keep Python's
field/signature/semantic gate order. `transitions` implements review binding,
verdict, decision and draft revision without I/O. Binding comparison checks all
four fields and refuses extra/missing fields, with Python numeric equality for
the revision. HMAC comparison remains in canon through `subtle::ConstantTimeEq`.
An edited review returns the exact replacement packet bytes as a required
preservation effect; the offline adapter keeps the effect local.

`ingest::Replay` owns its model, applies each admission transaction atomically,
and models the forced-save generation without creating a snapshot. Transactions
journal appended lengths and replaced rows, then undo in reverse order on failure;
they do not clone the tasks, history, control or pipeline tables per asset. It reproduces
prefix/name ordering, per-asset receipt identity, receipt pruning, digest checks,
missing-key skips, duplicate control events, one failure per digest per pass,
three-strike give-up and generation-bound receipts. The handlers cover requests,
local results, submission reviews, reviews, decisions, draft revisions, Mathesis,
prompt requeues, failed requeues, pool adoption and the two backfill families.
Source, ownership, packet, continuation and sent-note revision checks use supplied
rows/configuration and local recorded artifact bytes. Unknown model JSON stays
lossless. The immutable adapter exports every persistent table, including the
eight queue tables. Replay restores task migrations in memory and emits rows
sorted by primary key, with canonical JSON columns. Known numeric time fields
are replaced with their global order ranks, preserving ties and cross-table order.

The key comes from `OUTREACH_REVIEW_KEY`, `--key-file`,
`OUTREACH_REVIEW_KEY_FILE`, or Python's default local key path. File keys use
Python whitespace stripping, including `\x1c`–`\x1f`. Keys never enter output.

```sh
# Python sign-review output format: indent=2, original key order, UTF-8, newline.
rust/target/debug/outreach control sign-review unsigned.json --kind reply --output signed.json

# Stage bytes: canonical JSON without a newline; no Release upload.
mkdir -p /tmp/outreach-inputs
rust/target/debug/outreach control review signed.json --snapshot /path/snapshot.sqlite3 --output /tmp/outreach-inputs
rust/target/debug/outreach control decision decision.json --output /tmp/outreach-inputs
rust/target/debug/outreach control stage-draft-revision --snapshot /path/snapshot.sqlite3 \
  --task TASK --packet replacement.json --reason 'Checked correction' --output /tmp/outreach-inputs
rust/target/debug/outreach control enqueue request.json --root /path/data --output /tmp/outreach-inputs

rust/target/debug/outreach control replay-ingest --snapshot /path/snapshot.sqlite3 \
  --inputs /path/input-window --root /path/data --generation 100 --now 2000000000
```

`review` requires `--snapshot` and runs the coordinator verdict preflight on an
owned copy before staging the supplied signed envelope. Unsigned, stale or
otherwise rejected reviews produce no output file. `decision` stages exactly the
supplied value, including an unsigned envelope, without requiring a key or running
an admission preflight. A supplied snapshot remains protected from output writes.
`draft-revision` also stages the supplied envelope; its optional snapshot preflights
admission. Only `sign-review` signs a supplied envelope; it preserves Python's
narrower signing checks and automatic workflow rebind. The `stage-*` selection
builders construct their signed envelopes from the selected task and inputs.
`enqueue` also checks pinned local Git workflow inputs. `stage-thread`, `stage-inbound`, `stage-submission`,
`stage-mathesis` and `request-submission-review` use Python's selection arguments,
plus `--root`, `--output` and a snapshot for selections that need existing tasks.
Thread/submission builders resolve local Git pins; optional `--revision` and
`--policy-revision` select explicit already-present commits. Submission staging
copies the explicitly selected input bytes to the output directory's
`resources/<release-tag>/<name>` tree for later operator handling. It performs no
upload or readback against a service. Mathesis selection checks the local handoff
and produces its ordinary or collection envelope without delivering it.

The replay input directory contains control input JSON files. An optional
`assets.json` list supplies original Release metadata (`id`, `name`, `digest`),
including intentionally wrong digests; otherwise metadata uses the filename as
ID and the actual file hash. Local-result artifact readbacks require recorded
bytes at `resources/<release-tag>/<name>`. Absent resources are reported as
rejections; offline replay cannot establish a remote readback without its bytes.
Paths are confined to the supplied directory. `--passes` repeats the window on
the same owned model, including idempotence, give-up and generation behavior;
`--no-key` tests the production skip/refusal paths. Replay stdout can contain
private task data. All offline outputs use `create_new` by default, including
stage resources and signing output. `--overwrite` explicitly permits replacing an
existing output file. Outputs equal to the supplied snapshot, including symlink,
hard-link and parent-directory aliases, are always refused, even with that flag.

Input assets use a separate decoder matching `Path.read_text` and `json.loads`:
UTF-8 errors include the byte/range and reason, universal newlines are normalized,
and JSON syntax errors report Python's message and Unicode line/column/character
positions. Nonfinite numeric tokens reach the ordinary field/signature gates;
canonical encoding still refuses them. Persisted non-object `ingest_rejected`
values and task data return errors rather than panicking.

For an operator's real window, the local comparison script prints **counts only**:

```sh
python3.12 rust/replay_private_inputs.py /path/snapshot.sqlite3 /path/input-window \
  --root /path/private-data --generation 100 --now 2000000000 --key-file /path/review.key
```

It opens the original read-only, runs the live Python `Ledger.ingest` on a separate
temporary backup, runs Rust on its owned model, and compares canonical logical
dumps, errors and generations. Only file/policy/clock/Release ports are replaced;
Python's validators and transition methods run unchanged. It checks snapshot and
sidecar hashes before/after, saves no dump, downloads nothing and cleans its
temporary copy. The caller supplies the starting generation, clock, assets and
required resource bytes; no private window is discovered automatically.

The synthetic differential suites exercise 4,572 request/packet/source-gap/time
cases (4,048 identical rejections), 242 byte-equal signing outputs, 13 supported ingestion
families across 378 replay cases / 1,509 passes (654 matching rejections), and
17 byte-equal stage selections/resources subsequently accepted by Python.
Replay covers PASS/BOUNCE/HOLD, answer and close decisions, wrong states, stale
bindings, duplicate events, reply/firstcontact edits and edit refusals, agent-held
and parked results, owner/mail workflow rebinds, invalid signature types, all
eight queue tables, no-key ingestion and repeated rejection/give-up. Coverage
also includes basic/week ISO dates, offsets and fractional truncation. Reply
supersession compares integer UTC microseconds, so a one-microsecond difference
does not disappear when converting a timestamp to a float. Generators
use the live Python code with fixed random seeds. `validators.py` compares error
strings as well as acceptance. Comparison uses canonical bytes, so Python's
`True == 1 == 1.0` cannot hide a changed JSON type in a stored envelope.
Round-two regressions in `parity/ingestion_regressions.py` additionally check
32 committed malformed-asset fixtures across four passes, nonfinite and oversized
number gates, partial-batch rollback, command contracts, output aliases and
explicit overwrite, corrupted ledger values, and a fresh private-script process
with root-bound defaults and an explicit key file. These run inside the existing
signing/ingestion CI step. Rust must never panic for a negative case.
CI runs all three new suites alongside
the existing architecture, SQL, workflow, vector, fmt, clippy, test and deny gates.

The local synthetic transaction benchmark is reproducible with
`python3.12 rust/benchmark_ingest.py`. It uses 2,000 tasks with about 64 MB of
payloads, 100,000 historical events and 100 signed recovery assets. Each mode runs
three times in a fresh process and must produce the same final table digest.
On Darwin arm64, median process wall time / peak RSS was **1.465 s / 307.0 MiB**
with row journaling and **7.600 s / 524.5 MiB** when emulating the removed
five-table copy around those same transitions. These measurements include
synthetic model construction and final hashing; the baseline mode also retains
the new journal and adds back the old copy cost. This is a local measurement,
not a CI timing gate.

## Remaining boundaries and phase 3

- Production ingestion, durable forced-save intent boundaries, reconciliation,
  Release upload/download and all live ledger writes remain Python-owned.
  Live Rust network adapters, the other coordinator/delivery/publication stages
  and cutover belong to later phases. Phase 3a adds opt-in Python recording and
  offline Rust Mathesis delivery; it does not enable production replay. The local replay does not confer
  production write authority.
- Other phase-1 operator inventories (`work`, full `status`, `todo`) and network
  Release restore are not exposed by this CLI. The current work/status builders
  were read for their command contracts; the full automation inventory remains
  Python-owned.
- Strict canon parsing rejects nonfinite values before duplicate replacement;
  ingestion decoding follows Python's last-value replacement, so
  `{"a":NaN,"a":1}` decodes to `{"a":1}`. Nonfinite values cannot be canonically
  encoded or signed. Lone surrogates remain outside canon's representable domain.
- Python accepts 513 nested arrays; Rust deliberately rejects beyond 512.
  Registry affiliation regexes use Rust's regex syntax; unsupported Python regex
  constructs produce an explicit error rather than silently skipping a gate.
- `Ledger::tasks()` is the single full-task read path; `task_values()` has been
  removed. It returns typed `Task`/`Request`/`TaskData`, which the current view
  boundary consumes into ordered values without cloning payloads. Partial work
  views retain their permissive projection boundary; full request validation would
  reject some partial inputs that Python intentionally accepts here.
- Ledger and CLI now use error enums (`Io`, SQL/ledger, canon data, and argument or
  snapshot errors), not `Box<dyn Error>`. Core uses canon's typed data error. Rust
  reports command failures with exit 2. Ingestion rejection text is compared with
  Python wherever it is retained in the logical dump or returned error list.
  Config capacities and revisions use signed 64-bit integers; Python can represent
  larger values, which Rust rejects explicitly. Typed ledger reads also reject
  malformed/missing request IDs and current discriminants earlier than Python.
- The two-day owner-question policy is in core. Pipeline scenario IDs remain in
  the frozen SQL projection because it must track the live Python query exactly;
  moving that unrelated status selector into typed core policy remains later work with a
  corresponding live-query parity check.


## Offline stages and recording (phases 3a–3b)

Python remains the only production ledger writer. Rust ports Mathesis delivery,
publication `advance`, and sent-event reconciliation/status recording behind typed
service traits. No Rust network adapter, production replay, uploader or cutover
is enabled. Phase 3c adds the CMA pool and reviewed mail delivery; Python retains all production write authority.

`../scenarios/README.md` describes the **1,904 Python-first scenarios** and their
source-test map. Rust matches **1,896**, with eight intake cases skipped. Run:

```sh
python3.12 ../scenarios/runner.py
python3.12 scenarios.py
python3.12 -m unittest discover -s ../scenarios -p 'test_*.py'
```

`persistence::Ledger` seals the persistence port. Only that wrapper can acknowledge
an immutable saved snapshot; permit constructors are crate-private. A permit
contains the committed generation and snapshot digest, and the wrapper separately
checks that its logical input still matches the saved input. Every external write
consumes its typed `Persisted<I>`, including an uncertain call. Neither cloning
nor deserialization can reconstruct a permit. Four compile-fail doctests cover
construction, a raw-intent write, an external persistence implementation and reuse
after consumption. A unit test checks changed-state and failed-save refusal.
Scenario fakes check the actual committed digest/generation on every write.

Publication uses typed GitHub Release and short-note service ports. It preserves
standing decisions, exact PASS bindings, exclusions, retry schedules, priority,
success caps, immutable publication requests, Mathesis history and signed
selection ingestion. A note revision queued by a non-Mathesis row takes the
delivery-only path; its signed delivery envelope can recover the final queued
record after a lost save or interruption. The package service owns filesystem,
metadata/PDF/ZIP validation and preparation; extraction runs the real Python
validators, while Rust signs and ingests the resulting selection independently.
Live Rust implementations of these adapter contracts remain later work.

Sent events use a typed metadata-only mailbox and a contact-rendering/persistence
port. Matching uses exact receipt IDs, never subjects or bodies. Paging, aliases,
manual-event tolerance and deduplication run in core. After rendering, a page with
new events, scoped table changes or unpushed local commits gets a forced-save
`sent-events-pending` intent before the table commit. Clean pages use only the
ordinary debounced cursor save. The intent's contact-table digest binds Rust's
typed permit for parity; Python recovery reads only the cursor and deduplicates
against the actual contact rows. Recovery reuses the frozen `until`, including a
retried first page without a pagination link. A changed `live_since` immediately
deletes the stale intent, even if page admission or fetching fails. The cursor
advances only after persistence succeeds or no push is needed. The coordinator
entry point retains a content-free status.

Recording is off unless `OUTREACH_RECORD_DIR` names a private directory outside
the engine checkout. Use a new directory per run. Python writes locked canonical
JSONL to mode-0600 `calls-PID.jsonl`; recording failures cannot change a completed
effect. Transport records preserve ordered requests, body digests, original keys,
statuses, read responses, write receipts, exceptions, admission decisions and
committed Release pointers. Write bodies are digests. Multipart identity ignores
the random MIME boundary. There is no automatic upload or retention job;
`managed.recording.pending_uploads()` is an integration hook only.

Version-2 stage frames never embed a ledger dump. They contain streaming canonical
logical table digests, timestamps/admissions, the ordered service observations,
bounded row deltas and only opened data/policy files. A row-hash index of the
closed start snapshot supplies unchanged rows; each boundary delta observes
changes by earlier unported stages. After a supported frame, the next delta
is relative to that frame's final rows. Hashes verify both the delta's base and
its complete resulting logical state. They do not traverse `tables/` or `managed/`.
Only the rendering service's three table output files are captured. Each complete
stage event, including its JSONL newline, is limited to **1 MiB**; an individual
opened file is limited to **512 KiB**. A larger or incomplete capture emits a small
`<stage>_stage_unavailable` digest record and replay counts it as skipped. A test
uses 300 synthetic payload rows plus an unrelated 2 MiB file and verifies a bounded
frame without ledger rows or the unopened file. Another tests the overflow path.
The existing private transport records are separate from this stage-frame ceiling.

Redaction shares `run_log.SECRETS` and its environment-key registry. It masks
plain, URL, JSON, base64 and hex token encodings; credential fields, credential
URLs and generic CLI credential-header strings; and credential echoes in read
stdout and encoded files. Signed receipt HMACs remain verification material.
Redacted frames are skipped. Recordings remain private correspondence/research
data after credential masking.

The local command supports all five ported stages and prints **counts only**:

```sh
cargo build --locked --offline -p outreach-core --example scenarios
python3.12 replay_mathesis.py /private/run-recording /private/start.sqlite3 --key-file /private/review-key
```

The historical command name remains compatible. The closed snapshot is opened
read-only/immutable and hashed before and after. Its start pointer and logical
digests must match; no key or input is discovered automatically. The key travels
to the local runner in memory. Sent replay needs no signing key. Consecutive
supported frames use the preceding replay's owned logical rows plus the recorded
boundary delta. Unported stages are observations at those boundaries; their own
logic and external calls are not executed by Rust. Full tick execution remains phase 3d/later work.
Subprocess output is captured, so only `equal`/`different`/`skipped` counts escape.
Missing keys, redaction and unavailable captures are skipped; malformed evidence,
snapshot mismatches and unequal outcomes fail with `different`. No dump, upload,
coordinator tick or live service call is performed.


## CMA pool and reviewed mail (phase 3c)

`outreach-core::sandbox_pool` owns pool policy, lifecycle intent keys and group
retention. Its typed CMA port requires snapshot-bound permits for lifecycle and
retirement writes. Placement counts only accumulate as telemetry. A group's last
healthy Sandbox remains available across empty or paused ticks; idle retirement
of surplus capacity considers queued, active and pending-review work without
reading admission switches. Cleanup retains quota evidence, ownership, the
per-tick Agent allowance and operator checks. The existing CMA adapter owns the
bounded stop-before-delete protocol; its native tests remain in the Python suite.

`outreach-core::delivery` owns reviewed routing, owner gates, recipient conflicts,
queue reservations, send boundaries, receipts, retries, owner-sent observation,
draft replacement and stale-draft recovery. Draft and send permits have distinct
intent types. The mailbox port supplies fresh Graph preflight and readback;
`delivery::mathesis` also ports the once-per-correspondent policy for reuse by a
future Graph adapter. Registry registration/rendering remains a separate service
with checked evidence and compared files.

The sealed ledger can restore a capability from an independently verified saved
snapshot observation. Its stored logical digest is distinct from the immutable
snapshot SHA; changed input cannot reuse that capability. An unsuccessful save
cannot replace the earlier receipt. Compile-fail tests cover constructing or
reusing permits and passing an owner-draft permit to automatic submission.

There are 1,366 pool operation cases and 294 mail cases, extracted from 298
additional native tests. See `../scenarios/test-map.json` for every invocation and
`../scenarios/README.md` for native coverage and adapter boundaries. No expected
ledger output drives an adapter. Python passes the cases before Rust comparison.

Opt-in recording covers both stages with the existing 1 MiB frame ceiling and
streaming logical fingerprints. Pool frames retain bounded runtime inputs and
explicit group order; delivery frames retain task and queue clock observations.
A restored receipt is bound to the independently fingerprinted saved input.
The local replay command accepts both stages without a signing key and preserves
consecutive frame state. Its public output is `equal`/`different`/`skipped`
counts. No Rust network client, production ledger writer or cutover is enabled.

Phase 3d still owns mailbox intake (including the eight deferred scenarios),
advance/admission, pipeline and continuations, issue filing and maintenance.
Production switch-over, live adapters and rollback acceptance remain later gates.


## Phase 3d: mailbox intake and scientific issues

`outreach-core::mailbox` supplies a typed read port, Inbox intake, explicit
inbound hydration, coverage checks, Sent-subject cache transitions, owner and
refusal gates, and immutable attachment input preservation. Staged request
building and coverage reuse phase 2; task insertion reuses its supersession and
carried-answer transitions. The scenario adapters never open Graph or write a
production ledger.

`formalization_requests` and `prior_work_issues` share the typed `GitHubIssues`
port. Every issue POST consumes a single-use permit bound to the acknowledged
snapshot. Complete inventory, closed-issue reconciliation, uncertain outcomes,
backoff, bounded/deferred work and exact reviewed evidence all run in core.
Scientific evidence projection and local previews are independent Rust logic;
Python remains the production writer and service adapter.

All **2,203 scenarios** pass through Python before Rust, with equal ordered
external writes and final logical tables/files. This includes 262 phase-3d cases
from 137 source tests, plus 45 independent-review regression scenarios. See `../scenarios/README.md` and `test-map.json` for exact
conversion counts and native coverage retained by design. The Rust CI job runs
all stages and the local recording replay tests.

The local `replay_mathesis.py` check now accepts intake, inbound, formalization
and prior-work frames without a signing key. Its normal output remains counts
only. To preserve the existing recording scope, new intake/inbound frames containing Graph `body`, `uniqueBody`, `bodyPreview`,
attachment `contentBytes`, encoded `content_b64`, or standalone body/attachment
content emit a content-free unavailable record. Content-free frames replay normally. No coordinator workflow or live replay was enabled.

Phase 3e still needs advance/admission orchestration, pipeline and continuations,
maintenance, and the cutover switch with the remaining production composition
and rollback gates. Python remains the sole production ledger writer throughout
this branch.
