# Offline stage scenarios

The committed cases contain synthetic data and language-independent observations.
`schema.json` describes fixtures (version 1) and bounded recording frames (version
2). `test-map.json` maps original test IDs to every extracted invocation; an empty
list identifies a test retained in its native Python suite.

From the engine root, using CPython 3.12:

```sh
python3.12 scenarios/runner.py
python3.12 rust/scenarios.py
python3.12 -m unittest discover -s scenarios -p 'test_*.py'
```

There are **2,203 scenarios from 541 mapped tests** (539 original converted tests
and two independent-review scenario groups). Python runs all 2,203;
Rust matches all **2,203**, including mailbox intake, staged inbound hydration,
formalization requests and prior-work issues. The counts by source suite are:

| Source tests | Converted tests | Scenarios |
| --- | ---: | ---: |
| `test_mathesis_delivery.DeliveryTests` | 17 | 34 |
| `test_pre_reviewed.PreReviewedTests` | 13 | 21 |
| `test_publication.PublicationTests` | 48 | 142 |
| `test_sent_events.SentEventsTests` | 28 | 39 |
| `test_mailbox_triage.TriageTests` | 46 | 64 |
| `test_bounces` | 7 | 10 |
| `test_mailbox_attachments` | 12 | 21 |
| `test_formalization_requests` | 11 | 37 |
| `test_prior_work_issues` | 16 | 55 |
| `test_sandbox_pool` | 97 | 888 |
| `test_sandbox_groups` | 45 | 313 |
| `test_pool_adopt` | 2 | 5 |
| `test_cleanup_recovery` | 15 | 160 |
| `test_automation` | 73 | 173 |
| `test_owner_sent` | 37 | 54 |
| `test_draft_revision` | 22 | 53 |
| `test_stale_owner_draft` | 8 | 13 |
| `test_thread_staging` | 34 | 61 |
| `test_thread_recipients` | 5 | 5 |
| `test_reviewer_edit` | 1 | 5 |
| `test_engine_split.MathesisOnceTest` | 2 | 5 |
| Independent-review regressions | 2 | 45 |

The publication count includes one new equal-priority arrival-order regression.
Original tests remain, including assertions about restart recovery and operator
views. Seven publication tests retain native coverage: the six coordinator or
Mathesis multi-pass scheduling tests and
`test_registration_uses_current_table_sha_and_preserves_existing_pause`, which
exercises the Contents API adapter. The three `CommitTablesTests` in
`test_sent_events` retain their real temporary-Git tests for scoped commits,
rebase-on-push-failure and local persistence detection. Seven
`SentEventsRecoveryTests` retain native snapshot-upload and crash/restart checks.
Those 17 native tests have empty entries in the map.

Each fixture supplies initial SQLite tables, configuration, a clock, data/policy
files, ordered adapter responses or exceptions, the expected external-write
sequence and final logical tables/files. JSON-valued columns contain JSON values.
Initial rows and boundary deltas retain SQLite arrival order; final comparisons sort by primary key.
`sqlite_sequence` uses the table name as its logical key. Canonical byte comparison
preserves distinctions between booleans, integers and floats, including an
integer-valued injected clock in retry records.

Adapters use tagged bytes, files, directories, datetimes and callbacks. The
runners reject unused calls and mismatched arguments, even if a stage catches
the mismatch as a transport failure. Rust compares the calls it actually made
with the expected write sequence. Writes consume snapshot-bound permits, and
the fake checks their committed generation and snapshot digest. A failed or
unchanged save preserves the actual last committed receipt.

The short-note service supplies a validated scientific package: metadata,
canonical prepared file bytes, the source filename and its Lean-file observation.
Extraction executes the real Python package validators and exact artifact reads;
the independent Python runner and Rust fake then consume that service response.
GitHub registration, release reads, uploads and immutable-request readback remain
separate scripted calls. Rust builds and signs Mathesis selections independently.
Package/ZIP validation and filesystem materialization remain adapter contracts;
no production Rust adapter is enabled. Sent-event scenarios execute the real
contact selection and deduplication logic, then script the Python rendering
service with the calculated contact rows as checked input.

`boundary` observations inject failures at pure operations or immediately after
an enqueue. They contain no resulting ledger rows. Both implementations execute
their own transitions; expected final rows never drive a fake. This models tests
that deliberately stop before the final record or lose a response after a write.
`Stopped` is a terminal interruption, so it bypasses ordinary exception backoff.

Refresh intentionally with `python3.12 scenarios/extract.py`, then run Python
before Rust. Extraction keeps the original assertions and uses fixed clocks and
ZIP bytes. Do not refresh expectations to conceal a Rust difference.

The extraction made these Python contracts explicit:

- Publication `record()` passes `force=False` explicitly; other stages can call
  `save()` without that argument. An unchanged forced save can return `False`;
  only an exception denotes failure, and its original generation is retained.
- The initial short-note Release description is a prose marker, not JSON.
  An enqueue can save its immutable request and then lose its response.
- A revision queued by a non-Mathesis task still permits one Mathesis selection.
  That path performs no registration, staging, enqueue or request readback.
  A signed delivery envelope binding the revision and result recovers a lost
  final publication record. A different selection remains untouched.
- Retry clocks preserve JSON number types. New work precedes due retries;
  failures do not consume success slots. Earlier signed delivery handles and
  backoff remain intact across restoration.
- A repeated evidence read in one publication attempt reuses the exact local
  file, including when the prior-work check found no labeled section.
- Sent-event commit failures retain the original sweep/page handle. A saved
  `sent-events-pending` intent precedes a table commit when a page adds events,
  the scoped paths have Git changes, or local commits await pushing. Clean pages
  render and use a debounced cursor save without a forced snapshot or commit.
  The contact-table digest binds Rust's typed permit for parity; Python recovery
  reads only the cursor, retaining its frozen `until` even for the first page.
  A changed `live_since` deletes the stale intent before admission or page reads.
- Legacy/manual sent events use an inclusive ten-minute tolerance; exact events
  from this stage distinguish separate messages. Aliases share a person, but
  explicit recipient evidence remains address-specific.

Mathesis cases continue to cover ordinary and collection delivery, receipt
recovery, version conflicts, revision pins, exact public-file readback, reading
links, collection-before-withdrawal, checkpoint failure and legacy migration.
Intake and inbound staging now run in both languages.


Pool cases preserve the initial runtime as well as the ledger. A case executes
one constructor, health, idle, placement, ownership or capacity operation. Entry
arguments are rebound to the corresponding pool entry, preserving Python object
identity. Group order and idle configuration are explicit inputs; the expected
runtime is used only for comparison. Placement telemetry never governs admission
or retirement. A configured group retains its first observed healthy Sandbox;
surplus retirement waits for the whole group to be idle, independently of
admission switches. Capacity blocks, pending reviews, stalled runtimes, removed
groups, retained foreign work and operator acknowledgements are covered.

`CmaPool::retire_sandbox` is the existing CMA adapter boundary: its checkpointed
request includes previous cleanup state, known Agents, task ownership and the
bounded cleanup allowance. The adapter's stop-before-delete and original-handle
reconciliation remain covered by the native `test_sandbox_pool` and
`test_cleanup_recovery` assertions. No Rust live CMA adapter is enabled. The
five remaining adoption tests retain native signed-input/operator coverage;
phase-2 signing/ingestion parity also checks `pool-adopt`.

Mail cases execute the real task and transactional queue transitions. Mailbox
preflight supplies fresh Graph evidence; core checks owner gates, review binding,
routing, recipient reservations, draft conflicts, forced send-intent/send-call
checkpoints, exact receipts and bounded retries. Direct Mathesis policy cases
also check quoted text, incoming mail, earliest introduction and an earlier
introduction in a different conversation. Owner-sent matching and draft revision
or stale-draft recovery run independently in Rust. Contact registration/rendering
stays at the registry service boundary, with its evidence input checked and its
resulting files compared.

The native test
`test_owner_draft_receipts_intents_and_stale_conflicts_are_not_terminal` deliberately
replaces revision recovery; it retains native coverage instead of encoding that
replacement as a Rust transition. Pure validation, transport, CLI and
coordinator tests without a ported operation also keep native coverage. Runs with
more than 40 tasks keep their native backlog assertions; smaller operations from
the same test may still have mapped cases. Nonfinite constructor inputs remain
native because the canonical scenario format rejects them.

A restored revision can create from a previously committed intent without a new
save in that invocation. `saved_input` records the generation, immutable snapshot
SHA and independently verified input digest. The sealed ledger restores that
observation and refuses a permit if the mutable input has changed. This makes
Python's existing recovery contract explicit; no Python transition was changed.
Queue clock observations and task-update clocks are separate. All mailbox writes
and CMA lifecycle requests appear in the ordered write comparison, including
writes whose responses were lost. Fatal interruptions propagate without being
reclassified as ordinary retries.

Refresh all phases with `python3.12 scenarios/extract.py`, or only these stages
with `python3.12 scenarios/extract_pool_delivery.py`. Both retain the source test
assertions. The Rust runner accepts streaming `--jsonl` input so the combined
fixture set and consecutive bounded recording frames do not encounter canon's
per-document size limit. Normal runner and local replay output remains counts
only; `OUTREACH_SCENARIO_DEBUG=1` is an explicit local diagnostic mode.


Phase 3d adds **262 scenarios from 137 source tests** (including eight replaced
phase-3a intake fixtures and seven new issue-boundary regressions). The net increase
is 254 fixtures and 127 converted tests. `extract_intake_issues.py` refreshes only
these stages, retaining every original assertion. It captures request building,
coverage gates, hydration, scientific projections and previews as well as stage
invocations. Test IDs and operation IDs remain in `test-map.json`; the remaining
prior-work result-contract/policy test stays native. The alias fixtures replace
their invented `.co.uk` institutions with reserved `.invalid` domains before
execution, preserving distinct registrable domains and the source assertions.

The new stage counts are 111 intake; 59 inbound (38 hydration, 10 request
builders, 11 coverage gates); 37 formalization (12 advances and 25 helper calls);
and 55 prior-work (22 advances, 13 previews and 20 evidence projections/parses).
The six new native formalization tests cover failed checkpoints, incomplete or
malformed inventory, uncertain POSTs, invalid receipts, pause, the candidate
bound and same-tick deduplication. A seventh regression verifies that public
evidence excludes the installation's actual data repository, supplied explicitly
to the Rust projection rather than inferred from a repository name.

The scenarios fix these previously implicit Python contracts without changing
them:

- Equal received/sent timestamps select the first observation in adapter order.
  Registered messages and their attachment upload pass precede unregistered
  work. Deferred units retain their priority within each bounded candidate set.
- Active and finished reply coverage use different identity sets; research does
  not cover a reply. Parked tasks allow explicit restaging, and ingestion checks
  coverage again. Newer intake tasks reuse the existing supersession and carried
  answer transitions.
- Native Graph matching refreshes its Sent-subject cache through typed reads.
  The cache reuses its original `built_at` for less than a day, advances coverage
  incrementally, and rebuilds malformed entries. Fakes supply read responses,
  never resulting cache rows.
- Immutable attachment staging uses content-addressed assets and exact readback.
  Python performs no forced ledger save at that boundary; Rust preserves this
  existing staging contract. Issue creation separately consumes a snapshot-bound
  `Persisted<IssueIntent>` after a successful forced save.
- A complete all-state issue inventory is required before creation. Failed or
  uncertain effects keep the pending key and backoff; another creation during
  the same tick cannot use the stale inventory. Prior-work holds apply across
  every related note for the same result, including holds pinned to a sibling ID.
- Earlier public evidence must end strictly before the resolution date range.
  Overlap, missing context, private-source wording and unsigned legacy evidence
  remain visible pending work. Historical `private: 0` has the same public
  projection as Python's `False`; other truthy private markers stay private. A private `Kind:` introduced after public citation
  fields is an ambiguous evidence entry and raises `UnparseableEvidence`.
- Signed prior-work backfills store JSON string payloads. The offline loader now
  preserves those strings while retaining the queue's historical raw empty
  readback column. The production ledger loader is unchanged.

`replay_mathesis.py` also accepts the four new stages, with counts-only output and
no signing key requirement. New intake/hydration recording frames containing mail
bodies are deliberately unavailable; they emit only the existing bounded digest
and count record. Body-free frames replay normally, including native Graph
subject-cache observations. This does not broaden phases 3a–3c's existing mail
content recording. Recording remains inert without `OUTREACH_RECORD_DIR` and
adds no save, network call, or production tick work beyond the disabled hook
checks. `test_intake_issues.py` checks this boundary and snapshot immutability.

Round-two independent review adds **34 Python-first regression scenarios** in
`test_review_regressions.py`, with 13 intake, one inbound, 10 formalization and
10 prior-work cases. These are additional synthetic boundary cases derived from
the converted source tests; the 539 original converted-test count is unchanged.
Refresh only these cases with
`python3.12 scenarios/test_review_regressions.py --extract`.

The cases cover all four terminal interruptions at Sent matching, bounce
hydration and prior-context reads; eight refused budget units in each issue
stage; GET and uncertain POST failures from `URLError`, `JSONDecodeError`,
`ConnectionResetError` and a custom `ValueError` subclass; issue POST
interruptions; attachment ID changes that reuse the current immutable Release;
and equal Sent timestamps in both current and historical conversations.
Exception observations carry Python base classes so adapters can preserve
subclass handling, including custom classes. Rust compares both exception type
and exact message in intake, inbound and both issue stages. A separate negative
test alters only each stage's expected message and requires both runners to
reject it. Recording tests verify Graph body, unique-body, preview and attachment
content omission, standalone attachment responses, disabled hooks and replay of
issue exception ancestry.

Round-three review adds **11 Python-first regression scenarios** in
`test_review_round3.py`: four prior-work evidence cases and seven intake cases.
Each expected result was captured and replayed in Python, then independently
confirmed to differ on the round-two Rust before the fixes. The evidence cases
cover `NotImplementedError` with and without an HTTP status, a `GateError`
subclass, and an `UnparseableEvidence` subclass. Intake covers a custom exception
derived only from `BaseException` at Sent matching, bounce enrichment, prior
context, and attachment preservation, plus subclasses of both attachment holds
and Sent-subject deferral. Refresh only these cases with
`python3.12 scenarios/test_review_round3.py --extract`.

Rust uses recorded ancestry for these exception catches and propagates errors
outside Python's `Exception` hierarchy. Standard errors and the four original
interruption names retain their semantics when older fixtures omit ancestry.
Recording replay also verifies all four new evidence cases.
