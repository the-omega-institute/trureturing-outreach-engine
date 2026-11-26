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

There are **1,904 scenarios from 412 converted tests**. Python runs all 1,904; Rust
matches all **1,896 Mathesis, publication, sent-event, CMA pool and mail delivery
scenarios**, and skips the eight intake scenarios. The counts by source suite are:

| Source tests | Converted tests | Scenarios |
| --- | ---: | ---: |
| `test_mathesis_delivery.DeliveryTests` | 17 | 34 |
| `test_pre_reviewed.PreReviewedTests` | 13 | 21 |
| `test_publication.PublicationTests` | 48 | 142 |
| `test_sent_events.SentEventsTests` | 28 | 39 |
| `test_mailbox_triage.TriageTests` | 8 | 8 |
| `test_sandbox_pool` | 97 | 888 |
| `test_sandbox_groups` | 45 | 313 |
| `test_pool_adopt` | 2 | 5 |
| `test_cleanup_recovery` | 15 | 160 |
| `test_automation` | 62 | 157 |
| `test_owner_sent` | 37 | 54 |
| `test_draft_revision` | 22 | 53 |
| `test_stale_owner_draft` | 8 | 13 |
| `test_thread_staging` | 2 | 2 |
| `test_thread_recipients` | 5 | 5 |
| `test_reviewer_edit` | 1 | 5 |
| `test_engine_split.MathesisOnceTest` | 2 | 5 |

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
The eight intake cases remain Python-only for phase 3d.


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
replacement as a Rust transition. Pure validation, transport, CLI, intake and
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
