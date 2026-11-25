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

There are **242 scenarios from 112 converted tests**. Python runs all 242; Rust
matches all **234 Mathesis, publication and sent-event scenarios**, and skips the
eight intake scenarios. The counts by source suite are:

| Source tests | Converted tests | Scenarios |
| --- | ---: | ---: |
| `test_mathesis_delivery.DeliveryTests` | 17 | 34 |
| `test_pre_reviewed.PreReviewedTests` | 13 | 21 |
| `test_publication.PublicationTests` | 48 | 142 |
| `test_sent_events.SentEventsTests` | 26 | 37 |
| `test_mailbox_triage.TriageTests` | 8 | 8 |

The publication count includes one new equal-priority arrival-order regression.
Original tests remain, including assertions about restart recovery and operator
views. Seven publication tests retain native coverage: the six coordinator or
Mathesis multi-pass scheduling tests and
`test_registration_uses_current_table_sha_and_preserves_existing_pause`, which
exercises the Contents API adapter. The two `CommitTablesTests` in
`test_sent_events` retain their real temporary-Git tests for scoped commits and
rebase-on-push-failure. All nine have empty entries in the map.

Each fixture supplies initial SQLite tables, configuration, a clock, data/policy
files, ordered adapter responses or exceptions, the expected external-write
sequence and final logical tables/files. JSON-valued columns contain JSON values.
Initial rows retain SQLite arrival order; final comparisons sort by primary key.
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
  `sent-events-pending` intent binds the contact-table digest before the table
  commit; the cursor advances only after success. Replayed pages still render
  and commit when deduplication adds no events.
- Legacy/manual sent events use an inclusive ten-minute tolerance; exact events
  from this stage distinguish separate messages. Aliases share a person, but
  explicit recipient evidence remains address-specific.

Mathesis cases continue to cover ordinary and collection delivery, receipt
recovery, version conflicts, revision pins, exact public-file readback, reading
links, collection-before-withdrawal, checkpoint failure and legacy migration.
The eight intake cases remain Python-only for phase 3c.
