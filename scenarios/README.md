# Stage scenarios, version 1

The committed JSON cases are synthetic and independent of a programming
language. `schema.json` describes the format; `test-map.json` maps each original
Python test ID to every extracted invocation. Python runs the production stage
with scripted adapters, real `Tasks` SQLite transitions, the real review and
source gates, and no network or subprocess access. Rust runs the same Mathesis
cases against its owned logical ledger and typed fake ports. Original tests
remain in place for their assertions about checkpoints, restart recovery and
operator views.

From the engine root, using CPython 3.12:

```sh
python3.12 scenarios/runner.py
python3.12 rust/scenarios.py
python3.12 -m unittest discover -s scenarios -p 'test_*.py'
```

The first batch contains **85 scenarios from 55 original tests**: 8 intake,
10 sent-record, 55 Mathesis delivery, and 12 publication invocations. The source
suites are `test_mailbox_triage.TriageTests` (8 tests),
`test_sent_events.SentEventsTests` (9),
`test_mathesis_delivery.DeliveryTests` (17),
`test_pre_reviewed.PreReviewedTests` (13), and
`test_publication.PublicationTests` (8). A multi-pass or parameterized test can
produce several scenarios. Each generated scenario is checked independently;
expected rows never drive a fake response.

Each case specifies initial SQLite tables (column names, primary key and rows),
configuration, a fixed clock, initial data/policy files, and ordered adapter
calls with explicit responses or exceptions. JSON-valued columns are JSON
values, not strings containing another JSON document. Initial row order retains
arrival order; final logical rows sort by primary key. `sqlite_sequence` uses
its unique table name as the logical key. The final comparison includes task
history, pipeline reservations, revision counters and sequence counters.

Adapter arguments use ordinary JSON, with explicit tagged values for bytes
(`{"$bytes":"BASE64"}`), files (`{"$file":"name","base64":"BASE64"}`),
directories (`{"$directory":true}`), datetimes (`{"$datetime":"ISO"}`),
callbacks, and database references. File names are basenames; directory
locations are temporary and have no semantic identity. A Release download
carries the returned file under `download`; the runner materializes that read
response. The write sequence records adapter, method and exact arguments.
`ledger.save` is a scripted persistence boundary, not an external publication:
a forced exception prevents the following write, and subsequent recovery uses
the original handles. The runners reject unused calls and mismatched arguments,
including mismatches that a stage catches as ordinary transport exceptions.

`extract.py` is a development tool. It invokes the named original tests and
retains their assertions while capturing each stage boundary. It replaces only
adapter observations and clocks; `runner.py` does not import the old tests.
Refresh intentionally with `python3.12 scenarios/extract.py`, then run both
runners. Review changes to expectations with the Python change. Do not refresh
expectations to conceal a Rust difference.

Several boundaries became explicit during extraction:

- A pending ordinary receipt can enter legacy migration before the normal
  delivery pass. Pausing disables migration and new writes, while allowing
  reconciliation of the saved receipt.
- A successful forced save can return `False` when identical state is already
  durable. Failure is an exception; the return boolean is not a write permit.
- Conflicts without a useful lookup still increment a task revision so later
  rows can enter the bounded delivery window.
- `INSERT OR REPLACE` changes SQLite row arrival order. Logical equality uses
  primary keys; arrival order remains significant for initial scheduling.
- Several old collection tests mocked the entire delivery gate and omitted the
  contact/refusal tables it reads. Extraction supplies explicit empty synthetic
  tables, and the independent Python runner executes the real gate. No private
  contact table or implicit local checkout is used.

The Mathesis cases cover ordinary delivery and private-receipt recovery,
version conflicts and revision pins, collection delivery and exact public-file
readback, reading-link queue recovery, legacy collection-before-withdrawal,
checkpoint failure, and upgrading an unattempted old selection. Publication,
intake and sent-record scenarios are Python-only until their stage ports land
in phase 3b; Rust reports those 30 cases as skipped.
