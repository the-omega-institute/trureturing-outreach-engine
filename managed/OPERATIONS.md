# Operations

The hosted coordinator and data writers share a concurrency group. Staged
requests and reviews are immutable Release assets. A review binds the workflow
hash and exact result. Force-save the original intent before an external effect
and reconcile uncertain outcomes through the original handle.

The hosted jobs use `OUTREACH_DATA_TOKEN` for private checkout and Release access.
Set `OUTREACH_DATA_REPO` and `OUTREACH_DATA_BRANCH` to select its writable
branch. Place the operational `config.json` and operative `policy/` in that checkout. Use
`managed/config.example.json` to see the configuration structure. Public job
output contains timings and statuses. Detailed logs are retained in the private
control Release. Pull request checks use fixtures and the example configuration.

Hosted feed synchronization and delivery share the disposable
`pages-feed-cache.json` asset in the private `outreach-control` Release. Each
job downloads it to a temporary directory outside the private data checkout
and replaces only that asset when its bytes change. Its input digest
covers the catalog, submission board, note receipts, manifest asset identities,
projection code and Mathesis configuration. Cached manifests are reused by
immutable asset identity, so changing one note downloads only that note's
manifest. Receipt bookkeeping and timestamps do not create content changes.
Every synchronization still verifies the projected content against the live
Pages index and checks for a pending feed swap before skipping deployment.
Verification requires the exact publication set after excluding independent
papers from Pages' authored catalog; extra feed papers, including removals
left by a failed build, keep dispatch retrying on later jobs. Release asset
contents and prior cache hits never establish deployment. A missing/corrupt
cache or missing asset identity falls back to checked downloads; a stale or
unavailable site follows the recoverable feed delivery and dispatch path. The
cache can be deleted without losing delivery state. Missing/failed cache
downloads take the cold path; failed cache uploads leave delivery results
intact. Cache files are never written or committed to the private data branch.

The coordinator keeps its 540-second tick budget and 15-minute schedule.
Advance-stage readers reuse a snapshot until a task-table write or a write
through another SQLite connection invalidates it. Process-local SQLite TEMP
triggers track direct task writes; a Python connection epoch invalidates
snapshots on rollbacks and failed statements, including savepoint reuse,
without changing the persisted ledger format. Empty Mathesis passes inspect
delivery fields before loading the full ledger; unfinished legacy migration and uncertain delivery handles
still follow their original recovery paths.

For authorized local inspection, run `python3 -m managed.control status`,
`python3 -m managed.control work`, or `python3 -m managed.control reviews`
with the configured private data checkout and an authenticated GitHub CLI.

## Recover one short-note Zenodo publication

Use **Coordinator → Run workflow → action=notes** on a revision containing the
recovery implementation. Select `note_action=status` and set `note_id` to inspect
one note; leave the revision and recovery fields empty for status. The coordinator
runs a status read before delivery, within the existing single-writer concurrency
group. Detailed note status is captured in the private control Release run log;
public note-delivery output contains only counts, including warning annotations
when notes need attention. Dispatch selections are read from `GITHUB_EVENT_PATH`
inside the captured process, keeping note IDs, revisions and draft IDs out of
the step environment printed by Actions. Manual `action=notes` inputs
replace the entire `OUTREACH_NOTE_REQUEST` selection, including optional fields.
An invalid or incomplete selection fails rather than falling back to the secret.
Scheduled runs continue to drain the ordinary queue.
The single concurrency group permits only one pending run. Even with
`cancel-in-progress: false`, the next scheduled run can cancel a pending manual
dispatch. Confirm that the manual run was not cancelled and that delivery ran;
if it was cancelled, redispatch the same selection.

Inspect the `short-note-<id>` Release in the configured **private data repository**.
Its JSON body is the retained state: check `versions["<revision>"]` for `phase`,
`record_id`, `package_id`, and the corresponding `deliveries` entry. Download the
immutable `note-v<revision>.json`, PDF, source ZIP and original `request-*.json`
asset into a private directory. For example, these are read-only GitHub operations
(the local files belong outside the public engine checkout):

```sh
gh release view short-note-alpha --repo "$OUTREACH_DATA_REPO" --json body --jq .body
gh release download short-note-alpha --repo "$OUTREACH_DATA_REPO" \
  --pattern 'note-v2.*' --pattern 'source-v2.zip' --pattern 'request-*.json' \
  --dir /private/operator/alpha-v2
```

Check that the manifest hash matches `package_id`, and that both file SHA-256
hashes match the immutable manifest. Inspect the original Zenodo draft through
the account, checking its note keyword, version/concept, title, ordered creators,
affiliations, identifiers, license, publication/preprint types, access right,
file inventory and file checksums. A `creating` receipt without `record_id`
means the creation outcome is uncertain: find the exact draft ID in the account
before retrying. The creation fence prevents a second deposit. Never guess an ID,
clear a receipt, restage a modified package as the same revision, or change its
producer or Mathesis collection receipt.
If an HTTP 500 left `phase=creating` but account inspection confirms that no draft
was created, recovery still needs an operator-attested transition out of that
uncertain state. This change does not implement that transition; the creation
fence remains in place.

For `uploading`, reuse the retained `record_id`. For uncertain `creating`, supply
the recovered exact ID. Dispatch one publication request with `action=notes`,
`note_action=publish`, `note_id`, `note_revision`, and `recover_record`. For example:

```sh
gh workflow run outreach-control.yml \
  --repo the-omega-institute/trureturing-outreach-engine --ref main \
  -f action=notes -f note_action=publish -f note_id=alpha \
  -f note_revision=2 -f recover_record=12345678
```

Leave `reuse_draft_from` empty when resuming the same revision. Set it only when
an intentionally prepared **new artifact revision** must reuse a specific earlier
unpublished revision's draft; it must be positive and less than `note_revision`.
The selected package and creation fence remain authoritative. A supplied ID
cannot replace a retained ID or trigger creation without a retained intent.
`action=publish` executes only the selected immutable request, with `retry_failed`
limited to that request. This applies to ordinary publication as well as recovery;
other queued requests wait for their own dispatch or scheduled drain, and other
failed requests still require explicit retry.
Earlier request assets and delivery receipts are retained. Recovery uses the
existing Zenodo concept/draft checks and never submits a manuscript to Mathesis.
Existing collection links and producer identity stay in their original records.

Creation and metadata updates send the original title and creator-name punctuation,
preserving en dashes and consistency with earlier versions and the Mathesis title.
Creator names retain the existing family-name formatting. Only remote metadata
comparisons use Unicode NFC followed by ASCII mapping
of U+2018–U+201B apostrophes/single quotes, U+201C–U+201F double quotes and
U+2010–U+2015 hyphens/dashes. Thus Zenodo's U+2019 apostrophe rewrite is accepted
without changing what is sent. Accents, case, spacing, other metadata, immutable
manifests and artifact bytes retain their existing checks. A substantive mismatch
still needs inspection.

After dispatch, inspect the new private log and retained state: the exact version
must have `phase=published`, the original `record_id` and `package_id`, and its
published DOI. Check that the selected request is `dispatched`. A new delivery
failure fails that run and records `failed`; subsequent scheduled drains skip
that receipt, report `status=needs_attention` and list its request in private
JSON/status output. Public logs emit `processed`, `failed`, `remaining` and
`needs_attention` counts, with a count-only GitHub `::warning::` annotation when
`needs_attention > 0`. Previously recorded failures alone return success;
new failures and feed/verification failures still fail. Successful recovery marks
the old failed request `recovered` in the queue view while preserving its stored
historical receipt and removing it from the attention count. Other notes continue
through the scheduled queue independently.

## Sandbox pool retirement

A healthy pool Sandbox admits its group's work up to `agents_per_sandbox`,
less the reserved review slot, however many Agents it has hosted.
`agents_placed` is usage telemetry only. The pipeline is continuous: each
configured group keeps its last healthy Sandbox whatever the queue holds,
including an empty queue, a paused coordinator or only disabled kinds. A
CMA-paused last Sandbox is revived for work. That Sandbox leaves the pool only
through the corrupt, stalled, failed and removed-group paths, and the group's
next task creates its replacement. A surplus healthy Sandbox beyond the group's
last one retires after its group has had no queued, provisioning or running
work, no pending review and no creation in flight for the idle period
(`OUTREACH_POOL_IDLE_PAUSE_SECONDS`, default 600). Every row of the group
counts, whatever the coordinator pause, kind enablement or hosted-review switch
says; those switches govern admission only. A pool without configured groups
retires idle Sandboxes the same way. Running Agents are never stopped or moved.
The former `rotate_after_agents` and `quota_blocked_max_agents_placed` keys are
unused; they remain optional and are still validated as positive integers.

## CMA cleanup observations

Idle `ready` Agents can be deleted directly. Working Agents must first be
observed stopped; a `409 agent_not_ready` stop refusal for a terminal Agent
does not consume a cleanup attempt. An accepted Agent DELETE is persisted and
followed by GET observations of its original ID, even if inventory omits it or
CMA continues to report its old status. Observations do not consume the three
refusal attempts. After 24 hours without completion, or a terminal observation
refusal, cleanup raises an operator check. Transient read failures retain the
acceptance and the observation deadline.

Checked pool Agents remain observable on the retained-entry recheck cadence
(six hours). A resource-gone GET clears their checks from the Agent outcome,
Sandbox cleanup entry and status projection. A legacy task-owned Sandbox with
other Agents is retained, and raises a check after 24 hours. Signed check clears
retain an acknowledgement digest in subsequent lifecycle keys so retries cannot
reuse keys from an earlier refusal cycle.

## Publication discovery from mail

Receipt reconciliation reads arXiv notices and explicit public arXiv links in
incoming correspondence. After checking the public record, a paper naming every
configured `identity.team_authors` member can enter the existing publication
catalog without a private submission-board row. The catalog receives public
title, complete author order, abstract summary, date, identity and revision only.
Private message identifiers and correspondence remain outside that projection.
Known submission rows retain their separate reconciliation, and table writes
use `managed.data_writes` on the configured private data branch. The existing
Pages feed projection and delivery publish the result.

Discovery uses the bounded incoming-mail listing and its `uniqueBody`, without
reopening whole conversations or inspecting drafts. Every public arXiv link in
a message is considered. Duplicate identities share one public lookup per pass,
using the highest requested version. An unavailable or unverified public record
is reported as deferred while other verified papers continue to the catalog.
The next scheduled pass retries deferred identities from the same incoming-mail
window; no discovery cursor advances past an unverified paper.

## Close an owner question

To close reviewed reply or research questions without requesting another CMA
round, take the task ID and result hash from the `user_questions` section of
`work`. Create a decision envelope with these fields.

```json
{
  "schema_version": 1,
  "task_id": "reply-fixture",
  "result_hash": "<result_hash reported by work>",
  "action": "close",
  "reason": "The correspondent withdrew the request.",
  "evidence_ref": "fixture:withdrawal",
  "reviewer": "local-claude-loop"
}
```

Compute the hex HMAC-SHA256 of the envelope serialized with
`managed.queue.canonical`, using the same key as signed answers and reviews.
Add the hex digest as `signature` and stage the signed file with
`python3 -m managed.control decision /path/to/decision.json`. The coordinator
checks the signature and result hash and accepts closure while the task is
`awaiting-user`. It records `question-closed` with the reason and evidence
reference and discards any unsent letter in that result. The closure appears
in `work.user_questions_closed`.
