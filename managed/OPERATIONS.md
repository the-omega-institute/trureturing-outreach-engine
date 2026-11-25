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

## Sandbox pool retirement

A healthy pool Sandbox admits its group's work up to `agents_per_sandbox`,
less the reserved review slot, however many Agents it has hosted.
`agents_placed` is usage telemetry only. A Sandbox is retired only after its
group has had no queued, provisioning or running work, no pending review and no
creation in flight for the idle period (`OUTREACH_POOL_IDLE_PAUSE_SECONDS`,
default 600). Running Agents are never stopped or moved. Corrupt, stalled,
failed and removed-group Sandboxes keep their own retirement paths. The former
`rotate_after_agents` and `quota_blocked_max_agents_placed` keys are unused;
they remain optional and are still validated as positive integers.

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
