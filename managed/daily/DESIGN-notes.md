# Daily design decisions

The local daily uses preparation → existing independent review → exact signed
Claude review → deterministic publisher → verified receipt. Its entry is
`python3 -m managed.daily_local`; `managed.control work` exposes `daily_local`.
Transport vocabulary is shared; `sent` means X accepted a post request. Mail and
daily authorizations, budgets, policies and ledgers are separate. An hourly post,
daily publication or author-research result cannot authorize another channel.

## Preserve the current metric and editorial format

The source of the headline is committed origin/dev resolution markers during the
previous complete Singapore day. A family with several declarations counts once.
Keep baseline and cutoff commits, removed/reclassified IDs, source catalogue
revision and per-result public-release status. Upstream owns publication; this
consumer adapts to existing markers and reports without changing upstream.
Catalogue delay stays internal and does not become editorial qualification or
change the metric. A committed marker alone is insufficient evidence for a
blanket Lean claim: use existing trusted validation evidence or hold/accurately
revise the whole packet for fresh review.

The newer `results_copy` branch wins over `short_copy` in the inspected code and
configuration. Retain its core-research opening, dated exact count/topics,
evidence-backed theory feedback or one significant featured result, then repository
link. Do not revive the obsolete numbers-only or automatic 280-character template.
Determine account length/media capability before deployment; inability to fit
requires a reviewed edit or explicit thread decision, not silent truncation.

For merged dates, reserve each date for that account. A standalone October 16
report and an October 16–17 report conflict, even with different text. Imported
published or uncertain legacy spans reserve their dates before new scheduling.
The historical October 9–12 summary and October 29 receipt inspected for this
design are real existing publications, not sample items to replay. User-selected
merged-day configuration remains explicit, never automatic backlog compression.

Zero-result days need their own audited branch because v3's featured/explored
requirements assume nonzero additions. Recommend an honest zero-addition post
only if daily authority covers it: core opening if supported, no fabricated
feature, no invented three topics, accurate totals and matching card. Otherwise
hold with `zero_day_editorial_decision`. Likewise, fewer than three genuinely
supported topics is an editorial hold until an approved schema exception is
chosen. These edge cases are not patched into the existing pipeline in this task.

## Rendering and independent review

The user selected local daily production on 2026-10-31. Use the existing Mac
broadcast-kit renderer and omega-x publisher; CMA rendering is not a dependency.
Keep the local audit, review, date reservations and receipts under
`mail/outreach-maintenance/daily/`. Do not restart retired launchd jobs or use the
relocated legacy configuration's stale absolute paths. `pipeline.daily=false`
prevents duplicate CMA scheduling; publication remains paused.

The current 1600×900 template is broadcast-kit revision `56c1a2f`, 2026-10-31
16:31 SGT: light background; the OPEN PROBLEMS FROM THE LITERATURE, SOLVED.
headline; left running-total bars, one per calendar day; right total, coverage
and average when available, +N/date/proved/refuted, then Results, Around the clock
and Scale facts; footer CTA and repository link. Pass audited `facts.json` and
`cumulative.json` to research_digest_card.py through research_daily.py.
Inspect the rendered PNG at full resolution, including the total, the last bar,
axis ticks, date labels and repository link. Do not infer legibility from a
successful render.

The existing poster includes project branding distinct from email affiliations.
The new signature rule does not itself authorize changing that branding. Record
any branding proposal as a reviewable image/version change. Never overwrite
historical images or receipts. Persist source manifests, source renderer/font
versions, final PNG and concise visual-review evidence; clean reproducible caches.

The legacy daily gate requires real independent sshx reviews. This design adds
Claude review to every final managed packet and retains that legacy precondition
until the user explicitly selects replacement at cutover. No legacy job or review
script is modified, and no workers are started by this task.

## Review admission

Use the exact JSON verdict envelope in `../firstcontact/review.md` with a daily
item ID, current version, final text/media content hash and **daily** workflow
hash. Use `load_workflow_digests()` in queue.py: it hashes canonical JSON containing
the daily workflow plus both the exact UTF-8 shared review.md and these daily
notes. Firstcontact binds its workflow and shared review.md in the same manifest
form. Editing the shared contract invalidates approvals in both workflows; editing
daily instructions invalidates daily approvals. Pin these policy inputs to the
deployed repository revision rather than resolving mutable files mid-review.

The independent Claude daily report at evidence_ref must record:

- Reproduced day boundaries and baseline/cutoff marker-set calculation, family
  deduplication, added/removed IDs, total and approved merged coverage.
- Source truth, whether open-problem/Lean wording is justified, novelty/attribution
  limits, existing validation evidence, and any backfill or reclassification.
- Opening support from actual theory documents; topic coverage; significance and
  literal truth of the single featured result; cited supporting lines for feedback.
- Exact final text and full-resolution image inspection, every visible count/date,
  correct media bytes, no clip/overflow and the correct repository link.
- Existing legacy independent-review evidence if still required; current account,
  authority, stop/budget state and duplicate-day ledger check.

PASS only with all applicable checks supported; BOUNCE for a fixable editorial or
rendering defect; HOLD for unverifiable audit/source claims, prior publication,
uncertainty, blocked account, missing authority or a required user choice. Claude
cannot edit PASS content or post. A new image, edited headline, changed evidence
fingerprint, account or covered date invalidates both research and review.

## Managed execution and publication

The user-started local Claude loop checks the previous complete Singapore day
on each wake. `daily_local` reports the latest verified publication and missing
dates; preparation consumes the existing broadcast-kit editor, renderer and
independent review APIs. This entry installs no timer and creates no CMA daily
task. The archived launchd job is historical evidence. The local `enabled` flag
and versioned `enable_live_publication` flag separately gate daily publishing.
After inspecting the exact final body/PNG/evidence, the loop signs its returned
binding and records PASS/BOUNCE/HOLD with `daily_local review`. Only the deterministic
`daily_local publish` operation can consume a current PASS and active channel.

The publisher uses the existing `omega-x` service, verifies the exact expected
account, then prepares media. After durable intent/reservations, make one create
request. Known post ID is reconciled by GET; unknown create outcome is never
retried. Uploaded media alone can be recreated if no post attempt exists; after
an uncertain post, changing media would create another logical publication and
is forbidden. Store media IDs, expiry, upload evidence and the final approved
bytes to make readback meaningful. The adapter reuses the existing broadcast-kit
publisher rather than implementing another X transport. Published/merged/uncertain
historical dates remain reserved, and an unknown or rejected create cannot
automatically enter the old generic retry path.

On a positive no-effect rejection, bounded retry requires valid approval,
authority and policy. Do not generically label every 4xx safe; inspect the actual
endpoint contract/error. For 402, halt paid writes, alert privately and require a
budget check after at least one hour. The old repeated users/me/media storm must
not recur. For 429 respect Retry-After; 401/403 await owner resolution. Timeouts,
connection loss and ambiguous 5xx go to send-unknown and read-only reconciliation.
A verified public URL and observed content/media evidence complete the receipt;
registering that receipt again is idempotent and never reposts.
