# Research outreach engine

This repository describes a workflow for turning mathematical results and correspondence into checked research notes, letters and publication packages. A scheduled coordinator is the serialized writer of the task ledger. Research agents prepare artifacts and report their hashes. A separate reviewer signs a specific version. The signature records a quality judgment. Sending a letter or publishing an artifact requires separate authority.

The workflow covers research, short notes, first contact, replies, manuscript submission and publication handoff. The human owner decides questions of authorship, meetings and personal matters. The contact registry records refusals and actual responses, and is checked before each contact.

The public engine contains the coordinator, tests with synthetic data and reference policy. Correspondence, contact data and the operative policy of a particular deployment belong to its private data repository. The private repository also holds detailed logs. Public logs report only operational status.

See `docs/METHOD.md` for the method. The research record and formal sources are maintained separately. [Formal sources](https://github.com/the-omega-institute/trureturing) · [Research record](https://www.trureturing.org/) · [Mathesis](https://mathesis-omega-research.apps.chrono-ai.fun/)

License: not yet specified

## Production review

Set `independent_review.enabled=true` in the private data configuration to run
exact-version review inside the scheduled coordinator. Its `profile` selects the
actual CMA reviewer model; provenance records `hosted-cma-review`, its fresh Agent,
original operation/response IDs, actual model, checked artifact inventory and preserved verdict.
Mail queue and Mathesis selection records retain that reviewer identity.
Operator-signed reviews retain `local-claude-loop`; owner decisions still require
the operator signing protocol.
The local Claude loop remains an additional diagnostic and optimization tool.
It is not needed to start, observe or apply a routine production review.

The coordinator reserves one existing Sandbox slot for review. No extra Sandbox
is provisioned. `max_active` (default one, maximum four) bounds reviewer Agents;
`max_active_per_group` (default one) keeps notes and mail admission independent.
The example deployment permits two reviewers, one per group.
Author execution must stop before its independent reviewer starts. Review inputs
are immutable, hash-checked private Release assets. The reviewer receives no
signing key and cannot grant sending, publication or owner decisions. Only the
serialized writer signs a valid exact verdict, under the existing downstream
mailbox, source, authority and publication gates. Historical Claude signatures
remain readable; signed owner decisions still use their original protocol.

Admission disconnects resume the original body/key/operation. A changed result
or supplied policy/identity invalidates the old review. The snapshot includes all
workflow inputs, including the mail voice guide, plus operative `AGENTS.md`,
`WORKSPACE.md` and identity. Pause and kind disable stop new admissions while
existing responses and Agent retirement continue. Each pool group keeps its
last healthy Sandbox continuously. Completed reviewer cleanup stops and
deletes only its Agent; the pool owns Sandbox retirement. New correspondence
reviews precede stale policy rebinds, with newest work first. Published or
delivered rows and immutable delivery/draft intents keep their original review. Missing artifacts, invalid output and
terminal provider failures cannot PASS; their original handles and diagnostics
remain in `data.hosted_review` for targeted repair. Lifecycle-busy (409), 5xx
and transport failures retry the original handles with exponential delays capped
at one hour. Definitive failures park until an operator repairs their cause.
After the old Agent's cleanup completes, stage an exact signed reset with:

```sh
python3.12 -m managed.control independent-review-reset --task TASK_ID --reason 'Corrected reviewer profile'
```

The command signs the current row revision and review digest, then stages an
immutable request for the coordinator. Replays apply once; a changed row rejects
the reset. The coordinator archives the old run and gives the new attempt new
creation/admission keys. Reset grants no review or owner decision.

Reviewer access is configured separately in `independent_review.github` with
`service`, `user_service_id` and `read_only=true`. Provision a dedicated NyxID
service/connection backed by a GitHub fine-grained token restricted to **Contents:
read** in the input repositories (private data Releases and any referenced source
repositories). Both the service and connection must differ from the coordinator's;
the service must expose only the reader connection. Its actual
server/token scope must enforce read-only access; the config flag declares this
provisioning requirement and does not change token scopes. No fallback uses the
coordinator's write-capable PAT. Missing configuration parks preparation with a
diagnostic. The reviewer profile receives only this service, never the author's
Oracle, mail, publication or credential services. It can read repository content
and Release inputs available to that connection; public literature remains
available through the Agent's ordinary runtime networking. The prompt describes
review duties only; activation remains in private configuration.

With `independent_review.register_author_contacts=true`, the same serialized
writer verifies the signed source-author proposal, rechecks current refusals,
aliases and contact history, updates the maintained registry and reading views,
and persists that data branch. Registration grants no sending authority. Genuine
owner choices continue through signed decisions; a reviewer cannot answer them.

`pipeline.auto_backfill=true` also reconciles historical baseline entries without
an operator-maintained ID list. Reuse exact PASSed notes first, prioritize new
sources, and admit at most `backfill_per_tick` historical notes while fewer than
`backfill_pending_limit` notes remain pending. Defaults are one and sixteen.
An occupied live queue postpones new historical work while exact existing-note
coverage can still be linked. Explicit `backfill_result_ids` retain their existing
meaning. These settings are opt-in in the private deployment configuration.
