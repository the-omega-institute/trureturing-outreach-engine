# Method

## Coordination and review

The coordinator runs on a schedule as the serialized writer of the task ledger. Producers stage immutable requests. The coordinator records an intended external action before executing it. If the outcome is uncertain, it checks the original operation or message identifier before retrying. Requests identify the engine source and operative policy by revision so their workflow can be reconstructed.

Research agents prepare the mathematical argument, draft text and supporting files. They return the exact artifacts with hashes and identify the sources and checks that support their claims. A separate reviewer examines those artifacts and signs one exact version. Edits require review of the revised version. A signature establishes quality for the version it covers. Sending and publication each require separate authority and the applicable checks at the time of action. The human owner decides authorship, meetings and personal matters.

## Task kinds

| Task | Work and output |
| --- | --- |
| Research | Develop an argument or check a claim. Return the evidence and state which questions remain open. |
| Short note | Assemble a self-contained account of a result, its relation to prior work and the scope of any formal verification. |
| First contact | Connect a checked result to a specific question or contribution of the recipient. Prepare the exact letter and its supporting material. |
| Reply | Address the latest incoming message and verify the conversation, recipients and any promised deliverable. |
| Submission | Prepare a manuscript and its supporting files against the venue requirements and recorded author decisions. |
| Publication handoff | Transfer an authorized, reviewed package to a destination and reconcile the destination's receipts. |

## Correspondence and short-note closure

Before any contact, consult recorded refusals and ownership decisions for the intended recipients, including their known aliases. Record every actual response and keep its provenance. Praise never clears a refusal. Read the latest conversation before replying and check whether the owner has already answered or edited a saved draft.

Before writing a short note, look for an existing note or reusable argument. Check prior work against the exact claim and record the closest relevant results. If a Lean declaration is linked, state precisely which claim it verifies and identify the corresponding source. File any remaining formalization gaps so they can be tracked separately. [Formal sources](https://github.com/the-omega-institute/trureturing)

## Receipts and records

| State | Evidence |
| --- | --- |
| Prepared | The exact package and its recorded hashes. |
| Saved draft | A mailbox draft identifier and readback of the saved content. |
| Sent | A matched message in Sent Items or an equivalent confirmed transport receipt. |
| Published | A destination record that confirms public availability of the exact version. |
| Accepted | A recorded acceptance from the receiving venue. |

Review, dispatch, public availability and venue acceptance are recorded as separate events. An uncertain outcome remains pending until it can be reconciled against evidence from the relevant service.

For an uncertain Zenodo creation, note status first searches all versions for the quoted note marker and checks every returned record's exact marker and revision locally. A positive candidate remains pending until its original record and immutable package are verified; it does not claim a complete account inventory. An empty or unrelated search result falls through to authenticated inventory enumeration with all versions and an explicit chronological sort. Drafts precede published records, and the search and both partitions share one twenty-page budget. Both inventory partitions must finish for the account scan to be complete. Repeated pagination or the inspection limit leaves verification pending; status retains exact candidates, partition page sizes and distinct account records examined. An empty search result or inventory does not authorize another creation; the original intent remains pending until a verified original record or an authoritative rejection is available.

Zenodo GET requests retry transient server failures at most three times with a short backoff. A final account-read failure retains partial candidates and remains pending. Writes keep their original receipt and are submitted once per operation attempt.

## Engine and data

The engine contains executable code, synthetic fixtures and reference policy. It contains no correspondence, no contact data and no installation-specific operating policy. The private data repository holds the operative policy, contact registry, decisions, task records and detailed logs. The operative policy is placed over the reference files at their original paths. The engine source and policy have independent revisions, and agents verify files against the respective Git trees before recomputing the workflow digest.

Public logs give operational phase, status, duration and aggregate work counts. Message text, recipient identities, document details, service identifiers and diagnostic output stay in private records.
