# Review one reply in its complete correspondence

The reviewer is the user's separately started local Claude loop. Read the
complete latest incoming message, relevant previous incoming and sent messages,
the exact proposed packet, all attachments, the research report and
`workflow.json` in this directory. Current AGENTS.md and current user decisions
override older wording in imported rules.

Decide whether any reply is needed first. A correspondent who says they will read or study material we just sent receives no further material until they respond, unless they asked for it; an 'I wonder whether' remark is not a request. Answer the correspondent's actual
request; distinguish technical questions, practical arrangements, invitations
and personal or authorship decisions. Acknowledgements without an outstanding
question can require action rather than another email. BOUNCE a courtesy-only
letter (late-reply apology, thanks, congratulations or a reminder ping with no
new substance) to a no-reply judgment (the owner 2026-11-10). Research must provide a
useful checked result, not an empty promise. Respect owner-controlled lines,
declines, publication agreements and every unresolved user decision. Retain
verified Professor/Dr. salutations and latest-thread reply-all recipients.

Check that proposed results, proof scope, source citations and attachments agree.
Review all automatic warnings in context; never mechanically insert a question,
project pitch or AI disclosure. Readable mathematical work belongs in the body
or a self-contained LaTeX/PDF note. Link Lean verification to the corresponding
source file at a verified revision. Oracle research/editorial review is retained
where the imported workflow requires it; no new Lean run without the user's
selected execution machine.
Before returning a verdict, read the complete thread and apply the pinned
reply pre-return checks to the coordinator's mailbox history. Use
`latest_sent` across the correspondent's verified addresses to compare the
salutation and closing with the newest Sent. Use `drafts` and
`drafts_complete` to verify that the new letter incorporates and supersedes
any current owner draft. The result must report both checks with their evidence.
For each outgoing letter, apply `voice.md` to the exact body. BOUNCE if the
`self_audit` entry named `voice pass` is missing, incomplete, or contradicts
the body or correspondence. Check that the opening sentence answers its cited
incoming sentence, each paragraph picks up the cited connective or reference,
and the audit quotes every sentence about project scale or method. Check the
owner's actual decisions against any language that suggests acceptance. BOUNCE
if an unsettled decision reads as accepted or the audit overlooks it. Apply
these checks to the final body if the reviewer makes an edit.
Review notes and verification packages prepared in the reply task with the
exact packet. Check a separate source PASS for any existing Outreach short note
the letter attaches or cites.

The controller's `reviews` command lists every pending result, including old
items. Return a PASS, BOUNCE or HOLD on its exact binding; include a concrete
reason and evidence reference. Do not rewrite an approved packet in place.
Revisions are new task IDs and go through research and review again. Sign the
review envelope with the reviewer-only key using `sign-review`, then enqueue it
with `review`. An identical signed verdict is replay-safe; conflicting/stale
verdicts cannot replace it. A PASS does not grant sending authority.

Read `managed/automation.json` too. Every letter that ever required the user's
decision remains a user-owned draft, even after that decision is answered.
Review a `needs-decision` question against the actual source; PASS confirms the
question and routes it to the user, not to sending. Review no-reply judgments
and independent-paper classifications as carefully as letters. A collaboration
decline can stop mail while allowing independent manuscript work.

The user authorizes automatic sending only for replies that are a purely academic
exchange about the mathematics, including a letter produced from an answered
decision, after exact independent review and fresh mailbox checks. Anything personal, any question about us or the project (identity,
affiliation, methods/tools/AI, funding, plans, meetings, invitations,
collaboration, authorship, publication, byline order or submission timing) or anything else not purely academic
must carry routing.mail=owner; BOUNCE an auto-routed letter that violates this.
When unsure, route to the owner (user 2026-11-01). BOUNCE an owner draft whose
stated reason or framing rests on a reading of the latest incoming message that
the thread contradicts (for example declining a meeting for a purpose the
correspondent never proposed); quote the contradicting lines so the mismatch
reaches the owner as a decision question (P12b, the owner 2026-11-04). Review commands only stage a verdict. The serialized
coordinator performs writes only after the mailbox is activated and production
unpaused. Preserve web-edited drafts, live reply-all and actual Sent history.
