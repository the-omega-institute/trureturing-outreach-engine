# Review an independent-submission round

Read `managed/submission/workflow.json` (rounds, quality_bar), the round named in
`inputs.submission_round` (missing means improve), the previous round's result and
review, and the exact PDF, TeX, extracted source archive and evidence artifact.
Also apply `managed/research/review.md` for proofs, references and source/PDF identity.

Every round, check:

1. New content. Compare the manuscript with the previous round's version. List the
   new mathematical items yourself and confirm each against the evidence ledger.
   Rewording or reformatting alone is a BOUNCE.
2. Correctness. Read every new or changed proof completely. A claim backed only by
   computation, an Oracle answer or a repository is a BOUNCE unless the paper
   states it as a computation with exact scope.
3. Oracle independence. Direction, gap check and editor review must be separate
   fresh conversations with recorded identifiers and the observed model. A gap
   check that was not run, or ran in the proposing conversation, is a BOUNCE.
4. Positioning. The posing source, dated priority record and increment are stated
   accurately; nothing is inflated, retitled or uncredited.
5. Back matter and byline. Competing interests, funding, affiliations, roles and
   the AI statement match `managed/submission/author-policy.json` exactly; grep the
   source for invented statements.
6. Build. The archive rebuilds; the PDF matches the source; pages are readable.

Round-specific verdicts:

- improve: PASS only if the contribution gate is passed with cited posing source
  and a priority search, and every audited gap is closed. A failed gate is a stop:
  record a HOLD with the redirect and board proposal.
- sprint: PASS only with at least one new, completely proved theorem integrated
  into the paper.
- consult: PASS confirms a direction. Write the confirmed direction (or a better
  one, with reasons) and the fallback into the review reason; the uplift round
  executes it.
- uplift: PASS only with the confirmed uplift result proved and integrated, or with
  correct partial results that materially strengthen the paper.
- finalize: PASS only when the fresh editor assessment is at least minor revision
  at the chosen journal and every concrete point is addressed. Otherwise BOUNCE
  with one more uplift attempt or a venue change; after the revision limit, HOLD
  with an archive or reposition proposal for the board.

PASS on finalize routes the package to submission-ready. It never authorizes an
actual journal submission, which needs its own decision and receipt.

Value maximization (the owner 2026-11-01): BOUNCE any round whose manuscript buries
a resolved conjecture (not stated in the title or first abstract sentence), omits
sibling cases posed in the same source, or whose evidence does not say whether the
posing authors have been told.

Apply the expert-feedback checklist in managed/research/review.md to every
manuscript round as well.
