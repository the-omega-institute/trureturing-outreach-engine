# Engine development

Use Python and the standard library for engine code. Run
`python3 -m unittest discover -s managed/tests` and the short-note tests before
changing coordinator or delivery behavior.

Keep the engine checkout separate from the private data checkout. Use
`managed.location` for repository paths and `managed.data_writes` for branch-aware
persistence. Private inputs, generated contact views, credential material and
detailed run logs belong in the private data repository or its Releases.

Preserve workflow binding and review integrity. The serialized coordinator owns
the live ledger. Stage immutable requests and reconcile uncertain effects by their
original handles. Use synthetic examples and safe structured public output.
