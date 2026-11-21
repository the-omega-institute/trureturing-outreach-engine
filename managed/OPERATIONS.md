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

For authorized local inspection, run `python3 -m managed.control status`,
`python3 -m managed.control work`, or `python3 -m managed.control reviews`
with the configured private data checkout and an authenticated GitHub CLI.
