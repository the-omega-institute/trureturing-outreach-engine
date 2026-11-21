# Workspace layout

The engine checkout contains Python code, versioned reference workflow and review policy,
tests and GitHub Actions definitions. `OUTREACH_ENGINE_REPO` identifies the source
repository for new task pins.

`OUTREACH_DATA_ROOT` points to the private checkout with `config.json`,
`policy/`, `tables/` and generated reading views. The selected branch and repository are
controlled by `OUTREACH_DATA_BRANCH` and `OUTREACH_DATA_REPO`. Workspace paths in
registry records remain relative to the configured data checkout.

New requests pin the engine source and operative policy independently. Workers use isolated task workspaces. The coordinator verifies selected
deliverable bytes and preserves them in the task's private Release.
