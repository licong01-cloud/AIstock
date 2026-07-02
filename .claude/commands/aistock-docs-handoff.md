# aistock-docs-handoff

Use this command for AIstock documentation, README, handoff, and temporary Codex/Claude exchange notes.

## Classify paths first

- `docs-fast-update`: existing ordinary docs under `docs/architecture/`, `docs/analysis/`, `docs/design/`, `docs/handoff/`, `docs/operations/`, `docs/operations_*.md`, or `README.md`.
- `docs-fast-new`: new ordinary docs in the same doc set.
- `docs-controlled`: `docs/standards/**`, `docs/codex_project_memory.md`, `AGENTS*`, `.codex/**`, `.claude/**`, or workflow/client instructions.
- `scratch`: temporary notes under `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/`.
- `cleanup-fast`: move, archive, or delete a small number of docs/scratch/root-pollution files without changing executable behavior.

## Rules

- Use an isolated worktree/branch for tracked docs changes.
- For docs-fast, run only `git diff --check`; do not run nox, pytest, backend, frontend, CodeGraph, or UA validation.
- For cleanup-fast, keep the change mechanical: relocate/delete only the named files, do not rewrite them into reusable tools, do not add ignored `debug_tools/` content with `git add -f`, and do not run `py_compile`/`--help` unless the user explicitly asks to retain executable scripts.
- For docs-controlled, use controlled workflow and run workflow smoke for changed client/workflow files.
- Scratch notes are ignored by Git and are not durable PR, Issue, BUG, close-sync, or design acceptance evidence.
- Promote scratch notes into `docs/handoff/` only when they become durable evidence, then use the matching tracked workflow.

## Report

Report changed docs/files, validation, production gates as noop unless explicitly changed, and whether scratch files remain ignored.
