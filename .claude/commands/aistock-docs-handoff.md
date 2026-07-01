# aistock-docs-handoff

Use this command for AIstock documentation, README, handoff, and temporary Codex/Claude exchange notes.

## Classify paths first

- `docs-fast-update`: existing ordinary docs under `docs/architecture/`, `docs/analysis/`, `docs/design/`, `docs/handoff/`, `docs/operations/`, `docs/operations_*.md`, or `README.md`.
- `docs-fast-new`: new ordinary docs in the same doc set.
- `docs-controlled`: `docs/standards/**`, `docs/codex_project_memory.md`, `AGENTS*`, `.codex/**`, `.claude/**`, or workflow/client instructions.
- `scratch`: temporary notes under `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/`.

## Rules

- Use an isolated worktree/branch for tracked docs changes.
- For docs-fast, run only `git diff --check`; do not run nox, pytest, backend, frontend, CodeGraph, or UA validation.
- For docs-controlled, use controlled workflow and run workflow smoke for changed client/workflow files.
- Scratch notes are ignored by Git and are not durable PR, Issue, BUG, close-sync, or design acceptance evidence.
- Promote scratch notes into `docs/handoff/` only when they become durable evidence, then use the matching tracked workflow.

## Report

Report changed docs, validation, production gates as noop unless explicitly changed, and whether scratch files remain ignored.
