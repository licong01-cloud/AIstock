# aistock-docs-handoff

The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this command provides the docs procedure.

Use this command for AIstock documentation, README, handoff, and temporary Codex/Claude exchange notes.

## Classify paths first

- `docs-fast-update`: existing ordinary docs under `docs/architecture/`, `docs/analysis/`, `docs/design/`, `docs/handoff/`, `docs/operations/`, `docs/operations_*.md`, or `README.md`.
- `docs-fast-new`: new ordinary docs in the same doc set.
- `docs-controlled`: `docs/standards/**`, `docs/codex_project_memory.md`, `AGENTS*`, `.codex/**`, `.claude/**`, or workflow/client instructions.
- `scratch`: temporary notes under `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/`.
- `cleanup-fast`: move, archive, or delete a small number of docs/scratch/root-pollution files without changing executable behavior.

## Rules

- Follow `TOOL-RTK-001`: eligible supported high-output interactive commands must use RTK; direct fallback is limited to unsupported/unavailable calls, exact-raw-output diagnostics, or a first wrapper failure, with one concise reason. Never self-authorize `rtk trust`, and never make RTK or telemetry a task/PR/CI gate.
- Docs work never authorizes backend start/stop/restart; user backend process control remains user-owned even when documenting a runbook or restart contract.
- Use an isolated worktree/branch for tracked docs changes.
- For docs-fast, use `git diff --check` as the validation step.
- For cleanup-fast, keep the change mechanical: relocate/delete only the named files, preserve executable behavior by default, and use `git diff --check`; add focused `py_compile`/`--help` only when the user asks to retain executable scripts.
- For docs-controlled, use controlled workflow and run workflow smoke for changed client/workflow files.
- Scratch notes stay in ignored paths.
- Promote scratch notes into `docs/handoff/` only when they become durable evidence, then use the matching tracked workflow.

## Report

Report changed docs/files, validation, production gates as noop unless explicitly changed, and whether scratch files remain ignored.
