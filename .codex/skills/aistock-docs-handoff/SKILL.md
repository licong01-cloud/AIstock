---
name: aistock-docs-handoff
description: "Handle AIstock documentation, README, handoff, and temporary Codex/Claude exchange notes. Use for docs-fast-update, docs-fast-new, docs/handoff, tmp/handoff, scratch notes, version notes, docs exchange, temporary docs, and handoff cleanup."
---

# AIstock Docs Handoff

Use this lane for documentation and handoff work. Keep it lightweight; do not route ordinary docs through BUG or code validation.

## Classify paths first

- `docs-fast-update`: existing ordinary docs under `docs/architecture/`, `docs/analysis/`, `docs/design/`, `docs/handoff/`, `docs/operations/`, `docs/operations_*.md`, or `README.md`.
- `docs-fast-new`: new ordinary docs in the same doc set.
- `docs-controlled`: `docs/standards/**`, `docs/codex_project_memory.md`, `AGENTS*`, `.codex/**`, `.claude/**`, or workflow/client instructions.
- `scratch`: temporary notes under `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/`.

## Rules

- Use an isolated worktree/branch for tracked docs changes.
- For docs-fast, run only `git diff --check`; do not run nox, pytest, backend, frontend, CodeGraph, or UA validation.
- For docs-controlled, use the controlled workflow and run workflow smoke for changed client/workflow files.
- Temporary Codex/Claude exchange notes must stay in ignored scratch paths and are not PR, Issue, BUG, close-sync, or design acceptance evidence.
- Promote scratch notes into `docs/handoff/` only when they become durable evidence, then use the matching tracked workflow.

## Completion

Report changed docs, validation (`git diff --check` or controlled checks), production gates as noop unless explicitly changed, and whether any scratch files remain ignored.
