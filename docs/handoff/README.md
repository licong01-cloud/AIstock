# AIstock handoff directory policy

Version: 2026-07-01

## Purpose

`docs/handoff/` stores formal, versioned handoff material that should remain in Git history, such as:

- BUG analysis handoff notes that are referenced by PRs, Issues, or validation evidence.
- Feature or workflow handoff notes that are intended to be reviewed and preserved.
- Lightweight F0 Feature Cards only when they are part of the approved feature workflow.

## Temporary Codex / Claude handoff notes

Temporary cross-tool notes must not be written directly under `docs/handoff/`.

Use one of these ignored scratch paths instead:

- `tmp/handoff/` for general temporary Codex/Claude exchange notes.
- `docs/handoff/_scratch/` for notes that need to sit near formal handoff docs while still staying untracked.
- `docs/handoff/local/` for local-only notes, drafts, or one-window diagnostics.

Files under the scratch paths are intentionally ignored by Git. They are not durable evidence and must not be used as design acceptance artifacts, PR evidence, or close-sync records.

## Promotion rule

If a temporary note becomes durable project evidence, move or rewrite it into a normal tracked path under `docs/handoff/` and use the appropriate documentation or issue workflow:

- Ordinary handoff documentation: docs fast path with `git diff --check`.
- Workflow standards, `docs/codex_project_memory.md`, `.codex/**`, or `.claude/**`: controlled workflow.
- BUG closure evidence: standard BUG workflow with GitHub-linked BUG JSON and PR evidence.

Do not keep duplicate copies in scratch and tracked locations after promotion.
