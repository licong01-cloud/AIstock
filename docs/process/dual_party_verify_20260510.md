# Dual-Party Verify Protocol (2026-05-10)

How a bug record in `tests/aistock_validation/bugs/` transitions from
`status: fixed` to `status: verified` without the fixer being able to verify
their own work.

## Why dual-party

Single-party verify is trivially gameable: the agent (or human) that authored
the fix runs the tests, declares them green, and closes the bug. We want a
second pair of eyes — and a second toolchain — looking at the same evidence
before any fix is considered closed.

Concretely, dual-party verify guards against:

- **Test bias**: a fixer wrote the regression test alongside the fix; the
  test passes by construction. A different verifier reads the test and
  decides whether it actually exercises the failing scenario.
- **Reproduction drift**: the fix may "work" against the reproduction the
  fixer wrote but miss the original failure mode. A verifier replays the
  bug's stored `reproduce_command` against the *new* code.
- **Cross-tool boundary leaks**: an AIstock change touched a Codex-owned
  workspace (or vice versa). The other side should sign off before close.

## Roles

- **Fixer (`assigned_agent`)**: the agent or human that pushed the
  `fix_commit`. Updates `status: in_progress` → `fixed` and fills in
  `fix_branch` + `fix_commit` + `fixed_at`.
- **Verifier**: a *different* agent / human. Reads the bug, runs every
  command in `required_verification`, confirms every line of
  `closure_requirements`, then updates `status: fixed` → `verified` and
  fills in `verification_run_id` (drawer ID, CI run URL, or other durable
  pointer).

The verifier MUST NOT be the same identity as `assigned_agent`. The
canonical pairings are:

| Fixer | Verifier |
|-------|----------|
| `claude_code` | `codex_app` |
| `codex_app` | `claude_code` |
| `human` | `claude_code` or `codex_app` |

When the only humans available are the same person wearing two hats (rare
in this repo), the verifier MUST be a different *toolchain* (Claude Code vs
Codex App) so the static analysis + repro replay actually executes
independently.

## Status machine recap

```
open ─────► in_progress ─────► fixed ─────► verified
   │                                │
   └────────────────────────────────┴────► wontfix
```

Required fields per state (see `tests/aistock_validation/bugs/README.md`
for the full schema):

- `in_progress` requires `assigned_agent` + `fix_branch`
- `fixed` requires `fix_commit` + `fixed_at` + a fixer event in `events[]`
- `verified` requires `verification_run_id` + a verifier event in
  `events[]` whose `actor` ≠ `assigned_agent`
- `wontfix` requires a closing event in `events[]` recording the decision
  rationale

## Mechanism A: drawer-driven (current default)

Used when both sides already coordinate via cross-tool drawers
(`wing=cross-tool`, `room=codex-claude-coord`).

1. Fixer pushes `fix_commit` and posts a `[REVIEW]` drawer linking the
   commit + summary of changes + boundary confirmations.
2. Fixer marks the bug `status: fixed` with the new commit + `events[]`
   entry referencing the drawer.
3. Verifier reads the drawer, runs `required_verification`, optionally
   replays the original `reproduce_command` against the new commit.
4. Verifier posts a `[REVIEW] PASS` (or `[REVIEW] BLOCKED`) drawer.
5. **On PASS**: a coordinator (typically the strategy session that owns
   the bug registry) edits the bug JSON: `status` → `verified`,
   `verification_run_id` → the verifier's drawer ID, appends a verifier
   event to `events[]`. Either Mechanism B or a manual edit can apply
   this state change.
6. **On BLOCKED**: verifier files the new findings via
   `scripts/cross_tool_review_dispatch.py --apply` so each blocker
   becomes its own BUG-NNN. The original bug stays at `status: fixed`
   pending a follow-up fix round.

### Worked example: BUG-023

- Fixer: Codex App (commit `5bce68c` on
  `origin/codex/qe-governance-integration-20260509`)
- Fixer drawer: `drawer_cross-tool_codex-claude-coord_72c82167df76a2e0a646d8a8`
- Verifier: Claude Code strategy session
- Verifier action: read commit diff, confirmed the 32 regression tests in
  `backend/tests/strategy_package/test_repository_service.py` cover the
  atomic invariant + UniqueViolation mapping
- Verifier transition (Stage 4): set `status: verified`,
  `verification_run_id` to the drawer ID, appended verifier event to
  `events[]`

## Mechanism B: MCP-driven (Stage 3+)

Used when both sides have the `aistock-validation` MCP server configured
(see `docs/process/mcp_server_setup_claude_code_20260510.md`).

1. Verifier runs `aistock-validation/get_bug_agent_context(bug_id)` to
   retrieve the structured repair context.
2. Verifier runs every command in `required_verification` and confirms
   every item in `closure_requirements`.
3. Verifier edits the bug JSON file directly (the MCP server does not
   currently expose an `update_bug_status` tool — this is the Stage 6
   follow-up). The verifier MUST:
   - Set `status: verified`
   - Set `verification_run_id` to a durable pointer (drawer, CI run URL,
     local pytest output hash, etc.)
   - Append a verifier event to `events[]` with `actor` = verifier
     identity, `action: "verified"`, `note` describing what evidence was
     reviewed.
4. Commit the bug JSON change with a `chore(bugs): verify BUG-NNN` message.

## Mechanism C: CI-driven (Stage 4+)

When the GitHub Actions matrix in `.github/workflows/test.yml` runs all
`required_verification` commands and they pass on the head commit of a
PR that closes a bug, the workflow can flip the bug's status. **Not yet
implemented** — currently the workflow only PR-comments the failure case.
Stage 6 candidate: extend the workflow to:

- Detect `Closes: BUG-NNN` in the PR description / template
- After backend-tests pass on that PR, automatically set the bug's
  `status: verified` with `verification_run_id` = `ci:run/<run_id>`
- Require a second human reviewer to land the PR (the dual party here is
  CI vs. the human approving the PR)

## Specific protocol for BUG-006 / BUG-007 right now

These two are at `status: fixed` (commit `bd098f8` on
`origin/claude/dw-foundation-20260510`) awaiting Codex review of the T14b
synthesize logic.

When Codex posts a review drawer (whether `[REVIEW] PASS` or `[REVIEW]
BLOCKED`), the strategy session MUST:

- **PASS**: edit BUG-006 + BUG-007 JSON files. Set `status: verified`,
  `verification_run_id` = the Codex review drawer ID. Append a verifier
  event with `actor: codex_app` and `note` quoting the drawer's PASS
  verdict. Commit with message
  `chore(bugs): verify BUG-006/007 per Codex T14b synthesize review`.
- **BLOCKED**: dispatch each blocker via
  `scripts/cross_tool_review_dispatch.py --findings-json blockers.json
  --target-tool claude_code --apply`. The dw-foundation team picks up
  the new BUG-NNN entries and starts a fix round 3. BUG-006 / BUG-007
  stay at `status: fixed` until the next review cycle.

This document supersedes the informal "drawer says fixed → flip status"
practice. Future bug status changes should reference this protocol.

## What this document does NOT cover

- The `wontfix` decision path. That requires explicit user authorization
  per AIstock convention; document it in the bug's `events[]` and refer
  to the user-confirmed decision drawer.
- Reverting a `verified` bug. If a fix turns out to regress in
  production, file a *new* BUG-NNN whose `description` references the
  original bug. Do NOT rewind the original bug's status; the original is
  the historical record.
- Cross-repo bugs. AIstock is a single repo; this protocol does not
  attempt to coordinate across repos.

---

**Source of truth**: this document defines the *protocol*. Schema fields
remain authoritative in `backend/services/validation/finding_store.py`.
Any divergence between protocol and schema requires a synchronized PR
across both files.
