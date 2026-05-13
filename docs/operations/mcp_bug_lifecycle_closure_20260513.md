# MCP Bug Lifecycle Closure - 2026-05-13

This note records the post-GitHub-Issues integration MCP closure path.

## Source Of Truth

- `tests/aistock_validation/bugs/*.json` remains the versioned source of truth.
- GitHub Issues remains the workflow/UI mirror.
- Live GitHub writes require `GH_TOKEN` or GitHub CLI auth plus `GITHUB_REPOSITORY`.
- Production service restarts and production DB writes are not part of this flow.

## MCP Tools

| Tool | Purpose | Writes |
| --- | --- | --- |
| `report_bug` | Create a BUG-NNN JSON registry entry from a finding/log. | local bug JSON |
| `mcp_github_issue_create` | Create a source-of-truth bug JSON and optionally mirror to GitHub. | local bug JSON, optional GitHub Issue |
| `assign_bug` | Set `assigned_agent`, optional `fix_branch`, and move status to `in_progress`. | local bug JSON, optional GitHub sync |
| `update_bug_status` | Move lifecycle status through `open`, `in_progress`, `fixed`, `verified`, `wontfix`. | local bug JSON, optional GitHub sync |
| `mcp_github_issue_sync_bug` | Synchronize one BUG-NNN between JSON and GitHub in `json-to-github`, `github-to-json`, or `both` direction. | dry-run by default; optional local/GitHub writes |

## Default Future Flow

1. User reports a bug and pastes logs.
2. Agent deduplicates with `list_bugs` / `mcp_github_issue_search`.
3. Agent creates or reuses the BUG-NNN record with `report_bug` or `mcp_github_issue_create(create_github=True)`.
4. Agent assigns ownership with `assign_bug(sync_github=True)` after choosing a fix branch.
5. Agent fixes on a feature branch and links the PR with `Fixes #NNN` or `Closes #NNN`.
6. Agent records fix evidence with `update_bug_status(status="fixed", fix_commit=..., sync_github=True)`.
7. A separate reviewer or validation pass verifies required checks.
8. Agent records final verification with `update_bug_status(status="verified", verification_run_id=..., sync_github=True)`.
9. `mcp_github_issue_sync_bug(direction="github-to-json", apply=True)` can backfill links/status from GitHub when the Issue was created externally.

## Guardrails

- Use dry-run first for existing or historical GitHub Issues.
- Prefer `github-to-json` for link backfill when the GitHub Issue already exists.
- Prefer `json-to-github` when the bug JSON is authoritative and the Issue is missing or stale.
- Do not merge to `main` or restart production services without explicit user confirmation.
