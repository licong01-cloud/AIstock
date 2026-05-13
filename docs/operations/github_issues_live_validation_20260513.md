# GitHub Issues Integration Live Validation - 2026-05-13

## Scope

Branch: `codex/github-issues-integration-20260512`
Head at validation start: `8c7e002`; validation-blocker doc commit: `961bca4`
Worktree: `F:/Dev/AIstock_worktrees/github-issues-integration-20260512`

Goal: run the proposed pre-merge validation gate, including local regression checks and a real GitHub issue round-trip smoke.

## Results

### Local / Branch Checks

- Branch status: clean and aligned with `origin/codex/github-issues-integration-20260512` at `8c7e002` before live-smoke follow-up edits.
- `git diff --check origin/main...HEAD`: passed.
- Initial regression suite: `python -m pytest backend/tests/scripts/test_bug_github_sync.py backend/tests/scripts/test_bug_github_webhook.py backend/tests/test_aistock_mcp_server.py backend/tests/scripts/test_aistock_mcp_github_issue_tools.py -q`
  - Result: `69 passed in 2.19s`.
- YAML parse check for `.github/ISSUE_TEMPLATE/*.yml` and `.github/workflows/*.yml`: `Parsed 9 YAML files`.
- `cd frontend && npx tsc --noEmit --pretty false`: passed.
- Live smoke found UTF-8 BOM input fragility in temporary PowerShell-generated JSON files. `scripts/bug_github_sync.py` was hardened to read bug files, issue snapshots, and update targets with `utf-8-sig`; regression tests were added.
- Post-hardening regression suite: `python -m pytest backend/tests/scripts/test_bug_github_sync.py backend/tests/scripts/test_bug_github_webhook.py backend/tests/test_aistock_mcp_server.py backend/tests/scripts/test_aistock_mcp_github_issue_tools.py -q`
  - Result: `71 passed in 1.93s`.
- Env/bootstrap follow-up: `scripts/bug_github_sync.py` and `scripts/aistock_mcp_server.py` now load `.env.github-issues-local` for local repo/proxy defaults and fall back to `gh auth token` from the GitHub CLI keyring when `GITHUB_TOKEN`/`GH_TOKEN` is absent. Explicit process env tokens still take precedence for production/service deployments.
- Post-env-bootstrap regression suite: `python -m pytest backend/tests/scripts/test_bug_github_sync.py backend/tests/scripts/test_bug_github_webhook.py backend/tests/test_aistock_mcp_server.py backend/tests/scripts/test_aistock_mcp_github_issue_tools.py -q`
  - Result: `76 passed in 2.44s`.
- `cd frontend && npx tsc --noEmit --pretty false`: passed again after the env/bootstrap follow-up.
- `git diff --check origin/main...HEAD` and `git diff --check`: passed after the env/bootstrap follow-up.

### Offline GitHub Sync Simulation

- Full historical import dry-run: `python scripts/bug_github_sync.py --historical-import --all-severities --json`
  - Result: `status=planned`, `dry_run=true`, `create=36`.
- P0/P1-only auto-file dry-run: `python scripts/bug_github_sync.py --historical-import --p0-p1-only --json`
  - Result: `status=planned`, `dry_run=true`, `create=18`, `skip=18`.
- Synthetic GitHub issue webhook importer dry-run:
  - Result: `status=planned`, `dry_run=true`, `source=issues.opened`, `create_json=1`.

### Live GitHub Round-Trip

Authentication and network:

- `127.0.0.1:1080` SOCKS proxy was available and used for `gh auth login`.
- `127.0.0.1:7890` was not listening during validation.
- `gh auth status` after device-code login: logged in as `licong01-cloud`.
- Repo check: `licong01-cloud/AIstock`, `hasIssuesEnabled=true`.
- Local ignored env helper file `.env.github-issues-local` was created with only non-sensitive config (`GITHUB_REPOSITORY` + proxy URLs). No token was written to repo or env files.
- Follow-up env bootstrap validation intentionally removed `GITHUB_REPOSITORY`, `GITHUB_TOKEN`, `GH_TOKEN`, and proxy variables from the parent shell, then verified the scripts could still load local defaults and use the GitHub CLI keyring without a manual PowerShell token-export step.

Live smoke:

1. Created temporary bug JSON under `%TEMP%/aistock_github_issue_smoke_20260513/bugs`.
2. Ran `scripts/bug_github_sync.py --bugs-dir <temp> --repo licong01-cloud/AIstock --historical-import --apply --json`.
   - Result: `status=applied`, `create=1`.
   - Created issue: `https://github.com/licong01-cloud/AIstock/issues/1`.
3. Updated the temporary bug JSON status to `fixed` and reran the same sync command.
   - Result: `status=applied`, `update=1`.
   - Issue `#1` was closed.
4. Verified issue state with `gh issue view 1 --json number,title,state,url,labels --repo licong01-cloud/AIstock`.
   - Result: `state=CLOSED`, URL `https://github.com/licong01-cloud/AIstock/issues/1`.
   - Labels present: `P1`, `aistock:bug`, `import:historical`, `module:validation.center`, `risk:github_issues_live_smoke`, `severity:p1`, `status:fixed`.
5. Fetched live issue payload via `gh api repos/licong01-cloud/AIstock/issues/1`, wrapped it as an issues snapshot, and ran `issues-to-json` import against a temp bugs dir.
   - Dry-run result: `create_json=1`.
   - Apply result: `created_json`, imported bug `BUG-GH-SMOKE-20260513`, `status=fixed`, `github_issue_number=1`, `github_issue_url=https://github.com/licong01-cloud/AIstock/issues/1`.
6. Ran a read-only no-token-argument smoke through `scripts/bug_github_sync.py --bugs-dir <temp> --direction issues-to-json --json`.
   - Result: `status=planned`, `dry_run=true`, `create_json=1`, issue `#1`, bug `BUG-GH-SMOKE-20260513`.
7. Ran a read-only no-token-argument MCP client smoke through `scripts.aistock_mcp_server`.
   - Result: repo `licong01-cloud/AIstock`, token sourced from keyring with length 40, `issue_count=1`, first issue `#1`.
   - The MCP HTTP client now ignores proxy env for loopback Validation Center calls and disables unsupported SOCKS proxy env for GitHub calls when `socksio` is unavailable, avoiding the observed `httpx` SOCKS import error.

## Merge Readiness Assessment

Current status: **ready for PR / merge review** from the GitHub Issues integration perspective.

Reason: local tests, typecheck, YAML parse, offline dry-runs, and the real `bugs JSON -> GitHub Issue -> closed issue -> issue payload/imported bugs JSON` round-trip have passed.

Recommended remaining gates before merging to `main`:

1. Review temporary issue `#1` and keep it closed as validation evidence.
2. Confirm whether auto-created labels from the live smoke are acceptable; optionally recolor/standardize labels before importing historical bugs.
3. Open PR from `codex/github-issues-integration-20260512` to `main`.
4. Confirm GitHub Actions permissions allow issue writes on `main` after merge.
5. Do not run the 36-bug historical import until explicitly authorized after merge.
6. For unattended production/service use, prefer explicit `GITHUB_TOKEN` or `GH_TOKEN` in the service environment. For this single-machine development environment, the validated fallback is GitHub CLI keyring + `.env.github-issues-local`.

## Production Safety

- Production backend `8001`: not touched.
- Production frontend `3000`: not touched.
- Production DB: not touched.
- Live GitHub writes were limited to one temporary issue `#1`, which is closed.
- No historical bugs were imported into GitHub Issues.
