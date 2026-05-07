# L2 Event Signal Post-merge Validation - 2026-05-07

## Scope

- Post-merge validation for branch `codex/event-signal-st-llm-design-20260506` after merging latest `origin/main`.
- Confirms event-signal announcement classification, document queue planning, PDF smoke tooling, and DeepSeek small-batch analysis still work after the merge.
- No production port `8001` restart was performed.

## Merge Result

- Command: `git merge origin/main`.
- Result: merge completed with the `ort` strategy and no conflicts.
- Merge commit: `d86197d Merge remote-tracking branch 'origin/main' into codex/event-signal-st-llm-design-20260506`.

## Commands And Results

- `python -m pytest backend/tests/announcements/test_title_classifier.py backend/tests/event_signal -q` -> `108 passed in 1.67s`.
- `python -m compileall -q backend/services/event_signal backend/services/announcements/title_classifier.py backend/tests/event_signal backend/tests/announcements/test_title_classifier.py` -> passed.
- `git diff --check HEAD~1..HEAD` -> passed.
- `git diff --check origin/main..HEAD` -> passed.
- Protected-module scan over `backend/services/selection_center`, `backend/services/paper_trading_v2`, `backend/services/quantevolver`, `backend/infra/qmt_client.py`, and `backend/routers/qmt.py` -> no matches for new event-signal/PDF identifiers.
- `python -m backend.services.event_signal.document_pdf_smoke --limit 1 --candidate-scan-limit 5 --artifact-dir F:\Dev\AIstock_artifacts\event_signal_pdf_smoke --output-dir reports\event_signal\pdf_smoke --max-pages 4 --max-chunks 4 --max-tokens 900` -> analyzed 1 row with DeepSeek, failed 0.

## DeepSeek Evidence

- Post-merge report JSON: `reports/event_signal/pdf_smoke/document_pdf_smoke_20260507_145248.json` (ignored runtime report).
- Post-merge report Markdown: `reports/event_signal/pdf_smoke/document_pdf_smoke_20260507_145248.md` (ignored runtime report).
- Summary: `analyzed_rows=1`, `failed_rows=0`, event type `regulatory_investigation_penalty`.

## Test Port Validation

- Attempted FastAPI startup on test port `127.0.0.1:8012` with ingestion, strategy, paper, node-health, HMM, QE evolution, and QE experiment schedulers disabled.
- Result: startup failed before the app served `/api/v1/health` due an existing clean-worktree dependency issue: `backend/routers/rl_execution.py` imports `backend.services.rl_execution`, but that package is absent from the worktree.
- Root production workspace has ignored local files under `backend/services/rl_execution/`; `.gitignore` currently ignores `rl_execution/`, so a clean Git worktree does not contain that service package.
- This is unrelated to the event-signal/PDF changes and was not fixed in this task to avoid modifying RL execution scope.

## Business Outcome

- Event-signal tests and PDF/DeepSeek smoke validation pass after merging latest `origin/main`.
- The merge introduced no event-signal conflicts.
- Main backend clean-worktree startup remains blocked by the pre-existing ignored RL service package, so full test-port health verification requires a separate RL packaging/import fix.
