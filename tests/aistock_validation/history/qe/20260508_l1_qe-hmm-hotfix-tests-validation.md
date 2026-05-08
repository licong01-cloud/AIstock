# QE/HMM hotfix validation coverage - 2026-05-08

## Task

Agent C validation worker for `codex/qe-hmm-hotfix-validation-20260508`.

## Read Documents

- `docs/codex_project_memory.md`
- `docs/standards/aistock_development_standard_v1.1_20260504.md`
- `F:\Dev\AIstock_worktrees\qe-hmm-hotfix-handoff-20260508\docs\operations\qe_hmm_experiment_infra_issues_20260508.md`
- `F:\Dev\AIstock_worktrees\qe-hmm-hotfix-handoff-20260508\docs\architecture\qe_hmm_hotfix_and_governance_detailed_design_20260508.md`
- `F:\Dev\AIstock_worktrees\qe-hmm-hotfix-handoff-20260508\docs\operations\qe_hmm_hotfix_multi_agent_handoff_20260508.md`
- `F:\Dev\AIstock_worktrees\qe-hmm-hotfix-handoff-20260508\tests\aistock_validation\modules\qe_hmm_hotfix_and_governance.md`
- `F:\Dev\AIstock_worktrees\qe-hmm-hotfix-handoff-20260508\docs\architecture\qe_sota_strategy_package_asset_governance_design_20260508.md`

## Modified Validation Files

- `backend/tests/unified_engine/test_qe_backtest_recorder_isolation_hotfix.py`
- `backend/tests/unified_engine/test_qe_score_weighted_capacity_strategy.py`
- `backend/tests/strategy_package/test_score_weighted_capacity_contract.py`
- `frontend/tests/qe/qe-capacity-strategy-ui.spec.ts`
- `tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md`
- `tests/aistock_validation/history/qe/20260508_l1_qe-hmm-hotfix-tests-validation.md`

## Coverage Added

- Recorder isolation contract and behavior xfail tests for target `mlruns` symlink, source/target realpath collision, target under source, source params separate from target recorder, final reparse/path-swap validation, and shared source payload across two target backtest-only submissions.
- Capacity strategy contract tests for legacy `score_weighted_topk_v2` 5M default, new `score_weighted_topk_v2_capacity_v1` schema/default contract, config composer capacity kwargs allow-list, and explicit StrategyPackage/Paper target-value flow.
- Frontend static Playwright guard for capacity fields in QE evolution UI; currently expected-failing until Agent B UI work lands.

## Test Results

- `python -m py_compile backend/tests/unified_engine/test_qe_backtest_recorder_isolation_hotfix.py backend/tests/unified_engine/test_qe_score_weighted_capacity_strategy.py backend/tests/strategy_package/test_score_weighted_capacity_contract.py` -> PASS.
- `python -m pytest backend/tests/unified_engine/test_qe_backtest_recorder_isolation_hotfix.py backend/tests/unified_engine/test_qe_score_weighted_capacity_strategy.py backend/tests/strategy_package/test_score_weighted_capacity_contract.py -q -p no:cacheprovider` -> PASS with expected xfails: `5 passed, 8 xfailed`.
- `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1 --fail-new-only --baseline-json tmp/validation/guardrails/baseline_20260504.json` -> PASS, blocking=0.
- `npm run test:e2e -- --config=playwright.config.ts frontend/tests/qe/qe-capacity-strategy-ui.spec.ts --reporter=line` from `frontend/` -> NOT RUN, local `node_modules` absent and `playwright` command not recognized.
- `git diff --check` -> PASS.
- trailing whitespace scan for new untracked validation files -> PASS.

## Expected-Failing Items

- Recorder isolation runner implementation is not present in this origin/main-based worktree: `qe_recorder_isolation.json`, source params directory contract, and fail-fast error codes are expected-failing until Agent A lands implementation.
- Capacity strategy asset/registration is not present: new registration script and new StrategyPackage family/default are expected-failing until Agent B lands implementation.
- QE evolution UI still needs capacity schema rendering; frontend guard is expected-failing once runnable until Agent B lands UI changes.

## Safety Notes

- Production backend `8001`: not touched.
- Protected assets: no QE/RD-Agent worker workspace, source `mlruns`, model weights, HMM snapshots, StrategyPackage frozen manifests, Paper ledgers, or production DB touched.
- DB writes: none.
- Source/target `mlruns` realpath summary: no real workspace paths created or mutated; tests use static contracts/mocks only.
- New/old strategy source hash/default comparison: legacy registration defaults verified by static unit test; new strategy comparison pending Agent B implementation.

## Residual Risk

- Recorder path tests include tmp_path filesystem checks that stay xfailed until Agent A exposes concrete runner helper APIs; they should flip to pass when Agent A is integrated.
- Frontend Playwright cannot be executed without installing frontend dependencies.
- `git diff --check` does not include untracked files; a separate trailing-whitespace scan was run for the new validation files.
