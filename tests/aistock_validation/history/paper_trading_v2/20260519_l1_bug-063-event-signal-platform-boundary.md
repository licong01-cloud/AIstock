# BUG-063 event_signal platform capability boundary

- Module: paper_v2 / strategy_package / selection_center
- Level: L1
- Date: 2026-05-19T12:35:00+08:00
- Branch: `bug/BUG-063-paper-v2-event-signal-boundary`
- Worktree: `F:/Dev/AIstock_worktrees/bug-063-paper-v2-event-signal-boundary`
- GitHub issue: `https://github.com/licong01-cloud/AIstock/issues/71`
- Operator: Codex App

## Scope

- Changed files: `backend/services/selection_center/runtime_profile.py`, `backend/services/strategy_package/backtest_contract.py`, `backend/tests/paper_trading_v2/test_runtime_profile.py`, `backend/tests/strategy_package/test_backtest_contract.py`, `backend/tests/test_unified_event_signal_schema.py`, `backend/services/strategy_package/runtime_config.py`, `backend/tests/strategy_package/test_runtime_config_contract.py`, BUG registry JSON, and this validation record.
- Impacted flow: future event-signal runtime capability ownership between StrategyPackage, Selection runtime profile, and Paper v2 runtime profile activation.
- Business goal: event_signal capability is platform-owned and fail-fast; StrategyPackage runtime_config no longer carries reserved event_signal fields.
- Out of scope: event_signal provider implementation, actual event_signal data consumption, DB schema/migration, MiniQMT runtime, production backend/frontend restart.
- Protected assets reviewed: no StrategyPackage frozen manifest, model weights, QE/RD-Agent artifacts, HMM snapshots, selection artifacts, paper ledger, or QMT state files modified.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| StrategyPackage leak removal | StrategyPackage runtime_config does not expose reserved event-signal capability fields | runtime_config contract test | Pass |
| Platform profile ownership | Paper v2 / Selection runtime profile can persist platform-owned `event_signal_policy` when contract exists | paper_v2 runtime profile tests | Pass |
| Contract fail-fast | Runtime cannot enable event_signal_policy when QE/candidate contract did not enable it | backtest contract tests | Pass |
| Contract consistency | Runtime profile id/as-of/merge policy must match QE event_signal_policy contract | backtest contract tests | Pass |
| Isolation gate | Current-phase event_signal isolation allows only platform-owned contract/profile files and rejects all other trading-path hits | unified event signal schema test | Pass |
| Asset safety | No protected trading/research assets changed | changed-file review | Pass |

## Commands

```bash
python -m pytest -q backend/tests/strategy_package/test_backtest_contract.py backend/tests/paper_trading_v2/test_runtime_profile.py -p no:cacheprovider
python -m pytest -q backend/tests/test_unified_event_signal_schema.py backend/tests/strategy_package/test_runtime_config_contract.py backend/tests/strategy_package/test_backtest_contract.py backend/tests/paper_trading_v2/test_runtime_profile.py -p no:cacheprovider
python -m py_compile backend/services/selection_center/runtime_profile.py backend/services/strategy_package/backtest_contract.py backend/services/strategy_package/runtime_config.py
git diff --check
```

## Evidence

- Initial BUG-063 isolation reproduction failed after adding platform-owned event_signal_policy because the isolation gate still treated all trading-path references as consumption.
- Updated gate permits only the two platform-owned contract/profile files via `platform_owned_contract_files`: `backend/services/selection_center/runtime_profile.py` and `backend/services/strategy_package/backtest_contract.py`.
- Targeted platform tests passed: `12 passed in 0.85s` during development.
- Final combined tests: `25 passed in 0.96s`; `py_compile` exited 0; `git diff --check` passed.

## Result

- Final status: passed.
- Remaining risks: event_signal provider/data consumption is not implemented in this slice; this only defines ownership and fail-fast boundaries.
- Need production backend restart: not during validation; no production port was touched.
- Need dev service restart: no dev service was started.




