# BUG-061 StrategyPackage event-signal runtime_config isolation

- Module: strategy_package
- Level: L0
- Date: 2026-05-19T11:50:00+08:00
- Branch: `bug/BUG-061-event-signal-runtime-config`
- Worktree: `F:/Dev/AIstock_worktrees/bug-061-event-signal-runtime-config`
- GitHub issue: `https://github.com/licong01-cloud/AIstock/issues/70`
- Operator: Codex App

## Scope

- Changed files: `backend/services/strategy_package/runtime_config.py`, `backend/tests/strategy_package/test_runtime_config_contract.py`, `tests/aistock_validation/bugs/20260519_BUG-061-strategypackage-runtime-config-still-contains-event-signal-c.json`.
- Impacted flow: StrategyPackage unified runtime config contract and event-signal current-phase isolation guard.
- Business goal: StrategyPackage runtime_config must not carry reserved event-signal capability fields before there is a real runtime consumer.
- Out of scope: Paper v2 runtime-profile implementation, event_signal data generation, DB schema/migration, MiniQMT runtime, production backend/frontend restart.
- Protected assets reviewed: no StrategyPackage frozen manifest, model weights, QE/RD-Agent artifacts, HMM snapshots, selection artifacts, paper ledger, or QMT state files modified.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Isolation guard | Current-phase trading paths do not reference `event_signal` | `backend/tests/test_unified_event_signal_schema.py` | Pass |
| Runtime contract | `PlatformCapabilities` exposes only HMM and universe platform metadata | `test_platform_capabilities_do_not_expose_reserved_event_signal_fields` | Pass |
| Reserved-field scan | StrategyPackage runtime_config/tests have no `event_signal_enabled`, `event_signals`, or `PlatformSignalCapability` reserved fields | `rg` command below | Pass |
| Asset safety | No protected trading/research assets changed | changed-file review | Pass |

## Commands

```bash
python -m pytest -q backend/tests/test_unified_event_signal_schema.py -p no:cacheprovider
python -m pytest -q backend/tests/test_unified_event_signal_schema.py backend/tests/strategy_package/test_runtime_config_contract.py -p no:cacheprovider
rg -n "event_signal_enabled|event_signals|PlatformSignalCapability" backend/services/strategy_package backend/tests/strategy_package -S
git diff --check
```

## Evidence

- Pre-fix reproduction: `backend/tests/test_unified_event_signal_schema.py::test_event_signal_is_not_consumed_by_trading_paths_in_current_phase` failed with hit `backend/services/strategy_package/runtime_config.py`.
- Final targeted tests: `13 passed in 0.47s`.
- Reserved-field scan: no matches for `event_signal_enabled|event_signals|PlatformSignalCapability` under StrategyPackage runtime config/tests.
- Diff hygiene: `git diff --check` exited 0.

## Result

- Final status: passed for BUG-061 targeted scope.
- Remaining risks: BUG-063 still tracks the broader Paper v2/platform ownership decision for any future event-signal consumer.
- Need production backend restart: not during validation; no production port was touched.
- Need dev service restart: no dev service was started.
