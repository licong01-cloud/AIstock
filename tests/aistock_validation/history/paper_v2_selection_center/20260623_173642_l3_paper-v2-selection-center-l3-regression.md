# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-23T17:36:42
- Git commit: 374bf9ac
- Operator: lc999

## Scope

- Changed files: MiniQMT durable execution runtime Phase 6 gray switch controller, Phase 6 tests, Phase 7 fallback evaluation doc, and self-audit log.
- Impacted flows: MiniQMT event_loop canary switch and rollback control plane; Paper v2 L3 regression gates remain compiler-default inert.
- Business goal: prove Phase 6/7 changes keep B/compiler as default, require same-scope durable no-fatal shadow evidence before canary, and support audited rollback.
- Out of scope: PR merge, production service restart, production DB write/DDL, LIVE gray switch activation, deletion of compiler fallback.
- Protected assets reviewed: MiniQMT no-TDX guard, event_loop no synthetic timer guard, event_loop gateway no `return []`, vnpy_style attribution/source map, default compiler flag evidence.

## Environment

- Backend port: not started
- Frontend port: not started
- TDX port: not started
- Conda/env: repo default Python via `rtk`
- Database: existing configured validation DB was read by data-quality smoke; no production DDL or product writes by this PR
- Browser/headless: `PAPER_V2_L3_SKIP_UI=1`

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No blocking high-risk path/secret/fallback/asset finding | `rtk python -m nox -s l0` | PASS |
| Module registry | Ownership registry remains valid | `rtk python -m nox -s validation_module_registry_l0` | PASS |
| MiniQMT targeted tests | Phase 6/7 and existing event_loop runtime tests pass | `75 passed in 1.55s` | PASS |
| Flag inert and gray drill | Default compiler remains unchanged; scoped canary and rollback are audited | Phase 6 tests + flag inert tests `7 passed in 1.15s` | PASS |
| Paper v2 backend | Paper v2 + Selection Center backend tests pass | `paper_v2_backend`: 668 passed, 1 skipped, 1 deselected | PASS |
| Data quality | Paper v2 data quality smoke and deep checks pass | data-quality smoke success; `data_quality_deep`: 10 passed, 21 skipped | PASS |
| UI E2E | UI skipped by requested hosted-CI env | `PAPER_V2_L3_SKIP_UI=1` | SKIPPED |
| Asset safety | No protected MiniQMT event_loop TDX, JsonFile OMS authority, or vnpy_style fork | final section 10 guards in `docs/handoff/A_dev_selfaudit_log.md` | PASS |

## Commands

```bash
rtk python -m pytest backend/tests/miniqmt_execution_runtime/ -q
rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py::test_miniqmt_execution_runtime_flag_defaults_to_compiler_and_rejects_unknown_values backend/tests/miniqmt_execution_runtime/test_miniqmt_phase2_qmt_strategy_oms.py::test_event_loop_client_uses_qmt_strategy_oms_authority_and_compiler_default_is_inert -q
rtk python -m ruff check backend/services/miniqmt_execution_runtime/gray.py backend/services/miniqmt_execution_runtime/models.py backend/services/miniqmt_execution_runtime/__init__.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py
rtk git diff --check
rtk python -m nox -s l0
rtk python -m nox -s validation_module_registry_l0
rtk cmd /c "set AISTOCK_HOSTED_CI=1&& set PAPER_V2_L3_SKIP_UI=1&& python -m nox -s paper_v2_l3"
```

## Evidence

- MiniQMT runtime tests: 75 passed in 1.55s after rebase to `origin/main` at `6c338a58`.
- Phase 6 gray + flag inert tests: 7 passed in 1.15s.
- Ruff changed files: All checks passed.
- `git diff --check`: passed.
- `nox -s l0`: successful.
- `nox -s validation_module_registry_l0`: successful; 8 passed; ownership scan files=12, mapped=12, unmapped=0, ambiguous=0.
- `paper_v2_l3`: ran 5 sessions successfully: `paper_v2_l3`, `l0`, `paper_v2_backend`, `paper_v2_data_quality`, `data_quality_deep`.
- `paper_v2_backend`: 668 passed, 1 skipped, 1 deselected.
- `data_quality_deep`: 10 passed, 21 skipped.
- Data-quality warning: legacy Paper v2 ledger consistency warning remains non-blocking under this gate (`order_fill_quantity_mismatches=4`), not introduced by this change.
- Section 10 grep guards: event_loop `range(_timer_iterations)` count=0; event_loop gateway `return []` count=0; MiniQMT/event_loop TDX count=0; `backend/execution_algos/vnpy_style/` diff empty; JsonFile event_loop authority new core count=0; default compiler inert tests pass.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| None introduced in this Phase 6/7 gate | n/a | n/a | n/a |
| Legacy Paper v2 ledger consistency WARN | Existing historical ledger mismatch outside this MiniQMT Phase 6/7 change | Not changed or hidden by this PR | `paper_v2_l3` remains successful with WARN only |

## Result

- Final status: PASS
- Remaining risks: canary evidence should be expanded to more SIM portfolios/strategy slots; LIVE remains blocked until separate live admission gates and monitoring are complete; compiler fallback is retained.
- Need production backend restart: no
- Need dev service restart: no
- production_ddl_gate: noop
- production_frontend_dependency_gate: noop
- production_backend_dependency_gate: noop
