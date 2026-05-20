# BUG-093 MiniQMT shared strategy engine single decision path validation

- Module: paper_v2 / qmt_strategy_ledger / strategy_package
- Level: L3 backend integration + L0 guardrails
- Date: 2026-05-20T18:21:10Z
- Branch: `bug/BUG-093-shared-strategy-engine-single-decision-path`
- Worktree: `F:/Dev/AIstock_worktrees/bug-093-shared-strategy-engine-single-decision-path`
- GitHub issue: `https://github.com/licong01-cloud/AIstock/issues/112`
- Code fix commit: `83e297815533acc27ef19aa330ac7dc747e4e766`
- Operator: Codex App

## Scope

- Changed runtime files: `backend/services/strategy_package/runtime.py`, `backend/services/qmt_strategy_ledger/selection_order_builder.py`, `backend/tests/qmt_strategy_ledger/test_selection_order_builder.py`.
- Registry/evidence files: `tests/aistock_validation/bugs/20260520_BUG-093-shared-strategy-engine-single-decision-path.json`, this validation record.
- Business goal: MiniQMT managed-order generation must not keep an independent strategy decision engine. It must adapt the shared `TargetPositionEngine` + `RebalanceEngine` decision output into MiniQMT-specific managed order requests.
- Out of scope: production backend `8001`, frontend `3000`, production DB, MiniQMT runtime/process, strategy package assets, broker fills/NAV reconciliation, and future live-trading approval flow.
- Protected assets reviewed: no frozen StrategyPackage manifest, model/factor artifact, selection artifact, paper ledger, QMT account state, MiniQMT runtime config, or production database data was modified.

## DESIGN-COMPLIANCE-001 Matrix

| Design / closure item | Implementation refs | Test / evidence | Status | Gap or exception |
|---|---|---|---|---|
| Single decision engine or explicitly equivalent shared core is used before broker adapters | `backend/services/qmt_strategy_ledger/selection_order_builder.py:96`, `backend/services/qmt_strategy_ledger/selection_order_builder.py:97`, `backend/services/qmt_strategy_ledger/selection_order_builder.py:147`, `backend/services/qmt_strategy_ledger/selection_order_builder.py:155`, `backend/services/qmt_strategy_ledger/selection_order_builder.py:169` | `test_selection_order_builder_uses_shared_rebalance_engine`; `python -m pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider` => 86 passed | Pass | None |
| `SelectionOrderBuilder` is adapter-only, not a separate final strategy algorithm | Removed local `_target_quantity`; builder converts SelectionRun to `SignalSnapshot`, `PositionLot`, shared targets/intents, then to `ManagedOrderRequest`: `backend/services/qmt_strategy_ledger/selection_order_builder.py:272`, `backend/services/qmt_strategy_ledger/selection_order_builder.py:291`, `backend/services/qmt_strategy_ledger/selection_order_builder.py:347` | `rg _target_quantity backend/services/qmt_strategy_ledger/selection_order_builder.py` returns no independent target-sizing helper; qmt ledger suite 86 passed | Pass | None |
| Cross-adapter decision equivalence is covered for same score/positions/runtime inputs | Shared output is compared against MiniQMT request tuples in `backend/tests/qmt_strategy_ledger/test_selection_order_builder.py:602` | `test_selection_order_builder_preserves_shared_decision_intent_sequence`; broad Paper/Selection/Strategy/QMT regression => 357 passed, 1 skipped, 2 xfailed | Pass | None |
| MiniQMT-specific differences remain submission/availability/price/metadata adapter concerns | T+1 sell cap and pending availability stay in adapter metadata: `backend/services/qmt_strategy_ledger/selection_order_builder.py:410`, `backend/services/qmt_strategy_ledger/selection_order_builder.py:494`, `backend/services/qmt_strategy_ledger/selection_order_builder.py:582` | qmt ledger suite 86 passed; no production MiniQMT runtime touched | Pass | Runtime callback/fill behavior intentionally out of this bug scope |
| Shared engine uses canonical board-lot rules and records residuals instead of hard-coded 100-share assumptions | `backend/services/strategy_package/runtime.py:357`, `backend/services/strategy_package/runtime.py:787`, `backend/services/strategy_package/runtime.py:797`, `backend/services/strategy_package/runtime.py:807` | strategy package + qmt broad regression => 357 passed, 1 skipped, 2 xfailed; `paper_v2_backend` => 443 passed, 1 skipped, 2 xfailed | Pass | None |
| Dropped holdings / reduce-to-target sells are emitted by shared `RebalanceEngine` | `backend/services/strategy_package/runtime.py:791`; adapter consumes sell intents in `backend/services/qmt_strategy_ledger/selection_order_builder.py:385` | cross-adapter test expects `DROPPED_FROM_SELECTION` sell and reduce sell | Pass | None |
| Zero target quantity is valid for sell-out decisions and does not require a price when no order will be bought | `backend/services/strategy_package/runtime.py:333`, `backend/services/strategy_package/runtime.py:337`, `backend/services/strategy_package/runtime.py:359` | strategy package and qmt regression suites passed | Pass | None |
| No simplified / POC-only delivery is reported as complete | Implementation routes production MiniQMT order-building code through shared runtime engine, not test-only code | full validation commands below passed; branch contains production code + regression tests + registry/evidence | Pass | None |
| Production safety boundary is preserved | Work ran only in `F:/Dev/AIstock_worktrees/...`; no `8001`, `3000`, production DB, MiniQMT runtime, or strategy assets touched | git worktree preflight; command log; no service start/stop commands executed | Pass | Runtime restart/activation remains a separate operator action after merge |

## Commands and Results

```bash
git status --short --branch
git branch --show-current
git log --oneline -5
```

Result: dedicated worktree on `bug/BUG-093-shared-strategy-engine-single-decision-path`; branch had code fix commit `83e297815533acc27ef19aa330ac7dc747e4e766`; only BUG-093 registry JSON was untracked before evidence update.

```bash
python -m pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider
```

Result: `86 passed in 7.10s`.

```bash
python -m pytest backend/tests/paper_trading_v2 backend/tests/qmt_strategy_ledger backend/tests/strategy_package/test_rebalance_runtime.py backend/tests/strategy_package/test_score_weighted_capacity_contract.py backend/tests/selection_center/test_runtime_selection.py -q -p no:cacheprovider
```

Result: `357 passed, 1 skipped, 2 xfailed in 13.43s`.

```bash
python -m nox -s paper_v2_backend
```

Result: `443 passed, 1 skipped, 2 xfailed in 14.79s`; nox session successful.

```bash
python -m compileall backend/services/strategy_package/runtime.py backend/services/qmt_strategy_ledger/selection_order_builder.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py
```

Result: PASS.

```bash
git diff HEAD --check
```

Result: PASS.

```bash
python -m nox -s guardrail_changed_files -- --changed-only
```

Result: nox successful for current uncommitted change set; after code commit only BUG registry was untracked, so this did not rescan committed code diff.

```bash
python scripts/aistock_guardrail_scan.py backend/services/qmt_strategy_ledger/selection_order_builder.py backend/services/strategy_package/runtime.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py --baseline-json tests/aistock_validation/guardrails_baseline_20260511.json --fail-new-only --fail-on-severity P1 --output-json tmp/validation/guardrails/bug093_explicit_files.json --summary-md tmp/validation/guardrails/bug093_explicit_files.md
```

Result: `files=3, findings=1, blocking=0`; non-blocking P2 `ALGO-COMPLEXITY-001` at `backend/services/qmt_strategy_ledger/selection_order_builder.py:712` (`_order_remark`). This is outside the decision-engine logic changed by BUG-093 and does not block P1/P0 guardrails.

```bash
python scripts/aistock_module_ownership_scan.py backend/services/qmt_strategy_ledger/selection_order_builder.py backend/services/strategy_package/runtime.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py --fail-on-unmapped --fail-on-ambiguous --output-json tmp/validation/module_ownership/bug093_explicit_files.json --summary-md tmp/validation/module_ownership/bug093_explicit_files.md
```

Result: `files=3, mapped=3, unmapped=0, ambiguous=0`.

```bash
python -m nox -s validation_module_registry_l0
```

Result: `8 passed in 0.98s`; nox session successful.

```bash
python -m nox -s l0
```

Result: nox session successful. Repository-level guardrail output still reports baseline/non-blocking findings: existing raw-JSON UI medium findings, P2 complexity findings in unrelated ingestion/data-sync files, one baseline P0 in `completion_contract.py`, and one baseline P1 `SCRIPT-LOCATION-001` in `noxfile.py`; blocking count was 0.

## Business Outcomes Verified

- MiniQMT managed-order generation now invokes the shared `RebalanceEngine` exactly once for the strategy decision layer.
- Given the same SelectionRun candidates and current strategy positions, MiniQMT request tuples match the shared `TargetPositionEngine` + `RebalanceEngine` intent tuples before adapter translation.
- Dropped holdings create full sell intents with `DROPPED_FROM_SELECTION`; reduced holdings create sell intents to target; buys are generated from shared target positions.
- Board-lot application is centralized through `round_to_board_lot()` in the shared runtime engine, with requested/residual quantity trace metadata retained.
- MiniQMT adapter-specific logic remains limited to broker-facing managed order fields, T+1 available-lot caps, pending sell reservations, price/slippage, order remark, `strategy_name`, and skip reasons.

## Residual Risks

- This validation did not start production backend `8001`, frontend `3000`, MiniQMT, or write production DB state; runtime activation must be separately performed after merge/restart.
- GitHub issue #112 should remain open with `status:fixed-pending-review` until review/main merge and any required runtime smoke validation are completed.
- The non-blocking P2 `_order_remark` complexity guardrail is recorded for transparency; it is not a BUG-093 correctness blocker.

## Final Result

- Validation status: PASS.
- BUG-093 local registry can move from `in_progress` to `fixed` once this record is committed and GitHub issue #112 is label/comment synchronized.
- Production impact during validation: none.
