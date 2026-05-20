# BUG-087 MiniQMT Daily Selection Lifecycle Validation - 2026-05-20

## Scope

- Issue: BUG-087 / GitHub #109
- Branch: `bug/BUG-087-miniqmt-daily-selection-lifecycle`
- Worktree: `F:\Dev\AIstock_worktrees\bug-087-miniqmt-daily-selection-lifecycle`
- Module: `qmt_strategy_ledger`
- Production impact: no production backend `8001`, frontend `3000`, production DB, or live MiniQMT runtime touched.

## Fixed Behavior

BUG-087 fixes the MiniQMT StrategyPackage binding lifecycle so an ACTIVE virtual strategy binding is no longer a frozen historical `selection_run_id`. The binding now represents the StrategyPackage identity and runtime references, while each trading day records separate `qmt_strategy.strategy_binding_selection_evidence` rows. Order generation must resolve evidence for the requested `trade_date` and fails fast if the evidence is missing, stale, failed, manifest-mismatched, or runtime-hash-mismatched.

## DESIGN-COMPLIANCE-001 Matrix

| Requirement / design item | Implementation refs | Verification evidence | Status | Gap / exception |
|---|---|---|---|---|
| Active MiniQMT binding stores package/runtime identity separately from daily SelectionRun evidence. | `backend/services/qmt_strategy_ledger/models.py:375`; `backend/services/qmt_strategy_ledger/package_binding.py:82`; `backend/migrations/qmt_strategy_ledger_20260518.sql:89` | `backend/tests/qmt_strategy_ledger/test_package_binding.py:131`; `backend/tests/qmt_strategy_ledger/test_router_summary.py:232` | PASS | None |
| Daily selection evidence is persisted by binding and trade date, with uniqueness guards and DB comments. | `backend/services/qmt_strategy_ledger/repository.py:315`; `backend/migrations/qmt_strategy_ledger_20260518.sql:89`; `backend/migrations/qmt_strategy_ledger_20260518.sql:119` | `backend/tests/qmt_strategy_ledger/test_repository.py:135`; `backend/tests/qmt_strategy_ledger/test_migration_comments.py:49` | PASS | Migration must be applied by operator before DB-backed runtime uses the new table. |
| Historical `selection_run_id` columns on active binding cannot drive future daily order generation by default. | `backend/services/qmt_strategy_ledger/selection_order_builder.py:139`; `backend/migrations/qmt_strategy_ledger_20260518.sql:80` | `backend/tests/qmt_strategy_ledger/test_selection_order_builder.py:634`; `backend/tests/qmt_strategy_ledger/test_selection_order_builder.py:647` | PASS | Existing legacy DB rows without daily evidence now fail fast until current-day evidence is generated/resolved. |
| Daily order build validates SelectionRun status, trade_date, data_source, package membership, manifest hash, and runtime hash before creating orders. | `backend/services/qmt_strategy_ledger/selection_order_builder.py:315`; `backend/services/qmt_strategy_ledger/selection_order_builder.py:408` | `backend/tests/qmt_strategy_ledger/test_selection_order_builder.py:569`; `backend/tests/qmt_strategy_ledger/test_selection_order_builder.py:647`; `backend/tests/qmt_strategy_ledger/test_selection_order_builder.py:673` | PASS | None |
| Binding rollover no longer requires replacing ACTIVE binding for next-day SelectionRun of the same strategy identity. | `backend/services/qmt_strategy_ledger/package_binding.py:110`; `backend/routers/qmt_strategy_ledger.py:116` | `backend/tests/qmt_strategy_ledger/test_package_binding.py:222`; `backend/tests/qmt_strategy_ledger/test_router_summary.py:206` | PASS | Identity-changing updates still require explicit `replace_active=true`. |
| Protected artifact evidence remains auditable and authoritative when an artifact repository is used. | `backend/services/qmt_strategy_ledger/package_binding.py:188`; `backend/services/qmt_strategy_ledger/selection_order_builder.py:408` | `backend/tests/qmt_strategy_ledger/test_package_binding.py:148`; `backend/tests/qmt_strategy_ledger/test_selection_order_builder.py:710` | PASS | Unit tests may use in-memory evidence without artifact fields; production router supplies DB artifact repository. |
| No simplified/POC replacement path delivered as complete. | Full lifecycle in service, repository, migration, router, and tests; no runtime fallback to binding columns. | Focused qmt suite, paper_v2 backend nox, guardrails, l0, and diff check listed below. | PASS | None |

## Commands Executed

```powershell
python -m pytest backend/tests/qmt_strategy_ledger/test_package_binding.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py backend/tests/qmt_strategy_ledger/test_repository.py backend/tests/qmt_strategy_ledger/test_router_summary.py backend/tests/qmt_strategy_ledger/test_migration_comments.py -q
# 39 passed in 15.27s

python -m pytest backend/tests/qmt_strategy_ledger -q
# 89 passed in 18.01s

python -m pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/selection_center/test_runtime_selection.py -q
# 67 passed in 2.13s

python -m pytest backend/tests/selection_center backend/tests/strategy_package -q -p no:cacheprovider
# 221 passed in 24.56s

python -m nox -s paper_v2_backend
# 443 passed, 1 skipped, 2 xfailed in 30.95s; session successful

python -m nox -s guardrail_changed_files -- --changed-only
# Session successful; changed-only guardrail blocking=0; files=13; P2 ALGO-COMPLEXITY warnings recorded for changed qmt files.

python -m nox -s l0
# Session successful; baseline/new non-blocking findings only, blocking=0.

git diff --check
# Passed; only CRLF normalization warnings.
```

## Business Outcome

- A MiniQMT strategy can keep one ACTIVE StrategyPackage identity binding while recording `sel_a` for day 1 and `sel_b` for day 2 as separate daily evidence rows.
- If the requested trade date has no evidence, order generation fails before any order request is created.
- If an evidence row points to a stale SelectionRun, failed SelectionRun, wrong manifest, wrong data source, or mismatched runtime hash, order generation fails before any order request is created.
- Existing rebalance BUY/SELL semantics and multi-strategy lot isolation continue to pass the qmt strategy ledger regression suite.

## Residual Risk / Operator Notes

- The migration `backend/migrations/qmt_strategy_ledger_20260518.sql` now includes `qmt_strategy.strategy_binding_selection_evidence`; DB-backed runtime needs this migration before this branch can be used against a database.
- Existing legacy active bindings with only `selection_run_id/trade_date` columns will not silently trade. They must record current-day daily selection evidence through the package-binding flow or an equivalent resolver.
- No production service was restarted and no production database change was applied by this validation.
