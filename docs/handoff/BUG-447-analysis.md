# BUG-447 Analysis - MiniQMT qmt_strategy_ledger broker-authoritative reconciliation

- Date: 2026-06-21
- Worktree: `F:\Dev\AIstock_worktrees\BUG-447-p1-miniqmt-qmt-strategy-ledger-read-only-reconci-20260621`
- Issue: GitHub #1385 / BUG-447

## Independent conclusion

The issue description matches the current code.

`QmtStrategyLedgerReconciliationService.reconcile_snapshot` currently defaults
`broker_authoritative=False`. When callers omit the flag, the service runs the
legacy comparison branch and creates an `ERROR` issue with
`issue_type=POSITION_MISMATCH` whenever summed local strategy lots differ from
MiniQMT broker positions. This includes the key broker-authoritative case
`broker_quantity=0` and `strategy_quantity>0`.

The scheduler/autorun path already passes `broker_authoritative=True`; that path
projects strategy lots from broker positions and emits `WARNING`-level authority
adjustments (`UNBACKED_STRATEGY_POSITION` / `UNATTRIBUTED_BROKER_POSITION`)
instead of `ERROR POSITION_MISMATCH`. The read-only router endpoint calls
`reconcile_snapshot` without the flag, so the same broker truth can report a
warning in autorun but an error through the API.

## Evidence checked

- `backend/services/qmt_strategy_ledger/reconciliation.py:132-140`:
  `broker_authoritative` default is `False`.
- `backend/services/qmt_strategy_ledger/reconciliation.py:169-203`: the
  broker-authoritative branch projects local lots to broker truth and appends
  warning adjustments.
- `backend/services/qmt_strategy_ledger/reconciliation.py:204-218`: the legacy
  non-authoritative branch appends `ERROR POSITION_MISMATCH`.
- `backend/routers/qmt_strategy_ledger.py:339-343`: the read-only
  `/reconciliation` endpoint omits `broker_authoritative`.
- `backend/services/simulation_runtime/scheduler.py:4498-4504`: autorun passes
  `broker_authoritative=True` and must remain unchanged.

CodeGraph query against the canonical `.codegraph/codegraph.db` returned these
callers:

```text
backend/services/simulation_runtime/scheduler.py|2056
backend/tests/qmt_strategy_ledger/test_account_group_slots.py|124
backend/tests/qmt_strategy_ledger/test_reconciliation.py|68
backend/tests/qmt_strategy_ledger/test_reconciliation.py|86
```

The fix worktree has no `.codegraph/codegraph.db`, so I used the canonical
codegraph database. The workflow context pack states the codegraph
`graph_root_source` is the canonical worktree. A scoped
`rg reconcile_snapshot\(` of the current fix worktree also found the read-only
router call and the same scheduler/test callers.

## Reproduced behavior in current code

Using `InMemoryQmtStrategyLedgerRepository`, one strategy lot of 100 shares and
empty broker positions:

- default call (current read-only endpoint behavior): `POSITION_MISMATCH`,
  `ERROR`, `position_authority=strategy_lot_quantities`
- explicit broker-authoritative call (autorun behavior):
  `UNBACKED_STRATEGY_POSITION`, `WARNING`,
  `position_authority=broker_positions`

This confirms the regression is real on the read-only API surface.

## Fix plan

1. Make broker-authoritative reconciliation the safe default by changing
   `broker_authoritative` default to `True` in `reconcile_snapshot`.
2. Make the read-only router endpoint pass `broker_authoritative=True`
   explicitly so the endpoint documents and enforces the invariant even if the
   default changes later.
3. Keep the scheduler/autorun call unchanged; it already uses
   broker-authoritative projection.
4. Keep the legacy `False` branch only as an explicit compatibility path for
   deliberate internal tests/diagnostics; callers can no longer downgrade
   authority by omission.
5. Update regression tests so default/read-only-style calls assert warning
   authority adjustments and no `ERROR POSITION_MISMATCH`.

## Scope note

The user explicitly requested this analysis file in
`docs/handoff/BUG-447-analysis.md`. That path was not included in the issue
`allowed_write_scope`; I treat the direct user instruction as a one-file scope
expansion for analysis evidence only. Code changes remain limited to the issue
allowed write scope.
