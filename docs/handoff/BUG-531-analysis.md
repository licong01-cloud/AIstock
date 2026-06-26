# BUG-531 Analysis - MiniQMT event_loop STAR board-lot

## Root Cause

- The existing single source of truth is `backend/execution_algos/board_lot.py::board_lot_rule`: `688/689 -> (200, 1)`, `300/301/302 -> (100, 100)`, `60/00 -> (100, 100)`, and unknown prefixes raise `ValueError`.
- `backend/services/miniqmt_execution_runtime/runtime.py::create_vnpy_algo_instance` previously used hard-coded defaults `min_volume=100` and `volume_increment=100`, then persisted those values in instance metadata.
- `backend/services/miniqmt_execution_runtime/shadow.py::_drive_event_loop_runtime` creates A/event_loop algo instances from the same shadow intent input but does not pass board-lot parameters. STAR BUY intents such as 1215 were therefore rounded by the vn.py-style core with 100/100 and became 1200, causing `MINIQMT_SHADOW_CHILD_ORDER_QUANTITY_DRIFT` FATAL.
- The B/CompilerAdapter path in `backend/services/miniqmt_execution_runtime/client.py` already derives board-lot with `board_lot_rule` and passes explicit `min_volume/volume_increment`. This fix does not modify B.

## Fix

- `create_vnpy_algo_instance` now treats `min_volume` and `volume_increment` as `None` sentinels. If callers omit both values, runtime derives them from `board_lot_rule(symbol)` and persists the resolved values in metadata.
- Explicit caller-provided values still override the derived defaults, keeping B/CompilerAdapter compatibility.
- `_resolve_vnpy_board_lot_params` is loud:
  - missing both values: derive from `board_lot_rule(symbol)`.
  - missing only one value: raise `MINIQMT_EVENT_LOOP_BOARD_LOT_OVERRIDE_INCOMPLETE`.
  - non-integer or non-positive override: raise `MINIQMT_EVENT_LOOP_BOARD_LOT_OVERRIDE_INVALID`.
  - unknown/non-A-share symbol: raise `MINIQMT_EVENT_LOOP_BOARD_LOT_RULE_UNRESOLVED`; no silent default to 100.
- `_ensure_vnpy_core` also uses the same resolver, so missing/corrupt metadata is not masked by `or 100`.

## Regression Coverage

- STAR `688/689` BUY `target_quantity=1215` produces child `quantity=1215`.
- Main board `60/00` and ChiNext `300/301` BUY still follow the 100-share increment: `1215 -> 1200`.
- STAR SELL residual exemption is preserved: `688` SELL `123` remains `123`.
- Unknown/non-A-share symbols raise `MINIQMT_EVENT_LOOP_BOARD_LOT_RULE_UNRESOLVED` and create no algo/child order.
- B/CompilerAdapter explicit override still produces STAR child/request `quantity=1215`.
- Shadow A/B same-source STAR intent reconciles without `MINIQMT_SHADOW_CHILD_ORDER_QUANTITY_DRIFT` FATAL and keeps `broker_called=false`.

## Scope And Self-Audit

- Did not modify `client.py`; B/CompilerAdapter behavior remains explicit and unchanged.
- Did not modify LocalSim, TDX, scheduler, bridge scenario injection, main/ChiNext 100-share rules, or SELL residual-lot exemption logic.
- Runtime now reuses `board_lot.py` as the single truth source instead of introducing another board-lot table.
- Shadow remains dry-run; tests assert `broker_called=false` for A and B shadow snapshots.
- Production gates: `production_ddl_gate=noop`, `production_backend_dependency_gate=noop`, `production_frontend_dependency_gate=noop`.
