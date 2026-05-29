# vn.py Execution Source Inventory for MiniQMT

> Date: 2026-05-29  
> BUG: BUG-151 / GitHub issue #340  
> Scope: source inventory, MIT attribution gate, and characterization-test plan before porting vn.py-style execution algos into AIstock.  
> Non-goal: no runtime Sniper/BestLimit/TWAP implementation in this issue.

## 1. Upstream Baseline

| Item | Value |
|---|---|
| Repository | `https://github.com/vnpy/vnpy_algotrading` |
| Audited commit | `4133987530eb28f3538d1983545d81c4f83d7d59` |
| Local audit path | `F:\Dev\AIstock_artifacts\vnpy_source_audit_20260529\vnpy_algotrading` |
| License | MIT License |
| Copyright | Copyright (c) 2015-present, Xiaoyou Chen |
| Notice requirement | Include copyright and permission notice in all copied or substantial reused source. |

MIT compatibility conclusion: AIstock can copy, modify, and distribute selected `vnpy_algotrading` source files if the copyright and MIT permission notice are retained. The first implementation PR that copies source code should add a third-party notice/attribution file or equivalent file-level headers.

## 2. Reuse Policy

- Maximize direct reuse of `vnpy_algotrading` code semantics and structure for Sniper, BestLimit, TWAP, and the minimal AlgoTemplate lifecycle.
- Do not implement same-name Sniper/BestLimit/TWAP algorithms from scratch without derived-from evidence.
- Strip only runtime coupling that conflicts with AIstock: `EventEngine`, `AlgoEngine`, `MainEngine`, gateway calls, UI event emission, and vn.py DTO imports.
- Keep broker submit/cancel/query, raw status, audit, cost capture, and persistence in AIstock adapter/repository layers.
- Prove behavior with characterization tests before claiming mature-code reuse.

## 3. File-Level Mapping

| Upstream file | Phase | AIstock target | Reuse strategy | Preserve behavior | Strip/adapt | Characterization tests |
|---|---|---|---|---|---|---|
| `vnpy_algotrading/algos/sniper_algo.py` | P1 direct port | `backend/execution_algos/vnpy_style/sniper_core.py` | Copy and adapt the algorithm core. Strip vn.py object/BaseEngine imports, but preserve state variables and core on_tick/on_order/on_trade branches. | SniperAlgo class state: vt_orderid active-order marker.; on_tick: if vt_orderid exists, call cancel_all and return.; LONG path: submit only when ask_price_1 <= price.; SHORT path: submit only when bid_price_1 >= price. | Replace vnpy.trader.object.TickData/OrderData/TradeData with AIstock DTOs.; Replace BaseEngine/AlgoTemplate inheritance with AIstock vnpy_style base class.; Replace buy/sell gateway call with core action returned to MiniQMT adapter. | active vt_orderid causes cancel_all before any new submit; long submits only when ask_price_1 <= price; short submits only when bid_price_1 >= price |
| `vnpy_algotrading/algos/best_limit_algo.py` | P1 direct port | `backend/execution_algos/vnpy_style/best_limit_core.py` | Copy and adapt the algorithm core. Preserve quote-following, random child volume, order_price tracking, cancel_all on quote change, and terminal clearing behavior. | State variables: vt_orderid and order_price.; Settings: min_volume and max_volume validation.; LONG path: no active order -> buy_best_limit(bid_price_1).; SHORT path: no active order -> sell_best_limit(ask_price_1). | Replace random.uniform with injectable deterministic random provider for replayable tests.; Replace vn.py TickData/OrderData/TradeData with AIstock DTOs.; Return submit/cancel actions instead of calling AlgoEngine buy/sell directly. | long without active order submits at bid_price_1; short without active order submits at ask_price_1; quote price change triggers cancel_all |
| `vnpy_algotrading/algos/twap_algo.py` | P1 direct port as TWAP_LITE_MINIQMT | `backend/execution_algos/vnpy_style/twap_lite_core.py` | Copy and adapt timer/slicing core. Preserve time/interval counters, order_volume calculation, cancel_all before slice, quote guard, and finish-on-time/trade behavior. | Settings: time and interval.; State variables: order_volume, timer_count, total_count.; order_volume = volume / (time / interval), then round to min volume where contract data exists.; on_timer increments timer_count and total_count. | Replace vnpy.trader.utility.round_to with AIstock board-lot utility.; Inject tick and contract/min_volume through AIstock adapter rather than get_tick/get_contract.; Return actions instead of calling buy/sell directly. | slice quantity matches volume divided by time/interval; timer_count/total_count advance exactly as upstream; no submit before interval threshold |
| `vnpy_algotrading/template.py` | P1 base abstraction extraction | `backend/execution_algos/vnpy_style/base.py, backend/execution_algos/vnpy_style/models.py` | Extract maximum reusable lifecycle and helper semantics without importing AlgoEngine/MainEngine. Preserve method names and active order update order where possible. | active_orders dict keyed by order id.; update_tick calls on_tick only when active.; update_order updates active_orders before on_order.; update_trade increments traded before on_trade. | Remove algo_engine dependency and UI put_event/write_log side effects from core.; Represent buy/sell/cancel as AIstock action DTOs.; Persist/log via Paper v2 adapter and repository, not core. | update_order mutates active_orders before on_order callback; update_trade increments traded before on_trade callback; finish emits cancel_all and terminal finished state |
| `vnpy_algotrading/base.py` | P1 status model mapping | `backend/execution_algos/vnpy_style/base.py, backend/execution_algos/vnpy_style/models.py` | Map status enum concepts into AIstock core model while preserving semantic names where compatible. | active/paused/stopped/finished style lifecycle states where present in upstream. | Do not import vn.py package at runtime; copy compatible enum values with attribution. | state transitions match upstream lifecycle names used by AlgoTemplate. |
| `vnpy_algotrading/engine.py` | P1 adapter design extraction | `backend/services/paper_trading_v2/execution/minqmt_live_algo_adapter.py, backend/services/paper_trading_v2/execution/minqmt_order_state.py` | Do not copy runtime engine directly. Reuse event routing responsibilities and order-to-algo mapping behavior in AIstock MiniQMT adapter. | algo template registry concept; tick events dispatched to subscribed algo; trade/order events routed by order id mapping; timer events routed to active algos | Do not instantiate vn.py EventEngine/BaseEngine/MainEngine.; Use Paper v2 scheduler/live session as runtime owner.; Use MiniQMT broker backend for actual submit/cancel/query. | order event routed to owning algo by broker order id/order remark; trade event routed to owning algo and updates fill state; timer event does not run inactive/terminal algo |
| `vnpy_algotrading/algos/iceberg_algo.py` | P3 future candidate inventory only | `docs/architecture/vnpy_execution_source_inventory_20260529.md` | Inventory only in the current project phase. Do not implement until capital size or single-name participation justifies iceberg behavior. | timer_count; display_volume; vt_orderid; cancel/reprice when best quote crosses limit | No runtime code in P1/P2. | future: visible volume cap; future: interval gate; future: cancel/reprice guard |
| `vnpy_algotrading/algos/stop_algo.py` | P3 future candidate inventory only | `docs/architecture/vnpy_execution_source_inventory_20260529.md` | Inventory only. Stop/conditional order is not first-phase MiniQMT rebalance execution scope. | vt_orderid active state; trigger check in on_tick; on_order/on_trade terminal handling | No runtime code in P1/P2. | future: long/short trigger conditions; future: terminal order handling |

## 4. Attribution Requirements

Each copied or substantially derived AIstock file should include an attribution header or central mapping equivalent to:

```text
Derived from vn.py/vnpy_algotrading at commit 4133987530eb28f3538d1983545d81c4f83d7d59.
Source file: <upstream_file>.
Original license: MIT License, Copyright (c) 2015-present, Xiaoyou Chen.
AIstock changes: remove vn.py runtime dependencies, adapt DTO/action boundary, add MiniQMT audit/risk/cost integration.
```

The implementation should preserve the machine-readable summary in Section 8, or move it to a tracked successor artifact if project JSON ignore rules change.

## 5. First Implementation Order

1. `backend/execution_algos/vnpy_style/base.py` and `models.py`: extract `AlgoTemplate` lifecycle/action semantics and status mapping.
2. `backend/execution_algos/vnpy_style/sniper_core.py`: direct derived port of `sniper_algo.py`.
3. `backend/execution_algos/vnpy_style/best_limit_core.py`: direct derived port of `best_limit_algo.py`.
4. `backend/execution_algos/vnpy_style/twap_lite_core.py`: direct derived port of `twap_algo.py`.
5. `backend/services/paper_trading_v2/execution/minqmt_live_algo_adapter.py`: AIstock event/action adapter; no vn.py runtime engine.
6. Audit/state/report files under `backend/services/paper_trading_v2/execution/` after core behavior is characterized.

## 6. Validation Gate for Later Port Issues

A later port issue cannot be marked fixed unless it provides:

- Attribution or source mapping for every copied/derived file.
- Characterization tests covering the preserve behavior listed in this inventory.
- Import-boundary tests showing core modules do not import MiniQMT, DB, FastAPI, or vn.py runtime engines.
- Negative tests proving no silent fallback to MARKET/TWAP/close/default success.
- Production gates reported explicitly.

## 7. DESIGN-COMPLIANCE-001 Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| Record upstream source and license | Section 1; JSON `upstream` | Repo, commit, local path, MIT license recorded | complete | None |
| Maximize direct vn.py source reuse | Sections 2-3; JSON `reuse_policy`, `files` | File-level mapping lists reuse strategy and preserved behavior | complete | Runtime engine intentionally excluded |
| List files requiring direct reuse | Section 3; JSON `files` | Sniper, BestLimit, TWAP, template, base, engine, Iceberg, Stop inventoried | complete | Iceberg/Stop are future candidates only |
| Define characterization test plan | Section 3; JSON per-file `characterization_tests` | Tests listed per mapped file | complete | Tests implemented in later port issues |
| Preserve production safety | Sections 5-6; JSON production gates | No runtime code, no DDL, no dependency changes | complete | None |

## 8. Machine-Readable Summary

```json
{
  "schema_version": "aistock_vnpy_execution_source_inventory_v1",
  "bug_id": "BUG-151",
  "upstream_repo": "https://github.com/vnpy/vnpy_algotrading",
  "upstream_commit": "4133987530eb28f3538d1983545d81c4f83d7d59",
  "license": "MIT License",
  "files": [
    "vnpy_algotrading/algos/sniper_algo.py",
    "vnpy_algotrading/algos/best_limit_algo.py",
    "vnpy_algotrading/algos/twap_algo.py",
    "vnpy_algotrading/template.py",
    "vnpy_algotrading/base.py",
    "vnpy_algotrading/engine.py",
    "vnpy_algotrading/algos/iceberg_algo.py",
    "vnpy_algotrading/algos/stop_algo.py"
  ],
  "policy": "maximize direct source semantics reuse; strip only runtime coupling; prove behavior with characterization tests"
}
```
