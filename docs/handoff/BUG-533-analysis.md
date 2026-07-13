# BUG-533 分析 - MiniQMT shadow 场景覆盖

## 根因
- `backend/services/simulation_runtime/bridges.py::MiniQMTExecutionBridge.run_shadow_reconciliation` 硬编码 `MiniQMTShadowScenario.DELAY`，生产 SIM shadow 接线只能持久化 `scenario=delay` 的 `SHADOW_RECONCILIATION_REPORTED`。
- `backend/services/simulation_runtime/scheduler.py::_run_miniqmt_shadow_reconciliation_before_submit` 在 B submit 前只调用一次 bridge，因此没有路径产出 D3 canary 硬门要求的 6 个场景。
- `backend/services/miniqmt_execution_runtime/gray.py` 的 `covered_scenarios` 只从已接受的 durable shadow report 派生；如果只有 `delay`，`full_fill` / `partial_55_stream` / `reject` / `cancel` / `disconnect` / `restart_recovery` 永远缺失。本修复不修改 `gray.py`，不降低硬门。

## 修复设计
- 在 `backend/services/miniqmt_execution_runtime/shadow.py` 增加生产可复用 helper：基于同一组真实 `parent_intent` 与 tick 输入派生 scenario replay events，再追加各场景的 terminal / disconnect / restart 事件；未知、空或非法场景 loud fail。
- 将 `MiniQMTExecutionBridge` 参数化为 `run_shadow_reconciliations(..., scenarios=...)`；保留 legacy 单场景 `run_shadow_reconciliation`，但必须显式传入单个 `scenario`；未传场景或多场景误用都会 loud fail，避免静默回退到 `delay`。
- 调度器 MiniQMT SIM shadow 路径显式传入 canary required 6 场景，在 B submit 前为每个场景持久化一份 durable report；run payload 记录 `report_count`、`covered_scenarios`、`durable_event_ids` 与最后一份 report 摘要，便于运营观察。

## 护栏证据
- 未改 `gray.py`：D3 场景覆盖硬门未放宽；回归测试验证这些 report 喂给 `_shadow_evidence_for_scope` 后 `missing_scenarios=[]`。
- 不是纯造假刷绿：场景事件从生产 shadow 输入中的真实 execution plan intent 与 quote/tick payload 派生。
- shadow 仍为 dry-run：测试断言 A/B snapshot 均为 `broker_called=False`、`broker_mutated=False`；B submit 仍单独产生 broker payload，B 权威不变。
- 场景构造 helper 已从测试私有 `_real_replay_events` 模式提炼到生产模块，测试改为复用生产 helper。
- 未触碰 LocalSim、TDX、`client.py` / CompilerAdapter-B 数量逻辑、BUG-531 lotting、D4 switch 逻辑、生产服务或生产 DB/DDL。

## 验收断言
- `MINIQMT_SHADOW_ENABLED` 未开启时保持 inert，不产生 shadow 事件。
- `MINIQMT_SHADOW_ENABLED=true` + MiniQMT SIM 时，生产接线产出 6 个 canary-required scenario 的 durable `SHADOW_RECONCILIATION_REPORTED`。
- 每份 durable report metadata 含 `trade_date`、`portfolio_id`、`strategy_slot_id`、`binding_id`、`run_id`、`execution_plan_id`、`account_group_id`、`scenario`。
- 不支持的 scenario loud fail，reason_code=`MINIQMT_SHADOW_SCENARIO_UNSUPPORTED`；未显式传入 scenario loud fail，reason_code=`MINIQMT_SHADOW_SCENARIO_REQUIRED`，不静默回退到 `delay`。
