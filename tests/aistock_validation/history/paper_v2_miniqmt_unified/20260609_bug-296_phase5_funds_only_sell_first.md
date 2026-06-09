# BUG-296 Phase 5 funds-only capacity 与 SELL-first dependent-buy 验收记录

- BUG: BUG-296 / GitHub #871
- 分支: `bug/BUG-296-p0-miniqmt-phase5-funds-only-capacity-and-sell-f-20260609`
- 日期: 2026-06-09
- 阶段: Phase 5 - 资金容量与 SELL-first proceeds 模型
- 设计文档: `docs/architecture/miniqmt_unified_vnpy_execution_runtime_design_20260608.md`
- 生产影响: 未连接生产 MiniQMT，未修改 DB schema，未写生产库，未重启 backend/frontend/TDX/MiniQMT

## 1. 修复目标

BUG-296 覆盖 BUG-210 父门禁中的 Phase 5 阻断项：多策略 MiniQMT 不能继续依赖固定策略数量门槛，也不能把同批卖出订单的估算 proceeds 当作已成交资金直接提交依赖买单。

本次修复目标：

- 保留 SELL-first 提交顺序。
- same-batch sell proceeds 只能作为容量诊断和重试依据，不能在卖单未成交/未对账前让依赖买单直接触达 broker。
- 资金不足时给出 intent/slot 级显式状态 `SELL_PROCEEDS_REQUIRED` 或 `ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED`。
- 卖单成交并对账释放现金后，确定性 batch retry 可以只提交之前 deferred 的依赖买单，不重复提交已成功的卖单。
- MiniQMT account-group slot 数量不再由固定 package count 决定；受资金容量和交易规则限制。

## 2. 设计追踪矩阵

| 设计项 | 设计章节 | 实现文件 | 测试或证据 | 状态 |
|---|---|---|---|---|
| 策略数量只受资金容量和交易规则限制 | 3.3, 10.8 Phase 5, 11.1 | `backend/services/paper_trading_v2/broker/minqmtsim.py`, `backend/services/qmt_strategy_ledger/repository.py` | `test_account_group_slot_count_is_governed_by_cash_not_fixed_package_gate`; grep guard 仍命中 legacy/local compatibility 见第 6 节 | PASS |
| 资金不足为显式状态，不静默成功 | 3.3, 14 | `backend/services/qmt_strategy_ledger/order_service.py` | `SELL_PROCEEDS_REQUIRED`, `ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED`; batch metadata `dependent_buy_deferred=true` | PASS |
| SELL-first batch submit | 10.8 Phase 5, 11.2 | `backend/services/qmt_strategy_ledger/order_service.py` | `test_submit_batch_defers_dependent_buy_until_sell_proceeds_reconciled` 验证 broker payload 只有 SELL，且 SELL 在前 | PASS |
| 未成交卖单不能释放 proceeds 给依赖买单 | 10.8 Phase 5, 14 | `backend/services/qmt_strategy_ledger/order_service.py` | 低现金 rebalance 中 BUY 不创建 intent、不调用 broker，错误上下文包含 dependent sell orders | PASS |
| partial / residual 具备可恢复重试语义 | 10.8 Phase 5, 11.2 | `backend/services/qmt_strategy_ledger/order_service.py` | `test_submit_batch_dependent_buy_retry_submits_after_sell_proceeds_reconciled` 验证卖出后对账现金可重试依赖买单，且不重复提交卖单 | PASS |
| same-batch proceeds 参与 preflight 但不伪装可用现金 | 10.8 Phase 5 | `backend/services/qmt_strategy_ledger/order_service.py` | preflight context 记录 `same_batch_estimated_sell_proceeds`、`effective_cash`、`batch_required_cash`、`dependent_sell_orders` | PASS |
| 不引入新 MiniQMT 产品下单路径 | 3.1, 10.8 Phase 4/5 | 未新增 router/adapter owner | 本次只修改 managed-order OMS/preflight 和 MiniQMTSim capacity 描述 | PASS |

## 3. 正向验证

- `python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py -q` -> 11 passed
- `python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py backend/tests/qmt_strategy_ledger/test_order_service_preflight.py backend/tests/qmt_strategy_ledger/test_account_group_slots.py -q` -> 34 passed
- `python -m pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q` -> 43 passed
- `python -m compileall -q backend/services/qmt_strategy_ledger backend/services/paper_trading_v2/broker backend/tests/qmt_strategy_ledger` -> passed

覆盖能力：

- 低现金全仓换股：SELL 先提交，BUY 被标记 `SELL_PROCEEDS_REQUIRED`，不创建 BUY intent，不触达 broker。
- 已有现金可覆盖第一笔 BUY：独立 BUY 可以提交，后续需要卖出 proceeds 的 BUY 继续 deferred。
- 卖单成交对账后：同一 deterministic batch retry 只提交 deferred BUY，不重复提交已成功 SELL。
- account-group slot 数量：80 个 slot 在现金上限覆盖时可创建，证明没有 1/2/64 固定策略数产品门槛。

## 4. 负向验证

- `test_submit_batch_defers_dependent_buy_until_sell_proceeds_reconciled` 阻断 “卖单仅 accepted 就提交依赖买单”。
- `test_submit_batch_dependent_buy_retry_submits_after_sell_proceeds_reconciled` 阻断 “失败 batch 永远复读 deferred 结果”。
- `test_submit_batch_uses_available_cash_for_independent_buys_before_dependent_buy` 阻断 “同批所有 BUY 因一个依赖 proceeds 的 BUY 被全量阻断”。
- `test_submit_batch_aggregates_cash_before_broker_call` 和 `test_submit_batch_rejects_account_group_cash_overcommit_across_strategy_slots` 保持无 proceeds 时仍 fail-fast，不触达 broker。
- `test_submit_batch_preflight_failure_keeps_broker_called_false_for_restart_retry` 保持硬 preflight failure 不被缓存成成功 retry。

## 5. 实现说明

- `QmtManagedOrderService._batch_preflight()` 仍计算同批 sell proceeds，但当累计 BUY freeze 超过当前真实现金时，移除旧的 `INSUFFICIENT_CASH` 静默通过语义，改为显式 `SELL_PROCEEDS_REQUIRED`。
- 对 account-group cash limit 同理记录 `ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED`，避免跨 slot 把未成交卖出 proceeds 当作 group 可用资金。
- `submit_batch()` 将只有 dependent-buy proceeds 错误的 BUY 作为 deferred item：不创建 intent，不冻结现金，不调用 broker；SELL 和已由真实现金覆盖的 BUY 可继续提交。
- 对含 deferred BUY 的 partial batch，`compensation_required=false`，因为 SELL-first 是预期阶段性结果，不是 broker 部分失败需要撤单补偿。
- `_retry_dependent_buy_batch()` 允许同一 batch 在卖单成交并对账补足现金后只重试 deferred BUY；如果现金仍不足，继续保留原始 proceeds-required 诊断。
- `MiniQMTSimBackend.bind_capacity()` 的 account-group slots 容量描述改为 funds/trading-rule governed，不再把 `64` 作为产品门槛；legacy exclusive-account 的 `1` 保留为旧兼容模式说明，不是 account-group 产品路径。

## 6. 静态扫描

执行：

```powershell
rg -n "max_concurrent_packages|package_count\s*>|strategy_count\s*>" backend/services backend/routers
```

结果解释：

- `backend/services/paper_trading_v2/broker/minqmtsim.py` account-group slots: `max_concurrent_packages=1_000_000_000`，说明不再使用 64 作为固定策略数门槛；真实限制来自 slot funds/preflight。
- `backend/services/paper_trading_v2/broker/minqmtsim.py` legacy exclusive-account: `max_concurrent_packages=1`，仅保留旧 exclusive-account compatibility。
- `backend/services/paper_trading_v2/broker/localsim.py`: LocalSim per-portfolio compatibility，不属于 MiniQMT account-group 多策略产品路径。
- `backend/services/paper_trading_v2/broker/base.py` 和 `backend/services/simulation_runtime/models.py`: schema/guard keys，用于阻断固定策略数 gate 泄漏。

## 7. DESIGN-COMPLIANCE-001

| 检查项 | 状态 | 说明 |
|---|---|---|
| 完整实现当前 Phase 5 scope | PASS | 本 issue 限定 funds-only capacity、SELL-first proceeds、dependent-buy retry；未声明 Phase 6/7 完成 |
| 唯一路径 | PASS | 未新增 MiniQMT 执行入口；仍通过 runtime/managed OMS 边界 |
| 设计一致 | PASS | alpha/signal 层未新增 broker/order/cash 字段 |
| vn.py 复用 | PASS with boundary | 本 issue 是 OMS/cash model 阶段；不修改 Phase 3 vn.py-derived algo core |
| 无 silent fallback | PASS | dependent BUY 明确失败/等待，不以 batch success 或 broker_called=true 伪装成功 |
| 可恢复 | PASS | partial deferred batch 可在现金对账后 deterministic retry，且不重复提交已成功 SELL |
| 资金安全 | PASS | 不超真实现金冻结，不假设未成交 sell proceeds；account-group funds-only 测试覆盖 |
| 生产门禁 | PASS | DDL/依赖/重启均为 noop；未触碰生产 DB/runtime |

## 8. 生产门禁

- `production_ddl_gate`: noop。没有 SQL migration，没有 DB schema 变更。
- `production_frontend_dependency_gate`: noop。未改前端依赖。
- `production_backend_dependency_gate`: noop。未改 Python/Conda 依赖。
- 服务重启: 不需要；也未执行 backend/frontend/TDX/MiniQMT 重启。
- 生产 DB: 未读写生产库，未执行 DDL。

## 9. 剩余边界

- 本 issue 不关闭 BUG-210 父项；BUG-210 仍需 Phase 6 operator command runtime 化、Phase 7 L0-L5 总验收与 legacy deprecation 用户确认。
- 本 issue 不处理 LocalSim BUG-285；用户当前要求只处理 Paper v2 / MiniQMT 相关任务。
- 真盘时段的 L5 MiniQMT SIM 行为仍需在 Phase 7 或明日交易窗口中记录，不以本地 fake broker 测试替代。
