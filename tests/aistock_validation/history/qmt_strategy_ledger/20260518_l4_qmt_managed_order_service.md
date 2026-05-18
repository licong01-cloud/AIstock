# L4 MiniQMT 多策略托管下单服务验证记录（2026-05-18）

## 范围

- 分支：`codex/miniqmt-multi-strategy-plan-20260518`
- 阶段：Phase 4，托管下单入口
- 模块：
  - `backend/services/qmt_strategy_ledger/order_service.py`
  - `backend/routers/qmt_strategy_ledger.py`
  - `backend/services/qmt_strategy_ledger/repository.py`
  - `backend/tests/qmt_strategy_ledger/test_order_service_preflight.py`
  - `backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py`
  - `backend/tests/qmt_strategy_ledger/test_router_managed_orders_guard.py`
- 生产影响：未触碰生产后端 `8001`；未连接真实 MiniQMT；未真实下单；未执行 migration；未写生产数据库。

## 验证目标

Phase 4 的目标是实现托管下单的本地风控和账本写入边界，并保留真实 MiniQMT SIM POC 的显式开关：

- `orders/preview` 只做本地预检，不调用 broker。
- `submit_order` 先通过策略虚拟账户预检，再创建 intent、冻结现金、调用 broker、回写 order ledger/status event。
- `submit_batch` 逐笔执行，支持部分成功并给出补偿提示，不自动撤单。
- `cancel_order` 调用 broker cancel 后释放本地冻结现金，并写状态事件。
- Router 默认禁止真实 broker submit/cancel；必须设置 `AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS=1` 或 `AISTOCK_ALLOW_MINIQMT_SUBMIT_TEST=1`。
- 即使打开 SIM 提交开关，`LIVE` mode 仍默认禁止，除非另设 `AISTOCK_ALLOW_MINIQMT_LIVE_MANAGED_ORDERS=1`。

## 验证命令

```powershell
python -m py_compile backend/services/qmt_strategy_ledger/order_service.py backend/services/qmt_strategy_ledger/repository.py backend/routers/qmt_strategy_ledger.py
pytest backend/tests/qmt_strategy_ledger/test_order_service_preflight.py backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py backend/tests/qmt_strategy_ledger/test_router_managed_orders_guard.py -q -p no:cacheprovider
pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider
pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider
rg -n 'trade_password|QMT_TRADE_PASSWORD' backend/services/qmt_strategy_ledger backend/routers/qmt_strategy_ledger.py backend/tests/qmt_strategy_ledger
git diff --check
```

## 结果

- Phase 4 targeted tests：11 passed
- `pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider`：27 passed
- `pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider`：24 passed
- secret 扫描：未发现 `trade_password` / `QMT_TRADE_PASSWORD`
- `place_order` / `cancel_order` 仅存在于 managed order service 的显式 broker 边界和 fake broker tests，router submit/cancel 默认 403。
- `git diff --check`：通过

## 关键断言

- 空 `strategy_name` 被本地拒绝，broker 未调用。
- 重复 `order_remark` 被本地拒绝，broker 未调用。
- 买入资金不足和非 100 股整数倍被本地拒绝。
- 策略 T+1 可卖 lot 不足被本地拒绝。
- MiniQMT 账户级 `can_sell` 不足时，submit 前拒绝且 broker `place_order` 未调用。
- fake broker 成功返回 order id 后，本地创建 intent，冻结现金，写 order ledger/status event。
- fake broker 部分成功时，batch 标记 partial，需要人工补偿，不自动撤单。
- fake cancel 成功后释放冻结现金，intent 状态变为 `CANCELLED`。
- Router `orders/preview` 默认可用；Router `orders` 默认 403；LIVE mode 默认 403。

## 真实 MiniQMT SIM POC 前置条件

真实 SIM 验证不属于自动测试默认路径。执行前必须满足：

1. 使用开发后端端口，不触碰生产 `8001`。
2. 用户明确指定股票、数量、价格策略、策略名和 order_remark。
3. 设置 `AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS=1` 或 `AISTOCK_ALLOW_MINIQMT_SUBMIT_TEST=1`。
4. 请求 `mode=SIM`。
5. 下单前先执行只读 `sync-snapshot` 和 `reconciliation`。
6. 下单后再次执行只读同步和对账，记录订单、成交、冻结资金和 lot 变化。

## 残余风险

- 本阶段未执行真实 MiniQMT SIM 下单，真实 xtquant 返回字段仍需 POC 验证。
- 本阶段未实现 StrategyPackage/Selection Run 自动生成订单意图；该能力属于 Phase 5。
- 本阶段没有前端 UI；操作入口为后端 API 和服务层测试。
