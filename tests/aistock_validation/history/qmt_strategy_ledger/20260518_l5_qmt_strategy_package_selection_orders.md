# L5 MiniQMT 多策略 StrategyPackage/Selection 订单构建验证记录（2026-05-18）

## 范围

- 分支：`codex/miniqmt-multi-strategy-plan-20260518`
- 阶段：Phase 5，StrategyPackage / Selection Center 接入
- 模块：
  - `backend/services/qmt_strategy_ledger/package_binding.py`
  - `backend/services/qmt_strategy_ledger/selection_order_builder.py`
  - `backend/routers/qmt_strategy_ledger.py`
  - `backend/services/qmt_strategy_ledger/repository.py`
  - `backend/tests/qmt_strategy_ledger/test_package_binding.py`
  - `backend/tests/qmt_strategy_ledger/test_selection_order_builder.py`
- 生产影响：未触碰生产后端 `8001`；未连接真实 MiniQMT；未真实下单；未执行 migration；未写生产数据库。

## 验证目标

Phase 5 的目标是把可用 StrategyPackage 和 Selection Run 转换为托管订单请求，而不是直接下单：

- 虚拟账户可绑定 `package_id`、`manifest_sha256`、`selection_run_id`、`target_weight`、`top_k`。
- 绑定前校验 StrategyPackage 状态可用于 selection/paper，Selection Run 已成功，package 与 manifest hash 匹配。
- 订单构建优先使用 `SelectionCandidate.target_quantity`。
- 只有 `target_weight` 时，使用虚拟账户 `initial_cash * weight / reference_price` 估算目标股数，并按 A 股 board lot 取整。
- 已有策略 lot 会转换为差额订单：目标小于当前仓位时生成 SELL。
- 支持 top-k 裁剪。
- 缺价格、缺 active binding、Selection Run 未成功等 fail-fast。
- 生成结果只进入 `ManagedOrderRequest` / preview，不触发 broker。

## 验证命令

```powershell
python -m py_compile backend/services/qmt_strategy_ledger/package_binding.py backend/services/qmt_strategy_ledger/selection_order_builder.py backend/routers/qmt_strategy_ledger.py
pytest backend/tests/qmt_strategy_ledger/test_package_binding.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py -q -p no:cacheprovider
pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider
pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider
rg -n 'trade_password|QMT_TRADE_PASSWORD' backend/services/qmt_strategy_ledger backend/routers/qmt_strategy_ledger.py backend/tests/qmt_strategy_ledger
git diff --check
```

## 结果

- Phase 5 targeted tests：7 passed
- `pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider`：34 passed
- `pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider`：24 passed
- secret 扫描：未发现 `trade_password` / `QMT_TRADE_PASSWORD`
- `git diff --check`：通过

## 关键断言

- `target_quantity=1000` 的候选生成 1000 股 BUY 请求。
- `initial_cash=10,000,000`、`target_weight=0.02`、`reference_price=26.31` 时生成 7,600 股 BUY 请求，可复刻 POC 中 10,000,000 资金、约 2% 仓位的数量级。
- 当前已有 1,500 股、目标 1,000 股时生成 500 股 SELL 请求。
- `top_k=2` 只生成前两个候选的请求。
- package 为 `DRAFT`、Selection Run 为 `FAILED`、manifest hash 不匹配、候选缺价格均 fail-fast。

## 残余风险

- 本阶段未直接调用 StrategyPackage 运行推理，只消费 Selection Run 已持久化结果。
- 本阶段未自动提交订单；真实提交仍由 Phase 4 托管订单接口和显式 SIM 开关控制。
- 本阶段未实现前端 UI；操作入口为后端 API 和服务层测试。
