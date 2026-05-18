# L6 MiniQMT 虚拟策略 UI 看板验证记录 - 2026-05-18
## 范围

- 分支：`codex/miniqmt-multi-strategy-plan-20260518`
- 阶段：Phase 6，MiniQMT 虚拟策略 UI 看板
- 变更文件：
  - `backend/routers/qmt_strategy_ledger.py`
  - `backend/tests/qmt_strategy_ledger/test_router_summary.py`
  - `frontend/src/app/qmt/virtual-strategies/layout.tsx`
  - `frontend/src/app/qmt/virtual-strategies/page.tsx`
  - `frontend/src/lib/qmt-strategy-ledger/api.ts`
  - `frontend/src/lib/navigation/nav-groups.ts`
- 生产影响：未触碰生产后端 `8001`；未连接真实 MiniQMT；未真实下单；未执行撤单；未写生产数据库。

## 验证目标

Phase 6 的目标是把 MiniQMT 原生账户视角和 AIstock 虚拟策略账本视角放到同一看板里，并且只暴露 read-only sync / reconciliation / package binding / preview，不暴露真实 submit/cancel。

检查点：

- 可以读取虚拟策略汇总，展示策略现金、冻结、收益、active binding、lot 汇总和同股多策略重叠。
- 可以通过 `/sync-snapshot` 只读同步 MiniQMT 快照。
- 可以通过 `/reconciliation` 输出对账异常和 broker vs 策略 lot 数量差异。
- 可以通过 package binding 生成绑定，并通过 binding 生成订单预检。
- 页面不提供真实 submit/cancel 入口。

## 验证命令

```powershell
python -m py_compile backend/services/qmt_strategy_ledger/models.py backend/services/qmt_strategy_ledger/repository.py backend/services/qmt_strategy_ledger/sync_service.py backend/services/qmt_strategy_ledger/reconciliation.py backend/services/qmt_strategy_ledger/order_service.py backend/services/qmt_strategy_ledger/package_binding.py backend/services/qmt_strategy_ledger/selection_order_builder.py backend/routers/qmt_strategy_ledger.py backend/tests/qmt_strategy_ledger/test_router_summary.py backend/tests/qmt_strategy_ledger/test_repository.py backend/tests/qmt_strategy_ledger/test_package_binding.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py backend/tests/qmt_strategy_ledger/test_sync_service.py backend/tests/qmt_strategy_ledger/test_reconciliation.py backend/tests/qmt_strategy_ledger/test_order_service_preflight.py backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py backend/tests/qmt_strategy_ledger/test_router_managed_orders_guard.py
python -m pytest backend/tests/qmt_strategy_ledger/test_router_summary.py -q -p no:cacheprovider
python -m pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider
python -m pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider
node ./node_modules/next/dist/bin/next build
rg -n 'trade_password|QMT_TRADE_PASSWORD' backend/services/qmt_strategy_ledger backend/routers/qmt_strategy_ledger.py backend/tests/qmt_strategy_ledger frontend/src/app/qmt/virtual-strategies frontend/src/lib/qmt-strategy-ledger
git diff --check
```

## 结果

- `python -m py_compile ...`：通过。
- `pytest backend/tests/qmt_strategy_ledger/test_router_summary.py -q -p no:cacheprovider`：1 passed。
- `pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider`：35 passed。
- `pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider`：24 passed。
- `next build`：通过；仅有仓库里既存的 React Hook warning，没有新增编译错误。
- secret scan：未发现 `trade_password` / `QMT_TRADE_PASSWORD`。
- `git diff --check`：通过。

## 关键业务结论

- 页面已清楚区分 “MiniQMT 原生账户视角” 和 “AIstock 虚拟策略账户视角”。
- 页面只提供只读同步、对账、绑定和订单预检，不暴露真实提交/撤单按钮。
- 同股多策略、策略 lot、broker 合并数量、未归因订单/成交都可以在看板里观察。

## 残余风险

- 仍依赖用户在页面中提供正确的 `account_id`、`strategy_id`、`package_id` 和 `selection_run_id`。
- `next build` 仍然会打印仓库里其他页面的既有 hook warning，这些不属于本次 Phase 6 变更。
- 本次只完成 UI 和只读对账链路，真实下单仍然保持后端环境变量双重关闭。
