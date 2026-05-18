# L1 MiniQMT 多策略只读账本 POC 验证记录（2026-05-18）

## 范围

- 分支：`codex/miniqmt-multi-strategy-plan-20260518`
- 提交前状态：基于方案提交 `2b7fab7`
- 模块：`backend/services/qmt_strategy_ledger`
- 阶段：Phase 1，只读虚拟账本 POC
- 生产影响：未触碰生产后端 `8001`；未连接 MiniQMT；未下单；未写数据库。

## 业务目标

用 2026-05-18 MiniQMT SIM POC 订单/成交快照，证明 AIstock 可以在不依赖 MiniQMT 原生子账户的情况下，按 `strategy_name`、`order_id` 和 `order_remark` 重建策略级 lot，避免当前 monitor 将账户级同股持仓/PnL 重复计入多个策略。

## 数据样本

- fixture：`backend/tests/qmt_strategy_ledger/fixtures/miniqmt_poc_20260518_summary.json`
- account_id：`62266303`
- trade_date：`2026-05-18`
- 样本订单：17 笔
- 样本成交：30 行
- 覆盖状态：`50` open-like、`54` canceled、`56` filled、`57` rejected
- 覆盖异常：空 `strategy_name`、重复 `order_remark`

## 验证命令

```powershell
python scripts/qmt_strategy_ledger_reconstruct_poc.py --fixture backend/tests/qmt_strategy_ledger/fixtures/miniqmt_poc_20260518_summary.json --out .codex_tmp/qmt_strategy_ledger_poc_report.json --markdown-out .codex_tmp/qmt_strategy_ledger_poc_report.md
rg -n 'place_order|cancel_order|get_qmt_client|psycopg|connect\(|trade_password|QMT_TRADE_PASSWORD' backend/services/qmt_strategy_ledger scripts/qmt_strategy_ledger_reconstruct_poc.py backend/tests/qmt_strategy_ledger
python -m py_compile backend/services/qmt_strategy_ledger/models.py backend/services/qmt_strategy_ledger/reconstruct.py scripts/qmt_strategy_ledger_reconstruct_poc.py
pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider
pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider
```

## 结果

- 重建脚本输出：通过
- 反模式扫描：未发现 `place_order`、`cancel_order`、`get_qmt_client`、`psycopg`、`trade_password`、`QMT_TRADE_PASSWORD` 等 broker/DB/secret 触发点
- `py_compile`：通过
- `pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider`：5 passed
- `pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q -p no:cacheprovider`：24 passed

## 业务断言

| 策略 | 股票 | 重建持仓 | 说明 |
| --- | --- | ---: | --- |
| `pocD_A2a9_132553` | `001358.SZ` | 7,600 | 原始 6,600 + 加仓 1,000 |
| `pocD_A2a9_132553` | `301314.SZ` | 3,800 | Strategy A 原始 lot |
| `pocD_Bcfa_132553` | `001358.SZ` | 6,600 | Strategy B 原始 lot |
| `pocD_Bcfa_132553` | `301314.SZ` | 3,800 | Strategy B 原始 lot |
| `pocD_Coverlap_140844` | `001358.SZ` | 3,300 | 第三策略重叠买入 |
| `pocD_Coverlap_140844` | `301314.SZ` | 1,900 | 第三策略重叠买入 |

- 4 笔 `57` T+1 卖出失败订单未减少任何策略 lot。
- `000685.SZ` 状态 `50` 订单被识别为 open-like，并保留买入冻结资金动作。
- 5 笔 `54` 撤单/取消订单被识别为取消，并释放剩余买入冻结资金动作。
- 空 `strategy_name` 订单进入 `BLANK_STRATEGY_NAME` anomaly。
- 两笔重复 `order_remark` 订单进入 `DUPLICATE_ORDER_REMARK` anomaly。

## 残余风险

- 当前仍为只读内存重建，未实现 `qmt_strategy` 持久化 schema/repository。
- 当前未接入真实 MiniQMT sync API，只验证 fixture 级算法。
- 当前未计算最新价 PnL；Phase 1 重点是 lot 数量、成本和异常归因。
- 托管下单入口、T+1 预检和 UI 分仓视图仍在后续 Phase 3-6。

## 资产安全

- 未修改 StrategyPackage manifest、模型权重、HMM snapshot、QE/RD-Agent 产物、Paper v2 历史 run 或 MiniQMT 账户数据。
- `.codex_tmp/qmt_strategy_ledger_poc_report.*` 为本地临时验证输出，不提交。
