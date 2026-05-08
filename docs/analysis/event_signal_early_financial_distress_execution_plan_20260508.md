# AIstock 早期财务风险信号 2026-05-08 执行方案

日期：2026-05-08  
状态：研究执行方案，限定 `event_signal` 模块，只读验证，不接入交易消费者

## 1. 最终目标

在不修改 QE、Selection Center、Paper v2、模拟盘、QMT 和实盘逻辑的前提下，用本地真实数据回答：

1. `业绩预告 / 业绩快报 / 财务指标` 是否能提前发现未来 ST / 退市风险。
2. 哪些来源和事件类型最有研究价值。
3. 多源共振是否显著提高未来 ST 命中率。
4. 信号后 0/1/5/10/20/60 个交易日收益是否支持风险规避。
5. 财务信号到正式 ST 前，是否已经提前消化跌幅。
6. 哪些规则只能预警，哪些可以进入下一轮 `score_down` / `block_add_candidate` 研究。

## 2. 范围约束

```text
┌──────────────────────────────┬──────────────────────────────────────────────┐
│ 项目                         │ 约束                                         │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ DB                           │ 不建表、不改表、不写入业务数据               │
│ 交易消费者                   │ 不改 QE / Selection / Paper / QMT / 模拟盘    │
│ 信号动作                     │ 不启用 block_buy / force_exit / alpha_boost  │
│ PDF / LLM                    │ 不下载 PDF，不调用 LLM                       │
│ 输出                         │ 只写 docs、只读脚本、tests、validation record │
│ 报告                         │ reports/event_signal 下生成，不纳入交易运行时 │
└──────────────────────────────┴──────────────────────────────────────────────┘
```

## 3. 分阶段执行

```text
┌────────┬──────────────────────────────┬──────────────────────────────┬────────────────────┐
│ 阶段   │ 目标                         │ 产物                         │ 验证               │
├────────┼──────────────────────────────┼──────────────────────────────┼────────────────────┤
│ Phase A│ 固化研究设计和边界           │ research design doc           │ 文档审查           │
│ Phase B│ 覆盖率/命中率/领先时间       │ early research script v1       │ 单测 + 全窗口报告  │
│ Phase C│ 增加收益和 pre-ST 消化研究   │ script return-study 扩展       │ 单测 + 全窗口报告  │
│ Phase D│ 输出研究候选规则             │ candidate_rules JSON/Markdown  │ 禁用硬动作检查     │
│ Phase E│ 形成验证记录和下一步门槛     │ validation record              │ 模块回归 + 隔离扫描│
└────────┴──────────────────────────────┴──────────────────────────────┴────────────────────┘
```

## 4. 脚本设计

主脚本：`backend/services/event_signal/early_financial_distress_research.py`

输入：

- 财务结构化风险信号：`market.event_signal` / `unified_event_signal_rules_v0_20260506`
- ST 目标：`market.event_signal` / `unified_event_signal_rules_st_first_v1_20260506`
- 价格：`market.kline_daily_raw`
- 交易日：`market.trading_calendar`

输出：

- `cycle_coverage`：ST cycle 覆盖率和领先时间
- `precision`：90/180/365 日未来 ST 命中率
- `returns`：T0/T+1/T+5/T+10/T+20/T+60 收益聚合
- `candidate_rules`：研究候选规则，不允许硬交易动作

## 5. 通过标准

```text
┌──────────────────────────────┬──────────────────────────────────────────────┐
│ 检查项                       │ 通过标准                                     │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ 单元测试                     │ early_financial_distress_research tests PASS │
│ 模块回归                     │ backend/tests/event_signal PASS              │
│ 脚本全窗口运行               │ 2018-08-01 到 2026-05-07 成功生成报告       │
│ 隔离扫描                     │ 交易消费者目录无新研究脚本引用             │
│ 业务边界                     │ hard_block / force_exit / alpha_boost 全 false│
│ 生产影响                     │ 不触碰生产 8001，不改 DB schema             │
└──────────────────────────────┴──────────────────────────────────────────────┘
```

## 6. 当前不做

- 不把 `financial_forecast_loss` 或 `financial_express_loss` 直接接入策略。
- 不把多源共振直接做禁止买入。
- 不把任何财务信号做强制卖出。
- 不训练模型。
- 不做 LLM/PDF。

## 7. 下一轮研究方向

如果本轮结果支持，下一轮只继续研究，不接入交易：

1. `financial_forecast_loss` 与 `financial_express_loss` 的阈值细分。
2. 多源共振从“来源数”细化为“来源组合”：forecast+express、forecast+fina、express+fina。
3. 连续报告期亏损、扣非亏损、现金流利润背离、资产负债率压力。
4. 行业/市值分层，避免小盘垃圾股样本主导。
5. 与 QE Loop1 做离线 overlay 分析，但仍不改变 QE 运行时。
