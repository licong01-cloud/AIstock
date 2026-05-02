# P0 高优先级：qe_20260501_011054_c90a Loop19+ 审计执行计划

更新日期：2026-05-02

范围说明：本计划只覆盖当前已有 Loop19-28 的回测准确性、IC/RankIC 统计准确性、label_horizon 对齐、未来泄漏风险、Top bucket 转化和分段稳定性。资金规模、容量、冲击成本等专项不在本轮范围，等待后续独立实验后再单独审计。

## 1. 当前已完成事项

```text
Item                       Status  Evidence
-------------------------  ------  ------------------------------------------------------------
Loop19-28 artifact audit   DONE    已生成 P0_qe_20260501_011054_c90a_loop19_28_backtest_accuracy_leakage_audit_20260502.md
IC/RankIC recompute        DONE    pred.pkl/label.pkl 重算结果与 Qlib ic.pkl/ric.pkl 完全一致
Return/account check       DONE    report return 与 account pct_change 差异约 1e-16
Label horizon alignment    DONE    SignalDates 与 ReportRows 的差值分别匹配 5D/10D/20D
Static leakage scan        DONE    factor/model/prepare 代码未发现高风险未来函数模式
Top bucket conversion      DONE    已输出 Top50、Bottom50、D1-D10、LSWin
Year segment snapshot      DONE    已输出 2024/2025/2026 分年 IC、RankIC、收益、Sharpe、MDD
```

## 2. 当前最高优先级继续分析顺序

```text
Priority  Work                         Purpose
--------  ---------------------------  ------------------------------------------------------------
P0        动态截断重算检查              用真实重算确认因子值在 T 日不依赖 T 之后的数据
P0        持仓与 Top bucket 重叠         判断高 RankIC 是否真正进入 top50/top100 持仓
P0        no-fill / tradability 归因     若已有订单诊断 artifact，则核对停牌、涨跌停、价格缺失和精度差异
P0        Loop19/22/26 同屏对比          对比 GRU20D 高 RankIC 与 LSTM10D 高收益的转化差异
P1        市场状态分段                  按指数趋势、波动、回撤窗口拆分 IC 和收益
P1        训练稳定性与 seed             对 NN 模型追加多 seed 计划，判断是否偶然性
P1        成本敏感性                    只做现有回测费率倍增敏感性，不引入资金规模假设
P2        等 Loop1-18 重跑完成后审计     再做 no-alpha / Alpha158 严格对照
```

## 3. 本轮工具和输出

```text
Tool / Artifact                                                                 Purpose
-------------------------------------------------------------------------------  ------------------------------------------------------------
scripts/qe_loop_p0_audit.py                                                      只读审计 Loop artifact，重算 IC/RankIC、收益、label gap、bucket、分段、静态泄漏
C:/Users/lc999/.codex/skills/qe-evolution-diagnostics/scripts/qe_loop_p0_audit.py skill 内置副本，供后续复用
docs/analysis/artifacts/qe_20260501_011054_c90a_loop19_28_p0_audit_20260502.json  结构化结果，便于后续继续追加分析
docs/analysis/P0_qe_20260501_011054_c90a_loop19_28_backtest_accuracy_leakage_audit_20260502.md 当前结论文档
```

## 4. 下一步落地要求

- 动态截断检查必须给出样本日期、因子名、全量值、截断值、差异和结论，不能只靠静态代码扫描。
- no-fill / tradability 只能基于已有真实 artifact 或数据库逐笔核验，不能猜测停牌、涨跌停或缺价原因。
- Loop1-18 full_train 重跑结束前，不再把 no-alpha 与 Alpha158 做最终收益结论。
- 所有新增分析必须继续写入 `docs/analysis`，表格继续使用定宽对齐文本表。
