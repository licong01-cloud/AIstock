# P0 高优先级：qe_20260501_011054_c90a IC/RankIC 与收益转化审计计划

更新日期：2026-05-02

范围说明：本计划只分析当前已有 Loop19-28 的信号质量、收益转化、统计准确性和泄漏风险。资金规模相关审计等待独立实验后再处理，不混入本轮结论。

## 1. 当前证据摘要

```text
Check                         Result
-----------------------------  ------------------------------------------------------------
IC/RankIC 统计准确性           pred.pkl/label.pkl 重算均值与 Qlib artifact 完全一致
Enhanced summary 准确性        enhanced IC/RankIC 与重算均值差异为 0 或 1e-17 量级
回测日收益准确性               report return 与 account pct_change 最大差异约 1e-16
最终账户值准确性               report final account 与 enhanced final_total_value 仅有小数级四舍五入差异
label_horizon 日期对齐         5D/10D/20D 的 SignalDates 与 ReportRows 差值完全匹配
静态未来泄漏扫描              factor/model/prepare 代码未发现 shift(-n)、未来 Ref、centered rolling 等高风险模式
```

## 2. IC/RankIC 与收益不完全一致的解释框架

```text
Scenario                         Meaning                                      Follow-up
--------------------------------  -------------------------------------------  ------------------------------------------------------------
RankIC 高且 Top50 强             全市场排序和 top tail 都有效                 优先优化组合转化和持仓选择
RankIC 高但收益一般              排序有效但 top50/交易路径转化不足             检查持仓重叠、换手、现金拖累、no-fill
RankIC 中等但收益最高            top tail 捕捉或组合路径更好                  检查是否少数阶段贡献过高
IC 高但 RankIC/收益弱            线性相关强但排序或 top tail 不稳定            检查极端值、截面分布和分桶单调性
分年 IC 稳定但分年收益分化       信号稳定，执行/市场状态影响更大              做市场状态与回撤窗口拆分
```

## 3. 当前 Loop19-28 的重点对比对象

```text
Loop  Model  Horizon  Role
----  -----  -------  ------------------------------------------------------------
19    GRU    20D      高 Top50 转化样本，作为 GRU20D 基准
22    GRU    20D      最高 RankIC/Top50 强样本，检验 GRU20D 稳定性
26    LSTM   10D      最高 CAGR/Sharpe 样本，检验收益转化为何优于 RankIC 更高的模型
24    TCN    10D      高收益但回撤偏高样本，检查训练和组合风险
25    CAT    10D      最高 IC 样本，检查高 IC 是否进入 top bucket
27    XGB    10D      高 IC 但回撤最高样本，检查收益路径和风险暴露
```

## 4. 必须追加的验证

```text
Priority  Validation                  Required Output
--------  --------------------------  ------------------------------------------------------------
P0        动态截断重算                 factor/date 级别全量值、截断值、差异、是否可复现
P0        持仓与 Top50/Top100 重叠      每日 overlap ratio、均值、中位数、低重叠日期样本
P0        no-fill 逐笔归因             suspend_d、limit、DB close、minute close、Qlib close、prev_close、原因分类
P1        市场状态分段                 bull/bear/sideways/high-vol/low-vol 下 IC、RankIC、收益、Sharpe、MDD
P1        seed 稳定性                  同配置多 seed 的 IC、RankIC、CAGR、MDD 分布
P1        成本敏感性                  现有费率倍增后的 CAGR、Sharpe、MDD，不引入资金规模假设
```

## 5. 输出要求

- 当前结论文档：`docs/analysis/P0_qe_20260501_011054_c90a_loop19_28_backtest_accuracy_leakage_audit_20260502.md`
- 结构化结果：`docs/analysis/artifacts/qe_20260501_011054_c90a_loop19_28_p0_audit_20260502.json`
- 新增脚本必须同步补充到 `qe-evolution-diagnostics` skill。
- 所有表格必须使用定宽对齐文本表，便于后续复制和对比。
