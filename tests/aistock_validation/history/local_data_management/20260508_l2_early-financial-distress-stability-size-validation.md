# L2 早期财务风险信号稳定性与市值分层验证

日期：2026-05-08  
工作树：`F:\Dev\AIstock_worktrees\event-signal-policy-20260507`  
范围：`backend/services/event_signal/early_financial_distress_research.py` 只读研究脚本、单元测试、研究文档  
生产影响：未重启生产 `8001`，未写 DB，未改 QE / Selection Center / Paper v2 / QMT / 模拟盘

## 验证目标

- 在已有覆盖率、命中率、收益窗口研究基础上，增加候选规则年度稳定性、指标桶 + 来源组合交叉、市值分层、亏损/市值相对强度研究。
- 确认所有新增候选仍为研究标签，不启用 `hard_block`、`force_exit`、`alpha_boost`。
- 确认研究脚本没有被交易消费者引用。

## 最新只读研究报告

- JSON：`reports/event_signal/early_financial_distress/early_financial_distress_20180801_20260507_20260508_011821.json`
- Markdown：`reports/event_signal/early_financial_distress/early_financial_distress_20180801_20260507_20260508_011821.md`
- 研究结果文档：`docs/analysis/event_signal_early_financial_distress_research_result_20260508.md`

## 执行命令

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
$env:TDX_DB_HOST='127.0.0.1'
$env:TDX_DB_PORT='5432'
$env:TDX_DB_NAME='aistock'
$env:TDX_DB_USER='postgres'
$env:TDX_DB_PASSWORD='***'

python -m backend.services.event_signal.early_financial_distress_research `
  --start-date 2018-08-01 `
  --end-date 2026-05-07 `
  --lookback-days 365 `
  --cycle-gap-days 180 `
  --output-dir reports/event_signal/early_financial_distress

python -m py_compile `
  backend/services/event_signal/early_financial_distress_research.py `
  backend/tests/event_signal/test_early_financial_distress_research.py

python -m pytest backend/tests/event_signal/test_early_financial_distress_research.py -q

python -m pytest `
  backend/tests/test_unified_event_signal_schema.py `
  backend/tests/event_signal -q

rg -n "early_financial_distress|financial_distress" `
  backend/services/selection_center `
  backend/services/paper_trading_v2 `
  backend/services/quantevolver `
  backend/infra/qmt_client.py `
  backend/routers/qmt.py -S

git diff --check
```

## 命令结果

```text
┌──────────────────────────────────────────┬──────────────────────────────────────────────┐
│ 检查项                                   │ 结果                                         │
├──────────────────────────────────────────┼──────────────────────────────────────────────┤
│ 全窗口研究脚本                           │ PASS，生成 011821 JSON / Markdown 报告       │
│ py_compile                               │ PASS                                         │
│ focused unit tests                       │ PASS，13 passed in 0.31s                     │
│ event_signal + unified schema regression │ PASS，127 passed in 1.56s                    │
│ 交易消费者隔离扫描                       │ PASS，rg exit 1，无匹配                      │
│ git diff --check                         │ PASS，仅既有 LF→CRLF warning                 │
└──────────────────────────────────────────┴──────────────────────────────────────────────┘
```

## 关键研究结果

基础样本：

```text
┌──────────────────────────────┬────────┐
│ 指标                         │ 数值   │
├──────────────────────────────┼────────┤
│ 财务风险信号                 │ 69,941 │
│ ST / 退市风险原始事件        │ 11,290 │
│ ST / 退市风险 cycles         │ 1,366  │
│ 365 天内有提前财务信号 cycles│ 1,251  │
│ cycle 覆盖率                 │ 91.58% │
└──────────────────────────────┴────────┘
```

候选稳定性标签：

```text
┌───────────────────────────────┬──────┐
│ 决策标签                      │ 数量 │
├───────────────────────────────┼──────┤
│ qe_overlay_research_candidate │ 14   │
│ needs_threshold_refinement    │ 12   │
│ reject_for_instability        │ 9    │
│ warning_only                  │ 7    │
└───────────────────────────────┴──────┘
```

市值分层与相对亏损强度：

```text
┌────────────────────────┬────────┬─────────────┬─────────────┐
│ 规则/分层              │ 信号数 │ 180日命中率 │ 365日命中率 │
├────────────────────────┼────────┼─────────────┼─────────────┤
│ 全体信号，市值 <5亿    │ 43,824 │ 3.71%       │ 8.98%       │
│ 全体信号，市值 5-10亿  │ 11,683 │ 1.94%       │ 4.71%       │
│ 续亏>=10亿，市值 <5亿  │ 305    │ 27.88%      │ 49.62%      │
│ 续亏>=10亿，市值 5-10亿│ 154    │ 14.96%      │ 28.10%      │
│ 亏损/市值 >=100%       │ 251    │ 31.38%      │ 52.10%      │
│ 亏损/市值 50%-100%     │ 425    │ 20.00%      │ 37.03%      │
│ 亏损/市值 20%-50%      │ 1,340  │ 11.43%      │ 21.82%      │
└────────────────────────┴────────┴─────────────┴─────────────┘
```

## 验证结论

- 结构化财务信号适合作为早期风险雷达，但不能整体硬禁止或强制卖出。
- `financial_forecast_loss` 的有效部分不是“亏损”本身，而是 `续亏 + 亏损规模`、`亏损/市值`、小市值分层共同定义的高风险子集。
- `亏损/市值` 命中率呈明显单调性，是比绝对亏损金额更合理的下一阶段核心研究变量。
- `financial_express_loss` 有收益风险特征，但 2023-2025 的未来 ST 命中率明显下降，暂时只能保留为阈值细化对象。
- 所有候选均保持研究属性：`hard_block_allowed=false`、`force_exit_allowed=false`、`alpha_boost_allowed=false`。

## 残余风险

- 市值用信号生效交易日的 `daily_basic`，匹配率为 93.03%，未匹配样本进入 `mv_unknown`。
- 当前还未做行业分层、连续报告期亏损、扣非净利/现金流质量确认。
- 当前还未做离线 QE Loop overlay，不能推断对年化收益和最大回撤的真实影响。
- PDF/LLM 仍未进入本阶段，公告正文中可能存在的例外情况尚未利用。
