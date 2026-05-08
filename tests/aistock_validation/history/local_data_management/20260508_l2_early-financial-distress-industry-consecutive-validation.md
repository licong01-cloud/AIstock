# L2 早期财务风险信号行业分层与连续亏损验证

日期：2026-05-08  
工作树：`F:\Dev\AIstock_worktrees\event-signal-policy-20260507`  
范围：只读研究脚本、测试、研究文档；未接入 QE / Selection / Paper / QMT / 模拟盘  
生产影响：未重启生产 `8001`，未写 DB，未下载 PDF，未调用 LLM

## 用户确认的研究方向

```text
┌──────────────────────────────────────────────┬────────┐
│ 问题                                         │ 结论   │
├──────────────────────────────────────────────┼────────┤
│ 先做亏损/市值 + 行业分层 + 连续亏损          │ 是     │
│ 亏损/市值 >=50% 作为首批离线 QE overlay 候选 │ 是     │
│ 小市值风险单独分层处理                       │ 是     │
│ 财务类信号暂不硬禁止、不强制卖出             │ 是     │
└──────────────────────────────────────────────┴────────┘
```

## 最新报告

- JSON：`reports/event_signal/early_financial_distress/early_financial_distress_20180801_20260507_20260508_091910.json`
- Markdown：`reports/event_signal/early_financial_distress/early_financial_distress_20180801_20260507_20260508_091910.md`
- 研究文档：`docs/analysis/event_signal_early_financial_distress_research_result_20260508.md`

## 运行命令

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

## 验证结果

```text
┌──────────────────────────────────────────┬──────────────────────────────────────────┐
│ 检查项                                   │ 结果                                     │
├──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 全窗口研究脚本                           │ PASS，生成 091910 JSON / Markdown 报告   │
│ py_compile                               │ PASS                                     │
│ focused unit tests                       │ PASS，16 passed in 0.29s                 │
│ event_signal + unified schema regression │ PASS，130 passed in 2.56s                │
│ 交易消费者隔离扫描                       │ PASS，rg exit 1，无匹配                  │
│ git diff --check                         │ PASS，仅既有 LF→CRLF warning             │
└──────────────────────────────────────────┴──────────────────────────────────────────┘
```

## 数据拼接质量

```text
┌────────────────────────────┬────────┬────────┬────────┐
│ 拼接项                     │ 请求键 │ 命中键 │ 命中率 │
├────────────────────────────┼────────┼────────┼────────┤
│ 市值 daily_basic           │ 63,669 │ 59,232 │ 93.03% │
│ 行业 bak_basic/stock_basic │ 63,669 │ 57,641 │ 90.53% │
└────────────────────────────┴────────┴────────┴────────┘
```

## 研究结论

连续亏损报告期数量：

```text
┌────────────────────┬────────┬─────────────┬─────────────┐
│ 过去730天亏损期数  │ 信号数 │ 180日命中率 │ 365日命中率 │
├────────────────────┼────────┼─────────────┼─────────────┤
│ 0                  │ 53,250 │ 1.79%       │ 4.91%       │
│ 1                  │ 4,621  │ 3.18%       │ 6.76%       │
│ 2                  │ 4,328  │ 5.63%       │ 10.64%      │
│ 3                  │ 2,561  │ 5.12%       │ 12.44%      │
│ >=4                │ 5,181  │ 10.47%      │ 24.15%      │
└────────────────────┴────────┴─────────────┴─────────────┘
```

亏损/市值与连续亏损叠加：

```text
┌────────────────────┬────────────────────┬──────┬─────────────┬─────────────┐
│ 亏损/市值          │ 过去730天亏损期数  │ 样本 │ 180日命中率 │ 365日命中率 │
├────────────────────┼────────────────────┼──────┼─────────────┼─────────────┤
│ >=100%             │ >=4                │ 112  │ 34.29%      │ 62.86%      │
│ 50%-100%           │ >=4                │ 176  │ 25.90%      │ 45.12%      │
│ 20%-50%            │ >=4                │ 494  │ 17.11%      │ 30.75%      │
│ 20%-50%            │ 2                  │ 333  │ 11.15%      │ 18.48%      │
│ 10%-20%            │ >=4                │ 610  │ 12.61%      │ 24.63%      │
└────────────────────┴────────────────────┴──────┴─────────────┴─────────────┘
```

候选稳定性：

```text
┌───────────────────────────────┬──────┐
│ 决策标签                      │ 数量 │
├───────────────────────────────┼──────┤
│ qe_overlay_research_candidate │ 23   │
│ needs_threshold_refinement    │ 13   │
│ reject_for_instability        │ 14   │
│ warning_only                  │ 7    │
└───────────────────────────────┴──────┘
```

## 阶段判断

- `亏损/市值 >=50%` 已满足第一批离线 QE overlay 候选研究条件，但不允许进入硬禁止或强制卖出。
- 连续亏损显著提升风险识别能力，`过去730天亏损>=4期` 是下一阶段核心组合变量。
- 小市值应作为分层规则或降权规则研究，不应全市场统一硬处理。
- 行业分层存在价值，但 `industry_unknown` 命中率过高，说明行业缺失样本有退市/缺数据偏差，不能当成真实行业因子。
- 下一步进入离线 QE overlay 前，仍需补行业缺失处理、样本去偏、现金替代逻辑和组合层收益/回撤验证。
