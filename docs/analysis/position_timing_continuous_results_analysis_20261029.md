# 连续账户研究：只读分析交付及暂定结果

日期：2026-10-29。模块：position_timing。设计：
`docs/architecture/position_timing_continuous_results_analysis_f1_20261029.md`。

## 1. 结论与证据等级

本轮交付离线分析入口，不重跑 LocalSIM、不改执行政策，也没有启用新的运行态能力。
研究覆盖 2026-01-05～2026-09-30，181 日，13 账户，2353 账户日，两个家族共 24 个比较。
初始现金均为 1000 万元，公共终值时点 14:57。主延迟沿用事前冻结的 60 秒，不选收益最好的延迟。

**结果为 PROVISIONAL 描述性证据，不构成经济确认。** LocalSIM 已完成账户日物化，但原 exact retry 在发布
`results.json` 时发生 `LOCALSIM_TIMING_IMMUTABLE_ARTIFACT_CONFLICT`，仍为 FAILED / EXACT_RETRY。
本轮分析报告的 exact retry 已通过；二者不是同一验收。旧研究及其失败状态均未改写。

## 2. 横向结果

收益与 MDD 为百分比，MDD 为正 peak-to-trough；费用单位元。FILLED/PARTIAL/NO_FILL 为计划数量。
这是反馈闭环账户，计划数量和持仓路径会分化，不能按相同交易量解释为纯择时 alpha。

| 规则 | 延迟秒 | 净收益 % | 最大回撤 % | 费用元 | FILLED / PARTIAL / NO_FILL |
|---|---:|---:|---:|---:|---|
| native TWAP | — | -0.2398784 | 27.560668 | 405273.78 | 6364 / 6 / 42 |
| B0 FIRST_OBSERVABLE | 0 | 1.4108480 | 1.908295 | 23578.20 | 363 / 0 / 8329 |
| B0 FIRST_OBSERVABLE | 60 | 0.6916991 | 1.107552 | 11721.09 | 182 / 0 / 8730 |
| B0 FIRST_OBSERVABLE | 180 | 0 | 0 | 0 | 0 / 0 / 9050 |
| B0 FIRST_OBSERVABLE | 300 | 0 | 0 | 0 | 0 / 0 / 9050 |
| C1 PRICE_TOUCH | 0 | -5.4016905 | 18.432972 | 103921.07 | 1684 / 318 / 5614 |
| C1 PRICE_TOUCH | 60 | -6.5794383 | 19.967483 | 73812.80 | 1228 / 161 / 7017 |
| C1 PRICE_TOUCH | 180 | 0 | 0 | 0 | 0 / 0 / 9050 |
| C1 PRICE_TOUCH | 300 | 0 | 0 | 0 | 0 / 0 / 9050 |
| C2 BOUNDED_REVERSION | 0 | -5.1926259 | 13.519756 | 56234.80 | 901 / 199 / 6883 |
| C2 BOUNDED_REVERSION | 60 | -0.1512434 | 2.727410 | 15684.38 | 243 / 13 / 8480 |
| C2 BOUNDED_REVERSION | 180 | 0 | 0 | 0 | 0 / 0 / 9050 |
| C2 BOUNDED_REVERSION | 300 | 0 | 0 | 0 | 0 / 0 / 9050 |

主延迟 60 秒，同延迟 B0 基线：C1 净增量 **-727.11374 bps**、回撤恶化 **18.859930 个百分点**；
C2 净增量 **-84.29425 bps**、回撤恶化 **1.619858 个百分点**。这轮不支持将 C1/C2 宣称为已证明更优的提醒规则。

对 native 工作流，B0/60 增量 +93.15775 bps、C1/60 为 -633.95599 bps、C2/60 为 +8.86350 bps。
此比较包含工作流、成交覆盖、费用、资金暴露和反馈路径差异，不等于孤立的择时能力增益。
低成交与低回撤不可单独当作优点；本轮没有重建逐分钟现金利用率，不能据此给出平均仓位的精确数值。

### BUY/SELL 条件执行诊断

相对 B0/60，负数表示成交价格成本更优，正数更差；只统计双方实际有成交的交集。

| 候选 | BUY 价差成本 bps | BUY 配对/并集 | SELL 价差成本 bps | SELL 配对/并集 |
|---|---:|---:|---:|---:|
| C1/60 | -13.910066 | 8 / 8901 | -15.144390 | 19 / 2805 |
| C2/60 | +6.933775 | 19 / 8894 | -3.033908 | 29 / 340 |

C1 的已成交子集价格更优，却整体净值更差。这不是矛盾：配对样本很小，错失成交、部分成交、数量和账户路径均不同。
不得只展示成交交集而隐藏并集、NO_FILL 或缺失比例；BUY/SELL 不拆分归因账户净收益和回撤。

### 长延迟的解释边界

180/300 秒六账户均零成交、零费用、零收益。它们没有提供“延迟下仍然有效”的证据，更不是零风险策略。
例如 180 秒 OPPORTUNITY_EXPIRED_IN_QUEUE：B0=9036、C1=4760、C2=7595；原因可重叠，不是互斥失败人数。
这提示排队、机会有效期和反应延迟的可达性需要 LocalSIM owner 归因；本分析未修改这些合同，不能直接定性为程序 BUG 或规则失效。

## 3. 公共接口与固定身份

Python：`continuous_results.AnalysisSpec`、`continuous_results.analyze(spec, timing_root)`。
CLI 只读消费 spec 的绝对 FileRef，结果使用现有 freeze 在独立 namespace 不可变写入。
不导入 LocalSIM 实现、不重新撮合、不查询 DB 或行情、不运行 181 日任务。

原 study：`2c68bd1c1e0cce965d931b5f87d8d94257e0aeb98ad258f82e01f448e439488c`。
producer commit：`ff41587e8f665bf48b4bdb6d0700e4a8225688e8`。

| 文件/身份 | 完整 SHA256 |
|---|---|
| request.json | 77edfeacec61827dd98c17f1b3194c6670b09b6ac951bbd79381224b678ba71b |
| source_inspection.json | 16979b33b111c57a3d6fe989d17457e24771e73f380b5b8a22dd69015b371d71 |
| result_receipt.json | 694c111490cb2676bff450bd657fbbb951a8d4b7d1a302fcbe42f1aba405ea36 |
| results.json | e3ec272c8b5dc26fcf4eeab2051bd8b432f40385e281d077d6eb352c21a61a92 |
| 实际行情 candidate manifest | 321a966c55822db5777dda4ef0091422e8a1877b7bcb6c27ffb763e9a2441c73 |
| 原价格计划 candidate manifest | f5af5cc0506e502300ef225805b51aaeb7ea1b1bc352fdc81f363f4933f5f8ed |
| package manifest canonical | 5de2ec91c088e5cf9a9cc25673498a4165857b761cb9e4a4456151a1e04f0dc9 |
| selection inventory | 4ac705225404568db4254d00825f5d098a46f98e0301aa7e448575f5b9cffd33 |
| shared price policy document | 58e2fddf837f890e7eecadeef08e2907273f7edc3ab9759d62ae51e892e5b779 |
| cost policy | d9f8c97ea4593206b0509fcfeb5046a20925c84c618a9a3d3061f7945d9f3041 |
| analysis-spec.json | 53b31211e0fc7faf4bf422f19f312945e266a4360d07243b79f1f75bb0230c9b |

旧/新 candidate 不混为同一版本；后继关系由 request 显式 input_candidate 和原 producer 批次来源检查绑定。
本分析重新验证顶层来源文件的完整内容 SHA 与交叉身份，不重新核验原始 minute/bin 或 2353 个逐日 journal。
`source_verification_scope=DECLARED_TOP_LEVEL_FILES_AND_DELEGATED_PRODUCER_BATCH`；不可描述成独立重验所有底层来源。
首次研究收益读取严格在本分析 source_preflight 通过之后，阶段为 AFTER_ANALYSIS_SOURCE_PREFLIGHT。

## 4. WSL 验收与多轮审核

冻结输入/结果根：`X:/AIstock_temp/position-timing-continuous-analysis-20261029`，WSL `/mnt/x/AIstock_temp/...`。
spec 绝对路径为 `<根>/analysis-spec.json`，producer status 冻结副本为 `<根>/producer-status-snapshot.json`。
报告：`<根>/artifacts/intraday_v1/continuous_analyses/2b698f90ee7696c75e5b41f0f4ea22f939a137cdeb91857f0932f9808d5d53ab.json`。
完整文件 SHA：`737f1e0b34400e3d7763dab051261b6de4758f490492ea7ecba8e8d4886e045f`。
验收收据：`<根>/acceptance-2b698f90ee7696c75e5b41f0f4ea22f939a137cdeb91857f0932f9808d5d53ab.json`，
SHA：`f8640e6a9f2c768585f5918c10eb1d29478c47f3400da83c2b3ff159c93a1ead`。

WSL 现有环境 `/mnt/x/AIstock_temp/localsim-position-timing/wsl-env/bin/python`。
最终代码首跑 1.268 秒，exact retry 1.347 秒，返回相同 report FileRef 和 ALREADY_MATERIALIZED。
这是热缓存摘要分析耗时，不是账户回放耗时，不声明数据读取提速倍数。

```bash
PYTHONDONTWRITEBYTECODE=1 python -m backend.services.position_timing.intraday.continuous_results analyze \
  --spec /mnt/x/AIstock_temp/position-timing-continuous-analysis-20261029/analysis-spec.json \
  --spec-sha256 53b31211e0fc7faf4bf422f19f312945e266a4360d07243b79f1f75bb0230c9b \
  --timing-root /mnt/x/AIstock_temp/position-timing-continuous-analysis-20261029/artifacts
```

审核一（来源/隔离）：修正了旧 preflight 状态与已补齐计划来源的时序混淆；合法后继 candidate 不要求与旧模板强行相等。
审核二（统计/完整性）：补齐 ALL=BUY+SELL 的分母校验、完整人口日期、逐腿诊断不得归因账户收益、producer retry 独立性。
审核三（实现/复现）：固定分析实现及共享纯 helper 的 LF SHA；最终实现重新生成新不可变报告并 exact retry，不覆盖之前报告。
直接测试 15 项通过；一次最终相关小矩阵（本文件、旧静态比较、API、isolation）27 项通过；Ruff 与 F1 validator 通过。
L0 零 finding；四文件 ownership 全部映射且无歧义。classifier 为 targeted_ci_required，
workflow_gate=passed，unexecuted_test_files=[]，无 DEV DB 计划。CLI 读同一 spec 再次返回相同报告 SHA。
源文件/经济字段不匹配均 fail closed，未出现补零、删股票、改规则或回放。

## 5. 后续优先级及独立验收状态

1. LocalSIM owner 修复并交付原 results 不可变发布冲突的精确重试证明，优先复用已完成账户日；本窗口不修改其模块。
2. LocalSIM owner 对长延迟零成交及主延迟低完成率做排队/有效期可达性归因，先交事实，不依据经济结果临时延长机会期限。
3. 若 owner 提供新的有效结果或成功证明，本入口以新的 spec/report 复核；不得覆盖本轮暂定报告。
4. 上述问题闭合后才考虑新的执行政策假设；不因 C1 的已成交子集好价就调整 0.5% 或宣称超额收益。

共享分析接口：完成；13账户摘要消费：完成；producer exact retry：未通过；经济确认：false；运行态激活：未执行/不需要。
database/network/live_market/runtime/process 均 false；无服务重启、数据库或数据文件修改，无其他模块源码改动。
