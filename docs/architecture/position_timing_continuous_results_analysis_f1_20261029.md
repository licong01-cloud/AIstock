# LocalSIM 连续账户结果分析 — F1 详细设计

日期：2026-10-29。所属：position_timing。状态：IMPLEMENTED_VERIFIED（离线功能验证；不表示 producer 重试或经济验收通过）。

## Background

终极目标是判断分钟提醒是否改善人工交易的净收益和回撤，而不是增加规则、模型或研究平台。
现有 `research_results.py` 对比独立计划，不能将连续反馈账户强行转换为静态等数量实验。
本轮只消费 LocalSIM 已生成的连续账户摘要，不重新撮合、计费、清算或计算分钟净值。
181 日研究已生成 13 个账户、2353 个账户日；exact retry 在结果不可变发布处失败。
这不等于 181 日计算未完成，也不构成可重复性已经通过的证明。

## Scope

仅新增以下文件：

- `backend/services/position_timing/intraday/continuous_results.py`
- `backend/tests/position_timing/test_intraday_continuous_results.py`
- 本设计文件。
- `docs/analysis/position_timing_continuous_results_analysis_20261029.md`

公共入口：`analyze(spec: AnalysisSpec, timing_root) -> dict`；CLI：
`python -m backend.services.position_timing.intraday.continuous_results analyze --spec <绝对路径> --spec-sha256 <完整SHA> --timing-root <独立择时artifact根>`。
`AnalysisSpec` 冻结 request、source_inspection、result_receipt、可选 producer job status 的 FileRef。
spec 本身须内容 SHA 校验。支持正式 `FULL_13`：native TWAP 加三规则×四延迟，1～512 个唯一递增交易日。

## Non-goals

不修 LocalSIM 重试冲突，不启动/恢复/终止回放，不训练、不改规则/价格政策/阈值，不做显著性检验或自动晋升。
不改 LocalSIM、Advisory、QE、数据准备，不读取数据库/行情/网络，不操作服务或正式账户。
原 288 案例、正在运行或冻结的研究及其 journal 均不修改。无需后端重启。

## Contracts

### 输入与 source-before-outcomes

1. 先验证 spec、request、source_inspection、result_receipt 的文件 SHA、schema、请求内容身份和 release identity。
2. 验证 request 的全部日期、13 个规则/延迟/operation 身份、初始现金、执行合同、两个比较家族。
3. 对 source_request、preflight 的六个顶层输入、inventory、当前 candidate、package、price templates、price policy 做完整文件 SHA 核验。
   校验 preflight/package/inventory/policy/template 的交叉绑定与日期；保留 UNKNOWN，不补造价格。
4. source_inspection 必须为 COMPLETE_SOURCE_BATCH_INSPECTED 且绑定相同 request、producer source identity、candidate、policy 和日期。
   原价格计划 candidate 与后继行情 candidate 可以不同；只接受 request 显式 input_candidate 绑定，原来源等价性沿用 producer 的冻结批次检查。
   历史 asset preflight 的 BLOCKED_PLAN_INPUT_CONTRACT 先于价格模板交付，不能当成后来完整 source inspection 失败；二者分别保留身份，不改写旧状态。
5. 原始 minute/bin、逐日账户/journal、policy 内逐项价格来源由该 producer 批次检查负责。本分析不冒称独立重验这些底层事实。
   `source_verification_scope=DECLARED_TOP_LEVEL_FILES_AND_DELEGATED_PRODUCER_BATCH`，报告明确此边界。
6. 上述完成后才读取 results 文件，记录 `outcomes_read_stage=AFTER_ANALYSIS_SOURCE_PREFLIGHT`。
   results 必须绑定相同 request/source inspection/producer commit/执行合同/费用/包及完整 13 账户×全部日期。
   无效数值、重复/缺失账户和日期、不可用终值不得补零或删除后继续。
7. JSON 单文件解析上限 32 MiB；candidate 仅流式 SHA，不解析；最多 512 日×13 账户，不加载逐日 journal。
   这是离线读取资源界限，不是经济准入门禁。

### 分析口径

- 保留 producer terminal NAV、正向 peak-to-trough MDD、累计费用、每日 NAV、逐侧计划/成交数、原因和人工操作指标。
- 收益率 = terminal NAV / initial cash − 1；增量收益 bps = 净值差 / 相同初始现金 ×10000。
  正 mdd_reduction 表示降低回撤；正 fee_difference 表示增加费用。
- 两个家族分别为 NATIVE_VS_HUMAN_WORKFLOW 与 HUMAN_MATCHED_RULES，后者使用同延迟 FIRST_OBSERVABLE 基线。
  验证 24 个对比身份和净值差/回撤差/费用差与账户事实一致；不重新计算成交价差。
- BUY/SELL 仅显示 conditional_price_cost_bps（正数更差、负数更好）、配对数量、并集数量及 missing_fraction。
  必须保留无成交/不完全成交和条件样本分母；BUY/SELL 的账户收益和回撤归因始终为 null。
- 闭环中数量/持仓路径不同，quantity_matched=false、pure_timing_attribution=false。
  描述性结果不能证明纯择时 alpha，也不把 2353 账户日当独立样本。
- 主延迟从冻结执行合同继承 60 秒；其他延迟为诊断，不依据收益选最佳延迟或阈值。

### 可复用性与输出

producer job status 是可选的有 hash 快照：只有 COMPLETED/VERIFIED，RUN 与 EXACT_RETRY 指向相同 results，
重试 status=ALREADY_MATERIALIZED、computed_account_days=0，且请求/producer/13账户/全部日期一致，才标 VERIFIED。
缺少证据或原重试失败时仍可输出描述性结果，但标 REPRODUCIBILITY_UNVERIFIED、PROVISIONAL；不得经济确认。
分析器自身 exact retry 只证明报告不可变复用，不代替 producer exact retry。
后续 owner 新的成功证明产生新的 spec/report，不覆盖旧报告。

报告位于 timing-owned `intraday_v1/continuous_analyses/<内容身份>.json`，复用现有 `freeze`，
绑定输入 FileRef、producer 身份和分析器实现 SHA（LF 归一），不写 producer 目录。
结果无运行时激活含义。报告单独列出来源核验、producer 可复用性、分析完整性和经济确认 false。

## Design Acceptance Index

- F-001：仅择时所有文件及独立只读分析入口，零跨模块/运行态副作用。
- F-002：source-before-outcomes，完整内容身份和后继 candidate 合法绑定，委托核验边界显式。
- F-003：完整 13×日期人口和账户/比较事实 fail-closed，不补零、不删失败。
- F-004：两个家族及四延迟横向比较；BUY/SELL 符号、分母和闭环归因不混用。
- F-005：producer retry 与报告 retry 分离，失败证据保留且不可宣称经济确认。
- F-006：不可变报告、实现/输入身份、资源界限、WSL 冻结事实验证。

## Implementation Plan

一个交付块：完成分析器和直接测试，审核修复后读取现有冻结结果，交付报告和 PR。
不新增 worker、API、UI、调度器、审批流程或研究候选。

## Verification Plan

直接测试覆盖身份/源失败先于收益读取、合法 candidate 后继、全人口和日期完整性、非有限数值、比较器/符号/无成交分母、
可复用性失败/成功、源修订、报告 exact retry。稳定后运行一次相关静态比较器回归。
WSL 使用冻结文件执行分析及同输入 exact retry；报告实际耗时和输入/输出 SHA，不调用 LocalSIM 回放。
执行 F1 validator、L0、ownership/scope、targeted CI 与多轮 DESIGN-COMPLIANCE-001 逐项审查。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/position_timing/intraday/continuous_results.py:analyze | backend/tests/position_timing/test_intraday_continuous_results.py | VERIFIED | none |
| F-002 | backend/services/position_timing/intraday/continuous_results.py:source_preflight | backend/tests/position_timing/test_intraday_continuous_results.py | VERIFIED | none |
| F-003 | backend/services/position_timing/intraday/continuous_results.py:summarize | backend/tests/position_timing/test_intraday_continuous_results.py | VERIFIED | none |
| F-004 | backend/services/position_timing/intraday/continuous_results.py:summarize | backend/tests/position_timing/test_intraday_continuous_results.py | VERIFIED | none |
| F-005 | backend/services/position_timing/intraday/continuous_results.py:reproducibility | backend/tests/position_timing/test_intraday_continuous_results.py | VERIFIED | none |
| F-006 | backend/services/position_timing/intraday/continuous_results.py:analyze | artifact: docs/analysis/position_timing_continuous_results_analysis_20261029.md | VERIFIED | none |

## Risks

producer exact retry 尚未通过，结果只能暂定分析；不能用本报告掩盖该失败。
委托的原始账户/底层行情检查不是本分析器再次独立执行；持久化摘要为事实来源。
配对成交有选择偏差，费用与持仓路径会反馈；不得用单一价差推导账户收益。
源文件在读取时变化按 SHA fail closed；报告不携带运行时 mutable status，只接收冻结快照。

## Production Gates

database_read/write=false、network_accessed=false、market_accessed=false、runtime_action=false、process_control=false。
仅新增离线模块，无 DDL/DML、依赖安装、后端重启或生产激活。合入完成不表示可重复性或经济有效性通过。
