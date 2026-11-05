# 择时跨月价格计划与期间分析 F1 设计

日期：2026-11-06。状态：IMPLEMENTED_VERIFIED（择时软件及价集，不表示LocalSIM期间回放或经济效果完成）。Owner：position_timing。仅离线新增能力，不是新择时策略。

## Background

终极目标是检验分钟提醒是否在相同外生目标下改善净收益和回撤。10月17日850个原生目标已存在，无需QE重训。LocalSIM已发布v2期间准备，保留13个账户各自9月末状态；旧价格计划遗漏此前持仓，旧分析器只支持共同1000万元空期初。本设计消费正式准备，不重造账户或撮合。

## Scope

允许修改：`intraday/price_plan_campaign.py`、新增`intraday/period_context.py`、扩展`intraday/continuous_results.py`、对应position_timing直接测试和自身文档。路径前缀为`backend/services/position_timing/`或`backend/tests/position_timing/`。原v1入口保持行为；新期间使用明确版本化输入。

## Non-goals

不改LocalSIM/QE/Selection/Advisory/数据准备，不操作数据库、行情网络、服务、candidate或旧研究。不重训、不重选股、不调参、不重跑1—9月；不模拟订单、费用、股份或NAV。不以本次软件交付宣称10月回放/producer retry/经济验证已完成。LocalSIM独立期间runner及结果生产由其owner完成。

## Contracts

### 允许复用的API和来源

- `price_plans.build_price_plan`：既有50bps原始价格公式、NONE、可见时间/失效语义不变；见`position_timing_shared_price_plans_f1_20261024.md`。
- `research_inputs.checked_file`、`CandidateReader`、`research.freeze`：完整内容SHA、冻结日历/日价和不可变发布；不新增Reader。
- `continuous_results.load/number/require`：有界JSON、有限Decimal和明确错误；旧AnalysisSpec/analyze不变。
- LocalSIM `localsim_period_continuation_preflight_v2`、`localsim_period_continuation_preparation_v2`和`localsim_period_opening_account_v2`；来源为其`localsim_period_continuation_inputs_f2_20261106.md`、正式preflight及opening FileRef。这里只验证/消费，不复制其账本验证。

### 期间上下文

`load_period_context(preflight: FileRef)`验证完整SHA/schema、canonical identity/release、最后发布的preparation_receipt、日期、13个(rule,delay)与lineage、零新入金、初态/累计锚点、历史SELL并集和inventory身份。源码commit与数据SHA分别保留。receipt缺失、必要来源不通过、身份/人口冲突明确失败。父期经济锚点是已知初态，`outcomes_read`专指新期间结果。

只核验元数据、初态及声明的来源身份；底层行情/原生账本检查委托给LocalSIM已冻结准备，不声称再次完成账户核算或全量源审计。

### 价格计划

既有`CampaignPriceSpec`和CLI继续接受v1；新增接受v2正式preflight。日期取`trade_dates`，不得从年初扩展或以candidate截止日扩大期间。`seen`从13户opening中历史持仓并集起步，再按日加入当日目标。历史股票只生成SELL；当日目标仍BUY/SELL。不能利用未来目标填补历史BUY。来源缺价仍UNKNOWN/NONE，不补价、不删股。价集独立于规则和延迟。

每个新policy/audit/manifest绑定preparation request、receipt及opening FileRef，policy声明是0.5%工程偏好、非预测/生产默认。新内容身份不覆盖旧批次。reader在成功与异常后关闭。

### 期间分析公共合同

新增`PeriodAnalysisSpec`和`analyze_period(spec, timing_root)`，CLI `analyze-period`。spec绑定v2 preflight、新templates及`facts` FileRef；facts使用择时侧公开schema `position_timing_period_facts_v1`。它是LocalSIM未来结果的轻量消费投影，不是伪造的LocalSIM v1/v2执行请求。

facts字段：`preflight`、`price_templates`、`producer_source_commit`、`source_batch_verified=true`、`source_inspection` FileRef、`outcomes_read_stage=AFTER_COMPLETE_SOURCE_BATCH_INSPECTION`、`nav_observation_scope=ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS`、`accounts`。每账户包含`account_id`和日期顺序完全匹配的`days` FileRef。`source_inspection`无需冒用尚未定义的LocalSIM schema，但必须是含`preflight`、`price_templates`、`candidate_manifest`、`trade_dates`、`producer_source_commit`、`account_days_sha256=digest(facts.accounts)`和`stage=COMPLETE_SOURCE_BATCH_INSPECTED`的正式producer完整批次收据；额外owner字段可保留。顶层facts和全部day的完整SHA在打开新期间day收益前完成核验。source_inspection必须先验；source-before-outcomes不等于不hash结果文件。

source_inspection还须携带`cost_policy_file_sha256`，等于preflight.inputs.cost_policy.sha256；`execution_contract_sha256`等于preparation_request.execution_contract的canonical摘要，防止不同费用或执行参数借用同一来源验收。

这里的source_inspection是供结果消费者使用的**最终来源/结果绑定收据**，允许LocalSIM在回放完成后由原始source检查收据封装并加入account-day摘要。它不是开跑前的准入文件，不要求producer在生成结果前预知结果哈希；原始完整source检查仍必须先于producer读取收益。不能为等待最终绑定收据而阻止账户回放，也不能在结果生成后才首次进行原始source检查。

逐日账户事实复用既有LocalSIM day语义：`account_id`、`rule`、`delay_seconds`、`operation`、`trade_date`、`terminal_as_of`、`terminal_nav_cny`、`fees_cny`和完整`nav_observations=[{as_of,nav_cny}]`。账户日、费用、NAV由LocalSIM生产，本分析只消费；不以日终近似冒充分钟MDD。必须同日时区明确、严格递增且不超过14:57，末条等于共同终点和terminal NAV。JSON单文件上限32MiB，日期最多512日，逐账户日流式处理；13户完整，不能缺失后删行或补零。

每户期间PnL=`terminal-opening_nav`；期间收益=`terminal/opening_nav-1`；累计收益=`terminal/origin_initial_cash-1`；期间费用只累计新day；累计费用=父累计费用+期间费用。期间MDD从各自opening NAV起算；累计MDD接父高水位和父MDD，再顺序消费新期间完整NAV观察。不能补充值拉平不同账户。

保留两个家族、24比较及同延迟B0基线。期间增量人民币=`(endA-openA)-(endB-openB)`；另报各自期间收益率之差，不把不同分母混作统一资本收益。以共同原始资本归一化的该PnL差单列`period_increment_origin_bps`；累计净值差/共同原资本为`cumulative_increment_bps`。期间/累计MDD改善分别报告。BUY/SELL收益归因保持null，`quantity_matched=false`、`pure_timing_attribution=false`。

报告exact retry只证明分析产物复用。未提供独立producer economic exact retry证明时明确`REPRODUCIBILITY_UNVERIFIED`，不影响描述性分析，不增加研究准入门禁。不自动晋升规则，不用10月结果选阈值。

## Design Acceptance Index

| ID | 验收项 |
|---|---|
| F-301 | v2正式期间/receipt/初态来源和13户身份fail-closed，旧v1入口兼容 |
| F-302 | 历史持仓SELL覆盖、按日BUY因果、UNKNOWN和50bps政策不变 |
| F-303 | 新模板policy/manifest/来源身份闭合，inspect及exact retry不可变 |
| F-304 | 统计输入合同、全13户/全日期/时钟和source-before-outcomes，无账户重算 |
| F-305 | 期间/累计收益、费用和完整NAV回撤分离，24比较无旧净值差混入 |
| F-306 | 实际WSL冻结输入价集验证，原v1回归、多轮审查、明确LocalSIM后续接缝 |

## Implementation Plan

一个连续交付块，分三步完成：上下文和历史SELL价集；期间/累计统计；多轮审核修复、定向测试、真实WSL价集build/inspect/exact retry、设计验收及PR/CI。无额外调度器、平台或审批。LocalSIM runner未交付时交回准确消费合同，不把软件通过称为10月经济结果。

## Verification Plan

复用小型功能夹具，覆盖真实业务不变量：先前持仓无本月目标、未来目标不能提前BUY、缺价UNKNOWN、receipt/初态/来源篡改、全人口/日期缺失、相同末值但不同期初的PnL差、父高水位跨月回撤、费用承接、未来/重复NAV时钟、报告复用。真实17日文件只生成价集和核验初态；实际新期间收益只能在正式producer source检查后读。原v1两个直接测试文件仅运行一轮稳定回归，不扩展跨模块套件。

## Risks

LocalSIM尚未交付新runner/结果；本次公开合同使其可接入，不伪称已联跑。父来源的可见成交量差异按owner准备合同披露，不扩大为历史完整回放等价。0.5%非预测、历史可见时间为假设、未知价格不补造；数据与经济限制继续可见。

## Production Gates

全部新增为离线CLI/纯统计；database/network/live_market/process/runtime_action=false，无DDL、无服务重启或生产激活。所有新产物新身份发布，旧研究/288案例不改写。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-301 | intraday/period_context.py；price_plan_campaign.py | backend/tests/position_timing/test_intraday_period_continuation.py | VERIFIED | none |
| F-302 | intraday/price_plan_campaign.py | backend/tests/position_timing/test_intraday_period_continuation.py | VERIFIED | none |
| F-303 | intraday/price_plan_campaign.py | backend/tests/position_timing/test_intraday_price_plans.py；artifact: docs/analysis/position_timing_period_continuation_delivery_20261106.md | VERIFIED | none |
| F-304 | intraday/continuous_results.py | backend/tests/position_timing/test_intraday_period_continuation.py | VERIFIED | none |
| F-305 | intraday/continuous_results.py | backend/tests/position_timing/test_intraday_period_continuation.py | VERIFIED | none |
| F-306 | 本设计与交付文档 | artifact: docs/analysis/position_timing_period_continuation_delivery_20261106.md | VERIFIED | none |
