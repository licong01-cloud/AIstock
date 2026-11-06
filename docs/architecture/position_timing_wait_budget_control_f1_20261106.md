# 盘中择时：同等待预算对照 F1 设计

日期：2026-11-07。Owner：position_timing。状态：OCTOBER_DIAGNOSED_HISTORY_BLOCKED_SOURCE_FILES。
10月三户51账户日已由LocalSIM完成并由择时消费；1—9月扩展研究的接口、来源与账户执行分别验收。

## Background

目标是找到能改善人工执行净收益与回撤的简单机制，不是扩充规则库。
现有三规则×四延迟和TWAP的1—10月分析已完成；1分钟主口径下，C2累计相对B0高6.1162个百分点，但10月PnL差仅+27,200.42元，多数差异来自父期。
各臂10月期初资金、持仓不同，不能以该PnL差证明10月纯执行alpha。

正式结果与source/retry核验入口为
`X:/AIstock_temp/position-timing-october-account-analysis-20261106/verification-receipt.json`。
全13户逐日诊断为同根目录下
`intraday_v1/wait_budget_diagnostics/312e5cec8babb45f550b5c636540e15b1910ead5579315bb54848dcef0db57bf.json`，
完整文件SHA256：`f714c9e5d22e9d8d6d6e9ba4c0019aa0f6a7da945a92b3e420316357f2dc0311`。
其来源仍先经294项FileRef预检，再消费221账户日；没有启动回放或读取新行情。

1分钟反应延迟下的17日诊断：

| 规则 | 有成交父计划/生成父计划 | 日终现金比例均值 | SUBMIT可见至派单中位数 | ready至派单中位数 |
|---|---:|---:|---:|---:|
| B0 FIRST_OBSERVABLE | 576/589 | 8.97% | 17分钟 | 16分钟 |
| C1 PRICE_TOUCH | 204/652 | 25.62% | 1分钟 | 0分钟 |
| C2 BOUNDED_REVERSION | 507/587 | 9.66% | 15分钟 | 14分钟 |

分母是各账户实际生成的父计划，不是850个外生目标，不能将不同分母当配对完成率。
有成交包括部分成交；日终现金是逐日等权均值，不是盘中时间加权仓位。
派单分布只包括已派出的SUBMIT，不包括永未派出者，另保留未成交人口。
ready至派单为墙钟差，可含操作员排队、闭市等影响；它不是另一个固定反应延迟。
父计划reason_codes为整日累计原因集合，不能用TIME_BUDGET_RELEASE或BOUNDED_REVERSION出现次数推断实际成交归因。

## Scope

允许写入：`intraday/wait_control.py`、`intraday/wait_control_results.py`、`intraday/continuous_results.py`，对应`backend/tests/position_timing/test_intraday_wait_control.py`及`test_isolation.py`的公开SDK消费边界，本设计及上位蓝图进度段。三个实现文件前缀均为`backend/services/position_timing/`；冻结研究产物仅写本窗口X盘根目录。
实施仅限研究侧纯机会判断、直接测试和自身文档；LocalSIM账户接入由其owner单独完成。
复用现有CandidateReader、共享价格计划、core.evaluate、trading_minutes、FileRef、freeze和LocalSIM账本/撮合，不建第二套Reader或账户引擎。
2026-11-07扩展允许新增`intraday/wait_control_history.py`、`intraday/wait_control_diagnostics.py`及直接测试`test_intraday_wait_history.py`；更新`test_isolation.py`的精确只读公共接缝。本轮不改原plan绑定的12份实现文件，旧10月身份继续可核验。

## Non-goals

不改原三规则、默认参数、线上枚举/API、价格政策、成本或原13户/288案例；不调参、不重训、不重推理、不等待未来交易日。
不改LocalSIM/QE/Advisory/数据准备，不抓行情、不操作数据库、数据集、服务或真实账户；不将诊断称为新规则经济验证。
不因设计验证通过就把第四规则加入线上可选项。

## Contracts

### 1. 一个假设、三个账户、共同起点

新研究身份为WAIT_BUDGET_CONTROL_15M_V1，不冒用FULL_13、localsim_period_study_v2或既有报告schema。
首轮仅比较B0、研究对照W15、原C2；固定60秒人工反应延迟、单操作员容量/排序/取消规则、90秒机会TTL、14:57终点、allow_split=false。
不扩为四延迟矩阵，不加入C1或新窗口搜索；这是解释C2机制的有界实验，不是择优晋升。

三户都复制同一份B0-60秒9月末冻结opening，现金、股份、可卖量、成本、估值及分析锚点完全相同；仅新研究账户身份不同。
opening文件为准备根`465cb1fd201b93e9fda412ec85e8c5219752198c84a6d6a4bf708f23c0c5f706`下
`opening_accounts/lsperiod_9f055177af16233ac5310bf647c26ea79c4a338e.json`，
完整SHA256：`e75cbf5d63228e5c2499aa8edb82374244b00ab35a3b349881535db9d799214f`。
期初NAV为9,739,392.18元；原始资本仍为1000万元，不追加资金，不把NAV重新填为1000万元。
复制的是独立研究分支，不改原opening；账户ID不能改变symbol排序、操作员优先级或撮合排序。
共同复制经济状态和分析锚点，各臂的研究身份/规则状态单独新建，不把B0的policy hash、机会或计划状态当成C2/W15状态。
若opening仍有未结委托、预留资金/股份或未确认取消，须由LocalSIM沿原事实明确承接，不能清零以假装共同初态；前一日已终结的动作不能在新日期重派。

期间固定2026-10-08～2026-10-30的17个交易日、850个原生目标；同一包、inventory、candidate、0.5%工程偏好、成本与分钟来源均继承已冻结analysis_spec的来源链。
candidate manifest SHA为`2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958`；不得切换active/profile。
来源analysis_spec完整文件SHA为`a48db077873ca0cfd1b3494816eda33d2d0adc086bc960a43beb39429ef8df21`，其绝对路径见前述verification-receipt；开跑前绑定完整FileRef并沿链校验，不从当前active配置重建来源。
共3×17=51账户日。持仓/资金从共同opening起自然分叉，目标到合法委托数量仍由LocalSIM按各自账户计算；不强行等数量、不复用旧C2收益。
旧B0可作为新B0独立等价核验的预期，不作为未运行新臂的替代结果。

### 2. W15定义：相同最大预算，不承诺相同实际等待

W15在同一共享安全检查建立的batch_started_at起，等待15个交易分钟；预算释放前不产生可派单机会，释放后不再依赖均线/反转或PREFERRED价格是否满足。
不使用当日未来最高最低价，不以目标bar完整OHLC决定在其开盘成交。
计时严格沿用core.evaluate设置batch_started_at的位置：共享身份、账户、可见bar、停牌/涨跌停/新鲜度等条件后、HARD区间判断前；不简单替换为开盘或第一次成功成交。
午休不计交易分钟；建立时钟后缺bar/停牌等不重置或暂停交易日历时钟，但当前仍须通过安全检查才可发出机会，与原C2一致。
HARD约束永不豁免；PREFERRED来源可见性/有效期和缺失处理与共同输入合同一致，不能伪造NONE或无限区间来回避缺失。

C2允许15分钟内提前出现回归机会，W15没有提前机会，因此两者只有同一最大等待预算，不是相同实际派单时间。
预算到期不会绕过人工排队、反应延迟、机会TTL、修订取消、停牌涨跌停、现金/T+1或后续bar成交条件。
因排队导致的收益变化属于完整人工作业效果，不冒称剥离排队后的纯价格alpha。

### 3. 择时侧最小公共接缝

使用研究专用的`evaluate_wait_control(plan, market, execution_view, base_policy, state, as_of)`。
底层只调用既有FIRST_OBSERVABLE core.evaluate以复用安全检查，保留独立的外层control身份及状态；不复制core的安全/交易资格实现，不修改原Decision语义。
外层状态仅保留底层state、control_spec_sha256和首次真正释放机会时间；计划修订建立新状态，不跨日携带。
返回研究信封：control_id/spec hash、outer status/reasons、outer opportunity/decision ID、outer advice_available_at、underlying decision及其state hash。
有效期取底层本次decision.valid_until，不超过原计划/时段/价格约束；外层释放后才进入原90秒TTL与人工派单合同，不借底层较早的可见时间提前排队。
底层B0的OPPORTUNITY和first_opportunity_at仅供诊断，不可在外层WAIT时交给执行器派单；外层首次机会时间只能在预算释放且底层允许执行时写入。
opportunity/decision身份绑定W15控制身份、计划revision和batch，不能与B0共享提醒/派单去重键。硬失败、过期和未知状态按底层状态显式保留。
W15无价格预测、无订单/数量分配、无额外调度器；接口只供显式import研究调用，不注册API或加入线上PolicyId。LocalSIM消费仍需按下条合同联调。

### 4. LocalSIM最小需求（由用户转交其owner，不在本窗口修改）

- 允许新研究独立的三profile request和同源opening克隆，保留原FULL_13/period-v2验证，不放宽旧schema的人口。
- W15消费上述外层信封；B0/C2仍走原唯一核心。冻结三臂实现closure、输入和费用身份，使用现有单操作员队列、账本、撮合、后续bar代理及分日提交。
- 返回三户×17日的完整来源绑定、NAV、费用、父计划及成交/未成交/取消事实；source preflight先于收益。不同账户从共同opening自然反馈，不创建生产账户。
- 保留计划计时起点、首次外层机会、ready/dispatch及首笔成交时间和原因，足以区分预算等待、排队、取消/TTL与后续成交；不建设新事件平台。
- 现有PeriodAnalysisSpec/summarize_period固定13户，不能伪填10户或直接导入三户。由择时侧在实施时提供独立三户研究分析合同，复用统计函数，不放宽旧13户合同。

### 5. 评价和结论边界

主比较为C2−W15；W15−B0回答固定等待的整体作用，C2−B0提供总效果。全部来自新三户共同opening，不能拼接原不同opening结果。
报告净PnL/共同opening收益、完整分钟NAV正向MDD、费用、BUY/SELL全部父计划分母、未/部分成交、取消/过期、日终现金和可得的时间加权暴露；没有时间加权来源则显式UNAVAILABLE，不用日终均值冒充。
日内机制辅助报告机会到派单、ready到派单、派单到首笔成交，分别注明样本分母；未派单/未成交单列，不删掉后计算“完整”改善。
同opening只去除了父期起点差异，反馈导致后续持仓/数量仍不同，quantity_matched=false、pure_timing_attribution=false，BUY/SELL独立收益归因仍不提供。
使用已分析过的10月数据是事后机制诊断，不是新sealed holdout，不据此宣称样本外盈利、因果归因或选择最优阈值。任何正式推广结论另需冻结规格后的独立期间证据，但不阻碍本轮研究运行。

### 6. 可消费的软件合同

`wait_control.evaluate_wait_control`返回`(WaitControlDecision, WaitControlState)`；底层计划/Policy均须为未分批B0。外层返回完整underlying_decision供审计，但消费者只能按外层status/ID/advice_available_at派单。外层state可JSON往返恢复；换计划revision须显式使用新state。控制spec绑定base_policy.source_commit及15分钟固定值。
LocalSIM使用显式from-import消费上述公开决策/状态/spec/CONTROL_ID，以及wait_control_results中的inspect_plan、economic_opening和三个WaitBudget模型；隔离测试只允许这些公开符号，不开放prepare/analyze/source_preflight等发布或内部入口，也不允许其他业务模块反向导入。

`wait_control_results.prepare(parent_period_spec, source_commit, timing_root, input_kind="REAL_HISTORY")`冻结`position_timing_wait_budget_research_plan_v1`，不是LocalSIM可执行请求。真实研究精确绑定本文parent/common opening/candidate SHA与17日850目标，公开函数仅为测试另接受显式FUNCTIONAL_FIXTURE；CLI不开放fixture参数。plan绑定共享实现LF哈希、三profile、费用/执行/operation及共同经济初态摘要。克隆仅允许变更portfolio_id，不忽略股份、成本、可卖量、parent_state、现金或锚点。

LocalSIM返回以下择时侧消费投影（不冒称LocalSIM现有schema）：

- `WaitBudgetAnalysisSpec`：research_plan、facts的FileRef及可选producer_economic_retry FileRef。
- `position_timing_wait_budget_facts_v1`：research_plan、producer_request、source_inspection、producer_source_commit、study_identity、source_batch_verified=true、outcomes_read_stage=AFTER_COMPLETE_SOURCE_BATCH_INSPECTION、nav_observation_scope=ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS、accounts。
- accounts恰为三项，含account_id、arm_id=B0/W15/C2、opening FileRef和有序days FileRef。opening schema为`position_timing_wait_budget_opening_v1`，包含common_opening FileRef、account_id/arm_id/rule/delay_seconds/operation及完整源经济字段。W15的rule取CONTROL_ID，不把它伪装成线上B0 PolicyId。
- producer_request由LocalSIM自有schema定义，但必须含producer=LOCALSIM、research_plan、producer_source_commit、identity（排除identity/release_identity后canonical摘要），并完整匹配plan中的trade_dates/common_opening/profiles/operation/candidate_manifest/price_templates/cost_policy/execution_contract。其identity须等于facts.study_identity。
- `position_timing_wait_budget_source_inspection_v1`绑定stage=COMPLETE_SOURCE_BATCH_INSPECTED、research_plan、producer_request、producer_source_commit、study_identity、candidate_manifest、price_templates、cost_policy_file_sha256、execution_contract_sha256、common_opening、trade_dates及account_days_sha256=digest(facts.accounts)。全部opening/day FileRef在读取新研究NAV前先核验完整SHA；底层账本/市场批次检查仍委托producer，不声称本分析重算账本。
- day沿用现有账户日字段account_id/rule/delay_seconds/operation/trade_date/terminal_as_of/terminal_nav_cny/fees_cny/nav_observations，并提供cash_cny、view_counts.trades、parents（plan_id/side/status/target_quantity/filled_quantity）、human_operation_state.actions（action_id/kind/visible_at/ready_at/dispatch_at）。父计划和action_id不重复。不补造没有成交的计划或价格；全部未成交/不可用状态保留。NAV归零仍保留真实亏损和回撤，只将无法定义的现金比例标为null并报告覆盖日数。
- 可选exposure_observations为覆盖9:30至14:57的有序as_of/nav_cny/cash_cny，按上个观察值在交易时段内分段保持计算暴露；午休剔除，缺任一天则整体UNAVAILABLE并报告covered_days。可选timing_observations覆盖全部父计划，含plan_id/first_dispatch_at/first_fill_at；无成交的first_fill_at必须为null。没有完整来源时不伪造派单至首笔成交统计。
- 可选`position_timing_wait_budget_economic_retry_v1`包含status=EXACT_RETRY_VERIFIED、facts/producer_request FileRef、study_identity、recomputed_account_days=51、new_committed_account_days=0。缺少证明不阻塞描述性分析，仍标NOT_SUPPLIED；有证明则校验并单列producer状态，不用分析报告复用冒充经济重算。

CLI：`python -m backend.services.position_timing.intraday.wait_control_results prepare|inspect|analyze --spec <绝对路径> --spec-sha256 <完整SHA> --timing-root <独立根>`；prepare的spec是旧期间analysis_spec，另需`--source-commit <择时实现commit>`；inspect的spec是新research_plan，公开`inspect_plan(FileRef)`只校验冻结plan及全来源，不接收新账户收益；analyze的spec是新WaitBudgetAnalysisSpec。prepare不读取新三户收益；analyze先完成整批来源检查后才按原公共NAV归约函数统计。新报告3项比较，不改原13户/24比较入口，不模拟账户。所有plan/report以内容身份不可变发布。

## Design Acceptance Index

| ID | 本次设计交付验收 |
|---|---|
| F-401 | 来源、诊断分母和排队/反应时间语义明确，无伪成交归因 |
| F-402 | 三臂共同opening、固定日期/参数、独立研究身份和旧研究隔离 |
| F-403 | W15计时/安全/外层机会身份闭合，原三规则和线上能力不变 |
| F-404 | 两owner最小接缝及固定13户分析器差异如实列出 |
| F-405 | 评价区分等待、队列、暴露和期初差异，事后研究不冒称holdout |
| F-406 | 研究专用W15真实纯接口、共享安全检查、状态恢复和机会身份隔离 |
| F-407 | 真实冻结plan、共同经济初态与完整三户来源在收益前核验 |
| F-408 | 复用NAV统计，三项比较/完整分母/可选统计缺失显式；原13户不退化 |
| F-409 | 10月完整人口归因、提前机会与失效原因留证，缺失生命周期标UNKNOWN |
| F-410 | 独立历史plan绑定181日9050目标、修正版candidate及共同现金起点；旧实现不变 |
| F-411 | 历史三户全来源先于收益，月季分钟MDD与配对区块区间；消费边界明确 |

## Implementation Plan

设计交付#7091已合入。本次完成研究专用W15、三户来源/统计合同和直接测试；旧13户归约入口不放宽。WSL旧13户实际统计等价核验不重跑账户、不覆盖旧报告。
原Contracts-4/6接入已由LocalSIM PR #7109完成，51账户日与不可变产物重试已通过；择时统计已完成。下一步按文末Contracts-7/8实施1—9月扩展。普通回放不自动整批经济重算。
不拆更多阶段，不等待在线重启或未来行情。账户接口未接入时交回准确需求，不在择时侧临时模拟收益。

## Verification Plan

本次检查固定输入SHA/全13户诊断、完整分母、计划/排队定义及跨模块边界；设计验证不冒充代码测试。
直接测试覆盖15分钟边界/午休、t0之前失败、建立时钟后暂不可交易、HARD不豁免、未来bar、计划修订/过期、外层机会时间和B0去重身份隔离、共同初态/来源失败、三户统计、可选投影与NAV归零；不重复复制行情/账本测试。
联调验证同opening克隆、B0等价性、三户完整来源/NAV、未成交分母、原三规则不退化和exact retry新身份。代码审核与CI由各owner完成，不增加研究审批。

## Risks

同最大预算不等于同实际等待；单操作员排队可主导总延迟；C1费用降低可能混入少交易和低暴露。
10月不是未见样本；分钟成交为代理，不证明真实盘口成交。10月三户研究已完成，但仅为事后机制诊断，不能称样本外经济确认；1—9月新三户尚未回放。

## Production Gates

本次database/network/runtime/process/training/account_replay=false；无部署、重启、DDL/DML或生产激活。
DESIGN_VERIFIED仅表示设计闭合；择时软件验证、LocalSIM接入和新543账户日经济结果分别报告，不以fixture或prepare成功声称经济有效。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-401 | 本文Background及Contracts-5 | artifact:X:/AIstock_temp/position-timing-october-account-analysis-20261106/intraday_v1/wait_budget_diagnostics/312e5cec8babb45f550b5c636540e15b1910ead5579315bb54848dcef0db57bf.json | DESIGN_VERIFIED | none |
| F-402 | 本文Contracts-1 | artifact:X:/AIstock_temp/position-timing-october-account-analysis-20261106/verification-receipt.json | DESIGN_VERIFIED | none |
| F-403 | 本文Contracts-2/3 | backend/services/position_timing/intraday/core.py::evaluate；artifact:docs/architecture/position_timing_wait_budget_control_f1_20261106.md | DESIGN_VERIFIED | none |
| F-404 | 本文Contracts-4 | artifact:docs/architecture/position_timing_wait_budget_control_f1_20261106.md | DESIGN_VERIFIED | none |
| F-405 | 本文Contracts-5及Risks | artifact:docs/architecture/position_timing_wait_budget_control_f1_20261106.md | DESIGN_VERIFIED | none |
| F-406 | intraday/wait_control.py::evaluate_wait_control | backend/tests/position_timing/test_intraday_wait_control.py；backend/tests/position_timing/test_isolation.py | VERIFIED | none |
| F-407 | intraday/wait_control_results.py::prepare/source_preflight | backend/tests/position_timing/test_intraday_wait_control.py | VERIFIED | none |
| F-408 | intraday/continuous_results.py::period_account_rows；intraday/wait_control_results.py::analyze | backend/tests/position_timing/test_intraday_wait_control.py；backend/tests/position_timing/test_intraday_period_continuation.py | VERIFIED | none |
| F-409 | 本文Contracts-7；intraday/wait_control_diagnostics.py | backend/tests/position_timing/test_intraday_wait_history.py；本文2026-11-07真实归因证据 | VERIFIED | none |
| F-410 | 本文Contracts-8；intraday/wait_control_history.py | backend/tests/position_timing/test_intraday_wait_history.py；artifact:X:/AIstock_temp/position-timing-wait-history-20261107/source_blocker.json | BLOCKED | 原价格来源617份固定路径文件缺失，真实prepare未通过 |
| F-411 | 本文Contracts-8；intraday/wait_control_history.py | backend/tests/position_timing/test_intraday_wait_history.py；backend/tests/position_timing/test_isolation.py | BLOCKED | 定向功能测试通过，真实历史plan及LocalSIM三户未接入；不得标完整交付 |

## 2026-11-06离线软件交付证据（历史记录）

择时业务实现与plan来源commit：`78eb97333bcab3f0d93ed5c7255749463e497f74`。后续文档、SDK消费边界测试及主线同步不改变plan绑定的12份共享实现LF哈希；完整源码/测试交付以PR最终HEAD和CI为准。
本轮唯一交付research_plan为
`X:/AIstock_temp/position-timing-wait-budget-implementation-20261106/artifacts/intraday_v1/wait_budget_plans/d9274736e8270a9d332ac205fe7d2a7ca93c444702f7cb941c2d2c4578fab99e.json`，
完整文件SHA256：`faa6ea4fcb7361cc0ecf7ed2e2bb52ad9d4e958ad43e2aacace66f6d22af4d9e`。
WSL路径将`X:/`替换为`/mnt/x/`。共同经济opening摘要为`1cc6971f686552b2d897692780b158deab7455ec806b71e0ecf34b504047e266`。
开发中间plan保留为不可变历史，不是本次消费输入，不覆盖或删除。

真实WSL文件prepare→inspect→同输入prepare通过；inspect=VERIFIED，exact retry=ALREADY_MATERIALIZED且FileRef完全一致。
51为计划账户日数，不是已计算数量；本轮实际新账户回放0日、新三户收益读取false，database/network/runtime/process均false。
另以重构后的共享归约函数消费原冻结13户221账户日，与原报告所有13账户及24比较逐项完全一致，未覆盖旧报告、未重新撮合。
功能夹具证据：42项相关小矩阵通过；最终新接口17项通过，包括拒绝恢复状态中混入B0预算前可见时间，只读inspect/依赖指纹的受影响路径定向复核通过。夹具不证明真实新三户收益。
变更范围L0/ruff、F1 8项/F2 12项通过；classifier为targeted_ci_required、workflow_gate=passed、unexecuted_test_files=[]。补齐公开SDK消费边界后，测试债务生产48,549/测试12,692有效行，26.14%，没有调整归属或分类。
既有隔离测试债务BUG-2193已由独立源码PR #7099和close-sync #7100完成；原main复现失败，修复后四项隔离合同通过，只补精确只读接缝，不修改其他模块。新研究SDK仅开放公开符号，写入/发布函数及其他业务消费者继续被拒绝。
上述为软件交付当时状态；后续51账户日和分析已完成，见下节。线上激活仍独立报告。

## Contracts-7：10月结果、归因边界与本轮研究顺序

LocalSIM交付`X:/AIstock_temp/localsim-w15-wait-budget-20261106/delivery.json`，完整SHA为`5bb1e318c46a99e95d9a561d54d3ded704051ec84ad0263e28b93af2c090ef15`；source PR #7109已合入。正式三户分析位于`X:/AIstock_temp/position-timing-w15-account-analysis-20261107/intraday_v1/wait_budget_analyses/d98f6febe9044184629426e8c61cbafd308aa1205baa0f164a15ef7a6ae0afb8.json`，完整SHA为`69f024ad315fcf3849e0c9730f833771e597fab4f533896a4c2ac76df6869e56`。

| 比较 | 成本后增量（元） | 共同期初收益差（bps） | 正向MDD降低（百分点） | 费用差（元） |
|---|---:|---:|---:|---:|
| C2−W15（主比较） | 1,047.78 | 1.0758 | 0.0251 | -3,515.11 |
| W15−B0 | 51,954.46 | 53.3447 | 0.4203 | -898.07 |
| C2−B0 | 53,002.24 | 54.4205 | 0.4454 | -4,413.18 |

共同opening为9,739,392.18元；三户期间收益分别为-7.4617%/-6.9282%/-6.9175%。C2−W15费用加回差额为-2,467.33元，净优势不足以证明更好的成交价格。固定等待值得扩展检验，C2反转条件的附加价值尚未证实，不据此调参。
三户生成父计划589/582/587，未派单13/22/82；动作失效原因均为OPPORTUNITY_WAIT，而非明确的TTL超时。C2首次外层机会早于15分钟的修订为69/587；首次机会不等于实际派单或成交。现有signal_revisions只保存首次机会和最后状态，不能从最终reason集合恢复转WAIT当刻的细分原因与完整生命周期。无该证据时记录UNKNOWN，不推断成价格反转或现金不足。

P0只读消费完整51账户日，逐BUY/SELL报告提前机会的派单/成交/失效去向、原始动作原因、数量与费用；同日同股同向同目标数量的价格比较仅是条件化子集，保留配对覆盖与双方未成交人口，不宣称纯执行alpha。所有收益读取前完成来源检查。
P1固定B0/W15/C2做1—9月历史稳健性检验。P2保留一个“价格朝本次交易不利方向持续移动时提前执行”的候选，当前不实现、不训练、不扫描参数。模型仅在需要预测等待相对立即执行的成本差时另立单一假设；未见数据不作为本轮开跑前提。
本轮producer exact retry为EXACT_ARTIFACT_RETRY_VERIFIED，账户重算0、新提交0；producer_economic_retry=NOT_SUPPLIED合法。时间加权暴露0/17日覆盖，保持UNAVAILABLE，日终现金均值不替代。quantity_matched=false、pure_timing_attribution=false、economic_confirmation=false。

## Contracts-8：1—9月独立历史研究合同

### 8.1 固定人口、来源与期初

仅复用原生目标清单2026-01-05～09-30的181日/9,050目标，三臂共543账户日；主反应延迟60秒、W15预算15交易分钟、原C2及B0、安全/队列/费用/14:57截止合同不变。连续现金账户、共同期初1000万元及空持仓，与父研究`initial_cash_cny`一致；这是新研究的显式起点，不充值既有账户，不按月重置，不从9月末倒推1月账户。
权威父请求为`X:/AIstock_temp/localsim-post-renewal-study-20261029/full/studies/54e1a6a55b13fa8916cf1084b9a8ad4398755fc9ea5028e09c51d608775cc25d/request.json`，完整SHA `4c60878d91a4c7a90fdd3f73baf48422556241d7866d0a34b82816fb4d72d8ee`；其effective candidate SHA `321a966c55822db5777dda4ef0091422e8a1877b7bcb6c27ffb763e9a2441c73`。价格模板保留原生价格来源candidate `f5af5cc0506e502300ef225805b51aaeb7ea1b1bc352fdc81f363f4933f5f8ed`及显式input_candidate后继链，不混用10月candidate `2242c4…`，不读取active/profile。
来源完整检查复用continuous_results.source_preflight及price_plan_campaign原生projection/completion/PIT检查，验证181份原生projection/completion、价集来源和所有引用SHA；行情批次完整验证仍由LocalSIM在新收益前完成。本窗口不重新推理或补数据。源价格计划中的UNKNOWN/NONE按原政策保留。
历史price manifest固定完整SHA为`4781d4fa7c27dd7f7c632090c32e87fb17a7d74dba225f607bcd638ddd2323bd`。当前price构造器指纹与当时不同，因此不得以当前build重建替换旧模板，也不伪称通过其重建式inspect。新入口只作冻结历史产物内容、其policy/source audit、所有source/day FileRef及原生PIT投影的完整闭合检查，保留旧producer commit/implementation；不是新价格计划生产。任一旧来源缺失仍失败，不因effective candidate存在就跳过旧来源。

### 8.2 择时公共入口与LocalSIM最小接入

新增`wait_control_history.HistorySourceSpec`冻结父request/source_inspection/result_receipt及price_manifest四个FileRef；`prepare(spec, source_commit, timing_root)`生成`position_timing_wait_budget_history_plan_v1`和声明性common_opening文件，绑定source_spec、实现closure、全部日期、人口/包/candidate/价格/费用/执行/operation身份。它不创建账户，不是LocalSIM执行请求。
公开`inspect_history_plan(FileRef)`只读核验plan和全部来源并返回plan，不读新三户NAV；LocalSIM可显式import该函数与`HistoryAnalysisSpec`，写入函数不得跨模块消费。旧wait_control_results/旧plan和旧51账户日不修改。
LocalSIM需要一个独立history request（`localsim_wait_budget_history_request_v1`）：按此plan从共同现金起点初始化三户，复用既有三户执行器、账户引擎和按日提交；不得调用旧10月parent_account_receipt续接路径。返回既有WaitBudgetFacts三户模型，research_plan指向新plan；producer_request额外绑定history_scope=`JAN_SEP_2026_INDEPENDENT_CASH_ORIGIN_181D_9050_TARGETS`。opening采用`position_timing_wait_budget_history_opening_v1`，完整保留common_opening字段和值，仅schema_version替换，并增加account_id/arm_id/rule/delay_seconds/operation/common_opening；本轮初始持仓为空，不接收账户引擎的额外状态字段。source_inspection采用原三户投影字段并绑定全543账户日，每个day的study_identity必须等于producer_request身份；旧17日语义不放宽。LocalSIM owner修改自己的代码，本窗口不代写适配器。

### 8.3 完整性、统计与结论

`HistoryAnalysisSpec`包含research_plan、facts和可选producer_economic_retry；新`analyze`先核验全部来源、三个opening及543个day SHA，再调用既有period_account_rows和三户诊断。source检查完成前不读取新研究NAV；少日/重复日/未知账户/资金漂移/非有限或未来NAV失败显式。分析产物不可变，exact retry只复用产物，不触发账户重算。
全期间三项比较保持C2−W15主比较；月度和季度以各账户上一期间末NAV作为区间期初，MDD从该期初及区间全部分钟NAV重算，不能取每日MDD最大值。月/季报告不代表重置账户，不把月收益直接相加，不与独立10月共同opening研究拼接累计收益。
不确定性只对全期间每日配对净PnL差/共同初始资本做5交易日循环区块bootstrap（2000次、seed=20261107）；报告均值bps/日的95%区间及三比较Bonferroni区间，不冒称实际累计收益或MDD的区间。月/季及BUY/SELL、波动/方向情形仅诊断，不回选规格；当前先输出月/季，行情分层须有明确前缀字段来源，缺少时UNAVAILABLE，不追加模型或数据依赖。不设置MDE或盈利准入。
生产入口只有REAL_HISTORY；测试夹具仅为定向测试证据。WSL账户内逐日顺序、账户间有界并行由LocalSIM控制。当前真实543账户日状态为NOT_RUN_SOURCE_BLOCKED，来源修复后仍需OWNER_INTEGRATION，不将本地分析函数通过说成账户已回放。

## 2026-11-07真实归因与研发阻断证据

P0完整51账户日只读分析报告：`X:/AIstock_temp/position-timing-wait-history-20261107/artifacts/intraday_v1/wait_budget_detailed_diagnostics/c494b597de6a5412b4821c1931819d2e1138202223cd25155063508e0ad3aa66.json`，完整SHA `d6bde9206f080ce82fa310e54fe292f2480f8bdbcbe3266ec0b2a57a66ba517e`。
69个C2提前机会对应69父计划，其中BUY全成4/未成31、SELL全成3/未成31。7个成交父计划的首次实际派单均不早于15交易分钟（6个15分钟、1个27分钟）；这批数据不能证明提前信号被提前执行。其余62个没有派单；失效细因继续UNKNOWN，不能从first/last审计推断。
C2−W15同日同股同向同目标数量子集：BUY配对163（两侧全部父计划300/295），双方全成143，条件成交价改善合计-8,382.97元；SELL配对165（全部287/287），双方全成140，条件改善-4,105.49元。两项只对双方全成的条件化子集计算、不含费用，不外推全部账户收益或纯择时alpha。完整报告同时保留NO_FILL交叉分母。P2不因这组事后结果直接投产。

P1真实来源检查失败于原价格来源candidate：`X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260930-qe_hmm_full_v2-direct-monthly-v2-unified-candidate-correction-4ab0b48642c3-margin1-minute1/`。父preflight与价格source audit合并后617份FileRef在固定路径不存在，包含manifest和日线参考价组件；修正版321a…candidate存在，但不是这些缺失文件的同一身份。
不可变阻断回执：`X:/AIstock_temp/position-timing-wait-history-20261107/source_blocker.json`，完整SHA `f084b76462b919291297f13c50bb906157d9559d0cff5b03f985014e921ed437`。未修改数据、未尝试在线补数、未读取新研究NAV、未生成真实history plan或启动543账户日。数据owner需恢复准确固定源字节，或返回正式不可变迁移合同供另行设计消费；不能静默指向321a…、忽略源SHA或重建旧价格模板。
新增12项定向测试通过，最终相关小矩阵33项通过（含原17项W15与4项隔离）；两轮代码审核修正了跨修订父计划重复计数及新研究day身份绑定。ruff、L0和diff-check通过；classifier覆盖全部6个变更文件，targeted_ci_required、workflow_gate=passed、unexecuted_test_files=[]。P0同输入exact retry为ALREADY_MATERIALIZED，报告FileRef相同；旧plan绑定12份实现没有改动。
F2蓝图验证12项通过；F1明确保留F-410/411未完成，验证FAIL，不把缺口改标VERIFIED来通过检查。尚未提交PR/运行远端CI，不满足完整合入条件；功能夹具不解除真实来源阻断，不合入或宣称P1已交付。工作稿与代码保存在独立任务分支，main没有本轮更新。
