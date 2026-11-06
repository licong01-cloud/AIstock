# 盘中择时：同等待预算对照 F1 设计

日期：2026-11-06。Owner：position_timing。状态：TIMING_SOFTWARE_VERIFIED（源码交付证据见PR）。
已实现择时侧研究接口与三户统计；不表示LocalSIM接入或新账户实验已完成。

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

允许写入：`intraday/wait_control.py`、`intraday/wait_control_results.py`、`intraday/continuous_results.py`，对应`backend/tests/position_timing/test_intraday_wait_control.py`，本设计及上位蓝图进度段。三个实现文件前缀均为`backend/services/position_timing/`；冻结研究产物仅写本窗口X盘根目录。
实施仅限研究侧纯机会判断、直接测试和自身文档；LocalSIM账户接入由其owner单独完成。
复用现有CandidateReader、共享价格计划、core.evaluate、trading_minutes、FileRef、freeze和LocalSIM账本/撮合，不建第二套Reader或账户引擎。

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

## Implementation Plan

设计交付#7091已合入。本次完成研究专用W15、三户来源/统计合同和直接测试；旧13户归约入口不放宽。WSL旧13户实际统计等价核验不重跑账户、不覆盖旧报告。
后续由LocalSIM owner按Contracts-4/6完成最小研究接入，先短窗口联调，再WSL执行51账户日、inspect和不可变结果重试；择时消费其正式输出。普通回放不自动整批经济重算。
不拆更多阶段，不等待在线重启或未来行情。账户接口未接入时交回准确需求，不在择时侧临时模拟收益。

## Verification Plan

本次检查固定输入SHA/全13户诊断、完整分母、计划/排队定义及跨模块边界；设计验证不冒充代码测试。
直接测试覆盖15分钟边界/午休、t0之前失败、建立时钟后暂不可交易、HARD不豁免、未来bar、计划修订/过期、外层机会时间和B0去重身份隔离、共同初态/来源失败、三户统计、可选投影与NAV归零；不重复复制行情/账本测试。
联调验证同opening克隆、B0等价性、三户完整来源/NAV、未成交分母、原三规则不退化和exact retry新身份。代码审核与CI由各owner完成，不增加研究审批。

## Risks

同最大预算不等于同实际等待；单操作员排队可主导总延迟；C1费用降低可能混入少交易和低暴露。
10月不是未见样本；分钟成交为代理，不证明真实盘口成交。择时侧合同已实现，但LocalSIM新三户接入和真实新实验尚未执行，不能称经济验证已完成。

## Production Gates

本次database/network/runtime/process/training/account_replay=false；无部署、重启、DDL/DML或生产激活。
DESIGN_VERIFIED仅表示设计闭合；择时软件验证、LocalSIM接入和新51账户日经济结果分别报告，不以fixture或prepare成功声称经济有效。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-401 | 本文Background及Contracts-5 | artifact:X:/AIstock_temp/position-timing-october-account-analysis-20261106/intraday_v1/wait_budget_diagnostics/312e5cec8babb45f550b5c636540e15b1910ead5579315bb54848dcef0db57bf.json | DESIGN_VERIFIED | none |
| F-402 | 本文Contracts-1 | artifact:X:/AIstock_temp/position-timing-october-account-analysis-20261106/verification-receipt.json | DESIGN_VERIFIED | none |
| F-403 | 本文Contracts-2/3 | backend/services/position_timing/intraday/core.py::evaluate；artifact:docs/architecture/position_timing_wait_budget_control_f1_20261106.md | DESIGN_VERIFIED | none |
| F-404 | 本文Contracts-4 | artifact:docs/architecture/position_timing_wait_budget_control_f1_20261106.md | DESIGN_VERIFIED | none |
| F-405 | 本文Contracts-5及Risks | artifact:docs/architecture/position_timing_wait_budget_control_f1_20261106.md | DESIGN_VERIFIED | none |
| F-406 | intraday/wait_control.py::evaluate_wait_control | backend/tests/position_timing/test_intraday_wait_control.py | VERIFIED | none |
| F-407 | intraday/wait_control_results.py::prepare/source_preflight | backend/tests/position_timing/test_intraday_wait_control.py | VERIFIED | none |
| F-408 | intraday/continuous_results.py::period_account_rows；intraday/wait_control_results.py::analyze | backend/tests/position_timing/test_intraday_wait_control.py；backend/tests/position_timing/test_intraday_period_continuation.py | VERIFIED | none |

## 当前离线软件交付证据

择时实现来源commit：`54e4181988ba34d2b787eae48062a970888df2a9`（后续仅补交付文档；plan另绑定12份共享实现LF哈希）。
本轮唯一交付research_plan为
`X:/AIstock_temp/position-timing-wait-budget-implementation-20261106/artifacts/intraday_v1/wait_budget_plans/4c1c785fcc823072e72343bca5be764b72e4b007ae4a7aa3bf41363624d0141c.json`，
完整文件SHA256：`dd1d4890e7835c49949eb428191ce675acb781402db7a5404109a9d4d60463a5`。
WSL路径将`X:/`替换为`/mnt/x/`。共同经济opening摘要为`1cc6971f686552b2d897692780b158deab7455ec806b71e0ecf34b504047e266`。
开发中间plan保留为不可变历史，不是本次消费输入，不覆盖或删除。

真实WSL文件prepare→inspect→同输入prepare通过；inspect=VERIFIED，exact retry=ALREADY_MATERIALIZED且FileRef完全一致。
51为计划账户日数，不是已计算数量；本轮实际新账户回放0日、新三户收益读取false，database/network/runtime/process均false。
另以重构后的共享归约函数消费原冻结13户221账户日，与原报告所有13账户及24比较逐项完全一致，未覆盖旧报告、未重新撮合。
功能夹具证据：42项相关小矩阵通过；最终新接口16项通过，后续只读inspect/依赖指纹的受影响路径定向复核通过。夹具不证明真实新三户收益。
变更范围L0/ruff、F1 8项/F2 12项通过；classifier为targeted_ci_required、workflow_gate=passed、unexecuted_test_files=[]。提交前测试债务生产48,546/测试12,655有效行，26.07%，没有调整归属或分类。
CI、源码合入和本窗口状态以本次PR为准；LocalSIM接入、51日经济结果与线上激活仍分开报告，无需等待后端重启交付这些离线接口。
