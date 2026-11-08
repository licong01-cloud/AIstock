# P2：不利方向持续移动时提前结束等待

日期：2026-11-09。Owner：position_timing。Feature tier：F1。状态：IMPLEMENTING。

## Background

终极目标是改善明确BUY/SELL计划的成本后结果和回撤。既有181日三户研究中，C2相对B0净增584,422.39元、MDD降低2.8724个百分点；相对W15净增120,234.09元，但区块校正区间跨零且8月贡献超过全期。保留三者，不以收益回选月份或阈值。
本轮只研究一个假设：价格已经连续朝本次交易的不利方向移动时，继续等固定15交易分钟可能增加机会成本。BUY的不利方向是上涨，SELL是下跌。该假设不承诺盈利，也不预测当日最高最低点。前轮权威结果与完整SHA见[同等待预算设计](position_timing_wait_budget_control_f1_20261106.md)末尾。

## Scope

本窗口交付研究专用纯函数、可恢复状态、冻结研究plan/只读inspect、定向测试和真实来源验收；LocalSIM owner另行执行新P2账户181日，并交回事实后由择时继续经济分析。
允许写入：`backend/services/position_timing/intraday/adverse_wait_control.py`、`adverse_wait_research.py`、`backend/tests/position_timing/test_intraday_adverse_wait.py`、`test_isolation.py`及本文/既有同等待预算F1/日内F2蓝图。前缀不省略模块归属；operator和不可变验收产物仅在`X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/`。
复用core.evaluate、分钟/日级合同、CandidateReader、交易分钟时钟、freeze、原history来源预检；不修改旧core、W15、原统计函数或其冻结implementation closure，不另建Reader、撮合、账本或研究平台。

## Non-goals

不改线上PolicyId/API/UI或已有B0/W15/C2，不接QE/Advisory/HMM/MiniQMT，不训练/扫参，不用Oracle标签。新策略经济回放属于LocalSIM，本窗口不读生产数据库、不抓行情、不改数据集/active/profile、不控服务，不把软件或前缀机制检查称为P2盈利证据。

## Contracts

### 1. 唯一冻结规则

研究身份`ADVERSE_TREND_WAIT_15M_V1`，原始人民币分钟价。共享core FIRST_OBSERVABLE先验证身份、交易日/时段、可见前缀、停牌、方向性涨跌停、账户反馈和HARD区间。
从共享core实际设置的batch_started_at=t0起计时，最大等待仍为15交易分钟，午休按既有时钟剔除。15分钟以前，仅当最近5根已闭合且连续的一分钟bar满足以下条件，提前结束本批等待：5根end均晚于t0、均有成交量，BUY的5个close严格递增、SELL严格递减。5根复用既有core的5-bar结构，不新增幅度阈值、不按收益选择窗口，不使用未来high/low/VWAP或未来成交量。
一旦提前释放即锁定到本plan revision/batch；趋势随后消失不重新开始等待。之后仍逐次执行共享安全、HARD和有效期检查。若无提前信号，15分钟时按W15语义释放；这是策略明确的预算分支，不是数据故障fallback。缺少5根只会继续等待；来源错误、未来bar、修订冲突仍由核心显式拒绝。PREFERRED不构成额外价格门槛，与W15一致；NONE合法。

### 2. 状态和可见时间

`AdverseWaitSpec`显式绑定本策略source_commit、5根和15分钟，不改变底层B0 policy。`evaluate_adverse_wait(..., spec=...)`只返回外层decision/state，调用core.evaluate一次。LocalSIM只可用外层status/ID/advice_available_at，不使用底层B0更早的机会。
状态保存base_state及一次释放证据：ADVERSE_PREFIX保存当时5根bar，BUDGET保存预算时刻。恢复时核对spec、t0、历史时钟、已消费bar内容hash和原始可见时间；未来证据、缺失证据或重绑定返回UNAVAILABLE。相同plan/batch拥有稳定且与B0/W15/C2隔离的opportunity_id，decision_id绑定本次输入与前后状态。90秒TTL、14:57终点及原计划有效期保持；释放不是派单或成交，取消/修订必须使用新状态。

### 3. 一户新增、三户冻结对照

人口和日期沿用2026-01-05～09-30的181日、9050原生目标。新增P2独立1000万元现金/空仓账户，60秒反应、原操作员容量/排序、90秒TTL、14:57、费用、价集和分钟来源不变，账户内逐日顺序，未充值、不按月重置。
原B0/W15/C2的543账户日只读复用，不重放或覆盖。共同输入、期初、共享账户引擎/队列/撮合/账本身份必须一致；P2自己的adapter和纯策略实现独立绑定，不要求新adapter与旧producer字节一致。若共享经济链路确已改变，明确交回owner处理身份差异，不能把新旧链路当成同一对照。
固定effective candidate SHA `321a966c55822db5777dda4ef0091422e8a1877b7bcb6c27ffb763e9a2441c73`；旧f5af…仍仅作原价集来源。冻结原analysis_spec和报告FileRef；prepare/inspect先完整验证原plan、来源、opening及543日文件字节，再发布新研究plan，不读任何P2收益、不重建旧模板。

### 4. 最小LocalSIM合同

新plan为`position_timing_adverse_wait_research_plan_v1`，public `inspect_adverse_plan(FileRef)`返回已核验的plan和输入；仅该只读入口及纯P2函数/模型可跨模块import，prepare/发布/内部预检不开放。
LocalSIM新请求schema为`localsim_adverse_wait_history_request_v1`，绑定plan FileRef、producer commit、共享经济实现身份、单一P2 profile、共同opening和全部181日期；沿用已有分钟账户引擎和逐日提交，不改旧三户请求人口。返回单户P2的opening、有序181个day FileRef、全来源检查及不可变重试回执；day沿用现有NAV、父计划、动作/费用投影，增加P2释放证据的明确引用。实际数量/资金/撮合全部由LocalSIM负责，禁止使用旧账户执行反馈冒充新P2路径。
后续择时在完整来源检查后将新增181日与冻结543日合并评价；总证据724账户日，不产生旧三户的新提交。缺失时间加权暴露/完整生命周期保留UNAVAILABLE/UNKNOWN，不阻止新研究设计或运行，不建监控或ACK平台。

### 5. 评价冻结

主比较P2−W15，次比较P2−C2、P2−B0，共3项新增比较。沿用同初始资本成本后PnL/收益、全分钟NAV正向MDD、费用和BUY/SELL全部父计划分母；月/季按各自上期期末NAV承接。5交易日循环区块bootstrap、2000次、seed=20261107，报告每日配对净PnL差/共同初始资本的nominal 95%和三比较Bonferroni区间；不把它称为累计收益/MDD区间。
不挑月份、指数或成交子集做晋升，未成交保留；预算前机会、实际派单、成交分别报告。数据已被用于前轮机制研究，所有新结果属于探索性历史研究。quantity_matched=false、pure_timing_attribution=false、economic_confirmation=false；软件通过不晋升规则，不设置MDE、自然日等待或盈利准入。

## Implementation Plan

一个交付块完成本窗口：冻结本设计→纯函数/plan实现和定向验证→真实冻结来源prepare/inspect/exact retry与LocalSIM交接。账户经济验证由LocalSIM接入后继续，不新增细碎阶段或基础设施。

## Verification Plan

定向测试覆盖BUY/SELL对称、只用t0以后5个连续已闭合close、无趋势预算释放、锁定后安全约束仍有效、午休/缺bar、未来/重述、状态恢复与时钟/spec篡改、机会身份隔离、源缺失fail-closed及不可变重试。复用已有core测试夹具，不复制API/账本fixture。
真实验收只读冻结candidate/原价集和历史来源，先验证所有引用，再对固定预声明的小型真实分钟样本做前缀一致性和尾部扰动检查。工程人口预声明为000001.SZ/600000.SH、2026-01-05/01-06、BUY/SELL，共8条无量机会流；使用明确NONE工程约束，不改原共享价集或新经济研究的0.5%价集。缺数据不换股/日期，合法停牌保留。样本不挑收益，不宣称自然账户目标或新P2账户回测。运行于WSL的离线环境；旧账户/报告实现closure保持可核验。

## Risks

5根严格单调可能少触发，早释放也可能追涨杀跌或错过回落；本轮只冻结一次定义，是否有价值由后续完整账户结果回答。排队和资金反馈可能超过信号差异，不能用条件成交价格解释全账户收益。原研究已被观察，不是sealed holdout；缺失日志不反推原因。

## Production Gates

database_read=false；database_write=false；dataset_mutation=false；market_network=false；runtime_action=false；account_replay=false（本窗口）；backend_restart_required=false。不注册线上策略，不操作用户服务。

## Design Acceptance Index

| ID | 验收条款 |
|---|---|
| F-501 | 单一假设、固定5根/15分钟/方向对称，无收益回选 |
| F-502 | 共享安全authority且无旧规则/默认值漂移 |
| F-503 | 前缀、t0、闭合/连续/成交量约束，无未来输入 |
| F-504 | 一次释放锁定、证据恢复、时钟/身份拒绝 |
| F-505 | 机会/TTL与外层可见时间独立，提醒不等于成交 |
| F-506 | 原来源、543账户日及结果身份先检查后冻结新plan |
| F-507 | 单一新181日账户与旧三户只读对照，owner边界明确 |
| F-508 | 真实来源及前缀机制验收、inspect和exact retry |
| F-509 | 研究SDK隔离、无订单/服务/DB/旧资产写入 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-501 | 本文Contracts-1/5 | artifact:docs/architecture/position_timing_adverse_wait_p2_f1_20261109.md | DESIGN_VERIFIED | none |
| F-502 | intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | PENDING | implementation_pending |
| F-503 | intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | PENDING | implementation_pending |
| F-504 | intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | PENDING | implementation_pending |
| F-505 | intraday/adverse_wait_control.py | backend/tests/position_timing/test_intraday_adverse_wait.py | PENDING | implementation_pending |
| F-506 | intraday/adverse_wait_research.py | backend/tests/position_timing/test_intraday_adverse_wait.py | PENDING | implementation_pending |
| F-507 | 本文Contracts-3/4 | artifact:docs/architecture/position_timing_adverse_wait_p2_f1_20261109.md | DESIGN_VERIFIED | none |
| F-508 | intraday/adverse_wait_research.py | artifact:X:/AIstock_temp/position-timing-p2-adverse-wait-20261109/validation_receipt.json | PENDING | real_verification_pending |
| F-509 | intraday/adverse_wait_control.py；test_isolation.py | backend/tests/position_timing/test_isolation.py | PENDING | implementation_pending |
