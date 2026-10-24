# LocalSIM连续研究共享价格计划 F1 设计

日期：2026-10-24；状态：DESIGN_VERIFIED_IMPLEMENTATION_IN_PROGRESS。
基线：05ee0a292820f3b5c424c278af3378883d9773a6；上位：[分钟蓝图](position_timing_intraday_advice_localsim_blueprint_f2_20261023.md)。

## Background / Scope / Non-goals

为既定LightGBM＋12因子连续研究提供共同执行价格偏好，保留当日目标BUY/SELL与可能仍持有的既往目标SELL。包、人口及日期由用户指定preflight和原生projection决定，不重新选股或训练价格预测。原288场景及三规则/参数/比较器不变。
仅新增择时公共构造器、只读冻结输入适配和不可变模板；数量、资金、成交、费用、12账户与收益评价仍属LocalSIM。本次不改变其代码或控制服务，不读数据库或网络行情。

## Architecture / Contracts

写入范围：intraday/price_plans.py、price_plan_campaign.py，一个test_intraday_price_plans.py，本设计及实施/交接记录。公共入口：`build_price_plan(reference, side, policy, available_at, revision=1, lifecycle="ACTIVE", previous=None)`，返回明确AVAILABLE/UNKNOWN及事件，不依赖消费者或账户。
reference绑定symbol/date、raw参考价或UNKNOWN原因、完整source FileRef及source_available_at。公共构造器返回现有PriceConstraint和LocalSIM事件字段；不修改PriceConstraint定义或让全核心强制要求价集。B0/C2可继续NONE；C1缺价集必须可见不可用。

政策`RAW_PREVCLOSE_PREFERRED_50BPS_V1`：读取candidate对应D日`prev_close.day.bin`，单位raw CNY，复用raw_price_li恢复float精度；不读取D日OHLC、收益、因子变动、native reference_price或模型输出来推断区间。BUY=[0.995R,R]，SELL=[R,1.005R]，0.01元tick向外舍入，mode=PREFERRED。数量与硬成交限制由消费者负责。
扩展0.5%是本次用户授权范围下的独立预声明研究政策：与旧工程政策保持同一公式以减少自由参数，研究可解释性和延迟敏感性；不是最优阈值、策略包原生预测或生产默认。原288政策适用范围及产物不回写。已见过旧样本描述性结果，因此本扩展仍标探索性，不宣称独立验证。
历史source_available_at与模板available_at默认D09:00（Asia/Shanghai），是原生PIT目标及当日交易参考价的显式盘前可见假设，不冒充真实发布日志；盘中数据只由原核心控制。有效期D09:30～14:57，收盘失效，不跨日携带旧区间。实盘或需要证明真实发布时钟的使用方必须提供真实source_available_at；无该证据不能宣称已验证历史送达时延。
修订仅作用于同symbol/side/date，revision从1递增，available_at严格递增且不早于来源；首条必须ACTIVE，旧条ACTIVE才可修订/撤销。CANCELLED/COMPLETED终止，不能复活或覆盖历史；构造器不推断成交完成。冻结批次仅使用每日ACTIVE revision=1。

批次先核对preflight完整SHA/schema、六项输入文件、包manifest canonical身份、selection_inventory及全部181份projection/completion的SHA、包/日期/PIT cutoff/目标身份。原生日历与candidate day calendar完全一致；模板只使用截至D已经出现的股票，不用未来目标构造历史范围。所有来源文件SHA与实现LF归一化身份进入policy_document/manifest。
每D：当日targets发BUY；截至D所有曾出现targets发SELL。此累计集合是可能持仓的保守上界，不代表实际持有、不决定数量；无用的历史BUY不产生。预检统计为BUY=9050，SELL=64947，615只累计股票，共73997个计划模板。

### UNKNOWN与消费差异

读取冻结参考价已发现14个SELL不可用：002743.SZ在09-07非有限值；688496.SH在09-11～09-30的13个交易日超出该参考序列覆盖。保留原source身份与typed reason，不以旧价/factor/native reference_price补造区间，不删股。
未知事件使用mode=NONE、intervals=[]，保留同样日期/来源/有效期；typed availability与reason置于随policy绑定的reference audit。它符合提供的JSON schema，但LocalSIM `PriceTemplateEvent.coherent`当前固定按PRICE_TOUCH构造PlanDraft并要求非空intervals，故整批业务消费仍需要最小owner调整。交付清楚标`CONSUMER_UNKNOWN_REFERENCE_SUPPORT_REQUIRED`，不把schema通过说成可启动12账户。
最小差异：LocalSIM读policy_document绑定的reference audit，接受已声明UNKNOWN的NONE事件；B0/C2保留合法NONE；C1对实际所需UNKNOWN计划记不可用/未成交并保留人口与账户终值，不生成默认价集。未声明NONE、来源漂移或UNKNOWN与审计不一致仍失败。具体账户估值及成交处理归LocalSIM，择时不代改。
缺源文件为UNKNOWN并保留manifest声明；来源SHA/size漂移、非法bin结构或身份/目标冲突直接失败，不伪装普通缺价。

## Implementation Plan

同一实施包：冻结设计 → 纯构造器和batch CLI → 定向业务测试与多轮审核修订 → WSL整批构造/inspect/exact retry → CI与源码交付。无额外阶段或审批，不等待后端重启和未来交易日。
CLI `build --preflight <绝对路径> --preflight-sha256 <完整SHA> --templates-schema <路径> --templates-schema-sha256 <SHA> --event-schema <路径> --event-schema-sha256 <SHA> --producer-commit <commit> --timing-root <外置根>`；inspect使用manifest路径与完整SHA。输出policy_document、reference_audit、181个每日事件文件、templates.json和manifest，均不可变，新身份不覆盖旧产物。
复杂度：原生投影O(9050)，价序列每股票只读取/核验一次；模板O(9050+64947)，不乘三规则或四延迟，不读分钟或模型。内存为来源清单、615只日价序列和一个交易日事件，逐日发布；inspect重建摘要核对，不运行账户或收益。

## Verification Plan / Design Acceptance Index

| ID | 验收内容 |
|---|---|
| F-001 | 公共50bps构造、raw/tick/可见/生效/撤销语义正确，NONE不扩散为全核心必填 |
| F-002 | preflight/package/candidate/inventory/原生PIT来源身份完整；无重选与未来目标 |
| F-003 | 每日当日BUY及累计SELL完整，UNKNOWN保留、无合成区间与假成功 |
| F-004 | 政策/模板/source SHA绑定，不可变inspect与exact retry，整批schema检查 |
| F-005 | 最小消费差异明确；旧288/核心/其他owner/数据库/服务保持边界 |

测试仅覆盖有业务风险的买卖边界/修订、未来引用、整批人口、身份漂移与未知来源，不复制策略内部快照。真实数据验证只读来源并检查产物，不计算经济收益。

## Rollout / Rollback / Risks / Production Gates

纯离线新增，无路由/核心/运行配置变更，backend_restart_required=false。回滚停止引用新template manifest，旧产物保留。LocalSIM差异由用户转交，消费者未完成时明确未解除完整运行条件，但不阻止本模块交付。source_visibility为假设、50bps非预测、累计SELL非持仓事实是主要限制。

## Design Acceptance Matrix

DESIGN_VERIFIED仅表示设计闭合；开发后回填真实证据。消费者修改不是本次实现验收条款。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | price_plans.py公共合同 | artifact:docs/architecture/position_timing_shared_price_plans_f1_20261024.md | DESIGN_VERIFIED | none |
| F-002 | price_plan_campaign.py输入适配 | artifact:docs/architecture/position_timing_shared_price_plans_f1_20261024.md | DESIGN_VERIFIED | none |
| F-003 | price_plan_campaign.py人口/未知审计 | artifact:docs/architecture/position_timing_shared_price_plans_f1_20261024.md | DESIGN_VERIFIED | none |
| F-004 | price_plan_campaign.py不可变输出 | artifact:docs/architecture/position_timing_shared_price_plans_f1_20261024.md | DESIGN_VERIFIED | none |
| F-005 | 本设计边界/消费差异 | artifact:docs/architecture/position_timing_shared_price_plans_f1_20261024.md | DESIGN_VERIFIED | none |
