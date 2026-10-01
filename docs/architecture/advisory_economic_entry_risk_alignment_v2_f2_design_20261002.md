# Advisory 经济进入价值：风险口径对齐与每日消费身份 F2 设计 v0.3

> 日期2026-10-02；状态OFFLINE_RISK_LABEL_AUDIT_VERIFIED_REUSE_BLOCKED，设计PR #5233已合入19719beb。独立风险标签、停止复用检查、D网格/身份合同内核已实现并经多轮审核；没有v2研究模型或生产绑定，每日API/UI尚未交付。父设计：[经济进入价值v1](advisory_economic_entry_value_v1_f2_design_20261002.md)。本文件不修订已消费v1模型或其负向结论。

## 1. Background / 事实与问题

离线源码PR #5224已合入`dba2028faae9f659eaf85bd2b3f834e7c332d40a`；57定向测试和必需CI通过。最终v1 study `adveconomic_4962fd2ec68948033628954a`真实训练一个固定模型；405个Top5实际条件中197个期望净收益正，但风险门槛通过0、模型TAKE0。模型臂7笔进入均是UNKNOWN研究控制，其正收益不是模型盈利。

v1采用全policy episode峰值至谷值日级mark回撤q90，并以entry stop800 bps作风险预算引用。这不是同一风险量。只读同路径诊断保留7,720条原候选，不重训/改标签：test中104条peak风险>800而entry最大净损失≤800，其中35条最终盈利；test两指标q90为989.01/816.73 bps。诊断证明口径差异，不证明v2模型收益或某个风险阈值合理。

诊断工件为父study下`risk_semantics_diagnostic_v1/evaluated/risk_semantics.json`，其合同/脚本hash在运算前发布；旧study/模型/评价/hash不覆盖。窗口已消费，后续新合同只允许NAVIGATION_ONLY，不能冒充独立确认。

## 2. Scope / 交付与非目标

本阶段先使风险指标与其预算引用显式对齐，并完成每日预测身份/价格网格的消费合同。源代码及可用历史导航随后按独立里程碑交付，不宣布整个价格建议产品完成。

不修改QE、Selection、StrategyPackage公共源码、Paper、Execution、HMM或数据准备模块；不重新选股、不新建父包实验、不读sealed。无数据库写入/DDL、profile激活、安装或进程控制，临时X/持久F。不能将v1的不完整历史股票池身份变成原生COMPLETE。风险度量修复不授权资金仓位或真实成交。

## 3. Architecture / 风险与身份分别建模

三种量独立命名：

1. `entry_net_max_loss_bps`：同一实际进入点、冻结exit policy下，日级可见mark的最大成本后净损失，基准为入场已支付金额。
2. `episode_peak_to_trough_drawdown_bps`：全episode曾经达到的净价值peak至后续mark的最大回撤，保留为辅助风险诊断。
3. `stop_loss_bps`：既有policy触发规则，基准和执行时点沿原policy；不是实现损失硬上限或风险分位数。

v2研究只验证语义修复，不改原退出policy、候选、成本、seed、特征或return模型参数。不做风险预算搜索，不改800数值去凑通过数；保持800作为既有入场止损的**保守研究参考**，明确成本/跳空/等待可使实际损失超出，不能把它宣称为用户已批准的q90/资金风险上限。生产风险预算需独立且明确的合同，未配置时仍可返回价值/风险估计，推荐状态为RISK_CONTRACT_UNCONFIGURED，不伪装SKIP或开启新的审批平台。

训练来源身份与每日输入身份分开。训练bundle永久绑定原研究来源；PredictionInputContext绑定新的D/目标T、名单/候选/来源hash。消费者匹配package/manifest、feature schema/order、policy/cost、股票池定义及价格坐标算法版本；不要求每天的candidate/source hash等于训练原始hash，也不能通过复制旧hash假装匹配。

## 4. Contracts / 真实标签与固定研究

对旧AVAILABLE episode逐个适配，入场支付额`paid=policy_entry*(1+buy_cost)`，未来合法日级mark的净清算价值`liquidation=raw_mark*policy_coordinate*(1-sell_cost)`。新标签`max(0, 1-min(liquidation/paid))*10000`；持有期仅在同冻结实际退出开盘前读取open/close，退出日只读open。入场成本/卖出成本各一次，不重复扣除。停牌carry既存复权mark，不发明raw quote；未知路径、可交易性或坐标使该条UNAVAILABLE，完整候选/日期不删除。

原peak风险必须在相同路径重算parity，旧return目标保持原值，成熟/删失/label_information_end与purge保持不变。新schema明确risk_metric，不能把新标签放进v1的daily_mark_drawdown字段。新label/query/bundle版本和parent lineage独立，v1模型和工件保持原貌。

这里的清算mark只用于估值，不证明在该mark实际可卖，也不使用未来high/low。全天停牌重复前一既存policy-coordinate mark，入场前没有可carry值或退出端点不可证明时为UNAVAILABLE；同日S/R歧义也不能自行排序。已AVAILABLE但新增可交易性检查发现未知的行仍保留原status与v2状态两列。涨停不等于盘中绝无成交，但本次冻结的开盘进入合同不证明其可执行，明确UNKNOWN，不凭价格相等断言真实成交。

首轮v2预算一个候选、一个新的risk head；return head优先只读复用已冻结v1权重与校准/支持域，不重新优化收益。风险头仍固定LightGBM quantile q90、200轮、depth3/leaves7/minleaf30/lr0.05/seed20261002/2线程，不进行test early stopping。模型变体计1，复用不减少研究族累计尝试披露。若来源、实际训练eligible、模型版本/feature order或原return parity不一致，fail closed，不能静默重训。

复用检查分别报告train/validation/test：train或validation的风险标签未知若改变原拟合/校准eligible集合，停止本候选拟合并列精确差异，不把未知补旧risk、不自动改训练集或重训return。test只作查询/结算，不影响权重复用；新增端点检查导致的test UNKNOWN可以保留，但比较臂共同报告其支持限制，不宣称与v1评价逐样本完全同口径。只有原拟合矩阵、return目标、purge及权重文件hash完全核验后才复用原return头；risk头训练行及支持域随注册收据保存。

条件预测不是价格干预的因果最优解。实际观察的gap标签来自原policy episode；改变假设买价可能改变退出时点，但当前标签未重建该反事实policy。网格仅为这些观察条件支持域内的模型推断，不复制未来路径制造不同买价训练样本，不宣称“最佳可成交价”或无条件跨价格有效性。拟合成功仍须独立证明干预价值。

训练和validation使用原chronological windows及information_end purge；test不拟合。真实拟合前冻结v2目标、来源hash、完整配置、800参考的用途、规则臂、UNKNOWN研究控制、comparison horizon和evaluation算法。新结果即便正也只导航，不以重新命名或label修复获取新的OOS；一次固定试验后不扫描第二/第三个风险目标或更大预算寻找赢家。

800 bps及q90是本导航研究的明确固定约定，二者不自动成为实盘产品默认配置。生产风险合同可由既有合法业务配置提供，无需新建审批流程；缺值只显示估计及缺口。进入风险≤800不蕴含期望收益>0，二者分开检查；相对空槽期望正也不蕴含相对原policy有正增量，后者以matched评价单独报告。

## 5. 每日消费合同 / 日频DB与合法D网格

每日从既有Advisory公开只读来源取得D截止特征、冻结候选和PriceRangeRealtimeContext。只读复用`PostgresRealtimeFeatureSource`、现有价格参考及法规价解析；不修改公共数据生产者。目标T raw参考只使用D可见公司行动、D raw close/factor，tick、board/listed age/ST历史状态必须有PIT证据；缺失typed unavailable，不能取当前ST状态或未来T的high/low/close补齐。

合法D价格网格由既有法规范围和tick明确生成，固定分辨率和行数预算；有合法unlimited情形而缺查询范围时返回明确缺口，不造无限网格或任意截断为“最佳区间”。网格之外、无统计支持、模型风险预算未配置分别命名。模型只在真实支持域查询，多区间不桥接UNKNOWN或拒绝节点；T只选D已冻结节点，不盘后重写D输出。

当前v1来源为RECOVERED_LIMITED，不能仅因未来某日输入原生就宣称模型训练或股票池可正式部署。缺训练scope身份时可做明确标记的历史研究，但正式每日角色不绑定。新包/新指数组合仍通过既有公开股票池合同验证，不在Advisory重新做QE alpha/seed研究。

训练scope的股票池定义身份与某日完整成员名单身份不同。v1仅对冻结selection运行语义计算了hash，不能把该hash解释为原生股票池定义/完整成员证明。新消费者必须分别核验universe definition、当日membership/candidate身份与证据等级；缺项保留UNPROVEN，不以当前配置回填历史。模型scope不兼容时拒绝经济角色，不阻断原Selection或其它已有效建议角色。

后续API/UI设计采用独立ENTRY_VALUE版本，旧OPEN_DISTRIBUTION和ENTRY_PRICE响应不变。默认未配置返回typed状态，不加载失败模型提供BUY，不以规则价填模型栏。先实现消费者/合同，再登记精确Advisory router/UI写范围；本设计阶段不更改这些文件。

## 6. Implementation Plan / 实施顺序与终止条件

1. 多轮设计审核、risk公式的手算/同路径parity及训练—预测身份审核；更新父蓝图最新事实。
2. 新v2 labels/contracts、原bundle只读复用、固定risk模型、支持域和语义明确的条件输出。
3. 一个预登记v2导航试验；同Top5固定空槽/no-refill、冻结exit/cost与实际开盘规则，报告实际模型TAKE/UNKNOWN控制/干预日和成本后matched结果。
4. 日常D网格消费者及独立API/UI实施设计/实现（精确路径先登记）；使用已有已消费历史输入或合成合同测试，不越过sealed边界。
5. 独立确认/角色绑定是后续条件性阶段；v2无真实干预或无增量时只交付结果，不激活、不改旧合同、不关闭整个业务方向。

明确写范围：本设计、父经济设计、主蓝图；新增`backend/services/advisory_model_first/economic_risk_alignment_{contracts,labels,training,inference,pipeline}.py`及对应叶模块必要测试。文件名目前为计划，未存在不视为实现。旧七个economic_entry核心文件与旧工件本阶段不改，不通过源码hash漂移阻断旧study读回。

## 7. Verification Plan / 结果验证

手算路径体现“上涨后回撤”和“实际本金损失”差异；成本各一次；D和T时钟、未知/正常缺失、label-end purge、原risk/return parity、来源身份、原bundle复用/无silent retrain、不同每日hash正常消费但异policy/package fail closed、支持域/空集/非连续网格、无真实TAKE不得报告模型收益。用最小fixture和定向测试，不制造测试债务或跨模块ownership转移。

历史来源及训练成功≠业务有效；完整日常功能≠模型确认；历史导航≠自然前向。未经独立确认不返回生产可用状态；后端源更新若需生效，等待用户重启再只读验证，不自行操作。

## 8. Design Acceptance Index

| ID | 本设计验收 |
|---|---|
| F-551 | 风险三语义独立，旧结果保留 |
| F-552 | 真实同policy路径标签、成本一次、未知保留 |
| F-553 | 新版本/身份，无风险目标或预算搜索 |
| F-554 | 固定risk head、原return权重复用与PIT purge |
| F-555 | 训练来源与每日预测来源身份分离 |
| F-556 | 合法D网格、PIT法规属性、节点支持域与T选择 |
| F-557 | UNKNOWN控制归因、matched评价、无激活 |
| F-558 | Advisory边界、日频DB、只读/无进程控制 |
| F-559 | 源码/接口/模型/生产分别验收，无部分冒充完整 |

## 9. Design Acceptance Matrix

本矩阵验收批准的离线标签审计及每日合同内核切片，不是整个价格建议产品。v0.2两轮设计审核后，v0.3又执行来源/公式、模型复用/PIT及输出状态三轮本窗口源码审核；真实只读prepare完成但weight reuse停止，没有研究risk fit。每日API/UI和经济确认仍待后续独立里程碑，不利用内核或合成测试宣称业务已经完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-551 | economic_risk_alignment_contracts.py | artifact: backend/tests/advisory_model_first/test_economic_risk_alignment_labels.py | VERIFIED | none |
| F-552 | economic_risk_alignment_labels.py | artifact: backend/tests/advisory_model_first/test_economic_risk_alignment_labels.py | VERIFIED | none |
| F-553 | economic_risk_alignment_pipeline.py | artifact: backend/tests/advisory_model_first/test_economic_risk_alignment_pipeline.py | VERIFIED | none |
| F-554 | economic_risk_alignment_training.py | artifact: backend/tests/advisory_model_first/test_economic_risk_alignment_training.py | VERIFIED | none |
| F-555 | economic_risk_alignment_contracts.py、inference.py | artifact: backend/tests/advisory_model_first/test_economic_risk_alignment_inference.py | VERIFIED | none |
| F-556 | economic_risk_alignment_inference.py | artifact: backend/tests/advisory_model_first/test_economic_risk_alignment_inference.py | VERIFIED | none |
| F-557 | §4、§6～§7；本次零模型仅prepare，matched研究待后续 | artifact: docs/architecture/advisory_economic_entry_risk_alignment_v2_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-558 | economic_risk_alignment_pipeline.py、§2/§5 | artifact: backend/tests/advisory_model_first/test_economic_risk_alignment_pipeline.py | VERIFIED | none |
| F-559 | §6～§7；API/UI/独立确认未交付且不冒充 | artifact: docs/architecture/advisory_economic_entry_risk_alignment_v2_f2_design_20261002.md | DESIGN_VERIFIED | none |

## 10. Risks / 风险

风险口径修复不是新alpha；复用同窗口意味着整个族仍在开发期。即便800参考通过仍可能跳空亏损；不能拿风险分位数当收益置信界或用户资金上限。预计弱信息/信号衰减、价格条件段样本少及单package scope会限制可用性，不通过更换名字绕开。正常未知保留，不用缺失或现金掩盖失败。

### 10.1 多轮设计审核记录

- 第一轮经济/PIT审核：补充分位数预算与止损触发非等价、成本一次、停牌估值不是成交证明，以及观察条件预测不是反事实最优价。保留旧负向结果，禁止结果后扩预算。
- 第二轮实现/身份审核：明确train/validation eligible不匹配即停止复用、test UNKNOWN不反向改权重；区分训练来源/每日输入、股票池定义/成员hash；原v1 seven-core源码不改，防止已有study reader失效。API/UI尚无写授权范围，本阶段不以合同代替其交付。
- DESIGN-COMPLIANCE-001：不称完整产品或经济有效；缺口/未知显式；不改旧政策/成本/结果；不引入新审批门禁或跨模块修改。结构validator另行验证，不能代替这些语义检查。

### 10.2 源码审核与真实prepare

三轮源码审核修复：训练行顺序以D/T/symbol精确对齐而非位置假设；核验原拟合/校准矩阵和return目标hash、split receipt、权重/feature order/LGBM版本；路径只读open/close且退出日只open；法规价单位本来是CNY，不能再次÷1000；输出根禁止越界链接；缺risk合同不等于正常SKIP，全部特征未知不冒充可估值；D网格先算节点预算再分配，拒绝无限/截断；T开盘消费重验hash和合法价、不桥接拒绝/未知节点。旧七核心源码未改，旧study仍可读。

27个直接测试PASS（2.75s），Ruff PASS，L0两扫描0 findings/0 blocking。合成训练只验证固定头/无test拟合的工程合同，不是研究模型。真实prepare study `adventryloss_daadbb8de554e555061df5d8`，输出根`F:/Dev/AIstock_model_artifacts/advisory_entry_loss_alignment_v2_20261002`，prepared stage hash=`bf6151dd28bd6d7720d4bcdb7f6903576cd5817d20d0023e54a6b791fa22586e`。

7,720条原候选全部保留；新风险标签AVAILABLE=7,336+349=7,685（349条为label-end purged），UNAVAILABLE=31，NOT_ENTERED=3，CENSORED=1。31个新未知中ENTRY_OPEN_LIMIT_EXECUTION_UNPROVEN=14、EXIT_OPEN_LIMIT_EXECUTION_UNPROVEN=17；其中真实拟合eligible变化train=22、validation=2。结果`BLOCKED_ELIGIBILITY_DRIFT/REUSE_BLOCKED_NO_FIT`，registry planned=1/generated=0/evaluated=0，未拟合研究risk或return，不计算新模型收益、不改变原v1结论。原source仍RECOVERED_LIMITED，不是native COMPLETE。

下一步不是放宽本复用合同：先独立审定“同一新risk目标和执行性过滤下，return与risk两个头一起按新eligible重建”的显式方案；冻结参数/seed/窗口/800研究参考不变，保留所有未知行和原工件，不把新拟合称为exact retry或独立OOS。当前零模型prepare不关闭经济价格建议方向，也不能被报告为模型效果失败。每日API/UI另登记确切文件，不阻断其合同设计。

## 11. Production Gates

design_source_merge=merged_PR5233；offline_label_and_daily_contract_kernel=verified；source_merge=pending；v2_research_model_trained=false；daily_api_ui_implemented=false；qe_experiment_submitted=false；database_written=false；profile_activated=false；sealed_holdout_accessed=false；binding_activated=false；backend_restart_owner=user。

## 12. Rollout / Rollback

先设计审核/合入，再独立实现及导航交付；每日接口和生产角色另有明确实施验收，不交付占位BUY。旧M4/v1及已保存研究结果不覆盖。新消费者出现身份/模型/数据错误，仅该角色typed unavailable，不改排名/重选或订单；回滚只针对新角色版本，后台重启和生产操作仍由用户决定。
