# 申万L2已有持仓：VIX辅助的五日风险处理参考

> v1.1；2026-11-09；F2；父蓝图§1.29～§1.31/§7。状态：`REFERENCE_COMPLETED_STOP_NO_INCREMENTAL_CASE`。用户于2026-11-09明确答复“批准完整推荐合同”，批准下列 `L2-HELD-VIX-D1～D6`，允许实施及一次零fit参考；不是已批准QE消费或生产仓位公式。批准设计#7163、源码#7165已合入；唯一参考及独立算术读回完成，结论见§12。不新增QE账户或搜索第二公式。

## 1. Background / 目标与证据边界

终极目标是申万二级行业辅助信号在真实股票消费中改善收益/损失权衡。已有成员损失C＋自然新买调权六账户已结束，平均收益差近零、回撤未改善，不再追加原组合训练或账户。VIX新买5日关系也弱，不推进禁买。唯一保留的问题是：**已有L2持仓的短期风险处理，在扣除少持仓效应和恢复成本后，是否还有值得QE验证的增量？**

父蓝图§1.29的137日持仓5日相关−0.119911，只是固定持有参照。隔夜跳空已经在开盘前发生；原观察没有真实减仓、恢复和再投资账本，不能把参考差额当作可避免亏损或净收益。当前研究窗口被多次使用，VIX发布时间UNKNOWN/NON_VINTAGE；下文全部结果仍是探索性历史参考，不是严格as-known、未消费holdout或已证实的盈利能力。

本设计不训练市场模型，不替换C模型/20维/季度/10日标签，不改旧自然新买κ公式；新增的仅是一个**已有持仓处理参考**，其中5日是消费观察区间，不是将原模型偷换为5日预测器。原C的`target_first_return_date`/`target_last_return_date`仍是其原10日标签区间（例如4/1 prediction的标签4/2..4/16），不重写成本参考的开盘至恢复开盘区间；这项目标/消费时段不一致是待检验局限，不是批准重训或重复滞后的理由。

## 2. Scope / Architecture / 最小复用

HMM负责一个离线纯计算service、一个CLI及直接测试，输出一次紧凑结果；不开发调度、数据库表、API/UI、研究登记平台或环境变量开关。正式账户接线和实验始终归QE，荐股/LocalSIM本轮不接入。

实施范围仅为 `backend/services/hmm_risk/risk_l2_holding_vix_reference.py`、`scripts/hmm_risk/run_risk_l2_holding_vix_reference.py`、`backend/tests/hmm_risk/test_risk_l2_holding_vix_reference.py` 及本设计/父蓝图。三份源码已实现，未修改其他业务模块。正式request显式提供原资产绝对路径和WSL/node1只读文件解释器/连接目标，不使用环境变量或默认数据路径；不向远端写入程序或启动服务。

已核实的可复用入口：

- `backend/services/hmm_risk/risk_l2_member_loss.py::FrozenMemberSource(features, original, *, provider_absence_manifest)`：正式文件reader、calendar、security、分类projection、价格编码、停牌与输入稳定性；复用而不复制另一套解码/行业编号。
- 同模块 `pinned_json(path, expected)`：原文件pin验证。原 `risk_l2_member_release.py::validate_features`/`validate_model` 只用于核对载体及原模型身份；本研究不得调用fit、prepare训练或reinfer。
- 既有QE原生positions/actions/indicator文件及封存receipt：只读已完成B0账户，不调用QE提交API，不写其workspace。`stock_trades`推导摘要不能替代原生持仓/订单记录。

### Implementation Plan / 实施方案

按已批准合同，在同一任务内完成源码、反例测试、两轮作者审修、最小验证及授权源码交付，然后固定clean HEAD执行一次参考计算和独立算术复核，给出继续/停止建议。不把reader、测试、审核、PR各拆成独立研发阶段。

## 3. Contracts D1 / L2-HELD-VIX-D1：唯一假设与预算

唯一假设：当理论上已可得的最近VIX官方收盘相对前一个官方发布日上涨时，以冻结C的**L2相对损失秩**有界减少已有持仓；与同一时点、同一减仓金额的均匀减仓参照相比，是否仍能减少不值得承担的损失。

固定批准参数：`holding_sessions=5`、`max_stock_reduction=0.10`、`vix_change_trigger=>0`，不搜索阈值/恢复天数/行业分组/第二公式。0.10只是有界研究幅度先验，借用既有研究量级，不是继承旧自然新买公式的授权或已由产品价值推导的最优比例；VIX零变化自然分界由本次精确批准固定，不是此前描述性切片的自动升级。

新增fit=0；模型推断=0；新QE账户=0。只读既有C全131预测，不重新生成概率、标签或模型。三个seed只复用原B0不同持仓队列，不增加seed，不视作三份独立市场历史。

## 4. Contracts D2 / L2-HELD-VIX-D2：输入、时间与完整性

| 输入 | 本次明确冻结身份 |
|---|---|
| QE/HMM文件release | `20261030-monthly-v2-unified-correction-4388f6c6d166`；manifest canonical `772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d` |
| C预测 | `F:/Dev/AIstock_runtime/hmm_l2_member_loss/20261106/value_dual_v1/C_20260401_20261030.collection.json`；文件SHA `5d41be51fee66df0f58d47cda24f2eb07004427e163222288b8d18c749020ea7`；完整142日×131，原季度模型身份逐行保留 |
| VIX核定共享包 | `X:/AIstock_shared_inputs/tushare/20261109-vix-cboe-correction-v1`；manifest文件SHA `5cf59b8af771588baf9f81115a532991ef18b339ae20842fd97adf968d299bf2`；`vix_index.parquet` SHA `2a77f97ebe2cb4f16263eec9adb94788d599f3e44cf085cfd5e651bba11ce551` |
| 原B0队列 | `qe_20261107_123555_6217`，Loop7/8/9，seed42/2024/2026；2026-04-01..10-29完整141账户日；原receipt `F:/Dev/AIstock/tmp/handoff/hmm-member-value-dual-entry-20261106/qe_member_result_20261108.json` SHA `007ee44a7aa2622533881fddb47250efbbb593781c56a5fc882b51c83701ba42` |
| 上轮分析 | `F:/Dev/AIstock_worktrees/hmm-vix-actionable-value-20261109/tmp/handoff/hmm-vix-actionable-value-20261109/verified_result.json` SHA `979a2e652f4b8a450a9fcf1bd1fc7206a164258ab7c031cc6665b05cb65045ba`；只作已完成事实/原生文件pin入口，不重跑该诊断 |

正式request显式传root和各pin，验证真实文件、原生positions/actions、calendar、security、provider absence、PIT和C-013/release L2投影；pin来自原receipt/manifest，不能临时按“当前文件hash”自授权。旧研究执行HEAD仍是原值，不能改成新设计HEAD。禁止latest解析、旧路径回落、运行时数据库补数及candidate/profile写入。

每日决定时点固定A股09:15；`as_of=previous_A_open_day`、C `trade_date=decision_date`，不再shift一次。VIX用纽约16:15日终的DST换算作为**理论时间代理**，取该截止前最近两个真实Cboe发布日，`change=close_recent/close_previous-1`。沿用已核定发布日序列，不套用美股休市日历，不虚造缺失行；正常跨市场假日允许引用最近已发布事实并报告age/重复引用，不等于制造前填报价。重复引用同一VIX observation不得触发第二episode。未知缺日/非有限/冲突fail closed，历史发布时间继续UNKNOWN；不把理论日终或11月抓取时间当作早期发布证明。

研究decision集合保持原141日，文件估值最多读到同一release的2026-10-30，不增加新时期。迟于cutoff的恢复端点标为`RESTORE_ENDPOINT_NOT_MATURE`，全部decision/episode仍列出，不能静默缩窗或提前平仓。首日原持仓为空为`NO_PRIOR_HOLDING`，不是数据缺失。

## 5. Contracts D3 / L2-HELD-VIX-D3：减仓、等待与恢复

### 5.1 信号与L2范围

使用decision前一交易日收盘的实际B0持仓，不能按当天成交/未来存活股票选样本。模型侧完整131目录、availability/reason及申万文本代码保留，不用display TopK裁剪。

读取`hmm_member_prediction_transport_collection_v1`、arm=C，明确使用`expected_member_loss`（原clip[0,1]值），`raw_expected_member_loss`仅保留审计，不在看到结果后切换。验证`trade_date`、`as_of_date`、`vintage_id`及对应`model_sha256`，不读取标签结果。对decision当日available的C值按**全目录可用行业**作升序average-rank：`r_g=(rank_avg_g-1)/(n_available-1)`，`n_available>=2`。不在持仓行业中重新排序；并列保持平均秩，不用文本代码破同分。全同分时各r=0.5，行业定向与均匀参照应完全相同；少于两个可用行业为`NO_IDENTIFIABLE_L2_RANK`，不生成默认安全分数。

股票按**同release、as-of合法成员身份**映射L2；不复用已过期或缺股票的QE当日映射，不按未来分类回填。退出当前PIT但仍持有的股票不能删除：当as-of有正式映射时按该映射处理，否则显式`HELD_CLASSIFICATION_UNAVAILABLE`并保持原持有、不应用本overlay，不推断低风险。模型合法unavailable同理；未知代码、歧义、hash漂移则是合同错误，不混作合法NA。

记本episode可作风险处理的已有股票集合为U，其余已有股票在三臂均保留原持有和原权重，不能删除后重归一化。`c_i=0.10*r_g`（i∈U）是拟减少的**原持有数量比例**，不是概率、不是1-p，不影响新买名单、股票分数或未持有股票。

### 5.2 唯一episode状态机

- 先仅按VIX和calendar冻结三个seed共同的episode时序：空闲时，若最近可用VIX变化严格大于0且该observation未触发过episode，启动一次episode；不根据任一seed的收益/行业/持仓选择触发日期。其他decision保留明确no-action原因。触发只看当时输入，不看未来收益。
- 每个seed在同一episode分别核对已有持仓/U。空持仓、无可用L2或所有持仓c_i均为0时保留相应no-action原因，episode计时仍按共同日历推进；没有本金时不制造0收益参与均值。合法NA不影响其他seed的记录，但三seed主汇总必须披露完整配对mask。
- 触发日t的开盘作为参考减仓时点；持有区间为t至t+4五个A股交易session，t+5开盘为恢复端点。该端点比旧“第5日收盘”诊断多一个隔夜，必须披露，不能直接沿用旧0.4288个百分点参考收益。
- 活跃episode不叠加减仓、不逐日重排、不因VIX继续上涨延长计时、不因回落提前恢复。恢复日完成后最早下一A股交易日重新判断，避免同一时点卖出/买回再卖。
- 恢复只使用该减仓实际留下的参考现金预算，按恢复价格买回相同股票；价格上涨时恢复股数可能少于原减仓股数，不用额外现金、杠杆或强制补足原股数。实际恢复行为仍未被本研究执行。

正常停牌、跌停/涨停或端点不可成交不能伪造成成交。参考价格存在不证明市场单能成交；保持`execution_status=NOT_SIMULATED`，另报告已有真实事实能确认的受限状态，未知微观成交条件保持UNKNOWN。不得为了得到完整路径跳过行业、填价、前填或改窗口。

## 6. Contracts D4 / L2-HELD-VIX-D4：三臂与同敞口参照

这是**已知起点持仓的封闭五日参考事件**，不是沿原QE路径修改订单后得到的新账户。各episode起点仍取原B0实际持仓，不用上一episode假想持仓承接；不同episode收益不能复合成NAV，也不把原B0 episode内实际卖出/新买混进固定篮子。参考允许分数股数量，因而不声称已验证整手、最小手续费或实际订单可执行性；采用同一冻结QE计价基础，不能将raw、复权价、数量单位混用。

三臂固定：

- `B_hold`：同一完整起点股票篮子固定持有至恢复端点；不是原B0真实账户收益。
- `R_hold`：对U中股票按c_i减少已有数量，现金零利息，剩余固定持有；端点用留下预算恢复。
- `X_hold`：同一U按统一比例λ减少持有，使参考开盘减仓金额与R完全一致。设已知原数量q_i、参考减仓开盘价P_i，`M_R=sum_U(q_i*P_i*c_i)`、`V_U=sum_U(q_i*P_i)`、`λ=M_R/V_U`。U外不动；V_U为0时无可比较事件，不除零。

λ使用**执行参考时点的开盘价**匹配资金，而非09:15已知量；因此X明确为暴露归因参照，**不是已批准可下单的QE策略**，不能宣称09:15能够精确知道成交后敞口。未来QE若接线必须定义因果的目标敞口对照并报告真实成交偏差，不能直接复制本λ成为生产决策。三臂起点金额相同不意味着路径中敞口始终相同；同时报告逐日股票市值/现金漂移，不把它称为全程恒等暴露。

以完整原篮子的开盘市值`V_all=sum_all(q_i*P_i)`为固定分母，恢复端点价格为Q_i：

`R_minus_B_gross = -sum_U(q_i*c_i*(Q_i-P_i))/V_all`

`X_minus_B_gross = -λ*sum_U(q_i*(Q_i-P_i))/V_all`

`R_minus_X_gross = R_minus_B_gross-X_minus_B_gross`

这显式剔除“少持仓本身”的起点金额效应并展示原上行机会成本；不只算负收益。分母不是账户总净值，必须标注`prior_holding_budget_return`，不得换算成QE真实账户收益或年化收益。

固定0/5/10/20bp为**一整个卖出+恢复的名义来回费用**，乘以`M_R/V_all`，不逐边再乘2。它是示意成本而非原生实际手续费；参考卖出并恢复增加成本，不能写成省下新买成本。R/X起点减仓金额相同，所以同一名义费率的差分费用相同；这不能证明实际费用/可成交性相同。10bp为预先指定的主展示，其他三档仅敏感性，不按最有利费用择优。原QE NAV已经含费，不再扣这一层；本研究只计算独立参考事件。

若三臂共同必需的价格存在合法端点NA，整个配对事件保留typed NA及完整股票清单，不删股票重归一化或将零收益当NA；未知缺数/身份错误停止并列精确键。不能声称缺失事件通过或据合法NA判模型失败。价格编码、复权基准或公司行动无法闭合时标记`VALUATION_BASIS_UNRESOLVED`，不能把share变化当价格收益。

## 7. Contracts D5 / L2-HELD-VIX-D5：评价与结论

主观察是R−X的5日**配对参考净增量**，同时必须给R−B收益/下行/上行机会成本、减仓资金、恢复股数差、每日暴露漂移、可成交约束及成本敏感性。1日/10日只引用父蓝图旧诊断，不重跑以选最好horizon，不改变原模型标签。

完整列141个原decision、触发/等待/恢复/no-action/未成熟/合法NA及每类分母；模型输入142×131完整性不被episode筛选替代。按共同episode日期配对三个seed，再做日期等权汇总，三个seed不是三个独立市场样本。Q2/Q3/10月固定分段、单episode贡献及剔除最大绝对贡献episode的敏感性全部展示，不把该敏感性当新筛样本规则。

不对离散episode收益拼接净值、计算“账户MDD/Sharpe/CAGR”；可报告每episode固定篮子的最大不利路径，但不能称策略回撤。样本少、窗口已消费、选择偏差和发布时间未知均单独披露，不新增“全部月份/seed显著”门，也不把相关区间跨零单独当模型失败。

本参考最多支持以下一项取舍：

- `STOP_NO_INCREMENTAL_CASE`：未表现可解释的R−X增量，或R−B机会成本/恢复代价抵消其价值；不继续搜索阈值/天数/幅度。
- `LIMITED_RESEARCH_CASE_FOR_QE_REVIEW`：有值得研究的增量且限制可明确解释；仅交QE评审真实消费及交易合同，不自动准入、晋升或启动实验。
- `INCONCLUSIVE_REFERENCE_LIMITATION`：缺少必要估值/执行事实或样本无法支持取舍；列明确缺口，不假装收益已证实，不自动转成模型失败。

以上是完整结果后的研究建议，不是自动交易/晋升数值门。任何很小的正参考差不自动等于业务有价值；本设计没有批准可承受收益牺牲，也没有批准生产采用。HMM先做此一次低成本参考，只有确有继续价值才让QE承担真实执行验证。

## 8. Contracts D6 / L2-HELD-VIX-D6：实施、验证与停止

用户已精确批准D1～D6，批准设计#7163及源码#7165已合入，唯一参考已执行并以§12的STOP终态结束。审批仅针对新增消费语义/参数，沿用既有权限边界，不为旧已完成诊断另加审批。正式执行无fit、无新推断、无QE任务、无DB连接、无服务控制；旧C/M0/M1/preset_A及资产身份不改。

源码完成后最小HMM定向测试、Ruff、compile、diff/scope、F2及实际changed-files对应门禁，至少两轮作者审修；完整矩阵交CI，不本地反复运行。固定最终源码和输入pin，只执行一次正式参考；独立算术复核复用同一输入，不增加模型候选/账户/实验预算。失败保留具体错误，不换参数重试到成功；真实源码错误另走BUG修复，并保持合同和原失败结果可区分。

结束条件是“一份完整有限结果＋继续/停止建议＋必要owner交接”，或真实输入/权限阻断。后续合法实时C技术验证仍按父蓝图条件另行判断，不为了本研究等11/10、延长authority或部署经济未确认的C。

## 9. Verification Plan / Design Acceptance Index

- F-011：唯一已有持仓/L2假设、原模型保持、零fit及无搜索。
- F-012：真实数据/预测/原持仓身份、09:15与DST、完整131和合法NA。
- F-013：三臂金额守恒、单episode状态机、恢复现金与费用、可复算取舍和owner边界。

### Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | backend/services/hmm_risk/risk_l2_holding_vix_reference.py；scripts/hmm_risk/run_risk_l2_holding_vix_reference.py：固定参数/零计算守卫 | backend/tests/hmm_risk/test_risk_l2_holding_vix_reference.py：公式、参数、fit/inference/DB poison | VERIFIED_SOURCE_ONLY | 无 |
| F-012 | backend/services/hmm_risk/risk_l2_holding_vix_reference.py：signal_ranks/vix_context/_native_extract/ReferencePrices | backend/tests/hmm_risk/test_risk_l2_holding_vix_reference.py：完整目录/时间/原生前持仓/hash漂移/合法NA/退出PIT | VERIFIED_SOURCE_ONLY | 无 |
| F-013 | backend/services/hmm_risk/risk_l2_holding_vix_reference.py：episode_schedule/evaluate_basket/summarize | backend/tests/hmm_risk/test_risk_l2_holding_vix_reference.py：48组费用算术/现金守恒/共同episode/配对mask | VERIFIED_SOURCE_ONLY | 无 |

本矩阵验设计和源码合同，不代报真实经济结果；D1～D6已由用户单独明确批准。源码定向28项通过，覆盖无fit/predict/DB、0.10上界、原C pin、DST/重复observation/未来来源/缺pin/全目录/持仓退出/全同分/合法NA，以及金额/现金/费用/不叠加/末端未成熟。正式参考已完成，独立结果/取舍见§12；真实QE账户与生产采用未执行，28项合成与隔离测试不是这些业务状态的完成证明。

补充必须验证：打乱原文件行序不改变结果；末日仍在清单而不是缩窗；NA和unknown不混；未知行业不填安全值；reference λ的开盘信息不得进入09:15信号；新买记录不得进入已有持仓集合；三臂同分时R=X；独立实现重算差分/权重与现金守恒。测试只验证合同，不伪造真实收益、数据缺口闭合或已完成的研究结果。

## 10. Risks / Production Gates / Rollback

风险：10D C未必适合5日持仓；VIX只是条件关联；平均减仓规模可能很小；反弹和恢复摩擦可能消耗风险收益；公司行动/可成交性/历史publication不能从相关系数推导。接受负结果而非再修公式。未来QE账户会有卖出、再买、现金和原交易反馈，必须由其owner在自己的合同中验证，本设计不能提前保证增益。

production_ddl_gate=noop；production_dml_gate=noop；dependency_gates=noop；dataset_write=false；active_profile_write=false；runtime_activation=false；service_control=false。批准文档PR的runtime_impact=none；当前源码由canonical目录分类为backend、target_ids=[backend-main]、catalog_error=null，不降级分类。本CLI为独立离线入口，不因服务尚未加载新源码而阻断其clean-source参考；服务加载仍须用户重启及对应运行验证，不为记录结果申请重启。回滚只撤销未采用的新源码/结果指针，旧资产和消费者从未被本包替换。

## 11. 设计审修与交付状态

设计起草时基线`origin/main=f0a995f3c77c48c3a0de45b2627813f52c6899e6`，父蓝图v3.07；当时核实上轮verified_result与VIX manifest原SHA，未重跑原统计或读取新outcome。Codex当前profile的router/docs_handoff只读校验ready、blocking=0、warnings=0、不需重启。默认Claude全局目录router/docs_handoff过期，未修改/安装该客户端，也不冒称两个客户端均ready。

两轮作者审核（非独立第三方）：第一轮修正各seed各自触发造成episode不可比的问题，改为共同VIX/calendar时序；补齐空持仓/零减仓/合法NA和恢复不借款。第二轮核对实际C schema及`expected_member_loss`字段，明确原10日标签与本参考时段不同；X开盘金额匹配只作归因、分数股/示意费用不冒充实际交易，旧历史及合同保持。48组精确分数算术反例验证三臂、现金恢复、正负收益、费用及全同分恒等；3个DST用例和episode重复来源/恢复日/末端截尾检查通过。这些是设计算术检查，不是已实施业务测试或研究结果。F2结果及最终版本在PR记录。

批准记录：2026-11-09本窗口用户通过对应完整推荐合同问题明确答复“批准完整推荐合同”；问题同时列明VIX变化>0、共同episode、全L2平均秩、最大10%减持、五session现金恢复、同开盘金额X及10bp主展示/0/5/20bp敏感性。文档审修通过/合入不报告源码/研究/经济效果已完成。DESIGN-COMPLIANCE-001逐项：没有把封闭参考简化冒充完整账户；NA/成本/执行限制不隐藏、不造成功；原模型/旧消费及其他模块语义不改；没有全行业必过/额外审批或结果后阈值，新精确参数按本次用户批准固定。

源码两轮作者审修（非独立第三方）：第一轮核对共同episode、前收盘原生持仓、排序及三个金额路径，补齐路径形状/报价因子核验；第二轮核对冻结输入和文件输出隔离，未知参数/分类歧义fail closed，保留退出PIT但仍持有的合法成员，原生pickle必须先通过固定hash。定向28项、Ruff、compile和fresh-process CLI加载通过；ownership为hmm.risk，CI分类仅l0+hmm_risk_pr_slice，不本地盲跑完整历史矩阵。#7165最终HEAD `21ac1a6884cddbd314f1bcfa3b109d4fa6598a02` CI通过，merge=`09cd55b0d75983453283ad694d4941d13b9b6ba2`。本节前文设计/源码起草状态是当时快照；当前正式结果以下节为准。

## 12. 唯一参考结果与停止取舍（2026-11-09）

正式CLI在干净的合入HEAD `09cd55b0d75983453283ad694d4941d13b9b6ba2` 执行一次，结果未覆盖或重跑。输出：

- `F:/Dev/AIstock_runtime/hmm_l2_held_vix_reference/20261109/run_v1/result.json`；3,095,861 bytes；文件SHA `6d7c8d45ddb69e1c5d6bb559fbe27be2759d8a88b63a07d2be281c86970ba015`；canonical receipt `3ffd9467cdc8e99f1fd1f44e1a3f2951fb751cc3c7ebd814c32ff6f98e657d7d`。
- 同目录`request.json` SHA `e232fff31fc663ab70f10449d1ef6841a8e10f1becc15796ad5e6e01e0d8cdf8`；`verify_arithmetic.py` SHA `052f446586c0e70d95c03868a6760a48babf46c3badb71c304b4fcf41041a803`；取舍为同目录`review.json`，不回写不可变计算结果。
- 141个decision完整：19启动、76活跃等待、19恢复、27非正VIX不行动；19个共同episode×3seed=57个事件均可估值。142×131=18,602原C输入完整，末日10/29仍在审计清单，最后episode10/19→10/26；本次无未成熟恢复或配对估值NA，并非删去尾部后获得完整。
- 同一作者使用另一条Fraction库存＋现金算术路径完成8,468项复核（不是第三方审核）：金额匹配、直接库存价值、四档成本、现金恢复、每日敞口与共同episode均通过。后续文档HEAD不冒充上述执行HEAD。

以下均为**单次事件、原持仓预算分母的平均百分点**，不是账户累计/年化收益：

| 切片 | 事件数 | R−B（主展示10bp） | R−X（同金额归因参照） |
|---|---:|---:|---:|
| 全期三seed先配对后平均 | 19 | −0.0408657 | +0.0002019 |
| Q2 | 8 | −0.0092064 | +0.0010413 |
| Q3 | 9 | −0.1016577 | +0.0004806 |
| 10月 | 2 | +0.1060606 | −0.0044100 |
| 剔除最大绝对R−X贡献6/4，仅敏感性 | 18 | −0.0511721 | +0.0022958 |

全期0/5/10/20bp来回成本下R−B依次为−0.0351796/−0.0380227/−0.0408657/−0.0465519个百分点；**零成本时已为负**，不是交易费门禁阻挡一个有价值的公式。平均减仓仅为原持仓预算5.68614%；避免负贡献+0.1231402个百分点，小于错过正贡献0.1583198个百分点，主展示成本再减少0.0056861个百分点。19共同事件中11个R−X为正、9个R−B为正，但前者量级接近零；不是“所有月份/seed必须赢”的验收门。

seed42/2024/2026各19事件，R−X分别+0.0025709/−0.0023397/+0.0003746个百分点，R−B均为负；三个seed共享市场历史，不算57个独立市场样本。未拼接NAV、未计算账户MDD/Sharpe，不能把分散事件相加后宣称累计收益。参考路径R/X最大股票敞口差约0.002747个百分点，金额匹配不意味着每日敞口严格恒等。

完整2,866条持仓-事件记录中，`002542.SZ`在4/21的三个seed均为`classification_authority_unavailable`，按批准合同保留原持有、不应用overlay，不用未来分类或默认低风险替代。30条持仓-事件包含合法停牌事实（14条触发端、6条恢复端）；仍保留参考价格及`NOT_SIMULATED`，不声称可以卖出/恢复，不将自然约束判模型失败。限价/微观成交与实际费用未模拟，真实QE仍有额外约束；历史VIX发布时间UNKNOWN、NON_VINTAGE、回溯分类及已消费历史边界保持。

**取舍：`STOP_NO_INCREMENTAL_CASE`。** 在本次明确公式和时间窗内，行业定向相对同金额减仓没有值得继续投入的增量，错过上涨和恢复成本又抵消风险收益。停止这一精确消费组合，不追加阈值/天数/幅度/seed/fit，不将本结果交QE启动新账户；也不外推为所有VIX、全部L2风险研究或其他冻结模型均无效。其他窗口无需为本研究补数据或修改业务。若以后有独立的新信息或更贴持仓损失的目标，应重新提出单一可检验问题，而不是复活本次公式。

结果同步两轮作者审核：第一轮逐字段核对原回执、单位、分段/费用和三seed配对；第二轮核对停牌/分类NA、开盘匹配可执行性、没有NAV/晋升/新实验及原历史/数值合同不变。source/CI已完成；本CLI参考已完成；backend运行加载未验证且不是本研究前置。fit=0、模型推断=0、QE任务=0、数据库访问/写入=0、数据集/profile修改=0、服务操作=0。
