# Advisory 固定5TD价格条件化继续价值 F2详细设计

2026-10-08。假设 EXIT5-PRICE-CONDITIONED-CONTINUATION-1。设计PR #5705已合入fa5a6a686、自身官方清理完成；独立七文件源码77824c1经多轮复审/13直接合同/Ruff/L0/F2通过，一次新研究及四fit已完成。源码尚未公开合入，没有可激活或已确认盈利的raw卖价功能。

源码范围登记：feature/advisory-exit5-price-conditioned-value-source-20261008，精确下述七文件；显式原audit helper依赖#5697，不导入失败P21源码、不修改旧study或其他模块。

## Background / Goal

原九字段 Exit 审计 advexit5_6134993163be362f7e0bebc7 相对原固定终点 baseline −43.7236bps；持仓路径研究 advexitpath_16544dfaa2289406e21f539a 相对 baseline −47.4315bps、相对九字段 Exit −3.7078bps。两者均只结束自身 candidate，不重拟合、补证或救参数。clairvoyant +235.9270bps 是原标签动作空间的理论参照，不证明存在可学习模型。

原 audit 预测 E[V_continue|X_S]，将实际 U 出售净值作为外部比较。新的 U 价格可能包含后续继续价值的信息，S-only mean 不随价格场景更新，可能错卖后续上涨；这是待检验的条件化结构假设，不是已证明的程序 Bug、市场反转或根因。新假设只加入一个显式 hypothetical 净价格 query，估计 E[V_continue|X_S,query]，不继续追加持仓技术指标。

已核对现有 GP5 买价模型：它已经以 hypothetical gap 参与 mean/path 两头预测。因此本问题只针对有限 S-only Exit 审计，不宣称全部荐股模型均缺少价格条件化。用户目标仍为日频盈利买卖价格建议，而非预测开盘价、实现分钟线执行或保证收益。

本研究为 RISK_MANAGED_ADVISORY / LEARNABILITY_AUDIT / EXPLORATORY / NAVIGATION_ONLY。首版只测试继续价值的 mean 曲线，不能单凭正 mean 输出校准盈利概率、止盈止损或可信尾风险价带。

## Scope / Non-goals

文档范围仅本文和主蓝图。后续独立源码范围为七文件：backend/services/advisory_model_first/generic_exit_price_conditioned_5td_contracts_v1.py、generic_exit_price_conditioned_5td_models_v1.py、generic_exit_price_conditioned_5td_pipeline_v1.py；backend/tests/advisory_model_first/test_generic_exit_price_conditioned_5td_models_v1.py、test_generic_exit_price_conditioned_5td_pipeline_v1.py；本文和主蓝图。原 audit helper 直接复用 #5697 精确已审核版本，不修改原 study 或 P21 三字段源码；源码 PR 显式声明该依赖，不合成大 PR。

不修改 QE/Selection/HMM/StrategyPackage/公共 workflow、profile 或数据集、Paper/Execution/其他模块。不写数据库，不读新 test/sealed，不提交上游 QE 实验，不重选股/重新排序/删除日期，不重建数据、安装依赖或控制任何服务。直接消费既有策略包，不新增 Alpha、PBO、native receipt、coverage 资格门。临时 X 盘，正式新 study F 盘。后端重启和运行激活继续由用户决定。

## Architecture / Contracts

### 1. 共同价值单位、唯一 query 与标签不变

原 Top5 shadow episode 在 T 开盘入场、E=T+4 收盘结束；S 为原持仓 session 收盘，下一原 session U，T+1<=U<=E。原候选、episode/KEY、policy hash、费用、数量等价、标签、四个过去训练折和 E 成熟/一完整 session embargo 都不改变。原1525 episode/6100决策全保留；240评价cohort/238完整/2 UNKNOWN为实际原源状态，不硬编码。

令 R_S 为原固定数量在 S 价格上的假设净清算价值，C_E 为同数量在原 E 的净清算价值。原标签 y_hold=10000*(C_E/R_S−1) 不改变。新的 query z 是**假设的 U 净清算价值相对 R_S 的 bps**：V_sell_query=R_S*(1+z/10000)，z>-10000。不是“预测的开盘涨幅”、S 已观察的 U 价格，也不是新的实际交易。

S 输入 X_S 仍为原九字段和 remaining；query z 是调用者提供的情景变量，单独字段/身份/时钟，不混入九字段的 observed-through 声明。历史成熟训练样本可使用原 sell_scenario_bps 作为 z；它包含 U 当时观察的出售净值，明确标为 MATURE_TRAIN_QUERY_U，不冒称在 S 已知。训练监督的 C_E 和该 z 都只来自已在训练截止前成熟的原 episode。

模型 h(X_S,z) 预测原 y_hold；相对继续持有的预测出售 advantage 为 z−h(X_S,z)，保持共同 R_S 单位。原买入是沉没成本，不再收一次买入费；S reference、U sale 和 E continuation 各使用原一次卖出费用，不对 z 再扣费用。genuine missing reference、sale、执行状态或标签保持 UNKNOWN/NOT_HELD/UNSETTLED，不用0、当前配置或猜测数量填补。

### 2. S 发表与未来 U 查询的物理隔离

fold 模型只使用过去已成熟训练数据。先仅接收 episode_id、s_date、九字段、remaining 的严格白名单形成 S 曲线；该入口不得接收 U/E 价格、factor、sale/continue value、label 或目标结果，也不能通过路径回读这些值。输出冻结的 a(S)、b、训练支持域与 S 输入/model/policy identity，使 h(X_S,z)=a(S)+b*z 在不读取实际 U 的情况下成立。

S 曲线 immutable artifact 完成后，未来评价入口才读取原 label-only sell_scenario_bps，作为 U_OBSERVED_QUERY 查询此前冻结函数；它不能重新拟合、重算 S 状态、修改支持域或改变已发表曲线。测试改变未来 U/E 全部 label 字段不得改变曲线 bytes/hash；同一 S 仅改变 hypothetical query 必须允许相应预测变化。此顺序证明历史计算的 PIT 功能，不升级为原生捕获或自然前向证据。

z 的单位是 S 等价净价值，不是 raw U 元。真实 U quote 和已在 U 知晓的 quantity/factor 可在 **U 评价** 将原卖出净值转换为 z；它们不得参与 **S 发表**。S 若要给出未来 raw U 元区间，需要另行 S 可见的除权/拆股/费用/法律价格坐标映射。本切片不得以未来 f_U/f_S 映射 S raw 价带；未知映射显式 UNKNOWN。训练/评价允许未来结果的 label-only 数量等价，不意味着 S 产品可读取未来 factor。

### 3. 唯一候选、冻结模型与训练支持域

恢复原九 S 字段+已知 remaining，增加一个 query z/10000，共11连续输入，加原九字段 UNKNOWN flags，总20维；不引入已失败 P21 三路径字段、父包rank/score/id、seed ensemble或模型搜索。past train median/standardization 及缺失flags原协议；remaining和query必须已知，不产生虚假缺失标记。全列 UNKNOWN median=0/flag=1仅模型编码，不当观测事实。

模型仍 Ridge(alpha=1)，原5时间块/首warmup/后4 past-only expanding fold，每 episode 的四 S 全在同一评价块。监督训练仅 held=true、原 y_hold 与成熟 query 已知且符合原成熟训练折；UNKNOWN训练原因计数，但原全 roster 和评价人口不删。只有一 candidate、最多4新 PHYSICAL_FIT，无阈值/参数/seed/窗口选择。旧九字段模型和 P21、fixed baseline、oracle全部直接引用，不重拟合或重跑。

query 支持域从每fold成熟训练的已知 z 生成，按 remaining=1..4 分层：先取该层固定2.5%～97.5%分位边界，再使用100bps桶，每桶至少30原决策且5不同原 entry cohort，取桶与边界交集；上端排他，保留空洞，不将最小到最大包成连续支持域。不跨层、validation 或未来数据校准。support 是该价格模型对情景的证据描述，不是父包/股票池准入门；支持外标记 UNKNOWN_QUERY_SUPPORT，不删除股票、日期、原决策或默认认为不盈利。

Ridge 拟合采用全部原成熟已知监督样本，support 限制的是输出解释与动作使用，不为了改善结果筛选训练价格或评价人口。支持规则事先冻结，失败后不放宽桶/分位数。拟合样本为空时记录 UNKNOWN_NO_MATURE_QUERY_TRAIN、0 fit，不从旧模型或测试窗口补样本。

### 4. 公共研究流程与 fit 边界

输入 mini-plan 精确绑定原 base plan、prepared rows/folds/calendar、旧 predictions/cohort ledger、原准备/训练/评价 manifest hash以及当前代码/feature/query/label policy identity。金融键只来自已消费原开发窗口；不读取旧test/newsealed，不重新跑原准备入口。原 hash 或 KEY/折/标签矛盾是正常计算错误，不能通过回填今天配置/重新训练旧control绕过。

使用既有只追加 typed registry，以 LEARNABILITY_AUDIT、当前 hypothesis、唯一变量 HYPOTHETICAL_NET_PRICE_CONDITIONING、objective_contract 与 decision_use 登记。曲线/模型/评价分别原子发表、按 manifest 与 parent hash 读取。不得把一次预登记算完成 fit，oracle只引用原结果，不登记新的 ORACLE 研究。

每次实际 fit 前公开 QE single/custom_evo/multi-alpha 三路径 fresh 60秒内为0，之后读回，忙时只等待自己的下一 fit并每30分钟检查，继续不依赖 fit 的源代码工作，不控制QE或重复其 Alpha 实验。完成fold原样恢复，STARTED但未提交的fit不自动重试；新增次数以实际四fit为准，当前125研究fit+1旧INDEX_BUILD不提前变129。

### 5. 原动作链、双对照、业务判定

对同一原 episode，依原 S 顺序，实际 U 可执行、原出售值已知、查询支持内且 z−h>0 时在 U 出售，否则继续至原 E。模型查询未知时执行原“继续”默认动作，并报告 UNKNOWN_MODEL_QUERY_BASELINE_ACTION；这不是模型成功预测、模型拒买、空槽或0收益。若原 endpoint/必要财务值未知，cohort仍UNKNOWN，不能因默认动作标为complete。

只计算每episode一条动作链，不将四 S 当四笔交易或在事后选择最佳 S；T+1、固定 E、原股票、五槽分母和费用完全不变。同240原评价cohort报告candidate−原fixed baseline、candidate−已生成九字段 Exit；P21负向结果仅历史参考，不据它另选更差control以美化结果。必须核对同KEY原baseline与完整/UNKNOWN标志，默认动作和unknown支持日单列。

完整配对归因固定：避免亏损、额外亏损、增加盈利、错失盈利；归因净和除以原五槽及完整cohort数必须还原净均值。20干预episode/20完整cohort/10%完整可评价entry日为事先支持描述；若欠功效、regime UNKNOWN或区间跨0，结果仍探索性，不支持激活或关闭全方向。沿用原时间轴5cohort block/bootstrap2000次/seed20261008及MDE80，不能压缩UNKNOWN日期或当每日独立。

负/零增量结束本candidate，不扩大模型族、回选frontier、救阈值、重跑或补历史证据。正结果也仅为后继风险/校准/独立确认的候选，不直接变成raw卖价产品或绑定生产。mean条件化、盈利概率、尾风险和未来自然前向能力分别报告。观察性市场情景条件预测不是因果价格干预或真实成交OPE/DR证据；五槽重叠cohort不是资金NAV，不报年化Sharpe。

## Implementation Plan

独立设计三视角复审/F2 → 文档PR/合入 → 登记七文件源码 → 严格S-only曲线模型/序列化与 query evaluator、原helper同核复用 → 多轮审核修复/最小合同/L0/F2 → 新身份 prepare/最多4fold/评价 → 停本候选或设计风险/校准后继。不为公共BUG分类阻断停自身研发，不越界修公共合同；公开源码、实际模型、经济确认与生产激活分别交付。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-821 | 原episode/标签/policy/E/费用/折/股票池和五槽保持，不重选或重训旧control |
| F-822 | S观测和hypothetical净query分离；S曲线白名单、future U/E poison hash不变 |
| F-823 | R_S共同净价值单位/零重复费用；raw U映射未知不得未来factor回填 |
| F-824 | 固定20输入/原Ridge/四fit，median与remaining分层support只取past train/空洞保持 |
| F-825 | 曲线先发表后U查询；原T+1单动作链/默认继续/未知原因/两对照严格同人口 |
| F-826 | 干预/MDE/block/四归因和UNKNOWN时间轴，探索性不能激活或关闭全方向 |
| F-827 | 七文件Advisory、X临时、typed registry/fresh QE/恢复/0旧refit、无test/DB/QE改动 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-821 | pipeline_v1.py _load/prepare_exit_conditioned_v1 | artifact: docs/architecture/advisory_generic_exit_price_conditioned_5td_v1_f2_design_20261008.md §1/4/Review | SOURCE_VERIFIED | none |
| F-822 | models_v1.py _s_frame/seal_s_curves_v1/query_sealed_curves_v1 | test: backend/tests/advisory_model_first/test_generic_exit_price_conditioned_5td_models_v1.py::test_future_label_poison_cannot_change_sealed_curve_and_is_rejected_as_s_input | SOURCE_VERIFIED | none |
| F-823 | models_v1.py fixed net-query/unit advantage | test: backend/tests/advisory_model_first/test_generic_exit_price_conditioned_5td_models_v1.py::test_s_curve_query_changes_value_but_never_double_charges_fees | SOURCE_VERIFIED | none |
| F-824 | models_v1.py _matrix/_support/fit_price_conditioned_fold_v1 | test: backend/tests/advisory_model_first/test_generic_exit_price_conditioned_5td_models_v1.py::test_support_preserves_holes_and_remainings_are_not_pooled | SOURCE_VERIFIED | none |
| F-825 | pipeline_v1.py trained/evaluate stage; original action/cohort helper reuse | artifact: docs/architecture/advisory_generic_exit_price_conditioned_5td_v1_f2_design_20261008.md §2/5/Review | SOURCE_VERIFIED | none |
| F-826 | original summarize_exit_v1; pipeline_v1.py _paired_stats/evaluate | test: backend/tests/advisory_model_first/test_generic_exit_price_conditioned_5td_pipeline_v1.py::test_unknown_timeline_not_zero_and_constant_paired_block_statistics | SOURCE_VERIFIED | none |
| F-827 | pipeline_v1.py _record/_train_fold/exact scope | test: backend/tests/advisory_model_first/test_generic_exit_price_conditioned_5td_pipeline_v1.py::test_busy_zero_fit_and_completed_fold_resume_never_queries_or_refits | SOURCE_VERIFIED | none |

当前矩阵验收独立源码合同，不等于新模型已拟合、经济确认或生产产品。设计阶段DESIGN_REVIEW_VERIFIED和源码阶段SOURCE_VERIFIED分别记录，不能以设计合入冒称实现交付。最小直接测试为：同一S不同query预测/净单位手算、future labels poison与白名单拒收、过去训练标准化/support漏洞/remaining分层、support外UNKNOWN默认原动作、已卖不再出售/原五槽与双对照、源码身份/未提交fit恢复/fresh QE busy零fit。复用原22直接合同不堆重复fixture/快照；真实运行前后分别报告。

## Risks / Rollout / Rollback / Production Gates

实际run advexitquery_fed4d4edcba7bcb89ddaff94：prepare0.158秒，6100原决策/1525episode全保留；原监督标签和四折直接复用、S-only表不含query/label。四fit与curve发表2.311秒，evaluate0.894秒，逐fold前后公开QE三入口全0；旧control refit/新oracle study均0。累计125→129真实研究fit、旧INDEX_BUILD1另列，无正在运行的本窗口实验。

同240原评价cohort/238完整/2 UNKNOWN，候选相对原baseline −35.3964bps、CI95 [−72.7043,1.5447]、MDE80 53.4254；相对九字段 Exit +8.3272bps、CI95 [−11.4530,27.2837]、MDE80 28.0449。594完整干预episode/211cohort/88.6555%，4368支持内query、404支持外UNKNOWN、28原query值UNKNOWN，全部4800原评价决策保留；未知只默认原继续动作，不当模型预测成功/现金收益。四项归因净和−42121.7068除以五及238，精确还原−35.3964。

相对旧负向overlay的小改善不能掩盖仍劣于原baseline。结果EXPLORATORY/NAVIGATION_ONLY、regime UNKNOWN，未确认/未激活；停止本candidate，不扩模型族/救阈值、重复fit或为它补证。原oracle+235.9270只作动作空间参照，不是新模型效果。当前原baseline正均值与candidate正均值只是重叠shadow cohort，不是资金NAV或真实盈利承诺。

风险1：把后见U query误写为S观测；以两个物理入口、S曲线先发表及strict白名单阻止。风险2：把S等价净价格误称raw未来价格；现阶段不做raw报价产品，不读取未来factor实现S映射。风险3：价格conditioned拟合可能仅复刻已知价格持续/反转，支持外不推断；须看原baseline与绝对cohort收益，不能因相对差overlay改善就称Alpha。

风险4：重复消费开发窗带来的研究者偏差。新 hypothesis 与新增4fit计数明确、旧结果不改判，探索性只有导航价值，不使用新的 sealed；成功后也需另行独立确认，不能对当前窗口再选参数。风险5：mean头本身不识别风险，不能据正均值自动上线或承诺最佳卖点。

没有DB/服务/依赖变更，未指定生产bundle或config。回滚仅停止本候选、不改原artifact；公开依赖未合入不能宣称main业务链可用，后端重启仍由用户执行。未来raw卖价API/风险头/校准与消费者集成另立业务设计，不因当前F2通过而缩减交付。

## Review / 多轮自审

第一轮业务角色复核：核对原两轮负向事实和 GP5 买价源码，修正“全部价格模型都未条件化”的潜在误判；本假设只针对 Exit 继续价值，不加入已失败三技术字段，不把 mean 当风险/盈利概率或 raw 卖价产品。F-821/823 的目标和原政策范围完整，无简化为开盘分布。

第二轮时钟/单位复核：以 S 等价净清算值而非 raw U gap 定义 query，保留同 reference/一次费用；S 发表入口 strict 白名单/immutable 曲线先于 U 查询，未来 factor 只属 U 评价的数量等价，不参与 S raw 映射。成熟训练含历史 U 情景，但全部 E 成熟并满足原 embargo；F-822/823 没有把未来值伪装为 S 观测。

第三轮方法/边界复核：分位/100bps桶明确逐 remaining 仅 past train，hole/UNKNOWN/default 原动作和固定评价人口不变；冻结20输入/四fit/原模型/两对照，不重新跑旧control/oracle；原时间轴block/MDE/干预/四归因不改。F-824～827 完整，无跨模块、DB或服务操作。初次 F2 校验指出矩阵 DESIGNED 不是公共枚举；修订为 DESIGN_REVIEW_VERIFIED，明确验收的只是设计，不伪造未完成源码。

设计阶段最终复核：本文、蓝图当前队列与历史事实分离；设计合入时新源码/预登记/fit均0，累计125+1未预增。没有未解决设计阻断，实施逐项另行源码验收，不把文档审核计作经济确认或批准激活。

源码三轮复审：原label/policy/KEY/四折复用、S净单位与既有GP5角色、严格S白名单/先曲线后U查询、训练金融列按past episode过滤才decode、未知default行为与同cohort配对完整；修复日期表示在原helper合并前的正规化、None query的正常UNKNOWN、全空数值列的pandas兼容性、支持桶以groupby避免平方循环及非finite编码的显式错误。首轮12 PASS/1异常类型fixture修复；失败nodeid及全空警告nodeid先复验PASS，随后13直接合同/Ruff clean。fit单元测试使用微型FakeRidge，不计真实研究fit；源码验收当时新研究为0，之后实际四fit完成，当前结果见§Risks。

L0与F2 7/7通过、0blocking；两项P2复杂度提示已逐项审核：curve/query outer merge以原episode/S one-to-one、最多15000行，不产生row explosion；原cohort双对照约240行、20输入、四fold、bootstrap2000×原cohort有界；support按groupby分桶，不进行每桶扫全表的平方循环。未修改公共scanner/ownership，未添加平台或重复宽回归。当前代码已满足本地源切片合约，实际四fit完成、效果负向；公开交付仍要区分依赖/CI/merge，不把研究完成当经济确认。
