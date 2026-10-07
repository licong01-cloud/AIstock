# Advisory 固定5交易日剩余净价值卖价建议 F2详细设计

2026-10-08，独立源码范围登记：feature/advisory-exit5-remaining-value-audit-source-20261008；仅下述三个服务、两个直接测试、本文和蓝图。不修改其它模块，当前仍0新Exit fit/模型/经济确认。

## Background / Goal

用户目标是建议可能获利/规避后续下跌的买入卖出价格，不是预测实际开盘或最佳分钟成交。买价固定5TD已形成独立模型体系，但截至本切片执行前117研究fit＋1旧index均未证明通用5TD overlay成本后增量。selection-context两个fit已完成负向，不再重复。旧N2 Exit oracle有上限而固定learnability负/欠功效，liability/holding可预测不能代表退出价值可预测。本设计新增固定原持有终点的卖价目标，不重跑旧oracle、不为旧负模型补证。

## Scope / Non-goals

源码精确允许范围：backend/services/advisory_model_first/generic_remaining_value_exit_5td_contracts_v1.py、generic_remaining_value_exit_5td_labels_v1.py、generic_remaining_value_exit_5td_audit_v1.py；backend/tests/advisory_model_first/test_generic_remaining_value_exit_5td_labels_v1.py、test_generic_remaining_value_exit_5td_audit_v1.py；本文与蓝图，共七文件。标签/审计是本切片全部范围；未来预测模型/价格集合/API为独立角色任务，当前状态分别为尚未设计实现，不作为本审计的已有交付。

不修改QE、Selection、HMM、公共行情/股票池、Paper/Execution、数据库和资金仓位；不研发分钟执行、不下单、不自动止损/重选股，不读sealed holdout，不重复旧价格实验。无需运行QT/QE上游Alpha；实际拟合仍fresh QE三0、忙时半小时检查。X临时、F新独立artifact，0生产配置或模型激活/进程控制。

## Architecture / Contracts

### 1. 时钟、原position对象和固定终点

输入是原候选独立价格研究所模拟的持有episode，不读取/修改真实账户仓位。entry为T开盘，原终点E=T+4交易日收盘，首次持有5个交易日不因停牌延长。每个卖价决策在S日收盘后，只用<=S信息，建议下一session U的合法假设出售价p；U是S后立即下一交易日，T+1<=U<=E，以守A股T+1。不做“看完S收盘又在S收盘卖”的时钟作弊；entry日T不能卖。同一episode有1～4个合法sell decision，剩余sessions从4到1，不重置为另5日。

原entry KEY、完整calendar、T/E、S/U、原固定policy hash、候选rank及源包/池身份须一致。没有完整episode、卖出合法日历或价格坐标→UNKNOWN，保留episode；正常停牌/涨跌停不能合成可执行卖出。未建仓的原候选不伪装持有，状态NOT_HELD不进入卖价评价。是否持有是外部冻结shadow policy动作，不按未来盈利筛建仓人口。

本次shadow持有按原Top5与T已知可买入状态确定；E/未来价格缺失不反向删除已入场episode。既存geometry的300日期成熟cohort均分五个60日块，另5边界未结算cohort/25episode完整保留，不把其未来S行情解码为开发数据。必要的新标签转换复用原KEY/calendar，不重做历史geometry覆盖审计。数量采用原冻结provider policy的quantity-equivalent单位stake账本（entry一次buy成本），S/U/E同映射；这是同policy的影子计价，不是实际股数/现金分红成交账本或NAV。

### 2. 两臂增量价值与标签

两臂共享同一冻结shadow policy、原shares、S可见price basis、费用和corporate action处理：SELL_NEXT_U_AT_P取得下一合法出售假设价p的净清算价值；CONTINUE_TO_ORIGINAL_E取得原E到期相同净价值。标签advantage_sell_cny=V_sell−V_continue，bps以S原股数的明确净清算参考作为共同分母，不能各自换分母或把累计入场盈利当剩余预测。沉没buy佣金不再扣；每臂自己的未来sell fee/tax分别一次，既不双扣也不忽略E出口费用。若费率不同按同日期合同分别计算，不写死“必抵消”。split/dividend变化按同quantity-equivalent账本，不用未来factor进入S特征。

共同reference固定为原S raw close的quantity-equivalent净清算价值，采用S合同sell费；reference未知/非正为UNKNOWN，不用p或E变化分母。均值头预测CONTINUE剩余净终值/reference，风险头预测U..E的未来路径下行尾部，只有成熟后作为标签。假设卖价p并非实际已知未来open，使用S合法价域、训练观察性支持区间；标签另存真实U open/可执行状态用于一次导航。所有数值绑定label schema/policy hash/费用/终点与valuation denominator；基线policy变化使旧labels不可复用，必须新identity，不覆盖旧模型或标成exact retry。

这在形式上是advantage/uplift型相对动作目标，不等同已识别的因果uplift。当前只有原固定政策及观察价格，未记录随机动作概率/充分重叠，不承诺doubly robust/OPE可无偏；不能把shadow p成交当真实limit fill。双方观点中的OPE是未来有支持数据时的估计框架，不是本设计已满足其识别假设。

### 3. 首先做新标签审计，而不是直接训练

唯一先行假设EXIT5-REMAINING-VALUE-AUDIT-1：原固定5TD持有期内，下一合法SELL vs固定终点CONTINUE是否有可学习的剩余净价值。先target-free KEY/calendar geometry；只有geometry与T+1/期末成熟规则通过，才读取开发窗口成熟标签。第一次只做该新目标的oracle diagnostic与一个冻结learnability模型，不重算旧N2 oracle、不阅读新sealed窗口。

oracle可用真实未来决定最有利合法sell日，必须标CLAIRVOYANT/ORACLE_DIAGNOSTIC/NAVIGATION_ONLY；同时报告原真实open/停牌/涨跌停可执行约束版，不用每episode完美high或分钟极值。比较SELL_U与CONTINUE_E，所有episode原日期保留；缺未来出口不可当0收益/延长终点。完整/partial/unknown/unsettled与原槽现金分别报告。

固定learnability baseline：单一Ridge(alpha=1)一次预登记，features仅既存九日频字段+remaining_session_fraction=(E位置−U位置+1)/4；TRAIN_MEDIAN_PLUS_FLAGS在每fold past train fit，剩余时间字段已知，不偷读realized remaining holding。开发窗口按原entry cohort连续等分为五块，首块仅warmup，后四块各一次past-only expanding fit/evaluation；禁止把新的test/holdout纳入。每个fold训练成熟E至下一评价块最早S之间必须至少有一个完整交易日embargo；同episode全部S决策归同一块，不按行随机CV。warmup/缺成熟样本原行保留UNKNOWN，实际模型fit数与oracle0-fit分开登记，不将未执行fold计入fit。

Ridge目标为10000*(V_continue/reference−1)，动作增量预测为10000*(V_sell/reference−1)减该目标预测；实际U open只作为U时刻已观察的价格场景用于既有S预测查询，绝不进入S features/fit。每fold train median后连续特征按train mean/std标准化（零std取1），missing flags独立保留，remaining已知；test不校准或选点。这里审计只有一个均值head，尾风险head属于后续模型详细设计，不把未拟合风险写成已有输出。

准备器显式绑定既存原GP5 preregistered plan，开发范围不得超过其train+validation或触及其test_start；金融parquet先按日期与允许列进行projection/filter，再解码开发值，不能先读取test再过滤。原20候选仅投影原Top5 shadow episode；S特征计算的临时查询排名只用于复用九字段纯函数，声明HELD_STOCK_S_QUERY_NOT_SELECTION，不改变原episode/rank。S复权只以S及之前因子形成；U/E价格、policy数量等价坐标与可执行性只用于标签/评价。正常S行情缺失不掩盖已经可知的原E基线。所有原episode和边界行保留。

每fold fit前实时读取公开QE三路径并核查一分钟内三0，fit完成后再次读取；已完成fold以本study身份和artifact hash原样复用，无新的physical fit。出现尚未完成的STARTED标记须明确恢复，不自动重复拟合；这限制本study的重复训练，不新增父策略包门禁。仅已有四个固定fold，没有超参搜索或自动新假设。

不搜索alpha/窗口/label/seed。learnability缺正增量只说明“当前九信息+简单模型”的该目标不证实；不能证明Exit全局不可学。oracle高/learnability低先扩真正S信息再换模型族，oracle低仅降低本次合法动作空间优先级，NAVIGATION_ONLY不能支持关闭整体方向或激活。审计之后决定是否详细实现卖价模型，不让六层栈变六个并行项目。

### 4. 原槽经济评价与推断

主结果按原entry cohort五槽等权、完整E、现金和UNKNOWN分报，使用第一次合法原U真实open的sell判断，后续政策由冻结多决策模拟器统一，不能将1～4行当独立交易复利。learnability每个episode用一条冻结“首个正advantage且可执行则卖，否则原E继续”的动作链；对每个entry cohort与无新Exit的同输入baseline配对报告成本后lift、避免亏损、错卖后续上涨、实际干预天/次数、剩余期限分布与原策略回撤（只有有效NAV账本才报告NAV，不把cohort均值标年化）。

事前minimum intervention记录为20个不同episode且至少20个entry cohort、干预覆盖>=10%完整可评价entry日；regime若开发源已有固定分区则至少两个分区各>=5个干预episode，若没有则UNKNOWN_REGIME_SUPPORT不得支持方向关闭/激活。该数值是本次诊断证据说明而非QE或研发准入。完整配对cohort归因分开避免亏损、额外亏损、增加盈利与错失盈利，四项净和与原五槽增量一致，不能把所有正增量都叫“避免亏损”；partial/UNKNOWN另报。以entry cohort block bootstrap（固定seed、5 cohort连续block、2000rep）给区间，保留原时间轴上的UNKNOWN天不压缩重连，最小样本/时序不满足则仅exploratory，不挑显著样本。MDE与intervention数量前置报告；INCONCLUSIVE不改门槛/选点挽救。

### 5. 未来模型与价集，不混入买价模块

只有新标签/信息可用性审计完成且选择新明确假设后，独立设计EXIT_REMAINING_VALUE_5TD family；依S九字段、剩余期限与合法假设价生成SELL_CANDIDATE / CONTINUE / UNKNOWN多段价格集，mean adv>0与冻结尾风险约束按新研究policy定义，不机械把买价ACCEPTABLE取反。输出独立label/model/policy、original E/S/U、利润已实现与未来剩余价值分开，价格是建议不是资金动作或订单；如果预测函数只形成单阈值，诚实报告，不装饰成日内最优点。

已有买价模型可以作为明确冻结比较臂，不作为Exit训练成果或未经新期限适配的当前卖价模型。卖价API/UI和prospective activation在该模型功能交付之后单独计划；自然前向与历史导航不混等级，不需要等实时20日才能验证代码。

## Implementation Plan

三视角设计/F2通过及当前CI合入→独立新标签/审计实现范围、PIT/T+1/费用/原population精准测试→从既存F source做target-free geometry，不新选股/读取sealed→新开发期registry/一次oracle和固定past-only learnability（只实际fit时QE三0）→真实四象限分流。新study_type分别ORACLE_DIAGNOSTIC和LEARNABILITY_AUDIT，objective_contract=RISK_MANAGED_ADVISORY，decision_use=NAVIGATION_ONLY；oracle不计PHYSICAL_FIT，四fold物理fit按实际逐条计数。无增量不重训或为历史失败补证，不自动启动模型搜索；能形成真实新信息假设才后续模型设计。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-801 | E固定T+4、S后U合法T+1、remaining1..4、不用已见close当未见成交 |
| F-802 | 同shadow两臂/分母/quantity、沉没成本与各臂出口一次、policy身份 |
| F-803 | 未来只标签/成熟、无sealed、原episode正常缺失不删/延长 |
| F-804 | 新clairvoyant与past-only冻结learnability分离、episode分块与purge |
| F-805 | 原entry五槽/合法动作链、20episode/20cohort/10%与regime支持预登记 |
| F-806 | cohort块区间/MDE、导航不激活，不自称因果/OPE识别或最佳分钟点 |
| F-807 | 分层实施及新独立卖价family、0外模块/资金/DB写/生产操作 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-801 | generic_remaining_value_exit_5td_labels_v1.py exit_geometry_v1 | test: backend/tests/advisory_model_first/test_generic_remaining_value_exit_5td_labels_v1.py::test_fixed_endpoint_t_plus_one_and_remaining_four_to_one | SOURCE_VERIFIED | none |
| F-802 | generic_remaining_value_exit_5td_labels_v1.py build_exit_remaining_value_labels_v1 | test: backend/tests/advisory_model_first/test_generic_remaining_value_exit_5td_labels_v1.py::test_common_reference_sunk_buy_once_and_date_specific_sell_fees | SOURCE_VERIFIED | none |
| F-803 | generic_remaining_value_exit_5td_labels_v1.py; audit_v1.py _read_development/_feature_rows/_load | test: backend/tests/advisory_model_first/test_generic_remaining_value_exit_5td_audit_v1.py::test_s_features_do_not_change_when_later_price_and_factor_are_poisoned | SOURCE_VERIFIED | none |
| F-804 | generic_remaining_value_exit_5td_audit_v1.py exit_fold_schedule_v1/fit_exit_fold_v1 | test: backend/tests/advisory_model_first/test_generic_remaining_value_exit_5td_audit_v1.py::test_date_maturity_embargo_and_whole_episode_cohort_blocks | SOURCE_VERIFIED | none |
| F-805 | generic_remaining_value_exit_5td_audit_v1.py evaluate_exit_chains_v1/summarize_exit_v1 | test: backend/tests/advisory_model_first/test_generic_remaining_value_exit_5td_audit_v1.py::test_first_executable_intervention_one_chain_and_fixed_five_slots | SOURCE_VERIFIED | none |
| F-806 | generic_remaining_value_exit_5td_audit_v1.py summarize_exit_v1/preregister_exit5_v1/train_exit5_v1 | test: backend/tests/advisory_model_first/test_generic_remaining_value_exit_5td_audit_v1.py::test_unknown_cash_not_zero_and_sparse_interventions_not_activation | SOURCE_VERIFIED | none |
| F-807 | registered seven-file labels/audit scope only; later model design | artifact: docs/architecture/advisory_generic_remaining_value_exit_5td_v1_f2_design_20261007.md Scope/Implementation/Review | SOURCE_VERIFIED | none |

矩阵只证明本切片源码合同实现，不冒称盈利卖价模型、实际研究或自然运行验收。最小测试手算同股一晚/四晚、T日禁止卖/S后U、quantity等价拆股/费率异同/沉没入场、终点停牌UNKNOWN、future字段毒值/test projection sentinel、episode块不串/purge、同股多日不独立交易、干预支持不足时只是exploratory；四fit预算、QE busy零启动、完成fold复用和崩溃不得自动重拟合采用微型模拟，不计研究fit。既存旧Exit模型/实验不重跑。

## Risks / Rollout / Rollback / Production Gates

价格支持域、真实可执行性、当前DB非vintage与持有episode相关性都可能限制可学空间；标签正确本身不是经济信号。动态资金仓位没有授权，本设计只是shadow episode价值与独立建议，无账户写入。回滚停止新研究/consumer调用，正式artifact保持只读；无依赖安装、数据库迁移或生产激活。source/标签/研究/未来模型/API及用户重启独立报告，负结果不让旧门槛成为项目停工条件。

## Review / 多轮自审

目标轮：剩余净价值不是holding时长，也不是entry累计盈利；先新标签与固定learnability，非六线工程。时钟轮：S收盘后的U、T+1、固定E及未来label净价/qty同核，期末缺失不延长。推断轮：一个episode动作链/entry cohort聚类，20/20/10%支持事前固定；观察支持不足不能声称因果uplift或DR，期望值正不等于已校准胜率。追加资源/统计轮修订：明确五个entry块中的一warmup四fit，embargo为完整session而非自然日，实际fit计数。源码多轮复审修复None可执行判断、S缺价不得改变E基线、完整配对归因四项与不压缩UNKNOWN时间轴；22直接合同通过，Ruff clean。实际研究尚未执行时0Exit研究fit，微型测试并非研究证据；策略激活仍0。离线九字段计算依赖BUG-1778公共交付待修复，短进程显式固定的本地已审核依赖必须写入实际代码身份；不覆盖后端源码，不将本地研究读回当公开源码合入。
