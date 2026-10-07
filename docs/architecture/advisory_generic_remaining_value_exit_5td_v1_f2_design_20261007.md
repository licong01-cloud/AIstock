# Advisory 固定5交易日剩余净价值卖价建议 F2详细设计

2026-10-07，DESIGN_VERIFIED，仅设计；无新Exit fit、模型、策略或经济正结果。

## Background / Goal

用户目标是建议可能获利/规避后续下跌的买入卖出价格，不是预测实际开盘或最佳分钟成交。买价固定5TD已形成独立模型体系，但当前115研究fit＋1旧index均未证明通用5TD overlay成本后增量。旧N2 Exit oracle有上限而固定learnability负/欠功效，liability/holding可预测不能代表退出价值可预测。本设计新增固定原持有终点的卖价目标，不重跑旧oracle、不为旧负模型补证。

## Scope / Non-goals

本轮精确设计本文、通用5TD日频API设计与蓝图；下一实现必须独立register新假设/允许范围。预期源码：generic_remaining_value_exit_5td_contracts_v1.py、generic_remaining_value_exit_5td_labels_v1.py、generic_remaining_value_exit_5td_audit_v1.py及两个直接测试，均位于backend/services/advisory_model_first和backend/tests/advisory_model_first。标签/审计是本切片全部范围；未来预测模型/价格集合/API为独立角色任务，当前状态分别为尚未设计实现，不作为本审计的已有交付。

不修改QE、Selection、HMM、公共行情/股票池、Paper/Execution、数据库和资金仓位；不研发分钟执行、不下单、不自动止损/重选股，不读sealed holdout，不重复旧价格实验。无需运行QT/QE上游Alpha；实际拟合仍fresh QE三0、忙时半小时检查。X临时、F新独立artifact，0生产配置或模型激活/进程控制。

## Architecture / Contracts

### 1. 时钟、原position对象和固定终点

输入是原候选独立价格研究所模拟的持有episode，不读取/修改真实账户仓位。entry为T开盘，原终点E=T+4交易日收盘，首次持有5个交易日不因停牌延长。每个卖价决策在S日收盘后，只用<=S信息，建议下一session U的合法假设出售价p；U是S后立即下一交易日，T+1<=U<=E，以守A股T+1。不做“看完S收盘又在S收盘卖”的时钟作弊；entry日T不能卖。同一episode有1～4个合法sell decision，剩余sessions从4到1，不重置为另5日。

原entry KEY、完整calendar、T/E、S/U、原固定policy hash、候选rank及源包/池身份须一致。没有完整episode、卖出合法日历或价格坐标→UNKNOWN，保留episode；正常停牌/涨跌停不能合成可执行卖出。未建仓的原候选不伪装持有，状态NOT_HELD不进入卖价评价。是否持有是外部冻结shadow policy动作，不按未来盈利筛建仓人口。

### 2. 两臂增量价值与标签

两臂共享同一冻结shadow policy、原shares、S可见price basis、费用和corporate action处理：SELL_NEXT_U_AT_P取得下一合法出售假设价p的净清算价值；CONTINUE_TO_ORIGINAL_E取得原E到期相同净价值。标签advantage_sell_cny=V_sell−V_continue，bps以S原股数的明确净清算参考作为共同分母，不能各自换分母或把累计入场盈利当剩余预测。沉没buy佣金不再扣；每臂自己的未来sell fee/tax分别一次，既不双扣也不忽略E出口费用。若费率不同按同日期合同分别计算，不写死“必抵消”。split/dividend变化按同quantity-equivalent账本，不用未来factor进入S特征。

共同reference固定为原S raw close的quantity-equivalent净清算价值，采用S合同sell费；reference未知/非正为UNKNOWN，不用p或E变化分母。均值头预测CONTINUE剩余净终值/reference，风险头预测U..E的未来路径下行尾部，只有成熟后作为标签。假设卖价p并非实际已知未来open，使用S合法价域、训练观察性支持区间；标签另存真实U open/可执行状态用于一次导航。所有数值绑定label schema/policy hash/费用/终点与valuation denominator；基线policy变化使旧labels不可复用，必须新identity，不覆盖旧模型或标成exact retry。

这在形式上是advantage/uplift型相对动作目标，不等同已识别的因果uplift。当前只有原固定政策及观察价格，未记录随机动作概率/充分重叠，不承诺doubly robust/OPE可无偏；不能把shadow p成交当真实limit fill。双方观点中的OPE是未来有支持数据时的估计框架，不是本设计已满足其识别假设。

### 3. 首先做新标签审计，而不是直接训练

唯一先行假设EXIT5-REMAINING-VALUE-AUDIT-1：原固定5TD持有期内，下一合法SELL vs固定终点CONTINUE是否有可学习的剩余净价值。先target-free KEY/calendar geometry；只有geometry与T+1/期末成熟规则通过，才读取开发窗口成熟标签。第一次只做该新目标的oracle diagnostic与一个冻结learnability模型，不重算旧N2 oracle、不阅读新sealed窗口。

oracle可用真实未来决定最有利合法sell日，必须标CLAIRVOYANT/ORACLE_DIAGNOSTIC/NAVIGATION_ONLY；同时报告原真实open/停牌/涨跌停可执行约束版，不用每episode完美high或分钟极值。比较SELL_U与CONTINUE_E，所有episode原日期保留；缺未来出口不可当0收益/延长终点。完整/partial/unknown/unsettled与原槽现金分别报告。

固定learnability baseline：单一Ridge(alpha=1)一次预登记，features仅既存九日频字段+remaining_session_fraction=(E位置−U位置+1)/4；TRAIN_MEDIAN_PLUS_FLAGS在每fold past train fit，剩余时间字段已知，不偷读realized remaining holding。开发窗口按原entry cohort连续等分为五块，首块仅warmup，后四块各一次past-only expanding fit/evaluation；禁止把新的test/holdout纳入。每个fold训练成熟E至下一评价块最早S之间必须至少有一个完整交易日embargo；同episode全部S决策归同一块，不按行随机CV。warmup/缺成熟样本原行保留UNKNOWN，实际模型fit数与oracle0-fit分开登记，不将未执行fold计入fit。

Ridge目标为10000*(V_continue/reference−1)，动作增量预测为10000*(V_sell/reference−1)减该目标预测；实际U open只作为U时刻已观察的价格场景用于既有S预测查询，绝不进入S features/fit。每fold train median后连续特征按train mean/std标准化（零std取1），missing flags独立保留，remaining已知；test不校准或选点。这里审计只有一个均值head，尾风险head属于后续模型详细设计，不把未拟合风险写成已有输出。

不搜索alpha/窗口/label/seed。learnability缺正增量只说明“当前九信息+简单模型”的该目标不证实；不能证明Exit全局不可学。oracle高/learnability低先扩真正S信息再换模型族，oracle低仅降低本次合法动作空间优先级，NAVIGATION_ONLY不能支持关闭整体方向或激活。审计之后决定是否详细实现卖价模型，不让六层栈变六个并行项目。

### 4. 原槽经济评价与推断

主结果按原entry cohort五槽等权、完整E、现金和UNKNOWN分报，使用第一次合法原U真实open的sell判断，后续政策由冻结多决策模拟器统一，不能将1～4行当独立交易复利。learnability每个episode用一条冻结“首个正advantage且可执行则卖，否则原E继续”的动作链；对每个entry cohort与无新Exit的同输入baseline配对报告成本后lift、避免亏损、错卖后续上涨、实际干预天/次数、剩余期限分布与原策略回撤（只有有效NAV账本才报告NAV，不把cohort均值标年化）。

事前minimum intervention记录为20个不同episode且至少20个entry cohort、干预覆盖>=10%可评价entry日；regime若开发源已有固定分区则至少两个分区各>=5个干预episode，若没有则UNKNOWN_REGIME_SUPPORT不得支持方向关闭/激活。该数值是本次诊断证据说明而非QE或研发准入。以entry cohort block bootstrap（固定seed、5 cohort连续block、2000rep）给区间，最小样本/时序不满足则仅exploratory，不挑显著样本。MDE与intervention数量前置报告；INCONCLUSIVE不改门槛/选点挽救。

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
| F-801 | planned generic_remaining_value_exit_5td_contracts_v1.py | artifact: docs/architecture/advisory_generic_remaining_value_exit_5td_v1_f2_design_20261007.md §1/Review | DESIGN_VERIFIED | none |
| F-802 | planned generic_remaining_value_exit_5td_labels_v1.py | artifact: docs/architecture/advisory_generic_remaining_value_exit_5td_v1_f2_design_20261007.md §2/Review | DESIGN_VERIFIED | none |
| F-803 | planned new labels maturity/keys | artifact: docs/architecture/advisory_generic_remaining_value_exit_5td_v1_f2_design_20261007.md §3/Review | DESIGN_VERIFIED | none |
| F-804 | planned generic_remaining_value_exit_5td_audit_v1.py | artifact: docs/architecture/advisory_generic_remaining_value_exit_5td_v1_f2_design_20261007.md §3/Review | DESIGN_VERIFIED | none |
| F-805 | planned frozen episode action chain | artifact: docs/architecture/advisory_generic_remaining_value_exit_5td_v1_f2_design_20261007.md §4/Review | DESIGN_VERIFIED | none |
| F-806 | planned typed diagnostics/intervals | artifact: docs/architecture/advisory_generic_remaining_value_exit_5td_v1_f2_design_20261007.md §4/Review | DESIGN_VERIFIED | none |
| F-807 | new labels/audit scope only; later model design | artifact: docs/architecture/advisory_generic_remaining_value_exit_5td_v1_f2_design_20261007.md §Implementation/Review | DESIGN_VERIFIED | none |

矩阵只证明设计完整，不冒称模型、研究或自然运行验收。最小测试手算同股一晚/四晚、T日禁止卖/S后U、现金分红拆股/费率异同/沉没入场、终点停牌UNKNOWN、future字段毒值/holdout sentinel、episode块不串/purge、同股多日不独立交易、干预支持不足时只是exploratory。既存旧Exit模型/实验不重跑。

## Risks / Rollout / Rollback / Production Gates

价格支持域、真实可执行性、当前DB非vintage与持有episode相关性都可能限制可学空间；标签正确本身不是经济信号。动态资金仓位没有授权，本设计只是shadow episode价值与独立建议，无账户写入。回滚停止新研究/consumer调用，正式artifact保持只读；无依赖安装、数据库迁移或生产激活。source/标签/研究/未来模型/API及用户重启独立报告，负结果不让旧门槛成为项目停工条件。

## Review / 多轮自审

目标轮：剩余净价值不是holding时长，也不是entry累计盈利；先新标签与固定learnability，非六线工程。时钟轮：S收盘后的U、T+1、固定E及未来label净价/qty同核，期末缺失不延长。推断轮：一个episode动作链/entry cohort聚类，20/20/10%支持事前固定；观察支持不足不能声称因果uplift或DR，期望值正不等于已校准胜率。追加资源/统计轮修订：明确五个entry块中的一warmup四fit，embargo为完整session而非自然日，实际fit计数。三轮设计审查通过，0Exit fit/模型/策略激活。
