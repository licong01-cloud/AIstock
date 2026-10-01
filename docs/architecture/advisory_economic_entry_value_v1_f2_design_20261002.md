# Advisory 收益与风险驱动的日频买入价格建议 F2 详细设计 v1.1

> 日期：2026-10-02；Feature tier：F2；业务归属：Selection Center / Advisory。
> 状态：OFFLINE_ECONOMIC_PIPELINE_VERIFIED_MODEL_NO_TAKE_NOT_CONFIRMED；设计PR #5215已合入，标签/训练/条件查询与消费者工件闭环已完成真实运行；首轮模型TAKE=0，不具备激活依据。日常D冻结网格/API/UI及独立经济确认仍未交付，不宣称完整业务或生产完成。
> 用户需求：给出有预期净收益、风险可接受的买入/卖出价格条件；允许区间外不买和当日零推荐，不以开盘价覆盖率替代业务收益。
> 父级：[策略条件化模型蓝图](advisory_strategy_conditioned_model_blueprint_v1_20260710.md) §5.3、§6.3、§6.7、§6.8、§16。

## 1. Background / 当前事实和目标修正

旧M4 v3/v4学习 `open_T / reference_D - 1` 的q10/q50/q90，属于开盘价格分布；旧ENTRY_PRICE源码、独立API/UI和每日发布工具已交付，不等于收益型买入建议已实现。旧q90不是最高值得买价，q10不是安全抄底价，q50不是最佳买价。

BUG-1640 / PR #5150已经合入，用户重启后的语义验收及close-sync PR #5190已完成。批准29日计划 `advepc_4178fffbd9c93e0bd84216a5` 已执行预测、结算、评价：580个股票—日期样本，模型/市场可评价580/580，连续coverage=0.5637931034，业务tick coverage=0.5827586207，业务宽度/control=2.0664613119；模型减control的interval-score差=-0.0214053808，固定block CI95=[-0.0245338326,-0.0175375111]。结论NOT_CONFIRMED、HISTORICAL_REPLAY/NAVIGATION_ONLY，零binding、零数据库写入，不是收益型建议的胜率或盈利结论。

历史身份限制不变：9日使用既存原生名单证据，20日为恢复的非原生历史证据，17日仅成员内容匹配；2026-08-14/17/18完整原始成员未证明，20日恢复原生receipt=0。不得用新经济模型把这些旧输入升级为原生COMPLETE或独立确认样本。

本设计将业务主目标改为：**在明确价格条件、可交易性、持有/退出policy和成本下，买入相对保留空槽的预期净价值为正且下行风险在预算内；另行证明相对原荐股policy有增量。** 两项不相互替代。避免损失的价值和错失盈利的代价都必须报告。

## 2. Scope / 本轮交付单元

先实现收益型买入建议，日频数据库输入、次交易日开盘单观察点研究及建议，保持候选/排名/退出policy不变。输出是条件式可接受价格集合，允许多个不连续区间或空集；不是必须每天给出某个区间。

首轮最小研究单元不是五日固定持有替代既有policy：复用当前冻结Advisory shadow exit policy、成本及目标Top5槽位，研究只改变“进入还是保持空槽”。仅在既有policy无法产生完整且同坐标标签时报告输入缺口，不偷偷换成更好看的持有期限。1/5/10/20日收益可在预登记后作为辅助诊断，不以其中最好期限选模型。

本长任务包含完整买入建议方案及源码/数据前置开发、可用开发窗口导航实验；不承诺12～16小时内一定有经济正结果或生产激活。退出建议只完成接口/标签设计和已有实现差距核查，下一轮独立训练/交付，不把买入切片冒充全荐股完成。

## 3. Non-goals / 模块与操作边界

- 不修改QE、Selection、StrategyPackage公共源码、Paper、Execution、Position Timing、HMM或数据准备模块；不提交QE实验、不重选候选、不重训上游Alpha。
- 不训练分钟时点或成交模型，不读取分钟Bin，不输出最佳分钟/订单/拆单/fill/资金权重。
- 不改旧M4模型、旧coverage合同、旧frontier或历史工件；不沿用旧模型身份发布不同业务语义。
- 不写数据库，不执行DDL/DML、依赖安装、数据profile激活或用户服务/进程控制。临时数据只在X，持久工件只在显式Advisory F盘根。
- 不要求旧M4达到75%coverage才可研究买入价值；旧价格分布不能绕过自身发布合同，本新角色也不能借旧确认状态激活。

## 4. Architecture / 两时钟、三语义

### 4.1 相互独立的能力

1. **OPEN_DISTRIBUTION**：既有开盘分布，保留历史名字/API兼容并明确标签，仅作可选上下文。
2. **ENTRY_VALUE**：拟新增收益/风险驱动的条件式买入建议；独立目标、schema、模型、binding及证据状态，不覆盖旧ENTRY_PRICE。
3. **EXIT_VALUE**：拟新增退出相对继续持有的剩余净价值建议；独立标签和policy，不以买入成本决定是否应该继续持有。

现有公开入口只用于复用：`policy_episode_labels.build_policy_episode_labels(...)`、`shadow_portfolio_policy.replay_shadow_portfolio(...)`、`AdvisoryPolicyCostV1`、`PostgresRealtimeFeatureSource.load(...)`、`build_advisory_feature_matrix(...)`、研究registry和window合同。新入口/版本在源码交付前均为规划，不声称运行中API支持。

### 4.2 D收盘预测与T开盘条件选择

- D截止：冻结候选、PIT特征、包/股票池/成本/policy、模型和价格查询网格，输出条件式价格价值面及适用条件。`query_price` 是明确的假设条件参数，不是从尚未发生的T开盘读取的特征。
- T开盘：只在只读指定观察点的权威开盘价到达后，选择已冻结建议中的对应条件；不可读T的high/low/close或路径。不在开盘后改写D预测。盘前界面展示“若价格落入区间且有效条件仍成立”，不能展示无条件BUY。
- 当前无盘中实时开发。若未来在T观察点重新估计价值，必须独立标记其时钟、模型和可见字段，不能把T价或T信息倒装进D模型。
- 开盘偏离区间可执行预约定的SKIP，但不证明股票出问题。公司行动/有效参考价/法规价先正确投影；既有日频DB数据来源保持不变。

### 4.3 价格与收益不是机械反比

对同一股票，低开既改变成本，也可能改变未来价值的条件分布。不能把一个固定预测卖价除以任意更低买价就构造“越跌越值得买”。训练/推理必须对价格条件和跳空状态建模，或在没有可信信息时返回UNAVAILABLE/OUT_OF_SUPPORT。

模型学习的是条件预测，不声明干预价格会改变或不会改变市场。不能把同一实际未来路径复制到不同假设买价并称为真实成交样本。价格扫描的区间只有在真实观察的条件支持域内才有意义；区间内未被真实开盘或另行批准观察点检验的价格仍是模型推断，不能称为已验证可成交点。

## 5. Contracts / 标签、范围和结果

### 5.1 输入身份与价格坐标

请求至少绑定program/package/manifest/style/universe/candidate roster、D/T、feature schema及真实截止、训练来源、模型/标签schema、shadow exit policy hash、成本hash、公司行动/价格坐标版本、数据release/profile身份、decision_use、consumed windows和候选生成版本。拒绝run/list/hash/D-1/包/policy任一矛盾，正常缺失保留候选，金额只转换一次。

政策标签价格可来自Qlib复权坐标，展示价格是未复权CNY；必须完成同收益坐标parity审核、逐日公司行动转换及真实参考价记录。不同基准的entry/exit或经济label与价格查询不能直接相除。新的数据读取只调用已公开consumer合同，不修改或回退active profile/节点路径；输入不完整提出精确需求。

### 5.2 真实观察点的标签

训练行只来自真实T开盘、D已冻结候选/特征及同一policy simulator的成熟episode。每行至少保留：decision/target/symbol、D参考价、实际开盘及gap、可进入状态、entry/exit及有效日期、label_information_end、净收益、下行风险、退出reason、policy/cost/source hash、删失/未知/停牌状态。

在同一冻结模拟器生成：

```text
enter_value = 按真实允许的T开盘进入并继续冻结exit policy的成本后净价值
skip_value  = 相同评价时间跨度内保持对应固定槽位空缺的价值
entry_advantage = enter_value - skip_value
```

净收益中的成本只能扣一次。下行风险优先复用同policy的日级mark路径，明确是开盘/收盘mark风险，不声称盘中最坏成交风险。停牌/跌停等待、T+1、未知缺口与右删失分别typed，不把未成熟episode标成零收益或输家、不删原候选。真实无法进入不构造可进入的成功标签。

### 5.3 模型与支持域

首轮只使用一个固定LightGBM候选：净收益均值为regression头、同policy日级mark最大回撤为q90 quantile头；每头固定200轮、learning_rate=0.05、max_depth=3、num_leaves=7、min_data_in_leaf=30、seed=20261002、num_threads=2，无超参搜索或test early stopping。两个头共同构成一个candidate，registry计1个模型变体；规则对照分别计比较臂，不冒充新训练trial。实施前将完整effective参数、特征顺序和源hash写入request，不以训练结果换参数。

价格条件采用真实开盘gap作为训练条件、盘前显式假设价格作为查询参数。特征只从D截止数据构造，训练/变换/校准仅chronological past，并以label_information_end剔除跨边界标签。收益均值、回撤分位数和估计不确定性各自命名，q90回撤不被称为收益的置信下界。

原M3弱信号和旧Entry试验负结果是基线事实，不直接作为新角色的有效训练头。新hypothesis family定义为“同候选、同exit policy下的价格条件化进入价值”，不是P0主模型替换或旧动态q90阈值回选。先固定一个候选模型和透明对照；不扫描大量模型、持有期、阈值或亏损容差。

训练价格支持域、每个条件段有效样本量、日期分布和不确定性校准均由past-only训练/validation确定并写入工件，不能根据回放结果扩域。训练不足不伪造下界或默认风险，模型对单股未知信息不强行估值。新信息尚不可得时不机械宣布低开股票坏掉；可直接表示原推荐条件失效。

首轮特征顺序固定为：parent_combined_score、parent_rank_pct、leg_norm_score_gap、ret_1、ret_5、atr14_close、csi300_ret_5、market_up_ratio、query_gap_bps。前八项为既有D可见特征，第九项只在训练采用真实观察条件、盘前采用明确假设参数；不消费未来path列。支持域以训练期100 bps gap分桶，每桶至少30个真实观察且覆盖5个决策日，并限制在该桶真实观察min/max与其它D特征训练范围内。不确定性首先报告validation绝对误差p90诊断，不作为均值置信界或盈利概率。

规则比较臂固定gap [-300,+300] bps、风险预算引用既有800 bps stop policy；均为首次真实收益读取前确定，不能因结果追调。训练资源请求显式行数预算，默认100,000，固定2线程。模型工件保存前仍需正式run request及registry登记，本节不是已执行实验收据。

### 5.4 建议集合与合同状态

对预先冻结的合法价格网格，只保留：在支持域内、可交易条件满足、预期净价值严格高于预登记的最小经济效用且下行风险满足预算的节点。首轮进入vs空槽的净价值阈值为0 bps，要求`expected_net_return_bps > 0`（与既有cash_return=0一致，成本已经扣除），不是收益保证；matched增量另行评价。不确定性另报；不得把收益分位数称为期望收益置信下界。下行预算优先明确引用现有shadow policy的止损预算，注明这是风险预算参考而非跳空止损保证，正值及价格基础须前置核验；没有有效既有预算时只交付价值/风险估计并列出欠缺参数，不新增审批平台或擅自指定风险容忍。不能由TAKE数量反调。

非连续通过节点返回多个区间，不用min/max包住中间拒绝点；每个区间记录网格分辨率，不承诺节点间精确最优。全拒绝为NO_ACCEPTABLE_PRICE，数据/模型无法判断为UNAVAILABLE，正常不可交易为NOT_APPLICABLE/WAITING，未知不能伪装成正常SKIP。原股票仍保留并展示reason；Top5空槽不自动补第6名，无资金仓位。

拟新增业务响应至少含：`role=ENTRY_VALUE`、`objective_contract=RISK_MANAGED_ADVISORY`、prediction/bundle identity、D/T、validity conditions、price basis/tick/anchor、`acceptable_price_intervals`、条件化expected_net_return和downside_risk、统计支持与不确定性、recommendation/evidence/availability、policy/cost identity。旧OPEN_DISTRIBUTION仅作独立子对象，不把nominal coverage写成盈利概率。

### 5.5 退出建议的后续合同

Exit从已可见持仓状态出发，比较下一合法可交易点退出vs继续冻结policy的剩余净价值。收益回吐、尾部风险与过早卖出的机会损失均纳入评价。目标价格达到不等于最佳退出；历史日高/低/MFE/MAE只作标签描述，不能当最佳可成交点。先实现买入角色，不将该设计状态记为Exit源码或模型已交付。

## 6. Evaluation / 业务评价与证据分级

首轮只在已消费开发窗口作HISTORICAL_REPLAY/NAVIGATION_ONLY，真实29日旧输入若复用必须按原候选/日期和证据限制，不缩窗删股、不重选、不倒填native。成熟收益到不了退出时点的样本保留删失，不凭今天日期判成熟，也不等待自然20日才做功能验证。

冻结比较臂：原无保护Top5/原exit policy；透明上下界规则作为规则对照（规则明确不冒充模型）；一个价格价值模型。具体规则阈值、风险预算、数据窗口、训练切分、trial预算在首次收益读取/搜索前写入run request。不能为提高TAKE率追调阈值，且不默认复用旧3%/5%负结果作为独立确认。

主要报告：固定槽位净收益及相对无保护增量、MDD/尾部损失、保持现金比例、真实进入/跳过次数、干预日分布和支持度、被跳过的亏损与盈利、模型UNKNOWN比例、每价格条件段稳定性及跨regime描述。单笔平均收益和胜率辅助，不因只保留几笔赢家就宣布有效。

历史收益按同一价格/可交易/成本/exit simulator核算，不能把独立episode的总收益直接相加当组合收益；需要真正matched固定槽位组合模拟。episode分析与组合分析分别输出，组合parity未通过不宣称绝对/超额改善。日期block推断保留serial dependence；欠功效结果只能导航，不增加任意方向关闭门槛。

旧75～85%coverage和1.25x宽度仍约束旧价格分布角色，不约束收益型建议的输出比例或价格集合宽度。新模型的收益概率/风险分布若对外展示，必须单独验证校准。模型有效≠自动交易或保证利润，历史导航≠一次性holdout≠自然前向。独立确认前保持EXPERIMENTAL_SHADOW，不发布生产ENTRY_VALUE绑定。

## 7. Implementation Plan / 12～16小时顺序

| 阶段 | 预算 | 精确交付 | 验证 |
|---|---:|---|---|
| E0 | 2h | 蓝图正文统一、当前29日结果、新F2设计 | 两轮目标/数据时钟/兼容审核；feature validator；diff check |
| E1 | 2～3h | 新价格价值contracts及policy-bound真实观察标签adapter | 成本一次、标签成熟、身份矛盾、正常停牌保留、无未来feature及参考价转换测试 |
| E2 | 3～4h | 预登记轻量模型、past-only切分、支持域、区间内核、原子工件 | label-end purge、T数据毒化、分位数/均值分离、非连续集合、缺失及hash测试 |
| E3 | 2～3h | 只读输入spike、已消费开发窗口导航、固定槽位matched evaluation | 不调用QE、不改原候选、不读sealed；exact retry/恢复；真实资源与耗时 |
| E4 | 2h | 多轮审核修复、独立API/UI接入设计及可交付切片 | 源码/设计/经济证据分别判定；有缺口不称完整；生产仍未绑定 |
| E5 | 1～2h | Exit设计复用审计、结果/下一轮交接 | 日级建议与执行边界；不研发分钟策略、不重复QE |

本次明确文件范围：本设计、主蓝图；新`backend/services/advisory_model_first/economic_entry_{contracts,labels,training,inference,sources,pipeline,cli}.py`及对应Advisory定向测试。其中sources.py在2026-10-02源码阶段前登记为消费者侧只读DB批量读取和价格坐标parity，不修改公共数据生产者。文件名为计划范围，未存在不视为已实现。API/UI实际实施前另行登记精确Advisory文件/版本，不能把设计条目勾成已加载端点。不修改其它模块、全局CI、标准或AGENTS。

所有代码至少两轮：首轮核对经济语义/PIT/坐标/支持域，修复后第二轮核对恢复、身份、未知缺失和legacy回归；有新发现继续修复到通过。离线批量先核容量并固定最多2线程、单条只读SQL超时30秒，不按QE历史状态误阻断纯回放。临时目录X不可用即报告，不回退C；不人为烧满预算，完成可交付阶段提前交付。

## 8. Verification Plan / 结果验证

测试只覆盖必要业务合同：未来T路径毒化不改D输出、训练/validation/test时钟、候选唯一性和完整保留、policy/cost/包/池/坐标hash、label maturity与缺失、成本不重复、低开不能仅靠除法获利、支持域外拒绝、不连续价格集合不桥接、空集与未知区分、真实开盘选择冻结条件、旧M4/API不变、恢复/原子保存。避免重复fixture、实现快照、常量重复断言和测试债务膨胀。

设计审核不等同于源码验收。E1/E2定向测试使用可手算fixture；E3真实工件证明数据/运行闭环但只有导航证据。E4功能若未覆盖API/UI则明确标为内核切片，不声称用户端完整交付。正式激活需要独立研究合同与有效确认范围，不由本次历史重放自动授权。

## 9. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-540 | 开盘分布/买入价值/退出价值分离，旧覆盖率不能冒充收益 |
| F-541 | D冻结价格假设与T真实观察分开，拒绝未来路径特征 |
| F-542 | 真实可进入、成熟、policy/cost/价格坐标一致的增量标签 |
| F-543 | 价格条件化收益与下行风险，不用机械低价或M4 q90造边界 |
| F-544 | chronological训练、label-end purge、支持域和不确定性独立 |
| F-545 | 多区间/空集/未知分开，保留原候选及固定空槽，不补位 |
| F-546 | 收益/风险/错失机会/干预支持度matched比较，不能冒充成交 |
| F-547 | 旧身份证据分级、旧实验工件不变，开发导航不激活 |
| F-548 | 仅Advisory开发、日频DB-only、零QE训练/生产/分钟执行 |
| F-549 | 两轮以上语义/PIT/兼容审核、原子恢复及真实状态可接续 |
| F-550 | 买入先行，Exit独立剩余价值设计，未实现不得报完成 |

## 10. Design Acceptance Matrix

本矩阵在E0设计通过基础上验收本次明确交付的E1～E3**离线内核/研究消费者**，F-550仅验收退出后续设计。OFFLINE_ENGINEERING_VERIFIED只说明该切片合同成立，不代表经济模型有效；本次真实模型NO_MODEL_TAKE/NOT_CONFIRMED。完整D每日网格/API/UI/生产binding不在这次源码切片的完成声明内，仍按E4实施；不将本矩阵或历史模拟收益冒充其完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-540 | economic_entry_contracts.py、§15.2 | backend/tests/advisory_model_first/test_economic_entry_model.py | OFFLINE_ENGINEERING_VERIFIED | none |
| F-541 | economic_entry_training.py、economic_entry_inference.py、economic_entry_evaluation.py | backend/tests/advisory_model_first/test_economic_entry_model.py、backend/tests/advisory_model_first/test_economic_entry_evaluation.py | OFFLINE_ENGINEERING_VERIFIED | none |
| F-542 | economic_entry_labels.py、economic_entry_sources.py | backend/tests/advisory_model_first/test_economic_entry_labels.py、backend/tests/advisory_model_first/test_economic_entry_sources.py | OFFLINE_ENGINEERING_VERIFIED | none |
| F-543 | economic_entry_training.py、economic_entry_inference.py；§15.2风险口径缺陷限制激活 | backend/tests/advisory_model_first/test_economic_entry_model.py | OFFLINE_ENGINEERING_VERIFIED | none |
| F-544 | economic_entry_training.py | backend/tests/advisory_model_first/test_economic_entry_model.py | OFFLINE_ENGINEERING_VERIFIED | none |
| F-545 | economic_entry_inference.py、economic_entry_evaluation.py | backend/tests/advisory_model_first/test_economic_entry_model.py、backend/tests/advisory_model_first/test_economic_entry_evaluation.py | OFFLINE_ENGINEERING_VERIFIED | none |
| F-546 | economic_entry_evaluation.py、§15.2；本项不是模型晋级 | backend/tests/advisory_model_first/test_economic_entry_evaluation.py；artifact: F:/Dev/AIstock_model_artifacts/advisory_economic_entry_value_v1_20261002/adveconomic_4962fd2ec68948033628954a/evaluated/evaluation.json | OFFLINE_ENGINEERING_VERIFIED | none |
| F-547 | economic_entry_pipeline.py、§15.1 | backend/tests/advisory_model_first/test_economic_entry_pipeline.py | OFFLINE_ENGINEERING_VERIFIED | none |
| F-548 | economic_entry_sources.py、economic_entry_cli.py、§12 | backend/tests/advisory_model_first/test_economic_entry_sources.py；artifact: F:/Dev/AIstock_model_artifacts/advisory_economic_entry_value_v1_20261002/adveconomic_4962fd2ec68948033628954a/prepared/source_receipt.json | OFFLINE_ENGINEERING_VERIFIED | none |
| F-549 | economic_entry_pipeline.py、§14～§15.2 | backend/tests/advisory_model_first/test_economic_entry_pipeline.py；artifact: F:/Dev/AIstock_model_artifacts/advisory_economic_entry_value_v1_20261002/adveconomic_4962fd2ec68948033628954a/evaluated/manifest.json | OFFLINE_ENGINEERING_VERIFIED | none |
| F-550 | §16，已有exit_label_oracle.py/exit_learnability_contracts.py只读复用审计 | artifact: docs/architecture/advisory_economic_entry_value_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |

## 11. Risks / 风险与修订纪律

- 价格条件与市场信息混淆：真实gap训练只是条件预测，缺信息不能识别个股突发利空；不宣称因果或全局最优。
- 原policy/价格坐标不匹配：先消费者parity和标签成熟检查，错误交给所属模块，不越界修。
- 低覆盖/过度保守：coverage只约束分布角色；经济角色报告错失机会和实际干预，不能靠零推荐通过。
- 过拟合：只有预登记预算和past-only选择，旧失败不擦除，开发窗口不升级独立OOS；新假设新身份，不回选旧q90/frontier。
- 工程膨胀：复用既有consumer、模拟器和registry，不建新平台/数据库/审核UI，不以历史归档抢占主线。
- 实施成本超预算：交付可接续的真实阶段状态；不降低功能定义、不假装模型训练或后台监控仍在运行。

## 12. Production Gates / 生产状态

design_source_merge=PR_5215_MERGED(a0c7e5aba50f8b33c1f715115181a64fc56eab4c)；kernel_source_merge=NOT_SUBMITTED；runtime_activation=NOT_REQUESTED；backend_restart_owner=user；production_ddl_gate=NOOP；database_written=false；profile_changed=false；qe_experiment_submitted=false；real_data_model_trained=true；real_model_variant_count=1；economic_model_confirmed=false；binding_activated=false；sealed_holdout_accessed=false。

当前只授权本长任务的设计、开发和研究；部署、后端重启、生产操作均非本设计默认动作。按此前有效提交/合入授权交付源码时仍需满足完整审核和必需CI，设计合入不等于模型激活。清理仅限明确授权的本任务精确目标，不能删除其它worktree或旧实验。

## 13. Rollout / Rollback

先合入设计，再在独立Advisory切片实现离线内核与导航工具；程序返回成功只说明工件完整，不说明模型有效。新增API/UI只采用显式新版本并明确EXPERIMENTAL/NOT_CONFIGURED，默认旧接口及原baseline行为不变，未绑定不现场训练或填规则值。确认、绑定、源码加载、每日预测与成熟结算各自验收，后端源码需要生效时由用户重启。

新角色出现身份/模型/数据错误时仅该角色typed unavailable，保留原候选及其它角色，不默认改排名、重选股或发订单。回滚通过本角色独立版本/binding指针恢复先前已验证状态；本任务不执行生产指针更改。历史工件和旧模型不删除或覆盖，禁用建议不使历史结算失去原模型身份。API/角色尚未交付时不把这些规划动作记为已实现回滚。

## 14. 设计审核记录

第一轮：核对业务目标和既有标签/方法签名，修复旧路线以开盘coverage作为买入主线前置的偏差；补充真实观察点和价格条件非因果边界、候选/坐标/成本一致性。机器结构检查发现缺rollout/rollback，已补齐。

第二轮：核对PIT、支持域、旧接口、研究身份和实际进度；固定单候选模型/两个头的有效参数、相对现金净价值和既有风险预算来源，禁止收益分位数伪装均值置信下界。修正蓝图F-256的legacy范围及旧Entry/Exit回选禁令与新用户授权的区别，保留原P0/N3结果。源码/模型/生产全部保持未完成，不借DESIGN_VERIFIED宣称业务有效。

复验命令：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_economic_entry_value_v1_f2_design_20261002.md --tier F2`；主蓝图同命令独立校验；`git diff --check`。多轮审核是本窗口的不同检查面，不宣称使用了独立外部审核者。

E1元数据spike（未读取收益值、未训练）：既有policy dataset `81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd` manifest文件SHA=`cbf7378d25c90ba62e97a14769778c04510e34c1bc43c7be88560f95b7951518`，与N1冻结引用精确匹配；Parquet元数据为7,720行/386决策日（2024-07-04～2026-02-02，源截止2026-03-10）。既有shadow为Top5/exit rank40、确认2日、止损800 bps、移动止盈1800/700 bps、time stop20日；cost buy/sell=0.95/5.95 bps、cash=0。净收益沿用现有乘法成本公式而非简易减法，不能二次扣除。

该旧标签schema有entry/exit/net return和label_information_end，但没有D原始参考价、公司行动投影和同episode日级回撤路径；不能把缺字段补0或直接声称风险训练输入就绪。E1下一步只读补充真实观察gap与policy-bound日级mark路径，完成坐标/身份parity；若公开consumer不足提出精确需求，不修改数据/QE或伪造完成。最新只读QE profile响应为`20260928-v15-unified-moneyflow1`、cutoff2026-08-31、两个available nodes；这只是消费者入口状态，尚不构成新训练数据完整性或PIT证明，本任务没有修改或激活profile。

## 15. 内核开发检查点（非完整业务验收）

Advisory独立源码：economic_entry_contracts.py、economic_entry_labels.py、economic_entry_training.py、economic_entry_inference.py。入口绑定原policy/cost/候选和价格来源hash；不重建退出policy、不扣第二次成本。支持完整标签、正常未进入、右删失、未知缺失分离；真实gap校验raw reference，daily mark只到实际退出开盘。

多轮本窗口源码审核发现并修复：缺开盘不能当正常SKIP；嵌套policy突变需消费前重验hash；训练query condition不能采用调用者携带的未来列；D参考价时钟必须精确D；部分价格未知不能宣称全价格拒绝；非连续接受集合不能桥接拒绝节点；模型支持统计及风险输出非法必须失败。训练只使用固定白名单，按成熟信息截止purge；测试收益及未来path毒化不改变真实LightGBM训练工件。合成fixture拟合仅为单元测试，不是研究证据或经济有效结果。

定向测试：`pytest backend/tests/advisory_model_first/test_economic_entry_labels.py backend/tests/advisory_model_first/test_economic_entry_model.py`。Ruff与定向L0通过；L0的P2 ALGO-COMPLEXITY提示已按行数预算、精确键一对一join和9列矩阵做复杂度审核，不转移ownership或改变其它模块。

真实离线闭环已完成：只读DB/价格坐标消费者、独立预登记/registry、原子工件、恢复CLI、固定模型训练、实际开盘条件导航和matched组合比较。首次SQL超时、S/R歧义和DB scalar序列化错误的零模型输入尝试均保留；没有依结果改变模型、门槛或数据窗口。最终study `adveconomic_4962fd2ec68948033628954a` 绑定相同首轮研究配置。独立API/UI、完整日常D冻结网格消费者和经济确认仍未完成；本次源码交付单元是离线内核及研究消费者，不是完整价格建议产品。

### 15.1 消费者和工件实施合同

首次读取真实label/未来价格路径之前，先登记独立study plan：冻结已消费窗口、源文件hash、固定模型配置、规则臂和数据转换方法；prepare输出的新数据hash只是绑定已生成输入，不能借绑定调整研究参数。使用既有trial registry与research window授权函数，ledger仅在本次独立Advisory输出根追加，不写旧台账或数据库。

只读来源为现有DB的kline_daily_raw/adj_factor/stk_limit/suspend_d/dividend/trading_calendar。批量限制单条SQL30秒、一次repeatable-read readonly事务，完成后rollback；元/股、价格单位除数只转换一次。公司行动投影复用Advisory现有D可见计算。政策价格允许通过所有既存成熟entry/exit端点核验同一symbol常数×DB adj_factor坐标；这是坐标恢复，不是原生receipt，不能从收益效果反调系数。坐标相对误差预先固定2个float32 machine epsilon（2^-22），绝对误差1e-8，仅覆盖两个Qlib端点的表示误差，不是事后收益容忍；超过即fail closed，无法确定坐标的样本保留unknown。原始文件及候选不改。

工件发布采用同盘持久generation目录写完后原子目录rename，只有完成manifest校验的published stage可消费。未发布generation是可恢复的持久工件，不删除、不伪装完成；临时scratch和第三方TEMP只在X，不使用C。exact retry读取并核验同request/parent及全部文件hash，不再训练或查询；冲突、截断和外来文件fail closed。模型及工件不得自动生产绑定。

只读查询采用一个repeatable-read快照中的月度批次，joined表分别显式约束相同日期边界，以裁剪历史分区；不是每日重建工作区，不改变候选、窗口、模型或超时合同。源收据记录各query行数/耗时。停牌事件的原始S/R/timing逐条保留：R-only不作为停牌；S且全天时段、无R冲突才认定全天停牌。S/R同日歧义或日内停牌在本日频开盘合同中标记tradability_unknown，不任意决定先后；受影响进入/退出端点标签为DATA_UNAVAILABLE并保留候选，其它完整样本继续。不把正常歧义当全局请求失败、SKIP或零收益。未知类型/身份/窗口/重复报价等结构矛盾仍fail closed。

下一切片预登记范围：新增`backend/services/advisory_model_first/economic_entry_evaluation.py`及对应定向测试，只消费已发布输入和冻结模型做固定比较，不修改既有shadow simulator。首次评价仅为实际开盘条件价值导航，不声称全部D冻结网格/实时API/UI已完成；评估实现hash另行前置绑定，不能改模型训练方案。UNKNOWN明确报告；若采用baseline fallback作匹配研究控制，只能作为研究回放的控制处理，不能展示为模型TAKE，也不形成生产默认降级。组合沿用既存模拟器的固定槽位/cost口径，并单独核对episode精确净收益与每日组合口径的差别。

### 15.2 真实导航结果与风险语义审查

持久根：`F:/Dev/AIstock_model_artifacts/advisory_economic_entry_value_v1_20261002/adveconomic_4962fd2ec68948033628954a`。完整候选/标签7,720条、386决策日，AVAILABLE=7,716、正常NOT_ENTERED=3、右删失=1；缺旧特征1,160条保留UNKNOWN。数据库38万余行（380,240）、25条语句合计9.28s、最长0.578s；15,432个既存entry/exit坐标端点全匹配，最大相对误差1.113e-7。仍为RECOVERED_LIMITED，不升级原生历史receipt。

第二轮可交易性审核另修复条件查询消费者对开盘涨停/涨跌停身份缺失的显式UNAVAILABLE处理；原study查询没有显式过滤该条件，限制保留且无模型TAKE，不重跑或覆盖其结果。追加审核收据为`evaluation_review_35c03a06b550/evaluated/attribution.json`；初版归因收据也保留。实际成交仍不属于本日频模型验证。

唯一模型train=3,139、validation=1,509、purged=349，6个训练支持桶；validation收益绝对误差p90=1,108.48 bps、风险q90覆盖率=0.9642。该误差不是期望收益置信界，风险coverage不是荐股胜率。test及校准切分按原预登记，不消费sealed。

测试窗口2025-10-09～2026-02-02，共81决策日、1,620个Top20条件查询、405个Top5机会；共同组合估值期100日。基线/固定±300bps规则/模型臂共同期收益约+19.17%/+19.17%/+5.97%，MDD约-10.23%/-10.23%/-2.22%；模型减基线平均日收益=-12.47 bps。这是同一模拟器的历史研究口径，不是指数超额、自然前向或真实成交。固定规则未改变实际组合结果，不能把其7次候选SKIP冒充7次真实干预。

模型Top5动作SKIP=397、UNKNOWN=8、TAKE=0；397个受支持条件中197个期望净值为正，风险q90预算通过=0（预测最小约875 bps）。模型臂7笔进入全部来自UNKNOWN时的研究基线控制，不是模型TAKE；其+5.97%不能称为模型荐股正收益。追加归因独立保存于`evaluation_review/evaluated/attribution.json`，不改旧评价：48个基线进入信号被模型SKIP，其中29个原episode盈利、19个亏损；这些是episode描述，不相加成组合收益或因果避免损失。

首轮结论：**NOT_CONFIRMED / NO_MODEL_TAKE，不激活，不回选阈值。** 本轮暴露的首要设计风险是把“入场价止损800bps”引用为“全episode峰值至谷值日级回撤q90≤800bps”的预算；两者语义并不等价。不能据此宣称价格价值不可学，也不能把800提高到刚好放行模型。下一经济研究前先修订风险标签/预算关系：分别描述entry-anchored净下行、peak-to-trough回撤与止损规则；采用明确经济风险口径或相对冻结基线动作的风险增量，预先决定、独立新lineage，旧结果不改判。此项优先于换loss、加模型族或重训其它seed；源码工具可以验收，但当前模型禁止生产绑定。

E4产品身份必须区分训练来源identity与每日预测输入identity。本离线内核按同一历史研究身份绑定消费；不能用该相等检查要求未来每日候选/price source hash等于训练原始数据hash，从而只支持回测。下一产品消费者需独立PredictionInputContext，绑定新的D截止/候选/来源hash，同时显式核对训练scope的package/policy/cost/特征schema/股票池定义及坐标算法版本。预测来源变化不触发重训；scope不兼容则typed unavailable，不自动为新包宣称可用，也不修改QE。

工程审核证据：57项定向测试通过（2.57s）、Ruff通过、L0通过；feature扫描0finding，主扫描6项P2 ALGO-COMPLEXITY提示/0blocking。逐条审查：source端点与特征仅exact many-to-one/one-to-one加入；raw market行数500,000上限，本次380,240；特征100,000上限、实际7,720，模型矩阵9列；month批次单一快照，查询预算30s；实际条件查询1,620行、Top5匹配最多405机会，不做股票×日期笛卡尔积。公开API/UI、跨模块业务回归留给其实施阶段/CI，不用这些单测冒充已部署业务。DESIGN-COMPLIANCE-001四项已核查：不声明完整产品；未知/fail-closed和研究控制显式区分；旧实验/政策/成本/门槛不改；不增加新审批或跨模块修改。

## 16. Exit后续设计与已有能力复用核查（本轮design-only）

只增加日级EXIT_VALUE价格条件价值建议，不研发分钟执行或接管Paper持仓。已存在`exit_label_oracle.py`的退出vs保持政策增量标签、合法延迟/右删失/UNKNOWN状态；`exit_learnability_contracts.py`已有持有状态、rank、D可见收益/波动、距离止损/止盈及regime的固定特征。二者可复用合同和模拟器，但oracle标记future_information_ceiling=true、deployable=false，不是已激活退出模型；旧holding/liability信号也不能当作剩余收益预测。

新标签以D的合法持有状态和下一合法观察价为条件，在同一冻结终端/政策和价格坐标下比较：`exit_value = 当前合法卖价×(1-sell_cost)后在共同终端持有现金`；`hold_value = 从当前状态继续冻结退出政策的终端净价值`；`exit_advantage = exit_value-hold_value`。既往入场成本已沉没，不再次扣入退出动作差；两臂只对各自未来交易成本计一次。入场价、持有龄和历史peak可作为D状态/政策条件，但“回本”不是退出的经济目标。

必须记录episode/policy/cost/模型/状态/公司行动hash、D截止、实际可卖日期、共同终端、label_information_end与动作等待原因；T+1/停牌/限售或不可证明成交时返回WAITING/UNAVAILABLE，不把无法卖出标成应继续持有的获利样本。仅在合法T开盘条件查询时读取该点价格，不读取T后路径作feature；未来路径只生成成熟标签并按information_end purge。

后续实施顺序：先审计已有Exit标签和价格坐标能否对齐当前经济口径→冻结一组状态/实际观察标签和开发窗口→固定简单learnability基线→通过有效干预与matched增量后设计独立bundle/API/UI。可接受卖价集合按exit advantage和继续持有的尾部风险定义，允许不连续/空集/UNKNOWN；不得把买入价×固定止盈比例或M4开盘分位数改名为模型卖出区间。该阶段尚未训练新EXIT_VALUE模型，也未完成其产品接口；Entry风险合同修订不因这项设计被搁置。
