# QE 新版 L2 风险可选消费与 125 日受控评价

日期：2026-10-23。Tier：F2。合同：`QE-L2-RISK-ENTRY-GATE-D1..D7`。

审批状态：`APPROVED_IMPLEMENTED_FRESH_PROCESS_VERIFIED`。用户已明确批准 `QE-L2-RISK-ENTRY-GATE-D1～D7` 完整合同，批准回执与当时设计 SHA 位于 `F:/Dev/AIstock/tmp/handoff/qe-l2-risk-consumer-20261023/user_approval_receipt.json`。2026-10-25 复核确认：同一资产和合同的六账户已经完成并入仓；此次仅完成 QE 源码与测试收口，新增实验为0。常驻 API 未加载本分支，不声明已上线。

2026-10-24 HMM 消费交接中“无 QE 已批准公式”的描述未包含本窗口已有批准；本次核对了批准原文、风险 SHA、release、C-013/PIT 和三份 AF SHA，一致后沿用 D1～D7，不借用 HMM 离线公式。最新证据目录：`F:/Dev/AIstock/tmp/handoff/qe-l2-consumer-closure-20261025/`。

## 1. Background / 背景与问题

HMM→QE 交接为 `F:/Dev/AIstock/tmp/handoff/hmm-l2-risk-qe-contract-20261023/hmm_l2_qe_consumption_contract_handoff.json`。HMM 确认没有适用于本资产的 QE 专用消费合同。模型是冻结的 20 维监督逻辑回归 L2 绝对回撤风险模型，`warning = probability >= 0.20`；该报警标记本身不是交易权限。

本次业务问题：在完全相同的 AF 预测、PIT、起始资金和执行费用下，仅对 AF 原本计划的新买单使用 L2 风险拦截，能否改善成本后收益/回撤及新入场风险。它评价新买入风险控制；不把结果推广为全持仓去风险、轮动预测或生产收益保证。

HMM 原研究的 `(1-p)/131`、二日确认和额外一日延迟均不迁入 QE。本合同首次明确股票订单层的政策，必须独立批准。

## 2. Scope / 范围与 Non-goals / 边界

- 两臂：B=`AF_NO_L2_OVERLAY`；L=`AF_L2_ENTRY_GATE_V1`。seed=42/2024/2026，共 6 次正式分钟回测。
- 执行与估值窗口固定为 2026-04-01～2026-09-30，125 个交易日，不改为9月29日。
- 新消费者独立可选、默认关闭；旧 preset_A、旧 HMM、旧系数合同及默认策略保持原实现。
- HMM fit/inference=0；AF model fit/processor fit=0；基础 AF 推断已完成。后续两臂只重放相同冻结预测，不分别重新推断。
- 不改数据集、active profile、模型参数、训练窗、股票池定义、费用、资金限制、HMM阈值或默认策略，不重启服务，不扩展参数搜索。
- 不读未来结果选择参数、seed或股票；保留完整131行业、availability/reason及全部预测股票日。无新 UI、数据表、行业黑名单或生产默认策略切换。

## 3. Architecture / 架构

冻结 AF 三 seed 模型与已拟合预处理器 + 九月绑定因子面板 → 3 份共享预测及 manifest → 同一 C-013/PIT 全人口映射 → 两臂共用 compose/终端区间适配/分钟执行 → L 臂独立订单拦截 wrapper → 真实订单、成交、资金及收盘估值 → 成对评价与现有 QE Archive。

预测、映射、权威 spec 和运行包均使用现有 node1 CAS；运行前重新核验 hash。未使用展示 TopK 构造业务人口。每个 worker 是隔离 fresh process，使用显式绑定源码和运行包；不借现有 API 进程重载实现部署。

## 4. Contracts / 精确合同 D1～D7

### D1：资产、数据与共享预测

- HMM 原始预测 SHA256=`5e6a2784a304b2a34cb8cbf6859aab0a9c154a29ba5fac246ff35eebcd9ca0e5`，125×131=16,375行。原始概率与warning不改写。
- generation=`20260930-monthly-v2-unified-correction-4ab0b48642c3`；release=`qe_hmm_full_v2_20260930`；manifest identity=`522e8e35896e85fc9289ca2ceca1cd6127f4c7978a72766887c0429dc9b7f410`。
- 明确 pin 该不可变 release，不跟随后续 active 更新。若运行时身份不符即停止，不自动换用修复版；如需换版须先核验 HMM 与 AF 的共同身份适用性。
- 复用 AF `qe_20261020_111353_df4d/Loop1,2,3` 的模型和已拟合 `RobustZScoreNorm(clip=3)+Fillna(0)`。保留原11列及顺序；PDH为参考面板，不进入模型；不加ROC。
- 新特征来源 `qe_20261023_032933_601a/Loop2`。Loop2/4/6 的本轮切片摘要完全一致；该任务虽在训练前失败，已有原始因子面板的身份和内容独立核验通过。
- 每seed共享资产为655,343行、126个信号日（3月31日～9月30日）。本轮消费3月31日～9月29日的650,123行；9月30日的5,220个信号只保留，不延伸交易到10月。此为时间对齐，不是按风险或结果裁剪人口。
- 旧区间3月31日～8月31日每seed513,782行，用原模型/处理器重算后与原保存预测逐值相同。原标准化fit结束2024-06-28及历史缓存来源局限继续披露；不声称重新验证全部旧因子PIT。
- 两臂每seed读取同一CAS预测文件。三个seed文件SHA依次为：
  - 42：`7b721eea9991d03b565074d6a19a00c3a8ee1fa786aab133bc2f3f366a1bccb0`
  - 2024：`659d86d499e45d937379145a790f1c21d7793fd114a1741c1d9039a95390b462`
  - 2026：`37a63a55f2a20d8dd044ef86fd5ee3b3988e39d1bc0f28f43bb7d9275c3e3785`

### D2：C-013、PIT与信号时序

唯一时间联结为：执行日`t`使用`AF[prev_open(t)]`与`HMM[trade_date=t, as_of_date=prev_open(t)]`，股票行业也在`prev_open(t)`解析。不再对HMM额外shift；不使用t收盘后才产生的AF分数。

首日：3月31日AF及行业映射 + 4月1日HMM → 4月1日分钟执行。末日：9月29日AF及行业映射 + 9月30日HMM → 9月30日分钟执行及收盘估值。

权威spec原文件SHA=`bf434c6e66f99b53b65373daac84ea88ee29a0c8794854bf096cd628aadb06a2`；canonical SHA=`344dddef0d729ff231d697eb70d0d362195c3e2c2b30597c51f4e5d182b00e4c`。必须完整绑定：

- base bundle=`594b21ce0a4f7fe30dc3db58d26b83170ff30e9eb0d7d10f559dbaa56f4a2f86`；
- research basis=`historical_replay / stable_taxonomy_backcast`；contract=`fc55cd0de171b5552bc33195762ed279560b567a3b45910b4e15979aacd9876c`；
- derived candidate=`a228526856d23297edc2c1ed481abad16dd29ebfc418f7db288211d08a7e8472`；derived receipt=`b57943e5be5171ae218f907431ae77ae58705a38653ed7898f4334f5659cdc19`；
- 官方131行业文本投影=`b5d11bea282a41c87b288bf494255b3588780a5d8b49730452ce55b40a9de45e`；不把整数ID当数组位置；
- PIT membership=`bdfde6f667558f6ac368ee03b4d5e9c5b76b83ed621d87ca71021f0b8edd3347`，key=`aistock_equity_pit_canonical_v2`，rule=`shsz_a_252td_st_delist_asof_v2`。

已有全人口映射SHA=`231127acaf9f4f46e59842f013a7ec1649c5deb93049018fe7f644c7f6fffef1`，655,343行完整保留；617,712行resolved、37,631行正式`classification:classification_authority_unavailable`。125日消费人口中信号日PIT内612,310行全部resolved。41行在执行日刚进入资格、但信号日不在资格且行业不可用，不能改用执行日/最新分类补齐；按同一既有PIT路径及D4处理，必须单独对账。

两臂统一使用既有run-scoped预测池过滤和ST/停牌政策；不因L2可用性改变共同PIT集合。映射包含未最终入选的全部股票，不只映射Top50或成交股票。

### D3：评分、买入、持仓、权重与现金

本轮唯一新消费版本：`qe_l2_risk_entry_gate_v1`。基础评分、排序、softmax、Top50、动态换仓、基准卖出、已有持仓维护均由原AF策略决定。

先在各臂自己的当日真实持仓/现金状态上运行同一AF策略和既有风险过滤，得到基准计划订单`O_AF(t)`；L臂仅对其中每一笔BUY进行以下处理：

```text
allow(s,t) = mapping_resolved(s,prev_open(t))
             AND risk_availability(sector(s),t) == available
             AND probability(sector(s),t) < 0.20

BUY_quantity_L(s,t) = BUY_quantity_AF(s,t) if allow(s,t) else 0
SELL_orders_L(t) = SELL_orders_AF(t)
score_L(s,t) = score_AF(s,t)
```

- `p=0.20`即拦截；下一日`p<0.20`时恢复原AF正常买入资格，不追补昨日错过数量。没有二日确认、迟滞、额外阈值、最短政策持有或Top分位配额。
- 不通过删评分行触发幽灵持仓卖出；不因L2报警强制减仓或清仓。原AF/ST等政策应卖的订单仍执行，原hold_thresh、停牌、涨跌停及T+1限制不变。
- 对未拦截买单保留原离散数量、基准目标权重与价格限制；不按`1-p`缩权，不对剩余买单重归一化。不因空缺补买排名外股票，不扩大其他订单或切换TAIL_SUBSTITUTE。
- 被拦截的计划买入预算留在真实账户现金；实际现金还受真实卖出成交、交易费用和未成交影响，不能把计划预算当成交现金流。现金参考收益为0，无杠杆、利息或新增资产。
- 原策略当前不为普通留仓主动补仓；若其产生BUY，统一按上式控制该新增数量，已有数量不因L2改变。后续两臂持仓不同导致AF产生不同订单属于政策传导，不强行共享另一臂订单。
- 初始两臂均为空仓、现金100,000,000元；4月1日起独立核算。保留max_weight=5%、总目标95%、单笔新增买单金额上限5,000,000元、100股交易单位等原配置。不能拿原长历史AF净值直接作为本窗口B臂。

### D4：缺失、不可用与不合法输入

- 正式合法`availability=unavailable`或C-013正式`classification_authority_unavailable`：保留null概率和原reason；对新增BUY采取`BLOCK_NEW_BUY_INPUT_UNAVAILABLE`，已有持仓仍执行原AF维护。该显式政策不是“概率0/neutral/无风险”。
- 未知状态、未知行业代码、重复键、无依据缺行、身份/hash漂移、非法概率、时序错误：typed fail，停止该成对实验的经济结论，不回退旧HMM或其他分类。不能把未知错误转为上述合法NA。
- 原因账必须区分`RISK_WARNING_BLOCK`、`INPUT_UNAVAILABLE_BLOCK`、原PIT排除、ST/停牌/价格限制和执行未成交。完整131目录每日校验；允许合法自然NA，不新增“131全部available”的门槛。
- raw AF 全人口及合法NA行保留，既有PIT资格过滤对两臂相同；不按模型非空、risk warning或未来收益裁剪比较人口。

### D5：125日执行与9月30日估值边界

真实日线日历止于9月30日、分钟止于15:00。当前`TradeCalendarManager.get_step_time`访问`calendar[i+1]`在最后一根越界。修复仅用于本研究两臂的显式`qe_bounded_terminal_interval_v1`执行器：

- 普通interval与历史shift调用原实现；仅当索引等于实际calendar最后一根、且该根是本合同最终执行日，使用已知bar频率定义闭区间右端。
- 日频末根：`2026-09-30 00:00:00..23:59:59`；分钟末根：`2026-09-30 15:00:00..15:00:59`。这是既有末根的区间边界，不添加10月交易日、占位行情或未来价格。
- 实现为独立calendar子类及Nested/Simulator executor包装，在各实例的level infrastructure中替换calendar实例；继承Qlib reset/locate、账户及撮合逻辑，不修改Qlib安装文件、不全局patch常驻API、不改dataset calendar或future calendar。
- 禁止lookahead查询逃逸9月30日；超出边界、未知频率、内部日期空洞继续失败，不能用末根规则掩盖普通数据缺口。
- 日内所有订单与分钟执行按原TWAP+TAIL_BOOST；末日照常允许买卖。收盘记录125个日账本，使用9月30日真实收盘价/因子估值，保留剩余持仓；不强制清仓，不凭空增加平仓费。最终净资产=现金+真实持仓市值，费用按实际成交计入。
- 最后一期从前一收盘到账面最终收盘，首期以初始100,000,000元为分母，不丢弃首日收益。没有成熟标签的观察标为NA，不据此删净值日。
- 原只读探针中，日/分钟末根均复现IndexError；新规则各499个非末根interval与原实现一致，shift=1保持；末日PIT内4,913只均有日close、factor和分钟close。后续六账户真实执行、估值和费用收据补全了业务验收，本次重新核对其原件哈希。

### D6：compose、部署与实验执行

- 独立可选配置：`qe_l2_risk_enabled=false`默认；启用时必须提供contract version、原始预测SHA、映射SHA、权威spec/receipt SHA和共享AF预测SHA。无配置不进入新路径。
- 采用fresh-process研究CLI调用现有ConfigComposer、QE工作区传输/运行接口和统一pred-backtest能力；显式打包新wrapper、executor和已审核的runtime依赖。当前常驻后端是否已加载新源码不作为隐含前提；新CLI必须给出实际源码SHA和compose产物SHA。
- CLI/统一传输复用现有任务与archive合同，不直接操作数据库状态、不另建平台。若既有接口不能无重启携带新包，报告精确阻塞，不能改用服务重启或伪装原默认策略。
- 两臂config差异白名单只允许L2 enable/policy及各自artifact/run id；terminal executor和全部数据/预测/费用/策略参数相同。开启其他overlay或旧HMM为配置错误。
- dataset的推断/test取数区间必须包含3月31日，portfolio backtest另显式设置4月1日开始；不能让默认test_start连带删掉首个AF信号。两臂沿用原始label索引实施PIT约束，但不以label值是否非空筛行；9月未成熟h20标签只影响信号诊断，不删交易或估值日。起跑前核对该索引过滤与冻结PIT的实际执行人口，分别记录raw/executable预测摘要。
- 起跑前再次读回原始预测、映射、AF预测、PIT、数据组件与本研究runtime hash。CAS缺失或冲突停止。完整compose必须产生正确独立类路径，不能仅把JSON附在工作区而称消费成功。
- 同一研究请求保存提交intent及任务/loop映射，最多提交一次；传输结果不明先查任务，不盲目重发。每seed两个独立组合账户，固定顺序B后L，共6正式回测。无新fit、阈值搜索、旧HMM对照或第三经济臂。

### D7：验收、经济分析与停止条件

- 工程：125个净值日、全部映射/NA保留、读入文件hash一致、默认关闭等价、订单/资金约束、远端实际消费动作与原因账、Archive 6/6分别验收。历史自动入仓未全程成功，登记修复和冻结证据补齐已在后续授权中完成；不得把最终6/6描述为全部原生自动入仓成功。
- 经济：逐seed及等权汇总成本后净收益差、最大回撤、波动、换手/成交费用、现金暴露、持仓数、新买入拦截比例；全窗、4～6月、7～9月及逐月固定分段。报告每seed结果，不能挑最好seed。
- 固定执行费用来自原实际conf：买入`0.000095`、卖出`0.000595`、每单最低5元；原分钟close撮合、limit_up/limit_down、100股单位、TWAP+TAIL_BOOST(trigger=235, backup_depth=15)保持。benchmark原为null，不静默增加一个基准改变主评价。
- 解释：区分减少新入场风险、额外现金、错失上涨与交易费用变化。取消订单不是实际成交；没有实际订单证据不能声称精确避免了某笔损失。两臂不能独立识别现金敞口与择时的全部贡献，明确该限制。
- 4～8月历史已消费且分类为回溯研究；不能称严格历史as-known、未消费holdout或实盘可用。文件/工程通过不等于收益已证实。
- 6次完成、分析及入仓后停止。结果不佳不扩公式/阈值/窗口；技术失败保留原尝试和证据，不自动重复提交或恢复旧主线M/R/F。

## 5. Implementation Plan / 实施方案与允许文件

原设计审批前仅新增设计。独立准备脚本及回执位于原窗口忽略目录`F:/Dev/AIstock/tmp/handoff/qe-l2-risk-consumer-20261023/`，2026-10-25复核证据位于`F:/Dev/AIstock/tmp/handoff/qe-l2-consumer-closure-20261025/`；这些证据脚本不作为生产入口。

批准后允许的生产/测试文件清单：

1. `backend/services/quantevolver/l2_risk_consumer.py`：纯读取、身份/日期/完整性、manifest与compose合同。
2. `backend/services/quantevolver/config_composer.py`：仅独立可选打包/路由；默认路径不变。
3. `scripts/qe_l2_risk_consumer.py`：worker独立reader、reason ledger与策略包装。
4. `scripts/qe_terminal_interval_executor.py`：实例级calendar/Nested/Simulator包装。
5. `scripts/run_qe_l2_risk_controlled_study.py`：fresh compose、既有传输/运行/归档、提交去重和两臂manifest。
6. `backend/tests/quantevolver/test_qe_l2_risk_consumer.py`：schema/identity/time/NA/config直接合同测试。
7. `backend/tests/unified_engine/test_qe_l2_risk_strategy.py`：默认等价、买卖数量、现金与原因账不变量。
8. `backend/tests/unified_engine/test_qe_terminal_interval_executor.py`：末根、ordinary interval、shift、越界及真实字段只读smoke。
9. 本设计。
10. `tests/aistock_validation/catalog/file_ownership.yaml`：将三个新增研究脚本及直接测试归入现有QE core验证流程；F2检查发现其原先未映射。
11. `noxfile.py`：现有`qe_read_backend`增加本功能的三份直接合同测试；不改变服务、策略或数据配置。真实Qlib订单/边界测试还须消费远端worker验证回执，不能把Windows无Qlib的skip当成业务通过。
12. `scripts/qrun_limit_minute.py`：分钟校验路由显式识别本研究独立executor的精确类名与模块；原先只识别`NestedExecutor`字符串导致新包装未进入两项校验。只修识别，不改变交易、旧路径或冻结的已执行运行包。

如既有统一运行接口需要其他文件，先把精确文件及原因补入范围，不能跨到HMM、数据生产、常驻服务控制或默认策略改造。实施依次为D1/D2 reader与compose、D3/D4策略、D5边界、最小合同验证、fresh worker运行包、D6一次两臂研究、D7分析入仓。

## 6. Verification Plan / 验证方案

直接验证包括：关闭新consumer的完整订单/数量/顺序等价；p=.20边界；合法NA与未知缺失区分；负score保持原值；被取消BUY不改变SELL/其他BUY、不触发ghost sell、不再分配现金；两臂同初始状态/费用/预测；首次恢复不追补；131行业完整性及重复/hash反例；C-013回溯模式和信号日PIT；末根实例重置、日/分钟最后区间、shift=1、越界拒绝、末日真实close估值与125日账本。

只跑相关小矩阵、changed-file lint/compile及diff/scope。广域QE回归交既有CI/Validation Center，消费简要回执，不在本窗口重复全模块测试。正式实验物理覆盖和结算由原六账户真实产物及本次哈希复核证明；本次新源码通过 fresh-process 消费及原买单逐项等价验证，不冒充重新回测。

设计形式检查：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_l2_risk_optional_consumer_f2_20261023.md --tier F2`。设计阶段不把尚未实施矩阵伪改为实现PASS。

## 7. Design Acceptance Index / 设计验收索引

- F-001 / D1：同身份基础预测与零fit冻结。
- F-002 / D2：完整C-013/PIT人口和唯一时序。
- F-003 / D3：新BUY拦截与不变的评分、持仓、权重和现金语义。
- F-004 / D4：合法NA、非法输入及原因账。
- F-005 / D5：125日执行、末日估值与实例级边界。
- F-006 / D6：独立可选compose、远端hash、一次提交与默认隔离。
- F-007 / D7：真实订单净值、两臂分析、完整入仓与停止。

## 8. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/qe_l2_risk_consumer.py；scripts/run_qe_l2_risk_controlled_study.py | artifact: F:/Dev/AIstock/tmp/handoff/qe-l2-consumer-closure-20261025/live_audit_v2.json | PASS | - |
| F-002 | scripts/qe_l2_risk_consumer.py | artifact: F:/Dev/AIstock/tmp/handoff/qe-l2-consumer-closure-20261025/fresh_validation_3adf9ae2001e.json | PASS | - |
| F-003 | scripts/qe_l2_risk_consumer.py | backend/tests/unified_engine/test_qe_l2_risk_strategy.py；artifact: F:/Dev/AIstock/tmp/handoff/qe-l2-consumer-closure-20261025/fresh_validation_3adf9ae2001e.json | PASS | - |
| F-004 | scripts/qe_l2_risk_consumer.py | backend/tests/quantevolver/test_qe_l2_risk_consumer.py；backend/tests/unified_engine/test_qe_l2_risk_strategy.py | PASS | - |
| F-005 | scripts/qe_terminal_interval_executor.py | backend/tests/unified_engine/test_qe_terminal_interval_executor.py；artifact: F:/Dev/AIstock/tmp/handoff/qe-l2-consumer-closure-20261025/live_audit_v2.json | PASS | - |
| F-006 | backend/services/quantevolver/l2_risk_consumer.py；scripts/qrun_limit_minute.py | artifact: F:/Dev/AIstock/tmp/handoff/qe-l2-consumer-closure-20261025/default_off_equivalence.json；artifact: F:/Dev/AIstock/tmp/handoff/qe-l2-consumer-closure-20261025/fresh_validation_3adf9ae2001e.json | PASS | - |
| F-007 | scripts/run_qe_l2_risk_controlled_study.py；既有QE Archive与冻结结果 | artifact: F:/Dev/AIstock/tmp/handoff/qe-l2-consumer-closure-20261025/live_audit_v2.json；artifact: F:/Dev/AIstock/tmp/handoff/qe-mainline-continuation-20261023/l2-final-analysis/economic_analysis.json | PASS | - |

## 9. Rollout / Rollback / 发布与回退

批准后先完成隔离源码与最小验证，再通过现有流程提交研究运行包；需要PR/合入时独立报告实际状态。fresh worker显式使用新包；不要求常驻API重启，不把源码文件存在称API已加载。默认关闭和旧路径保留，回退为不选择新consumer；保留本次资产和所有失败尝试，不删除或覆盖历史。

## 10. Risks / 风险与限制

新买入门可能增加现金并错过上涨，对已有高风险持仓没有主动退出作用；这些都是本合同的明确适用范围。当前全窗不独立于既有研究，回溯分类不满足严格as-known。共享旧AF vintage不会回答滚动重训价值。原processor fit到valid末日的惯例保持，不将复现等同无泄漏新证明。数据修复若改变绑定，应停止核对，不能静默替换。末根实际账户/价格已有六账户收据并复核；Archive 内部 manifest_verification_status 仍为 not_verified，独立 worker 哈希校验与该字段分开披露。

## 11. Production Gates / 生产门禁与审批依据

当前：production_ddl_gate=noop；direct_production_dml=noop；dependency_gates=noop；dataset_write=false；active_profile_write=false；service_control=false；HMM_fits=0；AF_model_fits=0；processor_fits=0；本次 QE_backtests=0，原授权总计6次且全部完成。

审批依据为用户已批准完整D1～D7。原六次实验及历史登记/入仓修复的授权和回执保留在原目录；本次只读复核，数据库会话显式 readonly，不重写状态，不消耗新预算。最新用户禁止生产库写入和服务控制，本次严格遵守。

DESIGN-COMPLIANCE-001：F-001～F-007逐项有真实输入、订单、账户、入仓证据；默认隔离有当前 main 同进程环境下42文件和命令等价证明。功能验证通过不等于经济增益成立，也不代表常驻API部署完成。

## 12. 多轮审核与收口（2026-10-25）

- 第一轮核对批准与身份：原审批 SHA=`7417e2630968e99f0d1a22f713d816b51b394dd26b3ef27d1fe864da58e0952c`；预测SHA、冻结522 release、C-013/PIT、三seed共享AF均未变。原后四账户已在后续授权中完成，六账户入仓完成。此前停止与修复回执完整保留，不重投。
- 第一轮源码审核发现 compose 未比较实际数据绑定与风险 manifest；新增全绑定比较（只排除按请求窗口变化的 coverage receipt），并比较实际 conf provider/股票池。consumer 校验物理release manifest、日/分钟 calendar、meta_export和PIT文件，拒绝静默使用 active 的另一版本。
- 第二轮审核补上实际回放预测文件 SHA 校验。preflight 在 qrun 前校验 conf 与冻结预测，原料CAS正确不再足以掩盖本地回放副本被替换。风险资产明确限定为本批准 SHA；买单原因账新增原始 warning，合法NA仍为null。即使当日没有BUY，也核验decision/as_of时序。
- 修复不改变评分、阈值、买卖、数量或现金公式。远端fresh Python读取三seed完整655,343行AF及16,375行风险；新consumer重判原三seed共3,782笔计划BUY，其allow/reason/数量与旧账全部相同。原实验资产、代码与结果未覆盖。
- 本地15项直接测试通过；Windows缺Qlib的两模块跳过，远端同一真实Qlib环境5项测试全部通过且真实策略类解析成功。默认关闭当前main基线42文件及命令逐字节相同。三seed两臂在当前源码下重新compose，差异白名单通过；三份实际CLI preflight在新进程通过，最新回执为`fresh_validation_098b1acb2fe1.json`。期间验证脚本误将登记元数据传入策略导致一次compose失败，已按正式CLI的先compose后登记顺序修正；没有派发或产生实验记录。
- 六个原账户均为125日，含9月30日执行及收盘估值；report、positions、L2 ledger、分钟成交费用指标原件哈希实时复核一致。两臂共享预测，9月30日信号保留但不执行到10月，未成熟标签不补零。
- 原日表cost/turnover为0是内层 generate_portfolio_metrics=false 的既有记录局限。采用保存的分钟真实trade_cost并逐日现金核对，净值已扣费；不得再次扣费。本次 live_audit_v2 修正了第一份审计引用旧日表费用字段的问题，旧审计保留并明确被替代。
- 真实经济结果：seed42 B=-1.141738%、L=-1.489971%；seed2024 B=+2.152246%、L=-1.160929%；seed2026 B=+0.212989%、L=+0.943608%。仅1/3 seed净收益改善，均值增量约-0.97693个百分点。不能宣称收益增益已证实，不调参、晋升或自动追加实验。
- 代码交付走独立feature PR，未合入/部署阶段另列状态。当前API runtime merge_commit=`35d5fd356b1c995e341927ebf48205eca74665fb`，本分支不在其中；用户禁止服务控制，本次没有重启。fresh worker包验证无需API重启，但不冒充常驻API接入已激活。
