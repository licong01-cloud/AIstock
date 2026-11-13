# LocalSIM 原生执行与模拟人工操作：实现与验收

日期：2026-10-27。F2；权威为[双模式详细设计](../architecture/localsim_human_operation_dual_mode_f2_detailed_design_20261026.md)的F-149至F-164及[父蓝图](../architecture/simulation_platform_unified_authoritative_blueprint_20260715.md)。

## 交付边界与状态

本任务实现已批准设计的离线双模式、自动账户验证、原生事务恢复及产品读取面。旧TWAP默认、已有release、现有生产账户、回放与实时模拟的入口保持原合同。新研究是独立文件账户，不创建生产账户，不把已有账户改成人工模式。

| 状态 | 当前事实 |
|---|---|
| 功能开发 | 队列、延迟提交、原生TWAP对照、2/13账户矩阵、日恢复、只读API与UI均已实现 |
| 功能验证 | 最终小矩阵246项通过；此后共享源码升级防护所属5项、五类读取所属2项及更新后的UI专项再次通过；重叠执行不相加 |
| 真实历史 | 2026-01-05/06、13账户、26账户日完成；先13后接续13，前缀26个收据/摘要文件完全不变；exact retry零重算、结果SHA相同 |
| 源码/CI/合入 | 本报告记录提交前验收。最终PR、当前HEAD CI与merge状态以同任务外部`source-merge-receipt.json`及GitHub PR为准，不能从本地测试推断已合入 |
| 运行加载 | 待用户重启后端并核验；没有操作用户服务进程 |
| 在线人工模式 | 属设计§10第五阶段；本任务没有将生产profile/release或scheduler改成人工模式 |
| 经济支持 | 未成立；短窗口与功能合同不能证明稳定超额收益，181日新人工矩阵尚未完成 |

## 实现与兼容证据

- `human_operation.py`：完整不可变操作政策、显式模式/算法兼容、旧schema只读解释、账户级单人队列、ready排序、拒绝消耗额度、机会去重、哈希状态恢复与恢复时钟/容量负例。
- `human_portfolio.py`：只通过既有TimingFacade调用唯一`position_timing.intraday.evaluate`；机会先排队，实际派发时重新核验，随后才创建OMS父单。0/60/180/300秒共用60秒动作间隔，首轮不主动分批、不追价。
- `timing.py`和`runtime.py`：可选的真实提交时刻与facade接缝；旧调用不传新参数时保持原订单创建与成交合同。新增研究入口要求显式冻结文件provider，普通文件/数据库/实时路径的选择不变。
- `native_research.py`：对照使用原生TWAP的实际引擎与配置；共享事务/反馈框架，没有复制TWAP、撮合、费用或账本。强制择时evaluate抛错时，原生TWAP仍产生3次真实子成交、1000股、5元父单累计费及精确现金/净值。
- `dual_mode_research.py`：来源和政策先整批inspect；独立纯现金1000万元，FULL_13或BASIC_2；沿用原TargetPositionEngine/RebalanceIntentService、原生经济协调器和日journal。单writer、失败前缀、提交前/后账本回读、exact retry与日摘要恢复。
- `dual_mode_projection.py`与router：三个只读GET；校验请求、结果、日文件和原生journal身份。分页先用绑定结果的日计数定位，只读取本页涉及的交易日；不触发选股、交易、数据库或行情网络。
- 新比较UI：既有总览中新增入口，左侧导航不变；各账户单独展示资产、现金、持仓资产、累计及最新日盈亏、费用、回撤、完成率和操作负担；账户明细包含持仓、成交、委托、操作及区间盈亏。两模式曲线使用相同日期和资产比例尺。普通页面不显示package/run/release/hash/schema或JSON。
- 新UI计划登记到现有nox、plan_catalog和LocalSIM验证目录；CI选择回执明确包含新spec，新增Python测试没有未执行文件。没有建立第二套验证或调度平台。

证券简称仅从冻结目标/原生projection消费；冻结研究源未提供简称时明确显示“证券简称未提供”，不查询生产DB、不显示机器编码或补造公司名称。原工作台已有证券名称来源和详情读取逻辑未改。

合法无候选且账户纯现金的日也保存原生估值和空交易结果，不造一只证券去驱动行情入口。已有持仓缺估值、普通股票真缺行情或共享核心意外UNAVAILABLE均保持失败；审计已声明的NONE/C1不可用计划由原生计划编译器保留，不能删行或制造价格。

## 输入、源码与不可变身份

实现来源commit：`e95f2b148e3de7450e91289c7a5d487087b7dadf`；可见时钟/空目标日修复：`9cb3ddc3a76908792b22a347968adea650616a14`；现金序次与提交前校验：`2c86fdaccc5ea19e3e49b79a1879c244dedbb5cd`；取消竞态及本次实际经济结果producer：`1f248747f980abe011c15e0a947956eaca8bced0`。

最后审核补齐读取适配、模型族可读别名和共享owner源码冻结范围，没有修改经济计算。`economic-source-equivalence.json`核对52个执行/公共权威文件：共享权威和经济计算不变，`dual_mode_research.py`唯一变更函数是`implementation_identity`；完整收据SHA=`b15d0f4914aeac2542be4a05b0a4d847cee350c3ca8aca017fda27a1f2ea3b9c`。新创建研究会完整绑定择时核心、原生交易、目标规划等源码；仅共享owner升级也不能接续旧前缀。旧完整结果保持producer身份，只读展示允许历史结果；不通过改旧request使新代码冒充原producer。

| 输入 | 完整文件SHA256 |
|---|---|
| 原连续请求 `continuous/92bcea…/continuous_request.json` | `a9ab2c0388702fbe746061b466083706d189dd2f7d4f29a6c4fba6c7a0a14668` |
| 正式 `input_successor.json` | `00e95019b12fcd11dd6098486658b08376292acd4bef7dd9e795543103c366df` |
| 显式修复candidate manifest | `3d416575b4dbb3cdf6d17552908c38fcc03c96f4fe4b12fa30c6ee4d1db6a36a` |
| 原preflight（历史阻断已由正式价格模板及后继合同闭合） | `c7270eabf6830df970d4207d25bae9661e5fab464a0e70f5a65cba78f6cc8ff7` |
| 共享价格模板 | `bce2563f61ee2c3ee64b74d723dfac57baa95e127ed87e137229e86d16b327a1` |
| 费用文件 | `d9f8c97ea4593206b0509fcfeb5046a20925c84c618a9a3d3061f7945d9f3041` |
| 策略包manifest canonical身份 | `5de2ec91c088e5cf9a9cc25673498a4165857b761cb9e4a4456151a1e04f0dc9` |

原连续请求绝对路径：`X:/AIstock_temp/localsim-position-timing/continuous/92bcea72a816607b478c23324b9578ad04b96bd7303a1af95a9f215cbb36e1d3/continuous_request.json`。输入后继同目录。价格模板及candidate的准确路径从冻结FileRef读取，并核验完整文件SHA；不使用active profile，也不复制模型或大型数据。

账户初态为1000万元纯现金、无持仓、无可卖量、无杠杆及追加资金；SELL只能来自此前原生模拟成交并经过T+1。买/卖合计费率0.000095/0.000595，最低5元，父订单累计计费。共同终点14:57，MDD为正的peak-to-trough fraction。行情可见性为明确假设的bar end+5秒；人工成交必须strict-after真实派发时刻。

冻结原始外生目标仍为181日、9050个；首个工程窗口仅消费其年初连续两日，不据此宣称完成181日经济研究。模型为之后形成的冻结资产，本轮属于回顾性研究；没有重新训练，也没有新增样本外有效性声明。原生TWAP与人工next-open代理不同，主比较为整流程；人工同delay规则比较单独列出。条件成交价差只在双方成交计划交集上按计划等权报告，附配对数及缺失比例；连续反馈下的BUY/SELL净值与MDD归因保持null。

本次candidate实际绑定`aistock_equity_pit_canonical_v2 / shsz_a_252td_st_delist_asof_v2`，snapshot=`aistock_equity_pit_canonical_v2_32d2c372268841c8`，截止2026-09-30；没有按运行时active切换政策或数据库来源。

原产物实际重新核验未改：288结果完整SHA=`542bf38129337583b306aa1802b6158b1f4dd52b96e864333157cb13d9c10815`；原12账户连续结果完整SHA=`acdea8aba2f7ef3ad4ee26bc3ef4a2b0d389d303b62510b2f0d5d5e351d4f9ba`。旧冻结机会诊断不并入新人工队列的纯延迟效应。

## 多轮审核与修复

| 轮次 | 直接发现与修复 | 验证 |
|---|---|---|
| R1 | 响应/排队后才建OMS、同账户共享动作间隔、过期行保留、原生TWAP复用 | 队列与两股真实账本oracle、旧择时及TWAP回归 |
| R2 | Decimal恢复现金等值、原生journal信封、只读哈希与游标、单writer及失败前缀 | FULL_13两日、暂停/接续、exact retry零重算、摘要恢复、ASGI |
| R3 | 连续修订保留已排队撤单、派发时重验机会、政策bool严格类型及恢复时钟/容量验证 | 多次修订+部分成交+确认剩余量、恢复负例、四档未来信息隔离 |
| R4 | 反馈初始可见时间绑定真实提交、合法无目标纯现金日、内存不累计完整审计明细、分页只读目标日、前端旧请求取消 | 时钟补验46项、空目标日等8项、UI与类型/ESLint |
| R5 | 真实PRICE_TOUCH/D0多个部分成交同分钟，最终现金/持仓相同但cash_after序次不同；统一原生SELL/symbol/order处理顺序，提交日journal前先独立账本回读 | 三股并发部分成交、无效经济日不提交任何generation、修复后22项；R5及R6真实26账户日均通过 |
| R6 | 隐藏的已成交状态不能使操作员免做撤单；部分成交取消竞态也保留真实成交 | 隐藏全成/等待撤单中部分成交/连续修订3项；最终246项回归 |
| R7 | canonical订单经济fact不含created_at；读取绑定父单审计的真实提交时间，并逐单与经济fact核对；补LGBModel可读模型族 | 真实13账户65次五类API读取；新旧UI、类型/ESLint及读取回归通过 |
| R8 | 源码冻结补齐共享择时、原生交易与目标规划权威；不只冻结LocalSIM文件 | 模拟owner-only代码升级，旧请求在source inspect/账户输出前拒绝；所属5项通过；52文件经济源码等价收据 |

第一轮失败工程根：`X:/AIstock_temp/localsim-human-operation-implementation-20261026/studies/68b76f4e2371a3e5c8f555ca547e1c918430364b4d643cce577145f0fe93f46f`。6份journal中第6份回读失败，只有5份通过读回并生成日摘要；没有发布results或成功全区间收益。失败根保留，未改FAILED_TERMINAL、生产断点或账本。修复后的研究使用新源码/request身份及独立`published-studies`根，不把失败旧结果静默升级。

## 验证、性能与复杂度

直接证据均保存在`X:/AIstock_temp/localsim-human-operation-implementation-20261026`。最终矩阵为`pytest-delivery.log`：246 passed，155.83秒；后续`pytest-source-identity-final.log`：5 passed。新增API专项2项及真实文件65次API查询通过；`playwright-r3.log`为完整新旧工作台12项，`playwright-time-read.log`为最后更新的比较UI专项；类型和ESLint通过。`ci-selection-delivery.json`无未执行测试或未映射代码，包含新比较页及原有UI计划。最终L0/ownership/F2回执与source merge receipt同目录。

Playwright完整新旧工作台12项通过，最新增量新页面1项通过；TypeScript全量noEmit及改动页面ESLint通过。L0无阻断；RAW_JSON提示逐项定位为原有sessionStorage、请求/游标序列化或测试mock响应，不是渲染JSON。复杂度提示为既有逐日逐订单编排，未复制评分计算。最终当前HEAD CI另行回填。

本次实测如下；每次执行都包含全181日来源核验，不能解释为每日选股推理耗时。本研究直接消费冻结选股目标，没有重新运行模型推理。

| 阶段 | 耗时 | 峰值RSS |
|---|---|---|
| 最终输入冻结 | 5分16.97秒 | 952416KiB |
| 首13账户日 | 13分04.29秒 | 955596KiB |
| 接续另13账户日 | 12分53.81秒 | 950988KiB |
| exact retry（0重算） | 6分13.48秒 | 954540KiB |

两段执行合计25分58.10秒，含两次整批核验；峰值约0.91GiB/进程。最终研究目录34136296字节，约32.55MiB；没有复制模型或数据。第一轮失败研究及R5验证另行保留，不混入这项占用。测量不是隔离性能基准，部分阶段与上一轮工程验证重叠；不据此外推全年，也不预设每天40秒。

队列使用future/ready/expiry堆，实际入出队为O(log Q)；计划撤销与计数仍扫描该账户当日动作，Q受当日计划及机会数约束。共享核心的可见分钟前缀校验由唯一核心负责，存在逐计划×多轮前缀校验成本，本任务不跨模块改其算法。每天共享只读市场对象，各账户经济状态独立；内存仅保留必要日摘要、持仓、净值与小型计划统计，不累计每个历史日的完整订单/队列审计副本。产品分页只解析本页涉及的日文件和journal。

## 不可变结果与账户窗口

请求：`X:/AIstock_temp/localsim-human-operation-implementation-20261026/delivery-studies/3445fe8f94e8a63b7cd61c6784ae0d08f807e6cbdefedc01acf576b740b7f549/request.json`；canonical identity为目录名，完整文件SHA=`0eebd1084ded5e03696a3ffb066c9c4924129fa6ab4af56e92eb34598b799577`。

结果为同目录`results.json`；schema=`localsim_dual_mode_results_v1`；完整文件SHA=`13f1b569057dedda1f8a9fdbe60d289c3ecaeae60add7e6117b7023ed1102393`。`result_receipt.json`绑定请求和结果；账户日文件再绑定完整原生journal实体。`final-business-readback.json`完整SHA=`07962590ad767c3cac109b8ae4e69107230fd90fe256e197aee7c4d2c1ad29e8`，包含65次只读查询与26个前缀文件不变的证据。

在producer `1f248747f…`上，先`CHECKPOINTED/13`，再同一请求`MATERIALIZED/13`；exact retry=`ALREADY_MATERIALIZED/0`，完整结果SHA不变。`outcomes_read_stage=AFTER_COMPLETE_SOURCE_BATCH_INSPECTION`。最后身份防护升级后，旧请求若以不同源码执行将明确拒绝，历史只读结果仍可读取；不能把源码升级当作相同producer重放。

以下仅为2026-01-05/06工程窗口的真实费用后结果。计划数会随独立持仓反馈分化，不把不同账户的订单母数视为相同；新年度研究尚未完成。

| 模式/规则 | 响应秒 | 窗口净盈亏元 | 费用元 | FILLED / PARTIAL / NO_FILL / UNAVAILABLE |
|---|---:|---:|---:|---|
| 原生TWAP | — | 200016.10 | 3568.20 | 90 / 0 / 0 / 0 |
| 人工简单执行 | 0 | 6967.15 | 164.85 | 4 / 0 / 96 / 0 |
| 人工简单执行 | 60 | 4408.27 | 35.73 | 2 / 0 / 97 / 0 |
| 人工简单执行 | 180 | 0 | 0 | 0 / 0 / 100 / 0 |
| 人工简单执行 | 300 | 0 | 0 | 0 / 0 / 100 / 0 |
| 人工到价执行 | 0 | 14300.81 | 329.53 | 13 / 0 / 83 / 0 |
| 人工到价执行 | 60 | 11721.90 | 163.10 | 9 / 0 / 87 / 0 |
| 人工到价执行 | 180 | 0 | 0 | 0 / 0 / 100 / 0 |
| 人工到价执行 | 300 | 0 | 0 | 0 / 0 / 100 / 0 |
| 人工有限等待 | 0 | 10167.74 | 285.26 | 11 / 1 / 87 / 0 |
| 人工有限等待 | 60 | 2868.12 | 35.88 | 2 / 0 / 97 / 0 |
| 人工有限等待 | 180 | 0 | 0 | 0 / 0 / 100 / 0 |
| 人工有限等待 | 300 | 0 | 0 | 0 / 0 / 100 / 0 |

三/五分钟响应组机会在队列中过期，账户保留现金而未成交；该零盈亏来自实际现金估值，不是用零替代未知。没有延长共享核心机会窗口来提高完成率。所有未完成原因保存在`reason_counts`与原生父单/操作收据；原生全成与人工低完成率包含代理、延迟、操作容量和风险暴露差异，不能归因于纯择时优劣。分侧完成/等待、条件成交配对数及缺失比例、MDD差异在JSON中保留；BUY/SELL净值与MDD归因仍为null。本窗口没有证明稳定超额收益。

## DESIGN-COMPLIANCE-001

| 控制 | 逐项结论 |
|---|---|
| no_simplified_delivery | 源码、真实账户链、恢复、只读API与UI逐项验收；按已批准阶段交付离线能力，不宣称在线生产入口或完整181日研究已经完成 |
| no_silent_error | 意外核心UNAVAILABLE、缺行情/估值、身份或账本冲突报错；没有成功结果时不生成零收益占位；已声明NONE保留准确不可用行 |
| no_business_semantic_drift | 旧默认、TWAP、账户/资金/费用权威与原产物保留；人工模式、派发时钟、队列和新比较身份显式冻结 |
| no_unrequested_gate_or_approval | 全部人工响应由程序仿真，不增逐单人工确认；不重复请求已有合入授权；用户仅承担既有后端重启责任 |

## 运行交接与停止点

`production_ddl_gate=noop`；数据库、数据集、行情抓取、生产账户和生产回放均未操作。没有启动、停止或重启backend/worker/scheduler。

源码合入并同步main后，由用户按现有后端启动方式重启。若要让工作台登记本次离线研究，后端进程环境设置：

```powershell
$env:LOCALSIM_DUAL_MODE_RESEARCH_ROOT='X:\AIstock_temp\localsim-human-operation-implementation-20261026\delivery-studies'
```

重启后最小只读核验：实际加载main的merge身份；比较overview/detail/账户GET读回同一不可变结果和独立资产；旧原生账户列表、持仓/交易/区间盈亏读取正常；新UI不出现机器身份或JSON。不得借重启把旧账户迁移模式或自动启动生产回放。在线人工运行的正式profile/release与持久化仍按设计第五阶段单独验收。
