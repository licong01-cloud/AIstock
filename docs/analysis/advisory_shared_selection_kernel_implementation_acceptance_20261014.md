# 通用选股信号内核：Advisory 源码交付与设计验收

日期：2026-10-14。级别：F2/T3。原交付记录：Advisory 授权范围源码实现、多轮自审修复、真实原生评分对照及最终 F2/静态检查通过；当时待提交、最终 HEAD CI 与合入。BUG-1912（2026-10-15）修订当前 provider 时钟及相关验收；原性能收据不证明新 D eligibility，运行激活与跨 owner 接入另行报告。

## 背景、范围与非目标边界

实施依据为已合入的[完整详细设计 v1.3](../architecture/advisory_shared_selection_signal_kernel_detailed_design_20261014.md)，尤其 §9 的分 owner 交付。用户明确要求本窗口只修改荐股代码，模拟盘必须由专用窗口修改。本 PR 交付 Advisory 归属的通用输入、特征、冻结资源、节点批量/增量、完整评分合同及荐股私有消费；不复制公共基础选股算法，不以此宣称两个业务系统全部接入。

本文件同时是父设计的 F2 实施与验收补充：机器可识别的背景、架构、实施/验证方案、风险、生产状态和验收矩阵均在本文；F-001～F-010 逐项对应父设计 §11，不替换已批准语义。正式检查命令为 `python scripts/aistock_feature_workflow.py validate --design docs/analysis/advisory_shared_selection_kernel_implementation_acceptance_20261014.md --acceptance docs/analysis/advisory_shared_selection_kernel_implementation_acceptance_20261014.md --tier F2`。父设计自身的描述性章节标题不符合此检查器的固定章节别名，本 PR 不跨越登记范围改写父设计；以本文补齐其实施阶段机器检查，而非忽略缺项。

无策略包 ID 白名单；已支持的模型/特征/组合按原生元数据进入同一代码。优化不命中时仍使用原正确公式或 fresh-load，不新增 QE 训练、回测、收益、UI或人工准入条件。不共享两侧已计算结果。

原交付未修改 QE、StrategyPackage、Selection/HMM、模拟盘、数据服务、公共 CI 或 ownership。BUG-1912 按用户本次共享修复授权，仅额外修订原生 `selection_center/tradability.py` 的行业观察日期可选接口及直接测试；不修改 LocalSIM 私有源码、HMM producer、QE、数据服务或 ownership。无新训练、数据库写入、profile 激活、依赖安装和服务控制；后端重启仍由用户执行。

## 架构与接口契约

`SelectionSignalKernel` 只编排注入的原生准备与 `StrategyPackageSelectionComputation.compute`，不实现第二份过滤/融合/TopK。C/D/T 显式、计划不删日；按 [BUG-1912 provider 合同](../contracts/strategy_package_selection_provider_clocks_v1.md)，模型/参考价/risk/PIT 行业观察 C，停牌 eligibility 按冻结政策观察 D，artifact 决策日仍 D。C=D 的候选、排序和政策调用日期不变，新来源字段明确版本化。此前将整个 tradability 绑定 C 的实现已确认与 BUG-1904 冲突，旧 PASS 不作为 D eligibility 的验收证据。返回完整 raw 评分、输入身份、基础有效候选和 native stage。未公布完整成员时标记 `NOT_PUBLISHED`，已公布但不能评分的原成员保留 `UNKNOWN`，不能伪造停牌原因或默认评分。长任务使用 `iter_results` 逐日交给消费者自己的writer，仅保留完成日的轻量摘要，不累积每日全池数组。

节点仍执行 `InferenceEngine`，元数据驱动混合 Alpha158/动态因子、冻结 infer processors 和训练序列。`InferenceResourceSession` 隔离模型及 processor；不支持序列化/deepcopy的资源正常重新加载，不重训。

`InferenceInputSession` 仅缓存同消费者、同 run/revision 的点态只读原始数据；按成员交集复用、补新增成员完整历史、追加尾部或补不同包暖窗的前缀。每次按当前开始日期/截止/股票池重新构造视图、执行原静态 builder；任意递归/横截面代码只见合法单日输入。修订需明确切换 revision 或重建 session；不以持久目录命中冒充数据冻结。

Alpha158 仅对已知且等价的公式优化；未知原支持公式仍正常运行，原公式依赖的首见成员顺序也参与身份。动态因子仅在实际代码、适配代码、完整合法输入及执行环境完全相同时复用；无法绑定的导入、外部IO、时间或随机依赖仅使优化miss，正常执行原代码，不拒绝策略包。特征缓存绑定consumer/run/revision，输入修订即使数值不变也失效，不冒充旧来源。模型与代码命名空间、缓存、结果与恢复状态不跨消费者。

`FactorInputSession` 在本消费者、本run中按完整合法价格/静态输入及表示布局复用物理文件；相同视图只序列化一次，其余模型/因子建立只读hardlink/symlink。默认磁盘保留2GiB/2视图，按准确自有文件清单逐出，不复制输入、不跨run发现。源/alias变更拒绝发布；超预算正常生成独立临时视图后释放，不拒绝策略包。不同C的处理后/qfq视图不能切片复用。同日多模型在节点日期计划中各有 `request_id`、独立全评分文件，共用本run的输入/特征/冻结资源会话；native跨腿/跨包一次性计划组装仍由公共owner接入。

`AdvisoryBatchInferenceProvider` 是荐股私有薄适配：原生 single/multi-alpha 分派不变，每个原生模型腿在同一有界日期预取块只调用一次节点批量 runner；HMM/risk/tradability 和逐日 projector/CAS/名单执行仍按原政策。继承历史准备的 C=D 映射；其他 C/D/T 不能假装已由该 native 接口支持。通用节点日期计划允许显式 C/D/T。

例外不是换算法：若native manifest真的提供按日绑定的完整冻结成员，仅计算该workspace自己的日期，输出标记 `DATE_BOUND_FROZEN_PIT`；外层所有日期仍逐一消费自己的native workspace。不得拿D1清单算D2，也不伪造缺失PIT。节点计划已支持每项显式pit_identity，公共owner收集逐日身份后可接成一次多日计划；本PR不以按日固定清单限制宣称所有池已达相同批量性能。

原 public `load_daily_pv` 历史调用的复权归一化读取截至 today；本侧调用局部读适配严格限制截至 C，不修改 data_service globals。实际对照使用相同合法 C 视图；不得把旧含未来归一化的输入哈希直接当作本次等价基准。公共 loader 的修订责任交给对应 owner。

### 其他消费者的精确接入点（不代实施）

1. 计算使用相同 `InferenceEngine` / 节点CLI和 `SelectionSignalKernel`，不得复制Alpha/动态特征、processor/序列或native compute算法。每个调用显式创建自己的consumer/run/input/feature/resource/materialization会话，并在本run结束关闭；未知可用合同保持正常重算。
2. 公共owner负责用现有资产解析器收集实际模型腿/seed的冻结workspace，构造节点日期计划；同日多腿各给request_id，再委托原生融合/权重政策。股票池/PIT来自该包原元数据，不硬编码两包或Alpha158数量。当前native历史准备仍C=D；其他映射的prepare adapter必须把模型/参考价及 HMM 输入绑定 C，保留 HMM 原生标签/生效日；共享内核另将 risk/PIT 行业绑定 C、停牌 eligibility 绑定 D，不能仅修改raw结果日期或统一广播截止。
3. LocalSIM owner负责将自己计算的完整评分、原输入/人口及真实stage/source身份映射到原生产物，再按自己账户的事务/checkpoint顺序推进。不得查询或复制Advisory已完成结果，不以节点JSON等同账户发布。Advisory projector、价格模型和episode不进入这个共享基础计算入口。

## 实施方案与验证方案

1. 复用原性能分支的必要语义，再实现私有输入/特征会话、混合冻结资源和完整输出合同。
2. 接通荐股原生有界预取，保持公共政策、私有逐日投影和 writer；登记新 helper 到实际执行 code closure。
3. 多轮检查 PIT、池变化/修订、未知递归因子、线程/同名代码隔离、精确恢复、错误及资源边界；只重跑失败点，稳定后一次最终直接矩阵。
4. 两个真实包按相同全股票池、合法输入、冻结模型/processor/序列对照旧正确计算公式与优化公式。两个臂均采用同一个安全物理表示/链接构建，reference不执行旧六份HDF重复写入，不把这部分重复I/O计入加速收益；reference仍不复用输入/特征/冻结模板。输入来源必须相同；分报冷/热 wall time、全评分和输入身份、峰值内存、缓存读次数。SQL 次数未测保持未知。
5. F2 验收、静态 L0/测试可达性及最终 HEAD 必需 CI 通过后按已有授权合入。公共消费者回归依赖既有 CI/owner；不另加 UI 前置。

### 风险与失败模式

- 新日期缓冲不能穿透 C；qfq/横截面/填充不能截取晚日处理后结果。EWM 不用近似滚动值替代原移动起点。
- 缓存上限约束 retained bytes，不声称等于进程总 RSS；工作缓冲、模型 clone、库与 GPU 单列。
- 中断保存完成前缀并报告真实失败。OS 文件 lease 防并发写入；本 run 的代码/资产/输入声明及每日内容哈希绑定恢复，产物发布但 checkpoint 未推进也必须核对同一来源。
- `input_revision` 是调用方的真实版本合同，不是额外批准 token；普通新任务不要求它。跨进程 exact-resume 不可用随意字符串冒充已冻结 DB，只能在上游冻结身份保证下使用。未证明冻结时应新 run/重算，不能复用历史成功文件。
- 公共股票池/资产/接口真的不兼容时明确错误/owner 缺口，不改配置、删日/删股或假成功。
- 两包短窗口结果不是半年/全年吞吐，也不是收益/OOS/实盘激活结论。

## Design Acceptance Index

沿用父设计 F-001～F-010，不重新定义、删减设计条款。各行在下表记录本 owner 的精确交付与全系统仍需对应 owner 完成的边界。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | selection_signal_kernel.py、candidate_producer.py；唯一原生 compute | backend/tests/advisory_historical_range/test_selection_signal_kernel.py，R11 | PASS | - |
| F-002 | inference_engine.py、alpha158_batch.py、batch_inference_provider.py；无包ID条件 | backend/tests/advisory_historical_range/test_alpha158_batch_parity.py、test_batch_inference_provider.py，R11/R13 | PASS | - |
| F-003 | inference_session.py；冻结模型/processor、实际 step_len、原生单多腿分派 | backend/tests/advisory_historical_range/test_inference_cache_lifecycle.py；R24 LSTM/TCN 原生全评分、输入与源码 EXACT_PASS，实际 step_len=20 | PASS | - |
| F-004 | 公共准备/compute 委托与私有 projector 分离；显式 read-only providers | backend/tests/advisory_historical_range/test_selection_signal_kernel.py、backend/tests/advisory_historical_range/test_r5_composition.py::test_explicit_mutation_factory_composes_every_r1_r4_boundary，R11 | PASS | - |
| F-005 | inference_inputs.py、selection_signal_kernel.py；合法视图、人口/UNKNOWN | backend/tests/advisory_historical_range/test_inference_incremental_parity.py、backend/tests/advisory_historical_range/test_strategy_package_live_inference.py，R11及复权C测试 | PASS | - |
| F-006 | 每消费者/run 独立 input/feature/resource/输出与 OS lease | backend/tests/advisory_historical_range/test_selection_signal_kernel.py、backend/tests/advisory_historical_range/test_batch_inference_provider.py，R11/R13；节点独立Python3.10导入通过 | PASS | 用户明确批准的 owner 边界：LocalSIM 原生产物和账户接入由专用窗口交付；本PR不宣称完整双端验收。父设计§9。 |
| F-007 | 同一个 engine、batch adapter、原生 executor、revision/pool/warm-up 修订 | backend/tests/advisory_historical_range/test_inference_incremental_parity.py、test_r3_executor.py；R24 LSTM 两日首读/增量完整评分等价 | PASS | - |
| F-008 | factor_input_materialization.py、inference_session.py、performance_probe.py；只读大资产/X临时/有界缓存 | backend/tests/advisory_historical_range/test_inference_partition_reuse.py；R24 全池对照及真实峰值/读行数/物理表示实测，见下表的性能结论边界 | PASS | - |
| F-009 | 本文精确范围、composition.py code closure；其他 owner 源码 untouched | artifact: X:/AIstock_temp/shared-selection-ownership-r22.json；24/24 mapped、0 unmapped/ambiguous；artifact: X:/AIstock_temp/shared-selection-l0-final.json；artifact: X:/AIstock_temp/shared-selection-quality-final.json；0finding，CI可达性通过 | PASS | 用户明确批准分owner交付；StrategyPackage/Selection/LocalSIM适配由所属窗口实施，不越权修改。 |
| F-010 | 原 private writer/CAS 保留；node 原子发布/精确恢复/typed部分失败 | backend/tests/advisory_historical_range/test_strategy_package_live_inference.py、backend/tests/advisory_historical_range/test_r3_executor.py，R11/R13 | PASS | - |

## R24 稳定源码真实对照

两包按顺序执行；均未训练、读取收益、写数据库或控制其他任务。每个日点原始人口 4,913，真实可评分 4,907；未评分原成员保留 UNKNOWN。两个臂使用相同合法 C 视图、冻结模型/processor/实际 20 步序列及同一个无重复大文件的物理输入布局。完整评分、输入身份和 11 个实际源码文件的前后绑定均 EXACT_PASS。日期为 D=2026-01-05/C=2025-12-31；LSTM 另含 D=2026-01-06/C=2026-01-05。

| 实际策略包/样本 | 优化节点 wall time | 正确 reference 节点 wall time | 实测降幅 | 源码与全评分 |
|---|---:|---:|---:|---|
| LSTM `pkg_cc7eccb6202e4816b9d7c7d6b748bd37`，两日 | 534.484 秒 | 2,239.976 秒 | 76.14% | EXACT_PASS |
| TCN `pkg_8dbd396d27184de6aa5fe288011220f6`，一日 | 308.762 秒 | 1,151.951 秒 | 73.20% | EXACT_PASS |

证据为本机 `X:/AIstock_temp/shared-selection-kernel-20261014/lstm-final-r24/progress.json` 与 `tcn-final-r24/progress.json` 及各臂逐日输出；不作为跨机器 CI 自动可访问的固定路径。源码绑定为实测文件字节；Git 换行归一化后的提交身份与后端重启后的实际运行身份须分别核对，不能重新伪绑旧收据。

具体收益与限制：

- LSTM 核心计算首日/次日 305.000/224.989 秒，reference 为 1,111.080/1,125.602 秒。Alpha158 首日/次日约 28.16/27.25 秒，对照约 855.46/863.45 秒；TCN Alpha158 约 28.97 秒，对照约 878.48 秒。动态因子阶段没有实测加速，不能将其列为本次收益来源。
- LSTM 两日累计原始读行数 10,697,697→10,738,850，对照 13,485,408→26,969,809；次日冻结资源新增加载 0、命中 2。真实不同日特征视图命中为 0，不宣称已有跨包特征缓存收益。raw loader calls 优化臂 8→24、对照 10→20，不等同 SQL 次数，也不能宣称所有读取调用减少。
- 每日只生成 1 个静态 HDF 及 6 个只读链接；一套物理表示约 1.55 GiB，两个臂均无六份重复 HDF。原大模型/数据资产只读链接，不复制；临时输出全在 X 盘。
- 进程累计峰值内存 LSTM 优化约 8.17→9.43 GiB、对照约 7.08→8.88 GiB；TCN 优化约 8.04 GiB、对照约 6.92 GiB。缓存换吞吐会增加峰值内存；2 GiB raw /512 MiB feature 的 retained 预算不能当作总内存保证。GPU 峰值 LSTM 约 603→611 MiB，TCN 约 157 MiB。
- 串行运行时 WSL 可用内存约 35～36 GiB；未清 OS 文件缓存，首日称会话冷启动而非磁盘冷启动。上述三个日点不是全年回放 SLA、固定每包 40 秒承诺、完整逐日业务 wall time 或收益/OOS 证明。按日冻结成员的 native 准备路径仍受前述公共 owner 计划组装边界约束。

## 发布回滚与生产门禁

源码合入与运行激活分开。新 helper 默认由既有荐股历史工厂注入；不创建/修改生产 Program 或运行对象。若本版本改变代码/输入 identity，不跨版本冒充 exact retry，使用新的自身 run。

回退使用经明确选择的原正确计算模式重新计算，不能恢复未来归一化、未知来源缓存或复制资产。不得操作其他窗口任务/进程。模拟盘依旧独立运行，接入由其 owner 后续实施。

| 状态 | 当前事实 |
|---|---|
| Advisory 源码 | 授权范围实现、多轮自审修复、最终F2/静态检查通过，待提交/最终 HEAD CI/合入 |
| 实际只读验证 | R22定向矩阵57项通过；R23反例先FAIL后PASS、相关11项通过（与矩阵有重叠，不相加）；R24两包稳定源码完整评分/输入/来源 EXACT_PASS |
| QE训练/数据库DDL/DML/profile/依赖安装 | noop |
| 服务/其他窗口进程控制 | noop |
| backend runtime activation | pending_user_restart；本任务不重启 |
| LocalSIM原生接入/账户回归 | owner_pending；源码未修改 |
| 全系统完整共享/经济收益 | 未宣称完成 |
| 分支/工作树删除 | 本次无单独删除授权，保留 |

## DESIGN-COMPLIANCE-001 与审核记录

审核须分别检查：不简化为两包专用/假完成；不静默失败；不改变原模型/冻结processor/序列/组合/政策；不私增包、研究或UI门禁。分owner交付与用户要求完全一致，但不得将 Advisory 合入或 contract mock 当作 LocalSIM 实际验收。

R1：迁移原优化并核对 native 公共合同；修复旧测试缺 manifest 与变量名问题。

R2：检查池变化、修订、EWM起点、Alpha footprint、并发 writer/恢复、同名模型代码；补输入成员交集/新成员历史/长暖窗前缀、OS lease、同 run 内容绑定与错误保留。

R3：检查实际调用链；接入荐股有界原生预取，保留 public single/multi-alpha 与 private writer；补执行 code closure。测试及真实对照继续执行，尚未标记最终审核通过。

R4：实测发现512MiB raw输入缓存反复逐出；源码修订触发LSTM旧探针来源绑定失败，因此不使用其结果作为最终验收。改为按数据集保存成员区间，补乱序/不相交区间与空人口合同；不把未读空档宣称已覆盖。

R5：同日物理表示去重、输入改写/alias替换/缓存逐出、C<D政策日期绑定、嵌套模型源码身份、同日多模型与节点恢复复核。直接合同共48项通过，Ruff/diff和changed-test CI可达性通过；F2尚待真实全池对照，尚未宣称READY/合入。

R6：释放动态计算后不再使用的完整静态输入与中间结果，先保存真实receipt哈希/行数，不截短processor/序列历史；Alpha/stage合同6项通过。私有路径合同14项通过；源文件稳定后全池对照进行中。当前归属扫描24个文件全部mapped，无ambiguous/unmapped；两类L0扫描均0finding。各轮为本窗口自审，不声称独立外部审核。

R7：消除reference的旧六HDF重复布局。两臂共用同一个安全文件表示构建，仍使用各自的正确计算/缓存模式；该修改纳入新来源绑定，旧r12输出不作为最终源码收据。18项定向合同、Ruff和当前标准L0通过；等待旧只读执行自然返回，再验证稳定源码，不伪造结果或控制其他任务。

R8：复核Alpha成员顺序、特征来源修订、按日冻结股票池与未绑定因子依赖。补充上述可选缓存失效及DATE_BOUND_FROZEN_PIT语义，所有日期仍消费自身原生输入。最终57项定向测试通过，24/24变更文件映射至原owner、0 ambiguous/unmapped，当前标准L0零finding，CI预检无阻断且无需数据库验证。旧r12已自然结束并正确拒绝变更来源；稳定版r21重新执行真实两包对照，尚未将单元测试等同于真实性能或双消费者运行验收。

R9：具体反例证明NumPy随机子模块/导入别名、文件上下文/通配导入及外部二进制/HDF读取不能据根模块名认定为可复用。修复可选缓存判定，反例由FAIL转PASS，11项相关合同通过，仍正常执行原代码，不增加包门禁。R21已自然结束但来源发生变化，不作最终源码验收；未控制进程或篡改收据。R24稳定源码真实验证在35GiB以上可用内存下串行启动，完成后才判定READY。

R10：R24 两个真实包的节点进程均自然成功退出；11 文件来源前后绑定与完整评分/输入逐项 EXACT_PASS。复核每项 DESIGN-COMPLIANCE-001：

1. 不简化/假完成：元数据驱动同一原生算法，不设两包特例；Advisory 授权范围完整交付，公共按日腿计划组装及 LocalSIM 产物/账户接入明确 owner_pending，不声称双业务系统已全部上线。
2. 不静默失败：typed 错误及可核验完成前缀、所有原计划日期、原成员 UNKNOWN、来源/池/时钟恢复校验保留，不删日/删股、伪造股票池或输出空成功。
3. 不漂移业务语义：冻结模型/processor/实际序列/原生融合权重/过滤排序/policy 不变；输入严格截至 C，对照同一合法视图，不恢复旧未来归一化。不将本次优化说成收益改进。
4. 不私设人工门禁：缓存不命中正常原代码重算，未加策略包、QE、收益、UI准入；无数据库/生产配置/进程操作，后端重启仍用户执行。

各轮为本窗口重复自审和定向/真实验证，不声称独立外部审核。最终 F2 为 PASS（10 项/10 行、0 warning），L0 和 quality 均 0 finding，实际 probe 字节来源与当前源码 0 mismatch，diff 检查通过。最终 HEAD 必需 CI 和实际合入状态按交付结果另行报告。

## BUG-1912：停牌 eligibility 的能力时钟修正（2026-10-15）

本节取代原实现中“C<D 时整个 risk/tradability 都观察 C”的合同，不覆盖原性能历史。
原 BUG-1904 生产样本在 C/D 均停牌，只证明执行成功，不能证明 D 时钟正确。

| 需求/设计项 | 实现 | 验证 | 结论 |
|---|---|---|---|
| F-004/F-005：停牌 D、risk/PIT 行业 C | kernel 显式能力 adapter；TradabilityFilter 可选 industry_trade_date | 两个相反停复牌反例先 RED 后 GREEN；C=D/禁用/原生递补/risk/行业未来毒化 | 源码 PASS |
| F-005：模型/参考价 C，HMM 原生标签与输入分离 | kernel prepared clock 审计；不加载或生成 HMM 系数 | HMM C 输入/D 标签、输入/训练/可用日期/标签未来隔离；普通及递补缺价保持明确错误 | 源码 PASS |
| F-006/F-010：新时钟不得命中旧后处理 | clock evidence、原生 receipt hash、语义/storage 身份；LocalSIM 原有源码绑定保持 | 实际 LocalSIM pre-lookup 身份链；改变内核字节、合同版本、D 停牌成员分别改变身份，原始评分保持 | 源码/断点合同 PASS；生产 checkpoint 待 owner 复验 |
| F-001/F-007：唯一原生排序/过滤/TopK | 继续调用原 native compute 和 TradabilityFilter | 四文件最终矩阵 63 项通过；真实已存输入只读验证另绑定源字节 | 定向回归 PASS，不代表收益 |

最终矩阵：`test_selection_signal_kernel.py`、`test_tradability_provider_clocks.py`、
`test_reference_price_availability.py`、`test_localsim_shared_selection_signal.py`，
63 passed / 0 failed（5.39 秒）；报告 `X:/AIstock_temp/LOCALSIM/shared-eligibility-clock-20261015/final.xml`。
这些是公共能力和两侧消费的定向回归，不是生产账户重新运行。

审核 R1（本窗口自审）：确认 `_CutoffBoundProvider` 覆盖缺口；两个相反停复牌例均复现错误，修复后通过。
R2：拆分原生行业查询日期，保持普通缺价/递补缺价错误；补完整 receipt 身份，避免相同排除数量掩盖成员变化。
R3：复核 HMM 输入与原生标签，保留无 alpha 时的 NOT_APPLICABLE，不新增快照最早日期/包使用门禁；测试真实 LocalSIM checkpoint 来源空间而非只测共享纯 DTO。最终相关矩阵通过。

DESIGN-COMPLIANCE-001：①真实 kernel.run 及 native consumer 包装层，不以直接 compute 或 mock-only 冒称全链；②真缺价、未来/身份错误保留，零候选与失败分开；③仅纠正获授权时钟，冻结模型/政策、原生算法和 LocalSIM 私有实现不变；④无新增包/训练/人工准入。
源码 PR/HEAD/CI、最终 fresh-process 绑定和合入状态另行记录。运行态为 pending_user_restart_after_merge，
生产复验由 LocalSIM owner 执行；未修改数据库、数据集、QE、账户或生产回放。

最终只读新进程验证：`X:/AIstock_temp/LOCALSIM/shared-eligibility-clock-20261015/fresh-kernel-r2.json`，
源码提交 `866599e5ceda80b0be2759e983e85742d6045441`，实际读取字节及提交 LF 内容分别绑定。
经原 `LocalSimKernelComputation` → 真实 `SelectionSignalKernel.run`，复用已校验、未修改的本侧原始产物；
4912 评分/4918 成员/6 UNKNOWN、完整 raw 排序及 Top50 核心字段均精确一致。
停牌排除 `trade_date=2026-01-20`，risk 为 `2026-01-19`；该样本 C/D 均停牌，
时钟正确性另外由相反停复牌反例证明，不能仅靠 Top50 一致。
原生 normalized profile hash、release config hash 和带 point-in-time 的 projection config hash 分别核对，
三者定义不同，不混用同名摘要。参考价原始身份保留，不倒填捕获时间；未运行模型推理或账户。
F2 文档结构检查为 PASS（10 项/10 行/0 warning）；L0 两类扫描 0 finding，Ruff 与 diff 检查通过。
这些证据不等于用户后端已加载本次提交，也不等于生产回放或收益验证完成。

BUG-1911 责任边界补充：603616.SH/2026-01-21 已确认为合法停牌且无权威 pre_close，
不再笼统转交数据 owner 补数。共享链只负责按冻结政策在 D 过滤停牌资格，真缺价校验不变；
停牌持仓估值、旧计划修订及 TCN/LSTM 受控恢复由 LocalSIM owner 负责。
重启后按 provider 时钟合同的交接清单核对 01-20 已完成运行、301362.SZ 实际历史参考价、
D 排除/原生递补和新来源身份；保留旧产物与经济提交，不重做该日订单和账务。
