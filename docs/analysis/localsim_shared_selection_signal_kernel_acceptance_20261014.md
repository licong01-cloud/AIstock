# LocalSIM 共享选股内核接入验收

日期：2026-10-14。设计：[F2 接入设计](../architecture/localsim_shared_selection_signal_kernel_integration_20261014.md)。本记录覆盖源码本地验收；最终 HEAD 的 CI、合入及源码同步以独立 PR 的收据为准，运行态激活保持用户重启后的独立验证。

## 交付及原生语义

默认 LocalSIM 自动评分通过专属消费服务调用公共推理脚本、InferenceEngine 和 SelectionSignalKernel。原 StrategyPackage 准备、动态因子/Alpha158、冻结处理器、真实序列长度、多 Alpha/seed/权重及过滤/融合/排序继续由原生模块处理。新窗口未修改 Advisory、StrategyPackage、QE、HMM 或数据模块。

完整 raw、完整阶段投影、实际人口、UNKNOWN、C/D/T、原政策和 manifest/data/source 身份进入现有每日证据 JSON。普通路径沿用原 writer 的事务提交确认合同，并核验其返回证据属于本次来源后推进断点。提交后、断点前中断，靠原生 evidence ID 和内容哈希实际读回恢复；文件存在不能代表账户提交。未提交输入重算，账户幂等、执行计划、订单、持仓和账本由原 lifecycle 负责。

正式运行保留原生 HMM 处理和策略政策。验收探针显式注入只读 HMM 输入及内存每日证据 writer；所有数据库连接只读，未操作生产账户。探针不训练、不修改数据集或数据库，也不负责重启服务。

现存 `selection.daily_selection_evidence` 的只读索引核对确认：唯一键仅为 evidence_id 和 artifact_hash，没有消费者之间的 package/day 唯一冲突；原 writer 插入整份 JSON 并完成事务后返回。证据为 `X:/AIstock_temp/LOCALSIM/workflow/native-evidence-schema-r8.json`，transaction_read_only=on。本次性能计时含读取、特征、模型、原生选择政策及内存 writer 的确认/恢复，真实 PostgreSQL 写入耗时和交易执行不在此基准内。

## 多轮审核及修复

| 轮次 | 发现与修复 | 验证 |
|---|---|---|
| R1 | 全局 raw 唯一键没有消费者/run，不能作为本次计算命中；采用专属 raw 仓储和原每日证据 writer。补全共享 workspace 的 factor_order 资产声明。 | 独立 run、外部 raw 拒绝；LSTM 实际输出和原生完整产物验证。 |
| R2 | 原生业务类不能在节点 Python 3.10 导入；业务准备留在 Windows，节点仅运行公共计算。X/F 跨卷硬链接和 Windows 符号链接不可用，采用已校验的只读资产原址。 | 原生模型代码及因子顺序；新包身份/不同因子数和序列元数据；X/WSL 双向路径验证。 |
| R3 | 提交后恢复投影未绑定每日证据；补充完整投影、内容哈希、包集合和原生事务读回校验。失败记录保留原阶段、原因和完成前缀。 | 投影篡改、空包集合、外部证据、未提交文件、writer 失败、提交后中断及重复恢复。 |
| R4 | 保留 HMM 的 D 标签及其原生 C 输入含义；正式运行保留原政策，仅探针关闭输入生成。防止不同包的人口在适配器中被混合。 | HMM 时钟/只读探针；训练 PIT 与推理身份分离；原调度隔离与非阻塞回归。 |
| R5 | 原生 alpha receipt 的随机 raw artifact_id 被误当数学差异；保留真实 receipt/hash，逐一校验其指向自身产物后比较其余所有字段。Windows 按文件名求和重复计入硬链接别名；改为按卷/inode 去重。 | TCN 4,907 行逐项完全一致；故意改变非 ID 字段/指向外部 ID 必须失败；真实硬链接占用校准。 |
| R6 | 复核原生 writer 的提交确认与恢复读回合同，明确只读/内存验证范围；核对实际唯一键及调度后续路径不会按新 raw ID 读取全局评分仓储。 | 只读 schema receipt；原 writer/账户消费链路定点审阅；源码未改变。 |

以上是本窗口逐轮自审和修复，不宣称额外独立代理审核。

## 本地与委托验证

最终新增定向矩阵 29 项全部通过（4.65 秒）；既有直接调度回归 4 项通过，覆盖推理超时不冻结其他绑定、runtime release 隔离及显式注入服务的双 broker 缓存行为。改变后的模拟盘完整回归由现有 `simulation_core_l2` CI 承接；不在本地重复全模块矩阵。

可复现命令：`python -m pytest backend/tests/simulation_runtime/test_localsim_shared_selection_signal.py backend/tests/simulation_runtime/test_localsim_selection_inference.py -p no:cacheprovider -q`。本窗口基准目录均在 `X:/AIstock_temp/LOCALSIM/`，Windows `TEMP/TMP` 和 pytest basetemp 显式指定该盘；节点对应 `/mnt/x/AIstock_temp/LOCALSIM/`。首次 RTK 失败 tee 默认写 C 的单个本窗口日志已迁到 X，之后定向测试使用直接 Python。

CI 路由证据：`X:/AIstock_temp/LOCALSIM/workflow/ci-plan-r7.json`；`required=l0,simulation_core_l2`，DEV DB、dependency/install 均不需要，8 个文件全部可路由且新增测试没有遗漏。Nightly 可承接 `localsim_unattended_l3`、ops UI、双 broker 和交易时段链路；不借此声明当前运行态已经通过。

R8 本地执行 L0 两个原生扫描器，产物均指定 X 盘：quality 扫描 0 finding；guardrail 扫描 0 blocker、5 个 P2 复杂度提示。新增两个提示分别是固定大小的环境敏感值替换（环境项数乘日志长度）及只遍历本次验证目录的一秒磁盘采样（文件数线性）；没有股票间笛卡尔积或重写行情 join。其余三个指向原 scheduler 的未改动循环。ruff 和 diff whitespace 检查通过。

## 实际结果与性能

实际包来自原生绑定读取，代码不保存或匹配包 ID 清单。验收只读消费 LSTM/TCN 原冻结包，不重新训练。TCN 首轮稳定源码双臂：2026-01-05，合法 C=2025-12-31，D=T=2026-01-05；4,913 个原成员、4,907 行评分、6 个 UNKNOWN；动态因子 24、Alpha158 20、真实序列长度 20，TopK 25。

TCN R5 优化臂端到端 323.860 秒、参考臂 1,205.376 秒；节点峰值 RSS 分别 8,642,138,112 / 7,528,546,304 字节。完整评分（含排序）、TopK、排除项、状态、原生政策、输入/来源、manifest、资产闭包及 raw canonical payload 均精确一致。两臂属于各自独立计算，原生 alpha receipt 的 artifact_id 不同，原 receipt hash 和公共 semantic_identity 因而不同；不把这些身份伪造成相同值。其余完整阶段字段精确一致。R5 的 Windows 按路径磁盘峰值重复计入硬链接，不作为实际占用结论。

最终源码 LSTM 优化臂已经完成 2026-01-05/06/07：端到端分别 303.624 / 294.745 / 299.249 秒，节点峰值 RSS 8,636,059,648 / 8,694,300,672 / 8,681,062,400 字节；各日独立绑定股票池和输入，评分/原人口仍为 4,907 / 4,913，原生 TopK 为 50（TCN 为 25）。去重磁盘峰值分别 1,684,980,035 / 1,735,085,399 / 1,783,583,885 字节（后一日包含前日保留的证据）；三日最终保留 150,398,291 字节。自身同日恢复 0.865 / 1.358 / 1.492 秒，模型节点数和原生证据数分别为 1 / 2 / 3，没有重复推理或提交。

三日 C 分别为 2025-12-31、2026-01-05、2026-01-06，D=T 为对应目标日；run ID 和实际输入 hash 各为三个不同值。原生运行配置 hash 含日绑定截止，因此允许跨日不同，只要求同日双臂精确相等。LSTM 三日双臂全部完成：`status=COMPLETE`、`equivalence=EXACT_PASS`、`source_binding=EXACT_PASS`；各日 16 项全值及身份检查全部通过。公共阶段 hash/semantic_identity 差异仍仅为各自原生 alpha lineage UUID，并未改写凭据。

| 日期 | LSTM 优化链路秒 | 参考链路秒 | 加速比 | 参考节点峰值 RSS 字节 | 参考去重文件峰值字节 |
|---|---:|---:|---:|---:|---:|
| 2026-01-05 | 303.624 | 1,113.729 | 3.668 | 7,549,874,176 | 1,685,518,061 |
| 2026-01-06 | 294.745 | 1,117.297 | 3.791 | 7,578,296,320 | 1,735,795,641 |
| 2026-01-07 | 299.249 | 1,120.484 | 3.744 | 7,562,006,528 | 1,785,648,322 |

优化臂 RSS 比参考臂增加约 14–15%；GPU allocated peak 632,119,808 字节，参考臂 631,889,408 字节。Windows worker lifetime peak 优化最终为 650,993,664 字节，参考为 755,470,336 字节；不能当作每天独立峰值。参考三日自身恢复分别 1.475 / 2.026 / 1.424 秒，证据数/模型节点数同样为 1 / 2 / 3，首日提交后中断恢复通过。磁盘为一秒采样、卷/inode 去重后的唯一文件字节数，不包含原址资产，也不是整机文件系统扇区分配量。

TCN 最终 R8 独立计算完成：`status=COMPLETE`、`equivalence=EXACT_PASS`、`source_binding=EXACT_PASS`，16 项全值及身份检查全部通过。端到端 285.839 秒，节点 RSS 8,711,376,896 字节，GPU allocated peak 164,102,144 字节，Windows worker lifetime peak 578,232,320 字节；去重临时文件峰值 1,680,259,083 字节，最终保留 49,659,727 字节。同日恢复 0.866 秒，提交后中断恢复通过，节点数/原生证据数均为 1。

TCN 与明确指定的 R5 参考基线比较：共享/原生模块字节、包、release、原始数据/资产/政策全部匹配；生产消费者没有该基线输入功能。参考耗时 1,205.376 秒，跨次实测比为 4.217 倍，节点 RSS 相比参考增加约 15.7%；性能比受不同测量时间和主机缓存/负载影响，不作为稳定吞吐保证。R5 原始 NOT_PROVED 记录保留，其唯一原因是旧比较器要求独立 alpha UUID 的原生阶段哈希相等；R8 当前完整比较如实核验两臂 lineage 并保留原哈希差异。

实际证据：`X:/AIstock_temp/LOCALSIM/lstm-three-days-r7/progress.json`、`X:/AIstock_temp/LOCALSIM/tcn-final-r8/progress.json`，各日完整 native JSON 及原生 evidence/projection 位于对应自身目录。源码绑定为实际文件字节，提交/checkout 的换行规范化与后端运行身份另行核对，不改写旧收据绑定。源码稳定后两项实际测试都通过，无 QE 训练、模型/行情大文件重复复制及生产账户执行。

不得预设每天 40 秒，不将短窗口性能或源码验收作为收益证明。每臂首次日期显式注入“原生内存 writer 提交后、断点前异常”，随后用新的服务实例恢复。内存记录分开报告节点进程峰值 RSS、GPU allocated peak 和 Windows worker lifetime peak，不将它们误称成同一时刻全机合计。

## 公共 owner 接口交接与边界

公共推理/StrategyPackage owner 的最小接口需求见 [设计交接段](../architecture/localsim_shared_selection_signal_kernel_integration_20261014.md#公共接口-owner-交接)：日绑定成员/PIT/workspace/leg/seed 计划、只读资产地址和 consumer/run 内多日会话；数据 owner 还需为未提交输入跨进程复用提供不可变 revision 校验接口。本次按用户允许的逐日方案实现。

另已实测发现公共 `semantic_identity` 含原生 alpha receipt 的随机 artifact_id：相同数学结果和相同 raw canonical payload 可以有不同的该身份。模拟盘如实保留它，恢复不依赖其跨运行相等，不自行修改公共模块。公共 owner 可补充独立于 lineage UUID 的内容身份；本次值/政策/数据比较仍逐项校验所有实际字段，未改写原生 receipt。

## DESIGN-COMPLIANCE-001 与发布状态

1. 完整逐日正式接入和完整产物；多日公共 workspace 优化属于用户明确允许的后续接口工作。真实 LSTM 三日双臂及 TCN 独立对照全部完成，IO fixture 仅用于错误合同和调度隔离反例。
2. 未吞掉异常、伪造成功或用空候选代表失败；原生 UNKNOWN 不编造停牌/涨跌停原因。
3. 评分/融合/过滤/TopK 复用唯一原生算法；订单、持仓、资金、账本及正式 HMM 政策保持原语义。
4. 没有新增人工确认、审批或发布阻断；合入沿用本窗口授权，后端重启沿用用户负责的边界。

源码本地技术验收通过；F2 门禁、最终 HEAD 必需 CI 和合入属于接下来的 PR 阶段，其真实结果以该独立 PR 为准。用户明确批准通过后合入；没有另设审批。production_ddl_gate=noop；dependency_gate=noop；QE_training=noop。运行态激活：pending_user_restart，target=backend-main，runbook=`docs/operations/backend_main_runtime_restart_runbook.md`；不能用本次探针或源码合入替代生产运行证明。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | simulation_runtime/localsim_selection_inference.py、localsim_selection_signal.py；scheduler.py 自动评分路由 | backend/tests/simulation_runtime/test_localsim_shared_selection_signal.py；公共源码未改 | PASS | 无 |
| F-002 | 原生 artifact/runtime 准备；原址资产适配及原生多腿/seed 分派 | backend/tests/simulation_runtime/test_localsim_selection_inference.py；实际包 feature/sequence counters | PASS | 无 |
| F-003 | ContextVar、独立 LOCALSIM/run、X 根校验、WSL witness 和 own raw 仓储 | backend/tests/simulation_runtime/test_localsim_selection_inference.py；artifact: X:/AIstock_temp/LOCALSIM/tcn-short-r5/progress.json | PASS | 无 |
| F-004 | 完整 raw/projection 随原生每日证据提交；C/D/T、成员 UNKNOWN 及所有原生身份 | backend/tests/simulation_runtime/test_localsim_shared_selection_signal.py；TCN actual native evidence | PASS | 无 |
| F-005 | own intent、内容绑定、原生读回、失败前缀及 checkpoint | backend/tests/simulation_runtime/test_localsim_shared_selection_signal.py；TCN commit-before-checkpoint 实际故障注入 | PASS | 无 |
| F-006 | 每日/每个模型构建新 workspace；公共 owner 需求交接 | backend/tests/simulation_runtime/test_localsim_selection_inference.py；设计 owner 交接段 | PASS | 用户明确批准公共多日计划未齐备时先逐日调用 |
| F-007 | 只读独立双臂及全值/原生政策/输入身份比较 | artifact: X:/AIstock_temp/LOCALSIM/lstm-three-days-r7/progress.json；artifact: X:/AIstock_temp/LOCALSIM/tcn-final-r8/progress.json | PASS | 无 |
| F-008 | 连续日期、追加日期、同日自身恢复与故障注入 | artifact: X:/AIstock_temp/LOCALSIM/lstm-three-days-r7/progress.json；backend/tests/simulation_runtime/test_localsim_shared_selection_signal.py | PASS | 无 |
| F-009 | 端到端、节点 RSS、Windows worker peak、去重磁盘采样 | artifact: X:/AIstock_temp/LOCALSIM/lstm-three-days-r7/progress.json；artifact: X:/AIstock_temp/LOCALSIM/tcn-final-r8/progress.json | PASS | 无 |
| F-010 | 六轮自审、直接回归、既有 CI 路由、授权合入及用户重启边界 | artifact: X:/AIstock_temp/LOCALSIM/workflow/ci-plan-r7.json；backend/tests/simulation_runtime/test_localsim_shared_selection_signal.py；本 PR 最终 HEAD CI | PASS | 用户明确批准本地通过后交付独立 PR、必需 CI 通过后合入；后端重启由用户执行，运行态独立验收 |
