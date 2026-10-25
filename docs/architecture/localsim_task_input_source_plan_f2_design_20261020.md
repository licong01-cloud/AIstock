# LocalSIM 任务级输入来源计划

日期：2026-10-20；F2/T3。按用户授权接续 PIT 迁移，采用已合入 #6453 的
[公共接入合同](advisory_task_input_source_plan_f2_design_20261020.md)。

## Background

LocalSIM 已复用唯一 InferenceEngine、StrategyPackageSelectionArtifactService 和
SelectionSignalKernel，但旧任务只能选择整段文件或整段数据库。文件发布终点不能靠
异常触发数据库回退。新能力必须在创建时冻结单次来源切换，恢复只读原计划。

## Scope

精确写范围：`backend/services/simulation_runtime/` 下新增
`localsim_input_source_plan.py`，修改 `localsim_product_control.py`、
`successor_repository.py`、`localsim_file_inputs.py`、`localsim_replay_runtime.py`、
`localsim_selection_inference.py`、`localsim_selection_signal.py`；直接测试
`backend/tests/simulation_runtime/test_localsim_input_source_plan.py`（新增），必要的
对应现有 LocalSIM 直接测试；本文。真实只读验收脚本仅写 X 盘，收据记录路径及摘要。
超出列表前先在本文登记理由。API 属于现有 replay-create，不增加页面功能。

恢复范围补充（实现前）：`localsim_replay_recovery.py` 的正式 recover 预检也必须按保存
来源计划选择当日文件 reader，不能把新 source ID 送到旧单源 resolver；仅改输入绑定。

审核范围补充（实现前）：`localsim_pit_migration.py` 的本模块纯 release builder 增加
可选 metadata 转换回调，让已迁移政策的 catch-up/live 使用同一来源计划生命周期。
显式 PIT 迁移不得沿用与新政策冲突的旧冻结计划，计划预检返回阻断而不生成错误子任务。

## Non-goals

不修改 Advisory 私有容器、StrategyPackage、QE、HMM、PIT authority、数据文件或 schema。
不重训、不补数、不新建生产账户、不启动生产回放、不修改已完成经济前缀，不控制服务。
不复制评分、序列特征、融合、过滤、递补或 TopK 实现；左侧导航和现有页面不变。

## Architecture

LocalSIM 薄适配器调用公开 `input_source_plan` 冻结、校验、逐日路由和实际读取收据。
来源计划和创建请求指纹原子保存在现有 release metadata，job 的 source SHA 绑定 plan SHA。
使用独立 `lsinput_…` 运行身份；每日选股尝试仍有自己的 lsselection 身份、X 盘 workspace、
产物和事务断点。CLI 使用同一计划的 consumer_scope/run_id/input_revision。

原生模型窗口来自 StrategyPackage 只读投影及既有 FrozenSequenceLengthReader（纯资产
元数据读取，无荐股业务状态）；原生推理仍自行解析实际窗口并回传收据。计划只规定合法
输入下界，不装配另一套特征或模型执行器。动态因子、Alpha158、序列长度和腿身份不按包白名单。

## Contracts

1. `selection_input_source_mode` 可选三值 FILE_ONLY、DATABASE_ONLY、FILE_THEN_DATABASE。
   缺省/null 不改变旧请求的序列化语义、hash 或旧回放行为。显式模式有自己的 source ID。
2. 同语义创建重试先锁请求、读取持久计划，不能重解 active。新创建解析 active 一次；
   校验文件最小组件覆盖；完整模型历史窗随首个数据库日整体切换，不拼接半窗。
3. C 为 D 的前一交易日，D=T；停牌 D，risk/行业/参考价 C。日历、政策、包与 manifest
   均绑定原计划。FILE_ONLY 越界、文件内洞、真缺价、身份漂移均失败，不回退。
4. 恢复校验保存计划、时钟和 job hash，不修改原计划。后续 catch-up 是新的业务段，
   冻结新计划并保留前驱；实时切换不携带已过期历史文件上下文。
5. 原生 v2 归一化不改签：LocalSIM 在 provider metadata 保留完整原始 input_context，
   比对公共字段和实际四类输入收据。完整评分、股票池、UNKNOWN、排序和 TopK 保持原生。
6. 已提交每日证据恢复保持事务和幂等。未提交 DB 结果必须重读实际输入并比较完整旧结果；
   新物理 workspace 是新尝试，不伪称恢复旧 CLI checkpoint。其他 scope/run 的计划不可消费。
7. 新来源计划的共享选股请求使用内核既有逻辑来源 `DB_HISTORICAL`，原生产物从创建时即
   绑定此历史信号角色，不事后改签。物理 FILE/DB 来源由保存计划、完整原始上下文及实际
   读取收据单独验证；LocalSIM 分钟执行及日运行来源仍保留 DATASET_FILES/DB_HISTORICAL。
8. 共享内核边界重建 runtime config 时，保留 Selection admission 放在解析对象内的
   excluded canonical PIT 租约，并放回原生根键；不得使风险过滤回退到旧默认股票池。

## Design Acceptance Index

- F-401：兼容旧请求；新模式创建原子持久化与同请求重试。
- F-402：原生窗口和 C/D/T、政策身份；三模式与合法单次切换。
- F-403：恢复读取原计划，日内各线程有独立 reader，catch-up 新段及实时退出。
- F-404：独立 CLI 身份、缓存、workspace、实际输入收据和完整原始 context。
- F-405：失败前缀、已提交幂等和未提交 DB 输入漂移校验。
- F-406：真实 LSTM/TCN 文件末日及数据库首日，完整原生排序和 TopK 验收。
- F-407：多轮审核、定向回归、性能/内存/磁盘实测和部署边界。

## Implementation Plan

先完成独立计划适配和创建合同，再接 executor/provider，最后完成实际输入恢复验证。
每次扩范围先记录；所有写入保持现有 release/binding/job 原子事务，无 DDL。

## Verification Plan

直接合同回归覆盖旧字段缺省、三模式、C<D=T、跨边界、计划串用、hash/输入漂移、
创建重试不读 active、catch-up 和失败恢复。生产关键持久化在既有 DEV rollback fixture
验证。真实只读 native LSTM/TCN 用同 C/D/T、同原生政策比较完整评分/排序/TopK/UNKNOWN，
逐日测量时间、峰值内存、磁盘；不预设耗时。运行 feature validator、Ruff、diff check 和 CI。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-401 | localsim_product_control.py; successor_repository.py | backend/tests/simulation_runtime/test_localsim_input_source_plan.py; artifact: X:/AIstock_temp/LOCALSIM/input-source-plan-20261020/dev-required-plan-r3.xml | PASS | 无 |
| F-402 | localsim_input_source_plan.py | backend/tests/simulation_runtime/test_localsim_input_source_plan.py | PASS | 无 |
| F-403 | localsim_file_inputs.py; localsim_replay_runtime.py; localsim_replay_recovery.py | backend/tests/simulation_runtime/test_localsim_input_source_plan.py | PASS | 无 |
| F-404 | localsim_selection_inference.py; localsim_selection_signal.py | backend/tests/simulation_runtime/test_localsim_input_source_plan.py | PASS | 无 |
| F-405 | localsim_selection_signal.py | artifact: X:/AIstock_temp/LOCALSIM/input-source-plan-20261020/tcn-native-r4-recovery-completion-r1.json | PASS | 无 |
| F-406 | localsim_selection_inference.py; localsim_selection_signal.py | artifact: X:/AIstock_temp/LOCALSIM/input-source-plan-20261020/final-native-acceptance-r1.json | PASS | 无 |
| F-407 | 本文 | artifact: X:/AIstock_temp/LOCALSIM/input-source-plan-20261020/targeted-final-r8.xml; artifact: X:/AIstock_temp/LOCALSIM/input-source-plan-20261020/dev-required-plan-r3.xml; artifact: X:/AIstock_temp/LOCALSIM/input-source-plan-20261020/final-native-acceptance-r1.json | PASS | 无 |

## Risks / Failure Modes

数据库没有不可变 revision；计划 hash 不能替代内容收据。FILE 生产物理 PIT 身份和 DB
runtime 身份可不同，但必须由公共原生合同验证同 canonical 政策，不能业务层改名。
发布覆盖不足应向数据 owner 返回精确组件、日期和原因，不能静默切换。

## Review / DESIGN-COMPLIANCE-001

第一轮审核：写范围限 LocalSIM，使用公共来源计划、唯一推理 CLI 和纯计算内核；创建
重试先持久读回，DEV 验证五对象原子提交和插入失败回滚；不借用荐股业务状态。

第二轮审核：保留原始 context，校验 v2 公共字段、四类实际读取摘要、完整评分和成员。
真实文件输出暴露紧凑 PIT 收据不含完整成员的问题，改为对照完整原生请求验证，禁止在线补全。
历史信号逻辑来源与实际 FILE/DB 路由分别冻结，不事后改写密封原始评分。

第三轮审核：真实 C<D=T 过滤暴露 excluded canonical PIT 租约在内核交接时丢失，已修复
LocalSIM 序列化边界并添加原生风险 provider 观察回归；4917/4917/0 的原始文件
产物只读复核通过，风险 C、停牌 D。正式 recover 拒绝他人计划或日期作用域。
补追允许新增更早日历，但旧日历区间、政策及未完成起点仍须完全一致。

第四轮审核：真实数据库日注入提交前失败，另开物理 workspace 重读四类输入、重新完整
推理，与旧尝试的实际输入摘要及全部评分一致；提交后重读发现冻结 evidence 的数组
会变成 tuple，已在 LocalSIM 恢复边界还原规范 JSON，完整产物相等回归通过。
另补策略包显式 inference PIT 声明传递到公共 session 校验，禁止来源计划覆盖声明。
最终 276 项 LocalSIM 定向回归通过，10 项外部 DEV 用例在普通环境跳过；DEV 结果单列。
精确 changed-file 分类为 simulation_core_l2 + 既有 DEV repository gate，无前端改动。
原生 PIT 租约缺失返回明确的 source contract 错误，不自造政策；此补充回归独立通过。
既有 DEV successor repository 全计划及本次注册/重读/失败回滚共 8 项通过，无新数据库。
收据：`X:/AIstock_temp/LOCALSIM/input-source-plan-20261020/dev-required-plan-r3.xml`。

最终新进程直接读取两包原生资产、两日计划及真实 file/runtime PIT envelope，通过；
四个小型地址 workspace 共 470750 字节，权重和冻结处理器均直接引用原资产仓库。
收据：`X:/AIstock_temp/LOCALSIM/input-source-plan-20261020/final-native-assets-r2/receipt.json`。

真实四日技术验收（2026-08-31 FILE → 2026-09-01 DB；C 分别为 08-28、08-31）：

| 模型 | 来源 | 评分/股票池/UNKNOWN | 原生 TopK | 端到端秒 | 推理进程峰值 RSS GB | 自有工作目录峰值 GB |
|---|---|---|---|---|---|---|
| TCN | FILE | 4917/4917/0 | 25 | 647.736 | 8.627 | 1.648 |
| TCN | DB | 4911/4911/0 | 25 | 304.970 | 9.082 | 1.702 |
| LSTM | FILE | 4917/4917/0 | 50 | 411.005 | 8.523 | 1.644 |
| LSTM | DB | 4911/4911/0 | 50 | 307.175 | 9.035 | 1.765 |

完整有序评分和原始成员均与唯一 CLI 输出逐项相等；完整筛选、排序、TopK 及 trace 与
同原生准备输入直接调用原生 computation 的基线相等。两包各用自身原政策/输入，不把
FILE 和 DB 的不同日期/成员输出互相要求相等。TCN DB 单独复测补齐最终修复后端到端和
磁盘采样，与先前两个独立 DB 尝试的实际输入和完整评分再次精确对照通过。

提交前故障会保留失败前缀、重新读取 DB 并重算完整结果；已提交证据的重读耗时
1.670–3.208 秒，完整产物及选股相等，不重做推理。GB 为十进制；RSS 是推理子进程峰值，
不是整机总内存。磁盘按自有目录去重物理字节计量，不包含复用的原始大模型/数据。
当前逐日读取及因子计算仍占大头；没有声明固定每天 40 秒或全账户回放同等耗时。

总收据：`X:/AIstock_temp/LOCALSIM/input-source-plan-20261020/final-native-acceptance-r1.json`。
状态明确为 `TECHNICAL_PASS_DATA_ACCEPTANCE_BLOCKED`；F-401…F-407 是源码和独立只读
技术验收，下面的数据权威差异和运行加载未提升为通过，不代表生产账户接续完成。

真实分钟接续样本使用两个各 240 根真实分钟线输入及原生 TWAP/费用/估值；8 月 31 日
FILE 到 9 月 1 日 DB，内存账本完整承接现金和持仓。收据
`X:/AIstock_temp/LOCALSIM/input-source-plan-20261020/minute-execution-continuation-r1.json`。
它证明技术接续，不代表完整账户再平衡、正式生产回放或收益验收。

DESIGN-COMPLIANCE-001 逐项结论：

1. 实现登记的来源计划、持久化、恢复、逐日推理和业务段生命周期；使用真实两包与既有
   DEV，不以 mock-only、静态结果或分钟样本冒称生产账户验收。
2. 日期/政策/来源/输入内容或完整评分漂移均保留明确失败；文件内洞不回退，8 只股票的
   实际差异保留完整输出并交权威核查，不补值、删股或报告空成功。
3. 保留唯一原生评分、融合、风险/停牌过滤、递补及 TopK，C<D=T；模型资产、训练与
   原 profile provenance 不变，不改订单、资金、持仓和账本的经济语义。
4. 未增加训练或人工准入审批。运行加载和数据一致性依照用户已有边界分别登记，
   不把本轮技术验证提升为生产回放或收益证明。多轮审核为本窗口自审，不冒称独立外审。

## Public Interface Remaining Gaps

### Published FILE / DB PIT overlap: owner verification required

真实只读重叠区对照发现，同一 canonical authority、规则及参数下，当前文件绑定
`589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`
与数据库的历史成员不同。2026-08-28：文件 4917、DB 4909；2026-08-31：文件 4919、
DB 4911。文件在两天均多以下 8 只，DB 无独有成员：

| 股票 | DB 最近资格结束日 | DB 退出原因 | 文件当前资格终点 |
|---|---|---|---|
| 002217.SZ | 2026-06-18 | st_negative | 2026-08-31 |
| 002759.SZ | 2026-07-14 | st_negative | 2026-08-31 |
| 300087.SZ | 2026-06-26 | st_negative | 2026-08-31 |
| 300290.SZ | 2026-06-26 | st_negative | 2026-08-31 |
| 301139.SZ | 2026-05-08 | st_negative | 2026-08-31 |
| 600439.SH | 2026-07-31 | st_negative | 2026-08-31 |
| 600530.SH | 2026-07-31 | st_negative | 2026-08-31 |
| 688089.SH | 2026-07-24 | st_negative | 2026-08-31 |

证据：`X:/AIstock_temp/LOCALSIM/input-source-plan-20261020/` 下
`pit-same-cutoff-file-database-overlap-r1.json` 和 `pit-eight-stock-intervals-r1.json`。
这是实际输入版本差异，不能仅凭规则名称一致推断数据相同，也不能仅凭两份收据裁定
哪一份权威结果错误。源码技术验收使用各自合法冻结输入与同源原生基线比较；跨来源
生产数据一致性及正式账户接续仍待 PIT authority / 数据发布 owner 核查。

数据窗口最小交接：请核查上述 8 只的 ST 事件生效/恢复日期、当前权威资格区间与发布
文件的 day-selection universe / spans 衍生链。返回权威裁定、版本/政策/输入摘要、
上述两日完整成员差异及重新发布回执。需要更正时发布新不可变版本，不覆盖旧冻结文件。
LocalSIM 不删股拼池、不改名、不静默回退、不改账户/断点，也不重训策略包。

### PIT coverage extension and multi-day workspace

公共 PIT owner 需提供“同政策覆盖扩展”的正式恢复合同：现有 generation 校验还比较
`state_source_digest`、`coverage_start/end`，覆盖扩展可能令旧租约即使规则参数未变也
按原生合同阻断。本轮真实来源计划已经冻结到 20 日，未主动修改权威制造漂移。
LocalSIM 不忽略摘要、不重绑旧计划；若要
接续，owner 需给出冻结历史区间未改变的权威证明，或明确新段/新计划的受控迁移入口。
这是后续跨权威状态接续的公共依赖，不是文件越界或数据库缺失。

公共多日腿 workspace 尚未提供可独立消费的完整计划。当前按要求逐日调用同一共享 CLI，
不复制模型、特征、融合或 TopK；后续 owner 可提供按冻结模型腿与实际输入收据复用的
workspace/session 接口。v2 context 新字段仍由 LocalSIM metadata 保存并严格核对。

## Rollout / Rollback

源码独立 PR，CI 通过按窗口授权合入。运行态激活等待用户重启；旧任务不热迁移到新模式。
回滚只影响未来显式创建，不删除保存计划或经济事实。已冻结计划用原实现读取，失败关闭。

## Production Gates

无 schema、生产 DML、服务操作、数据发布或经济回放。本轮真实验证限原生只读链和既有
DEV rollback。源码、CI、用户重启、运行加载、正式回放验收分别报告；技术通过不证明收益。
