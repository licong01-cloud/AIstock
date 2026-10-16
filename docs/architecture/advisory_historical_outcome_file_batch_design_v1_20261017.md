# Advisory 历史收益文件批处理设计

版本：v1；关联 BUG-1946 / Issue #6202；等级 F2；状态：源码直接验证通过，CI/源码交付及用户重启分别登记。

## 1. 背景、目标、范围与边界

历史选股已完成，收益结算却仍逐股票/窗口/投影查询行情、复权、涨跌停和日历，并重复加载相同主体。高频短连接与 Windows TCP 端口耗尽同时出现，不能简单归因于数据库未启动或永久连接泄漏。

本设计仅替代 Advisory Phase1R R4/R5 的历史结算数据适配与批处理执行，沿用 `PositionPathValuationCore`、Recommendation/Executable/Episode 三类投影及冻结费用、日历、成熟度规则，不修改收益目标、选股、评分、TopK 或既有结果。

LocalSIM 不导入本次结算模块；它通过共享选股服务运行自己的持仓、账务和收益链。本次不修改共享选股、LocalSIM、QE、HMM、数据模块，也不宣称同时优化 LocalSIM 收益统计。仅评估接口不变性；模拟盘若需要文件化收益统计，由其 owner 独立接入。

共享架构只覆盖回放与每日运行的选股信号：InferenceEngine → 原生评分/融合 → SelectionSignalKernel → 原生 eligibility/递补/TopK → 各消费者自己的产物适配。只共享代码，不共享运行结果、run identity、缓存命中结论、账户状态或断点。不得把本设计的收益来源、收益标签、结算政策、统计口径、持仓/订单/账务或未来标签数据反向引入共享信号链。后续共享选股优化仍须两端 owner 分别验收；本次不要求模拟盘增加适配器或消费 Advisory 收益模块。

本设计对旧 R4 设计中“数据库为行情来源”的要求作显式增补：新文件来源合同与旧数据库来源并存；旧请求仍可复验，不能把文件哈希冒充旧数据库分区身份。

## 2. 架构、接口契约、冻结来源与版本

新 R5 结算命令在登记时解析活动 profile，将完整 profile 快照、快照摘要和已解析 controller 根目录写入内部子请求。重试仅使用已登记快照，不重新绑定新 generation。路径是只读 locator，不复制数据文件。

请求来源为空表示旧 DB 合同，保持原请求哈希算法；新来源非空进入请求哈希。文件角色的 query/schema/revision 身份显式标注文件适配版本、profile/manifest 哈希和窗口，不允许隐式回退数据库。来源变化产生新请求与新输入身份；不覆盖旧请求、断点、计算证据或已完成经济产物。

新命令在 operation 登记前发布原生 REQUEST 产物，原子建立仅创建、不覆盖的 command-ref 索引。输入记录 API 身份、完整子请求及来源快照；异步分发带冻结子请求，fresh-process 和 HTTP exact retry 均从原输入恢复，不重读 active profile。API 或 operation 身份不一致明确冲突；旧命令不能被默默升级为文件来源，需新 idempotency key。无 DDL 或新数据库。

读取 profile pin 的 manifest、日历、股票池、停牌、行业和指数文件；先校验文件哈希，再消费。日频 OHLC/复权采用 `daily_pv.h5`；manifest 未单独 pin 此 H5 时，在命令登记前显式计算并保存 consumer-observed 内容 SHA256，不冒称生产者已提供该 pin。原始涨跌停价采用日频 bin，角色分区哈希绑定实际消费数值。完整性验证每个冻结文件一次，内容缓存绑定路径、大小和修改时间，漂移报错，不静默刷新。

## 3. 文件计算语义

- 一次按结算最小起点至 label-as-of 批量读取 OHLC/复权；后续股票、期限和投影复用同一内存窗口。请求超出已冻结覆盖范围明确失败；未到期计算继续原生 NOT_DUE/MATURING。
- qfq OHLC 已含 factor，必须先按声明 schema 恢复原始 CNY，再转换为 `_li`，交给同一个财务内核复权一次。使用交易价格精度并校验恢复误差，不能二次复权；复权因子的归一常数不影响同股票收益率。
- 停牌仅依据权威 suspend parquet 的全日 S 声明；不把 NaN 自动当停牌。合法停牌保留无成交价格状态，不补造停牌价格；普通可交易日缺行情/复权/涨跌停仍明确失败。
- 日历仅用于收益观察；收益读取的未来标签不得反流到选股。原冻结候选、名单和评分不重跑。
- 股票池仅使用 profile 的 PIT 成员区间，不用物理 provider all.txt 充当池身份。重复区间保留。
- 行业文件是 L2，旧 SQL 优先 L3。新文件来源的行业分组显式使用 `SW_L2_FILE_V1`，不可冒称旧 L3 分组相同；收益数值不依赖行业分组。缺行业保持明确失败，不写假行业。
- 不增加新 benchmark/费用政策。只有冻结政策已有定义才消费指数；不得把缺失 benchmark 自动视为零或新增超额收益口径。

## 4. 主体与控制面批处理

从已完成 R3 run/day 的候选和 day receipt 取原始主体；只用有限 SQL 获取 run/day 索引和状态，候选/episode 事实优先由原生哈希产物读取。同一工作项 subject/policy 仅解析一次，严格绑定工作项完整哈希，缓存大小有界。

剩余任务状态、结果事实和断点事务仍使用数据库，不承诺整个服务零数据库。Advisory 专用连接工厂有界复用连接，保留事务提交/回滚、只读设置、进程隔离和原 process-worker DSN 合同；不改公共 pg_pool 或系统网络参数。行情热路径 SQL 次数必须为零。

## 5. 恢复与失败

BUG-1950（重启后发现的错误读回缺口）：新收益父任务的 FAILED 和 RETRYABLE_FAILED 均记录真实失败原因，并与原生 attempt 一致。旧 REFRESH_OUTCOMES/RETRYABLE_FAILED 若 error_json 为空，公开只读投影仅可从同 operation、attempt_no、fencing_token、status、request hash 的已持久化 attempt 提取明确 reason_codes，并标注 `PERSISTED_OPERATION_ATTEMPT_REASON_CODES`、原 error_json 缺失、attempt 与 receipt hash；不猜测原异常类型，不覆盖数据库或 CAS。来源不完整/错配继续显示缺口并使正式烟测失败；内部 worker 仍读取原记录，不把展示投影当修复或恢复指令。无需 DDL、数据库修补或公共验证器放宽，新增源码合入后仍需用户重启。本项只解决业务错误展示，不升级旧失败任务或收益结果。

同一旧缺陷还将可重试 attempt 的状态写进没有 result ref/hash/version 的 `result_status`。新任务不再为 RETRYABLE_FAILED 声称已发布结果。旧读回仅在上述身份和原因验证通过、原值恰为 RETRYABLE_FAILED 且 ref/hash/version 全空时，把该值明确保存在错误来源的 `stored_result_status`，公开结果状态为空；原 operation/attempt 状态仍失败，内部原值不变。任何不同值或结果引用矛盾继续暴露，不靠字段清空掩盖真实冲突。

保留 CAS/fencing、单调 keyset 和原生原子结果提交。只能复用经过精确输入身份验证的结果，旧 DB 来源断点不得跳过新文件来源工作项。硬中断时不能无证据推进游标；若仅有结果无完成收据，允许重读已提交前缀但不得重算已命中的同身份结果。

失败收尾须保留已提交结果，保持租约覆盖最终校验和状态落库；不得将未完成改成功。去除重复主体和行情读取、减少连接创建及不可变产物重复校验，不删除身份、PIT、候选唯一性或真正缺价校验。

## 6. Design Acceptance Index / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | models.py / runtime_factories.py / service.py：新来源冻结、旧请求兼容 | backend/tests/advisory_historical_range/test_outcome_file_batch.py | PASS | none |
| F-002 | outcome_file_source.py：每个结算子运行窗口批读、零行情 SQL | backend/tests/advisory_historical_range/test_outcome_file_batch.py；BUG-1946 runtime_contract 中真实文件 smoke | PASS | none |
| F-003 | outcome_source.py / outcome_evaluator.py：同一财务内核、复权/停牌/缺价 | backend/tests/advisory_historical_range/test_r4_outcome_evaluator.py；BUG-1946 runtime_contract 中原生财务只读对照 | PASS | none |
| F-004 | outcome_file_inputs.py：主体与政策复用 | backend/tests/advisory_historical_range/test_outcome_file_batch.py | PASS | none |
| F-005 | composition.py：有界连接、事务及只读复位 | backend/tests/advisory_historical_range/test_r5_composition.py | PASS | none |
| F-006 | 精确 diff：LocalSIM/共享信号/收益边界不变 | backend/tests/advisory_historical_range/test_r5_service_boundaries.py；backend/tests/advisory_historical_range/test_r5_protected_module_isolation.py | PASS | none |
| F-007 | 原生 outcome_service.py / 修改 service.py、repository.py：恢复及失败收尾 | backend/tests/advisory_historical_range/test_r4_outcome_service.py；backend/tests/advisory_historical_range/test_outcome_file_batch.py | PASS | none |

## 7. 实施方案与验证方案

顺序为：冻结来源及精确模型兼容 → 批量文件适配和原生输入索引 → 连接/重复读取及失败收尾优化 → RED/GREEN、多轮审查及真实只读 smoke → CI、源码交付及用户重启。每次修复先运行失败 nodeid，末轮相关矩阵只跑一次，广泛矩阵交给必需 CI。不因停牌填价，不用回测产物收益字段替代财务内核，不重跑原候选。

末轮相关矩阵52项通过，补充的来源版本集摘要错配回归也通过，共53个直接测试场景。真实文件 smoke 的行情窗口只批读一次、数据库和网络连接尝试为零；这是文件读取证明，不是整个服务零数据库。另一次真实原生对照复用既存候选603696.SH和原冻结政策，5日 Recommendation/Executable 两组的原始入场价、收益/路径数值及未完成原因与 DB 适配一致；两组各有4个成熟数值和1个基准缺失的 PENDING。基准缺失并未被本次来源优化补造；分组状态继续原生 NOT_DUE，不得用适配器一致性 PASS 冒充整组经济结果 COMPLETE。

真实1000候选、1484主体索引准备单独测得约128秒，文件/DB对照整次约142秒。这是有限只读元数据和不可变产物准备的测量，不是1000候选全量结算速度；完整性能需在用户重启后原生运行时另测，不承诺固定每交易日耗时。

一次有 profiler 开销的同范围诊断约318秒，其中20个 day 的产物处理约318秒，元数据 SQL 等其余部分合计约0.04秒。热点是原生 canonical JSON、内容哈希与 typed payload 验证；该测量不能和无 profiler 的128秒直接比较。本次只复用已经验证的不可变产物及精确来源版本集，并限制缓存容量，不改公共 canonical 算法、不跳过首次哈希或类型/闭包校验。

DESIGN-COMPLIANCE-001：完整实现本设计的文件来源与批量准备，不以 mock 代替真实读取；Financial/Selection 原生内核不复制；L2 分组和新来源身份等必要差异已同步设计；没有新增研究审批、收益门槛或用户操作要求。四项源码符合性检查通过，不代表运行态激活或投资收益证明。

### LocalSIM 窗口提示词

仅参考 BUG-1946 的边界约定：回放选股继续复用唯一的 InferenceEngine / StrategyPackageSelectionArtifactService / SelectionSignalKernel 和原生评分、融合、eligibility、递补及 TopK，不复制算法，也不消费另一场景的结果、run identity、缓存命中结论或断点。此 PR 只优化 Advisory 私有历史收益结算，未改共享信号接口，LocalSIM 无需接入本次 outcome 文件适配器。持仓、订单、账务、估值和收益分析仍由 LocalSIM 独立负责；若未来共享选股源码发生变更，由两端用各自合法 C/D/T 与数据/政策身份做完整排序、TopK、UNKNOWN、停牌资格和断点隔离回归，不得把收益标签引入选股。

## 8. 发布、回滚、风险与生产门禁

主要风险：qfq 二次复权、普通缺价误判停牌、L2/L3 分组混用、active profile 漂移、旧断点冒充新来源，以及源码合入被误报运行激活。逐项由 F-001～F-007 验证。来源验证失败必须报错，不能回退 SQL 或旧目录。

发布仅交付源码及兼容原接口；新文件命令用新输入身份。回滚仅回滚源码版本，保留所有新旧产物，不篡改结果或游标；旧代码不得消费不认识的文件来源请求。无需 DDL、依赖安装或数据激活；后端重启由用户执行。

多轮审查与修订、直接测试、fresh-process 真实文件只读 smoke、DESIGN-COMPLIANCE-001 四项和必需 CI 分开报告。代码合入不等于运行加载；用户重启后再由各业务窗口执行原生链验收。当前任务不启动生产回放、不写生产数据库、不调整 active profile、不训练 QE、不控制服务。技术验收不代表模型收益证明。
