# Advisory 历史收益文件批处理设计

版本：v1.3；关联 BUG-1946 / Issue #6202、BUG-1950、BUG-1954 / Issue #6233、BUG-1967 / Issue #6280；等级 F2；状态：BUG-1967 定向源码回归和完整只读验证通过；既往验证保留，当前 CI、合入、用户重启和业务完成分别登记。

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
- 行业文件是 L2，旧 SQL 优先 L3。新文件来源的行业分组显式使用 `SW_L2_FILE_V1`，不可冒称旧 L3 分组相同；收益数值不依赖行业分组。BUG-1967 将文件 summary 的“无可见 membership”表示为 `None`，在受影响覆盖组显示缺失计数及 `PIT_INDUSTRY_CONTEXT_UNAVAILABLE`，不阻断已可计算的收益，不写假行业。冲突 membership、未知映射、来源/hash 错配仍失败；要求行业的严格接口仍报缺失，其他消费者不随之降级。
- 不增加新 benchmark/费用政策。只有冻结政策已有定义才消费指数；不得把缺失 benchmark 自动视为零或新增超额收益口径。

## 4. 主体与控制面批处理

从已完成 R3 run/day 的候选和 day receipt 取原始主体；只用有限 SQL 获取 run/day 索引和状态，候选/episode 事实优先由原生哈希产物读取。同一工作项 subject/policy 仅解析一次，严格绑定工作项完整哈希，缓存大小有界。

剩余任务状态、结果事实和断点事务仍使用数据库，不承诺整个服务零数据库。Advisory 专用连接工厂有界复用连接，保留事务提交/回滚、只读设置、进程隔离和原 process-worker DSN 合同；不改公共 pg_pool 或系统网络参数。行情热路径 SQL 次数必须为零。

## 5. 恢复与失败

BUG-1950（重启后发现的错误读回缺口）：新收益父任务的 FAILED 和 RETRYABLE_FAILED 均记录真实失败原因，并与原生 attempt 一致。旧 REFRESH_OUTCOMES/RETRYABLE_FAILED 若 error_json 为空，公开只读投影仅可从同 operation、attempt_no、fencing_token、status、request hash 的已持久化 attempt 提取明确 reason_codes，并标注 `PERSISTED_OPERATION_ATTEMPT_REASON_CODES`、原 error_json 缺失、attempt 与 receipt hash；不猜测原异常类型，不覆盖数据库或 CAS。来源不完整/错配继续显示缺口并使正式烟测失败；内部 worker 仍读取原记录，不把展示投影当修复或恢复指令。无需 DDL、数据库修补或公共验证器放宽，新增源码合入后仍需用户重启。本项只解决业务错误展示，不升级旧失败任务或收益结果。

同一旧缺陷还将可重试 attempt 的状态写进没有 result ref/hash/version 的 `result_status`。新任务不再为 RETRYABLE_FAILED 声称已发布结果。旧读回仅在上述身份和原因验证通过、原值恰为 RETRYABLE_FAILED 且 ref/hash/version 全空时，把该值明确保存在错误来源的 `stored_result_status`，公开结果状态为空；原 operation/attempt 状态仍失败，内部原值不变。任何不同值或结果引用矛盾继续暴露，不靠字段清空掩盖真实冲突。

保留 CAS/fencing、单调 keyset 和原生原子结果提交。只能复用经过精确输入身份验证的结果，旧 DB 来源断点不得跳过新文件来源工作项。硬中断时不能无证据推进游标；若仅有结果无完成收据，允许重读已提交前缀但不得重算已命中的同身份结果。

失败收尾须保留已提交结果，保持租约覆盖最终校验和状态落库；不得将未完成改成功。去除重复主体和行情读取、减少连接创建及不可变产物重复校验，不删除身份、PIT、候选唯一性或真正缺价校验。

### 5.1 BUG-1954：汇总运行上下文、闭包重复读取与子租约

持久化 `HistoricalRangeOutcomeFactV1` 不包含 `range_run_id`，不得向金融事实补造列或改写金融产物。summary loader 在已通过 run/subject/ref 精确查询的结果上，构造独立 `HistoricalRangeSummarySubjectIdentityV1`（run、subject type、subject ID），传给 SQL/文件上下文 provider。文件 provider 据此解析原冻结元数据；缓存绑定 run 与完整 subject ref，不从当前 Selection 或其他运行回填。

市场形态属于冻结的 decision-T 上下文。同一命令中相同完整 ref 只解析一次轻量结果，`None` 也缓存；DAY_RECEIPT 的候选引用及完整依赖指纹一并保留。第一次仍做原生文件 SHA、payload、envelope 和精确 ref 验证，文件路径、大小、mtime、inode 变化继续失败；不复制大型资产或修改公共 canonical 算法。缓存有界且不跨进程/运行共享。

原生上游遍历将“节点已访问”与“新传入边已验证”分开：即使节点已由另一 day-context 分支遍历，本次边仍须通过 request/range/day 验证，随后登记完整 ref 加消费者时钟/身份的验证缓存；不能在 `visited` 分支直接退出而漏记缓存，使每条 outcome 再读取约30MB候选。缓存以完整 ref 而非仅 semantic hash 区分，绑定封存文件指纹且有界。最终收据提交前检查缓存快照，防止未改动的缓存父节点掩盖已改变的后代；不在每条事实追加时重复全图扫描。

REFRESH_OUTCOMES_RUN 自己持有 fenced lease，不得借父任务心跳证明子任务健康。子心跳和 worker 的游标更新串行 CAS，保持 operation/attempt/worker/lease/fencing 身份不变；续租覆盖工作、汇总以及成功/失败收据的递归校验。复用 repository 既有 `finalize_lease` 接口，在校验后、终态 CAS 前停止并取得最新 row version，不提前停心跳。汇总发布前检查 claim；续租失败、失租或接管不能写成 COMPLETED。原有稳定游标和已发布结果保留，过期 claim 不能被本 worker 自行复活。

本修复不改金融计算、金融 producer/source/policy/hash、候选/排序或成熟度口径；summary 源码身份自然更新。经过完整输入身份验证的既有8,340条金融 projection可复用，不重跑 Selection，也不因 summary 优化重算金融事实。收据文件存在不等于 operation 已完成，父 RUNNING 不等于子健康，超额收益的原生 benchmark 缺失仍为 PENDING。

### 5.2 BUG-1967：只读复用、显式最终汇总与独立调度

瓶颈不是训练：既往 8,340 条金融事实展开为 41,700 行 summary 的无 profiler 只读计算为 467.75 秒；收据闭包仍反复加载大型候选。旧一次性客户端还把 1,800 秒汇报间隔当成下阶段分发间隔，并用整个 backend HEAD 替代实际输入兼容性判断。本次不删除首次验证，也不把源码合入直接当作经济结果通过。

1. **只读产物与已验证类型复用。** 首次依旧执行完整文件 SHA、envelope/schema、payload、精确 ref、run/day/upstream 校验。随后按完整 ref 与文件指纹（设备、inode、大小、mtime、ctime）复用封存的嵌套只读对象；普通 `load` 仍返回可修改的深拷贝，不能修改缓存。缓存按序列化文件重量上限 128 MiB、最多 4,096 项淘汰；这不是进程 RSS 上限。金融 fact 首次验证后保留 typed artifact 和规范化 JSON，私有缓存不进入序列化或内容 hash；复制/替换 JSON 不得借用原校验结果。负向缺行业也缓存，但不得填零、虚构行业或修改收益。
2. **文件验证会话。** 同一 runtime 中的 profile binding 与文件 provider 共享有界 SHA 证明（最多 512 项），每次使用重查指纹；内容或文件身份变化即失败，不重新绑定。fresh process 重新执行完整验证。OHLC 批读窗口和内存表沿用原生实现，不复制文件、不跨运行共享结果，也不把行情文件缓存误当 SQL 控制面可以移除。
3. **三种显式汇总模式。** `REFRESH` 为兼容旧请求/hash 的默认模式；`DEFER` 只完成该批金融结算，收据显式 `summary_deferred=true`，不得宣称整体 summary 已完成；`ONLY` 仅对显式 run 的既有事实执行最终汇总，不遍历金融 planner、不调用 evaluator、不追加金融事实，且必须生成真实 SUMMARY ref。ONLY 的文件上下文只验证/读取实际消费的行业成员与映射，不建立无用 OHLC/停牌/涨跌停价格窗口；该上下文拒绝金融结算入口，金融路径仍完整验证其消费文件。中间阶段可 DEFER，所有依赖完成后执行一次 ONLY；默认及已有调用行为不变。失败、CAS/fencing、成熟度、费用、缺价和上游闭包均沿用原生合同。
   父收据根据实际子收据聚合延迟标记，而不是根据计划假设实际已经延迟；旧收据无该字段时按旧版不支持 DEFER 的合同处理为 false。没有实际子收据不能凭空声称延迟结算完成。
4. **身份不伪装。** summary、文件适配源码和 producer 的真实 hash 按现有计算更新；不能宣称本轮文件代码调整后金融 producer hash 未变。既存完成事实及原 producer 身份保留，ONLY 可在不重算金融事实的情况下更新 summary。需要重算的后续金融请求必须遵守原生新 producer / correction / evidence 协议；旧请求 exact retry 只能恢复原冻结计划，不能被新代码默默升级。可选 `expected_execution_identity` 精确绑定 financial producer、file binding、policy 三项；旧 API 不传时保持兼容，新 continuation 必须传入，不能用整库 HEAD 代替这三项。
5. **通用 continuation 而非改写旧客户端。** 新清单 schema 为 `advisory_outcome_continuation_v1`，明确单 run 阶段、依赖、冻结请求与身份；不接管运行中的 legacy 清单或 checkpoint。原子 X 盘 checkpoint 绑定清单 hash，POST 前保存完整原请求，恢复逐页查询原生父/子操作，确认原 key、request hash、operation 身份、租约及结果 ref。响应丢失仅执行保存请求的官方 exact retry；未知状态或失败不能标成功。轮询默认 10 秒、汇报默认 1,800 秒，调度不等待汇报；不同 batch 可按显式资源并发预算执行，同 batch 保留原生 inflight 限制，不增加 QE 全局空闲或双包同时到齐门禁。最终完整 summary 由清单的最终 ONLY 阶段表达，DEFER 完成不等于整项业务完成。

临时状态/报告仅在 X 盘（WSL 对应 `/mnt/x/AIstock_temp`），单客户端锁和原子替换只作用于自己的新状态文件。不得停止旧客户端、控制后端、修改旧 checkpoint 或自动发起生产结算。源码性能验证只读使用既存产物/事实，不发布 summary、不提交金融事实、不改数据库。

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
| F-008 | summary_service.py：显式 run/subject 上下文、轻量完整 ref regime 缓存 | backend/tests/advisory_historical_range/test_r4_summary_service.py；BUG-1954 runtime_contract 中既存 outcome 原生只读 loader/calculate | PASS | none |
| F-009 | repository.py：visited 边登记、完整 ref/时钟隔离与封存快照检查 | backend/tests/advisory_historical_range/test_r4_outcome_service.py；BUG-1954 runtime_contract 中既存失败收据原生只读闭包校验 | PASS | none |
| F-010 | outcome_service.py / summary_service.py：子 lease 覆盖汇总和全部终态校验 | backend/tests/advisory_historical_range/test_r4_outcome_service.py：成功、失败、续租失败、CAS 回调 | PASS | none |
| F-011 | artifact_store.py / outcome_file_inputs.py / models.py：封存只读对象、typed fact 复用、普通可变读取兼容 | backend/tests/advisory_historical_range/test_outcome_optimization.py | PASS | none |
| F-012 | outcome_file_source.py / composition.py：指纹绑定的有界验证会话 | backend/tests/advisory_historical_range/test_outcome_optimization.py | PASS | none |
| F-013 | api_models.py / models.py / outcome_service.py / runtime_factories.py / service.py：REFRESH/DEFER/ONLY 与原生恢复 | backend/tests/advisory_historical_range/test_outcome_optimization.py；backend/tests/advisory_historical_range/test_r4_outcome_service.py | PASS | none |
| F-014 | outcome_file_inputs.py / summary_service.py：可选行业缺口显式化而非虚构行业 | backend/tests/advisory_historical_range/test_outcome_optimization.py | PASS | none |
| F-015 | outcome_continuation.py：依赖分发、分页、父子 lease、精确重试及 X 盘单写者 | backend/tests/advisory_historical_range/test_outcome_optimization.py | PASS | none |
| F-016 | 精确 diff、真实只读 loader/calculate；金融字段/hash 保持与模式隔离 | backend/tests/advisory_historical_range/test_outcome_optimization.py；BUG-1967 runtime_contract 的完整只读报告 | PASS | none |

## 7. 实施方案与验证方案

顺序为：冻结来源及精确模型兼容 → 批量文件适配和原生输入索引 → 连接/重复读取及失败收尾优化 → RED/GREEN、多轮审查及真实只读 smoke → CI、源码交付及用户重启。每次修复先运行失败 nodeid，末轮相关矩阵只跑一次，广泛矩阵交给必需 CI。不因停牌填价，不用回测产物收益字段替代财务内核，不重跑原候选。

末轮相关矩阵52项通过，补充的来源版本集摘要错配回归也通过，共53个直接测试场景。真实文件 smoke 的行情窗口只批读一次、数据库和网络连接尝试为零；这是文件读取证明，不是整个服务零数据库。另一次真实原生对照复用既存候选603696.SH和原冻结政策，5日 Recommendation/Executable 两组的原始入场价、收益/路径数值及未完成原因与 DB 适配一致；两组各有4个成熟数值和1个基准缺失的 PENDING。基准缺失并未被本次来源优化补造；分组状态继续原生 NOT_DUE，不得用适配器一致性 PASS 冒充整组经济结果 COMPLETE。

真实1000候选、1484主体索引准备单独测得约128秒，文件/DB对照整次约142秒。这是有限只读元数据和不可变产物准备的测量，不是1000候选全量结算速度；完整性能需在用户重启后原生运行时另测，不承诺固定每交易日耗时。

一次有 profiler 开销的同范围诊断约318秒，其中20个 day 的产物处理约318秒，元数据 SQL 等其余部分合计约0.04秒。热点是原生 canonical JSON、内容哈希与 typed payload 验证；该测量不能和无 profiler 的128秒直接比较。本次只复用已经验证的不可变产物及精确来源版本集，并限制缓存容量，不改公共 canonical 算法、不跳过首次哈希或类型/闭包校验。

BUG-1954 的末轮直接相关矩阵41项通过，随后新增的“新 operation 精确复用既有金融事实、不调用 evaluator”定向用例也通过，合计42个直接场景。只读验证完整既存失败收据及上游闭包约292秒，大型候选读取40次，不再随数千条 outcome 重复读取；此测量不包含金融重算或状态写入。另一带 profiler 的完整 loader/calculate 用时约931秒。最终无 profiler fresh-process 全量只读验证耗时467.75秒（约7分48秒，覆盖整个20日窗口，不是每日耗时），读取8,340条既存金融结果，展开41,700个计算投影；metrics、coverage、summary 哈希与前次只读结果完全一致，金融 producer hash 未变，三个运行源码 SHA256 与当前工作树逐一匹配。最终收据为 BUG-1954 runtime_contract 引用的 `summary-native-final.json`。这些测量不是运行态性能承诺，也未发布业务 summary 或恢复后台任务。

F-008～F-010 的 PASS 仅指源码与上述只读/定向合同验证。未发布新的业务 summary、未推进原操作、未升级原失败收据、未重启服务；原后台仍需用户重启后的独立运行态验收。临时30分钟客户端补查子操作 lease 的逻辑已 RED→GREEN，但不会声称运行中的旧客户端自动加载了新代码，原 checkpoint 不覆盖。

DESIGN-COMPLIANCE-001：完整实现本设计的文件来源与批量准备，不以 mock 代替真实读取；Financial/Selection 原生内核不复制；L2 分组和新来源身份等必要差异已同步设计；没有新增研究审批、收益门槛或用户操作要求。四项源码符合性检查通过，不代表运行态激活或投资收益证明。

BUG-1954 四项复审：①真实全量只读 loader/calculate 与闭包验证均完成，非 mock-only 或子集交付；②过期/续租失败、哈希错配和文件变化继续报错，不伪造原操作成功或缺失 benchmark；③仅纠正上下文和收尾实现，金融/选股/成熟度与原输出不变，差异已写入§5.1；④复用现有 lease/CAS、CI 和用户重启边界，没有新增研究、交易或人工审批门槛。

### 7.1 BUG-1967 定向验收与发布状态

最终一次相关矩阵覆盖 78 个场景：77 项通过，一项严格行业缺失错误文案兼容性不匹配，恢复兼容文案后仅重跑该 nodeid 通过。随后增加“诊断 ref 不能冒充完成收据”回归，受影响的 7 个客户端场景通过；最后发现私有缓存参与 Pydantic 对象相等比较，补 RED→GREEN 并重跑事实/hash 两项通过，合计覆盖 80 个不同场景，其中新增优化测试 24 项。覆盖缓存淘汰/精确 ref 篡改、JSON 替换不借用旧校验、默认请求身份不变、DEFER/ONLY、真实 SUMMARY 要求、价格窗口不被汇总消费、身份与分页/子租约/精确重试。changed-file Ruff、diff check、L0 通过；广泛回归交给 CI/Nightly，不重复执行整套本地矩阵。

完整既存结果的第一轮只读 load/calculate 展开 41,700 行，装载 386.82 秒、公式计算 0.82 秒；包含当时完整价格窗口准备的总计 521.06 秒。该轮金融输入已不同于 BUG-1954，不能与旧 467.75 秒直接折算提速；它用于确认公式非瓶颈并促成 ONLY 轻量上下文改动。轻量路径完整测量为准备 145.35 秒、装载 336.65 秒、计算 0.68 秒，总计 482.69 秒（整个 20 日窗口，不是每天）；OHLC 窗口读取为 0。两次 covered-outcome、summary-input、metrics、coverage hash 完全一致，未写数据库或发布经济结果。单次测量有缓存/系统负载影响，不据此承诺稳定加速倍数；剩余成本仍在首次既存产物/事实装载与校验，真正的整任务收益还包括减少中间全量 summary 及不再等待 1,800 秒汇报。

上述完整测量记录于 BUG-1967 runtime_contract 的 `bug1967-summary-final-readonly.json`，测量开始/结束四项源码指纹一致；之后只修正私有缓存的对象相等比较，不改 loader、文件价格语义、汇总公式或经济 hash。同步最新 main 时另继承已合入的 #6281：文件价格读取/原价换算调用共享 `frozen_dataset` helper，没有在本任务改写该共享模块；交点的 14 项文件来源与轻量汇总回归通过。最终源码另由 `bug1967-final-source-smoke-r2.json` 与定向测试验证精确原生产物/DB fact/typed fact、缓存读取后相等性及当前源码指纹，无需为无关对象比较和 helper 接线修正重复长时全量计算。各证据标明版本，不把前一测量冒充最终 commit 的运行激活或生产性能保证。

BUG-1967 的 DESIGN-COMPLIANCE-001 四项复审：①交付全部登记优化及真实完整只读链路，不以小样本/假成功代替；②首次校验、真正缺价、冲突、CAS 与失败仍明确报错，ONLY 无 SUMMARY 不得成功；③仅显式汇总模式、可选行业缺口呈现及真实 producer 身份变化按 §5.2 同步，费用/收益/选股/成熟度不变；④无新研究审批、整库 HEAD 或 QE 空闲门禁，后端重启仍由用户执行。矩阵 PASS 只指上述源码合同，CI、源码合入和后端激活另记。

首轮扩展 CI 为 497 通过、2 跳过、4 个父任务兼容场景失败（旧测试请求没有新 summary_mode）。随后将父标记改为从实际子收据聚合，四个原失败场景与真实 DEFER 父子标记场景共 5 项定向回归通过，不修改旧测试或降低验证要求。至此本地覆盖 84 个不同场景；最终 CI 以修复后的新 HEAD 独立结果为准，不重跑旧 run 冒充修复版本。

### LocalSIM 窗口提示词

仅参考 BUG-1946 的边界约定：回放选股继续复用唯一的 InferenceEngine / StrategyPackageSelectionArtifactService / SelectionSignalKernel 和原生评分、融合、eligibility、递补及 TopK，不复制算法，也不消费另一场景的结果、run identity、缓存命中结论或断点。此 PR 只优化 Advisory 私有历史收益结算，未改共享信号接口，LocalSIM 无需接入本次 outcome 文件适配器。持仓、订单、账务、估值和收益分析仍由 LocalSIM 独立负责；若未来共享选股源码发生变更，由两端用各自合法 C/D/T 与数据/政策身份做完整排序、TopK、UNKNOWN、停牌资格和断点隔离回归，不得把收益标签引入选股。

## 8. 发布、回滚、风险与生产门禁

主要风险：qfq 二次复权、普通缺价误判停牌、L2/L3 分组混用、active profile 漂移、旧断点冒充新来源，以及源码合入被误报运行激活。逐项由 F-001～F-007 验证。来源验证失败必须报错，不能回退 SQL 或旧目录。

发布仅交付源码及兼容原接口；新文件命令用新输入身份。回滚仅回滚源码版本，保留所有新旧产物，不篡改结果或游标；旧代码不得消费不认识的文件来源请求。无需 DDL、依赖安装或数据激活；后端重启由用户执行。

多轮审查与修订、直接测试、fresh-process 真实文件只读 smoke、DESIGN-COMPLIANCE-001 四项和必需 CI 分开报告。代码合入不等于运行加载；用户重启后再由各业务窗口执行原生链验收。当前任务不启动生产回放、不写生产数据库、不调整 active profile、不训练 QE、不控制服务。技术验收不代表模型收益证明。
