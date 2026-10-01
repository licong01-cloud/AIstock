# Advisory ENTRY_PRICE 绑定、每日发布与读回 F2 详细设计 v1.3

> 日期：2026-09-28；Feature tier：F2；业务归属：Advisory。
> 状态：SOURCE_MERGED_RUNTIME_VERIFIED_BINDING_PENDING（2026-09-30）。PR #5099已合入源码及v2坐标，用户重启及BUG-1623语义验证通过；ENTRY_PRICE未绑定、真实价格prediction/settlement未发布。本窗口没有操作后端进程。
> 前置：[独立角色源码](advisory_entry_price_independent_role_f2_design_20260928.md)、[价格确认合同](advisory_entry_price_confirmation_f2_design_20260928.md)。

## 1. Background / 当前接入缺口

旧 `publish_price_range_binding` 写包/manifest/style级指针，旧Program descriptor只有一个Ranking角色；将新v4直接写入旧指针既不能解决P0-D短路，也会影响同包其他Program。生产接入需要最小Program级ENTRY_PRICE指针，而非构建所有角色的通用管理平台。

旧独立自然价格通道由CLI交付；`prospective_price_cli prepare/capture` 和 `prospective_price_evaluation_cli settle/aggregate` 不自行接入每日scheduler。PR #5099已实现本设计Program级角色及每日capture/settle接入，并由用户重启加载；当前角色未配置，因此9月15日旧CLI artifact仍不能解释为“当前角色正在自动累计”。

## 2. Scope / 交付结果

本切片只增加ENTRY_PRICE的版本化binding、只读状态接口、每日预测/结算接入以及回滚。完整交付必须同时具备：确认结果、精确binding、运行源码、真实盘前prediction、价格API/UI读回以及成熟后settlement。若当天无合格候选，报告有依据的NO_CANDIDATES并用历史同核场景验收代码；不能伪造生产AVAILABLE。

每日收集的源码和历史时钟回归可在等待确认时完成；生产binding需confirmation通过。本设计仅确认价格分布，Ranking收益、Admission和止盈止损状态独立。

## 3. Non-goals / 权限边界

不修改QE公共代码，不提交实验，不自动重训，不改Program/候选/排名/ReviewPolicy，不写行情/研究数据库，不新增DDL或调度服务。只修改Advisory自有源码及文件artifact。生产后端重启由用户执行；设计合入本身不激活模型或控制进程。

## 4. Architecture / Program级角色指针

### 4.1 身份与存储

已实现路径：`<model_root>/entry_price_roles/<program_id>/<binding_version_id>/versions/<role_sha256>.json`及同目录`active.json`。一Program/binding一个ENTRY_PRICE指针；不复用旧 `price_range_bindings` 或改写 `program_bindings`。路径校验复用 `AdvisoryModelBindingResolver.descriptor_path` 的ID/根约束模式，禁止目录穿越及symlink逃逸。

版本载荷：schema=`advisory_entry_price_role_binding_v1`、role、program_id、binding_version_id、package/manifest/style/policy、universe与membership规则hash、candidate projection identity、source feature schema、price model/member hash、training parent/outcome来源ID、projection producer version、confirmation ID/hash、confirmation scope、effective_from_target_date、created_at、previous_role_sha256。active仅包含当前role hash、enabled及写入时间。全payload canonical hash、临时文件同目录原子rename，发布前后读回。

确认scope至少包含package、冻结manifest/style/policy/feature schema、候选来源/Top20及universe。确认版本与运行版本的entry算子/投影identity必须一致；换API展示文字不改变模型身份，数值投影变更则需要重新确认。更换Program绑定不会自动复制角色；新binding默认未配置，只有exact scope兼容的显式发布才可使用。

### 4.2 CAS、并发及回滚

已实现角色publish及rollback操作，使用expected_current_role_sha256执行CAS；首次发布expected为空且active必须不存在，更新/回滚要求当前hash精确匹配。复用已有descriptor互斥锁与原子写模式，不用“最后写入胜出”。先写不可变version，再原子替换active；崩溃的孤立version不影响当前指针。同内容retry返回原receipt，冲突typed拒绝。

回滚仅指向本Program/binding既有版本或禁用entry，必须经过相同CAS和身份检查；被退役/失效bundle不能因为回滚绕过检查。保留历史文件，不清理其他worktree或artifact。一次运行开始时固定role版本、候选及数据身份；运行中指针改变不得混合两个版本，发布时再检scope，已发布预测保持不变。

### 4.3 发布前校验

只接受与上述scope完全一致且结论为CONFIRMED_PRICE_DISTRIBUTION的确认artifact。校验模型文件hash、候选/输入资产可消费性和当前Program身份；2026-09-14六个旧包读回所见的runtime_asset_admission问题（实施时重新核查现状）不因entry设计被豁免。确认成功也不证明当前数据输入已具备，应分别返回MODEL_NOT_CONFIRMED、INPUT_UNAVAILABLE、SCOPE_MISMATCH或READY。

解析旧v4来源ID仅作不可变训练关系校验；不要求运行M3、不读取当前P0-D作为parent。价格role不依赖旧包级v1 binding，可用性也不由Ranking role决定。

## 5. Contracts / 管理、读回与UI

已实现只读 `GET /api/v1/advisory/programs/{program_id}/entry-price/status`：configured、role hash、bundle ID、confirmation scope/state、effective date、latest prediction/settlement D/T与状态、未完成阶段/reason。只读允许返回明确错误，不修改active或生成artifact。未配置分支返回NOT_CONFIGURED，不伪造角色身份或价格可用性。

已实现Advisory CLI `entry_price_delivery_cli`：`inspect`、`prepare-binding`、`apply-binding`、`rollback-binding`。所有命令显式model-root、program/binding/spec；prepare只生成待发布request，apply/rollback显式expected-current-hash、授权引用及有效日期。API不新增交易按钮，UI显示区间、目标日、价格基准、实验/已确认价格分布状态；高级详情展开完整identity。角色管理是本模块配置步骤，不另建审批UI。

`model-shadow?price_contract=entry-v2`来自独立角色设计，仍是无写GET。entry子信封只读盘前持久化prediction，未捕获则标记WAITING_CAPTURE/NOT_CAPTURED；不为GET即时推理或发布，不允许把事后计算写成PROSPECTIVE。只有下面的capture通道产生正式价格记录，旧Ranking GET计算语义不变。

实施接口细化：默认`legacy-v1`响应字段完全不变；显式`entry-v2`新增独立`entry_price`子字段，保留原`price_range`及Ranking字段。页面主动请求V2，将entry卡片放在Ranking可用性条件之外；默认仅展示盘前冻结结果，未捕获显示WAITING_CAPTURE/NOT_CAPTURED，不为GET执行或写入预测。旧M3派生辅助价格只有身份与投影完全一致时可附加，各角色错误独立。

历史列表读回显式携带可选`entry_list_version_id`：必须与Program/T匹配，仅在T已开盘时允许读取该列表原binding/role下的冻结prediction，即使角色后来禁用或Program换绑也不丢失历史。优先原binding的活动角色已发布版本；没有对应版本时只有唯一exact-list历史产物可读，多个版本歧义则typed拒绝，不按文件时间猜测。盘前仍只读当前启用角色，不借历史读回恢复已禁用模型。该参数不改变旧Ranking计算或列表内容。

## 6. 每日发布与结算

### 6.1 capture

复用 `backend/services/advisory_forward/service.py::run_once` 所属执行周期，先完成基线发布，再执行有界entry工作项。每次仅处理已启用Program的当前下一个目标日，默认最多1个capture和1个settlement工作项，总预算30秒/周期，按最久未处理顺序轮转，超时状态DEFERRED继续下一周期；不新增线程池/daemon，也不能在主循环无限追历史。

允许时窗为D日数据可见且推荐输入已发布之后、T日09:30 Asia/Shanghai之前。输入基于同一个已发布list/review/Selection及其冻结候选组，必须记录list来源和真实as-of。输入尚未具备为WAITING_INPUT；资源冲突为WAITING_RESOURCE；过时窗且从未发布为MISSED_CAPTURE，不能盘后补写。首个有效日期不得早于用户重启后实际部署时间所允许的下一时窗。

定时自然影子收集遵守QE实验不并行约束：消费已有QE公开只读任务查询（`GET /api/v1/quantevolver/experiments`及其正式任务详情合同），完整分页核对非终态任务；pending/running及其他未确认终态均延后收集，响应不全、分页失败或状态未知也延后；不停止QE、不修改QE registry。该查询是消费者侧资源前检，不是跨模块原子锁：查询后QE仍可能开始。发现运行中冲突即在当前有界步骤结束后暂停本价格工作项、不控制QE；若要求严格零重叠，正式离线确认必须使用QE窗口已确认的独占时段，不能把轮询宣称为互斥保证。普通Advisory基线继续工作。重型历史确认同样先检查无QE并行。价格UI普通读取与实验任务执行分开，不为读取页面创建训练/实验。

公开实验列表必须使用平铺模式include_children=false：它包含父实验与子运行，total/offset与行数一致。include_children=true按父实验分页但附带展开子行，不能用于本资源检查。legacy canonical_status缺失、paused或未知身份均继续WAITING_RESOURCE，不因为修正分页而放行。

冻结bundle一次加载可在进程内按role hash缓存；输入和context按D/T隔离。捕获通过已实现entry-only入口复用独立scorer，旧prospective v1请求依赖M1/M3且保持不变。新产物根为 `entry_price_daily_predictions/<role_sha256>/<T>/`，自然类型=PROSPECTIVE_OOS，真实published_at严格早于T开盘。身份/特征错误为FAILED，正常逐股不可用保留；系统失败不能记为无推荐。

资源状态读回细化：列表中的`planned`可能是已完成演进任务的模板实验，不能将模板状态误认为仍在训练。对于这类记录，只读查询所属QE task的公开summary，只有任务身份一致且明确终态才放行；确认未提交的无task draft可作为无资源占用处理。任何running/pending、身份冲突、未知状态或不完整分页仍WAITING_RESOURCE。该判别只消费QE公开合同，不修改或修复QE状态。

### 6.2 settle与重试

T日18:00且该日kline/suspend双审计ready后结算已存在的prediction。行情未完成为WAITING_DATA；权威停牌为NOT_APPLICABLE并保留；未知缺行或冲突为FAILED_INPUT，不删除行。按冻结prediction计算指标，禁止重新推理后覆盖旧价格。结果写 `entry_price_daily_settlements/<role_sha256>/<T>/`。

工作项唯一键为Program、binding、role hash、target date、stage；用文件互斥及原子artifact实现exact retry与并发保护，不新增DB锁表。重复scheduler tick返回既有结果，不重复写记录；依赖故障仅重试未完成阶段。settlement恢复可以晚于T，capture不能越过开盘边界回补；旧/禁用角色已发布的prediction仍允许结算，不重新推理。当前不另建状态索引，持久状态从不可变artifact读回；scheduler最后尝试是内存状态、重启可丢失，不能当作预测证据。

### 6.3 质量读回

按同role和scope累计coverage、宽度、miss、mid error、可用性及实际样本日期；旧P0-D observation不加入。达到20日/300行只意味着可作原合同描述统计，不自动重训或切换bundle。漂移给出`QUALITY_REVIEW_REQUIRED`并展示，自动撤销仅限身份损坏/模型不可加载等正确性失败；质量阈值不在看到数据后临时设定。用户可以显式回滚价格角色。

生产每日运行与实验历史验证共用核心推理/投影/结算，调度钟点仅用于自然前向，不阻断开发测试或历史确认。

## 7. Implementation Plan / 精确范围及顺序

新增 `backend/services/advisory_model_first/entry_price_role_binding.py`、`entry_price_daily_service.py`、`entry_price_delivery_cli.py`。修改 `backend/services/advisory_forward/service.py`（末尾有界接入）、`backend/routers/advisory.py`、Advisory页面与API类型。独立角色service已由配套设计负责，不重复实现。需要状态合同放在上述新文件，避免无必要扩张到公共scheduler/registry。

实施评审补充的精确Advisory范围：`backend/services/advisory_model_first/realtime_feature_source.py`与`fresh_hmm.py`仅增加可选的总预算检查回调，默认行为和数值计算保持不变；对应已有实时特征/HMM测试补截止与parity。daily service中的消费者adapter复用Program/Selection repository的现有`conn_factory`注入和权威calendar只读接口，所有SQL按剩余预算缩短statement_timeout，连接建立显式限时；HMM按行业/递推块检查预算。不得修改公共DB连接池、Selection或QE源码，不使用全局monkeypatch。GET不触发calendar cache刷新或落盘。达到预算时停止未开始的小步骤并返回DEFERRED；已原子发布的结果保持可恢复，不能只在整次推理结束后记录耗时。

日级发布复用descriptor锁的文件名/OS锁身份，但以nonblocking方式取得，竞争立即DEFERRED；不修改原公共binding锁。发布时同时短暂持有active角色锁与目标artifact锁，避免角色CAS与最终发布竞态；推理不在锁内执行。

新增对应 `backend/tests/advisory_model_first/test_entry_price_role_binding.py`、`test_entry_price_daily_service.py`、`test_entry_price_delivery_cli.py`；扩展现有Advisory forward定向测试和Advisory UI spec。计划不写数据库，若实际方案需schema或DML，必须先另作具体设计并按DEV/生产授权处理，不能以本设计授权推断。

顺序：版本binding及CAS/readback → 日级有限工作项与fake-clock回归 → UI/API状态 → 历史单日/批量parity → 多轮审核/CI → 合入 → 用户重启 → 精确binding apply及API读回 → 下一合法capture/settle。配置发布需核对当时授权是否覆盖精确Program/binding目标；本轮文档提交合入授权不等于后续模型激活授权，重启仍由用户负责。

## 8. Verification Plan / 完成判据

测试保护业务行为：两个Program共包而binding隔离；Ranking轮换不覆盖entry；不兼容scope不能搬用；文件篡改/并发CAS/崩溃恢复；M3不可用仍返回entry；无配置保留原基线；capture盘前成功、盘后missing不可补、settlement晚到可恢复；QE占用不交叉运行；超时不饿死基线与其他Program；旧模型observation不计数。

历史测试用fake clock覆盖完整一天，不需要等待交易日；真实运行读回分别报告SOURCE_MERGED、USER_RESTARTED、BINDING_APPLIED、PREDICTION_PUBLISHED、SETTLED。若只完成前四项必须明确settlement待成熟，不声称业务闭环完成。不能用仅进程存活、GET返回200或空AVAILABLE作为验收。

用户重启后读取role status、V2 model-shadow及每日artifact，核对同bundle/hash和逐股价格；同时核对基线候选/顺序/ReviewPolicy未被价格通道改变。声明完成仅针对entry预测展示，不声明价格带交易有超额收益。

## 9. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-530 | Program/binding级ENTRY_PRICE独立指针，确认scope严格匹配 |
| F-531 | CAS/锁/原子发布/rollback及旧descriptor隔离 |
| F-532 | 管理CLI与只读API职责明确、状态和identity可读 |
| F-533 | 每日同核capture时钟、无回填、有效期与真实部署时钟一致 |
| F-534 | 有界scheduler接入、QE资源冲突延后、基线连续 |
| F-535 | 真实prediction后结算、正常停牌保留、错误不伪装成功 |
| F-536 | 幂等恢复、跨角色不混样本、质量信息不自动调参 |
| F-537 | 合入/用户重启/binding/预测/结算分报，生产无DDL |

## 10. Design Acceptance Matrix

本矩阵验收源码及定向回归；SOURCE_VERIFIED不表示生产完整交付。坐标前置项由v2关闭，PR #5099 CI/合入、用户重启及未配置状态语义验收已完成；确认窗口、模型效果、精确binding发布和真实prediction/settlement仍未完成，不降低§8最终验收条件。UI独立entry浏览器用例已由隔离临时前端验证通过。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-530 | §4.1、§4.3；entry_price_role_binding.py | backend/tests/advisory_model_first/test_entry_price_role_binding.py | SOURCE_VERIFIED | none |
| F-531 | §4.2 | backend/tests/advisory_model_first/test_entry_price_role_binding.py | SOURCE_VERIFIED | none |
| F-532 | §5；router/CLI | backend/tests/advisory_model_first/test_entry_price_delivery_cli.py；test_forward_api.py | SOURCE_VERIFIED | none |
| F-533 | §6.1 | backend/tests/advisory_model_first/test_entry_price_daily_service.py | SOURCE_VERIFIED | none |
| F-534 | §6.1、§7；forward/service.py | backend/tests/advisory_model_first/test_entry_price_daily_service.py；test_forward_date_clock.py | SOURCE_VERIFIED | none |
| F-535 | §6.2 | backend/tests/advisory_model_first/test_entry_price_daily_service.py | SOURCE_VERIFIED | none |
| F-536 | §6.2～§6.3 | backend/tests/advisory_model_first/test_entry_price_daily_service.py | SOURCE_VERIFIED | none |
| F-537 | §8、§11～§13 | backend/tests/advisory_model_first/test_entry_price_delivery_cli.py | SOURCE_VERIFIED | none |

## 11. Rollout / Rollback

部署实现不自动发布binding。依次确认scope、source SHA及用户重启状态，再apply精确role文件；仅在覆盖精确配置目标的授权范围内执行，不能读取旧包指针直接覆盖。回滚通过相同CLI的CAS禁用或恢复该角色，基线和Ranking继续；不通过git回滚生产目录，不自动重启。历史confirmed结果保留不可变。

## 12. Risks / 工程限制

先捕获的价格与稍后重新计算可能因源修订不同：正式展示优先持久化prediction，发生身份漂移返回冲突。30秒预算必须由支持超时的只读输入调用执行，不能仅结束后记录超时；不能因预算结束丢掉已经原子发布的artifact。若现有调用无法中止，实施时改为可恢复小步骤，并先评审该Advisory内部改动，不创建新worker平台。

新策略包缺对应模型/确认是明确依赖，不能把老包v4强绑到新包。自然样本缺失要显示missed日期与原因，不能宣传“被动积累中”而没有具体调度入口。

## 13. Production Gates / 四项符合性

本次源码：DB、依赖安装、训练、binding、进程操作均noop；后端重启=user，role文件apply需明确目标，runtime evidence待实际读回。v2角色准备必须引用同一v2确认identity；旧v1角色仍可读，不自动迁移。DESIGN-COMPLIANCE-001：入口、状态和恢复源码已交付定向验证，真实业务仍按§8独立验收；错误可见；候选/交易语义不变；复用授权和现有工作流，不私增审批或治理平台。

## 14. 2026-09-28 文档审核与修订记录

- 第一轮（源码与合同）：核对P0-D短路、M3依赖、V1全字段、旧包级binding和旧test回放入口；明确entry-v2、Program级独立指针及新历史推理，保留训练来源约束。
- 第二轮（跨文档与证据）：修正旧文档“等待首日结算”和自动积累误述；统一先源码/用户重启、确认通过后apply的顺序；确认使用已有CONFIRMATION枚举。补充窗口消费、模型与tick空间分报、失败/欠功效/输入不全分类，禁止事后改门槛。
- 第三轮（可实施性与权限）：补齐不可用响应的nullable身份、条件式API兼容、CAS、时钟恢复及有界步骤；明确只读QE前检不等于原子资源互斥，严格不并行须有已确认独占时段。文档合入不授权具体运行绑定，不把DESIGN_READY当源码或模型通过。
- 本次仅审核设计，未运行模型、数据库或业务测试。七份文档分别运行F2结构检查并进行diff/链接复核；正式输出以PR的最终提交与CI结果为准。没有缩减条款、规则替代模型或把设计完成冒充业务交付。

## 15. 2026-09-29 源码审核与本地验证

多轮审核修复包括：预算传播至连接/SQL/权威PIT校验/HMM循环；最旧未尝试工作项优先，耗尽预算的capture不饿死待settlement；逐Program错误隔离；禁用/历史binding预测可继续结算；历史list读回不错误重绑；原子发布用非阻塞角色锁，忙时DEFERRED；artifact完整scope与连续/tick价格重新核验。

既有136项定向回归通过，BUG-1623最终Advisory模块门禁为1,029项通过、6项跳过；前端类型检查0诊断、独立entry浏览器用例1 passed（现有依赖、独立3312临时端口、无后端/DB）。本地审核阶段未发布真实角色、未生成新自然样本或正式历史确认结果；测试fake clock不作为PROSPECTIVE_OOS证据。原有生产目录中的旧样本数量没有因这些测试增加。v2坐标已PASS，但confirmation尚未产生，因此仍不能执行binding apply。

2026-09-30状态更新：PR #5099已合入、用户已重启，BUG-1623运行语义验证及PR #5102 close-sync完成。只读status显示configured=false、NOT_CONFIGURED、database_written=false，collection来源标为SCHEDULER_MEMORY_NOT_DURABLE_EVIDENCE；可证明源码/状态读取正常，不能证明实际预测积累。BUG-1632消费者分页修复合入后，后端再次加载该修复仍由用户执行；离线新进程可验证源修复，不冒充后端生效。DB、角色发布及进程控制继续noop。
