# RD-Agent 因子研发辅助整合详细设计

日期：2026-10-08；版本：1.1；级别：F2（跨 AIstock 研究消费与 RD-Agent 生成边界）；状态：AIstock 离线读取/提案检查已实现，生成端仍待 owner，未运行因子研究。依据[蓝图 v1.8 §20](factor_research_evolution_blueprint_20260908.md#rdagent-integration)与[方法论 v2.5 §2.5](../analysis/factor_research_methodology.md#rdagent-assistance)。本文落实接口、映射、实现范围和验收，不复制或改变方法论 C-1～C-7。

1.0（PR #5756）仅交付设计及蓝图/方法论入口链接，当时没有代码实施。1.1 仅实施 §8 的 AIstock 离线边界，实测和未交付项见 §13；不改 Skill、数据库、数据集、模型配置或研究计算。P1/P2/研究比较既有历史验收与研究结果不追溯修改。

<a id="scope"></a>
## 1. 背景、目标、范围与非目标

AIstock 因子研发继续为主线：Codex/Claude 提出和审查假设，AIstock 提供确定性评价与历史记录，RD-Agent 提供可选经验和候选。不恢复其整套 SOTA 自主循环，不将检索可用、旧回测成功或更强模型当作新因子有效。

交付目标：一次研究可以按问题读取已有经验，选择直接研发或取得 RD-Agent 候选，通过相同受审查脚本/评价路径得出结论，后续窗口从原研究表恢复。经验或生成支路不可用时，返回实际缺口，不阻塞现有研究。不要求在两节点全量盘点后才开始使用。

不新增表、枚举、writer、HTTP/MCP/UI、常驻服务、调度器、向量库、embedding 批处理、知识图谱、模型网关、审批或资源门禁。不修改官方指标、相关性/评级引擎、QE/数仓、股票池、交易规则或数据集；不导出、补录、冻结或全量哈希数据。完整评价仍是既有研究交付要求，不增设生产准入线；正式因子入库继续由用户批准。

<a id="baseline"></a>
## 2. 已有能力与真实缺口

源码核对基线为 AIstock `29a862fadb23d3d7bbf0f8b2446a7bd2ae08e3b5`；RD-Agent 为本地 `F:/Dev/RD-Agent-main` 执行时只读源码，不据此宣称任何节点已部署这些能力。

| 现有位置 | 已确认事实 | 本设计处理 |
|---|---|---|
| `scripts/factor_research.py` | list/show/context/run/record/attach；salvage 系列在 DB configure 之前分发 | 新增两个离线纯读子命令，同样在 configure 之前分发；旧参数/返回不变 |
| `factor_research/service.py:context` | 返回表达式、声明范围依赖提示、指标和相关性；指标日期筛选为 calculated_at | 读取已有输出，不把它视为全库搜索或行情日期过滤；不改 service/repository |
| `factor_research/repository.py:show/list` | 研究任务、records、分页和 revision 已有 | 由已有命令提供相关结果；新辅助不增加 SQL 或隐式写入 |
| `factor_research/rdagent_salvage.py` | source_inventory/candidate_groups/review_candidates 等 JSONL、代码身份、同名冲突及适配 | 优先消费已有 source_inventory；不运行 salvage 扫描全部节点，review_candidates 不能代表全部研究历史 |
| `rdagent_candidate_service.py:get_task_loops` | 会确保任务入库；缓存未命中可能访问节点并写缓存 | 新纯读辅助不调用，不假装 GET 无写入；需要新读取能力交 owner |
| `rdagent/components/workflow/rd_loop.py` | RDLoop 构造 coder/runner，后续包含执行和反馈 | 不实例化整条 Loop，不通过启动后及时取消实现“仅生成” |
| `rdagent/components/proposal/__init__.py` | HypothesisGen 与 Hypothesis2Experiment 分开请求 LLM | 可供 RD-Agent owner 提取生成边界，AIstock 不直接导入 RD-Agent 包 |
| `rdagent/scenarios/qlib/proposal/factor_proposal.py` | convert_response 构造 QlibFactorExperiment，并按名称过滤历史任务 | 不能直接复用为纯提案接口；需保持同名异公式并跳过实验对象创建 |
| `rdagent/scenarios/qlib/experiment/factor_experiment.py` | 默认 scenario 读取旧场景/数据介绍；experiment 构造 workspace | 生成时使用本次问题包提供的语义，不隐式绑定旧数据或创建实验 workspace |

CoSTEER 的 pickle、独立向量库和实时节点记录并非本设计首版必读源；不声称已全面解码或已接通 RAG。生成适配是明确待交付项，不以现有类存在或 mock 通过冒充完成。

<a id="architecture"></a>
## 3. 架构与职责

```text
既有研究/目录查询结果 + 已提取 RD-Agent 记录 + 精确知识文本
                             ↓ experience（只读）
                         研究上下文包
                             ↓ Codex/Claude
              直接假设/代码 ←┴→ RD-Agent 仅提案入口（owner）
                             ↓ proposal-inspect（只读）
                    受审查候选及明确输入
                             ↓ 原 run/完整评价/比较
                    原 record/attach 记录结果
                             ↓ 用户批准后正式交付；QE另交接
```

辅助层只做记录读取、来源映射与提案检查，不读行情面板、不执行候选、不自动选赢家。模型生成、候选计算和数据库登记分别可见；不把检索输出自动喂入执行器。新模块保持惰性导入，读取帮助/本地 JSON 不建立 DB、节点或 LLM 连接。

<a id="experience"></a>
## 4. 经验读取接口契约

### 4.1 CLI（1.1 已实现）

```text
python scripts/factor_research.py experience --input <request.json> --format summary|json
python scripts/factor_research.py proposal-inspect --input <proposal.json> --request <proposal-request.json> [--context <context.json>] --format summary|json
```

两个命令不接受 env-file/target，不调用 configure，不写文件/数据库、不联网、不执行代码。输出复用 models.response/encode 的包络；context.json 是调用者保存的 experience 完整 JSON 输出。需要查询现有 DB 时，先使用已有 list/show/context 的显式目标及授权；辅助层仅消费其结果文件。输出如需留存，使用当次允许的 X 盘研究目录和既有记录流程，不创造自动导出/同步命令。

### 4.2 experience 请求

版本 `factor_research_experience_request_v1`；必填 `schema_version`、`query`、`sources`；可选 `limit`（默认20，正整数）、`offset`（默认0，非负）。分页是输出控制，不是资源准入门禁。

| 字段 | 规格 |
|---|---|
| query | `problem` 非空；`terms` 为显式关键词数组，至少一项；可选 `factor_names/input_fields` 数组；不以 LLM 隐式扩写词项 |
| sources[] | 非空数组；唯一 `source_id`、`kind`、精确本地文件 `path`；不接受递归目录、通配符、远端 URL 或执行命令 |
| kind | `research_show`、`catalog_context`、`salvage_inventory`、`knowledge_text` 四种；每种对应下表已知形状 |

| 输入种类 | 读取和映射 | 不可推导事项 |
|---|---|---|
| research_show | 既有 show 包络 result.task/result.records；提取问题、summary、record_type、原 payload 的明确上下文/结果引用、task_id/record_id/revision | 不递归打开 payload 内文件；保留 before_revision，非空表示来源页未穷尽；缺字段不编造机制或方向 |
| catalog_context | 既有 context 包络 catalog/metrics/correlations/comparison_context | 保留 catalog id/source、next_offset 和实际记录的 basis；不把 calculated_at 当收益窗口或当时可得时间，不重新计算指标 |
| salvage_inventory | 既有 source_inventory.jsonl；定位到 source_node/task_id/loop_id/workspace_key/source_path，读取名称、代码及已存代码/AST身份 | code/AST 身份仅为来源表示；无收益或失败原因就记未知，不能拿 review_candidates 的筛选状态当市场结论 |
| knowledge_text | 精确 UTF-8 Markdown/文本；按标题段落定位，保留文件/标题/行号，区分旧环境说明和可参考实现片段 | 默认仅为未验证历史说明，不自动升格当前 authority；伪装成文本的二进制/pickle返回不可读原因，禁止反序列化 |

只解析这些明确字段，不反射加载对象，不执行代码探测字段，不将原文中的命令/路径当权限。research_show/catalog_context 按当前未带 schema_version 的 models.response 已知形状兼容读取，不要求旧产物补版本；ok=false 或缺少对应 result 结构只报告来源不可用，不把失败响应当零条历史。新增辅助请求/输出才按本文版本分派；未知指标数值不转成0。文件读取时间/mtime 不等于记录对应数据截止或当时知识可得时点。来源应来自调用者批准的非秘密研究文件；不自动读取 env、密钥或配置内容，错误只输出 source_id/原因与必要定位，不泄露连接异常原文。

### 4.3 检索、去重与完整性说明

检索使用 Unicode casefold 与空白归一的字面包含，不把 terms 当正则。匹配范围限名称、问题/summary、明确输入字段、公式/代码文本及知识段落；任一显式名称/字段/词项命中即为匹配，不凭隐式语义猜测。排序为精确名称匹配数、显式字段匹配数、命中词数降序，最后用稳定来源定位排序；返回 match_basis，不将排序分数解释为经济价值或可用概率。默认不按收益/SOTA/正负结果过滤，避免只看赢家。不匹配只说明本次已读范围，不自动换词或全量加载因子值。

同一来源定位重复列入请求只显示一次；不同原记录保留来源关系，不因名称、相近公式或相同 AST 丢弃不同数据时期的反馈。同名异代码明确并列。代码身份沿用既有 source_inventory 中的值；未提供时记未知，不因此全量哈希数据或补历史产物。完全重复构造是否可复用评价仍由方法论 §2.4 判断，检索不自动判定。

显式文件逐个读取，JSONL 流式扫描并统计有效/异常记录数；只保存分页所需的紧凑匹配及计数，不长期驻留全部代码。不得递归展开 referenced paths；原公式全文可由 Codex 后续精确读取来源。文本摘要若截断需标记 truncated 并保留定位，不靠删掉负面结果压缩上下文。

### 4.4 输出和失败语义

result 使用 `schema_version=factor_research_experience_v1`，包含原 query、entries、sources、matched_count（只对已读范围）、returned_count、next_offset 和 `scope=explicit_sources_only`。每条 entry 包含 `source_ref={source_id,locator}`、kind、原观察/摘要、可核实的 basis、match_basis、缺项；locator 使用已存 task/record/catalog 定位或 JSONL 行号/文本标题与行号，保留类型，不拼接成可能歧义的名称。LLM 后续解释另存在研究记录，不覆盖原观察。来源文件只有部分页或只覆盖部分任务时，即使读取成功也不宣称知识库完整。

每个来源报告 `read_state=read|partial|unavailable`、已读/异常行数、既有源分页标记；原因限实际发生的 missing/unreadable/invalid_format/unsupported_kind，未知内容类型不猜测。文档适用性默认 unverified_historical，旧环境事实不能自动覆盖当前合同。来源更改导致读取失败或观察到前后 size/mtime 不一致时注明 source_changed，仅限制该来源的解释；不冻结文件、不计算内容 hash，也不承诺 size/mtime 能证明不可变。

请求结构无效：既有 ResearchError 包络、退出码1，不读取额外源。请求有效但部分/全部源不可读：退出码0表示查询动作完成，result 明确 `retrieval_status=partial|unavailable`；全指定文件正常读取时为 `available`。另列 `match_status=matched|no_match_in_read_scope|not_evaluated`，全部来源不可读用 not_evaluated；分页 offset 超出范围也不抹掉已有 matched_count。`ok=true` 不表示找到经验、更不表示覆盖全部历史。来源不可用不阻断直接研究，实际错误不可伪装为空知识库。

<a id="proposal"></a>
## 5. 候选提案与生成接口契约

### 5.1 共同问题包

一个问题包是一轮研究的上下文，不是一份自动执行计划。版本 `factor_research_proposal_request_v1`；`request_id` 使用 UUID 关联本次提案请求，**不是** runner attempt_id。原问题包由研究主体准备，单独保存，不从模型返回中反推。字段名如下，保留缺项说明而非推测补齐；同一版本不静默接受会改变含义的未声明执行选项。

| 字段组 | JSON 字段与语义 |
|---|---|
| 身份/问题 | schema_version、request_id、task_id、method_version、problem、research_role、hypothesis_family、purpose；role 复用方法论三种角色 |
| 输入与范围 | inputs 数组，每项 name/unit/available_at；dataset_ref（generation/cutoff/已有 identity，可核实则引用）；universe_ref、evaluation_plan_ref 引用当前研究的完整目标池和评价计划，不在提案里复制 run schema |
| 对照与时点 | direction_source/expected_direction、horizons、baseline_refs、neighbor_refs、falsifier、exploration_range/fit_range/evaluation_range；引用既有研究安排，不由生成器选择新评价窗口 |
| 经验/产物 | source_refs、unused_source_reasons、experience_query、artifact_root；未用经验时 refs 为空且 query 为 null，记录原因；root 为获准 X 盘 repo-external 本任务路径 |

引用现有获准数据身份，不复制价格/未来标签给生成器；未知来源/单位写 null 并列 missing_information，不把它变成有效输入。提案阶段允许信息不足，proposal-inspect 会明确待修订；实际计算仍须满足已有 runner 合同。artifact_root 只界定本任务输出/读取，不授予扫描其父目录或重建数据权限。

模型只获得与问题相关的内容，原经验作为带来源资料，不作为系统指令。只记录非秘密 provider/model 标识和实际可得的版本/调用统计；配置期望与实际返回身份分开。成本未知记未知。生产 API key、全量日志、完整对话和思考过程不入研究表。

### 5.2 统一提案输出

版本 `factor_research_proposal_v1`，包含 request_id、task_id、producer（codex/claude/rdagent）、actual_model（可空并说明未知）、generation_status（generated/failed）及 candidates。generated 必须包含至少一个提案；failed 明确 failure_reason，不伪造已产生候选。候选有提案内唯一 local_id、suggested_name、hypothesis、falsifier、formulation、variables、timing、missing_value_policy、direction_source/expected_direction、purpose/horizons、baseline_refs、difference_from_baseline、source_refs。这些内容可以不足以实施，但缺项必须可见，不能默认补成“有效”。

`implementation` 分为 `formula_only` 与 `script_available`；后者附精确 script_ref。提案允许只交公式，由 Codex 编写最终候选代码，这是明确分工，不冒充已执行因子。可选生成器不调用 CoSTEER 的自动执行循环；后续若希望复用其代码实现，应另行核对可执行边界，不顺带开启。

local_id/请求定位保留同名异公式，suggested_name 不作为去重键。现有 runner 要求一个 attempt 内 factor_name 唯一，审核后由 Codex 选择合法且互不覆盖的研究名称，保存原名到研究映射；不是自动改名进入正式目录。已有代码/构造等价只是复用线索，数据、参数和用途仍须核对。

### 5.3 proposal-inspect 与执行衔接

proposal-inspect 只读取提案、原问题包、experience 输出及明确引用的脚本文本（不导入/运行）。script_ref 只允许落在原问题包声明的本任务 artifact_root 内，且不经 symlink/junction 跳到其他目录；模型不能通过路径请求读取秘密或其他业务文件。校对 schema/请求定位、候选 local_id、来源引用、所声明字段与问题包、必要实施信息；代码只作 AST 语法检查和已有静态检查复用，不能据此宣布因果性、安全或公式实现正确。来源/字段存在只表示可定位，不保证数据覆盖、时间对齐或市场价值。

`--request` 独立读取 §5.1 原问题包，`--input` 只读取 §5.2 模型提案；两侧 request_id/task_id 一致，禁止只验证模型自带的 request/response 自洽。使用经验时须提供 --context；experience_query 与其 query 一致，选用 source_refs 必须在原包及所给 context entries 中可定位。来源缺失/分页未提供对应项或问题包与输出冲突列具体 finding，不自动修复公式、删字段或收缩股票池。未使用经验的请求可省略 --context；不可强迫直接研发伪造空查询或引用。旧直接研究不必经过新提案命令。

返回 result `schema_version=factor_research_proposal_inspection_v1`，列 `findings[]`、每候选 `implementation_status`、`inspection_status=reviewable|requires_revision`、`execution_performed=false`。格式无法解析退出1；可解析但尚有缺项退出0并明确 requires_revision；reviewable 只表示可进入现有代码审核，不是批准执行/入库。formula_only 可以是可审查的研究提案，但执行仍需补受审查脚本。没有强制机器晋升阈值或新审批层。

之后沿已有 create/record 登记研究取舍，由 Codex 准备原 run 请求；candidates **仍只含 factor_name/script**，不能把新 provenance 字段塞入严格 run schema。来源和 request_id 存 task.context_json 或 record.payload 中的 `assistance` 对象；run 的数据、股票池、日期、full_evaluation/comparison 仍按现行合同显式准备，不由模型响应覆盖。没有新增“inspect 后自动 run”命令。

<a id="owner"></a>
## 6. RD-Agent owner 交付与跨模块边界

提出一项需求 `RD-ASSIST-01`（本文局部交接标识，不新增 BUG/平台）：在现有获准模型配置中提供“问题包→提案 JSON”的离线单次入口；具体 CLI 名由 owner 在实现 PR 确认，本文不编造当前可运行命令。输入/输出精确采用 §5；交付后因子窗口可读取同一文件合同，不需要在 backend 导入 RD-Agent 或新增远端编排器。

owner 可复用现有 hypothesis/template/APIBackend 的合适部分，但必须：使用问题包而非旧默认 scenario 数据说明；只提取公式描述和 variables，不创建 QlibFactorExperiment/workspace、不按名称抛弃候选；不构造 RDLoop/coder/runner、不训练/回测、不访问行情目录、数据库或旧 SOTA 接受器。仅模型调用与本任务 repo-external 提案输出允许有副作用；输出及所用 SDK 的缓存/日志若有，均使用现有获准研究位置，不污染代码仓库或共享业务任务。

原 Hypothesis2Experiment 的重试/字段清理不能默默改变问题包：格式重试沿既有客户端设置并披露实际调用；改变公式、字段或方向属于候选修订，不以错误兜底隐藏。接口不能满足时报告不支持，不依赖用户及时取消来限制执行。模型能力升级复用现有配置，不因此安装依赖、改生产 Conda 或创建新模型代理。

真实验收由 owner 提供一次受授权的 fresh-process 提案生成、实际模型身份或未知说明、无实验/行情/业务写入的执行范围说明；因子窗口用真实返回验证 §5 映射。构造 fixture 只证明消费合同，不能代替这项端到端交付。未交付则 B 支路保持“待 owner”，A 与 AIstock 直接研发继续，不对外声称整合全部完成。

如需读取实时 RD-Agent DB/节点而非既有产物，另提精确只读需求；不得复用 get_task_loops 的隐式同步。QE/数仓/数据/评级无需为本版修改；后续真实策略对照仍交 QE owner，研究不等待其排队完成。

<a id="execution"></a>
## 7. 统一评价、记录与恢复

候选来源不会改变评价。技术有效且无等价完整结果的本轮股票候选完成[完整独立评价](../analysis/factor_research_methodology.md#full-evaluation)：完整目标历史 PIT、全有效历史、h1/h5/h10/h20、存量可评价相关，优先解释2024以后、逐年及近期。复用已有 runner/full_evaluation/comparison 与官方基础口径；行业/条件角色按方法论对应范围，不凭无条件 IC 否决。

研究层完整指标和用途比较不等同 QE 收益，旧 SOTA、低相关、LLM 赞同和实现通过都不是接受结论。两个信息/用途结论轴及 C-1～C-7 原文不在本文重定义。相同模型/数据的经验有无比较优先，其后才比较生成路径；模型升级单独保持其余条件可比。问题、总预算口径、参考范围和评价方法事先说明，两侧上下文接触如实披露，历史回放不冒充未接触 OOS。样本少只报告个案，不估总体发现率，不以生成数定成败。

提案讨论沿用 progress/decision/request/correction 等 records；只有正式计算才使用现有 attempt/run 生命周期，不能把 LLM 格式重试冒充多个独立科学假设。记录实际公式/参数/方向尝试与已查看的范围，未知不补齐。紧凑 assistance 内容为 request_id、producer/model、实际来源引用/采用理由、候选到研究名称/脚本的映射及结果引用，不复制全库/完整对话。

来源查询本身不登记研究事务。研究写入在当次获准 DEV/生产目标上通过原 writer 完成；已有 --target production 不是授权。普通记录重试复用原 record_id/请求；计算成功但登记失败按 attach 恢复，不能重算；不双写 RD-Agent KB。已完整评价且不支持的规格只留最小结果，不追加抢救、归档或连续变体；未来按新问题检索即可。

<a id="implementation"></a>
## 8. 实施方案与文件所有权

| 工作包 | 因子窗口范围 | 结束条件 |
|---|---|---|
| A 经验辅助 | 新纯读取模块 `backend/services/factor_research/assistance.py`；扩展 `scripts/factor_research.py` 两个离线分发；新增紧凑 `backend/tests/factor_research/test_assistance.py` | 精确源读取、可见缺项、稳定定位、无副作用、旧入口不变；真实相关产物读取有结果或明确原因 |
| B 提案消费与比较 | 同一 assistance 模块的提案检查与现有记录/runner 衔接；RD-ASSIST-01 由 owner 实施 | 消费合同、owner 真实提案读回、获准研究的完整评价及用途结论分别有状态；无增益可停生成支路，不抹掉 A 的成果 |

A/B 是同一增补工作的连续包，不建立新阶段/审批状态机，不为未知 owner 接口预建启动器。提案只读检查与经验读取可同一次实现；真实生成和研究计算分别授权，不靠等待接口交付停止新因子研究。

默认不改 service.py/repository.py/runner.py/models.py、数据库 schema、官方评价或 RD-Agent 源码。复用 helpers 若发现签名/副作用不合适，在新增纯模块做必要的字段适配，不复制指标和 writer；超出明确文件范围先登记实际缺口。需要 nox 收集时只把新文件加入所属既有计划，不扩大测试矩阵。现有 factor-research/develop-factor skill 已引用方法论；如实际 CLI 使用需要入口说明，仅对获准的现有因子 skill/对应 Claude command 另列精确修改，不新建 skill 家族。

文件归属按 actual changed files → file_ownership.yaml → module_registry.yaml → test_plans.yaml。当前研究源码归 factor_library.research；普通计划 factor_research_backend。DEV 计划是 delegated/runner_enabled=false，适用条件当前列有 repository/service/dev tests/migration，本设计默认不触及这些路径。纯离线读取与文档不跑 DEV 写库测试；如果实施必须改变 SQL/事务/数据库调用合同，按实际变更申请既有 DEV 验证，不改 ownership 规避。

<a id="verification"></a>
## 9. 验证方案与可合入标准

保持小型参数化合同，不为每个字段复制一套 CLI/repository/恢复测试，不机械重跑全库评价。新增源码按所属最小门禁验证；原因果性、PIT、四周期、事务/attach 直接复用已覆盖测试，不在新模块重建测试债务。

| 合同用例 | 断言/能发现的错误 |
|---|---|
| 四类来源＋分页 | 读真实形状的缩小 fixture；来源分页不被说成完整，calculated_at 不冒充行情时期；只读不调用 configure/DB/API |
| 丢失/损坏/伪文本 | 一个有效源搭配缺失/坏 JSONL/二进制；有效结果保留，partial/unavailable 明确，不执行 pickle，不伪装无匹配 |
| 检索及重复 | 输入顺序置换输出排序一致；同名异代码、相同代码不同版本反馈均保留；不因 SOTA 或收益字段优先筛赢家 |
| 旧经验/指令文本 | 旧路径/退市过滤/执行命令只作为历史文本；不递归读引用、不执行，不覆盖问题包 |
| 提案身份/角色 | formula_only、script_available、原请求错配、缺字段/缺 source_ref、script_ref 越出声明目录；不能以模型自带原请求绕过对照，只给 inspection 结果，名称冲突保留，无自动 run/写库 |
| 真实衔接 | 构造已审查本地脚本走现有参数合同，provenance 留研究 payload，不增 run 字段；fixture 不证明 RD-Agent owner 已交付 |
| owner 提案烟测 | 单次真实生成得到公式 JSON，不创建实验 workspace/启动 coder/runner；任何自动实验路径使该支路验收未通过，不阻断 A |
| 研究效果 | 明确问题、共同数据和可比预算；每个技术有效候选有完整评价或真实失败；结论允许无增量，不按预设收益验收 |

错误/异常用例应能在故意触发原错误行为的实现中失败，不能只断言函数返回非空。代码验收关注行为和隔离，不把 AST 检查冒充执行安全，不用模拟收益证明有效因子。后续按所属计划执行必要 lint/compile、定向测试、git diff --check、L0/适用 CI；registry 验证仅在对应 catalog 变更时运行，不增加无关测试。

1.0 文档走 docs-fast-new/update，多轮审查与修订后通过 PR #5756 合入。设计接受与实现/研究通过分开，当时未触发 DEV 表验证或因子计算；1.1 代码验证见 §13。

<a id="acceptance"></a>
## 10. Design Acceptance Index 与 1.0 设计验收历史

F-001 主线和无平台边界；F-002 来源读取/查询完整性；F-003 提案合同/非执行；F-004 RD-Agent owner 交接；F-005 完整统一评价；F-006 原记录/恢复；F-007 最小范围/验证；F-008 发布/权限与可回退。编号仅为本文验收映射，不是新业务门禁。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 本文 §1/3 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#architecture | 用户批准设计先行 | 用户批准本轮仅设计；代码未实现，不替代 AIstock 主线 |
| F-002 | 本文 §4 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#experience | 用户批准设计先行 | 用户批准本轮仅设计；源读取与无副作用待实现测试 |
| F-003 | 本文 §5 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#proposal | 用户批准设计先行 | 用户批准本轮仅设计；规划 CLI/JSON 不能声称可运行 |
| F-004 | 本文 §6 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#owner | 用户批准设计先行 | 用户批准本轮仅设计；owner 真实仅提案入口未交付 |
| F-005 | 本文 §7/9 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#execution | 用户批准设计先行 | 用户批准本轮仅设计；未计算、未发现或晋升新因子 |
| F-006 | 本文 §7 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#execution | 用户批准设计先行 | 用户批准本轮仅设计；原 writer 复用，不新增 DB 实现 |
| F-007 | 本文 §8/9 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#verification | 用户批准设计先行 | 用户批准本轮仅设计；后续按真实 changed files 选计划 |
| F-008 | 本文 §11/12 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#rollout | 用户批准设计先行 | 用户批准本轮仅设计；源码/运行/数据/研究分开验收 |

<a id="risks"></a>
## 11. 风险、失败模式与停止边界

经验过时/不全：显示来源与未知，不永久封禁方向，不恢复旧路径；读取失败不触发同步。高收益检索偏差：按问题匹配成功/失败，已接触历史标 research_feedback。源码相同但版本不同：保留反馈 basis，不自动复用数值。模型幻觉/指令污染：提案非命令、代码受审查，事实以当前合同/实测为准。

RD-Agent 默认 scenario/workspace/按名过滤：owner 单独适配，不把完整 Loop 当提案接口。API 不兼容或实际模型未知：明确失败或未知，不更改生产环境、不假称升级有效。异节点不可达或旧 pickle 不可读：只限制该资料/支路，不发起全库抢救。工具对照接触污染：分别组织上下文并披露共同历史，不承诺统计独立或提升发现概率。

实施发现必须改现有合同/数据/跨 owner 业务时，只提交精确需求，不能自行降级、填零或扩范围。没有增益停止生成支路；既定研究问题已被回答或无值得追加的证据时结束，不为满时长、凑因子或存档继续投入。

<a id="rollout"></a>
## 12. 发布、回滚与 Production gates

默认旧命令/研究路径完全不变，新辅助显式调用。实现先在现有环境验证离线 fixture，再读取当次允许的真实产物；owner 支路有真实交付后才说明端到端可用。生成和计算按具体授权，不因文档合入启动长任务。需要回退时停止使用新辅助并走原 CLI/直接研发，不删除任务记录、历史结果或源码产物；不改数据库或关闭任何服务。

1.0 文档交付的 `runtime_impact=none`、`backend_restart_required=false`。1.1 代码分类见 §13，不能把文档 none 复制为代码结论；backend_restart_owner=user。两次均保持 `production_ddl_gate=noop`、`production_dml_gate=noop`、`dependency_install=noop`、`client_install=noop`、`production_activation=false`、`process_control=false`、`research_execution=false`。任何生产数据库操作按精确目标单独授权并先 DEV 验证。

DESIGN-COMPLIANCE-001：不把文档或 fixture 当完整接入；不把缺资料/接口/成本隐藏成成功；不改变评价/PIT/写库/owner 语义；不新增审批或资源限制。1.0 合入只交付本文及两处入口引用，1.1 交付范围见下节；独立 worktree/分支清理需相应授权，不删除已有研究资料。

<a id="implementation-readback"></a>
## 13. 1.1 AIstock 实现、旧资料梳理与验收

### 13.1 实现与使用

新增 `backend/services/factor_research/assistance.py`，原 CLI 在 configure 前分发 experience/proposal-inspect；没有第二个 writer、服务、数据库调用、RDLoop 或生成器启动器。`noxfile.py` 只为原所属计划添加一个测试文件。

JSON 细化：variables 为所用 inputs.name 的字符串数组；baseline_refs/neighbor_refs 为原计划引用字符串数组；研究角色复用 predictive_increment/replacement/conditional。这些是提案信息，不覆盖 run schema。未知语义字段、原请求冲突或缺项返回 requires_revision；formula_only 可以 reviewable，但不能自动执行。script_available 复用既有 inspect_factor_code 的 AST/入口检查，语法通过不证明代码安全、因果性或公式正确，仍须代码审核。

source_ref 保留结构化 locator；长 observation/basis 以带 truncated 标记的片段呈现，完整原文仍在精确来源。检索不执行文本内指令或读取其引用路径。knowledge_text 的 legacy_environment_cues 只是字面词命中，帮助定位旧环境描述，不推测真实行情截止日期；所有来源均标 unverified_historical，当前适用性需研究主体结合原问题包判断。没有自动修旧数据、hash 全数据或刷新官方指标。

操作者在获准 X 盘任务目录保存非秘密请求后，按 §4.1 调用两个入口。来源顺序不影响排序；分页只影响返回数量，不伪装搜索全库。提案的原请求单独提供；用到经验时提供原查询返回，没用则记录不用的原因。检查后由 Codex 准备原 create/record/run 请求，assistance 元数据只进入原 context_json/payload，不进入 candidates.factor_name/script 之外的执行字段。

### 13.2 只读梳理结论（2026-10-08）

读取范围是 `F:/Dev/RD-Agent-main/RAG/01..06` 六个精确 Markdown 文件及 `X:/AIstock_factor_research/rdagent_salvage/20260917-dry-run-1/artifacts-v7/source_inventory.jsonl`。未读取价格面板、生产 DB、节点状态或 pickle；没有重启旧任务或修改 RD-Agent 文件。

| 旧资料观察 | 对本次研究的处理 |
|---|---|
| 01 文档仍称当前 provider 为 qlib_bin_20251209 | 只能证明文档包含旧路径，不能证明当前任何节点实际加载它；不跟随该路径，不继承其数据身份 |
| 01 声称 market: all 已排除 ST/退市/停牌股票 | 不用作当前历史 PIT 规则；以本轮显式 universe_ref 与现行研究合同为准 |
| 02 单位写“可能已转换”，字段说明依赖旧导出 | 不能由它猜测新数据单位/覆盖；inputs.unit/available_at 未知时保持缺项 |
| 03 默认当前目录 result.h5 mode=w、epsilon；05 建议放宽窗口来修 NaN | 可借鉴索引对齐与诊断，不照搬覆盖写入、epsilon 或窗口修改；缺失处理按当前受审查公式 |
| 04 使用旧 close 成交、0.095 阈值，且承认 T+1/日内结构缺口 | 旧收益只作历史反馈，不等同 AIstock 当前评价或 QE 可交易价值 |
| 05/06 的字段核对、保留索引、显式错误、按问题挑经验 | 可作为工程/研究思路线索；不升级为当前数据 authority，也不宣称能保证找到 alpha |

真实 fresh-process experience 查询：7/7 指定源可读，inventory 1,258 行；知识文本分段50条；所选词共命中1,279条，分页返回8条、next_offset=8。这是已读范围的检索结果，不是因子候选数量或价值评估。实际请求保存在本次 X 盘任务目录；没有新增历史证据固化/全库扫描任务。

### 13.3 设计验收矩阵（1.1 实现）与限制

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | assistance.py；CLI configure 前分发 | backend/tests/factor_research/test_assistance.py | passed | 无 |
| F-002 | assistance.experience/_rows | backend/tests/factor_research/test_assistance.py；artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#implementation-readback | passed | 无 |
| F-003 | assistance.inspect_proposal/_script_status | backend/tests/factor_research/test_assistance.py | passed | 无 |
| F-004 | 原 §6 RD-ASSIST-01；提案消费合同 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#owner | 用户批准分工 | 按用户批准详细设计，RD-Agent owner 单次真实生成仍未交付；不宣称端到端完成 |
| F-005 | 原 runner/full_evaluation/comparison 未修改 | backend/tests/factor_research/test_contracts.py | not_applicable | 本轮授权实施代码，不运行新因子研究；完整评价仍按用户批准 §7 |
| F-006 | 原 models.response/encode 与 record/attach 未修改 | backend/tests/factor_research/test_recovery.py | passed | 无 |
| F-007 | 直接测试与 nox 原模块收集 | pytest backend/tests/factor_research/test_assistance.py | passed | 无 |
| F-008 | 无服务/依赖/数据操作；catalog 原分类 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#rollout | passed | 无 |

直接测试经历缺模块 RED→GREEN。两轮代码审查修复了独立请求绑定、路径/类型边界、源漂移/缺失可见性和 AST 入口复用；最终审查逐项覆盖 DESIGN-COMPLIANCE-001：①不以消费端/fixture 冒充 owner 真生成；②不可读/未知/冲突均显式；③不改评价、PIT、DB、跨模块业务；④未新增生产门禁、审批或资源限制。源码合入以最终 HEAD 的 CI verdict 为准，不提前记录 CI 通过。

实际 changed files 分类为 targeted_ci_required，factor_research_backend；dev_db_required=false，unexecuted_test_files=[]。仅修改 nox 收集触发其既有静态/计划验证。按执行时 runtime catalog，新 assistance.py 命中 backend-main：runtime_impact=backend，backend_restart_owner=user；未修改 catalog 降级。**更正运行时说明**：catalog 的 operator_runbook_ref、identity_ref、business_smoke_ref 并非空值，而是 `bug_record.runtime_contract.*` 动态引用。上轮用空 record 调用 BUG 专用解析器，得到的是缺少具体 BUG 记录，不是 catalog 字段缺失，不能据此认定流水线缺陷或为本 feature 新增重启前置。分类结果与实际生效证据分开：当前生产源码调用者只有 scripts/factor_research.py，服务端没有导入；CLI fresh process 可独立使用，不需要启动常驻后端。本轮交付源码/离线入口，不宣称后端 runtime identity 已更新。生产 DDL/DML、数据集/缓存/官方指标、依赖安装、客户端安装、进程控制均 noop。

### 13.4 合入后只读复验与剩余交接（2026-10-08）

PR #5765 已合入，source HEAD `2fbcb38ae4b9de121c7922c5fcb82ec1c881b63c`，merge `7acdc7268338d2d8cb7c178db167ba6314ef5c8b`，CI verdict SUCCESS。主线新进程的 proposal-inspect --help 正常；experience 精确读取 05_runtime_failure_patterns.md，5条命中、分页返回2条，均标 unverified_historical。该只读复验没有重跑全库评价或触碰数据/DB/服务。

余下是 §6 的 RD-ASSIST-01 真实生成交付，而不是“先补 catalog 再重启才能研究”。按已批准职责，RD-Agent owner 在其独立工作树实现单次问题包→提案入口，交回源码 PR/commit、原请求、提案文件、实际模型身份或未知原因，以及没有启动 Loop/coder/runner、访问行情或业务写入的执行说明。生成只使用本轮问题包，不跟随旧数据目录、旧股票池/单位/成交假设，不修改或补齐旧数据集；原始资料只作为带出处的经验参考。

本窗口收到真实文件后，使用主线 `scripts/factor_research.py proposal-inspect --request <original-request.json> --input <proposal.json> [--context <experience.json>] --format json` 读回，核对两侧身份/输入/来源/边界；不以 fixture 代替真实返回，不自动执行候选或入库。CLI 文件均使用获准 X 盘研究目录。若用户希望本窗口实施 RD-Agent 源码，应先明确接管该 owner 范围并同步职责，不能从“继续任务”推导跨模块接管。真实因子评价另按 §7 执行，不以等待生成端阻断直接研究。
