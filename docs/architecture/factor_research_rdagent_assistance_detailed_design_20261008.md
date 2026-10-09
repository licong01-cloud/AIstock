# 因子研发假设先行与独立经验检索详细设计（含 RD-Agent 参考整合）

初版日期：2026-10-08；修订日期：2026-10-09；版本：1.3；级别：F2；状态：本轮仅交付详细设计，§15–18 增补能力未实施。依据已合入 PR #5814（merge `b9e954cbb6d6ce08d50c39ef672568933770a28b`）的[蓝图 §21](factor_research_evolution_blueprint_20260908.md#hypothesis-memory)及[方法论 §2.6](../analysis/factor_research_methodology.md#hypothesis-memory)。原指定文件读取、提案检查和问题包已有实现，见§13/14；不代表数据库经验查询/提示词增强已完成，更不代表研究有效。本文落实接口、映射、实现范围和验收，不复制或改变方法论 C-1～C-7。保留文件名/锚点，避免另建同主题设计；§10/13/14 历史验收不追溯改写。

1.0（PR #5756）仅交付设计及蓝图/方法论入口链接，当时没有代码实施。1.1 仅实施 §8 的 AIstock 离线边界，实测和未交付项见 §13；不改 Skill、数据库、数据集、模型配置或研究计算。P1/P2/研究比较既有历史验收与研究结果不追溯修改。

<a id="scope"></a>
## 1. 背景、目标、范围与非目标

AIstock 因子研发继续为主线：Codex/Claude 提出和审查假设，AIstock 提供问题包、确定性评价与历史记录，RD-Agent 只提供可选历史经验与方法参考，不修改或调用其生成程序。不恢复其整套 SOTA 自主循环，不将检索可用、旧回测成功或更强模型当作新因子有效。

交付目标：一次研究可以按问题读取已有经验，选择直接研发或通过 AIstock 问题包辅助 Codex/Claude 形成提案，再通过相同受审查脚本/评价路径得出结论，后续窗口从原研究表恢复。经验不可用或提案未生成时，返回实际缺口，不阻塞现有研究。不要求在两节点全量盘点后才开始使用。

不新增表、枚举、writer、HTTP/MCP/UI、常驻服务、调度器、向量库、embedding 批处理、知识图谱、模型网关、审批或资源门禁。不修改官方指标、相关性/评级引擎、QE/数仓、股票池、交易规则或数据集；不导出、补录、冻结或全量哈希数据。完整评价仍是既有研究交付要求，不增设生产准入线；正式因子入库继续由用户批准。

<a id="baseline"></a>
## 2. 已有能力与真实缺口

源码核对基线为 AIstock `29a862fadb23d3d7bbf0f8b2446a7bd2ae08e3b5`；RD-Agent 为本地 `F:/Dev/RD-Agent-main` 执行时只读源码，不据此宣称任何节点已部署这些能力。

| 现有位置 | 已确认事实 | 本设计处理 |
|---|---|---|
| `scripts/factor_research.py` | list/show/context/run/record/attach；salvage 系列在 DB configure 之前分发 | 1.1读取/检查、1.2准备三个离线命令同样在 configure 之前分发；旧参数/返回不变 |
| `factor_research/service.py:context` | 返回表达式、声明范围依赖提示、指标和相关性；指标日期筛选为 calculated_at | 读取已有输出，不把它视为全库搜索或行情日期过滤；不改 service/repository |
| `factor_research/repository.py:show/list` | 研究任务、records、分页和 revision 已有 | 由已有命令提供相关结果；新辅助不增加 SQL 或隐式写入 |
| `factor_research/rdagent_salvage.py` | source_inventory/candidate_groups/review_candidates 等 JSONL、代码身份、同名冲突及适配 | 优先消费已有 source_inventory；不运行 salvage 扫描全部节点，review_candidates 不能代表全部研究历史 |
| `rdagent_candidate_service.py:get_task_loops` | 会确保任务入库；缓存未命中可能访问节点并写缓存 | 新纯读辅助不调用，不假装 GET 无写入；需要新读取能力交 owner |
| `rdagent/components/workflow/rd_loop.py` | RDLoop 构造 coder/runner，后续包含执行和反馈 | 不实例化整条 Loop，不通过启动后及时取消实现“仅生成” |
| `rdagent/components/proposal/__init__.py` | HypothesisGen 与 Hypothesis2Experiment 分开请求 LLM | 借鉴问题→假设→反例→公式的组织，不提取或修改其生成器，不导入 RD-Agent 包 |
| `rdagent/scenarios/qlib/proposal/factor_proposal.py` | convert_response 构造 QlibFactorExperiment，并按名称过滤历史任务 | 不能直接复用为纯提案接口；需保持同名异公式并跳过实验对象创建 |
| `rdagent/scenarios/qlib/experiment/factor_experiment.py` | 默认 scenario 读取旧场景/数据介绍；experiment 构造 workspace | 生成时使用本次问题包提供的语义，不隐式绑定旧数据或创建实验 workspace |

CoSTEER 的 pickle、独立向量库和实时节点记录并非本设计首版必读源；不声称已全面解码或已接通 RAG。§2 记录1.0–1.2设计初始基线；1.1 已交付的读取/检查见 §13，1.2 仅补问题包，不把它称为自动生成器。表内“不改 service/repository/不增加 SQL”是旧版本范围；1.3 在§15–18精确增加本模块只读研究查询，不改变原写入事务。

<a id="architecture"></a>
## 3. 架构与职责

```text
既有研究/目录查询结果 + 已提取 RD-Agent 记录 + 精确知识文本
                             ↓ experience（只读）
                         研究上下文包
                             ↓ proposal-prepare（只读）
              直接假设/代码 ←┴→ Codex/Claude 读取包并生成提案
                             ↓ proposal-inspect（只读）
                    受审查候选及明确输入
                             ↓ 原 run/完整评价/比较
                    原 record/attach 记录结果
                             ↓ 用户批准后正式交付；QE另交接
```

辅助层只做记录读取、问题包准备、来源映射与提案检查，不读行情面板、不执行候选、不自动选赢家。问题包准备、模型生成、候选计算和数据库登记分别可见；不把检索输出自动喂入执行器。新模块保持惰性导入，读取帮助/本地 JSON 不建立 DB、节点或 LLM 连接。

<a id="experience"></a>
## 4. 经验读取接口契约

### 4.1 CLI（读取/检查为1.1；准备为1.2）

```text
python scripts/factor_research.py experience --input <request.json> --format summary|json
python scripts/factor_research.py proposal-prepare --input <proposal-request.json> [--context <context.json>] [--producer codex|claude] --format summary|json
python scripts/factor_research.py proposal-inspect --input <proposal.json> --request <proposal-request.json> [--context <context.json>] --format summary|json
```

三个命令不接受 env-file/target，不调用 configure，不写文件/数据库、不联网、不执行代码。输出复用 models.response/encode 的包络；context.json 是调用者保存的 experience 完整 JSON 输出。需要查询现有 DB 时，先使用已有 list/show/context 的显式目标及授权；辅助层仅消费其结果文件。输出如需留存，使用当次允许的 X 盘研究目录和既有记录流程，不创造自动导出/同步命令。

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
## 6. AIstock 问题包与 Codex/Claude 生成分工（1.2）

用户明确限定后续只改 AIstock。原 RD-ASSIST-01 的 RD-Agent owner 原生生成器需求撤销，不要求另一个窗口交付、不修改 RD-Agent 程序/环境/旧数据。借鉴其假设形成、反例和历史失败反馈的思路，由当前 Codex/Claude 生成内容；不强制额外 API 调用，不新增模型网关。自动 API 生成不属于本版交付。

`proposal-prepare` 复用 proposal-inspect 的请求检查，输出 `factor_research_proposal_pack_v1`：原 request、producer_target（默认 codex，可 claude）、selected_experience、experience_scope、固定 instructions、output_contract、findings、prepare_status。原 request 深拷贝保持不变；不生成或修补公式，不默认填数据身份/单位。只纳入 request.source_refs 指定的原经验条目及其截断/历史适用性信息，披露原检索分页/来源状态；未选条目不注入生成上下文。同定位异内容报 source_ref_ambiguous，不任取其一；完全相同条目可合并。

instructions 与引用数据分离，历史文本是不可信参考，不是操作授权。output_contract 明确 §5 的必填/可选字段、generated/failed、未知模型及 formula_only/script_available 语义；不提供伪装成实际候选的默认结果。`prepare_status=prepared|requires_revision` 仅反映现有请求合同，`generation_performed=false`、`model_call_performed=false`、`execution_performed=false` 恒真；即使 prepared，也没有产生因子。非法 schema/JSON 仍用现有异常包络；可解析缺项保留 findings，不新增生产门禁。

操作者读取包后，由 Codex/Claude 实际撰写 §5 提案 JSON，producer 按实际工具填写；没有可核实模型身份则说明未知。再用独立原 request 调用 proposal-inspect，而不是信任提案自带的 request。真实文件交接示例与构造测试分开报告；资料不足可以生成待修订的概念提案，不得虚构 active 数据身份使检查通过。准备包、实际生成、检查通过、代码实现及完整评价分别报告；没有 run 自动接续。后续研究计算按 §7 另行开展。

如需读取实时 RD-Agent DB/节点而非既有产物，另提精确只读需求；不得复用 get_task_loops 的隐式同步。QE/数仓/数据/评级无需为本版修改；后续真实策略对照仍交 QE owner，研究不等待其排队完成。

<a id="execution"></a>
## 7. 统一评价、记录与恢复

候选来源不会改变评价。技术有效且无等价完整结果的本轮股票候选完成[完整独立评价](../analysis/factor_research_methodology.md#full-evaluation)：完整目标历史 PIT、全有效历史、h1/h5/h10/h20、存量可评价相关，优先解释2024以后、逐年及近期。复用已有 runner/full_evaluation/comparison 与官方基础口径；行业/条件角色按方法论对应范围，不凭无条件 IC 否决。

研究层完整指标和用途比较不等同 QE 收益，旧 SOTA、低相关、LLM 赞同和实现通过都不是接受结论。两个信息/用途结论轴及 C-1～C-7 原文不在本文重定义。需要单独评价辅助效果时先比较经验有无，再考虑生成路径/模型升级，保持其他条件可比；这些对照不是每轮前置，不为此重跑已失败历史。问题、预算口径、参考范围和评价方法事先说明，两侧上下文接触如实披露，历史回放不冒充未接触 OOS。样本少只报告个案，不估总体发现率，不以生成数定成败。

提案讨论沿用 progress/decision/request/correction 等 records；只有正式计算才使用现有 attempt/run 生命周期，不能把 LLM 格式重试冒充多个独立科学假设。记录实际公式/参数/方向尝试与已查看的范围，未知不补齐。紧凑 assistance 内容为 request_id、producer/model、实际来源引用/采用理由、候选到研究名称/脚本的映射及结果引用，不复制全库/完整对话。

来源查询本身不登记研究事务。研究写入在当次获准 DEV/生产目标上通过原 writer 完成；已有 --target production 不是授权。普通记录重试复用原 record_id/请求；计算成功但登记失败按 attach 恢复，不能重算；不双写 RD-Agent KB。已完整评价且不支持的规格只留最小结果，不追加抢救、归档或连续变体；未来按新问题检索即可。

<a id="implementation"></a>
## 8. 1.1–1.2 实施方案与文件所有权（保留，1.3见§17）

| 工作包 | 因子窗口范围 | 结束条件 |
|---|---|---|
| A 经验辅助 | 新纯读取模块 `backend/services/factor_research/assistance.py`；扩展 `scripts/factor_research.py` 两个离线分发；新增紧凑 `backend/tests/factor_research/test_assistance.py` | 精确源读取、可见缺项、稳定定位、无副作用、旧入口不变；真实相关产物读取有结果或明确原因 |
| B 问题包与提案交接 | 同一 assistance 模块/CLI 增加 proposal-prepare，复用请求检查；Codex/Claude 实际生成，proposal-inspect 消费 | 准备合同、实际工具提案读回分别报告；研究评价/辅助收益不属于代码已交付的证明 |

A/B 是同一增补工作的连续包，不建立新阶段/审批状态机，不预建 RD-Agent 启动器。实际生成由当前研发工具完成，研究计算仍按当次范围执行；不靠等待生成接口停止新因子研究。

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
| 问题包合同 | 原请求不变、选用经验精确、歧义可见、准备与生成分离；fixture 不证明真实生成或因子有效 |
| Codex/Claude 文件衔接 | 实际提案 JSON 经 fresh-process proposal-inspect 读回；资料不足如实待修订，不创建 RD-Agent workspace/启动实验；不是自动 API 烟测 |
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

RD-Agent 默认 scenario/workspace/按名过滤：不调用其生成程序，不把完整 Loop 当提案接口。实际模型未知：明确未知，不更改生产环境、不假称升级有效。异节点不可达或旧 pickle 不可读：只限制该资料，不发起全库抢救。工具对照接触污染：分别组织上下文并披露共同历史，不承诺统计独立或提升发现概率。

实施发现必须改现有合同/数据/跨 owner 业务时，只提交精确需求，不能自行降级、填零或扩范围。没有增益停止生成支路；既定研究问题已被回答或无值得追加的证据时结束，不为满时长、凑因子或存档继续投入。

<a id="rollout"></a>
## 12. 发布、回滚与 Production gates

默认旧命令/研究路径完全不变，新辅助显式调用。实现先验证离线合同，再验证 Codex/Claude 文件交接；问题包准备不是自动模型生成，提案可检查不是研究有效。生成和计算按具体授权，不因文档合入启动研究长任务。需要回退时停止使用新辅助并走原 CLI/直接研发，不删除任务记录、历史结果或源码产物；不改数据库或关闭任何服务。

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

### 13.4 1.1 合入后只读复验与当时剩余交接（已由1.2 §6替代）

PR #5765 已合入，source HEAD `2fbcb38ae4b9de121c7922c5fcb82ec1c881b63c`，merge `7acdc7268338d2d8cb7c178db167ba6314ef5c8b`，CI verdict SUCCESS。主线新进程的 proposal-inspect --help 正常；experience 精确读取 05_runtime_failure_patterns.md，5条命中、分页返回2条，均标 unverified_historical。该只读复验没有重跑全库评价或触碰数据/DB/服务。

余下是 §6 的 RD-ASSIST-01 真实生成交付，而不是“先补 catalog 再重启才能研究”。按已批准职责，RD-Agent owner 在其独立工作树实现单次问题包→提案入口，交回源码 PR/commit、原请求、提案文件、实际模型身份或未知原因，以及没有启动 Loop/coder/runner、访问行情或业务写入的执行说明。生成只使用本轮问题包，不跟随旧数据目录、旧股票池/单位/成交假设，不修改或补齐旧数据集；原始资料只作为带出处的经验参考。

本窗口收到真实文件后，使用主线 `scripts/factor_research.py proposal-inspect --request <original-request.json> --input <proposal.json> [--context <experience.json>] --format json` 读回，核对两侧身份/输入/来源/边界；不以 fixture 代替真实返回，不自动执行候选或入库。CLI 文件均使用获准 X 盘研究目录。若用户希望本窗口实施 RD-Agent 源码，应先明确接管该 owner 范围并同步职责，不能从“继续任务”推导跨模块接管。真实因子评价另按 §7 执行，不以等待生成端阻断直接研究。

<a id="proposal-pack-readback"></a>
## 14. 1.2 AIstock-only 问题包实现与验收

用户后续明确“由本窗口接手，只能修改 AIstock、不得修改 RD-Agent”。因此 §6 替换原生生成器 owner 需求，§13.4 保留的旧交接不再执行。新增的 proposal-prepare 是准备命令，不是 LLM 调用器；Codex/Claude 在现有会话中生成，保留统一 proposal-inspect 和后续原 runner。没有外部 API 适配、RD-Agent 原生生成能力或因子效果交付承诺。

源码范围：assistance.py 抽出原请求共用检查并增加 prepare_proposal；scripts/factor_research.py 在 configure 之前分发；test_assistance.py 增加一个紧凑合同测试并扩充既有 fresh-process 测试。本轮同时更新本文、蓝图 §20 和方法论 §2.5，不修改 RD-Agent、service/repository/runner/models、nox、catalog、官方指标或跨业务源码。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | assistance.prepare_proposal；CLI离线分发 | backend/tests/factor_research/test_assistance.py::test_cli_fresh_process_no_database | passed | 无 |
| F-002 | assistance._inspect_request；选用经验与检索范围说明 | backend/tests/factor_research/test_assistance.py::test_prepare_preserves_request_and_selects_only_declared_experience | passed | 无 |
| F-003 | 原 request深拷贝、共用检查、输出合同 | backend/tests/factor_research/test_assistance.py | passed | 无 |
| F-004 | §6用户批准的新分工；Codex/Claude文件提案 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#proposal-pack-readback | passed | 无 |
| F-005 | 原完整评价未修改 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#execution | not_applicable | 用户批准本轮实施辅助代码；研究计算不属本轮范围，完整评价要求保持§7 |
| F-006 | models.response/encode复用，未调用writer | backend/tests/factor_research/test_assistance.py::test_cli_fresh_process_no_database | passed | 无 |
| F-007 | 精确6文件归属、直接合同测试及CI计划 | backend/tests/factor_research/test_assistance.py | passed | 无 |
| F-008 | 原catalog分类未降级；CLI fresh-process | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#proposal-pack-readback | passed | 无 |

实际文件交接位于 `X:/AIstock_factor_research/proposal-pack-20261008/request.json` 和 `proposal.json`：由当前 Codex 撰写一项资金流先后次序的概念公式，无模型身份回执则明确 unknown。fresh-process prepare/inspect 都正常返回，数据身份、字段映射/单位/可得时点及研究日期未知，所以正确为 requires_revision；没有虚构当前 active profile 使其通过。请求中的 task_id 仅为本接口示例关联 UUID，没有对应数据库研究任务。该示例不是待晋升因子，不证明行情适配、辅助增益或生成模型质量；完整请求的 prepared/reviewable 行为由程序合同测试验证。

本地验证：新增函数前 RED ImportError；实现和审核修正后定向9项通过、Ruff通过、py_compile通过、L0 blocking=0、git diff --check通过。fresh-process 测试禁止导入DB、RD-Agent、模型SDK和backend.infra，覆盖准备、经验读取、检查三条离线分发。多轮顺序审核（未委派其他agent）：第一轮核对AIstock-only范围与生成/准备区别；第二轮核对原请求不变、选用经验/歧义、缺项与身份、无自动执行并修复重复findings与缺context状态；第三轮核对蓝图/方法论/详细设计、测试和真实文件交接结论一致。实际classifier为 targeted_ci_required，backend_sessions=[factor_research_backend]，dev_db_required=false，unexecuted_test_files=[]；完整所属计划由PR CI执行，不机械重跑评价。

DESIGN-COMPLIANCE-001：①按用户批准的新分工完整交付问题包/检查，不把模板、概念公式或fixture冒充自动生成器/有效因子；②未知数据/模型/经验冲突可见，不静默修补；③不改变评价、PIT、记录、官方写入与其他业务，旧历史验收不改写；④无新增生产门禁、审批、资源限制或平台。catalog仍按backend-main默认分类（runtime_impact=backend，backend_restart_owner=user），不以CLI用途手工降级；本次CLI新进程可独立使用，不依赖后端重启，也不宣称常驻服务身份更新。production_ddl_gate、production_dml_gate、dependency_install、client_install均noop；数据写入、因子入库、正式指标刷新、QE实验、进程控制均未执行。

<a id="memory-contract"></a>
## 15. 1.3 独立经验检索与单一历史权威

### 15.1 本次增补与非目标

本节以下均为待实施合同，不是当前 CLI 帮助或运行事实。基线为蓝图合入后的 `b9e954cbb6d6ce08d50c39ef672568933770a28b`。已存在：两表记录/恢复、显式文件 experience、proposal-prepare/inspect、runner 与完整评价；待增加：研究库记录级查询、关系化更正呈现、紧凑经验字段映射和假设先行指导。实现不修改 RD-Agent、官方指标/评级/QE、研究 writer、schema 或数据集。

“独立”指内容由 AIstock 研究记录积累并可独立检索，不是另外建历史库。首版不需要向量索引、embedding、后台同步、摘要服务、网页或模型 API；无需全量整理既有历史才能开始使用。数据库事实、模型解释和来源引用分开，未知项不自动补齐。下列逻辑字段放入已有 JSON，不扩数据库枚举，不改变原事务/幂等/恢复合同。

### 15.2 最小内容模型与入库路径

| 逻辑内容 | 已有落点及建议 JSON 内容 | 说明 |
|---|---|---|
| 当前假设 | `task.context_json.hypothesis_note`：`basis`、`statement`、`competing_explanation`、`observable_difference`、`falsifier`、`proxy_limitations` | basis 为文字说明事实/经验/文献/推测及未知；沿用 context_json 的 research_role、hypothesis_family、方向、期限和原评价引用，不造第二套参数 |
| 一次经验结论 | 原 `result/decision` 的 `payload.experience_note`：`observation`、`interpretation`、`conditions`、`reason`、`next_evidence`、`source_refs` | conditions 声明实际数据/股票范围/期限/用途或引用原记录；无需复制指标矩阵。observed 与 inferred 不混写，不产生永久黑名单 |
| 选择性外部参考 | 原 `progress/decision` 的同一 experience_note，source_refs 含来源路径/任务定位、选定短片段、已知时期及未知项 | RD-Agent/论文只作外部参考。没有实际本地评价就不填写或推断评价数值；可直接使用已有来源，不要求导入 |
| 更正 | 原 `correction` + `related_record_id`，在 experience_note 描述更正范围与原因 | 原记录仍在；部分更正不抹掉其他观察，不要求链尾一定是唯一正确答案 |

这些字段是建议内容，不是新强制 request schema。没有 note 的旧记录继续可检索，不补写旧记录满足模板；普通 result/decision 顺带记录经验即可，不为失败候选再建归档任务。模型不能直接调用新 writer：Codex 经审阅，用现有 `create/record --dry-run` 与获准目标提交，读回；生产写入必须有精确授权。读查询绝不顺带导入或生成摘要。

新note的文字项为字符串，source_refs为定位对象数组，conditions为对象，可选research_role、hypothesis_family、input_fields、horizons、dataset_ref、universe_ref、evaluation_ref；数组/引用保持原已知值，未知省略或null。只从这些显式字段或明确关联的原尝试读取过滤依据，不从中文叙述猜成结构化事实；同条明确来源相互矛盾时报condition_conflict并保留值/定位，不任取一个作精确过滤。旧任意JSON不被新note格式拒绝写入，读取时只标缺项/不支持部分。

现有 `repository._insert_record` 将原请求嵌入 `payload_json.request`；查询只读取顶层业务摘要及明确 note，不把嵌套 request 重复计入，不递归展开 research_result/候选值/日志/路径。同一外部片段可被不同研究引用，各次解释保留；展示可合并同一 source locator 的片段，但不能合并不同研究结论或同名异公式。定位不是新行情 hash，不新算历史内容身份。

### 15.3 新的只读 CLI 合同

新增独立命令，保留原 `list/show/context/experience/proposal-*` 参数及行为：

```text
# 设计中的命令，代码实施后才可运行
python scripts/factor_research.py memory-search --env-file <existing-env> --target dev|production --input <query.json> --format summary|json
```

只查询明确目标的现有两张研究表，沿用 `configure`、`ResearchRepository.cursor(write=False)` 的只读事务及 ResearchError/response；不使用会 bootstrap 或回写的 RD-Agent/candidate/catalog 服务。`--help` 在 configure 前返回，不连接数据库；旧 experience/proposal 三命令继续离线且无 DB 依赖。此命令的目标选择不是生产写入许可。

请求 `schema_version=factor_research_memory_query_v1`：

| 字段 | 精确语义 |
|---|---|
| query | `problem` 非空；`terms` 至少一个非空字面词；`factor_names/input_fields` 可选数组，沿用 experience 的名称/字段导航，不当正则/SQL |
| filters | 可选 `research_role/hypothesis_family/horizons`，每键为非空字符串数组；已声明多个键取 AND，单键多个值取 OR；未设则不限。期限按记录原声明匹配，不根据字符串猜成熟日期 |
| include_unknown | 默认 true；有 filters 而记录相应字段未知时仍可返回，但逐字段标 unknown，不能说满足过滤。false 只返回已知匹配项并披露被排除未知项数 |
| limit / offset | 默认20/0，正整数/非负整数；仅分页，不是研发/资源门禁。明细引用保留，不声称当前页等于全部历史 |

JSON解析、空词/非法分页等请求错误退出1且不查询。过滤值合法性复用现有 role/期限定义；hypothesis_family 为精确字符串而非新受控词表。SQL 参数化，LIKE 若采用则转义 `%/_`；不能拼接用户输入或让字段名控制 SQL 标识符。

查询返回既有 `models.response` 包络；result 使用可被现有提案检查消费的 `factor_research_experience_v1`，沿用 query/entries/sources/matched_count/returned_count/next_offset/retrieval_status/match_status。query保留原四类查询项，并在新命令结果中带上实际filters/include_unknown，调用者将整个query原样填入experience_query，避免只绑定词项却丢失过滤条件。增加查询专属信息 `scope=research_database`、`unknown_filter_count`、`queried_at`、`related_entries`、`relations_complete`。新信息只描述查询，不覆盖原提案请求；不得把数据库查询输出传给旧 experience 当文件请求，直接作为 proposal 的 context 使用。

source_ref 为 `{source_id: "aistock_research:<target>", locator: {task_id, record_id, revision}}`；任务投影条目的 record_id 为 null，revision 为实际 task revision。sources 描述目标别名、查询范围和读取状态，不含连接串、凭据或 env 内容。每 entry 保留原 observation、明确 basis、match_basis、缺项与原 source_refs；kind 为查询输出标签 `research_record`，不是新增数据库 record_type。每个记录条目另有related_refs、relations_complete及实际缺项原因；全局relations_complete仅是本页记录条目的汇总，不表示全库关系完整，任务投影的未检查关系记not_checked。DB 历史与外部引用均不是当前行情 authority。

查询成功但零命中：available + no_match_in_read_scope，不写“全库无相关机制”。分页越界保留总匹配数。matched_count是文本命中且通过过滤策略的主条目数；unknown_filter_count是文本命中且无已知过滤冲突、但至少一项条件未知的条目数，include_unknown决定它们计入还是排除。连接/SQL失败退出1、ok=false、安全错误原因，不把失败伪装成空库；依赖方可继续原直接研究，不能静默称经验检索成功。截断摘要或关系不全时明确partial/原因/定位，不能由LLM猜出遗漏结论。

请求示例仅说明查询，不授权重启示例所涉研究，也不是现在可执行的命令：

```json
{"schema_version":"factor_research_memory_query_v1",
 "query":{"problem":"寻找被反转解释的旧研究及其更正","terms":["反转","reversal"],"factor_names":[],"input_fields":[]},
 "filters":{"research_role":["predictive_increment"],"horizons":["10d","20d"]},
 "include_unknown":true,"limit":20,"offset":0}
```

### 15.4 匹配、分页和更正关系

匹配单位为任务投影或一条记录，不是整任务全部历史拼成一段。查询读取任务 title/objective/completed_summary、明确 context_json 字段，以及记录 summary/experience_note 和已有 assistance 来源摘要。记录时点条件只来自当条 payload、原请求所声明的上下文或明确关联尝试；不得用任务当前 context 给旧记录补造旧数据版本/方向。当前 task.factor_names 只作任务导航，不证明旧记录中的具体公式身份。note不存在时保留旧summary；note类型异常时保留可读内容并列invalid_note，不用整个payload字符串搜索来掩盖不支持的历史结构。

首版优先在只读事务中分批读取上述紧凑投影、由 Python 复用 experience 的 casefold/空白归一字面匹配与排序；不 SELECT 全部 payload、不加载大指标/行情/代码矩阵。精确名称、明确输入字段、命中词数依次排序，最后按完整 locator 稳定排序；不按收益、接受标志或研究新旧优先。多个匹配词对同条只计一次该词，不靠重复嵌套 JSON 提高排序。不隐式调用 LLM 扩词；中文/英文同义词可由操作者显式写入 terms，返回原查询及匹配理由。

filters 先排除已知不匹配；unknown 按 include_unknown 处理，不能因不填元数据永久找不到旧研究。文本匹配只说明相关线索，不推断经济机制相同或来源等价。内存只保留页所需候选及计数；读取范围/耗时如实报告。先量测现有紧凑研究元数据查询，再决定是否提出索引优化；本设计不申请 DDL，不提前建立新缓存或数据库。跨页有并发新记录时可能变化，queried_at 与 revision 说明观察时点，不冻结数据库、不承诺跨请求快照一致。

命中任一记录时，额外在同一 task 内读取它的 correction 祖先/后代及被关联原记录，即使更正文本不命中关键词也展示；任务投影命中而非记录命中时只提供原 show 定位，不伪称已检查全部更正。多次更正/并行分支并列，不能按时间或最高 revision 挑一个“最终真相”。关系查询明确 same-task，检测异常环/断链/越界并标出，不能无限递归或忽略异常。返回 related_entries，主条目数与附带关联条目数分开，分页主条目不吞更正。

关系读取与主条目使用同一只读一致性观察：沿原cursor，在首次SELECT前为这次查询设置REPEATABLE READ并保持READ ONLY，查询结束即释放，不改写入事务或整个连接池默认级别。结果说明观察时点，新 correction 在查询结束后出现属下一次读取范围。这是短数据库读取的一致性，不是数据集冻结或历史快照产品，不导出文件、不锁住业务写入、不创建持久状态；异常也须按原事务管理释放连接。

正常情况下对当前页读取完整关系，长关系以原定位/截断标记输出并给 show 继续读取方法；截断不能展示一个被修正的旧结果却隐藏“存在未展示更正”。两项独立任务即使结论相反也不自动建立 supersedes 或宣布冲突已解决。显式更正关联是一种事实，语义冲突由研究者结合版本/范围解释。

### 15.5 经验如何进入问题包而不复制历史库

memory-search 的完整 JSON 可由操作者保存在获准 X 盘任务目录，供原 proposal-prepare/inspect 的 `--context` 使用；它是临时查询产物，不是历史权威或要求永久保留的档案。source_refs 仅选用实际返回的 entries/related_entries 中定位，experience_query 与返回的 query 一致；所选条目的 filters/未知/关系信息进入 experience_scope 或 selected_experience，不因转包丢失限制。旧显式文件 experience 输出仍可独立使用，两个旧接口不突然要求数据库。

若选用命中记录已有更正，问题包一并携带与它相关的 related_entries 和关系完整性；不能选择性只注入旧成功结论，也不注入其他未选条目的无关关系。relations_complete按主条目声明，不能用全局true掩盖某一条截断。关系条目只作为所选来源的上下文，不自动变成原请求批准的candidate.source_refs；候选只可引用原request.source_refs。若要单独引用关联条目，调用者明确将它加入source_refs，共用检查在entries与related_entries的并集中按完整locator唯一匹配；同定位异内容仍沿用source_ref_ambiguous，不任取其一。旧无related_entries的context兼容。

未展示或读取失败则在经验范围说明辅助证据不足，不编造或增加新的研究阻断；沿原提案合同处理真正缺引用/请求不一致，不能新增partial即禁止所有直接研究的规则。最小改动是在assistance共用选择逻辑中传递可选关系/查询范围，不改变原runner输入。接收来自文件的关系资料仍是不可信内容，须核对task/关联定位而非跟随任意路径。

同时需要外部经验时，先用原 experience 读取明确 RD-Agent 文本/已提取产物。要形成脱离 RD-Agent 路径仍可参考的本地知识，操作者只将确实采用的短片段、原定位和解释经既有 record 写入获准研究任务，再由 memory-search 查询；无需新合并上下文命令、实时节点连接或知识导入器。不要求每次外部只读使用都入库；没有写入授权时可独立用原经验 context，不宣称已完成持久导入。

新结果顺带写 experience_note；正常重试复用原 record_id，不由检索去做自动 upsert/去重写入。旧研究仅按本次问题定位必要信息，不全量回填、重算或生成摘要。不把模型总结再次当成独立支持证据；同一来源被引用多次仍是一个来源，不凭引用次数提高可信度。

<a id="hypothesis-prompts"></a>
## 16. 假设先行的提示词与候选交接

三个步骤由当前 Codex/Claude 执行，不要求三个 agent、额外模型 API 或强制三次调用。研究者可在同一会话完成，反方审阅不是统计独立性证明。原问题包必须带方向/基准等，故尚未形成假设时不能为了调用 prepare 填造字段：先用方法论和查询资料形成紧凑假设笔记，必要时通过原 progress/decision 记录，方向/基准明确后才组装原 §5 请求。直接研究仍可使用原入口。

### 16.1 指导模板（借鉴结构，不复制 RD-Agent 旧约束）

**A：问题与假设，暂不要求公式。**

> 依据当前问题和现有资料，提出可检验的金融假设或明确标注机制未知的经验现象。区分事实、历史观察和推测；解释可能的行为/摩擦及为何价格可能尚未反映，同时给出最强竞争解释。说明预期期限、适用对象、可观察差别和削弱假设的结果。已有数据无法区分解释时明确限制，不用机构身份、财务质量或盘中路径等不可观测叙事填空。不凑候选数量，不重启已被同口径结果回答的规格。

**B：最简表达与明显对照。**

> 在已声明输入、时点与研究范围内，用最简可计算代理表达假设，逐项说明数学操作的作用。给出简单基准、已核对的结构近邻及代理局限；区分新增来源信息、已知信息的新表示和替换改进。复杂操作仅在能说明用途时使用，不要求新字段、跨源乘积或窗口配额。保留未知、因果时点和缺失，不擅自改方向、期限、数据或评价范围；按原提案 schema 输出，不虚构已运行代码或指标。

**C：反方审阅及结果后的解释。**

> 实现前检查假设/代理是否一致，竞争解释是否可区分，是否只是简单反转、动量、规模、波动或流动性的再表达；不是要求所有候选必须中性化后有效。结果后先核对技术/样本口径，再按三种研究角色和两条结论轴解释完整评价，区分不支持、条件价值、证据不足和实现问题。不凭单周期、高相关、故事合理或 LLM 共识判价值；不事后挑窗口、翻方向来延续原结论。只记录必要经验和下一步，不为失败候选追加存档工程。

### 16.2 实现映射与兼容

模板的原则唯一来源是方法论2.6；文字指导在现有 assistance.py 的 instructions 组装中复用，不建 prompt 平台或远程配置。§16.1 A可在正式包之前由研究者直接使用；B/C随 prepare 的现有 instructions 供参考，保持 generation/model_call/execution_performed=false。金融论证不由正则/关键词或新分数自动判“通过”。

原 proposal 的 hypothesis/falsifier/difference_from_baseline/timing 等字段容纳最终提案；额外思考摘要只放原研究 JSON note，不向严格 candidates/run schema 塞新字段，不强迫旧 proposal 补 note。未知数据导致 requires_revision 沿原合同处理，不新增“金融故事完整度” finding 或准入线。

本版不改现有 skill/Claude command，因为它们已引用方法论；后续若实际入口证明有冲突，另列精确受控变更，不把 skill 全文再复制方法或模板。本轮不安装客户端。代码引用新设计、设计引用唯一方法，避免三份统计标准。

<a id="implementation-plan-v13"></a>
## 17. 具体任务规划、文件范围与验证

### 17.1 一个实施增补，两个连续工作包

不新设 P4/P5 或互锁阶段，不承诺耗满时长。实施授权后，优先完成本模块工具，再在独立获准研究任务中检验实际帮助；工具验收不需要发现 alpha 或等待 QE。

| 顺序 | 任务/精确交付 | 复用、依赖与完成条件 |
|---|---|---|
| P0 同一实现任务 | memory-search、紧凑字段映射、分页及更正呈现，保留旧离线调用 | 复用两表/read-only cursor/response；已有DEV执行实际SQL合同验证，读取不产生写入，未知和异常不冒充完整 |
| P1 同一实现任务 | 三步提示词、研究 JSON note 约定、查询结果到原问题包的关系/范围传递 | 复用 assistance/record；无需新表、writer或模型网关。离线 prepare 与真实查询 context 可对接，旧请求不变 |
| P2 工具定向验证与交付 | 多轮代码审核修复、changed-files对应测试/DEV必要验证、PR/CI与获准合入 | 源码/客户端/运行时分别报告，不因CLI用途手动降级 runtime；后端重启如需由用户执行 |
| P3 一次紧凑真实研究 | 从现有历史选择少量尚未回答且数据可支持的问题，记录实际采用的经验/竞争解释、实现和完整评价 | 不预定必须产出几个因子；不重复失败历史。不启动QE、不自动入正式库；有用途线索则提交批量QE候选交接 |

P0–P3是执行顺序/优先级，不是额外研发阶段。P0/P1可一次代码PR交付；本次只完成设计，不登记实施完成、不启动P3。查询工具局部失败时仅该能力未完成，现有直接研究仍可继续。结束于：工具合同通过或明确实际问题；真实研究完整评价及结论已读回或明确未完成项，无值得追加的依据即停止，不以生成数、记录数或耗时作为成绩。

### 17.2 后续代码允许范围（本次不修改）

| 文件 | 最小责任 |
|---|---|
| `backend/services/factor_research/repository.py` | 新增紧凑只读检索/关联读取方法；不改create/record/replay/事务写入语义 |
| `backend/services/factor_research/assistance.py` | 复用匹配/输出组装、提示词与可选关系信息；保留原离线函数不导入DB |
| `scripts/factor_research.py` | 新命令显式配置DB目标、惰性分发，旧命令/--help保持 |
| `backend/tests/factor_research/test_assistance.py` | 复用已有参数化合同，补查询结果/提案衔接和非执行边界 |
| `backend/tests/factor_research/test_repository_dev.py` | 复用既有DEV授权保护，验证实际SQL/关联/无副作用；不新建测试DB |

纯检索组装可由 CLI 向 assistance 传入 repository 已读取的紧凑迭代结果，不让 assistance 隐式连接 DB。如需新增一个本模块纯 helper，先在实施文件范围中登记；不是本设计要求必须拆文件。默认不改 service.py/models.py/runner.py、nox、ownership、registry、migration、RD-Agent 或其他业务源码。不得通过归属转移避开测试债务或 DEV 验证。

按执行时 actual changed files → file_ownership → module_registry → test_plans 分类；已核对基线归 `factor_library.research`，普通计划 `factor_research_backend`，repository/SQL改动触发 `factor_research_dev_db`（delegated，runner_enabled=false）。CI的DEV文件仍默认skip，不连接数据库；授权DEV只在既有环境显式打开。测试预算沿原规范，扩充紧凑参数化用例而非全套重复repository/CLI/恢复矩阵。

### 17.3 小型测试与真实验收

| 用例组 | 应发现的错误/验证边界 |
|---|---|
| 查询/投影 | 中英文词、名称/字段、filters已知/未知、无note/异常note旧记录、payload.request重复副本、分页；有明确命中不漏掉，无匹配不叫全库无经验 |
| 更正 | 旧结论命中但更正不含原词、链与分支、task隔离、断链/异常环及截断；不把旧成功孤立展示、不最后写入覆盖 |
| 来源/提示词 | 外部命令当数据、原观察与解释分离、未知条件/旧数据保留；模板不强制新字段或凑数，不检测故事关键词来判科学性 |
| 旧接口/关系传递 | 原文件 experience 和无关系的 context 仍可用；memory-search结果选源后关联与范围不丢；缺资料非静默成功，无自动run/model/DB写入 |
| 真实DEV | 精确自有fixture在既有DEV按原验证方式创建/回滚或清理；查询走READ ONLY，验证参数化、同任务关联、短事务一致性及读取前后行/revision不变；读连接异常退出后事务级别不泄漏到后续连接，所有生产连接禁用 |
| 真实研究 | 只在独立获准任务中，用真实问题记录采用/不采用的资料、选题或公式变化、完整评价/用途增量及实测成本；结果可不支持，不把接口fixture当有效因子 |

前五组为代码验收职责，最后一组是研究验收，不能互相替代。实现前先写能暴露错误行为的直接测试，再修复；至少两轮实质审核分别看合同/权限与实现/反例，失败只重跑直接失败项，稳定后运行一次所属最小计划。不重跑全库因子或QE来验证检索。

纯文档阶段只有 UTF-8/链接、diff check、F2设计结构及PR必要CI，不运行DEV测试。代码阶段按真实变更运行Ruff/compile/直接pytest、所属计划及适用L0；catalog未变不额外重跑其全矩阵。DEV SQL合同与生产授权分开，本版无生产 DML/DDL需求。

<a id="acceptance-v13"></a>
## 18. 1.3 设计验收、发布边界与剩余风险

沿用F-001～F-008，F-004在1.2已由RD-Agent owner交接改为AIstock提案交接；不新增审批编号。下表只验收本轮设计覆盖，不宣称代码或研究通过。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §15.1/16 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#hypothesis-prompts | 用户批准设计先行 | 用户批准本轮仅设计；工具/研究另行实施 |
| F-002 | §15.3–15.5 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#memory-contract | 用户批准设计先行 | 用户批准本轮仅设计；DB查询及更正待代码验证 |
| F-003 | §16.1–16.2 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#hypothesis-prompts | 用户批准设计先行 | 用户批准本轮仅设计；旧合同兼容待实现证明 |
| F-004 | §15.5/16.2 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#memory-contract | 用户批准设计先行 | 用户批准本轮仅设计；不修改或依赖RD-Agent生成程序 |
| F-005 | §7/17.1/17.3 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#implementation-plan-v13 | 用户批准设计先行 | 用户批准本轮仅设计；完整评价未启动，QE/入库未执行 |
| F-006 | §15.2/15.4–15.5 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#memory-contract | 用户批准设计先行 | 用户批准本轮仅设计；原两表/writer/恢复复用，不双写 |
| F-007 | §17.2–17.3 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#implementation-plan-v13 | 用户批准设计先行 | 用户批准本轮仅设计；按实际变更实施DEV/定向代码验证 |
| F-008 | §18 | artifact: docs/architecture/factor_research_rdagent_assistance_detailed_design_20261008.md#acceptance-v13 | 用户批准设计先行 | 用户批准本轮仅设计；源码/运行时/DB/研究分开 |

本轮文档 `runtime_impact=none`、`backend_restart_required=false`；DEV/production DDL/DML、依赖/客户端安装、研究运行、数据导出/激活、进程控制均noop。用户授权设计提交/合入不授予未来代码或生产操作；worktree清理须相应授权。合入后同步干净主线，不自动启动研发计算。

实现时由catalog按实际文件推导runtime，backend restart owner=user，不沿用文档none或手工降级。新CLI fresh-process与常驻backend加载身份分开；回退方式是停止使用新辅助入口，继续旧直接研究，不删除原记录或关闭业务服务。任何新增持久索引/DDL/跨owner需求另行分析，不顺带执行。

剩余风险：旧记录元数据不足只能显式unknown，不能保证所有历史可语义发现；词项检索可能漏掉同义表述，先披露并积累真实漏检案例；紧凑投影扫描成本需实测，不承诺固定秒数；已接触历史和模型解释可能强化偏见，保留反例与research_feedback；三步提示词和检索可以改善研究组织，但不保证新增有效因子。既有无价值规格只留最小结论，不为缓解这些风险做全历史回填或证据工程。

### 18.1 本轮顺序审核与修订记录

本窗口顺序审核，不宣称另一个agent或客户端独立复核。第一轮按实际models/repository/assistance合同核对，区分尚未成型的假设与原严格提案请求，明确payload.request不重复检索、旧记录不能继承当前task条件。第二轮按反例修正更正关系跨分页/转包丢失风险、query过滤范围绑定和同定位歧义；第三轮核对未知过滤计数、关系只随所选来源注入、异常note/任务级未查关系的可见性及读事务不改变写入默认值。按蓝图1.9/方法2.6复核无统计口径或生产权限变化。

DESIGN-COMPLIANCE-001逐条：①§15–18为完整设计范围，所有代码/真实研究明确未实施，不冒充功能完成；②读取错误、未知、局部关系和旧数据不伪装成功；③原PIT/四周期/单写指标/owner边界及历史验收保留；④没有新增金融故事评分、准入门禁、审批、平台或资源阈值。F2结构检查8项、32行（含历史矩阵）通过、warnings=0；当前设计矩阵为8行。最终HEAD的UTF-8/本地链接/diff检查和PR必要CI分别记录，不执行DEV测试或业务计算。
