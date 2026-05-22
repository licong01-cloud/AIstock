# AIstock 研究与实验综合助理实施阶段与功能验证矩阵

> 日期：2026-05-22
> 类型：实施验收 companion 文档 v2（对话型主入口纠偏版）
> 来源：`docs/architecture/aistock_research_agent_console_design_20260520.md`
> 用途：作为后续开发、验收、外部审核和合入 main 前检查的逐项矩阵；任何阶段不得交付静态占位、脚本替代或低完整度版本。
> 补充：所有原先失败的旧版页面统一视为后台管理页或审计页，不得再作为主对话入口。
> ???????? `origin/main = 856b832` ? AIstock ??????????? Research Assistant ?? / MCP / ???? / ?????? Phase 1 ??????????????????????????????????????????????

---

## -1. 2026-05-22 纠偏验收增补

本矩阵从 v2 起增加“对话型主入口纠偏”验收。当前 main 中已合入的 Research Assistant Phase 1 基础设施不能视为对话型助理完成，必须补齐以下差距后才可重新声明 Phase 1 可用。

### -1.1 当前实现差距矩阵

| 差距编号 | 当前状态 | 必须修复结果 | 合入阻断 |
|---|---|---|---|
| GAP-CHAT-001 | Chat 只创建任务 | assistant-ui 对话主入口真实回复 | 是 |
| GAP-LLM-001 | 没有真实 LLM 调用 | 用户消息触发主模型 completion 并写 trace | 是 |
| GAP-UI-001 | 主入口显示 JSON/ID/payload | 默认无 raw JSON、无后台日志、无乱码 | 是 |
| GAP-STATE-001 | 任务进度是列表/事件 | 左侧图形化任务状态轨道 | 是 |
| GAP-CAP-001 | MCP/Skill 目录不完整 | Capability Registry 覆盖首批 MCP/Skill | 是 |
| GAP-QE-001 | QE 创建流程不可用 | QE 10 loop 草案端到端通过 | 是 |
| GAP-PROMPT-001 | 无正式 Prompt Pack | system/intent/tool/qe/result prompt 版本化 | 是 |
| GAP-MODEL-001 | 只有 model profile | 主/次模型真实调用、fallback、cost trace | 是 |

### -1.2 对话主入口验收用例

固定输入：

```text
帮我创建一个 QE 10 loop 实验，先不要执行。
```

必须通过：

| 步骤 | 必须结果 | 证据 |
|---|---|---|
| 发送消息 | 页面显示正在理解需求 | UI 截图 / Playwright |
| 模型调用 | 后端真实调用主模型 | trace 记录包含 model/provider/latency |
| 需求理解 | 助理中文复述任务目标 | UI 截图 |
| 澄清确认 | 助理提出必要确认问题 | UI 截图 |
| 计划生成 | 生成 QE 实验草案计划卡 | UI 截图 |
| 状态展示 | 左侧状态轨道进入等待确认 | UI 截图 |
| 安全门禁 | 确认前不得 materialize/run | 后端事件/审计记录 |
| 无 JSON | 主窗口没有 raw JSON/payload/schema/日志 | 自动检查 + 截图 |
| 后台审计 | Admin 可查看 trace 和技术详情 | 后台链接证据 |

### -1.3 Workflow 与自主 Planner 验收

| 验收项 | 必须结果 | 阻断条件 |
|---|---|---|
| 自主分析 | 未知任务能先分析、澄清、提出只读探索计划 | 只能机械匹配固定流程 |
| Workflow Pack | QE/GitHub/Validation 等高频高风险任务有明确流程包 | 高风险任务只靠模型自由发挥 |
| 安全门禁 | Workflow 和自主 Planner 都必须经过 risk/preflight/approval | Planner 可绕过确认直接执行 |
| 能力目录 | Prompt 可读取 MCP/Skill/Capability 摘要 | 模型不知道有哪些工具 |
| 人类可读 | Workflow 输出渲染成计划卡/确认卡/结果卡 | 输出 planner JSON |


### -1.4 树型提示词验收矩阵

| 验收项 | 必须结果 | 证据 | 阻断条件 |
|---|---|---|---|
| Prompt DB 固化 | 根提示词、子提示词、边、状态、checksum 全部在数据库 | DB schema/API 测试 | 生产提示词只存在代码或前端 |
| 树型关系 | 每个提示词有 tree_path、parent/child、trigger、risk | 单测；样例导出 | 平铺提示词无法按层选择 |
| 动态选择 | QE 创建只加载 root/governance/intent/QE 分支 | selection trace | 每次加载全部提示词 |
| 多分支任务 | 跨模块任务可同时加载 QE + Issue + Memory 分支 | selection trace；E2E | 只能选一个分支导致任务缺失 |
| 父子闭包 | 命中子分支时自动补齐 root/governance/intent 等祖先节点 | selector 单测；trace | 只加载叶子节点导致安全约束丢失 |
| Bundle Signature | 每次装配生成包含 prompt 版本、checksum、模型、阶段的签名 | selection trace；缓存键测试 | 缓存键不能区分版本或阶段 |
| 阶段化装配 | 计划、执行前、执行中、结果汇报阶段加载不同提示词 | trace 回放 | 一次性加载所有执行提示词 |
| 工具 guard | MCP/Skill 执行前加载对应 guard prompt | preflight trace | 高风险工具无 guard |
| Renderer | 工具返回后加载 result renderer，输出人类可读 | UI 截图 | 直接展示工具 JSON |
| 文件缓存 | 缓存有 checksum，失效可重建，不改变选择结果 | 缓存命中/失效测试 | 缓存成为事实源或过期不失效 |


### -1.5 多模型调度、Prompt Lab 与自我学习验收矩阵

| 验收项 | 必须结果 | 证据 | 阻断条件 |
|---|---|---|---|
| 主模型调度 | 主模型能拆分任务并生成次模型 delegation plan | trace；UI 计划卡 | 次模型自由执行或无主模型复核 |
| 次模型结构化输出 | 次模型只返回 JSON/Schema 格式 WorkerResult，schema 校验可追踪 | 单测；trace；schema 失败用例 | 次模型散文输出直接进入最终回复 |
| 权限边界 | 次模型不能直接调用高风险 MCP、写 approved 记忆或发布提示词 | 权限测试；失败用例 | 次模型绕过审批执行 |
| 联合路由 | 模型选择和 prompt branch 选择同时记录原因 | route trace | 只能看到模型或只能看到提示词，无法回放 |
| Prompt Lab | 支持 prompt variant、eval case、eval run、release candidate | DB/API 测试；评估报告 | 候选提示词未评估直接生产使用 |
| Shadow 测试 | 候选 prompt 可后台评估且不影响真实执行 | shadow run 证据 | 候选 prompt 影响生产 MCP 行为 |
| 自我学习 | 用户偏好、操作模式、研究结论只能先进入候选/证据链 | memory candidate 审批记录 | 单次对话自动改写核心规则 |
| 研究记忆 | QE/HMM/因子结论区分事实、假设、指标、下一步 | experiment lineage API；样例报告 | 无证据假设被当作结论 |
| 提示词优化建议 | 能根据失败/纠错生成 Prompt Improvement Proposal | 样例 proposal；评估用例 | 只道歉不沉淀改进建议 |


### -1.6 可审计自我学习详细验收矩阵

| 验收项 | 必须结果 | 证据 | 阻断条件 |
|---|---|---|---|
| 用户画像记忆 | 用户偏好、工作风格、风险偏好可创建、审批、覆盖、废弃 | API 测试；UI 画像卡；冲突样例 | 用户画像只存在 prompt 或自由文本中 |
| Operation Playbook | QE/Issue/Validation 等核心操作有 approved playbook、步骤、门禁和失败模式 | DB/API 测试；QE playbook 样例 | 助理每次靠模型猜工具顺序 |
| Reflection Card | 用户纠错、任务失败、MCP 失败能生成反思卡和防复发建议 | 失败用例；reflection 记录 | 失败后只输出道歉，无可复用学习资产 |
| Experiment Lineage | QE/HMM/因子研究有假设、实验、结果、反思、下一步节点和关系 | lineage API；样例研究报告 | 实验结论无证据或无法追溯来源 |
| Research Curriculum | 能基于历史实验和失败方向提出候选研究任务队列 | curriculum 样例；用户确认记录 | 随机提出研究方向，不引用历史证据 |
| Prompt Feedback | 提示词问题能转成 feedback、eval case、variant 或 release candidate | prompt feedback 样例；eval case | 重复 prompt 问题没有进入评估体系 |
| 主次模型 JSON 通信 | DelegationRequest、WorkerResult、PrimaryReview 全部按 JSON schema 校验和 trace | schema 单测；失败重试测试；trace | 主次模型靠自由自然语言传递执行参数 |
| 候选升级路径 | 单次事件、重复事件、稳定规则、评估通过分别进入正确存储层 | 端到端回放 | 记忆、prompt、playbook、eval case 边界混乱 |
| 参考方案落地 | MemGPT/LangMem/Reflexion/Voyager/Graphiti/DSPy 等只以采纳模块体现，不作为空泛参考 | 设计映射表；模块测试 | 文档只列参考资料，未形成实现模块 |

## 0. 验收适用范围

本矩阵对应主设计方案中的以下核心约束：

- 长期记忆不是 RAG：Memory Ledger 是事实源，向量/RAG 只做辅助召回。
- UI 主入口采用 assistant-ui + Codex 式对话体验：保留 AIstock 左侧导航，研究助理主入口使用对话窗口和左侧图形化任务状态；后台管理页才允许表格、抽屉和审计详情。
- Firecrawl 不做默认搜索入口：中文搜索优先低成本 provider，Firecrawl/Jina 作为高质量抓取/抽取备用。
- Phase 1 不引入图数据库：轻量知识图谱使用 AIstock 原生关系表。
- Codex / Claude Code 可通过 External Agent Connector 接入，但不得越权。

---

## 16. 阶段实施目标和功能边界

### Phase 0：设计冻结和实施准备

| 目标 | 交付物 | 验收标准 |
|---|---|---|
| 设计冻结 | 本文档 v4、用户确认记录 | 不存在互相冲突的阶段目标 |
| 实施分支准备 | 独立 worktree、独立 feature 分支 | 不在 main 或生产根目录开发 |
| 数据迁移规划 | Phase 1 表结构、回滚方案 | DDL gate 明确 |
| API/MCP 契约规划 | API、MCP tools、risk level、schema | 写操作有 preflight/approval/idempotency |
| UI 原型规划 | 页面结构、顶部导航、卡片/表格/抽屉模式 | 与现有 Sidebar 不冲突 |

### Phase 1：核心助理能力完整交付

必须实现：

1. Research Assistant 主页面必须是 assistant-ui 驱动的 Codex 式对话主入口，并带左侧图形化任务状态轨道。
2. MCP 工具目录、schema 展示、健康状态、risk level、preflight 和执行事件。
3. Task Ledger、Agent Task Event Stream、失败 triage、idempotency key。
4. MCP 执行工作台：配置草稿、配置 diff、preflight、执行进度、tool result、业务深链。
5. 原生 Memory Ledger 和非 RAG Context Pack。
6. 轻量知识图谱原生表和关系检索。
7. 本地 Skill Catalog 和首批 Skill。
8. Validation / Pipeline Discovery Stream。
9. External Agent Connector 合同。
10. 多模型路由、真实 LLM 调用、主/次模型选择、次模型调度、调用 trace 和临时记忆。
11. Prompt Lab、提示词评估、候选发布和自我学习候选记忆。
12. 可审计自我学习：用户画像、Operation Playbook、Reflection Card、Experiment Lineage、主次模型 JSON 通信。
13. UI 审批中心。
14. 候选 Issue 队列和 GitHub 正式入库门禁。
15. 今日事项、晨报、提醒和 personal namespace。
16. Web 内通知。
17. 原生 trace 和成本/耗时记录。

明确不做：

- 不控制鼠标键盘。
- 不写代码、提交代码、创建 PR 或合入 main。
- 不自动创建正式 Issue。
- 不自动运行长时间或高成本实验。
- 不接入语音。
- 不引入图数据库。
- 不接入外部 memory engine 到运行路径。
- 不接入公共 Skill 市场。
- 不注册任何实盘交易 MCP/Skill/审批入口。
- 不实现多窗口对话。

### Phase 2：协同展示、外部增强和业务扩展

必须实现：

1. 对话确认执行：NLU 意图识别、plan digest 匹配、确认原文回放。
2. 多状态窗口 workspace session。
3. HMM/QE/因子/事件 Research Streams 长期任务视图。
4. Mem0/Graphiti/LangMem/Letta adapter 只读或镜像 PoC。
5. QE 10 loop 配置版本、diff、preflight、审批、执行状态完整交互。
6. 股票分析 MCP：复用现有股票分析能力，输入股票代码生成报告，不触发交易。
7. 基础驾驶舱卡片。
8. 外部搜索 provider PoC：中文低成本 provider + 学术 MCP + Firecrawl/Jina 抽取备用。
9. 桌面通知或 IM 通知。
10. 语音 Realtime 试点。

### Phase 3：长期自治研究助手

必须实现：

1. 每日晨报和定时提醒自动化。
2. 自动推进 read-only/dry-run 白名单任务。
3. 多 Agent 分工和 orchestrator 仲裁。
4. 长期记忆审计报告和图谱审计报告。
5. 自动候选 Issue 生成和人工入库审批。
6. 本地 STT/TTS 混合路线验证。
7. Temporal 或同等级工作流引擎技术验证。
8. AIstock 架构图、MCP 拓扑、任务依赖图和资源状态图形化展示。

### Phase 4：独立产品化

必须实现：

1. 抽离 `assistant_product_core`，AIstock 成为首个 domain adapter。
2. 支持其他 MCP/API 应用接入同一助理框架。
3. 支持可替换 Memory Provider、Skill Provider、MCP Gateway、Channel Provider。
4. 支持人工筛选公共 skill 后本地导入。
5. 保持 AIstock 私有策略、生产边界和研究记忆不外泄。

---

## 17. 开发功能验证矩阵

### 17.1 总体验收红线

| 验收项 | 验收标准 | 阻断条件 |
|---|---|---|
| 阶段完整性 | Phase 1 清单中的每个模块均有数据模型、API/MCP、UI、审计和测试证据 | 任一 Phase 1 模块只有静态占位或脚本代替 |
| 真实数据 | UI 接真实 API/MCP 数据，空状态必须说明原因 | 用 mock 数据冒充完成 |
| 可回放 | 任务计划、Context Pack、MCP 调用、Skill 使用、审批、结果、记忆写入都可追溯 | 关键动作缺少 event/trace |
| 非 RAG 记忆 | Memory Ledger 是事实源，向量/RAG 只做辅助召回 | 用向量召回结果决定事实或审批 |
| 安全边界 | 默认无鼠标键盘控制、无代码写入、无 main 合入、无实盘路径 | 任一越权路径存在 |
| GitHub 一致 | 正式 Issue 必须有 GitHub URL 和状态回写 | 本地正式 BUG JSON 无 GitHub 链接 |

### 17.2 Phase 1 验收矩阵

| 模块 | 必须功能 | 验收证据 |
|---|---|---|
| 对话主入口 UI | assistant-ui 消息流、左侧图形化任务状态、计划卡、确认卡、结果卡；默认无 raw JSON | Playwright/UI smoke；截图；无 JSON 检查；路由清单 |
| MCP 目录 | server/tool/schema/risk/health、preflight | API 测试；MCP contract 测试 |
| Task Ledger | 创建任务、状态流转、事件写入、失败 triage、idempotency key | 后端单测；事件流回放测试 |
| Workbench | 配置草稿、diff、preflight、执行进度、tool result、业务深链 | E2E 流程测试 |
| Memory Ledger | 分层记忆写入、检索、审批、supersedes/contradicts/valid_to | 后端单测；记忆审计导出 |
| Context Pack | 必载规则、token budget、source refs、可回放 | 快照测试；回放测试 |
| 轻量知识图谱 | entity/relation/evolution path、证据绑定 | 图谱 API 测试；关系检索测试 |
| Skill Catalog | 本地 skill 注册、checksum、权限、trace、禁用 | 后端单测；UI 列表和详情测试 |
| Validation Discovery | 夜间报告、候选 Issue、流水线证据绑定 | Validation MCP 测试；候选 Issue 测试 |
| External Agent Connector | Codex/Claude session、context pack 读取、证据写入、候选 Issue | Contract test；权限边界测试 |
| 多模型路由 | model profile、routing policy、真实模型调用、主模型调度次模型、cost、fallback、temp memory | 单测；真实模型调用 trace 样例；delegation trace |
| 审批中心 | risk、plan digest、config version、审批失效、审批回放 | E2E；状态机测试 |
| 候选 Issue | 去重、证据、复现、审批、GitHub 正式同步门禁 | 后端单测；GitHub dry-run/同步测试 |
| Web 通知 | assistant_notifications、待处理计数、详情跳转 | API/UI 测试 |
| Trace/成本 | LLM/MCP/Skill 调用次数、耗时、成本、model profile | trace 样例；报告验证 |
| Prompt Tree | 数据库固化、树型选择、多分支装配、文件缓存、selection trace | DB/API 单测；QE/跨模块 E2E；缓存失效测试 |
| Prompt Lab | variant、eval case、shadow run、release candidate、审批发布 | DB/API 单测；评估报告；发布回放 |
| 自我学习 | 用户画像、Operation Playbook、Reflection Card、Experiment Lineage、Prompt Feedback 形成完整候选和证据链 | Memory/API/Playbook/Lineage 测试；样例晨报；候选审批记录 |
| 可审计自我学习 | 用户画像、Operation Playbook、Reflection Card、Experiment Lineage、Prompt Feedback、候选升级路径 | API 单测；端到端失败反思回放；样例研究谱系报告 |
| 主次模型结构化通信 | DelegationRequest、WorkerResult、PrimaryReview 使用 JSON schema，校验失败可重试/升级 | schema 单测；trace；错误注入测试 |

### 17.3 Phase 2 验收矩阵

| 模块 | 必须功能 | 验收证据 |
|---|---|---|
| 对话确认 | chat_text approval、plan digest 匹配、版本变化失效 | E2E；审批回放 |
| 多状态窗口 | 主窗口发令，状态窗口同步，SSE/WebSocket 推送 | 多标签测试；事件同步测试 |
| 外部 Memory Adapter PoC | 只读/镜像接入，效果对照，不写 approved 记忆 | PoC 报告；回滚测试 |
| 股票分析 MCP | 输入股票代码生成报告，不触发交易 | MCP 测试；安全测试 |
| 外部搜索 | 中文 provider、学术 MCP、Firecrawl/Jina 抽取备用、证据保存 | 搜索报告样例；中文搜索 PoC；prompt 注入测试 |
| 通知 | 桌面/IM 通知，任务完成/失败/待审批 | 通知测试；订阅配置 |
| 语音 Realtime | 语音转文本、播报、文本审批绑定 | transcript 测试；审批安全测试 |

### 17.4 UI 验收矩阵

| 页面 | 必须展示 | 必须交互 | 验收证据 |
|---|---|---|---|
| 总览 | 今日待确认、运行中任务、失败、候选 Issue、成本 | 点击卡片进入详情 | UI smoke / 截图 |
| Chat | assistant-ui 主对话、左侧任务状态、计划卡、配置讨论、确认入口、无 JSON 默认展示 | 真实 LLM 回复、生成计划、提交确认、查看上下文 | E2E；截图；LLM trace |
| Admin Workbench | MCP 调用、配置 diff、preflight、日志、深链 | 执行 dry-run、打开详情、失败 triage | E2E；仅后台入口 |
| Tasks | 状态、事件、证据、耗时、模型 | 筛选、打开事件、暂停/恢复 | UI/API 测试 |
| Memory | 记忆类型、审批、冲突、来源 | 审批、废弃、查看 source_ref | UI/API 测试 |
| Graph | entity/relation/evolution path | 查看关系详情、证据、有效期 | UI/API 测试 |
| MCP Tools | server/tool/schema/risk/health | 查看 schema、执行 preflight | UI/API 测试 |
| Skills | 本地 skill、checksum、权限、trace | 启用/禁用、查看使用记录 | UI/API 测试 |
| Approvals | risk、plan digest、配置版本、确认原文 | 批准/拒绝、查看执行结果 | E2E |
| Reports | 晨报、实验报告、候选 Issue 报告 | 查看来源、导出、跳转详情 | UI/API 测试 |
| Models | provider、profile、routing、成本 | 启用/禁用、调整策略 | UI/API 测试 |

---

## 18. 已确认决策记录

| 决策项 | 结论 |
|---|---|
| 首批 Skill | QE 诊断、因子库分析、因子研发任务包、RDAgent 任务分析、数据健康检查 |
| Phase 1 通知 | Web 内通知和通知数据模型；桌面/IM 放 Phase 2 |
| 外部搜索 | Firecrawl 不做默认搜索入口；优先中文低成本 provider + 学术 MCP，Firecrawl/Jina 做抽取备用 |
| Graphiti PoC | 优先只读镜像 AIstock 原生图谱核心实体关系，论文图谱作为补充 |
| 语音路线 | Phase 2/3 优先托管 Realtime 试点，保留本地 STT/TTS 混合路线 |
| 长期记忆 | 不是 RAG；Memory Ledger 是事实源，向量/RAG 只做辅助召回 |
| Codex/Claude 接入 | 可作为外部主模型，但不可越权 |
| 图数据库 | Phase 1 不引入；后续只作为增强 PoC |
| Skill 公共平台 | 当前不设计；未来人工筛选后本地导入 |

---

## 19. 待后续确认问题

1. Phase 2 中文搜索 provider 首选博查、秘塔，还是先做 SearXNG 自托管。
2. Phase 2 Firecrawl/Jina/自建 Playwright crawler 的抽取备用优先级。
3. Phase 2 桌面通知优先浏览器通知、Windows toast，还是 IM/企业微信。
4. Phase 2 语音试点使用哪个托管 Realtime provider。
5. Phase 2 国内模型 provider 的首批上线清单和预算。

---
