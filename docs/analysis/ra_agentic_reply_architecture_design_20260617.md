# BUG-403 设计方案：Research Assistant Agentic 回复架构（agent 循环 + 工具，废除答案模板）

- 版本/日期：v1 / 2026-06-17
- 关联：GitHub Issue #1208 / BUG-403 / worktree bug/BUG-403-ra-reply-synthesis-...
- 性质：设计方案（仅分析，不含代码）。作为 `--mode plan` 设计依据，待 Tier2/用户评审后实现。

## 1. 目标（用户最终要求）
助手以"钢铁侠 Jarvis"方式对话：**理解任意自然语言问题 → 用 agent 循环 + 工具按语义自主调用 → 拿到结果由 LLM 合成人类友好回答**。架构上杜绝"答非所问的模板式回答"。采用与 Claude Code / Codex / OpenCode 相同的 agent-loop + tool-calling 范式。

## 2. 现状架构与根因
- 链路：`dialogue_intent 分类 → semantic_tool_planner 选 1 个工具 → auto-execute 单工具 → _render_*reply 模板拼答案`。
- 根因（service.py:4263-5060，~800 行）：`_compose_assistant_reply` 是 if/elif 级联，业务 response_mode 命中即 `return` 模板并**丢弃 LLM 回答**；15+ 个 `_render_*reply` 每业务域一个、**均不接收 user_message**。
- 典型失败："哪些实验还在运行" → `_render_qe_experiment_status_reply` dump 全部 18 completed + 2 created（答非所问，无法识别"running=0"）。
- 差距编号：G1 模板化合成丢弃 LLM；G2 工具目录无参数 schema、planner 无法构造过滤参数；G3 结果端无语义过滤/否定回答；G4 LLM 仅当路由器非推理者；G5 单工具非多步；G6 evidence guard 只查格式不查切题。

## 3. 目标架构（对照开发工具）
单一 agent 循环（参考 OpenCode `SessionPrompt.loop()`、Claude Code/Codex 的 tool-use 循环）：
```
system(prompt+工具schema+证据契约) + 记忆/图谱context + user
  → 模型发起 tool_use 调用(自带参数,含过滤)
  → 执行网关(只读直执 / 写操作走审批门禁)
  → tool_result 回灌上下文
  → 模型多步推理(可再调工具)
  → 模型亲笔写最终人话答案(引用 source/as_of)
  → grounding guard 校验(切题+防编造+无占位)→不过则带违规原因回灌重生成(有限次)
  → 渲染(结构化卡片作UI数据, prose 由模型产出)
```
确定性只保留三处（同开发工具）：**工具本身 / 安全门禁 / 证据数据**。答案 prose 永远由模型产出。

## 4. 组件设计
### 4.1 工具注册表 + JSON Schema（解 G2）
- 单一源 `TOOL_MANIFEST` + 每工具 input schema（参数、类型、可选过滤如 status/symbol/analysis_date/limit/order_by）。
- 以 OpenAI/litellm function-calling `tools=[...]` 形式喂模型，使模型能自构造 `status=running` 等过滤参数。
- 工具风险/门禁元数据沿用现有 manifest（read_only=direct，write/confirmed/long_running/production_adjacent=preflight）。

### 4.2 Agent 循环（解 G4/G5）
- 新建 orchestrator（演进现有 `react_grounding`）：bounded steps（max_tool_iterations + token budget），模型驱动多步工具调用，工具反馈回灌"grounding"防跑偏。
- 取代 `semantic_tool_planner` 单发选择 + 模板路径（planner 逻辑被循环吸收；澄清作为模型可发起的一种结果）。

### 4.3 LLM 客户端 function-calling（双协议兼容）
- 扩展 `ResearchAssistantLlmClient` 支持 litellm `tools`/`tool_choice` 与 `tool_calls` 解析。
- **provider 无关 + 模型可替换**：经 litellm 统一调用，任意主流 LLM 可接入；原生 function-calling 可用则用，不可用回退到现有"结构化文本工具协议"（react_grounding 已能解析 `<assistant_tool_choice>`）。接口对两种后端统一。
- **默认协议**：主模型（如 DeepSeek，OpenAI 兼容、支持 function-calling）默认走原生 tools；文本协议作回退/兜底（保障无原生 tools 的模型与降级场景）。具体模型的 tool 支持度在实现时按 model_profile 探测确认。

### 4.8 模型分层（主模型 / 辅助模型，= Claude Code 方式）
**不变量**：长期记忆（L1 记忆树 + 知识图谱）、工作模式（agent 循环 + 证据契约 + 安全门禁）**与模型无关**，模型可按需替换、不影响记忆与流程。模型身份是配置（`model_profiles` + `routing_policies`，role=primary_reasoner 已存在），非硬编码。

| 层 | 角色 | 用在哪 | 要求 |
|---|---|---|---|
| **主模型** primary_reasoner（如 DeepSeek V4 Pro） | 驱动 agent 循环：理解 NL、决定工具调用、多步推理、**亲笔写最终用户答案** | 主对话回合、最终合成 | 必须支持 function-calling；强推理 |
| **辅助模型** auxiliary/cheap_worker（如 Flash/Haiku 档） | 廉价/快速的认知子任务 | 大工具结果**预压缩/摘要**后回灌(控 token)、上下文压缩/key-facts、记忆 curator、主动晨报聚合(Phase9 cheap_worker)、分类/澄清草拟、Prompt Lab judge | 快、便宜；function-calling 非必需 |

- 澄清：**"脚本执行"由确定性工具层/harness 执行，不由辅助模型执行**；辅助模型只承担廉价 LLM 认知子任务（摘要/压缩/分类/聚合）。这与 Claude Code 用低成本模型跑后台/子任务一致。
- 路由：role→model_profile 由 `routing_policies` 决定；切换模型只改配置，循环/记忆/契约不动（呼应 L0 模型路由属 core）。
- 反幻觉边界：**最终面向用户的答案合成走主模型**（保质量）；辅助模型只做中间处理，其输出仍须经 grounding guard 与证据契约。

### 4.4 执行网关 + 安全门禁（保留确定性，= OpenCode permission）
- 只读工具：直执，返回 summary-first envelope（带 source/as_of/source_refs，已具备）。
- 写/确认/长任务/production_adjacent：**强制经 ActionProposal + 审批/确认门禁**，循环不得自动执行；门禁决策确定性、确认文案确定性。模型只能"提议"，由网关裁决。
- 阻断/审批结果作为结构化数据，模型据其措辞，但 gate 决策不被模型左右。

### 4.5 答案合成 = 模型最终轮（解 G1/G3）
- 无模板。模型基于工具结果 + 用户原问写答案；要求：诚实处理空/否定结果（如"无正在运行的实验；2 created、18 completed"）、解释状态语义（created≠running）、每条事实引用工具返回的真实字段。

### 4.6 Grounding/Evidence Guard 升级（解 G6，反幻觉关键）
- 从"格式校验(有无 source/as_of)"升级为：①事实性断言必须引用工具返回的真实 source/as_of；②禁占位符 XX/X%/约X 与伪造 as_of；③答案需切题（回答了问题或诚实说 insufficient/none）。
- 违规 → 带违规原因回灌模型重生成（有限次）→ 仍不过则诚实 insufficient 阻断卡。**绝不因"放开让 LLM 自由总结"而允许编造。**

### 4.7 结构化卡片保留（UI 数据）
- evidence/blocker/plan card 仍作 UI 结构化数据，chat prose 由模型产出。（卡片渲染缺陷由 BUG-402 单独修。）

## 5. 保留 vs 废除
- 保留：TOOL_MANIFEST、MCP 工具与 envelope、审批/风险门禁、证据数据(source/as_of)、记忆树/图谱 context、prompt pack(系统提示)、litellm provider 抽象、结构化卡片(UI)。
- 废除/退役：15+ `_render_*reply` prose 模板、`_compose_assistant_reply` 业务 render 级联、`semantic_tool_planner` 单发路由(被循环吸收)、意图级 `fallback_reply` prose(改模型措辞)。

## 6. 分阶段实施（均在 BUG-403 下，可拆多 PR）
- **P1（核心、高 ROI）**：function-calling LLM 客户端 + 工具 schema 注册表 + agent 循环核心(只读工具) + 模型亲笔答案 + grounding guard 升级；用合成取代 QE/local_data/stock 业务模板分支。→ 立即消灭主要模板 + 修复"答非所问"。
- **P2**：写/确认工具纳入循环并强制门禁；多步链；退役剩余 `_render_*` 与 fallback_reply prose。
- **P3**：打磨——澄清由模型发起、provider/persona 配置(参考 OpenCode plan/build 双 agent)、回归清理。

## 7. 验收/测试（严格、防回退——本节是本方案成败关键）

> **为何要强化**：上一版设计（Phase 7 证据契约）也"要求"反幻觉/证据，但验收只查"有无 source/as_of"——**模板能满足该检查**，于是实现退化成模板仍"通过验收"。本节验收必须**结构性强制 LLM 合成、禁止模板**，否则历史会重演。

### 7.1 杀手级断言：同数据 × 不同问题 → 不同且切题的答案（防模板核心）
- 用同一份工具结果，喂 N 个不同问题（"哪些在跑"/"完成几个"/"最优 loop 用什么模型"/"对比 loop9 与 loop12"），断言产生 **N 个不同且各自切题** 的答案。
- 模板对同数据只会产出近乎相同结构 → 该断言天然证明"非模板、真合成"。**这是上一版缺失的断言。**

### 7.2 否定/空结果诚实回答
- "哪些实验还在运行" → "目前无正在运行；2 个 created、18 个 completed"（识别 running=0、解释 created≠running），**不得 dump 全量**。

### 7.3 多维度 → 多工具合成（覆盖"国城矿业全方位分析"现场失败）
- "国城矿业的基本情况、近期走势、未来趋势怎样 / 给我全方位的分析" → agent 循环**自主多步调用** quote+financials+fund_flow+technicals+联网基本面，**合成一份连贯分析**；**不得**因"选不出单一工具"而澄清打断，**不得**模板 dump。

### 7.4 防漂移代码断言（anti-drift）
- 业务回合答案必须经 LLM 合成函数产出；断言模板特征串（如"已汇总 QE 实验状态如下""状态汇总："等 `_render_*reply` 固定话术）**不出现**在合成答案中。
- `_render_*reply` 业务 prose 模板在合成路径中**不被调用**（退役后删除或仅留安全门禁结构）。仿 Phase8-12 在 crosscheck 增"业务回合非模板"断言。

### 7.5 反幻觉 / 安全门禁（不可因放开模板而退化）
- 事实性断言必须引用工具返回的真实 source/as_of；编造/占位 XX/X%/约X → 被 guard 拒并回灌重生成；仍不过→诚实 insufficient。
- 写/确认请求仍经 ActionProposal 门禁（approval required），循环禁直执；断言无未授权 write / mcp_tool_events。

### 7.6 必跑套件
- l0 / research_assistant_backend / research_assistant_mcp_contract / ra_phase7_full_accept / validation_module_registry_l0。

> 验收门禁：7.1（变体差异）+ 7.3（多维度合成）+ 7.4（非模板断言）三者全过，才算"杜绝模板式回答"达标；任一不过即打回。

### 7.7 必须随本 PR 更新的耦合测试（防 nox 再红）
- 本 PR 重构/删除 service.py 旧路径函数（如 `_maybe_auto_execute_read_only_mcp_route`、`_compose_assistant_reply` 业务级联、`_render_*reply`）。`backend/tests/research_assistant/test_phase0_blueprint_baseline.py` 用 `_line_number(SERVICE, "...")` 锚定这些**旧函数名**，删除后该测试会抛错 → **本 PR 必须把锚点更新到新 agent-loop 等价函数**（不要留指向已删函数的 needle）。
- 若本 PR 调整了 profile/manifest 工具集或计数，需同步 `test_profiles_registry_gateway.py` / `test_mcp_catalog_sync.py` 期望。
- 注意：BUG-406 只修"本 PR 之前"的 upstream 漂移；本 PR 自身造成的行/结构变化由本 PR 负责，不可依赖 BUG-406。

## 8. 风险与缓解
- 国产模型 function-calling 兼容 → 双协议(原生 tools / 结构化文本回退)。
- 多步循环 token/延迟 → bounded steps + summary-first envelope + token budget。
- guard 被绕过风险 → guard 不可关闭、违规回灌重生成、最终诚实阻断；Tier2 审核重点。
- 安全门禁退化风险 → 写/确认工具一律经现有 ActionProposal/approval，循环禁直执；测试断言无未授权写。

## 9. 待用户/Tier2 确认项
- 是否按 P1→P2→P3 分阶段（推荐）。
- P1 是否先只覆盖 QE/local_data/stock 三业务域的合成替换作为首个 PR。
- 国产模型清单与其 function-calling 支持度（决定双协议默认走哪条）。
