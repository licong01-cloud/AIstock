# Research Assistant 架构升级蓝图：工具接地 + 自生长树形记忆 + Agent Teams + QE 自主演进

> 日期：2026-05-30
> 分支：`claude/ra-upgrade-blueprint-20260530`
> worktree：`F:\Dev\AIstock\.claude\worktrees\claude+ra-upgrade-blueprint-20260530`
> 任务分级：T3 / 统一架构升级设计
> 运行边界：本文档不启动、不停止、不重启 `8001` / `3000`，不直接改生产配置、不改运行时；本 PR 仅新增设计文档。
> 目标读者：可被其他模型 / 工具直接据此完整实现，不允许"最小实现 / 简化版 / POC / 建了不用"。

---

## 0. 本文档的定位与使命

本蓝图是 Research Assistant（AIstock 内部"智能助手 / 类 Jarvis 编排器"）的**架构升级总纲**。它在保留现有可复用资产（约 90%）的前提下，补齐五个被反复确认的结构性缺陷：

1. **生成不接地（幻觉根因）**：对话是"LLM 先凭参数知识单次生成 → 事后旁挂只读工具结果"，工具结果**从不回灌 LLM**。
2. **记忆建了不用**：记忆账本只按类型平铺取前 N 条，**与当前问题无关**；无树形分类召回、无打分、无反思。
3. **知识图谱建了不用**：实体/关系/演进路径表与 API 齐全，但 `build_context_pack` 中 `graph_relation_refs` **硬编码为空**，从不进入推理。
4. **无 Agent Teams**：单体服务，无主从并行编排。
5. **QE 无自主闭环**：有单 loop 决策，但**跨 loop 自主流转 / 停止条件 / 预算守护缺失**。

并额外补回一条**早期设计过、但代码零实现、且在后续文档中丢失**的能力：**外部搜索 / 学术论文检索（证据优先）**。

本蓝图的硬性要求（针对历史教训）：
- **不得出现设计与实现不一致**：每条设计项必须在第 12 章「可追溯性矩阵」中映射到实现文件 + 测试 + 验收命令。
- **不得"建了不用"**：凡新增存储 / 表 / API，必须有"被推理链路真实消费"的断言测试（见第 11 章防漂移门禁）。
- **不得设计丢失 / 业务断层**：第 1.3、第 2.2 节锁定全部前序设计来源（含已丢失的外部搜索设计），逐条说明承接方式。
- **分阶段但不得降级**：第 10 章每个 Phase 都有独立验收标准；任一 DAI 未过，不得宣称"完整实现完成"。

---

## 1. 现状基线（实现证据 + 设计承接）

> 本章是"防止设计漂移"的事实锚点。所有判断基于 2026-05-30 对 `F:\dev\aistock` 的代码核查，给出 `文件:行号` 证据。后续实现必须以本章为"改造前基线"。

### 1.1 已落地且保留复用的资产（L0）

| 能力 | 证据 | 处置 |
|---|---|---|
| 真实 LLM 调用（litellm，fail-fast，无 mock 兜底） | `backend/services/research_assistant/service.py:542-581` `ResearchAssistantLlmClient.complete` | 保留 |
| 多级模型路由（primary_reasoner / cheap_worker / long_context，按 role+risk+token） | `service.py:3467` `route_model`；`service.py:280-360` 默认 profile/policy | 保留，国产为主 |
| 模式切换状态机（dialogue/analysis/planning/preflight/execution/audit/recovery） | `models.py:77` `DIALOGUE_MODES`；`service.py` mode router | 保留 |
| Prompt Tree（树结构 + 祖先闭包选择 + 多分支命中） | 表 `assistant_prompt_nodes(category, tree_path, parent_key)` `init_research_assistant_schema_20260521.py:548-565`；`service.py:1295-1343` `_select_prompt_nodes` | **保留并作为 L1 记忆树召回的引擎范本** |
| 风险审批闭环（提议→preflight→approval→执行） | `backend/services/research_assistant/execution.py` | 保留 |
| MCP 统一 Gateway + profiles（9 个 gateway module + 3 个 legacy 脚本 server） | `backend/mcp/gateway.py`、`backend/mcp/profiles.py:19-57` | 保留，legacy 后续迁移 |
| token summary-first 契约 | `research_assistant_unified_mcp_natural_language_orchestration_design_20260527.md` §10.3.1 | 保留 |
| 前端 10 页（chat/memory/approvals/trace/models/mcp-tools…） | `frontend/src/app/research-assistant/` | 保留，按各层增量扩展 |

### 1.2 缺陷清单（必须改造，附证据）

| 编号 | 缺陷 | 证据 | 影响 |
|---|---|---|---|
| DEF-01 | chat 单次生成，工具结果不回灌 | `service.py:1724` LLM 生成在前；`:1763` `_maybe_auto_execute_read_only_mcp_route` 事后旁挂；无二次 `llm_client.complete` | 幻觉根因 |
| DEF-02 | 记忆平铺按类型取前 N，与问题无关 | `service.py:3097-3105` `build_context_pack` 仅 `list_records(filters=memory_type)` | 长期记忆"取不准" |
| DEF-03 | 无树形记忆召回 / 无打分 / 无反思 | 全仓 grep `importance/recency/relevance/reflection/embedding` 在记忆检索层 = 0 命中 | 记忆不可靠 |
| DEF-04 | 知识图谱建了不用 | `service.py:3137` `"graph_relation_refs": []` 硬编码空 | 跨模块理解缺失 |
| DEF-05 | 无 Agent Teams | 单体 `ResearchAssistantService`；无 orchestrator/worker | 无法并行多任务 |
| DEF-06 | QE 无跨 loop 自主闭环 | `qe_evolution_service.py:133-312` `AutoEvolutionScheduler` 有单 loop 决策，rerun/retry 均被动触发 | 不能自主演进 |
| DEF-07 | 外部搜索/学术检索零实现 | 后端 grep `arxiv/scholar/tavily/web_search/paper_search` = 0 文件 | 无文献接地 |
| DEF-08 | 记忆表无真树列 | `research_memory_items` 仅 `namespace + memory_type + 点分键`，无 `parent_key/tree_path`（仅 `assistant_prompt_nodes` 有） | 树存储是弱约定 |
| DEF-09 | 记忆类型不含个人维度 | `models.py:61-71` `MEMORY_TYPES` 无 `user_preference/directive/analysis_note` | 无法装个人习惯/指令 |

### 1.3 前序设计来源与承接（防止设计丢失）

本蓝图**整合而非取代**以下设计；每条注明承接关系，**不形成 competing architecture**：

| 前序设计 | 提供内容 | 本蓝图承接 |
|---|---|---|
| `research_assistant_unified_mcp_natural_language_orchestration_design_20260527.md` | MCP 统一编排、token 契约、股票证据门禁 | L0 保留；股票门禁泛化为 L2 全局证据契约 |
| `research_assistant_mcp_skill_execution_closure_design_20260525.md` | 模式分离、审批闭环、ReAct/MemGPT 等理念引用 | L0 保留；ReAct/MemGPT 由"纸面引用"升级为 L1/L2 真实实现 |
| `research_assistant_prompt_pack_runtime_design_20260524.md` | Prompt Pack/Tree、CrewAI/LangSmith 引用 | L0 保留，作为 L1 记忆树引擎范本 |
| `research_assistant_prompt_context_runtime_governance_design_20260524.md` | runtime config、上下文预算、reactive compact | L0 保留 |
| `research_assistant_context_compression_design_20260524.md` | 压缩触发 | L1 反思巩固承接 |
| `research_assistant_memory_graph_bootstrap_design_20260523.md` | 记忆 ledger 本体、知识图谱本体（module/consumes/owned_by/relates_to）、`external` 证据类型 | **L1 双树 + 图谱关系层直接承接** |
| `aistock_research_agent_console_design_20260520.md` §12 | **外部搜索多 provider + 学术 MCP（arXiv/Semantic Scholar/Paper Search）+ 证据优先红线** | **L2.5 直接承接（此设计此前已丢失、零实现）** |
| `aistock_research_agent_console_validation_matrix_20260521.md` | 外部搜索 provider PoC 验收项 | L2.5 验收承接 |

---

## 2. 设计原则与红线

### 2.1 核心原则

1. **工具接地优先**：任何事实型结论必须来自真实 MCP/工具返回，不得来自模型参数化知识。
2. **无 RAG / 纯分类树形记忆**：内部记忆与知识检索使用**确定性分类 + 树形召回 + 打分排序**，**不使用 embedding / 向量相似度**（理由：AIstock 为有界领域，分类可靠、可审计；向量相似度会引入"语义近但事实错"的幻觉源）。语义检索仅发生在**外部搜索 provider 端**（L2.5），其结果作为带来源证据入库，本系统仍不自建向量库。
3. **证据优先**：数值/事实结论必须带 `source_refs` + `as_of`/`trade_date`/`report_period`；无证据则降级为"证据不足"，禁止占位符（`XX`、`X%`、`约X`）。
4. **国产模型为主**：主推理 deepseek（`primary_reasoner`）；worker/反思/curator/压缩 用 glm/qwen（`cheap_worker`）；长上下文 qwen-long。与现有 `route_model` 三档对齐。
5. **不引入替换性框架**：吸收 Claude Code / OpenClaw / LangGraph / AutoGen / Mem0 / MemTree 等的**理念**，在现有 FastAPI/PostgreSQL/litellm 上自建，不引入 LangGraph/CrewAI/Temporal 作为运行时依赖。
6. **fail-fast**：无静默降级、无空 `except: pass`、无默认值掩盖错误（遵循 AIstock 开发标准 v1.5 §6.3-6.4）。

### 2.2 红线（禁止形态）

- 禁止把任一层落地为 read-only-only 却声称完整。
- 禁止"建了表/API 不接推理链路"（必须有消费断言测试，见 §11）。
- 禁止外部论文/搜索结果直接成为结论（只能进 `external_evidence` / `research_hypothesis`）。
- 禁止 Agent Teams 让模型自行决定高风险执行（高风险动作仍走 preflight + approval）。
- 禁止 QE 自主演进绕过预算 / 停止条件 / 审批。
- 禁止任何阶段触碰 `8001` / `3000`。

---

## 3. 目标架构总览

```
┌─ L0 现有资产（保留）：模式切换 / 风险审批 / MCP catalog / token契约 / route_model三档 / Prompt Tree / 前端
│
├─ L1 记忆与关系引擎（无 RAG，纯分类树 + 知识图谱关系层）
│    🌲 project.*   项目知识树（受审批、权威；与知识图谱互为骨架）
│    🌳 personal.*  个人工作记忆树（自生长：习惯/偏好/指令/分析笔记/任务进展）
│    🕸 知识图谱关系层：依赖/谱系遍历，注入推理 + 喂 L3 任务分解
│
├─ L2 工具接地推理内核（消除幻觉）
│    ReAct 回灌循环 + 真实能力闸门 + 全局证据契约 + Reflexion 复盘
│
├─ L2.5 外部研究/检索（受控、证据优先、无幻觉）
│    中文 provider + 学术 MCP（arXiv/Semantic Scholar/Paper Search）；结果入 external.*/topic.*
│
├─ L3 Agent Teams（主范本 = Claude Code subagent；配置范本 = OpenClaw 声明式）
│    orchestrator（分解/派发/记忆/汇聚/仲裁） + workers（QE/HMM/因子/数据诊断）
│    隔离上下文 + 工具子集 + 结构化返回 + 并行 + 共享黑板 + 审批门禁
│
├─ L4 QE 自主演进闭环（作为 L3 的 QE worker）
│    loopN→Evaluator→Analyst方向→生成loopN+1→预算/停止守护→自动提交
│
└─ L5 范式兑现 + 验收门禁：对照兑现表 + 可追溯性矩阵 + 防漂移门禁
```

模型分级映射（沿用 `assistant_model_profiles` / `assistant_routing_policies`）：

| 角色 | 用途 | 默认 provider/model（可配置） |
|---|---|---|
| `primary_reasoner` | 主 orchestrator、最终推理、ReAct 决策 | deepseek（deepseek-chat / v4-pro） |
| `cheap_worker` | 记忆 curator、反思巩固、worker 子任务、压缩 | glm / qwen |
| `long_context` | 长日志/长文档/论文归纳 | qwen-long |

---

## 4. L1：记忆与关系引擎（双树 + 知识图谱关系层，无 RAG）

> 解决 DEF-02/03/04/08/09。学术背书：MemTree（ICLR 2025, arXiv 2410.14052，在线动态树自扩展）、RAPTOR（ICLR 2024, arXiv 2401.18059，树组织检索，collapsed-tree 优于刚性逐层）、H-MEM（arXiv 2507.22925）、Mem0（arXiv 2504.19413，user/session/agent scope + self-edit 去重）、MemoryBank（recency×relevance×importance 打分）。

### 4.1 双树模型

| | 🌲 项目知识树 `project.*` | 🌳 个人工作记忆树 `personal.*` |
|---|---|---|
| 内容 | 模块/MCP/架构/红线/数据链路/实验谱系 | 习惯、偏好、长期指令、分析笔记、跨天任务进展 |
| 来源 | 代码、设计文档、知识图谱、QE/Paper 产物 | 对话中用户陈述 + 助理分析结论 |
| 生命周期 | 稳定、版本化、受审批 | 快速、自生长、轻量自动入树 |
| 治理 | 改规则需 approval | 个人偏好自动入；助理推断打低信任 |
| 召回 | 按 domain/module 分类召回 | `directive.*`/`preference.*` **常驻**；`topic.*`/`task.*` 分类召回 |
| 根分支约定 | `project.module.*`、`project.architecture.*`、`project.rule.*`、`project.mcp.*`、`project.experiment_lineage.*` | `personal.preference.*`、`personal.directive.*`、`personal.habit.*`、`personal.topic.*`、`personal.task.*`、`personal.episodic.*` |

两树共享**同一棵树引擎**（`parent_key`/`tree_path`/分类召回），但根不同、治理不同。

### 4.2 数据库变更（DDL，触发 production_ddl_gate，须逐项报告）

为 `research_memory_items` 增加真树与治理列（保留向后兼容，旧行默认值由迁移补齐）：

```sql
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS tree_path     TEXT;       -- 例: personal.preference.language
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS parent_key    TEXT;       -- 父节点 memory_key，根节点为 NULL
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS node_type     TEXT NOT NULL DEFAULT 'fact';   -- branch | fact
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS scope         TEXT NOT NULL DEFAULT 'project';-- project | personal
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS importance    REAL NOT NULL DEFAULT 0.5;      -- 0..1
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS last_used_at  TIMESTAMPTZ;
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS use_count     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS auto_created  BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS trust_level   TEXT NOT NULL DEFAULT 'user_stated'; -- user_stated | assistant_inferred
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb; -- {conversation_id, message_id, turn, source}
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS resident      BOOLEAN NOT NULL DEFAULT FALSE;  -- 常驻注入(directive/preference)

CREATE INDEX IF NOT EXISTS idx_rmi_tree ON research_memory_items(scope, tree_path, approval_status, importance DESC);
CREATE INDEX IF NOT EXISTS idx_rmi_parent ON research_memory_items(parent_key);
CREATE INDEX IF NOT EXISTS idx_rmi_resident ON research_memory_items(scope, resident) WHERE resident = TRUE;
COMMENT ON COLUMN research_memory_items.tree_path IS '点分树路径(project.* / personal.*)，用于祖先闭包召回';
COMMENT ON COLUMN research_memory_items.trust_level IS 'user_stated=用户明说(高信任直入); assistant_inferred=助理推断(低信任待确认)';
COMMENT ON COLUMN research_memory_items.resident IS 'true=每轮常驻注入(长期指令/偏好)，不受分类门控';
```

`MEMORY_TYPES`（`models.py:61`）扩充：新增 `user_preference`、`directive`、`habit`、`analysis_note`（保留旧值，向后兼容）。

> 兼容性：所有新列均有默认值；现有读路径不依赖新列即可工作。迁移脚本须为存量行回填 `scope='project'`、`tree_path` 由现有点分 `memory_key` 推导。

### 4.3 树形召回服务（复用 Prompt Tree 引擎）

新增 `backend/services/research_assistant/memory_tree.py`：

```python
def select_memory_branches(user_message: str, intent, *, repo, runtime_config) -> MemoryRetrievalResult:
    """无 RAG 的树形记忆召回。
    1) classify: 复用 _classify_dialogue_intent + domain_ontology 把输入映射到 0..N 个分支前缀
       (跨模块/复杂问题可命中多个分支，对应 project.* 多分支 + personal.topic.*)。
    2) collapsed selection: 命中分支下，按 tree_path 前缀取候选(非刚性逐层；参考 RAPTOR collapsed-tree 优于逐层遍历)。
    3) score within branch: importance × recency(last_used_at) 排序，按 token 预算取 top-K，不使用向量。
    4) always-resident: 追加 scope=personal AND resident=true 的全部 directive/preference(不受分类门控)。
    5) graph expand: 对命中的 project.module.* 节点，调用 graph 关系层取一跳邻居(consumes/owned_by/relates_to)用于跨模块。
    返回 refs + 命中分支 + route_reason + omitted_refs(被预算裁掉的，写审计)。
    """
```

`build_context_pack`（`service.py:3093`）改造：用 `select_memory_branches` 的结果替换"按类型平铺取前 N"；`graph_relation_refs` 不再硬编码空，填入 §4.5 的图谱邻居（**直接修复 DEF-04**）。

### 4.4 自动扩展（curator）+ 反思巩固 + 治理

新增 `backend/services/research_assistant/memory_curator.py`，在 `chat_turn` 末尾异步触发（用 `cheap_worker`）：

1. **抽取**：从本轮对话抽取候选记忆（偏好/习惯/指令/分析结论/任务进展）。
2. **分类挂载**：用同一分类引擎挂到现有分支；**无合适分支 → 自动新建 branch 节点**（生成 `tree_path` + `parent_key`，`auto_created=true`）——实现 MemTree 式在线自扩展。
3. **self-edit 去重**：与同分支已有事实冲突/重复时，更新原节点而非堆叠（Mem0 式）。
4. **信任分级**：用户明说 → `trust_level=user_stated`（高信任）；助理推断 → `assistant_inferred`（低信任，标记待确认；本期先入库后审查，后续优化）。
5. **入库门禁（防幻觉记忆）**：每条带 `provenance_json`（来源 conversation/message/turn）；无来源不入。
6. **反思巩固（reflection）**：定时任务（独立 worker，`cheap_worker`）周期把零散 `personal.episodic.*` 提炼为 `personal.topic.*`/`personal.habit.*` 语义节点。
7. **增长治理**：`importance` 衰减 + 陈旧分支归档（`approval_status='superseded'`）+ 分支度/深度护栏（超阈值触发合并）；A-MEM 教训——必须治理节点间一致性。

审批分级（承接 §2.2）：`personal.preference/habit/analysis_note` 低风险自动入树；`project.rule.*` 与改写 `directive.*` 须 approval。

### 4.5 知识图谱关系层（修复 DEF-04）

承接 `research_assistant_memory_graph_bootstrap_design_20260523.md` 本体。现状：表 `research_memory_entities/relations/evolution_paths` + `get_graph_entity`/`graph_summary`（`service.py:3180-3221`）已存在，但**未进推理**。

改造：
- 新增 `backend/services/research_assistant/graph_context.py`：`expand_neighbors(entity_keys, hops=1, relation_filter=...)` 返回依赖/谱系邻居摘要（summary-first，不内联大图）。
- `build_context_pack` 注入命中模块的图邻居到 `graph_relation_refs`（**断言测试：跨模块问题的 context pack 必须 `len(graph_relation_refs) > 0`**，见 §11）。
- L3 orchestrator 任务分解时调用 `expand_neighbors` 决定派哪些 worker。

**定位分工**：树负责"逐级分类召回"，图负责"关系/依赖遍历"，互补（GraphRAG / Zep-Graphiti 思路），均无 embedding。

---

## 5. L2：工具接地推理内核（消除幻觉）

> 解决 DEF-01。范式：ReAct（工具使用循环）、Reflexion（失败复盘）、OpenClaw agentic loop。

### 5.1 ReAct 回灌循环（替换"单次生成 + 事后旁挂"）

改造 `chat_turn`（`service.py:1596+`）为有界多步循环（默认 `max_tool_iterations` 由 runtime config 控制，如 4）：

```
while step < max_tool_iterations:
    llm_out = llm_client.complete(messages)            # 结构化输出: {thought, tool_calls[], final?}
    if not llm_out.tool_calls: break                    # 模型认为可以收口
    for call in llm_out.tool_calls:
        assert_tool_in_catalog(call.server, call.tool)  # 真实能力闸门(见 5.2)，否则拒绝并回灌错误
        result = execute_via_proposal(call)             # 只读自动；写入/高风险→preflight+approval(沿用 execution.py)
        messages.append(tool_result_message(result))    # 关键: 结果回灌(修复 DEF-01)
final_answer = compose_with_evidence_guard(llm_out, collected_results)  # 证据契约(见 5.3)
```

- 只读工具：循环内自动执行并回灌。
- 写入/高风险工具：循环内**只生成 preflight + 确认卡**，不在循环内执行写入；用户确认后再走 `execute_action_proposal`（沿用现有审批闭环，不放权给模型）。
- 过程（thought/tool/observation）进 trace/audit，不外显主气泡（沿用 OpenHands"过程进审计"理念）。

### 5.2 真实能力闸门（禁止幻想工具）

新增 `assert_tool_in_catalog`：模型选择的 `server/tool` 必须存在于 `assistant_capabilities` / `mcp_capability_catalog`。不存在 → 拒绝执行，回灌"该工具不存在，请改用 catalog 内工具或声明能力缺口"。**保证只依赖系统真实存在的 MCP/LLM 能力。**

### 5.3 全局证据契约（泛化股票门禁到所有领域）

承接 unified 设计 §9.7/§10.9A 的股票门禁，**升级为全域规则**：
- 事实型结论的每个数值必须可追溯 `source_refs` + `as_of`/`trade_date`/`report_period`；否则输出"证据不足"。
- 禁止占位符 `XX`、`X%`、`约X亿元`、无来源 PE/资金流/支撑位。
- 渲染层 `compose_with_evidence_guard` 做最终校验：若答案含数值但无对应 tool 结果来源 → 阻断并降级。

### 5.4 Reflexion 复盘

工具失败 / 证据不足 / preflight 阻塞时，模型生成简短复盘（写 trace），并在剩余 iteration 内自我修正重试（如换工具、补参数、缩小范围）；超过 `max_tool_iterations` 则 fail-fast 给出"已尝试 X，仍缺 Y"。

---

## 6. L2.5：外部研究 / 检索（受控、证据优先）

> 解决 DEF-07，承接 `aistock_research_agent_console_design_20260520.md` §12（此前丢失、零实现）。

### 6.1 形态

新增受控 MCP（统一 gateway 形态）`backend/mcp/modules/external_research.py` + profile `external_research` + server key `aistock-external-research`；后端 façade `/api/v1/external-research/*`。

| 层 | provider | 用途 | 默认策略 |
|---|---|---|---|
| L1 中文综合搜索 | 博查/秘塔/SearXNG（可配置，低成本优先） | 行业/事件/概念资料 | 只读自动，结果存证据 |
| L2 学术/技术搜索 | arXiv MCP、Semantic Scholar MCP、Paper Search MCP、GitHub search | 因子/模型/HMM/事件研究论文 | 只读自动 |
| 抽取备用 | Firecrawl/Jina | 高质量网页正文抽取 | 非默认入口，按需 |

### 6.2 工具（首批）

| 工具 | 类型 | 功能 | 策略 |
|---|---|---|---|
| `external_research_search_web` | read_only | 中文综合搜索，返回带 URL/as_of 的摘要列表 | 自动，summary-first |
| `external_research_search_papers` | read_only | 学术检索（arXiv/Semantic Scholar/Paper Search） | 自动 |
| `external_research_fetch_extract` | read_only | 抽取指定 URL 正文为证据（Firecrawl/Jina 备用） | 自动，正文走 detail/ref |
| `external_research_save_evidence` | draft_only | 把检索结果存为 `personal.topic.*` / `external.*` 记忆候选 | 草稿，带 provenance |

### 6.3 红线（承接原设计 §12）

- 外部资料**只能进** `external_evidence` / `research_hypothesis`（即 `external.*` / `personal.topic.*` 分支），**绝不直接成为结论**。
- 喂给 L4：论文/资料 → 候选假设 → 低成本验证，**不直接排高成本实验**。
- summary-first + provenance + as_of；长正文走 detail/ref，遵守 token 契约。
- 仍然**不自建向量库**：语义匹配由 provider 完成。

---

## 7. L3：Agent Teams（主从并行编排）

> 解决 DEF-05。主范本 = Claude Code subagent 模型；配置范本 = OpenClaw 声明式 agent（SOUL.md 思想）；编排范式 = orchestrator-workers（Anthropic）。不引入 LangGraph/CrewAI 运行时。

### 7.1 角色模型

| 角色 | 职责 | 模型档 |
|---|---|---|
| **Orchestrator（主 agent）** | 任务分解、依赖分析（查知识图谱）、派发、记忆管理、汇聚 reduce、冲突仲裁；**不干具体活** | `primary_reasoner`（deepseek） |
| **Worker（子 agent）** | 执行单一领域子任务，返回结构化结果 | `cheap_worker`（glm/qwen），复杂者可升 primary |

首批 worker（声明式定义，见 7.3）：`qe_experiment_designer`、`hmm_evolution`、`factor_developer`、`local_data_doctor`。

### 7.2 借鉴 Claude Code 的机制映射

| Claude Code 机制 | AIstock 落地 |
|---|---|
| 主 agent 派生 subagent（隔离上下文） | 每个 worker 独立 context pack（独立 `build_context_pack`），主 agent 不被细节淹没 |
| subagent 专属工具子集 | worker 定义 `allowed_servers/allowed_tools`，经 §5.2 闸门强约束 |
| subagent 结构化返回 | worker 返回 `{summary, artifacts, evidence_refs, status}` reduce 回主 agent |
| 并行派发 | 主 agent 并行启动多 worker（asyncio / ThreadPoolExecutor；后者已在 `backend/agents/ai_agents_impl.py` 验证可用） |
| 共享任务清单作协调 | 复用 `research_agent_tasks` / `agent_task_events` / `research_evolution_paths` 作黑板总线 |

### 7.3 声明式 worker 配置（OpenClaw SOUL.md 思想）

新增 `configs/research_assistant/agent_teams.yaml`，每个 worker 声明：`agent_key / role / goal / allowed_servers / allowed_tools / model_role / prompt_nodes / max_tool_iterations / output_schema`。新增 worker 只改配置，不散落代码。

### 7.4 数据库变更（DDL）

```sql
CREATE TABLE IF NOT EXISTS assistant_agent_runs (
    agent_run_id   TEXT PRIMARY KEY,
    parent_task_id TEXT NOT NULL,           -- orchestrator 任务
    agent_key      TEXT NOT NULL,           -- 来自 agent_teams.yaml
    role           TEXT NOT NULL,           -- orchestrator | worker
    status         TEXT NOT NULL,           -- queued|running|succeeded|failed|cancelled
    input_json     JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json    JSONB,                   -- 结构化返回(summary/artifacts/evidence_refs)
    model_profile_id TEXT,
    trace_id       TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_aar_parent ON assistant_agent_runs(parent_task_id, status);
COMMENT ON TABLE assistant_agent_runs IS 'Agent Teams 主从运行记录；worker 结果 reduce 回 orchestrator';
```

### 7.5 编排流程

```
orchestrator.run(user_goal):
  1. 记忆+图谱: 加载相关分支 + 依赖邻居(L1)
  2. 分解: 拆成 worker 子任务(每个绑定 agent_key)，写 assistant_agent_runs(queued)
  3. 并行派发: 各 worker 在隔离上下文 + 工具子集中跑 L2 ReAct 循环
  4. 汇聚 reduce: 收集结构化结果，冲突仲裁，证据校验(L2 契约)
  5. 记忆: curator 把进展/结论写回 personal.task.* / project.*
  6. 高风险动作: 仍走 preflight + approval(不放权)
```

### 7.6 风险与边界

- 并发上限可配；worker 失败隔离（一个失败不拖垮整队，结果标 `failed` 并 reduce）。
- 所有写入/高成本/生产敏感动作经统一审批门禁，**Agent Teams 不绕过**。
- 主气泡只显示 orchestrator 的自然语言汇总；worker 过程进 Workbench/Trace。

---

## 8. L4：QE 自主演进闭环

> 解决 DEF-06。承接现有 `AutoEvolutionScheduler`（`qe_evolution_service.py:133-312`）+ Analyst 两步 + Evaluator 三层。范式：Voyager（技能库+自主课程）、AI-Scientist（研究循环）。作为 L3 的 `qe_experiment_designer` worker 落地。

### 8.1 自主主循环

在现有调度器上加"自主流转"（默认关闭，须用户显式开启 + 设定边界）：

```
autonomous_evolve(task_id, methodology, stop_conditions, budget):
  while not should_stop():
     loop_metrics = run_or_wait_loop_N()              # 复用现有 loop 执行
     verdict = run_evaluator(loop_metrics)            # 现有 Evaluator 三层
     direction = run_analyst_step2(verdict, trends)   # 现有 Analyst 方向决策
     next_cfg = generate_loop_config(direction, methodology, external_hypotheses)  # 可纳入 L2.5 论文候选
     if budget.exhausted() or stop_conditions.met(verdict): break  # 停止守护
     submit_loop(next_cfg)                            # 自动提交下一 loop
  archive_and_report()
```

### 8.2 停止条件 + 预算守护（防失控）

| 守护 | 默认 |
|---|---|
| 达标停止 | 指标达到 target |
| 无改进停止 | 连续 N 轮无 SOTA 提升 |
| 预算守护 | 最大 loop 数 / 最大累计耗时 / 最大 GPU 占用 |
| 异常停止 | 连续失败 / 数据缺口 → fail-fast 并报告 |
| 审批 | 高成本 run 仍可要求人工确认（沿用 `qe_*_confirmed`） |

### 8.3 数据库变更（DDL）

```sql
CREATE TABLE IF NOT EXISTS qe_autonomous_evolution_runs (
    auto_run_id     TEXT PRIMARY KEY,
    qe_task_id      TEXT NOT NULL,
    methodology_ref TEXT,                    -- 方法论/演进路线记忆 ref
    stop_conditions_json JSONB NOT NULL,
    budget_json     JSONB NOT NULL,
    status          TEXT NOT NULL,           -- running|stopped_target|stopped_no_improve|stopped_budget|failed
    loops_completed INTEGER NOT NULL DEFAULT 0,
    last_verdict_json JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE qe_autonomous_evolution_runs IS 'QE 自主演进主循环运行记录，含停止条件与预算守护';
```

---

## 9. L5：外部范式兑现对照表（防止"纸面引用"）

> 直接回应"参考是否被真实执行"。每个借鉴必须有"AIstock 落地位置 + 验收命令"。

| 范式 / 来源 | 借鉴点 | AIstock 落地位置 | 验收命令 | 不采用项 |
|---|---|---|---|---|
| ReAct（工具使用循环） | 调工具→观察→再推理 | L2 `chat_turn` 回灌循环（`service.py`） | `pytest test_react_tool_loop.py`：断言工具结果进入 messages 后再生成答案 | 不外显推理链 |
| Reflexion | 失败复盘自修正 | L2 §5.4 | `pytest test_reflexion_retry.py` | 不把反思当每轮输出 |
| MemTree（ICLR2025） | 在线树自扩展 | L1 curator `memory_curator.py` | `pytest test_memory_autogrow.py`：新主题自动建分支 | 不用向量聚类 |
| RAPTOR（ICLR2024） | collapsed-tree 召回 | L1 `select_memory_branches` | `pytest test_memory_tree_retrieval.py`：多分支命中 | 不用 embedding |
| Mem0 | user/session/agent scope + self-edit | L1 双树 + 去重 | `pytest test_memory_dedup_scope.py` | 不引入 Mem0 框架 |
| MemoryBank | recency×importance 打分 | L1 §4.3 分支内排序 | `pytest test_memory_scoring.py` | 不用 embedding 相关性 |
| 知识图谱 + GraphRAG/Zep | 关系/依赖遍历 | L1 `graph_context.py` 注入 | `pytest test_graph_injected_into_context.py`：跨模块 pack `graph_relation_refs>0` | 不引入图数据库 |
| Claude Code subagent | 隔离上下文+工具子集+结构化返回+并行 | L3 orchestrator/worker | `pytest test_agent_teams_parallel.py` | 不引入外部 agent 运行时 |
| OpenClaw | 声明式 agent 配置 + agentic loop | L3 `agent_teams.yaml` + L2 循环 | `pytest test_agent_teams_config.py` | 不做消息平台网关 |
| Anthropic orchestrator-workers | 主分解/worker执行/主综合 | L3 §7.5 | `pytest test_orchestrator_reduce.py` | 不全自动放权高风险 |
| Voyager / AI-Scientist | 自主课程 + 研究循环 | L4 自主主循环 | `pytest test_qe_autonomous_loop.py` | 不绕过预算/审批 |
| 外部搜索（console 设计 §12） | 多 provider + 学术 MCP + 证据优先 | L2.5 `external_research` | `python debug_tools/mcp/list_tools_smoke.py --server aistock-external-research` | Firecrawl 非默认入口 |

---

## 10. 实施阶段与验收标准

> 允许分 PR 推进，但**每个 Phase 必须明确"尚未完整"，所有 DAI/防漂移门禁全过才可称完整实现完成**。每个 Phase 验收均**不启动 8001/3000**；运行时验证由用户启动后另做只读 smoke。

### Phase 0：基线锁定与脚手架
- **交付**：本蓝图合入；新增 `backend/tests/research_assistant/` 占位测试目录；DDL 迁移脚本骨架（不执行生产 DDL）。
- **验收**：`git diff --check` 通过；本文档 §1.2 缺陷清单与 §12 矩阵 cross-check 无遗漏；`rg "DEF-0" 本文件` 命中 9 项。

### Phase 1：L1 记忆树（DDL + 召回 + curator）
- **交付**：§4.2 DDL 迁移 + `memory_tree.py` + `memory_curator.py` + `build_context_pack` 改造（树形召回替换平铺）+ 常驻注入。
- **验收**：
  - `pytest test_memory_tree_retrieval.py test_memory_autogrow.py test_memory_scoring.py test_memory_dedup_scope.py` 全绿。
  - **防漂移断言**：`build_context_pack` 对给定 query 返回的 refs 必须来自命中分支（含 `route_reason`），且 `directive/preference` 常驻项必现。
  - DDL gate：迁移在 8011/8012 验证库可重复执行（幂等），生产库不动，逐列报告 COMMENT。

### Phase 2：L1 知识图谱关系层接入
- **交付**：`graph_context.py` + `build_context_pack` 注入 `graph_relation_refs`。
- **验收**：`pytest test_graph_injected_into_context.py`：跨模块 query 的 context pack `len(graph_relation_refs) > 0`；**直接消灭 DEF-04 的"建了不用"**。

### Phase 3：L2 工具接地内核
- **交付**：`chat_turn` ReAct 回灌循环 + `assert_tool_in_catalog` 闸门 + `compose_with_evidence_guard` 证据契约 + Reflexion。
- **验收**：
  - `pytest test_react_tool_loop.py test_reflexion_retry.py test_evidence_guard.py test_tool_catalog_gate.py` 全绿。
  - **端到端断言（消灭 DEF-01）**：模拟"数仓有没有漏入仓"→ 必须先调真实只读 MCP→结果回灌→答案含来源；快照不含 `XX`/`X%`/`约X`。
  - 写入类工具在循环内只产 preflight+确认卡，不写入。

### Phase 4：L2.5 外部研究检索
- **交付**：`external_research` MCP module + profile + façade + 证据入库 + `.mcp.json` 登记。
- **验收**：`list_tools_smoke.py --server aistock-external-research` 显示工具 schema；`pytest test_external_research_evidence_first.py`：搜索结果只入 `external.*`/`topic.*`，不直接成结论；token 契约测试通过。

### Phase 5：L3 Agent Teams
- **交付**：`assistant_agent_runs` DDL + `agent_teams.yaml` + orchestrator/worker 实现 + 并行派发 + reduce + 隔离上下文 + 工具子集闸门。
- **验收**：
  - `pytest test_agent_teams_parallel.py test_orchestrator_reduce.py test_agent_teams_config.py test_worker_tool_isolation.py` 全绿。
  - **端到端**：一个跨模块目标被分解为 ≥2 个 worker 并行执行并 reduce；worker 只能用其 `allowed_tools`；高风险动作仍触发 approval。

### Phase 6：L4 QE 自主演进闭环
- **交付**：`qe_autonomous_evolution_runs` DDL + 自主主循环 + 停止条件 + 预算守护，挂为 `qe_experiment_designer` worker。
- **验收**：`pytest test_qe_autonomous_loop.py`：模拟连续 N 轮无改进 → 自动停止并报告；预算耗尽 → 停止；高成本 run 仍可要求确认。

### Phase 7：前端 + 全量验收
- **交付**：前端记忆树视图、Agent Teams 运行视图、证据卡；§12 可追溯性矩阵全部勾选。
- **验收**：全部 DAI（§13）通过；`pytest backend/tests/research_assistant -q` 全绿；用户启动后 Playwright 只读验收（输入"600584 是否值得买入"→ 证据/阻断卡，无占位符）。

---

## 11. 防设计漂移门禁（强制）

针对历史"设计与实现不一致 / 建了不用 / 设计丢失 / 业务断层"：

| 门禁 | 规则 | 自动化检查 |
|---|---|---|
| ANTI-DRIFT-01 | 每条设计项必须在 §12 矩阵映射到实现文件 + 测试 | CI 脚本校验矩阵无空行 |
| ANTI-DRIFT-02 | **禁止"建了不用"**：新增表/API 必须有"被推理链路消费"的断言测试 | 记忆→context pack、图谱→`graph_relation_refs>0`、工具结果→messages 的断言必须存在 |
| ANTI-DRIFT-03 | 禁止设计丢失 | §1.3 列全部前序来源；新增"外部搜索"承接项必须有 Phase 4 落地 |
| ANTI-DRIFT-04 | 禁止业务断层 | 每个 Phase 有端到端断言（非仅单元 mock） |
| ANTI-DRIFT-05 | 禁止占位/简化交付 | 快照测试禁 `XX`/`X%`/`约X`；read-only-only 不得声称完整 |
| ANTI-DRIFT-06 | 范式兑现 | §9 每行范式必须有验收命令且通过 |

---

## 12. 可追溯性矩阵（设计项 → 实现 → 测试）

| 设计项 | 缺陷 | 实现文件 | 测试 |
|---|---|---|---|
| 树形召回（无 RAG） | DEF-02/03 | `memory_tree.py`、`service.py:build_context_pack` | `test_memory_tree_retrieval.py` |
| 记忆真树 DDL | DEF-08 | `init_research_assistant_schema_*.py`、迁移脚本 | DDL 幂等测试 |
| 个人维度类型 | DEF-09 | `models.py:MEMORY_TYPES` | `test_memory_dedup_scope.py` |
| 自动扩展 curator | — | `memory_curator.py` | `test_memory_autogrow.py` |
| 反思巩固 + 治理 | — | `memory_curator.py`（定时） | `test_memory_reflection.py` |
| 知识图谱注入 | DEF-04 | `graph_context.py`、`build_context_pack` | `test_graph_injected_into_context.py` |
| ReAct 回灌 | DEF-01 | `service.py:chat_turn` | `test_react_tool_loop.py` |
| 能力闸门 | — | `service.py:assert_tool_in_catalog` | `test_tool_catalog_gate.py` |
| 证据契约 | — | `compose_with_evidence_guard` | `test_evidence_guard.py` |
| 外部研究 MCP | DEF-07 | `backend/mcp/modules/external_research.py`、façade | `test_external_research_evidence_first.py` |
| Agent Teams | DEF-05 | `assistant_agent_runs`、`agent_teams.yaml`、orchestrator/worker | `test_agent_teams_parallel.py` 等 |
| QE 自主闭环 | DEF-06 | `qe_evolution_service.py`、`qe_autonomous_evolution_runs` | `test_qe_autonomous_loop.py` |

> 实现时每完成一项，在本矩阵对应行追加 PR 链接与提交哈希，保持设计-实现强一致。

---

## 13. Design Acceptance Index（DAI）

| 编号 | 用户要求 | 设计位置 | 验收标准 |
|---|---|---|---|
| DAI-MEM-001 | 真·长期记忆（多轮/跨天/跨对话） | L1 §4 | 跨对话 query 召回相关 `personal.task.*`/`topic.*`，非平铺 |
| DAI-MEM-002 | 个人偏好/习惯/指令记忆 | L1 §4.1/4.4 | `directive/preference` 常驻注入；偏好自动入树 |
| DAI-MEM-003 | 自生长树（自动扩展分支） | L1 §4.4 | 新主题自动建 branch（MemTree 式） |
| DAI-MEM-004 | 双树（项目/个人）分治 | L1 §4.1 | 两根独立、治理分离、引擎共享 |
| DAI-MEM-005 | 无 RAG / 纯分类 | L1 §4.2/4.3 | 全链路无 embedding；外部语义仅在 provider 端 |
| DAI-GND-001 | 自然语言无幻觉调度真实 MCP | L2 §5 | 工具结果回灌后再生成；能力闸门拒绝幻想工具 |
| DAI-GND-002 | 只依赖真实 MCP/LLM 能力 | L2 §5.2 | catalog 外工具一律拒绝 |
| DAI-GND-003 | 全域证据契约 | L2 §5.3 | 无来源数值降级"证据不足"，禁占位符 |
| DAI-GRAPH-001 | 知识图谱真被使用 | L1 §4.5 | 跨模块 pack `graph_relation_refs>0` |
| DAI-EXT-001 | 外部搜索/学术检索 | L2.5 §6 | 学术 MCP 可 list_tools；结果只作证据 |
| DAI-TEAM-001 | Agent Teams 并行调度 | L3 §7 | ≥2 worker 并行 + reduce + 隔离上下文 |
| DAI-TEAM-002 | 主 agent 管调度+记忆 | L3 §7.5 | orchestrator 不干活，只分解/汇聚/记忆 |
| DAI-QE-001 | QE 基于方法论自主演进 | L4 §8 | 自主主循环 + 停止条件 + 预算守护 |
| DAI-PARADIGM-001 | 参考范式真兑现 | L5 §9 | 每范式有落地位置 + 验收命令 |
| DAI-DRIFT-001 | 无设计-实现漂移 | §11/§12 | 矩阵无空行；消费断言存在 |

---

## 14. 运行与生产边界

- 本文档阶段不触碰 `8001` / `3000`；后续实现也不得把"代码可见"误解为"重启生效"。
- 所有 DDL（§4.2/§7.4/§8.3）触发 `production_ddl_gate`，须逐项报告 COMMENT 与迁移幂等性，生产库由用户决定执行时机。
- 任何写入/高成本/生产敏感动作（数据修复、入仓补录、QE run、GitHub 正式写入、外部抓取）均经 preflight + 确认 + 审计；Agent Teams 与自主演进**不绕过**。
- MCP 不直接连 DB、不执行任意 Shell、不绕过 backend façade（沿用统一 gateway 原则）。

## 15. 合入标准

进入 `main` 前：
1. 文档通过 `git diff --check`。
2. 整合并引用全部前序设计来源（§1.3），不形成 competing architecture，且**承接已丢失的外部搜索设计**。
3. 明确 L0–L5 + L1 双树/图谱 + L2.5 外部研究的边界与承接。
4. 明确分阶段实施（§10）与每阶段验收标准、防漂移门禁（§11）、可追溯性矩阵（§12）、DAI（§13）。
5. 本 PR 为 docs-only：`production_ddl_gate=noop`、`production_backend_dependency_gate=noop`、`production_frontend_dependency_gate=noop`（DDL 在后续实现 PR 中按 §14 处理）。
