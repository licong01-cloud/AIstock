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
- **可移植/剥离是贯穿约束，不是尾部章节**：助手未来要能独立成产品、对接任意 MCP 应用，因此"核心-适配器解耦、最小化对 AIstock 的依赖"**必须在下面每一章、每个设计里就地考虑**（每章设「🔗 剥离考虑」小节标注 core/adapter 归属与解耦方式）。§17 仅是这些就地考虑的**汇总与横切规范**，不替代各章自身的剥离设计。**核心引擎不得直接依赖 AIstock 的 façade/DB/领域符号**（见 §2.1 原则 7、§11 ANTI-DRIFT-11）。

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
| DEF-06 | QE 无跨 loop 自主闭环 | `qe_evolution_service.py:133` `AutoEvolutionScheduler` + `:1600-1634` `submit_next_loop` 仍是单 loop 流转；`:5243`/`:5409`/`:5506` rerun/custom loop 均被动触发 | 不能自主演进 |
| DEF-07 | 外部搜索/学术检索零实现 | 后端 grep `arxiv/scholar/tavily/web_search/paper_search` = 0 文件 | 无文献接地 |
| DEF-08 | 记忆表无真树列 | `init_research_assistant_schema_20260521.py:143-171` `research_memory_items` 仅 `namespace + memory_type + 点分键`，无 `parent_key/tree_path`（仅 `assistant_prompt_nodes` `:548-565` 有） | 树存储是弱约定 |
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
7. **可移植性与核心解耦（贯穿每一层）**：每一层都区分 `assistant_product_core`（领域无关引擎）与 `aistock_domain_adapter`/`aistock_knowledge_pack`（AIstock 领域内容）。core 一律通过 **provider 接口**（MCP/Memory/Storage/Model/Skill/Channel/KnowledgePack）访问外部，**不得 import AIstock 的 façade/DB/业务 service/领域符号**；AIstock 只是"第一个 adapter"。目标姿态为**可移植接缝**（现在不物理拆包，但耦合点全部收敛到接口背后，未来剥离成本低）。每章的「🔗 剥离考虑」小节给出该层的 core/adapter 划分与解耦做法；汇总见 §17。

### 2.2 红线（禁止形态）

- 禁止把任一层落地为 read-only-only 却声称完整。
- 禁止"建了表/API 不接推理链路"（必须有消费断言测试，见 §11）。
- 禁止外部论文/搜索结果直接成为结论（只能进 `external_evidence` / `research_hypothesis`）。
- 禁止 Agent Teams 让模型自行决定高风险执行（高风险动作仍走 preflight + approval）。
- 禁止 QE 自主演进绕过预算 / 停止条件 / 审批。
- 禁止任何阶段触碰 `8001` / `3000`。
- **禁止核心引擎直接耦合 AIstock**：`assistant_product_core` 模块不得 import/调用 AIstock 的 `8001` façade、AIstock DB schema、AIstock 业务 service 或领域符号；一切经 provider 接口。任何新增层/表/服务若把 AIstock 领域内容写死进 core，视为违规（见 §11 ANTI-DRIFT-11）。

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

> **每层都是 core + adapter 两半**：上图每一层的"机制/引擎"属 `assistant_product_core`（领域无关，经 provider 接口工作），其"AIstock 领域内容"（12 个 MCP、业务本体/图谱 seed、QE 方法论、股票证据规则、worker 定义）属 `aistock_domain_adapter` / `aistock_knowledge_pack`。各层正文末尾的「🔗 剥离考虑」小节给出具体划分；横切规范与 provider 接口清单见 §17。

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

### 4.6 🔗 剥离考虑（core/adapter）

- **core（`assistant_product_core`）**：树引擎（`parent_key`/`tree_path`/分类召回/collapsed 选择/`importance×recency` 打分/curator 自扩展/反思巩固）、`MEMORY_TYPES` 的通用维度（`core/episodic/user_preference/directive/habit/analysis_note/task_state`）、`Memory Provider` 与 `Storage Provider` 接口。记忆引擎**不认识** AIstock。
- **adapter/pack（`aistock_knowledge_pack`）**：`project.*` 项目知识树的具体内容（AIstock 模块/MCP/红线本体）、知识图谱 seed（`module/consumes/owned_by`）、QE/Paper 来源绑定。
- **解耦做法**：① 记忆读写一律经 `Memory/Storage Provider`，**不直连 AIstock DB**（当前默认实现指向 AIstock PostgreSQL，但通过接口）；② `project.*` 树与图谱 seed 作为**可加载知识包**，core 默认空树空图，加载 AIstock pack 后才有领域内容；③ `personal.*` 个人树与 AIstock 无关，天然可移植。
- **最小依赖断言**：`memory_tree.py`/`graph_context.py` 不得 import AIstock 业务 service（§11 ANTI-DRIFT-11）。

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

### 5.5 🔗 剥离考虑（core/adapter）

- **core**：ReAct 回灌循环、`assert_tool_in_catalog` 能力闸门、证据契约**机制**（"无源不下数值结论"这一通用规则）、Reflexion 复盘。这些都与领域无关。
- **adapter/pack**：证据契约里的**领域专用规则**（股票 PE/资金流/支撑位、QE 指标口径）、catalog 里的 AIstock MCP 工具集。
- **解耦做法**：① 工具调用经 `MCP Provider`（通用 `list_tools` 发现，见 §17.5），**不写死 AIstock 12 个 server**；② 闸门只校验"工具是否在当前已审核 catalog 内"，与具体是不是 AIstock 工具无关；③ 领域证据规则放进 `aistock_knowledge_pack` 的 evidence-rule 配置，core 只保留通用"占位符禁令 + 无源降级"。
- **最小依赖断言**：ReAct 内核不 import 任何 AIstock 领域模块；领域规则经 KnowledgePack Provider 注入。

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

### 6.4 🔗 剥离考虑（core/adapter）

- **core**：外部检索**连接框架** + `Search Provider` 接口 + 证据入库机制（结果→`external.*` 候选、provenance、证据优先红线）。
- **adapter/pack**：具体 provider 选型（博查/秘塔/arXiv/Semantic Scholar）与 AIstock 的领域查询偏好（因子/HMM/事件研究）。
- **解耦做法**：检索 provider 全部经 `Search Provider` 接口可替换；core 不绑定任何搜索厂商；"外部资料只作证据不作结论"是通用红线，随 core 走。
- **最小依赖断言**：`external_research` 模块经 provider 接口工作，不依赖 AIstock 领域 service。

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

### 7.7 🔗 剥离考虑（core/adapter）

- **core**：orchestrator/worker **运行时**（分解/并行派发/隔离上下文/结构化返回/reduce/仲裁）、`assistant_agent_runs` 表、声明式 worker 加载机制。
- **adapter/pack**：具体 worker 定义（`qe_experiment_designer`/`hmm_evolution`/`factor_developer`/`local_data_doctor` 均为 AIstock 领域）及其 `allowed_tools`（AIstock MCP）。
- **解耦做法**：① worker 全部经 `agent_teams.yaml` **声明式定义**（role/goal/tools/model），运行时不硬编码任何具体 worker；② worker 的工具子集经 `MCP Provider`；③ orchestrator 与"有哪些 worker"解耦——换领域只换 worker 配置 + 知识包，运行时不动。
- **最小依赖断言**：orchestrator/worker runtime 不 import 具体领域 worker 的业务逻辑（业务在 adapter 侧实现，经接口注册）。

---

## 8. L4：QE 自主演进闭环

> 解决 DEF-06。承接现有 `AutoEvolutionScheduler`（`qe_evolution_service.py:133` + `submit_next_loop` `:1600-1634`）+ Analyst 两步 + Evaluator 三层。范式：Voyager（技能库+自主课程）、AI-Scientist（研究循环）。作为 L3 的 `qe_experiment_designer` worker 落地。

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

### 8.4 🔗 剥离考虑（core/adapter）

> L4 是**领域性最强**的一层（QE 是 AIstock 特有），但"自主演进"这套状态机可抽象复用。

- **core**：自主主循环**框架**（`loop→evaluate→decide→generate→budget/stop guard→submit` 状态机）、停止条件/预算守护机制——以"评估器 / 方向决策器 / loop 执行器"为**可插拔回调**。
- **adapter/pack**：QE 特有的 Evaluator 三层、Analyst 两步、loop 执行、`qe_autonomous_evolution_runs`、QE 方法论/演进路线。
- **解耦做法**：自主循环框架经回调接口调 QE，不被 QE 反向绑死；未来其他领域（因子/HMM 演进）可复用同一框架，只换回调实现。AIstock QE 是该框架的第一个实例。
- **最小依赖断言**：自主循环框架模块不 import QE 业务 service，经注册的回调接口交互。

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
| 插件/适配器 + MCP 通用接口 + OpenClaw provider 形态 | core 稳定、领域可插拔、对接任意 MCP 应用 | 各层 §x.x「🔗 剥离考虑」+ §17 横切规范 | `pytest test_core_no_adapter_import.py`（依赖方向） | 现在不物理拆包，仅逻辑接缝 |

---

## 10. 实施阶段与验收标准

> 允许分 PR 推进，但**每个 Phase 必须明确"尚未完整"，所有 DAI/防漂移门禁全过才可称完整实现完成**。每个 Phase 验收均**不启动 8001/3000**；运行时验证由用户启动后另做只读 smoke。
>
> **跨阶段剥离门禁（适用于每一个 Phase）**：任一 Phase 新增的 core 机制/表/服务，验收时必须同时满足该层「🔗 剥离考虑」的 core/adapter 划分——即 core 代码经 provider 接口工作、不 import AIstock 领域符号（`pytest test_core_no_adapter_import.py` 依赖方向检查通过）。**剥离不是 §17 单独阶段才做，而是每个 Phase 的交付门禁**（§17 的 P13–P15 是对这一约束的集中收敛与验证，不替代各 Phase 的就地遵守）。

### Phase 0：基线锁定与脚手架
- **交付**：本蓝图合入；新增 `backend/tests/research_assistant/` 占位测试目录；DDL 迁移脚本骨架（不执行生产 DDL）。
- **验收**：`git diff --check` 通过；本文档 §1.2/§16.1 缺陷清单与 §12/§16.9/§17.10 矩阵 cross-check 无遗漏；`rg -n "DEF-0|DEF-1" 本文件` 覆盖 DEF-01~13。

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
| ANTI-DRIFT-11 | **核心解耦贯穿每层**：`assistant_product_core` 不得 import/依赖 AIstock façade/DB/领域符号；每层「🔗 剥离考虑」的 core/adapter 划分必须落实 | 依赖方向检查 `test_core_no_adapter_import.py`；core 模块一律经 provider 接口工作（详见 §17.9 ANTI-DRIFT-11~13） |

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
| 核心/适配器解耦（贯穿每层） | DEF-13 | 各层「🔗 剥离考虑」、7 类 provider 接口、依赖检查脚本、§17 | `test_core_no_adapter_import.py` |

> 实现时每完成一项，在本矩阵对应行追加 PR 链接与提交哈希，保持设计-实现强一致。

> Phase 0 基线锁定锚点：`docs/process/research_assistant_baseline_verification_20260531.md` 逐条复验 DEF-01~12 与 §1.1 资产；`backend/tests/research_assistant/test_phase0_blueprint_baseline.py`、`tests/aistock_validation/catalog/module_registry.yaml`、`tests/aistock_validation/catalog/file_ownership.yaml`、`tests/aistock_validation/catalog/test_plans.yaml`、`noxfile.py` 将本矩阵登记为 `ra_phase0_baseline` 闸门。Phase 0 原始实现 commit `53a0f03d6a2bb05049a99f57998c3845b7d681f1`，rebase 后合入前 HEAD `cff0b243`；G1-central run_id `research-assistant_20260601_011521_l0_ra-phase0-baseline_fba1c3de_runner-validation__289612b1db`，validated_commit `fba1c3de`，`return_code=0`。本锚点只声明基线和登记，不把未来行标记为已实现。

> Phase 1 G3 回填锚点：`backend/db/migrations/ra_upgrade/001_memory_tree.sql`、`backend/db/init_research_assistant_schema_20260521.py`、`backend/services/research_assistant/models.py`、`backend/services/research_assistant/memory_tree.py`、`backend/services/research_assistant/memory_curator.py`、`backend/services/research_assistant/service.py`、`configs/research_assistant/runtime_context.yaml`、`backend/tests/research_assistant/test_memory_tree_ddl_contract.py`、`backend/tests/research_assistant/test_memory_tree_retrieval.py`、`backend/tests/research_assistant/test_memory_scoring.py`、`backend/tests/research_assistant/test_memory_autogrow.py`、`backend/tests/research_assistant/test_memory_dedup_scope.py`、`backend/tests/research_assistant/test_core_no_adapter_import.py`、`tests/aistock_validation/catalog/test_plans.yaml`、`backend/services/validation/plan_catalog.py`、`noxfile.py` 将树形召回、记忆真树 DDL、个人维度类型、自动扩展 curator 接入 `ra_phase1_memory_tree` 本地闸门。Phase 1 本地 G1 commit 待提交后回填；G1-central 仍受 Validation Center branch-local plan blocker 约束，未补 canonical run_id 前不得宣称 Phase 1 完成或合入。

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

---

## 16. 蓝图增补（2026-05-31）：代码智能 / 主动汇报 / 自我学习

> 增补背景：复查发现三项"原始设计存在、但未实现且未被本蓝图覆盖"的能力（典型"设计丢失"）。它们出自 issue-workflow / 研究 Agent 控制台 设计线，不在 §1.3 原 RA 命名文档清单内，故初版蓝图遗漏。本章按与正文一致的标准补齐：缺陷证据、来源承接、目标架构、DDL、分阶段实施、每阶段审计/验收、防漂移、可追溯性、DAI、外部范式兑现。本章同样不触碰 `8001`/`3000`，DDL 延后到实现 PR 按 §14 处理。

### 16.1 新增缺陷（附证据）

| 编号 | 缺陷 | 证据 | 影响 |
|---|---|---|---|
| DEF-10 | 代码智能适配器存在但未接入助手推理 | `scripts/code_intelligence_adapter.py`（+ `backend/tests/scripts/test_code_intelligence_adapter.py`、`tests/aistock_validation/catalog/code_intelligence.yaml`）已存在；但 `backend/services/research_assistant/` 无 codegraph/impact 引用，未注入 Context Pack | 跨模块/"这个模块怎么工作/改这里影响谁"靠猜，token 浪费 |
| DEF-11 | 主动晨报 / 实验日报仅占位、未生成 | `service.py:3598-3599` 仅有"研究助理晨报模板"，body 注明"夜间自动任务将在后续阶段写入具体晨报" | 无主动汇报（Jarvis 式主动性缺失） |
| DEF-12 | 无自我学习 / 提示词自评估闭环 | backend 无 `prompt_lab/reflection_card/research_curriculum` 实现；记忆有反思巩固但无"提示词/策略自改进" | 助手不能"越用越准"，提示词靠人工维护 |

> 澄清（防误判）：复查中曾被怀疑"未实现"的上下文压缩 + key facts（`assistant_context_segments`/`assistant_context_key_facts`，写入见 `service.py:2381-2417`，回灌见 `:2316-2323`）、external_agent_session（`service.py:3402` + schema:490）、LLM 真实调用（`service.py:569` litellm）、mode router / `_select_prompt_nodes`（`:1295`）/ `tool_router.py` / `domain_ontology.py` 均**已实现并接入**，不属于缺口；用户画像由 L1 `personal.preference/habit` 覆盖；experiment lineage 由 `research_evolution_paths` + L4 覆盖。

### 16.2 设计来源承接（补 §1.3）

| 前序设计 | 提供内容 | 本章承接 |
|---|---|---|
| `aistock_code_intelligence_integration_design_20260526.md` | CodeGraph（tree-sitter 代码结构/调用链/影响半径/受影响测试，MCP/CLI）+ Understand Anything（代码知识图谱）作为 Context Pack/Research Assistant 证据层；只存轻量 manifest/impact summary/context refs，带 provenance + approval；不替代 nox/CI/生产门禁 | L1.6 直接承接 |
| `aistock_research_agent_console_design_20260520.md` | 晨报/实验报告；Prompt Lab、Reflection Card、Research Curriculum、prompt feedback 自我学习闭环 | L6 / L7 承接 |
| `aistock_research_agent_console_validation_matrix_20260521.md` | 上述能力的 PoC/验收项 | L6/L7 验收承接 |

### 16.3 外部范式兑现（补 §9，前沿论文/工具，均可配国产模型、可门禁化）

| 范式 / 来源 | 借鉴点 | AIstock 落地 | 验收命令 | 不采用项 |
|---|---|---|---|---|
| Codebase-Memory（arXiv 2603.27277，tree-sitter KG via MCP，10x 省 token） | 确定性代码图 + 结构化查询工具 | L1.6 复用 `code_intelligence_adapter.py` 的 CodeGraph 输出 | `pytest test_code_intel_context_injection.py` | 不在 MCP 内做全仓 LLM 扫描 |
| Reliable Graph-RAG for Codebases（arXiv 2601.08773，AST确定性 > LLM抽取） | 用 AST 确定性图，避免随机性 | L1.6 仅用 tree-sitter/AST 派生图，**无 embedding** | 同上 | 不用 LLM 抽取代码图 |
| RepoGraph（agent +32.8%） / CodeRAG 双图（arXiv 2504.10046） | repo 级代码图提升 agent | L1.6 注入 L3 任务分解 | `pytest test_code_intel_decomposition.py` | 不引入外部 agent 运行时 |
| GEPA（arXiv 2507.19457，反思式提示词进化，比 RL 省 35x rollout，已入 DSPy） | 反思式提示词优化（ASI 反馈 + Pareto 池） | L7 Prompt Lab 优化器 | `pytest test_prompt_lab_gepa_offline.py` | 不引入 RL/权重训练 |
| DSPy（声明式自改进，MIPROv2/BootstrapFewShot） | 提示词作可优化参数 + 评估驱动 | L7 优化与评估编排（可选直接用 dspy 库） | 同上 | 不强绑 DSPy 运行时（可纯自实现） |
| Reflexion（arXiv 2303.11366，言语强化 + 情景反思缓冲） | 失败言语反思入情景记忆 | L7 Reflection Card → `personal.episodic.*` | `pytest test_reflection_card_loop.py` | 不外显推理链 |
| Voyager（arXiv 2305.16291）/ SAGE（arXiv 2512.17102，技能库 + 经验回放） | 可复用技能库 + 自主课程 | L7 技能库（沉淀成功 workflow/prompt 为可复用技能）+ L4 课程 | `pytest test_skill_library.py` | 不做权重级 RL |
| LLM-as-Judge / Agent-as-a-Judge（arXiv 2508.02994） | 评估信号闭环；**自改进须作为受门禁的提议** | L7 评估用 judge，改进一律走 approval | `pytest test_prompt_lab_judge_gated.py` | judge 不自动上线，须人工 approve |

> 安全基线（采纳论文共识）：**所有自我学习只产出"提议"，经评估 + 人工 approval 才生效**，绝不自动改写线上提示词/策略（呼应 §2.2 红线与既有 approval 闭环）。

### 16.4 L1.6 代码智能层（解决 DEF-10）

定位：把**代码级**结构图（函数/类/调用链/导入/影响半径/受影响测试）作为助手证据层，与 L1 的**业务级**知识图谱互补——业务图答"模块依赖谁消费谁"，代码图答"具体哪个函数/调用链/测试受影响"。**纯 AST 确定性，无 embedding**（采纳 arXiv 2601.08773 结论）。

- **复用现有资产**：`scripts/code_intelligence_adapter.py`（CodeGraph/Understand-Anything 适配，已存在）。L1.6 不重写适配器，只做"接入 Research Assistant"。
- **后端 façade**：新增 `/api/v1/research-assistant/code-intelligence/*`（或在 RA 服务内封装），调用 adapter，返回 summary-first 的轻量结果（manifest/impact summary/context refs），带 `provenance` + `as_of`，遵守 token 契约。
- **注入推理**：`build_context_pack` 对涉及具体代码/模块的 query，追加 `code_context_refs`（调用链/影响半径/受影响测试摘要）；喂 L3 orchestrator 任务分解（"改这里→受影响 worker/测试"）。
- **证据契约**：代码结论须可追溯到 adapter 输出（文件/符号/边），无则降级；不替代 nox/pytest/CI（沿用原设计红线）。

DDL（轻量缓存，触发 production_ddl_gate）：

```sql
CREATE TABLE IF NOT EXISTS assistant_code_context_refs (
    code_ref_id   TEXT PRIMARY KEY,
    task_id       TEXT,
    query_scope   TEXT NOT NULL,           -- symbol/module/path
    manifest_json JSONB NOT NULL,          -- adapter 输出的轻量 manifest/impact summary
    source        TEXT NOT NULL,           -- codegraph | understand_anything
    provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb,  -- {commit, file, symbol, generated_at}
    as_of         TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE assistant_code_context_refs IS '代码智能(CodeGraph/Understand)注入 Context Pack 的轻量引用，AST确定性，无embedding';
```

**🔗 剥离考虑（core/adapter）**：core = 代码智能**注入框架** + `Code-Intelligence Provider` 接口（输入 query → 返回结构化 manifest/impact，与具体工具无关）；adapter = `code_intelligence_adapter.py` 对 AIstock 仓库的绑定。解耦：core 只依赖 provider 接口，底层换成任意 CodeGraph/Understand/其它实现都行；对接别的产品时只换 provider 指向其代码库，注入框架不动。

### 16.5 L6 主动汇报（解决 DEF-11）

定位：把占位的"晨报模板"落地为**真实生成**的主动汇报，作为 L3 的**定时 orchestrator 任务**（不是用户每次提问才触发）。

- **聚合源**（全部只读、证据优先）：任务/任务事件、QE/实验状态、Validation/BUG/Issue、本地数据健康、Agent Teams 运行、`personal.task.*` 进展。
- **生成**：orchestrator 用 `cheap_worker` 汇总为自然语言晨报 + 关键证据 + 待办；每条事实带来源（沿用 L2 证据契约，禁占位符）。
- **触发**：定时（如交易日早间）；产物写报告表，前端/推送展示。
- **边界**：只读聚合 + 汇报，不触发任何写入/高风险动作。

DDL：

```sql
CREATE TABLE IF NOT EXISTS assistant_proactive_reports (
    report_id     TEXT PRIMARY KEY,
    report_type   TEXT NOT NULL,           -- morning_brief | experiment_daily
    report_date   DATE NOT NULL,
    summary_md    TEXT NOT NULL,
    sections_json JSONB NOT NULL,          -- 各板块摘要 + source_refs
    source_refs_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    status        TEXT NOT NULL DEFAULT 'generated',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_apr UNIQUE (report_type, report_date)
);
COMMENT ON TABLE assistant_proactive_reports IS '主动晨报/实验日报，只读聚合 + 证据优先，不触发写入';
```

**🔗 剥离考虑（core/adapter）**：core = 汇报**生成框架**（聚合源注册表 → 证据优先汇总 → 报告表）；adapter = 具体聚合源（QE/Validation/本地数据 均为 AIstock 数据源）。解耦：聚合源经 registry/provider 注册，core 不写死 AIstock 数据源；换领域时换一组聚合源清单即可复用同一汇报框架。

### 16.6 L7 自我学习（解决 DEF-12，全程"提议→评估→approval"门禁）

定位：让助手基于成败**自改进提示词与策略**，并沉淀**可复用技能**——但绝不自动上线。

三个子机制（均可配国产模型，经 litellm）：
1. **Reflection Card（Reflexion）**：任务失败/纠偏后生成结构化反思（错因/教训/下次策略），写入 `personal.episodic.*` 并可被 L1 召回；不外显推理链。
2. **Prompt Lab（DSPy/GEPA）**：以历史 trace 为评估集，用 **GEPA 反思式优化** 生成候选提示词；用 **LLM-as-judge** 离线评估；**仅产出 candidate**，经人工 approval 才切换 prompt activation（复用现有 Prompt Pack 版本/激活机制）。
3. **技能库（Voyager/SAGE）**：把验证成功的 workflow/prompt 组合沉淀为可复用"技能"，相似任务经验回放；纳入 L4 自主课程。

DDL：

```sql
CREATE TABLE IF NOT EXISTS assistant_reflection_cards (
    card_id       TEXT PRIMARY KEY,
    task_id       TEXT,
    trigger       TEXT NOT NULL,           -- failure | correction | low_confidence
    lesson_md     TEXT NOT NULL,
    structured_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    memory_ref    TEXT,                    -- 写入的 personal.episodic.* memory_id
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS assistant_prompt_lab_runs (
    lab_run_id    TEXT PRIMARY KEY,
    target_prompt_key TEXT NOT NULL,
    optimizer     TEXT NOT NULL,           -- gepa | dspy_mipro | manual
    eval_set_ref  TEXT NOT NULL,           -- 历史 trace 评估集引用
    candidate_text TEXT NOT NULL,
    judge_score_json JSONB NOT NULL,       -- LLM-as-judge 评估结果
    status        TEXT NOT NULL DEFAULT 'candidate',  -- candidate | approved | rejected
    approval_request_id TEXT,              -- 复用 assistant_approval_requests
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS assistant_skill_library (
    skill_id      TEXT PRIMARY KEY,
    skill_key     TEXT NOT NULL UNIQUE,
    description   TEXT NOT NULL,
    recipe_json   JSONB NOT NULL,          -- 可复用 workflow/prompt/tool 组合
    success_count INTEGER NOT NULL DEFAULT 0,
    provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status        TEXT NOT NULL DEFAULT 'draft',  -- draft | approved | deprecated
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE assistant_prompt_lab_runs IS '提示词自优化候选(GEPA/DSPy)+LLM-judge评估；仅候选，approval后才激活';
```

**🔗 剥离考虑（core/adapter）**：三机制（Reflection/Prompt Lab/技能库）**全部属 core、天然可移植**——它们作用于"本系统自己的 trace/prompt/skill"，与领域无关。adapter 侧仅是"评估集取自哪个领域的 trace、技能库沉淀哪个领域的 recipe"。解耦：自我学习引擎对任意领域的 trace/prompt 都适用，AIstock 只是数据来源；自改进产物经 approval 门禁，与剥离姿态一致。

### 16.7 分阶段实施与验收（接 §10，Phase 8–12；不启动 8001/3000）

#### Phase 8：L1.6 代码智能接入（DEF-10）
- 交付：façade `/code-intelligence/*` 包裹现有 adapter；`assistant_code_context_refs` DDL；`build_context_pack` 注入 `code_context_refs`。
- 验收：
  - `pytest test_code_intel_context_injection.py test_code_intel_decomposition.py` 全绿。
  - **防漂移消费断言**：涉及具体符号/模块的 query，context pack `code_context_refs` 非空且每条带 provenance（直接消灭 DEF-10"建了不用"）。
  - 纯 AST，无 embedding 依赖；token 契约（summary-first）测试通过。

#### Phase 9：L6 主动晨报生成（DEF-11）
- 交付：`assistant_proactive_reports` DDL；orchestrator 定时任务聚合生成；替换占位模板。
- 验收：
  - `pytest test_proactive_report_generation.py`：晨报含任务/实验/Issue/数据健康板块，每条带 source_refs，快照无 `XX`/`X%`/`约X`。
  - 只读断言：生成过程不产生任何写入/高风险 action proposal。

#### Phase 10：L7 Reflection Card（DEF-12 之一）
- 交付：`assistant_reflection_cards` DDL；失败/纠偏触发生成并写 `personal.episodic.*`。
- 验收：`pytest test_reflection_card_loop.py`：失败任务生成反思卡 + 入记忆树 + 可被 L1 召回；不外显推理链。

#### Phase 11：L7 Prompt Lab（DEF-12 之二，门禁化）
- 交付：`assistant_prompt_lab_runs` DDL；GEPA/DSPy 离线优化 + LLM-judge 评估；candidate → approval → 激活（复用 Prompt Pack 激活）。
- 验收：
  - `pytest test_prompt_lab_gepa_offline.py test_prompt_lab_judge_gated.py`：产出 candidate + judge 分数；**未经 approval 不得改 activation**（断言）。
  - 离线：不调用生产、不改线上 prompt。

#### Phase 12：L7 技能库 + 课程（DEF-12 之三）
- 交付：`assistant_skill_library` DDL；成功 workflow 沉淀为技能；接入 L4 自主课程经验回放。
- 验收：`pytest test_skill_library.py`：成功任务沉淀技能（draft）+ approval 后可复用；技能复用经审批，不绕过风险门禁。

### 16.8 防设计漂移门禁（补 §11）

| 门禁 | 规则 | 检查 |
|---|---|---|
| ANTI-DRIFT-07 | 代码智能必须被消费 | context pack `code_context_refs` 注入断言（DEF-10） |
| ANTI-DRIFT-08 | 晨报必须真实生成且证据优先 | 生成断言 + 无占位符快照（DEF-11） |
| ANTI-DRIFT-09 | 自我学习仅提议、须 approval | Prompt Lab 未审批不得改 activation 的断言（DEF-12） |
| ANTI-DRIFT-10 | 自我学习只读训练、离线评估 | 不调用生产、不改线上 prompt 的断言 |

### 16.9 可追溯性矩阵（补 §12）

| 设计项 | 缺陷 | 实现文件 | 测试 |
|---|---|---|---|
| 代码智能接入 | DEF-10 | `code_intelligence_adapter.py`(复用)、façade、`build_context_pack`、`assistant_code_context_refs` | `test_code_intel_context_injection.py` |
| 主动晨报 | DEF-11 | orchestrator 定时任务、`assistant_proactive_reports` | `test_proactive_report_generation.py` |
| Reflection Card | DEF-12 | `assistant_reflection_cards`、curator | `test_reflection_card_loop.py` |
| Prompt Lab | DEF-12 | `assistant_prompt_lab_runs`、GEPA/DSPy+judge | `test_prompt_lab_gepa_offline.py` / `_judge_gated.py` |
| 技能库 | DEF-12 | `assistant_skill_library`、L4 课程 | `test_skill_library.py` |

> Phase 0 增补锚点：`research_assistant.code_intelligence`、`research_assistant.proactive_reports`、`research_assistant.reflection_card`、`research_assistant.prompt_lab`、`research_assistant.skill_library` 已在 module/file ownership catalog 中登记；Phase 0 实现 commit `53a0f03d6a2bb05049a99f57998c3845b7d681f1`。

### 16.10 Design Acceptance Index（补 §13）

| 编号 | 用户要求 | 设计位置 | 验收标准 |
|---|---|---|---|
| DAI-CODE-001 | 代码智能助 LLM 理解任务/跨模块 | L1.6 §16.4 | 代码 query 的 pack `code_context_refs` 非空且带 provenance |
| DAI-CODE-002 | 代码图无 embedding、AST 确定性 | L1.6 §16.4 | 无 embedding 依赖；纯 adapter/AST 输出 |
| DAI-REPORT-001 | 主动晨报/汇报 | L6 §16.5 | 真实生成、证据优先、只读 |
| DAI-LEARN-001 | 自我学习/提示词自改进 | L7 §16.6 | Reflection + Prompt Lab + 技能库 |
| DAI-LEARN-002 | 自改进受门禁 | L7 §16.6 | 仅候选，approval 后激活；不自动上线 |
| DAI-DRIFT-002 | 增补项无漂移 | §16.8/§16.9 | 矩阵无空行；消费/门禁断言存在 |

### 16.11 边界

- 本章 DDL（§16.4/16.5/16.6）均触发 `production_ddl_gate`，实现 PR 逐项报告 COMMENT 与幂等性，生产库由用户决定执行时机。
- 代码智能不替代 nox/pytest/CI/生产门禁；自我学习不自动改线上提示词/策略；主动汇报只读。
- 本 PR 仍为 docs-only：`production_ddl_gate=noop`、`production_backend_dependency_gate=noop`、`production_frontend_dependency_gate=noop`。

---

## 17. 横切设计（2026-05-31）：可移植性与独立产品化

> 背景：助手当前是 AIstock 的一个模块，未来可能**独立成一个智能工具软件产品，对接任意提供 MCP 接口的应用**。原始设计 `aistock_research_agent_console_design_20260520.md` 行 2142「Phase 4：独立产品化」**仅有 5 条意图占位**（抽离 `assistant_product_core`、AIstock 作首个 adapter、可替换 Memory/Skill/MCP/Channel Provider、私有数据不外泄），无具体架构；初版蓝图（L0–L7 + §16）**未考虑此点，深度耦合 AIstock**（loopback `8001` façade、AIstock DB 表、写死 12 个 AIstock MCP、AIstock 业务本体）。本章把它升级为**横切架构约束**——因为它影响每一层的构建方式，若不在建设期留好接缝，未来独立产品化将整体返工。

> **重要（2026-05-31 修订）**：可移植/剥离不再只放本章。已下沉为**贯穿全文的一等约束**——§2.1 原则 7、§2.2 红线、§3 总览、以及 §4.6/§5.5/§6.4/§7.7/§8.4/§16.4/§16.5/§16.6 每层的「🔗 剥离考虑」小节，都就地给出该层的 core/adapter 划分与解耦做法；§10 把"core/adapter 边界"列为**每个 Phase 的交付门禁**，§11 ANTI-DRIFT-11 强制依赖方向检查。**本章 §17 退为这些就地考虑的汇总与横切规范（provider 接口清单、通用 MCP 客户端、知识包隔离、P13–P15 集中验证）**，不替代各章自身的剥离设计。本章不触碰 `8001`/`3000`，DDL 延后到实现 PR。

### 17.1 新增缺陷

| 编号 | 缺陷 | 证据 | 影响 |
|---|---|---|---|
| DEF-13 | 助手核心与 AIstock 领域强耦合，无 core/adapter 边界、无通用 MCP 客户端、无 provider 抽象 | 蓝图各层依赖 `8001` façade、AIstock DB schema、写死 12 个 MCP server；原设计仅有 Phase 4 意图占位（console 设计 :2142-2150） | 未来独立产品化/对接外部 MCP 应用需整体返工 |

### 17.2 设计姿态（用户决策）：可移植接缝（非现在物理拆分）

- **现在仍以 AIstock 为主交付**，不要求 Phase 0–12/§16 立即物理拆包重构。
- 但**所有耦合点收敛到 provider 接口背后**，并**定义清晰的 core/adapter 逻辑边界**；新代码一律按接口写，旧耦合点逐步收敛。
- 用**静态依赖方向检查**保证"核心不反向依赖 AIstock 领域"，使未来物理抽离成本低、当前每层成本仅小幅增加。

### 17.3 Core / Adapter 边界（回贴 L0–L7 + §16）

| 层 | 归属 | 说明 |
|---|---|---|
| L0 模型路由 / Prompt Tree 引擎 / 审批风险引擎 / 模式状态机 | **core** | 领域无关运行时 |
| L1 记忆树**引擎**（分类召回/打分/curator/反思机制） | **core** | 机制领域无关 |
| L1 业务知识图谱 seed + AIstock 本体 | adapter/pack | AIstock 领域内容 |
| L1.6 代码智能**注入框架** | **core** | 通用 |
| L1.6 `code_intelligence_adapter.py` 绑定与仓库 | adapter | AIstock 仓库特定 |
| L2 ReAct 接地循环 + 能力闸门 + 证据契约**机制** | **core** | 通用 |
| L2 证据契约里的**股票/QE 专用规则** | adapter/pack | AIstock 领域规则 |
| L2.5 外部研究**连接框架** | **core** | provider 化 |
| L3 Agent Teams orchestrator/worker **运行时** | **core** | 通用编排 |
| L3 worker 定义（QE/HMM/因子/数据诊断） | adapter/pack | AIstock 领域 worker |
| L4 QE 自主演进循环**框架** | **core** | 通用循环 |
| L4 QE 方法论/演进路线**内容** | adapter/pack | AIstock 领域 |
| L6 主动汇报**框架** | **core** | 通用 |
| L6 聚合源（QE/Validation/本地数据…） | adapter | AIstock 数据源 |
| L7 自我学习引擎（Reflection/Prompt Lab/技能库） | **core** | 通用 |
| 12 个 AIstock MCP + façade + 领域 prompt pack | adapter/pack | AIstock 领域 |

`assistant_product_core` = 领域无关引擎；`aistock_domain_adapter` + `aistock_knowledge_pack` = AIstock 全部领域内容。**AIstock 是第一个 adapter，不是唯一。**

### 17.4 Provider 接口（可替换，逻辑接缝）

| Provider | 抽象内容 | AIstock 默认实现 | 未来可替换为 |
|---|---|---|---|
| **MCP Gateway/Client Provider** | 连接任意 MCP server、能力发现、调用 | AIstock 统一 gateway + 12 server | 任意 MCP 应用（见 17.5） |
| **Memory Provider** | 树形记忆存储/召回 | AIstock PostgreSQL | 其他 DB / 嵌入式存储 |
| **Storage Provider** | 任务/审批/trace 持久化 | AIstock PostgreSQL | 可替换后端 |
| **Model Provider** | LLM 调用（已天然可移植） | litellm + deepseek/glm/qwen | 任意 BYOK provider |
| **Skill Provider** | 本地技能目录 | AIstock skill catalog | 其他技能源（人工审核导入） |
| **Channel Provider** | 对话/通知渠道 | AIstock 前端 10 页 | 其他渠道（如 IM/CLI，参考 OpenClaw） |
| **Knowledge Pack Provider** | 领域本体+图谱seed+证据规则+领域prompt | AIstock domain pack | 其他领域包；core 默认空载 |

### 17.5 通用 MCP 客户端 + 能力发现（用户决策：自动发现 + 人工审核分级）

- 对**任意 MCP server** 用 `list_tools` **自动发现**能力，动态生成能力目录（不再写死 AIstock 12 个）——这正是 MCP 协议的通用客户端用途。
- 新发现的 server/工具进入 **`quarantine`（隔离待审）** 状态：必须经**人工审核分级**（risk_level + auto_call_policy + summary-first 契约校验）后才 `approved` 可用。
- 与 §5.2 能力闸门联动：**未审核(quarantine)工具不得被 ReAct 循环调用**——既保通用接入，又防幻觉/越权。

DDL（触发 production_ddl_gate）：

```sql
CREATE TABLE IF NOT EXISTS assistant_mcp_connections (
    connection_id TEXT PRIMARY KEY,
    server_key    TEXT NOT NULL UNIQUE,    -- 任意 MCP 应用的 server key
    transport     TEXT NOT NULL,           -- stdio | http | sse
    endpoint      TEXT NOT NULL,
    domain_pack   TEXT,                    -- 关联的知识包(可空=通用)
    status        TEXT NOT NULL DEFAULT 'discovered',  -- discovered | quarantine | approved | disabled
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reviewed_by   TEXT,
    review_status TEXT NOT NULL DEFAULT 'pending'       -- pending | approved | rejected
);
COMMENT ON TABLE assistant_mcp_connections IS '通用 MCP 客户端连接登记；list_tools 自动发现，新工具经人工审核分级后方可调用';
```

发现的工具进入既有 `assistant_capabilities`，新增 `review_status` 列（`pending|approved|rejected`）；闸门检查 `review_status='approved'` 才放行。

### 17.6 数据隔离与不外泄（承接原设计第 5 条）

- AIstock 私有策略、生产边界、研究记忆只存在于 `aistock_domain_adapter` + `aistock_knowledge_pack` + AIstock 存储，**不进 core 默认逻辑**。
- core 设计为**多租户/多领域就绪**：知识包隔离、记忆 namespace 隔离；切换/卸载 AIstock pack 不应泄露其私有数据。

### 17.7 外部范式兑现（补 §9）

| 范式 | 借鉴点 | 落地 | 不采用 |
|---|---|---|---|
| MCP 协议（通用工具接口） | 任意应用经 MCP 接入同一客户端 | 17.5 通用客户端 + list_tools 发现 | 不写死特定应用 |
| OpenClaw（自托管助手：网关+模型路由BYOK+渠道+skills） | core/provider/channel 可插拔形态 | 17.4 provider 接口 | 不做 IM 网关产品形态（仅留 Channel Provider 接口） |
| 插件/适配器架构（core + domain adapter） | 核心稳定、领域可插拔 | 17.3 core/adapter 边界 | 现在不物理拆包 |

### 17.8 分阶段实施与验收（接 §10/§16，Phase 13–15；不启动 8001/3000）

#### Phase 13：定义 core/adapter 边界 + provider 接口收敛（DEF-13 之一）
- 交付：按 17.3 标注各模块归属；定义 17.4 provider 接口；把现有耦合点（MCP/记忆/存储/模型/渠道）改为经 provider 接口访问（**逻辑接缝，不物理拆包**）。
- 验收：
  - **静态依赖方向检查**：`assistant_product_core` 模块不得 import AIstock 领域符号（CI 依赖检查脚本，**断言无反向依赖**）。
  - provider 接口契约测试通过；现有功能行为不变（回归全绿）。

#### Phase 14：通用 MCP 客户端 + 自动发现 + 人工审核（DEF-13 之二）
- 交付：`assistant_mcp_connections` DDL + `capabilities.review_status`；list_tools 自动发现；quarantine→review→approved 流程；闸门联动。
- 验收：
  - `pytest test_generic_mcp_discovery.py`：接入一个**非 AIstock 的样例 MCP server**，工具被自动发现并进入 `quarantine`。
  - `pytest test_quarantine_tool_blocked.py`：**未审核工具不得被 ReAct 调用**（断言，接 §5.2 闸门）；审核 approved 后可用。

#### Phase 15：AIstock 知识包抽离 + 数据隔离（DEF-13 之三）
- 交付：把 AIstock 本体/图谱seed/证据规则/领域prompt 抽为可加载 `aistock_knowledge_pack`；core 默认空载。
- 验收：
  - `pytest test_core_empty_boot.py`：core **空载启动**不报错、能力为空。
  - `pytest test_pack_load_isolation.py`：加载 AIstock pack 后能力恢复；卸载后**私有记忆/策略不外泄**（隔离断言）。

### 17.9 防设计漂移门禁（补 §11）

| 门禁 | 规则 | 检查 |
|---|---|---|
| ANTI-DRIFT-11 | core 不反向依赖 adapter | 静态依赖方向检查（DEF-13） |
| ANTI-DRIFT-12 | 未审核 MCP 工具不得被调用 | quarantine 工具被闸门拒绝的断言 |
| ANTI-DRIFT-13 | 知识包隔离、私有数据不外泄 | core 空载 + pack 卸载隔离断言 |

### 17.10 可追溯性矩阵（补 §12）

| 设计项 | 缺陷 | 实现位置 | 测试 |
|---|---|---|---|
| core/adapter 边界 + provider 接口 | DEF-13 | core 包结构、provider 接口、依赖检查脚本 | `test_core_no_adapter_import.py` |
| 通用 MCP 客户端 + 发现/审核 | DEF-13 | `assistant_mcp_connections`、`capabilities.review_status`、闸门 | `test_generic_mcp_discovery.py` / `test_quarantine_tool_blocked.py` |
| 知识包抽离 + 数据隔离 | DEF-13 | `aistock_knowledge_pack`、Knowledge Pack Provider | `test_core_empty_boot.py` / `test_pack_load_isolation.py` |

> Phase 0 解耦锚点：`research_assistant.product_core`、`research_assistant.core_adapter`、`research_assistant.generic_mcp_client`、`research_assistant.aistock_domain_adapter`、`research_assistant.aistock_knowledge_pack` 已在 module/file ownership catalog 中登记；Phase 0 实现 commit `53a0f03d6a2bb05049a99f57998c3845b7d681f1`。

### 17.11 Design Acceptance Index（补 §13）

| 编号 | 用户要求 | 设计位置 | 验收标准 |
|---|---|---|---|
| DAI-PORT-001 | 可独立成产品、对接任意 MCP 应用 | §17 | core/adapter 边界 + 通用 MCP 客户端落地 |
| DAI-PORT-002 | 可移植接缝（现在不物理拆分） | §17.2/17.3 | provider 接口收敛 + 依赖方向检查通过 |
| DAI-PORT-003 | 任意 MCP 应用自动发现 + 人工审核 | §17.5 | 非 AIstock server 可发现；未审核工具被闸门拒绝 |
| DAI-PORT-004 | AIstock 私有数据不外泄 | §17.6 | 知识包隔离断言通过 |

### 17.12 边界

- 本章为**逻辑接缝**，不要求 Phase 0–12/§16 立即物理拆包重构；新代码按 provider 接口写，旧耦合点逐步收敛。
- DDL（§17.5）触发 `production_ddl_gate`，实现 PR 逐项报告。
- 不引入替换性外部 agent 框架；Channel/IM 产品形态仅保留接口、不在本期实现。
- 本 PR 仍为 docs-only：`production_ddl_gate=noop`、`production_backend_dependency_gate=noop`、`production_frontend_dependency_gate=noop`。
