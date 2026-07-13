# Research Assistant Phase 0 基线复验报告

- batch_id: `ra_phase0`
- branch: `codex/ra-baseline-20260601`
- baseline_source_commit: `9aa1811a087f83225bbd80892d4a83a9f11cb200`
- implementation_commit: `53a0f03d6a2bb05049a99f57998c3845b7d681f1`
- 验证计划: `ra_phase0_baseline`
- 生产影响: `production_ddl_gate=noop`; `production_backend_dependency_gate=noop`; `production_frontend_dependency_gate=noop`
- 运行边界: 未启动、停止、重启 `8001`/`3000`; Phase 0 只做静态脚手架、登记和基线复验。

## RA-P0-01 基线复验

### §1.1 已落地资产

| 资产 | 当前 HEAD 证据 | 结论 |
|---|---|---|
| 真实 LLM 调用 | `service.py:542` `ResearchAssistantLlmClient`; `service.py:569` `litellm.completion` | 保留，非 mock 兜底 |
| 多级模型路由 | `service.py:285` 默认 profile; `service.py:3467` `route_model` | 保留 |
| 模式切换状态机 | `models.py:77` `DIALOGUE_MODES` | 保留 |
| Prompt Tree | `init_research_assistant_schema_20260521.py:548` prompt node 表; `service.py:1295` `_select_prompt_nodes` | 保留，作为记忆树召回范本 |
| 审批闭环 | `backend/services/research_assistant/execution.py` | 保留 |
| MCP Gateway/profile | `backend/mcp/gateway.py`; `backend/mcp/profiles.py` | 保留 |
| token summary-first 契约 | `docs/architecture/research_assistant_unified_mcp_natural_language_orchestration_design_20260527.md` | 保留 |
| 前端 Research Assistant 页面 | `frontend/src/app/research-assistant/` | 保留 |

### §1.2 DEF-01~09

| 编号 | 当前 HEAD 文件:行号 | 复验结论 |
|---|---|---|
| DEF-01 | `service.py:1724` 首次 LLM 生成；`service.py:1763` 只读 MCP 事后旁挂 | 缺陷仍成立，工具结果未进入同轮二次 LLM messages |
| DEF-02 | `service.py:3093` `build_context_pack`; `service.py:3097` 按 memory_type 取数 | 缺陷仍成立，记忆召回与 query 无关 |
| DEF-03 | `service.py:3097` 附近无 importance/recency/relevance/reflection/embedding 打分链路 | 缺陷仍成立，检索层无树形召回与打分 |
| DEF-04 | `service.py:3137` `graph_relation_refs` 硬编码空 | 缺陷仍成立，图谱未进入推理上下文 |
| DEF-05 | `service.py:584` 单体服务 | 缺陷仍成立，无 orchestrator/worker Agent Teams |
| DEF-06 | `qe_evolution_service.py:149` scheduler; `qe_evolution_service.py:1616` 单 loop 提交; `qe_evolution_service.py:5810` 被动 custom/rerun 入口 | 缺陷仍成立，无跨 loop 自主闭环、停止条件、预算守护 |
| DEF-07 | `backend/services/research_assistant/` grep `arxiv/scholar/tavily/web_search/paper_search` = 0 | 缺陷仍成立，无外部搜索/学术检索接地实现 |
| DEF-08 | `init_research_assistant_schema_20260521.py:143` `research_memory_items` 无 `parent_key/tree_path`; `init_research_assistant_schema_20260521.py:548` 仅 prompt nodes 有树列 | 缺陷仍成立，记忆表不是真树 |
| DEF-09 | `models.py:61` `MEMORY_TYPES` | 缺陷仍成立，不含 `user_preference/directive/analysis_note` |

### §16.1 DEF-10~12 与澄清栏

| 编号 | 当前 HEAD 文件:行号 | 复验结论 |
|---|---|---|
| DEF-10 | `scripts/code_intelligence_adapter.py`; `backend/tests/scripts/test_code_intelligence_adapter.py`; `tests/aistock_validation/catalog/code_intelligence.yaml`; `backend/services/research_assistant/` grep `codegraph/impact` = 0 | 缺陷仍成立，CodeGraph 资产未进入助手 Context Pack |
| DEF-11 | `service.py:3598` 晨报标题; `service.py:3599` 占位正文 | 缺陷仍成立，主动晨报/实验日报未真实生成 |
| DEF-12 | `backend/services/research_assistant/` grep `prompt_lab/reflection_card/research_curriculum` = 0 | 缺陷仍成立，无自我学习和提示词自评闭环 |
| 澄清-A | `service.py:2382-2417` 写入 context segments/key facts；`service.py:2316-2320` 回灌 | 上下文压缩/key facts 已实现，不列为缺口 |
| 澄清-B | `service.py:3402`; `init_research_assistant_schema_20260521.py:490` | external agent session 已实现，不列为缺口 |
| 澄清-C | `service.py:569`; `service.py:1295` | LLM 真实调用和 Prompt Tree 选择已实现，不列为缺口 |

### §17 DEF-13 追溯登记

DEF-13 是后续 Phase 13~15 的解耦缺口。Phase 0 已将 `research_assistant.product_core`、`research_assistant.core_adapter`、`research_assistant.generic_mcp_client`、`research_assistant.aistock_domain_adapter`、`research_assistant.aistock_knowledge_pack` 登记进模块与文件归属目录；本报告不宣称 DEF-13 已修复。

## RA-P0-02 脚手架

| 项目 | 证据 | done |
|---|---|---|
| pytest 可收集包 | `backend/tests/research_assistant/__init__.py` | true |
| Phase 0 静态验收测试 | `backend/tests/research_assistant/test_phase0_blueprint_baseline.py` | true |
| 迁移命名空间 | `backend/db/migrations/ra_upgrade/README.md` | true |
| 不连生产库 | README 声明 Phase 0 不定义/执行 DDL，验证计划 `writes_database=false` | true |

## RA-P0-03 模块登记

| 目录 | 证据 | done |
|---|---|---|
| module_registry | `tests/aistock_validation/catalog/module_registry.yaml` 新增 18 个 `research_assistant.*` 子模块，`owner=claude_code_boundary` | true |
| file_ownership | `tests/aistock_validation/catalog/file_ownership.yaml` 新增 Phase 0 docs/tests/migration/runtime 和各未来模块路径归属 | true |
| Validation plan | `tests/aistock_validation/catalog/test_plans.yaml` 新增 `ra_phase0_baseline` 且 `runner_enabled=true` | true |
| Runner allowlist | `backend/services/validation/plan_catalog.py` 新增 `nox_ra_phase0_baseline` | true |
| nox session | `noxfile.py` 新增 `ra_phase0_baseline` | true |

## DESIGN-COMPLIANCE-001 closure requirements

| requirement | evidence | done |
|---|---|---|
| DEF-01~12 + §1.1 资产逐条复验，行号对齐当前 HEAD，附 commit | 本报告 §RA-P0-01，`baseline_source_commit` 已记录 | true |
| Phase 0 脚手架存在，不提前塞入未验证实现 | `backend/tests/research_assistant/__init__.py`; `backend/db/migrations/ra_upgrade/README.md`; 未新增业务实现 | true |
| 蓝图全部新模块登记到 module_registry + file_ownership，owner=claude_code_boundary | `module_registry.yaml`; `file_ownership.yaml`; `test_phase0_blueprint_baseline.py` | true |
| `ra_phase0_baseline` 接入 Validation Center runner | `test_plans.yaml`; `plan_catalog.py`; `noxfile.py` | true |
| 不触碰生产端口和生产库 | `requires_backend=false`; `requires_frontend=false`; `writes_database=false`; `production_ddl_gate=noop` | true |

## G1-central 回填

- G1-central run_id: `research-assistant_20260601_011521_l0_ra-phase0-baseline_fba1c3de_runner-validation__289612b1db`
- G1-central status: passed; `return_code=0`; canonical runner accepted `ra_phase0_baseline`.
- validated_commit: `fba1c3de`（run_id 内记录的受控验证提交）。
- implementation_commit_original: `53a0f03d6a2bb05049a99f57998c3845b7d681f1`。
- implementation_commit_rebased_before_merge: `cff0b243`（rebase 到 `origin/main` 后的 Phase 0 HEAD，回填前）。
- production_ddl_gate: `noop`; production runtime touched: false.
