# Research Assistant 升级——Codex 启动指令（防缩水 · 复制即用）

> 日期：2026-06-01
> 用途：每次让 Codex（或其它实现 agent）开始某个 Phase 的开发时，**把下面「复制即用启动指令」整段作为任务下发**，确保它按设计完整实现、并执行验证，不缩水、不跳步。
> 配套：蓝图 `docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md`；Runbook `docs/process/research_assistant_blueprint_execution_runbook_20260531.md`；执行样板 `docs/process/research_assistant_phase0_1_execution_pack_20260531.md`。

## 0. 怎么用

1. 选定要开发的 `Phase N`。
2. 复制 §1「启动指令」整段，把 `<PHASE_N>` 等占位替换为具体阶段。
3. 下发给 Codex。Codex 必须先读强制清单，再按执行环逐卡实现，最后用三闸门自检并报告。
4. 每个 Phase 一段指令；同模块多卡共享一个 worktree + `batch_id`。

## 1. 复制即用启动指令（模板）

```text
你是 AIstock 的实现 agent。本次任务：实现 Research Assistant 架构升级蓝图的 <PHASE_N>。
严格按既有设计与流程执行，禁止缩水、禁止简化、禁止占位。

【第一步：强制阅读（必须先读，不得跳过）】
1. docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md
   —— 重点：该 Phase 对应层的设计 + 该层「🔗 剥离考虑」+ §11 防漂移 + §12 可追溯 + §13 DAI。
2. docs/process/research_assistant_blueprint_execution_runbook_20260531.md
   —— 重点：§2 防缩水三闸门、§4 执行环、§3 验证计划接入时机、§5 A/B/C 模板。
3. docs/process/research_assistant_phase0_1_execution_pack_20260531.md
   —— 该 Phase 的任务卡(A)/验证计划草案(B)/closure_requirements(C)；若是 Phase 2+，按 Runbook §5 模板先生成本 Phase 的 A/B/C。
4. docs/standards/aistock_development_standard_v1.5_20260523.md（P0/P1 红线 + §15.3 DESIGN-COMPLIANCE-001）
5. docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md（worktree/batch/close-sync）

【第二步：环境与边界（不可违反）】
- 从 origin/main 开独立 worktree，分支 codex/<phase-slug>-<yyyymmdd>（Codex 用 codex/ 前缀）。一个 Phase 一个 batch_id。
- 验证只在 8011/8012（后端）+ 3011/3012（前端）。绝不启动/停止/重启 8001 / 3000。
- 不修改 Codex-owned 文件：codex_project_memory.md / AGENTS.md / AGENTS.override.md。
- fail-fast：无静默降级、无空 except: pass、无默认值掩盖错误。
- 临时产物放 F:\Dev\AIstock_artifacts 或 debug_tools/，不留根目录。

【第三步：核心解耦（每层都要遵守）】
- 区分 assistant_product_core（领域无关）vs aistock_domain_adapter/pack（AIstock 领域）。
- core 一律经 provider 接口工作，不得 import AIstock 的 8001 façade / DB schema / 业务 service / 领域符号。
- 必须保证 test_core_no_adapter_import.py（依赖方向检查）通过（ANTI-DRIFT-11）。

【第四步：逐卡实现（按本 Phase 任务卡 A）】
- 每张卡：按 target_files/signatures/ddl 实现，满足全部 definition_of_done。
- DDL：迁移在 8011/8012 验证库幂等执行两次无差异；production_ddl_gate 逐列报告 COMMENT；生产库不动。
- 测试先行：贴着真实签名补全测试代码，但断言意图不得弱于验证计划草案(B)的"消费断言"。

【第五步：接入并执行验证（每阶段都要验证）】
- 本 Phase 实现的第一步：把验证计划以 runner_enabled: true 登记进 tests/aistock_validation/catalog/test_plans.yaml（plan_key 用草案里的）。
- 用 Validation Center 跑：start_validation_execution(plan_key, expected_branch, expected_commit) → get_validation_execution_status，必须 exit_code=0（G1 绿 run_id）。
- 新模块登记 module_registry.yaml + file_ownership.yaml。

【第六步：防缩水三闸门自检（缺一不可，否则不得合入）】
- G1：本 Phase 验证计划绿灯 run_id（含消费断言：记忆→context pack、图谱→graph_relation_refs>0、工具结果→messages、core 无 AIstock import）。
- G2：本 Phase closure_requirements(C) 逐项 done=true，按 DESIGN-COMPLIANCE-001 item-by-item 核对；无 POC/简化/占位/read-only 冒充完整/mock-only。
- G3：回填蓝图 §12 可追溯矩阵对应行：实现文件 + commit 哈希。

【第七步：交付】
- 提交（结尾 Co-Authored-By），PR 到 main，PR 描述附：三闸门状态、run_id、closure_requirements 勾选表、§12 矩阵 diff、production_ddl_gate 报告。
- 通过 GitHub Actions / branch protection 后合入；按 issue-fix 标准 close-sync。

【停止条件（重要：不许缩水）】
- 任一闸门不满足、或发现设计无法按原样实现：必须停下并报告"哪条做不到、为什么、建议怎么办"，用 report_bug 登记，等待决策。
- 绝不通过弱化测试、删减 DoD、简化为 POC、或 mock 充真来"假装完成"。只有 G1+G2+G3 全过才可声明 <PHASE_N> 完成。

现在开始：先读强制清单，再回报你对 <PHASE_N> 的实现计划（任务卡清单 + 验证计划 + closure_requirements），等我确认后再动代码。
```

## 2. 各 Phase 的 phase-slug 速查

| Phase | slug | 层/内容 | 验证计划 plan_key |
|---|---|---|---|
| Phase 0 | `ra-baseline` | 基线锁定 + 脚手架 + 模块登记 | `ra_phase0_baseline` |
| Phase 1 | `ra-memory-tree` | L1 记忆树（DDL+召回+curator） | `ra_phase1_memory_tree` |
| Phase 2 | `ra-graph-context` | L1 知识图谱关系层接入 | `ra_phase2_graph_context` |
| Phase 3 | `ra-react-grounding` | L2 工具接地 ReAct 内核 | `ra_phase3_react_grounding` |
| Phase 4 | `ra-external-research` | L2.5 外部研究检索 | `ra_phase4_external_research` |
| Phase 5 | `ra-agent-teams` | L3 Agent Teams | `ra_phase5_agent_teams` |
| Phase 6 | `ra-qe-autonomy` | L4 QE 自主演进 | `ra_phase6_qe_autonomy` |
| Phase 7 | `ra-frontend-accept` | 前端 + 全量验收 | `ra_phase7_full_accept` |
| Phase 8 | `ra-code-intel` | L1.6 代码智能接入 | `ra_phase8_code_intel` |
| Phase 9 | `ra-proactive-report` | L6 主动晨报 | `ra_phase9_proactive_report` |
| Phase 10 | `ra-reflection-card` | L7 Reflection Card | `ra_phase10_reflection_card` |
| Phase 11 | `ra-prompt-lab` | L7 Prompt Lab（门禁化） | `ra_phase11_prompt_lab` |
| Phase 12 | `ra-skill-library` | L7 技能库 + 课程 | `ra_phase12_skill_library` |
| Phase 13 | `ra-core-adapter` | §17 core/adapter 边界 + provider | `ra_phase13_core_adapter` |
| Phase 14 | `ra-generic-mcp` | §17 通用 MCP 客户端 + 审核 | `ra_phase14_generic_mcp` |
| Phase 15 | `ra-knowledge-pack` | §17 知识包抽离 + 数据隔离 | `ra_phase15_knowledge_pack` |

> Phase 2–15 的任务卡(A)/验证计划(B)/closure_requirements(C) 由 Codex 按 Runbook §5 模板在该 Phase 启动时生成，经你确认后再实现。

## 3. 测试草案 vs 实现的时序（澄清）

- **现在锁死**：验证计划的 plan_key、命令、**断言意图（尤其消费断言）**——防止开发后把测试改弱。
- **实现时补全**：测试代码贴真实签名写出（测试先行），但意图不得弱于草案。
- **合入时验证**：G1 绿 run_id。G2 检查"补出来的测试仍满足草案断言意图"。
- **结论**：不是"开发后补测试"，而是"草案先锁意图 → 实现期测试先行补全 → 门禁防弱化"。纯开发后补 = 缩水口，禁止。
