# Research Assistant 蓝图执行 Runbook（防缩水 · Codex 严格执行指南）

> 日期：2026-05-31
> 配套设计：`docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md`（下称「蓝图」）
> 适用执行体：Codex / Claude Code 等实现 agent
> 运行边界：不启动/停止/重启 `8001` / `3000`；验证只在 `8011/8012`（后端）+ `3011/3012`（前端）。
> 红线：本文不修改 Codex 全局规范，不触碰 Codex-owned 文件（`codex_project_memory.md` / `AGENTS.md` / `AGENTS.override.md`）。

## 0. 本文定位

蓝图回答"做什么/为什么"；本 Runbook 回答"**怎么严格做完、怎么机械证明、怎么防止缩水**"。它**不新建流程**，而是把蓝图的 DAI/可追溯矩阵/防漂移门禁，**接进 AIstock 已有的执行与验证流水线**：

- `docs/standards/aistock_development_standard_v1.5_20260523.md`（P0/P1 红线、§15.3 `DESIGN-COMPLIANCE-001`）
- `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md`（issue 生命周期、scope、batch 执行、`batch_id`）
- Validation Center：`tests/aistock_validation/catalog/test_plans.yaml`（runner-enabled 验证计划）、`tests/aistock_validation/bugs/*.json`（机器可读记录）、`module_registry.yaml`、`file_ownership.yaml`
- Validation Center MCP 工具：`list_plans`、`start_validation_execution`、`get_validation_execution_status`、`report_bug`、`update_bug_status`、`get_module_quality_summary`

## 1. 不改全局规范（重要）

- 本 Runbook 是 `docs/` 下的**项目执行文档**，只**引用**上述标准，不替代、不分叉。
- 若将来要把"阶段闭环三闸门"升级为**强制合入门**（CI 阻断），属可选硬化项，由用户/Codex 在全局层决定；本文不擅自改全局规范。

## 2. 防缩水三闸门（核心机制）

> "设计完善但开发缩水"的根因：设计是散文，没有机械闸门。下面把蓝图 §11/§12/§13 转成**一个 Phase 不满足三条就不能 close** 的硬门。

| 闸门 | 要求 | 工件 | 检查方式 |
|---|---|---|---|
| **G1 验证绿灯** | 该 Phase 的验证计划在 Validation Center 跑出**绿色 `run_id`**（含"消费断言"测试：记忆→context pack、图谱→`graph_relation_refs>0`、工具结果→messages、core 无 AIstock import） | `test_plans.yaml` 计划 + run 记录 | `start_validation_execution(plan_key)` → `get_validation_execution_status` exit=0 |
| **G2 逐项符合** | 该 Phase 的 `closure_requirements` **逐项打勾**，无简化/占位/POC/read-only 冒充完整 | 本阶段 closure_requirements（见 §5 模板 C） | `DESIGN-COMPLIANCE-001` 逐项人工/自审 |
| **G3 可追溯回填** | 蓝图 §12 可追溯矩阵对应行回填**实现文件 + commit 哈希** | 蓝图 §12 表 | PR 描述附矩阵行 diff |

**任一闸门红/缺 → 不得合入 main，不得宣称该 Phase 完成。** 缩水会卡在 G1 红灯、G2 未打勾、G3 空行上。

## 3. 验证计划接入时机（回答"草案何时进 test_plans.yaml"）

1. **设计/辅助文档阶段（现在）**：验证计划只在辅助文档里写**草案**（`plan_key` + 命令 + 断言），**不进** `test_plans.yaml`，避免空/占位计划污染流水线。
2. **某 Phase 开始实现**：该 Phase 实现 PR 的**第一步**，把对应计划以 `runner_enabled: true` 登记进 `test_plans.yaml`（此刻已有真实测试文件支撑）。
3. **该 Phase 合入前**：必须跑出绿灯 `run_id`（G1）。

即：**草案先锁验收口径（防被偷偷弱化）→ 实现时接入 → 合入时验证**。

## 4. 每阶段执行环（Codex 按此循环，不许跳步）

```
1. 从 origin/main 开独立 worktree，分支 claude/<phase-slug>-<yyyymmdd>（同模块多卡共享 worktree + batch_id）
2. 取该 Phase 的「任务卡」(模板 A)：逐卡实现，每卡有精确文件/签名/DDL/DoD/验收命令/消费断言
3. 实现 PR 第一步：把该 Phase 验证计划登记进 test_plans.yaml(runner_enabled)
4. 跑验证：start_validation_execution(plan_key) → 取 run_id；红则修，直到绿(G1)
5. closure_requirements 逐项打勾(G2，DESIGN-COMPLIANCE-001)；禁 POC/简化/占位
6. 回填蓝图 §12 可追溯矩阵：实现文件 + commit(G3)
7. DDL 阶段：迁移在 8011/8012 验证库幂等执行，production_ddl_gate 逐项报告，生产库不动
8. PR → GitHub Actions / branch protection → 合入 → close-sync（沿用 issue-fix 标准）
```

- 一个 Phase = 一个 `batch_id`；同模块卡共享 worktree/上下文/验证。
- 验证端口 8011/8012 + 3011/3012；**绝不碰 8001/3000**。

## 5. 每阶段交付物模板（供 Phase 2–15 自生成）

> Phase 0–1 的填好实例见 `research_assistant_phase0_1_execution_pack_20260531.md`。其余 Phase 按本模板自生成。

### 模板 A：任务卡（Implementation Task Card）

```yaml
card_id: RA-P<phase>-<seq>
phase: <Phase N>
title: <一句话目标>
target_files:                 # 精确到文件；新增/修改
  - path: backend/services/research_assistant/<file>.py
    change: create|modify
signatures:                   # 关键函数/类签名
  - "def <fn>(...) -> <ret>: ..."
ddl: |                        # 如涉及，贴 DDL（带 COMMENT）
acceptance_command: "pytest <test_file> -p no:cacheprovider"
consumption_assertion: "<必须存在的'被消费'断言，对应蓝图 ANTI-DRIFT-02/11>"
definition_of_done:           # 逐条，缺一不可
  - "<DoD-1>"
forbidden: "无 POC/简化/占位/默认值掩盖错误；core 不 import AIstock 领域符号"
traceability_row: "<蓝图 §12 对应行>"
```

### 模板 B：验证计划草案（→ 实现时进 test_plans.yaml）

```yaml
plan_key: ra_phase<phase>_<slug>
title: <Phase N 验收>
runner_enabled: true          # 登记进 test_plans.yaml 时为 true
commands:
  - "pytest <test1> <test2> -p no:cacheprovider"
assertions:                   # 机器可判定
  - "<断言1，含消费断言/无占位符快照>"
gates: [G1]
ports: "8011/8012；不碰 8001"
```

### 模板 C：closure_requirements（DESIGN-COMPLIANCE-001 逐项）

```yaml
phase: <Phase N>
design_ref: "蓝图 §<x>"
items:                        # 每条对应一项交付物，合入前逐项打勾
  - id: CR-P<phase>-01
    requirement: "<必须交付的具体内容（非'大致实现'）>"
    done: false
    evidence: "<run_id / 文件:行 / 测试名>"
anti_shrink_checks:
  - "无 read-only 冒充完整"
  - "无 mock-only 冒充真实链路"
  - "每个新增表/API 有'被消费'断言（不得建了不用）"
sign_off: "G1 绿 run_id + 本表全 done=true + §12 矩阵回填，方可合入"
```

## 6. 禁止形态（同蓝图 §2.2 / §12A，合入即检）

最小实现 / 简化版 / 子集版 / 占位版 / mock-only / 只登记目录不实现 / 只做 read-only 却称完整 / 只做后端不接路由 / 跳过验证或只 smoke / 建了表/API 不接推理链路。

## 7. Validation Center MCP 调用速查

| 步骤 | 工具 | 说明 |
|---|---|---|
| 看有哪些计划 | `list_plans` | 确认该 Phase plan_key 已登记 |
| 跑验证 | `start_validation_execution(plan_key, expected_branch, expected_commit)` | 仅 runner_enabled 计划可跑 |
| 看结果 | `get_validation_execution_status(execution_id)` | exit_code=0 + artifacts 才算 G1 绿 |
| 记录缺陷 | `report_bug(...)` / `update_bug_status(...)` | 缩水/未达 DoD 即登记，close-sync |
| 模块质量 | `get_module_quality_summary(module)` | 新模块须登记 module_registry |

## 8. 与蓝图阶段对应

蓝图 Phase 0–15（§10 + §16.7 + §17.8）每一个，都对应一份「执行 Pack」= 任务卡(A) + 验证计划(B) + closure_requirements(C)，并受 §2 三闸门约束。Phase 0–1 已出样板；其余按 §5 模板生成，逐阶段推进、逐阶段验证、逐阶段防缩水。
