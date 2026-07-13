# Workflow Skill Token Lean Design - 2026-07-03

## 背景

近期 BUG 和小型清理任务反复读取完整规范、quickstart、跨场景 skill 和长日志，导致单文件修改也可能出现数百条命令和高 token 消耗。目标是在不降低代码质量的前提下，将普通任务交互式 Codex/Claude token 降到旧流程的约 20%-30%，把深度验证集中到流水线和夜间任务。

## 设计原则

1. 只读一次总则：`docs/codex_project_memory.md` 和 `docs/standards/README.md` 只承担宪法和路由索引职责。
2. 单场景单 skill：每个任务只进入一个场景 skill/Claude command；该 skill 是本场景执行权威。
3. 不跨场景读取：BUG 不读 feature 设计细则；docs 不读 BUG 流程；merge 不读实现流程；只读分析不读修复流程。
4. 最小本地验证：Codex/Claude 只做 changed-file lint/compile、直接命中测试或 contract smoke、`git diff --check`、scope 和 production gates。
5. 深度验证委托：UI/API/business-flow、跨模块、LLM 设计漂移和长矩阵验证交给 Validation Center/CI/Nightly；DeepSeek 可选计划和诊断，确定性 allowlist runner 执行。
6. compact receipt：开发窗口默认只读取 PASS/FAIL/DEFERRED/BLOCKED 摘要、top failures、artifact refs 和 token usage。

## 场景路由

| 场景 | Codex skill | Claude command | 默认读取 |
| --- | --- | --- | --- |
| 不明确任务 | `aistock-task-router` | `aistock-task-router.md` | project memory + router |
| BUG / Issue | `fix-aistock-issue` | `fix-aistock-issue.md` | task-card/context pack/allowed scope |
| 新功能 | `verify-aistock-feature` | `aistock-feature-workflow.md` | Feature Card/design + acceptance ids |
| 文档/临时交互/cleanup | `aistock-docs-handoff` | `aistock-docs-handoff.md` | named files only |
| 合入/close-sync/DDL gate | `aistock-merge-aftercare` | `aistock-merge-aftercare.md` | PR + source worktree + gates |
| 只读诊断 | `aistock-readonly-triage` | `aistock-readonly-triage.md` | status/user paths |
| 复杂验证委托 | `aistock-validation-delegation` | `aistock-validation-delegation.md` | compact request/receipt |

## 验收标准

- `docs/standards/README.md` 不再要求 agent 启动时读取完整标准或 quickstart。
- `docs/codex_project_memory.md` 明确“一次总则 + 单 skill 执行”的上下文预算规则。
- BUG skill 和 Claude command 不再内嵌 docs-fast 细则，只路由到 docs skill。
- Feature skill 不再默认读取完整开发标准和旧测试体系文档，只读 feature lane 必要内容。
- 新增 validation delegation skill/command，并被 `install-client` manifest 和 CI change classifier 覆盖。
- `doctor` compact 输出能报告 validation delegation client entry 状态。
- 本次变更只跑轻量 workflow/client 相关验证，不跑业务模块大矩阵。

## 非目标

- 不改变业务代码。
- 不改变 production DB/DDL/服务进程。
- 不实现新的测试执行后端；本次只建立 skill/规范/manifest 接入和委托协议。
