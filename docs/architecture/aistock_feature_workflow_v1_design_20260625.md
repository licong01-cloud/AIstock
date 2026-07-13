# AIstock Feature Workflow v1 设计方案

## Background

近期新功能和业务模块开发中出现过设计与实现偏移、简化交付、静默降级和合入前验收不够聚焦的问题。现有 BUG workflow 已覆盖问题修复，但新功能开发缺少一个轻量、可执行、不会明显增加 token 和耗时的设计驱动流程。

## Scope

- F-001: 定义 F0/F1/F2 三档新功能开发流程，区分轻量 Feature Card、标准设计文档和完整架构设计。
- F-002: 要求新功能使用稳定的 `Design Acceptance Index`，合入前用设计验收矩阵逐项核对实现和证据。
- F-003: 禁止未获用户批准的简化版、POC、mock-only、静态成功、静默兜底或“文档对齐现实”交付。
- F-004: 提供紧凑 CLI 检查，帮助 Codex 和 Claude Code 在 PR 前发现缺失章节、未批准缺口和开发漂移。
- F-005: 保持效率：不引入复杂 feature registry，不强制全量扫描，不输出长 JSON，默认只跑目标测试和必要 nox gate。

## Non-Goals

- 不替代 BUG/Issue workflow；BUG 修复继续使用 `scripts/aistock_issue_workflow.py`。
- 不新增生产服务、调度器、DB 表、DDL 或持久化 feature registry。
- 不要求所有文档改动走完整代码验证；文档类仍按文档三档流程处理。

## Architecture

Feature Workflow v1 由三部分组成：

1. 标准条款：在 `docs/standards/aistock_development_standard_v1.5_20260523.md` 中新增 `FEATURE-WORKFLOW-001`。
2. 机器可读控制：在同名 YAML 中新增 manual-review 规则和 control，供工具和人工复核引用。
3. CLI 辅助：`scripts/aistock_feature_workflow.py` 读取设计/验收 markdown，按 F0/F1/F2 校验必需章节、`Design Acceptance Index`、验收矩阵和禁止性措辞。

## Contracts

- 输入：markdown 设计文档或 Feature Card；可选独立 acceptance markdown。
- 分级：`--tier F0|F1|F2`。
- 必需验收表字段：`design_item`、`implementation_refs`、`test_or_evidence`、`status`、`gap_or_exception`。
- 输出：默认 compact summary；仅在 `--format json` 时输出结构化 payload。
- 失败语义：未批准缺口、缺少必需章节、缺少矩阵、设计项未覆盖、简化/POC/mock-only 作为完成交付，均返回非零退出码。

## Design Acceptance Index

- F-001: 标准文档新增新功能开发规则，明确 F0/F1/F2、设计路径、验收索引、矩阵和合入规则。
- F-002: YAML 新增 `FEATURE-WORKFLOW-001` rule/control，与 markdown 标准同步。
- F-003: CLI 支持校验 F0/F1/F2 设计文档和验收矩阵，输出紧凑摘要。
- F-004: CLI 能拒绝未批准缺口、缺失矩阵、缺失章节和简化交付措辞。
- F-005: 测试覆盖有效 F0/F2、缺失章节、未批准缺口、批准偏差、简化交付措辞和紧凑输出。
- F-006: nox/ownership 纳入新脚本和测试，确保后续流水线可识别该工具。
- F-007: 本变更无 DDL、无依赖变更、无服务重启。
- F-008: Codex 和 Claude Code 的项目级入口都能发现并执行 Feature Workflow v1。

## Implementation Plan

- 新增 `scripts/aistock_feature_workflow.py`，实现纯本地 markdown 校验。
- 新增 `backend/tests/scripts/test_aistock_feature_workflow.py`，覆盖核心规则和输出。
- 更新标准 markdown/YAML、`noxfile.py` 和 file ownership。
- 使用 targeted tests、`validation_module_registry_l0`、`l0` 和 `git diff --check` 做合入前验证。

## Verification Plan

- `python -m ruff check scripts/aistock_feature_workflow.py backend/tests/scripts/test_aistock_feature_workflow.py`
- `python -m pytest backend/tests/scripts/test_aistock_feature_workflow.py -q -p no:cacheprovider`
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/aistock_feature_workflow_v1_design_20260625.md --tier F1`
- `python -m nox -s validation_module_registry_l0`
- `python -m nox -s l0`
- `git diff --check`

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | docs/standards/aistock_development_standard_v1.5_20260523.md | standard diff review | verified | - |
| F-002 | docs/standards/aistock_development_standard_v1.5_20260523.yaml | validation_module_registry_l0 | verified | - |
| F-003 | scripts/aistock_feature_workflow.py | pytest + CLI self-validate | verified | - |
| F-004 | scripts/aistock_feature_workflow.py | pytest gap/simplified/missing-section cases | verified | - |
| F-005 | backend/tests/scripts/test_aistock_feature_workflow.py | pytest targeted suite | verified | - |
| F-006 | noxfile.py; tests/aistock_validation/catalog/file_ownership.yaml | nox validation_module_registry_l0; nox l0 | verified | - |
| F-007 | docs/architecture/aistock_feature_workflow_v1_design_20260625.md | production gates report | verified | - |
| F-008 | AGENTS.md; AGENTS.override.md; docs/codex_project_memory.md; .codex/skills/verify-aistock-feature/SKILL.md; .claude/commands/aistock-feature-workflow.md | client entrypoint review + skill validation | verified | - |

## Rollout / Rollback

- Rollout：通过 PR 合入 main 后，所有新窗口读取项目记忆和标准时即可按 `FEATURE-WORKFLOW-001` 执行；无需重启后端、前端或 VC。
- Rollback：如规则过严，可回滚该 PR 或调整 CLI 必需章节/措辞规则；无 DB 或 runtime 状态需要回滚。

## Risks

- 规则过严可能增加小功能成本：用 F0 Feature Card 和 compact summary 降低成本。
- 规则过松可能漏掉设计偏移：F1/F2 必须有验收矩阵和用户批准偏差留痕。
- 中英文标题差异可能导致误判：CLI 使用中英文 alias，并允许通过测试继续扩展 alias。

## Production Gates

- production_ddl_gate=noop。
- production_frontend_dependency_gate=noop。
- production_backend_dependency_gate=noop。
- 未重启任何生产服务，未写生产 DB。
