# AIstock Issue Workflow Compatibility Pointer

> 状态：兼容入口，不是开发规范
> 唯一权威：`docs/standards/aistock_development_standard_v1.5_20260523.md`

本文保留旧链接兼容性，不再维护独立的风险分级、worktree、测试、合入或 Agent 规则。

Issue/BUG 执行方式：

1. 从 Codex `aistock-task-router` 或 Claude Code `aistock-task-router` 进入 `fix-aistock-issue` lane。
2. 使用 `python scripts/aistock_issue_workflow.py doctor` 检查入口状态。
3. 使用 `submit-bug`、`run --mode plan`、`resume`、`finish --plan-only`、PR 和 aftercare 命令完成生命周期。
4. 任务分级、scope、测试路由、批处理、生产 gate 和完成证据统一执行权威开发规范。

操作示例位于 `docs/standards/aistock_issue_workflow_quickstart.md`；工具实现位于 `scripts/aistock_issue_workflow.py`。
