# AIstock Validation LLM Guarded Rollout Runbook

版本：v1.0
日期：2026-06-09
适用范围：GitHub Models / DeepSeek 作为 CI、Nightly、Validation Center issue intake 的智能增强层。

## 1. 边界

- LLM 只提供 triage、issue 草案、测试计划建议、夜间调度建议和结果解释。
- LLM 不修代码、不合入 PR、不关闭 GitHub Issue、不写 BUG JSON、不执行 shell、不触碰生产端口或生产库。
- Deterministic CI/Nightly/Validation Center 结果仍是最终事实源。
- GitHub Issue 自动创建仍由现有 deterministic issue payload 控制；LLM guarded rollout 只决定 LLM 建议是否可参与 auto-file 增强。

## 2. 模式

配置文件：`configs/validation/llm_triage.yaml`。

- `off`：全局 kill switch；LLM advice 不参与 issue 增强，系统回退 deterministic workflow。
- `warning_only`：默认模式；生成 gate/evidence，但不允许 LLM 触发 auto-file 增强。
- `opt_in_auto_file`：仅在显式 opt-in 且模块 allowlist、issue sections、deterministic policy、evaluation threshold 全部通过时，允许 LLM 增强 auto-file 内容。

环境变量：

- `AISTOCK_LLM_TRIAGE_MODE=off|warning_only|opt_in_auto_file`
- `AISTOCK_LLM_AUTO_FILE=true|false`

## 3. Allowlist

默认 allowlist 仅覆盖 validation/workflow、QE、Paper v2 selected modules、Research Assistant。
新增模块必须通过 PR 修改 `guarded_rollout.module_allowlist`，并补充测试。

## 4. Issue 质量要求

自动 issue body 必须包含：

- Failure Summary
- Regression Locator
- Agent Handoff
- Token Policy
- Production Gates

缺少任何 section 时，guarded rollout gate 不允许 LLM auto-file 增强。

## 5. 验证命令

```powershell
python scripts/llm_provider_adapter.py --json guarded-rollout-gate --provider github_models --mode warning_only --module validation.runner --issue-section "Failure Summary,Regression Locator,Agent Handoff,Token Policy,Production Gates"
python scripts/llm_provider_adapter.py --json guarded-rollout-gate --provider github_models --mode opt_in_auto_file --opt-in --module validation.runner --issue-section "Failure Summary,Regression Locator,Agent Handoff,Token Policy,Production Gates"
python -m pytest backend\tests\scripts\test_llm_provider_adapter.py backend\tests\scripts\test_ci_failure_issue_summary.py -q
```

## 6. 回滚

1. 设置 `AISTOCK_LLM_TRIAGE_MODE=off`。
2. 不需要停用 GitHub Actions、Validation Center、nox 或 BUG workflow。
3. 确认 issue payload 中 `llm_guarded_rollout_gate.fallback=deterministic_issue_workflow`。
4. 如需长期回滚，清空 `guarded_rollout.module_allowlist` 或回退相关 PR。

## 7. Production Gates

本能力不需要 DDL，不需要前端依赖，不需要后端依赖：

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
