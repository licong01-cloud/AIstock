# QE 月度数据集更新产品化 F2 实现验收回执（2026-08-12）

关联设计：`docs/architecture/qe_monthly_dataset_release_productization_f2_design_20260811.md`

## Evidence boundary

- 源码、隔离 fixture、Windows/WSL 临时平台 smoke 已验证。
- 用户明确要求本轮不得导出真实数据、读取或修改既有候选、访问真实 DB/TDX/Tushare、激活生产或控制现有服务。
- 因此真实 2026-07-31 source scan/candidate/signoff 与真实 full-scale 性能为明确批准的后续运行时证据，不属于本源码 PR 的伪造通过项。
- production activation、node1 distribution、DDL/DML、runtime restart、client install 与 cleanup 均为 `not_requested/not_authorized`。

## Validation receipts

### DR-F2-20260812-01 — unified isolated regression

```powershell
python -m pytest -q backend/tests/dataset_release backend/tests/routers/test_dataset_releases.py backend/tests/scripts/test_update_backtest_dataset_monthly.py backend/tests/scripts/test_export_qe_qlib_candidate.py backend/tests/scripts/test_dataset_release_source_stage.py backend/tests/scripts/test_dataset_release_source_recheck.py backend/tests/scripts/test_dataset_release_build_stage.py backend/tests/test_deps_dataset_release.py
```

结果：`743 passed, 5 skipped, 0 failed`。5 个 skip 均为条件/平台分支；Windows junction 与 Windows+WSL 实际 smoke 另有通过证据。168 个 warning 仅来自测试 fixture 逐列构造 DataFrame。

该全量隔离回归绑定本任务代码与当时最新主线合并后的本地 HEAD `9a62788bf88440e3431fed208f2405d6891f94f0`。随后热合并的主线仅包含任务范围外变更；最终源码合入不得仅复用该历史 HEAD receipt，必须同时满足：当前分支 clean、F2 validator/最小门禁在最终本地 HEAD 通过、PR `headRefOid` 等于最终推送 HEAD、required checks 的 `head_sha` 与该 HEAD 一致且全绿。最终 merge SHA 由 GitHub merge readback 单独记录。

### DR-F2-20260812-02 — isolated target-platform smoke

```powershell
python tests/aistock_validation/dataset_release_platform_smoke.py
```

结果（rebase 当时最新 `origin/main` 后复跑）：Windows PASS；WSL PASS；Windows job peak commit `23,699,456` bytes；WSL cgroup peak `28,565,504` bytes；swap peak `0`；最终 active processes `0`。安全计数 database/provider/export/production/service-controls 全为 `0`。

### DR-F2-20260812-03 — static and skill gates

```powershell
python -m ruff format --check backend/services/dataset_release backend/routers/dataset_releases.py scripts/dataset_release_*.py scripts/update_backtest_dataset_monthly.py tests/aistock_validation/dataset_release_platform_smoke.py
python -m ruff check <all changed Python files>
python -m compileall -q backend/services/dataset_release backend/routers/dataset_releases.py scripts tests/aistock_validation/dataset_release_platform_smoke.py
git diff --check
python -X utf8 <skill-creator>/scripts/quick_validate.py .codex/skills/update-backtest-dataset
```

结果：79 个新月更包/入口 Python 文件通过 format-check；154 个变更 Python 文件 Ruff clean；compileall、diff-check 与 Skill validation 全部通过。对既有 `backend/main.py`、legacy exporter/field-map 只保留最小功能 diff，没有为通过格式门禁扩大无关改动。

### DR-F2-20260812-04 — CI ownership and L0 guardrails

```powershell
python scripts/aistock_module_ownership_scan.py <PR files> --fail-on-unmapped --fail-on-ambiguous
python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py <PR files> --fail-on HIGH
python scripts/aistock_guardrail_scan.py <PR files> --baseline-json tests/aistock_validation/guardrails_baseline_20260511.json --fail-new-only --fail-on-severity P1
python -m pytest -q backend/tests/test_validation_module_ownership.py backend/tests/test_validation_catalog_integrity.py
```

结果：`168/168 mapped`、`unmapped=0`、`ambiguous=0`；quality guardrail `0 finding`；historical guardrail `blocking=0`；catalog `15 passed`。月度发布包、API/CLI、测试、Skill 和验收文档统一归属 `qlib_data`，没有绕过 classifier 或增加 guardrail baseline 豁免。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/dataset_release/contracts.py; control_store.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_contracts.py | verified | - |
| F-002 | backend/services/dataset_release/decision.py; mixed_planner.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_incremental_planner.py | verified | - |
| F-003 | backend/services/dataset_release/attestation.py; signoff.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_reattest_existing.py | verified | - |
| F-004 | backend/services/dataset_release/fingerprints.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_fingerprints.py | verified | - |
| F-005 | backend/services/dataset_release/source_manifest.py; source_authority.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_source_manifest.py | verified | - |
| F-006 | backend/services/dataset_release/build_stage.py; daily_minute_materializer.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_build_stage.py | verified | - |
| F-007 | backend/services/dataset_release/dependency_graph.py; mixed_planner.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_dependency_graph.py | verified | - |
| F-008 | backend/services/dataset_release/copy_on_write.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_copy_on_write.py | verified | - |
| F-009 | backend/services/dataset_release/control_store.py; lease.py; cas_store.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_control_store.py | verified | - |
| F-010 | backend/services/dataset_release/windows_job.py; wsl_cgroup.py | validation-receipt: DR-F2-20260812-02; tests/aistock_validation/dataset_release_platform_smoke.py | verified | - |
| F-011 | backend/services/dataset_release/resource_budget.py; worker.py | validation-receipt: DR-F2-20260812-02; backend/tests/dataset_release/test_resource_budget.py | verified | - |
| F-012 | backend/services/dataset_release/control_service.py; backend/routers/dataset_releases.py | validation-receipt: DR-F2-20260812-01; backend/tests/routers/test_dataset_releases.py | verified | - |
| F-013 | backend/services/dataset_release/worker.py; scripts/dataset_release_worker.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_worker_cli.py | verified | - |
| F-014 | backend/services/dataset_release/state_machine.py; publisher.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_state_machine.py | verified | - |
| F-015 | backend/services/dataset_release/worker_commands.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_worker.py | verified | - |
| F-016 | backend/deps.py; backend/services/dataset_release/api_models.py | validation-receipt: DR-F2-20260812-01; backend/tests/test_deps_dataset_release.py | verified | - |
| F-017 | backend/services/dataset_release/resolution_processor.py; profile.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_profile.py | verified | - |
| F-018 | backend/services/dataset_release/minute_overlay.py; artifact_ready_source.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_minute_overlay.py | verified | - |
| F-019 | backend/services/dataset_release/worker.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_worker.py | verified | - |
| F-020 | backend/services/dataset_release/build_processor.py; signoff.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_build_processor.py | verified | - |
| F-021 | backend/services/dataset_release/component_manifest_producer.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_component_manifest_producer.py | verified | - |
| F-022 | scripts/update_backtest_dataset_monthly.py; .codex/skills/update-backtest-dataset/SKILL.md | validation-receipt: DR-F2-20260812-03; backend/tests/scripts/test_update_backtest_dataset_monthly.py | verified | - |
| F-023 | backend/services/dataset_release/log_store.py; backend/routers/dataset_releases.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_log_store.py | verified | - |
| F-024 | backend/services/dataset_release/reconciler.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_reconciler.py | verified | - |
| F-025 | backend/services/dataset_release/control_service.py; scripts/export_qe_qlib_candidate.py | validation-receipt: DR-F2-20260812-01; backend/tests/scripts/test_export_qe_qlib_candidate.py | verified | - |
| F-026 | tests/aistock_validation/dataset_release_platform_smoke.py | validation-receipt: DR-F2-20260812-01; tests/aistock_validation/dataset_release_platform_smoke.py | approved_by_user | 用户明确批准: 本轮禁止真实数据、既有候选和生产访问，源码仅使用 temp/fixture/platform smoke 验收 |
| F-027 | .codex/skills/update-backtest-dataset/SKILL.md; docs/operations/qe_backtest_dataset_monthly_update_runbook.md | validation-receipt: DR-F2-20260812-03; backend/tests/scripts/test_update_backtest_dataset_monthly.py | verified | - |
| F-028 | backend/services/dataset_release/index_contract.py; candidate_validator.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_candidate_validator.py | verified | - |
| F-029 | backend/services/dataset_release/performance.py; synthetic_benchmark.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_performance.py | approved_by_user | 用户明确批准: 本轮禁止真实导出，真实 full-scale 性能延后到获授权月更；synthetic gate 已验证 |
| F-030 | docs/architecture/qe_monthly_dataset_release_productization_f2_design_20260811.md | validation-receipt: DR-F2-20260812-01..04; final-head F2/minimal gate; PR #3310 head/check/merge readback | verified_source_review | - |
| F-031 | backend/services/dataset_release/component_artifact_manifest.py; canonical_lineage.py | validation-receipt: DR-F2-20260812-01; backend/tests/dataset_release/test_canonical_lineage.py | verified | - |

## DESIGN-COMPLIANCE-001 readback

1. required control/API/worker/source/build/validator/publisher/skill/CLI 路径均为可执行实现，没有用 subset、placeholder 或 silent fallback 替代。
2. no-op、reuse、re-attest、waiting、blocked、failed 与 source revision/resource/identity 错误均为 typed durable outcome；无伪成功。
3. PIT、QFQ、moneyflow share/CNY、12-index/HMM benchmark、TDX-first/Tushare missing-only 与 candidate-only 业务边界保持不变。
4. 没有新增人工审批；真实数据、生产、DB、runtime、client 与 cleanup 仍按动作/目标单独授权。
