# QE Results-Only Loop Retry Feature Card (F1)

## Background

QE custom_evo 的 loop retry 目前只有 `auto` / `backtest_only` / `full_train` 三档。对 post-compute 类失败，模型训练和分钟回测已经完成，`pred.pkl` 与 portfolio analysis 报告也已经落盘，但注册、上传或读取结果阶段失败；继续使用 `backtest_only` 会重跑整段分钟回测，恢复成本高且没有新增计算价值。

本功能由 GitHub Issue #1914 跟踪：<https://github.com/licong01-cloud/AIstock/issues/1914>。按 FEATURE-WORKFLOW-001 判定为 F1：单模块 QE retry 能力增强，不改变生产默认 auto 路由，不引入 DDL。

## Scope

- 在 QE loop retry mode 中新增显式 `results_only`，包含 alias 归一化。
- 仅当调用方显式传入 `retry_mode=results_only` 时生效；`auto` 继续沿用原有 backtest/full_train 判定。
- 为 results-only 路径增加前置 artifact gate：验证存量 `pred.pkl`、IC/RankIC 指标或廉价重算条件、必要 portfolio analysis 报告。
- gate 通过后只执行 prediction-store 上传与 QE warehouse 注册/upsert，不构造、不提交、不执行 qrun/training/backtest。
- gate 或注册失败必须 loud 上报，包含 `reason_code`、artifact、task、loop、node 与可操作恢复建议。

## Non-Goals

- 不修改 BUG-605 已合并的 qlib/MLflow 空指标读重试逻辑。
- 不改变 `auto` retry 的默认路由，不加入智能 auto results-only 判定。
- 不新增前端 UI 设计；本 PR 先交付后端/API 可显式调用能力。
- 不修改训练、回测、因子、模型、策略执行逻辑。
- 不引入生产 DDL、服务重启或生产数据写入操作。

## Design Acceptance Index

- F-001: retry mode normalization includes explicit `results_only` and safe aliases; `auto` behavior remains unchanged.
- F-002: `results_only` path never constructs or submits `BacktestExecutor`, qrun, training, or backtest commands.
- F-003: artifact gate validates `pred.pkl` exists, is loadable, non-empty, has datetime x instrument coverage, and prediction scores are not all NaN/non-finite.
- F-004: artifact gate accepts existing non-empty IC/RankIC metrics or recomputes IC/RankIC locally from `pred.pkl` + `label.pkl` without portfolio backtest.
- F-005: artifact gate validates required portfolio analysis report artifacts before any registration/upload.
- F-006: gate failure, invalid artifacts, upload failure, and DB registration failure are loud with `reason_code` and zero partial registration.
- F-007: registration/upload is idempotent: QE experiment rows use upsert and prediction-store writes replace/update the same run key without duplicate warehouse rows.
- F-008: targeted tests cover success, missing/invalid `pred.pkl`, all-NaN predictions, idempotency, alias normalization, and no-qrun behavior.

## Implementation Plan

1. Add `QE_LOOP_RETRY_MODE_RESULTS_ONLY` and extend retry mode alias normalization.
2. Add results-only artifact gate helper in the QE retry service boundary:
   - read loop recorder binding (`qe_current_recorder.json` / `qe_extracted_recorder.json`);
   - download `mlruns/<experiment>/<recorder>/artifacts/pred.pkl`;
   - validate prediction shape/coverage/non-NaN;
   - read metrics from existing results files/API or recompute IC/RankIC from `label.pkl`;
   - verify required portfolio report `portfolio_analysis/report_normal_1day.pkl`.
3. Add results-only branch in `AutoEvolutionScheduler.retry_loop` before queueing or executor construction.
4. Gate-passing branch writes prediction-store artifacts and QE DB registration using idempotent upserts, then recomputes custom_evo task status.
5. Add focused unit tests using fake workspace client and fake DB cursor; assert no qrun/backtest submit is reachable.

## Verification Plan

- Run feature workflow validation:
  - `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_results_only_retry_feature_card_20260708.md --tier F1`
- Run focused unit tests:
  - `python -m pytest backend/tests/unified_engine/test_qe_results_only_retry.py backend/tests/unified_engine/test_qe_config_truth.py::test_qe_loop_retry_mode_normalization -q -p no:cacheprovider`
- Run changed-file syntax gate:
  - `python -m py_compile backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/results_only_retry.py backend/tests/unified_engine/test_qe_results_only_retry.py`
- Run diff hygiene:
  - `git diff --check`

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/quantevolver/qe_evolution_service.py` retry mode constants and `normalize_qe_loop_retry_mode` | `test_qe_loop_retry_mode_normalization` | verified | - |
| F-002 | `AutoEvolutionScheduler.retry_loop` results-only branch; `results_only_retry.py` has no executor/qrun invocation | results-only success test asserts no submit/create call and source guard checks | verified | - |
| F-003 | `results_only_retry.py` prediction validation gate | missing/invalid pred test and all-NaN pred test | verified | - |
| F-004 | `results_only_retry.py` metric validation/recompute | success test verifies Rank IC equals original/recomputed value | verified | - |
| F-005 | `results_only_retry.py` required report artifact gate | success requires report; invalid tests stop before registration | verified | - |
| F-006 | `ResultsOnlyGateError` and retry_loop failure handling | failure tests assert loud reason_code and zero registration | verified | - |
| F-007 | QE experiment upsert and prediction-store same run key write | idempotency test calls registration twice and asserts single logical row | verified | - |
| F-008 | targeted unit test file plus config truth alias test | focused pytest command in PR evidence | verified | - |

## Risks

- Remote workspace file endpoints may return missing recorder binding for very old loops. The gate fails loud and instructs operators to use `backtest_only` or `full_train`.
- If metrics files are absent and label artifacts are missing, IC/RankIC cannot be recomputed safely. The gate fails loud rather than registering incomplete metrics.
- Prediction-store upload writes local backend artifact-store files during live use. Local unit tests inject a fake uploader/store and do not touch production runtime.
- Archive/research best-effort hooks remain optional follow-up observers; the required QE DB registration and prediction-store upload fail loud.

## Production Gates

- `production_ddl_gate`: noop; no schema or migration changes.
- `production_frontend_dependency_gate`: noop; no frontend dependency changes.
- `production_backend_dependency_gate`: noop; no backend dependency changes.
- Runtime activation gate: code merge does not restart backend or trigger live QE retry; operators must explicitly call retry with `retry_mode=results_only`.
