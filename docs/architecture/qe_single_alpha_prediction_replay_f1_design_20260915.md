# QE Registered Single-Alpha Prediction Replay (F1)

## 1. Background

Add one explicit registered QE loop mode that replays the immutable
`pred.pkl` of a completed single-alpha loop under a new portfolio policy.  The
mode exists so P00/P10/P01/P11 comparisons can change HMM and/or the PIT sector
blacklist without retraining a model or repeating inference.

This is an F1, QE-only capability.  It extends the existing custom-evolution
submission and minute `qrun_limit_minute.py --pred-backtest` path.  It does not
change Qlib, dataset candidates, database schemas, default full-train or
model-reuse behavior.

## 2. Scope

The scope is the registered single-alpha custom-evolution request, typed QE
configuration, source prediction resolution, backtest executor, execution
manifest, QE MCP contract, and task/loop summary readback needed for immutable
prediction replay.

## 3. Non-Goals

- Do not modify Qlib source, the prediction algorithm, model training, factor
  computation, dataset construction, HMM training, or blacklist data creation.
- Do not add DDL/DML, dependencies, a new scheduler, a second artifact store,
  a new approval gate, or a new portfolio backtest implementation.
- Do not activate runtime, submit MA-E23, or mutate dataset candidates in this
  feature delivery.

## 4. Fixed invariants

- The caller must explicitly set `prediction_replay=true` and identify one
  source task and positive source loop index.
- Replay and existing `backtest_only` are mutually exclusive.
- The source loop must be terminal `completed`, its workspace catalog must be
  complete, and the catalog must contain exactly one `*/artifacts/pred.pkl`.
- The backend downloads the source bytes through the source loop's registered
  execution node, computes SHA256, and rejects any caller-supplied expected
  digest that does not match.
- The target workspace receives immutable replay bytes and a source receipt;
  no model package, training command, inference command, V25 path, daily
  execution fallback, or market-data database read is permitted.
- Execution must reuse the existing minute runner:
  `qrun_limit_minute.py conf.yaml --pred-backtest frozen_prediction.pkl`.
- The existing run-scoped dataset intersection remains the sole eligibility
  filter.  Prediction replay must not implement a second blacklist filter.
- Two identities are distinct and visible: the immutable source prediction
  SHA256, and the executable prediction-panel SHA256 after the target PIT/
  blacklist intersection.
- Replay is capacity-accounted as backtest-only work, never parallel training.
  Existing task `node_parallelism` remains authoritative.
- All failures are loud and stable; there is no fallback to model reuse,
  training, an old dataset, an old node, or a different prediction artifact.

## 5. Public and persisted contract

Custom-evolution loop input adds:

- `prediction_replay: bool = false`
- `prediction_source_task_id: str | null`
- `prediction_source_loop_index: int | null`
- `prediction_source_sha256: str | null`

The digest supplied by a caller is an expected pin.  The server-resolved
digest is authoritative and is persisted with source task, loop, node,
catalog path, byte size, and replay mode in the loop config and execution
manifest.  These fields are control-plane metadata and must not enter model
parameters or strategy kwargs.

Task/loop/API/MCP/UI summaries expose replay mode, source task/loop, source
digest, and executable-panel digest when available.  Existing unregistered,
full-train, model-reuse, custom-evolution, and multi-alpha contracts remain
unchanged.

## 6. Implementation Plan

1. Request validation:
   `backend/routers/quantevolver_evolution.py` and
   `backend/mcp/modules/qe_experiment.py` apply the same mutual-exclusion,
   required-source, positive-index, and SHA256-shape rules.
2. Typed configuration:
   `experiment_config.py` and `experiment_config_builders.py` carry replay
   identity separately from strategy/model configuration.
3. Source resolution:
   `qe_evolution_service.py` resolves the source loop/node/catalog, downloads
   one bounded prediction file, computes/verifies its digest, and stages
   `frozen_prediction.pkl.b64` plus
   `qe_prediction_replay_source_ref.json`.
4. Execution:
   `executors/backtest.py` adds an explicit prediction-replay mode and invokes
   the existing minute pred-backtest runner without model payloads.
5. Audit/readback:
   `execution_manifest.py`, `payload_summary.py`, and the evolution Loop detail
   panel expose the frozen source and executable-panel identities without
   leaking them into executable strategy parameters or adding manual ID/SHA
   inputs.

No DDL/DML, dependency installation, candidate write, Qlib source edit,
backend process control, or experiment submission belongs to this feature PR.

## 7. Risks and failure modes

The request or submission fails before remote execution for malformed or
incomplete replay identity, non-completed source, partial catalog, zero or
multiple prediction artifacts, wrong node, unreadable/empty/oversized bytes,
or digest mismatch.  The runner remains responsible for rejecting an invalid
prediction object, an incompatible label panel, an empty eligibility
intersection, missing minute execution configuration, or no executed trades.

## 8. Design Acceptance Index

- `F-001`: explicit replay request fields exist in router and QE MCP with
  identical fail-closed validation.
- `F-002`: replay is mutually exclusive with model-reuse `backtest_only` and
  never silently becomes full training.
- `F-003`: source resolution accepts only one completed-loop prediction from
  a complete registered workspace catalog and pins its bounded byte digest.
- `F-004`: target command contains exactly one `--pred-backtest`, uses minute
  execution, and contains no training/model-package/V25/daily fallback.
- `F-005`: source identity is persisted consistently in config, workspace
  receipt, execution manifest, and compact task/loop readback.
- `F-006`: source prediction SHA and executable-panel SHA are represented as
  different fields; blacklist filtering remains single-sourced in the runner.
- `F-007`: replay consumes existing backtest capacity and is never counted as
  GPU/parallel training.
- `F-008`: legacy full-train, model-reuse, unregistered, custom-evolution and
  multi-alpha behavior remains semantically unchanged.
- `F-009`: focused tests cover success and every pre-execution rejection,
  command purity, byte identity, readback, and WSL/node1 catalog shapes.
- `F-010`: production gates are all noop; runtime activation remains pending
  user-owned backend restart and post-restart business verification.

## 9. Verification Plan

- `python -m pytest -q backend/tests/unified_engine/test_qe_prediction_replay.py`
- The focused replay contract is collected by the existing
  `qe_read_backend` nox plan; no new CI lane or approval gate is introduced.
- `python -m pytest -q backend/tests/unified_engine/test_backtest_executor.py backend/tests/quantevolver/test_payload_summary.py backend/tests/quantevolver/test_execution_manifest_json_safe.py`
- `python -m pytest -q backend/tests/quantevolver/test_qe_registered_submission.py backend/tests/test_aistock_qe_mcp_servers.py`
- `python -m pytest -q backend/tests/multi_alpha/test_qe_submission_coordinator.py`
- TypeScript syntax and ESLint checks for the two changed QE evolution UI
  files, reusing the canonical frontend dependencies without installing any.
- Run changed-file Ruff/py_compile, `git diff --check`, changed-file guardrail,
  and the F1 feature validator.

## 10. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | router and MCP request contract | `python -m pytest -q backend/tests/unified_engine/test_qe_prediction_replay.py` | verified | - |
| F-002 | request validation, executor mode selection | `backend/tests/unified_engine/test_qe_prediction_replay.py` | verified | - |
| F-003 | source prediction resolver | `backend/tests/unified_engine/test_qe_prediction_replay.py` | verified | - |
| F-004 | BacktestExecutor prediction replay mode | `python -m pytest -q backend/tests/unified_engine/test_backtest_executor.py` | verified | - |
| F-005 | loop config, source receipt, execution manifest, compact summary and UI detail card | `backend/tests/unified_engine/test_qe_prediction_replay.py` | verified | - |
| F-006 | runner output/readback contract | `backend/tests/unified_engine/test_qe_prediction_replay.py` | verified | - |
| F-007 | reservation metadata, SQL cohort proof and executor context | `python -m pytest -q backend/tests/multi_alpha/test_qe_submission_coordinator.py` | verified | - |
| F-008 | unchanged default modes | `python -m pytest -q backend/tests/quantevolver/test_qe_registered_submission.py` | verified | - |
| F-009 | focused and related QE regression suite; existing `qe_read_backend` plan collects the replay contract | `python -m nox -s qe_read_backend`: 602 passed, 1 pre-existing optional-import skip; `python -m pytest -q backend/tests/unified_engine/test_qe_prediction_replay.py`: 25 passed | verified | - |
| F-010 | production gates and runtime boundary | `backend/tests/unified_engine/test_qe_prediction_replay.py`: DDL, DML, dependency installation, candidate writes, process control and experiment submission all noop | verified | - |

## 11. MA-E23 activation use

After source merge and the user-owned backend restart, MA-E23 may create nine
replay arms from MA-E22 Loop2/3/4.  WSL uses six LSTM-policy arms with
parallelism two; rdagent-node1 uses three LGBM-policy arms with configured cap
four.  Admission additionally requires same-release frozen HMM and PIT sector
policy files on both nodes.  Their absence remains a data-preparation blocker,
not a reason to weaken this feature.

## 12. Production Gates

- `production_ddl_gate`: noop.
- `production_frontend_dependency_gate`: noop.
- `production_backend_dependency_gate`: noop.
- Backend restart/runtime activation: pending user action after merge.

## 13. DESIGN-COMPLIANCE-001

1. No simplified/POC delivery: the registered request, resolver, executor,
   manifest, readback, MCP, and tests form one complete usable path.
2. No silent errors: every identity, catalog, digest, compatibility, and
   execution failure stops loudly without fallback.
3. No business-logic drift: existing prediction filtering, strategy,
   execution, costs, risk filters, and legacy modes remain authoritative.
4. No extra approvals: the feature introduces no new business gate; it only
   enforces the immutable input needed by the requested causal comparison.
