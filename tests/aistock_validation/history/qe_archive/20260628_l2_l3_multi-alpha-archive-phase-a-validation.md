# Multi-Alpha QE Archive Phase A 验证记录

- 日期：2026-06-28
- 模块：`qe_archive` / `multi_alpha`
- Worktree：`F:\Dev\AIstock_worktrees\multi-alpha-warehouse-archive-phase-a-20260628`
- 分支：`feature/multi-alpha-warehouse-archive-phase-a-20260628`
- 验证基线：`71636d45`（已 fast-forward 到最新 `origin/main`）
- Upstream status：`## feature/multi-alpha-warehouse-archive-phase-a-20260628...origin/main`
- 设计：`docs/architecture/multi_alpha_warehouse_archive_f2_design_20260628.md`
- 范围：仅 Phase A（§3B 步骤 1-7）；Phase B UI 与 Phase C 失败治理不在本轮范围。

## 关键结论

- scratch 真实写入验证通过：从生产 DB 只读复制 28 个 macb run、41 条 scheme、79 条 loo、1226 个 seed ref 到 dev scratch DB，随后执行 forward migration、真实 backfill 写入、幂等复跑、显式 skip 检查、rollback，并清理 scratch DB。
- 入仓覆盖率：`28/28` run；`multi_alpha_run=28`、`multi_alpha_leg=84`、`multi_alpha_leg_source=1226`、`multi_alpha_scheme=41`、`multi_alpha_loo=79`、`qe_archive.run` macb heads=`28`。
- 来源解析率：`1226/1226`，其中 `qear_run_*`=`1004`、`qe_*_L*`=`222`；`provenance_complete` legs=`84/84`。
- 业务 oracle：`multi_alpha_scheme` 的 `weights_json` / `per_window_weights_json` / 指标 mismatch=`0`；`multi_alpha_loo` mismatch=`0`；删除业务 roster seed 后，已入仓 `leg_source` 仍可恢复 `43/43` resolved provenance。
- 生产安全：未执行生产 DDL、未写生产 DB、未重启 backend/frontend/TDX/worker；生产 DB 仅作为只读证据源。

## 验证命令

| 层级 | 命令 | 结果 |
|---|---|---|
| L2/L3/L5 scratch | `python C:\Users\lc999\Documents\Codex\2026-06-28\alpha-qe-phase-a-macb-qe\work\run_macb_scratch_validation.py` | PASS；scratch `aistock_codex_macb_phasea_20260628164759` 已删除 |
| L0 compile | `python -m compileall -q backend/services/qe_archive backend/services/multi_alpha scripts/qe_archive_backfill.py` | PASS |
| L1 pytest | `python -m pytest backend/tests/qe_archive/test_multi_alpha_archive_handler.py backend/tests/test_multi_alpha_combine_backtest.py backend/tests/test_qe_archive_repository_static.py backend/tests/test_qe_archive_schema.py -q` | PASS：108 passed in 17.79s |
| 覆盖率 | `python -m coverage run --branch -m pytest backend/tests/qe_archive/test_multi_alpha_archive_handler.py -q` + coverage report | PASS：line 91%，branch 84% |
| diff hygiene | `git diff --check` | PASS：exit=0 |
| F2 workflow | `python scripts/aistock_feature_workflow.py validate --design docs/architecture/multi_alpha_warehouse_archive_f2_design_20260628.md --tier F2` | PASS：17/17，warnings=0 |
| guardrail | `python scripts/aistock_guardrail_scan.py --changed-only --baseline-json tests/aistock_validation/guardrails_baseline_20260511.json --fail-new-only --fail-on-severity P1 --output-json tmp/validation/guardrails/macb_phase_a_changed.json --summary-md tmp/validation/guardrails/macb_phase_a_changed.md` | PASS：blocking=0；9 个 P2 `ALGO-COMPLEXITY-001` review findings |

## Scratch DB 证据

```json
{
  "scratch_db": "aistock_codex_macb_phasea_20260628164759",
  "scratch_dropped": true,
  "prod_readonly": {"host": "127.0.0.1", "port": 5432, "dbname": "aistock", "user": "postgres"},
  "dev_scratch": {"host": "127.0.0.1", "port": 5433, "dbname": "aistock_codex_macb_phasea_20260628164759", "user": "postgres"},
  "prod_copy_counts": {
    "qe_archive.run": 740,
    "qe_archive.run_config": 740,
    "qe_archive.run_source": 740,
    "public.qe_evolution_loops": 758,
    "strategy_pkg.multi_alpha_combine_backtest_run": 28,
    "strategy_pkg.multi_alpha_combine_backtest_scheme_result": 41,
    "strategy_pkg.multi_alpha_combine_backtest_loo": 79
  },
  "backfill_summary": {
    "processed_count": 28,
    "candidate_count": 28,
    "ingested_count": 28,
    "skipped_count": 0,
    "failed_count": 0,
    "provenance_report": {
      "leg_count": 84,
      "provenance_complete_leg_count": 84,
      "provenance_complete_leg_rate": 1.0,
      "leg_source_count": 1226,
      "resolved_source_count": 1226,
      "source_resolve_rate": 1.0
    }
  },
  "archive_counts": {
    "multi_alpha_run": 28,
    "multi_alpha_leg": 84,
    "multi_alpha_leg_source": 1226,
    "multi_alpha_scheme": 41,
    "multi_alpha_loo": 79,
    "archive_run_heads": 28
  },
  "provenance_counts": [
    {"seed_ref_kind": "archive_run_id", "resolved": true, "count": 1004},
    {"seed_ref_kind": "evolution_loop_id", "resolved": true, "count": 222}
  ],
  "scheme_mismatch": 0,
  "loo_mismatch": 0,
  "rollback": {
    "multi_alpha_run": null,
    "multi_alpha_leg": null,
    "multi_alpha_leg_source": null,
    "multi_alpha_scheme": null,
    "multi_alpha_loo": null,
    "macb_archive_run_rows_after_rollback": 0,
    "existing_qe_run_rows_after_rollback": 740
  }
}
```

## Phase A 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/migrations/qe_archive_multi_alpha_phase_a_20260628.sql`; `backend/migrations/qe_archive_multi_alpha_phase_a_20260628.rollback.sql`; `backend/db/init_qe_archive_schema.py` | scratch forward+rollback PASS；5 张表；5 个 table comment；66 个 column comment；rollback 删除五表和 macb archive heads | verified | `production_ddl_gate=pending`；未应用生产 |
| F-002 | `backend/services/qe_archive/handlers/multi_alpha_combine_archive_handler.py`; `backend/services/qe_archive/multi_alpha_provenance.py` | scratch `multi_alpha_leg=84`；`provenance_complete_leg_count=84`；`survives_business_seed_delete before=43 after=43` | verified | 仅 Phase A 归档；UI detail panel 不在本轮范围 |
| F-003 | `backend/services/qe_archive/handlers/multi_alpha_combine_archive_handler.py`; `backend/services/qe_archive/repository.py` | scratch `multi_alpha_scheme=41`；`scheme_mismatch=0`；weights/per_window/metrics 与业务表逐值一致 | verified | - |
| F-004 | `backend/services/qe_archive/event_capture.py`; `backend/services/qe_archive/worker_service.py`; `backend/services/multi_alpha/combine_backtest.py` | pytest 覆盖 worker registration/dispatch；failed-run 测试证明 archive event enqueue 前业务终态已持久化 | verified | archive capture 遵守 env gate；仅 sidecar |
| F-005 | `backend/services/qe_archive/handlers/multi_alpha_combine_archive_handler.py`; `backend/services/qe_archive/repository.py` | scratch 真实写入 28 个 archive heads + 五表；targeted pytest 覆盖 handler path | verified | - |
| F-006 | `backend/services/qe_archive/backfill_service.py`; `scripts/qe_archive_backfill.py`; `backend/routers/qe_archive.py` | backfill `--source multi-alpha`：processed=28 candidate=28 ingested=28 failed=0 | verified | - |
| F-007 | `backend/services/qe_archive/repository.py` | scratch 第二次 `include_archived` backfill 后计数不变：28/84/1226/41/79/28 | verified | - |
| F-008 | `backend/services/qe_archive/worker.py`; `backend/services/qe_archive/repository.py`; `backend/services/qe_archive/handlers/multi_alpha_combine_archive_handler.py` | scratch missing macb source 生成 `skip_registry` 的 `multi_alpha_archive_policy`；worker skipped=10（含 paper policy skips） | verified | P2 note：scratch 复制了既有 paper telemetry events；policy separation 已验证 |
| F-009 | `backend/migrations/qe_archive_multi_alpha_phase_a_20260628.sql`; `backend/services/multi_alpha/combine_backtest.py`; `backend/services/qe_archive/handlers/multi_alpha_combine_archive_handler.py` | scratch 历史 logical `partial_failed` 已对齐到 business status `partial_failed=6` 和 archive status `partial_failed=6` | verified | UI 展示属于 Phase B，本轮未实现 |
| F-013 | `backend/services/qe_archive/backfill_service.py`; `scripts/qe_archive_backfill.py` | 覆盖报告：run 28/28，provenance 1226/1226，leg provenance 84/84 | verified | 未执行生产写入；仅 scratch |
| F-014 | `backend/services/qe_archive/worker.py`; `backend/services/qe_archive/repository.py`; scratch validation | rollback 后 `existing_qe_run_rows_after_rollback=740`；run_type_counts 包含 `evolution_loop=714`、`single_experiment=26`；paper telemetry 走 `paper_v2_throwaway_policy` skip | verified | - |
| F-015 | `backend/services/qe_archive/multi_alpha_provenance.py`; `backend/tests/qe_archive/test_multi_alpha_archive_handler.py` | `qear_run_* resolved=1004`；`qe_*_L* resolved=222`；unresolved failure modes 已有单测覆盖 | verified | - |
| F-017 | `backend/services/qe_archive/handlers/multi_alpha_combine_archive_handler.py`; `backend/services/qe_archive/repository.py` | `multi_alpha_scheme` 的 `weights_json` / `per_window_weights_json` / metrics 与业务一致；normalize/walk_forward/baseline 存入 `multi_alpha_run` | verified | 权重 UI 展示属于 Phase B，本轮未实现 |

## Guardrail P2 说明

`aistock_guardrail_scan` changed-only P1 gate 通过，`blocking=0`。新增 9 个 P2 `ALGO-COMPLEXITY-001` findings 位于 `backend/services/qe_archive/repository.py` 的 SQL helper/upsert 行。Phase A repository 路径按单 run 有界写入，使用 `execute_values(page_size=500)`，并通过 scratch 28 run / 1226 seed 验证；记录为可接受 P2 review 风险，不作为合入阻断。

## 未实现 / 未验证项

- Phase B UI：`F-010`、`F-011`、`F-016` 以及 `F-017` 的 UI 展示部分本轮未实现。
- Phase C：`F-012` 失败治理、远程派发联动、conf parse bug 治理本轮未实现。
- 生产 DDL：未获授权、未应用。合并 main / 生产激活前，必须按 §9 生产门禁应用并验证已提交 migration。

## 生产门禁

- `production_ddl_gate=pending`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- 生产 DB：仅作为只读证据源；无 DDL、无写入。
- 生产 runtime：未重启或激活 backend/frontend/TDX/worker。

## 证据文件

- Sidecar evidence JSON（git 忽略）：`C:/Users/lc999/Documents/Codex/2026-06-28/alpha-qe-phase-a-macb-qe/work/20260628_l2_l3_multi-alpha-archive-phase-a-evidence.json`
- Scratch full report：`C:/Users/lc999/Documents/Codex/2026-06-28/alpha-qe-phase-a-macb-qe/work/macb_phase_a_scratch_validation_report.json`
- Scratch backfill output：`C:/Users/lc999/Documents/Codex/2026-06-28/alpha-qe-phase-a-macb-qe/work/macb_phase_a_scratch_backfill_output.json`
- Coverage JSON：`C:/Users/lc999/Documents/Codex/2026-06-28/alpha-qe-phase-a-macb-qe/work/multi_alpha_phase_a_coverage.json`
- Guardrail JSON：`tmp/validation/guardrails/macb_phase_a_changed.json`
