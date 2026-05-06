# HMM Stage3 sparse backtest-only QE start - 2026-05-05 21:10

## Scope

Continue HMM R&D after the successful backtest-only validation. This run tests sparse penalty maps derived from retrained Stage3 HMM score panels, using QE backtest-only mode so the base LGBM model is not retrained.

## Script-Level Screen

Command:

```powershell
python scripts/hmm_stage3_sparse_penalty_screen_20260505.py --task-id qe_20260505_123035_bf80 --output-dir .codex_tmp/hmm_stage3_sparse_penalty_screen_20260505
```

Result:

- Tested candidates: 200
- Screen report: `.codex_tmp/hmm_stage3_sparse_penalty_screen_20260505/qe_20260505_123035_bf80/sparse_penalty_screen_report.md`
- Ranked holdout table: `.codex_tmp/hmm_stage3_sparse_penalty_screen_20260505/qe_20260505_123035_bf80/sparse_topk_holdout_ranked.csv`
- Coefficients: `.codex_tmp/hmm_stage3_sparse_penalty_screen_20260505/qe_20260505_123035_bf80/candidate_coefficients/`

Selected for QE:

| Candidate | Holdout changed days | avg entered/day | 10d DB net ret | Rationale |
| --- | ---: | ---: | ---: | --- |
| `SPARSE_turnover_light_n3_util_low_B05_PEN_0p995_stage3_only` | 4 | 0.0167 | 0.0542 | Top score, ultra-sparse sanity candidate |
| `SPARSE_turnover_light_n3_util_low_B10_PEN_0p995_stage3_only` | 8 | 0.0335 | 0.0496 | Top turnover-light candidate with positive 20d check |
| `SPARSE_flow_breadth_n2_util_low_B05_PEN_0p995_stage3_only` | 4 | 0.0167 | 0.0553 | Independent flow-breadth source cross-check |
| `SPARSE_turnover_light_n3_util_low_B15_PEN_0p995_stage3_only` | 15 | 0.0628 | 0.0317 | Broader turnover-light sensitivity |
| `SPARSE_flow_breadth_n2_util_low_B20_PEN_0p995_stage3_only` | 30 | 0.1297 | 0.0199 | Broader flow-breadth sensitivity |

## Registry

New registration script:

- `scripts/register_hmm_stage3_sparse_qe_candidates_20260505.py`

Validation:

```powershell
python -m py_compile scripts/register_hmm_stage3_sparse_qe_candidates_20260505.py
python scripts/register_hmm_stage3_sparse_qe_candidates_20260505.py --dry-run
python scripts/register_hmm_stage3_sparse_qe_candidates_20260505.py
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py -k hmm -q
```

Results:

- `py_compile`: pass
- Dry run: pass
- Actual registry: pass
- HMM config truth test: 4 passed, 41 deselected
- Visible QE HMM selector remains clean with exactly 2 `sector_hmm` configs: Loop10 and Loop2.
- Sparse candidates are hidden under `model_type=sector_hmm_experimental_stage3_sparse_20260505`, but direct snapshot resolution works.

Registered snapshots:

| Display name | Snapshot |
| --- | --- |
| `HMM_TEST_STAGE3_SPARSE_TL_B05_PEN_0p995__qe20260505` | `7d7e78c0-1e2c-4796-a97d-dbe7371b08ef` |
| `HMM_TEST_STAGE3_SPARSE_TL_B10_PEN_0p995__qe20260505` | `9869553f-632d-498c-8021-b1e15c2c1db8` |
| `HMM_TEST_STAGE3_SPARSE_FB_B05_PEN_0p995__qe20260505` | `c5fe7775-1b32-47a9-9d8b-e02610f89f4d` |
| `HMM_TEST_STAGE3_SPARSE_TL_B15_PEN_0p995__qe20260505` | `db001359-2ef4-4db3-8cab-c68bc1ea18b2` |
| `HMM_TEST_STAGE3_SPARSE_FB_B20_PEN_0p995__qe20260505` | `19382026-9950-4764-bc7f-46cb1778b29e` |

Registry evidence:

- Result: `.codex_tmp/hmm_registry_updates/hmm_stage3_sparse_registry_result_20260505_210214.json`
- Backup: `.codex_tmp/hmm_registry_updates/hmm_stage3_sparse_registry_before_20260505_210214.json`

## QE Task

Created through production backend API `http://127.0.0.1:8001/api/v1/quantevolver/evolution/custom-tasks`.

- Task ID: `qe_20260505_210355_155f`
- Task name: `HMM_stage3_sparse_candidates_backtest_only_remote_p4_20260505_210355`
- Source trained model: `qe_20260505_123035_bf80` Loop1
- Mode: `backtest_only=true` for all loops
- Node: `rdagent-node1`
- Execution mode: `parallel_4`
- Node parallelism: `{"rdagent-node1": 4}`
- Initial status: `running`
- Initial submitted loops: Loop1-Loop4 running

Loop set:

| Loop | Candidate |
| ---: | --- |
| 1 | no-HMM backtest-only control |
| 2 | Loop10 current best `6ea64754-003d-48d8-ad9e-d0e7857716c8` |
| 3 | Loop2 drawdown control `bbec3863-fb67-445f-938e-66f092d18696` |
| 4 | Stage3 sparse TL B05 `7d7e78c0-1e2c-4796-a97d-dbe7371b08ef` |
| 5 | Stage3 sparse TL B10 `9869553f-632d-498c-8021-b1e15c2c1db8` |
| 6 | Stage3 sparse FB B05 `c5fe7775-1b32-47a9-9d8b-e02610f89f4d` |
| 7 | Stage3 sparse TL B15 `db001359-2ef4-4db3-8cab-c68bc1ea18b2` |
| 8 | Stage3 sparse FB B20 `19382026-9950-4764-bc7f-46cb1778b29e` |

Evidence:

- Payload: `.codex_tmp/hmm_stage3_sparse_qe_20260505/payload.json`
- Create response: `.codex_tmp/hmm_stage3_sparse_qe_20260505/create_response.json`
- Initial task detail: `.codex_tmp/hmm_stage3_sparse_qe_20260505/task_detail_initial.json`
- Initial remote proof: `.codex_tmp/hmm_stage3_sparse_qe_20260505/initial_remote_backtest_only_proof.json`

## Initial No-Training Check

Loop1-Loop4 remote logs already show:

- `qrun_limit_minute.py conf.yaml --backtest-only && python read_exp_res.py`
- `Symlink mlruns` from `qe_20260505_123035_bf80/Loop1/mlruns`

The explicit `skipping model training` line usually appears later in the loop log after factor preparation. Recheck all 8 loops after completion.

## Next Check

After task completion:

```powershell
python scripts/qe_evolution_diagnostic.py qe_20260505_210355_155f --json --api-base http://127.0.0.1:8001/api/v1
```

Then compare against Loop10 and no-HMM, and inspect all remote logs for final `Backtest-only mode: skipping model training, loading existing model`.
