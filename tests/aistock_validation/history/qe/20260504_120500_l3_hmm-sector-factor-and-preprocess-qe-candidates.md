# HMM sector-factor 与输入预处理 QE 候选验证记录（2026-05-04）

## 范围

- 以 `qe_20260502_131502_9b54` Loop1 的非 HMM 设置为固定基线，验证 Loop2 old-covfix 与 Loop10 penalty-only 两条 HMM 主线。
- 新增 sector-factor gate/confirmation hidden candidates：`boost_confirm`、`penalty_confirm`、`both_confirm`、`risk_only_overlay`。
- 新增 HMM 输入预处理 hidden candidates：train-only zscore、winsor(1/99)+zscore、robust zscore、sector cross-sectional rank、sector cross-sectional zscore。
- 增加候选归因脚本，输出换手、成本、changed days、TopK enter/exit attribution 的分析文件。

## 代码/资产安全

- 未修改生产 FastAPI `8001` 进程，实验创建使用 dev backend `8011`。
- 默认 QE HMM selector 仍只保留 Loop2 与 Loop10 两个 `sector_hmm` 配置；新增候选使用 hidden `model_type`，只能通过 custom QE loop 的 snapshot id 引用。
- HMM 模型资产写入 `backend/data/hmm_models/...`，该目录属于运行资产，不纳入本次 git 提交。
- `.env`、`.codex_tmp`、远端 QE workspace、生成模型资产均不提交。
- 远端资源保护：`qe_20260504_110457_5400` 当前 4 个 V25 回测进程占用约 73/78Gi 内存；未叠加启动第二个 p4 任务，避免 swap 污染回测结果，改为延迟提交。

## 已注册候选

### sector-factor hidden candidates

- model_type: `sector_hmm_experimental_stacking_20260504`
- registry result: `.codex_tmp/hmm_registry_updates/hmm_sector_factor_gate_registry_result_20260504_105718.json`
- snapshots: `17809fe6-bcaf-487e-9205-d11b47fe08f9`, `9761439e-06d1-4303-a6a7-1a4836c8b3f8`, `b45f6571-19b0-4e0a-9a20-ab182e59a68a`, `decfdc2c-f395-4cda-aac6-8636c5fcde50`, `040570a9-3a34-4201-8057-42299ec92c3e`, `f405daee-f922-449d-bf37-ca91b2fd9995`, `9a5c67d6-3fbc-41ee-93b1-36031ae181ad`, `b19d4beb-8e77-4ddc-a30f-d9f07e7fcda2`

### HMM input preprocess hidden candidates

- model_type: `sector_hmm_experimental_preprocess_20260504`
- latest registry result: `.codex_tmp/hmm_registry_updates/hmm_preprocess_registry_result_20260504_115526.json`
- latest snapshots: `71e966b4-6f7e-4767-b012-a19798df73bc`, `d2a56dad-b777-4fd6-964a-0420241b444f`, `fef38650-e591-4145-a62f-cfab9e2c10eb`, `acc27436-6e87-43fe-8e25-78261b80d47f`, `a72f7e35-b52a-4969-b1e7-1b1ec21270b0`, `d40c97fd-40ff-4ea5-9089-a3650ab26afe`, `b49a82e1-1fe3-466d-8b70-1632e267c442`, `c5647469-52b0-4d2c-a224-2f3a54b27d18`, `3b9ef5f6-e16c-4328-be2d-86447542b690`, `9ae55e28-0227-48bf-af8c-dcedae275609`
- verification: 10/10 `completed`, `sector_count=131`, model/coefficient files exist from Windows, coefficient artifact coverage `2024-07-01 -> 2026-04-27`, `dates=442`, first/last day sectors `(131, 131)`, `stock_sector_map=5847`。

## QE 任务

### sector-factor task

- task_id: `qe_20260504_110457_5400`
- created by: dev backend `http://127.0.0.1:8011/api/v1/quantevolver/evolution/custom-tasks`
- visible through UI/API on production backend `8001` because task rows are in DB。
- loops: 11 (no-HMM, Loop2 baseline, Loop10 baseline, 8 sector-factor candidates)
- remote node: `rdagent-node1`
- parallelism: `{"rdagent-node1": 4}`
- observed at 2026-05-04 12:01: Loop1-4 `running`; Loop5-11 `not_found` on remote because first wave has not finished yet。

### preprocess task

- payload prepared: `.codex_tmp/hmm_preprocess_custom_evo_payload_dev8011_20260504.json`
- loops: 13 (no-HMM, Loop2 baseline, Loop10 baseline, 10 preprocess candidates)
- remote node: `rdagent-node1`
- parallelism: `{"rdagent-node1": 4}`
- deferred launcher: `.codex_tmp/launch_hmm_preprocess_after_sector_task.ps1`
- launcher PID at start: `107580`
- launcher log: `.codex_tmp/hmm_preprocess_custom_evo_deferred_launcher_20260504.log`
- launch policy: wait until `qe_20260504_110457_5400` has no `running/not_found/ERR` remote loop statuses, then POST the preprocess payload to dev backend `8011` and write `.codex_tmp/hmm_preprocess_custom_evo_create_response_dev8011_20260504.json`。

## Commands and results

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile scripts/train_register_hmm_legacy_preprocess_candidates_20260504.py scripts/register_hmm_sector_factor_gate_candidates_20260504.py scripts/diagnostics/hmm_qe_candidate_attribution.py backend/services/hmm_training_service.py backend/services/quantevolver/experiment_config_builders.py backend/services/quantevolver/config_composer.py
# PASS

wsl bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/AIstock && python scripts/train_register_hmm_legacy_preprocess_candidates_20260504.py --replace-existing --delete-existing-files"
# PASS, latest result .codex_tmp/hmm_registry_updates/hmm_preprocess_registry_result_20260504_115526.json, count=10

C:/Users/lc999/miniconda3/envs/AIstock/python.exe .\.codex_tmp\verify_preproc_registry.py
# PASS, count 10, files_ok=True for every row

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/unified_engine/test_experiment_config.py -k hidden_snapshot -q
# PASS: 1 passed, 35 deselected

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/unified_engine/test_qe_config_truth.py -k hidden_config -q
# PASS: 1 passed, 33 deselected

C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/diagnostics/hmm_qe_candidate_attribution.py qe_20260504_110457_5400 --api-base http://127.0.0.1:8001/api/v1 --registry .codex_tmp/hmm_registry_updates/hmm_preprocess_registry_result_20260504_115526.json --output-dir .codex_tmp/hmm_qe_preprocess_candidate_attribution
# PASS, output .codex_tmp/hmm_qe_preprocess_candidate_attribution/qe_20260504_110457_5400/
```

## Preliminary offline attribution

- sector-factor offline TopK best: L10 `penalty_confirm` / `both_confirm`，holdout `net_mean_db_ret_10d` 约 `0.014774`；这是替换归因，不是完整 QE minute backtest。
- preprocess offline TopK best: L10 `train_zscore`，holdout `net_mean_db_ret_10d` 约 `0.010203`；其次 L10 `winsor01_zscore` 约 `0.009276`；Loop10 baseline 约 `0.008780`。
- L2 preprocess candidates in offline TopK attribution did not beat Loop2 baseline on holdout `net_mean_db_ret_10d`。

## Residual risks / pending

- `qe_20260504_110457_5400` is still running; final annual return, max drawdown, Sharpe, turnover, and cost conclusions are pending real QE completion。
- preprocess QE task is deliberately deferred until the sector-factor task releases remote memory; final task id and backtest metrics will appear after launcher POST succeeds。
- Offline TopK attribution is useful for direction selection but cannot replace full V25 minute execution QE results。
- The dev backend `8011` should remain running until the deferred preprocess launcher creates the task; do not stop PID `90376` before launch unless manually submitting the payload later。
