# HMM Loop10 Bottom-Penalty QE Overnight Run - L3

## Scope

- Registered Loop10-centered FPB_VALZ sparse bottom-sector penalty HMM candidates for QE selection.
- Created remote QE custom evolution task with parallelism 2 because other backtests are running.
- Installed a 40-minute Windows scheduled monitor that checks progress, analyzes terminal tasks, and conditionally launches a focused stage-2 attempt.

## Stage 1 QE Task

- Task: `qe_20260505_003452_0eab`
- Name: `HMM_L10_FPBVALZ_bottom_penalty_qe_20260502_131502_9b54_remote_p2_20260505_003452`
- Loops: 6
- Execution mode: `parallel_2`
- Node parallelism: `rdagent-node1=2`
- Initial observed status: `running`, with Loop1 and Loop2 running.

## Registered HMM Snapshots

- `HMM_TEST_L10_FPBVALZ_BOTTOM15_PENALTY_0p98__qe20260505` / snapshot `d33d3f6d-f98d-4250-b238-47e23394fe62`
- `HMM_TEST_L10_FPBVALZ_BOTTOM20_PENALTY_0p98__qe20260505` / snapshot `94de95bb-6c6e-42ec-8d9e-ec0f8af6360a`
- `HMM_TEST_L10_FPBVALZ_BOTTOM25_PENALTY_0p98__qe20260505` / snapshot `d313cf3c-e71d-4cba-a4d0-1446fd8eac39`

## Automation

- Scheduled task: `AIstock_HMM_QE_Overnight_20260505`
- Interval: 40 minutes
- Wrapper: `.codex_tmp/hmm_l10_bottom_penalty_qe_20260505/run_overnight_monitor_once.ps1`
- Monitor script: `scripts/automation/hmm_qe_overnight_monitor_20260505.py`
- State: `.codex_tmp/hmm_l10_bottom_penalty_qe_20260505/overnight_monitor_state.json`
- Log: `.codex_tmp/hmm_l10_bottom_penalty_qe_20260505/overnight_monitor.log`

## Commands Executed

```powershell
python scripts/register_hmm_loop10_bottom_penalty_candidates_20260505.py --dry-run
python scripts/register_hmm_loop10_bottom_penalty_candidates_20260505.py
python -m py_compile scripts/register_hmm_loop10_bottom_penalty_candidates_20260505.py scripts/automation/hmm_qe_overnight_monitor_20260505.py
schtasks /Create /TN AIstock_HMM_QE_Overnight_20260505 /SC MINUTE /MO 40 /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File <wrapper>" /F
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .codex_tmp/hmm_l10_bottom_penalty_qe_20260505/run_overnight_monitor_once.ps1
```

## Business Outcome

- The three script-screened HMM versions are now registered as selectable `sector_hmm` snapshots.
- QE task is visible via both backend ports `8001` and `8011`.
- The monitor is idempotent and will only launch stage 2 if a stage-1 bottom-penalty candidate beats Loop10.

## Residual Risk

- If Windows sleeps or the backend/remote node stops, scheduled checks may be delayed or fail; the state/log files will show the failure.
- The automation does not delete low-value HMM snapshots; it only records valuable versions.

## Fallback Stage Update

The monitor was updated after user instruction: if stage 1 does not beat Loop10, it will no longer stop. It will automatically launch stage 2 with other optimization directions:

- FPB_VALZ bottom 15% sparse penalty at 0.985.
- FPB_VALZ bottom 20% sparse penalty at 0.985.
- VOLCOMP bottom 15% sparse penalty at 0.99.
- VOLCOMP risk-only overlay at 0.995.

Controls in stage 2 remain no-HMM, Loop10, and the best stage-1 candidate when available. Parallelism remains 2.

Validation:

```powershell
python -m py_compile scripts/automation/hmm_qe_overnight_monitor_20260505.py scripts/register_hmm_loop10_bottom_penalty_candidates_20260505.py
python scripts/register_hmm_loop10_bottom_penalty_candidates_20260505.py --dry-run --candidates-json .codex_tmp/hmm_l10_bottom_penalty_qe_20260505/fallback_stage2_specs_dryrun.json
schtasks /Run /TN AIstock_HMM_QE_Overnight_20260505
```

Scheduled task run verified with Last Result 0 at 2026-05-05 00:59:10.
