# HMM Regime Redefinition QE Handoff (2026-05-10)

This note is the current continuation point for the HMM regime-redefinition QE work. Read this together with `docs/codex_project_memory.md` and `docs/analysis/hmm_training_current_status_20260503.md` when resuming a new window.

## Current State

- The previous backtest-only comparison task `qe_20260510_010004_8c2d` is complete.
- Its result still shows `no-HMM` as the best option; the best HMM overlays did not beat the baseline.
- The second task `qe_20260510_102726_4fd3` is complete.
- It was backtest-only, ran on `rdagent-node1`, used parallelism 4, and kept all non-HMM settings aligned with `qe_20260502_131502_9b54` Loop1.
- The result again shows `no-HMM` as the best option, although the gentlest new HMM candidate improved versus several older HMM overlays.

## Completed Loop Results: qe_20260510_102726_4fd3

| Loop | Role | Annual Return | Sharpe/IR | Max Drawdown | Read |
| --- | --- | ---: | ---: | ---: | --- |
| Loop1 | No-HMM control | 38.18% | 1.690 | -15.50% | Best in this round |
| Loop2 | Retrained regime-linear, gentle boost/penalty | 37.75% | 1.653 | -17.21% | Best HMM in this round, still below no-HMM |
| Loop3 | Retrained top/bottom regime-linear, gentle boost/penalty | 37.39% | 1.640 | -16.88% | Below Loop2 and no-HMM |
| Loop4 | Retrained top/bottom regime-linear with stronger bottom risk penalty | 36.05% | 1.576 | -16.80% | Stronger risk penalty hurt return |

## HMM Assets Already Registered

- `d2da20b1-f3c5-410b-aee9-9d71dff4e846`
- `41e5cea2-a8be-47ee-a3ca-831c9609be16`
- `8834983a-7a44-4073-8108-d509faa92a31`

These snapshots are readable by the backend and can be selected in QE.

## Current Decision

- Do not continue Loop10-style micro-tuning as the main direction.
- The completed bounded/redefined HMM validation still did not beat no-HMM.
- Next direction should be a clearer HMM retrain or input-space redesign, not more tiny coefficient nudges.
- Sector-factor should remain a gate/confirmation path, not a direct replacement for HMM.

## Cleanup Guidance

- Keep:
  - `backend/data/hmm_models/*`
  - current or still-referenced QE task workspaces and artifacts
  - the summary docs that capture the QE conclusions
- Safe to delete after confirmation:
  - old completed AIstock-side `rdagent_assets/qe_experiments/<experiment_id>` folders whose results are already documented and no longer referenced
  - stale local diagnostic dumps that are not used by the current task
- Do not treat the old `qe_experiments` folders as the main space target; they are small validation artifacts.
- For actual disk recovery, inspect the larger `rdagent_assets` subtrees separately before deleting anything.

## Next Session Checklist

1. Read `docs/codex_project_memory.md`.
2. Read this handoff note.
3. Treat `qe_20260510_102726_4fd3` as completed and use the table above as the latest checkpoint.
4. If continuing HMM R&D, move to a deeper HMM retrain/input redesign direction instead of coefficient micro-tuning.
5. Keep no-HMM as the current production comparison baseline until a HMM version beats it in QE.
