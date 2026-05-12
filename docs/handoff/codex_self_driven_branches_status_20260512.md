# Codex Self-Driven Branches Status - 2026-05-12

## Scope

- Repo: `F:/Dev/AIstock`.
- Audit worktree: `F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512`.
- Authorized phase: Phase D only from long task `cd53ec84` partial start.
- Safety: read-only evaluation of existing branches; no DB, services, ports, branch deletion, archive, merge, or source edits.
- Phase B/C remain on hold until explicit 9:30 GO.

## Summary Recommendation

- `codex/hmm-sector-regime-20260509`: preserve evidence, write a final Round 2 conclusion if resumed, then archive or extract only narrow reusable fixes. Do not continue coefficient-sweep HMM tuning as-is.
- `codex/financial-distress-rerank-20260508`: research branch is effectively complete for its current hypothesis and found no true-QE promotion candidate. Archive/review rather than continue implementation.
- Overall: Codex should stop self-driven research expansion until strategy assigns a concrete integration/cherry-pick/archive action. Both branches contain useful research evidence but are not ready for blind merge to `main`.

## Branch Status Table

| Branch | Worktree | Dirty State | Ahead vs `origin/main` | Last Commit | Progress | Recommendation |
|---|---|---|---:|---|---|---|
| `codex/hmm-sector-regime-20260509` | `F:/Dev/AIstock_worktrees/hmm-sector-regime-20260509` | tracked files clean; untracked `.codex_tmp/` scratch remains | 5 | `ce8f099` 2026-05-10 10:35 +08 `docs(hmm): add regime qe handoff` | R&D loop complete enough to conclude no-HMM still wins in documented Round 1 and scratch Round 2 | Review then archive; optionally cherry-pick reusable forward-filter/test/doc pieces |
| `codex/financial-distress-rerank-20260508` | `F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508` | clean | 30 | `07b7caa` 2026-05-12 00:21 +08 `feat(event): screen non q_ocf structured distress rules` | ~95% complete for research objective; production/QE integration intentionally 0% | Archive/review; no further implementation without new hypothesis |

## `codex/hmm-sector-regime-20260509`

### Observed State

- Upstream: `origin/codex/hmm-sector-regime-20260509`.
- Ahead/behind vs current `origin/main`: 5 ahead, about 200 behind at audit time.
- Not contained in `origin/main`.
- Unique commits:
  - `69f5153 feat(hmm): add sector rotation redefine screen`
  - `15446df fix(hmm): trim redefine screen eof whitespace`
  - `f01cdad feat(hmm): screen bounded regime candidates`
  - `2733cb7 feat(hmm): register bounded regime candidate for qe`
  - `ce8f099 docs(hmm): add regime qe handoff`

### Key Files

- `backend/quant_models/hmm/sector_hmm.py`
- `backend/tests/test_hmm_forward_filter.py`
- `scripts/hmm_sector_rotation_redefine_screen_20260509.py`
- `scripts/hmm_regime_bounded_candidate_screen_20260509.py`
- `scripts/register_hmm_regime_bounded_qe_candidate_20260510.py`
- `docs/analysis/hmm_regime_bounded_screen_20260509.md`
- `docs/analysis/hmm_regime_redefinition_qe_handoff_20260510.md`
- `tests/aistock_validation/history/hmm/20260509_1930_l2_hmm-sector-rotation-redefine-screen.md`
- `tests/aistock_validation/history/hmm/20260509_2024_l2_hmm-regime-bounded-screen.md`

### Interpretation

- The branch moved away from Loop10 coefficient micro-tuning and explored redefined sector-regime HMM candidates with bounded coefficients.
- The handoff records Round 1 QE `qe_20260510_010004_8c2d`: no-HMM remained best, while COVFIX, Loop10, and the new regime-bounded candidate underperformed.
- Local scratch monitor evidence under `.codex_tmp/qe_monitor/` indicates Round 2 `qe_20260510_102726_4fd3` completed with `sota_count: 0`; this is useful evidence but is not committed.
- Idle estimate: roughly 45 hours since latest scratch activity and last commit.

### Recommendation

- Do not delete immediately: the branch contains nontrivial HMM R&D artifacts and a possible reusable forward-filter compatibility test/fix.
- Do not continue the same coefficient-sweep line without a new strategy decision.
- If resumed, first write a final Round 2 conclusion doc from `.codex_tmp/qe_monitor/qe_20260510_102726_4fd3_diagnostic.json`, then choose one of:
  1. Archive after preserving the conclusion.
  2. Extract only reusable infrastructure/fixes onto a fresh `origin/main` branch.
  3. Start a new HMM branch focused on risk-gating/no-trade thresholds and changed-days attribution instead of boost/penalty micro-tuning.

## `codex/financial-distress-rerank-20260508`

### Observed State

- Upstream: `origin/codex/financial-distress-rerank-20260508`.
- Ahead/behind vs current `origin/main`: 30 ahead, about 230 behind at audit time.
- Not contained in `origin/main`.
- Worktree is clean.
- Diff footprint: about 74 files, dominated by research scripts, docs, validation history, and tests.

### Key Files

- `backend/services/event_signal/financial_distress_qe_overlay_research.py`
- `backend/services/event_signal/financial_distress_direct_event_research.py`
- `backend/services/event_signal/financial_distress_pred_materializer.py`
- `backend/services/event_signal/early_financial_distress_research.py`
- `backend/tests/event_signal/test_financial_distress_qe_overlay_research.py`
- `backend/tests/event_signal/test_financial_distress_direct_event_research.py`
- `backend/tests/event_signal/test_financial_distress_pred_materializer.py`
- `scripts/financial_distress_phase24_signal_family_screen.py` through `scripts/financial_distress_phase31_non_qocf_structured_screen.py`
- `docs/analysis/event_signal_financial_distress_research/progress.md`
- `docs/analysis/event_signal_financial_distress_phase31_non_qocf_structured_screen_result_20260511.md`

### Interpretation

- The branch is research-only by design. The progress docs explicitly say not to merge to `main` until user request, and report no QE/Paper/Selection/QMT integration.
- Phase 31 completed a non-q_ocf structured screen with 14 rules and 84 stability rows / 1848 validations, but found `true-QE candidates = 0` and `NO_WSL_TRUE_QE_RERUN`.
- The final interpretation is watchlist/direct-event research only: no buy ban, forced sell, score boost, DB policy write, Paper hook, or QE hook.

### Recommendation

- Continue: no, unless the user approves a new financial-distress hypothesis beyond Phase 31.
- Archive: yes, preserve the branch or cherry-pick docs into a research archive.
- Delete: not now; the branch is not contained in `origin/main` and holds unique research evidence.
- Parallel help: useful only for read-only archival review/cherry-pick planning, not for new implementation.

## Commands Used

```powershell
git -C F:/Dev/AIstock_worktrees/hmm-sector-regime-20260509 status --short --branch
git -C F:/Dev/AIstock_worktrees/hmm-sector-regime-20260509 log -1 --format="%h %cI %s"
git -C F:/Dev/AIstock_worktrees/hmm-sector-regime-20260509 rev-list --count origin/main..HEAD
git -C F:/Dev/AIstock_worktrees/hmm-sector-regime-20260509 diff --name-status origin/main...HEAD

git -C F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508 status --short --branch
git -C F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508 log -1 --format="%h %cI %s"
git -C F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508 rev-list --count origin/main..HEAD
git -C F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508 diff --name-status origin/main...HEAD
```

## Parallel Agent Notes

- Agent Bohr audited the HMM branch and recommended review/archive, with optional extraction of reusable forward-filter/test/doc pieces.
- Agent Nietzsche audited the financial-distress branch and recommended archive/review, not continued implementation.
- Main thread integrated their findings into this status report and did not modify either source branch.
