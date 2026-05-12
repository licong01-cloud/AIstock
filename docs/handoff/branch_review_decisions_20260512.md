# Branch Review Decisions - 2026-05-12

## Scope

- Dispatch: Task 13 from cross-tool channel, 2026-05-12 10:10 BJ.
- Output branch: `codex/qe-cleanup-and-pr005-prep-20260512`.
- Output file: `docs/handoff/branch_review_decisions_20260512.md`.
- Baseline refreshed before this review: `origin/main` = `73eed1b` (`docs(qe): add HMM hotfix multi-agent handoff`).
- Cleanup branch head at review start: `a1cfe19` (`docs(qe): add cleanup audit and miniqmt sim prep`).
- Mode: read-only branch review plus this handoff doc only; no source branch deletion, archive, merge, cherry-pick, service start, port use, or DB access was performed.

## Executive Decision

Current `origin/main` already contains patch-equivalent content for five of the six review-lane branches. The only branch with a non-patch-equivalent remaining commit is the mixed historical `validation-real-port-ui-smoke` branch, and that non-equivalent commit touches Paper v2 / selection / strategy runtime paths with conflicts against current main. Therefore:

| Decision | Branches | Rationale |
|---|---:|---|
| MERGE | 0 | Direct merges are stale and conflict-prone; no branch is suitable for merge as-is. |
| CHERRY-PICK | 0 | Useful patches are already on current `origin/main`; cherry-pick would duplicate or conflict. |
| ARCHIVE | 1 | Preserve the one mixed branch with non-equivalent historical runtime work for owner traceability. |
| DELETE | 5 | Patch-equivalent duplicates already represented on `origin/main`; delete only after branch-owner cleanup approval. |

## Dispatch Count Reconciliation

The Phase A audit doc counted exactly five rows with class `REVIEW`. Task 13 asks for six `REVIEW` branches. This review treats the sixth lane as `origin/codex/factor-cache-wsl-path-policy-20260506`, because Phase A marked it `CHERRY-PICK` but also said "review and cherry-pick if still needed." After refreshing to `origin/main=73eed1b`, it no longer needs cherry-pick because the patch is already represented on main.

`origin/codex/validation-ui-targets-20260507` is not part of the six active review-lane branches after `git fetch origin --prune`: the remote ref is absent. Its commit `10fc249` is already contained in `origin/main`, so the only remaining action is "already deleted / no action" in the historical table.

## Per-Branch Recommendations

| Branch | Review-Lane Reason | Current Evidence | Recommendation | Preconditions Before Acting |
|---|---|---|---|---|
| `origin/codex/factor-cache-wsl-path-policy-20260506` | Phase A `CHERRY-PICK` candidate requiring review | `git cherry -v origin/main <branch>` marks `- 5bdd86d`; equivalent main commit is `e56acb3` with the same subject. `merge-tree` is clean, but no content remains to apply. | DELETE | Delete only after branch-owner approval; optional archive tag if provenance retention is required. |
| `origin/codex/factor-st-pit-metrics-20260506` | Phase A `REVIEW` | `git cherry` marks `- 59f7405`; equivalent main commit is `26905f3`. Direct merge reports conflicts in `backend/services/quantevolver/factor_official_evaluation_service.py`, `progress.md`, and `scripts/backfill_factor_cache.py`. | DELETE | Delete only after confirming owner accepts patch-equivalent main coverage; do not merge stale branch. |
| `origin/codex/validation-nav-cn-20260506` | Phase A `REVIEW` | `git cherry` marks `- fadecdc`; equivalent main commit is `acadcbd`. Direct merge conflicts in validation center page/tests, `nav-groups.ts`, `noxfile.py`, and `module_registry.yaml`. | DELETE | Delete only after branch-owner approval; do not cherry-pick or merge. |
| `origin/codex/validation-real-port-ui-smoke` | Phase A `REVIEW` | Four branch commits: three patch-equivalent to main (`2d00052`, `5da9bad`, `b2b4391`) and one non-equivalent (`15b7d81 fix(paper-v2): enforce QE runtime contract`). The non-equivalent commit touches Paper v2 / selection / strategy runtime files and conflicts heavily with current main. | ARCHIVE | Archive for historical traceability; do not merge/cherry-pick unless Paper v2 owner explicitly extracts a narrow modern patch. |
| `origin/codex/validation-real-port-ui-smoke-clean` | Phase A `REVIEW` | All three commits are patch-equivalent to main (`39a63f3`, `085dac9`, `f4cd57d`). It has the same stable diff patch-id as `validation-smoke-merge-20260506_081641`. | DELETE | Delete only after owner approval; archive first only if old smoke branch names must be retained. |
| `origin/codex/validation-smoke-merge-20260506_081641` | Phase A `REVIEW` | All three commits are patch-equivalent to main (`8430d02`, `5878f68`, `457b51b`). Functionally duplicates `validation-real-port-ui-smoke-clean`; extra old-history differences compare as already-main content. | DELETE | Delete after owner approval; no cherry-pick/merge needed. |

## Important Non-Actions

- No source branch was deleted or archived.
- No commit was cherry-picked or merged into `main`.
- No Claude Code worktree was modified.
- No production backend `8001`, frontend `3000`, Paper v2 daemon, live broker, production DB, or dev DB was touched.
- HMM and event-driven signal research branches remain outside this Codex task scope.

## Evidence Summary

### Patch-Equivalence Checks

```text
git cherry -v origin/main origin/codex/factor-cache-wsl-path-policy-20260506
- 5bdd86d fix(qe): allow WSL factor cache AIstock artifact paths

git cherry -v origin/main origin/codex/factor-st-pit-metrics-20260506
- 59f7405 feat(qe): align factor metrics cache with ST PIT universe

git cherry -v origin/main origin/codex/validation-nav-cn-20260506
- fadecdc feat(validation): map navigation coverage to modules

git cherry -v origin/main origin/codex/validation-real-port-ui-smoke
- 2d00052 feat(validation): add real-port UI smoke
- 5da9bad feat(data): track announcement first seen time
+ 15b7d81 fix(paper-v2): enforce QE runtime contract
- b2b4391 test(validation): record full backend real-port smoke

git cherry -v origin/main origin/codex/validation-real-port-ui-smoke-clean
- 39a63f3 feat(validation): add real-port UI smoke
- 085dac9 test(validation): record full backend real-port smoke
- f4cd57d test(validation): record clean PR branch smoke

git cherry -v origin/main origin/codex/validation-smoke-merge-20260506_081641
- 8430d02 feat(validation): add real-port UI smoke
- 5878f68 test(validation): record full backend real-port smoke
- 457b51b test(validation): record clean PR branch smoke
```

### Main Related Commits

```text
e56acb3 fix(qe): allow WSL factor cache AIstock artifact paths
26905f3 feat(qe): align factor metrics cache with ST PIT universe
acadcbd feat(validation): map navigation coverage to modules
405adcb feat(validation): add real-port UI smoke
039a552 test(validation): record full backend real-port smoke
eaee76e test(validation): record clean PR branch smoke
6ee060e feat(data): track announcement first seen time
760ded2 fix(paper-v2): enforce QE runtime contract
10fc249 feat(validation): add ui target coverage catalog
```

All entries above are patch-equivalent to their reviewed branch counterpart except `760ded2`, which is a same-subject main commit related to `15b7d81` but not a stable patch-id equivalent. That is why `origin/codex/validation-real-port-ui-smoke` remains an `ARCHIVE` recommendation instead of `DELETE`.

### Conflict Checks

`git merge-tree --write-tree origin/main <branch>` showed:

- `factor-cache-wsl-path-policy`: clean tree result, but patch already on main.
- `factor-st-pit-metrics`: conflicts in `factor_official_evaluation_service.py`, `progress.md`, and `scripts/backfill_factor_cache.py`.
- `validation-nav-cn`: conflicts in Validation Center page/test/catalog files.
- `validation-real-port-ui-smoke`: conflicts across Paper v2 runtime, strategy package runtime, validation tests, `noxfile.py`, and evidence docs.
- `validation-real-port-ui-smoke-clean`: conflicts in validation real-port test, `noxfile.py`, and validation catalog/module docs.
- `validation-smoke-merge-20260506_081641`: same conflict shape as the clean branch.

## Follow-Up Queue

1. Strategy/branch owner can delete the five duplicate remote refs after approving cleanup.
2. Strategy should archive `origin/codex/validation-real-port-ui-smoke` or ask Paper v2 owner whether `15b7d81` has any historical diff worth extracting.
3. No Codex implementation work is required from Task 13 unless strategy asks for an actual branch deletion/archive operation.

## Commands Used

```powershell
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 fetch origin --prune
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 status --short --branch
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 branch -r --format='%(refname:short)'
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 rev-list --left-right --count origin/main...<branch>
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 log --oneline --right-only --cherry-mark origin/main...<branch>
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 cherry -v origin/main <branch>
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 diff --name-status origin/main...<branch>
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 merge-tree --write-tree --name-only origin/main <branch>
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 log --all --oneline --grep='<subject>'
```
