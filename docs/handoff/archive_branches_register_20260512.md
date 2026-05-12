# Archive Branches Register - 2026-05-12

## Scope

- Dispatch source: cross-tool drawer `e54762fe60be80480a470cf9`, Task 14.
- Worktree: `F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512`.
- Branch: `codex/qe-cleanup-and-pr005-prep-20260512`.
- Baseline after `git fetch --prune origin`: `origin/main` = `da648066473b` (`docs(qe): add branch review decisions`).
- Inventory covers all remote refs under `refs/remotes/origin/archive` after fetch/prune: 8 refs.
- Source branches are inferred from archive ref naming and prior handoff docs; all inferred source remote refs are absent after prune.

## Register

| Archive ref | Inferred source branch | Head SHA / subject | Why archived | Retention / review owner category | 30-day review |
|---|---|---|---|---|---|
| `origin/archive/backup-pre-factor-eval-unify-20260417-20260512` | `origin/backup/pre-factor-eval-unify-20260417` | `8e0dc6d4e4d` - `chore: checkpoint quantevolver work before factor eval unification` | Backup checkpoint before factor-evaluation unification. Tip is already contained in `origin/main`, so the archive keeps provenance while avoiding an active backup branch. | QE/factor platform owner plus repo hygiene owner; review against backup retention policy. | Target `2026-06-11`; review window `2026-06-10` to `2026-06-14` BJ. |
| `origin/archive/claude-paper-v2-baseline-post-r5-20260511-20260512` | `origin/claude/paper-v2-baseline-post-r5-20260511` | `2cf998bf90d` - `docs(cross-tool): 5-layer VERIFY Codex paper_v2 coldstart sentinel endpoint (9f31ac8)` | R5 baseline / verification evidence branch. Superseded by R6 and cleanup flow; most commits are patch-equivalent to main, with one unique historical R5 baseline doc retained for traceability. | Paper v2 validation / baseline owner; decide whether any unique doc should be promoted before archive deletion. | Target `2026-06-11`; review window `2026-06-10` to `2026-06-14` BJ. |
| `origin/archive/claude-paper-v2-baseline-post-r6-20260512-20260512` | `origin/claude/paper-v2-baseline-post-r6-20260512` | `c8f2d1a8fec` - `test(paper-v2): fix 4 enable_paper invariant fixtures for R6 governance gate` | Post-R6 baseline/fix evidence branch. `git cherry` shows both commits patch-equivalent to current `origin/main`; archive preserves the original verification lane. | Paper v2 baseline plus governance-gate test owner; confirm main retains equivalent baseline/fix evidence. | Target `2026-06-11`; review window `2026-06-10` to `2026-06-14` BJ. |
| `origin/archive/claude-paper-v2-branch-baseline-codex-qe-20260511-20260512` | `origin/claude/paper-v2-branch-baseline-codex-qe-20260511` | `60ee470acb7` - `docs(cross-tool): FIX baseline caveats - stk_limit refresh BLOCKED + noxfile delegation` | Older codex/qe branch-baseline caveat lane. Superseded by later R6 flow, but one caveat doc remains unique and useful for historical review. | Paper v2 baseline / strategy owner; decide whether the unique caveat doc is still needed. | Target `2026-06-11`; review window `2026-06-10` to `2026-06-14` BJ. |
| `origin/archive/codex-financial-distress-rerank-20260508-20260512` | `origin/codex/financial-distress-rerank-20260508` | `07b7caaa45d` - `feat(event): screen non q_ocf structured distress rules` | Large event-signal research branch. Prior status says the current hypothesis is complete and found no true-QE promotion candidate; archive preserves evidence instead of continuing as integration work. | Event-signal / QE research owner; only resume with a new hypothesis or explicit extraction request. | Target `2026-06-11`; review window `2026-06-10` to `2026-06-14` BJ. |
| `origin/archive/codex-hmm-sector-regime-20260509-20260512` | `origin/codex/hmm-sector-regime-20260509` | `ce8f099df3c` - `docs(hmm): add regime qe handoff` | HMM R&D branch. Prior status says no-HMM still wins in documented and scratch results; archive preserves the conclusion and possible reusable fixes. | HMM / QE research owner; do not continue coefficient-sweep tuning as-is. | Target `2026-06-11`; review window `2026-06-10` to `2026-06-14` BJ. |
| `origin/archive/codex-qe-hmm-hotfix-handoff-20260508-20260512` | `origin/codex/qe-hmm-hotfix-handoff-20260508` | `6d3ae046715` - `docs(qe): add HMM hotfix multi-agent handoff` | Single docs-only HMM/QE handoff branch. Archived to preserve the handoff without merging a stale branch base. | HMM / QE governance owner; check whether the handoff doc is represented on main or should remain archive-only. | Target `2026-06-11`; review window `2026-06-10` to `2026-06-14` BJ. |
| `origin/archive/codex-validation-real-port-ui-smoke-20260512` | `origin/codex/validation-real-port-ui-smoke` | `b2b4391208e` - `test(validation): record full backend real-port smoke` | Mixed validation/runtime branch. Task 13 found three patch-equivalent commits and one non-equivalent Paper v2 runtime contract commit (`15b7d81`) with heavy current-main conflict risk; archive keeps owner traceability. | Validation Center owner plus Paper v2 runtime owner; extract only a narrow modern patch if explicitly requested. | Target `2026-06-11`; review window `2026-06-10` to `2026-06-14` BJ. |

## 30-Day Review SOP

1. On or before `2026-06-11` BJ, run `git fetch --prune origin` and regenerate the `origin/archive/*` inventory.
2. Re-check each archive ref against current `origin/main` with `git rev-list --left-right --count`, `git merge-base --is-ancestor`, and `git cherry -v`.
3. Confirm the inferred source branch still does not exist, then ask the listed owner category for one of: keep archive, promote a narrow doc/patch, convert to longer-term tag, or delete the archive ref.
4. For unique research/evidence branches, require owner sign-off before deletion even when no merge is planned.
5. Record the decision in a dated handoff doc and, if cross-tool coordination is still active, publish a tagged `[INFO]` drawer summary.

## Explicit Non-Actions / Safety

- No archive ref was created, deleted, renamed, pushed, merged, cherry-picked, or force-updated by this task.
- `main` was not checked out or modified.
- No source branch or Claude worktree was touched.
- No production backend `8001`, frontend `3000`, dev port, production DB, dev DB, Paper v2 daemon, live broker, or scheduler was touched.
- This task edited only `docs/handoff/archive_branches_register_20260512.md`.

## Evidence Used

### Commands

```powershell
git fetch --prune origin
git for-each-ref refs/remotes/origin/archive --format='%(refname:short)|%(objectname:short)|%(objectname)|%(committerdate:iso8601)|%(subject)'
git rev-parse --short=12 origin/main
git log -1 --format='%H|%ci|%s' origin/main
git rev-list --left-right --count origin/main...<archive-ref>
git merge-base --is-ancestor <archive-ref> origin/main
git cherry -v origin/main <archive-ref>
git show-ref --verify --quiet refs/remotes/<inferred-source-ref>
```

### Archive Inventory After Fetch/Prune

```text
origin/archive/backup-pre-factor-eval-unify-20260417-20260512|8e0dc6d|8e0dc6d4e4d66caaf82ff173a36d5f247cba98a8|chore: checkpoint quantevolver work before factor eval unification
origin/archive/claude-paper-v2-baseline-post-r5-20260511-20260512|2cf998b|2cf998bf90d9a764250c8b2b199958ea080be53d|docs(cross-tool): 5-layer VERIFY Codex paper_v2 coldstart sentinel endpoint (9f31ac8)
origin/archive/claude-paper-v2-baseline-post-r6-20260512-20260512|c8f2d1a|c8f2d1a8fec4ecff50fc8f2220ae5eac9e42530f|test(paper-v2): fix 4 enable_paper invariant fixtures for R6 governance gate
origin/archive/claude-paper-v2-branch-baseline-codex-qe-20260511-20260512|60ee470|60ee470acb77ba9d6161be4149d60210f382f4b2|docs(cross-tool): FIX baseline caveats - stk_limit refresh BLOCKED + noxfile delegation
origin/archive/codex-financial-distress-rerank-20260508-20260512|07b7caa|07b7caaa45dc49c78b155cb469abc58d7a07fc85|feat(event): screen non q_ocf structured distress rules
origin/archive/codex-hmm-sector-regime-20260509-20260512|ce8f099|ce8f099df3c5e971fbb4bfd3f07c7c2ce0c7116d|docs(hmm): add regime qe handoff
origin/archive/codex-qe-hmm-hotfix-handoff-20260508-20260512|6d3ae04|6d3ae046715d9aef227bded150c521e13ce0677c|docs(qe): add HMM hotfix multi-agent handoff
origin/archive/codex-validation-real-port-ui-smoke-20260512|b2b4391|b2b4391208eda87a28109bb67b019e316c6e95e5|test(validation): record full backend real-port smoke
```

### Containment / Patch Evidence Versus `origin/main=da648066473b`

| Archive ref | Rev-list `main...archive` | Merge-base containment | `git cherry -v` summary |
|---|---:|---|---|
| `origin/archive/backup-pre-factor-eval-unify-20260417-20260512` | `559 0` | Contained in main | No unique patch output. |
| `origin/archive/claude-paper-v2-baseline-post-r5-20260511-20260512` | `94 7` | Not contained | 6 patch-equivalent commits, 1 unique historical R5 baseline doc (`779e904`). |
| `origin/archive/claude-paper-v2-baseline-post-r6-20260512-20260512` | `7 2` | Not contained | 2 patch-equivalent commits (`372d0f3`, `c8f2d1a`). |
| `origin/archive/claude-paper-v2-branch-baseline-codex-qe-20260511-20260512` | `137 2` | Not contained | 1 patch-equivalent baseline doc, 1 unique caveat doc (`60ee470`). |
| `origin/archive/codex-financial-distress-rerank-20260508-20260512` | `236 30` | Not contained | 30 unique research commits. |
| `origin/archive/codex-hmm-sector-regime-20260509-20260512` | `206 5` | Not contained | 5 unique HMM research commits. |
| `origin/archive/codex-qe-hmm-hotfix-handoff-20260508-20260512` | `232 1` | Not contained | 1 unique docs-only HMM/QE handoff commit. |
| `origin/archive/codex-validation-real-port-ui-smoke-20260512` | `304 4` | Not contained | 3 patch-equivalent commits, 1 unique runtime-contract commit (`15b7d81`). |

### Source-Ref Existence Check

All inferred source remote refs were absent after fetch/prune:

```text
origin/backup/pre-factor-eval-unify-20260417
origin/claude/paper-v2-baseline-post-r5-20260511
origin/claude/paper-v2-baseline-post-r6-20260512
origin/claude/paper-v2-branch-baseline-codex-qe-20260511
origin/codex/financial-distress-rerank-20260508
origin/codex/hmm-sector-regime-20260509
origin/codex/qe-hmm-hotfix-handoff-20260508
origin/codex/validation-real-port-ui-smoke
```

### Supporting Handoff Docs Read

- `docs/handoff/branch_audit_cleanup_plan_20260512.md`
- `docs/handoff/branch_review_decisions_20260512.md`
- `docs/handoff/codex_self_driven_branches_status_20260512.md`
