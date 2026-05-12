# Branch Audit Cleanup Plan - 2026-05-12

## Scope

- Repo: `F:/Dev/AIstock`.
- Audit worktree: `F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512`.
- Baseline: `origin/main` at `48b6bef` when this plan was finalized.
- Authorized phases: Phase A branch audit and Phase D status only at initial partial start; Phase B/C were later unlocked and delivered as separate docs in this branch.
- Safety: no branch delete/archive/merge was executed; no production DB, services, or ports were touched.
- Excluded from cleanup candidates: `origin/main` and synthetic remote HEAD aliases.

## Summary

- Remote non-main branches audited: 40.
- Branches with unique commits ahead of `origin/main`: 12.
- Branch tips already contained in `origin/main`: 28.
- MERGE: 0.
- CHERRY-PICK: 4.
- ARCHIVE: 4.
- DELETE: 27.
- REVIEW: 5.

## High-Level Recommendations

- Do not delete anything automatically; treat this document as a decision plan only.
- Safe deletion candidates are branches whose tip is already contained in `origin/main`; they still need owner approval before `git push origin --delete`.
- Cherry-pick candidates are mostly docs-only or single-purpose changes that may be useful without merging stale branch bases.
- Archive/review candidates preserve unique research or Claude-owned evidence where direct deletion risks losing context.
- HMM and event/financial-distress research branches should not be touched by Codex unless explicitly reassigned; current recommendation is preservation plus owner review.

## Branch Table

| Branch | Ahead | Last Commit | Class | Recommendation | Reason |
|---|---:|---|---|---|---|
| `origin/claude/paper-v2-baseline-post-r5-20260511` | 7 | 2026-05-12 2cf998b docs(cross-tool): 5-layer VERIFY Codex paper_v2 coldstart sentinel endpoint (9f31ac8) | docs-only | CHERRY-PICK | docs-only Paper v2 verify/quickstart evidence; selectively keep useful docs, R5 baseline itself is superseded by R6 |
| `origin/claude/paper-v2-baseline-post-r6-20260512` | 2 | 2026-05-12 c8f2d1a test(paper-v2): fix 4 enable_paper invariant fixtures for R6 governance gate | backend+tests+docs | CHERRY-PICK | current post-R6 baseline/fix evidence; cherry-pick docs only if not already represented on main |
| `origin/claude/paper-v2-branch-baseline-codex-qe-20260511` | 2 | 2026-05-11 60ee470 docs(cross-tool): FIX baseline caveats  stk_limit refresh BLOCKED + noxfile delegation | docs-only | ARCHIVE | older branch-baseline caveat docs; superseded by later R6 flow but useful for traceability |
| `origin/codex/factor-cache-wsl-path-policy-20260506` | 1 | 2026-05-06 5bdd86d fix(qe): allow WSL factor cache AIstock artifact paths | backend+tests+docs | CHERRY-PICK | single focused WSL artifact-path policy/test/doc change; review and cherry-pick if still needed |
| `origin/codex/factor-st-pit-metrics-20260506` | 1 | 2026-05-06 59f7405 feat(qe): align factor metrics cache with ST PIT universe | backend+scripts+db/migrations+tests+docs | REVIEW | backend plus migration/test change for ST PIT metrics; needs migration/currentness review |
| `origin/codex/financial-distress-rerank-20260508` | 30 | 2026-05-12 07b7caa feat(event): screen non q_ocf structured distress rules | backend+scripts+tests+docs | ARCHIVE | large research branch concluded no promotion candidate; preserve evidence, do not continue as integration without new hypothesis |
| `origin/codex/hmm-sector-regime-20260509` | 5 | 2026-05-10 ce8f099 docs(hmm): add regime qe handoff | backend+scripts+tests+docs | ARCHIVE | HMM R&D did not beat no-HMM in documented/scratch results; preserve conclusion and reusable fixes before cleanup |
| `origin/codex/qe-hmm-hotfix-handoff-20260508` | 1 | 2026-05-08 6d3ae04 docs(qe): add HMM hotfix multi-agent handoff | docs-only | CHERRY-PICK | single HMM/QE handoff doc may still be useful; avoid merging stale branch base |
| `origin/codex/validation-nav-cn-20260506` | 1 | 2026-05-06 fadecdc feat(validation): map navigation coverage to modules | backend+frontend+scripts+tests+docs | REVIEW | single but broad validation UI/catalog/nox change; validate against current Validation Center |
| `origin/codex/validation-real-port-ui-smoke` | 4 | 2026-05-06 b2b4391 test(validation): record full backend real-port smoke | backend+frontend+scripts+db/migrations+tests+docs | REVIEW | mixed validation/runtime contract branch; needs owner review |
| `origin/codex/validation-real-port-ui-smoke-clean` | 3 | 2026-05-06 f4cd57d test(validation): record clean PR branch smoke | backend+frontend+scripts+tests+docs | REVIEW | possible duplicate smoke branch but not identical; compare before archive/delete |
| `origin/codex/validation-smoke-merge-20260506_081641` | 3 | 2026-05-06 457b51b test(validation): record clean PR branch smoke | backend+frontend+scripts+tests+docs | REVIEW | similar to validation smoke clean branch but tree differs; compare before archive/delete |
| `origin/backup/pre-factor-eval-unify-20260417` | 0 | 2026-04-17 8e0dc6d chore: checkpoint quantevolver work before factor eval unification | no diff vs main | ARCHIVE | backup namespace and already contained; keep archival pointer or delete only after backup policy review |
| `origin/codex/event-signal-st-llm-design-20260506` | 0 | 2026-05-07 f9b4b3c test(event): record post-merge PDF smoke validation | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/factor-cache-taskdir-20260506` | 0 | 2026-05-06 ff52a32 fix(qe): support loader-style factors in cache backfill | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/factor-oos-deferred-20260507` | 0 | 2026-05-07 217b497 docs(factor): record deferred OOS as-of selection issue | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/merge-unified-event-20260506` | 0 | 2026-05-06 f6f92d3 test(event): record integration merge validation | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/paper-v2-selection-qe-inference-20260506` | 0 | 2026-05-06 aea8ae8 test(selection): align WSL runtime cache policy after merge | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/prod-reconcile-20260506_085409` | 0 | 2026-05-06 87c5593 docs(prod-sync): record remaining excluded artifacts | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-factor-cache-streaming-20260506` | 0 | 2026-05-06 2c64f07 fix(qe): stream factor cache node sync | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-governance-prod-readonly-preflight-20260509` | 0 | 2026-05-09 83a569f test(qe): add production readonly governance preflight | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-governance-review-fixes-20260509` | 0 | 2026-05-09 c1308d7 fix(qe): address governance review blockers | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-hmm-hotfix-integration-20260508` | 0 | 2026-05-09 bceeaf7 merge(main): sync qe hmm hotfix with latest main | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-phase-0-terminology-20260509` | 0 | 2026-05-09 74bbe6f test(qe): add governance validation matrix | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-phase-1-manual-sota-flow-20260509` | 0 | 2026-05-09 a984a45 feat(qe): add manual SOTA promotion review gate | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-phase-2-asset-ledger-20260509` | 0 | 2026-05-09 a62fe15 feat(qe): add strategy package asset ledger | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-phase-3-paper-retest-gate-20260509` | 0 | 2026-05-09 8a19b49 fix(qe): require original retest before paper enable | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-phase-4-seed-contract-20260509` | 0 | 2026-05-09 0d57a19 feat(qe): add master seed contract foundation | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-phase-5-model-library-20260509` | 0 | 2026-05-09 0cdaa60 feat(qe): add model registry foundation | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-phase-5-model-registry-bridge-20260509` | 0 | 2026-05-09 21ef17e feat(qe): add model registry bridge read api | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-phase-5-model-registry-migration-20260509` | 0 | 2026-05-09 b47bcf0 feat(qe): add model registry migration smoke | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-phase-6-1-integration-fixes-20260509` | 0 | 2026-05-09 e31732a fix(qe): address governance integration review gaps | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-phase-6-runtime-variants-20260509` | 0 | 2026-05-09 63bcc31 feat(qe): add strategy package runtime variants | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-phase-7-stability-scoring-20260509` | 0 | 2026-05-09 f593498 feat(qe): add validation stability scoring | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-phase-7-validation-modes-20260509` | 0 | 2026-05-09 f95d31c feat(qe): add strategy package validation runs | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/qe-recorder-binding-20260506` | 0 | 2026-05-06 1503dd8 docs(qe): record parallel recorder binding smoke | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/selection-st-pit-health-20260506` | 0 | 2026-05-07 7602166 fix(paper-v2): preflight HMM selection health | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/unified-event-signal-backfill-20260506` | 0 | 2026-05-06 5f6b363 fix(event): clean package init whitespace | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/unified-merge-20260506_081257` | 0 | 2026-05-06 453ae87 fix(strategy-package): resolve legacy cached model assets | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |
| `origin/codex/validation-ui-targets-20260507` | 0 | 2026-05-07 10fc249 feat(validation): add ui target coverage catalog | no diff vs main | DELETE | tip is already contained in origin/main; no unique commits after R6/main |

## Delete Candidates

- `origin/codex/event-signal-st-llm-design-20260506`
- `origin/codex/factor-cache-taskdir-20260506`
- `origin/codex/factor-oos-deferred-20260507`
- `origin/codex/merge-unified-event-20260506`
- `origin/codex/paper-v2-selection-qe-inference-20260506`
- `origin/codex/prod-reconcile-20260506_085409`
- `origin/codex/qe-factor-cache-streaming-20260506`
- `origin/codex/qe-governance-prod-readonly-preflight-20260509`
- `origin/codex/qe-governance-review-fixes-20260509`
- `origin/codex/qe-hmm-hotfix-integration-20260508`
- `origin/codex/qe-phase-0-terminology-20260509`
- `origin/codex/qe-phase-1-manual-sota-flow-20260509`
- `origin/codex/qe-phase-2-asset-ledger-20260509`
- `origin/codex/qe-phase-3-paper-retest-gate-20260509`
- `origin/codex/qe-phase-4-seed-contract-20260509`
- `origin/codex/qe-phase-5-model-library-20260509`
- `origin/codex/qe-phase-5-model-registry-bridge-20260509`
- `origin/codex/qe-phase-5-model-registry-migration-20260509`
- `origin/codex/qe-phase-6-1-integration-fixes-20260509`
- `origin/codex/qe-phase-6-runtime-variants-20260509`
- `origin/codex/qe-phase-7-stability-scoring-20260509`
- `origin/codex/qe-phase-7-validation-modes-20260509`
- `origin/codex/qe-recorder-binding-20260506`
- `origin/codex/selection-st-pit-health-20260506`
- `origin/codex/unified-event-signal-backfill-20260506`
- `origin/codex/unified-merge-20260506_081257`
- `origin/codex/validation-ui-targets-20260507`

## Cherry-Pick Candidates

- `origin/claude/paper-v2-baseline-post-r5-20260511`: docs-only Paper v2 verify/quickstart evidence; selectively keep useful docs, R5 baseline itself is superseded by R6.
- `origin/claude/paper-v2-baseline-post-r6-20260512`: current post-R6 baseline/fix evidence; cherry-pick docs only if not already represented on main.
- `origin/codex/factor-cache-wsl-path-policy-20260506`: single focused WSL artifact-path policy/test/doc change; review and cherry-pick if still needed.
- `origin/codex/qe-hmm-hotfix-handoff-20260508`: single HMM/QE handoff doc may still be useful; avoid merging stale branch base.

## Archive / Review Risks

- `origin/backup/pre-factor-eval-unify-20260417` (ARCHIVE): backup namespace and already contained; keep archival pointer or delete only after backup policy review.
- `origin/claude/paper-v2-branch-baseline-codex-qe-20260511` (ARCHIVE): older branch-baseline caveat docs; superseded by later R6 flow but useful for traceability.
- `origin/codex/factor-st-pit-metrics-20260506` (REVIEW): backend plus migration/test change for ST PIT metrics; needs migration/currentness review.
- `origin/codex/financial-distress-rerank-20260508` (ARCHIVE): large research branch concluded no promotion candidate; preserve evidence, do not continue as integration without new hypothesis.
- `origin/codex/hmm-sector-regime-20260509` (ARCHIVE): HMM R&D did not beat no-HMM in documented/scratch results; preserve conclusion and reusable fixes before cleanup.
- `origin/codex/validation-nav-cn-20260506` (REVIEW): single but broad validation UI/catalog/nox change; validate against current Validation Center.
- `origin/codex/validation-real-port-ui-smoke` (REVIEW): mixed validation/runtime contract branch; needs owner review.
- `origin/codex/validation-real-port-ui-smoke-clean` (REVIEW): possible duplicate smoke branch but not identical; compare before archive/delete.
- `origin/codex/validation-smoke-merge-20260506_081641` (REVIEW): similar to validation smoke clean branch but tree differs; compare before archive/delete.

## Commands Used

```powershell
git -C F:/Dev/AIstock fetch origin --prune
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 branch -r --format=%(refname:short)
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 rev-list --count origin/main..<branch>
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 merge-base --is-ancestor <branch> origin/main
git -C F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512 diff --name-only origin/main...<branch>
```

## Notes From Parallel Agents

- Agent Peirce: reviewed governance/QE/Paper/validation branches; confirmed most QE phase branches are contained in main and non-contained validation branches need owner review.
- Agent Cicero: reviewed HMM/factor/event/financial-distress branches; flagged `financial-distress-rerank` and `hmm-sector-regime` as unique research branches requiring preservation/review.
- Main thread: generated remote branch inventory and integrated recommendations without modifying or deleting any source branch.
