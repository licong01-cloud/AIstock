# QE Governance Validation Matrix

Date: 2026-05-09
Status: Phase 0 baseline matrix for integration-branch merge
Scope: QE candidate terminology, manual SOTA promotion review, StrategyPackage lifecycle, and Phase 4 seed-contract cross-module gate.
Related design: `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md`
Related hotfix matrix: `tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md`

## 1. Business Goal

QE governance must prevent an automatically good QE loop from silently becoming an approved SOTA asset or Paper-ready StrategyPackage. The governed path is:

```text
QE result -> QE candidate signal -> manual promotion review -> SOTA approved -> StrategyPackage lifecycle gate -> Paper-ready / future live-candidate gate
```

Phase 0 defines the terminology and validation matrix only. It does not change QE runtime, StrategyPackage runtime, frontend QE pages, production DB rows, protected assets, or production backend `8001`.

## 2. Standard Terms

| Standard term | Meaning | Must not be used as |
| --- | --- | --- |
| `QE candidate` | Automatic or manual candidate signal from a QE experiment/loop/task; research signal only. | A formal SOTA approval, a StrategyPackage, or a Paper-ready asset. |
| `SOTA approved` | Human-approved SOTA Hall state after review evidence is accepted. | An automatic QE best-loop marker or a DB row created without review. |
| `promotion review` | Auditable review record created by an explicit user action such as "Join SOTA Hall". | A direct Paper v2 enablement or implicit lifecycle mutation. |
| `StrategyPackage lifecycle` | Package-level status and selectability state governing frozen assets and downstream eligibility. | A UI label only, or a substitute for immutable manifest evidence. |
| `paper-ready StrategyPackage` | StrategyPackage that passed required SOTA/retest/asset/seed gates for Paper v2 selection. | Any raw QE experiment, loop, model catalog entry, or unreviewed SOTA candidate. |
| `live-candidate StrategyPackage` | Future stricter candidate for live/semi-live trading preparation. | A guarantee of live trading readiness or authorization. |
| `runtime variant` | A validated variation around a frozen package core, tracked with a variant hash. | A mutation of the frozen model/factor core or original manifest. |
| `protected asset` | Frozen model/factor/config/manifest artifact that must not be modified in place. | A temporary QE workspace artifact or dev scratch output. |

## 3. Terminology Alignment Scan Checklist

Run this checklist for Phase 0 and again before Phase 1/4 integration:

| Check id | Command | Pass condition | Risk boundary |
| --- | --- | --- | --- |
| TERM-L0-001 | `rg -n "auto.*SOTA|automatic.*SOTA|自动.*SOTA|自动加入.*SOTA|SOTA.*自动加入" docs tests backend frontend` | Hits are reviewed; user-facing wording must not imply automatic SOTA approval. | Scan only; do not mass-edit business UI in Phase 0. |
| TERM-L0-002 | `rg -n "candidate|候选|SOTA approved|promotion review|StrategyPackage lifecycle|paper-ready|live-candidate" docs tests backend frontend` | New/changed references use the standard terms from this matrix or document an intentional legacy context. | Documentation and changed files only unless Phase 1 owns UI/API edits. |
| TERM-L0-003 | `rg -n "qe_sota_registry|SOTA_APPROVED|REVIEW_PENDING|REVIEW_REJECTED|PAPER_CANDIDATE|PAPER_ENABLED|LIVE_CANDIDATE" backend frontend docs tests` | Lifecycle enum usage maps to review/package state, not raw QE ranking. | Static review; no DB writes. |
| TERM-L0-004 | `rg -n "StrategyPackage|strategy package|策略包|package lifecycle|lifecycle_status|promotion_status|paper_selectable" docs tests backend frontend` | StrategyPackage is the downstream standard asset object; Paper v2 does not select raw QE loops/models. | Avoid runtime edits outside the owning phase. |
| TERM-L0-005 | `git diff --name-only` plus manual file-owner review | Phase 0 changes stay in validation/docs scope and avoid `backend/services/quantevolver`, frontend QE pages, and runtime package code. | Prevents conflicts with Phase 1 / Phase 4 agents. |

## 4. L0 Static Governance Gate

| Gate | Command | Required evidence | Risk boundary |
| --- | --- | --- | --- |
| QE-GOV-L0-001 branch/worktree preflight | `git status --short --branch; git branch --show-current; git log --oneline -5` | Dedicated feature branch/worktree, not dirty `main`; base recorded. | Read-only Git inspection. |
| QE-GOV-L0-002 whitespace | `git diff --check` | No whitespace errors. | Static only. |
| QE-GOV-L0-003 guardrail scan | `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1` | No new P0/P1 guardrail findings for changed files. | Writes only tool evidence if the scanner does so; no business state. |
| QE-GOV-L0-004 terminology scan | Commands in section 3 | Findings classified as aligned, legacy-only, or owner-follow-up. | No broad UI/runtime edits in Phase 0. |
| QE-GOV-L0-005 production isolation | `rg -n "8001|Stop-Process|taskkill|kill|restart|reload|uvicorn" --glob '!node_modules/**' .` filtered to changed files | No command or script touches production `8001`. | Dev ports only for later runtime gates. |
| QE-GOV-L0-006 protected asset isolation | `git diff --name-only` and `git diff --stat` | No changes under `rdagent_assets`, model weights, HMM snapshots, frozen manifests, or production DB dumps. | Protected assets remain untouched. |
| QE-GOV-L0-007 DB write isolation | Manual review of changed files for SQL/DML/migrations/scripts | Phase 0 has no DB write path; any future DDL must be additive, commented, and non-public schema. | No production DB writes. |

## 5. L1 Contract And Enum Gate

| Gate | Suggested command | Expected assertion | Risk boundary |
| --- | --- | --- | --- |
| QE-GOV-L1-001 candidate is not approval | `python -m pytest backend/tests -q -k "sota and candidate"` | Automatic best/candidate markers never create `SOTA_APPROVED`. | Mocked or isolated test DB only. |
| QE-GOV-L1-002 review state enum | `python -m pytest backend/tests -q -k "promotion_review or review_status"` | Review states include pending/approved/rejected/audit metadata and reject unknown transitions. | No production DB. |
| QE-GOV-L1-003 package lifecycle enum | `python -m pytest backend/tests -q -k "strategy_package and lifecycle"` | Lifecycle/selectability states are explicit and do not rely on UI-only labels. | Package service tests only. |
| QE-GOV-L1-004 Phase 4 seed schema contract | `python -m pytest backend/tests -q -k "seed_contract or master_seed"` | Same manifest + master seed can be recorded and compared deterministically. | Synthetic/minimal artifacts until Phase 4 owns runtime tests. |
| QE-GOV-L1-005 legacy compatibility | `python -m pytest backend/tests -q -k "legacy and strategy_package"` | Legacy packages are classified as legacy/unset seed policy, not paper-ready by default. | Must not mutate existing records unless user-authorized migration phase. |

These L1 commands are placeholders until Phase 1/4 implementation lands. Before tests exist, the Phase 0 accepted evidence is a static scan plus a documented gap in the run record.

## 6. L2 API / Repository Gate

| Gate | Suggested command | Expected assertion | Risk boundary |
| --- | --- | --- | --- |
| QE-GOV-L2-001 create promotion review | `python -m pytest backend/tests -q -k "promotion_review and api"` | Explicit user action creates a `REVIEW_PENDING` review record and audit trail. | Test-owned DB/session only; no production rows. |
| QE-GOV-L2-002 reject direct Paper enablement | `python -m pytest backend/tests -q -k "paper_ready and promotion"` | Candidate/rejected review cannot become Paper-selectable. | No Paper v2 runtime run required. |
| QE-GOV-L2-003 additive schema safety | `rg -n "CREATE TABLE|ALTER TABLE|COMMENT ON" backend docs tests` plus migration tests | New governance tables live in `strategy_pkg` / `model_registry`, include comments, and avoid destructive DDL. | No writes to existing production schema during validation. |
| QE-GOV-L2-004 validation-run persistence | `python -m pytest backend/tests -q -k "package_validation_run"` | Retest/Mode A-F evidence is appended as independent validation runs. | Do not overwrite original QE metrics. |
| QE-GOV-L2-005 protected asset manifest metadata | `python -m pytest backend/tests -q -k "protected_asset or asset_manifest"` | Asset hash/size/source/protected flag is recorded and immutable after freeze. | Use temp artifacts only. |

## 7. L3 UI / Dev-Port Workflow Gate

| Gate | Suggested command | Expected assertion | Risk boundary |
| --- | --- | --- | --- |
| QE-GOV-L3-001 QE review entry UI | `python -m nox -s qe_governance_l3` or targeted Playwright after Phase 1 | QE experiment/loop shows explicit "Join SOTA Hall" action; click creates review pending, not approved SOTA. | Dev backend/frontend ports only, typically `8011`/`3011`; never production `8001`. |
| QE-GOV-L3-002 SOTA Hall review UI | Same L3 suite | Pending/rejected/approved views are distinguishable and audit metadata is visible. | UI verifies state from dev API, not mock-only when claiming business success. |
| QE-GOV-L3-003 StrategyPackage eligibility UI | Same L3 suite | Paper v2 package lists only eligible StrategyPackages, not raw QE loops or model catalog entries. | No Paper run execution required. |
| QE-GOV-L3-004 terminology UI smoke | Playwright text assertions or manual screenshot checklist | User-facing text distinguishes QE candidate, SOTA approved, promotion review, and paper-ready package. | Avoid unrelated UI restyling. |

## 8. L4 Cross-Module Business Gate

| Gate | Suggested command | Expected assertion | Risk boundary |
| --- | --- | --- | --- |
| QE-GOV-L4-001 candidate-to-package trace | Future integration suite: `python -m nox -s qe_governance_l4` | QE candidate -> review pending -> approved -> frozen StrategyPackage trace is complete and auditable. | Controlled dev/test IDs only (`pkg_dev_*`, `qe_dev_*`); no production rows. |
| QE-GOV-L4-002 Phase 4 strict seed reproducibility | Future Phase 4 suite | Same manifest + same `master_seed` produces NAV difference < 0.01bp and 100% identical holdings, or records nondeterministic flags. | This is the core Phase 4 gate; failures block integration. |
| QE-GOV-L4-003 Mode A-F validation evidence | Future integration suite | Retest modes write independent validation runs and do not overwrite source QE metrics. | Uses controlled assets and dev artifact paths. |
| QE-GOV-L4-004 Paper-ready selection boundary | Future integration suite | Paper v2 only selects approved, frozen, validated StrategyPackages. | Does not authorize live trading; no real broker/QMT path. |
| QE-GOV-L4-005 protected asset boundary | Future integration suite plus artifact hash check | Frozen assets are read-only; runtime variants do not mutate frozen core. | Dev protected path only: `rdagent_assets/strategy_package_runtime_dev/` or worktree-local assets. |

## 9. Phase-Specific Required Gates

| Phase | Minimum gates before integration branch | Merge-blocking business assertion |
| --- | --- | --- |
| Phase 0 terminology baseline | QE-GOV-L0-001 through QE-GOV-L0-007; terminology scan checklist; run-record template exists. | Documentation and test matrix consistently separate QE candidate, SOTA approved, promotion review, and StrategyPackage lifecycle. |
| Phase 1 manual SOTA flow | Phase 0 gates plus QE-GOV-L1-001/002/003, QE-GOV-L2-001/002, QE-GOV-L3-001/002. | QE automation cannot create approved SOTA; user action creates review pending with audit. |
| Phase 4 seed contract | Phase 0 gates plus QE-GOV-L1-004, QE-GOV-L4-002, and seed fragility evidence. | Same manifest + same master seed satisfies deterministic NAV/holding oracle or is explicitly quarantined/non-deterministic. |

## 10. Run Record Requirements

Each Phase 0/1/4 governance run must archive a record under `tests/aistock_validation/history/qe_governance/` with:

- Branch, commit, base commit, and worktree path.
- Documents read and key files inspected but intentionally not modified.
- Changed files and protected-file review.
- Exact commands, environment variables, and result summaries.
- Terminology scan findings, including legacy contexts and follow-up owners.
- Whether production `8001` was touched.
- Whether protected assets were touched.
- Whether any DB write happened and, if yes in later phases, which isolated DB/schema/ID namespace was used.
- Business validation result and residual risks.
