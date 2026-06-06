# Research Assistant Phase 1 Memory Tree Validation

- batch_id: `ra_phase1`
- branch: `codex/ra-memory-tree-20260601`
- base_branch_after_rebase: `main` at Phase 0 PR #448 merge commit `4eeaf2bc8978de575f29e7e2b7f8394c266b724d`.
- validation_plan: `ra_phase1_memory_tree`
- runtime_boundary: no production `8001` / `3000` start, stop, restart, or smoke was performed.
- production_ddl_gate: `pending_before_merge`; Phase 1 adds `backend/db/migrations/ra_upgrade/001_memory_tree.sql` and schema/bootstrap changes. Production DB was not touched.
- validation_db_gate: `applied_and_verified_in_ephemeral_dev_schema`; migration ran twice in `aistock_dev` temporary schema and the schema was dropped after verification.

## G1-local Evidence

| Gate | Evidence | Status |
| --- | --- | --- |
| Target nox | `nox -s ra_phase1_memory_tree` | pass: 14 targeted tests, catalog integrity 0 findings, ownership scan mapped=16/16 ambiguous=0 |
| RA backend regression | `python -m pytest backend/tests/research_assistant -q -p no:cacheprovider` | pass: 101 tests |
| DDL idempotency | `001_memory_tree.sql` executed twice in an isolated `aistock_dev` schema seeded with the pre-Phase1 `research_memory_items` shape | pass |
| Anti-drift grep | `rg -n "embedding|vector|semantic_search" backend/services/research_assistant/memory_tree.py backend/services/research_assistant/memory_curator.py backend/tests/research_assistant/test_core_no_adapter_import.py` | pass: no matches |
| Production isolation | validation catalog summary reported `production_8001_touched=false`, `production_db_touched=false` | pass |

## DDL Verification Summary

Verified new columns: `tree_path`, `parent_key`, `node_type`, `scope`, `importance`, `last_used_at`, `use_count`, `auto_created`, `trust_level`, `provenance_json`, `resident`.

Verified indexes: `idx_rmi_tree`, `idx_rmi_parent`, `idx_rmi_resident`.

Verified constraints: `ck_rmi_type`, `ck_rmi_node_type`, `ck_rmi_scope`, `ck_rmi_importance`, `ck_rmi_trust_level`, `ck_rmi_use_count`.

Verified comments on every new column.

Verified backfill for a legacy row: `scope='project'`, `tree_path='project.rule.old'`, `node_type='fact'`, `importance=0.5`, `use_count=0`, `auto_created=false`, `trust_level='user_stated'`, `provenance_json={}`, `resident=false`.

## DESIGN-COMPLIANCE-001 Closure Requirements

| ID | Requirement | Evidence | Done |
| --- | --- | --- | --- |
| CR-P1-01 | True tree/governance DDL is idempotent; COMMENT coverage complete; `MEMORY_TYPES` expanded; legacy rows backfilled | `001_memory_tree.sql`; `init_research_assistant_schema_20260521.py`; `test_memory_tree_ddl_contract.py`; dev-schema double-run evidence | true |
| CR-P1-02 | `memory_tree.py` implements tree retrieval without RAG and `build_context_pack` consumes it instead of flat type retrieval | `memory_tree.py`; `service.py`; `test_memory_tree_retrieval.py`; `test_memory_scoring.py` | true |
| CR-P1-03 | `memory_curator.py` auto-grows branches, deduplicates, records trust/provenance, and enforces approval boundary | `memory_curator.py`; `service.py`; `test_memory_autogrow.py`; `test_memory_dedup_scope.py` | true |
| CR-P1-04 | Core modules use provider protocols and do not import AIstock facade/DB/domain services | `test_core_no_adapter_import.py`; no forbidden import/static retrieval-token matches | true |
| CR-P1-05 | Validation plan is runner-enabled and allowlisted | `test_plans.yaml`; `plan_catalog.py`; `noxfile.py`; `test_memory_tree_ddl_contract.py` | true |
| CR-P1-06 | G3 traceability matrix anchors are updated | blueprint §12 Phase 1 anchor records G1-central run_id `research-assistant-memory-tree_20260601_013143_l1_ra-phase1-memory-tree_df500446_runner-validation__9756376ad7` plus original/rebased commits | true |

## G1-central Evidence

- G1-central run_id: `research-assistant-memory-tree_20260601_013143_l1_ra-phase1-memory-tree_df500446_runner-validation__9756376ad7`
- G1-central status: passed; `return_code=0`; canonical runner accepted `ra_phase1_memory_tree`.
- validated_commit: `df500446` (commit encoded in the canonical run_id).
- implementation_commit_original: `d12f9aaca07960a577e055d46df47cc46d09aeca`.
- determinism_fix_commit_original: `b4c021345dd187f964bedfcfdf77ddeba1481d02`.
- implementation_commit_rebased_before_merge: `1d2bafbd`.
- determinism_fix_commit_rebased_before_merge: `47affe04`.
- production_ddl_gate: `pending_before_merge`; production runtime touched: false; production DB touched: false.

## 2026-06-01 Central Runner Determinism Fix

Canonical run `research-assistant-memory-tree_20260601_011622_l1_..._d4e4391667` exposed a non-deterministic test failure in `test_chat_turn_triggers_memory_curator_after_assistant_reply`: curator branch nodes reused the leaf `memory_type` (`user_preference`), so raw `list_records(filters={memory_type})` could return either the auto-created branch or fact first depending on repository ordering. The fix makes curator branch nodes use `memory_type='core'` while preserving leaf `memory_type` only on `node_type='fact'` rows. Local revalidation after the fix:

- `python -m pytest backend/tests/research_assistant/test_memory_autogrow.py backend/tests/research_assistant/test_memory_dedup_scope.py backend/tests/research_assistant/test_memory_tree_retrieval.py -q -p no:cacheprovider`: 6 passed
- `nox -s ra_phase1_memory_tree`: 14 targeted tests passed; catalog integrity 0 findings; ownership scan mapped=18/18 ambiguous=0
- `python -m pytest backend/tests/research_assistant -q -p no:cacheprovider`: 101 passed
