# Research Assistant Phase 8 closure progress

- branch：`codex/ra-code-intel-20260602`
- final_head_commit：`5c4c6320c07d8fb8073f461f1eb78db8ab4c5a3c`
- g1_central_run_id：`research-assistant-code-intelligence_20260602_163726_l4_ra-phase8-code-intel_e5458fe3_runner-validation__c437b541de`
- production_ddl_gate：`required_pending_user_approval`

| ID | 状态 | 证据 |
|---|---|---|
| CR-P8-01 | done | worktree `F:/Dev/AIstock_worktrees/ra-code-intel-20260602` from `origin/main` |
| CR-P8-02 | done | `test_plans.yaml`, `noxfile.py`, `plan_catalog.py` |
| CR-P8-03 | done | `code_intelligence_core.py`, `test_core_no_adapter_import.py` |
| CR-P8-04 | done | `code_intelligence_adapter_provider.py`, `test_code_intel_true_reuse.py` |
| CR-P8-05 | done | `scripts/research_assistant_phase8_code_intel_guard.py` |
| CR-P8-06 | done | `test_code_intel_determinism.py` |
| CR-P8-07 | done | `test_code_intel_evidence_contract.py` |
| CR-P8-08 | done | `004_code_context_refs.sql`, `test_code_context_refs_ddl_contract.py` |
| CR-P8-09 | done | `test_code_context_refs_ddl_contract.py` real local dev PG two-run diff |
| CR-P8-10 | done | `test_code_context_refs_ddl_contract.py::test_code_context_refs_ddl_env_guard_fails_fast_for_missing_or_unsafe_env` |
| CR-P8-11 | done | validation record production gate; production DDL not applied |
| CR-P8-12 | done | `test_code_intel_context_injection.py`; review fix covers Windows-style `backend\...\service.py` path normalization |
| CR-P8-13 | done | `CodeContextRef` fields and `test_code_intel_context_injection.py` |
| CR-P8-14 | done | `test_code_intel_decomposition.py` |
| CR-P8-15 | done | `test_code_intel_token_safe.py` |
| CR-P8-16 | done | `test_code_intel_evidence_contract.py` |
| CR-P8-17 | done | `test_code_intel_not_test_replacement.py` |
| CR-P8-18 | done | `test_code_intel_context_injection.py::test_non_code_query_has_empty_refs_with_reason_code_and_does_not_call_provider` |
| CR-P8-19 | done | `test_code_intel_true_reuse.py::test_adapter_provider_failure_is_not_swallowed` |
| CR-P8-20 | done | `test_core_no_adapter_import.py` covers `code_intelligence_core.py` |
| CR-P8-21 | done | `module_registry.yaml`, `file_ownership.yaml`; ownership scan files=29 mapped=29 unmapped=0 ambiguous=0 |
| CR-P8-22 | done_impl_head | blueprint §12/§16.9/§16.10 backfilled with implementation/control commit and G1-central run_id |
| CR-P8-23 | done_impl_head | validation record backfilled with G1-central job/run evidence |
| CR-P8-24 | done_g1_local | `python -m nox -s ra_phase8_code_intel` passed; 40 pytest passed, catalog 0 findings, guardrail 0 blocking |
| CR-P8-25 | done_impl_head | controlled runner passed on clean implementation/control HEAD; post-backfill doc drift requires final rerun |
| CR-P8-26 | pending_pr | PR checks/mergeStateStatus pending after push/PR |
| CR-P8-27 | done | no production port/DB touched during implementation |
| CR-P8-28 | in_progress | DESIGN-COMPLIANCE-001 matrix in this file + validation record |
| CR-P8-29 | done | no weakened DoD; no stop condition hit so far |
| CR-P8-30 | pending_post_merge | close-sync is post-merge only; no linked issue close performed yet |

## 6 hard closures

1. 真复用不 fork：done；adapter provider direct import + spy tests.
2. 真 PG DDL：done；DDL contract real localhost dev/validation PG, unsafe env AssertionError.
3. 消费链路到 L3：done；pack refs feed worker inputs and runtime trace.
4. provenance/as_of 不伪造：done；missing refs excluded/evidence_insufficient, no current-clock as_of in Phase8 files.
5. token-safe + 不替代测试：done；summary/ref/detail only, impacted/recommended only.
6. G3 不漂移：implementation/control commit and G1-central run_id backfilled；post-backfill doc-only drift will be revalidated rather than fake-green.
