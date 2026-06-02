# Research Assistant Phase 8 / L1.6 代码智能接入验证记录

- 日期：2026-06-02
- batch_id：`ra_phase8`
- plan_key：`ra_phase8_code_intel`
- branch：`codex/ra-code-intel-20260602`
- final_head_commit：`PENDING_FINAL_HEAD_AFTER_COMMIT`
- validation_run_id：`PENDING_G1_CENTRAL`
- production_ddl_gate：`required_pending_user_approval`
- production_8001_touched：`false`
- production_3000_touched：`false`
- production_19080_touched：`false`
- production_db_touched：`false`

## 范围

本阶段只把 `DEF-10`、`DAI-CODE-001`、`DAI-CODE-002` 从 `future_phase_pending` 推进到 `hard_pass`。未实现 Phase9+ 主动汇报、自我学习、通用 MCP 或知识包抽离。

## 实现摘要

- `code_intelligence_core.py` 定义 provider-neutral `CodeIntelligenceProvider`、确定性 manifest、token-safe pack/worker ref 压缩与 evidence-insufficient 降级。
- `code_intelligence_adapter_provider.py` 直接 import 并调用既有 `scripts.code_intelligence_adapter` 五个符号，不复制 CodeGraph / Understand-Anything 引擎。
- `service.py:build_context_pack` 对代码/模块 query 注入 `pack_json.code_context_refs`，并持久化到 `assistant_code_context_refs`。
- `agent_teams/runtime.py` 与 `agent_teams/models.py` 将 context pack 产生的 refs 作为 L3 worker inputs 消费，trace 标记 `orchestrator_consumed_code_context_refs=true`。
- `004_code_context_refs.sql` 新增真实 PG 幂等 DDL，含 JSONB manifest/provenance、`as_of`、表/全列 COMMENT、约束和索引。

## G1-local

当前阶段目标测试已通过：

```text
python -m pytest backend/tests/scripts/test_code_intelligence_adapter.py backend/tests/research_assistant/test_code_intel_true_reuse.py backend/tests/research_assistant/test_code_intel_determinism.py backend/tests/research_assistant/test_code_intel_token_safe.py backend/tests/research_assistant/test_code_context_refs_ddl_contract.py backend/tests/research_assistant/test_code_intel_context_injection.py backend/tests/research_assistant/test_code_intel_decomposition.py backend/tests/research_assistant/test_code_intel_evidence_contract.py backend/tests/research_assistant/test_code_intel_not_test_replacement.py backend/tests/research_assistant/test_core_no_adapter_import.py -q -p no:cacheprovider
result: 39 passed

python scripts/research_assistant_phase8_code_intel_guard.py --fail-on-embedding --fail-on-nondeterminism --fail-on-core-adapter-import
result: passed
```

接管 review 后修复两项阻断风险：Windows 反斜杠路径可被 `extract_repo_paths` 规范化为 repo path；DDL 约束同步禁止 affected_tests `state=passed/verified/...`。复测结果：

```text
python -m nox -s ra_phase8_code_intel
result: passed in 34s
pytest result: 40 passed
catalog_integrity: passed; findings=0; warnings=0
ownership_scan: files=29, mapped=29, unmapped=0, ambiguous=0
guardrail_scan: files=28, findings=0, blocking=0
production_8001_touched=false
```

最终文档回填后如产生 doc/evidence drift，将按最终 HEAD 再执行 G1-local/G1-central，不把旧 HEAD 结果伪装为最终绿证。

## G1-central

- job_id：`PENDING_G1_CENTRAL`
- run_id：`PENDING_G1_CENTRAL`
- return_code：`PENDING`
- expected_branch：`codex/ra-code-intel-20260602`
- expected_commit：`PENDING_FINAL_HEAD_AFTER_COMMIT`
- production_8001_touched：`false`
- arbitrary_shell_allowed：`false`

## 六条硬收口映射

1. 真复用不 fork：`test_code_intel_true_reuse.py`
2. 真 PG DDL：`test_code_context_refs_ddl_contract.py`
3. 消费链路到 L3：`test_code_intel_context_injection.py`、`test_code_intel_decomposition.py`
4. provenance/as_of 不伪造：`test_code_intel_evidence_contract.py`
5. token-safe + 不替代测试：`test_code_intel_token_safe.py`、`test_code_intel_not_test_replacement.py`
6. G3 不漂移：本文件、`20260602_ra_phase8_code_intel_progress.md`、蓝图 §12/§16.9/§16.10，最终以 G1-central run_id 回填。

## production gates

- 未启动、停止、重启或调用生产 `8001` / `3000` / `19080`。
- 未 apply 任何生产 DDL。
- Phase5 `assistant_agent_runs` 和 Phase6 `qe_autonomous_evolution_runs` 生产 DDL 仍待用户单独批准。
- Phase8 新增 `assistant_code_context_refs` 生产 DDL 也待用户单独批准。
