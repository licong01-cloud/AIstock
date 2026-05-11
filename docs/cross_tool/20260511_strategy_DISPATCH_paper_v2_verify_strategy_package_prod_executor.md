# paper-v2 DISPATCH — Verify Codex 2fb81b3 strategy_package prod executor

**From**: Strategy session
**To**: paper-v2 worktree team
**Sent**: 2026-05-11 ~19:35
**Type**: Type C audit (dual-party verify Codex prod executor)
**Parallel with**: Codex Task 5 protected_asset_ledger executor

## 上下文

Codex Task 4 deliver (drawer `abafc500`, commit `2fb81b3`) on codex/qe-governance-integration-20260509:

- `scripts/strategy_package_governance_evidence_backfill_prod_executor.py` (new)
- `backend/tests/scripts/test_strategy_package_governance_evidence_backfill_prod_executor.py` (24 tests)
- runbook §7.2 aligned to final CLI/env/token/mutex/pre-apply contract
- 53 total tests passed
- guardrail 0 blocking P1, only P2 ALGO-COMPLEXITY warnings documented as bounded four-package point-query

paper-v2 baseline v2 GREEN 已确认 (e8ffbdd), R6 readiness GO. 但 prod executor 必须经双侧 verify 才能授权 prod --apply。

## 任务

4-layer audit of Codex `2fb81b3`.

### Layer 1 — Static safety invariants

读 `scripts/strategy_package_governance_evidence_backfill_prod_executor.py`:

- [ ] **5-guard chain 真正存在 + 先于 connect**:
  - Guard 1: --apply explicit flag (not default)
  - Guard 2: exact token `APPLY_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD` literal compare
  - Guard 3: env `AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD_APPLY_ENABLED=true`
  - Guard 4: mutex env `AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_MUTEX_HELD=true`
  - Guard 5: target_db=prod + port=5432 + dbname=aistock + DR snapshot ref valid + plan preview sha256 confirmed + operator typed confirmation
- [ ] 任一 guard 失败 → exit non-zero, **不开 DB connection**
- [ ] dry-run path 真 offline (no `psycopg2.connect`, no `engine.connect`)
- [ ] audit row 在 INSERT 完成后 emit, 含 operator / timestamp / plan_hash / rows_applied
- [ ] per-package txn: 单 package fail 不污染其他

### Layer 2 — Tests rerun

```bash
cd F:/Dev/AIstock-worktrees/baseline-post-r5  # 或 fresh worktree
git fetch origin codex/qe-governance-integration-20260509
git checkout 2fb81b3 -- scripts/strategy_package_governance_evidence_backfill_prod_executor.py backend/tests/scripts/test_strategy_package_governance_evidence_backfill_prod_executor.py
python -m pytest backend/tests/scripts/test_strategy_package_governance_evidence_backfill_prod_executor.py -v
```

预期: 24 passed, 0 failed
- 覆盖度检查: dry-run / token wrong-replay-missing / 2 envs missing / mutex absent / target_db dev rejected / DR snapshot stale-missing / plan preview sha mismatch / operator confirmation incomplete / per-package fail rollback / audit emit / JSON schema

### Layer 3 — Dry-run JSON 验证

读 `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/*.json`:
- [ ] status=passed, db_connection_opened=false, db_writes_executed=false, ddl=false
- [ ] 4 packages, planned rows 与 dev backfill script (`b976c23` 输出) 一致
- [ ] plan_hash 一致性 (重跑 dry-run 同一输入 → 同 hash)

### Layer 4 — Semantic + runbook alignment

读 `docs/operations/r6_prod_apply_runbook_20260511.md` §7.2:
- [ ] CLI 命令与 executor argparse 一致
- [ ] env vars 名字字面匹配
- [ ] token literal 与 executor source 一致
- [ ] operator confirmation 步骤完整 (token + DB label + plan sha + DR ref + 4 package_ids)
- [ ] DR snapshot ref 格式 (snapshot path + timestamp + row counts) 在 executor 与 runbook 一致

### Layer 5 (新增) — P2 ALGO-COMPLEXITY 验证

Codex 标 P2 为 "bounded four-package point-query"。复验:
- 实际是否真 bounded (无 unbounded scan / cursor)
- 4 package_ids 是否硬编码或 config-bound (不可被攻击者扩为 N)
- 如发现 unbounded → 升级为 BLOCKER

## 输出

`docs/cross_tool/20260511_paper_v2_VERIFY_codex_strategy_package_prod_executor.md`:
- 5 layers each PASS/FAIL/PARTIAL
- guard chain detailed analysis
- 24 tests rerun result
- P2 ALGO-COMPLEXITY 复验结论
- verdict: READY / READY-WITH-CAVEATS / BLOCKED
- 任何 findings 列 file:line

## Deliver

drawer:
```
[VERIFY] paper-v2 4-layer verify of Codex 2fb81b3 strategy_package prod executor
commit: <new>
branch: claude/paper-v2-baseline-post-r5-20260511 (or new)
L1 Static: PASS/FAIL
L2 Tests (24): PASS/FAIL
L3 JSONs: PASS/FAIL
L4 Semantic+runbook: PASS/FAIL
L5 ALGO-COMPLEXITY: bounded confirmed / not bounded
prod apply readiness: READY|CAVEATS|BLOCKED
doc: docs/cross_tool/20260511_paper_v2_VERIFY_codex_strategy_package_prod_executor.md
```

## SLA

**≤ 60 min** (~20:35 deliver)

## Do NOT

- ❌ 不要 INSERT dev DB
- ❌ 不要 connect prod DB
- ❌ 不要 fix Codex 代码 (发现 finding 报告即可)
- ❌ 不要 merge codex branch

## References

- Codex deliver doc: `docs/cross_tool/20260511_codex_to_claude_INFO_r6_prod_apply_runbook_handoff.md`
- prev paper-v2 verify b976c23 protocol: `docs/cross_tool/20260511_paper_v2_VERIFY_codex_backfill_scripts.md` (cherry-picked to main as bdcdb4b)
- 实盘目标: 明早 9:30 A股开市
- main HEAD: c515cf4
