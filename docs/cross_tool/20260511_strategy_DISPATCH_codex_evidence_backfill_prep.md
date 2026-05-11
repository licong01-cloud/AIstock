# Codex DISPATCH — R6 Evidence Backfill Script 准备 + main hotfix audit

**From**: Strategy session (Claude Code 战略 window)
**To**: Codex App
**Sent**: 2026-05-11 ~17:00
**Type**: Type B (coordinated) + Type C (audit)
**Branch scope**: codex/qe-governance-integration-20260509

## 上下文

- Codex 7bf840d (drawer 3fc0478a) — 4 BUG-PREP fix + 4 dry-run JSON PASS ✅
- paper-v2 ee2e56f (drawer 36b484c9) — verify Codex 7bf840d PASS ✅
- R6 prep layer 100% ready，但 R6 merge 仍 BLOCK on:
  - 4 prod packages stability evidence backfill (script 未写)
  - protected_asset_ledger backfill (script 未写)
  - 用户 prod DB write 授权 (未给)

战略 session 不接手 Codex 自驱代码工作，所以 backfill script 应由 Codex 写在自己 branch。

另：战略 session 在 main 上做了 hotfix (post-R4)，Codex 之前没 audit 过。

## Task 1 (HIGH) — R6 Evidence Backfill Script 准备 + dev dry-run

**Branch**: codex/qe-governance-integration-20260509 (Codex own)

**目标**: 写 backfill script，dry-run on dev DB 5433，输出 JSON，等用户授权 prod 后再实际跑。

**Scope**:
1. **4 prod packages stability evidence backfill**:
   - 范围: `strategy_package_governance` 4 prod packages (具体 package_id 在 dev DB 查)
   - 写 `scripts/strategy_package_evidence_backfill.py`
   - 字段: 按 governance evidence schema, evidence_type / evidence_payload / verified_at / source_run_id 等 (查 governance schema 表确认)
   - 输入: package_id 列表, source_run_id (可选)
   - 输出: dev DB SELECT preview + 计划 INSERT 行数 (dry-run 模式不实际 INSERT)
   - 必须支持 `--dry-run` (默认) 和 `--apply` (require explicit flag) 双模式
   - JSON 输出 schema: `{status: passed|failed, db_writes: bool, ddl: bool, packages: [{id, evidence_planned, evidence_existing}], dry_run: bool, target_db: dev|prod}`

2. **protected_asset_ledger backfill**:
   - 写 `scripts/protected_asset_ledger_backfill.py`
   - 类似 dry-run/apply 双模式
   - 输出同 schema

3. **Dev DB dry-run**:
   - 在 dev DB 5433/aistock_dev 跑两个 script `--dry-run`
   - 验证 SELECT-only, db_writes=false, ddl=false
   - 输出 JSON 到 `tests/aistock_validation/dry_runs/20260511_evidence_backfill_dry_run/`

4. **Tests**:
   - 写 `backend/tests/scripts/test_strategy_package_evidence_backfill.py` 覆盖 dry-run 路径 + exit code + JSON schema
   - 写 `backend/tests/scripts/test_protected_asset_ledger_backfill.py` 同上

5. **Acceptance**:
   - 2 script + 2 test files
   - 4 dry-run JSON outputs (2 scripts × dev DB)
   - All tests PASS
   - Guardrail 0 findings
   - prod_touched=false (不要触碰 prod DB / prod backend / prod frontend)

**Do NOT**:
- ❌ 不要 apply 到 prod DB (5432)
- ❌ 不要 INSERT 到 dev DB (只 SELECT preview)
- ❌ 不要 merge codex/qe-governance 到 main
- ❌ 不要触碰 paper-v2 worktree / main 已合并代码

**Deliver**: drawer to cross-tool/codex-claude-coord, short summary + detail doc reference.

## Task 2 (MEDIUM) — Main Hotfix Audit

**Scope**: 战略 session 在 main 上做了 2 个 hotfix commits (在 R4 merge tip 2d1f820 之后, HEAD 4a3fa60 之前):

1. `tests/aistock_validation/catalog/module_registry.yaml` — 合并 rl_execution 重复条目 (R0 cherry-pick + R1 merge 双注册产物)
2. `backend/tests/data_quality/test_cross_table_consistency.py` — `test_paper_v2_fill_count_matches_archive_per_run` 中 archive_run_n==0 时从 pytest.fail 改 pytest.skip per D5 Q2.c (worker default disabled)

**目的**: 防止战略 session 单方面在 main 做改动 unchecked。

**Audit 范围**:
- 改动是否符合 R0-R6 协议 (modular parallel validation + serial merge)?
- module_registry 合并是否丢失任何 metadata?
- data_quality test skip 是否真正符合 D5 Q2.c qualified-yes 意图，不会掩盖真实回归?
- 改动 commit 是否应该走 R0 风格 (cherry-pick 而非直接 main commit)?

**Output**: 短 audit report (<200 words) drawer + 必要时 detail doc。
- 如发现 issue: 列为 BUG-AUDIT-XXX 写到 `tests/aistock_validation/bugs/`
- 如无 issue: 简短 PASS drawer 即可

**Do NOT**:
- ❌ 不要修改 main code
- ❌ 不要 revert hotfix (即使发现问题, 报告即可)

## 接手 contingency 状态

**Codex DO NOT take over.** 战略 session 继续主导。Codex 保持 self-driven + dispatched 任务模式。

## SLA

- Task 1: ~2-3 hours (含 dry-run + tests)
- Task 2: ~30 min (audit + drawer)

## References

- Codex prep work: 7bf840d (drawer 3fc0478a)
- paper-v2 verify: ee2e56f (drawer 36b484c9)
- main HEAD: 4a3fa60
- R4 merge tip: 2d1f820
- takeover doc: docs/handoff/codex_takeover_contingency_20260511.md
- playbook v2 §R6: governance merge 前置条件
- D5 Q2.c qualified-yes: archive worker default disabled per ops decision
