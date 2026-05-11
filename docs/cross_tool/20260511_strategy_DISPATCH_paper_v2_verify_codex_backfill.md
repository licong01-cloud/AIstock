# paper-v2 DISPATCH — Verify Codex b976c23 R6 Evidence Backfill Scripts

**From**: Strategy session
**To**: paper-v2 worktree team
**Sent**: 2026-05-11 ~18:00
**Type**: Type C audit (dual-party verify Codex deliver)
**Prereq**: Codex `b976c23` deliver (drawer `d5816559`), paper-v2 baseline GREEN (`535c539`, drawer `926b68f0`)

## 上下文

Codex `b976c23` on codex/qe-governance-integration-20260509:
- `scripts/strategy_package_evidence_backfill.py`
- `scripts/protected_asset_ledger_backfill.py`
- `backend/tests/scripts/test_strategy_package_evidence_backfill.py`
- `backend/tests/scripts/test_protected_asset_ledger_backfill.py`
- 4 dry-run JSON outputs in `tests/aistock_validation/dry_runs/20260511_evidence_backfill_dry_run/`

Codex 自检:
- 17 pytest passed (scope: 2 backfill test files)
- 54 passed (broader governance smoke)
- guardrail 0 P1 findings
- negative safety: dry-run/apply 拒绝 target_db=dev + port 5433 + dbname=aistock before connect/apply
- read-only agent recheck PASS after dbname guard fix

**目标实盘时间**: 明早 9:30 开市 (路径 A 今晚完成 R5+R6+prod 配置)
所以这次 verify SLA 严格: **≤ 1 小时**。

## 任务

### Step 1: Sync codex branch

```bash
cd F:/Dev/AIstock-worktrees/paper-v2-vnpy-mvp-20260508
git fetch origin codex/qe-governance-integration-20260509
git log --oneline 7bf840d..origin/codex/qe-governance-integration-20260509 | head -10
# 应看到 33ecb1d, 75470f5, b976c23 (merge)
```

### Step 2: 4 层 verify

**Layer 1 - Static / safety invariants**:
- 读 `scripts/strategy_package_evidence_backfill.py` + `scripts/protected_asset_ledger_backfill.py`
- 检查:
  - `--dry-run` 是默认值，`--apply` 必须 explicit flag
  - target_db / port / dbname 三重校验先于 connect (Codex 报告说 negative safety PASS, 复验)
  - 无 raw INSERT 在 dry-run path
  - JSON output schema 包含 `db_writes`, `ddl`, `dry_run`, `target_db`, `packages`
  - exit code: dry-run pass=0, fail=非0; apply 同样

**Layer 2 - Test coverage**:
- 跑 `python -m pytest backend/tests/scripts/test_strategy_package_evidence_backfill.py backend/tests/scripts/test_protected_asset_ledger_backfill.py -v`
- 应看到 17 passed
- 检查 test 覆盖: dry-run 路径, exit code, JSON schema, negative safety reject

**Layer 3 - Dry-run JSON 内容**:
- 读 4 个 JSON outputs in `tests/aistock_validation/dry_runs/20260511_evidence_backfill_dry_run/`
- 验证:
  - `status=passed`, `db_writes=false`, `ddl=false`, `dry_run=true`, `target_db=dev`
  - `strategy_package_evidence_backfill_dev_dry_run.json`: 4 packages, 12 planned strategy evidence rows
  - `protected_asset_ledger_backfill_dev_dry_run.json`: 4 packages, 4 planned protected ledger rows
  - limit2 variants: 2 packages each
  - planned rows 数字合理 (1 strategy package = 3 evidence rows = 4 packages × 3 = 12 ✓)

**Layer 4 - R6 semantic correctness**:
- evidence_type / evidence_payload schema 是否匹配 governance evidence 表结构 (查 dev DB 或 schema doc)
- backfill 是否覆盖 4 prod packages (具体 package_id 是否对得上)
- protected_asset_ledger 字段是否完整 (查 schema)
- **关键**: --apply 模式连接 prod DB 5432 时的逻辑路径是否有 dry-run / staging 中间步 (即使 user 跑 --apply, 也应该 print plan + require confirmation)
- 如果 --apply 直接 INSERT 无 confirmation step, **必须 BLOCK** (P1 finding)

### Step 3: 输出 verify doc

写到 paper-v2 branch:
- 路径: `docs/cross_tool/20260511_paper_v2_VERIFY_codex_backfill_scripts.md`
- 字段:
  - target commit: b976c23
  - 4 layers each PASS/FAIL/PARTIAL
  - any P1/P2/P3 findings (列 file:line)
  - prod apply readiness verdict: READY / READY-WITH-CAVEATS / BLOCKED
  - 如 READY-WITH-CAVEATS: 列前置条件 (e.g., 用户 confirmation step, DR snapshot, etc.)

### Step 4: deliver drawer

短 drawer 到 cross-tool/codex-claude-coord:
```
[VERIFY] paper-v2 verify Codex b976c23 R6 backfill scripts
commit: <new commit on paper-v2 branch>
verdict: <PASS|PASS-WITH-CAVEATS|BLOCKED>
layers: static=<X> tests=<Y> json=<Z> semantic=<W>
findings: <count P1/P2/P3>
prod apply readiness: <READY|CAVEATS|BLOCKED>
doc: docs/cross_tool/20260511_paper_v2_VERIFY_codex_backfill_scripts.md
```

## SLA

**≤ 60 min** (实盘目标驱动)

## 不要做

- ❌ 不要 INSERT to dev DB (只 SELECT verify)
- ❌ 不要 connect prod DB
- ❌ 不要 fix Codex 代码 (发现 finding 报告即可, Codex 自己 fix)
- ❌ 不要 merge codex branch

## References

- Codex deliver doc: `docs/cross_tool/20260511_codex_to_claude_REVIEW_evidence_backfill_prep.md`
- 之前 paper-v2 verify protocol: `docs/cross_tool/20260511_paper_v2_VERIFY_codex_governance_prep_fixes.md`
- 实盘目标时间线: 明早 9:30 A股开市
