# paper-v2 DISPATCH — 9:30 LocalSim 模拟盘 Quick-start Operator Cheatsheet

**From**: Strategy session
**To**: paper-v2 worktree team
**Sent**: 2026-05-12 ~00:00
**Type**: Type C — operator ergonomics doc
**Priority**: MED — 与 Codex Task 8 sentinel endpoint 并行, 不阻断

## 上下文

paper-v2 fix round done (drawer `9a9f7d26`):
- Fix1 stk_limit BLOCKED (dispatch 矛盾, 5433 vs 5432 connect 权限)
- Fix2 noxfile drift DELEGATED to Codex
- R6 GO-WITH-CAVEATS unchanged, **LocalSim mock-only tomorrow not blocked**

Codex Task 8 进展 (drawer `66fe3e4c`):
- 实施 sentinel endpoint 中, ~00:20 deliver
- 发现 capture columns 在 codex branch base 缺 → Codex 写 branch-local guarded DDL

明早 9:30 LocalSim 模拟盘前用户需要 ~30-60 min 实操 (R6 merge 后到 9:30 前)，需要一份 **简化 cheatsheet** 让用户照着跑, 避免遗漏。

## 任务

写 **`docs/operations/930_localsim_quickstart_20260512.md`** on `claude/paper-v2-baseline-post-r5-20260511` branch (复用现有 paper-v2 branch)。

### 内容要求 (~150-250 行 markdown)

**§1 前置 GO/NO-GO 决策表**

列必须全 GREEN 才进入 cutover:
- R5 in main baseline GREEN (e8ffbdd / c515cf4) ✅
- 2 prod executors verify READY (c2ef5f5 + 94242c1) ✅
- Codex Task 6 sanity automation deliver (c2352a9) ✅
- Codex Task 7 E2E wrapper deliver (a72411d) ✅
- Codex Task 8 sentinel endpoint deliver (待 ~00:20) ⏳
- paper-v2 verify Task 8 PASS (待 Codex 完成后) ⏳
- R6 merge to main ⏸️
- baseline post-R6 GREEN ⏸️
- prod DB DR snapshot ⏸️

每项标 ✅ / ⏳ / ⏸️ / ❌, 全 ✅ 才 GO。

**§2 必须用户手动确认的环境前置**

- prod DB 5432 `paper_v2.fills` 表 schema 含 capture columns (intended_price + fill_market_context + created_at + updated_at)
  - 命令: `psql -h <prod-host> -U <user> -d aistock -c "\d paper_v2.fills"`
  - 期望: 4 列存在
  - 如缺 → 立停, 联系战略 session
- stk_limit 表 trade_date >= t-3 (3 个交易日内)
  - 命令: `psql -h <prod-host> -U <user> -d aistock -c "SELECT max(trade_date) FROM stk_limit"`
  - 如 stale → 跑 refresh 脚本前置
- governance enable_paper gate state:
  - 命令: `SELECT package_id, enable_paper FROM strategy_package_governance WHERE enable_paper = true`
  - 至少 1 个 package enabled, 否则没策略包可跑

**§3 Cutover Step 序列** (按时间)

```
T-90min  DR snapshot + DR ref JSON 生成
T-75min  evidence backfill --apply (strategy_package executor)
T-60min  evidence backfill --apply (protected_asset_ledger executor)
T-45min  6 migrations apply (per-file txn)
T-30min  R6 git merge to main (战略 session)
T-25min  baseline post-R6 GREEN check
T-20min  prod backend 8001 restart
T-15min  paper-v2 daemon enable
T-10min  cold-start sanity --mode=prod (verify sentinel endpoint round-trip)
T-5min   sanity verdict GO → 实盘准备
T-0      9:30 A股开市
```

每 step 给 1 行命令 + 1 行 abort-on-fail 处置。

**§4 异常 abort 决策树**

- DR snapshot fail → 立停, rollback (无 prod 影响)
- evidence backfill fail → 立停, DB 单步 rollback (用 DR snapshot)
- migrations fail → 立停, per-file txn 自动 rollback 该 migration
- R6 merge conflict → 立停, 用 git reset --hard 回 main pre-merge
- baseline post-R6 RED → 立停, 调查 root cause (可能需要 hotfix)
- daemon enable fail → 立停, 检查 process / config
- cold-start sanity NO-GO → 立停, 看 failed_checks JSON + remedial_action

**§5 9:30 之后监控点**

- daemon process 持续运行 (PID 监控)
- paper_v2.fills 行数增长率合理
- outbox emit 无积压
- governance evidence 实时写入
- 任何 typed error → 立刻 abort

**§6 引用**

- R6 runbook §1-§11 (完整版): `docs/operations/r6_prod_apply_runbook_20260511.md`
- 2 prod executor: `scripts/strategy_package_governance_evidence_backfill_prod_executor.py` + `scripts/protected_asset_ledger_backfill_prod_executor.py`
- coldstart sanity: `scripts/paper_v2_coldstart_sanity.py`
- E2E wrapper: `scripts/r6_prod_cutover_e2e_wrapper.py`

### 风格

- 用户照着复制粘贴 + 简单决策 (GO/NO-GO + abort)
- 每 step <= 5 行 markdown
- 命令含完整路径 + 必需 env vars 占位 (不写 token literal)
- 不重复 R6 runbook §1-§10 详细内容, 仅指引

### Deliver drawer

```
[CHEATSHEET] paper-v2 930 LocalSim quickstart
branch: claude/paper-v2-baseline-post-r5-20260511
commit: <new>
doc: docs/operations/930_localsim_quickstart_20260512.md
sections: 6
T-X step count: ~9
abort decision tree: covers DR/backfill/migrations/merge/baseline/daemon/sanity
ready for 9:30: TRUE (作为用户实操 cheatsheet)
```

## SLA

**≤ 45 min** (~00:45 deliver)

## Do NOT

- ❌ 不要包含 prod credentials / token literal
- ❌ 不要 INSERT dev DB
- ❌ 不要 touch prod
- ❌ 不要 merge to main (cheatsheet 自带在 paper-v2 branch, 战略可后续 cherry-pick)

## References

- R6 runbook: `docs/operations/r6_prod_apply_runbook_20260511.md`
- Codex Task 8 dependency note: drawer `66fe3e4c`
- paper-v2 fix round: drawer `9a9f7d26`
- 实盘目标: 明早 9:30 LocalSim 模拟盘 ✅ (实盘暂不开发, miniqmt_sim PR-005 实施延后)
- main HEAD: `568c16c`
