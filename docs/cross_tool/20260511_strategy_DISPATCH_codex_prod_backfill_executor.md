# Codex DISPATCH — Prod-capable Evidence Backfill Executor (解锁 R6 prod path)

**From**: Strategy session
**To**: Codex App
**Sent**: 2026-05-11 ~18:35
**Type**: Type B (coordinated, prod ops critical path)
**Branch**: codex/qe-governance-integration-20260509
**Severity**: BLOCKER for 9:30 实盘

## 上下文

paper-v2 verify of `b976c23` (drawer `9a2668d5`):
- L4 verdict: `--apply` 5-guard PASS but **dev-locked**, cannot prod
- READY-WITH-CAVEATS

Codex runbook (`55ac10d`, drawer `09cd1a6c`) 明确警告:
> Do not use scripts/strategy_package_evidence_backfill.py --apply or scripts/protected_asset_ledger_backfill.py --apply against production; no reviewed prod-capable executor/SQL package by 09:00 CST => no-go/hold R6.

当前 backfill scripts 的 negative safety check (拒绝 target_db=prod+5432+aistock) 是 design feature — **保留**。但需要 separate prod entrypoint 解锁 R6 prod ops。

## 任务

写 **prod-capable evidence backfill executor** on `codex/qe-governance-integration-20260509`。

### 设计原则 (硬约束)

1. **不修改** 现有 dev-locked scripts (`strategy_package_evidence_backfill.py` / `protected_asset_ledger_backfill.py`)
2. 现有 dev-locked scripts 是 dev verify 工具，prod 路径独立
3. Prod executor 必须**保留全部 5-guard 等价机制**: flag + mutex + token + env + triple-check
4. Prod executor **必须**:
   - 显示 dry-run preview before --apply
   - 要求 DR snapshot 引用 (e.g., snapshot 文件路径 + 时间戳 + row counts)
   - 要求 explicit `--confirm-token <SHA256_OF_OPERATOR_INPUT>` 而非 `--confirm` 布尔
   - 显示 plan summary + require operator typed confirmation
   - emit audit row (operator, timestamp, plan_hash, rows_applied)
   - per-package txn (单 package 失败立停, 不污染其他)
   - JSON output 含 actual rows_inserted + final_status + per_package_breakdown

### 实现选项 (Codex 自决, 但要在 deliver drawer 说明选择)

**Option A**: 新 script `scripts/strategy_package_evidence_apply_prod.py` + `scripts/protected_asset_ledger_apply_prod.py`
- Python entrypoint, 与现 dev script 平级
- 内部 import 现 dev script 的 plan builder (复用 schema + plan logic), 但 connect 路径完全独立
- pros: 与现有 codebase 风格一致, 可测试
- cons: 多 2 个 entrypoint

**Option B**: SQL package `backend/migrations/2026_05_11_R6_evidence_backfill_prod.sql`
- 纯 SQL, DBA 用 psql 直接 apply
- pros: 极简, 无 Python 路径
- cons: 失去 plan preview / token confirmation / audit emit, 必须人工保证安全

**Option C** (推荐): Hybrid - Python wrapper executes SQL package
- Python 做 plan preview + token + audit + per-package txn
- 实际 INSERT 用 prepared SQL templates (从 dev script 提取)
- pros: 安全 + 可读 SQL plan
- cons: 复杂度中等

**首选**: Option C, fallback Option A. **不要** Option B (失去 audit + token guard)。

### 测试

- 单元测试 covers:
  - dry-run preview path
  - token reject path (wrong / missing / replay)
  - DR snapshot ref reject path (missing / stale / wrong format)
  - per-package txn rollback (mock single package fail)
  - audit row emission
  - JSON output schema
- pytest target: `backend/tests/scripts/test_strategy_package_evidence_apply_prod.py` + `..._protected_asset_ledger_apply_prod.py`
- **Acceptance**: ≥ 15 passed per script, 0 guardrail finding

### Dev DB dry-run (强制 prod-only mode 也能 dev preview)

- 跑 `--dry-run --target-env=prod-preview-via-dev` 等价模式 against dev DB 5433
- 输出 4 JSON 在 `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/`
- 验证 prod executor 的 plan builder 与 dev script 输出一致 (row counts / package_ids match)

### Update runbook

修改 `docs/operations/r6_prod_apply_runbook_20260511.md`:
- §3 Evidence backfill --apply 改用 prod executor 命令
- 保留原 caveat 但移除 NO-GO 警告
- 添加新 executor 的 usage example + token generation steps

### Do NOT

- ❌ 不要修改现有 dev-locked scripts (保留 negative safety)
- ❌ 不要执行 prod apply (这是 ops 操作)
- ❌ 不要 INSERT dev DB (dry-run only)
- ❌ 不要 commit prod credentials / token
- ❌ 不要 merge codex branch to main

### Deliver

- commit to codex/qe-governance branch
- drawer 含: chosen option (A/B/C), files added, test counts, dry-run JSON paths, runbook updates, R6 prod readiness verdict (READY / READY-WITH-CAVEATS / BLOCKED)

### SLA

**≤ 1.5h** (实盘目标驱动, ~20:00 前 deliver)

## 引用

- paper-v2 verify drawer: `9a2668d5`
- Codex Task 1 deliver: drawer `d5816559`, commit `b976c23`
- Codex Task 3 runbook: drawer `09cd1a6c`, commit `55ac10d`
- 实盘目标: 明早 9:30 A股开市
