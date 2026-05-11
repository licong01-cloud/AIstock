# Codex DISPATCH — Protected Asset Ledger Prod Executor (Task 5)

**From**: Strategy session
**To**: Codex App
**Sent**: 2026-05-11 ~19:35
**Type**: Type B (coordinated, prod ops critical path)
**Branch**: codex/qe-governance-integration-20260509
**Severity**: BLOCKER for 9:30 实盘 (与 strategy_package executor 配对)

## 上下文

Codex Task 4 (commit `2fb81b3`, drawer `abafc500`) deliver `strategy_package_governance_evidence_backfill_prod_executor.py` 完成 + 24 tests passed。

Codex 在 deliver 中明确说:
> Protected asset ledger production executor remains a separate runbook placeholder unless strategy dispatches it.

**Strategy 正式 dispatch**: 实盘需要 **两个** backfill 都有 prod path, protected_asset_ledger executor 必须 deliver。

## 任务

写 **`scripts/protected_asset_ledger_backfill_prod_executor.py`** on `codex/qe-governance-integration-20260509`。

### 设计 (与 strategy_package executor 平级)

完全镜像 `strategy_package_governance_evidence_backfill_prod_executor.py` (commit 2fb81b3) 的设计:
- 默认 dry-run, offline, no DB connection
- `--apply` 需:
  - exact token: `APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_PROD` (推荐, Codex 可自决具体字面值)
  - env: `AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_PROD_APPLY_ENABLED=true`
  - mutex env: `AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_MUTEX_HELD=true`
  - `--target-db prod --target-port 5432 --target-dbname aistock`
  - DR snapshot ref validation
  - plan preview SHA256 confirmation
  - operator confirmation (typed): token + target DB label + plan preview sha256 + DR snapshot ref + all package_ids
- per-package txn
- audit row emit (operator, timestamp, plan_hash, rows_applied)
- JSON output with rows_inserted + final_status + per_package_breakdown

### 不修改

- 不动 `scripts/protected_asset_ledger_backfill.py` (dev-locked 保留)
- 不动 `strategy_package_governance_evidence_backfill_prod_executor.py`

### 测试

`backend/tests/scripts/test_protected_asset_ledger_backfill_prod_executor.py`:
- 覆盖 dry-run / token / 2 envs / mutex / DR ref / plan preview / operator confirmation / per-package txn rollback / audit emit / JSON schema
- target: ≥ 20 tests passed
- 0 P1 guardrail finding

### Dev preview JSON

跑 dry-run 输出到 `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/protected_asset_ledger_apply_prod_dev_preview_*.json`

### Update runbook

Append `docs/operations/r6_prod_apply_runbook_20260511.md` §7.3 (protected_asset_ledger apply order) 与新 executor 命令 align。

### Do NOT

- ❌ 不要执行 prod apply
- ❌ 不要连 prod DB
- ❌ 不要 INSERT dev DB
- ❌ 不要 commit credentials / token literal in tests (use placeholder)
- ❌ 不要 merge codex branch to main

### Deliver

drawer + detail doc:
- commit hash
- files added
- test counts
- dry-run JSON paths
- runbook §7.3 update
- R6 readiness for 9:30 实盘: confirmed / blocked

### SLA

**≤ 60 min** (~20:30 deliver, with paper-v2 verify in parallel)

## 引用

- Codex Task 4 deliver: drawer `abafc500`, commit `2fb81b3`
- strategy_package executor 设计: `scripts/strategy_package_governance_evidence_backfill_prod_executor.py`
- 实盘目标: 明早 9:30 A股开市
- main HEAD: `c515cf4`
