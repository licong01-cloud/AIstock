# Codex DISPATCH — Cold-Start Sanity Automation Script (Task 6)

**From**: Strategy session
**To**: Codex App
**Sent**: 2026-05-11 ~20:30
**Type**: Type B (coordinated, real trading go/no-go gate)
**Branch**: codex/qe-governance-integration-20260509
**Severity**: HIGH — 9:30 实盘前 final automated gate

## 上下文

paper-v2 verify 2866f66 (Task 5 protected_asset_ledger) 中。Codex Task 1-5 已 deliver:
- dev backfill scripts (b976c23)
- governance prep BUG-PREP fix (7bf840d)
- main hotfix audit (drawer b3d63611)
- R6 prod apply runbook (55ac10d)
- strategy_package prod executor (2fb81b3)
- protected_asset_ledger prod executor (2866f66)

实盘路径剩余:
- paper-v2 verify 2866f66
- codex/qe-governance branch baseline (流水线验证)
- R6 merge
- baseline post-R6
- 用户 prod 操作 (DR + 2 executor --apply + 6 migrations)
- prod backend 8001 + daemon enable
- **cold-start sanity** ← 真盘前 final go/no-go gate (本任务)
- 9:30 实盘

cold-start sanity 是 daemon enable 后第一笔模拟单验证 full audit chain。runbook §8 已含手动 checklist, 但需要 **automation script** 让用户一键跑 + 一键判定 go/no-go。

## 任务

写 **`scripts/paper_v2_coldstart_sanity.py`** on codex/qe-governance branch.

### 设计

**执行模式**: 单次 invocation, 默认 `--mode=dry-run` (no prod connection), `--mode=prod` 需 explicit flag + token guard。

**Phase 1 — Pre-flight**:
- 检查 prod backend 8001 健康 (HTTP `/health` 200)
- 检查 paper-v2 daemon process 运行 (psutil 或 OS process check)
- 检查 prod DB 5432 connection (read-only ping, no write)
- 检查 governance evidence 表存在 (4 prod packages stability evidence + protected_asset_ledger rows)
- 检查 enable_paper gate state = enabled (for sanity simulated order)
- 任一 fail → exit non-zero, **不进入 Phase 2**

**Phase 2 — Simulated round-trip** (non-trading hours only):
- 触发一笔 **sentinel paper-v2 order** (preset: symbol='000001.SZ', side=BUY, qty=100, intended_price=固定测试值 e.g., 10.00, run_id='sanity-<timestamp>')
- 通过 paper-v2 API 或 daemon 直接调用 (Codex 自决最佳路径)
- 等待 fill 写入 paper_v2.fills (timeout 30s, poll interval 1s)
- 验证 fill row 含:
  - intended_price = 10.00
  - fill_market_context (T6.1) non-null
  - created_at / updated_at (T6.2) populated
  - run_id = sanity-<timestamp>
  - audit chain intact (governance evidence rows referencing this run_id)
- 验证 outbox emit:
  - routing_class = 'telemetry' (T13)
  - payload schema correct
  - emit_status = 'pending' or 'sent'

**Phase 3 — Audit chain verify**:
- 查 governance evidence table 对此 run_id 是否有 evidence row
- 查 protected_asset_ledger 是否有对应 ledger entry (if applicable)
- 验证 audit timestamps order (fill ts < evidence ts < ledger ts)

**Phase 4 — Cleanup**:
- DELETE sentinel fill + outbox row + evidence row (per-table txn)
- 输出 JSON report with each check PASS/FAIL

**Phase 5 — Verdict**:
- 全 PASS → exit 0, JSON `{verdict: "GO", real_trading_ready: true}`
- 任一 FAIL → exit non-zero, JSON `{verdict: "NO-GO", failed_checks: [...], remedial_action: [...]}`

### 5-Guard (沿用 prod executor 设计)

- Default mode `--mode=dry-run` (only Phase 1 read-only, skip Phase 2-4)
- `--mode=prod` 需:
  - exact token `RUN_PAPER_V2_COLDSTART_SANITY_PROD`
  - env `AISTOCK_PAPER_V2_COLDSTART_SANITY_PROD_ENABLED=true`
  - mutex env (防止 9:30 开市前并发跑)
  - non-trading hours check (reject if 9:30-15:00 + 11:30-13:00 ranges)
  - operator confirmation (typed)

### 测试

`backend/tests/scripts/test_paper_v2_coldstart_sanity.py`:
- Phase 1 各 fail case (backend down / daemon down / DB unreachable / evidence missing / gate disabled)
- Phase 2 sentinel order with mocked daemon
- Phase 3 audit chain queries (mocked DB)
- Phase 4 cleanup rollback on fail
- Phase 5 verdict JSON schema
- 5-guard reject paths
- non-trading hours check
- ≥ 20 tests passed, 0 P1 guardrail

### Update runbook

Append `docs/operations/r6_prod_apply_runbook_20260511.md` §8.5 (cold-start sanity automation):
- CLI usage
- expected JSON output
- abort criteria
- 9:30 实盘前必跑

### Dev preview JSON

跑 `--mode=dry-run` 输出到 `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/paper_v2_coldstart_sanity_dry_run.json`

### Do NOT

- ❌ 不要执行 prod sanity (这是 user 实盘前手动跑)
- ❌ 不要连 prod DB (sanity 本身 connect, 但 dispatch 阶段不要触)
- ❌ 不要 INSERT dev DB
- ❌ 不要 commit token literal in test
- ❌ 不要 merge codex branch

### Deliver

drawer:
- commit hash
- files added
- test count
- runbook §8.5 update
- 9:30 实盘 go/no-go automation ready: confirmed / blocked

### SLA

**≤ 1.5h** (~22:00 deliver)

## 引用

- runbook §8 (cold-start sanity manual): main `30879c2` 已合
- paper-v2 capture 字段定义: backend/services/paper_trading_v2/repository.py
- T13 routing_class: backend/services/paper_trading_v2/daemon/
- governance evidence schema: backend/db/migrations/...
- 实盘目标: 明早 9:30 A股开市
- main HEAD: `3435f21`
