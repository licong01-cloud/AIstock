# 9:30 LocalSim 模拟盘 Quick-Start Cheatsheet — 2026-05-12

> **作者**: paper-v2 team Lead
> **目的**: operator 照表执行 9:30 LocalSim mock-only 实盘 cutover
> **范围**: LocalSim 模拟盘（非真实 prod 交易，按用户 2026-05-11 决定 miniqmt_sim 延后）
> **配套**: r6_prod_apply_runbook_20260511.md §1-§10（**本文档不重复，仅引用 §**）
> **风格**: 每步 ≤ 5 行，可复制粘贴

---

## §1 — 前置 GO/NO-GO 决策表

| # | Deliverable | Commit | Verdict |
|---|---|---|---|
| 1 | R5 merge baseline on main | `<main HEAD ref>` | ✅ |
| 2 | Codex b976c23 prep scripts verify | `1acc15f` | ✅ READY-WITH-CAVEATS (dev-only) |
| 3 | Codex 2fb81b3 strategy_package prod executor verify | `c2ef5f5` | ✅ READY |
| 4 | Codex 2866f66 protected_asset_ledger prod executor verify | `94242c1` | ✅ READY |
| 5 | Codex c2352a9 coldstart sanity verify | `1dc2e60` | ⚠️ READY-WITH-CAVEATS (sentinel endpoint pending) |
| 6 | Stage 6 baseline post-R5 with .env | `e8ffbdd` | ✅ GREEN |
| 7 | codex branch baseline | `7c18a1d` | ⚠️ YELLOW (stk_limit env-only) |
| 8 | fix round baseline caveats | `60ee470` | ⏸️ Fix 1 BLOCKED (operational), Fix 2 DELEGATED |
| 9 | Codex Task 8 sentinel endpoint implementation | — | ⏳ in-flight ETA ~00:20 |

**Decision rule**: 第 1/3/4/6 必须 ✅；第 5/9 接受 ⚠️/⏳（用 `--sentinel-endpoint` 覆盖）；第 7/8 caveats 已知接受。

---

## §2 — 必须用户手动确认的环境前置（cutover 前 30-60 分钟）

### 2.1 prod DB `paper_v2.fills` schema 含 capture columns
- 命令: `psql "<OPERATOR_INPUT_PROD_CONN>" -c "\d paper_v2.fills"`（operator 手敲 prod connection — 不要复制密码进文档）
- 期望含: `intended_price` / `fill_market_context` / `created_at` / `updated_at` 4 列
- 缺失则 abort cutover

### 2.2 `stk_limit` 数据新鲜度
- 命令: `psql "<OPERATOR_INPUT_PROD_CONN>" -c "SELECT MAX(trade_date) FROM stk_limit;"`
- 期望: latest >= t-3 (today minus 3 days)
- 不满足: 等 Tushare 上游补齐 OR 走 staleness 容差（Lead 建议联系 Codex/owner 实施）

### 2.3 governance `enable_paper` 至少 1 个 package = true
- 命令: `psql "<OPERATOR_INPUT_PROD_CONN>" -c "SELECT package_id, governance_eligibility->>'paper_ready' AS ready FROM strategy_pkg.package WHERE governance_eligibility->>'paper_ready' = 'true' LIMIT 5;"`
- 期望: 至少 1 行
- 全部 false: abort（没有 paper-ready package 无法进入 paper trading）

---

## §3 — Cutover Step 序列（T-90 → T-0）

#### T-90 — DR snapshot
- 命令: `python scripts/r6_dr_snapshot.py --output <SECURE_EVIDENCE_DIR>/r6_dr_snapshot_verified.json`
- 期望: `r6_dr_snapshot_verified.json` 生成在 `<SECURE_EVIDENCE_DIR>/`
- 异常 → §4.1
- 引用: R6 runbook §6.1

#### T-75 — strategy_package executor --apply
- 命令: `APPLY_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD=${TOKEN_FROM_PASSWORD_MANAGER} python scripts/strategy_package_governance_evidence_backfill_prod_executor.py --apply`
- 期望: per-package txn 全部 commit；audit log 记录 N packages updated
- 异常 → §4.2
- 引用: R6 runbook §7.2

#### T-60 — protected_asset_ledger executor --apply
- 命令: `APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_PROD=${TOKEN_FROM_PASSWORD_MANAGER} python scripts/protected_asset_ledger_backfill_prod_executor.py --apply`
- 期望: ledger rows backfilled；audit log GREEN
- 异常 → §4.3
- 引用: R6 runbook §7.3

#### T-45 — 6 migrations apply (per-file txn)
- 命令: `alembic upgrade +1` 逐个执行 6 次（每次 verify 后再下一个）
- 期望: 每个 migration 单独 txn 成功；`alembic current` 推进
- 异常 → §4.4
- 引用: R6 runbook §6

#### T-30 — R6 git merge
- 操作: 战略 session 在 main 执行 R6 PR merge（squash）
- 期望: main HEAD 推进；CI GREEN
- 异常 → §4.5
- 引用: R6 runbook §5

#### T-25 — baseline post-R6 GREEN check
- 操作: 派 paper-v2 team 跑 Stage 6 baseline on main post-R6
- 期望: 全 session GREEN（含 frontend / backend / tests）
- 异常 → §4.6
- 引用: R6 runbook §9

#### T-20 — prod backend 8001 restart
- 操作: operator 手动 restart（per `feedback_no_service_start` 由 user 执行）
- 期望: AIstock backend port 8001 健康；`/healthz` 200
- 异常 → 检查 logs；abort 当前窗口
- 引用: R6 runbook §8.1

#### T-15 — daemon enable
- 命令: `systemctl --user enable --now paper_v2_daemon` (operator 按实际环境调整)
- 期望: daemon active；log 无 ERROR
- 异常 → §4.7
- 引用: R6 runbook §8.4

#### T-10 — cold-start sanity `--mode=prod`
- 命令: `python scripts/paper_v2_coldstart_sanity.py --mode=prod [--sentinel-endpoint <URL>]`
- 期望: 5 phases 全 PASS（sentinel endpoint 必须已 wire by Codex Task 8 OR 通过 `--sentinel-endpoint` 覆盖）
- 异常 → §4.8
- 引用: R6 runbook §8.5

#### T-5 — GO 决策
- 操作: operator 复审 §1 决策表 + §3 全步骤 status
- 期望: 所有必需项 ✅；caveats 已知接受
- 异常 → 停 T-0；保留环境待下一窗口
- 引用: R6 runbook §10

#### T-0 — LocalSim 启动 mock 行情
- 操作: 本场景是 **LocalSim mock-only**（按用户 2026-05-11 决定，miniqmt_sim 延后）
- 命令: `python scripts/localsim_start.py --mode=mock --feed=market_data_replay`
- 期望: LocalSim 开始撮合 mock 订单；非 prod 真实交易
- 异常 → 进入 §5 监控；必要时停 LocalSim

---

## §4 — 异常 abort 决策树

### §4.1 DR snapshot 失败
- Symptom: 脚本 exit ≠ 0 或 `r6_dr_snapshot_verified.json` 缺失
- Action: 检查 prod DB 连接 + 磁盘空间。若 prod DB 不可达 → 全程 abort，下次窗口
- Rollback: 无需（未触 prod 写）

### §4.2 strategy_package backfill 失败
- Symptom: `--apply` 异常 / per-package txn rollback
- Action: operator 检查 audit log（`qe_archive.audit_log` 最近 N 条），定位失败 package_id
- Rollback: per-package txn 自动 rollback；已 commit 的 package 保留（部分推进可接受）

### §4.3 protected_asset_ledger backfill 失败
- Symptom: executor exit ≠ 0
- Action: 检查 audit log + ledger 表当前行数
- Rollback: per-row txn 自动 rollback；联系 Codex 评估是否需要清理已 backfill 数据

### §4.4 6 migrations apply 失败
- Symptom: `alembic upgrade` 某一步 ERROR
- Action: 立即对失败的 migration 执行 `alembic downgrade -1`；停止后续 migration
- Rollback: 该 migration revert；联系 Codex；abort cutover

### §4.5 R6 merge 失败
- Symptom: CI 失败 / merge conflict
- Action: 战略 session 联系 Codex；不重试 merge
- Rollback: 无（未 merge）；本窗口 abort

### §4.6 baseline post-R6 FAIL
- Symptom: 任何 session RED
- Action: revert R6 merge OR identify caveats（是否操作性 RED vs 阻断 RED）
- Rollback: `git revert` R6 squash commit；CI re-verify

### §4.7 daemon enable 失败
- Symptom: port 占用 / config 错误 / daemon log ERROR
- Action: 检查 port 8001/其他占用；review daemon config
- Rollback: `systemctl --user disable --now paper_v2_daemon`；abort T-0

### §4.8 cold-start sanity NO-GO
- Symptom: 5 phases 任何一个 FAIL
- Action: 立即 abort T-0；按 phase 错误信息排查（sentinel / governance / data freshness 等）
- Rollback: 无 prod 写入；停 daemon；联系 Codex

---

## §5 — 9:30 之后监控点（前 30 分钟）

### 5.1 daemon PID 存活
- 命令: `ps aux | grep paper_v2_daemon`（operator 视环境调整）
- 期望: 至少 1 个进程
- 异常: PID 丢失 → 立即 restart daemon + 检查 crash log

### 5.2 `paper_v2.fills` 增长率
- 命令: `psql "<OPERATOR_INPUT_PROD_CONN>" -c "SELECT COUNT(*) FROM paper_v2.fills WHERE created_at >= NOW() - INTERVAL '5 min';"`（每 5 分钟跑）
- 期望: > 0 (LocalSim 模拟撮合产生 fills)
- 0 fills 持续 10 min: 检查 daemon + market_data feed

### 5.3 `qe_archive.outbox_event` 无积压
- 命令: `psql "<OPERATOR_INPUT_PROD_CONN>" -c "SELECT COUNT(*) FROM qe_archive.outbox_event WHERE consumed_at IS NULL;"`
- 期望: < 100（handler 正常消费）
- 增长趋势 > 50/min: 联系 dw-foundation

### 5.4 governance `enable_paper` 实时
- 命令: `psql "<OPERATOR_INPUT_PROD_CONN>" -c "SELECT package_id, package_status, last_transition_at FROM strategy_pkg.package WHERE package_status='PAPER_ENABLED';"`
- 期望: 至少 1 个；`last_transition_at` 应稳定
- 异常 transition 频繁: 联系 Codex / 检查 governance gate

---

## §6 — 引用

- R6 prod apply runbook: `docs/operations/r6_prod_apply_runbook_20260511.md`（codex branch c2352a9，主线 R6 merge 后到 main）
- 2 prod executor scripts:
  - `scripts/strategy_package_governance_evidence_backfill_prod_executor.py`（2fb81b3, verify c2ef5f5）
  - `scripts/protected_asset_ledger_backfill_prod_executor.py`（2866f66, verify 94242c1）
- Coldstart sanity: `scripts/paper_v2_coldstart_sanity.py`（c2352a9, verify 1dc2e60）
- E2E cutover wrapper（Codex Task 7, MED）: 引用待 Codex deliver 后补
- Baselines: `e8ffbdd`（Stage 6 post-R5 GREEN）/ `7c18a1d`（codex branch YELLOW）/ `60ee470`（fix round）
- 上游 prep verify: `1acc15f`（b976c23 prep scripts READY-WITH-CAVEATS dev-only）

---

**END OF CHEATSHEET** — copy-paste-friendly; 不替代 R6 runbook 完整描述
