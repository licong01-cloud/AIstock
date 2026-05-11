# Codex 接手 Sprint 2026-05-11 应急 Handover

**作者**: Claude Code 战略 session 2026-05-11
**状态**: CONTINGENCY-READY — 当前不转交，预案就绪
**触发条件**: Claude Code token 耗尽或战略 session 不可用

## §1 当前 Sprint 进度快照（2026-05-11 ~15:55）

```
代码实施完成:        ████████████████████ 100%
Codex review pass:   ████████████████████ 100%
R0-R6 merge 进度:    ████████████████░░░░  73%
  R0 cherry-pick rl_execution    ✅ done
  R1 pipeline-foundation         ✅ merged + branch deleted
  R2 frontend-pipeline-pages     ✅ merged + branch deleted
  R3 factor-emit-hook            ✅ merged + branch deleted
  R4 dw-foundation               ✅ merged + branch deleted
  R5 paper-v2                    ⏸️ 待 baseline 重跑 GREEN
  R6 codex/qe-governance         ⏸️ 待 prep scripts dry-run + 4 BUG-PREP fix + evidence backfill

main HEAD: 2d1f820 (R4 dw merge tip)
活跃分支: 4 (paper-v2, codex governance, financial-distress, hmm)
冷僵分支: 3 (codex prod-readonly-preflight, qe-phase-2-asset-ledger, qe-phase-3-paper-retest-gate)
```

## §2 Codex 接手能做什么

### ✅ 完全可以做（自驱已具备授权）

1. **review Claude Code 已 deliver 工作**
   - 当前 inbound: paper-v2 baseline + Codex prep scripts review (paper-v2 已自验)
   - 后续 R5 paper-v2 PR 时再次 review

2. **修自己 governance branch 代码**
   - paper-v2 review 发现 4 BUG-PREP (M:2 L:2) on 924d717
   - Codex 已收到 dispatch (drawer 98b342a6)，自驱修复

3. **dry-run prep scripts on dev DB**
   - `governance_production_apply_plan.py`
   - `strategy_package_governance_evidence_backfill_plan.py`
   - Codex 已收到 dispatch，自驱执行

4. **执行 R6 evidence backfill on prod DB**
   - 需用户明确授权 prod DB write
   - 但实际操作 Codex 可独立运行 backfill script

5. **自驱完成 Phase 4-7 governance / HMM / financial-distress**
   - 这些都在 codex/* 分支，Codex 自己 owner

### ⚠️ 需要 strategy session 或用户辅助

1. **执行 main merge (R5/R6/...)**
   - main 是 shared resource
   - Codex 应避免主动 merge main (per 协议)
   - 需 strategy session 协调 + 用户授权门

2. **派发新任务给 Claude Code worktrees**
   - 实际：Claude Code worktree sessions 不 auto-poll mempalace
   - **用户必须人工中继派发文本到 worktree 窗口**
   - Codex 写好 dispatch text 后，用户复制粘贴

3. **决定 R1-R6 顺序 / 风险等级**
   - 已在 playbook v2 文档化
   - Codex 应严格遵守 R1-R6 顺序，不可重排

4. **更新 BUG 注册表 + dual-party verify**
   - Codex 可独立写 BUG-AUDIT-XXX 入 pipeline-foundation/tests/aistock_validation/bugs/ → 但 pipeline-foundation branch 已合 main + 删除
   - **现在 BUG entries 直接 commit 到 main** 即可

5. **写 docs/handoff/ 进度文档**
   - 战略 session 风格的总结报告
   - Codex 可写但风格可能不同

### ❌ 不应做的事

1. **prod DB write 不经用户授权**
2. **prod backend 8001 / frontend 3000 启动**
3. **删除 main 上 commit / force push main**
4. **修改 Claude Code worktree 代码（已合并 branch 已删，但避免触碰未合 paper-v2 worktree）**

## §3 接手切换清单（Codex 醒来后第一时间读）

按顺序读以下文档（每个 < 10 min）:

1. `docs/handoff/codex_takeover_contingency_20260511.md` — 本文档（接手入口）
2. `docs/operations/production_rollout_playbook_v2_20260511.md` — R1-R6 rollout 计划
3. `docs/handoff/morning_status_20260511.md` — Sprint 全程状态
4. `docs/process/cross_tool_communication_protocol_v3_20260511.md` — 协议
5. `docs/process/branch_convergence_strategy_20260511.md` — 分支收敛
6. `docs/process/codex_write_task_framework_20260511.md` — Codex 写任务边界
7. `tests/aistock_validation/bugs/` — 41 BUG entries 索引

mempalace 端：调 `mempalace_list_drawers(wing="cross-tool", room="codex-claude-coord", limit=20, offset=200)` 看最新协调状态。

## §4 当前正在进行 / 等待项

| 任务 | 持有方 | ETA | 接手指示 |
|---|---|---|---|
| paper-v2 Stage 6 baseline 重跑 | paper-v2 worktree | ~30 min | 等 deliver → 决定 R5 GO/NO-GO |
| Codex dispatch 任务 (Task 1 dry-run + Task 2 fix 4 BUG-PREP) | Codex | ~1-2h | Codex 自驱推进 |
| R5 paper-v2 merge | strategy session / user | baseline GREEN 后 | 命令在 playbook v2 §R5 |
| R6 governance merge | user 授权 | evidence backfill 完成后 | 命令需新写 (playbook v2 草稿) |
| 4 packages stability evidence backfill | Codex + user | 等 prep scripts dry-run + 用户授权 prod DB | Codex 写 backfill script 在 codex/qe-governance |
| paper-v2 baseline doc 已 commit (21c6dd7) | done | — | 已收 review |

## §5 R5 merge 命令（接手后可执行）

需用户授权（R1-R4 都是用户授权战略 session 执行）:

```bash
cd F:/Dev/AIstock
git checkout main
git pull origin main

git log --oneline origin/claude/paper-v2-vnpy-mvp-20260508 ^origin/main | head -15

git merge --no-ff origin/claude/paper-v2-vnpy-mvp-20260508 \
  -m "merge: R5 paper-v2 Sprint 2026-05-11 (T5/T6.1/T6.2 capture + daemon outbox + T13 routing_class + INT 5/5 + audit)"

# Verify
ls backend/services/paper_trading_v2/  # daemon emit + capture
grep -n "intended_price\|fill_market_context" backend/services/paper_trading_v2/*.py | head -5

git push origin main
git push origin --delete claude/paper-v2-vnpy-mvp-20260508
```

## §6 R6 merge 命令（最后阶段，需用户 + Codex 双方授权）

```bash
# 前置条件：
# - Codex 4 BUG-PREP fix delivered + Codex own verify
# - Codex prep scripts dry-run JSON outputs reviewed
# - 4 prod packages stability evidence + protected_asset_ledger backfill 执行完成
# - 用户单独授权 prod DB governance migrations apply

cd F:/Dev/AIstock
git checkout main
git pull origin main

git merge --no-ff origin/codex/qe-governance-integration-20260509 \
  -m "merge: R6 codex/qe-governance Sprint 2026-05-11 (Q1 enable_paper strict gate + Q2 409 + 6 migrations + audit fixes + prep scripts)"

git push origin main

# prod DB apply 单独操作（不在 git merge 内）
# - DR snapshot first
# - 然后用户授权 governance migrations apply 到 prod 5432
# - 监控 + rollback ready

git push origin --delete codex/qe-governance-integration-20260509
```

## §7 关键 BUG / fragile areas

接手 session 应特别注意:

1. **BUG-008 partial-fix** — `session_day.data_quality` derive 在 sparse capture 下 under-report。worker enable 前需补 source schema actual_bar_count 列
2. **BUG-009..011 schema follow-up** — cash_ledger/reset_audit/session_day handler synthesize, 长期 plan
3. **BUG-032..034 regime_label P2** — 6m calendar / weekday backfill / cron prod ops review
4. **BUG-AUDIT-001/002/003** — Codex 1462099 已 fix Claude verified ✅
5. **BUG-PREP M:2 L:2** — paper-v2 review 924d717 发现, Codex dispatch 修复中

## §8 沟通协议（接手必读）

v3 协议: **drawer 短摘要 + detail_doc reference**
- drawer < 800 字符
- detail_doc 在 `docs/cross_tool/<YYYYMMDD>_<sender>_<verb>_<topic>.md`
- Claude Code worktree teams 不 auto-poll mempalace → 用户人工中继

如果 Codex 接手后给 Claude Code worktree 派任务:
- 写 detail_doc 到 docs/cross_tool/
- 写短 drawer ref detail_doc
- **必须通过用户中继**到对应 worktree 窗口
- 用户用 §6 风格 "═══ 复制以下内容给 <team> ═══" 边界格式

## §9 contingency activation criteria

Codex 应在以下情况开始 takeover:

1. 用户明确通知"Claude Code token 已耗尽"
2. 战略 session drawer 24h+ 无新增 + 用户授权
3. Critical 卡点 (R6 evidence backfill / prod rollout) 需推进 + 战略 session 不可用

## §10 当前 4 个活跃分支处置建议

| 分支 | Codex 接手后处理 |
|---|---|
| `claude/paper-v2-vnpy-mvp-20260508` | 等 baseline GREEN → 用户授权 → R5 merge → 删除 |
| `codex/qe-governance-integration-20260509` | Codex 自驱完成 prep + evidence backfill → R6 merge → 删除 |
| `codex/financial-distress-rerank-20260508` | Codex 自驱推进，merge timing 由 Codex 自决 |
| `codex/hmm-sector-regime-20260509` | 29h idle, Codex 评估是否继续或归档 |

3 冷僵分支可清理：
- `codex/qe-governance-prod-readonly-preflight-20260509`
- `codex/qe-phase-2-asset-ledger-20260509`
- `codex/qe-phase-3-paper-retest-gate-20260509`

## §11 接手不丢失上下文的最小集

如 Codex 仅读 1 份文档，读 `docs/handoff/morning_status_20260511.md` + 本文档 §1-§7。

如读 5 份: 加 playbook v2 + protocol v3 + branch convergence + 当前 task tracker。

如时间充足: 全 §3 七个文档 + 最新 cross-tool drawers + tests/aistock_validation/bugs/。

## §12 References

- `docs/operations/production_rollout_playbook_v2_20260511.md`
- `docs/process/cross_tool_communication_protocol_v3_20260511.md`
- `docs/process/branch_convergence_strategy_20260511.md`
- `docs/process/codex_write_task_framework_20260511.md`
- `docs/handoff/morning_status_20260511.md`
- mempalace cross-tool wing: 328 drawers
- BUG registry: tests/aistock_validation/bugs/ (41 entries)

---

## Update Log

### 2026-05-11 16:30 — Codex 完成 paper-v2 dispatched 2 tasks

- Codex commit `7bf840d` on codex/qe-governance-integration-20260509
- 4 BUG-PREP (M:2 L:2) 全 fix + 4 dry-run JSON outputs PASS
- 37 tests + 0 guardrail findings
- prod_touched=false

R6 governance merge prep:
- ✅ prep scripts (924d717)
- ✅ 4 BUG-PREP fix (7bf840d, dry-runs included)
- 🔄 paper-v2 verify Codex fix (dispatched, ETA 30-60 min)
- ⏸️ 4 prod packages evidence backfill (待 verify + 用户授权)
- ⏸️ R6 merge (待 evidence backfill + 用户授权)

### Codex 当前状态变化
- 主要 dispatched tasks 完成
- 现可 self-driven: HMM / financial-distress / governance Phase 4-7
- 等 paper-v2 verify deliver → R6 merge 评估

Strategy session 继续主导，contingency 未激活。

### 2026-05-11 16:43 — paper-v2 verify Codex 7bf840d = PASS

- paper-v2 commit `ee2e56f` on claude/paper-v2-vnpy-mvp-20260508
- Verify doc: `docs/cross_tool/20260511_paper_v2_VERIFY_codex_governance_prep_fixes.md`
- Drawer ref: `36b484c9` (cross-tool / codex-claude-coord)
- 结果:
  - BUG-PREP-001 (M exit codes): PASS
  - BUG-PREP-002 (M gate rename): PASS
  - BUG-PREP-003 (L fail-close): PASS
  - BUG-PREP-004 (L apply-plan tests): PASS
  - JSON safety invariants: OK (10 files, status=passed, ddl=false, db_writes=false)
  - Regressions: none
  - R6 readiness: READY (prep layer only; DDL exec + evidence writer remain separate gates)

### R6 governance merge prep 进度更新
- ✅ prep scripts (924d717)
- ✅ 4 BUG-PREP fix + dry-runs (7bf840d)
- ✅ paper-v2 verify Codex fix (ee2e56f, drawer 36b484c9)
- ⏸️ 4 prod packages evidence backfill (待用户授权 prod DB write)
- ⏸️ R6 merge (待 evidence backfill + 用户授权)

### 2026-05-11 ~16:55 — 派 paper-v2 baseline post-R4 重跑

- main HEAD: 4a3fa60 (R4 + handoff + hotfix)
- 目的: R5 merge 前最后 gate, 验证 main GREEN + R4 内容(qe_archive_backend / e2e / paper_v2_l3)
- 期望: GREEN ≥8 sessions + 5 SKIP (UI service-policy)
- 输出: BASELINE post-R4 doc on paper-v2 branch
- drawer: 500f98cb (cross-tool / codex-claude-coord)

### 2026-05-11 ~17:00 — 派 Codex 2 任务并行

drawer: d370a52f (cross-tool / codex-claude-coord)
detail_doc: docs/cross_tool/20260511_strategy_DISPATCH_codex_evidence_backfill_prep.md

- Task 1 HIGH: R6 evidence backfill script prep + dev dry-run
  - branch: codex/qe-governance-integration-20260509
  - scripts/strategy_package_evidence_backfill.py + scripts/protected_asset_ledger_backfill.py
  - --dry-run/--apply 双模式, dev DB SELECT-only preview
  - 2 test files + 4 JSON outputs
  - SLA 2-3h
  - 不 apply prod, 不 INSERT dev, 不 merge codex branch

- Task 2 MED: main hotfix audit (post-R4)
  - 范围: module_registry.yaml dedup + test_cross_table_consistency.py skip
  - 验证 R0-R6 协议合规 + 不丢 metadata + skip 不掩盖真实回归
  - 输出 <200 words drawer, 必要时 BUG-AUDIT-XXX
  - SLA 30 min

Codex 接手 contingency 未激活, 战略 session 继续主导。

### 2026-05-11 ~17:08 — paper-v2 baseline post-R4 = GREEN

- commit `535c539` (drawer `926b68f0`)
- verdict: GREEN, 11G/0F/5SKIP UI/1 nox-skip (model_registry_backend)/0 MISS
- hotfix Fix A verified: validation_module_registry_l0 8 passed, 12 mapped
- hotfix Fix B verified: data_quality_deep 10p/21s, archive-empty SKIP per D5 Q2.c
- R4 (T12/T14) verified: qe_archive_backend 70 passed, qe_archive_data_quality 27/27 tables
- **R5 readiness: GO**

### 2026-05-11 ~17:11 — Codex Task 2 hotfix audit = PASS

- drawer `b3d63611`
- PASS, no BUG-AUDIT findings, 协议合规 OK

### 2026-05-11 ~17:51 — Codex Task 1 R6 backfill prep = COMPLETE

- commit `b976c23` (drawer `d5816559`)
- 2 scripts + 2 test files + 4 dry-run JSON outputs
- 17 pytest passed + 54 governance smoke passed
- guardrail 0 P1 findings
- negative safety check PASS (反 target_db/port/dbname 误用)
- production_touched=false, services_touched=false, main_merged=false

### 2026-05-11 ~18:00 — 派 paper-v2 verify Codex b976c23

- drawer `013ab7f7`
- 4-layer audit: static / 17 tests / dry-run JSON / R6 semantic
- KEY check: --apply mode 是否有 confirmation step before prod INSERT
- SLA 60 min (实盘目标驱动)
- 输出 verdict: READY / CAVEATS / BLOCKED

### 2026-05-11 ~18:05 — 派 Codex Task 3 R6 prod apply runbook

- drawer `5dd13e99`
- 目标: 写 `docs/operations/r6_prod_apply_runbook_20260511.md`
- 含 preflight / DR snapshot / backfill --apply / 6 migrations / R6 merge / daemon enable / cold-start / rollback / 9:30 cutover / time budget
- 与 paper-v2 verify 并行
- SLA 1.5h

### 实盘目标 — 明早 9:30 A股开市

路径 A 时间线（今晚完成 prod 配置）:
- 19:00 paper-v2 verify deliver
- 19:00 R5 merge (用户授权后)
- 19:15 DR snapshot
- 20:00 evidence backfill --apply
- 22:00 6 migrations apply
- 23:00 R6 merge
- 23:30 prod backend + daemon enable + cold-start
- 明早 9:30 实盘 ✅

### 2026-05-11 18:20 — R5 paper-v2 merged to main ✅

- merge commit: `3cfe10f`
- pushed to origin/main
- claude/paper-v2-vnpy-mvp-20260508 remote branch 已删除
- conflict 解决: .gitignore (合并 exception lists + 注释)
- 97 files changed, 16296 insertions(+), 203 deletions(-)
- 验证: backend/services/paper_trading_v2/repository.py 含 intended_price/fill_market_context/created_at/updated_at capture

### 2026-05-11 ~18:25 — 派 paper-v2 baseline post-R5 (流水线验证 R5)

- drawer `9263d55b`
- 用户明确要求: 所有功能必须经流水线验证 → R5 入 main 后必须跑 baseline 才能进 R6 prod
- target: main HEAD 3cfe10f, expect GREEN ≥ 11 sessions
- 重点: paper_v2_backend (T5/T6/INT) + paper_v2_l3 (daemon outbox) + qe_archive_backend (T14a)
- 输出: docs/baseline/stage6_baseline_post_r5_20260511.md on 新 branch
- SLA 60 min

### 当前并行任务 (18:25 起)

| Task | 持有方 | drawer | SLA |
|---|---|---|---|
| baseline post-R5 流水线验证 | paper-v2 | 9263d55b | 60 min |
| verify Codex b976c23 backfill scripts | paper-v2 | 013ab7f7 | 60 min |
| R6 prod apply runbook | Codex | 5dd13e99 | 1.5h |

paper-v2 端 2 任务可串行 (推荐先 baseline 再 verify) 或并行。

### R6 prod ops 流水线验证补充

用户要求: R6 merge 后也必须跑流水线验证。新增计划:
- R6 merge 后立即派 paper-v2 baseline post-R6
- baseline GREEN 后才能 prod backend 8001 启动 + daemon enable
- daemon enable 后跑 cold-start sanity (模拟单 round-trip)
- sanity PASS 后才能进 9:30 实盘

时间线推迟约 30-60 min: 实盘从 23:30 cold-start 推迟到 00:00-00:30 范围, 明早 9:30 开市仍可行 (用户睡眠时间被压缩)。

### 2026-05-11 ~18:30 — 三方 deliver

| Source | Verdict | 关键发现 |
|---|---|---|
| paper-v2 verify b976c23 (drawer 9a2668d5, commit 1acc15f) | READY-WITH-CAVEATS | scripts dev-locked, prod 需 separate entrypoint |
| baseline post-R5 v1 (drawer b203c431, commit 779e904) | YELLOW 13G/3F/14SKIP | 3 fails 全 env-only (psycopg2 no-password); 0 R5 code regression |
| Codex R6 runbook (drawer 09cd1a6c, commit 55ac10d) | COMPLETE | 明确警告 backfill scripts 不可 apply prod, 无 prod-capable executor by 09:00 → R6 NO-GO |

### 🚨 关键阻断点: backfill scripts dev-locked, 无 prod path

实盘 BLOCKER. Codex Task 1 的 negative safety check 是 by-design (反 prod 误用), 但反过来阻断了真 prod backfill.

### 2026-05-11 ~18:40 — 战略 session cherry-pick docs 到 main (R0 风格)

- main HEAD: `30879c2` (bdcdb4b verify doc + 30879c2 runbook doc)
- `claude/paper-v2-vnpy-mvp-20260508` remote branch 再次删除 (paper-v2 worktree 不应再 push 到此 branch)

### 2026-05-11 ~18:35 — 派 Codex Task 4 + paper-v2 双任务

| Task | 持有方 | drawer | SLA |
|---|---|---|---|
| Codex Task 4: Prod-capable evidence backfill executor | Codex | d1c285d3 | 1.5h |
| paper-v2 Task A: 推 verify commit (实际已 done, 战略 cherry-pick 后 paper-v2 可跳过) | paper-v2 | 2ba8573b | done |
| paper-v2 Task B: baseline RE-RUN with env | paper-v2 | 2ba8573b | 45 min |

### R6 prod ops 路径 (含新阻断)

- ⏸️ 等 Codex Task 4 deliver: prod executor ready
- ⏸️ 等 paper-v2 baseline v2 GREEN: 流水线验证 R5 in main 无 env-fail
- ⏸️ 用户 prod DR snapshot + 授权
- ⏸️ Codex prod executor `--apply` on prod
- ⏸️ 6 migrations apply prod
- ⏸️ R6 git merge
- ⏸️ paper-v2 baseline post-R6 (流水线验证 R6 in main)
- ⏸️ prod backend 8001 + daemon enable + cold-start sanity
- ⏸️ 9:30 实盘

### 2026-05-11 ~18:50 — baseline v2 post-R5 = GREEN ✅

- commit `e8ffbdd` (drawer `01984455`)
- env-fix 3/3 flipped: paper_v2_data_quality + qe_archive_data_quality + local_data_management_audit
- 16G/0F/14SKIP-UI/1NOX-SKIP, paper_v2_backend 264p, qe_archive_backend 70p
- R6 GO for 9:30 实盘

### 2026-05-11 ~19:28 — Codex Task 4 strategy_package prod executor = COMPLETE (partial)

- commit `2fb81b3` (drawer `abafc500`)
- scripts/strategy_package_governance_evidence_backfill_prod_executor.py
- 24 executor tests + 53 total passed
- 5-guard: token + 2 envs + mutex + target_db/port + DR snapshot ref + plan preview sha + operator confirmation
- runbook §7.2 aligned
- guardrail 0 P1, P2 ALGO-COMPLEXITY documented as bounded
- **residual**: protected_asset_ledger prod executor 未做 (Codex 标 placeholder)

### 2026-05-11 ~19:40 — 战略 cherry-pick baseline v2 doc 到 main

- main HEAD: `c515cf4` (e8ffbdd cherry-pick)

### 2026-05-11 ~19:35 — 派 Codex Task 5 + paper-v2 verify (并行)

| Task | 持有方 | drawer | SLA |
|---|---|---|---|
| Codex Task 5: protected_asset_ledger prod executor (补缺) | Codex | c48d8347 | 60 min, ~20:30 deliver |
| paper-v2 5-layer verify Codex 2fb81b3 | paper-v2 | 3e7fc4de | 60 min, ~20:35 deliver |

### 2026-05-11 ~19:56 — paper-v2 verify 2fb81b3 strategy_package = READY

- commit `c2ef5f5`, drawer `1d75214d`
- L1-L5 全 PASS (8-guard fail-fast, 24/24 tests, JSON deterministic, runbook §7.2 9 fields match, ALGO bounded)
- **R6 prod GO**

### 2026-05-11 ~20:18 — Codex Task 5 protected_asset_ledger prod executor = COMPLETE

- commit `2866f66`, drawer `b113a7a2`
- 33 executor tests + 57 paired + 86 broader passed
- 0 P1, 7 P2 ALGO bounded helper loops
- runbook §7.3 + 2 dry-run JSON

### 2026-05-11 ~20:25 — 战略 cherry-pick verify 2fb81b3 doc 到 main + 派 verify 2866f66 + Codex Task 6

| Action | drawer/commit |
|---|---|
| main cherry-pick c2ef5f5 → `3435f21` | — |
| 派 paper-v2 verify 2866f66 | drawer `009e23d7` |
| 派 Codex Task 6 coldstart sanity automation | drawer `87c8f58a` |

### 2026-05-11 ~21:08 — paper-v2 verify 2866f66 protected_asset_ledger = READY

- commit `94242c1`, drawer `979e62d8`
- L1-L5 全 PASS (8-guard, 33/33 tests + sister 24/24 regression, JSON det bundle_sha 47b48f72, runbook §7.3 5/5 + §7.2/§7.3 pair-consistent, 7 P2 bounded P=4 plan-gated all SQL bounded)
- **R6 ledger GO=YES. Combined w/ c2ef5f5: full R6 GO**

### 2026-05-11 ~21:39 — Codex Task 6 coldstart sanity automation = COMPLETE

- commit `c2352a9`, drawer `ecf4adeae`
- 30 tests + 87 with prod executors regression
- guardrail 0 P1, P2 ALGO bounded
- runbook §8.5
- caveat: `/paper-v2/coldstart-sanity/sentinel-order` 端点必须对应 approved prod paper-v2 entry (paper-v2 verify L5 验)

### 2026-05-11 ~21:30 — 战略 cherry-pick verify 2866f66 doc 到 main + 派 paper-v2 双任务 + Codex Task 7

main HEAD: `f498246` (含 94242c1 cherry-pick)

| Task | 持有方 | drawer | SLA |
|---|---|---|---|
| paper-v2 Task A: codex branch baseline c2352a9 (R6 merge 前置流水线验证) | paper-v2 | 003593038 | 60 min, ~22:25 |
| paper-v2 Task B: 5-layer verify Codex c2352a9 coldstart sanity | paper-v2 | 003593038 (同 dispatch) | 60 min, 串行后 ~23:25 |
| Codex Task 7: r6_prod_cutover_e2e_wrapper.py | Codex | 206deb97 | 1.5h, ~23:00 |

### R6 readiness 状态 (21:40)

✅ baseline post-R5 v2 GREEN (R5 in main 流水线)
✅ 2 prod executors verify READY (strategy_package + protected_asset_ledger)
✅ Cold-start sanity automation deliver (c2352a9, 等 paper-v2 verify)
⏸️ codex/qe-governance branch baseline (paper-v2 Task A 进行中)
⏸️ R6 merge to main (等 branch baseline GREEN)
⏸️ baseline post-R6 (R6 in main 流水线)
⏸️ E2E cutover wrapper (Codex Task 7 进行中, MEDIUM 优先)

### 实盘时间线更新 (21:40)

```
21:40 现在 → 3 任务并行
22:25 paper-v2 Task A (branch baseline) deliver
23:00 Codex Task 7 (E2E wrapper) deliver
23:25 paper-v2 Task B (verify sanity) deliver
23:30 战略 cherry-pick verify+sanity verify docs + R6 merge (用户授权)
00:30 baseline post-R6 GREEN
01:00 用户 prod cutover (按 E2E wrapper 或 runbook 手动)
03:00 cold-start sanity PASS
09:30 实盘 ✅
```
