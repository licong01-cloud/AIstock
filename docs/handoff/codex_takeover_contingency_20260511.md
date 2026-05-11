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
