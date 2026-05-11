# Branch Convergence Strategy — Sprint 2026-05-11

**Author**: Claude Code 战略 session 2026-05-11
**Status**: AUTHORITATIVE — 用户 2026-05-11 已认可分支收敛需求
**Trigger**: 当前 9 个活跃分支，merge to main 后逐步收敛到 ≤3 个

## §1 当前分支盘点（git fetch + for-each-ref 2026-05-11）

| 分支 | HEAD | 最近 commit | 状态 |
|---|---|---|---|
| `origin/main` | `e55a02f` | docs(protocol) v2 §10 | 基础 |
| `origin/claude/dw-foundation-20260510` | `7912b9f` | T14b/c r3 | Codex r3 PASS ✅ |
| `origin/claude/pipeline-foundation-20260510` | `d84d3eb` | Stage 7.3 | Codex BLOCKED 3 P1 🚨 |
| `origin/claude/paper-v2-vnpy-mvp-20260508` | `91643f7` | T13 routing_class | Codex T13 PASS ✅ |
| `origin/claude/frontend-pipeline-pages-20260511` | `401cb67` | Stage 7.1 part 1 | 2/4 页面完成 |
| `origin/codex/qe-governance-integration-20260509` | `d1ca0ba` | Q1+Q2 governance gate | 等 4 packages evidence backfill 后合 main |
| `origin/codex/financial-distress-rerank-20260508` | （独立）| screen financial distress | Codex 独立 event 工作面 |
| `origin/codex/hmm-sector-regime-20260509` | （独立）| HMM sector regime | Codex 独立 HMM 工作面 |
| `origin/fix/rl_execution_module_visibility-20260510` | `6275e9d` | rl_execution import fallback | 已被 codex/governance 吸收 (per REV-8) |

**总计**: 9 个活跃分支。

## §2 收敛目标

**短期 (Sprint 完成时, ~2026-05-20)**: 5 个分支
- main (含所有 Sprint 2026-05-11 工作)
- codex/qe-governance-integration (待 evidence backfill)
- codex/financial-distress-rerank (Codex 独立)
- codex/hmm-sector-regime (Codex 独立)
- claude/<下一 Sprint worktree>（按需新建）

**中期 (~2026-06-01)**: ≤3 个分支
- main
- codex/governance-integration (合 main 后删除)
- 当前活跃 Sprint worktree

**长期 (~2026-07-01)**: ≤2 个分支 + 临时 fix branches
- main
- 临时 fix branches (PR + 立即合 + 删除)

## §3 收敛路径

### Phase 1: Sprint 2026-05-11 完成（5-9 天，per playbook R1-R4）

**R1 pipeline-foundation 合 main**（最低风险）
- 触发: Stage 7.3 fix round PASS + Stage 7.4 完成 + Codex 双向 review PASS
- 内容: bugs registry + MCP server + CI workflow + DR + nox 扩展 + data quality + 35 BUG JSON entries
- 合后: `claude/pipeline-foundation-20260510` 删除

**R2 frontend-pipeline-pages 合 main**
- 触发: Stage 7.1 4/4 页面完成 + Codex review PASS
- 内容: qe-archive + market-regime + rl-execution + strategy-package-governance UI + tests
- 合后: `claude/frontend-pipeline-pages-20260511` 删除

**R3 dw-foundation 合 main**
- 触发: T14b/c r3 PASS（已）+ Stage 7.2 cross-module E2E PASS + Codex 全 review PASS
- 内容: T12 22 张表 + T14a/b/c handlers + Batch A/C scripts + T16 regime_label cron + completion marker
- 合后: `claude/dw-foundation-20260510` 删除
- prod DB 上 apply T12 + ALTER TABLE archive_complete marker（用户授权 + DR snapshot）

**R4 paper-v2 合 main**
- 触发: R1+R2+R3 完成 + paper-v2 INT 全 PASS + 4 prod packages evidence backfill 评估完成
- 内容: T5/T6.1/T6.2 capture + daemon outbox + T13 routing_class + 9cd4c9b enable_paper invariants + 4528a32 409 mapping + adb362e/2b9c7ac docs
- 合后: `claude/paper-v2-vnpy-mvp-20260508` 删除
- prod backend 8001 重启（用户操作）

**Codex governance branch**（等 evidence backfill 后单独合）
- 触发: 4 prod packages stability_evidence + protected_asset_ledger 补齐 + 用户单独授权
- 内容: Q1 enable_paper hard gate + Q2 409 mapping + BUG-023 atomicity + 6 governance migrations
- 合后: `codex/qe-governance-integration-20260509` 删除
- prod DB 上 apply governance 6 个 migrations（用户授权 + DR snapshot）

**fix/rl_execution_module_visibility-20260510**: 已被 codex/governance 吸收，可独立合 main 或废弃。推荐 **废弃**（governance merge 时一起带过来）。

### Phase 2: Codex 独立工作面（按 Codex 自驱时序）

- `codex/financial-distress-rerank-20260508`: 等 Codex 完成 financial distress 信号筛选工作后单独合
- `codex/hmm-sector-regime-20260509`: 等 HMM sector regime work 完成后单独合

战略 session 不主动协调这两个分支，按 Codex peer 自驱节奏。

### Phase 3: 长期 fix branches 政策

未来所有 fix 直接：
1. 从 main 创建临时 branch `fix/<topic>-<YYYYMMDD>`
2. fix + commit + push
3. 立即 PR + review + merge main
4. 删除 fix branch（不留长期）

避免 fix branches 长期累积（如 fix/rl_execution_module_visibility 这种）。

## §4 反对意见 / 风险

### 风险 1: R3+R4 同步 merge 带来大量代码变更
- 缓解: 严格按 playbook R1→R3→R4 顺序，每步 monitoring + rollback ready

### 风险 2: Codex governance branch 长期不合
- 缓解: 4 packages evidence backfill 是单独 workitem，需要用户驱动
- 容忍: governance branch 可保留至 2026-06 中下旬，但不能更久

### 风险 3: 多团队工作面合并冲突
- 缓解: protocol v3 §3 规则 6 严禁 team ↔ team 直接通信
- 战略 session 协调所有 cross-branch 改动

## §5 monitoring + verification

每次 merge 后：
1. CI workflow (GitHub Actions) 跑 L0 + L2 + L3 全套
2. nightly DR snapshot
3. monitoring dashboard (Validation Center) + manual smoke

每次 merge 失败：
1. `git revert <merge-commit>` + push
2. 自动 BUG 入库 (CI failure → ci_register_failure_as_bug.py)
3. 战略 session 协调 fix round

## §6 时间表

```
2026-05-11 (今天):    9 分支
2026-05-12 ~ 14:      Stage 7 全绿
2026-05-15 ~ 17:      R1 + R2 合 main (pipeline + frontend)
2026-05-17 ~ 19:      R3 合 main (dw-foundation) + T12 prod apply
2026-05-19 ~ 21:      R4 合 main (paper-v2) + backend 重启
2026-05-22 ~:         5 分支 (main + governance 等 + 2 Codex 独立 + 新 sprint)
2026-06-01:           ≤3 分支
2026-07-01:           ≤2 分支 + 临时 fix
```

## §7 References

- production_rollout_playbook_20260511.md
- cross_tool_communication_protocol_v3_20260511.md
- morning_status_20260511.md
