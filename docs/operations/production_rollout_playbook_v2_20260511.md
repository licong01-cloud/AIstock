# Production Rollout Playbook v2 — Modular Parallel + Risk-Ascending Merge

**Author**: Claude Code 战略 session
**Date**: 2026-05-11
**Status**: AUTHORITATIVE — 用户 2026-05-11 已确认 3 决策
**Supersedes**: production_rollout_playbook_20260511.md (v1, R1-R4)

## §1 三大原则（用户 2026-05-11 确认）

1. **模块化并行验证**: 各模块在自己 worktree 内并行跑流水线，不阻塞其他模块
2. **风险升序串行 merge**: 风险低的模块先合 main，高的最后
3. **每模块独立 4 步**: 代码互查 → 流水线验证 → 合 main → 用户验证

## §2 风险升序 R1-R6 顺序（取代 v1 R1-R4）

| Phase | 模块 | 分支 | 风险 | 影响 |
|---|---|---|---|---|
| **R1** | pipeline-foundation | `claude/pipeline-foundation-20260510` | 🟢 最低 | 流水线工具 + docs + tests，无 prod runtime 影响 |
| **R2** | frontend-pipeline-pages | `claude/frontend-pipeline-pages-20260511` | 🟢 低 | UI 改动，用户可见但不影响 trading |
| **R3** | factor-emit-hook | `claude/factor-emit-hook-20260511` | 🟡 中 | factor pipeline 加 emit hook，worker disabled 安全 |
| **R4** | dw-foundation | `claude/dw-foundation-20260510` | 🟠 中高 | T12 22 张表 + handlers，需 prod DDL apply |
| **R5** | paper-v2 | `claude/paper-v2-vnpy-mvp-20260508` | 🔴 高 | prod runtime 改 (capture / daemon)，需 backend 8001 重启 |
| **R6** | codex governance | `codex/qe-governance-integration-20260509` | 🔴 最高 | enable_paper 严格收紧 + governance migrations + 4 prod packages 影响 |

## §3 每模块 4 步流程

### Step 1 代码互查（双向）
- Claude side reviewer (战略 session 或其他 Claude team) audit
- Codex side audit (Codex agent)
- 输出: REVIEW doc + verdict
- BLOCKED → fix round → re-review 循环

### Step 2 流水线验证
- 该模块对应 nox sessions 全跑
- Coverage 阈值 + BUG 注册表 P0/P1 = 0
- Validation Center evidence 归档
- 输出: `docs/cross_tool/<YYYYMMDD>_<module>_RELEASE_readiness.md`

### Step 3 合 main (Rn merge)
- 用户授权门
- `git merge --no-ff origin/<branch>` + push origin/main
- GitHub Actions CI 自动跑

### Step 4 用户验证
- 用户实盘/集成 smoke
- monitoring + alerting
- 24h 观察

## §4 并行验证可行性

```
模块 X 在 worktree X 跑 nox session-X
模块 Y 在 worktree Y 跑 nox session-Y
两者共用 dev DB 5433 read-only / 写测试数据隔离
互不干扰

仅 Step 3 (merge main) 串行，因 main 是 shared resource
```

**关键**: Step 1+2 完全并行，Step 3 按 R1-R6 顺序串行。

## §5 当前各模块就绪度（2026-05-11 12:00）

| 模块 | 完成度 | Step 1 互查 | Step 2 流水线 | 阻塞 | 最快 R-N |
|---|---|---|---|---|---|
| paper-v2 | 98% | ✅ Codex T13 PASS / paper-v2 audit Codex 进行中 | ✅ 自验通过 | 仅 audit 完成 | R5 ~Day 1.5 |
| frontend | 90% | ⏳ Codex Lane C review | ✅ 自验 | 等 Codex verdict | R2 ~Day 1 |
| pipeline | 85% | 🔄 Stage 7.3 fix r2 进行 + Codex 7.4 Lane A | ✅ 自验 | fix r2 + 2 verdict | R1 ~Day 1 |
| dw-foundation | 88% | 🔄 Stage 7.2 fix round 进行 | ✅ 自验 | fix + 2 verdict | R4 ~Day 2 |
| factor-emit-hook | 60% | ⏳ Codex Lane B review | ✅ 自验 | 等 Codex verdict | R3 ~Day 1.5 |
| governance | 70% | ✅ Codex 自验 / paper-v2 audit 进行 | ⏸️ 等 audit 通过 | 4 packages evidence backfill | R6 ~Day 5-7 |

## §6 时序

```
Day 0 (今天 12:00):
  - 5 worktree 全部有任务在跑
  - Codex 4-Agent 启动 (Lane A/B/C/D)

Day 0.5 (~Day 1, 24h 内):
  - Codex Lane A/B/C verdict 到达
  - Stage 7.3 r2 + Stage 7.2 fix round deliver
  - pipeline ready (Stage 7.4 PASS) → R1 merge main
  - frontend ready (7.1 part 2 PASS) → R2 merge main (与 R1 并行验证可)

Day 1.5:
  - factor-emit-hook ready (T15 PASS) → R3 merge main
  - paper-v2 audit Codex 完成 → R5 merge main 候选

Day 2:
  - dw-foundation ready (7.2 fix + review PASS) → R4 merge main (prod DDL apply T12 + DR snapshot)
  
Day 2.5:
  - paper-v2 ready → R5 merge main + prod backend 8001 重启
  - 用户实盘 capture 验证

Day 3-5:
  - 4 prod packages evidence backfill (Codex + user)
  - governance branch ready

Day 5-7:
  - R6 governance merge main + prod governance migrations apply
  - 用户验证 strict gate

Day 7: 全 Sprint merge 完成 + monitoring 启用
```

**比 v1 估快 2 天**（因并行验证 + 模块化 merge）。

## §7 R1 pipeline merge 准备（最快 ready）

合 main 前 checklist:
- [ ] Stage 7.3 fix round 2 deliver + Codex r3 PASS
- [ ] Stage 7.4 DR validation Codex Lane A PASS
- [ ] pipeline nox -s l0 + paper_v2_backend + qe_archive_backend + validation_center_backend + data_quality_deep + dr_validate 全 green
- [ ] BUG 注册表 P0/P1 = 0 (current state OK)
- [ ] release_readiness_pipeline.md 写完
- [ ] 用户授权

R1 merge 命令（用户执行）:
```bash
cd F:/Dev/AIstock
git checkout main
git pull origin main
git merge --no-ff origin/claude/pipeline-foundation-20260510 \
  -m "merge: pipeline-foundation Sprint 2026-05-11 (Stage 1-5 + Stage 7.3+7.4 + 35 BUGs + MCP + DR + CI)"
git push origin main
git worktree remove F:/Dev/AIstock_worktrees/pipeline-foundation-20260510
git branch -D claude/pipeline-foundation-20260510  # 本地
git push origin --delete claude/pipeline-foundation-20260510  # 远程
```

## §8 Rollback 策略（不变于 v1）

详见 v1 §4。每 phase merge 前 DR snapshot，失败 `git revert <merge-commit>`。

## §9 References

- v1: `production_rollout_playbook_20260511.md`
- branch convergence: `branch_convergence_strategy_20260511.md`
- protocol v3: `cross_tool_communication_protocol_v3_20260511.md`
- handoff: `morning_status_20260511.md`
