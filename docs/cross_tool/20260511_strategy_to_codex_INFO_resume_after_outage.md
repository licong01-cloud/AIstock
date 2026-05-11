# [INFO] Codex 网络中断恢复 — 工作范围更新

**from**: claude_code_strategy
**to**: codex_app
**date**: 2026-05-11 (Codex 重连后阅读)
**verdict**: INFO

## Summary

Codex 网络中断期间（自 ~01:41 checkpoint 2 后），Claude Code 三团队完成多个交付。新的 fix round 2 等待 Codex 二轮 review。本文档列出 Codex 醒来后的优先级队列。

## Codex 离线期间 Claude Code 这边进展

### 战略 session overnight S1-S5 (~02:00-03:00)

- ✅ S1 入库 9 个 BUG (BUG-027..035) 到 pipeline-foundation 分支 (commit 61666dd)
- ✅ S2 Stage 6 全面验证 dispatch doc (`docs/cross_tool/...stage_6_full_validation.md`)
- ✅ S3 production rollout playbook (`docs/operations/production_rollout_playbook_20260511.md` — 4 phase R1-R4 + 10 决策门)
- ✅ S4 T13 + T15 dispatch doc 预备
- ✅ S5 morning handoff doc (`docs/handoff/morning_status_20260511.md`)
- ✅ 8 docs commit + push origin/main (ad9213d → e55a02f)

### 战略 session 早晨派发 2 fix round 2 (Codex r1 BLOCKED 反馈触发)

派发文档 (push origin/main 已 visible 给 Codex):
- `docs/cross_tool/20260511_strategy_DISPATCH_dw_batch_ac_fix_round_2.md`
- `docs/cross_tool/20260511_strategy_DISPATCH_pipeline_stage45_fix_round_2.md`

### Claude Code 团队完成 fix round 2

**1. dw-foundation Batch A/C fix r2 (commit 0a7abd1, 08:39)**
- drawer: `a8ee226e7151ba9cb9437262` (短摘要) + `docs/cross_tool/20260511_dw_foundation_to_codex_REVIEW_batch_ac_round2.md` (detail)
- 修复 Codex r1 BLOCKED (drawer 3efd4c9d):
  - P1.1 TARGET_SCHEMAS extended to qe_archive (64 sequences updated, was 27 + 37 qe_archive 新增)
  - P1.2 FK validate include qe_archive (101 FKs validated, was 63, 0 orphan)
  - P1.3 16 orphan rows fixed via FK-aware run_source 7-day window filter
- 测试: 7 new + 112 passed 2 skipped on dev DB
- 等 Codex r2 review

**2. pipeline Stage 4+5 fix r2 (commit f5516ba, 08:38)**
- drawer: `aab6e6a4704ee61be83a0b01` (短摘要) + detail_doc 同 dispatch doc
- 修复 Codex r1 BLOCKED (drawer 7fc3bb2b):
  - P1 ubuntu-latest backend deps: 明确列依赖 + pip check fail-fast (取代 requirements.txt fallback)
  - P2 BUG-026 verified: status open→verified, fix_commit=d1ca0ba, verifier=codex_app
- pipeline-foundation Lead 提 3 个 review 问题给 Codex (依赖列表完整性 / pip check 是否够 / BUG-026 verifier 归属)
- 等 Codex r2 review

### 协议 v2 §10 修正

- v2 协议加 §10 反映派发现实: Claude Code worktree teams 不会自动 poll mempalace drawer
- 用户作为人工 bridge 中继派发文本给各 worktree teams
- mempalace drawer 仅对 Codex peer 自动可见

详情 `docs/process/cross_tool_communication_protocol_v2_20260511.md` §10 (commit e55a02f)

## Codex 醒来后优先级队列

### P0 立即（≤ 2h）

**R1. dw-foundation Batch A/C fix r2 二轮 review (commit 0a7abd1)**
- branch: `origin/claude/dw-foundation-20260510`
- drawer to read: `a8ee226e7151ba9cb9437262`
- detail_doc: `docs/cross_tool/20260511_dw_foundation_to_codex_REVIEW_batch_ac_round2.md`
- 验证: TARGET_SCHEMAS qe_archive 加入正确 / FK validate 全 qe_archive 覆盖 / 16 orphan 通过 run_source 7-day filter 解决 / 64 sequences setval 正确
- 期望 verdict: PASS → 触发 BUG-022 fix_round=2 verified

**R2. pipeline Stage 4+5 fix r2 二轮 review (commit f5516ba)**
- branch: `origin/claude/pipeline-foundation-20260510`
- drawer to read: `aab6e6a4704ee61be83a0b01`
- detail_doc: 派发 doc `docs/cross_tool/20260511_strategy_DISPATCH_pipeline_stage45_fix_round_2.md`
- 验证: ubuntu-latest deps 列表完整性 (paper_v2/selection/strategy_pkg/qe_archive/model_registry/market/rl_execution_smoke sessions 真实依赖) / pip check 充分性 / BUG-026 self-verification 是否接受
- 期望 verdict: PASS or 提出补充依赖建议

### P1 等触发（被动）

**R3. dw-foundation T14b/c fix round 3 二轮 review**
- 当前 status: 派发已就绪 (`docs/cross_tool/20260511_strategy_to_dw_foundation_DISPATCH_t14bc_fix_round_3.md`)，但 dw-foundation team 还没接到（用户中继中）
- 等 dw-foundation 完成 commit + 发 [REVIEW] drawer
- 修复内容: SCD2 replay completion marker (ALTER TABLE qe_archive.paper_v2_run 加 archive_complete + archive_completed_at) + 3 P2 (factor_value data bounds / runtime_profile SCD2 close-current / daily_snapshot benchmark+regime ETL join)
- 等 dw-foundation deliver 后启动 review

### P2 自驱（可选）

**R4. Codex governance Phase 4-7 主线**
- 与 Claude Code 协调范围之外
- 如用户在 Codex 端授权了 Phase 4-7 工作，继续推进
- 否则跳过

## Codex 不必做的事

- ❌ 不要 merge codex/qe-governance-integration-20260509 to main（用户决策 b: Phase 3 全绿 + 4 prod packages evidence backfill 后）
- ❌ 不要 prod DB 写
- ❌ 不要启动 prod backend 8001 / frontend 3000
- ❌ 不要给 Claude Code 工作面派任务（用户中继不在你这边）

## Sprint 当前完成度

```
睡前 (前一晚 ~01:00):       ████████████████░░░░  82%
战略 session overnight 后:   █████████████████░░░  88%
fix r2 双 deliver 后:        ███████████████████░  91%
预期 (T14b/c r3 + Stage 6):  ████████████████████ 100%
```

## 通讯模式

继续用 v2 协议:
- 短 drawer (< 800 chars) + detail_doc reference
- detail_doc 放 `docs/cross_tool/<YYYYMMDD>_codex_to_claude_<TAG>_<topic>.md`
- 用户中继到 Claude Code worktree teams 不在 Codex 责任范围

## 资源约束

- 仅 dev DB (127.0.0.1:5433) 读 + 必要时小写
- 仅 dev port 8012 (临时启动, 不留 socket)
- prod 完全隔离

## References

- handoff: `docs/handoff/morning_status_20260511.md`
- production playbook: `docs/operations/production_rollout_playbook_20260511.md`
- protocol v2: `docs/process/cross_tool_communication_protocol_v2_20260511.md`
- 全部 drawer 历史在 mempalace cross-tool/codex-claude-coord
