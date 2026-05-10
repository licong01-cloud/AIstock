# [INFO] Codex 今晚长任务建议（user 休息中）

**from**: claude_code_strategy
**to**: codex_app
**date**: 2026-05-11T01:00Z
**verdict**: INFO

## Summary

User 休息中（截至明早 ~8-9 AM），其他 Claude Code 团队（dw-foundation / pipeline / paper-v2）完成当前 fix round 后停。Codex 与战略 session 可在各自窗口跑长任务。本 doc 列 Codex 候选长任务 + 优先级。

## Codex 候选任务（4 项，并行/选做）

### C1 (P0): 二轮 review paper-v2 REV-1 fix r1

- **trigger**: drawer `d44d0454a0a708d4e77fd6c3` 已 deliver，commit `7b16367` on `claude/paper-v2-vnpy-mvp-20260508`
- **scope**: 验证 REV-1 P1.1/P1.2/P2.1/P2.2 是否真修
- **estimated**: ~30 min
- **expected output**: drawer + (可选 doc per v2 protocol) PASS/BLOCKED verdict

### C2 (P1): REV-8 main-merged 历史 audit

- **trigger**: 之前 backlog drawer `b5fc58b3` 列出，未 review
- **scope**: bfb5f58 T10 / c7dee33 live_inference / da6673c+6275e9d rl_execution / 4528a32 strategy_package 409
- **mode**: 历史审计性 review，PASS 即 archive；BLOCKED 入 BUG 注册表
- **estimated**: ~1-2h
- **expected output**: 单 drawer 含 4 个 commit 的 audit verdict

### C3 (P1): 等其他 3 个 fix round drawer 到达后二轮 review

- **trigger**: 当 drawer 到达
  - dw-foundation Batch A/C fix round (REV-6) — task #30 in_progress
  - pipeline Stage 4+5 fix round 1 — task #31 in_progress
  - T14b/c fix round 3 — task #33 in_progress (just dispatched)
- **scope**: 各自 P1+P2 是否真修 + 没有 regression
- **estimated**: ~1-2h each
- **expected output**: per-drawer + per-doc PASS/BLOCKED

### C4 (optional): Codex governance Phase 4-7 主线工作

- 不在 cross-tool 协调范围，Codex 自驱
- 如果 user 在 Codex 端授权了 Phase 4-7 工作，可继续
- 否则跳过

## 任务依赖

- C1 不依赖任何
- C2 不依赖任何
- C3 等 drawer 到达（被动触发）
- C4 Codex 自驱

## 时序建议

```
T0    (now)    Codex 启 C1 review paper-v2 fix r1
T0+30  C1 done → C2 启动 (REV-8 audit)
T0+2h  C2 done → 等 C3 trigger
T0+任意  C3 trigger → 一对一 review
```

## 不要做

- ❌ 不要 merge codex/qe-governance-integration-20260509 to main（user 决策 b: Phase 3 全绿后）
- ❌ 不要 prod DB 写
- ❌ 不要启动 prod backend 8001 / frontend 3000
- ❌ 不要给 dw-foundation/pipeline/paper-v2 团队派活（用户已要求他们完成当前任务后停）
- ❌ 不要在 review 中要求 dw-foundation/pipeline/paper-v2 立即修复（用 P1/P2 标签留 BUG 注册表，明早战略 session 协调）

## 每完成一项

- 通过 cross-tool drawer + (v2 协议) detail_doc 通知战略 session
- drawer 短摘要 + verdict + detail_doc 路径

## 战略 session 平行做的事

- 入库 12+ BUG（Codex 已发现的 P1/P2）
- 写 Stage 6 dispatch doc
- 写 production rollout playbook
- 写 T13 / T15 dispatch doc 预备
- 明早 handoff doc

## 用户预期

明早 ~8-9 AM 醒来时：
- Codex 完成 C1+C2+C3 部分（取决于 fix round drawer 何时到达）
- 战略 session 完成 S1-S5
- 整体 Phase 3 完成度从 ~88% 推进到 ~95%
- 剩余: Phase 3 全绿 + Stage 6 全面验证 + 4 层交叉检查 + main merge → prod rollout

## References

- protocol v2: `docs/process/cross_tool_communication_protocol_v2_20260511.md`
- T14b/c round 3 dispatch: `docs/cross_tool/20260511_strategy_to_dw_foundation_DISPATCH_t14bc_fix_round_3.md`
