# paper-v2 DISPATCH — Stage 6 Baseline post-R5 (流水线验证 R5)

**From**: Strategy session
**To**: paper-v2 worktree team
**Sent**: 2026-05-11 ~18:25
**Type**: Type C - 流水线验证 (用户要求所有 main 合入都经流水线验证)
**Parallel with**: paper-v2 verify Codex b976c23 (drawer 013ab7f7) — 可串行或并行执行

## 上下文

战略 session 已执行 R5 paper-v2 merge:
- **main HEAD: `3cfe10f`** (merge: R5 paper-v2 Sprint 2026-05-11)
- 已 push origin main + 删除 `claude/paper-v2-vnpy-mvp-20260508` remote branch
- R5 内容: T5/T6.1/T6.2 capture (intended_price + fill_market_context + created_at/updated_at) + daemon outbox + T13 routing_class + INT 5/5 + audit

用户 prod cutover 路径 A 时间线驱动 → **明早 9:30 实盘**。
**所有功能必须经流水线验证** — R5 在 main 后必须跑 baseline 才能进入 R6 prod operations。

## 任务

### Step 1: 同步 main

```bash
cd F:/Dev/AIstock-worktrees/paper-v2-vnpy-mvp-20260508
# 注意: 你的本地 paper-v2 branch 已被远端删除. 直接切到 main 跑.
# 或者用新 worktree
git fetch origin
git log --oneline ee2e56f..origin/main | head -10
# 应看到: 3cfe10f merge R5, 06aaba9 strategy dispatches, 535c539..ee2e56f..fca9d69 paper-v2 commits 现已在 main
```

### Step 2: 跑 Stage 6 baseline on main HEAD 3cfe10f

可以在新 worktree 跑（推荐）:
```bash
git worktree add F:/Dev/AIstock-worktrees/baseline-post-r5 3cfe10f
cd F:/Dev/AIstock-worktrees/baseline-post-r5
```

或就地 checkout main 3cfe10f。

完整 plan keys (与 535c539 baseline 一致):
- `l0`, `guardrail_changed_files`
- `validation_coverage_backend`, `validation_module_registry_l0`
- `validation_center_backend`, `validation_center_live_readonly`, `validation_center_ui`
- `qe_data_contract_backend`, `qe_archive_backend`, `qe_archive_data_quality`, `qe_archive_l3`, `qe_read_l3`
- `paper_v2_backend`, `paper_v2_l3` ← R5 内容核心
- `model_registry_backend`, `market_regime_label`, `rl_execution_smoke`
- `data_quality_deep`, `dr_validate`
- 5 UI SKIP

### Step 3: 重点验证 (R5 新增内容)

R5 引入了 paper-v2 daemon outbox + T13 routing_class + T5/T6.1/T6.2 capture。重点：

1. **`paper_v2_backend`**: 应通过 T5/T6.1/T6.2 capture 单元测试 + repository.py 写 intended_price/fill_market_context/created_at/updated_at
2. **`paper_v2_l3`**: 应通过 INT 5/5 (daemon → outbox → emit + capture round-trip)
3. **`qe_archive_backend`**: 应通过 T14a PaperV2ArchiveHandler 端到端 (但需要 archive worker enable 才会真正 archive, 这里只是结构验证)
4. **routing_class 标 telemetry**: T13 应让 paper-v2 daemon emit 不走 archive (默认 telemetry)
5. **新 worktree fresh-clone scenario**: 注意 backend/db/dev/ 已 gitignore, 不应 stale

### Step 4: 与 535c539 baseline 对比

差异预期:
- paper_v2_backend test 数量增加 (T5/T6.1/T6.2 单测加入)
- paper_v2_l3 test 数量增加 (INT 5/5 + outbox 测)
- 总 GREEN sessions 数应 ≥ 11
- qe_archive_data_quality 仍 27/27 (R4 内容未变)
- 5 SKIP UI 仍 SKIP

如出现 REGRESSION (新 RED), 立即报告, 不要自动 fix。

### Step 5: 输出 BASELINE post-R5 doc

paper-v2 branch 已删除. 选项:
- A. 写在新建分支 `claude/paper-v2-baseline-post-r5-20260511` (推荐, 后续 R5 merge 后所有验证都用 separate branch)
- B. 写 commit 到 main (谨慎, R5 后 main 应保持稳定)

推荐 A. 路径: `docs/baseline/stage6_baseline_post_r5_20260511.md`
内容:
- main HEAD: 3cfe10f
- 每 plan key result
- 总计 g/s/f
- vs 535c539 delta
- R5 内容验证结论 (paper_v2_backend / paper_v2_l3 / qe_archive T14a 端到端)
- R6 readiness verdict: READY / READY-WITH-CAVEATS / BLOCKED

### Step 6: deliver drawer

```
[BASELINE] Stage 6 post-R5 on main@3cfe10f
commit: <new commit>
branch: <branch name>
verdict: GREEN / YELLOW / RED
sessions: NG / MF / KSKIP
R5 paper_v2_backend: PASS/FAIL
R5 paper_v2_l3: PASS/FAIL
qe_archive_backend T14a: PASS/FAIL
delta vs 535c539: <summary>
R6 readiness: READY / CAVEATS / BLOCKED
doc: docs/baseline/stage6_baseline_post_r5_20260511.md
```

## SLA

**≤ 60 min** (实盘目标驱动, 与 verify b976c23 共享时间预算)

可与 verify Codex b976c23 (drawer 013ab7f7) 任务并行或串行。如串行, 先做 baseline (验证 R5 入 main 后 main 稳定) 再做 verify (Codex prod-apply readiness)。

## 不要做

- ❌ 不要 fix 任何 finding (报告即可)
- ❌ 不要 INSERT dev DB
- ❌ 不要 touch prod
- ❌ 不要 merge 到 main (你的 baseline doc 在 separate branch 即可)

## References

- main HEAD post-R5: 3cfe10f
- 之前 baseline 535c539 (post-R4 GREEN)
- R5 merge content: T5/T6.1/T6.2 + daemon outbox + T13 + INT 5/5
- 实盘目标: 明早 9:30 A股开市
- verify b976c23 dispatch: drawer 013ab7f7
