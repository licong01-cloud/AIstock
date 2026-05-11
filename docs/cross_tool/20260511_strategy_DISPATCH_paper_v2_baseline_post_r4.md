# paper-v2 Stage 6 Baseline 重跑 (post-R4 + hotfix) — R5 merge 前最后 gate

**From**: Strategy session (Claude Code 战略 window)
**To**: paper-v2 worktree team
**Sent**: 2026-05-11 ~16:55
**Type**: Type C - Audit-triggered re-baseline
**Prereq**: ee2e56f verify PASS（Codex 7bf840d 4 BUG-PREP fix 已确认）

## 上下文

paper-v2 之前 baseline post-R3 (`a31365f`) 为 YELLOW 5g/2f：
- ❌ `validation_module_registry_l0`: rl_execution module_id 重复 (R0 cherry-pick + R1 merge 双注册)
- ❌ `data_quality_deep`: archive 空时 pytest.fail（应当 graceful skip per D5 Q2.c, worker 默认 disabled）

战略 session 已在 main 上 hotfix：
- `tests/aistock_validation/catalog/module_registry.yaml`: 合并重复 rl_execution 条目 (R0+R1 合并产物)
- `backend/tests/data_quality/test_cross_table_consistency.py`: archive 空时改 pytest.skip + 引用 D5 Q2.c

之后 R4 dw-foundation 已 merge：T12 22 表 DDL + T14a/b/c PaperV2/FactorValue archive handlers + SCD2 + completion marker。

**main HEAD**: `4a3fa60` (R4 merge tip + handoff doc + hotfix)

## 任务

### Step 1: 同步 main → paper-v2 worktree

```bash
cd F:/Dev/AIstock-worktrees/paper-v2-vnpy-mvp-20260508
git fetch origin main
git log --oneline ee2e56f..origin/main  # 确认看到 R4 merge + hotfix commits
```

不要 merge 进 paper-v2 branch — 只是看 main 上的 hotfix + R4 内容。baseline 重跑要在 **main HEAD `4a3fa60`** 上跑，模拟 R5 merge 后状态。

### Step 2: 重跑 Stage 6 baseline on main

切换或 worktree-checkout 到 main `4a3fa60`：

```bash
git worktree add F:/Dev/AIstock-worktrees/baseline-post-r4 4a3fa60
cd F:/Dev/AIstock-worktrees/baseline-post-r4

# 跑全套 Stage 6 baseline 流水线 (与 a31365f 那次相同 plan keys)
# 期望 GREEN ≥8 sessions + 5 SKIP (UI service-policy)
```

完整 plan keys (与 a31365f 一致):
- `l0`, `guardrail_changed_files`
- `validation_coverage_backend`, `validation_module_registry_l0`
- `validation_center_backend`, `validation_center_live_readonly`, `validation_center_ui`
- `qe_data_contract_backend`, `qe_archive_backend`, `qe_archive_data_quality`, `qe_archive_l3`, `qe_read_l3`
- `paper_v2_backend`, `paper_v2_l3`
- `model_registry_backend`, `market_regime_label`, `rl_execution_smoke`
- `data_quality_deep`, `dr_validate`
- 5 UI SKIP: `validation_center_real_port_ui`, `strategy_package_governance_ui`, `market_regime_ui`, `rl_execution_ui`, （+其他 service-policy）

### Step 3: 重点验证 (R4 新增内容)

R4 dw-foundation merge 引入了 T12+T14 工作。重点关注：

1. **`qe_archive_backend`**: T12 22 表 DDL 应已生效（dev DB schema 匹配）
2. **`qe_archive_data_quality`**: `test_cross_table_consistency.py` 现在应 SKIP（archive 空 → D5 Q2.c）, **不要 FAIL**
3. **`paper_v2_l3`**: 应覆盖 T14a PaperV2 handler 工作
4. **`validation_module_registry_l0`**: rl_execution 现在应只有 1 个 module_id 条目

### Step 4: 产出 BASELINE post-R4 doc

写到 paper-v2 branch：
- 路径: `docs/baseline/stage6_baseline_post_r4_20260511.md`
- 字段: main HEAD, 每个 plan key 结果 (GREEN/SKIP/FAIL), 总计 g/s/f, 差异 vs a31365f, hotfix + R4 内容验证结论
- 如有 FAIL: 列出 file:line + traceback head, 不要自动 fix

### Step 5: deliver drawer

写短 drawer 到 mempalace cross-tool / codex-claude-coord：
```
paper-v2 baseline post-R4 done. HEAD=4a3fa60. Result: <GREEN|YELLOW|RED> Ng/Ms/Kf.
hotfix verified: module_registry dup fixed=Y/N, data_quality skip=Y/N.
R4 verified: qe_archive_backend=PASS/FAIL, paper_v2_l3=PASS/FAIL.
doc: docs/baseline/stage6_baseline_post_r4_20260511.md
delta vs a31365f: <summary>
R5 merge readiness: READY / BLOCKED-<reason>
```

## 期望 SLA

- 总耗时 ~30-45 min
- 期望 GREEN ≥ 8 sessions + 5 SKIP, 0 FAIL
- 如 GREEN: R5 merge 可执行（战略 session 等用户授权后跑 playbook v2 §R5）
- 如 YELLOW/RED: R5 merge BLOCK, 调查 root cause

## 不要做

- ❌ 不要修改任何代码（如发现新 bug, 报告即可, 不自动 fix）
- ❌ 不要 merge main 进 paper-v2 branch（baseline 跑在 main 上, paper-v2 branch 保持 ee2e56f）
- ❌ 不要启动 prod backend 8001 / prod frontend 3000
- ❌ 不要写 prod DB

## References

- 之前 baseline a31365f (post-R3 YELLOW)
- main hotfix commits (在 4a3fa60 之前)
- R4 merge tip: 2d1f820
- Codex 7bf840d verify doc: `docs/cross_tool/20260511_paper_v2_VERIFY_codex_governance_prep_fixes.md`
- playbook v2 §R5: paper-v2 merge 命令
- D5 Q2.c qualified-yes (archive worker default disabled)
