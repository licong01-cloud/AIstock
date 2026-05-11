# paper-v2 DISPATCH — Branch Baseline Fix Round (stk_limit + nox session)

**From**: Strategy session
**To**: paper-v2 worktree team
**Sent**: 2026-05-11 ~22:50
**Type**: Type C — fix round on YELLOW baseline
**Priority**: LOW — 不阻断 R6 merge (R6 spotlights 104/104 PASS)

## 上下文

paper-v2 branch baseline c2352a9 (commit `7c18a1d`, drawer `2272c1c7`):
- verdict YELLOW
- R6 in-branch 104/104 spotlights PASS (代码层面 GO)
- **1 FAIL**: paper_v2_data_quality stk_limit stale (operational data freshness)
- **5 MISS**: data_quality_deep / dr_validate / model_registry_backend / market_regime_label / rl_execution_smoke — 未在 codex branch noxfile.py 注册

## 任务

### Fix 1 — paper_v2_data_quality stk_limit stale

**症状**: dev DB stk_limit 表 limit_up/limit_down 数据陈旧 (上次刷新 > N 天)。

**实际原因调查**:
- 查 `backend/db/stk_limit` 或 `data/stk_limit/` 数据源
- 查 dev DB `stk_limit` 表 (5433/aistock_dev) 的 trade_date 最新值
- 查刷新脚本 (`scripts/update_stk_limit.py` 或类似)

**解决路径选项**:
1. **A: 重跑刷新脚本** (推荐) — 让 dev DB stk_limit 数据回到 t-1
2. **B: 在测试 fixture 内 freeze 测试用日期** — 让 test 不依赖 stk_limit 数据 freshness
3. **C: 添加 staleness 容差到测试** — assert age < 7 天 不 0 天

如选 A, 不算代码 fix; 如选 B/C, 是代码 fix 需 commit。

### Fix 2 — codex branch noxfile.py 补 5 个 missing session

**症状**: codex branch `noxfile.py` 缺以下 nox session:
- `data_quality_deep`
- `dr_validate`
- `model_registry_backend`
- `market_regime_label`
- `rl_execution_smoke`

main `noxfile.py` 有这些 session (从 baseline post-R5 v2 e8ffbdd 跑过 GREEN). codex branch 因独立分支 noxfile 漂移 missing。

**解决**:
- 从 main `noxfile.py` 复制这 5 个 session 定义到 codex branch noxfile.py
- 或: 改 codex noxfile.py 用 includes 引入 main 的 session 定义 (避免 drift)

推荐: 直接复制 5 个 session 定义 (简单 + 与 main 完全一致)。

注意: codex branch 是 `codex/qe-governance-integration-20260509` (Codex 持有), 你应在你的 paper-v2 worktree 看到的是该 branch 的 noxfile.py。如不愿动 Codex branch, 可:
- A: 战略 session 在 R6 merge 前先 cherry-pick 5 session 到 codex branch (但战略不审 codex 代码)
- B: 派 Codex 自己补 (但 Codex 自驱补 noxfile 没问题)
- C: 你在 codex branch 上 commit (paper-v2 团队对 codex branch 历史不熟, 风险)

推荐 B: 让 Codex 在自己分支补。如选 B, 派单转给 Codex (本任务保留 Fix 1, Fix 2 交 Codex)。

### 输出

- 写 `docs/cross_tool/20260511_paper_v2_FIX_baseline_caveats.md`
- 列 Fix 1 选项 + 实施 + 验证
- Fix 2 推荐路径 (我倾向 B, 战略 session 派 Codex 补)
- baseline RE-RUN with stk_limit 刷新后, 期望 12 GREEN / 0 FAIL

### deliver drawer

```
[FIX] paper-v2 baseline caveats fix round
fix1 stk_limit stale: <option chosen> done Y/N
fix2 noxfile 5 missing: dispatched to Codex / done in branch
re-run baseline: GREEN/YELLOW/RED
doc: docs/cross_tool/20260511_paper_v2_FIX_baseline_caveats.md
```

## SLA

**≤ 45 min** (~23:35 deliver)

## Do NOT

- ❌ 不要修改 R6 prod executor / sanity script 代码 (不在 fix 范围)
- ❌ 不要 touch prod DB
- ❌ 不要 merge

## References

- branch baseline doc: main `568c16c`
- main HEAD: `568c16c`
- 实盘目标: 明早 9:30 A股开市 LocalSim 模拟盘
- 优先级: LOW (R6 merge GO 不依赖此 fix)
