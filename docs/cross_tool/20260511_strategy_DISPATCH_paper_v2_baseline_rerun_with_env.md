# paper-v2 DISPATCH — Push verify commit + Baseline RE-RUN with env

**From**: Strategy session
**To**: paper-v2 worktree team
**Sent**: 2026-05-11 ~18:35
**Type**: Type C cleanup + 流水线验证 RE-RUN
**Parallel with**: Codex Task 4 prod executor

## 上下文

刚 deliver 2 任务:
- baseline post-R5 (drawer `b203c431`, commit `779e904`): YELLOW, 13G/3F/14SKIP
- verify Codex b976c23 (drawer `9a2668d5`, commit `1acc15f`): PASS L1-L4, READY-WITH-CAVEATS

**问题 1**: verify commit `1acc15f` 写在已删除 remote branch `claude/paper-v2-vnpy-mvp-20260508` — 只在 local，需要推到新 branch 避免丢失。

**问题 2**: baseline 3 fails 全是 env-only (psycopg2 no-password to prod 5432) — 不是 R5 代码 regression，但 baseline 流程 GREEN 是 R6 prod ops 的硬前置（用户要求"所有功能必须经流水线验证"）。

## Task A — 推 verify commit 到新 branch

```bash
cd F:/Dev/AIstock-worktrees/paper-v2-vnpy-mvp-20260508
# (或你当前持有 1acc15f 的 worktree)
git log --oneline -3  # 确认 1acc15f 是 HEAD
git branch claude/paper-v2-verify-b976c23-20260511 1acc15f
git push origin claude/paper-v2-verify-b976c23-20260511
```

确认 push 成功后, 该 commit 安全保留。

## Task B — Baseline RE-RUN with env

### Step 1: 设 env

baseline 779e904 3 fails 都是 `psycopg2.OperationalError: connection to server at "localhost", port 5432 failed: fe_sendauth: no password supplied`

需要确认:
- worktree 是否 source `.env` 或 `.env.dev`
- `TDX_DB_PASSWORD` / `AISTOCK_DB_PASSWORD` 是否设置
- 如果 baseline 跑在 fresh worktree (你之前用了 `git worktree add F:/Dev/AIstock-worktrees/baseline-post-r5 3cfe10f`), 该 worktree 的 `.env` 可能缺漏

```bash
cd F:/Dev/AIstock-worktrees/baseline-post-r5
# 检查 .env 存在 + 含 DB password
ls -la .env*
# 复制 main repo 的 .env 过来 (因为 .env gitignored)
cp F:/Dev/AIstock/.env .env  # 如必要
```

### Step 2: RE-RUN baseline on 3cfe10f

跑相同 plan keys 与之前 baseline 779e904 一致 (l0/guardrail/validation_*/qe_*/paper_v2_*/model_registry/market_regime/rl_execution_smoke/data_quality_deep/dr_validate + 5 UI SKIP)。

### Step 3: 验证

- 期望 3 env-fails 全消失 (变 GREEN 或 SKIP)
- 总 sessions ≥ 13G (vs 779e904), 0 FAIL
- vs 535c539 (post-R4 GREEN, 11G/0F/5SKIP) 差异预期: +2 G (paper_v2 R5 内容)
- vs 779e904 (post-R5 YELLOW, 13G/3F/14SKIP) 差异预期: 3 fails → GREEN, ~9 SKIP 变 GREEN

### Step 4: 输出 BASELINE post-R5 v2 doc

写到新 branch `claude/paper-v2-baseline-post-r5-20260511` (继续使用)。
- 路径: `docs/baseline/stage6_baseline_post_r5_v2_20260511.md`
- 字段: main HEAD 3cfe10f, env diff vs v1, 每 plan key 结果, env-fail 已修, R6 readiness verdict

### Step 5: deliver drawer

```
[BASELINE] Stage 6 post-R5 v2 on main@3cfe10f (env-fixed)
commit: <new>
branch: claude/paper-v2-baseline-post-r5-20260511
verdict: GREEN / YELLOW
sessions: NG / MF / KSKIP
env-fail 3 → 0: Y/N
R5 spotlights: paper_v2_backend / paper_v2_l3 / qe_archive_backend status
R6 readiness: READY / CAVEATS / BLOCKED
doc: docs/baseline/stage6_baseline_post_r5_v2_20260511.md
delta vs v1: <summary>
```

## SLA

**≤ 45 min** (Task A 5 min push + Task B 40 min baseline + doc)

## Do NOT

- ❌ 不要 fix 代码 (env 是 ops 修, 不是 code 修)
- ❌ 不要 INSERT dev DB
- ❌ 不要 touch prod
- ❌ 不要 merge to main

## References

- 之前 baseline v1: 779e904, drawer b203c431
- 之前 verify: 1acc15f, drawer 9a2668d5
- main HEAD: 3cfe10f
- 实盘目标: 明早 9:30 A股开市
- Codex Task 4 并行: 写 prod-capable backfill executor
