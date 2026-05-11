# Codex DISPATCH — Overnight Long Task (10+ hours): Branch Audit + Cleanup + PR-005 Prep

**From**: Strategy session
**To**: Codex App
**Sent**: 2026-05-11 (启动时间: Codex Task 8 sentinel endpoint deliver 后)
**Type**: Type A self-driven long task (multi-phase)
**Branch**: Codex 自决 (建议 codex/qe-cleanup-and-pr005-prep-20260512 新分支)
**Duration**: ~10-14 小时 (sequential), Codex 可 parallel Worker A/B/C/D 加速

## 上下文 + 用户决策 (2026-05-11)

1. **实盘正式交易不实施** — minqmt_live 不开放, R-Q9.1 决策不动
2. **miniQMT 模拟盘 (miniqmt_sim) 需实施** — 设计已完整 (paper_v2_dual_broker_pr_split_plan §3 PR-3 等), 可进入 PR-005 prep + 实施
3. **本任务启动条件**: 明早模拟盘 (LocalSim) 准备就绪 (Codex Task 8 sentinel endpoint + R6 merge + baseline post-R6 + prod cutover 完成)
4. **战略思路**: 现有功能逐一验证 → 流水线 GREEN → 合 main → 清理分支 → 再开发新功能 (含 PR-005)

## Phase A — Branch Audit + Cleanup Plan (~4-5h)

**目标**: 仓库当前 20+ branch (含 origin/codex/* 多 sprint 遗留), 需逐一审查 + 输出 cleanup plan。

### A.1 — Branch Inventory

跑 `git branch -r` + `git fetch --all`, 列出所有 `origin/*` branch (除 main + main 派生)。

对每个 branch 做以下分析:
- `git log --oneline origin/<branch> ^origin/main | wc -l` — 多少 commits ahead of main
- `git merge-base --is-ancestor origin/<branch> origin/main` — 是否已被 main 完全包含
- `git log -1 --format=%cI origin/<branch>` — 最后 commit 时间
- 内容粗分类: 设计 doc / 代码 / fixture / 实验性

### A.2 — 推荐处置

每 branch 推荐之一:
- **MERGE**: 有价值 unmerged 内容, 推荐合 main (写 merge plan)
- **CHERRY-PICK**: 仅部分 commits 有价值, 列具体 commit list
- **ARCHIVE**: 实验性 / 历史保留, 推荐 rename to `archive/<branch>-yyyy-mm-dd`
- **DELETE**: 已 merge / 重复 / 废弃, 推荐 `git push origin --delete`

### A.3 — 输出

写 **`docs/handoff/branch_audit_cleanup_plan_20260512.md`** on Codex 自决分支:
- 表格: branch / ahead commits / last commit date / 内容性质 / 推荐处置 / 理由
- 总计: MERGE n / CHERRY-PICK n / ARCHIVE n / DELETE n
- 风险评估: 哪些 branch 删除有信息损失风险

### A.4 — 不做

- ❌ 不要 actually delete / archive 任何 branch (战略 session 醒后审 + 执行)
- ❌ 不要修改任何 branch 内容
- ❌ 不要 merge (只写 plan)

## Phase B — codex branch noxfile.py Fix + Branch Baseline RE-RUN (~2-3h)

**目标**: 补 codex/qe-governance-integration-20260509 noxfile.py 5 missing sessions, 解决 paper-v2 branch baseline 5 MISS caveat。

### B.1 — 复制 5 sessions

从 main `noxfile.py` 复制以下 5 session 定义到 `codex/qe-governance-integration-20260509` 的 `noxfile.py`:
- `data_quality_deep`
- `dr_validate`
- `model_registry_backend`
- `market_regime_label`
- `rl_execution_smoke`

注意保留 codex branch 已有 session 定义 (不要覆盖)。

### B.2 — 验证

- `python -m nox -l | grep -E "(data_quality_deep|dr_validate|model_registry|market_regime|rl_execution_smoke)"` 应列 5 个
- 在 codex branch 跑这 5 session 各一次, 期望 GREEN

### B.3 — 触发 paper-v2 baseline RE-RUN

写 drawer 通知 paper-v2 团队: codex branch noxfile 已补 5 sessions, 可重跑 branch baseline。

### B.4 — 不做

- ❌ 不要 merge codex branch to main
- ❌ 不要 touch prod
- ❌ 不要修改其他文件

## Phase C — PR-005 MiniQMTSim Implementation Plan + Prep (~2-3h)

**目标**: 把 PR-005 实施 step-by-step plan 写完, 让战略 session 醒后授权后 Codex 可直接进入实施。

### C.1 — 读完 4 份设计 doc

- `docs/discussion/paper_v2_dual_broker_pr_split_plan_20260509.md` §3 PR-3 (整段)
- `docs/architecture/strategy_engine_design_20260508.md` §3.6 (BrokerBackend protocol + R-Q9)
- `docs/architecture/broker_backend_switch_flow_20260509.md`
- `docs/analysis/vnpy_poc_result_20260508.md`

### C.2 — 写实施 plan

输出 **`docs/architecture/pr_005_miniqmt_sim_implementation_plan_20260512.md`**:

#### Plan Sections

1. **Scope** (按 PR-3 spec):
   - MiniQMTSimBackend 类 (xtquant 直调)
   - Singleton 保护 (MINIQMTSIM_SINGLETON_HELD flag + threading.Lock + close protocol)
   - bind_capacity() returns max_concurrent_packages=1
   - StrategySpec custom_extension.broker_compatible reader (OPEN-EXT-3 占位)
   - Portfolio 启动 / Engine init 校验 broker_compatible 与 broker_backend_id 兼容性
   - xtquant 错误码 → typed errors 映射

2. **File-by-file change list**:
   - 新增: `backend/services/paper_trading_v2/broker/minqmtsim.py`
   - 改: `backend/services/paper_trading_v2/broker/__init__.py` (添加 MiniQMTSimBackend export)
   - 改: `backend/services/paper_trading_v2/service.py` (添加 broker 实例化 dispatch)
   - 新增: `backend/tests/paper_trading_v2/test_minqmtsim_broker.py` (mock xtquant, ≥ 25 tests)
   - 新增: `backend/tests/paper_trading_v2/test_minqmtsim_integration.py` (mark `@pytest.mark.integration_minqmt`, ≥ 4 Mode G 用例)

3. **xtquant 环境准备**:
   - 评估 `.venv-vnpy-poc/` 复用 vs 新建 `.venv-paper-v2-prod/`
   - 列 xtquant 包源 (vendored `xtquant/` gitignored, 不在 Python path)
   - 写 `requirements-minqmt.txt` (xtquant + 依赖)

4. **测试策略**:
   - Mock 单元测试: 无外部依赖, CI 必跑
   - Integration test: `@pytest.mark.integration_minqmt`, CI skip, 本地 PoC env 必跑
   - PR description 强制贴运行截图

5. **R-Q9 invariants 验证**:
   - D1 broker_backend immutable
   - D2 multi-package LocalSim 隔离
   - D3 行情通道强绑定 broker (cross-config validator)
   - D4 BindCapacity 强制 (单实例)
   - Mode G 4 用例:
     - `engine_modeg_localsim_vs_minqmtsim_orderintents`
     - `engine_modeg_minqmt_capacity_reject`
     - `engine_modeg_broker_compat_reject`
     - (`engine_modeg_multi_package_localsim_isolation` 已在 PR-2 跑过)

6. **错误码映射**:
   - rc=-1 → BrokerConnectivityError
   - rc=-2 → BrokerRejectedError
   - rc=-3 → BrokerSubmitError
   - timeout → BrokerConnectivityError
   - 完整 mapping table

7. **OPEN-EXT-3 衔接**:
   - 本 PR 用 `custom_extension.broker_compatible` 占位 reader (R-Q2 audit-only)
   - OPEN-EXT-3 落地后改一处 reader 函数 (manifest schema 加 broker_compatible 一等公民)

8. **预估**:
   - 实施: 3-5 天
   - Mock 单测: 1 天
   - Integration test (PoC env): 1 天
   - Code review + cross-tool verify: 1-2 天
   - 流水线 baseline + R-merge: 1 天

9. **风险评估**:
   - PoC 阶段 1 PASS 大幅降低未知
   - 依赖外部 miniQMT SIM 服务稳定性
   - vnpy_xt 方案 B 触发条件 (备选)

### C.3 — 不做

- ❌ 不要实际写 MiniQMTSimBackend 代码 (战略醒后授权)
- ❌ 不要安装 xtquant (只写依赖文档)
- ❌ 不要 push 到主线 branch (写在 docs/architecture/ 即可)

## Phase D — Self-Driven Codex Branches Status Eval (~1h)

**目标**: 评估 Codex 持有的 self-driven 分支当前进度, 写 status report 让战略 session 醒后决策。

### D.1 — 评估 codex/hmm-sector-regime-20260509

- 当前 idle 时间 (~30h+)
- last commit 内容
- 是否值得继续 / 归档 / 删除?
- 如继续, 接下来 step?

### D.2 — 评估 codex/financial-distress-rerank-20260508

- 当前进度百分比
- last commit 内容
- 接下来 step + ETA
- 是否需要派单加速?

### D.3 — 输出

写 **`docs/handoff/codex_self_driven_branches_status_20260512.md`**:
- 每 branch: 进度 / last commit / 推荐 next step
- 综合推荐: 是否 Codex 继续自驱, 还是停下来等指派?

## 总输出 + Deliver

完成 Phase A-D 后, 写一个 master drawer:

```
[REVIEW] Codex overnight long task complete (Phase A-D)

Phase A: Branch audit cleanup plan
  doc: docs/handoff/branch_audit_cleanup_plan_20260512.md
  total branches: N
  MERGE: n / CHERRY-PICK: n / ARCHIVE: n / DELETE: n

Phase B: codex noxfile 5 sessions fix
  branch: codex/qe-governance-integration-20260509
  commit: <new>
  5 sessions added, all GREEN on RE-RUN
  paper-v2 branch baseline can RE-RUN now

Phase C: PR-005 MiniQMTSim implementation plan
  doc: docs/architecture/pr_005_miniqmt_sim_implementation_plan_20260512.md
  scope/file-list/tests/env/timeline 完整
  实施 ETA: 5-7 days (实施 3-5 + 测试 2)

Phase D: Self-driven branches status
  doc: docs/handoff/codex_self_driven_branches_status_20260512.md
  HMM: <recommendation>
  financial-distress: <progress + ETA>
```

## 时间预算

| Phase | SLA | Worker 并行可能? |
|---|---|---|
| A | 4-5h | Yes (Worker A/B/C 分 branch 组) |
| B | 2-3h | No (单 codex branch noxfile) |
| C | 2-3h | Partial (一个 Worker 读 doc, 一个 Worker 写 plan) |
| D | 1h | Yes (2 branches 并行评估) |
| Total | 10-14h (sequential) / 6-8h (parallel) | — |

## 启动条件

**必须等以下完成后才启动本任务**:
1. ✅ Codex Task 8 sentinel endpoint deliver
2. ✅ paper-v2 verify Task 8 sentinel endpoint
3. ✅ R6 merge to main
4. ✅ baseline post-R6 GREEN
5. ✅ 用户 prod cutover (DR + 2 executor + 6 migrations + daemon enable + cold-start sanity)
6. ✅ 9:30 LocalSim 模拟盘 GO

如以上未完成, Codex 应优先解决, 不进入本长任务。

## Do NOT (适用于所有 Phase)

- ❌ 不要实际删除 / archive 任何 branch
- ❌ 不要 merge 任何 branch 到 main
- ❌ 不要修改主线代码 (除 Phase B noxfile)
- ❌ 不要安装外部依赖 (xtquant 等)
- ❌ 不要 touch prod
- ❌ 不要执行 prod 操作

## References

- 用户决策 2026-05-11: 实盘不开发, 模拟盘 (LocalSim + MiniQMTSim) 实施
- 设计 docs: §C.1 列表
- 现有未合分支统计 (战略 session 估算): 18-22 个
- main HEAD (本任务启动时): R6 merge tip (TBD)
