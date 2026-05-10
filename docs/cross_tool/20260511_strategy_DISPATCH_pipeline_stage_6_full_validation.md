# [DISPATCH] Pipeline Stage 6 — 本次 Sprint 全面流水线验证

**from**: claude_code_strategy
**to**: pipeline-foundation team Lead
**date**: 2026-05-11 (drafted overnight, dispatch when Phase 3 全绿 + 4 layer review pass)
**verdict**: DISPATCH-PREP

## Summary

Phase 3 三方绿 + Codex 全部 review PASS + 4 层交叉检查通过后，启动 Stage 6: 跑本次 Sprint 涉及的 8 个 nox session + 归档 evidence + 准备 main merge readiness 报告。

## 触发条件（5 必满足）

```
✅ Codex T14b/c fix round 3 review = PASS
✅ Codex paper-v2 INT fix round 1 二轮 review = PASS
✅ Codex dw-foundation Batch A/C fix round 1 二轮 review = PASS
✅ Codex pipeline Stage 4+5 fix round 1 二轮 review = PASS
✅ 4 层交叉检查（task #15）通过：static / unit / integration / mutation
```

不足任一 → 返回相应 fix round → Stage 6 待重新触发。

## 范围（8 个 nox session）

按 catalog 注册的现有 + Stage 2 新增 session 全跑：

| # | nox session | 模块 | 耗时估 |
|---|---|---|---|
| 1 | `nox -s l0` | 静态门控 (guardrail + module ownership) | 5 min |
| 2 | `nox -s validation_module_registry_l0` | 模块注册 + 文件归属验证 | 3 min |
| 3 | `nox -s paper_v2_backend` | paper_v2 + selection_center + strategy_package | 10 min |
| 4 | `nox -s qe_archive_backend` | qe_archive schema + repo + handler contract + handlers | 15 min |
| 5 | `nox -s model_registry_backend` | Codex governance migrations + phase5 view | 5 min |
| 6 | `nox -s market_regime_label` | T10/T16 regime_label DDL + classify | 5 min |
| 7 | `nox -s rl_execution_smoke` | rl_execution module visibility regression | 2 min |
| 8 | `nox -s validation_center_backend` | Validation Center API + MCP server tests | 10 min |

**总耗时**: ~55 min on local Windows + conda AIstock env.

## 实施步骤

### Step 0: 切到 pipeline worktree

```bash
cd F:/Dev/AIstock_worktrees/pipeline-foundation-20260510
git pull origin claude/pipeline-foundation-20260510
conda activate AIstock
```

### Step 1: 跑全 8 个 session，归档 evidence

```bash
mkdir -p tests/aistock_validation/history/sprint_2026_05_11/

for session in l0 validation_module_registry_l0 paper_v2_backend qe_archive_backend \
               model_registry_backend market_regime_label rl_execution_smoke \
               validation_center_backend; do
  echo "=== running nox -s $session ==="
  nox -s "$session" 2>&1 | tee "tests/aistock_validation/history/sprint_2026_05_11/${session}.log"
  echo "Exit: $?"
done
```

任一 session 失败 → 立即 stop → 入库 BUG → cross-tool drawer 通知战略 session。

### Step 2: 生成 sprint readiness report

```bash
python scripts/aistock_validate.py record \
  --module sprint_2026_05_11 \
  --level L3 \
  --title "Sprint 2026-05-11 - DW + Paper v2 capture + governance + pipeline" \
  --output tests/aistock_validation/history/sprint_2026_05_11/release_readiness.json
```

### Step 3: 收集所有相关分支 commit 清单

```bash
git log --oneline origin/main..origin/claude/dw-foundation-20260510 \
        > tests/aistock_validation/history/sprint_2026_05_11/dw-foundation_commits.txt

git log --oneline origin/main..origin/claude/paper-v2-vnpy-mvp-20260508 \
        > tests/aistock_validation/history/sprint_2026_05_11/paper-v2_commits.txt

git log --oneline origin/main..origin/claude/pipeline-foundation-20260510 \
        > tests/aistock_validation/history/sprint_2026_05_11/pipeline-foundation_commits.txt

git log --oneline origin/main..origin/codex/qe-governance-integration-20260509 \
        > tests/aistock_validation/history/sprint_2026_05_11/codex-governance_commits.txt
```

### Step 4: BUG 注册表全状态扫

```bash
python -c "
from backend.services.validation.finding_store import ValidationFindingStore
store = ValidationFindingStore()
summary = store.bug_summary()
print(summary)
" > tests/aistock_validation/history/sprint_2026_05_11/bug_summary.txt
```

期望: 多数 BUG fixed/verified, P0 BLOCKER = 0, P1 open = 0 (或显式 deferred 标记)。

### Step 5: 写 release readiness markdown 报告

```markdown
# Sprint 2026-05-11 Release Readiness

## Verdict: GREEN | YELLOW | RED

## Summary
- N+ commits across 4 branches
- M+ tests passing across 8 nox sessions
- K BUG verified, J BUG deferred (P2/P3)
- Cross-tool review bidirectional pass

## Risks
- ...

## Open BUG (P0/P1 only)
- ...

## Next Steps
- ...
```

放在 `tests/aistock_validation/history/sprint_2026_05_11/release_readiness.md`。

### Step 6: cross-tool drawer 通知

drawer 短消息 + detail_doc 引用本 doc 路径 + history 路径。

```
[REVIEW] Sprint 2026-05-11 Stage 6 全面验证 done

verdict=GREEN | YELLOW
detail_doc=tests/aistock_validation/history/sprint_2026_05_11/release_readiness.md

8 sessions ran. M tests passed. K BUG verified. Recommended action: <merge approve | defer for round X fix>.
```

### Step 7: pre-commit + commit + push

```bash
git add tests/aistock_validation/history/sprint_2026_05_11/
git commit -m "test(pipeline): T-PIPE-6 Sprint 2026-05-11 full validation evidence"
git push origin claude/pipeline-foundation-20260510
```

## 验收判据

| 维度 | 要求 |
|---|---|
| 静态门控 | 0 P1 finding |
| 单元测试 | 全 pass，0 fail |
| 集成测试 | dev DB 全 pass，0 orphan FK，无未提交 cleanup |
| Coverage | 各模块 line ≥ 70%, branch ≥ 55% (Stage 4 之前已设阈值) |
| BUG 注册表 | P0/P1 open = 0 |
| Codex review | 全部 round 通过 |
| Cross-tool | 双向 review 闭环 + bug_agent_context 可被 AI 拉取 |

## 失败处理（不同维度）

### 单 session 失败
- 立即 stop 全套
- 入库 P1 BUG
- 通知战略 session via drawer
- 等待对应 team fix round 后重启 Stage 6

### Coverage 不达标
- 评估是否要补测试还是降阈值
- 战略 session 协调

### BUG 注册表残留 P1 open
- 评估每条 P1 是否真的 prod-blocker
- 决定 defer 还是 fix（需用户授权）

## 关联

- 触发依赖: 4 个 fix round（task #30 done / #31 done / #32 done / #33 in_progress）
- 后续: Sprint 全绿 → 用户授权 main merge → prod rollout playbook（见 `production_rollout_playbook_20260511.md`）

## 估时

- Stage 6 完整执行: 1-2 hour
- failure 复盘 + 修复迭代: 1-2 day（最坏）

## Boundary Confirmations

- production_5432_touched=false
- production_8001_touched=false
- 仅 dev DB + dev 端口
- 仅本地执行，CI 在 GitHub Actions 上单独跑
