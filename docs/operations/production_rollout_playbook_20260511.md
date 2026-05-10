# Production Rollout Playbook - Sprint 2026-05-11

**Author**: Claude Code 战略 session (overnight 2026-05-11)
**Status**: DRAFT — adoption pending Stage 6 GREEN
**Scope**: paper_v2 capture + dw-foundation T12+T14a/b/c + Codex governance + pipeline + market.regime_label

## §1 Sprint 2026-05-11 涉及 4 个分支

| 分支 | 主要内容 | 风险 |
|---|---|---|
| `claude/paper-v2-vnpy-mvp-20260508` | T5/T6.1/T6.2 capture fields + daemon outbox | 中（生产 runtime 改动）|
| `claude/dw-foundation-20260510` | T12 22 张表 + T14a/b/c handlers + Batch A/C scripts | 中（仅 qe_archive schema，handler default off）|
| `claude/pipeline-foundation-20260510` | bugs 注册表 + nox 扩展 + MCP server + CI workflow + DR | 低（流水线和文档为主）|
| `codex/qe-governance-integration-20260509` | 6 governance migrations + enable_paper 严格收紧 | **高**（4 prod packages 当前都 paper_ready=false → 收紧后全 reject）|

## §2 Rollout 顺序（4 阶段）

### Phase R1: 流水线和文档（最低风险，可先合）

1. `claude/pipeline-foundation-20260510` → main
   - 只动 `tests/aistock_validation/` + `scripts/dev_db/` + `scripts/aistock_mcp_server.py` + `.github/workflows/` + `.mcp.json` + docs
   - 不动业务代码
   - main merge 后立即可用 MCP + nightly CI

合 main 步骤：
```bash
git checkout main
git fetch origin
git merge --no-ff origin/claude/pipeline-foundation-20260510 \
  -m "merge: pipeline-foundation Sprint 2026-05-11 (Stage 1-5 + fix round 1)"
git push origin main
```

合后验证：
```bash
nox -s l0  # 静态门控
nox -s validation_module_registry_l0  # 模块注册
pytest backend/tests/test_aistock_mcp_server.py -q  # MCP server
# 33 tests should pass
```

### Phase R2: market.regime_label 算法（dw-foundation 子集）

仅取 dw-foundation 分支中 T10/T16 的 commit（regime_label DDL + cron + fetch_percentile）：

```bash
# 已在 main: bfb5f58 T10 DDL
# 待合: 13dd03c T16 fetch_percentile + cron skeleton
# 但 13dd03c 与 dw-foundation 其他工作耦合，建议整体合 dw-foundation 而非 cherry-pick
```

**决策**: 不单独 cherry-pick T10/T16，等 R3 整体合 dw-foundation。

### Phase R3: dw-foundation（schema + handlers）

**前置门**:
- ✅ Codex T14b/c fix round 3 review = PASS
- ✅ Stage 6 GREEN
- ✅ BUG-008 评估（partial fix，决定 defer 或 fix）
- ✅ 用户授权

合 main 步骤：
```bash
git checkout main
git pull origin main  # 拉 R1 后的 main

git merge --no-ff origin/claude/dw-foundation-20260510 \
  -m "merge: dw-foundation Sprint 2026-05-11 (T12 + T14a-d + Batch A/C + T16)"
git push origin main
```

**生产 DDL apply**（不在 git merge 内）:
```bash
# 用户授权后，在 prod DB 上 apply T12
docker exec aistock-pg psql -v ON_ERROR_STOP=1 -U postgres -d aistock <<SQL
BEGIN;
\\i /tmp/init_qe_archive_paper_v2_extension_20260510.sql
\\i /tmp/migration_archive_complete_marker.sql  -- T14b/c round 3 ALTER TABLE
SELECT count(*) FROM pg_tables WHERE schemaname='qe_archive';
-- 期望: 27 baseline + 22 new + 4 partition = 53
COMMIT;
SQL
```

**Worker default 仍 disabled**（D5 Q2.c），需要 ops 单独通过 env 启用。

### Phase R4: paper-v2 capture + Codex governance（最高风险，最后合）

**前置门**:
- ✅ R1 + R2 + R3 全过
- ✅ Codex paper-v2 fix round 1 二轮 review = PASS
- ✅ Phase 3 三方绿 + Stage 6 GREEN
- ✅ 4 prod packages 的 stability evidence + protected_asset_ledger 已 backfill 评估
- ✅ 用户**单独授权**（这是最危险的一步）

**风险**: Codex governance 严格收紧后，4 prod packages 当前都 paper_ready=false。merge 后 paper v2 实盘将无法 enable_paper 任何 package，**直到 evidence 补齐**。

**两种合法路径**:

#### R4-A: 严格门，先补 evidence 再合 governance
```
1. 先合 paper-v2 分支（capture + daemon），不影响 enable_paper
2. 评估 4 prod packages 的 stability evidence + protected_asset_ledger 缺失原因
3. 决策: 补 evidence（可能耗 1-2 day）还是接受 P1 临时 disabled
4. evidence 就绪后合 codex governance 分支
5. 实盘验证 paper v2 capture + new strict gate 都 OK
```

#### R4-B: 同步合，接受短期 disabled
```
1. 一次性合 paper-v2 + codex governance 两个分支
2. 文档记录 4 packages 当前 disabled 是已知预期
3. evidence 补齐后才能恢复 enable_paper
4. 实盘验证 capture（disabled 也能验 capture 写路径）
```

**推荐 R4-A**：可控性更高，可以分步验证。

合 main 步骤（R4-A 路径）：
```bash
# Step 1: 先合 paper-v2
git checkout main
git pull
git merge --no-ff origin/claude/paper-v2-vnpy-mvp-20260508 \
  -m "merge: paper-v2 Sprint 2026-05-11 (T5/T6.1/T6.2 capture + daemon outbox)"
git push origin main

# Step 2: 用户授权后 prod backend 8001 重启（用户操作）

# Step 3: 验证 paper v2 实盘 capture 行为（dev port 8012 → prod 8001）

# Step 4: evidence backfill（如有）

# Step 5: 合 codex governance（用户单独授权）
git merge --no-ff origin/codex/qe-governance-integration-20260509 \
  -m "merge: codex governance Sprint 2026-05-11 (Q1+Q2+BUG-023 + Phase 1A migrations)"
git push origin main

# Step 6: prod DB 上 apply Codex governance 6 个 migrations（用户授权）
```

## §3 关键决策门

| 门 | 决策者 | 内容 |
|---|---|---|
| G1: R1 合 main | 用户 | pipeline 合 main，无生产影响 |
| G2: R3 合 main | 用户 | dw-foundation 合 main，含 schema 改动但 handler default off |
| G3: prod DB apply T12 | 用户 | qe_archive 22 张新表加到 prod DB |
| G4: R4-A vs R4-B | 用户 | 严格门 vs 接受短期 disabled |
| G5: paper-v2 合 main | 用户 | capture 改 prod runtime |
| G6: prod backend 8001 重启 | 用户 | 让 capture 在 prod 生效 |
| G7: governance 合 main | 用户 | enable_paper 严格收紧 |
| G8: governance prod DB apply | 用户 | strategy_pkg 6 个 migrations |
| G9: evidence backfill | 用户 + Codex | 4 packages 重新 paper_ready |
| G10: worker enable | 用户 + ops | 启 archive worker 处理 outbox |

## §4 Rollback 策略

| Phase | Rollback 方案 |
|---|---|
| R1 失败 | git revert merge commit + push（pipeline 工作面隔离，零生产影响）|
| R3 失败 | git revert + 写 down migration drop 22 张新表（如已 apply prod DB）|
| R4 失败 | git revert paper-v2 → restart prod backend 8001 → 旧代码生效 |
| Governance 失败 | git revert + 跑 down migration（高风险，需测试反向 migration）|

**强烈建议**: 每次 prod DDL apply 前用 `dr_snapshot_prod_db.py`（Stage 5 实施）做快照，rollback 时可还原。

## §5 Monitoring + Alerting（合后 24h）

### paper_v2 capture 字段
```sql
-- 每小时跑一次
SELECT
  COUNT(*) FILTER (WHERE created_at IS NULL) AS missing_created_at,
  COUNT(*) FILTER (WHERE updated_at IS NULL) AS missing_updated_at,
  COUNT(*) FILTER (WHERE intended_price IS NULL) AS market_orders,
  COUNT(*) FILTER (WHERE fill_market_context IS NULL) AS missing_market_context,
  COUNT(*) AS total
FROM paper_v2.fills
WHERE filled_at >= NOW() - INTERVAL '1 hour';
```

期望: missing_* = 0；market_orders 大量（因为 strategy_package/runtime.py:716 全 MARKET），missing_market_context = 0。

### qe_archive worker（即使 disabled，监控 outbox 增长）
```sql
SELECT event_type, count(*) AS pending_count
FROM qe_archive.outbox_event
WHERE status='pending'
GROUP BY event_type;
```

期望: pending 不超过 10000（worker disabled 时增长，enable 后下降）。

### enable_paper 失败率（governance 合后）
```bash
# 在 backend 8001 logs grep
grep "StrategyPackageValidationError.*paper_ready" logs/backend.log | tail -10
```

期望: 4 prod packages 都报错（已知）。其他 packages 不应报错。

## §6 Communication

### 合 main 前
- 用户在 cross-tool drawer 通知 Codex 即将合 main
- Codex 知会 governance 工作面无并发改动

### 合 main 后
- 战略 session 在 cross-tool drawer 报告合并完成 + 各分支 SHA
- Codex ACK
- 各 worktree team Lead 知会下一阶段任务

## §7 Failure Triage

### Sev1（P0）— 实盘交易中断
- 立即 prod backend 8001 rollback (git revert + 重启)
- DR snapshot 备用
- 10 min 内通知用户

### Sev2（P1）— capture 字段批量 NULL
- 不立即 rollback，但停 enable_paper（防 governance 误触发）
- 评估 root cause
- 2h 内决策

### Sev3（P2）— 单 package enable_paper 失败
- 标记 package 为 known issue
- 系统继续运行
- 24h 内修

## §8 Timeline 估计（最快路径）

```
Day 1 (今天)         战略 session overnight + Codex 4 review verdicts
Day 2 (明天)         T14b/c fix round 3 完成 + Codex review
                     Stage 6 全面验证启动
Day 3                Stage 6 GREEN + 4 层交叉检查
Day 3 evening        R1 合 main + 验证
Day 4 morning        R3 合 main + prod DDL apply T12
Day 4 evening        R4-A 第 1 步: paper-v2 合 main + prod backend 8001 重启
Day 5                 capture 行为验证（实盘）
Day 5+               evidence backfill 决策 + governance 合 main
```

总: ~5 天到 prod rollout 完成。

## §9 References

- `docs/process/cross_tool_communication_protocol_v2_20260511.md` — 协议
- `docs/cross_tool/20260511_strategy_DISPATCH_pipeline_stage_6_full_validation.md` — Stage 6
- `docs/architecture/data_warehouse_extension_design_20260510.md` — DW design
- `docs/process/dual_party_verify_20260510.md` — 双方验证流程
