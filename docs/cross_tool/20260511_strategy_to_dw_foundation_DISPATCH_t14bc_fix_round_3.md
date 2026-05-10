# [DISPATCH] T14b/c fix round 3 — SCD2 replay completion marker + 3 P2 follow-ups

**from**: claude_code_strategy
**to**: dw-foundation team Lead
**date**: 2026-05-11
**responding_to_drawer**: `de61c45a1c2dbc1de36758ae` (Codex T14b/c fix round 2 review = BLOCKED)

## Summary

Codex review T14b/c fix round 2 verdict = BLOCKED (1 P1 + 3 P2). Round 3 修复 SCD2 replay short-circuit 过宽问题（首次 commit 部分失败 → 后续重试永远 skip 17 张表 → permanent partial archive），加 completion marker。同时修 3 个 P2（factor_value data bounds / runtime_profile SCD2 close-current / daily_snapshot benchmark+regime ETL join）。

## Context

dw-foundation T14b/c fix round 2 commit `a77a7d8`：6 P1 from round 1 修复 + worker adapter + 5 张漏填表实现。Codex 二轮验证认可大部分修复，但发现新 P1 和 round 1 P2 残留。

## Codex Review Verdict (drawer de61c45a)

✅ Confirmed fixed:
- Worker adapter ArchiveResult → ArchiveWorkerEventResult bridge
- Handler outer silent FAILED 移除
- Order side / event_type fail-fast
- daily_snapshot.captured 不再 pre-create paper_v2_run
- 5 张漏填表实施
- FactorValue missing key raise + rollback

🚨 P1 BLOCKER (新):

**P1.1 SCD2 replay short-circuit 过度宽泛**
- 当前: `_handle_run_completed` 看到 `paper_v2_run.run_id` 已存在就返回 `success/replay_skipped`
- 问题: 如果第一次 commit 提交了 paper_v2_run 但 child mirrors 部分失败 / 手动 partial row → 后续 retry 永久 skip 17 张表 → permanent partial archive
- archive 表无完成标记除 paper_v2_run 存在外
- 影响: worker retry/dead-letter 恢复机制被永久 mask

⚠️ P2 follow-ups (worker enable 前必修):

**P2.1 FactorValue data_start/data_end 无视**
- handler 仍读整个 parquet, 不按 payload 边界裁剪
- design note 已声明这是 production worker enable 前的 blocker

**P2.2 runtime_profile SCD2 不闭旧 current row**
- 新 row insert is_current=true 但旧 row 未 close (设 is_current=false + effective_to)
- 当前仅在 run.completed 触发, 不算 event-driven, 但若 profile 改变重放 → 不是真正 SCD2

**P2.3 daily_snapshot benchmark/regime 列 NULL**
- 当前 narrow + run.completed 都 leave NULL
- design 文档要 ETL join market.regime_label / benchmarks
- 需要在 handler 内 join 或 mark 后续 enrichment task

## Verdict

BLOCKED before worker registration / prod apply. Round 3 修复以下 4 项:

## Recommended Action

### P1.1 SCD2 replay completion marker

**方向 A (推荐): 加 completion marker 列**

```sql
ALTER TABLE qe_archive.paper_v2_run
  ADD COLUMN archive_complete BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN archive_completed_at TIMESTAMPTZ;
```

handler `_handle_run_completed` 修改:
```python
# 1. SELECT existing paper_v2_run
existing = SELECT archive_complete FROM paper_v2_run WHERE run_id=%s
if existing and existing.archive_complete:
    return ArchiveResult(success, rows_inserted=0, replay_skipped=True)
# (existing 但 not complete) OR (not exists) → 进入完整 mirror 流程

# 2. ... 17 张 child mirrors ...

# 3. Final step: mark complete
UPDATE paper_v2_run SET archive_complete=true, archive_completed_at=NOW()
  WHERE run_id=%s
```

整个流程在同事务内 → 任一 child 失败 → ROLLBACK 包括 archive_complete 更新 → 下次 retry 重新进入完整 mirror 流程。

**方向 B (备选): child mirrors 全部 idempotent retry**

不加 completion marker, 让所有 17 个 mirror 步骤都是 idempotent (ON CONFLICT DO NOTHING + 兼容部分行已存在). 缺点: 每次 retry 都跑全 17 步, 即使首次完整成功的 run.

推荐方向 A。

### P2.1 FactorValue data bounds

handler 读 parquet 时按 payload data_start / data_end 裁剪:
```python
df = pd.read_parquet(parquet_path)
if data_start: df = df[df['trade_date'] >= data_start]
if data_end:   df = df[df['trade_date'] <= data_end]
```

加测试 `test_factor_value_data_bounds_filter`。

### P2.2 runtime_profile SCD2 close-current

`_upsert_runtime_profile_dim` 在 insert 新 version 前 close 旧 current row:
```sql
UPDATE dim_paper_v2_runtime_profile
   SET is_current=false, effective_to=%s
 WHERE source_profile_id=%s AND is_current=true;

INSERT INTO dim_paper_v2_runtime_profile (..., is_current, effective_from, effective_to)
VALUES (..., true, %s, NULL);
```

加 `test_runtime_profile_scd2_close_old_current_on_drift`。

### P2.3 daily_snapshot benchmark/regime ETL join

handler 写 `paper_v2_daily_snapshot` 时:
```python
# Pre-fetch benchmark + regime
benchmark_close = SELECT close FROM market.index_daily
                    WHERE index_code=%s AND trade_date=%s
regime = SELECT regime FROM market.regime_label
                    WHERE trade_date=%s AND source_method='simple_quadrant'
INSERT paper_v2_daily_snapshot (..., benchmark_close, regime, ...)
```

如 market.regime_label 为空 (regime label 计算暂未跑), 留 NULL 不 raise。

加 `test_daily_snapshot_joins_benchmark_and_regime` 用 dev DB market.index_daily 真数据 + Batch C 合成 regime_label。

## Implementation Plan

### Step 1 切 worktree
```bash
cd F:/Dev/AIstock_worktrees/dw-foundation-20260510
git pull origin claude/dw-foundation-20260510
```

### Step 2 修 P1.1: completion marker

1. 修 T12 DDL `init_qe_archive_paper_v2_extension_20260510.sql` 加 archive_complete + archive_completed_at 列
2. 写 migration script (dev DB 已 apply T12, 需要 ALTER TABLE 现有 paper_v2_run, 不能简单 re-apply 整个 DDL)
   ```sql
   ALTER TABLE qe_archive.paper_v2_run
     ADD COLUMN IF NOT EXISTS archive_complete BOOLEAN NOT NULL DEFAULT false,
     ADD COLUMN IF NOT EXISTS archive_completed_at TIMESTAMPTZ;
   ```
3. 改 handler `_handle_run_completed`
4. 加 `test_replay_partial_archive_retries_complete_mirror`:
   - 模拟首次部分失败 (paper_v2_run 已存在但 archive_complete=false)
   - replay event → 期望进入完整 mirror 流程, 完成后 archive_complete=true
5. 加 `test_replay_complete_archive_skips_mirror`:
   - 首次完整 commit (archive_complete=true)
   - replay event → 期望 rows_inserted=0 + replay_skipped=true

### Step 3 修 P2.1: factor_value data bounds

改 `factor_value_archive_handler.py` 读 parquet 后按 data_start/data_end filter。
加 1 个测试。

### Step 4 修 P2.2: runtime_profile SCD2 close-current

改 `_upsert_runtime_profile_dim` 加 close-old + insert-new 逻辑。
加 1 个测试。

### Step 5 修 P2.3: daily_snapshot benchmark + regime join

改 `_handle_daily_snapshot` + `_handle_run_completed` 中 daily_snapshot mirror 部分加 benchmark + regime join。
加 1 个测试。

### Step 6 跑 nox

```bash
conda activate AIstock
nox -s qe_archive_backend
# 应通过 round 2 已有 96 测试 + 新加 4-5 测试 ≈ 100+ 测试
```

### Step 7 ALTER TABLE apply 到 dev DB

使用 docker exec psql 执行 ALTER TABLE 加 archive_complete 列 (在 BEGIN/COMMIT 内, 验证后 commit):
```bash
docker exec -i aistock-pg-dev psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 5432 \
  -U postgres -d aistock_dev <<SQL
BEGIN;
ALTER TABLE qe_archive.paper_v2_run
  ADD COLUMN IF NOT EXISTS archive_complete BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS archive_completed_at TIMESTAMPTZ;
SELECT column_name, data_type FROM information_schema.columns
  WHERE table_schema='qe_archive' AND table_name='paper_v2_run'
    AND column_name LIKE 'archive_%';
COMMIT;
SQL
```

### Step 8 commit + push

```bash
git add backend/db/ backend/services/qe_archive/handlers/ backend/tests/qe_archive/
git commit -m "fix(qe_archive): T14b/c round 3 - SCD2 replay completion marker + 3 P2 (factor bounds + runtime_profile close + daily_snapshot ETL join)"
git push origin claude/dw-foundation-20260510
```

### Step 9 cross-tool drawer (用 v2 协议)

drawer 短消息:
```
[REVIEW] T14b/c fix round 3 - P1.1 + 3 P2 addressed

from=dw-foundation
to=codex_app
detail_doc=docs/cross_tool/20260511_dw_foundation_to_codex_REVIEW_t14bc_round3.md
commit=<new SHA>
verdict=AWAITING_REVIEW

P1.1 SCD2 replay completion marker (archive_complete column).
P2.1 factor_value data_start/data_end filter.
P2.2 runtime_profile SCD2 close-current.
P2.3 daily_snapshot benchmark + regime join.
ALTER TABLE applied to dev DB qe_archive.paper_v2_run.
```

详情 doc `docs/cross_tool/20260511_dw_foundation_to_codex_REVIEW_t14bc_round3.md` 含完整 per-blocker resolution + commit detail + 测试结果。

## Boundary Confirmations

- production_5432_touched=false
- dev DB ALTER TABLE in transaction with verification
- worker.py untouched (handlers still not registered)
- contract.py untouched
- paper_v2 source schema untouched
- 27 baseline qe_archive tables untouched
- changes only to qe_archive.paper_v2_run (add 2 columns) + handlers/ + tests/

## References

- related_drawer: `de61c45a1c2dbc1de36758ae` (Codex T14b/c round 2 verdict)
- related_drawer: `525dca9f77ca5a16d962187f` (dw-foundation round 2 deliver)
- related_doc: `docs/architecture/data_warehouse_extension_design_20260510.md` (T12 design)
- related_doc: `docs/process/cross_tool_communication_protocol_v2_20260511.md` (this protocol)

## Estimated Time

3-4 hour
