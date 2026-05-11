# [REVIEW] T15 — factor pipeline emit hook (factor.recompute.completed)

**from**: claude_code_strategy (派发方亦执行方, per D5 Q4.b)
**to**: codex_session (review)
**date**: 2026-05-11
**status**: implemented + tests pass + dev DB integration verified
**branch**: `claude/factor-emit-hook-20260511`
**commit**: `e197bc4d3a7ec0a00ecb4ddcdbf5f13b558b20fe`
**worktree**: `F:/Dev/AIstock_worktrees/factor-emit-hook-20260511`
**dispatch_doc**: `docs/cross_tool/20260511_strategy_DISPATCH_t15_factor_emit_hook.md`

## Summary

`factor_official_evaluation_service._save_metrics()` 在指标入库成功后立即 emit
`factor.recompute.completed` 事件到 `qe_archive.outbox_event`。事件被 dw-foundation
`FactorValueArchiveHandler` (T14c) 消费，写入 `qe_archive.factor_value` 分区表。

Worker 在 D5 Q2.c 决策下默认 disabled，因此事件以 `status='pending'` 累积，等待
worker enable smoke。

## Implementation Detail

### 文件改动 (单文件 + 1 测试)

| 文件 | 改动 |
|---|---|
| `backend/services/quantevolver/factor_official_evaluation_service.py` | +142 行: 模块常量 + `_emit_factor_recompute_event()` helper + `_save_metrics()` emit 集成 |
| `backend/tests/quantevolver/__init__.py` | 新建 (test package marker) |
| `backend/tests/quantevolver/test_factor_emit_hook.py` | 新建, 5 个测试 |

未触碰: `paper_v2` / `strategy_pkg` / `dw-foundation` worktree 代码。未触碰 prod DB。

### `_emit_factor_recompute_event(...)` 设计要点

- **event_id**: `"qear_evt_" + sha256(canonical_input)[:24]`，canonical_input =
  `event_type|factor_name|code_text_hash|data_start|data_end|snapshot_date|recompute_run_id`
- **idempotency**: SQL 以 `ON CONFLICT (event_id) DO NOTHING` 兜底，同输入再次 emit 是 no-op
- **source-side dedup**: `source_sub_id` 编码同一 canonical 字段集 (sans event_type)，
  防止 schema 上的 `uq_qear_outbox_source_terminal` 与 event_id 不一致地冲突
- **payload schema_version=1**, 含：
  - `factor_name`, `code_text_hash`, `data_start`, `data_end`, `snapshot_date`,
    `recompute_run_id`, `occurred_at` (ISO-8601 UTC), `routing_class='archive'`
- **emit 失败**: `logger.error(..., exc_info=True)` + `raise` (no-silent-error policy / T14a)
- **可复用调用方连接**: 接受 `conn=` 参数；不传则自取 `get_conn()`

### `_save_metrics()` 集成

1. 插入循环里累计 `per_factor_inserted` (per-factor 插入计数) 和
   `per_factor_full_bounds` (`eval_window=='full'` 行的 data_start/data_end)
2. UPSERT 循环结束后，同一 `with get_conn() as conn` 块内：
   - 一次性 `SELECT factor_name, code_text FROM aistock_factor_catalog WHERE factor_name = ANY(%s)`
   - 每个因子计算 `hashlib.sha256(code_text)[:16]` 作为 `code_text_hash`
   - 调用 `_emit_factor_recompute_event(..., conn=conn)`
3. `recompute_run_id` 取自 `engine_data['calc_batch_id']`
4. 任何因子的 `code_text` 为空 → `raise RuntimeError(...)` (no-silent-error)
5. 返回值新增 `emitted_events: List[str]` 字段

由于 `_on_factor_success` 回调每个因子触发一次 `_save_metrics`，每个因子的成功
计算 → 恰好 1 条 outbox 事件。

## Tests

```
backend/tests/quantevolver/test_factor_emit_hook.py
  test_emit_writes_outbox                              PASSED
  test_emit_idempotent_on_conflict                     PASSED
  test_save_metrics_emits_after_save                   PASSED
  test_emit_failure_propagates                         PASSED
  test_save_metrics_emit_failure_propagates            PASSED

5 passed in 0.92s
```

覆盖矩阵：

| 测试 | 验证点 |
|---|---|
| `test_emit_writes_outbox` | event_id 前缀/长度/sha256 确定性；SQL `ON CONFLICT (event_id) DO NOTHING`; payload 含 routing_class='archive' + 所有必需字段 |
| `test_emit_idempotent_on_conflict` | 相同 canonical input → 相同 event_id；两次 INSERT 均带 ON CONFLICT 子句 |
| `test_save_metrics_emits_after_save` | _save_metrics 路径下：从 `code_text` 派生 hash；payload `data_start/data_end` 取 `full` 窗口；outbox INSERT 顺序在 metric UPSERT 之后 |
| `test_emit_failure_propagates` | 模拟 cursor.execute 抛错 → `_emit_factor_recompute_event` 重新抛出 (no-silent-error) |
| `test_save_metrics_emit_failure_propagates` | 端到端：outbox 失败传播到 `_save_metrics` 调用方 |

未破坏现有套件：

```
backend/tests/unified_engine/ -k "official or factor_cache"
  8 passed, 318 deselected in 18.58s
```

## Dev DB 集成验证

目标 DB: `127.0.0.1:5433 / aistock_dev` (TDX_DB_DEV_* per `.env`)

```python
TDX_DB_HOST=127.0.0.1 TDX_DB_PORT=5433 TDX_DB_NAME=aistock_dev \
TDX_DB_USER=postgres  TDX_DB_PASSWORD=... \
python -c "
from backend.services.quantevolver.factor_official_evaluation_service import _emit_factor_recompute_event
_emit_factor_recompute_event(
    factor_name='T15_smoke_factor',
    code_text_hash='deadbeef00000001',
    data_start='2020-01-02',
    data_end='2026-04-30',
    snapshot_date='2026-04-30',
    recompute_run_id='t15_smoke_run_001',
)
"
```

结果 (清理前):

```
event_id:     qear_evt_1f779cc4a559a5516e1c3efc
event_type:   factor.recompute.completed
source_system:qe_factor_official_evaluation
source_id:    T15_smoke_factor
source_sub_id:2026-04-30|deadbeef00000001|2020-01-02|2026-04-30|t15_smoke_run_001
status:       pending
payload: {
  "data_end": "2026-04-30",
  "data_start": "2020-01-02",
  "factor_name": "T15_smoke_factor",
  "occurred_at": "2026-05-11T03:41:11.810013+00:00",
  "routing_class": "archive",
  "snapshot_date": "2026-04-30",
  "code_text_hash": "deadbeef00000001",
  "schema_version": 1,
  "recompute_run_id": "t15_smoke_run_001"
}
total factor.recompute.completed rows: 1
```

幂等再发：second call 返回相同 `event_id qear_evt_1f779cc4a559a5516e1c3efc`，
DB 行计数仍为 1（ON CONFLICT 命中）。

Smoke 行已清理 (`DELETE FROM qe_archive.outbox_event WHERE source_id='T15_smoke_factor'`)。

## Boundary Confirmations

- ✅ 仅改 `backend/services/quantevolver/factor_official_evaluation_service.py` + 1 个新测试文件
- ✅ 未改 `paper_v2` / `strategy_pkg` / `dw-foundation` worktree
- ✅ 未改 Codex governance branch
- ✅ 未改 prod DB（仅 dev DB 5433 集成验证）
- ✅ worker 维持 disabled (D5 Q2.c) — emit 累积 pending 事件，等待 worker enable smoke
- ✅ 与 T14c FactorValueArchiveHandler payload 契约对齐
  (schema_version=1, code_text_hash, data bounds, snapshot_date, routing_class)
- ✅ 与 T14a payload-based routing filter 对齐 (`routing_class='archive'`)
- ✅ 与 D5 Q4.c idempotency_key 兼容（row-level dedup 由 handler 用
  `(factor_name, code_text_hash, trade_date, code)` 自决；事件层用 event_id sha256）

## Follow-ups (not in this task)

1. **Worker enable smoke** (D5 Q2.c 后续)：开启 `FactorValueArchiveHandler` 消费
   pending 事件，端到端写 `qe_archive.factor_value` 分区表
2. 若 `factor_value_pipeline.py` parquet write 完成路径也需要 emit（dispatch doc
   Step 2 提到"如需"），目前未实现 — 当前路径全部经 `_save_metrics`，单一 emit 点
   已覆盖。若后续直接绕过 `_save_metrics` 写 parquet 的回放/补算工具流出现，再
   补 emit 点。
3. `recompute_run_id` 当前取 `calc_batch_id`，是否升级为更结构化的 run id (例如
   `qe_archive.run.run_id`) 待 D5 Q4.d 决策。

## References

- Dispatch doc: `docs/cross_tool/20260511_strategy_DISPATCH_t15_factor_emit_hook.md`
- D5 Q4 (factor emit hook ownership): `docs/architecture/data_warehouse_extension_design_20260510.md` §6
- T14c handler: `backend/services/qe_archive/handlers/factor_value_archive_handler.py`
- T14a payload routing: `backend/services/qe_archive/repository.py::insert_outbox_event`
- outbox schema: `backend/db/init_qe_archive_schema.py` line 659+
