# [DISPATCH-PREP] T15 — factor pipeline emit hook (factor.recompute.completed event)

**from**: claude_code_strategy
**to**: AIstock 主仓 worktree (战略 session 直接派发, 无独立 RDAgent 团队)
**date**: 2026-05-11 (drafted overnight, dispatch when T14b/c round 3 PASS + governance branch merge plan finalized)
**verdict**: DISPATCH-PREP

## Summary

factor_pipeline_v2 / factor_official_evaluation_service 当前不发 outbox events。dw-foundation FactorValueArchiveHandler (T14c) 等 `factor.recompute.completed` 事件触发 archive。T15 在 factor 计算完成处加 emit hook，以幂等 idempotency_key 触发 handler。

per D5 Q4.b 决策（用户 2026-05-10 校正）: factor pipeline emit hook 由战略 session 直接派发到 AIstock 主仓 worktree，无独立 RDAgent 团队。

## 触发条件

- ✅ T14b/c fix round 3 PASS（FactorValueArchiveHandler 接口稳）
- ✅ BUG-029 (factor_value data bounds filter) 已修
- ✅ Codex governance branch merge 计划明确（避免 emit_hook 与 enable_paper 收紧并发改动）
- ✅ 战略 session 协调 paper-v2 + dw-foundation 时序

## 范围

- 修改 `backend/services/quantevolver/factor_official_evaluation_service.py:_save_metrics()` 加 emit hook
- 修改 `backend/services/quantevolver/factor_value_pipeline.py` parquet write 完成处加 hook（如需）
- 用 PG outbox helper 写 `qe_archive.outbox_event`
- payload schema 含: factor_name + code_text_hash + data_start + data_end + snapshot_date + recompute_run_id（optional）
- routing_class='archive'（per T14a payload-based filter）
- idempotency_key per D5 Q4.c: (factor_name, code_text_hash, trade_date, code)

## 实施步骤

### Step 1 切到 AIstock 主仓（不是 worktree, 主仓 working directory）

```bash
cd F:/Dev/AIstock
# 创建 emit hook 工作分支
git checkout -b claude/factor-emit-hook-20260511 origin/main
```

### Step 2 实施 emit hook in _save_metrics()

```python
# backend/services/quantevolver/factor_official_evaluation_service.py

import hashlib
from datetime import datetime, timezone
from backend.db.pg_pool import get_conn

EVENT_TYPE = 'factor.recompute.completed'
SCHEMA_VERSION = 1
ROUTING_CLASS = 'archive'

def _emit_factor_recompute_event(
    factor_name: str,
    code_text_hash: str,
    data_start: str,
    data_end: str,
    snapshot_date: str,
    recompute_run_id: str | None = None,
) -> str:
    """Emit factor.recompute.completed to qe_archive.outbox_event.
    
    Idempotency: ON CONFLICT (event_id) DO NOTHING.
    event_id = qear_evt_<sha256[:24] of canonical input>
    """
    canonical_input = "|".join([
        EVENT_TYPE,
        factor_name,
        code_text_hash,
        data_start,
        data_end,
        snapshot_date,
        recompute_run_id or "",
    ])
    event_id = "qear_evt_" + hashlib.sha256(canonical_input.encode()).hexdigest()[:24]
    
    payload = {
        'schema_version': SCHEMA_VERSION,
        'factor_name': factor_name,
        'code_text_hash': code_text_hash,
        'data_start': data_start,
        'data_end': data_end,
        'snapshot_date': snapshot_date,
        'recompute_run_id': recompute_run_id,
        'occurred_at': datetime.now(timezone.utc).isoformat(),
        'routing_class': ROUTING_CLASS,
    }
    
    sql = """
        INSERT INTO qe_archive.outbox_event
            (event_id, event_type, payload, status, occurred_at, source_run_id)
        VALUES
            (%s, %s, %s::jsonb, 'pending', NOW(), %s)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
    """
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                event_id,
                EVENT_TYPE,
                json.dumps(payload),
                recompute_run_id,
            ))
            inserted = cur.fetchone()
            conn.commit()
    
    return event_id


# 在 _save_metrics() 末尾调用
def _save_metrics(self, ...):
    # ... existing save logic ...
    
    # T15 emit hook
    try:
        event_id = _emit_factor_recompute_event(
            factor_name=self.factor_name,
            code_text_hash=self.code_text_hash,
            data_start=str(self.data_start),
            data_end=str(self.data_end),
            snapshot_date=str(self.snapshot_date),
            recompute_run_id=getattr(self, 'recompute_run_id', None),
        )
        logger.info(f"factor.recompute.completed emitted: {event_id}")
    except Exception as exc:
        # T14a no-silent-error policy: emit failure should not silently break factor save
        # but factor save itself succeeded; log warning + raise to surface
        logger.error(f"factor emit hook failed for {self.factor_name}: {exc}")
        raise
```

### Step 3 加测试

```python
# backend/tests/quantevolver/test_factor_emit_hook.py

def test_emit_factor_recompute_event_writes_outbox():
    # mock conn + cursor, capture INSERT
    # assert event_id starts with 'qear_evt_'
    # assert payload contains routing_class='archive'

def test_emit_idempotent_on_conflict():
    # call twice with same args
    # assert ON CONFLICT DO NOTHING; second call same event_id, no error

def test_save_metrics_emits_after_save():
    # integration: mock save_metrics, verify _emit_factor_recompute_event called

def test_emit_failure_propagates():
    # mock conn.execute raises; assert _save_metrics raises
    # (per no-silent-error policy)
```

### Step 4 跑现有 quantevolver 测试套

```bash
conda activate AIstock
pytest backend/tests/unified_engine/ -k "factor" -v
pytest backend/tests/quantevolver/test_factor_emit_hook.py -v
```

确保 emit 不破坏现有 factor save 行为。

### Step 5 dev DB 集成验证

```bash
# 手动跑一个 factor recompute (dev DB, 5433):
python -m backend.services.quantevolver.factor_official_evaluation_service \
  --factor sample_factor \
  --target-db dev \
  --emit-hook-dry-run False  # 真发 event

# 然后查 dev DB outbox_event:
docker exec -i aistock-pg-dev psql -U postgres -d aistock_dev -c "
  SELECT event_id, event_type, payload->>'factor_name', status
  FROM qe_archive.outbox_event
  WHERE event_type='factor.recompute.completed'
  ORDER BY occurred_at DESC LIMIT 5
"
```

期望: 1 行新 event，status=pending（因 worker disabled）。

### Step 6 BUG 状态更新

如有相关 BUG（目前没有 BUG specific to T15 emit hook），通过 MCP report_bug 创建 BUG-036:
- title: T15 factor emit hook missing
- status: open → fixed (本 commit)

### Step 7 commit + push

```bash
git add backend/services/quantevolver/factor_official_evaluation_service.py \
        backend/tests/quantevolver/test_factor_emit_hook.py
git commit -m "feat(quantevolver): T15 factor emit hook (factor.recompute.completed -> qe_archive.outbox_event)"
git push -u origin claude/factor-emit-hook-20260511
```

### Step 8 cross-tool drawer 通知（v2 协议）

```
[REVIEW] T15 factor emit hook implemented

from=claude_code_strategy
detail_doc=docs/cross_tool/<this doc>
commit=<sha>
branch=claude/factor-emit-hook-20260511

Emits factor.recompute.completed to qe_archive.outbox_event with payload routing_class='archive'.
Idempotent via event_id sha256.
Awaiting Codex review + dw-foundation worker enable smoke.
```

## 验收

- emit hook 实施 + 4 测试覆盖
- dev DB 集成验证: outbox_event 含新 event
- 现有 quantevolver 测试不破
- payload 含 routing_class='archive'
- idempotency_key 与 D5 Q4.c 一致

## 后续衔接

- worker enable（D5 Q2.c default off）后:
  - FactorValueArchiveHandler 自动消费 outbox events
  - 写 qe_archive.factor_value 分区表
  - dw-foundation 端集成测试覆盖

## 估时

3-4 hour（含 dev DB 集成验证）

## Boundary Confirmations

- 战略 session 直接派发到主仓（无独立 RDAgent 团队，per 用户 2026-05-10 校正）
- 不修改 paper-v2 / dw-foundation worktree 代码
- 不修改 Codex governance branch
- 不动 prod DB（仅 dev DB 集成验证）
- worker default 仍 disabled（不在本任务范围）

## References

- D5 Q4 (factor pipeline emit hook ownership): `docs/architecture/data_warehouse_extension_design_20260510.md` §6
- T14c FactorValueArchiveHandler: `backend/services/qe_archive/handlers/factor_value_archive_handler.py`
- BUG-029 factor_value data bounds: `tests/aistock_validation/bugs/20260511_BUG-029-factor-value-data-bounds-ignored.json`
