# [DISPATCH-PREP] T13 — paper-v2 daemon emit() 加 payload routing_class

**from**: claude_code_strategy
**to**: paper-v2 team Lead
**date**: 2026-05-11 (drafted overnight, dispatch when T14b/c round 3 PASS)
**verdict**: DISPATCH-PREP

## Summary

paper-v2 daemon `event_log.emit()` 当前只写 event_type + payload，未加 `payload['routing_class']`。dw-foundation T14a 设计是 payload-based filter (PaperV2ArchiveHandler.can_handle 用 routing_class 过滤 telemetry)。INT-5b xfailed 等本任务。BUG-035 已入库。

## 触发条件

- ✅ T14b/c fix round 3 review PASS（dw-foundation 端 contract 稳定）
- ✅ 战略 session 协调 paper-v2 + dw-foundation 时序

## 范围

- 修改 `backend/services/paper_trading_v2/event_log.py` 中 `emit()` 函数
- 给所有 paper.daemon.* events 加 `payload['routing_class'] = 'telemetry'`
- 给所有 portfolio_run.completed / daily_snapshot.captured / config.changed events 加 `payload['routing_class'] = 'archive'`
- 同时（或单独 ALTER TABLE）保证 dev/prod outbox_event 表无 routing_class column 改动（payload 内即可）
- 加测试覆盖 + 取消 INT-5b 的 xfail

## 实施步骤

### Step 1 切到 paper-v2 worktree

```bash
cd F:/Dev/AIstock_worktrees/paper-v2-vnpy-mvp-20260508
git pull origin claude/paper-v2-vnpy-mvp-20260508
```

### Step 2 修 event_log.emit()

```python
# backend/services/paper_trading_v2/event_log.py

ARCHIVE_EVENTS = {
    'paper.portfolio_run.completed',
    'paper.daily_snapshot.captured',
    'paper.config.changed',
}

DAEMON_EVENTS = {
    'paper.daemon.run_started',
    'paper.daemon.intent_created',
    'paper.daemon.order_submitted',
    'paper.daemon.fill_received',
    'paper.daemon.order_rejected',
    'paper.daemon.order_cancelled',
    'paper.daemon.position_updated',
    'paper.daemon.run_completed',
    'paper.daemon.run_failed',
}

def _routing_class_for(event_type: str) -> str:
    if event_type in ARCHIVE_EVENTS:
        return 'archive'
    if event_type in DAEMON_EVENTS:
        return 'telemetry'
    raise ValueError(f"Unknown event_type {event_type!r}; must be in ARCHIVE_EVENTS or DAEMON_EVENTS")

def emit(event_type: str, payload: dict, ...):
    payload['routing_class'] = _routing_class_for(event_type)
    # existing emit logic ...
```

### Step 3 加测试

```python
# backend/tests/paper_trading_v2/test_daemon_pg_outbox.py 或新文件

def test_emit_archive_event_has_routing_class_archive():
    payload = {'run_id': 'paper_xxx', 'trade_date': '2026-05-11'}
    emit('paper.portfolio_run.completed', payload, ...)
    # assert payload['routing_class'] == 'archive'

def test_emit_daemon_event_has_routing_class_telemetry():
    payload = {'run_id': 'paper_xxx'}
    emit('paper.daemon.run_started', payload, ...)
    # assert payload['routing_class'] == 'telemetry'

def test_emit_unknown_event_type_raises():
    with pytest.raises(ValueError):
        emit('paper.unknown.foo', {}, ...)
```

### Step 4 取消 INT-5b xfail

修 `backend/tests/paper_trading_v2/test_daemon_outbox_dev_db.py`:

```python
# 之前: @pytest.mark.xfail(reason="T13 daemon routing_class payload pending")
# 改为: 直接跑

def test_routing_class_payload_telemetry_for_daemon_events(dev_dsn):
    # query qe_archive.outbox_event 中 paper.daemon.* events
    # assert payload->>'routing_class' == 'telemetry'
```

### Step 5 跑 nox

```bash
conda activate AIstock
nox -s paper_v2_backend
# 应 248+ passed, 0 failed, 0 xfail (INT-5b 已正常 pass)
```

### Step 6 BUG 状态更新

通过 MCP report_bug 或直接编辑 BUG-035:
- status: open → fixed
- fix_commit: <new SHA>
- 加 event: T13 implemented 路由通过 payload

### Step 7 commit + push

```bash
git add backend/services/paper_trading_v2/event_log.py \
        backend/tests/paper_trading_v2/
git commit -m "feat(paper-v2): T13 - daemon emit adds payload routing_class (archive/telemetry)"
git push origin claude/paper-v2-vnpy-mvp-20260508
```

### Step 8 cross-tool drawer 通知（v2 协议）

```
[REVIEW] T13 paper-v2 daemon routing_class - payload-based per T14a design

from=paper-v2-team
detail_doc=docs/cross_tool/<this doc>
commit=<sha>

Resolves BUG-035. INT-5b xfail removed (now passing).
```

## 验收

- 所有 paper.daemon.* events 在 outbox_event.payload 含 routing_class='telemetry'
- 所有 portfolio_run.completed / daily_snapshot.captured / config.changed events 含 routing_class='archive'
- INT-5b xfail 移除并 pass
- BUG-035 status=fixed
- Codex 后续 review 时可 verify

## 估时

1-2 hour

## Boundary Confirmations

- 仅 paper-v2 worktree
- 无 schema migration（payload-based）
- 不动 dw-foundation handler 代码（依赖 payload 已准备好）
- 不动 prod DB
