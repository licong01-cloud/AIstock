import datetime as dt
from pathlib import Path

from backend.services.data_sync_autonomy import (
    DataSyncAlertGate,
    DataSyncPolicyEngine,
    DataSyncTargetRepository,
    SyncTargetInput,
)


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))
        if "RETURNING target_id" in sql:
            self._row = (params[0],)
        elif "RETURNING attempt_id" in sql:
            self._row = (params[0],)
        elif "FROM market.data_sync_targets" in sql:
            self._row = None
            self._rows = [
                {
                    "target_id": "target-1",
                    "dataset": "cyq_perf",
                    "target_date": dt.date(2026, 5, 18),
                    "status": "retry_waiting",
                }
            ]

    def fetchone(self):
        return self._row

    def fetchall(self):
        return getattr(self, "_rows", [])


class _FakeConn:
    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, *args, **kwargs):
        return _FakeCursor(self)


def test_policy_waits_before_release_without_alerting():
    now = dt.datetime(2026, 5, 18, 17, 0, tzinfo=dt.timezone.utc)
    release_at = dt.datetime(2026, 5, 18, 18, 10, tzinfo=dt.timezone.utc)

    decision = DataSyncPolicyEngine().decide(
        now=now,
        release_at=release_at,
        final_deadline_at=dt.datetime(2026, 5, 18, 23, 30, tzinfo=dt.timezone.utc),
        current_status="planned",
    )

    assert decision.state == "waiting_release"
    assert decision.should_retry is False
    assert decision.should_alert is False


def test_policy_retries_zero_rows_before_final_deadline_without_alerting():
    now = dt.datetime(2026, 5, 18, 22, 0, tzinfo=dt.timezone.utc)

    decision = DataSyncPolicyEngine().decide(
        now=now,
        release_at=None,
        final_deadline_at=dt.datetime(2026, 5, 18, 23, 30, tzinfo=dt.timezone.utc),
        current_status="pending_publish",
        zero_rows=True,
        zero_rows_allowed=False,
        failure_category="empty_invalid",
    )

    assert decision.state == "retry_waiting"
    assert decision.should_retry is True
    assert decision.should_alert is False
    assert decision.next_retry_at is not None


def test_policy_final_blocked_alerts_only_after_deadline():
    now = dt.datetime(2026, 5, 18, 23, 31, tzinfo=dt.timezone.utc)

    decision = DataSyncPolicyEngine().decide(
        now=now,
        release_at=None,
        final_deadline_at=dt.datetime(2026, 5, 18, 23, 30, tzinfo=dt.timezone.utc),
        current_status="retry_waiting",
        zero_rows=True,
        zero_rows_allowed=False,
    )

    assert decision.state == "final_blocked"
    assert decision.should_retry is False
    assert decision.should_alert is True
    assert decision.operator_action_required is True


def test_target_repository_merges_duplicate_dataset_date_across_sources_and_reasons():
    conn = _FakeConn()
    repo = DataSyncTargetRepository(conn_factory=lambda: conn)
    target = SyncTargetInput(
        dataset="cyq_perf",
        target_date=dt.date(2026, 5, 18),
        source="freshness_check",
        reason="audit_missing",
        metadata={"job_status": "success"},
    )

    first = repo.upsert_target(target)
    second = repo.upsert_target(
        SyncTargetInput(
            dataset="cyq_perf",
            target_date=dt.date(2026, 5, 18),
            source="auto_retry",
            reason="retry_exhausted",
            metadata={"retry": 1},
        )
    )

    assert first
    assert second
    sql, params = conn.executed[0]
    assert "ON CONFLICT (fingerprint)" in sql
    assert params[1:6] == (
        "cyq_perf",
        dt.date(2026, 5, 18),
        "queued",
        "freshness_check",
        "audit_missing",
    )
    assert conn.executed[0][1][-1] == conn.executed[1][1][-1]


def test_target_repository_lists_due_retry_targets():
    conn = _FakeConn()
    repo = DataSyncTargetRepository(conn_factory=lambda: conn)

    rows = repo.list_due_targets(now=dt.datetime(2026, 5, 18, 23, tzinfo=dt.timezone.utc))

    assert rows[0]["target_id"] == "target-1"
    sql, params = conn.executed[0]
    assert "next_retry_at" in sql
    assert "status = 'running'" in sql
    assert params[2] == 100


def test_alert_gate_ignores_recoverable_states_and_flushes_final_only():
    flushed = []

    class _Alerter:
        def flush(self, alerts):
            flushed.extend(alerts)
            return {"error": len(alerts)}

    gate = DataSyncAlertGate(alerter=_Alerter())
    counts = gate.flush_final_alerts(
        [
            {"dataset": "cyq_perf", "target_date": dt.date(2026, 5, 18), "status": "retry_waiting"},
            {
                "target_id": "t1",
                "dataset": "cyq_perf",
                "target_date": dt.date(2026, 5, 18),
                "status": "final_blocked",
                "failure_category": "retry_exhausted",
                "source": "auto_retry",
                "reason": "audit_missing",
            },
        ]
    )

    assert counts == {"error": 1}
    assert len(flushed) == 1
    assert flushed[0].dataset == "cyq_perf"
    assert flushed[0].alert_type == "final_blocked"


def test_target_repository_can_close_recovered_target():
    conn = _FakeConn()
    repo = DataSyncTargetRepository(conn_factory=lambda: conn)

    repo.update_target_status(
        target_id="target-1",
        status="success",
        metadata={"source": "unit"},
        clear_failure=True,
        clear_retry=True,
    )

    sql, params = conn.executed[0]
    assert "UPDATE market.data_sync_targets" in sql
    assert "failure_category = CASE" in sql
    assert params[0] == "success"
    assert params[1] is True
    assert params[3] is True
    assert params[-1] == "target-1"


def test_data_sync_migration_extends_final_blocked_alert_type():
    migration = Path("backend/migrations/data_sync_autonomy_20260519.sql").read_text(encoding="utf-8")

    assert "DROP CONSTRAINT IF EXISTS data_alerts_alert_type_check" in migration
    assert "'final_blocked'" in migration
