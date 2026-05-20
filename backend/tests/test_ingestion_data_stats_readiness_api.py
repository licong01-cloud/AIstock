import datetime as dt

from backend.routers import ingestion


def test_data_stats_overlays_audit_readiness_and_cache_state(monkeypatch):
    calls = []

    def _fake_fetchall(sql, params=()):
        calls.append(sql)
        if "FROM market.data_stats ds" in sql:
            return [
                {
                    "data_kind": "cyq_perf",
                    "table_name": "market.cyq_perf",
                    "min_date": dt.date(2026, 5, 1),
                    "max_date": dt.date(2026, 5, 17),
                    "row_count": 100,
                    "table_bytes": 1,
                    "index_bytes": 1,
                    "last_updated_at": dt.datetime(2026, 5, 18, 23, 0, tzinfo=dt.timezone.utc),
                    "stat_generated_at": dt.datetime(2026, 5, 18, 23, 0, tzinfo=dt.timezone.utc),
                    "extra_info": {"desc": "Tushare cyq_perf"},
                    "audit_ready_date": dt.date(2026, 5, 18),
                    "audit_row_count": 5000,
                    "audit_refreshed_at": dt.datetime(2026, 5, 18, 23, 20, tzinfo=dt.timezone.utc),
                    "audit_quality_status": "ok",
                }
            ]
        if "FROM market.data_sync_targets" in sql:
            return [
                {
                    "dataset": "cyq_perf",
                    "target_date": dt.date(2026, 5, 18),
                    "sync_status": "retry",
                    "failure_category": "audit_success_stats_stale",
                    "next_retry_at": dt.datetime(2026, 5, 19, 0, 0, tzinfo=dt.timezone.utc),
                    "final_deadline_at": dt.datetime(2026, 5, 19, 1, 0, tzinfo=dt.timezone.utc),
                    "target_updated_at": dt.datetime(2026, 5, 18, 23, 30, tzinfo=dt.timezone.utc),
                }
            ]
        return []

    monkeypatch.setattr(ingestion, "_fetchall", _fake_fetchall)

    response = ingestion.list_data_stats()

    item = response["items"][0]
    assert item["data_kind"] == "cyq_perf"
    assert item["ready_date"] == "2026-05-18"
    assert item["audit_ready_date"] == "2026-05-18"
    assert item["stats_max_date"] == "2026-05-17"
    assert item["cache_state"] == "stale"
    assert item["readiness_source"] == "dataset_date_refresh_audit"
    assert item["operator_action_required"] is False
    assert item["sync_status"] == "retry"
    assert item["next_retry_at"].startswith("2026-05-19T00:00:00")


def test_data_stats_marks_final_target_as_operator_action_required(monkeypatch):
    def _fake_fetchall(sql, params=()):
        if "FROM market.data_stats ds" in sql:
            return [
                {
                    "data_kind": "cyq_perf",
                    "table_name": "market.cyq_perf",
                    "min_date": dt.date(2026, 5, 1),
                    "max_date": dt.date(2026, 5, 18),
                    "row_count": 100,
                    "table_bytes": 1,
                    "index_bytes": 1,
                    "last_updated_at": None,
                    "stat_generated_at": None,
                    "extra_info": {},
                    "audit_ready_date": dt.date(2026, 5, 18),
                    "audit_row_count": 100,
                    "audit_refreshed_at": None,
                    "audit_quality_status": "ok",
                }
            ]
        if "FROM market.data_sync_targets" in sql:
            return [
                {
                    "dataset": "cyq_perf",
                    "target_date": dt.date(2026, 5, 19),
                    "sync_status": "final_blocked",
                    "failure_category": "provider_unavailable",
                    "next_retry_at": None,
                    "final_deadline_at": None,
                    "target_updated_at": None,
                }
            ]
        return []

    monkeypatch.setattr(ingestion, "_fetchall", _fake_fetchall)

    item = ingestion.list_data_stats()["items"][0]

    assert item["cache_state"] == "fresh"
    assert item["operator_action_required"] is True
    assert item["sync_target_date"] == "2026-05-19"


def test_data_stats_marks_provider_contract_failure_as_operator_action_required(monkeypatch):
    def _fake_fetchall(sql, params=()):
        if "FROM market.data_stats ds" in sql:
            return [
                {
                    "data_kind": "cyq_perf",
                    "table_name": "market.cyq_perf",
                    "min_date": dt.date(2026, 5, 1),
                    "max_date": dt.date(2026, 5, 18),
                    "row_count": 100,
                    "table_bytes": 1,
                    "index_bytes": 1,
                    "last_updated_at": None,
                    "stat_generated_at": None,
                    "extra_info": {},
                    "audit_ready_date": dt.date(2026, 5, 18),
                    "audit_row_count": 100,
                    "audit_refreshed_at": None,
                    "audit_quality_status": "ok",
                }
            ]
        if "FROM market.data_sync_targets" in sql:
            return [
                {
                    "dataset": "cyq_perf",
                    "target_date": dt.date(2026, 5, 19),
                    "sync_status": "retry",
                    "failure_category": "provider_contract_error",
                    "next_retry_at": None,
                    "final_deadline_at": None,
                    "target_updated_at": None,
                }
            ]
        return []

    monkeypatch.setattr(ingestion, "_fetchall", _fake_fetchall)

    item = ingestion.list_data_stats()["items"][0]

    assert item["sync_status"] == "retry"
    assert item["failure_category"] == "provider_contract_error"
    assert item["operator_action_required"] is True


def test_preset_stats_physical_fallback_is_display_only(monkeypatch):
    monkeypatch.setattr(ingestion, "_DAILY_PRESETS", [("cyq_perf", "incremental")])

    def _fake_fetchone(sql, params=()):
        if "FROM market.data_stats_config" in sql:
            return {"table_name": "market.cyq_perf", "date_column": "trade_date"}
        if "FROM market.dataset_date_refresh_audit" in sql:
            return None
        return None

    class _Cursor:
        description = [("max",)]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=()):
            return None

        def fetchone(self):
            return (dt.date(2026, 5, 18),)

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return _Cursor()

    monkeypatch.setattr(ingestion, "_fetchone", _fake_fetchone)
    monkeypatch.setattr(ingestion, "get_conn", lambda: _Conn())

    item = ingestion.get_preset_stats()["items"][0]

    assert item["current_max_date"] is None
    assert item["ready_date"] is None
    assert item["physical_max_date"] == "2026-05-18"
    assert item["stats_source"] == "physical_fallback_display_only"
    assert item["readiness_source"] == "dataset_date_refresh_audit"
    assert item["audit_missing"] is True


def test_auto_range_with_audit_cursor_requires_reconcile_when_audit_missing(monkeypatch):
    def _fake_fetchall(sql, params=()):
        if "FROM market.data_stats_config" in sql:
            return [
                {
                    "data_kind": "cyq_perf",
                    "table_name": "market.cyq_perf",
                    "date_column": "trade_date",
                    "extra_info": {"cursor_source": "refresh_audit"},
                }
            ]
        if "FROM market.dataset_date_refresh_audit" in sql:
            return [{"mx": None}]
        return []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=()):
            return None

        def fetchone(self):
            return (dt.date(2026, 5, 18),)

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return _Cursor()

    monkeypatch.setattr(ingestion, "_fetchall", _fake_fetchall)
    monkeypatch.setattr(ingestion, "get_conn", lambda: _Conn())

    response = ingestion.get_ingestion_auto_range("cyq_perf")

    assert response["start_date"] is None
    assert response["current_max_date"] is None
    assert response["data_max_date"] == "2026-05-18"
    assert response["cursor_source"] == "refresh_audit_missing"
    assert response["readiness_source"] == "dataset_date_refresh_audit"
    assert response["audit_missing"] is True
    assert response["needs_reconcile"] is True
    assert response["up_to_date"] is False
