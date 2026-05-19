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
                    "sync_status": "retry_waiting",
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
    assert item["sync_status"] == "retry_waiting"
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
                    "sync_status": "retry_waiting",
                    "failure_category": "provider_contract_error",
                    "next_retry_at": None,
                    "final_deadline_at": None,
                    "target_updated_at": None,
                }
            ]
        return []

    monkeypatch.setattr(ingestion, "_fetchall", _fake_fetchall)

    item = ingestion.list_data_stats()["items"][0]

    assert item["sync_status"] == "retry_waiting"
    assert item["failure_category"] == "provider_contract_error"
    assert item["operator_action_required"] is True
