from pathlib import Path

from backend.ingestion.tdx_scheduler import TDXScheduler
from backend.services.tushare_dataset_specs import DATASET_REGISTRY


def test_cyq_perf_routes_through_engine_registry_and_chips_stays_legacy():
    assert "cyq_perf" in DATASET_REGISTRY
    assert "cyq_chips" not in DATASET_REGISTRY
    assert TDXScheduler._default_ingestion_script("cyq_perf", "incremental") is None
    chips_script = TDXScheduler._default_ingestion_script("cyq_chips", "incremental")
    assert chips_script is not None
    assert chips_script.name == "ingest_tushare_cyq.py"


def test_legacy_cyq_script_is_only_kept_for_cyq_chips():
    scheduler_source = Path("backend/ingestion/tdx_scheduler.py").read_text(encoding="utf-8")
    assert "DEFAULT_INGEST_TUSHARE_CYQ" in scheduler_source
    assert "ingest_tushare_cyq.py" in scheduler_source
    assert 'if dataset == "cyq_perf"' in scheduler_source
    assert 'if dataset == "cyq_chips"' in scheduler_source


def test_data_freshness_check_defers_alerts_and_writes_retry_target(monkeypatch):
    import datetime as dt
    from types import SimpleNamespace

    import backend.ingestion.tdx_scheduler as scheduler_module

    scheduler = TDXScheduler.__new__(TDXScheduler)
    scheduler._db_cfg = {}
    executed = []
    scheduler._execute = lambda sql, params=(): executed.append((sql, params))
    scheduler._update_ingestion_schedule = lambda *args, **kwargs: None

    class FakeChecker:
        def __init__(self, _db_cfg):
            pass

        def check_all(self):
            return [
                SimpleNamespace(
                    dataset="cyq_perf",
                    status="stale",
                    expected_date=dt.date(2026, 5, 18),
                    failure_category="audit_missing",
                    summary=lambda: {
                        "dataset": "cyq_perf",
                        "status": "stale",
                        "expected_date": "2026-05-18",
                    },
                )
            ]

    class FakeRepo:
        def __init__(self):
            self.records = []

        def upsert_target(self, record):
            self.records.append(record)
            return {"target_id": "dst_test"}

    repo = FakeRepo()

    class ForbiddenAlerter:
        def __init__(self, _db_cfg):
            raise AssertionError("freshness check must not instantiate DataHealthAlerter")

    monkeypatch.setattr(scheduler_module, "AuditBackedDataHealthChecker", FakeChecker)
    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: repo)
    monkeypatch.setattr(scheduler_module, "DataHealthAlerter", ForbiddenAlerter)

    scheduler._run_data_freshness_check(
        run_id=__import__("uuid").uuid4(),
        schedule_id=None,
        triggered_by="pytest",
        options={},
    )

    assert len(repo.records) == 1
    record = repo.records[0]
    assert record.dataset == "cyq_perf"
    assert record.data_source == "readiness_gate"
    assert record.target_status == "retry"
    assert record.target_date == dt.date(2026, 5, 18)
    assert record.metadata["alert_gate"] == "deferred_until_retry_final_state"
    assert not any("data_alerts" in sql for sql, _params in executed)
