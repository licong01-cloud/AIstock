import datetime as dt
from pathlib import Path

import pytest

from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.trading_core.errors import DataUnavailableError


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))

    def fetchone(self):
        return self._conn.fetchone_result


class _FakeConn:
    def __init__(self, fetchone_result=None):
        self.executed = []
        self.fetchone_result = fetchone_result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, *args, **kwargs):
        return _FakeCursor(self)


def test_record_success_writes_enhanced_audit_fields():
    conn = _FakeConn()
    repo = DataRefreshAuditRepository(conn_factory=lambda: conn)
    trade_date = dt.date(2026, 5, 5)

    repo.record_success(
        dataset="daily_basic",
        trade_date=trade_date,
        row_count=5200,
        job_id="00000000-0000-0000-0000-000000000001",
        written_rows=5200,
        expected_rows=5200,
        coverage_ratio=1.0,
        quality_status="ok",
        metadata={"mode": "incremental"},
    )

    sql, params = conn.executed[-1]
    assert "data_max_at" in sql
    assert "written_rows" in sql
    assert "expected_rows" in sql
    assert "coverage_ratio" in sql
    assert "quality_status" in sql
    assert "failure_category" in sql
    assert params[0:6] == (
        "daily_basic",
        trade_date,
        "tushare",
        "00000000-0000-0000-0000-000000000001",
        "success",
        5200,
    )
    assert params[-5:-1] == (5200, 5200, 1.0, "ok")


def test_require_success_rejects_unusable_quality_status():
    trade_date = dt.date(2026, 5, 5)
    conn = _FakeConn(
        {
            "dataset": "stk_limit",
            "trade_date": trade_date,
            "data_source": "tushare",
            "status": "success",
            "row_count": 0,
            "refreshed_at": dt.datetime(2026, 5, 5, 9, 5, tzinfo=dt.timezone.utc),
            "job_id": None,
            "error_message": None,
            "metadata": {},
            "data_max_at": None,
            "written_rows": 0,
            "expected_rows": None,
            "coverage_ratio": None,
            "quality_status": "empty_invalid",
            "failure_category": "empty_invalid",
        }
    )
    repo = DataRefreshAuditRepository(conn_factory=lambda: conn)

    with pytest.raises(DataUnavailableError):
        repo.require_success(dataset="stk_limit", trade_date=trade_date)


def test_dataset_refresh_audit_schema_comments_are_declared_in_migrations():
    root = Path(__file__).resolve().parents[2]
    migration = (root / "backend/migrations/dataset_refresh_audit_enhancement_20260505.sql").read_text(encoding="utf-8")
    init_schema = (root / "backend/db/init_trading_core_v2_schema.py").read_text(encoding="utf-8")
    required_columns = {
        "dataset",
        "trade_date",
        "data_source",
        "job_id",
        "status",
        "row_count",
        "refreshed_at",
        "error_message",
        "metadata",
        "data_max_at",
        "written_rows",
        "expected_rows",
        "coverage_ratio",
        "quality_status",
        "failure_category",
    }

    assert "COMMENT ON TABLE market.dataset_date_refresh_audit" in migration
    assert "COMMENT ON TABLE market.dataset_date_refresh_audit" in init_schema
    for column in required_columns:
        needle = f"COMMENT ON COLUMN market.dataset_date_refresh_audit.{column}"
        assert needle in migration
        assert needle in init_schema
