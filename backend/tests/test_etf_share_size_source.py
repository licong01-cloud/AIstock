import datetime as dt
import uuid
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import backend.services.tushare_sync_engine as sync_engine
from backend.routers import ingestion
from backend.db.init_tushare_schedules import get_default_schedule_catalog
from backend.ingestion.tdx_scheduler import TDXScheduler
from backend.services.data_completeness import (
    DATASET_TABLE_MAP,
    LIGHT_TIER,
    DataCompletenessChecker,
)
from backend.services.tushare_dataset_specs import (
    DATASET_REGISTRY,
    ETF_BASIC_SNAPSHOTS,
    ETF_SHARE_SIZE,
    QueryMode,
)
from backend.services.tushare_sync_engine import TushareSyncEngine
from backend.services.tushare_sync_engine import _etf_share_size_publication_quality


class _Cursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))


class _Connection:
    def __init__(self) -> None:
        self.cursor_instance = _Cursor()

    def cursor(self):
        return self.cursor_instance


def test_etf_dataset_specs_keep_fact_and_observation_time_separate() -> None:
    assert DATASET_REGISTRY["etf_share_size"] is ETF_SHARE_SIZE
    assert ETF_SHARE_SIZE.query_mode == QueryMode.BY_DATE
    assert ETF_SHARE_SIZE.primary_keys == ["trade_date", "ts_code"]
    assert list(ETF_SHARE_SIZE.columns) == [
        "trade_date",
        "ts_code",
        "total_share",
        "total_size",
        "nav",
        "close",
    ]
    assert ETF_SHARE_SIZE.trading_day_only is True
    assert ETF_SHARE_SIZE.min_expected_rows is None
    assert ETF_SHARE_SIZE.row_limit == 5000

    assert DATASET_REGISTRY["etf_basic_snapshots"] is ETF_BASIC_SNAPSHOTS
    assert ETF_BASIC_SNAPSHOTS.query_mode == QueryMode.SINGLE_CALL
    assert ETF_BASIC_SNAPSHOTS.supports_incremental is False
    assert ETF_BASIC_SNAPSHOTS.snapshot_date_column == "snapshot_date"
    assert ETF_BASIC_SNAPSHOTS.primary_keys == ["snapshot_date", "ts_code"]
    assert ETF_BASIC_SNAPSHOTS.single_call_param_sets == [
        {"list_status": "L"},
        {"list_status": "D"},
        {"list_status": "P"},
    ]


def test_snapshot_date_is_local_only_and_not_requested_from_tushare(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def query(api_name, params, fields, *, token):
        captured.update(api_name=api_name, params=params, fields=fields, token=token)
        return [{"ts_code": "510300.SH", "list_status": "L"}]

    monkeypatch.setattr(sync_engine, "_query_tushare_dataapi", query)
    monkeypatch.setattr(sync_engine, "get_limiter", lambda *_args: SimpleNamespace(acquire=lambda: None))
    monkeypatch.setattr(TushareSyncEngine, "_tushare_token", lambda _self: "test-token")

    rows = TushareSyncEngine(target_repository=None)._fetch_from_tushare(
        ETF_BASIC_SNAPSHOTS,
        {"list_status": "L"},
    )

    assert captured["api_name"] == "etf_basic"
    assert "snapshot_date" not in str(captured["fields"]).split(",")
    assert rows == [{
        column: ("510300.SH" if column == "ts_code" else "L" if column == "list_status" else None)
        for column in ETF_BASIC_SNAPSHOTS.columns
        if column != "snapshot_date"
    }]


def test_single_call_injects_same_observation_date_before_upsert(monkeypatch) -> None:
    engine = TushareSyncEngine(target_repository=None)
    conn = _Connection()
    written: list[list[dict[str, object]]] = []

    monkeypatch.setattr(
        engine,
        "_fetch_from_tushare",
        lambda _spec, params: [{"ts_code": f"{params['list_status']}00000.SH"}],
    )
    monkeypatch.setattr(engine, "_upsert_batch", lambda _conn, _spec, rows: written.append(rows) or len(rows))
    monkeypatch.setattr(engine, "_update_progress", lambda *_args: None)

    result = engine._sync_single_call(conn, ETF_BASIC_SNAPSHOTS, uuid.uuid4())

    assert result.failed_batches == 0
    assert result.inserted_rows == 3
    shanghai_today = dt.datetime.now(ZoneInfo("Asia/Shanghai")).date()
    assert [row[0]["snapshot_date"] for row in written] == [shanghai_today] * 3


def test_etf_share_size_reuses_existing_schedule_and_health_paths() -> None:
    catalog = get_default_schedule_catalog()
    assert catalog["complete"] is True
    templates = {(row["dataset"], row["mode"]): row for row in catalog["templates"]}
    assert templates[("etf_share_size", "incremental")]["options"]["at"] == "20:00"
    assert templates[("etf_basic_snapshots", "init")]["options"]["at"] == "20:05"
    assert templates[("_data_freshness_check", "incremental")]["options"]["at"] == "22:00"
    assert templates[("_auto_retry_stale", "incremental")]["options"]["at"] == "23:00"
    assert "etf_share_size" in LIGHT_TIER.tables
    assert DATASET_TABLE_MAP["etf_share_size"] == ("market.etf_share_size", "trade_date")
    checker = DataCompletenessChecker.__new__(DataCompletenessChecker)
    assert checker._expected_rows("etf_share_size") is None


def test_etf_snapshot_schedule_rejects_incremental_mode() -> None:
    try:
        ingestion._validate_dataset_schedule_mode("etf_basic_snapshots", "incremental")
    except ingestion.HTTPException as exc:
        assert exc.status_code == 400
        assert exc.detail == "etf_basic_snapshots only supports init mode"
    else:
        raise AssertionError("incremental ETF snapshot schedule must be rejected")

    ingestion._validate_dataset_schedule_mode("etf_basic_snapshots", "init")


def test_ingestion_run_rejects_invalid_etf_modes_and_missing_init_range() -> None:
    with pytest.raises(ingestion.HTTPException, match="only supports init mode"):
        ingestion.trigger_ingestion_run(
            ingestion.IngestionRunRequest(
                dataset="etf_basic_snapshots",
                mode="incremental",
            )
        )

    with pytest.raises(ingestion.HTTPException, match="requires start_date"):
        ingestion.trigger_ingestion_run(
            ingestion.IngestionRunRequest(dataset="etf_share_size", mode="init")
        )

    with pytest.raises(ingestion.HTTPException, match="requires end_date"):
        ingestion.trigger_ingestion_run(
            ingestion.IngestionRunRequest(
                dataset="etf_share_size",
                mode="init",
                options={"start_date": "2026-09-01"},
            )
        )


def test_etf_share_size_only_retries_when_core_source_field_is_wholly_unpublished() -> None:
    pending_receipt, pending_quality = _etf_share_size_publication_quality(
        [{"total_share": 100.0, "total_size": None}]
    )
    assert pending_receipt["total_share_finite_count"] == 1
    assert pending_receipt["total_size_finite_count"] == 0
    assert pending_quality == {
        "quality_status": "low_coverage",
        "failure_category": "required_source_field_unpublished",
    }

    ready_receipt, ready_quality = _etf_share_size_publication_quality(
        [
            {"total_share": 100.0, "total_size": 250.0},
            {"total_share": 50.0, "total_size": None},
        ]
    )
    assert ready_receipt["total_size_finite_count"] == 1
    assert ready_quality == {"quality_status": "ok"}


def test_etf_unpublished_source_field_retry_is_anchored_to_exact_date() -> None:
    scheduler = TDXScheduler.__new__(TDXScheduler)
    target_date = dt.date(2026, 9, 22)
    scheduler._compute_auto_range = lambda _dataset: (_ for _ in ()).throw(  # type: ignore[method-assign]
        AssertionError("generic auto-range must not be used for an exact quality retry")
    )

    assert scheduler._retry_range_for_health_failure(
        "etf_share_size",
        target_date=target_date,
        failure_category="required_source_field_unpublished",
    ) == (target_date, target_date)


def test_etf_migration_is_idempotent_and_registers_both_stats_rows() -> None:
    sql = Path("backend/db/migrations/add_etf_share_sources_20260923.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS market.etf_share_size" in sql
    assert "CREATE TABLE IF NOT EXISTS market.etf_basic_snapshots" in sql
    assert "create_hypertable" in sql
    assert "'etf_share_size'" in sql
    assert "'etf_basic_snapshots'" in sql
    assert "ON CONFLICT (data_kind) DO UPDATE" in sql
    assert "availability', 'next_trading_day_for_research'" in sql
