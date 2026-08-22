import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.trading_core.errors import DataUnavailableError
from scripts import seed_dataset_refresh_audit as audit_seed


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

    def fetchall(self):
        return self._conn.fetchall_result


class _FakeConn:
    def __init__(self, fetchone_result=None, fetchall_result=None):
        self.executed = []
        self.fetchone_result = fetchone_result
        self.fetchall_result = list(fetchall_result or [])

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


def test_refresh_failure_cannot_downgrade_usable_success_evidence():
    conn = _FakeConn()
    repo = DataRefreshAuditRepository(conn_factory=lambda: conn)

    repo.record_failure(
        dataset="suspend_d",
        trade_date=dt.date(2026, 7, 14),
        error_message="cannot execute DELETE in a read-only transaction",
        failure_category="provider_or_persistence_error",
    )

    sql, params = conn.executed[-1]
    assert "INSERT INTO market.dataset_date_refresh_audit AS existing" in sql
    assert "existing.status <> 'success'" in sql
    assert "existing.quality_status, 'unknown'" in sql
    assert "NOT IN ('ok', 'empty_valid')" in sql
    assert "EXCLUDED.status = 'success'" in sql
    assert params[0:6] == (
        "suspend_d",
        dt.date(2026, 7, 14),
        "tushare",
        None,
        "failed",
        0,
    )


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


def test_dataset_release_audit_seed_specs_match_registered_source_authority():
    dated = {
        spec.audit_dataset
        for spec in audit_seed.PRODUCTION_QUERY_SPECS.values()
        if spec.audit_dataset is not None
    }

    assert set(audit_seed.SPECS) == dated
    assert all(audit_seed.AUTHORITY in spec.eligible_sources for spec in audit_seed.SPECS.values())
    assert all(
        set(audit_seed.SPECS[source.audit_dataset].non_null_columns)
        == set(source.non_null_value_columns).intersection(source.value_columns)
        for source in audit_seed.PRODUCTION_QUERY_SPECS.values()
        if source.audit_dataset is not None
    )
    assert audit_seed.SPECS["kline_minute_raw"].start_policy == "minute"
    assert audit_seed.SPECS["suspend_d"].sparse_ok is True
    assert audit_seed.SPECS["trading_calendar"].table_identity == "market.trading_calendar"


def test_dataset_release_audit_seed_excludes_derived_values_from_physical_checks():
    day = dt.date(2026, 7, 31)
    source = audit_seed.PRODUCTION_QUERY_SPECS["sector_data"]
    conn = _FakeConn(fetchall_result=[(day, 100, None, 0)])
    profile = SimpleNamespace(indices=(), source_date_chunk_months=3)

    counts = audit_seed._physical_counts(
        conn,
        audit_seed.SPECS["sector_data"],
        day,
        day,
        profile=profile,
    )

    assert source.derived_value_columns == ("l2_code_id",)
    assert source.non_null_value_columns == ("l2_code_id",)
    assert audit_seed.SPECS["sector_data"].non_null_columns == ()
    assert "l2_code_id" not in conn.executed[0][0]
    assert counts == {day: 100}


def test_dataset_release_audit_seed_connection_disables_parallel_gather(monkeypatch):
    captured = {}
    sentinel = object()

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(audit_seed.psycopg2, "connect", fake_connect)
    config = audit_seed.DatabaseConfig(
        target="dev",
        host="127.0.0.1",
        port=5433,
        user="dev_user",
        password="",
        dbname="aistock_dev",
        credential_location="F:/Dev/AIstock/.env",
    )

    assert audit_seed._connect(config) is sentinel
    assert "-c max_parallel_workers_per_gather=0" in captured["options"]
    assert captured["application_name"] == "AIstock-dataset-release-audit-seed"


def test_dense_physical_gap_blocks_but_registered_sparse_gap_is_empty_valid():
    day1 = dt.date(2026, 7, 30)
    day2 = dt.date(2026, 7, 31)
    dense = audit_seed._build_dataset_plan(
        audit_seed.SPECS["adj_factor"],
        start=day1,
        end=day2,
        expected_dates=(day1, day2),
        physical_counts={day1: 5},
        existing_ready_dates=(),
    )
    sparse = audit_seed._build_dataset_plan(
        audit_seed.SPECS["suspend_d"],
        start=day1,
        end=day2,
        expected_dates=(day1, day2),
        physical_counts={day1: 1},
        existing_ready_dates=(),
    )

    assert dense.blocked_dates == (day2,)
    assert [row.quality_status for row in dense.planned_rows] == ["ok"]
    assert sparse.blocked_dates == ()
    assert [(row.row_count, row.quality_status) for row in sparse.planned_rows] == [
        (1, "ok"),
        (0, "empty_valid"),
    ]


def test_existing_registered_authority_is_reused_without_seed_rewrite():
    day = dt.date(2026, 7, 31)
    plan = audit_seed._build_dataset_plan(
        audit_seed.SPECS["index_daily"],
        start=day,
        end=day,
        expected_dates=(day,),
        physical_counts={day: 12},
        existing_ready_dates=(day,),
    )

    assert plan.existing_ready_dates == 1
    assert plan.planned_rows == ()
    assert plan.blocked_dates == ()


def test_index_physical_seed_requires_every_profile_index_for_the_day():
    day = dt.date(2026, 7, 31)
    indices = tuple(
        SimpleNamespace(daily_code=code, required_from=dt.date(2018, 8, 1))
        for code in ("000001.SH", "000300.SH", "399006.SZ")
    )
    profile = SimpleNamespace(indices=indices, source_date_chunk_months=3)
    incomplete = _FakeConn(fetchall_result=[(day, 20, ["000001.SH", "000300.SH"], 0)])
    complete = _FakeConn(fetchall_result=[(day, 30, [value.daily_code for value in indices], 0)])

    incomplete_counts = audit_seed._physical_counts(
        incomplete,
        audit_seed.SPECS["index_daily"],
        day,
        day,
        profile=profile,
    )
    complete_counts = audit_seed._physical_counts(
        complete,
        audit_seed.SPECS["index_daily"],
        day,
        day,
        profile=profile,
    )

    assert incomplete_counts[day] == 0
    assert complete_counts[day] == 30


def test_physical_count_chunks_never_exceed_profile_three_month_boundary():
    chunks = audit_seed._date_chunks(
        dt.date(2024, 1, 2),
        dt.date(2024, 7, 31),
        months=3,
    )

    assert chunks == (
        (dt.date(2024, 1, 2), dt.date(2024, 3, 31)),
        (dt.date(2024, 4, 1), dt.date(2024, 6, 30)),
        (dt.date(2024, 7, 1), dt.date(2024, 7, 31)),
    )


def test_required_non_null_violation_blocks_physical_seed_date():
    day = dt.date(2026, 7, 31)
    conn = _FakeConn(fetchall_result=[(day, 5000, None, 1)])
    profile = SimpleNamespace(indices=(), source_date_chunk_months=3)

    counts = audit_seed._physical_counts(
        conn,
        audit_seed.SPECS["kline_daily_raw"],
        day,
        day,
        profile=profile,
    )

    assert counts[day] == 0


def test_apply_requires_authorization_and_production_requires_matching_dev_receipt(tmp_path):
    profile = SimpleNamespace(
        profile="qe_hmm_full_v2",
        config_digest="a" * 64,
        semantic_profile_digest="b" * 64,
    )
    with pytest.raises(audit_seed.AuditSeedError, match="authorization-ref"):
        audit_seed._require_apply_authorization(
            target="dev",
            authorization_ref=None,
            dev_receipt=None,
            profile=profile,
            end_date=dt.date(2026, 7, 31),
            datasets=("adj_factor",),
        )
    with pytest.raises(audit_seed.AuditSeedError, match="dev-receipt"):
        audit_seed._require_apply_authorization(
            target="production",
            authorization_ref="ISSUE-3669",
            dev_receipt=None,
            profile=profile,
            end_date=dt.date(2026, 7, 31),
            datasets=("adj_factor",),
        )

    receipt = tmp_path / "dev-receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "schema_version": audit_seed.RECEIPT_SCHEMA_VERSION,
                "mode": "apply",
                "database_target": "dev",
                "status": "PASS",
                "profile": profile.profile,
                "profile_config_digest": profile.config_digest,
                "semantic_profile_digest": profile.semantic_profile_digest,
                "end_date": "2026-07-31",
                "dataset_names": ["adj_factor"],
                "required_failures": 0,
            }
        ),
        encoding="utf-8",
    )
    audit_seed._require_apply_authorization(
        target="production",
        authorization_ref="ISSUE-3669",
        dev_receipt=receipt,
        profile=profile,
        end_date=dt.date(2026, 7, 31),
        datasets=("adj_factor",),
    )


def test_receipt_records_credential_location_but_never_secret_value(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "TDX_DB_DEV_HOST=127.0.0.1",
                "TDX_DB_DEV_PORT=5432",
                "TDX_DB_DEV_USER=dev_user",
                "TDX_DB_DEV_PASSWORD=super-secret-value",
                "TDX_DB_DEV_NAME=aistock_dev",
            ]
        ),
        encoding="utf-8",
    )
    target = audit_seed._load_database_config("dev", env_file)
    profile = SimpleNamespace(
        profile="qe_hmm_full_v2",
        config_digest="a" * 64,
        semantic_profile_digest="b" * 64,
    )
    value = audit_seed._receipt(
        mode="plan",
        target=target,
        profile=profile,
        end_date=dt.date(2026, 7, 31),
        plans=(),
        plan_digest="c" * 64,
        authorization_ref=None,
        rows_changed=0,
        required_failures=0,
    )
    encoded = json.dumps(value)

    assert value["credential_location"] == str(env_file.resolve())
    assert value["credential_values_recorded"] is False
    assert "super-secret-value" not in encoded


def test_receipt_path_is_control_root_scoped_and_immutable(tmp_path):
    root = tmp_path / "operator_receipts"
    target = root / "receipt.json"
    audit_seed._write_receipt(target, {"status": "PASS"}, allowed_root=root)

    assert json.loads(target.read_text(encoding="utf-8")) == {"status": "PASS"}
    with pytest.raises(audit_seed.AuditSeedError, match="immutable"):
        audit_seed._write_receipt(target, {"status": "PASS"}, allowed_root=root)
    with pytest.raises(audit_seed.AuditSeedError, match="direct child"):
        audit_seed._write_receipt(
            tmp_path / "outside.json",
            {"status": "PASS"},
            allowed_root=root,
        )


def test_cli_defaults_to_read_only_plan_mode():
    args = audit_seed._parser().parse_args(["--database", "dev", "--end-date", "2026-07-31"])

    assert args.mode == "plan"
    assert args.authorization_ref is None
    assert args.dev_receipt is None
