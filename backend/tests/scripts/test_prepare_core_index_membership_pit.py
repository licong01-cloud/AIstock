from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

from backend.services.core_index_membership import MembershipInterval
from scripts import prepare_core_index_membership_pit as subject


def authority_row(**overrides):
    row = {
        "pool_id": "csi300",
        "index_code": "000300.SH",
        "ts_code": "000001.SZ",
        "effective_from": "2018-08-01",
        "effective_to_exclusive": None,
        "source_provider": "CSI",
        "source_reference": "official:test",
    }
    row.update(overrides)
    return row


def test_authority_file_accepts_official_dates_and_rejects_tushare_as_authority(tmp_path: Path) -> None:
    path = tmp_path / "authority.json"
    path.write_text(json.dumps([authority_row()]), encoding="utf-8")

    rows = subject.load_authority_rows(path)

    assert rows[0].effective_from == date(2018, 8, 1)
    assert rows[0].source_provider == "CSI"

    path.write_text(
        json.dumps([authority_row(source_provider="TUSHARE_CROSSCHECK")]),
        encoding="utf-8",
    )
    with pytest.raises(subject.CoreIndexMembershipOperatorError, match="catalog mismatch"):
        subject.load_authority_rows(path)


def test_authority_file_rejects_duplicate_and_overlapping_intervals(tmp_path: Path) -> None:
    path = tmp_path / "authority.json"
    path.write_text(json.dumps([authority_row(), authority_row()]), encoding="utf-8")
    with pytest.raises(subject.CoreIndexMembershipOperatorError, match="duplicate"):
        subject.load_authority_rows(path)

    path.write_text(
        json.dumps(
            [
                authority_row(effective_to_exclusive="2024-06-17"),
                authority_row(effective_from="2024-06-14", effective_to_exclusive=None),
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(subject.CoreIndexMembershipOperatorError, match="overlapping"):
        subject.load_authority_rows(path)


def test_production_result_requires_matching_dev_apply(tmp_path: Path) -> None:
    path = tmp_path / "dev-result.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": subject.SCHEMA_VERSION,
                "database_target": "dev",
                "mode": "apply",
                "status": "PASS",
                "migration": subject.MIGRATION_NAME,
                "pool_ids": ["csi300"],
                "readback": {"pool_count": 1},
            }
        ),
        encoding="utf-8",
    )

    value = subject._validate_dev_result(path, pool_ids=("csi300",), expected_mode="apply")
    assert value["database_target"] == "dev"

    with pytest.raises(subject.CoreIndexMembershipOperatorError, match="does not match"):
        subject._validate_dev_result(
            path,
            pool_ids=("csi300", "csi500"),
            expected_mode="apply",
        )


def test_physical_coverage_reports_exact_missing_symbols_without_writing(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    daily = candidate / "components" / "daily_bin_candidate" / "features"
    minute = candidate / "components" / "minute_bin_candidate" / "features"
    (daily / "000001.sz").mkdir(parents=True)
    (minute / "600000.sh").mkdir(parents=True)

    result = subject._physical_coverage(candidate, {"000001.SZ", "600000.SH"})

    assert result["status"] == "DATA_GAPS"
    assert result["missing_count"] == 2
    assert result["daily_missing"] == ["600000.SH"]
    assert result["minute_missing"] == ["000001.SZ"]


def test_tushare_monthly_snapshot_is_crosscheck_only(monkeypatch) -> None:
    membership = (
        MembershipInterval(
            pool_id="csi300",
            index_code="000300.SH",
            ts_code="000001.SZ",
            effective_from=date(2018, 8, 1),
            effective_to_exclusive=None,
            source_provider="CSI",
            source_reference="official:test",
            updated_at=datetime(2026, 9, 4, 8, 0),
        ),
    )

    class Repository:
        def __init__(self, _factory):
            pass

        def fetch_membership_intervals(self, pool_ids, start_date, end_date):
            del pool_ids, start_date, end_date
            return membership

    monkeypatch.setattr(subject, "CoreIndexMembershipRepository", Repository)

    result = subject._crosscheck_tushare(
        config=subject.DatabaseConfig("dev", "127.0.0.1", 5433, "u", "p", "aistock_dev", ".env"),
        pool_ids=("csi300",),
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        fetcher=lambda _code, _start, _end: pd.DataFrame([{"con_code": "000001.SZ", "trade_date": "20240131"}]),
    )

    assert result["checked_month_count"] == 1
    assert result["mismatch_month_count"] == 0
    assert result["blocking_error_count"] == 0
    assert result["authority_effect"] == "advisory_only_l1_official_wins"

    backcast = subject._crosscheck_tushare(
        config=subject.DatabaseConfig("dev", "127.0.0.1", 5433, "u", "p", "aistock_dev", ".env"),
        pool_ids=("csi300",),
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        fetcher=lambda _code, _start, _end: pd.DataFrame(
            [{"con_code": "001872.SZ", "trade_date": "20240131"}]
        ),
    )

    assert backcast["mismatch_month_count"] == 1
    assert backcast["blocking_error_count"] == 0
    assert backcast["mismatch_examples"][0]["extra_in_database"] == ["000001.SZ"]


def test_tushare_successor_code_backcast_does_not_override_official_pit(monkeypatch) -> None:
    class Repository:
        def __init__(self, _factory):
            pass

        def fetch_pool_coverage(self, pool_ids):
            return {
                pool_id: type(
                    "Coverage",
                    (),
                    {"first_effective_from": subject.POOL_DEFINITIONS[pool_id].history_start},
                )()
                for pool_id in pool_ids
            }

    interval = type("Interval", (), {"ts_code": "000022.SZ"})()
    resolved = type(
        "Resolved",
        (),
        {"intervals": (interval,), "membership_revision": "official:test"},
    )()
    monkeypatch.setattr(subject, "CoreIndexMembershipRepository", Repository)
    monkeypatch.setattr(subject, "resolve_universe", lambda *_args, **_kwargs: resolved)
    monkeypatch.setattr(
        subject,
        "_crosscheck_tushare",
        lambda **_kwargs: {
            "checked_month_count": 1,
            "upstream_unavailable_month_count": 0,
            "mismatch_month_count": 1,
            "blocking_error_count": 0,
            "authority_effect": "advisory_only_l1_official_wins",
            "mismatch_examples": [
                {
                    "pool_id": "csi1000",
                    "snapshot_date": "2018-08-31",
                    "missing_in_database": ["001872.SZ"],
                    "extra_in_database": ["000022.SZ"],
                }
            ],
        },
    )

    result = subject.validate_full_database(
        subject.DatabaseConfig("dev", "127.0.0.1", 5433, "u", "p", "aistock_dev", ".env"),
        pool_ids=("csi1000",),
        start_date=date(2018, 8, 1),
        end_date=date(2018, 8, 31),
        candidate_root=None,
        tushare_fetcher=lambda *_args: None,
    )

    assert result["status"] == "PASS"
    assert result["error_count"] == 0
    assert result["tushare_crosscheck"]["mismatch_month_count"] == 1


def test_physical_price_gaps_are_reported_without_overriding_authority_status(
    monkeypatch, tmp_path: Path
) -> None:
    class Repository:
        def __init__(self, _factory):
            pass

        def fetch_pool_coverage(self, pool_ids):
            return {
                pool_id: type(
                    "Coverage",
                    (),
                    {"first_effective_from": subject.POOL_DEFINITIONS[pool_id].history_start},
                )()
                for pool_id in pool_ids
            }

    interval = type("Interval", (), {"ts_code": "000001.SZ"})()
    resolved = type(
        "Resolved",
        (),
        {"intervals": (interval,), "membership_revision": "official:test"},
    )()
    candidate = tmp_path / "candidate"
    (candidate / "components" / "daily_bin_candidate" / "features").mkdir(parents=True)
    (candidate / "components" / "minute_bin_candidate" / "features").mkdir(parents=True)
    monkeypatch.setattr(subject, "CoreIndexMembershipRepository", Repository)
    monkeypatch.setattr(subject, "resolve_universe", lambda *_args, **_kwargs: resolved)

    result = subject.validate_full_database(
        subject.DatabaseConfig("dev", "127.0.0.1", 5433, "u", "p", "aistock_dev", ".env"),
        pool_ids=("csi300",),
        start_date=date(2018, 8, 1),
        end_date=date(2018, 8, 31),
        candidate_root=candidate,
        tushare_fetcher=None,
    )

    assert result["status"] == "PASS"
    assert result["error_count"] == 0
    assert result["reported_data_gap_count"] == 2
    assert result["physical_coverage"]["status"] == "DATA_GAPS"


def test_migration_is_one_table_without_freeze_hash_or_extension() -> None:
    sql = (subject.REPO_ROOT / "backend" / "migrations" / subject.MIGRATION_NAME).read_text(encoding="utf-8")
    lowered = sql.lower()

    assert "create table if not exists market.core_index_membership_pit" in lowered
    assert lowered.count("create table") == 1
    assert "create extension" not in lowered
    assert "sha256" not in lowered
    assert "freeze" not in lowered


def test_raw_price_source_migration_allows_truthful_tushare_fallback() -> None:
    root = subject.REPO_ROOT / "backend" / "migrations"
    preflight = (root / "kline_raw_tushare_source_20260905.preflight.sql").read_text(encoding="utf-8")
    migration = (root / "kline_raw_tushare_source_20260905.sql").read_text(encoding="utf-8")
    rollback = (root / "kline_raw_tushare_source_20260905.rollback.sql").read_text(encoding="utf-8")

    assert "ALTER TABLE" not in preflight
    assert "market.kline_daily_raw" in migration
    assert "market.kline_minute_raw" in migration
    assert migration.count("'tushare_api'") >= 2
    assert migration.count("NOT VALID") == 2
    assert "VALIDATE CONSTRAINT" not in migration
    assert "rollback refused while tushare_api rows exist" in rollback


def test_upsert_orders_intervals_before_batch_write(monkeypatch) -> None:
    observed = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, *_args):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

    monkeypatch.setattr(
        subject.pgx,
        "execute_values",
        lambda _cur, _sql, values, **_kwargs: observed.extend(values) or [],
    )
    rows = tuple(
        subject.AuthorityRow.from_mapping(item)
        for item in (
            authority_row(effective_from="2024-06-17"),
            authority_row(effective_from="2018-08-01", effective_to_exclusive="2024-06-17"),
        )
    )

    assert subject.upsert_authority_rows(Connection(), rows) == 0
    assert [row[3] for row in observed] == [date(2018, 8, 1), date(2024, 6, 17)]
