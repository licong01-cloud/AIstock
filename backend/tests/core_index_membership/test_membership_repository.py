from __future__ import annotations

from datetime import date, datetime

import pytest

from backend.services import core_index_membership as subject


def test_membership_row_requires_catalog_identity_and_official_provider() -> None:
    row = {
        "pool_id": "csi300",
        "index_code": "000300.SH",
        "ts_code": "000001.SZ",
        "effective_from": date(2024, 1, 2),
        "effective_to_exclusive": None,
        "source_provider": "CSI",
        "source_reference": "official:test",
        "updated_at": datetime(2026, 9, 4, 8, 0),
    }

    parsed = subject._membership_from_row(row)

    assert parsed.pool_id == "csi300"
    assert parsed.ts_code == "000001.SZ"

    with pytest.raises(subject.CoreIndexMembershipUnavailable):
        subject._membership_from_row({**row, "source_provider": "TUSHARE_CROSSCHECK"})


def test_membership_row_rejects_index_identity_drift() -> None:
    with pytest.raises(subject.CoreIndexMembershipUnavailable) as captured:
        subject._membership_from_row(
            {
                "pool_id": "csi300",
                "index_code": "000905.SH",
                "ts_code": "000001.SZ",
                "effective_from": date(2024, 1, 2),
                "effective_to_exclusive": None,
                "source_provider": "CSI",
                "source_reference": "official:test",
                "updated_at": datetime(2026, 9, 4, 8, 0),
            }
        )

    assert captured.value.reason is subject.UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID


def test_repository_uses_canonical_trading_calendar_not_sparse_price_rows() -> None:
    observed: dict[str, object] = {}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql, params):
            observed["sql"] = " ".join(sql.split())
            observed["params"] = params

        def fetchall(self):
            return [(date(2018, 8, 1),), (date(2018, 8, 2),)]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self, **_kwargs):
            return Cursor()

    repository = subject.CoreIndexMembershipRepository(lambda: Connection())

    result = repository.fetch_trading_dates(date(2018, 8, 1), date(2018, 8, 2))

    assert result == (date(2018, 8, 1), date(2018, 8, 2))
    assert "FROM market.trading_calendar" in str(observed["sql"])
    assert "is_trading IS TRUE" in str(observed["sql"])
    assert "kline_daily_raw" not in str(observed["sql"])
    assert observed["params"] == (date(2018, 8, 1), date(2018, 8, 2))
