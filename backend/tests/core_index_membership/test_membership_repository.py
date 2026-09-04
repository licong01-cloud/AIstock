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
