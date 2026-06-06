from __future__ import annotations

import pytest

from backend.data_service import qe_data_service


def test_normalize_and_validate_accepts_canonical_and_prefixed_codes() -> None:
    result = qe_data_service._normalize_and_validate_instruments(
        ["000001.SZ", "sh600000", "BJ430047"],
        source="test",
        start_date="2026-06-01",
        end_date="2026-06-01",
    )

    assert result == ["000001.SZ", "600000.SH", "430047.BJ"]


def test_build_static_factors_rejects_malformed_ts_code_before_sql() -> None:
    with pytest.raises(ValueError) as exc_info:
        qe_data_service.build_static_factors(
            ["000001.SZ", "603819.S2026-06-01T01:59:30.734977444Z"],
            "2026-06-01",
            "2026-06-01",
        )

    message = str(exc_info.value)
    assert "invalid ts_code values before SQL execution" in message
    assert "source=build_static_factors" in message
    assert "invalid_count=1" in message
    assert "603819.S2026-06-01T01:59:30.734977444Z" in message
