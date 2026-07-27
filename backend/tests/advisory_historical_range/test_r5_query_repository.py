from __future__ import annotations

import pytest

from backend.services.advisory_historical_range.query_repository import (
    HistoricalRangeCursorCodec,
    HistoricalRangeQueryError,
    PostgresHistoricalRangeQueryRepository,
)


def test_cursor_round_trip_binds_order_and_filters() -> None:
    filters = {"batch_id": "ahrb_1", "status": ["RUNNING"]}
    cursor = HistoricalRangeCursorCodec.encode(order_key=(3, "day_3"), filter_payload=filters)
    assert HistoricalRangeCursorCodec.decode(cursor, filter_payload=filters, key_size=2) == (3, "day_3")
    with pytest.raises(HistoricalRangeQueryError, match="invalid") as mismatch:
        HistoricalRangeCursorCodec.decode(cursor, filter_payload={"batch_id": "ahrb_2"}, key_size=2)
    assert mismatch.value.reason_code == "ADVISORY_HR_CURSOR_INVALID"


def test_invalid_limit_fails_before_opening_connection() -> None:
    repository = PostgresHistoricalRangeQueryRepository(
        conn_factory=lambda: (_ for _ in ()).throw(AssertionError("connection must not open"))
    )
    with pytest.raises(HistoricalRangeQueryError) as error:
        repository.list_batches(limit=501)
    assert error.value.reason_code == "ADVISORY_HR_LIMIT_INVALID"
