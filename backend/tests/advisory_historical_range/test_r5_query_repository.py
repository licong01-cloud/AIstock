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
    schema = ((int, False), (str, False))
    assert HistoricalRangeCursorCodec.decode(cursor, filter_payload=filters, key_schema=schema) == (3, "day_3")
    with pytest.raises(HistoricalRangeQueryError, match="invalid") as mismatch:
        HistoricalRangeCursorCodec.decode(cursor, filter_payload={"batch_id": "ahrb_2"}, key_schema=schema)
    assert mismatch.value.reason_code == "ADVISORY_HR_CURSOR_INVALID"


@pytest.mark.parametrize("order_key", [("3", "day_3"), (True, "day_3"), (3, None)])
def test_cursor_rejects_wrong_types_and_nullability(order_key) -> None:
    filters = {"batch_id": "ahrb_1"}
    cursor = HistoricalRangeCursorCodec.encode(order_key=order_key, filter_payload=filters)
    with pytest.raises(HistoricalRangeQueryError) as raised:
        HistoricalRangeCursorCodec.decode(
            cursor,
            filter_payload=filters,
            key_schema=((int, False), (str, False)),
        )
    assert raised.value.reason_code == "ADVISORY_HR_CURSOR_INVALID"


def test_invalid_domain_enum_fails_before_opening_connection() -> None:
    repository = PostgresHistoricalRangeQueryRepository(
        conn_factory=lambda: (_ for _ in ()).throw(AssertionError("connection must not open"))
    )
    with pytest.raises(HistoricalRangeQueryError) as error:
        repository.list_batches(statuses=("NOT_A_BATCH_STATUS",))
    assert error.value.reason_code == "ADVISORY_HR_FILTER_INVALID"


def test_invalid_limit_fails_before_opening_connection() -> None:
    repository = PostgresHistoricalRangeQueryRepository(
        conn_factory=lambda: (_ for _ in ()).throw(AssertionError("connection must not open"))
    )
    with pytest.raises(HistoricalRangeQueryError) as error:
        repository.list_batches(limit=501)
    assert error.value.reason_code == "ADVISORY_HR_LIMIT_INVALID"
