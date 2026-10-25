from __future__ import annotations

import pytest

from backend.services.advisory_historical_range.query_repository import (
    HistoricalRangeCursorCodec,
    HistoricalRangeQueryError,
    PostgresHistoricalRangeQueryRepository,
    _public_row,
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


def _legacy_failed_row():
    return {
        "operation_id": "parent", "operation_type": "REFRESH_OUTCOMES",
        "status": "RETRYABLE_FAILED", "attempt_no": 3, "fencing_token": 3,
        "request_payload_sha256": "a" * 64, "error_json": None,
        "result_status": "RETRYABLE_FAILED", "result_ref": None,
        "result_hash": None, "result_row_version": None,
        "_failure_attempt": {
            "operation_id": "parent", "attempt_id": "attempt-3", "attempt_no": 3,
            "fencing_token": 3, "status": "RETRYABLE_FAILED", "input_hash": "a" * 64,
            "attempt_receipt_hash": "b" * 64,
            "reason_codes_json": ["ADVISORY_HR_DATABASE_UNAVAILABLE"],
        },
    }


def test_legacy_failure_readback_uses_explicit_persisted_attempt_origin():
    row = _legacy_failed_row()
    public = _public_row(row)
    error = public["error_json"]
    assert error is not None
    assert error["reason_codes"] == ["ADVISORY_HR_DATABASE_UNAVAILABLE"]
    assert error["evidence_origin"] == "PERSISTED_OPERATION_ATTEMPT_REASON_CODES"
    assert error["attempt_id"] == "attempt-3"
    assert error["stored_error_json_missing"] is True
    assert error["stored_result_status"] == "RETRYABLE_FAILED"
    assert public["result_status"] is None
    assert row["result_status"] == "RETRYABLE_FAILED"
    assert public["status"] == "RETRYABLE_FAILED"
    assert row["error_json"] is None
    assert "_failure_attempt" not in public


@pytest.mark.parametrize("field,value", [
    ("operation_id", "another"), ("attempt_no", 2), ("fencing_token", 2),
    ("status", "COMPLETED"), ("input_hash", "c" * 64),
    ("reason_codes_json", []), ("reason_codes_json", [""]),
    ("attempt_receipt_hash", "not-a-hash"),
])
def test_unproven_or_mismatched_legacy_failure_does_not_create_error(field, value):
    row = _legacy_failed_row()
    row["_failure_attempt"][field] = value
    public = _public_row(row)
    assert public["error_json"] is None
    assert "_failure_attempt" not in public


def test_legacy_failure_without_attempt_and_existing_error_are_unchanged():
    row = _legacy_failed_row()
    row["_failure_attempt"] = None
    assert _public_row(row)["error_json"] is None
    row["error_json"] = {"reason_code": "ORIGINAL", "error_type": "OriginalFailure"}
    row["_failure_attempt"] = _legacy_failed_row()["_failure_attempt"]
    assert _public_row(row)["error_json"] == row["error_json"]


@pytest.mark.parametrize("field,value", [
    ("status", "COMPLETED"), ("operation_type", "RESUME"),
])
def test_legacy_evidence_cannot_turn_another_state_or_command_into_failure(field, value):
    row = _legacy_failed_row()
    row[field] = value
    assert _public_row(row)["error_json"] is None


@pytest.mark.parametrize("field,value", [("attempt_no", 0), ("fencing_token", None), ("operation_id", "")])
def test_matching_but_invalid_legacy_identity_is_not_evidence(field, value):
    row = _legacy_failed_row()
    row[field] = row["_failure_attempt"][field] = value
    assert _public_row(row)["error_json"] is None


@pytest.mark.parametrize("field,value", [
    ("result_status", "FAILED"), ("result_hash", "c" * 64), ("result_row_version", 8),
])
def test_legacy_readback_does_not_hide_conflicting_published_result(field, value):
    row = _legacy_failed_row()
    row[field] = value
    public = _public_row(row)
    assert public["result_status"] == row["result_status"]
    assert public[field] == value
