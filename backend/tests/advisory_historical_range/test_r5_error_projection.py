from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.routers.advisory import _raise_historical_range_http
from backend.services.advisory_historical_range.query_repository import HistoricalRangeQueryError
from backend.services.advisory_historical_range.service import HistoricalRangeServiceError


def test_structured_503_preserves_reason_and_retryability() -> None:
    with pytest.raises(HTTPException) as raised:
        _raise_historical_range_http(
            HistoricalRangeServiceError(
                "ADVISORY_HR_CONFIGURATION_UNAVAILABLE",
                "missing explicit root",
                http_status=503,
                retryable=True,
                context={"missing_configuration": ["ROOT"]},
            )
        )
    assert raised.value.status_code == 503
    assert raised.value.detail["reason_code"] == "ADVISORY_HR_CONFIGURATION_UNAVAILABLE"
    assert raised.value.detail["retryable"] is True
    assert raised.value.detail["correlation_id"].startswith("ahr-corr-")


def test_cursor_error_projects_as_422() -> None:
    with pytest.raises(HTTPException) as raised:
        _raise_historical_range_http(HistoricalRangeQueryError("ADVISORY_HR_CURSOR_INVALID", "bad cursor"))
    assert raised.value.status_code == 422
    assert raised.value.detail["reason_code"] == "ADVISORY_HR_CURSOR_INVALID"


def test_unexpected_500_does_not_leak_internal_error_text(caplog) -> None:
    with pytest.raises(HTTPException) as raised:
        _raise_historical_range_http(
            RuntimeError(r"password=secret SQL failed at F:\private\artifact-root")
        )
    assert raised.value.status_code == 500
    assert raised.value.detail["message"] == "Unexpected historical-range service error"
    assert raised.value.detail["context"] == {}
    assert "secret" not in str(raised.value.detail)
    correlation_id = raised.value.detail["correlation_id"]
    assert sum(correlation_id in record.message for record in caplog.records) == 1
