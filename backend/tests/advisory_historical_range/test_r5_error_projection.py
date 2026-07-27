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
