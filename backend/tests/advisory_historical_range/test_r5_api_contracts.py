from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from backend.routers.advisory import router
from backend.services.advisory_historical_range.api_models import (
    HistoricalRangeBuildBridgeRequest,
    HistoricalRangeCreateRequest,
)


def test_create_contract_is_strict_and_supports_both_program_sources() -> None:
    request = HistoricalRangeCreateRequest.model_validate(
        {
            "program_specs": [
                {
                    "source_kind": "EXISTING_PROGRAM",
                    "program_id": "advp_1",
                    "expected_program_version": 3,
                    "expected_binding_version_id": "advbind_3",
                },
                {
                    "source_kind": "RESEARCH_PROGRAM_SPEC",
                    "program_name": "native multi alpha research",
                    "package_id": "pkg_parent",
                    "target_count": 5,
                    "review_policy": {},
                    "runtime_config": {},
                },
            ],
            "start_trade_date": "2026-07-01",
            "end_trade_date": "2026-07-21",
        }
    )
    assert request.start_trade_date == date(2026, 7, 1)
    assert len(request.program_specs) == 2
    with pytest.raises(ValidationError):
        HistoricalRangeCreateRequest.model_validate(
            {**request.model_dump(mode="json"), "artifact_root": "C:/forbidden"}
        )


def test_bridge_contract_rejects_unknown_maturity() -> None:
    with pytest.raises(ValidationError):
        HistoricalRangeBuildBridgeRequest.model_validate(
            {
                "operation_idempotency_key": "key",
                "expected_row_version": 1,
                "requested_horizons": [1],
                "requested_maturity_statuses": ["NOT_DUE"],
            }
        )


def test_all_r5_routes_are_registered() -> None:
    paths = {route.path for route in router.routes}
    expected = {
        "/advisory/historical-range-options",
        "/advisory/historical-range-batches",
        "/advisory/historical-range-batches/{batch_id}",
        "/advisory/historical-range-batches/{batch_id}/runs",
        "/advisory/historical-range-batches/{batch_id}/operations",
        "/advisory/historical-range-batches/{batch_id}/resume",
        "/advisory/historical-range-batches/{batch_id}/cancel",
        "/advisory/historical-range-batches/{batch_id}/refresh-outcomes",
        "/advisory/historical-range-batches/{batch_id}/build-dataset-bridge",
        "/advisory/historical-range-runs/{range_run_id}",
        "/advisory/historical-range-runs/{range_run_id}/days",
        "/advisory/historical-range-runs/{range_run_id}/days/{trade_date}",
        "/advisory/historical-range-runs/{range_run_id}/lists/{trade_date}",
        "/advisory/historical-range-runs/{range_run_id}/outcomes",
        "/advisory/historical-range-runs/{range_run_id}/summaries",
        "/advisory/historical-range-operations/{operation_id}",
    }
    assert expected <= paths
