from __future__ import annotations

import inspect

import pytest
from fastapi import HTTPException

from backend.routers import qmt


@pytest.mark.parametrize(
    ("endpoint", "call"),
    [
        ("/api/v1/qmt/order", lambda: qmt.place_order({"trade_password": "valid"})),
        ("/api/v1/qmt/order/batch", lambda: qmt.batch_place_order({"orders": [{}]})),
        ("/api/v1/qmt/cancel", lambda: qmt.cancel_order({"order_id": "123"})),
    ],
)
def test_raw_qmt_broker_write_routes_are_permanently_retired_before_client_access(
    endpoint: str,
    call,
    monkeypatch: pytest.MonkeyPatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("AISTOCK_ALLOW_QMT_RAW_ORDER_DIAGNOSTICS", "1")
    monkeypatch.setenv("QMT_TRADE_PASSWORD", "valid")
    monkeypatch.setattr(qmt, "_get_client", lambda: pytest.fail("retired route accessed QMT client"))

    with pytest.raises(HTTPException) as exc_info:
        call()

    assert exc_info.value.status_code == 410
    assert exc_info.value.detail == {
        "error_code": "EXECUTION_PATH_NOT_CANONICAL",
        "reason_code": "MINIQMT_RAW_BROKER_ROUTE_RETIRED",
        "message": qmt.RAW_ORDER_DIAGNOSTIC_WARNING,
        "endpoint": endpoint,
        "required_runtime_owner": "MiniQMTExecutionRuntime",
        "replacement": "/api/v1/simulation-runtime",
        "broker_called": False,
        "legacy_fallback": False,
    }


def test_raw_qmt_route_functions_contain_no_direct_broker_write_calls() -> None:
    source = "\n".join(
        inspect.getsource(function)
        for function in (qmt.place_order, qmt.batch_place_order, qmt.cancel_order)
    )
    assert ".place_order(" not in source
    assert ".cancel_order(" not in source
    assert "_get_client(" not in source
