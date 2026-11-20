"""QMT read-side diagnostic contracts."""


from __future__ import annotations

from datetime import UTC, datetime

from backend.infra.qmt_client import build_qmt_order_diagnostic


def test_build_qmt_order_diagnostic_marks_stale_cancelable_and_bad_status_msg() -> None:
    stale_epoch = int(datetime(2024, 1, 2, 9, 31, tzinfo=UTC).timestamp())
    diagnostic = build_qmt_order_diagnostic(
        {
            "order_id": "1090519216",
            "order_status": 50,
            "order_time": str(stale_epoch),
            "status_msg": "[COUNTER][260200][\u00e5\u008f",
        },
        cancelable_only=True,
    )

    assert diagnostic["schema_version"] == "qmt_order_diagnostic_v1"
    assert diagnostic["broker_error_code"] == "260200"
    assert diagnostic["status_msg_maybe_truncated"] is True
    assert diagnostic["status_msg_encoding_warning"] is True
    assert diagnostic["diagnostic_gap"] is True
    assert diagnostic["cancelable_stale_warning"] is True
    assert diagnostic["cancelable_stale_reason"] == "historical_cancelable_order_reported_by_broker"

    code_only = build_qmt_order_diagnostic({"order_status": 57, "status_msg": "[COUNTER][260200]"})
    assert code_only["diagnostic_completeness"] == "broker_status_msg_code_only"
    assert code_only["status_msg_maybe_truncated"] is False


def test_router_exposes_order_diagnostic_metadata_as_top_level_fields() -> None:
    from backend.routers.paper_trading_v2 import _expose_order_diagnostics

    row = {
        "order_id": "ord_rejected",
        "status": "REJECTED",
        "metadata": {
            "broker_status": "rejected",
            "broker_raw_status": 57,
            "broker_status_msg": "[COUNTER][260200] insufficient buying power",
            "broker_rejection_reason": "[COUNTER][260200] insufficient buying power",
            "broker_diagnostic": {
                "schema_version": "miniqmt_order_diagnostic_v1",
                "broker_error_code": "260200",
                "broker_rejection_classification": "counter_260200",
                "diagnostic_completeness": "best_available",
                "diagnostic_gap": False,
                "status_msg_best_available": "[COUNTER][260200] insufficient buying power",
            },
        },
    }

    exposed = _expose_order_diagnostics(row)

    assert exposed["broker_raw_status"] == 57
    assert exposed["broker_status_msg"] == "[COUNTER][260200] insufficient buying power"
    assert exposed["status_msg"] == "[COUNTER][260200] insufficient buying power"
    assert exposed["error_msg"] == "[COUNTER][260200] insufficient buying power"
    assert exposed["broker_diagnostic"]["schema_version"] == "miniqmt_order_diagnostic_v1"
    assert exposed["broker_error_code"] == "260200"
    assert exposed["broker_rejection_classification"] == "counter_260200"
    assert exposed["diagnostic_completeness"] == "best_available"
    assert exposed["diagnostic_gap"] is False
    assert exposed["status_msg_best_available"] == "[COUNTER][260200] insufficient buying power"
