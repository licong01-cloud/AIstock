from __future__ import annotations

from datetime import date

from backend.services.simulation_runtime.tca_observation_metrics import TcaObservationMetricsEmitter


def test_observation_metrics_keep_reason_stage_and_alert_only_failures() -> None:
    emission = TcaObservationMetricsEmitter().emit(
        outcomes=(
            {
                "status": "REBUILT",
                "reason_code": None,
                "stage": "TCA_EOD_REBUILD",
                "run_id": "run-ok",
                "binding_id": "binding-ok",
                "receipt_id": "receipt-ok",
            },
            {
                "status": "FAILED",
                "reason_code": "ADAPTIVE_IS_TCA_EOD_REBUILD_EXCEPTION",
                "stage": "TCA_EOD_REBUILD",
                "run_id": "run-failed",
                "binding_id": "binding-failed",
            },
        ),
        trade_date=date(2026, 7, 10),
        source="unit-test",
    )

    assert [item["status"] for item in emission.metrics] == ["REBUILT", "FAILED"]
    assert emission.metrics[1]["reason_code"] == "ADAPTIVE_IS_TCA_EOD_REBUILD_EXCEPTION"
    assert emission.metrics[1]["stage"] == "TCA_EOD_REBUILD"
    assert emission.metrics[1]["execution_gate"] is False
    assert emission.metrics[1]["observation_only"] is True
    assert emission.alerts == (
        {
            "alert_type": "MINIQMT_TCA_OBSERVATION_FAILURE",
            "severity": "WARNING",
            "reason_code": "ADAPTIVE_IS_TCA_EOD_REBUILD_EXCEPTION",
            "stage": "TCA_EOD_REBUILD",
            "trade_date": "2026-07-10",
            "run_id": "run-failed",
            "binding_id": "binding-failed",
            "execution_gate": False,
            "observation_only": True,
        },
    )
