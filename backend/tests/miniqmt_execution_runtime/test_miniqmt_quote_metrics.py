from __future__ import annotations

import pytest

from backend.services.miniqmt_execution_runtime.quote_metrics import QuoteEvidenceMetricsEmitter


def test_metric_labels_are_bounded_and_exclude_runtime_symbol_and_business_ids() -> None:
    emission = QuoteEvidenceMetricsEmitter().emit(
        health={
            "status": "DEGRADED",
            "high_priority_backlog": 3,
            "cadence_slots": 1,
            "health_slots": 1,
            "outbox_capacity": 4,
            "persistence_failures": 1,
            "runtime_id": "runtime-must-not-be-a-label",
            "symbol": "000001.SZ",
        },
        ingress_health={
            "status": "DEGRADED",
            "writer_heartbeat_age_ms": 100,
            "owner_present": True,
            "generation": 2,
            "callback_total": 8,
            "quote_age_ms_quantiles": {"0.5": 10, "0.95": 30},
            "runtime_id": "runtime-must-not-be-a-label",
            "market_data_id": "md-must-not-be-a-label",
        },
        markout={"60": {"due": 20, "captured": 18, "unavailable": 2}},
    )
    forbidden = {"runtime_id", "binding_id", "parent_id", "child_id", "trade_id", "market_data_id", "symbol", "account"}
    assert emission.alerts
    for metric in emission.metrics:
        assert not (set(metric["labels"]) & forbidden)
        assert metric["observation_only"] is True
    for alert in emission.alerts:
        assert alert["requires_acknowledge"] is False
        assert alert["execution_gate"] is False


def test_metrics_reject_missing_health_and_fake_zero_markout_inputs() -> None:
    emitter = QuoteEvidenceMetricsEmitter()
    with pytest.raises(ValueError, match="missing required fields"):
        emitter.emit(health={}, ingress_health={})
    health = {
        "status": "HEALTHY",
        "high_priority_backlog": 0,
        "cadence_slots": 0,
        "health_slots": 0,
        "outbox_capacity": 10,
        "persistence_failures": 0,
    }
    ingress = {"status": "HEALTHY", "writer_heartbeat_age_ms": 1}
    with pytest.raises(ValueError, match="exact due/captured/unavailable"):
        emitter.emit(health=health, ingress_health=ingress, markout={"60": {}})
    with pytest.raises(ValueError, match="cannot exceed due"):
        emitter.emit(
            health=health,
            ingress_health=ingress,
            markout={"60": {"due": 1, "captured": 1, "unavailable": 1}},
        )
