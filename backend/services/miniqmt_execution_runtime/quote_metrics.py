"""Bounded-cardinality observation metrics and automatic alert facts for P1-D."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Mapping


logger = logging.getLogger(__name__)

_ALLOWED_LABELS = frozenset({"market", "capture_type", "state", "reason_code", "stage", "horizon", "source_method", "quantile"})


@dataclass(frozen=True)
class QuoteMetricsEmission:
    metrics: tuple[Mapping[str, Any], ...]
    alerts: tuple[Mapping[str, Any], ...]


class QuoteEvidenceMetricsEmitter:
    """Emit structured facts only; alerts never execute, acknowledge, or gate broker actions."""

    def emit(
        self,
        *,
        health: Mapping[str, Any],
        ingress_health: Mapping[str, Any],
        markout: Mapping[str, Mapping[str, int]] | None = None,
    ) -> QuoteMetricsEmission:
        metrics: list[Mapping[str, Any]] = []
        alerts: list[Mapping[str, Any]] = []
        required_health = {
            "status",
            "high_priority_backlog",
            "cadence_slots",
            "health_slots",
            "outbox_capacity",
            "persistence_failures",
        }
        missing_health = sorted(required_health - set(health))
        if missing_health:
            raise ValueError(f"quote evidence health is missing required fields: {', '.join(missing_health)}")
        if "status" not in ingress_health or "writer_heartbeat_age_ms" not in ingress_health:
            raise ValueError("quote ingress health requires status and writer_heartbeat_age_ms")
        state = str(health["status"]).strip()
        ingress_state = str(ingress_health["status"]).strip()
        if not state or not ingress_state:
            raise ValueError("quote metric health status values must be non-empty")
        high_backlog = _required_non_negative_int(health, "high_priority_backlog")
        cadence_slots = _required_non_negative_int(health, "cadence_slots")
        health_slots = _required_non_negative_int(health, "health_slots")
        persistence_failures = _required_non_negative_int(health, "persistence_failures")
        capacity = _required_positive_int(health, "outbox_capacity")
        heartbeat_age_ms = _required_non_negative_number(ingress_health, "writer_heartbeat_age_ms")
        backlog = high_backlog + cadence_slots + health_slots
        ratio = high_backlog / capacity
        metrics.extend(
            (
                self._metric("miniqmt_quote_evidence_outbox_backlog", backlog, {"state": state}),
                self._metric("miniqmt_quote_market_data_persist_failures_total", persistence_failures, {"stage": "PERSIST"}),
                self._metric("miniqmt_quote_writer_heartbeat_age_ms", heartbeat_age_ms, {}),
            )
        )
        ingress_metric_fields = (
            ("miniqmt_quote_ingress_owner", "owner_present", "state"),
            ("miniqmt_quote_subscription_generation", "generation", None),
            ("miniqmt_quote_bootstrap_coverage_ratio", "bootstrap_coverage_ratio", None),
            ("miniqmt_quote_callback_total", "callback_total", None),
            ("miniqmt_quote_coalesced_total", "coalesced_total", None),
            ("miniqmt_quote_capacity_rejected_total", "capacity_rejected_total", None),
            ("miniqmt_quote_consumer_restart_total", "consumer_restart_total", None),
            ("miniqmt_quote_valid_depth_ratio", "valid_depth_ratio", None),
            ("miniqmt_quote_action_ready_ratio", "action_ready_ratio", None),
            ("miniqmt_b0_quote_v2_parity_violations_total", "b0_quote_v2_parity_violations_total", None),
        )
        for metric_name, key, label_key in ingress_metric_fields:
            value = (
                int(ingress_health[key])
                if key == "owner_present" and isinstance(ingress_health.get(key), bool)
                else _metric_number(ingress_health, key)
            )
            if value is None:
                continue
            labels = {label_key: ingress_state} if label_key else {}
            metrics.append(self._metric(metric_name, value, labels))
        for value_key, metric_name in (
            ("quote_age_ms_quantiles", "miniqmt_quote_age_ms"),
            ("clock_age_divergence_ms_quantiles", "miniqmt_quote_clock_age_divergence_ms"),
        ):
            quantiles = ingress_health.get(value_key)
            if quantiles is None:
                continue
            if not isinstance(quantiles, Mapping):
                raise ValueError(f"{value_key} must be an explicit quantile mapping")
            for quantile, value in sorted(quantiles.items(), key=lambda item: str(item[0])):
                if str(quantile) not in {"0.5", "0.9", "0.95", "0.99"}:
                    raise ValueError(f"{value_key} contains an unregistered quantile")
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValueError(f"{value_key} quantile value must be numeric")
                metrics.append(self._metric(metric_name, value, {"quantile": str(quantile)}))
        if ratio is not None:
            metrics.append(self._metric("miniqmt_quote_evidence_outbox_capacity_ratio", ratio, {"state": state}))
            if ratio >= 1:
                alerts.append(self._alert("MINIQMT_QUOTE_EVIDENCE_OUTBOX_FULL", "CRITICAL", "ADAPTIVE_IS_MARKET_DATA_EVIDENCE_OUTBOX_FULL", "PERSIST"))
            elif ratio >= 0.8:
                alerts.append(self._alert("MINIQMT_QUOTE_EVIDENCE_OUTBOX_HIGH", "WARNING", "ADAPTIVE_IS_MARKET_DATA_EVIDENCE_OUTBOX_FULL", "PERSIST"))
        if persistence_failures > 0:
            alerts.append(self._alert("MINIQMT_QUOTE_EVIDENCE_PERSIST_FAILURE", "CRITICAL", "ADAPTIVE_IS_MARKET_DATA_EVIDENCE_PERSIST_FAILED", "PERSIST"))
        for horizon, counts in (markout or {}).items():
            if str(horizon) not in {"60", "300", "900"}:
                raise ValueError("markout metrics horizon must be 60, 300, or 900 seconds")
            if not isinstance(counts, Mapping):
                raise ValueError("markout metrics counts must be a mapping")
            if set(counts) != {"due", "captured", "unavailable"}:
                raise ValueError("markout metrics counts require exact due/captured/unavailable fields")
            due = _required_non_negative_int(counts, "due")
            captured = _required_non_negative_int(counts, "captured")
            unavailable = _required_non_negative_int(counts, "unavailable")
            if captured + unavailable > due:
                raise ValueError("markout captured plus unavailable cannot exceed due")
            coverage = None if due <= 0 else captured / due
            metrics.append(self._metric("miniqmt_quote_markout_due_total", due, {"horizon": str(horizon)}))
            metrics.append(self._metric("miniqmt_quote_markout_unavailable_total", unavailable, {"horizon": str(horizon)}))
            if coverage is not None:
                metrics.append(self._metric("miniqmt_quote_markout_coverage_ratio", coverage, {"horizon": str(horizon)}))
                if due >= 20 and coverage < 0.95:
                    alerts.append(self._alert("MINIQMT_QUOTE_MARKOUT_COVERAGE_LOW", "WARNING", "ADAPTIVE_IS_MARKOUT_QUOTE_UNAVAILABLE", "MARKOUT", horizon=str(horizon)))
        for metric in metrics:
            logger.info("miniqmt_quote_metric %s", json.dumps(metric, ensure_ascii=False, sort_keys=True), extra={"aistock_metric": metric})
        return QuoteMetricsEmission(metrics=tuple(metrics), alerts=tuple(alerts))

    @staticmethod
    def _metric(name: str, value: Any, labels: Mapping[str, str]) -> Mapping[str, Any]:
        if set(labels) - _ALLOWED_LABELS:
            raise ValueError("quote metric contains an unbounded or unregistered label")
        return {"metric": name, "value": value, "labels": dict(labels), "schema_version": "miniqmt_quote_metric_v1", "observation_only": True}

    @staticmethod
    def _alert(alert_type: str, severity: str, reason_code: str, stage: str, **context: str) -> Mapping[str, Any]:
        return {
            "schema_version": "miniqmt_quote_alert_v1",
            "alert_type": alert_type,
            "severity": severity,
            "reason_code": reason_code,
            "stage": stage,
            "context": dict(context),
            "execution_gate": False,
            "observation_only": True,
            "requires_acknowledge": False,
        }


def _metric_number(values: Mapping[str, Any], key: str) -> int | float | None:
    if key not in values or values[key] is None:
        return None
    value = values[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{key} must be numeric when supplied")
    return value


def _required_non_negative_int(values: Mapping[str, Any], key: str) -> int:
    if key not in values:
        raise ValueError(f"{key} is required")
    value = values[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{key} must be a non-negative integer")
    return value


def _required_positive_int(values: Mapping[str, Any], key: str) -> int:
    value = _required_non_negative_int(values, key)
    if value == 0:
        raise ValueError(f"{key} must be positive")
    return value


def _required_non_negative_number(values: Mapping[str, Any], key: str) -> int | float:
    if key not in values:
        raise ValueError(f"{key} is required")
    value = values[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{key} must be a non-negative number")
    return value


__all__ = ["QuoteEvidenceMetricsEmitter", "QuoteMetricsEmission"]
