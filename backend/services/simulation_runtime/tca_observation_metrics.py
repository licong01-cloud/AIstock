"""Structured observation-only metrics and alerts for MiniQMT TCA EOD work."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TcaObservationEmission:
    metrics: tuple[Mapping[str, Any], ...]
    alerts: tuple[Mapping[str, Any], ...]


class TcaObservationMetricsEmitter:
    """Emit facts only; no metric or alert can change execution eligibility."""

    def emit(
        self,
        *,
        outcomes: Sequence[Mapping[str, Any]],
        trade_date: date,
        source: str,
    ) -> TcaObservationEmission:
        metrics: list[Mapping[str, Any]] = []
        alerts: list[Mapping[str, Any]] = []
        for outcome in outcomes:
            metric = {
                "metric": "miniqmt_tca_observation",
                "schema_version": "miniqmt_tca_observation_metric_v1",
                "trade_date": trade_date.isoformat(),
                "source": source,
                "status": str(outcome.get("status") or "UNKNOWN"),
                "reason_code": outcome.get("reason_code"),
                "stage": str(outcome.get("stage") or "TCA_EOD_OBSERVATION"),
                "run_id": outcome.get("run_id"),
                "binding_id": outcome.get("binding_id"),
                "receipt_id": outcome.get("receipt_id"),
                "execution_gate": False,
                "observation_only": True,
            }
            logger.info(
                "miniqmt_tca_observation %s",
                json.dumps(metric, ensure_ascii=False, sort_keys=True),
                extra={"aistock_metric": metric},
            )
            metrics.append(metric)
            if metric["status"] == "FAILED":
                alerts.append(
                    {
                        "alert_type": "MINIQMT_TCA_OBSERVATION_FAILURE",
                        "severity": "WARNING",
                        "reason_code": metric["reason_code"] or "ADAPTIVE_IS_TCA_EOD_OBSERVATION_FAILED",
                        "stage": metric["stage"],
                        "trade_date": metric["trade_date"],
                        "run_id": metric["run_id"],
                        "binding_id": metric["binding_id"],
                        "execution_gate": False,
                        "observation_only": True,
                    }
                )
        return TcaObservationEmission(metrics=tuple(metrics), alerts=tuple(alerts))
