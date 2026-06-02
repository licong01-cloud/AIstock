"""Post-decision diagnostics for advisory entry bands and stop zones."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from math import isfinite
from statistics import mean
from typing import Any, Iterable, Mapping

from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.price_guard import STOP_LOSS_TRIGGERED


FORBIDDEN_DECISION_INPUT_KEYS = {
    "next_close",
    "future_close",
    "future_high",
    "future_low",
    "forward_return_bps",
    "realized_return_bps",
    "post_decision_return_bps",
}


@dataclass(frozen=True)
class AdvisoryDiagnosticRecord:
    code: str
    trade_date: str
    current_price: float
    entry_band_json: Mapping[str, Any]
    action: str
    reason_code: str
    score_bucket: str | None = None
    gap_bucket: str | None = None
    regime: str | None = None
    liquidity_bucket: str | None = None
    board_type: str | None = None
    decision_input_json: Mapping[str, Any] | None = None
    day_low: float | None = None
    day_high: float | None = None
    forward_alpha_bps: float | None = None
    stop_saved_loss_bps: float | None = None
    stop_whipsaw_cost_bps: float | None = None
    realized_reward_bps: float | None = None
    realized_risk_bps: float | None = None


def generate_quality_report(
    records: Iterable[AdvisoryDiagnosticRecord | Mapping[str, Any]],
    *,
    min_bucket_size: int = 30,
) -> dict[str, Any]:
    rows = [_coerce_record(item) for item in records]
    _validate_no_future_inputs(rows)
    overall = _metrics(rows)
    bucket_map: dict[tuple[str, str, str, str, str], list[AdvisoryDiagnosticRecord]] = defaultdict(list)
    for row in rows:
        bucket_map[_bucket_key(row)].append(row)
    buckets = []
    parent_map: dict[tuple[str, str, str], list[AdvisoryDiagnosticRecord]] = defaultdict(list)
    for row in rows:
        parent_map[_parent_key(row)].append(row)
    for key, bucket_rows in sorted(bucket_map.items()):
        parent_key = key[:3]
        shrunk = len(bucket_rows) < min_bucket_size
        metric_rows = parent_map[parent_key] if shrunk else bucket_rows
        buckets.append(
            {
                "bucket": {
                    "score_bucket": key[0],
                    "gap_bucket": key[1],
                    "regime": key[2],
                    "liquidity_bucket": key[3],
                    "board_type": key[4],
                },
                "sample_count": len(bucket_rows),
                "metrics_sample_count": len(metric_rows),
                "shrunk_to_parent": shrunk,
                "parent_bucket": {
                    "score_bucket": parent_key[0],
                    "gap_bucket": parent_key[1],
                    "regime": parent_key[2],
                }
                if shrunk
                else None,
                "metrics": _metrics(metric_rows),
            }
        )
    return {
        "report_type": "post_decision_diagnostics",
        "validated_pnl": False,
        "pnl_label": "not_validated_pnl",
        "sample_count": len(rows),
        "min_bucket_size": min_bucket_size,
        "metrics": overall,
        "buckets": buckets,
        "warnings": [
            "post-decision diagnostics only; not QE validated PnL",
            "selection bias and sample-size effects must be reviewed before QE/Paper adoption",
            "future outcome fields are used only after the decision timestamp for diagnostics",
        ],
    }


def _metrics(rows: list[AdvisoryDiagnosticRecord]) -> dict[str, float | int | None]:
    n = len(rows)
    if not rows:
        return _empty_metrics()
    in_zone = [_entry_zone_hit(row) for row in rows]
    fillable = [_entry_zone_fillable(row) for row in rows]
    entered_alpha = [row.forward_alpha_bps for row in rows if _entry_zone_hit(row) and _finite(row.forward_alpha_bps)]
    chased_alpha = [row.forward_alpha_bps for row in rows if not _entry_zone_hit(row) and _finite(row.forward_alpha_bps)]
    missed_alpha = [row.forward_alpha_bps for row in rows if row.action in {"SKIP", "WAITING"} and _finite(row.forward_alpha_bps)]
    soft_stop_rows = [row for row in rows if "SOFT" in str(row.reason_code).upper()]
    hard_stop_rows = [row for row in rows if row.reason_code == STOP_LOSS_TRIGGERED or "HARD" in str(row.reason_code).upper()]
    saved = [row.stop_saved_loss_bps for row in rows if _finite(row.stop_saved_loss_bps)]
    whipsaw = [row.stop_whipsaw_cost_bps for row in rows if _finite(row.stop_whipsaw_cost_bps)]
    reward = [row.realized_reward_bps for row in rows if _finite(row.realized_reward_bps)]
    risk = [abs(float(row.realized_risk_bps)) for row in rows if _finite(row.realized_risk_bps) and float(row.realized_risk_bps) != 0]
    return {
        "sample_count": n,
        "entry_zone_hit_rate": sum(1 for item in in_zone if item) / n,
        "entry_zone_fillable_rate": sum(1 for item in fillable if item) / n,
        "alpha_if_entered_zone": _avg(entered_alpha),
        "alpha_if_chased_above_zone": _avg(chased_alpha),
        "missed_alpha_if_not_entered": _avg(missed_alpha),
        "soft_stop_trigger_rate": len(soft_stop_rows) / n,
        "hard_stop_trigger_rate": len(hard_stop_rows) / n,
        "stop_saved_loss_bps": _avg(saved),
        "stop_whipsaw_cost_bps": _avg(whipsaw),
        "reward_risk_realized": (_avg(reward) / _avg(risk)) if reward and risk and _avg(risk) else None,
    }


def _empty_metrics() -> dict[str, float | int | None]:
    return {
        "sample_count": 0,
        "entry_zone_hit_rate": None,
        "entry_zone_fillable_rate": None,
        "alpha_if_entered_zone": None,
        "alpha_if_chased_above_zone": None,
        "missed_alpha_if_not_entered": None,
        "soft_stop_trigger_rate": None,
        "hard_stop_trigger_rate": None,
        "stop_saved_loss_bps": None,
        "stop_whipsaw_cost_bps": None,
        "reward_risk_realized": None,
    }


def _entry_zone_hit(row: AdvisoryDiagnosticRecord) -> bool:
    max_buy = _max_buy_price(row.entry_band_json)
    return max_buy is not None and float(row.current_price) <= max_buy


def _entry_zone_fillable(row: AdvisoryDiagnosticRecord) -> bool:
    max_buy = _max_buy_price(row.entry_band_json)
    if max_buy is None:
        return False
    day_low = row.day_low if _finite(row.day_low) else row.current_price
    return float(day_low) <= max_buy


def _max_buy_price(entry_band: Mapping[str, Any]) -> float | None:
    for path in (("max_buy_price",), ("yellow", "max_price"), ("green", "max_price")):
        value: Any = entry_band
        for key in path:
            if not isinstance(value, Mapping):
                value = None
                break
            value = value.get(key)
        if _finite(value):
            return float(value)
    return None


def _coerce_record(item: AdvisoryDiagnosticRecord | Mapping[str, Any]) -> AdvisoryDiagnosticRecord:
    if isinstance(item, AdvisoryDiagnosticRecord):
        return item
    return AdvisoryDiagnosticRecord(
        code=str(item.get("code")),
        trade_date=str(item.get("trade_date")),
        current_price=float(item.get("current_price")),
        entry_band_json=dict(item.get("entry_band_json") or {}),
        action=str(item.get("action") or ""),
        reason_code=str(item.get("reason_code") or ""),
        score_bucket=item.get("score_bucket"),
        gap_bucket=item.get("gap_bucket"),
        regime=item.get("regime"),
        liquidity_bucket=item.get("liquidity_bucket"),
        board_type=item.get("board_type"),
        decision_input_json=item.get("decision_input_json"),
        day_low=_optional_float(item.get("day_low")),
        day_high=_optional_float(item.get("day_high")),
        forward_alpha_bps=_optional_float(item.get("forward_alpha_bps")),
        stop_saved_loss_bps=_optional_float(item.get("stop_saved_loss_bps")),
        stop_whipsaw_cost_bps=_optional_float(item.get("stop_whipsaw_cost_bps")),
        realized_reward_bps=_optional_float(item.get("realized_reward_bps")),
        realized_risk_bps=_optional_float(item.get("realized_risk_bps")),
    )


def _validate_no_future_inputs(rows: list[AdvisoryDiagnosticRecord]) -> None:
    for row in rows:
        decision_inputs = row.decision_input_json or {}
        hits = sorted(_future_key_hits(decision_inputs))
        if hits:
            raise RuntimeConfigInvalidError(
                "advisory quality decision_input_json contains future outcome fields",
                context={"code": row.code, "trade_date": row.trade_date, "future_keys": hits},
            )


def _future_key_hits(value: Any, *, prefix: str = "decision_input_json") -> set[str]:
    hits: set[str] = set()
    if isinstance(value, Mapping):
        for key, child in value.items():
            path = f"{prefix}.{key}"
            if str(key) in FORBIDDEN_DECISION_INPUT_KEYS:
                hits.add(path)
            hits.update(_future_key_hits(child, prefix=path))
    elif isinstance(value, list):
        for idx, child in enumerate(value):
            hits.update(_future_key_hits(child, prefix=f"{prefix}[{idx}]"))
    return hits


def _bucket_key(row: AdvisoryDiagnosticRecord) -> tuple[str, str, str, str, str]:
    return (
        str(row.score_bucket or "score_unknown"),
        str(row.gap_bucket or "gap_unknown"),
        str(row.regime or "regime_unknown"),
        str(row.liquidity_bucket or "liq_unknown"),
        str(row.board_type or "board_unknown"),
    )


def _parent_key(row: AdvisoryDiagnosticRecord) -> tuple[str, str, str]:
    return (
        str(row.score_bucket or "score_unknown"),
        str(row.gap_bucket or "gap_unknown"),
        str(row.regime or "regime_unknown"),
    )


def _avg(values: list[float | int | None]) -> float | None:
    clean = [float(item) for item in values if _finite(item)]
    return mean(clean) if clean else None


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if isfinite(parsed) else None


def _finite(value: Any) -> bool:
    try:
        return isfinite(float(value))
    except (TypeError, ValueError):
        return False
