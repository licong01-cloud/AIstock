"""Select a capped first-wave announcement document sample.

The refined queue can still be too large for immediate PDF download.  This
module selects a deterministic, stratified sample for parser/LLM validation.
It is side-effect free and does not create persistent queues.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Mapping, Optional

from backend.services.event_signal.document_queue_refiner import RefinedDocumentDecision


DEFAULT_EVENT_TYPE_CAPS: Mapping[str, int] = {
    "capital_occupation_illegal_guarantee": 1600,
    "regulatory_investigation_penalty": 1600,
    "audit_opinion_internal_control_risk": 1200,
    "debt_default_overdue": 1000,
    "pledge_shareholder_change_reduction": 900,
    "guarantee_financial_assistance_related_party": 700,
    "litigation_arbitration_freeze": 700,
    "control_change_ma_restructuring": 300,
}


@dataclass(frozen=True)
class FirstWaveConfig:
    total_cap: int = 5000
    default_event_type_cap: int = 300
    per_event_year_cap: int = 120
    event_type_caps: Mapping[str, int] = field(default_factory=lambda: DEFAULT_EVENT_TYPE_CAPS)


def _year(decision: RefinedDocumentDecision) -> int:
    effective = decision.base.effective_trade_date
    return effective.year if effective else 0


def _amount_score(decision: RefinedDocumentDecision) -> Decimal:
    return decision.materiality.max_amount_yuan or Decimal("0")


def decision_sort_key(decision: RefinedDocumentDecision) -> tuple[Any, ...]:
    """Sort high priority, material, large amount, and recent records first."""

    effective = decision.base.effective_trade_date or dt.date.min
    return (
        -decision.refined_priority_score,
        not decision.materiality.is_material,
        -_amount_score(decision),
        -effective.toordinal(),
        decision.base.ts_code or "",
        decision.base.ann_id or 0,
    )


def select_first_wave_candidates(
    decisions: Iterable[RefinedDocumentDecision],
    *,
    config: FirstWaveConfig = FirstWaveConfig(),
) -> list[RefinedDocumentDecision]:
    """Select a deterministic capped sample from refined document decisions."""

    if config.total_cap <= 0:
        raise ValueError("total_cap must be positive")
    if config.default_event_type_cap <= 0:
        raise ValueError("default_event_type_cap must be positive")
    if config.per_event_year_cap <= 0:
        raise ValueError("per_event_year_cap must be positive")

    eligible = [decision for decision in decisions if decision.require_document]
    eligible.sort(key=decision_sort_key)

    year_counts: dict[tuple[str, int], int] = {}
    event_counts: dict[str, int] = {}
    selected: list[RefinedDocumentDecision] = []
    for decision in eligible:
        event_type = decision.base.event_type
        year_key = (event_type, _year(decision))
        event_cap = config.event_type_caps.get(event_type, config.default_event_type_cap)
        if event_counts.get(event_type, 0) >= event_cap:
            continue
        if year_counts.get(year_key, 0) >= config.per_event_year_cap:
            continue
        selected.append(decision)
        event_counts[event_type] = event_counts.get(event_type, 0) + 1
        year_counts[year_key] = year_counts.get(year_key, 0) + 1
        if len(selected) >= config.total_cap:
            break
    return selected


def summarize_first_wave(
    selected: Iterable[RefinedDocumentDecision],
    *,
    eligible_count: Optional[int] = None,
) -> dict[str, Any]:
    rows = list(selected)
    by_event_type: dict[str, int] = {}
    by_year: dict[str, int] = {}
    material_rows = 0
    for decision in rows:
        by_event_type[decision.base.event_type] = by_event_type.get(decision.base.event_type, 0) + 1
        year = str(_year(decision))
        by_year[year] = by_year.get(year, 0) + 1
        if decision.materiality.is_material:
            material_rows += 1
    return {
        "eligible_document_rows": eligible_count if eligible_count is not None else len(rows),
        "selected_rows": len(rows),
        "material_rows": material_rows,
        "by_event_type": dict(sorted(by_event_type.items())),
        "by_year": dict(sorted(by_year.items())),
    }


def compact_first_wave_row(decision: RefinedDocumentDecision) -> dict[str, Any]:
    return {
        "ann_id": decision.base.ann_id,
        "ts_code": decision.base.ts_code,
        "event_type": decision.base.event_type,
        "risk_level": decision.base.risk_level,
        "title": decision.base.title,
        "effective_trade_date": decision.base.effective_trade_date,
        "priority_score": decision.refined_priority_score,
        "materiality": decision.materiality.to_dict(),
        "reason_codes": decision.reason_codes,
    }
