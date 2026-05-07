"""Plan which announcement documents should enter future LLM review.

This module is intentionally deterministic and side-effect free.  It does not
download PDFs, parse files, call LLMs, write database rows, or generate trading
signals.  It only turns existing title classifications plus optional structured
context into an auditable review/download decision.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Optional


QUEUE_SKIP = "skip"
QUEUE_DOCUMENT_REQUIRED = "document_required"
QUEUE_DOCUMENT_CANDIDATE = "document_candidate"
QUEUE_SAMPLE_ONLY = "sample_only"

LLM_STAGE_NONE = "none"
LLM_STAGE_FIRST_BATCH = "first_batch"
LLM_STAGE_SAMPLED = "sampled"
LLM_STAGE_LATER = "later"

TITLE_ONLY_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "stock_delisting_confirmed",
        "stock_delisting_risk_warning",
        "stock_st_imposed",
        "stock_st_added_or_continued",
        "stock_st_removal_applied",
        "stock_st_removed_confirmed",
        "convertible_bond_delisting_or_redemption",
        "generic_bond_delisting_or_repayment",
        "periodic_report_neutral",
        "meeting_resolution_neutral",
        "governance_document_neutral",
        "investor_relations_neutral",
        "ipo_refinancing_review_neutral",
        "routine_correction_supplement_neutral",
        "routine_professional_report_neutral",
        "routine_personnel_change_neutral",
    }
)

STRUCTURED_FINANCIAL_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "financial_forecast_loss",
        "financial_forecast_large_decline",
        "financial_forecast_turnaround",
        "financial_forecast_large_growth",
        "financial_express_loss",
        "financial_express_large_decline",
        "financial_express_large_growth",
        "financial_indicator_loss",
        "financial_indicator_large_decline",
        "financial_indicator_large_growth",
        "financial_positive_but_miss_expectation",
        "performance_forecast_revision_impairment",
    }
)

FIRST_BATCH_REQUIRED_ROUTES: Mapping[str, tuple[str, ...]] = {
    "audit_opinion_internal_control_risk": ("audit_opinion_internal_control_risk",),
    "regulatory_investigation_penalty": ("regulatory_investigation_penalty",),
    "capital_occupation_illegal_guarantee": ("capital_occupation_illegal_guarantee",),
    "debt_default_overdue": ("debt_default_overdue",),
}

AMOUNT_SENSITIVE_ROUTES: Mapping[str, tuple[str, ...]] = {
    "litigation_arbitration_freeze": ("litigation_arbitration_freeze",),
    "pledge_shareholder_change_reduction": ("litigation_arbitration_freeze",),
    "guarantee_financial_assistance_related_party": ("capital_occupation_illegal_guarantee",),
    "control_change_ma_restructuring": ("regulatory_investigation_penalty",),
}

CONTEXT_SENSITIVE_ROUTES: Mapping[str, tuple[str, ...]] = {
    "inquiry_concern_letter": ("inquiry_concern_letter",),
    "key_personnel_change": ("regulatory_investigation_penalty",),
    "suspension_resumption": ("inquiry_concern_letter",),
    "stock_price_abnormal_volatility": ("inquiry_concern_letter",),
    "financing_dilution_debt_instruments": ("debt_default_overdue",),
}

HIGH_AMOUNT_YUAN = Decimal("50000000")
HIGH_AMOUNT_RATIO = Decimal("0.05")


@dataclass(frozen=True)
class DocumentReviewDecision:
    """One deterministic document-review decision for an announcement."""

    ann_id: Optional[int]
    ts_code: Optional[str]
    event_type: str
    risk_level: str
    queue_action: str
    llm_stage: str
    priority_score: int
    route_event_types: tuple[str, ...]
    reason_codes: tuple[str, ...]
    require_document: bool
    require_llm: bool
    max_chunks: int
    max_chars_per_chunk: int
    title: Optional[str] = None
    effective_trade_date: Optional[dt.date] = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if self.effective_trade_date is not None:
            payload["effective_trade_date"] = self.effective_trade_date.isoformat()
        return payload


def _decimal(value: Any) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _bool_context(context: Mapping[str, Any], key: str) -> bool:
    return bool(context.get(key) is True or str(context.get(key)).lower() in {"1", "true", "yes"})


def _priority_base(risk_level: str) -> int:
    if risk_level == "P0_BLOCK":
        return 100
    if risk_level == "P1_HIGH":
        return 85
    if risk_level == "P2_REVIEW":
        return 60
    if risk_level == "P3_POSITIVE_CANDIDATE":
        return 30
    return 10


def _effective_date(row: Mapping[str, Any]) -> Optional[dt.date]:
    value = row.get("effective_trade_date")
    if value is None or isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value))


def _ann_id(row: Mapping[str, Any]) -> Optional[int]:
    value = row.get("ann_id") or row.get("source_pk")
    if value in (None, ""):
        return None
    return int(value)


def _amount_is_material(context: Mapping[str, Any]) -> tuple[bool, tuple[str, ...]]:
    amount_yuan = _decimal(context.get("amount_yuan") or context.get("case_amount_yuan"))
    amount_ratio = _decimal(context.get("amount_to_market_cap") or context.get("amount_to_assets"))
    reasons: list[str] = []
    if amount_yuan is not None and amount_yuan >= HIGH_AMOUNT_YUAN:
        reasons.append("material_amount_yuan")
    if amount_ratio is not None and amount_ratio >= HIGH_AMOUNT_RATIO:
        reasons.append("material_amount_ratio")
    return bool(reasons), tuple(reasons)


def _decision(
    row: Mapping[str, Any],
    *,
    queue_action: str,
    llm_stage: str,
    priority_score: int,
    route_event_types: Iterable[str] = (),
    reason_codes: Iterable[str] = (),
    max_chunks: int = 8,
    max_chars_per_chunk: int = 1600,
) -> DocumentReviewDecision:
    require_document = queue_action in {QUEUE_DOCUMENT_REQUIRED, QUEUE_DOCUMENT_CANDIDATE}
    require_llm = llm_stage in {LLM_STAGE_FIRST_BATCH, LLM_STAGE_SAMPLED, LLM_STAGE_LATER}
    return DocumentReviewDecision(
        ann_id=_ann_id(row),
        ts_code=str(row["ts_code"]) if row.get("ts_code") else None,
        event_type=str(row.get("event_type") or "unclassified_archive"),
        risk_level=str(row.get("risk_level") or "P4_NEUTRAL"),
        queue_action=queue_action,
        llm_stage=llm_stage,
        priority_score=priority_score,
        route_event_types=tuple(route_event_types),
        reason_codes=tuple(reason_codes),
        require_document=require_document,
        require_llm=require_llm,
        max_chunks=max_chunks,
        max_chars_per_chunk=max_chars_per_chunk,
        title=str(row["title"]) if row.get("title") else None,
        effective_trade_date=_effective_date(row),
    )


def plan_document_review(
    row: Mapping[str, Any],
    *,
    context: Optional[Mapping[str, Any]] = None,
) -> DocumentReviewDecision:
    """Return a deterministic PDF/LLM review decision for one title row.

    ``context`` can contain structured signals such as ``financial_anomaly`` or
    ``amount_yuan``.  Missing context always downgrades to a safer candidate or
    skip decision rather than inventing evidence.
    """

    ctx = context or {}
    event_type = str(row.get("event_type") or "unclassified_archive")
    risk_level = str(row.get("risk_level") or "P4_NEUTRAL")
    needs_llm = str(row.get("needs_llm") or "").upper()
    priority = _priority_base(risk_level)

    if event_type == "unclassified_archive":
        return _decision(
            row,
            queue_action=QUEUE_SKIP,
            llm_stage=LLM_STAGE_NONE,
            priority_score=0,
            reason_codes=("unclassified_archive_for_rule_mining_not_auto_llm",),
        )

    if event_type in TITLE_ONLY_EVENT_TYPES:
        return _decision(
            row,
            queue_action=QUEUE_SKIP,
            llm_stage=LLM_STAGE_NONE,
            priority_score=0,
            reason_codes=("title_or_structured_data_sufficient",),
        )

    if event_type.startswith("financial_") or event_type in STRUCTURED_FINANCIAL_EVENT_TYPES:
        if _bool_context(ctx, "financial_anomaly") and event_type == "performance_forecast_revision_impairment":
            return _decision(
                row,
                queue_action=QUEUE_DOCUMENT_CANDIDATE,
                llm_stage=LLM_STAGE_SAMPLED,
                priority_score=priority + 8,
                route_event_types=("performance_forecast_revision_impairment",),
                reason_codes=("financial_structured_anomaly_linked", "sample_financial_text_reason"),
                max_chunks=6,
            )
        return _decision(
            row,
            queue_action=QUEUE_SKIP,
            llm_stage=LLM_STAGE_NONE,
            priority_score=0,
            reason_codes=("structured_financial_source_preferred",),
        )

    if event_type in FIRST_BATCH_REQUIRED_ROUTES:
        return _decision(
            row,
            queue_action=QUEUE_DOCUMENT_REQUIRED,
            llm_stage=LLM_STAGE_FIRST_BATCH,
            priority_score=priority + 15,
            route_event_types=FIRST_BATCH_REQUIRED_ROUTES[event_type],
            reason_codes=("first_batch_high_risk_text_required",),
            max_chunks=10,
        )

    if event_type in AMOUNT_SENSITIVE_ROUTES:
        material, amount_reasons = _amount_is_material(ctx)
        if material or _bool_context(ctx, "financial_anomaly"):
            reasons = amount_reasons + (("financial_anomaly_linked",) if _bool_context(ctx, "financial_anomaly") else ())
            return _decision(
                row,
                queue_action=QUEUE_DOCUMENT_REQUIRED,
                llm_stage=LLM_STAGE_FIRST_BATCH,
                priority_score=priority + 10,
                route_event_types=AMOUNT_SENSITIVE_ROUTES[event_type],
                reason_codes=("amount_sensitive_high_value",) + reasons,
                max_chunks=8,
            )
        return _decision(
            row,
            queue_action=QUEUE_DOCUMENT_CANDIDATE,
            llm_stage=LLM_STAGE_SAMPLED,
            priority_score=priority,
            route_event_types=AMOUNT_SENSITIVE_ROUTES[event_type],
            reason_codes=("amount_sensitive_needs_threshold",),
            max_chunks=6,
        )

    if event_type in CONTEXT_SENSITIVE_ROUTES:
        if _bool_context(ctx, "financial_anomaly") or _bool_context(ctx, "repeat_inquiry"):
            return _decision(
                row,
                queue_action=QUEUE_DOCUMENT_REQUIRED,
                llm_stage=LLM_STAGE_FIRST_BATCH,
                priority_score=priority + 8,
                route_event_types=CONTEXT_SENSITIVE_ROUTES[event_type],
                reason_codes=("context_linked_high_value_review",),
                max_chunks=8,
            )
        return _decision(
            row,
            queue_action=QUEUE_DOCUMENT_CANDIDATE,
            llm_stage=LLM_STAGE_SAMPLED,
            priority_score=priority - 5,
            route_event_types=CONTEXT_SENSITIVE_ROUTES[event_type],
            reason_codes=("context_sensitive_sample_first",),
            max_chunks=5,
        )

    if needs_llm == "YES":
        return _decision(
            row,
            queue_action=QUEUE_SAMPLE_ONLY,
            llm_stage=LLM_STAGE_LATER,
            priority_score=max(1, priority - 20),
            reason_codes=("title_rule_marked_llm_yes_but_not_first_batch",),
            max_chunks=4,
        )

    if needs_llm == "OPTIONAL":
        return _decision(
            row,
            queue_action=QUEUE_SAMPLE_ONLY,
            llm_stage=LLM_STAGE_SAMPLED,
            priority_score=max(1, priority - 25),
            reason_codes=("optional_llm_sample_only",),
            max_chunks=4,
        )

    return _decision(
        row,
        queue_action=QUEUE_SKIP,
        llm_stage=LLM_STAGE_NONE,
        priority_score=0,
        reason_codes=("no_document_review_rule",),
    )


def plan_review_batch(
    rows: Iterable[Mapping[str, Any]],
    *,
    context_by_ann_id: Optional[Mapping[int, Mapping[str, Any]]] = None,
    dedupe: bool = True,
    limit: Optional[int] = None,
) -> list[DocumentReviewDecision]:
    """Plan a sorted review queue for many classification rows."""

    decisions: list[DocumentReviewDecision] = []
    context_map = context_by_ann_id or {}
    for row in rows:
        ann_id = _ann_id(row)
        decision = plan_document_review(row, context=context_map.get(ann_id or -1, {}))
        decisions.append(decision)

    if dedupe:
        best_by_ann_id: dict[int, DocumentReviewDecision] = {}
        no_id: list[DocumentReviewDecision] = []
        for decision in decisions:
            if decision.ann_id is None:
                no_id.append(decision)
                continue
            current = best_by_ann_id.get(decision.ann_id)
            if current is None or decision.priority_score > current.priority_score:
                best_by_ann_id[decision.ann_id] = decision
        decisions = list(best_by_ann_id.values()) + no_id

    decisions.sort(
        key=lambda item: (
            -item.priority_score,
            item.effective_trade_date or dt.date.min,
            item.ts_code or "",
            item.ann_id or 0,
        )
    )
    return decisions[:limit] if limit is not None else decisions


def summarize_decisions(decisions: Iterable[DocumentReviewDecision]) -> dict[str, Any]:
    """Return compact counts for validation reports and future queue sizing."""

    by_action: dict[str, int] = {}
    by_stage: dict[str, int] = {}
    by_event_type: dict[str, int] = {}
    required = 0
    for decision in decisions:
        by_action[decision.queue_action] = by_action.get(decision.queue_action, 0) + 1
        by_stage[decision.llm_stage] = by_stage.get(decision.llm_stage, 0) + 1
        by_event_type[decision.event_type] = by_event_type.get(decision.event_type, 0) + 1
        if decision.require_document:
            required += 1
    return {
        "rows": sum(by_action.values()),
        "document_required_or_candidate": required,
        "by_action": dict(sorted(by_action.items())),
        "by_llm_stage": dict(sorted(by_stage.items())),
        "by_event_type": dict(sorted(by_event_type.items())),
    }
