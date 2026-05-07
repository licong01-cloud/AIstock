"""Refine future announcement document-review queue candidates.

The base planner answers whether an event type is interesting.  This module
adds title-level materiality, conservative downgrades, and stable dedupe keys so
the first real PDF/LLM wave does not try to process every broad P2 title.
It is deterministic and side-effect free.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Optional

from backend.services.event_signal.document_review_planner import (
    AMOUNT_SENSITIVE_ROUTES,
    CONTEXT_SENSITIVE_ROUTES,
    HIGH_AMOUNT_RATIO,
    HIGH_AMOUNT_YUAN,
    LLM_STAGE_FIRST_BATCH,
    LLM_STAGE_NONE,
    LLM_STAGE_SAMPLED,
    QUEUE_DOCUMENT_CANDIDATE,
    QUEUE_DOCUMENT_REQUIRED,
    QUEUE_SAMPLE_ONLY,
    QUEUE_SKIP,
    DocumentReviewDecision,
    plan_document_review,
)


REFINED_DEFER_MATERIALITY = "defer_until_materiality"
REFINED_DEDUPED = "deduped"

AMOUNT_PATTERN = re.compile(
    r"(?P<num>\d+(?:,\d{3})*(?:\.\d+)?)\s*(?P<unit>亿元|亿人民币|亿|万元|人民币万元|元)"
)
PERCENT_PATTERN = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*%")


@dataclass(frozen=True)
class MaterialityContext:
    max_amount_yuan: Optional[Decimal]
    max_percent: Optional[Decimal]
    amount_count: int
    percent_count: int
    material_amount: bool
    material_percent: bool

    @property
    def is_material(self) -> bool:
        return self.material_amount or self.material_percent

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in ("max_amount_yuan", "max_percent"):
            if payload[key] is not None:
                payload[key] = str(payload[key])
        return payload


@dataclass(frozen=True)
class RefinedDocumentDecision:
    base: DocumentReviewDecision
    refined_action: str
    refined_llm_stage: str
    refined_priority_score: int
    require_document: bool
    require_llm: bool
    materiality: MaterialityContext
    dedupe_key: str
    reason_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "base": self.base.to_dict(),
            "refined_action": self.refined_action,
            "refined_llm_stage": self.refined_llm_stage,
            "refined_priority_score": self.refined_priority_score,
            "require_document": self.require_document,
            "require_llm": self.require_llm,
            "materiality": self.materiality.to_dict(),
            "dedupe_key": self.dedupe_key,
            "reason_codes": self.reason_codes,
        }


def _decimal(value: Any) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _amount_to_yuan(number: Decimal, unit: str) -> Decimal:
    if unit in {"亿元", "亿人民币", "亿"}:
        return number * Decimal("100000000")
    if unit in {"万元", "人民币万元"}:
        return number * Decimal("10000")
    return number


def extract_amounts_yuan(text: str) -> tuple[Decimal, ...]:
    amounts: list[Decimal] = []
    for match in AMOUNT_PATTERN.finditer(text or ""):
        number = _decimal(match.group("num"))
        if number is None:
            continue
        amounts.append(_amount_to_yuan(number, match.group("unit")))
    return tuple(amounts)


def extract_percentages(text: str) -> tuple[Decimal, ...]:
    values: list[Decimal] = []
    for match in PERCENT_PATTERN.finditer(text or ""):
        number = _decimal(match.group("num"))
        if number is not None:
            values.append(number / Decimal("100"))
    return tuple(values)


def build_materiality_context(
    text: str,
    *,
    context: Optional[Mapping[str, Any]] = None,
    amount_threshold_yuan: Decimal = HIGH_AMOUNT_YUAN,
    percent_threshold: Decimal = HIGH_AMOUNT_RATIO,
) -> MaterialityContext:
    ctx = context or {}
    amounts = list(extract_amounts_yuan(text))
    explicit_amount = _decimal(ctx.get("amount_yuan") or ctx.get("case_amount_yuan"))
    if explicit_amount is not None:
        amounts.append(explicit_amount)
    percents = list(extract_percentages(text))
    explicit_percent = _decimal(ctx.get("amount_to_market_cap") or ctx.get("amount_to_assets"))
    if explicit_percent is not None:
        percents.append(explicit_percent)
    max_amount = max(amounts) if amounts else None
    max_percent = max(percents) if percents else None
    return MaterialityContext(
        max_amount_yuan=max_amount,
        max_percent=max_percent,
        amount_count=len(amounts),
        percent_count=len(percents),
        material_amount=bool(max_amount is not None and max_amount >= amount_threshold_yuan),
        material_percent=bool(max_percent is not None and max_percent >= percent_threshold),
    )


def normalize_title_signature(title: str) -> str:
    text = re.sub(r"\s+", "", title or "")
    text = re.sub(r"20\d{2}年\d{1,2}月\d{1,2}日", "", text)
    text = re.sub(r"20\d{6}", "", text)
    text = re.sub(r"\d+(?:,\d{3})*(?:\.\d+)?", "#", text)
    text = re.sub(r"第[一二三四五六七八九十\d]+次", "第#次", text)
    text = re.sub(r"(公告|进展公告|提示性公告|专项公告)$", "", text)
    return text[:120] or "empty_title"


def _period_bucket(value: Optional[dt.date], *, window_days: int) -> int:
    if value is None:
        return 0
    return value.toordinal() // max(1, window_days)


def build_dedupe_key(
    row: Mapping[str, Any],
    decision: DocumentReviewDecision,
    *,
    window_days: int = 30,
) -> str:
    effective = decision.effective_trade_date
    if effective is None and row.get("ann_date") is not None:
        ann_date = row.get("ann_date")
        effective = ann_date if isinstance(ann_date, dt.date) else dt.date.fromisoformat(str(ann_date))
    title_signature = normalize_title_signature(str(row.get("title") or decision.title or ""))
    payload = "|".join(
        [
            decision.ts_code or "",
            decision.event_type,
            str(_period_bucket(effective, window_days=window_days)),
            title_signature,
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def refine_document_review_decision(
    row: Mapping[str, Any],
    *,
    context: Optional[Mapping[str, Any]] = None,
    window_days: int = 30,
) -> RefinedDocumentDecision:
    """Apply materiality and first-wave queue rules to one base decision."""

    base = plan_document_review(row, context=context)
    title = str(row.get("title") or base.title or "")
    materiality = build_materiality_context(title, context=context)
    reasons = list(base.reason_codes)
    refined_action = base.queue_action
    refined_stage = base.llm_stage
    priority = base.priority_score

    if base.queue_action == QUEUE_DOCUMENT_CANDIDATE and base.event_type in AMOUNT_SENSITIVE_ROUTES:
        if materiality.is_material:
            refined_action = QUEUE_DOCUMENT_REQUIRED
            refined_stage = LLM_STAGE_FIRST_BATCH
            priority += 12
            reasons.append("materiality_upgrade_to_first_batch")
        else:
            refined_action = REFINED_DEFER_MATERIALITY
            refined_stage = LLM_STAGE_NONE
            priority = max(0, priority - 45)
            reasons.append("no_materiality_evidence_defer_download")

    elif base.queue_action == QUEUE_DOCUMENT_CANDIDATE and base.event_type in CONTEXT_SENSITIVE_ROUTES:
        refined_action = QUEUE_SAMPLE_ONLY
        refined_stage = LLM_STAGE_SAMPLED
        priority = max(1, priority - 25)
        reasons.append("context_sensitive_sample_before_download")

    elif base.queue_action == QUEUE_SAMPLE_ONLY:
        refined_stage = LLM_STAGE_SAMPLED if base.require_llm else LLM_STAGE_NONE
        priority = min(priority, 10)
        reasons.append("sample_only_not_download_queue")

    require_document = refined_action in {QUEUE_DOCUMENT_REQUIRED, QUEUE_DOCUMENT_CANDIDATE}
    require_llm = refined_stage in {LLM_STAGE_FIRST_BATCH, LLM_STAGE_SAMPLED}
    return RefinedDocumentDecision(
        base=base,
        refined_action=refined_action,
        refined_llm_stage=refined_stage,
        refined_priority_score=priority,
        require_document=require_document,
        require_llm=require_llm,
        materiality=materiality,
        dedupe_key=build_dedupe_key(row, base, window_days=window_days),
        reason_codes=tuple(dict.fromkeys(reasons)),
    )


def summarize_refined_decisions(decisions: Iterable[RefinedDocumentDecision]) -> dict[str, Any]:
    by_action: dict[str, int] = {}
    by_stage: dict[str, int] = {}
    by_event_type: dict[str, int] = {}
    document_rows = 0
    llm_rows = 0
    material_rows = 0
    rows = 0
    for decision in decisions:
        rows += 1
        by_action[decision.refined_action] = by_action.get(decision.refined_action, 0) + 1
        by_stage[decision.refined_llm_stage] = by_stage.get(decision.refined_llm_stage, 0) + 1
        by_event_type[decision.base.event_type] = by_event_type.get(decision.base.event_type, 0) + 1
        if decision.require_document:
            document_rows += 1
        if decision.require_llm:
            llm_rows += 1
        if decision.materiality.is_material:
            material_rows += 1
    return {
        "rows": rows,
        "document_rows": document_rows,
        "llm_rows": llm_rows,
        "material_rows": material_rows,
        "by_action": dict(sorted(by_action.items())),
        "by_llm_stage": dict(sorted(by_stage.items())),
        "by_event_type": dict(sorted(by_event_type.items())),
    }


def dedupe_refined_decisions(decisions: Iterable[RefinedDocumentDecision]) -> list[RefinedDocumentDecision]:
    best_by_key: dict[str, RefinedDocumentDecision] = {}
    for decision in decisions:
        current = best_by_key.get(decision.dedupe_key)
        if current is None or decision.refined_priority_score > current.refined_priority_score:
            best_by_key[decision.dedupe_key] = decision
    return sorted(
        best_by_key.values(),
        key=lambda item: (
            -item.refined_priority_score,
            item.base.effective_trade_date or dt.date.min,
            item.base.ts_code or "",
            item.base.ann_id or 0,
        ),
    )
