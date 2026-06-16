"""Offline Prompt Lab candidate generation and judging.

The module intentionally keeps Phase 11 deterministic and offline: it reads
historical trace rows, generates a GEPA/DSPy-style candidate prompt addendum,
and scores that candidate through an injected offline judge contract.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Protocol

from .models import sha256_json

PROMPT_LAB_RUN_SCHEMA = "aistock_research_assistant_prompt_lab_run_v1"
PROMPT_LAB_JUDGE_SCHEMA = "aistock_research_assistant_prompt_lab_judge_v1"
PROMPT_LAB_EVAL_SET_SCHEMA = "aistock_research_assistant_prompt_lab_eval_set_v1"
PROMPT_LAB_OPTIMIZERS = {"gepa", "dspy_mipro", "manual"}

_MAX_TRACE_TEXT_CHARS = 220
_PROMPT_LAB_REASON_HINTS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("source", "evidence", "hallucinat", "ground"), "Require explicit source_refs before stating factual conclusions."),
    (("approval", "activation", "gate", "production"), "Keep activation changes behind an approval gate and report the gate state."),
    (("tool", "mcp", "route", "intent"), "Resolve semantic intent first, then select the narrowest read-only tool path."),
    (("fallback", "silent", "missing", "empty"), "When evidence is missing, degrade with reason_codes and warnings instead of inventing data."),
)


class OfflinePromptJudge(Protocol):
    """Offline judge interface used by Prompt Lab tests and future adapters."""

    def evaluate(
        self,
        *,
        target_prompt_key: str,
        baseline_text: str,
        candidate_text: str,
        eval_items: list[dict[str, Any]],
        source_refs: list[str],
    ) -> Mapping[str, Any]:
        ...


class DeterministicOfflinePromptJudge:
    """Local, side-effect-free judge used when no external offline judge is injected."""

    def evaluate(
        self,
        *,
        target_prompt_key: str,
        baseline_text: str,
        candidate_text: str,
        eval_items: list[dict[str, Any]],
        source_refs: list[str],
    ) -> Mapping[str, Any]:
        evidence_score = 1.0 if "source_refs" in candidate_text or "evidence" in candidate_text.lower() else 0.55
        gate_score = 1.0 if "approval" in candidate_text.lower() and "activation" in candidate_text.lower() else 0.6
        trace_score = min(1.0, 0.55 + 0.15 * len(eval_items))
        specificity_score = 0.9 if _distinct_lessons(eval_items) else 0.65
        score = round((evidence_score + gate_score + trace_score + specificity_score) / 4, 4)
        return {
            "schema_version": PROMPT_LAB_JUDGE_SCHEMA,
            "judge": "deterministic_offline_judge",
            "target_prompt_key": target_prompt_key,
            "status": "scored",
            "score": score,
            "dimensions": {
                "evidence_grounding": evidence_score,
                "approval_gate": gate_score,
                "trace_coverage": trace_score,
                "specificity": specificity_score,
            },
            "source_refs": list(source_refs),
            "reason_codes": ["prompt_lab_offline_judge_deterministic"],
            "warnings": [] if eval_items else ["prompt_lab_offline_judge_used_empty_eval_set"],
            "evaluated_at": _utc_now(),
        }


def collect_prompt_lab_eval_set(
    repository: Any,
    *,
    target_prompt_key: str,
    limit: int = 20,
) -> dict[str, Any]:
    """Collect prompt-related historical traces without calling production systems."""

    reason_codes: list[str] = []
    warnings: list[str] = []
    trace_items: list[dict[str, Any]] = []
    source_refs: list[str] = []
    try:
        page = repository.list_records("trace_events", limit=max(1, limit), offset=0)
    except Exception as exc:  # explicit degradation: repository kind/table can be unavailable in older schemas.
        reason_codes.append("prompt_lab_trace_collection_failed")
        warnings.append(f"trace_events unavailable for Prompt Lab eval set: {type(exc).__name__}: {exc}")
        page = {"items": []}
    for raw in page.get("items") or []:
        item = _trace_item_from_record(raw, target_prompt_key=target_prompt_key)
        if not item:
            continue
        trace_items.append(item)
        source_refs.append(item["source_ref"])
    if not trace_items:
        reason_codes.append("prompt_lab_eval_set_empty")
        warnings.append(f"no historical trace rows matched target_prompt_key={target_prompt_key}")
    eval_set_ref = _eval_set_ref(target_prompt_key, trace_items)
    return {
        "schema_version": PROMPT_LAB_EVAL_SET_SCHEMA,
        "target_prompt_key": target_prompt_key,
        "eval_set_ref": eval_set_ref,
        "items": trace_items,
        "source_refs": source_refs,
        "reason_codes": reason_codes,
        "warnings": warnings,
    }


def build_prompt_lab_candidate(
    *,
    target_prompt_key: str,
    baseline_text: str,
    optimizer: str,
    eval_set: Mapping[str, Any],
) -> dict[str, Any]:
    """Generate a Prompt Lab candidate from trace-derived lessons."""

    if optimizer not in PROMPT_LAB_OPTIMIZERS:
        raise ValueError(f"optimizer must be one of {sorted(PROMPT_LAB_OPTIMIZERS)}")
    if not target_prompt_key.strip():
        raise ValueError("target_prompt_key is required")
    if not baseline_text.strip():
        raise ValueError("baseline_text is required")
    items = [dict(item) for item in eval_set.get("items") or []]
    lessons = _lessons_from_traces(items)
    if not lessons:
        lessons = [
            "Preserve evidence-first answers and attach source_refs to factual claims.",
            "If the eval set is empty or unavailable, return explicit reason_codes and warnings.",
            "Never change prompt activation from Prompt Lab output without a human approval gate.",
        ]
    trace_summary = _trace_summary(items)
    candidate_text = "\n".join(
        [
            baseline_text.rstrip(),
            "",
            "## Prompt Lab Candidate Addendum",
            f"- Target prompt key: {target_prompt_key}",
            f"- Optimizer: {optimizer}",
            f"- Eval set: {eval_set.get('eval_set_ref')}",
            f"- Trace summary: {trace_summary}",
            "- Required behavior:",
            *[f"  - {lesson}" for lesson in lessons],
            "  - Preserve the approval gate: Prompt Lab may only create candidate text, and prompt activation changes require human approval.",
        ]
    ).strip()
    return {
        "schema_version": PROMPT_LAB_RUN_SCHEMA,
        "target_prompt_key": target_prompt_key,
        "optimizer": optimizer,
        "eval_set_ref": str(eval_set.get("eval_set_ref") or _eval_set_ref(target_prompt_key, items)),
        "candidate_text": candidate_text,
        "trace_count": len(items),
        "source_refs": list(eval_set.get("source_refs") or []),
        "reason_codes": list(eval_set.get("reason_codes") or []),
        "warnings": list(eval_set.get("warnings") or []),
    }


def judge_prompt_lab_candidate(
    *,
    judge: OfflinePromptJudge | None,
    target_prompt_key: str,
    baseline_text: str,
    candidate_text: str,
    eval_items: list[dict[str, Any]],
    source_refs: list[str],
) -> dict[str, Any]:
    """Run the offline judge with explicit degradation on adapter failure."""

    active_judge = judge or DeterministicOfflinePromptJudge()
    try:
        raw = active_judge.evaluate(
            target_prompt_key=target_prompt_key,
            baseline_text=baseline_text,
            candidate_text=candidate_text,
            eval_items=eval_items,
            source_refs=source_refs,
        )
    except Exception as exc:  # explicit degradation: the candidate remains auditable.
        return {
            "schema_version": PROMPT_LAB_JUDGE_SCHEMA,
            "judge": type(active_judge).__name__,
            "target_prompt_key": target_prompt_key,
            "status": "degraded",
            "score": 0.0,
            "dimensions": {},
            "source_refs": list(source_refs),
            "reason_codes": ["prompt_lab_offline_judge_failed"],
            "warnings": [f"offline judge failed: {type(exc).__name__}: {exc}"],
            "evaluated_at": _utc_now(),
        }
    score = dict(raw)
    score.setdefault("schema_version", PROMPT_LAB_JUDGE_SCHEMA)
    score.setdefault("target_prompt_key", target_prompt_key)
    score.setdefault("status", "scored")
    score.setdefault("source_refs", list(source_refs))
    score.setdefault("reason_codes", [])
    score.setdefault("warnings", [])
    score.setdefault("evaluated_at", _utc_now())
    return score


def prompt_lab_plan_digest(*, target_prompt_key: str, candidate_text: str, eval_set_ref: str) -> str:
    return sha256_json(
        {
            "purpose": "prompt_lab_candidate_activation",
            "target_prompt_key": target_prompt_key,
            "candidate_text": candidate_text,
            "eval_set_ref": eval_set_ref,
        }
    )


def _trace_item_from_record(record: Mapping[str, Any], *, target_prompt_key: str) -> dict[str, Any] | None:
    payload = dict(record.get("payload_json") or {})
    prompt_key = str(payload.get("target_prompt_key") or payload.get("prompt_key") or "")
    component = str(record.get("component") or "")
    if prompt_key and prompt_key != target_prompt_key:
        return None
    if not prompt_key and "prompt" not in component.lower():
        return None
    trace_id = str(record.get("trace_id") or "")
    if not trace_id:
        return None
    return {
        "trace_id": trace_id,
        "source_ref": f"assistant_trace_events:{trace_id}",
        "status": str(record.get("status") or "unknown"),
        "component": component,
        "event_type": str(record.get("event_type") or "unknown"),
        "target_prompt_key": prompt_key or target_prompt_key,
        "input": _clip(payload.get("input") or payload.get("user_message") or payload.get("query") or ""),
        "failure_mode": _clip(payload.get("failure_mode") or payload.get("reason_code") or payload.get("error") or ""),
        "judge_feedback": _clip(payload.get("judge_feedback") or payload.get("feedback") or payload.get("lesson") or ""),
    }


def _lessons_from_traces(items: list[dict[str, Any]]) -> list[str]:
    text = " ".join(
        str(item.get(field) or "").lower()
        for item in items
        for field in ("status", "component", "event_type", "input", "failure_mode", "judge_feedback")
    )
    lessons: list[str] = []
    for needles, lesson in _PROMPT_LAB_REASON_HINTS:
        if any(needle in text for needle in needles):
            lessons.append(lesson)
    if items and not lessons:
        lessons.append("Summarize the user's actual intent before selecting tools or composing final answers.")
    return lessons[:6]


def _trace_summary(items: list[dict[str, Any]]) -> str:
    if not items:
        return "0 historical traces; candidate remains gated and explicitly degraded."
    statuses: dict[str, int] = {}
    for item in items:
        status = str(item.get("status") or "unknown")
        statuses[status] = statuses.get(status, 0) + 1
    status_text = ", ".join(f"{key}={value}" for key, value in sorted(statuses.items()))
    return f"{len(items)} historical traces ({status_text})"


def _distinct_lessons(items: list[dict[str, Any]]) -> bool:
    return len(_lessons_from_traces(items)) >= 2


def _clip(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:_MAX_TRACE_TEXT_CHARS]


def _eval_set_ref(target_prompt_key: str, items: list[dict[str, Any]]) -> str:
    safe_key = re.sub(r"[^A-Za-z0-9_.-]+", "_", target_prompt_key).strip("_") or "prompt"
    digest = sha256_json({"target_prompt_key": target_prompt_key, "trace_ids": [item.get("trace_id") for item in items]})[:16]
    return f"prompt_lab_eval_set_{safe_key}_{digest}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
