"""Deterministic Reflection Card artifacts for Research Assistant self-learning."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from .models import sha256_json

REFLECTION_CARD_SCHEMA = "aistock_research_assistant_reflection_card_v1"
REFLECTION_MEMORY_SCHEMA = "aistock_research_assistant_reflection_memory_v1"
REFLECTION_TRIGGERS = {"failure", "correction", "low_confidence"}

_FAILURE_EVENTS = {"mcp_failed", "skill_failed", "llm_failed", "mcp_execution_timeout"}
_CORRECTION_EVENTS = {"triage_required"}
_CORRECTION_TERMS = ("correction", "corrected", "revise", "revision", "\u7ea0\u504f", "\u4fee\u6b63")
_LOW_CONFIDENCE_THRESHOLD = 0.5
_PROHIBITED_EXTERNAL_TERMS = ("chain of thought", "reasoning chain", "\u601d\u7ef4\u94fe", "\u63a8\u7406\u94fe")


def reflection_trigger_from_event(event_type: str, *, message: str, payload_json: Mapping[str, Any] | None) -> str | None:
    """Resolve the self-learning trigger from an existing task-event payload."""

    payload = dict(payload_json or {})
    if event_type in _FAILURE_EVENTS:
        return "failure"
    if event_type in _CORRECTION_EVENTS or any(term in message.lower() for term in _CORRECTION_TERMS):
        return "correction"
    if payload.get("low_confidence") is True:
        return "low_confidence"
    confidence = _float(payload.get("confidence"))
    if confidence is not None and confidence < _LOW_CONFIDENCE_THRESHOLD:
        return "low_confidence"
    return None


def build_reflection_artifacts(
    *,
    task: Mapping[str, Any],
    trigger: str,
    source_event: Mapping[str, Any] | None,
    card_id: str,
    memory_id: str,
    created_at: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Build DB rows for a Reflection Card and its personal.episodic memory."""

    if trigger not in REFLECTION_TRIGGERS:
        raise ValueError(f"reflection trigger must be one of {sorted(REFLECTION_TRIGGERS)}")
    task_id = str(task.get("task_id") or "")
    if not task_id:
        raise ValueError("reflection card requires task_id")
    created_at = created_at or _utc_now()
    event = dict(source_event or {})
    event_id = str(event.get("event_id") or "manual")
    source_refs = _source_refs(task_id=task_id, event_id=event_id)
    observed = _sanitize_external_text(_observed_signal(task, event))
    cause = _cause_summary(trigger, observed)
    lesson = _lesson_for_trigger(trigger)
    next_strategy = _next_strategy_for_trigger(trigger)
    lesson_md = _sanitize_external_text(
        "\n".join(
            [
                "## Reflection Card",
                f"- Trigger: {trigger}",
                f"- Observed signal: {observed}",
                f"- Likely cause: {cause}",
                f"- Lesson: {lesson}",
                f"- Next strategy: {next_strategy}",
            ]
        )
    )
    structured_json = {
        "schema_version": REFLECTION_CARD_SCHEMA,
        "trigger": trigger,
        "task_id": task_id,
        "source_event_id": event_id if event_id != "manual" else None,
        "observed_signal": observed,
        "cause_summary": cause,
        "lesson": lesson,
        "next_strategy": next_strategy,
        "source_refs": source_refs,
        "reason_codes": [f"reflection_card_{trigger}"],
        "warnings": [],
        "safety": {
            "external_reasoning_hidden": True,
            "prompt_or_strategy_changed": False,
            "action_proposals_created": False,
            "next_strategy_is_proposal_only": True,
        },
    }
    memory_row = {
        "memory_id": memory_id,
        "memory_type": "episodic",
        "namespace": str(task.get("namespace") or "aistock"),
        "subject_key": f"personal.episodic.reflection.{task_id}",
        "title": f"Reflection for {task.get('title') or task_id}",
        "content_json": {"schema_version": REFLECTION_MEMORY_SCHEMA, "reflection_card_id": card_id, **structured_json},
        "content_text": lesson_md,
        "source_type": "reflection_card",
        "source_ref": f"reflection_card:{card_id}",
        "confidence": 0.74,
        "approval_status": "approved",
        "risk_level": "low",
        "evidence_refs": source_refs,
        "checksum": sha256_json({"card_id": card_id, "lesson_md": lesson_md, "source_refs": source_refs}),
        "created_by": "assistant_reflection",
        "tree_path": f"personal.episodic.reflection.{task_id}",
        "parent_key": "personal.episodic.reflection",
        "node_type": "fact",
        "scope": "personal",
        "importance": 0.72,
        "auto_created": True,
        "trust_level": "assistant_inferred",
        "provenance_json": {"source": "reflection_card", "card_id": card_id, "task_id": task_id, "event_id": event_id, "captured_at": created_at},
        "resident": False,
        "created_at": created_at,
    }
    card_row = {
        "card_id": card_id,
        "task_id": task_id,
        "trigger": trigger,
        "lesson_md": lesson_md,
        "structured_json": structured_json,
        "memory_ref": memory_id,
        "created_at": created_at,
    }
    return {"card": card_row, "memory": memory_row}


def _source_refs(*, task_id: str, event_id: str) -> list[str]:
    refs = [f"research_agent_tasks:{task_id}"]
    if event_id and event_id != "manual":
        refs.append(f"agent_task_events:{event_id}")
    return refs


def _observed_signal(task: Mapping[str, Any], event: Mapping[str, Any]) -> str:
    parts = [
        f"task_status={task.get('status') or 'unknown'}",
        f"event_type={event.get('event_type') or 'manual'}",
    ]
    message = str(event.get("message") or "").strip()
    if message:
        parts.append(f"message={message[:160]}")
    return "; ".join(parts)


def _cause_summary(trigger: str, observed: str) -> str:
    if trigger == "failure":
        return f"The task hit a failure signal before producing a verified result: {observed}"
    if trigger == "correction":
        return f"The task required a correction or triage step, so the previous path was not sufficient: {observed}"
    return f"The task confidence was below the safe execution threshold: {observed}"


def _lesson_for_trigger(trigger: str) -> str:
    if trigger == "failure":
        return "Surface the failing evidence source and preserve explicit reason codes before retrying."
    if trigger == "correction":
        return "Capture the corrected assumption and validate it against source refs before continuing."
    return "Ask for narrower scope or additional evidence before presenting a confident answer."


def _next_strategy_for_trigger(trigger: str) -> str:
    if trigger == "failure":
        return "Next time, isolate the failing step, verify the smallest read-only evidence path, then retry once."
    if trigger == "correction":
        return "Next time, compare the initial assumption with the correction and update the task plan first."
    return "Next time, keep the response in analysis mode and request confirmation when evidence is thin."


def _sanitize_external_text(text: str) -> str:
    cleaned = text
    for token in _PROHIBITED_EXTERNAL_TERMS:
        cleaned = re.sub(re.escape(token), "internal rationale", cleaned, flags=re.IGNORECASE)
    return cleaned


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
