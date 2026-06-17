"""Gated Skill Library support for Research Assistant Phase 12.

The library is intentionally deterministic and repository-backed: successful
task evidence becomes a draft recipe, human approval promotes that draft, and
reuse is routed through Action Proposal gates instead of direct execution.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Protocol

from .models import sha256_json, utc_now

SKILL_LIBRARY_RECIPE_SCHEMA = "aistock_research_assistant_skill_recipe_v1"
SKILL_LIBRARY_REPLAY_SCHEMA = "aistock_research_assistant_skill_replay_v1"
SKILL_LIBRARY_APPROVAL_TYPE = "skill_library.approve"
SKILL_LIBRARY_REUSE_CAPABILITY_KEY = "skill_library.reuse"
SKILL_LIBRARY_REUSE_CONFIRMATION = "CONFIRM_SKILL_REUSE"
SKILL_LIBRARY_APPROVAL_PREFIX = "APPROVE SKILL"

SUCCESS_EVENT_TYPES = {"mcp_done", "skill_done"}
SUCCESS_TASK_STATUSES = {"completed"}
_MAX_TEXT = 240


class SkillLibraryReplayProvider(Protocol):
    def find_reusable_skills(self, *, task_key: str, evidence_refs: list[str], limit: int) -> list[dict[str, Any]]:
        ...


class RepositorySkillLibraryExperienceReplayProvider:
    """Read-only L4 experience replay provider backed by assistant_skill_library."""

    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def find_reusable_skills(self, *, task_key: str, evidence_refs: list[str], limit: int) -> list[dict[str, Any]]:
        replay = search_approved_skill_recipes(
            self.repository,
            query=task_key,
            limit=limit,
            evidence_refs=evidence_refs,
        )
        if replay.get("status") == "degraded":
            return [
                {
                    "schema_version": SKILL_LIBRARY_REPLAY_SCHEMA,
                    "status": "degraded",
                    "reason_codes": list(replay.get("reason_codes") or []),
                    "warnings": list(replay.get("warnings") or []),
                    "source_refs": list(evidence_refs),
                }
            ]
        return list(replay.get("items") or [])


def normalize_skill_key(value: str) -> str:
    key = re.sub(r"[^A-Za-z0-9_.:-]+", "_", str(value or "").strip()).strip("_").lower()
    if not key:
        raise ValueError("skill_key is required")
    return key[:120]


def skill_library_plan_digest(*, skill_key: str, recipe_json: Mapping[str, Any], provenance_json: Mapping[str, Any]) -> str:
    return sha256_json(
        {
            "purpose": "skill_library_approval",
            "skill_key": skill_key,
            "recipe_json": dict(recipe_json),
            "provenance_json": dict(provenance_json),
        }
    )


def build_successful_workflow_recipe(
    repository: Any,
    *,
    task_id: str,
    skill_key: str | None = None,
    description: str | None = None,
    event_limit: int = 50,
) -> dict[str, Any]:
    """Build a reusable recipe only when task/event evidence proves success."""

    task = repository.get_record("tasks", task_id)
    if not task:
        return _degraded("skill_library_task_not_found", f"task not found: {task_id}")
    try:
        events_page = repository.list_records("task_events", filters={"task_id": task_id}, limit=max(1, event_limit), offset=0)
    except Exception as exc:  # explicit degradation: older schemas may lack task events.
        return _degraded("skill_library_event_collection_failed", f"task events unavailable: {type(exc).__name__}: {exc}")
    events = [dict(item) for item in events_page.get("items") or []]
    success_events = [event for event in events if _is_success_event(event)]
    if str(task.get("status") or "") not in SUCCESS_TASK_STATUSES and not success_events:
        return _degraded("skill_library_no_success_evidence", f"task has no completed status or success event: {task_id}")

    raw_key = skill_key or _skill_key_from_task(task)
    normalized_key = normalize_skill_key(raw_key)
    source_refs = _source_refs(task, events)
    workflow_steps = [_step_from_event(event) for event in events if _step_from_event(event)]
    tool_refs = _unique_dicts(_tool_refs_from_events(events))
    prompt_refs = _unique_strings(_prompt_refs_from_events(events))
    capability_keys = _unique_strings(_capability_keys_from_events(events))
    action_proposal_ids = _unique_strings(_action_proposal_ids_from_events(events))
    evidence_status = "ready" if source_refs else "degraded"
    reason_codes = [] if source_refs else ["skill_library_source_refs_empty"]
    warnings = [] if source_refs else ["skill recipe was built without source_refs; approval should reject until evidence is present"]
    recipe = {
        "schema_version": SKILL_LIBRARY_RECIPE_SCHEMA,
        "status": evidence_status,
        "skill_key": normalized_key,
        "description": description or _description_from_task(task),
        "task": {
            "task_id": str(task.get("task_id") or task_id),
            "task_type": str(task.get("task_type") or ""),
            "title": str(task.get("title") or ""),
            "status": str(task.get("status") or ""),
        },
        "workflow_steps": workflow_steps,
        "tool_refs": tool_refs,
        "prompt_refs": prompt_refs,
        "capability_keys": capability_keys,
        "action_proposal_ids": action_proposal_ids,
        "source_refs": source_refs,
        "risk_gate": {
            "skill_approval_required": True,
            "reuse_capability_key": SKILL_LIBRARY_REUSE_CAPABILITY_KEY,
            "reuse_confirmation": SKILL_LIBRARY_REUSE_CONFIRMATION,
            "action_proposal_required": True,
            "direct_execution_allowed": False,
        },
        "reason_codes": reason_codes,
        "warnings": warnings,
    }
    provenance = {
        "schema_version": "aistock_research_assistant_skill_provenance_v1",
        "source": "research_assistant_successful_workflow",
        "task_id": task_id,
        "source_refs": source_refs,
        "created_from_status": str(task.get("status") or ""),
        "generated_at": utc_now().isoformat(),
    }
    return {
        "status": evidence_status,
        "skill_key": normalized_key,
        "description": recipe["description"],
        "recipe_json": recipe,
        "provenance_json": provenance,
        "source_refs": source_refs,
        "reason_codes": reason_codes,
        "warnings": warnings,
    }


def search_approved_skill_recipes(
    repository: Any,
    *,
    query: str,
    limit: int = 5,
    evidence_refs: list[str] | None = None,
) -> dict[str, Any]:
    """Return approved recipes for L4 experience replay without side effects."""

    try:
        page = repository.list_records("skill_library", filters={"status": "approved"}, limit=max(1, limit), offset=0)
    except Exception as exc:  # explicit degradation: production DDL may still be pending.
        return {
            "schema_version": SKILL_LIBRARY_REPLAY_SCHEMA,
            "status": "degraded",
            "items": [],
            "source_refs": list(evidence_refs or []),
            "reason_codes": ["skill_library_replay_unavailable"],
            "warnings": [f"skill_library read failed: {type(exc).__name__}: {exc}"],
        }
    items = [dict(item) for item in page.get("items") or []]
    ranked = sorted(items, key=lambda item: _match_score(query, item), reverse=True)
    selected = [_replay_item(item, query=query) for item in ranked if _match_score(query, item) > 0]
    if not selected:
        return {
            "schema_version": SKILL_LIBRARY_REPLAY_SCHEMA,
            "status": "empty",
            "items": [],
            "source_refs": list(evidence_refs or []),
            "reason_codes": ["skill_library_no_approved_match"],
            "warnings": [f"no approved Skill Library recipe matched query={query!r}"],
        }
    return {
        "schema_version": SKILL_LIBRARY_REPLAY_SCHEMA,
        "status": "ready",
        "items": selected[: max(1, limit)],
        "source_refs": list(evidence_refs or []),
        "reason_codes": [],
        "warnings": [],
    }


def _degraded(reason_code: str, warning: str) -> dict[str, Any]:
    return {
        "status": "degraded",
        "skill_key": None,
        "description": "",
        "recipe_json": {},
        "provenance_json": {},
        "source_refs": [],
        "reason_codes": [reason_code],
        "warnings": [warning],
    }


def _skill_key_from_task(task: Mapping[str, Any]) -> str:
    task_type = str(task.get("task_type") or "workflow")
    title = str(task.get("title") or task.get("task_id") or "skill")
    digest = sha256_json({"task_id": task.get("task_id"), "title": title})[:10]
    return f"{task_type}.{title}.{digest}"


def _description_from_task(task: Mapping[str, Any]) -> str:
    title = _clip(task.get("title") or "successful workflow")
    task_type = _clip(task.get("task_type") or "workflow")
    return f"Reusable recipe from successful {task_type}: {title}"


def _is_success_event(event: Mapping[str, Any]) -> bool:
    if str(event.get("event_type") or "") in SUCCESS_EVENT_TYPES:
        return True
    payload = event.get("payload_json") if isinstance(event.get("payload_json"), Mapping) else {}
    return str(payload.get("status") or "").lower() in {"succeeded", "success", "completed"}


def _source_refs(task: Mapping[str, Any], events: list[dict[str, Any]]) -> list[str]:
    refs = [f"research_agent_tasks:{task.get('task_id')}"] if task.get("task_id") else []
    for event in events:
        if event.get("event_id"):
            refs.append(f"agent_task_events:{event['event_id']}")
        refs.extend(str(ref) for ref in event.get("evidence_refs") or [] if ref)
    return _unique_strings(refs)


def _step_from_event(event: Mapping[str, Any]) -> dict[str, Any] | None:
    event_type = str(event.get("event_type") or "")
    if not event_type:
        return None
    payload = event.get("payload_json") if isinstance(event.get("payload_json"), Mapping) else {}
    return {
        "event_id": str(event.get("event_id") or ""),
        "event_type": event_type,
        "message": _clip(event.get("message") or ""),
        "capability_key": str(payload.get("capability_key") or ""),
        "action_proposal_id": str(payload.get("action_proposal_id") or ""),
    }


def _tool_refs_from_events(events: list[dict[str, Any]]) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for event in events:
        payload = event.get("payload_json") if isinstance(event.get("payload_json"), Mapping) else {}
        for value in (payload, payload.get("selected_tool"), payload.get("mcp_tool"), payload.get("route")):
            if not isinstance(value, Mapping):
                continue
            server_key = str(value.get("server_key") or "")
            tool_name = str(value.get("tool_name") or "")
            if server_key or tool_name:
                refs.append({"server_key": server_key, "tool_name": tool_name})
    return refs


def _prompt_refs_from_events(events: list[dict[str, Any]]) -> list[str]:
    refs: list[str] = []
    for event in events:
        payload = event.get("payload_json") if isinstance(event.get("payload_json"), Mapping) else {}
        for key in ("prompt_key", "target_prompt_key", "prompt_bundle_id", "prompt_bundle_signature"):
            if payload.get(key):
                refs.append(f"{key}:{payload[key]}")
    return refs


def _capability_keys_from_events(events: list[dict[str, Any]]) -> list[str]:
    keys: list[str] = []
    for event in events:
        payload = event.get("payload_json") if isinstance(event.get("payload_json"), Mapping) else {}
        if payload.get("capability_key"):
            keys.append(str(payload["capability_key"]))
    return keys


def _action_proposal_ids_from_events(events: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    for event in events:
        payload = event.get("payload_json") if isinstance(event.get("payload_json"), Mapping) else {}
        if payload.get("action_proposal_id"):
            ids.append(str(payload["action_proposal_id"]))
    return ids


def _replay_item(item: Mapping[str, Any], *, query: str) -> dict[str, Any]:
    recipe = item.get("recipe_json") if isinstance(item.get("recipe_json"), Mapping) else {}
    provenance = item.get("provenance_json") if isinstance(item.get("provenance_json"), Mapping) else {}
    return {
        "schema_version": SKILL_LIBRARY_REPLAY_SCHEMA,
        "skill_id": str(item.get("skill_id") or ""),
        "skill_key": str(item.get("skill_key") or ""),
        "description": str(item.get("description") or ""),
        "status": str(item.get("status") or ""),
        "success_count": int(item.get("success_count") or 0),
        "match_score": _match_score(query, item),
        "recipe_ref": {
            "tool_refs": list(recipe.get("tool_refs") or []),
            "capability_keys": list(recipe.get("capability_keys") or []),
            "risk_gate": dict(recipe.get("risk_gate") or {}),
        },
        "source_refs": list(recipe.get("source_refs") or provenance.get("source_refs") or []),
        "reason_codes": [],
        "warnings": [],
    }


def _match_score(query: str, item: Mapping[str, Any]) -> int:
    query_tokens = _tokens(query)
    if not query_tokens:
        return 1
    recipe = item.get("recipe_json") if isinstance(item.get("recipe_json"), Mapping) else {}
    haystack = " ".join(
        [
            str(item.get("skill_key") or ""),
            str(item.get("description") or ""),
            str(recipe.get("capability_keys") or ""),
            str(recipe.get("tool_refs") or ""),
        ]
    )
    hay_tokens = _tokens(haystack)
    return len(query_tokens & hay_tokens)


def _tokens(text: str) -> set[str]:
    return {token for token in re.split(r"[^A-Za-z0-9_\u4e00-\u9fff]+", str(text).lower()) if token}


def _unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _unique_dicts(values: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    result: list[dict[str, str]] = []
    for value in values:
        key = (str(value.get("server_key") or ""), str(value.get("tool_name") or ""))
        if key in seen:
            continue
        seen.add(key)
        result.append({"server_key": key[0], "tool_name": key[1]})
    return result


def _clip(value: Any) -> str:
    text = " ".join(str(value or "").split())
    return text[:_MAX_TEXT]
