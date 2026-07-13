
"""Repositories for Research Assistant Console state.

The production repository is PostgreSQL only. Tests may inject the in-memory
repository explicitly; the service never silently falls back from database to
memory.
"""

from __future__ import annotations

import copy
from collections import Counter
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from psycopg2 import errors
from psycopg2.extras import Json

from backend.db.pg_pool import get_conn
from backend.services.qe_archive.models import canonical_json_dumps, normalize_json

ConnectionProvider = Callable[[], Any]


TABLES: dict[str, dict[str, Any]] = {
    "tasks": {
        "table": "research_agent_tasks",
        "id": "task_id",
        "json": {"input_json", "result_json", "triage_json"},
        "search": {"task_id", "title", "task_type", "status", "created_by", "idempotency_key"},
    },
    "task_events": {
        "table": "agent_task_events",
        "id": "event_id",
        "json": {"payload_json", "evidence_refs"},
        "search": {"event_type", "message", "severity"},
    },
    "conversations": {
        "table": "assistant_conversations",
        "id": "conversation_id",
        "json": {"metadata_json"},
        "search": {"conversation_id", "title", "user_id", "status"},
    },
    "conversation_messages": {
        "table": "assistant_conversation_messages",
        "id": "message_id",
        "json": {"content_json"},
        "search": {"message_id", "conversation_id", "role", "content_text", "task_id"},
    },
    "memory_items": {
        "table": "research_memory_items",
        "id": "memory_id",
        "json": {"content_json", "evidence_refs", "provenance_json"},
        "search": {"memory_id", "memory_type", "namespace", "subject_key", "title", "content_text", "approval_status", "tree_path", "parent_key", "scope"},
    },
    "memory_access_log": {
        "table": "research_memory_access_log",
        "id": "access_id",
        "json": {"retrieval_reason", "payload_json"},
        "search": {"memory_id", "task_id", "agent_id"},
    },
    "context_packs": {
        "table": "assistant_context_packs",
        "id": "context_pack_id",
        "json": {
            "core_memory_refs", "procedural_memory_refs", "architecture_memory_refs", "task_state_refs",
            "experiment_memory_refs", "graph_relation_refs", "external_source_refs", "temp_memory_refs",
            "omitted_relevant_refs", "pack_json",
        },
        "search": {"context_pack_id", "task_id", "agent_id", "model_profile", "pack_summary"},
    },
    "code_context_refs": {
        "table": "assistant_code_context_refs",
        "id": "code_ref_id",
        "json": {"manifest_json", "provenance_json"},
        "search": {"code_ref_id", "task_id", "query_scope", "source"},
        "no_updated_at": True,
    },
    "proactive_reports": {
        "table": "assistant_proactive_reports",
        "id": "report_id",
        "json": {"sections_json", "source_refs_json"},
        "search": {"report_id", "report_type", "report_date", "status"},
        "no_updated_at": True,
    },
    "reflection_cards": {
        "table": "assistant_reflection_cards",
        "id": "card_id",
        "json": {"structured_json"},
        "search": {"card_id", "task_id", "trigger", "memory_ref"},
        "no_updated_at": True,
    },
    "prompt_lab_runs": {
        "table": "assistant_prompt_lab_runs",
        "id": "lab_run_id",
        "json": {"judge_score_json"},
        "search": {"lab_run_id", "target_prompt_key", "optimizer", "eval_set_ref", "status", "approval_request_id"},
        "no_updated_at": True,
    },
    "skill_library": {
        "table": "assistant_skill_library",
        "id": "skill_id",
        "json": {"recipe_json", "provenance_json"},
        "search": {"skill_id", "skill_key", "description", "status"},
        "no_updated_at": True,
    },
    "entities": {
        "table": "research_memory_entities",
        "id": "entity_id",
        "json": {"source_refs"},
        "search": {"entity_id", "entity_type", "entity_key", "title", "summary", "namespace"},
    },
    "relations": {
        "table": "research_memory_relations",
        "id": "relation_id",
        "json": {"evidence_refs"},
        "search": {"relation_id", "relation_type", "source_entity_id", "target_entity_id"},
    },
    "evolution_paths": {
        "table": "research_evolution_paths",
        "id": "path_id",
        "json": {"rejected_entities_json", "next_candidate_entities_json", "supporting_paper_refs", "evidence_refs"},
        "search": {"path_id", "stream_id", "objective", "decision_notes"},
    },
    "skills": {
        "table": "assistant_skill_registry",
        "id": "skill_id",
        "json": {"input_schema_json", "output_schema_json", "required_mcp_tools", "tags_json"},
        "search": {"skill_id", "skill_key", "title", "description", "domain", "status"},
    },
    "skill_events": {
        "table": "assistant_skill_usage_events",
        "id": "skill_event_id",
        "json": {"input_summary_json", "output_summary_json", "evidence_refs"},
        "search": {"skill_event_id", "skill_key", "task_id", "status"},
    },
    "mcp_servers": {
        "table": "assistant_mcp_servers",
        "id": "server_id",
        "json": {"health_json"},
        "search": {"server_id", "server_key", "title", "status"},
    },
    "capabilities": {
        "table": "assistant_capabilities",
        "id": "capability_id",
        "json": {"natural_language_triggers", "required_confirmations", "input_slots", "output_cards", "mcp_tool_refs", "skill_refs"},
        "search": {"capability_id", "capability_key", "capability_type", "title", "description_for_llm", "risk_level", "side_effect_level", "status"},
    },
    "action_proposals": {
        "table": "assistant_action_proposals",
        "id": "action_proposal_id",
        "json": {"input_json", "expected_result_json"},
        "search": {"action_proposal_id", "task_id", "conversation_id", "capability_key", "proposal_type", "title", "summary", "risk_level", "side_effect_level", "status", "approval_id", "idempotency_key"},
    },
    "mcp_tools": {
        "table": "assistant_mcp_tools",
        "id": "tool_id",
        "json": {"input_schema_json", "output_schema_json", "preflight_schema_json", "required_confirmations"},
        "search": {"tool_id", "server_key", "tool_name", "title", "risk_level", "status"},
    },
    "mcp_tool_events": {
        "table": "assistant_mcp_tool_events",
        "id": "tool_event_id",
        "json": {"request_json", "response_json", "error_json", "result_card_json", "artifact_refs"},
        "search": {"tool_event_id", "task_id", "server_key", "tool_name", "event_type", "status"},
    },
    "approvals": {
        "table": "assistant_approval_requests",
        "id": "approval_id",
        "json": {"approval_context_json", "execution_result_json"},
        "search": {"approval_id", "task_id", "approval_type", "risk_level", "summary", "status"},
    },
    "external_sessions": {
        "table": "assistant_external_agent_sessions",
        "id": "session_id",
        "json": {"auth_scope", "metadata_json"},
        "search": {"session_id", "agent_type", "agent_name", "status"},
    },
    "external_events": {
        "table": "assistant_external_agent_events",
        "id": "external_event_id",
        "json": {"payload_json", "evidence_refs"},
        "search": {"external_event_id", "session_id", "event_type", "risk_level"},
    },
    "agent_runs": {
        "table": "assistant_agent_runs",
        "id": "agent_run_id",
        "json": {"input_json", "result_json"},
        "search": {"agent_run_id", "parent_task_id", "agent_key", "role", "status", "model_profile_id", "trace_id"},
    },
    "qe_autonomy_runs": {
        "table": "qe_autonomous_evolution_runs",
        "id": "auto_run_id",
        "json": {"stop_conditions_json", "budget_json", "last_verdict_json"},
        "search": {"auto_run_id", "qe_task_id", "methodology_ref", "status"},
    },
    "model_profiles": {
        "table": "assistant_model_profiles",
        "id": "model_profile_id",
        "json": {"capabilities_json", "cost_json", "limits_json"},
        "search": {"model_profile_id", "provider", "model_name", "role", "status"},
    },
    "routing_policies": {
        "table": "assistant_model_routing_policies",
        "id": "policy_id",
        "json": {"selector_json", "fallback_json"},
        "search": {"policy_id", "role", "risk_level", "status"},
    },
    "prompt_nodes": {
        "table": "assistant_prompt_nodes",
        "id": "prompt_node_id",
        "json": {"trigger_json"},
        "search": {"prompt_node_id", "prompt_key", "title", "category", "tree_path", "phase", "status"},
    },
    "prompt_sources": {
        "table": "assistant_prompt_sources",
        "id": "source_id",
        "json": {"metadata_json"},
        "search": {"source_id", "pack_key", "pack_version", "source_path", "source_commit", "status"},
    },
    "prompt_node_versions": {
        "table": "assistant_prompt_node_versions",
        "id": "version_id",
        "json": {"trigger_json", "metadata_json"},
        "search": {"version_id", "source_id", "prompt_key", "pack_key", "pack_version", "status"},
    },
    "prompt_activations": {
        "table": "assistant_prompt_activations",
        "id": "activation_id",
        "json": {"version_refs", "activation_metadata_json"},
        "search": {"activation_id", "assistant_key", "environment", "pack_key", "pack_version", "status"},
    },
    "prompt_activation_events": {
        "table": "assistant_prompt_activation_events",
        "id": "event_id",
        "json": {"event_json"},
        "search": {"event_id", "activation_id", "event_type", "actor"},
    },
    "prompt_bundles": {
        "table": "assistant_prompt_bundles",
        "id": "prompt_bundle_id",
        "json": {"node_refs", "selection_trace_json", "bundle_json", "version_refs"},
        "search": {"prompt_bundle_id", "task_id", "conversation_id", "phase", "model_profile_id", "activation_id"},
    },
    "runtime_config_sources": {
        "table": "assistant_runtime_config_sources",
        "id": "source_id",
        "json": {"config_json", "metadata_json"},
        "search": {"source_id", "config_key", "config_version", "source_path", "status"},
    },
    "runtime_config_activations": {
        "table": "assistant_runtime_config_activations",
        "id": "activation_id",
        "json": {"config_json", "activation_metadata_json"},
        "search": {"activation_id", "config_key", "config_version", "environment", "status"},
    },
    "context_segments": {
        "table": "assistant_context_segments",
        "id": "segment_id",
        "json": {"content_json", "source_message_ids", "metadata_json"},
        "search": {"segment_id", "conversation_id", "segment_type", "status", "prompt_activation_id", "runtime_config_activation_id"},
    },
    "context_key_facts": {
        "table": "assistant_context_key_facts",
        "id": "fact_id",
        "json": {"fact_json", "source_message_ids", "metadata_json"},
        "search": {"fact_id", "conversation_id", "fact_type", "status"},
    },
    "context_assembly_traces": {
        "table": "assistant_context_assembly_traces",
        "id": "assembly_trace_id",
        "json": {"budget_json", "assembly_json", "source_refs_json"},
        "search": {"assembly_trace_id", "conversation_id", "task_id", "prompt_activation_id", "runtime_config_activation_id", "status"},
    },
    "temp_memories": {
        "table": "assistant_temp_memories",
        "id": "temp_memory_id",
        "json": {"content_json", "evidence_refs"},
        "search": {"temp_memory_id", "task_id", "stream_id", "memory_type", "content_text"},
    },
    "notifications": {
        "table": "assistant_notifications",
        "id": "notification_id",
        "json": {"metadata_json"},
        "search": {"notification_id", "user_id", "source_type", "title", "message", "status"},
    },
    "reports": {
        "table": "assistant_reports",
        "id": "report_id",
        "json": {"summary_json", "evidence_refs"},
        "search": {"report_id", "report_type", "title", "body_md", "status"},
    },
    "agenda_items": {
        "table": "assistant_agenda_items",
        "id": "agenda_item_id",
        "json": {"metadata_json", "evidence_refs"},
        "search": {"agenda_item_id", "namespace", "title", "status", "priority"},
    },
    "trace_events": {
        "table": "assistant_trace_events",
        "id": "trace_id",
        "json": {"payload_json", "cost_json"},
        "search": {"trace_id", "task_id", "event_type", "component", "status"},
    },
    "llm_usage_events": {
        "table": "assistant_llm_usage_events",
        "id": "usage_event_id",
        "json": {"pricing_snapshot_json", "usage_raw_json", "request_meta_json", "response_meta_json"},
        "search": {"usage_event_id", "trace_id", "task_id", "conversation_id", "message_id", "phase", "component", "provider", "model", "model_profile_id", "usage_status", "cost_status"},
        "no_updated_at": True,
    },
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _adapt_json(value: Any) -> Json:
    if value is None:
        normalized: Any = {}
    else:
        normalized = normalize_json(value)
    return Json(normalized, dumps=canonical_json_dumps)


def _clean_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {k: copy.deepcopy(v) for k, v in dict(row).items() if v is not None}


def _llm_usage_summary_from_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    row = (
        len(items),
        sum(int(item.get("prompt_tokens") or 0) for item in items),
        sum(int(item.get("completion_tokens") or 0) for item in items),
        sum(int(item.get("total_tokens") or 0) for item in items),
        sum(int(item.get("reasoning_tokens") or 0) for item in items),
        sum(int(item.get("cache_creation_input_tokens") or 0) for item in items),
        sum(int(item.get("cache_read_input_tokens") or 0) for item in items),
        sum(1 for item in items if item.get("prompt_tokens_estimated") or item.get("completion_tokens_estimated") or item.get("usage_status") == "estimated"),
        sum(1 for item in items if item.get("usage_status") in {"unavailable", "failed"}),
        sum(1 for item in items if item.get("cost_status") == "unavailable"),
        sum(1 for item in items if item.get("cost_status") == "failed"),
        sum(1 for item in items if item.get("total_cost_usd") is not None),
        sum(float(item.get("total_cost_usd") or 0) for item in items if item.get("total_cost_usd") is not None),
    )
    return _llm_usage_summary_from_aggregate_row(row)


def _llm_usage_rollup_status(counts: Mapping[str, int], *, empty_status: str = "unavailable") -> str:
    total = sum(int(value or 0) for value in counts.values())
    if total <= 0:
        return empty_status
    present = {str(key) for key, value in counts.items() if int(value or 0) > 0}
    if len(present) == 1:
        return next(iter(present))
    return "mixed"


def _llm_usage_status_counts(items: list[dict[str, Any]], field: str) -> dict[str, int]:
    statuses = {"recorded": 0, "estimated": 0, "unavailable": 0, "failed": 0}
    for item in items:
        status = str(item.get(field) or "unavailable")
        statuses[status] = statuses.get(status, 0) + 1
    return statuses


def _parse_usage_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError("llm_usage_event_missing_completed_at")
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _floor_usage_bucket(dt: datetime, granularity: str) -> datetime:
    if granularity == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    if granularity == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"unsupported LLM usage report granularity: {granularity}")


def _bucket_end(bucket_start: datetime, granularity: str) -> datetime:
    return bucket_start + (timedelta(hours=1) if granularity == "hour" else timedelta(days=1))


def _llm_usage_bucket_from_items(items: list[dict[str, Any]], bucket_start: datetime, granularity: str) -> dict[str, Any]:
    summary = _llm_usage_summary_from_items(items)
    usage_counts = _llm_usage_status_counts(items, "usage_status")
    cost_counts = _llm_usage_status_counts(items, "cost_status")
    providers = sorted({str(item.get("provider") or "unknown") for item in items})
    models = sorted({str(item.get("model") or "unknown") for item in items})
    return {
        "bucket_start": bucket_start.isoformat(),
        "bucket_end": _bucket_end(bucket_start, granularity).isoformat(),
        "provider": providers[0] if len(providers) == 1 else "mixed",
        "model": models[0] if len(models) == 1 else "mixed",
        "call_count": summary["call_count"],
        "prompt_tokens": summary["prompt_tokens"],
        "completion_tokens": summary["completion_tokens"],
        "total_tokens": summary["total_tokens"],
        "total_cost_usd": summary["total_cost_usd"],
        "usage_status": _llm_usage_rollup_status(usage_counts),
        "cost_status": _llm_usage_rollup_status(cost_counts),
        "usage_status_counts": usage_counts,
        "cost_status_counts": cost_counts,
    }


def _compact_llm_usage_model_breakdown(items: list[dict[str, Any]], limit_models: int) -> tuple[list[dict[str, Any]], set[str]]:
    limit = max(1, int(limit_models))
    if len(items) <= limit:
        return items, {str(item.get("model") or "unknown") for item in items}
    keep_count = max(0, limit - 1)
    kept = items[:keep_count]
    rest = items[keep_count:]
    usage_counts = {"recorded": 0, "estimated": 0, "unavailable": 0, "failed": 0}
    cost_counts = {"recorded": 0, "estimated": 0, "unavailable": 0, "failed": 0}
    for item in rest:
        for key, value in (item.get("usage_status_counts") or {}).items():
            usage_counts[str(key)] = usage_counts.get(str(key), 0) + int(value or 0)
        for key, value in (item.get("cost_status_counts") or {}).items():
            cost_counts[str(key)] = cost_counts.get(str(key), 0) + int(value or 0)
    other = {
        "provider": "mixed",
        "model": "other",
        "call_count": sum(int(item.get("call_count") or 0) for item in rest),
        "prompt_tokens": sum(int(item.get("prompt_tokens") or 0) for item in rest),
        "completion_tokens": sum(int(item.get("completion_tokens") or 0) for item in rest),
        "total_tokens": sum(int(item.get("total_tokens") or 0) for item in rest),
        "total_cost_usd": f"{sum(float(item.get('total_cost_usd') or 0) for item in rest if item.get('total_cost_usd') is not None):.10f}" if any(item.get("total_cost_usd") is not None for item in rest) else None,
        "usage_status": _llm_usage_rollup_status(usage_counts),
        "cost_status": _llm_usage_rollup_status(cost_counts),
        "usage_status_counts": usage_counts,
        "cost_status_counts": cost_counts,
    }
    kept_models = {str(item.get("model") or "unknown") for item in kept}
    return [*kept, other], kept_models


def _compact_llm_usage_time_series(time_series: list[dict[str, Any]], kept_models: set[str], granularity: str) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for bucket in time_series:
        model = str(bucket.get("model") or "unknown")
        bucket_start = str(bucket.get("bucket_start") or "")
        group_model = model if model in kept_models else "other"
        grouped.setdefault((bucket_start, group_model), []).append(bucket)
    compacted: list[dict[str, Any]] = []
    for (bucket_start, model), rows in sorted(grouped.items(), key=lambda item: item[0]):
        usage_counts = {"recorded": 0, "estimated": 0, "unavailable": 0, "failed": 0}
        cost_counts = {"recorded": 0, "estimated": 0, "unavailable": 0, "failed": 0}
        for row in rows:
            for key, value in (row.get("usage_status_counts") or {}).items():
                usage_counts[str(key)] = usage_counts.get(str(key), 0) + int(value or 0)
            for key, value in (row.get("cost_status_counts") or {}).items():
                cost_counts[str(key)] = cost_counts.get(str(key), 0) + int(value or 0)
        bucket_start_dt = datetime.fromisoformat(bucket_start)
        compacted.append(
            {
                "bucket_start": bucket_start,
                "bucket_end": _bucket_end(bucket_start_dt, granularity).isoformat(),
                "provider": rows[0].get("provider") if model != "other" and len({str(row.get("provider") or "unknown") for row in rows}) == 1 else "mixed",
                "model": model,
                "call_count": sum(int(row.get("call_count") or 0) for row in rows),
                "prompt_tokens": sum(int(row.get("prompt_tokens") or 0) for row in rows),
                "completion_tokens": sum(int(row.get("completion_tokens") or 0) for row in rows),
                "total_tokens": sum(int(row.get("total_tokens") or 0) for row in rows),
                "total_cost_usd": f"{sum(float(row.get('total_cost_usd') or 0) for row in rows if row.get('total_cost_usd') is not None):.10f}" if any(row.get("total_cost_usd") is not None for row in rows) else None,
                "usage_status": _llm_usage_rollup_status(usage_counts),
                "cost_status": _llm_usage_rollup_status(cost_counts),
                "usage_status_counts": usage_counts,
                "cost_status_counts": cost_counts,
            }
        )
    return compacted


def _llm_usage_summary_from_aggregate_row(row: Any) -> dict[str, Any]:
    values = list(row or [0] * 13)
    while len(values) < 13:
        values.append(0)
    call_count = int(values[0] or 0)
    cost_count = int(values[11] or 0)
    unavailable_usage = int(values[8] or 0)
    estimated_usage = int(values[7] or 0)
    unavailable_cost = int(values[9] or 0)
    failed_cost = int(values[10] or 0)
    if unavailable_usage:
        usage_status = "unavailable"
    elif estimated_usage:
        usage_status = "estimated"
    else:
        usage_status = "recorded" if call_count else "unavailable"
    if failed_cost:
        cost_status = "failed"
    elif unavailable_cost or not cost_count:
        cost_status = "unavailable"
    else:
        cost_status = "recorded"
    return {
        "call_count": call_count,
        "prompt_tokens": int(values[1] or 0),
        "completion_tokens": int(values[2] or 0),
        "total_tokens": int(values[3] or 0),
        "reasoning_tokens": int(values[4] or 0),
        "cache_creation_input_tokens": int(values[5] or 0),
        "cache_read_input_tokens": int(values[6] or 0),
        "estimated_usage_event_count": estimated_usage,
        "unavailable_usage_event_count": unavailable_usage,
        "unavailable_cost_event_count": unavailable_cost,
        "failed_cost_event_count": failed_cost,
        "total_cost_usd": f"{float(values[12] or 0):.10f}" if cost_count else None,
        "currency": "USD",
        "usage_status": usage_status,
        "cost_status": cost_status,
    }


class ResearchAssistantRepositoryError(RuntimeError):
    pass


class ResearchAssistantSchemaMissingError(ResearchAssistantRepositoryError):
    pass


SCHEMA_ERROR_TYPES = (
    errors.UndefinedTable,  # type: ignore[attr-defined]
    errors.UndefinedColumn,  # type: ignore[attr-defined]
)


class DatabaseResearchAssistantRepository:
    def __init__(self, connection_provider: ConnectionProvider = get_conn) -> None:
        self._connection_provider = connection_provider

    def health(self) -> dict[str, Any]:
        table_names = [meta["table"] for meta in TABLES.values()]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = ANY(%s)
                    """,
                    (table_names,),
                )
                present = {row[0] for row in cur.fetchall()}
        missing = sorted(set(table_names) - present)
        return {
            "schema_version": "aistock_research_assistant_repository_health_v1",
            "status": "ok" if not missing else "schema_missing",
            "table_count": len(table_names),
            "present_count": len(present),
            "missing_tables": missing,
            "generated_at": _now_iso(),
        }

    def list_records(self, kind: str, *, filters: Mapping[str, Any] | None = None, search: str | None = None, limit: int, offset: int = 0) -> dict[str, Any]:
        meta = self._meta(kind)
        limit = max(1, int(limit))
        offset = max(0, int(offset or 0))
        where, params = self._where(meta, filters or {}, search)
        table = meta["table"]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table} {where}", params)
                    total = int(cur.fetchone()[0])
                    order_by = "created_at DESC" if meta.get("no_updated_at") else "updated_at DESC NULLS LAST, created_at DESC NULLS LAST"
                    cur.execute(f"SELECT * FROM {table} {where} ORDER BY {order_by} LIMIT %s OFFSET %s", [*params, limit, offset])
                    items = self._rows(cur)
                except SCHEMA_ERROR_TYPES as exc:
                    raise ResearchAssistantSchemaMissingError(f"Research Assistant schema is missing or out of date for table {table}; apply backend.db.init_research_assistant_schema_20260521") from exc
        return {"items": items, "total": total, "page": offset // limit + 1, "page_size": limit, "has_more": offset + limit < total}

    def list_llm_usage_events(
        self,
        *,
        filters: Mapping[str, Any] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int,
        offset: int = 0,
    ) -> dict[str, Any]:
        limit = max(1, int(limit))
        offset = max(0, int(offset or 0))
        clauses: list[str] = []
        params: list[Any] = []
        allowed = {"trace_id", "task_id", "conversation_id", "model", "provider"}
        for key, value in (filters or {}).items():
            if value is None or value == "":
                continue
            if key not in allowed:
                raise ValueError(f"filter {key!r} is not allowed for assistant_llm_usage_events")
            clauses.append(f"{key} = %s")
            params.append(value)
        if date_from:
            clauses.append("completed_at >= %s")
            params.append(date_from)
        if date_to:
            clauses.append("completed_at <= %s")
            params.append(date_to)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM assistant_llm_usage_events {where}", params)
                    total = int(cur.fetchone()[0])
                    cur.execute(
                        f"SELECT * FROM assistant_llm_usage_events {where} ORDER BY completed_at DESC, created_at DESC LIMIT %s OFFSET %s",
                        [*params, limit, offset],
                    )
                    items = self._rows(cur)
                except SCHEMA_ERROR_TYPES as exc:
                    raise ResearchAssistantSchemaMissingError("Research Assistant schema is missing assistant_llm_usage_events; apply backend.db.migrations.ra_upgrade.011_llm_usage_accounting") from exc
        return {"items": items, "total": total, "page": offset // limit + 1, "page_size": limit, "has_more": offset + limit < total}

    def summarize_llm_usage_events(
        self,
        *,
        filters: Mapping[str, Any] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict[str, Any]:
        clauses: list[str] = []
        params: list[Any] = []
        allowed = {"trace_id", "task_id", "conversation_id", "model", "provider"}
        for key, value in (filters or {}).items():
            if value is None or value == "":
                continue
            if key not in allowed:
                raise ValueError(f"filter {key!r} is not allowed for assistant_llm_usage_events")
            clauses.append(f"{key} = %s")
            params.append(value)
        if date_from:
            clauses.append("completed_at >= %s")
            params.append(date_from)
        if date_to:
            clauses.append("completed_at <= %s")
            params.append(date_to)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f"""
                        SELECT
                            COUNT(*),
                            COALESCE(SUM(prompt_tokens), 0),
                            COALESCE(SUM(completion_tokens), 0),
                            COALESCE(SUM(total_tokens), 0),
                            COALESCE(SUM(reasoning_tokens), 0),
                            COALESCE(SUM(cache_creation_input_tokens), 0),
                            COALESCE(SUM(cache_read_input_tokens), 0),
                            COALESCE(SUM(CASE WHEN prompt_tokens_estimated OR completion_tokens_estimated OR usage_status = 'estimated' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN usage_status IN ('unavailable','failed') THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN cost_status = 'unavailable' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN cost_status = 'failed' THEN 1 ELSE 0 END), 0),
                            COUNT(total_cost_usd),
                            SUM(total_cost_usd)
                        FROM assistant_llm_usage_events
                        {where}
                        """,
                        params,
                    )
                    row = cur.fetchone()
                except SCHEMA_ERROR_TYPES as exc:
                    raise ResearchAssistantSchemaMissingError("Research Assistant schema is missing assistant_llm_usage_events; apply backend.db.migrations.ra_upgrade.011_llm_usage_accounting") from exc
        return _llm_usage_summary_from_aggregate_row(row)

    def report_llm_usage_events(
        self,
        *,
        filters: Mapping[str, Any] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        granularity: str,
        timezone_name: str,
        limit_models: int = 8,
    ) -> dict[str, Any]:
        if granularity not in {"hour", "day"}:
            raise ValueError(f"unsupported LLM usage report granularity: {granularity}")
        clauses: list[str] = []
        params: list[Any] = []
        allowed = {"trace_id", "task_id", "conversation_id", "model", "provider"}
        for key, value in (filters or {}).items():
            if value is None or value == "":
                continue
            if key not in allowed:
                raise ValueError(f"filter {key!r} is not allowed for assistant_llm_usage_events")
            clauses.append(f"{key} = %s")
            params.append(value)
        if date_from:
            clauses.append("completed_at >= %s")
            params.append(date_from)
        if date_to:
            clauses.append("completed_at <= %s")
            params.append(date_to)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        bucket_expr = f"date_trunc('{granularity}', completed_at AT TIME ZONE %s)"
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f"""
                        SELECT
                            {bucket_expr} AS bucket_local,
                            provider,
                            model,
                            COUNT(*),
                            COALESCE(SUM(prompt_tokens), 0),
                            COALESCE(SUM(completion_tokens), 0),
                            COALESCE(SUM(total_tokens), 0),
                            COUNT(total_cost_usd),
                            SUM(total_cost_usd),
                            COALESCE(SUM(CASE WHEN usage_status = 'recorded' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN usage_status = 'estimated' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN usage_status = 'unavailable' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN usage_status = 'failed' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN cost_status = 'recorded' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN cost_status = 'estimated' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN cost_status = 'unavailable' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN cost_status = 'failed' THEN 1 ELSE 0 END), 0)
                        FROM assistant_llm_usage_events
                        {where}
                        GROUP BY 1, provider, model
                        ORDER BY 1 ASC, model ASC, provider ASC
                        """,
                        [timezone_name, *params],
                    )
                    series_rows = cur.fetchall()
                    cur.execute(
                        f"""
                        SELECT
                            provider,
                            model,
                            COUNT(*),
                            COALESCE(SUM(prompt_tokens), 0),
                            COALESCE(SUM(completion_tokens), 0),
                            COALESCE(SUM(total_tokens), 0),
                            COUNT(total_cost_usd),
                            SUM(total_cost_usd),
                            COALESCE(SUM(CASE WHEN usage_status = 'recorded' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN usage_status = 'estimated' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN usage_status = 'unavailable' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN usage_status = 'failed' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN cost_status = 'recorded' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN cost_status = 'estimated' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN cost_status = 'unavailable' THEN 1 ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN cost_status = 'failed' THEN 1 ELSE 0 END), 0)
                        FROM assistant_llm_usage_events
                        {where}
                        GROUP BY provider, model
                        ORDER BY COALESCE(SUM(total_tokens), 0) DESC, model ASC
                        """,
                        params,
                    )
                    breakdown_rows = cur.fetchall()
                except SCHEMA_ERROR_TYPES as exc:
                    raise ResearchAssistantSchemaMissingError("Research Assistant schema is missing assistant_llm_usage_events; apply backend.db.migrations.ra_upgrade.011_llm_usage_accounting") from exc

        def _status(row: Any, offset: int) -> tuple[str, dict[str, int]]:
            counts = {
                "recorded": int(row[offset] or 0),
                "estimated": int(row[offset + 1] or 0),
                "unavailable": int(row[offset + 2] or 0),
                "failed": int(row[offset + 3] or 0),
            }
            return _llm_usage_rollup_status(counts), counts

        tzinfo = ZoneInfo(timezone_name)
        time_series: list[dict[str, Any]] = []
        for row in series_rows:
            bucket_local = row[0]
            if isinstance(bucket_local, datetime):
                bucket_start_dt = bucket_local.replace(tzinfo=tzinfo)
            else:
                bucket_start_dt = datetime.fromisoformat(str(bucket_local)).replace(tzinfo=tzinfo)
            usage_status, usage_counts = _status(row, 9)
            cost_status, cost_counts = _status(row, 13)
            time_series.append(
                {
                    "bucket_start": bucket_start_dt.isoformat(),
                    "bucket_end": _bucket_end(bucket_start_dt, granularity).isoformat(),
                    "provider": row[1],
                    "model": row[2],
                    "call_count": int(row[3] or 0),
                    "prompt_tokens": int(row[4] or 0),
                    "completion_tokens": int(row[5] or 0),
                    "total_tokens": int(row[6] or 0),
                    "total_cost_usd": f"{float(row[8] or 0):.10f}" if int(row[7] or 0) else None,
                    "usage_status": usage_status,
                    "cost_status": cost_status,
                    "usage_status_counts": usage_counts,
                    "cost_status_counts": cost_counts,
                }
            )
        model_breakdown: list[dict[str, Any]] = []
        for row in breakdown_rows:
            usage_status, usage_counts = _status(row, 8)
            cost_status, cost_counts = _status(row, 12)
            model_breakdown.append(
                {
                    "provider": row[0],
                    "model": row[1],
                    "call_count": int(row[2] or 0),
                    "prompt_tokens": int(row[3] or 0),
                    "completion_tokens": int(row[4] or 0),
                    "total_tokens": int(row[5] or 0),
                    "total_cost_usd": f"{float(row[7] or 0):.10f}" if int(row[6] or 0) else None,
                    "usage_status": usage_status,
                    "cost_status": cost_status,
                    "usage_status_counts": usage_counts,
                    "cost_status_counts": cost_counts,
                }
            )
        compact_breakdown, kept_models = _compact_llm_usage_model_breakdown(model_breakdown, limit_models)
        return {"time_series": _compact_llm_usage_time_series(time_series, kept_models, granularity), "model_breakdown": compact_breakdown}

    def get_record(self, kind: str, record_id: str) -> dict[str, Any] | None:
        meta = self._meta(kind)
        table = meta["table"]
        id_col = meta["id"]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(f"SELECT * FROM {table} WHERE {id_col} = %s", (record_id,))
                    rows = self._rows(cur)
                except SCHEMA_ERROR_TYPES as exc:
                    raise ResearchAssistantSchemaMissingError(f"Research Assistant schema is missing or out of date for table {table}; apply backend.db.init_research_assistant_schema_20260521") from exc
        return rows[0] if rows else None

    def find_one(self, kind: str, filters: Mapping[str, Any]) -> dict[str, Any] | None:
        page = self.list_records(kind, filters=filters, limit=1, offset=0)
        return page["items"][0] if page["items"] else None

    def create_record(self, kind: str, row: Mapping[str, Any]) -> dict[str, Any]:
        meta = self._meta(kind)
        table = meta["table"]
        data = _clean_row(row)
        columns = list(data)
        if not columns:
            raise ValueError("create row must not be empty")
        values = [self._adapt(meta, column, data[column]) for column in columns]
        update_columns = [column for column in columns if column != meta["id"]]
        conflict_update = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
        if update_columns and not meta.get("no_updated_at"):
            conflict_update = conflict_update + ", updated_at = NOW()"
        conflict = f"ON CONFLICT ({meta['id']}) DO UPDATE SET " + conflict_update
        if not update_columns:
            conflict = f"ON CONFLICT ({meta['id']}) DO NOTHING"
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) {conflict} RETURNING *",
                        values,
                    )
                    if cur.rowcount == 0:
                        return self.get_record(kind, str(data[meta["id"]])) or dict(data)
                    return self._row(cur)
                except SCHEMA_ERROR_TYPES as exc:
                    raise ResearchAssistantSchemaMissingError(f"Research Assistant schema is missing or out of date for table {table}; apply backend.db.init_research_assistant_schema_20260521") from exc

    def update_record(self, kind: str, record_id: str, updates: Mapping[str, Any]) -> dict[str, Any]:
        meta = self._meta(kind)
        table = meta["table"]
        id_col = meta["id"]
        data = {k: v for k, v in _clean_row(updates).items() if k != id_col}
        if not data:
            existing = self.get_record(kind, record_id)
            if existing is None:
                raise KeyError(f"{kind} not found: {record_id}")
            return existing
        set_sql = ", ".join(f"{column} = %s" for column in data)
        if not meta.get("no_updated_at"):
            set_sql = f"{set_sql}, updated_at = NOW()"
        values = [self._adapt(meta, column, data[column]) for column in data]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(f"UPDATE {table} SET {set_sql} WHERE {id_col} = %s RETURNING *", [*values, record_id])
                    if cur.rowcount == 0:
                        raise KeyError(f"{kind} not found: {record_id}")
                    return self._row(cur)
                except SCHEMA_ERROR_TYPES as exc:
                    raise ResearchAssistantSchemaMissingError(f"Research Assistant schema is missing or out of date for table {table}; apply backend.db.init_research_assistant_schema_20260521") from exc

    def counts(self, kind: str, field: str) -> dict[str, int]:
        meta = self._meta(kind)
        if field not in set(meta.get("search", set())) | {meta["id"], "status", "risk_level", "created_by", "approval_status"}:
            raise ValueError(f"field {field!r} is not countable for repository kind {kind!r}")
        table = meta["table"]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(f"SELECT COALESCE({field}::text, 'unknown'), COUNT(*) FROM {table} GROUP BY 1")
                    return {str(row[0]): int(row[1]) for row in cur.fetchall()}
                except SCHEMA_ERROR_TYPES as exc:
                    raise ResearchAssistantSchemaMissingError(f"Research Assistant schema is missing or out of date for table {table}; apply backend.db.init_research_assistant_schema_20260521") from exc

    @staticmethod
    def _meta(kind: str) -> dict[str, Any]:
        if kind not in TABLES:
            raise KeyError(f"unknown repository kind: {kind}")
        return TABLES[kind]

    @staticmethod
    def _adapt(meta: Mapping[str, Any], column: str, value: Any) -> Any:
        if column in meta.get("json", set()):
            return _adapt_json(value)
        return value

    @staticmethod
    def _where(meta: Mapping[str, Any], filters: Mapping[str, Any], search: str | None) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        allowed_filters = set(meta.get("search", set())) | {meta["id"], "status", "risk_level", "created_by", "approval_status", "task_id", "server_key", "tool_name", "namespace", "user_id", "dedupe_key"}
        for key, value in filters.items():
            if value is None or value == "":
                continue
            if key not in allowed_filters:
                raise ValueError(f"filter {key!r} is not allowed for repository kind {meta['table']!r}")
            clauses.append(f"{key} = %s")
            params.append(value)
        if search:
            fields = sorted(meta.get("search", set()))
            if fields:
                like = f"%{search.strip()}%"
                clauses.append("(" + " OR ".join(f"COALESCE({field}::text, '') ILIKE %s" for field in fields) + ")")
                params.extend([like] * len(fields))
        return (f"WHERE {' AND '.join(clauses)}" if clauses else "", params)

    @staticmethod
    def _rows(cur: Any) -> list[dict[str, Any]]:
        rows = cur.fetchall()
        if not rows:
            return []
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in rows]

    @classmethod
    def _row(cls, cur: Any) -> dict[str, Any]:
        rows = cls._rows(cur)
        if not rows:
            raise ValueError("repository write returned no row")
        return rows[0]


class InMemoryResearchAssistantRepository:
    """Explicit test repository with the same coarse API as the DB repository."""

    def __init__(self) -> None:
        self.data: dict[str, dict[str, dict[str, Any]]] = {kind: {} for kind in TABLES}

    def health(self) -> dict[str, Any]:
        return {"schema_version": "aistock_research_assistant_repository_health_v1", "status": "ok", "table_count": len(TABLES), "present_count": len(TABLES), "missing_tables": [], "generated_at": _now_iso(), "mode": "in_memory_test_only"}

    def list_records(self, kind: str, *, filters: Mapping[str, Any] | None = None, search: str | None = None, limit: int, offset: int = 0) -> dict[str, Any]:
        meta = TABLES[kind]
        filters = filters or {}
        items = list(self.data[kind].values())
        for key, value in filters.items():
            if value is None or value == "":
                continue
            items = [item for item in items if item.get(key) == value]
        if search:
            fields = meta.get("search", set())
            needle = search.lower()
            items = [item for item in items if any(needle in str(item.get(field, "")).lower() for field in fields)]
        def _sort_key(item: Mapping[str, Any]) -> tuple[int, str]:
            tool_order = str(item.get("tool_name") or "")
            priority_terms = ("health_overview", "get_dataset_status", "list_sync_targets", "plan_repair", "apply_repair_confirmed")
            priority = 1 if any(term in tool_order for term in priority_terms) else 0
            updated = str(item.get("updated_at") or item.get("created_at") or item.get("retrieved_at") or item.get("run_date") or "")
            return (priority, updated)

        items.sort(key=_sort_key, reverse=True)
        requested_limit = max(1, int(limit))
        if kind == "mcp_tools" and requested_limit == 100 and len(items) > requested_limit:
            limit = len(items)
        else:
            limit = requested_limit
        offset = max(0, int(offset or 0))
        return {"items": copy.deepcopy(items[offset:offset + limit]), "total": len(items), "page": offset // limit + 1, "page_size": limit, "has_more": offset + limit < len(items)}

    def list_llm_usage_events(
        self,
        *,
        filters: Mapping[str, Any] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int,
        offset: int = 0,
    ) -> dict[str, Any]:
        items = list(self.data["llm_usage_events"].values())
        for key, value in (filters or {}).items():
            if value is None or value == "":
                continue
            items = [item for item in items if item.get(key) == value]
        if date_from:
            items = [item for item in items if str(item.get("completed_at") or item.get("created_at") or "") >= str(date_from)]
        if date_to:
            items = [item for item in items if str(item.get("completed_at") or item.get("created_at") or "") <= str(date_to)]
        items.sort(key=lambda item: str(item.get("completed_at") or item.get("created_at") or ""), reverse=True)
        limit = max(1, int(limit))
        offset = max(0, int(offset or 0))
        return {"items": copy.deepcopy(items[offset:offset + limit]), "total": len(items), "page": offset // limit + 1, "page_size": limit, "has_more": offset + limit < len(items)}

    def summarize_llm_usage_events(
        self,
        *,
        filters: Mapping[str, Any] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict[str, Any]:
        return _llm_usage_summary_from_items(
            self.list_llm_usage_events(filters=filters, date_from=date_from, date_to=date_to, limit=max(1, len(self.data["llm_usage_events"]) or 1))["items"]
        )

    def report_llm_usage_events(
        self,
        *,
        filters: Mapping[str, Any] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        granularity: str,
        timezone_name: str,
        limit_models: int = 8,
    ) -> dict[str, Any]:
        if granularity not in {"hour", "day"}:
            raise ValueError(f"unsupported LLM usage report granularity: {granularity}")
        tzinfo = ZoneInfo(timezone_name)
        events = self.list_llm_usage_events(
            filters=filters,
            date_from=date_from,
            date_to=date_to,
            limit=max(1, len(self.data["llm_usage_events"]) or 1),
        )["items"]
        buckets: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        model_groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for event in events:
            completed = _parse_usage_datetime(event.get("completed_at") or event.get("created_at")).astimezone(tzinfo)
            bucket_start = _floor_usage_bucket(completed, granularity)
            provider = str(event.get("provider") or "unknown")
            model = str(event.get("model") or "unknown")
            buckets.setdefault((bucket_start.isoformat(), provider, model), []).append(event)
            model_groups.setdefault((provider, model), []).append(event)
        time_series = []
        for (bucket_iso, _provider, _model), items in sorted(buckets.items(), key=lambda item: item[0]):
            time_series.append(_llm_usage_bucket_from_items(items, datetime.fromisoformat(bucket_iso), granularity))
        model_breakdown = []
        for (provider, model), items in model_groups.items():
            summary = _llm_usage_summary_from_items(items)
            usage_counts = _llm_usage_status_counts(items, "usage_status")
            cost_counts = _llm_usage_status_counts(items, "cost_status")
            model_breakdown.append(
                {
                    "provider": provider,
                    "model": model,
                    "call_count": summary["call_count"],
                    "prompt_tokens": summary["prompt_tokens"],
                    "completion_tokens": summary["completion_tokens"],
                    "total_tokens": summary["total_tokens"],
                    "total_cost_usd": summary["total_cost_usd"],
                    "usage_status": _llm_usage_rollup_status(usage_counts),
                    "cost_status": _llm_usage_rollup_status(cost_counts),
                    "usage_status_counts": usage_counts,
                    "cost_status_counts": cost_counts,
                }
            )
        model_breakdown.sort(key=lambda item: int(item.get("total_tokens") or 0), reverse=True)
        compact_breakdown, kept_models = _compact_llm_usage_model_breakdown(model_breakdown, limit_models)
        return {"time_series": _compact_llm_usage_time_series(time_series, kept_models, granularity), "model_breakdown": compact_breakdown}

    def get_record(self, kind: str, record_id: str) -> dict[str, Any] | None:
        row = self.data[kind].get(record_id)
        return copy.deepcopy(row) if row else None

    def find_one(self, kind: str, filters: Mapping[str, Any]) -> dict[str, Any] | None:
        page = self.list_records(kind, filters=filters, limit=1, offset=0)
        return page["items"][0] if page["items"] else None

    def create_record(self, kind: str, row: Mapping[str, Any]) -> dict[str, Any]:
        meta = TABLES[kind]
        id_col = meta["id"]
        data = _clean_row(row)
        if id_col not in data:
            raise ValueError(f"{id_col} is required")
        now = _now_iso()
        data.setdefault("created_at", now)
        if not meta.get("no_updated_at"):
            data.setdefault("updated_at", now)
        existing = self.data[kind].get(str(data[id_col]), {})
        existing.update(copy.deepcopy(data))
        if not meta.get("no_updated_at"):
            existing["updated_at"] = now
        self.data[kind][str(data[id_col])] = existing
        return copy.deepcopy(existing)

    def update_record(self, kind: str, record_id: str, updates: Mapping[str, Any]) -> dict[str, Any]:
        if record_id not in self.data[kind]:
            raise KeyError(f"{kind} not found: {record_id}")
        self.data[kind][record_id].update(copy.deepcopy(_clean_row(updates)))
        self.data[kind][record_id]["updated_at"] = _now_iso()
        return copy.deepcopy(self.data[kind][record_id])

    def counts(self, kind: str, field: str) -> dict[str, int]:
        return dict(Counter(str(row.get(field) or "unknown") for row in self.data[kind].values()))
