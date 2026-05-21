
"""Repositories for Research Assistant Console state.

The production repository is PostgreSQL only. Tests may inject the in-memory
repository explicitly; the service never silently falls back from database to
memory.
"""

from __future__ import annotations

import copy
from collections import Counter
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any

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
        "search": {"task_id", "title", "task_type", "status", "created_by"},
    },
    "task_events": {
        "table": "agent_task_events",
        "id": "event_id",
        "json": {"payload_json", "evidence_refs"},
        "search": {"event_type", "message", "severity"},
    },
    "memory_items": {
        "table": "research_memory_items",
        "id": "memory_id",
        "json": {"content_json", "evidence_refs"},
        "search": {"memory_id", "memory_type", "namespace", "subject_key", "title", "content_text", "approval_status"},
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
    "mcp_tools": {
        "table": "assistant_mcp_tools",
        "id": "tool_id",
        "json": {"input_schema_json", "output_schema_json", "preflight_schema_json", "required_confirmations"},
        "search": {"tool_id", "server_key", "tool_name", "title", "risk_level", "status"},
    },
    "mcp_tool_events": {
        "table": "assistant_mcp_tool_events",
        "id": "tool_event_id",
        "json": {"request_json", "response_json", "error_json"},
        "search": {"tool_event_id", "task_id", "server_key", "tool_name", "event_type", "status"},
    },
    "approvals": {
        "table": "assistant_approval_requests",
        "id": "approval_id",
        "json": {"approval_context_json", "execution_result_json"},
        "search": {"approval_id", "task_id", "approval_type", "risk_level", "summary", "status"},
    },
    "issue_candidates": {
        "table": "assistant_issue_candidates",
        "id": "candidate_id",
        "json": {"evidence_refs", "github_sync_json"},
        "search": {"candidate_id", "title", "severity", "module", "status", "problem_statement", "dedupe_key"},
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
    "validation_discovery_reports": {
        "table": "assistant_validation_discovery_reports",
        "id": "discovery_report_id",
        "json": {"summary_json", "candidate_issue_refs", "validation_run_refs", "evidence_refs"},
        "search": {"discovery_report_id", "run_date", "status", "title"},
    },
    "trace_events": {
        "table": "assistant_trace_events",
        "id": "trace_id",
        "json": {"payload_json", "cost_json"},
        "search": {"trace_id", "task_id", "event_type", "component", "status"},
    },
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _adapt_json(value: Any) -> Json:
    return Json(normalize_json(value or {}), dumps=canonical_json_dumps)


def _clean_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {k: copy.deepcopy(v) for k, v in dict(row).items() if v is not None}


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

    def list_records(self, kind: str, *, filters: Mapping[str, Any] | None = None, search: str | None = None, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        meta = self._meta(kind)
        limit = max(1, min(int(limit or 50), 500))
        offset = max(0, int(offset or 0))
        where, params = self._where(meta, filters or {}, search)
        table = meta["table"]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table} {where}", params)
                    total = int(cur.fetchone()[0])
                    cur.execute(f"SELECT * FROM {table} {where} ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST LIMIT %s OFFSET %s", [*params, limit, offset])
                    items = self._rows(cur)
                except SCHEMA_ERROR_TYPES as exc:
                    raise ResearchAssistantSchemaMissingError(f"Research Assistant schema is missing or out of date for table {table}; apply backend.db.init_research_assistant_schema_20260521") from exc
        return {"items": items, "total": total, "page": offset // limit + 1, "page_size": limit, "has_more": offset + limit < total}

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
        conflict = f"ON CONFLICT ({meta['id']}) DO UPDATE SET " + ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns) + ", updated_at = NOW()"
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
        values = [self._adapt(meta, column, data[column]) for column in data]
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(f"UPDATE {table} SET {set_sql}, updated_at = NOW() WHERE {id_col} = %s RETURNING *", [*values, record_id])
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

    def list_records(self, kind: str, *, filters: Mapping[str, Any] | None = None, search: str | None = None, limit: int = 50, offset: int = 0) -> dict[str, Any]:
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
        def _sort_key(item: Mapping[str, Any]) -> str:
            return str(item.get("updated_at") or item.get("created_at") or item.get("retrieved_at") or item.get("run_date") or "")

        items.sort(key=_sort_key, reverse=True)
        limit = max(1, min(int(limit or 50), 500))
        offset = max(0, int(offset or 0))
        return {"items": copy.deepcopy(items[offset:offset + limit]), "total": len(items), "page": offset // limit + 1, "page_size": limit, "has_more": offset + limit < len(items)}

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
        data.setdefault("updated_at", now)
        existing = self.data[kind].get(str(data[id_col]), {})
        existing.update(copy.deepcopy(data))
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
