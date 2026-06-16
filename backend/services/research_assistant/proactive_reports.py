"""Read-only proactive report generation for Research Assistant."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

PROACTIVE_REPORT_SCHEMA = "aistock_research_assistant_proactive_report_v1"
PROACTIVE_SECTION_SCHEMA = "aistock_research_assistant_proactive_report_section_v1"
PROACTIVE_ITEM_SCHEMA = "aistock_research_assistant_proactive_report_item_v1"
PROACTIVE_REGISTRY_SCHEMA = "aistock_research_assistant_proactive_provider_registry_v1"

PLACEHOLDER_TOKENS = ("XX", "X%", "约X")


@dataclass(frozen=True)
class ProactiveReportProvider:
    """Read-only evidence provider registered by the report framework."""

    provider_id: str
    title: str
    section_key: str
    collect: Callable[["ProactiveReportContext"], dict[str, Any]]
    required: bool = True


@dataclass(frozen=True)
class ProactiveReportContext:
    repository: Any
    report_date: date
    report_type: str = "morning_brief"
    max_items_per_section: int = 5


class ProactiveReportProviderRegistry:
    def __init__(self, providers: Sequence[ProactiveReportProvider] | None = None) -> None:
        self._providers: list[ProactiveReportProvider] = []
        for provider in providers or []:
            self.register(provider)

    def register(self, provider: ProactiveReportProvider) -> None:
        if any(existing.provider_id == provider.provider_id for existing in self._providers):
            raise ValueError(f"duplicate proactive report provider: {provider.provider_id}")
        self._providers.append(provider)

    @property
    def providers(self) -> tuple[ProactiveReportProvider, ...]:
        return tuple(self._providers)

    def to_manifest(self) -> dict[str, Any]:
        return {
            "schema_version": PROACTIVE_REGISTRY_SCHEMA,
            "providers": [
                {
                    "provider_id": provider.provider_id,
                    "title": provider.title,
                    "section_key": provider.section_key,
                    "required": provider.required,
                }
                for provider in self._providers
            ],
        }


def build_default_proactive_report_registry() -> ProactiveReportProviderRegistry:
    return ProactiveReportProviderRegistry(
        [
            ProactiveReportProvider("tasks", "任务与事件", "tasks", collect_task_activity_section),
            ProactiveReportProvider("experiments", "QE/实验状态", "experiments", collect_experiment_status_section),
            ProactiveReportProvider("issues", "Validation/BUG/Issue", "issues", collect_issue_validation_section),
            ProactiveReportProvider("data_health", "本地数据健康", "data_health", collect_local_data_health_section),
            ProactiveReportProvider("agent_teams", "Agent Teams 运行", "agent_teams", collect_agent_team_section),
            ProactiveReportProvider("personal_tasks", "personal.task.* 进展", "personal_tasks", collect_personal_task_section),
        ]
    )


def generate_proactive_report(
    *,
    context: ProactiveReportContext,
    registry: ProactiveReportProviderRegistry,
    report_id_factory: Callable[[str], str],
    cheap_worker: Callable[[dict[str, Any]], str] | None = None,
) -> dict[str, Any]:
    """Generate a read-only evidence-first proactive report."""

    sections: list[dict[str, Any]] = []
    source_refs: list[str] = []
    reason_codes: list[str] = []
    warnings: list[str] = []
    for provider in registry.providers:
        try:
            section = provider.collect(context)
        except Exception as exc:  # noqa: BLE001 - provider degradation is explicit.
            section = _empty_section(
                provider=provider,
                status="degraded",
                reason_codes=[f"{provider.provider_id}_provider_failed"],
                warnings=[f"{provider.provider_id} provider failed: {type(exc).__name__}: {exc}"],
            )
        section = _normalize_section(section, provider=provider)
        sections.append(section)
        source_refs.extend(_as_strings(section.get("source_refs")))
        reason_codes.extend(_as_strings(section.get("reason_codes")))
        warnings.extend(_as_strings(section.get("warnings")))

    unique_source_refs = _dedupe(source_refs)
    if not unique_source_refs:
        reason_codes.append("proactive_report_no_evidence")
        warnings.append("proactive report generated with no source refs")
    summary_md = cheap_worker(_summary_payload(context, sections, registry)) if cheap_worker else _deterministic_summary(context, sections)
    placeholder_hits = [token for token in PLACEHOLDER_TOKENS if token in summary_md]
    if placeholder_hits:
        reason_codes.append("proactive_report_placeholder_detected")
        warnings.append(f"placeholder tokens detected in summary: {', '.join(placeholder_hits)}")
        summary_md = _strip_placeholders(summary_md)
    status = "generated" if not warnings else "generated_with_warnings"
    return {
        "schema_version": PROACTIVE_REPORT_SCHEMA,
        "report_id": report_id_factory("apr"),
        "report_type": context.report_type,
        "report_date": context.report_date.isoformat(),
        "summary_md": summary_md,
        "sections_json": {
            "schema_version": PROACTIVE_SECTION_SCHEMA,
            "provider_registry": registry.to_manifest(),
            "sections": sections,
            "reason_codes": _dedupe(reason_codes),
            "warnings": _dedupe(warnings),
            "read_only": True,
            "high_risk_actions_proposed": False,
            "trigger": {
                "trigger_type": "scheduled_orchestrator",
                "user_query_triggered": False,
            },
        },
        "source_refs_json": unique_source_refs,
        "status": status,
        "created_at": _utc_now(),
    }


def collect_task_activity_section(context: ProactiveReportContext) -> dict[str, Any]:
    tasks = _list_records(context.repository, "tasks", limit=context.max_items_per_section)
    events = _list_records(context.repository, "task_events", limit=context.max_items_per_section)
    items = []
    for task in tasks:
        source_refs = [f"research_agent_tasks:{task.get('task_id')}"]
        items.append(_item(title=str(task.get("title") or task.get("task_id")), body=f"状态={task.get('status') or 'unknown'}；类型={task.get('task_type') or 'unknown'}。", source_refs=source_refs))
    for event in events[: max(0, context.max_items_per_section - len(items))]:
        source_refs = [f"agent_task_events:{event.get('event_id')}"]
        items.append(_item(title=str(event.get("event_type") or "task_event"), body=str(event.get("message") or "任务事件已记录。"), source_refs=source_refs))
    return _section("tasks", "任务与事件", items, empty_reason="tasks_no_evidence")


def collect_experiment_status_section(context: ProactiveReportContext) -> dict[str, Any]:
    records = _try_list_records(context.repository, ("qe_autonomous_runs", "qe_autonomy_runs", "evolution_paths"), limit=context.max_items_per_section)
    items = [
        _item(
            title=str(row.get("objective") or row.get("qe_task_id") or row.get("auto_run_id") or row.get("path_id") or "experiment"),
            body=f"状态={row.get('status') or 'unknown'}。",
            source_refs=[f"{kind}:{row.get('auto_run_id') or row.get('path_id') or row.get('qe_task_id') or index}"],
        )
        for index, (kind, row) in enumerate(records)
    ]
    return _section("experiments", "QE/实验状态", items, empty_reason="experiments_no_evidence")


def collect_issue_validation_section(context: ProactiveReportContext) -> dict[str, Any]:
    issues = _list_records(context.repository, "issue_candidates", limit=context.max_items_per_section)
    validations = _list_records(context.repository, "validation_discovery_reports", limit=context.max_items_per_section)
    items = [
        _item(
            title=str(item.get("title") or item.get("issue_candidate_id") or item.get("discovery_report_id")),
            body=f"状态={item.get('status') or 'unknown'}。",
            source_refs=[f"issue_validation:{item.get('issue_candidate_id') or item.get('discovery_report_id') or index}"],
        )
        for index, item in enumerate([*issues, *validations][: context.max_items_per_section])
    ]
    return _section("issues", "Validation/BUG/Issue", items, empty_reason="issues_no_evidence")


def collect_local_data_health_section(context: ProactiveReportContext) -> dict[str, Any]:
    events = _list_records(context.repository, "trace_events", filters={"component": "local_data_health"}, limit=context.max_items_per_section)
    items = [
        _item(
            title=str(event.get("event_type") or "local_data_health"),
            body=f"状态={event.get('status') or 'unknown'}。",
            source_refs=[f"assistant_trace_events:{event.get('trace_id') or index}"],
        )
        for index, event in enumerate(events)
    ]
    return _section("data_health", "本地数据健康", items, empty_reason="data_health_no_evidence")


def collect_agent_team_section(context: ProactiveReportContext) -> dict[str, Any]:
    tasks = _list_records(context.repository, "tasks", filters={"task_type": "agent_team"}, limit=context.max_items_per_section)
    items = [
        _item(
            title=str(task.get("title") or task.get("task_id")),
            body=f"Agent Teams 状态={task.get('status') or 'unknown'}。",
            source_refs=[f"research_agent_tasks:{task.get('task_id')}"],
        )
        for task in tasks
    ]
    return _section("agent_teams", "Agent Teams 运行", items, empty_reason="agent_teams_no_evidence")


def collect_personal_task_section(context: ProactiveReportContext) -> dict[str, Any]:
    memories = _list_records(context.repository, "memory_items", filters={"scope": "personal"}, search="personal.task.", limit=context.max_items_per_section)
    items = [
        _item(
            title=str(memory.get("title") or memory.get("tree_path") or memory.get("memory_id")),
            body=str(memory.get("content_text") or "personal.task 进展已记录。"),
            source_refs=[f"research_memory_items:{memory.get('memory_id')}"],
        )
        for memory in memories
    ]
    return _section("personal_tasks", "personal.task.* 进展", items, empty_reason="personal_tasks_no_evidence")


def _summary_payload(context: ProactiveReportContext, sections: list[dict[str, Any]], registry: ProactiveReportProviderRegistry) -> dict[str, Any]:
    return {
        "schema_version": PROACTIVE_REPORT_SCHEMA,
        "report_type": context.report_type,
        "report_date": context.report_date.isoformat(),
        "sections": sections,
        "provider_registry": registry.to_manifest(),
        "instruction": "Summarize as a concise evidence-first morning brief. Do not invent facts.",
    }


def _deterministic_summary(context: ProactiveReportContext, sections: list[dict[str, Any]]) -> str:
    lines = [f"# 研究助理晨报（{context.report_date.isoformat()}）", ""]
    todos: list[str] = []
    for section in sections:
        items = section.get("items") if isinstance(section.get("items"), list) else []
        lines.append(f"## {section.get('title')}")
        if items:
            for item in items:
                refs = ", ".join(_as_strings(item.get("source_refs")))
                lines.append(f"- {item.get('title')}：{item.get('body')} 证据：{refs}")
        else:
            reason = ", ".join(_as_strings(section.get("reason_codes"))) or "evidence_missing"
            lines.append(f"- 证据不足：{reason}。")
        todos.extend(_as_strings(section.get("todo_items")))
        lines.append("")
    lines.append("## 待办")
    if todos:
        lines.extend(f"- {todo}" for todo in todos)
    else:
        lines.append("- 暂无证据支持的新增待办。")
    return "\n".join(lines).strip()


def _section(section_key: str, title: str, items: list[dict[str, Any]], *, empty_reason: str) -> dict[str, Any]:
    if items:
        return {
            "schema_version": PROACTIVE_SECTION_SCHEMA,
            "section_key": section_key,
            "title": title,
            "status": "ok",
            "items": items,
            "source_refs": _dedupe(ref for item in items for ref in _as_strings(item.get("source_refs"))),
            "todo_items": [],
            "reason_codes": [],
            "warnings": [],
        }
    return {
        "schema_version": PROACTIVE_SECTION_SCHEMA,
        "section_key": section_key,
        "title": title,
        "status": "degraded",
        "items": [],
        "source_refs": [],
        "todo_items": [],
        "reason_codes": [empty_reason],
        "warnings": [f"{title} has no read-only evidence"],
    }


def _item(*, title: str, body: str, source_refs: list[str]) -> dict[str, Any]:
    clean_refs = _dedupe(source_refs)
    if not clean_refs:
        return {
            "schema_version": PROACTIVE_ITEM_SCHEMA,
            "title": title,
            "body": "证据不足。",
            "source_refs": [],
            "reason_codes": ["item_no_source_refs"],
            "warnings": ["report item lacks source refs"],
        }
    return {
        "schema_version": PROACTIVE_ITEM_SCHEMA,
        "title": title,
        "body": body,
        "source_refs": clean_refs,
        "reason_codes": [],
        "warnings": [],
    }


def _empty_section(*, provider: ProactiveReportProvider, status: str, reason_codes: list[str], warnings: list[str]) -> dict[str, Any]:
    return {
        "schema_version": PROACTIVE_SECTION_SCHEMA,
        "section_key": provider.section_key,
        "title": provider.title,
        "status": status,
        "items": [],
        "source_refs": [],
        "todo_items": [],
        "reason_codes": reason_codes,
        "warnings": warnings,
    }


def _normalize_section(section: Mapping[str, Any], *, provider: ProactiveReportProvider) -> dict[str, Any]:
    normalized = dict(section)
    normalized.setdefault("schema_version", PROACTIVE_SECTION_SCHEMA)
    normalized.setdefault("section_key", provider.section_key)
    normalized.setdefault("title", provider.title)
    normalized.setdefault("status", "ok")
    normalized.setdefault("items", [])
    normalized.setdefault("source_refs", [])
    normalized.setdefault("todo_items", [])
    normalized.setdefault("reason_codes", [])
    normalized.setdefault("warnings", [])
    item_refs = [ref for item in normalized.get("items") or [] if isinstance(item, Mapping) for ref in _as_strings(item.get("source_refs"))]
    normalized["source_refs"] = _dedupe([*_as_strings(normalized.get("source_refs")), *item_refs])
    if not normalized["source_refs"]:
        normalized["status"] = "degraded"
        normalized["reason_codes"] = _dedupe([*_as_strings(normalized.get("reason_codes")), f"{provider.provider_id}_no_evidence"])
        normalized["warnings"] = _dedupe([*_as_strings(normalized.get("warnings")), f"{provider.title} has no source refs"])
    return normalized


def _list_records(repository: Any, kind: str, *, limit: int, filters: dict[str, Any] | None = None, search: str | None = None) -> list[dict[str, Any]]:
    page = repository.list_records(kind, filters=filters, search=search, limit=limit)
    return [dict(item) for item in page.get("items") or [] if isinstance(item, Mapping)]


def _try_list_records(repository: Any, kinds: Sequence[str], *, limit: int) -> list[tuple[str, dict[str, Any]]]:
    records: list[tuple[str, dict[str, Any]]] = []
    for kind in kinds:
        try:
            records.extend((kind, item) for item in _list_records(repository, kind, limit=limit))
        except KeyError:
            continue
    return records[:limit]


def _as_strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def _dedupe(values: Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _strip_placeholders(text: str) -> str:
    cleaned = text
    for token in PLACEHOLDER_TOKENS:
        cleaned = cleaned.replace(token, "证据不足")
    return cleaned


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
