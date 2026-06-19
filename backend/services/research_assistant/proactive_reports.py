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
VALIDATION_ISSUE_FACT_SOURCE_UNAVAILABLE = "validation_issue_fact_source_unavailable"
RA_DRAFT_STORAGE_NOTICE = "\u975e\u6743\u5a01\u5bf9\u8bdd\u8349\u7a3f/\u89e3\u91ca\u7f13\u5b58\uff0c\u5f85 Phase 2 \u9000\u573a"


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
    issue_fact_source: Any | None = None


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
    records, reason_codes, warnings = _try_list_records(context.repository, ("qe_autonomy_runs", "evolution_paths"), limit=context.max_items_per_section)
    items = [
        _item(
            title=str(row.get("objective") or row.get("qe_task_id") or row.get("auto_run_id") or row.get("path_id") or "experiment"),
            body=f"状态={row.get('status') or 'unknown'}。",
            source_refs=[f"{kind}:{row.get('auto_run_id') or row.get('path_id') or row.get('qe_task_id') or index}"],
        )
        for index, (kind, row) in enumerate(records)
    ]
    return _section("experiments", "QE/experiment status", items, empty_reason="experiments_no_evidence", reason_codes=reason_codes, warnings=warnings)


def collect_issue_validation_section(context: ProactiveReportContext) -> dict[str, Any]:
    if context.issue_fact_source is None:
        return _empty_section(
            provider=ProactiveReportProvider("issues", "Validation/BUG/Issue", "issues", collect_issue_validation_section),
            status="degraded",
            reason_codes=[VALIDATION_ISSUE_FACT_SOURCE_UNAVAILABLE],
            warnings=[f"Validation issue fact source is not injected; RA draft tables are {RA_DRAFT_STORAGE_NOTICE} and cannot substitute for facts."],
        )
    try:
        page = context.issue_fact_source.issue_candidates(page=1, page_size=context.max_items_per_section)
    except Exception as exc:  # noqa: BLE001 - degraded read is explicit and never falls back to RA drafts.
        return _empty_section(
            provider=ProactiveReportProvider("issues", "Validation/BUG/Issue", "issues", collect_issue_validation_section),
            status="degraded",
            reason_codes=[VALIDATION_ISSUE_FACT_SOURCE_UNAVAILABLE],
            warnings=[f"Validation issue fact source unavailable: {type(exc).__name__}: {exc}"],
        )
    records = [item for item in page.get("items") or [] if isinstance(item, Mapping)]
    items = [
        _item(
            title=str(item.get("title") or item.get("candidate_id")),
            body=f"status={item.get('status') or 'unknown'}; source_type={item.get('source_type') or 'unknown'}; source_of_truth=Validation issue candidates.",
            source_refs=_issue_candidate_source_refs(item, fallback_index=index),
        )
        for index, item in enumerate(records)
    ]
    return _section("issues", "Validation/BUG/Issue", items, empty_reason="issues_no_evidence", reason_codes=[], warnings=[])


def _issue_candidate_source_refs(item: Mapping[str, Any], *, fallback_index: int) -> list[str]:
    candidate_id = str(item.get("candidate_id") or "").strip()
    refs: list[str] = [f"validation_issue_candidates:{candidate_id}"] if candidate_id else [f"validation_issue_candidates:missing_candidate_id:{fallback_index}"]
    for key in ("source_ref", "github_issue_payload_ref", "source_path"):
        if item.get(key):
            refs.append(str(item[key]))
    source_paths = item.get("source_paths") if isinstance(item.get("source_paths"), list) else []
    refs.extend(str(path) for path in source_paths if str(path or "").strip())
    return _dedupe(refs)

def collect_local_data_health_section(context: ProactiveReportContext) -> dict[str, Any]:
    records, reason_codes, warnings = _try_list_records(context.repository, ("external_events", "task_events"), search="local_data", limit=context.max_items_per_section)
    items = [
        _item(
            title=str(event.get("event_type") or "local_data_health"),
            body=f"状态={event.get('status') or 'unknown'}。",
            source_refs=_source_refs_for_record(kind, event, fallback_index=index),
        )
        for index, (kind, event) in enumerate(records)
    ]
    return _section("data_health", "local data health", items, empty_reason="data_health_no_evidence", reason_codes=reason_codes, warnings=warnings)


def collect_agent_team_section(context: ProactiveReportContext) -> dict[str, Any]:
    runs = _list_records(context.repository, "agent_runs", limit=context.max_items_per_section)
    items = [
        _item(
            title=str(run.get("agent_key") or run.get("role") or run.get("agent_run_id")),
            body=f"Agent Teams status={run.get('status') or 'unknown'}.",
            source_refs=[f"assistant_agent_runs:{run.get('agent_run_id')}"],
        )
        for run in runs
    ]
    return _section("agent_teams", "Agent Teams runs", items, empty_reason="agent_teams_no_evidence")


def collect_personal_task_section(context: ProactiveReportContext) -> dict[str, Any]:
    memories = _list_records(context.repository, "memory_items", filters={"scope": "personal"}, search="personal.task.", limit=context.max_items_per_section)
    items = [
        _item(
            title=str(memory.get("title") or memory.get("tree_path") or memory.get("memory_id")),
            body=str(memory.get("content_text") or "personal.task progress recorded."),
            source_refs=[f"research_memory_items:{memory.get('memory_id')}"],
        )
        for memory in memories
    ]
    return _section("personal_tasks", "personal.task.* progress", items, empty_reason="personal_tasks_no_evidence")


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


def _section(
    section_key: str,
    title: str,
    items: list[dict[str, Any]],
    *,
    empty_reason: str,
    reason_codes: list[str] | None = None,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    extra_reason_codes = list(reason_codes or [])
    extra_warnings = list(warnings or [])
    if items:
        return {
            "schema_version": PROACTIVE_SECTION_SCHEMA,
            "section_key": section_key,
            "title": title,
            "status": "ok",
            "items": items,
            "source_refs": _dedupe(ref for item in items for ref in _as_strings(item.get("source_refs"))),
            "todo_items": [],
            "reason_codes": _dedupe(extra_reason_codes),
            "warnings": _dedupe(extra_warnings),
        }
    return {
        "schema_version": PROACTIVE_SECTION_SCHEMA,
        "section_key": section_key,
        "title": title,
        "status": "degraded",
        "items": [],
        "source_refs": [],
        "todo_items": [],
        "reason_codes": _dedupe([empty_reason, *extra_reason_codes]),
        "warnings": _dedupe([f"{title} has no read-only evidence", *extra_warnings]),
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


def _try_list_records(
    repository: Any,
    kinds: Sequence[str],
    *,
    limit: int,
    filters: dict[str, Any] | None = None,
    search: str | None = None,
) -> tuple[list[tuple[str, dict[str, Any]]], list[str], list[str]]:
    records: list[tuple[str, dict[str, Any]]] = []
    reason_codes: list[str] = []
    warnings: list[str] = []
    for kind in kinds:
        try:
            records.extend((kind, item) for item in _list_records(repository, kind, limit=limit, filters=filters, search=search))
        except KeyError as exc:
            reason_codes.append(f"{kind}_repository_kind_unavailable")
            warnings.append(f"repository kind {kind!r} is unavailable: {exc}")
    return records[:limit], _dedupe(reason_codes), _dedupe(warnings)


def _source_refs_for_record(kind: str, row: Mapping[str, Any], *, fallback_index: int) -> list[str]:
    embedded_refs = _as_strings(row.get("evidence_refs"))
    if kind == "external_events":
        return _dedupe([*embedded_refs, f"assistant_external_agent_events:{row.get('external_event_id') or fallback_index}"])
    if kind == "task_events":
        return _dedupe([*embedded_refs, f"agent_task_events:{row.get('event_id') or fallback_index}"])
    return _dedupe([*embedded_refs, f"{kind}:{fallback_index}"])


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
