from __future__ import annotations

from datetime import date
from typing import Any

from backend.services.research_assistant.proactive_reports import (
    ProactiveReportContext,
    ProactiveReportProvider,
    ProactiveReportProviderRegistry,
    build_default_proactive_report_registry,
    generate_proactive_report,
)
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.runtime_config import DEFAULT_RUNTIME_CONFIG_PATH, RUNTIME_CONFIG_KEY, load_runtime_config
from backend.services.research_assistant.service import ResearchAssistantService


class SpyRepository(InMemoryResearchAssistantRepository):
    def __init__(self) -> None:
        super().__init__()
        self.write_calls: list[tuple[str, str]] = []

    def create_record(self, kind: str, row: dict[str, Any]) -> dict[str, Any]:
        self.write_calls.append(("create_record", kind))
        return super().create_record(kind, row)

    def update_record(self, kind: str, record_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        self.write_calls.append(("update_record", kind))
        return super().update_record(kind, record_id, updates)


def _provider(provider_id: str, title: str, section_key: str, source_ref: str) -> ProactiveReportProvider:
    def collect(context: ProactiveReportContext) -> dict[str, Any]:
        return {
            "section_key": section_key,
            "title": title,
            "status": "ok",
            "items": [
                {
                    "title": f"{title} 摘要",
                    "body": f"{title} 已基于只读证据生成。",
                    "source_refs": [source_ref],
                    "reason_codes": [],
                    "warnings": [],
                }
            ],
            "source_refs": [source_ref],
            "todo_items": [f"跟进 {title}"],
            "reason_codes": [],
            "warnings": [],
        }

    return ProactiveReportProvider(provider_id, title, section_key, collect)


def _seed_runtime_config(repo: SpyRepository) -> None:
    runtime = load_runtime_config(DEFAULT_RUNTIME_CONFIG_PATH).config
    repo.create_record(
        "runtime_config_activations",
        {
            "activation_id": "runtime_phase9",
            "config_key": RUNTIME_CONFIG_KEY,
            "config_version": "test",
            "environment": "dev",
            "status": "active",
            "config_json": runtime,
        },
    )
    repo.write_calls.clear()


def _section_by_key(report: dict[str, Any], section_key: str) -> dict[str, Any]:
    sections = report["sections_json"]["sections"]
    return next(section for section in sections if section["section_key"] == section_key)


def test_proactive_report_contains_evidence_sections_and_no_placeholders() -> None:
    registry = ProactiveReportProviderRegistry(
        [
            _provider("tasks", "任务与事件", "tasks", "research_agent_tasks:task-1"),
            _provider("experiments", "QE/实验状态", "experiments", "qe_experiments:qe-1"),
            _provider("issues", "Validation/BUG/Issue", "issues", "issue_candidates:issue-1"),
            _provider("data_health", "本地数据健康", "data_health", "local_data_health:2026-06-16"),
        ]
    )
    report = generate_proactive_report(
        context=ProactiveReportContext(repository=InMemoryResearchAssistantRepository(), report_date=date(2026, 6, 16)),
        registry=registry,
        report_id_factory=lambda prefix: f"{prefix}_test",
    )

    sections = report["sections_json"]["sections"]
    assert {section["section_key"] for section in sections} >= {"tasks", "experiments", "issues", "data_health"}
    for section in sections:
        assert section["source_refs"], section
        for item in section["items"]:
            assert item["source_refs"], item
    snapshot = str(report)
    for placeholder in ("XX", "X%", "约X"):
        assert placeholder not in snapshot
    assert report["sections_json"]["read_only"] is True
    assert report["sections_json"]["high_risk_actions_proposed"] is False
    assert report["sections_json"]["trigger"]["user_query_triggered"] is False


def test_proactive_report_degrades_missing_provider_evidence_with_reason_and_warning() -> None:
    registry = ProactiveReportProviderRegistry(
        [
            ProactiveReportProvider(
                "tasks",
                "任务与事件",
                "tasks",
                lambda context: {"section_key": "tasks", "title": "任务与事件", "items": [], "source_refs": []},
            )
        ]
    )

    report = generate_proactive_report(
        context=ProactiveReportContext(repository=InMemoryResearchAssistantRepository(), report_date=date(2026, 6, 16)),
        registry=registry,
        report_id_factory=lambda prefix: f"{prefix}_missing",
    )

    section = report["sections_json"]["sections"][0]
    assert section["status"] == "degraded"
    assert "tasks_no_evidence" in section["reason_codes"]
    assert section["warnings"]
    assert "proactive_report_no_evidence" in report["sections_json"]["reason_codes"]


def test_default_registry_collects_real_repository_issue_and_data_health_sources() -> None:
    repo = InMemoryResearchAssistantRepository()
    repo.create_record(
        "issue_candidates",
        {
            "candidate_id": "issue_phase9",
            "title": "Phase 9 issue evidence",
            "severity": "P1",
            "module": "research_assistant",
            "status": "needs_review",
            "problem_statement": "issue_candidates should feed proactive reports",
            "dedupe_key": "phase9-issue",
            "evidence_refs": ["issue://phase9"],
        },
    )
    repo.create_record(
        "external_events",
        {
            "external_event_id": "extev_local_data_health",
            "session_id": "local_data",
            "event_type": "local_data_health_overview",
            "payload_json": {"status": "ok"},
            "evidence_refs": ["local_data://health"],
            "risk_level": "low",
        },
    )

    report = generate_proactive_report(
        context=ProactiveReportContext(repository=repo, report_date=date(2026, 6, 16)),
        registry=build_default_proactive_report_registry(),
        report_id_factory=lambda prefix: f"{prefix}_real_registry",
    )

    issue_section = _section_by_key(report, "issues")
    data_health_section = _section_by_key(report, "data_health")
    assert issue_section["status"] == "ok"
    assert issue_section["source_refs"]
    assert issue_section["items"][0]["source_refs"] == ["assistant_issue_candidates:issue_phase9"]
    assert "issues_provider_failed" not in report["sections_json"]["reason_codes"]
    assert data_health_section["status"] == "ok"
    assert data_health_section["source_refs"]
    assert "local_data://health" in data_health_section["source_refs"]
    assert "data_health_provider_failed" not in report["sections_json"]["reason_codes"]


def test_proactive_report_generation_is_read_only_until_explicit_persist() -> None:
    repo = SpyRepository()
    registry = ProactiveReportProviderRegistry(
        [_provider("tasks", "任务与事件", "tasks", "research_agent_tasks:task-1")]
    )

    report = generate_proactive_report(
        context=ProactiveReportContext(repository=repo, report_date=date(2026, 6, 16)),
        registry=registry,
        report_id_factory=lambda prefix: f"{prefix}_readonly",
    )

    assert report["summary_md"]
    assert repo.write_calls == []


def test_service_persists_scheduled_proactive_report_without_action_proposals() -> None:
    repo = SpyRepository()
    _seed_runtime_config(repo)
    service = ResearchAssistantService(repository=repo, llm_client=object())
    registry = ProactiveReportProviderRegistry(
        [
            _provider("tasks", "任务与事件", "tasks", "research_agent_tasks:task-1"),
            _provider("experiments", "QE/实验状态", "experiments", "qe_experiments:qe-1"),
            _provider("issues", "Validation/BUG/Issue", "issues", "issue_candidates:issue-1"),
            _provider("data_health", "本地数据健康", "data_health", "local_data_health:2026-06-16"),
        ]
    )

    row = service.generate_scheduled_proactive_report(report_date=date(2026, 6, 16), registry=registry)

    assert row["report_type"] == "morning_brief"
    assert row["sections_json"]["trigger"]["trigger_type"] == "scheduled_orchestrator"
    assert row["source_refs_json"]
    assert ("create_record", "proactive_reports") in repo.write_calls
    assert all(kind != "action_proposals" for _, kind in repo.write_calls)
