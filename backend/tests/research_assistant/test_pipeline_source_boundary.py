
from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

from backend.services.research_assistant.proactive_reports import (
    ProactiveReportContext,
    build_default_proactive_report_registry,
    collect_issue_validation_section,
)
from backend.services.research_assistant.react_grounding import McpToolResult, ReactGroundingConfig, compose_with_evidence_guard
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService


class FakeIssueFactSource:
    def __init__(self, items: list[dict[str, Any]] | None = None, *, fail: bool = False) -> None:
        self.items = items or [_candidate()]
        self.fail = fail
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def issue_candidates(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("issue_candidates", kwargs))
        if self.fail:
            raise RuntimeError("validation candidate API offline")
        page = int(kwargs.get("page") or 1)
        page_size = int(kwargs.get("page_size") or 20)
        status = kwargs.get("status")
        module = kwargs.get("module")
        items = list(self.items)
        if status:
            items = [item for item in items if item.get("status") == status]
        if module:
            items = [item for item in items if item.get("module_id") == module]
        start = (page - 1) * page_size
        end = start + page_size
        return {"items": items[start:end], "total": len(items), "page": page, "page_size": page_size, "has_more": end < len(items), "data_state": "complete"}

    def issue_candidate_summary(self) -> dict[str, Any]:
        self.calls.append(("issue_candidate_summary", {}))
        if self.fail:
            raise RuntimeError("validation candidate API offline")
        return {"candidate_count": len(self.items), "by_source_type": {"nightly_bug_candidate": len(self.items)}, "data_state": "complete"}


def _candidate(candidate_id: str = "VC-1") -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "title": "Validation candidate X",
        "module_id": "research_assistant",
        "severity": "P1",
        "status": "new",
        "actual": "Validation source returned X",
        "source_type": "nightly_bug_candidate",
        "source_plan_key": "nightly_validation_api_probe",
        "active_discovery_reason": "root cleanliness regression",
        "source_path": "tmp/validation/nightly_failure_issue/VC-1.json",
        "source_paths": ["tmp/validation/nightly_failure_issue/VC-1.json"],
        "evidence_refs": ["tmp/validation/nightly_failure_issue/VC-1.json"],
        "last_seen_at": "2026-06-19T00:00:00Z",
    }


def _service(fact_source: FakeIssueFactSource) -> ResearchAssistantService:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), issue_fact_source=fact_source)
    svc.seed_catalogs()
    return svc


def test_issue_candidates_read_validation_fact_source_not_ra_draft_table() -> None:
    fact_source = FakeIssueFactSource()
    service = _service(fact_source)
    service.repository.create_record(
        "issue_candidates",
        {
            "candidate_id": "RA-DRAFT-1",
            "title": "RA draft must not leak",
            "severity": "P1",
            "module": "research_assistant",
            "status": "needs_review",
            "problem_statement": "draft table is not authoritative",
            "dedupe_key": "ra-draft-1",
            "evidence_refs": ["assistant_issue_candidates:RA-DRAFT-1"],
        },
    )

    page = service.list_pipeline_issue_candidates(limit=10)

    assert [item["candidate_id"] for item in page["items"]] == ["VC-1"]
    assert fact_source.calls[0][0] == "issue_candidates"
    assert page["source_of_truth"] == "validation_pipeline_issue_candidates"
    assert page["draft_storage_authoritative"] is False
    assert page["assistant_draft_substitution_blocked"] is True
    assert page["items"][0]["source_refs"][0] == "validation_issue_candidates:VC-1"
    assert "RA-DRAFT-1" not in str(page)


def test_issue_candidates_degrade_loudly_without_ra_draft_substitution() -> None:
    service = _service(FakeIssueFactSource(fail=True))
    service.repository.create_record(
        "issue_candidates",
        {
            "candidate_id": "RA-DRAFT-2",
            "title": "RA draft must not substitute for facts",
            "severity": "P1",
            "module": "research_assistant",
            "status": "needs_review",
            "problem_statement": "draft table is not authoritative",
            "dedupe_key": "ra-draft-2",
            "evidence_refs": ["assistant_issue_candidates:RA-DRAFT-2"],
        },
    )

    page = service.list_pipeline_issue_candidates(limit=10)

    assert page["data_state"] == "degraded"
    assert "validation_issue_fact_source_unavailable" in page["reason_codes"]
    assert page["items"] == []
    assert page["assistant_draft_substitution_blocked"] is True
    assert "RA-DRAFT-2" not in str(page)


def test_validation_discovery_summary_is_derived_from_validation_candidates() -> None:
    service = _service(FakeIssueFactSource())

    summary = service.validation_discovery_summary()

    assert summary["source_of_truth"] == "validation_pipeline_issue_candidates"
    assert summary["discovery_report_mode"] == "derived_from_validation_candidates"
    assert summary["discovery_manifest_api_available"] is False
    assert summary["draft_storage_authoritative"] is False
    assert summary["latest_reports"][0]["source_ref"] == "validation_issue_candidates:VC-1"
    assert summary["candidate_issues_needing_review"][0]["source_refs"][0] == "validation_issue_candidates:VC-1"


def test_validation_discovery_summary_degrades_loudly_when_fact_source_fails() -> None:
    service = _service(FakeIssueFactSource(fail=True))

    summary = service.validation_discovery_summary()

    assert summary["data_state"] == "degraded"
    assert "validation_issue_fact_source_unavailable" in summary["reason_codes"]
    assert summary["latest_reports"] == []
    assert summary["candidate_issues_needing_review"] == []
    assert summary["assistant_draft_substitution_blocked"] is True


def test_proactive_report_issue_section_uses_validation_candidate_refs() -> None:
    repo = InMemoryResearchAssistantRepository()
    fact_source = FakeIssueFactSource()

    section = collect_issue_validation_section(
        ProactiveReportContext(repository=repo, report_date=__import__("datetime").date(2026, 6, 19), issue_fact_source=fact_source)
    )

    assert section["status"] == "ok"
    assert section["items"][0]["source_refs"][0] == "validation_issue_candidates:VC-1"
    assert all(not ref.startswith("assistant_issue_candidates:") for ref in section["source_refs"])


def test_proactive_report_issue_section_degrades_without_fact_source() -> None:
    repo = InMemoryResearchAssistantRepository()
    section = collect_issue_validation_section(ProactiveReportContext(repository=repo, report_date=__import__("datetime").date(2026, 6, 19)))

    assert section["status"] == "degraded"
    assert "validation_issue_fact_source_unavailable" in section["reason_codes"]
    assert section["source_refs"] == []


def test_scheduled_report_keeps_validation_source_refs_grounded() -> None:
    repo = InMemoryResearchAssistantRepository()
    service = ResearchAssistantService(repository=repo, issue_fact_source=FakeIssueFactSource())
    service.seed_catalogs()

    row = service.generate_scheduled_proactive_report(report_date=__import__("datetime").date(2026, 6, 19), registry=build_default_proactive_report_registry())

    assert "validation_issue_candidates:VC-1" in row["source_refs_json"]
    decision = compose_with_evidence_guard(
        "Validation candidate VC-1 is grounded by source=validation_issue_candidates:VC-1 as_of=2026-06-19.",
        [
            McpToolResult(
                server_key="research-assistant",
                tool_name="generate_scheduled_proactive_report",
                status="succeeded",
                summary="morning report generated",
                source_refs=["validation_issue_candidates:VC-1"],
                as_of="2026-06-19",
                payload_json={"source_refs": row["source_refs_json"], "as_of": "2026-06-19"},
            )
        ],
        ReactGroundingConfig(max_tool_iterations=2),
    )
    assert decision.allowed is True


def test_github_sync_is_block_only_and_never_records_direct_create() -> None:
    service = _service(FakeIssueFactSource())
    issue = service.create_issue_candidate({"title": "Draft only", "problem_statement": "must use workflow"})

    sync = service.github_sync_issue_candidate(issue["candidate_id"], {"mode": "dry_run", "requested_by": "pytest"})

    assert sync["github_sync_status"] == "blocked"
    assert sync["github_sync_json"]["direct_github_create_performed"] is False
    assert "mcp_github_issue_sync_bug" in sync["github_sync_json"]["recommended_tools"]
    assert sync["draft_storage_authoritative"] is False


def test_research_assistant_static_boundary_forbids_write_pipeline_imports_but_allows_read_sources() -> None:
    root = Path(__file__).resolve().parents[3]
    allowed_read_imports = {
        "backend.services.validation.pipeline_center.ValidationPipelineCenterService",
        "backend.services.validation.finding_store.ValidationFindingStore",
        "backend.services.research_assistant.service.IssueCandidateFactSource",
    }
    forbidden_modules = {
        "scripts.aistock_issue_workflow",
        "scripts.nightly_bug_candidate_queue",
        "scripts.aistock_mcp_server",
    }
    scanned = [root / "backend/services/research_assistant", root / "backend/routers/research_assistant.py"]
    imports: set[str] = set()
    forbidden_hits: list[str] = []
    for target in scanned:
        paths = target.rglob("*.py") if target.is_dir() else [target]
        for path in paths:
            rel = path.relative_to(root).as_posix()
            text = path.read_text(encoding="utf-8-sig")
            tree = ast.parse(text)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.add(alias.name)
                        if alias.name in forbidden_modules:
                            forbidden_hits.append(f"{rel}: import {alias.name}")
                elif isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    for alias in node.names:
                        imports.add(f"{module}.{alias.name}")
                    if module in forbidden_modules or any(f"{module}.{alias.name}" in forbidden_modules for alias in node.names):
                        forbidden_hits.append(f"{rel}: from {module} import ...")
            forbidden_text_markers = (
                "tests/aistock_validation/bugs",
                "BUGS_ROOT",
                "gh issue create",
                "aistock_issue_workflow.py",
                "nightly_bug_candidate_queue.py",
                "scripts.aistock_mcp_server",
            )
            for marker in forbidden_text_markers:
                if marker in text:
                    forbidden_hits.append(f"{rel}: {marker}")
    assert not forbidden_hits
    assert allowed_read_imports & imports
