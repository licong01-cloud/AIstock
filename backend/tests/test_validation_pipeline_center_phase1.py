from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from backend.services.validation.pipeline_center import ValidationPipelineCenterService


class _FakeFindingStore:
    def __init__(self, bugs: list[dict], findings: list[dict] | None = None) -> None:
        self.bugs = bugs
        self.findings = findings or []

    def list_bugs(self, **kwargs):
        items = list(self.bugs)
        severity = kwargs.get("severity")
        module = kwargs.get("module")
        if severity:
            items = [item for item in items if item.get("severity") == severity]
        if module:
            items = [item for item in items if item.get("module") == module]
        return {"items": items, "total": len(items), "page": 1, "page_size": kwargs.get("page_size", 20), "has_more": False}

    def get_bug(self, bug_id: str):
        return next((item for item in self.bugs if item.get("bug_id") == bug_id), None)

    def list_findings(self, **kwargs):
        source_type = kwargs.get("source_type")
        items = [item for item in self.findings if not source_type or item.get("source_type") == source_type]
        return {"items": items, "total": len(items), "page": 1, "page_size": kwargs.get("page_size", 20), "has_more": False}


class _FakeHistoryStore:
    def list_evidence_manifests(self, **_kwargs):
        return {"items": [{"manifest_id": "evd_validation", "module": "validation.center"}], "total": 1}

    def list_runs(self, **_kwargs):
        return {"items": [{"run_id": "run_validation", "module": "validation_center", "level": "L2", "status": "passed"}], "total": 1}


class _FakePlanCatalog:
    def load(self):
        return {
            "plans": [
                {
                    "plan_key": "validation_center_backend",
                    "title": "Validation backend",
                    "module": "validation_center",
                    "level": "L2",
                    "nox_session": "validation_center_backend",
                }
            ]
        }


class _FakeExecutionRunner:
    def list_jobs(self, **_kwargs):
        return {"items": [{"job_id": "job_validation", "plan_key": "validation_center_backend", "status": "passed", "archive": {"evidence_manifest_path": "evd_validation"}}]}


class _FakeGitStatusProvider:
    def branch_status(self):
        return {"branch": "bug/BUG-001-validation", "head_commit": "abc123", "behind_count": 0, "data_state": "complete"}

    def workspace_status(self):
        return {
            "dirty": True,
            "files": [{"path": "backend/routers/validation.py", "primary_module": "validation.center"}],
            "summary": {"changed_files": 1},
        }


class _FakeModuleQuality:
    def module_quality_summary(self, **_kwargs):
        return {
            "summary": {"module_count": 1},
            "modules": [
                {
                    "module_id": "validation.center",
                    "display_name": "Validation Center",
                    "registry_risk_level": "critical",
                    "workspace": {"changed_file_count": 1},
                    "coverage": {"status": "missing", "line_percent": None},
                    "quality": {"finding_count": 0, "bug_count": 1, "by_severity": {"P1": 1}},
                }
            ],
            "global_reason_codes": [],
        }


class _FakeUiTargetCatalog:
    def summary(self):
        return {
            "target_count": 1,
            "nav_group_count": 1,
            "warning_count": 1,
            "targets_requiring_action": 1,
            "by_nav_group": [{"nav_group": "Validation", "target_count": 1}],
            "by_coverage_status": {"partial": 1},
            "by_risk_level": {"medium": 1},
        }

    def list_targets(self, **kwargs):
        return {"items": [{"route_id": "validation.center", "href": "/validation-center"}], "total": 1, "page": kwargs.get("page", 1), "page_size": kwargs.get("page_size", 20), "has_more": False}

    def get_target(self, route_id: str):
        return {"route_id": route_id, "href": "/validation-center"} if route_id == "validation.center" else None


class _FakeOwnership:
    def match_path(self, path: str):
        return SimpleNamespace(primary_module="validation.center", impact_modules=("validation.runner",))

    def list_rules(self):
        return [
            SimpleNamespace(
                primary_module="validation.center",
                impact_modules=("validation.runner",),
                include=("backend/routers/validation.py",),
            )
        ]


class _FakeModuleRegistry:
    def list_modules(self):
        return [SimpleNamespace(module_id="validation.center"), SimpleNamespace(module_id="validation.runner")]


def _service(repo_root: Path | None = None, extra_bugs: list[dict] | None = None) -> ValidationPipelineCenterService:
    return ValidationPipelineCenterService(
        repo_root=repo_root,
        history_store=_FakeHistoryStore(),
        plan_catalog=_FakePlanCatalog(),
        finding_store=_FakeFindingStore(
            bugs=[
                {
                    "bug_id": "BUG-001",
                    "title": "Validation scope regression",
                    "module": "validation.center",
                    "severity": "P1",
                    "status": "in_progress",
                    "allowed_write_scope": ["docs/"],
                    "required_verification": ["python -m nox -s validation_center_backend"],
                    "closure_requirements": ["record verification"],
                },
                {
                    "bug_id": "BUG-002",
                    "title": "Missing triage scope",
                    "module": "validation.center",
                    "severity": "P2",
                    "status": "detected",
                    "allowed_write_scope": [],
                },
            ] + list(extra_bugs or []),
            findings=[
                {
                    "finding_id": "legacy_1",
                    "source_type": "legacy_inventory",
                    "module": "validation.center",
                    "severity": "P2",
                    "category": "legacy",
                }
            ],
        ),
        execution_runner=_FakeExecutionRunner(),
        git_status_provider=_FakeGitStatusProvider(),
        module_quality_service=_FakeModuleQuality(),
        ui_target_catalog=_FakeUiTargetCatalog(),
        module_registry=_FakeModuleRegistry(),
        file_ownership_catalog=_FakeOwnership(),
    )


def test_merge_gate_blocks_dirty_scope_and_missing_strict_coverage() -> None:
    gate = _service().merge_gate_summary()

    assert gate["decision"] == "blocked"
    assert "workspace_dirty" in gate["blocking_reasons"]
    assert "scope_violation" in gate["blocking_reasons"]
    assert "touched_module_coverage_missing_or_stale" in gate["blocking_reasons"]
    assert "merge_to_main_requires_user_confirmation" in gate["manual_confirmations"]
    assert gate["production_8001_touched"] is False


def test_issue_workflow_and_github_sync_distinguish_missing_scope_and_links() -> None:
    service = _service()
    workflow = service.issue_workflow_summary()
    github = service.github_issues_summary()

    assert workflow["missing_scope_count"] == 1
    assert workflow["triage_only_count"] == 1
    assert github["missing_link_count"] == 1

    detail = service.issue_workflow_detail("BUG-002")
    assert detail is not None
    assert detail["gate_state"] == "triage_only_until_allowed_write_scope_is_set"


def test_issue_candidate_queue_reads_context_pack_and_dedupes_payloads(tmp_path: Path) -> None:
    candidate_dir = tmp_path / "tests" / "aistock_validation" / "runs" / "candidates"
    candidate_dir.mkdir(parents=True)
    context_pack = {
        "schema_version": "aistock_ci_failure_context_pack_v1",
        "pack_id": "CP-CI-abc123",
        "module": "validation",
        "severity": "P1",
        "problem_statement": "Nightly validation failed",
        "github_issue_number": "521",
        "github_issue_url": "https://github.com/licong01-cloud/AIstock/issues/521",
        "failure_event": {
            "event_id": "FE-CI-abc123",
            "fingerprint": "ci-abc123",
            "candidate_status": "new",
            "evidence_refs": ["https://github.com/licong01-cloud/AIstock/actions/runs/1"],
        },
        "agent_handoff": {
            "required_verification": ["python -m nox -s validation_center_backend"],
            "allowed_write_scope": ["backend/services/validation/pipeline_center.py"],
        },
    }
    github_payload = {
        "schema_version": "aistock_ci_failure_github_issue_payload_v1",
        "title": "P1 Nightly failed",
        "labels": ["P1", "severity:p1", "module:validation"],
        "dedupe": {"fingerprint": "ci-abc123", "marker": "<!-- marker -->"},
        "run_count": 2,
    }
    (candidate_dir / "context-pack.json").write_text(json.dumps(context_pack), encoding="utf-8")
    (candidate_dir / "github-payload.json").write_text(json.dumps(github_payload), encoding="utf-8")

    service = _service(repo_root=tmp_path)
    payload = service.issue_candidates(page=1, page_size=10)
    summary = service.issue_candidate_summary()

    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["candidate_id"] == "CP-CI-abc123"
    assert item["fingerprint"] == "ci-abc123"
    assert item["module_id"] == "validation"
    assert item["github_issue_number"] == "521"
    assert item["run_count"] == 2
    assert item["recommended_validation"] == ["python -m nox -s validation_center_backend"]
    assert item["allowed_write_scope"] == ["backend/services/validation/pipeline_center.py"]
    assert len(item["source_paths"]) == 2
    assert summary["candidate_count"] == 1
    assert summary["linked_issue_count"] == 1


def test_issue_candidate_queue_reads_persisted_history_without_tmp(tmp_path: Path) -> None:
    history_dir = tmp_path / "tests" / "aistock_validation" / "history" / "issue_candidates" / "nightly"
    history_dir.mkdir(parents=True)
    history_payload = {
        "schema_version": "aistock_ci_failure_candidate_history_v1",
        "created_at": "2026-06-02T00:00:00Z",
        "last_seen_at": "2026-06-02T01:00:00Z",
        "run_count": 2,
        "candidate": {
            "candidate_id": "CAND-CI-abc123",
            "title": "P1 Nightly failed",
            "module": "validation",
            "severity": "P1",
            "status": "new",
            "fingerprint": "ci-abc123",
            "allowed_write_scope": [".github/workflows/nightly.yml"],
            "required_validation": ["python -m nox -s validation_center_backend"],
            "evidence": ["https://github.com/licong01-cloud/AIstock/actions/runs/1"],
        },
    }
    (history_dir / "ci-abc123.json").write_text(json.dumps(history_payload), encoding="utf-8")

    payload = _service(repo_root=tmp_path).issue_candidates(page=1, page_size=10)

    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["candidate_id"] == "CAND-CI-abc123"
    assert item["source_type"] == "ci_candidate_history"
    assert item["run_count"] == 2
    assert item["source_path"] == "tests/aistock_validation/history/issue_candidates/nightly/ci-abc123.json"


def test_issue_candidate_queue_reads_nightly_bug_candidate_artifacts(tmp_path: Path) -> None:
    queue_dir = tmp_path / "tmp" / "validation" / "nightly_failure_issue" / "bug-candidates"
    queue_dir.mkdir(parents=True)
    candidate_payload = {
        "schema_version": "aistock_bug_candidate_v1",
        "candidate_id": "NC-20260618-abc123",
        "title": "Nightly root clean guard detected dirty path",
        "source_plan_key": "nightly_root_clean_guard",
        "module": "validation.runner",
        "severity": "P1",
        "status": "draft",
        "confidence": 0.91,
        "summary": "Nightly found a root pollution candidate.",
        "llm_hypothesis": "DeepSeek suggested the root clean guard after graph freshness changed.",
        "expected": "Nightly should keep the root checkout clean.",
        "actual": "A generated file appeared under the root checkout.",
        "verification_result": "workflow_discovery_root_clean_guard reproduced the dirty path.",
        "reproduce": ["python -m nox -s nightly_root_clean_guard"],
        "fingerprint": "nc-abc123",
        "dedupe_fingerprint": "nc-abc123",
        "evidence_refs": ["scripts/nightly_bug_candidate_queue.py"],
        "suggested_validation": ["python -m nox -s nightly_bug_candidate_queue"],
        "allowed_write_scope": ["scripts/nightly_bug_candidate_queue.py"],
        "codegraph_refs": ["tmp/validation/code-intelligence/codegraph-freshness.json"],
        "ua_refs": ["tmp/validation/code-intelligence/ua-summary-manifest.json"],
        "github_issue_payload_ref": "tmp/validation/code-intelligence/9001/bug-candidates/issue-payloads/NC-20260618-abc123.json",
        "promotion_mode": "deterministic_quality_gate",
        "llm_enhancement_opt_in": False,
        "active_discovery_reason": "root cleanliness regression",
        "quality_gate": {
            "workflow_gate": "ready",
            "issue_payload_ready": True,
            "auto_submit_allowed": False,
            "reasons": [],
        },
    }
    issue_payload = {
        "schema_version": "aistock_bug_candidate_github_issue_payload_v1",
        "title": "[P1] Nightly root clean guard detected dirty path",
        "candidate": {
            "candidate_id": "NC-20260618-abc123",
            "module": "validation.runner",
            "severity": "P1",
            "status": "draft",
            "fingerprint": "nc-abc123",
        },
        "dedupe": {"fingerprint": "nc-abc123", "marker": "<!-- aistock-nightly-bug-candidate:nc-abc123 -->"},
    }
    (queue_dir / "candidate.json").write_text(json.dumps(candidate_payload), encoding="utf-8")
    (queue_dir / "issue-payload.json").write_text(json.dumps(issue_payload), encoding="utf-8")

    payload = _service(repo_root=tmp_path).issue_candidates(page=1, page_size=10)

    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["candidate_id"] == "NC-20260618-abc123"
    assert item["source_type"] == "nightly_bug_candidate"
    assert item["fingerprint"] == "nc-abc123"
    assert item["recommended_validation"] == ["python -m nox -s nightly_bug_candidate_queue"]
    assert item["allowed_write_scope"] == ["scripts/nightly_bug_candidate_queue.py"]
    assert item["issue_payload_ready"] is True
    assert item["quality_gate_state"] == "ready"
    assert item["why_not_submitted"] == "awaiting_operator_promotion / auto_submit_disabled"
    assert item["codegraph_refs"] == ["tmp/validation/code-intelligence/codegraph-freshness.json"]
    assert item["ua_refs"] == ["tmp/validation/code-intelligence/ua-summary-manifest.json"]
    assert item["expected"] == "Nightly should keep the root checkout clean."
    assert item["actual"] == "A generated file appeared under the root checkout."
    assert item["llm_hypothesis"] == "DeepSeek suggested the root clean guard after graph freshness changed."
    assert item["verification_result"] == "workflow_discovery_root_clean_guard reproduced the dirty path."
    assert item["promotion_mode"] == "deterministic_quality_gate"
    assert item["llm_enhancement_opt_in"] is False
    assert item["active_discovery_reason"] == "root cleanliness regression"
    assert len(item["source_paths"]) == 2


def test_issue_candidate_queue_reads_code_intelligence_bug_candidate_artifacts(tmp_path: Path) -> None:
    queue_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "9001" / "bug-candidates"
    candidate_dir = queue_dir / "candidates"
    payload_dir = queue_dir / "issue-payloads"
    candidate_dir.mkdir(parents=True)
    payload_dir.mkdir(parents=True)
    candidate = {
        "schema_version": "aistock_bug_candidate_v1",
        "candidate_id": "NC-20260618-ready",
        "source_plan_key": "nightly_validation_api_probe",
        "title": "Validation API probe found stale plan catalog",
        "module": "validation.center",
        "severity": "P1",
        "status": "draft",
        "confidence": 0.94,
        "summary": "DeepSeek hypothesis was confirmed by the validation probe.",
        "expected": "Validation Center should load the requested plan catalog.",
        "actual": "The API returned plan not found.",
        "reproduce": ["python -m nox -s validation_center_backend"],
        "fingerprint": "nc-ready",
        "dedupe_fingerprint": "nc-ready",
        "evidence_refs": ["tmp/validation/code-intelligence/9001/discovery-results.json"],
        "suggested_validation": ["python -m nox -s validation_center_backend"],
        "allowed_write_scope": ["backend/services/validation/pipeline_center.py"],
        "codegraph_refs": ["tmp/validation/code-intelligence/9001/codegraph-freshness.json"],
        "ua_refs": ["tmp/validation/code-intelligence/9001/ua-summary-manifest.json"],
        "github_issue_payload_ref": "tmp/validation/code-intelligence/9001/bug-candidates/issue-payloads/NC-20260618-ready.json",
        "quality_gate": {"workflow_gate": "ready", "issue_payload_ready": True, "auto_submit_allowed": False, "reasons": []},
    }
    issue_payload = {
        "schema_version": "aistock_bug_candidate_github_issue_payload_v1",
        "candidate": candidate,
        "auto_submit_allowed": False,
        "dedupe": {"fingerprint": "nc-ready", "marker": "<!-- aistock-nightly-bug-candidate:nc-ready -->"},
    }
    (candidate_dir / "NC-20260618-ready.json").write_text(json.dumps(candidate), encoding="utf-8")
    (payload_dir / "NC-20260618-ready.json").write_text(json.dumps(issue_payload), encoding="utf-8")

    service = _service(
        repo_root=tmp_path,
        extra_bugs=[
            {
                "bug_id": "BUG-999",
                "title": "Validation API probe found stale plan catalog",
                "module": "validation.center",
                "severity": "P1",
                "status": "verified",
                "github_issue_number": 1250,
                "github_issue_url": "https://github.com/licong01-cloud/AIstock/issues/1250",
            }
        ],
    )
    payload = service.issue_candidates(page=1, page_size=10)
    summary = service.issue_candidate_summary()

    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["source_type"] == "nightly_bug_candidate"
    assert item["source_plan_key"] == "nightly_validation_api_probe"
    assert item["confidence"] == 0.94
    assert item["issue_payload_ready"] is True
    assert item["github_issue_payload_ref"].endswith("issue-payloads/NC-20260618-ready.json")
    assert item["why_not_submitted"] == "awaiting_operator_promotion / auto_submit_disabled"
    assert item["codegraph_refs"] == ["tmp/validation/code-intelligence/9001/codegraph-freshness.json"]
    assert item["ua_refs"] == ["tmp/validation/code-intelligence/9001/ua-summary-manifest.json"]
    assert summary["nightly_candidate_count"] == 1
    assert summary["issue_payload_ready_count"] == 1
    assert summary["draft_count"] == 1
    assert summary["no_submit_reason_counts"] == {"auto_submit_disabled": 1, "awaiting_operator_promotion": 1}
    assert summary["outcome_metrics"]["promoted_issue_count"] == 0
    assert summary["outcome_metrics"]["confirmed_issue_count"] == 0


def test_issue_candidate_summary_reports_discovery_outcomes(tmp_path: Path) -> None:
    queue_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "9002" / "bug-candidates"
    queue_dir.mkdir(parents=True)
    candidate = {
        "schema_version": "aistock_bug_candidate_v1",
        "candidate_id": "NC-20260618-promoted",
        "source_plan_key": "workflow_discovery_root_clean_guard",
        "title": "Root dirty file was promoted",
        "module": "validation.runner",
        "severity": "P1",
        "status": "draft",
        "confidence": 0.95,
        "summary": "A real workflow issue was found and fixed.",
        "expected": "Root stays clean.",
        "actual": "Root became dirty.",
        "reproduce": ["python -m nox -s workflow_discovery_root_clean_guard"],
        "fingerprint": "nc-promoted",
        "dedupe_fingerprint": "nc-promoted",
        "evidence_refs": ["tmp/validation/code-intelligence/9002/root-clean.json"],
        "suggested_validation": ["python -m nox -s validation_workflow_automation"],
        "allowed_write_scope": ["scripts/aistock_issue_workflow.py"],
        "github_issue_number": 1251,
        "github_issue_url": "https://github.com/licong01-cloud/AIstock/issues/1251",
        "quality_gate": {"workflow_gate": "ready", "issue_payload_ready": True, "auto_submit_allowed": False, "reasons": []},
    }
    (queue_dir / "candidate.json").write_text(json.dumps(candidate), encoding="utf-8")

    summary = _service(
        repo_root=tmp_path,
        extra_bugs=[
            {
                "bug_id": "BUG-998",
                "title": "Root dirty file was promoted",
                "module": "validation.runner",
                "severity": "P1",
                "status": "closed",
                "github_issue_number": 1251,
                "github_issue_url": "https://github.com/licong01-cloud/AIstock/issues/1251",
            }
        ],
    ).issue_candidate_summary()

    assert summary["outcome_metrics"]["candidate_count"] == 1
    assert summary["outcome_metrics"]["promoted_issue_count"] == 1
    assert summary["outcome_metrics"]["confirmed_issue_count"] == 1
    assert summary["outcome_metrics"]["confirmation_rate"] == 1.0
    assert "confirmed_nightly_issue" in summary["reason_codes"]


def test_phase1_cards_include_all_top_navigation_domains() -> None:
    cards = _service().cards_summary()["cards"]
    card_ids = {item["card_id"] for item in cards}

    assert {
        "merge_gate",
        "issue_workflow",
        "pipeline_tests",
        "features",
        "modules",
        "github_issues",
        "branches_prs",
        "legacy_debt",
        "automation",
    }.issubset(card_ids)


def test_automation_summary_blocks_github_write_without_repo_target(monkeypatch) -> None:
    service = _service()
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    service._gh_auth_state = lambda: (True, "gh ok")  # type: ignore[method-assign]
    service._github_repo_from_env_file = lambda: None  # type: ignore[method-assign]
    service._git_one = lambda _args, default=None: default  # type: ignore[method-assign]

    payload = service.automation_summary()
    github_write = next(item for item in payload["actions"] if item["action_type"] == "github_issue_write")

    assert payload["summary"]["gh_authenticated"] is True
    assert payload["summary"]["github_repository_configured"] is False
    assert payload["summary"]["github_issue_write_ready"] is False
    assert github_write["enabled"] is False
    assert "github_repository_unconfigured" in payload["reason_codes"]


def test_automation_summary_enables_github_write_with_auth_and_repo(monkeypatch) -> None:
    service = _service()
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    service._gh_auth_state = lambda: (True, "gh ok")  # type: ignore[method-assign]
    service._github_repo_from_env_file = lambda: "owner/repo"  # type: ignore[method-assign]

    payload = service.automation_summary()
    github_write = next(item for item in payload["actions"] if item["action_type"] == "github_issue_write")

    assert payload["summary"]["gh_authenticated"] is True
    assert payload["summary"]["github_repository_configured"] is True
    assert payload["summary"]["github_issue_write_ready"] is True
    assert payload["github_repository"] == "owner/repo"
    assert github_write["enabled"] is True
    assert payload["reason_codes"] == []
