from __future__ import annotations

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


def _service() -> ValidationPipelineCenterService:
    return ValidationPipelineCenterService(
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
            ],
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
