from __future__ import annotations

from pathlib import Path

import pytest

import noxfile
from scripts.ci_workflow_policy_scan import (
    build_contract_evidence,
    scan_environment_contracts,
    scan_nox_text,
    scan_workflow_text,
    scan_workflows,
)


def test_policy_scan_rejects_install_and_disposable_database() -> None:
    findings = scan_workflow_text(
        """
        jobs:
          build:
            services:
              postgres:
                image: timescale/timescaledb:latest
            steps:
              - uses: actions/setup-python@v5
              - run: python -m pip install pytest
              - run: docker run postgres:16
        """,
        "test.yml",
    )

    assert len(findings) == 5
    assert {item["reason"] for item in findings} == {
        "CI workflow services are prohibited; use the existing DEV database lane",
        "disposable postgres/timescale image is prohibited in CI",
        "setup-* actions install mutable toolchains; use a prebuilt runner",
        "dependency installation is prohibited in CI",
        "creating a postgres/timescale container is prohibited in CI",
    }


def test_policy_scan_ignores_comments_and_read_only_container_probe() -> None:
    findings = scan_workflow_text(
        """
        # python -m pip install is only a historical note
        run: docker ps --filter name=timescaledb
        run: echo 'dependency installation is prohibited'
        """,
        "nightly.yml",
    )

    assert findings == []


def test_repository_workflows_pass_zero_install_and_database_service_policy() -> None:
    findings = scan_workflows(sorted(Path(".github/workflows").glob("*.yml")))

    assert findings == []


def test_nox_dependency_bootstrap_requires_ci_fail_closed_guard() -> None:
    findings = scan_nox_text('session.run("npm", "ci", external=True)')
    assert findings and findings[0]["reason"].startswith("Nox contains npm ci")


def test_repository_runner_contract_is_explicit() -> None:
    findings = scan_environment_contracts(sorted(Path(".github/workflows").glob("*.yml")))
    assert findings == []


def test_repository_contract_evidence_matches_machine_standard() -> None:
    paths = sorted(Path(".github/workflows").glob("*.yml"))
    evidence = build_contract_evidence(paths)

    assert evidence
    assert all(evidence.values())
    assert "nox_ci_install_fail_closed_guard" in evidence
    assert "pr_quality_no_external_report_action_dependency" in evidence
    assert "superseded_pr_runs_cancel_in_progress" in evidence
    assert "bounded_pr_base_fetch_retry" in evidence
    assert evidence["stable_merge_quality_contexts_are_always_published"] is True
    assert "pr_ci_no_separate_failure_publisher_job" in evidence
    assert "pr_ci_no_external_artifact_action_dependency" in evidence
    assert evidence["pr_ci_static_gate_reuses_classifier_checkout"] is True
    assert evidence["pr_ci_workflow_validation_reuses_ci_verdict_runner"] is True
    assert evidence["codeql_reuses_single_security_runner_allocation"] is True
    assert "pr_workflows_no_external_report_action_dependency" in evidence
    assert "nightly_dr_operational_lane_is_explicit_and_does_not_create_or_start_database" in evidence
    assert evidence["nightly_l3_uses_prebuilt_aistock_ci_and_linked_frontend_dependencies"] is True
    assert evidence["self_hosted_workspace_frontend_link_is_lockfile_verified_and_cleanup_safe"] is True
    assert evidence["nightly_retry_receipt_is_repo_scoped_bound_and_fail_closed"] is True
    assert evidence["nightly_retries_failed_or_missing_sessions_plus_new_impact"] is True
    assert evidence["nightly_change_scoped_l0_uses_explicit_receipt_paths"] is True
    assert evidence["bounded_dual_runner_roles"] is True
    assert evidence["policy_evidence_remains_one_scanner_step"] is True
    assert evidence["javascript_actions_use_approved_native_node24_majors"] is True


def test_dual_runner_policy_does_not_lock_nightly_job_count(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "nightly.yml":
            text += (
                "\n  future-ordinary-lane:\n"
                "    runs-on: [self-hosted, Windows, aistock-ci]\n"
                "    steps:\n"
                "      - run: echo future\n"
            )
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["bounded_dual_runner_roles"] is True


def test_workflow_validation_runner_reacquisition_is_detected(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "test.yml":
            text = text.replace(
                "\n  ci-verdict:\n",
                "\n  workflow-validation-tests:\n"
                "    name: Workflow validation fast lane\n"
                "    runs-on: [self-hosted, Windows, aistock-ci]\n"
                "    steps:\n"
                "      - run: echo separate runner allocation\n"
                "\n  ci-verdict:\n",
                1,
            )
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["pr_ci_workflow_validation_reuses_ci_verdict_runner"] is False


def test_codeql_security_runner_reacquisition_is_detected(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "codeql.yml":
            text = text.replace(
                "jobs:\n",
                "jobs:\n"
                "  analyze:\n"
                "    runs-on: [self-hosted, Windows, aistock-ci-security]\n"
                "    steps:\n"
                "      - run: echo separate runner allocation\n\n",
                1,
            )
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["codeql_reuses_single_security_runner_allocation"] is False


def test_ci_verdict_owns_workflow_validation_and_fails_closed() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    jobs = workflow["jobs"]

    assert "workflow-validation-tests" not in jobs
    verdict = jobs["ci-verdict"]
    assert "workflow-validation-tests" not in verdict["needs"]
    assert verdict["timeout-minutes"] == 15

    steps = verdict["steps"]
    workflow_test = next(step for step in steps if step.get("id") == "workflow_validation")
    workflow_policy = next(step for step in steps if step.get("id") == "workflow_policy")
    final_verdict = next(step for step in steps if step.get("name") == "Require every selected CI lane to pass")

    assert "AISTOCK_USER_BACKEND_PROCESS_CONTROL" in workflow_test["env"]
    assert workflow_test["env"]["AISTOCK_USER_BACKEND_PROCESS_CONTROL"] == "forbidden"
    assert "ci_workflow_policy_scan.py" in workflow_policy["run"]
    assert final_verdict["if"] == "always()"
    assert final_verdict["env"]["WORKFLOW_TEST_RESULT"] == "${{ steps.workflow_validation.outcome }}"
    assert final_verdict["env"]["WORKFLOW_POLICY_RESULT"] == "${{ steps.workflow_policy.outcome }}"
    assert "workflow_validation=${WORKFLOW_TEST_RESULT}" in final_verdict["run"]
    assert "workflow_policy=${WORKFLOW_POLICY_RESULT}" in final_verdict["run"]


def test_merge_quality_contract_detects_issue_workflow_name_drift(tmp_path: Path) -> None:
    issue_workflow = tmp_path / "aistock_issue_workflow.py"
    issue_workflow.write_text('MERGE_QUALITY_CHECK_CONTEXTS = ("CI verdict",)\n', encoding="utf-8")

    evidence = build_contract_evidence(
        sorted(Path(".github/workflows").glob("*.yml")),
        issue_workflow_path=issue_workflow,
    )

    assert evidence["stable_merge_quality_contexts_are_always_published"] is False


def test_ci_standard_declares_direct_codeql_and_current_efficiency_contracts() -> None:
    import yaml

    standard_path = Path("docs/standards/aistock_development_standard_v1.5_20260523.md")
    catalog_path = Path("docs/standards/aistock_development_standard_v1.5_20260523.yaml")
    standard = standard_path.read_text(encoding="utf-8")
    catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    rule = next(item for item in catalog["rules"] if item["rule_id"] == "CI-ENVIRONMENT-PARITY-001")
    required = set(rule["checker"]["required_evidence"])
    expected = {
        "codeql_remote_action_download_is_eliminated",
        "codeql_exact_local_workspace_fetch_is_bounded",
        "codeql_pr_test_only_analysis_is_skipped_without_weakening_main_push",
        "code_intelligence_refresh_is_scheduled_or_manual_only",
        "code_intelligence_refresh_has_no_external_artifact_action_dependency",
        "javascript_actions_use_approved_native_node24_majors",
        "stable_merge_quality_contexts_are_always_published",
        "bounded_dual_runner_roles",
        "policy_evidence_remains_one_scanner_step",
        "pr_ci_static_gate_reuses_classifier_checkout",
        "pr_ci_workflow_validation_reuses_ci_verdict_runner",
        "codeql_reuses_single_security_runner_allocation",
    }

    assert expected <= required
    assert "immutable CodeQL Action release" not in standard
    assert "禁止使用 `github/codeql-action`、`actions/checkout` 或其他远端 `uses:`" in standard
    for action_ref in (
        "actions/checkout@v7",
        "actions/upload-artifact@v7",
        "actions/download-artifact@v8",
        "actions/github-script@v9",
    ):
        assert action_ref in standard


def test_nox_frontend_dependency_gap_fails_closed_in_ci(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frontend = tmp_path / "frontend"
    frontend.mkdir()
    monkeypatch.setattr(noxfile, "ROOT", tmp_path)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")

    class Session:
        def error(self, message: str) -> None:
            raise RuntimeError(message)

    with pytest.raises(RuntimeError, match="cannot run npm ci"):
        noxfile._ensure_frontend_node_modules(Session())  # type: ignore[arg-type]
