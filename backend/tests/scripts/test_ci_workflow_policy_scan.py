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
    assert "pr_ci_no_separate_failure_publisher_job" in evidence
    assert "pr_ci_no_external_artifact_action_dependency" in evidence
    assert "nightly_dr_operational_lane_is_explicit_and_does_not_create_or_start_database" in evidence


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
