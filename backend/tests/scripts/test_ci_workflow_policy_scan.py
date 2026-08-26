from __future__ import annotations

from pathlib import Path

from scripts.ci_workflow_policy_scan import scan_workflow_text, scan_workflows


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
