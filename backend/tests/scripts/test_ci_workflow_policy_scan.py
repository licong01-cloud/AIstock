from __future__ import annotations

import json
import shutil
import subprocess
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


def test_self_hosted_checkout_requires_verified_git_mirror_with_bounded_fallback(tmp_path: Path) -> None:
    workflow = tmp_path / "test.yml"
    workflow.write_text(
        "env:\n"
        "  GIT_HTTP_LOW_SPEED_LIMIT: '524288'\n"
        "  GIT_HTTP_LOW_SPEED_TIME: '30'\n"
        "  GIT_CONFIG_COUNT: '1'\n"
        "  GIT_CONFIG_KEY_0: http.version\n"
        "  GIT_CONFIG_VALUE_0: HTTP/1.1\n"
        "jobs:\n"
        "  scan:\n"
        "    runs-on: [self-hosted, Windows, aistock-ci]\n"
        "    steps:\n"
        "      - uses: actions/checkout@v8\n"
        "        timeout-minutes: 5\n",
        encoding="utf-8",
    )

    findings = scan_environment_contracts([workflow])

    assert any("must use a verified local Git object mirror" in item["reason"] for item in findings)


def test_self_hosted_workflow_must_bound_stalled_or_unusably_slow_git_http_transfer(tmp_path: Path) -> None:
    workflow = tmp_path / "codeql.yml"
    workflow.write_text(
        "env:\n"
        "jobs:\n"
        "  scan:\n"
        "    runs-on: [self-hosted, Windows, aistock-ci-security]\n",
        encoding="utf-8",
    )

    findings = scan_environment_contracts([workflow])

    assert any("must bound stalled or unusably slow Git HTTP transfers" in item["reason"] for item in findings)


def test_contract_evidence_rejects_unbounded_self_hosted_git_http(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "test.yml":
            text = text.replace("  GIT_HTTP_LOW_SPEED_TIME: '30'\n", "", 1)
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["self_hosted_git_http_stalls_are_bounded"] is False


def test_contract_evidence_rejects_previous_ineffective_git_http_threshold(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "test.yml":
            text = text.replace("  GIT_HTTP_LOW_SPEED_LIMIT: '524288'\n", "  GIT_HTTP_LOW_SPEED_LIMIT: '1'\n", 1)
            text = text.replace("  GIT_HTTP_LOW_SPEED_TIME: '30'\n", "  GIT_HTTP_LOW_SPEED_TIME: '60'\n", 1)
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["self_hosted_git_http_stalls_are_bounded"] is False


def test_self_hosted_checkout_requires_hard_timeout_and_literal_pack_cleanup(tmp_path: Path) -> None:
    workflow = tmp_path / "test.yml"
    workflow.write_text(
        "env:\n"
        "  GIT_HTTP_LOW_SPEED_LIMIT: '524288'\n"
        "  GIT_HTTP_LOW_SPEED_TIME: '30'\n"
        "  GIT_CONFIG_COUNT: '1'\n"
        "  GIT_CONFIG_KEY_0: http.version\n"
        "  GIT_CONFIG_VALUE_0: HTTP/1.1\n"
        "jobs:\n"
        "  ci:\n"
        "    runs-on:\n"
        "      - self-hosted\n"
        "      - Windows\n"
        "      - aistock-ci\n"
        "    steps:\n"
        "      - uses: actions/checkout@v8\n",
        encoding="utf-8",
    )

    findings = scan_environment_contracts([workflow])

    reasons = {item["reason"] for item in findings}
    assert "self-hosted actions/checkout must have a five-minute hard step timeout" in reasons
    assert (
        "self-hosted checkout must reclaim interrupted Git pack fragments by literal path without blocking the PR"
        in reasons
    )


def test_contract_evidence_rejects_vacuous_checkout_discovery(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8").replace("actions/checkout@v7", "local/checkout@v7")
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["self_hosted_checkout_steps_have_hard_timeout"] is False
    assert evidence["self_hosted_checkout_cleans_interrupted_pack_fragments"] is False


def test_contract_evidence_accepts_a_checkout_action_version_upgrade(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8").replace("actions/checkout@v7", "actions/checkout@v8")
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["self_hosted_checkout_steps_have_hard_timeout"] is True
    assert evidence["self_hosted_checkout_cleans_interrupted_pack_fragments"] is True


def test_checkout_and_cleanup_contracts_are_bound_to_each_exact_step(tmp_path: Path) -> None:
    workflow = tmp_path / "test.yml"
    workflow.write_text(
        "env:\n"
        "  GIT_HTTP_LOW_SPEED_LIMIT: '524288'\n"
        "  GIT_HTTP_LOW_SPEED_TIME: '30'\n"
        "  GIT_CONFIG_COUNT: '1'\n"
        "  GIT_CONFIG_KEY_0: http.version\n"
        "  GIT_CONFIG_VALUE_0: HTTP/1.1\n"
        "jobs:\n"
        "  ci:\n"
        "    runs-on: [self-hosted, Windows, aistock-ci]\n"
        "    steps:\n"
        "      - uses: actions/checkout@v8\n"
        "        timeout-minutes: 5\n"
        "      - uses: actions/checkout@v9\n"
        "      - name: Unrelated tolerated diagnostic\n"
        "        continue-on-error: true\n"
        "        run: echo diagnostic\n"
        "      - name: Reclaim interrupted Git pack fragments\n"
        "        shell: powershell\n"
        "        run: |\n"
        "          Get-ChildItem -LiteralPath $packRoot -File -Filter 'tmp_pack_*'\n"
        "          Remove-Item -LiteralPath $fragment.FullName -Force -ErrorAction Stop\n",
        encoding="utf-8",
    )

    findings = scan_environment_contracts([workflow])
    reasons = [item["reason"] for item in findings]

    assert reasons.count("self-hosted actions/checkout must have a five-minute hard step timeout") == 1
    assert (
        "self-hosted checkout must reclaim interrupted Git pack fragments by literal path without blocking the PR"
        in reasons
    )


def test_repository_contract_evidence_matches_machine_standard() -> None:
    paths = sorted(Path(".github/workflows").glob("*.yml"))
    evidence = build_contract_evidence(paths)

    assert evidence
    assert all(evidence.values())
    assert "nox_ci_install_fail_closed_guard" in evidence
    assert "pr_quality_no_external_report_action_dependency" in evidence
    assert "superseded_pr_runs_cancel_in_progress" in evidence
    assert "bounded_pr_base_fetch_retry" in evidence
    assert evidence["merge_quality_contexts_are_change_scoped"] is True
    assert "pr_ci_no_separate_failure_publisher_job" in evidence
    assert "pr_ci_no_external_artifact_action_dependency" in evidence
    assert evidence["pr_ci_static_gate_reuses_classifier_checkout"] is True
    assert evidence["pr_ci_selected_lanes_reuse_ci_verdict_runner"] is True
    assert evidence["changed_tests_reachable_from_selected_ci_plan"] is True
    assert evidence["dependency_update_pr_validation_reuses_ci_verdict"] is True
    assert evidence["pr_ci_frontend_dependencies_are_lockfile_matched_after_checkout"] is True
    assert evidence["codeql_reuses_single_security_runner_allocation"] is True
    assert evidence["codeql_bundle_path_is_runner_independent"] is True
    assert evidence["security_workflows_fail_fast_before_runner_allocation"] is True
    assert evidence["nightly_code_intelligence_has_single_scheduled_owner"] is True
    assert evidence["nightly_preflight_requires_distinct_runner_roles"] is True
    assert evidence["redundant_issue_event_workflows_retired"] is True
    assert "pr_workflows_no_external_report_action_dependency" in evidence
    assert "nightly_dr_operational_lane_is_explicit_and_does_not_create_or_start_database" in evidence
    assert evidence["nightly_l3_uses_prebuilt_aistock_ci_and_linked_frontend_dependencies"] is True
    assert evidence["self_hosted_workspace_frontend_link_is_lockfile_verified_and_cleanup_safe"] is True
    assert evidence["nightly_retry_receipt_is_repo_scoped_bound_and_fail_closed"] is True
    assert evidence["nightly_retries_failed_or_missing_sessions_plus_new_impact"] is True
    assert evidence["nightly_change_scoped_l0_uses_explicit_receipt_paths"] is True
    assert evidence["bounded_dual_runner_roles"] is True
    assert evidence["runner_lifecycle_is_pinned_and_supervised"] is True
    assert evidence["self_hosted_workflows_use_verified_git_object_mirror"] is True
    assert evidence["git_object_mirror_maintenance_is_bounded_and_offline"] is True
    assert evidence["self_hosted_git_http_stalls_are_bounded"] is True
    assert evidence["pr_ci_heavy_lanes_short_circuit_after_prerequisites"] is True
    assert evidence["selected_validation_plans_are_subsumed_once"] is True
    assert evidence["policy_evidence_remains_one_scanner_step"] is True
    assert evidence["javascript_actions_use_approved_native_node24_majors"] is True


def test_runner_lifecycle_contract_rejects_missing_supervisor(tmp_path: Path) -> None:
    supervisor = tmp_path / "missing-supervisor.ps1"

    evidence = build_contract_evidence(
        sorted(Path(".github/workflows").glob("*.yml")),
        runner_supervisor_path=supervisor,
    )

    assert evidence["runner_lifecycle_is_pinned_and_supervised"] is False


def test_git_object_mirror_contract_rejects_missing_maintenance_helper(tmp_path: Path) -> None:
    evidence = build_contract_evidence(
        sorted(Path(".github/workflows").glob("*.yml")),
        git_mirror_maintenance_path=tmp_path / "missing-mirror-helper.ps1",
    )

    assert evidence["git_object_mirror_maintenance_is_bounded_and_offline"] is False


def test_git_object_mirror_preflight_does_not_probe_an_empty_workspace_with_git(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "test.yml":
            text = text.replace(
                "$workspaceHead = Join-Path $env:GITHUB_WORKSPACE '.git\\HEAD'",
                "& git -C $env:GITHUB_WORKSPACE rev-parse --verify 'HEAD^{commit}'",
                1,
            )
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["self_hosted_workflows_use_verified_git_object_mirror"] is False


def test_git_object_mirror_helper_builds_from_aligned_local_main_without_network(tmp_path: Path) -> None:
    powershell = shutil.which("powershell.exe") or shutil.which("powershell")
    if not powershell:
        pytest.skip("PowerShell helper is Windows-only")
    source = tmp_path / "source"
    allowed = tmp_path / "prebuilt"
    mirror = allowed / "git" / "AIstock.git"
    source.mkdir()

    def git(*args: str, cwd: Path = source) -> str:
        completed = subprocess.run(
            ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
        )
        return completed.stdout.strip()

    git("init", "--initial-branch=main")
    git("config", "user.email", "ci@example.invalid")
    git("config", "user.name", "CI")
    git("remote", "add", "origin", "https://github.com/licong01-cloud/AIstock.git")
    (source / "tracked.txt").write_text("mirror seed\n", encoding="utf-8")
    git("add", "tracked.txt")
    git("commit", "-m", "seed")
    main_sha = git("rev-parse", "HEAD")
    git("update-ref", "refs/remotes/origin/main", main_sha)

    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            "scripts/maintain_aistock_git_mirror.ps1",
            "-SourceRoot",
            str(source),
            "-MirrorRoot",
            str(mirror),
            "-AllowedRoot",
            str(allowed),
            "-Apply",
            "-Json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(completed.stdout)
    manifest = json.loads(Path(payload["manifest_path"]).read_text(encoding="utf-8-sig"))
    assert payload["status"] == "ready"
    assert payload["main_sha"] == main_sha
    assert payload["network_accessed"] is False
    assert payload["process_control_performed"] is False
    assert manifest["repository"] == "licong01-cloud/AIstock"
    assert git("-C", str(mirror), "rev-parse", "refs/heads/main") == main_sha


def test_heavy_lane_short_circuit_contract_rejects_direct_backend_execution(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "test.yml":
            text = text.replace(
                "steps.prerequisite_gate.outputs.heavy_lanes_allowed == 'true'",
                "steps.classify.outcome == 'success'",
            )
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["pr_ci_heavy_lanes_short_circuit_after_prerequisites"] is False


def test_pr_ci_frontend_dependency_attach_cannot_be_removed(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "test.yml":
            text = text.replace("            --attach-frontend-only `\n", "", 1)
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["pr_ci_frontend_dependencies_are_lockfile_matched_after_checkout"] is False


def test_linux_runner_exception_is_bounded_to_close_sync_metadata(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "test.yml":
            text = text.replace(
                "runner_kind=github_hosted_metadata",
                "runner_kind=unexpected_metadata",
                1,
            )
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["no_linux_or_production_environment_fallback"] is False


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


def test_security_runner_preflight_contract_rejects_direct_queueing(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "codeql.yml":
            text = text.replace("    needs: security-runner-preflight\n", "", 1)
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["security_workflows_fail_fast_before_runner_allocation"] is False
    assert evidence["no_linux_or_production_environment_fallback"] is False


def test_nightly_receipt_policy_rejects_non_scalar_candidate_stream(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "nightly.yml":
            text = text.replace(
                "--jq '.[] | [.databaseId, .headSha] | @tsv'",
                "--jq '.'",
                1,
            )
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["nightly_retry_receipt_is_repo_scoped_bound_and_fail_closed"] is False


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

    assert evidence["pr_ci_selected_lanes_reuse_ci_verdict_runner"] is False


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


def test_codeql_bundle_path_cannot_depend_on_a_runner_workspace(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "codeql.yml":
            text = text.replace(
                "prebuilt\\CodeQL\\2.26.3\\x64\\codeql",
                "aistock\\_work\\_tool\\CodeQL\\2.26.3\\x64\\codeql",
                1,
            )
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["codeql_bundle_path_is_runner_independent"] is False


def test_ci_verdict_owns_workflow_validation_and_fails_closed() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    jobs = workflow["jobs"]

    assert set(jobs) == {"ci-verdict"}
    verdict = jobs["ci-verdict"]
    assert "needs" not in verdict
    assert verdict["timeout-minutes"] == 120

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
    assert 'failures+=("dev_db=' not in final_verdict["run"]
    assert "### External DEV database validation" in final_verdict["run"]
    assert "does not run database DDL/DML" in final_verdict["run"]


def test_dev_db_requirement_cannot_be_reintroduced_as_ci_lane_failure(tmp_path: Path) -> None:
    for source in Path(".github/workflows").glob("*.yml"):
        text = source.read_text(encoding="utf-8")
        if source.name == "test.yml":
            text = text.replace(
                'echo "### External DEV database validation"',
                'failures+=("dev_db=external_DEV_validation_required:${DEV_DB_PLANS}")',
                1,
            )
        (tmp_path / source.name).write_text(text, encoding="utf-8")

    evidence = build_contract_evidence(sorted(tmp_path.glob("*.yml")))

    assert evidence["existing_dev_database_lane_reference"] is False


def test_merge_quality_contract_detects_issue_workflow_name_drift(tmp_path: Path) -> None:
    issue_workflow = tmp_path / "aistock_issue_workflow.py"
    issue_workflow.write_text(
        'MERGE_QUALITY_CHECK_CONTEXTS = ("CI verdict", "CodeQL verdict")\n',
        encoding="utf-8",
    )

    evidence = build_contract_evidence(
        sorted(Path(".github/workflows").glob("*.yml")),
        issue_workflow_path=issue_workflow,
    )

    assert evidence["merge_quality_contexts_are_change_scoped"] is False


def test_codeql_is_nightly_only_before_runner_allocation() -> None:
    import yaml

    for workflow_name in ("pr-quality.yml", "semgrep.yml"):
        workflow = yaml.safe_load(Path(".github/workflows", workflow_name).read_text(encoding="utf-8"))
        assert set(workflow[True]) == {"workflow_dispatch"}

    codeql = yaml.safe_load(Path(".github/workflows/codeql.yml").read_text(encoding="utf-8"))
    assert set(codeql[True]) == {"schedule", "workflow_dispatch"}
    assert codeql[True]["schedule"] == [{"cron": "27 20 * * *"}]
    assert list(codeql["jobs"]) == ["security-runner-preflight", "codeql-nightly"]
    assert codeql["jobs"]["security-runner-preflight"]["runs-on"] == "ubuntu-latest"
    assert codeql["jobs"]["codeql-nightly"]["needs"] == "security-runner-preflight"
    assert codeql["jobs"]["codeql-nightly"]["name"] == "CodeQL nightly full scan"


def test_code_intelligence_and_nightly_do_not_schedule_the_same_refresh() -> None:
    import yaml

    nightly = yaml.safe_load(Path(".github/workflows/nightly.yml").read_text(encoding="utf-8"))
    refresh = yaml.safe_load(Path(".github/workflows/code-intelligence-refresh.yml").read_text(encoding="utf-8"))

    assert refresh[True]["schedule"] == [{"cron": "30 18 * * *"}]
    assert refresh["jobs"]["refresh-after-main"]["needs"] == "security-runner-preflight"
    assert "concurrency" not in refresh
    assert refresh["jobs"]["refresh-after-main"]["concurrency"] == {
        "group": "code-intelligence-refresh-main",
        "cancel-in-progress": True,
    }
    legacy = nightly["jobs"]["code-intelligence-weekly"]
    assert legacy["if"] == "github.event_name == 'workflow_dispatch' && inputs.run_code_intelligence"
    assert legacy["concurrency"]["cancel-in-progress"] is True
    assert nightly[True]["workflow_dispatch"]["inputs"]["run_code_intelligence"]["default"] is False


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
        "codeql_daily_nightly_full_scan",
        "codeql_pr_merge_gate_removed",
        "code_intelligence_refresh_is_scheduled_or_manual_only",
        "code_intelligence_refresh_has_no_external_artifact_action_dependency",
        "security_workflows_fail_fast_before_runner_allocation",
        "nightly_code_intelligence_has_single_scheduled_owner",
        "nightly_preflight_requires_distinct_runner_roles",
        "redundant_issue_event_workflows_retired",
        "javascript_actions_use_approved_native_node24_majors",
        "merge_quality_contexts_are_change_scoped",
        "bounded_dual_runner_roles",
        "self_hosted_git_http_stalls_are_bounded",
        "self_hosted_checkout_steps_have_hard_timeout",
        "self_hosted_checkout_cleans_interrupted_pack_fragments",
        "runner_lifecycle_is_pinned_and_supervised",
        "policy_evidence_remains_one_scanner_step",
        "pr_ci_static_gate_reuses_classifier_checkout",
        "pr_ci_selected_lanes_reuse_ci_verdict_runner",
        "changed_tests_reachable_from_selected_ci_plan",
        "dependency_update_pr_validation_reuses_ci_verdict",
        "codeql_reuses_single_security_runner_allocation",
        "codeql_bundle_path_is_runner_independent",
    }

    assert expected <= required
    assert "immutable CodeQL Action release" not in standard
    assert "GIT_HTTP_LOW_SPEED_LIMIT=524288" in standard
    assert "GIT_HTTP_LOW_SPEED_TIME=30" in standard
    assert "GIT_CONFIG_KEY_0=http.version" in standard
    assert "GIT_CONFIG_VALUE_0=HTTP/1.1" in standard
    assert "`actions/checkout` step 必须设置 `timeout-minutes: 5`" in standard
    assert "`tmp_pack_*`" in standard
    assert "`continue-on-error: true`" in standard
    assert "不得覆盖业务测试结论或新增合入门禁" in standard
    assert "该设置不是新的 PR 门禁" in standard
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
