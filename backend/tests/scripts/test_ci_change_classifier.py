from __future__ import annotations

import json
from pathlib import Path

from scripts import ci_change_classifier as classifier


def _write_bug(
    path: Path,
    *,
    status: str = "fixed",
    module: str = "validation",
    allowed_write_scope: list[str] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "bug_id": "BUG-191",
                "status": status,
                "module": module,
                "allowed_write_scope": allowed_write_scope or [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_close_sync_bug_json_skips_backend_matrix(tmp_path: Path) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-example.json"
    _write_bug(bug, status="fixed")

    payload = classifier.classify_changed_files(
        ["tests/aistock_validation/bugs/20260601_BUG-191-example.json"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "close_sync_metadata_only"
    assert payload["close_sync_metadata_only"] is True
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is False
    assert payload["static_gate_required"] is True
    assert payload["pr_quality_required"] is True


def test_open_bug_registry_change_keeps_backend_matrix(tmp_path: Path) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-example.json"
    _write_bug(bug, status="open")

    payload = classifier.classify_changed_files(
        ["tests/aistock_validation/bugs/20260601_BUG-191-example.json"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "full_ci_required"
    assert payload["backend_required"] is True
    assert any("status=open" in reason for reason in payload["reasons"])


def test_non_registry_change_keeps_backend_matrix(tmp_path: Path) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-example.json"
    _write_bug(bug, status="fixed")

    payload = classifier.classify_changed_files(
        [
            "tests/aistock_validation/bugs/20260601_BUG-191-example.json",
            "scripts/aistock_issue_workflow.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "full_ci_required"
    assert payload["backend_required"] is True
    assert payload["non_bug_registry_files"] == ["scripts/aistock_issue_workflow.py"]


def test_workflow_validation_only_uses_focused_fast_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            ".github/workflows/dependency-update-validate.yml",
            ".github/workflows/test.yml",
            ".github/requirements/pr-quality.txt",
            ".github/requirements/semgrep.txt",
            "scripts/ci_change_classifier.py",
            "backend/tests/scripts/test_ci_change_classifier.py",
            "docs/architecture/aistock_pr_quality_p0p1_evidence_gate_design_20260602.md",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True
    assert payload["prompt_evaluation_required"] is False


def test_docs_fast_update_skips_code_validation(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["docs/analysis/example.md", "docs/design/example.md"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "docs_fast_update"
    assert payload["docs_fast_tier"] == "docs_fast_update"
    assert payload["docs_fast_required"] is True
    assert payload["docs_controlled_required"] is False
    assert payload["backend_required"] is False
    assert payload["static_gate_required"] is False


def test_docs_fast_new_records_new_doc_tier(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["docs/handoff/new-handoff.md"],
        repo_root=tmp_path,
        added_files=["docs/handoff/new-handoff.md"],
    )

    assert payload["classification"] == "docs_fast_new"
    assert payload["docs_fast_tier"] == "docs_fast_new"
    assert payload["backend_required"] is False


def test_docs_controlled_keeps_normal_guardrails(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["docs/standards/aistock_issue_workflow_quickstart.md", "AGENTS.md"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "docs_controlled"
    assert payload["docs_fast_required"] is False
    assert payload["docs_controlled_required"] is True
    assert payload["backend_required"] is True
    assert payload["static_gate_required"] is True


def test_unrelated_workflow_validation_change_does_not_run_prompt_evaluation(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            ".github/workflows/pr-quality.yml",
            "scripts/issue_flow.py",
            "backend/tests/scripts/test_issue_flow.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True
    assert payload["prompt_evaluation_required"] is False
    assert payload["close_sync_metadata_only"] is False


def test_code_intelligence_nightly_workflow_change_uses_focused_fast_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            ".github/workflows/nightly.yml",
            "scripts/code_intelligence_adapter.py",
            "backend/tests/scripts/test_code_intelligence_adapter.py",
            "docs/standards/aistock_issue_workflow_quickstart.md",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True


def test_validation_llm_prompt_pack_change_uses_focused_fast_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "prompt_packs/validation_llm/triage_failure.prompt.yml",
            "prompt_packs/validation_llm/design_drift_audit.prompt.yml",
            "prompt_packs/validation_llm/silent_degradation_audit.prompt.yml",
            "prompt_packs/validation_llm/evaluation_cases/historical_failure_fixtures.json",
            "configs/validation/llm_triage.yaml",
            "configs/validation/design_drift_audit.yaml",
            "configs/validation/silent_degradation_audit.yaml",
            "docs/operations/validation_llm_guarded_rollout_runbook_20260609.md",
            "scripts/llm_provider_adapter.py",
            "scripts/nightly_adaptive_scheduler.py",
            "scripts/nightly_design_drift_audit.py",
            "scripts/nightly_silent_degradation_audit.py",
            "backend/tests/scripts/test_llm_provider_adapter.py",
            "backend/tests/scripts/test_nightly_adaptive_scheduler.py",
            "backend/tests/scripts/test_nightly_design_drift_audit.py",
            "backend/tests/scripts/test_nightly_silent_degradation_audit.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True


def test_issue_on_test_fail_workflow_change_uses_focused_fast_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            ".github/workflows/issue-on-test-fail.yml",
            "scripts/ci_failure_issue_summary.py",
            "backend/tests/scripts/test_ci_failure_issue_summary.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True


def test_workflow_validation_only_allows_same_task_bug_metadata(tmp_path: Path) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260604_BUG-257-workflow-fast-lane.json"
    allocator_rel = "tests/aistock_validation/bugs/.bug_id_allocator.json"
    bug = tmp_path / bug_rel
    allocator = tmp_path / allocator_rel
    _write_bug(
        bug,
        status="in_progress",
        module="validation",
        allowed_write_scope=[
            "scripts/ci_change_classifier.py",
            "backend/tests/scripts/test_ci_change_classifier.py",
            bug_rel,
            allocator_rel,
        ],
    )
    allocator.write_text(json.dumps({"last_allocated": 257}), encoding="utf-8")

    payload = classifier.classify_changed_files(
        [
            "scripts/ci_change_classifier.py",
            "backend/tests/scripts/test_ci_change_classifier.py",
            bug_rel,
            allocator_rel,
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True
    assert payload["workflow_bug_metadata_files"] == [bug_rel]


def test_workflow_validation_only_allows_fixed_same_task_bug_metadata_and_client_wrappers(tmp_path: Path) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260605_BUG-266-workflow-fast-lane.json"
    allocator_rel = "tests/aistock_validation/bugs/.bug_id_allocator.json"
    bug = tmp_path / bug_rel
    allocator = tmp_path / allocator_rel
    _write_bug(
        bug,
        status="fixed",
        module="validation",
        allowed_write_scope=[
            "scripts/aistock_issue_workflow.py",
            "backend/tests/scripts/test_aistock_issue_workflow.py",
            ".codex/skills/fix-aistock-issue/SKILL.md",
            ".claude/commands/fix-aistock-issue.md",
            bug_rel,
            allocator_rel,
        ],
    )
    allocator.write_text(json.dumps({"last_allocated": 266}), encoding="utf-8")

    payload = classifier.classify_changed_files(
        [
            "scripts/aistock_issue_workflow.py",
            "backend/tests/scripts/test_aistock_issue_workflow.py",
            ".codex/skills/fix-aistock-issue/SKILL.md",
            ".claude/commands/fix-aistock-issue.md",
            bug_rel,
            allocator_rel,
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True
    assert payload["workflow_bug_metadata_files"] == [bug_rel]


def test_workflow_bug_metadata_with_business_scope_keeps_backend_matrix(tmp_path: Path) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260604_BUG-258-business-scope.json"
    bug = tmp_path / bug_rel
    _write_bug(
        bug,
        status="in_progress",
        module="validation",
        allowed_write_scope=[
            "scripts/ci_change_classifier.py",
            "backend/routers/validation.py",
            bug_rel,
        ],
    )

    payload = classifier.classify_changed_files(
        [
            "scripts/ci_change_classifier.py",
            bug_rel,
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "full_ci_required"
    assert payload["backend_required"] is True
    assert payload["workflow_validation_required"] is False


def test_workflow_validation_fast_lane_rejects_business_files(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "scripts/aistock_issue_workflow.py",
            "backend/routers/validation.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "full_ci_required"
    assert payload["backend_required"] is True
    assert payload["workflow_validation_required"] is False


def test_github_workflow_wires_workflow_validation_fast_lane() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    jobs = workflow["jobs"]

    assert jobs["classify-changes"]["outputs"]["workflow_validation_required"].endswith(
        "steps.classify.outputs.workflow_validation_required }}"
    )
    assert jobs["classify-changes"]["outputs"]["prompt_evaluation_required"].endswith(
        "steps.classify.outputs.prompt_evaluation_required }}"
    )
    assert jobs["backend-tests"]["if"] == "needs.classify-changes.outputs.backend_required != 'false'"
    assert jobs["workflow-validation-tests"]["if"] == (
        "needs.classify-changes.outputs.workflow_validation_required == 'true'"
    )
    prompt_eval = jobs["prompt-evaluation"]
    assert prompt_eval["if"] == "needs.classify-changes.outputs.prompt_evaluation_required == 'true'"
    prompt_eval_run_steps = "\n".join(str(step.get("run", "")) for step in prompt_eval["steps"])
    assert "scripts/llm_provider_adapter.py --json prompt-evaluation" in prompt_eval_run_steps
    assert "prompt-evaluation" in jobs["failure-bug-register"]["needs"]
    assert "workflow-validation-tests" in jobs["failure-bug-register"]["needs"]


def test_static_gate_uses_registry_metadata_fast_lane() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    static_gate_steps = workflow["jobs"]["static-gate"]["steps"]
    registry_steps = [
        step
        for step in static_gate_steps
        if isinstance(step, dict) and str(step.get("name") or "") == "BUG registry metadata check"
    ]

    assert len(registry_steps) == 1
    assert registry_steps[0]["if"] == "needs.classify-changes.outputs.close_sync_metadata_only == 'true'"
    assert "scripts/bug_registry_metadata_check.py" in registry_steps[0]["run"]
    assert "--close-sync-only" in registry_steps[0]["run"]

    nox_steps = [
        step
        for step in static_gate_steps
        if isinstance(step, dict) and str(step.get("name") or "").startswith("nox -s ")
    ]
    assert nox_steps
    assert all("close_sync_metadata_only != 'true'" in str(step.get("if") or "") for step in nox_steps)


def test_pr_quality_has_single_lane_and_registry_sync_record() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/pr-quality.yml").read_text(encoding="utf-8"))
    steps = workflow["jobs"]["pr-quality"]["steps"]
    names = [str(step.get("name") or "") for step in steps if isinstance(step, dict)]

    assert names.count("Detect PR quality lane") == 1
    assert names.count("Build registry-sync quality record") == 1
    assert names.count("Comment PR summary") == 1
    assert names.count("Upload PR quality artifacts") == 1
    assert not any("Legacy" in name for name in names)

    registry_step = next(step for step in steps if isinstance(step, dict) and step.get("name") == "Build registry-sync quality record")
    assert registry_step["if"] == "steps.quality_lane.outputs.registry_sync == '1'"
    assert "scripts/bug_registry_metadata_check.py" in registry_step["run"]
    assert "--close-sync-only" in registry_step["run"]

    normal_lane_steps = [
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("name")
        in {
            "Set up Python 3.12",
            "Install quality tooling",
            "Build AIstock PR quality summary",
            "Build code intelligence PR artifact",
            "Ruff changed Python files",
            "Semgrep AIstock guardrails (report-only phase)",
        }
    ]
    assert normal_lane_steps
    assert all("registry_sync != '1'" in str(step.get("if") or "") for step in normal_lane_steps)


def test_codeql_uses_registry_sync_fast_lane() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/codeql.yml").read_text(encoding="utf-8"))
    jobs = workflow["jobs"]
    fast_lane = jobs["docs-lite"]
    analyze_steps = jobs["analyze"]["steps"]

    assert fast_lane["outputs"]["registry_sync"].endswith("steps.fast_lane.outputs.registry_sync }}")
    detect_step = next(step for step in fast_lane["steps"] if step.get("name") == "Detect CodeQL fast lane")
    assert "scripts/ci_change_classifier.py" in detect_step["run"]
    assert "close_sync_metadata_only" in detect_step["run"]

    no_op = next(step for step in analyze_steps if step.get("name") == "Fast-lane CodeQL no-op")
    assert "registry_sync == '1'" in str(no_op["if"])
    gated_steps = [step for step in analyze_steps if step.get("name") in {"Initialize CodeQL", "Perform CodeQL Analysis"}]
    assert gated_steps
    assert all("registry_sync != '1'" in str(step.get("if") or "") for step in gated_steps)


def test_semgrep_uses_registry_sync_fast_lane() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/semgrep.yml").read_text(encoding="utf-8"))
    steps = workflow["jobs"]["semgrep"]["steps"]

    detect_step = next(step for step in steps if step.get("name") == "Detect Semgrep fast lane")
    assert detect_step["id"] == "fast_lane"
    assert "scripts/ci_change_classifier.py" in detect_step["run"]
    assert "close_sync_metadata_only" in detect_step["run"]

    semgrep_steps = [
        step
        for step in steps
        if step.get("name") in {"Set up Python 3.12", "Install Semgrep", "Run Semgrep"}
    ]
    assert semgrep_steps
    assert all("registry_sync != '1'" in str(step.get("if") or "") for step in semgrep_steps)
    no_op = next(step for step in steps if step.get("name") == "Emit fast-lane semgrep no-op record")
    assert "registry_sync == '1'" in str(no_op["if"])


def test_allocator_change_keeps_backend_matrix(tmp_path: Path) -> None:
    allocator = tmp_path / "tests" / "aistock_validation" / "bugs" / ".bug_id_allocator.json"
    allocator.parent.mkdir(parents=True, exist_ok=True)
    allocator.write_text(json.dumps({"last_allocated": 191}), encoding="utf-8")

    payload = classifier.classify_changed_files(
        ["tests/aistock_validation/bugs/.bug_id_allocator.json"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "full_ci_required"
    assert payload["backend_required"] is True
    assert any("allocator" in reason for reason in payload["reasons"])


def test_cli_writes_github_outputs(tmp_path: Path, capsys) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-example.json"
    out = tmp_path / "summary.json"
    github_out = tmp_path / "github_output.txt"
    _write_bug(bug, status="closed")

    assert classifier.main([
        "--repo-root",
        str(tmp_path),
        "--changed-file",
        "tests/aistock_validation/bugs/20260601_BUG-191-example.json",
        "--output-json",
        str(out),
        "--github-output",
        str(github_out),
    ]) == 0

    assert json.loads(out.read_text(encoding="utf-8"))["backend_required"] is False
    assert "backend_required=false" in github_out.read_text(encoding="utf-8")
    assert "workflow_validation_required=false" in github_out.read_text(encoding="utf-8")
    assert "prompt_evaluation_required=false" in github_out.read_text(encoding="utf-8")
    assert json.loads(capsys.readouterr().out)["classification"] == "close_sync_metadata_only"
