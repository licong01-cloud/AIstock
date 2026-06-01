from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest
import yaml

import scripts.issue_flow as flow


def _candidate(**overrides: object) -> dict[str, object]:
    candidate: dict[str, object] = {
        "schema_version": "aistock_issue_candidate_v1",
        "candidate_id": "IC-20260524-test",
        "source_event_id": "FE-20260524-test",
        "source": "pytest",
        "module": "validation.guardrails",
        "risk_level": "medium",
        "severity_guess": "P2",
        "candidate_type": "bug",
        "title": "Issue flow candidate",
        "expected": "Expected behavior",
        "actual": "Actual behavior",
        "fingerprint": "fingerprint-123",
        "dedupe_key": "validation.guardrails|l0|fingerprint",
        "suggested_owner": "codex_app",
        "suggested_validation": ["l0", "guardrail_changed_files"],
        "suggested_scope": ["scripts/issue_flow.py"],
        "promotion_target": "bug_registry",
        "evidence_refs": ["pytest"],
        "reproduce_command": "python scripts/issue_flow.py --help",
        "status": "new",
        "created_at": "2026-05-24T00:00:00Z",
    }
    candidate.update(overrides)
    return candidate


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_write_json_dash_writes_stdout_without_dash_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.chdir(tmp_path)

    flow._write_json(Path("-"), {"ok": True})

    assert json.loads(capsys.readouterr().out) == {"ok": True}
    assert not (tmp_path / "-").exists()


def test_candidate_create_outputs_event_candidate_and_stable_fingerprint(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    args = [
        "candidate-create",
        "--source",
        "manual",
        "--title",
        "Validation guardrail failed",
        "--module",
        "validation.guardrails",
        "--severity-guess",
        "P1",
        "--actual",
        "guardrail failure",
        "--expected",
        "guardrail pass",
        "--reproduce-command",
        "python -m nox -s guardrail_changed_files",
        "--changed-file",
        "scripts/issue_flow.py",
    ]

    assert flow.main(args) == 0
    first = json.loads(capsys.readouterr().out)
    assert flow.main(args) == 0
    second = json.loads(capsys.readouterr().out)

    assert first["event"]["schema_version"] == "aistock_failure_event_v1"
    assert first["candidate"]["schema_version"] == "aistock_issue_candidate_v1"
    assert first["candidate"]["fingerprint"] == second["candidate"]["fingerprint"]
    assert first["candidate"]["risk_level"] == "high"
    assert "guardrail_changed_files" in first["candidate"]["suggested_validation"]
    assert first["candidate"]["suggested_scope"] == ["scripts/issue_flow.py"]


def test_issue_form_parse_converts_github_body_to_candidate(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    body = """
### Severity
P1 - release-blocking or major regression

### AIstock module
validation.center

### Summary
Validation Center cannot open a run detail page.

### Reproduction steps
`python -m nox -s validation_center_backend`

### Expected behavior
The run detail page opens.

### Actual behavior
The page returns a blank state.

### Evidence
- run: https://example.invalid/run/1
"""
    issue_body = tmp_path / "issue.md"
    issue_body.write_text(body, encoding="utf-8")

    assert flow.main([
        "issue-form-parse",
        "--issue-body-file",
        str(issue_body),
        "--template-type",
        "bug",
        "--issue-number",
        "321",
        "--issue-url",
        "https://github.example/issues/321",
    ]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["schema_version"] == "aistock_issue_form_candidate_v1"
    assert payload["parsed"]["fields"]["module"] == "validation.center"
    assert payload["candidate"]["candidate_type"] == "bug"
    assert payload["candidate"]["source_issue_number"] == 321
    assert payload["candidate"]["severity_guess"] == "P1"
    assert payload["candidate"]["promotion_target"] == "bug_registry"
    assert "https://github.example/issues/321" in payload["candidate"]["evidence_refs"]


def test_candidate_dedupe_accepts_combined_candidate_payload(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    candidate = _candidate()
    candidate_path = _write_json(tmp_path / "candidate.json", candidate)
    existing_dir = tmp_path / "existing"
    _write_json(existing_dir / "combined.json", {"event": {"event_id": "FE"}, "candidate": candidate})

    assert flow.main([
        "candidate-dedupe",
        "--candidate-json",
        str(candidate_path),
        "--candidates-dir",
        str(existing_dir),
    ]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["deduplicated"] is True
    assert payload["matches"][0]["candidate_id"] == candidate["candidate_id"]


def test_candidate_transition_enforces_state_machine(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    candidate_path = _write_json(tmp_path / "candidate.json", _candidate())

    assert flow.main([
        "candidate-transition",
        "--candidate-json",
        str(candidate_path),
        "--to-status",
        "accepted",
        "--reason",
        "triaged",
    ]) == 0
    accepted = json.loads(capsys.readouterr().out)
    assert accepted["status"] == "accepted"
    assert accepted["status_events"][-1]["from_status"] == "new"

    promoted_path = _write_json(tmp_path / "promoted.json", _candidate(status="promoted"))
    assert flow.main(["candidate-transition", "--candidate-json", str(promoted_path), "--to-status", "accepted"]) == 2
    assert "invalid candidate transition" in capsys.readouterr().err


def test_promote_bug_dry_run_and_apply_guard(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    candidate_path = _write_json(tmp_path / "candidate.json", {"candidate": _candidate()})

    assert flow.main(["promote-bug", "--candidate-json", str(candidate_path), "--bug-id", "BUG-123"]) == 0
    planned = json.loads(capsys.readouterr().out)
    assert planned["applied"] is False
    assert planned["record"]["bug_id"] == "BUG-123"
    assert "github_issue_number" not in planned["record"]

    assert flow.main(["promote-bug", "--candidate-json", str(candidate_path), "--bug-id", "BUG-123", "--apply"]) == 2
    assert "--apply requires --github-issue-number" in capsys.readouterr().err


def test_promote_feature_keeps_feature_out_of_bug_registry(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    candidate_path = _write_json(tmp_path / "candidate.json", _candidate(candidate_type="feature", promotion_target="github_issue"))

    assert flow.main(["promote-feature", "--candidate-json", str(candidate_path)]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["title"].startswith("[FEATURE]")
    assert "enhancement" in payload["labels"]
    assert payload["candidate_id"] == "IC-20260524-test"


def test_context_pack_records_token_budget_and_rejects_ambiguous_inputs(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    candidate_path = _write_json(tmp_path / "candidate.json", _candidate())

    assert flow.main(["context-pack", "--candidate-json", str(candidate_path)]) == 0
    pack = json.loads(capsys.readouterr().out)

    assert pack["schema_version"] == "aistock_context_pack_v1"
    assert pack["token_budget"]["full_docs_allowed"] is False
    assert pack["token_budget"]["target_tokens"] == 12000
    assert "docs/standards/aistock_development_standard_v1.5_20260523.md#CONTEXT-BUDGET-001" in pack["standards_refs"]

    assert flow.main(["context-pack"]) == 2
    assert "requires exactly one" in capsys.readouterr().err


def test_batch_plan_allows_same_module_and_rejects_cross_module(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    one = _write_json(tmp_path / "one.json", _candidate(candidate_id="IC-1"))
    two = _write_json(tmp_path / "two.json", _candidate(candidate_id="IC-2", fingerprint="fingerprint-456"))
    other = _write_json(tmp_path / "other.json", _candidate(candidate_id="IC-3", module="paper_v2"))

    assert flow.main(["batch-plan", "--issue-json", str(one), "--issue-json", str(two)]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["schema_version"] == "aistock_batch_plan_v1"
    assert plan["module"] == "validation.guardrails"
    assert plan["issues"] == ["IC-1", "IC-2"]
    assert "guardrail_changed_files" in plan["shared_validation"]

    assert flow.main(["batch-plan", "--issue-json", str(one), "--issue-json", str(other)]) == 2
    assert "must share one module" in capsys.readouterr().err


def test_validation_select_maps_catalog_plans_and_production_gates(capsys: pytest.CaptureFixture[str]) -> None:
    assert flow.main([
        "validation-select",
        "--changed-file",
        "scripts/issue_flow.py",
        "--changed-file",
        "backend/migrations/example.sql",
        "--changed-file",
        "requirements.txt",
    ]) == 0
    payload = json.loads(capsys.readouterr().out)
    plans = flow._plans_by_key()

    assert payload["schema_version"] == "aistock_validation_selection_v1"
    assert "validation.guardrails" in payload["impacted_modules"]
    assert all(plan in plans for plan in payload["required_plans"])
    assert payload["production_gates"] == {
        "ddl": "required",
        "frontend_dependency": "noop",
        "backend_dependency": "required",
    }


def test_validation_select_keeps_watchlist_bug_on_narrow_plans(capsys: pytest.CaptureFixture[str]) -> None:
    assert flow.main([
        "validation-select",
        "--module",
        "watchlist",
        "--changed-file",
        "backend/core/data_source_manager_impl.py",
        "--changed-file",
        "backend/tests/watchlist/test_realtime_amount_units.py",
        "--changed-file",
        "tests/aistock_validation/bugs/20260602_BUG-195-watchlist-turnover-amount-column-displays-1000x-too-small-values.json",
    ]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert "watchlist" in payload["impacted_modules"]
    assert payload["ownership"]["unmatched_files"] == []
    assert "validation_center_backend" not in payload["required_plans"]
    assert payload["required_plans"] == ["l0", "validation_module_registry_l0"]


def test_pr_check_reports_scope_and_dependency_gates(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    issue = _write_json(tmp_path / "bug.json", {
        "bug_id": "BUG-124",
        "module": "validation.guardrails",
        "allowed_write_scope": ["scripts/issue_flow.py"],
    })

    assert flow.main([
        "pr-check",
        "--issue-json",
        str(issue),
        "--changed-file",
        "scripts/issue_flow.py",
        "--changed-file",
        "frontend/package.json",
    ]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["scope_check"]["status"] == "failed"
    assert summary["scope_check"]["violations"] == ["frontend/package.json"]
    assert summary["production_frontend_dependency_gate"] == "required"

    assert flow.main([
        "pr-check",
        "--issue-json",
        str(issue),
        "--changed-file",
        "frontend/package.json",
        "--fail-on-scope",
    ]) == 2


def test_close_sync_and_cleanup_apply_are_dry_run_only(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    issue = _write_json(tmp_path / "bug.json", {"bug_id": "BUG-125", "status": "fixed"})

    assert flow.main(["close-sync", "--issue-json", str(issue), "--pr-url", "https://github.example/pull/1"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "aistock_close_sync_plan_v1"
    assert payload["dry_run"] is True

    assert flow.main(["close-sync", "--issue-json", str(issue), "--apply"]) == 2
    assert "intentionally not implemented" in capsys.readouterr().err

    assert flow.main(["cleanup-after-merge", "--branch", "feature/example", "--apply"]) == 2
    assert "intentionally not implemented" in capsys.readouterr().err


def test_pr_check_infers_linkage_and_scope_from_bug_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)
    bug_path = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260529_BUG-155-workflow-efficiency.json"
    bug_path.parent.mkdir(parents=True, exist_ok=True)
    bug_path.write_text(
        json.dumps(
            {
                "bug_id": "BUG-155",
                "github_issue_number": 355,
                "module": "validation",
                "allowed_write_scope": ["scripts/issue_flow.py", "tests/aistock_validation/bugs/**"],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(flow, "_git_output", lambda args, cwd=flow.REPO_ROOT, check=True: "bug/BUG-155-workflow-efficiency" if args[:2] == ["branch", "--show-current"] else "fix BUG-155")

    assert flow.main(
        [
            "pr-check",
            "--changed-file",
            "scripts/issue_flow.py",
            "--changed-file",
            "tests/aistock_validation/bugs/20260529_BUG-155-workflow-efficiency.json",
        ]
    ) == 0
    summary = json.loads(capsys.readouterr().out)

    assert "BUG-155" in summary["linked_issues"]
    assert "#355" in summary["linked_issues"]
    assert summary["scope_check"]["status"] == "passed"
    assert summary["scope_check"]["status_source"] == "inferred_from_bug_json"
    assert summary["linkage_inference"]["status"] == "inferred"


def test_pr_check_infers_linkage_from_pr_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    issue = _write_json(
        tmp_path / "bug.json",
        {
            "module": "validation",
            "allowed_write_scope": ["scripts/issue_flow.py"],
        },
    )
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("AISTOCK_PR_TITLE", "Fixes BUG-156")
    monkeypatch.setenv("AISTOCK_PR_BODY", "Closes #356")
    monkeypatch.setattr(flow, "_git_output", lambda args, cwd=flow.REPO_ROOT, check=True: "")

    assert flow.main(
        [
            "pr-check",
            "--issue-json",
            str(issue),
            "--changed-file",
            "scripts/issue_flow.py",
        ]
    ) == 0
    summary = json.loads(capsys.readouterr().out)

    assert "BUG-156" in summary["linked_issues"]
    assert "#356" in summary["linked_issues"]
    assert summary["scope_check"]["status"] == "passed"
    assert summary["scope_check"]["status_source"] == "issue_record"
    assert summary["linkage_inference"]["pr_metadata_present"] is True
    assert summary["linkage_inference"]["status"] == "inferred"


def test_pr_check_does_not_use_stale_history_when_diff_log_is_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)

    def fake_git(args: list[str], cwd: Path = flow.REPO_ROOT, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "feature/pr-quality-gate"
        return ""

    monkeypatch.setattr(flow, "_git_output", fake_git)

    assert flow.main(["pr-check", "--changed-file", "scripts/issue_flow.py"]) == 0
    summary = json.loads(capsys.readouterr().out)

    assert summary["linked_issues"] == []
    assert summary["task_tier"] == "T0"
    assert summary["linkage_inference"]["status"] == "missing"


def test_pr_check_p1_evidence_gate_warns_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)
    bug_path = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260602_BUG-198-pr-quality-gate.json"
    bug_path.parent.mkdir(parents=True, exist_ok=True)
    bug_path.write_text(
        json.dumps(
            {
                "bug_id": "BUG-198",
                "github_issue_number": 504,
                "severity": "P1",
                "module": "validation",
                "allowed_write_scope": ["scripts/issue_flow.py", "tests/aistock_validation/bugs/**"],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(flow, "_git_output", lambda args, cwd=flow.REPO_ROOT, check=True: "bug/BUG-198-pr-quality-gate")

    assert flow.main(
        [
            "pr-check",
            "--changed-file",
            "scripts/issue_flow.py",
            "--changed-file",
            "tests/aistock_validation/bugs/20260602_BUG-198-pr-quality-gate.json",
        ]
    ) == 0
    summary = json.loads(capsys.readouterr().out)

    gate = summary["p0p1_evidence_gate"]
    assert gate["workflow_gate"] == "warning"
    assert gate["enforced"] is False
    assert "validation_evidence" in gate["warnings"]


def test_pr_check_p1_evidence_gate_blocks_when_enforced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)
    bug_path = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260602_BUG-198-pr-quality-gate.json"
    bug_path.parent.mkdir(parents=True, exist_ok=True)
    bug_path.write_text(
        json.dumps(
            {
                "bug_id": "BUG-198",
                "github_issue_number": 504,
                "severity": "P1",
                "module": "validation",
                "allowed_write_scope": ["scripts/issue_flow.py", "tests/aistock_validation/bugs/**"],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(flow, "_git_output", lambda args, cwd=flow.REPO_ROOT, check=True: "fix BUG-198 P1")

    assert flow.main(
        [
            "pr-check",
            "--changed-file",
            "scripts/issue_flow.py",
            "--changed-file",
            "tests/aistock_validation/bugs/20260602_BUG-198-pr-quality-gate.json",
            "--enforce-p0-p1-evidence",
        ]
    ) == 2
    summary = json.loads(capsys.readouterr().out)

    gate = summary["p0p1_evidence_gate"]
    assert gate["workflow_gate"] == "blocked"
    assert gate["enforced"] is True
    assert gate["severity"] == "P1"
    assert gate["checks"]["linked_issue"] is True
    assert gate["checks"]["scope_passed"] is True
    assert "validation_evidence" in gate["blocking"]
    assert "production_gates" in gate["blocking"]


def test_pr_check_p1_evidence_gate_passes_with_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)
    bug_path = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260602_BUG-198-pr-quality-gate.json"
    bug_path.parent.mkdir(parents=True, exist_ok=True)
    bug_path.write_text(
        json.dumps(
            {
                "bug_id": "BUG-198",
                "github_issue_number": 504,
                "severity": "P1",
                "module": "validation",
                "allowed_write_scope": ["scripts/issue_flow.py", "tests/aistock_validation/bugs/**"],
                "production_ddl_gate": "noop",
                "production_frontend_dependency_gate": "noop",
                "production_backend_dependency_gate": "noop",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "AISTOCK_PR_BODY",
        "Validation Evidence: python -m nox -s l0 -> passed\n"
        "production_ddl_gate=noop\n"
        "production_frontend_dependency_gate=noop\n"
        "production_backend_dependency_gate=noop",
    )
    monkeypatch.setattr(flow, "_git_output", lambda args, cwd=flow.REPO_ROOT, check=True: "fix BUG-198 P1")

    assert flow.main(
        [
            "pr-check",
            "--changed-file",
            "scripts/issue_flow.py",
            "--changed-file",
            "tests/aistock_validation/bugs/20260602_BUG-198-pr-quality-gate.json",
            "--enforce-p0-p1-evidence",
        ]
    ) == 0
    summary = json.loads(capsys.readouterr().out)

    gate = summary["p0p1_evidence_gate"]
    assert gate["workflow_gate"] == "passed"
    assert gate["blocking"] == []
    assert gate["checks"] == {
        "linked_issue": True,
        "scope_passed": True,
        "validation_evidence": True,
        "production_gates": True,
    }


def test_pr_check_t3_feature_warns_without_design_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("AISTOCK_PR_TITLE", "feat: T3 Research Assistant workflow pack")
    monkeypatch.setenv("AISTOCK_PR_BODY", "Feature implementation for a T3 architecture change.")
    monkeypatch.setattr(flow, "_git_output", lambda args, cwd=flow.REPO_ROOT, check=True: "feature/t3-ra-workflow-pack")

    assert flow.main(["pr-check", "--changed-file", "docs/architecture/example.md"]) == 0
    summary = json.loads(capsys.readouterr().out)

    gate = summary["design_compliance_gate"]
    assert summary["task_tier"] == "T3"
    assert gate["workflow_gate"] == "warning"
    assert gate["blocking"] == []
    assert gate["warnings"] == ["design_acceptance_matrix"]


def test_pr_check_t3_feature_passes_with_design_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("AISTOCK_PR_TITLE", "Feature: T3 validation workflow")
    monkeypatch.setenv(
        "AISTOCK_PR_BODY",
        "## Design Acceptance Matrix\n- API contract: pass\n- Validation evidence: pass",
    )
    monkeypatch.setattr(flow, "_git_output", lambda args, cwd=flow.REPO_ROOT, check=True: "feature/t3-validation-workflow")

    assert flow.main(["pr-check", "--changed-file", "docs/architecture/example.md"]) == 0
    summary = json.loads(capsys.readouterr().out)

    gate = summary["design_compliance_gate"]
    assert gate["workflow_gate"] == "passed"
    assert gate["warnings"] == []


def test_pr_check_design_compliance_ignores_non_t3_bug(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(flow, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("AISTOCK_PR_TITLE", "fix: BUG-515 P1 workflow issue")
    monkeypatch.setattr(flow, "_git_output", lambda args, cwd=flow.REPO_ROOT, check=True: "bug/BUG-515-workflow")

    assert flow.main(["pr-check", "--changed-file", "scripts/issue_flow.py"]) == 0
    summary = json.loads(capsys.readouterr().out)

    gate = summary["design_compliance_gate"]
    assert gate["workflow_gate"] == "not_applicable"
    assert gate["warnings"] == []


def test_open_source_tooling_configs_are_parseable() -> None:
    assert yaml.safe_load(Path(".pre-commit-config.yaml").read_text(encoding="utf-8"))["repos"]
    assert yaml.safe_load(Path(".semgrep.yml").read_text(encoding="utf-8"))["rules"]
    assert tomllib.loads(Path("ruff.toml").read_text(encoding="utf-8"))["lint"]["select"]
    assert json.loads(Path(".github/renovate.json").read_text(encoding="utf-8"))["dependencyDashboard"] is True
    for workflow in [
        ".github/workflows/pr-quality.yml",
        ".github/workflows/semgrep.yml",
        ".github/workflows/codeql.yml",
        ".github/workflows/dependency-update-validate.yml",
    ]:
        loaded = yaml.safe_load(Path(workflow).read_text(encoding="utf-8"))
        assert loaded["name"]
        assert loaded["jobs"]
