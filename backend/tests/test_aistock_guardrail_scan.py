from __future__ import annotations

import importlib.util
import hashlib
import json
import sys
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = ROOT / "scripts" / "aistock_guardrail_scan.py"
CATALOG_PATH = ROOT / "docs" / "standards" / "aistock_development_standard_v1.5_20260523.yaml"
RUNTIME_TARGET_CATALOG_PATH = ROOT / "docs" / "standards" / "aistock_runtime_targets_v1.yaml"


def _load_module():
    spec = importlib.util.spec_from_file_location("aistock_guardrail_scan", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _unified_controls(catalog: dict) -> dict[str, dict]:
    controls: dict[str, dict] = {}
    for item in catalog["rules"]:
        control_id = item["rule_id"]
        assert control_id not in controls
        controls[control_id] = item
    for item in catalog.get("manual_review_controls", []):
        control_id = item["control_id"]
        assert control_id not in controls
        controls[control_id] = item
    return controls


def test_catalog_loads_and_compiles_regex_rules() -> None:
    scanner = _load_module()

    catalog = scanner.load_catalog(CATALOG_PATH)
    rules = scanner.compile_rules(catalog)

    rule_ids = {rule.rule_id for rule in rules}
    assert "ARCH-WSL-001" in rule_ids
    assert "ERR-FALLBACK-001" in rule_ids
    assert "MEMORY-DATAFRAME-001" in rule_ids
    assert "BACKEND-RESTART-OWNERSHIP-001" in rule_ids
    assert "DB-COMMENT-001" not in rule_ids  # external checker, not regex scanner scope


def test_catalog_references_current_human_readable_standard() -> None:
    scanner = _load_module()

    catalog = scanner.load_catalog(CATALOG_PATH)
    standard_path = ROOT / catalog["source_standard"]
    standard_text = standard_path.read_text(encoding="utf-8")

    assert catalog["source_version"] == "1.5"
    assert catalog["rule_sync_policy"]["catalog_role"] == "machine_enforcement_metadata_only"
    normalized_standard = standard_text.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")
    assert catalog["source_digest_normalization"] == "utf8_lf"
    assert catalog["source_sha256"] == hashlib.sha256(normalized_standard).hexdigest()
    assert standard_path.name == "aistock_development_standard_v1.5_20260523.md"
    effects = set(catalog["control_taxonomy"]["effects"])
    phases = set(catalog["control_taxonomy"]["enforcement_phases"])
    for rule in catalog["rules"]:
        if not rule.get("enabled", True):
            continue
        assert rule.get("standard_ref", "").startswith(catalog["source_standard"])
        assert rule["rule_id"] in standard_text
        assert rule["effect"] in effects
        assert rule["enforcement_phase"] in phases
        if (rule.get("checker") or {}).get("type") == "manual_review":
            assert rule.get("failure_policy")
    for control in catalog.get("manual_review_controls", []):
        assert control.get("standard_ref", "").startswith(catalog["source_standard"])
        control_ref = control["standard_ref"].split("#", 1)[1]
        assert control_ref in standard_text
        assert control["effect"] in effects
        assert control["enforcement_phase"] in phases
        assert control.get("failure_policy")

    controls = _unified_controls(catalog)
    assert len(controls) == 29
    assert set(item["control_id"] for item in catalog["manual_review_controls"]) == {
        "DESIGN-COMPLIANCE-001",
        "ISSUE-GITHUB-SYNC-001",
        "DESIGN-MAIN-001",
        "STD-SYNC-001",
    }
    effect_counts = {effect: sum(item["effect"] == effect for item in controls.values()) for effect in effects}
    assert effect_counts == {"block": 21, "warn": 5, "advisory": 3}


def test_rdagent_release_identity_control_is_fail_closed() -> None:
    scanner = _load_module()

    catalog = scanner.load_catalog(CATALOG_PATH)
    controls = _unified_controls(catalog)
    control = controls["RDAGENT-RELEASE-IDENTITY-001"]

    assert control["failure_policy"] == "block_release_deploy_restart_and_verified_claims"
    assert {
        "merged_commit_in_target_branch",
        "clean_source_checkout",
        "repository_merge_tree_manifest_match",
        "immutable_release_path",
        "repo_external_rdagent_state_root",
        "deployment_receipt",
        "atomic_current_pointer",
        "separate_source_deploy_restart_runtime_states",
        "rollback_target",
        "no_source_overlay",
    } <= set(control["checker"]["required_evidence"])


def test_restart_controls_and_runtime_target_catalog_fail_closed() -> None:
    scanner = _load_module()
    catalog = scanner.load_catalog(CATALOG_PATH)
    controls = _unified_controls(catalog)

    assert controls["BACKEND-RESTART-OWNERSHIP-001"]["failure_policy"] == (
        "block_process_control_and_runtime_verified_claims"
    )
    assert controls["BUG-RESTART-EFFECTIVE-001"]["failure_policy"] == (
        "block_issue_close_sync_and_verified_claims"
    )

    runtime_catalog = yaml.safe_load(RUNTIME_TARGET_CATALOG_PATH.read_text(encoding="utf-8"))
    assert runtime_catalog["schema_version"] == "aistock_runtime_target_catalog_v1"
    assert runtime_catalog["policy"]["backend_restart_owner"] == "user"
    assert runtime_catalog["policy"]["post_restart_verify_mode"] == "read_only"
    assert runtime_catalog["targets"]["backend-main"]["production_port"] == 8001
    assert runtime_catalog["targets"]["backend-main"]["isolated_validation_ports"] == [8011, 8012]


@pytest.mark.parametrize(
    "command",
    [
        "Restart-Service backend-api\n",
        "python scripts/_restart_backend.py\n",
        "python backend/main.py\n",
        "uvicorn backend.main:app --port " + str(8000 + 1) + "\n",
        "sc.exe stop backend-api\n",
    ],
)
def test_scanner_blocks_user_backend_process_control_in_client_workflow(
    tmp_path: Path,
    command: str,
) -> None:
    scanner = _load_module()
    command_file = tmp_path / ".claude" / "commands" / "unsafe.md"
    command_file.parent.mkdir(parents=True)
    command_file.write_text(command, encoding="utf-8")

    catalog = scanner.load_catalog(CATALOG_PATH)
    findings = scanner.scan_files([command_file], rules=scanner.compile_rules(catalog), root=tmp_path)

    assert any(finding.rule_id == "BACKEND-RESTART-OWNERSHIP-001" for finding in findings)


def test_scanner_detects_silent_fallback_in_runtime_code(tmp_path: Path) -> None:
    scanner = _load_module()
    runtime_file = tmp_path / "backend" / "services" / "example.py"
    runtime_file.parent.mkdir(parents=True)
    runtime_file.write_text(
        "def run():\n"
        "    try:\n"
        "        do_work()\n"
        "    except Exception:\n"
        "        return []\n",
        encoding="utf-8",
    )

    catalog = scanner.load_catalog(CATALOG_PATH)
    rules = scanner.compile_rules(catalog)
    findings = scanner.scan_files([runtime_file], rules=rules, root=tmp_path)

    assert any(finding.rule_id == "ERR-FALLBACK-001" for finding in findings)


def test_scanner_respects_rule_exclude_globs_for_tests(tmp_path: Path) -> None:
    scanner = _load_module()
    test_file = tmp_path / "backend" / "tests" / "test_example.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text(
        "def test_fixture():\n"
        "    try:\n"
        "        raise RuntimeError()\n"
        "    except Exception:\n"
        "        return []\n",
        encoding="utf-8",
    )

    catalog = scanner.load_catalog(CATALOG_PATH)
    rules = scanner.compile_rules(catalog)
    findings = scanner.scan_files([test_file], rules=rules, root=tmp_path)

    assert not any(finding.rule_id == "ERR-FALLBACK-001" for finding in findings)


def test_trading_fallback_guardrail_ignores_test_file_names(tmp_path: Path) -> None:
    scanner = _load_module()
    test_file = tmp_path / "backend" / "tests" / "selection_center" / "test_price_guidance.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text(
        "def test_missing_signal_ref_price_degrades_without_default_price():\n"
        "    assert True\n",
        encoding="utf-8",
    )

    catalog = scanner.load_catalog(CATALOG_PATH)
    rules = scanner.compile_rules(catalog)
    findings = scanner.scan_files([test_file], rules=rules, root=tmp_path)

    assert not any(finding.rule_id == "TRADING-FALLBACK-001" for finding in findings)


def test_scanner_detects_root_pollution_by_path(tmp_path: Path) -> None:
    scanner = _load_module()
    root_script = tmp_path / "one_off_debug.py"
    root_script.write_text("print('debug')\n", encoding="utf-8")

    catalog = scanner.load_catalog(CATALOG_PATH)
    rules = scanner.compile_rules(catalog)
    findings = scanner.scan_files([root_script], rules=rules, root=tmp_path)

    assert any(finding.rule_id == "ROOT-POLLUTION-001" for finding in findings)


def test_scanner_allows_debug_tools_one_off_scripts(tmp_path: Path) -> None:
    scanner = _load_module()
    debug_script = tmp_path / "debug_tools" / "qe" / "20260504_issue" / "one_off_debug.py"
    debug_script.parent.mkdir(parents=True)
    debug_script.write_text("print('debug')\n", encoding="utf-8")

    catalog = scanner.load_catalog(CATALOG_PATH)
    rules = scanner.compile_rules(catalog)
    findings = scanner.scan_files([debug_script], rules=rules, root=tmp_path)

    assert not any(finding.rule_id == "ROOT-POLLUTION-001" for finding in findings)


def test_scanner_detects_concat_inside_loop_without_backtracking(tmp_path: Path) -> None:
    scanner = _load_module()
    runtime_file = tmp_path / "scripts" / "build_large_dataset.py"
    runtime_file.parent.mkdir(parents=True)
    runtime_file.write_text(
        "import pandas as pd\n\n"
        "def build(parts):\n"
        "    out = pd.DataFrame()\n"
        "    for part in parts:\n"
        "        frame = load(part)\n"
        "        out = pd.concat([out, frame])\n"
        "    return out\n",
        encoding="utf-8",
    )

    catalog = scanner.load_catalog(CATALOG_PATH)
    rules = scanner.compile_rules(catalog)
    findings = scanner.scan_files([runtime_file], rules=rules, root=tmp_path)

    assert any(finding.rule_id == "MEMORY-DATAFRAME-001" for finding in findings)


def test_git_changed_files_uses_utf8_for_unicode_paths(tmp_path: Path, monkeypatch) -> None:
    scanner = _load_module()

    def fake_check_output(args, **kwargs):
        assert kwargs["encoding"] == "utf-8"
        assert kwargs["errors"] == "replace"
        if args[:3] == ["git", "diff", "--name-only"]:
            return "docs/鍒嗘瀽鎶ュ憡.md\n"
        return "scripts/娴嬭瘯鑴氭湰.py\n"

    monkeypatch.setattr(scanner.subprocess, "check_output", fake_check_output)

    paths = scanner.git_changed_files(tmp_path)

    assert [path.relative_to(tmp_path).as_posix() for path in paths] == [
        "docs/鍒嗘瀽鎶ュ憡.md",
        "scripts/娴嬭瘯鑴氭湰.py",
    ]


def test_git_staged_files_uses_cached_diff_only(tmp_path: Path, monkeypatch) -> None:
    scanner = _load_module()

    def fake_check_output(args, **kwargs):
        assert args[:4] == ["git", "diff", "--cached", "--name-only"]
        assert kwargs["encoding"] == "utf-8"
        return "backend/services/new_feature.py\n"

    monkeypatch.setattr(scanner.subprocess, "check_output", fake_check_output)

    paths = scanner.git_staged_files(tmp_path)

    assert [path.relative_to(tmp_path).as_posix() for path in paths] == ["backend/services/new_feature.py"]


def test_changed_line_filter_ignores_unchanged_baseline_findings(tmp_path: Path) -> None:
    scanner = _load_module()
    findings = [
        scanner.Finding(
            rule_id="ERR-FALLBACK-001",
            title="Broad exception handlers must not return fake success or defaults",
            severity="P0",
            category="error_handling",
            file="backend/main.py",
            line=90,
            message="Broad exception handlers must not return fake success or defaults",
            remediation="Fail fast.",
            baseline_policy="block_new_only",
            fingerprint="old90",
        ),
        scanner.Finding(
            rule_id="ERR-FALLBACK-001",
            title="Broad exception handlers must not return fake success or defaults",
            severity="P0",
            category="error_handling",
            file="backend/main.py",
            line=504,
            message="Broad exception handlers must not return fake success or defaults",
            remediation="Fail fast.",
            baseline_policy="block_new_only",
            fingerprint="new504",
        ),
    ]

    filtered = scanner.filter_findings_to_changed_lines(
        findings,
        {"backend/main.py": {504}},
    )

    assert [finding.fingerprint for finding in filtered] == ["new504"]


def test_changed_line_numbers_handles_pure_insertion_hunks(tmp_path: Path, monkeypatch) -> None:
    scanner = _load_module()

    def fake_git_output(args, root):
        assert args[:3] == ["git", "diff", "--unified=0"]
        return (
            "diff --git a/backend/main.py b/backend/main.py\n"
            "--- a/backend/main.py\n"
            "+++ b/backend/main.py\n"
            "@@ -506,0 +507,2 @@ def create_app() -> FastAPI:\n"
            "+    app.include_router(paper_trading_v2.router, prefix=\"/api/v1\")\n"
            "+    app.include_router(simulation_runtime.router, prefix=\"/api/v1\")\n"
        )

    monkeypatch.setattr(scanner, "_git_output", fake_git_output)

    changed = scanner._changed_line_numbers(
        tmp_path,
        [tmp_path / "backend" / "main.py"],
        staged=True,
    )

    assert changed == {"backend/main.py": {507, 508}}


def test_baseline_status_and_new_only_blocking(tmp_path: Path) -> None:
    scanner = _load_module()
    finding = scanner.Finding(
        rule_id="ERR-FALLBACK-001",
        title="Broad exception handlers must not return fake success or defaults",
        severity="P0",
        category="error_handling",
        file="backend/services/example.py",
        line=10,
        message="Broad exception handlers must not return fake success or defaults",
        remediation="Fail fast.",
        baseline_policy="block_new_only",
        fingerprint="abc123",
    )
    baseline_json = tmp_path / "baseline.json"
    baseline_json.write_text(json.dumps({"findings": [{"fingerprint": "abc123"}]}), encoding="utf-8")

    baseline_fingerprints = scanner.load_baseline_fingerprints(baseline_json)
    classified = scanner.apply_baseline_status([finding], baseline_fingerprints)

    assert classified[0].baseline_status == "baseline"
    assert scanner.blocking_findings(classified, "P1", fail_new_only=True) == []
    assert scanner.blocking_findings(classified, "P1", fail_new_only=False) == classified


def test_missing_baseline_marks_findings_as_new() -> None:
    scanner = _load_module()
    finding = scanner.Finding(
        rule_id="ERR-FALLBACK-001",
        title="Broad exception handlers must not return fake success or defaults",
        severity="P0",
        category="error_handling",
        file="backend/services/example.py",
        line=10,
        message="Broad exception handlers must not return fake success or defaults",
        remediation="Fail fast.",
        baseline_policy="block_new_only",
        fingerprint="new123",
    )

    classified = scanner.apply_baseline_status([finding], set())

    assert classified[0].baseline_status == "new"
    assert scanner.blocking_findings(classified, "P1", fail_new_only=True) == classified


def test_guardrail_scope_summary_classifies_repository_areas() -> None:
    scanner = _load_module()

    assert scanner._finding_scope("backend/services/example.py") == "runtime_or_pipeline"
    assert scanner._finding_scope("scripts/aistock_guardrail_scan.py") == "runtime_or_pipeline"
    assert scanner._finding_scope("frontend/src/app/page.tsx") == "frontend_runtime"
    assert scanner._finding_scope("tests/aistock_validation/catalog/test_plans.yaml") == "config_or_metadata"
    assert scanner._finding_scope("backend/tests/test_example.py") == "test_or_validation"
    assert scanner._finding_scope("docs/architecture/legacy.md") == "docs_or_historical"


def test_guardrail_summarize_includes_scope_visibility_without_changing_blocking() -> None:
    scanner = _load_module()
    findings = [
        scanner.Finding(
            rule_id="ERR-FALLBACK-001",
            title="Broad exception handlers must not return fake success or defaults",
            severity="P0",
            category="error_handling",
            file="backend/services/example.py",
            line=10,
            message="Broad exception handlers must not return fake success or defaults",
            remediation="Fail fast.",
            baseline_policy="block_new_only",
            fingerprint="runtime",
            baseline_status="new",
        ),
        scanner.Finding(
            rule_id="ARCH-WSL-001",
            title="Historical WSL path reference",
            severity="P2",
            category="portability",
            file="docs/architecture/legacy.md",
            line=3,
            message="Historical WSL path reference",
            remediation="Classify before remediation.",
            baseline_policy="block_new_only",
            fingerprint="docs",
            baseline_status="baseline",
        ),
    ]

    summary = scanner.summarize(findings)

    assert summary["by_scope"] == {"docs_or_historical": 1, "runtime_or_pipeline": 1}
    assert summary["by_scope_and_severity"]["runtime_or_pipeline"]["P0"] == 1
    assert summary["top_runtime_or_pipeline_rules"] == [{"rule_id": "ERR-FALLBACK-001", "count": 1}]
    assert scanner.blocking_findings(findings, "P1", fail_new_only=True) == [findings[0]]


def test_scanner_writes_json_and_markdown_summary(tmp_path: Path) -> None:
    scanner = _load_module()
    runtime_file = tmp_path / "backend" / "services" / "example.py"
    runtime_file.parent.mkdir(parents=True)
    runtime_file.write_text(
        "def run():\n"
        "    try:\n"
        "        do_work()\n"
        "    except Exception:\n"
        "        pass\n",
        encoding="utf-8",
    )
    json_path = tmp_path / "result.json"
    md_path = tmp_path / "summary.md"

    catalog = scanner.load_catalog(CATALOG_PATH)
    rules = scanner.compile_rules(catalog)
    findings = scanner.scan_files([runtime_file], rules=rules, root=tmp_path)
    findings = scanner.apply_baseline_status(findings, set())
    scanner.write_json(
        json_path,
        findings=findings,
        files_scanned=1,
        mode="unit_test",
        fail_on_severity="P1",
        fail_new_only=True,
    )
    scanner.write_summary_md(md_path, findings=findings, files_scanned=1, mode="unit_test", max_findings=10)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    summary = md_path.read_text(encoding="utf-8")
    assert payload["schema_version"] == "aistock_guardrail_scan_result_v1"
    assert payload["gate"]["status"] == "failed"
    assert payload["summary"]["by_baseline_status"]["new"] >= 1
    assert payload["summary"]["by_scope"]["runtime_or_pipeline"] >= 1
    assert payload["summary"]["total_findings"] >= 1
    assert "AIstock Guardrail Baseline Scan" in summary
    assert "Summary By Baseline Status" in summary
    assert "Summary By Scope" in summary
    assert "Top Runtime Or Pipeline Rules" in summary
    assert "ERR-FALLBACK-001" in summary


def test_success_stdout_is_compact_when_details_are_artifacts(capsys) -> None:
    scanner = _load_module()
    finding = scanner.Finding(
        rule_id="ARCH-WSL-001",
        title="Historical WSL path reference",
        severity="P2",
        category="portability",
        file="docs/architecture/legacy.md",
        line=3,
        message="Historical WSL path reference",
        remediation="Classify before remediation.",
        baseline_policy="block_new_only",
        fingerprint="docs",
        baseline_status="baseline",
    )

    scanner.print_stdout_summary(
        findings=[finding],
        blocked=[],
        files_scanned=1,
        mode="unit_test",
        output_json="tmp/validation/guardrails/example.json",
        summary_md="tmp/validation/guardrails/example.md",
        verbose_findings=False,
        max_stdout_findings=80,
    )

    stdout = capsys.readouterr().out
    assert "ARCH-WSL-001 docs/architecture/legacy.md" not in stdout
    assert "findings=1, blocking=0" in stdout
    assert "details=tmp/validation/guardrails/example.json,tmp/validation/guardrails/example.md" in stdout


def test_failure_stdout_prints_only_blocking_findings(capsys) -> None:
    scanner = _load_module()
    baseline = scanner.Finding(
        rule_id="ARCH-WSL-001",
        title="Historical WSL path reference",
        severity="P2",
        category="portability",
        file="docs/architecture/legacy.md",
        line=3,
        message="Historical WSL path reference",
        remediation="Classify before remediation.",
        baseline_policy="block_new_only",
        fingerprint="docs",
        baseline_status="baseline",
    )
    blocking = scanner.Finding(
        rule_id="ERR-FALLBACK-001",
        title="Broad exception handlers must not return fake success or defaults",
        severity="P0",
        category="error_handling",
        file="backend/services/example.py",
        line=10,
        message="Broad exception handlers must not return fake success or defaults",
        remediation="Fail fast.",
        baseline_policy="block_new_only",
        fingerprint="runtime",
        baseline_status="new",
    )

    scanner.print_stdout_summary(
        findings=[baseline, blocking],
        blocked=[blocking],
        files_scanned=2,
        mode="unit_test",
        output_json=None,
        summary_md=None,
        verbose_findings=False,
        max_stdout_findings=80,
    )

    stdout = capsys.readouterr().out
    assert "ERR-FALLBACK-001 backend/services/example.py" in stdout
    assert "ARCH-WSL-001 docs/architecture/legacy.md" not in stdout
    assert "findings=2, blocking=1" in stdout
