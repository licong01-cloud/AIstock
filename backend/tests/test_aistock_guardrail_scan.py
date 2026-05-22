from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = ROOT / "scripts" / "aistock_guardrail_scan.py"
CATALOG_PATH = ROOT / "docs" / "standards" / "aistock_development_standard_v1.5_20260523.yaml"


def _load_module():
    spec = importlib.util.spec_from_file_location("aistock_guardrail_scan", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_catalog_loads_and_compiles_regex_rules() -> None:
    scanner = _load_module()

    catalog = scanner.load_catalog(CATALOG_PATH)
    rules = scanner.compile_rules(catalog)

    rule_ids = {rule.rule_id for rule in rules}
    assert "ARCH-WSL-001" in rule_ids
    assert "ERR-FALLBACK-001" in rule_ids
    assert "MEMORY-DATAFRAME-001" in rule_ids
    assert "DB-COMMENT-001" not in rule_ids  # external checker, not regex scanner scope


def test_catalog_references_current_human_readable_standard() -> None:
    scanner = _load_module()

    catalog = scanner.load_catalog(CATALOG_PATH)
    standard_path = ROOT / catalog["source_standard"]
    standard_text = standard_path.read_text(encoding="utf-8")

    assert catalog["source_version"] == "1.5"
    assert standard_path.name == "aistock_development_standard_v1.5_20260523.md"
    for rule in catalog["rules"]:
        if not rule.get("enabled", True):
            continue
        assert rule.get("standard_ref", "").startswith(catalog["source_standard"])
        assert rule["rule_id"] in standard_text
    for control in catalog.get("manual_review_controls", []):
        assert control.get("standard_ref", "").startswith(catalog["source_standard"])
        control_ref = control["standard_ref"].split("#", 1)[1]
        assert control_ref in standard_text


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
    assert payload["summary"]["total_findings"] >= 1
    assert "AIstock Guardrail Baseline Scan" in summary
    assert "Summary By Baseline Status" in summary
    assert "ERR-FALLBACK-001" in summary
