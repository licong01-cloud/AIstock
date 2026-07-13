from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = ROOT / ".codex" / "skills" / "verify-aistock-feature" / "scripts" / "scan_quality_guardrails.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("verify_aistock_feature_guardrail_scan", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_success_stdout_is_compact_and_details_are_artifact_only(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    scanner = _load_module()
    source = tmp_path / "src.py"
    source.write_text("API_KEY = '1234567890'\n", encoding="utf-8")
    output = tmp_path / "guardrail.json"
    monkeypatch.chdir(tmp_path)

    exit_code = scanner.main([str(source), "--fail-on", "NONE", "--output-json", str(output)])
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert "POSSIBLE_SECRET" not in stdout
    assert "Guardrail scan completed with 1 finding(s)." in stdout
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["finding_count"] == 1
    assert payload["findings"][0]["code"] == "POSSIBLE_SECRET"


def test_failure_stdout_keeps_actionable_findings(tmp_path: Path, monkeypatch, capsys) -> None:
    scanner = _load_module()
    source = tmp_path / "src.py"
    source.write_text("API_KEY = '1234567890'\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    exit_code = scanner.main([str(source), "--fail-on", "HIGH"])
    stdout = capsys.readouterr().out

    assert exit_code == 1
    assert "HIGH POSSIBLE_SECRET" in stdout
    assert "Guardrail scan failed" in stdout
