from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts import nightly_design_drift_audit as audit


def _write_config(root: Path, *, drift_markers: list[str] | None = None) -> Path:
    config = {
        "schema_version": "aistock_design_drift_audit_config_v1",
        "defaults": {
            "max_design_chars_per_doc": 800,
            "max_code_files_per_module": 4,
            "max_code_excerpt_chars_per_file": 500,
        },
        "modules": [
            {
                "module": "demo_runtime",
                "risk": "P1",
                "design_docs": ["docs/architecture/demo_runtime_design.md"],
                "code_paths": ["backend/services/demo_runtime"],
                "requirement_keywords": ["durable", "event loop"],
                "expected_code_markers": ["DurableRuntime", "EventLoop"],
                "drift_risk_markers": drift_markers or [],
            }
        ],
    }
    path = root / "configs" / "validation" / "design_drift_audit.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def _write_llm_config(root: Path) -> Path:
    payload = yaml.safe_load(audit.llm_adapter.DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    path = root / "configs" / "validation" / "llm_triage.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _init_repo(root: Path) -> None:
    audit.subprocess.run(["git", "-C", str(root), "init"], check=True, capture_output=True)
    audit.subprocess.run(["git", "-C", str(root), "config", "user.email", "test@example.com"], check=True)
    audit.subprocess.run(["git", "-C", str(root), "config", "user.name", "Test"], check=True)
    audit.subprocess.run(["git", "-C", str(root), "add", "."], check=True)
    audit.subprocess.run(["git", "-C", str(root), "commit", "-m", "fixture"], check=True, capture_output=True)


def _write_fixture(root: Path, *, source_text: str) -> None:
    design = root / "docs" / "architecture" / "demo_runtime_design.md"
    design.parent.mkdir(parents=True, exist_ok=True)
    design.write_text("# Demo Runtime\n\nRequires DurableRuntime and durable EventLoop semantics.\n", encoding="utf-8")
    code = root / "backend" / "services" / "demo_runtime" / "runtime.py"
    code.parent.mkdir(parents=True, exist_ok=True)
    code.write_text(source_text, encoding="utf-8")


def test_design_drift_audit_deterministic_ready_when_markers_exist(tmp_path: Path) -> None:
    _write_fixture(tmp_path, source_text="class DurableRuntime:\n    EventLoop = object\n")
    config_path = _write_config(tmp_path)
    llm_config = _write_llm_config(tmp_path)
    _init_repo(tmp_path)

    payload = audit.build_audit(
        root=tmp_path,
        config_path=config_path,
        llm_config_path=llm_config,
        provider="deterministic",
        modules=["demo_runtime"],
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["findings"] == []
    assert payload["warning_only"] is True
    assert payload["official_bug_creation_allowed"] is False
    assert payload["github_issue_creation_allowed"] is False
    assert payload["side_effects"]["writes_source"] is False
    assert payload["production_gates"]["production_ddl_gate"] == "noop"


def test_design_drift_audit_detects_missing_marker_without_bug_creation(tmp_path: Path) -> None:
    _write_fixture(tmp_path, source_text="class DurableRuntime:\n    pass\n")
    config_path = _write_config(tmp_path)
    llm_config = _write_llm_config(tmp_path)
    _init_repo(tmp_path)

    payload = audit.build_audit(
        root=tmp_path,
        config_path=config_path,
        llm_config_path=llm_config,
        provider="deterministic",
        modules=["demo_runtime"],
    )

    assert payload["workflow_gate"] == "warning"
    assert payload["summary"]["finding_count"] == 1
    finding = payload["findings"][0]
    assert finding["official_bug_created"] is False
    assert finding["github_issue_created"] is False
    assert finding["next_action"] == "manual_analysis_required_before_bug_registration"
    assert "EventLoop" in finding["suspected_drift"]


def test_design_drift_audit_rejects_llm_action_fields(monkeypatch, tmp_path: Path) -> None:
    _write_fixture(tmp_path, source_text="class DurableRuntime:\n    EventLoop = object\n")
    config_path = _write_config(tmp_path)
    llm_config = _write_llm_config(tmp_path)
    _init_repo(tmp_path)

    def fake_invoke(*args, **kwargs):
        return {
            "provider": "deepseek_api",
            "model": "deepseek-v4-pro",
            "credential_source": "test",
            "payload": {"findings": [{"module": "demo_runtime", "command": "python bad.py"}]},
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
        }

    monkeypatch.setattr(audit.llm_adapter, "invoke_provider_json", fake_invoke)

    payload = audit.build_audit(
        root=tmp_path,
        config_path=config_path,
        llm_config_path=llm_config,
        provider="deepseek_api",
        modules=["demo_runtime"],
        invoke_llm=True,
    )

    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert payload["llm_invocation_evidence"]["reason"] == "design_drift_audit_live_provider_failed_fallback"
    assert payload["llm_gate"] == "degraded"
    assert payload["workflow_gate"] == "warning"
    assert payload["findings"] == []
    assert payload["summary"]["degraded_reason"] == "llm_provider_failed_no_marker_findings_emitted"
    assert payload["official_bug_creation_allowed"] is False


def test_design_drift_audit_uses_compact_llm_input_and_large_output_budget(monkeypatch, tmp_path: Path) -> None:
    long_source = "\n".join(
        f"class DurableRuntime{i}: pass  # fallback runtime marker {i} " + "x" * 300
        for i in range(12)
    )
    _write_fixture(tmp_path, source_text=long_source)
    design = tmp_path / "docs" / "architecture" / "demo_runtime_design.md"
    design.write_text(
        "# Demo Runtime\n\n"
        + "Requires DurableRuntime and durable EventLoop semantics. "
        + "design-detail " * 500,
        encoding="utf-8",
    )
    config_path = _write_config(tmp_path, drift_markers=["fallback"])
    llm_config = _write_llm_config(tmp_path)
    _init_repo(tmp_path)
    captured: dict[str, object] = {}

    def fake_invoke(provider, config, *, purpose, messages, max_tokens=900, timeout_seconds=45):
        captured["provider"] = provider
        captured["purpose"] = purpose
        captured["messages"] = messages
        captured["max_tokens"] = max_tokens
        captured["timeout_seconds"] = timeout_seconds
        return {
            "provider": provider,
            "model": "deepseek-v4-pro",
            "credential_source": "test",
            "payload": {"summary": "ok", "findings": []},
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
        }

    monkeypatch.setattr(audit.llm_adapter, "invoke_provider_json", fake_invoke)

    payload = audit.build_audit(
        root=tmp_path,
        config_path=config_path,
        llm_config_path=llm_config,
        provider="deepseek_api",
        modules=["demo_runtime"],
        invoke_llm=True,
    )

    user_payload = json.loads(captured["messages"][1]["content"])  # type: ignore[index]
    target = user_payload["input"]["review_targets"][0]
    assert payload["llm_gate"] == "ready"
    assert captured["max_tokens"] == audit.LLM_MAX_OUTPUT_TOKENS
    assert captured["timeout_seconds"] == audit.LLM_TIMEOUT_SECONDS
    assert len(target["design_docs"][0]["excerpt"]) <= audit.LLM_MAX_DESIGN_CHARS
    assert len(target["code_samples"][0]["excerpt"]) <= audit.LLM_MAX_CODE_SAMPLE_CHARS
    assert len(target["drift_risk_hits"]) <= audit.LLM_MAX_MARKER_HITS


def test_design_drift_audit_llm_failure_does_not_emit_marker_findings(monkeypatch, tmp_path: Path) -> None:
    _write_fixture(tmp_path, source_text="class DurableRuntime:\n    pass  # temporary fallback runtime\n")
    config_path = _write_config(tmp_path, drift_markers=["fallback"])
    llm_config = _write_llm_config(tmp_path)
    _init_repo(tmp_path)

    def fake_invoke(*args, **kwargs):
        raise audit.llm_adapter.ProviderAdapterError("provider output JSON schema invalid")

    monkeypatch.setattr(audit.llm_adapter, "invoke_provider_json", fake_invoke)

    payload = audit.build_audit(
        root=tmp_path,
        config_path=config_path,
        llm_config_path=llm_config,
        provider="deepseek_api",
        modules=["demo_runtime"],
        invoke_llm=True,
    )
    artifact = audit.public_artifact(payload)
    markdown = tmp_path / "out" / "design.md"
    audit.write_markdown(markdown, payload)

    assert payload["llm_gate"] == "degraded"
    assert payload["workflow_gate"] == "warning"
    assert payload["findings"] == []
    assert payload["summary"]["finding_count"] == 0
    assert payload["summary"]["deterministic_signal_count"] >= 1
    assert artifact["llm_invocation_evidence"]["error_type"] == "ProviderAdapterError"
    assert artifact["llm_invocation_evidence"]["error_fingerprint"]
    text = markdown.read_text(encoding="utf-8")
    assert "LLM Audit Degraded" in text
    assert "marker-only deterministic signals were not promoted" in text


def test_design_drift_audit_cli_writes_public_artifacts(tmp_path: Path, capsys) -> None:
    _write_fixture(tmp_path, source_text="class DurableRuntime:\n    EventLoop = object\n")
    config_path = _write_config(tmp_path)
    llm_config = _write_llm_config(tmp_path)
    _init_repo(tmp_path)
    output = tmp_path / "out" / "audit.json"
    markdown = tmp_path / "out" / "audit.md"

    exit_code = audit.main([
        "--json",
        "--root",
        str(tmp_path),
        "--config",
        str(config_path),
        "--llm-config",
        str(llm_config),
        "--provider",
        "deterministic",
        "--module",
        "demo_runtime",
        "--output",
        str(output),
        "--markdown-output",
        str(markdown),
    ])
    stdout = capsys.readouterr().out
    artifact = json.loads(output.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert '"check": "nightly-design-drift-audit"' in stdout
    assert artifact["public_artifact"] is True
    assert artifact["candidate_only"] is True
    assert artifact["source_modifications_allowed"] is False
    assert "Nightly LLM Design Drift Audit" in markdown.read_text(encoding="utf-8")
