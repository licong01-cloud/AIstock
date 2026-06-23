from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts import nightly_silent_degradation_audit as audit


def _write_config(root: Path, *, silent_markers: list[str] | None = None) -> Path:
    config = {
        "schema_version": "aistock_silent_degradation_audit_config_v1",
        "defaults": {
            "max_reference_chars_per_doc": 800,
            "max_code_files_per_module": 4,
            "max_code_excerpt_chars_per_file": 500,
        },
        "modules": [
            {
                "module": "demo_runtime",
                "risk": "P1",
                "reference_docs": ["docs/architecture/demo_runtime_contract.md"],
                "code_paths": ["backend/services/demo_runtime"],
                "requirement_keywords": ["durable", "event loop"],
                "semantic_expected_markers": ["DurableRuntime", "EventLoop"],
                "silent_degradation_markers": silent_markers or [],
            }
        ],
    }
    path = root / "configs" / "validation" / "silent_degradation_audit.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def _write_llm_config(root: Path) -> Path:
    payload = yaml.safe_load(audit.llm_adapter.DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    path = root / "configs" / "validation" / "llm_triage.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _write_prompt_pack(root: Path) -> Path:
    payload = {
        "purpose": "Review compact evidence for hidden fallback risks.",
        "allowed_output": {
            "summary": "short string",
            "findings": [
                {
                    "module": "module key",
                    "severity": "P1|P2|P3",
                    "title": "short title",
                    "suspected_silent_degradation": "risk",
                    "expected_behavior": "contract",
                    "observed_code_evidence": "evidence",
                    "reference_refs": ["doc"],
                    "code_refs": ["file:line"],
                    "confidence": 0.0,
                }
            ],
        },
        "safety": {
            "warning_only": True,
            "candidate_only": True,
            "no_source_modification": True,
            "no_bug_json_write": True,
            "no_github_issue_create": True,
        },
    }
    path = root / "prompt_packs" / "validation_llm" / "silent_degradation_audit.prompt.yml"
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
    reference = root / "docs" / "architecture" / "demo_runtime_contract.md"
    reference.parent.mkdir(parents=True, exist_ok=True)
    reference.write_text(
        "# Demo Runtime\n\nRequires DurableRuntime and durable EventLoop semantics with explicit failure reporting.\n",
        encoding="utf-8",
    )
    code = root / "backend" / "services" / "demo_runtime" / "runtime.py"
    code.parent.mkdir(parents=True, exist_ok=True)
    code.write_text(source_text, encoding="utf-8")


def test_silent_degradation_audit_deterministic_ready_when_markers_exist(tmp_path: Path) -> None:
    _write_fixture(tmp_path, source_text="class DurableRuntime:\n    EventLoop = object\n")
    config_path = _write_config(tmp_path)
    llm_config = _write_llm_config(tmp_path)
    prompt_pack = _write_prompt_pack(tmp_path)
    _init_repo(tmp_path)

    payload = audit.build_audit(
        root=tmp_path,
        config_path=config_path,
        llm_config_path=llm_config,
        prompt_pack_path=prompt_pack,
        provider="deterministic",
        modules=["demo_runtime"],
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["findings"] == []
    assert payload["warning_only"] is True
    assert payload["candidate_only"] is True
    assert payload["source_modifications_allowed"] is False
    assert payload["official_bug_creation_allowed"] is False
    assert payload["github_issue_creation_allowed"] is False
    assert payload["side_effects"]["writes_source"] is False
    assert payload["production_gates"]["production_ddl_gate"] == "noop"


def test_silent_degradation_audit_detects_missing_marker_without_bug_creation(tmp_path: Path) -> None:
    _write_fixture(tmp_path, source_text="class DurableRuntime:\n    pass\n")
    config_path = _write_config(tmp_path)
    llm_config = _write_llm_config(tmp_path)
    prompt_pack = _write_prompt_pack(tmp_path)
    _init_repo(tmp_path)

    payload = audit.build_audit(
        root=tmp_path,
        config_path=config_path,
        llm_config_path=llm_config,
        prompt_pack_path=prompt_pack,
        provider="deterministic",
        modules=["demo_runtime"],
    )

    assert payload["workflow_gate"] == "warning"
    assert payload["summary"]["finding_count"] == 1
    finding = payload["findings"][0]
    assert finding["official_bug_created"] is False
    assert finding["github_issue_created"] is False
    assert finding["next_action"] == "manual_analysis_required_before_bug_registration"
    assert finding["manual_validation_suggestion"]
    assert "EventLoop" in finding["suspected_silent_degradation"]


def test_silent_degradation_audit_detects_risk_marker_as_candidate_only(tmp_path: Path) -> None:
    _write_fixture(
        tmp_path,
        source_text="class DurableRuntime:\n    EventLoop = object\n    def run(self):\n        return []  # fallback empty success\n",
    )
    config_path = _write_config(tmp_path, silent_markers=["return []", "fallback"])
    llm_config = _write_llm_config(tmp_path)
    prompt_pack = _write_prompt_pack(tmp_path)
    _init_repo(tmp_path)

    payload = audit.build_audit(
        root=tmp_path,
        config_path=config_path,
        llm_config_path=llm_config,
        prompt_pack_path=prompt_pack,
        provider="deterministic",
        modules=["demo_runtime"],
    )

    assert payload["workflow_gate"] == "warning"
    finding = payload["findings"][0]
    assert finding["source"] == "deterministic_silent_degradation_marker_check"
    assert finding["why_this_is_not_normal_fallback"]
    assert finding["official_bug_created"] is False


def test_silent_degradation_audit_rejects_llm_action_fields(monkeypatch, tmp_path: Path) -> None:
    _write_fixture(tmp_path, source_text="class DurableRuntime:\n    EventLoop = object\n")
    config_path = _write_config(tmp_path)
    llm_config = _write_llm_config(tmp_path)
    prompt_pack = _write_prompt_pack(tmp_path)
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
        prompt_pack_path=prompt_pack,
        provider="deepseek_api",
        modules=["demo_runtime"],
        invoke_llm=True,
    )

    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert payload["llm_invocation_evidence"]["reason"] == "silent_degradation_audit_live_provider_failed_fallback"
    assert payload["official_bug_creation_allowed"] is False


def test_silent_degradation_audit_uses_llm_output_instead_of_marker_templates(monkeypatch, tmp_path: Path) -> None:
    _write_fixture(
        tmp_path,
        source_text="class DurableRuntime:\n    EventLoop = object\n    def run(self):\n        return []\n",
    )
    config_path = _write_config(tmp_path, silent_markers=["return []"])
    llm_config = _write_llm_config(tmp_path)
    prompt_pack = _write_prompt_pack(tmp_path)
    _init_repo(tmp_path)

    def fake_invoke(*args, **kwargs):
        messages = kwargs["messages"]
        assert "Review compact evidence for hidden fallback risks." in messages[0]["content"]
        return {
            "provider": "deepseek_api",
            "model": "deepseek-v4-pro",
            "credential_source": "test",
            "payload": {
                "summary": "legitimate explicit fallback; no candidate",
                "findings": [],
            },
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
        }

    monkeypatch.setattr(audit.llm_adapter, "invoke_provider_json", fake_invoke)

    payload = audit.build_audit(
        root=tmp_path,
        config_path=config_path,
        llm_config_path=llm_config,
        prompt_pack_path=prompt_pack,
        provider="deepseek_api",
        modules=["demo_runtime"],
        invoke_llm=True,
    )

    assert payload["llm_invocation_evidence"]["invoked"] is True
    assert payload["findings"] == []
    assert payload["workflow_gate"] == "ready"


def test_silent_degradation_audit_cli_writes_public_artifacts(tmp_path: Path, capsys) -> None:
    _write_fixture(tmp_path, source_text="class DurableRuntime:\n    EventLoop = object\n")
    config_path = _write_config(tmp_path)
    llm_config = _write_llm_config(tmp_path)
    prompt_pack = _write_prompt_pack(tmp_path)
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
        "--prompt-pack",
        str(prompt_pack),
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
    assert '"check": "nightly-silent-degradation-audit"' in stdout
    assert artifact["public_artifact"] is True
    assert artifact["candidate_only"] is True
    assert artifact["source_modifications_allowed"] is False
    assert artifact["review_targets"][0]["silent_degradation_hit_count"] == 0
    assert "Nightly LLM Silent Degradation Audit" in markdown.read_text(encoding="utf-8")
