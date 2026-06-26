from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts import nightly_silent_degradation_audit as audit


def _write_config(
    root: Path,
    *,
    silent_markers: list[str] | None = None,
    suppressions: list[dict] | None = None,
) -> Path:
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
    if suppressions is not None:
        config["suppressions"] = suppressions
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


def _demo_suppression(**overrides) -> dict:
    suppression = {
        "module": "demo_runtime",
        "code_refs_any": ["backend/services/demo_runtime/runtime.py"],
        "title_contains": "silent degradation",
        "reason": "manual false-positive review",
        "dismissed_by": "tier2-review",
        "dismissed_at": "2026-06-24",
        "expires_at": "2099-12-31",
    }
    suppression.update(overrides)
    return suppression


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


def test_silent_degradation_audit_suppresses_matching_finding_and_ready_gate(tmp_path: Path) -> None:
    _write_fixture(
        tmp_path,
        source_text="class DurableRuntime:\n    EventLoop = object\n    def run(self):\n        return []  # fallback empty success\n",
    )
    config_path = _write_config(
        tmp_path,
        silent_markers=["return []"],
        suppressions=[_demo_suppression()],
    )
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
    assert payload["summary"]["finding_count"] == 0
    assert payload["summary"]["suppressed_count"] == 1
    suppressed = payload["suppressed_findings"][0]
    assert suppressed["module"] == "demo_runtime"
    assert suppressed["suppressed_by"]["reason"] == "manual false-positive review"
    assert suppressed["suppressed_by"]["matched_suppression_index"] == 0


def test_silent_degradation_audit_keeps_nonmatching_suppression_as_warning(tmp_path: Path) -> None:
    _write_fixture(
        tmp_path,
        source_text="class DurableRuntime:\n    EventLoop = object\n    def run(self):\n        return []  # fallback empty success\n",
    )
    config_path = _write_config(
        tmp_path,
        silent_markers=["return []"],
        suppressions=[
            _demo_suppression(module="other_runtime"),
            _demo_suppression(code_refs_any=["backend/services/other_runtime/runtime.py"]),
        ],
    )
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
    assert payload["summary"]["suppressed_count"] == 0
    assert payload["suppressed_findings"] == []


def test_silent_degradation_audit_expired_suppression_is_inactive(tmp_path: Path) -> None:
    _write_fixture(
        tmp_path,
        source_text="class DurableRuntime:\n    EventLoop = object\n    def run(self):\n        return []  # fallback empty success\n",
    )
    config_path = _write_config(
        tmp_path,
        silent_markers=["return []"],
        suppressions=[_demo_suppression(expires_at="2026-01-01")],
    )
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
    assert len(payload["findings"]) == 1
    assert payload["suppressed_findings"] == []


def test_silent_degradation_audit_configured_miniqmt_sda_2d29_is_suppressed() -> None:
    config = audit.read_yaml(audit.DEFAULT_CONFIG_PATH)
    finding = audit.make_finding(
        module="miniqmt_execution_runtime",
        severity="P1",
        title="MiniQMT runtime 缺 fail-closed",
        suspected_silent_degradation="manual candidate",
        reference_refs=["docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md"],
        code_refs=["backend/services/miniqmt_execution_runtime/runtime.py:845"],
        confidence=0.7,
        source="unit_test",
    )
    finding["finding_id"] = "SDA-2d2969408339"

    findings, suppressed_findings = audit.apply_suppressions(
        [finding],
        config,
        audit_date=audit.date(2026, 6, 24),
    )

    assert findings == []
    assert len(suppressed_findings) == 1
    assert suppressed_findings[0]["suppressed_by"]["dismissed_by"] == "tier2-review"
    assert suppressed_findings[0]["suppressed_by"]["expires_at"] == "2026-09-24"


def test_silent_degradation_audit_invalid_suppression_without_match_key_raises(tmp_path: Path) -> None:
    _write_fixture(tmp_path, source_text="class DurableRuntime:\n    EventLoop = object\n")
    config_path = _write_config(
        tmp_path,
        suppressions=[
            {
                "module": "demo_runtime",
                "reason": "invalid",
                "dismissed_by": "tier2-review",
                "dismissed_at": "2026-06-24",
            }
        ],
    )
    llm_config = _write_llm_config(tmp_path)
    prompt_pack = _write_prompt_pack(tmp_path)
    _init_repo(tmp_path)

    try:
        audit.build_audit(
            root=tmp_path,
            config_path=config_path,
            llm_config_path=llm_config,
            prompt_pack_path=prompt_pack,
            provider="deterministic",
            modules=["demo_runtime"],
        )
    except audit.SilentDegradationAuditError as exc:
        assert "finding_id or code_refs_any" in str(exc)
    else:
        raise AssertionError("invalid suppression must raise")


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
    assert payload["llm_gate"] == "degraded"
    assert payload["workflow_gate"] == "warning"
    assert payload["findings"] == []
    assert payload["summary"]["degraded_reason"] == "llm_provider_failed_no_marker_findings_emitted"
    assert payload["official_bug_creation_allowed"] is False


def test_silent_degradation_audit_llm_failure_does_not_emit_marker_findings(monkeypatch, tmp_path: Path) -> None:
    _write_fixture(
        tmp_path,
        source_text="class DurableRuntime:\n    EventLoop = object\n    def run(self):\n        return []\n",
    )
    config_path = _write_config(tmp_path, silent_markers=["return []"])
    llm_config = _write_llm_config(tmp_path)
    prompt_pack = _write_prompt_pack(tmp_path)
    _init_repo(tmp_path)

    def fake_invoke(*args, **kwargs):
        raise audit.llm_adapter.ProviderAdapterError("provider output JSON schema invalid")

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
    artifact = audit.public_artifact(payload)
    markdown = tmp_path / "out" / "silent.md"
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
    assert "suppressed_findings" in artifact
    assert artifact["source_modifications_allowed"] is False
    assert artifact["review_targets"][0]["silent_degradation_hit_count"] == 0
    assert "Nightly LLM Silent Degradation Audit" in markdown.read_text(encoding="utf-8")


def test_silent_degradation_audit_public_artifact_includes_suppressed_findings() -> None:
    payload = {
        "schema_version": audit.SCHEMA_VERSION,
        "generated_at": "2026-06-24T00:00:00Z",
        "run_id": "unit",
        "commit": "abc",
        "branch": "test",
        "provider": "deterministic",
        "model": "deterministic",
        "effective_provider": "deterministic",
        "effective_model": "deterministic",
        "llm_gate": "degraded",
        "workflow_gate": "ready",
        "review_targets": [],
        "findings": [],
        "suppressed_findings": [
            {
                "finding_id": "SDA-test",
                "module": "demo_runtime",
                "title": "suppressed",
                "suppressed_by": {"reason": "manual review"},
            }
        ],
        "summary": {"review_target_count": 0, "finding_count": 0, "suppressed_count": 1},
        "llm_invocation_evidence": {"invoked": False},
        "side_effects": {},
        "production_gates": audit.PRODUCTION_GATES,
    }

    artifact = audit.public_artifact(payload)

    assert artifact["suppressed_findings"][0]["finding_id"] == "SDA-test"


def test_silent_degradation_audit_no_suppressions_keeps_existing_behavior(tmp_path: Path) -> None:
    _write_fixture(
        tmp_path,
        source_text="class DurableRuntime:\n    EventLoop = object\n    def run(self):\n        return []  # fallback empty success\n",
    )
    config_path = _write_config(tmp_path, silent_markers=["return []"])
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
    assert payload["summary"]["suppressed_count"] == 0
    assert payload["suppressed_findings"] == []
