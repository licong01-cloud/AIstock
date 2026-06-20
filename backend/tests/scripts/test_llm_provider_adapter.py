from __future__ import annotations

import json

import pytest

from scripts import llm_provider_adapter as adapter


def test_llm_triage_config_defaults_are_safe():
    config = adapter.load_config()

    adapter.validate_config(config)

    assert config["default_provider"] == "github_models"
    assert config["providers"]["deepseek_api"]["enabled"] is True
    assert config["providers"]["deepseek_api"]["model"] == "deepseek-v4-pro"
    selector = config["providers"]["github_models"]["model_selector"]
    assert selector["required_model_family"] == "deepseek-r1"
    assert selector["preferred_models"] == ["deepseek/deepseek-r1"]
    assert selector["allow_lower_tier_fallback"] is False


def test_parse_json_response_extracts_fenced_or_prose_wrapped_object():
    payload = adapter.parse_json_response('Here is the answer:\n```json\n{"summary":"ok","suggested_plan_keys":["l0"]}\n```')

    assert payload["summary"] == "ok"
    assert payload["suggested_plan_keys"] == ["l0"]


def test_validate_config_allows_enabled_deepseek_v4_pro_fallback():
    config = adapter.load_config()
    config["providers"]["deepseek_api"]["enabled"] = True

    adapter.validate_config(config)

    config["providers"]["deepseek_api"]["model"] = "deepseek-v3"
    with pytest.raises(adapter.ProviderAdapterError):
        adapter.validate_config(config)


def test_validate_deepseek_provider_bootstraps_from_canonical_env_file(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        'DEEPSEEK_API_KEY="env-file-secret"\nDEEPSEEK_BASE_URL="https://api.deepseek.com/v1"\n',
        encoding="utf-8",
    )
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.delenv("DEEPSEEK_BASE_URL", raising=False)
    monkeypatch.setenv("AISTOCK_LLM_ENV_FILE", str(env_file))

    payload = adapter.validate_deepseek_provider(adapter.load_config(), require_api_key=True)

    assert payload["has_api_key"] is True
    assert payload["credential_source"] == "env:DEEPSEEK_API_KEY"
    assert payload["base_url"] == "https://api.deepseek.com/v1"


def test_github_model_catalog_selects_deepseek_r1():
    selector = adapter.load_config()["providers"]["github_models"]["model_selector"]
    catalog = {
        "models": [
            {
                "id": "openai/gpt-test",
                "publisher": "OpenAI",
                "capabilities": ["tool-calling", "reasoning"],
            },
            {
                "id": "deepseek/deepseek-r1",
                "publisher": {"name": "DeepSeek"},
                "capabilities": {"tool-calling": True, "reasoning": True, "streaming": True},
            },
        ]
    }

    selected = adapter.select_github_model(catalog, selector)

    assert selected["model_id"] == "deepseek/deepseek-r1"
    assert selected["publisher"] == "DeepSeek"
    assert selected["capabilities"] == ["reasoning", "streaming", "tool-calling"]


def test_github_model_catalog_fails_closed_without_r1():
    selector = adapter.load_config()["providers"]["github_models"]["model_selector"]
    catalog = {
        "models": [
            {
                "id": "deepseek/deepseek-v3-0324",
                "publisher": "DeepSeek",
                "capabilities": ["tool-calling"],
            }
        ]
    }

    with pytest.raises(adapter.ProviderAdapterError):
        adapter.select_github_model(catalog, selector)


def test_fetch_github_models_catalog_falls_back_to_gh_auth_token(monkeypatch):
    config = adapter.load_config()
    calls: list[list[str]] = []

    def fake_run(args, **kwargs):
        calls.append(args)

        class Result:
            returncode = 0
            stdout = "gh-token\n"
            stderr = ""

        return Result()

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return json.dumps({"models": [{"id": "deepseek/deepseek-r1", "publisher": "DeepSeek"}]}).encode("utf-8")

    captured_headers: dict[str, str] = {}

    def fake_urlopen(request, timeout=20):
        captured_headers.update(dict(request.header_items()))
        return FakeResponse()

    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.setattr(adapter.subprocess, "run", fake_run)
    monkeypatch.setattr(adapter.urllib.request, "urlopen", fake_urlopen)

    payload = adapter.fetch_github_models_catalog(config)

    assert payload["models"][0]["id"] == "deepseek/deepseek-r1"
    assert calls == [["gh", "auth", "token"]]
    assert captured_headers["Authorization"] == "Bearer gh-token"


def test_fetch_github_models_catalog_uses_gh_token_env_before_cli(monkeypatch):
    config = adapter.load_config()
    called_cli = False

    def fake_run(*args, **kwargs):
        nonlocal called_cli
        called_cli = True

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b'{"models":[]}'

    captured_headers: dict[str, str] = {}

    def fake_urlopen(request, timeout=20):
        captured_headers.update(dict(request.header_items()))
        return FakeResponse()

    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setenv("GH_TOKEN", "env-gh-token")
    monkeypatch.setattr(adapter.subprocess, "run", fake_run)
    monkeypatch.setattr(adapter.urllib.request, "urlopen", fake_urlopen)

    adapter.fetch_github_models_catalog(config)

    assert captured_headers["Authorization"] == "Bearer env-gh-token"
    assert called_cli is False


def test_invalid_provider_json_fails_closed():
    with pytest.raises(adapter.ProviderAdapterError):
        adapter.parse_json_response("{not-json")

    with pytest.raises(adapter.ProviderAdapterError):
        adapter.parse_json_response("[1, 2, 3]")


def test_validate_config_cli_uses_compact_success_output(capsys):
    exit_code = adapter.main(["--json", "validate-config", "--provider", "deterministic"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"gate": "passed"' in captured.out
    assert "DEEPSEEK_API_KEY" not in captured.out


def test_triage_quality_smoke_has_schema_issue_draft_and_allowlisted_plans():
    payload = adapter.build_triage_quality_smoke("github_models", adapter.load_config())

    assert payload["schema_version"] == adapter.TRIAGE_ADVICE_SCHEMA_VERSION
    assert payload["provider"] == "github_models"
    assert payload["model"] == "deepseek/deepseek-r1"
    assert payload["actionability"]["is_actionable"] is True
    assert payload["issue_draft"]["contains_reproduce_command"] is True
    assert payload["issue_draft"]["full_logs_included"] is False
    assert payload["prompt_quality"]["full_repo_scan_allowed"] is False
    assert [item["plan_key"] for item in payload["test_plan_advice"]] == ["validation_module_registry_l0", "l0"]
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert payload["deterministic_gate"]["plan_keys_allowlisted"] is True


def test_triage_quality_smoke_cli_uses_compact_success_output(capsys, tmp_path):
    output = tmp_path / "triage-advice.json"

    exit_code = adapter.main(
        [
            "--json",
            "triage-quality-smoke",
            "--provider",
            "github_models",
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"check": "triage-quality-smoke"' in captured.out
    assert '"suggested_plan_count": 2' in captured.out
    assert "DeepSeek API key" not in captured.out
    assert output.exists()


def test_test_plan_advice_allows_runner_enabled_catalog_plan():
    payload = adapter.build_test_plan_advice(
        "github_models",
        adapter.load_config(),
        plan_keys=["l0", "validation_catalog_integrity", "validation_center_backend"],
        changed_files=["scripts/llm_provider_adapter.py"],
        module="validation.runner",
    )

    assert payload["schema_version"] == adapter.TEST_PLAN_ADVICE_SCHEMA_VERSION
    assert payload["provider"] == "github_models"
    assert payload["deterministic_gate"]["workflow_gate"] == "ready"
    assert payload["deterministic_gate"]["runner_enabled_only"] is True
    assert payload["deterministic_gate"]["command_keys_allowlisted"] is True
    assert payload["deterministic_gate"]["validation_select_compatible"] is True
    assert payload["deterministic_gate"]["shell_commands_allowed"] is False
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert [item["plan_key"] for item in payload["test_plan_advice"]] == [
        "l0",
        "validation_catalog_integrity",
        "validation_center_backend",
    ]


def test_test_plan_advice_blocks_unknown_non_runner_and_production_state_plans():
    config = adapter.load_config()

    unknown = adapter.build_test_plan_advice("deterministic", config, plan_keys=["missing_plan"])
    assert unknown["deterministic_gate"]["workflow_gate"] == "blocked"
    assert unknown["test_plan_advice"][0]["rejection_reasons"] == ["unknown_plan_key"]

    non_runner = adapter.build_test_plan_advice("deterministic", config, plan_keys=["validation_center_ui"])
    assert non_runner["deterministic_gate"]["workflow_gate"] == "blocked"
    assert "runner_not_enabled" in non_runner["test_plan_advice"][0]["rejection_reasons"]

    business_state = adapter.build_test_plan_advice("deterministic", config, plan_keys=["miniqmt_sim_trading_hours_l5"])
    assert business_state["deterministic_gate"]["workflow_gate"] == "blocked"
    assert "runner_not_enabled" in business_state["test_plan_advice"][0]["rejection_reasons"]
    assert "writes_business_state" in business_state["test_plan_advice"][0]["rejection_reasons"]


def test_test_plan_advice_rejects_unregistered_workspace_path(tmp_path):
    payload = adapter.build_test_plan_advice(
        "deterministic",
        adapter.load_config(),
        plan_keys=["l0"],
        workspace_path=str(tmp_path),
    )

    assert payload["deterministic_gate"]["workflow_gate"] == "blocked"
    assert payload["workspace_gate"]["allowed"] is False
    assert payload["workspace_gate"]["reason"] == "workspace_path_not_registered_git_worktree"


def test_test_plan_advice_rejects_shell_command_fields():
    with pytest.raises(adapter.ProviderAdapterError):
        adapter.validate_test_plan_advice(
            {
                "schema_version": adapter.TEST_PLAN_ADVICE_SCHEMA_VERSION,
                "test_plan_advice": [{"plan_key": "l0", "command": "python -m nox -s l0"}],
                "deterministic_gate": {"shell_commands_allowed": False},
            }
        )


def test_test_plan_advice_cli_uses_compact_success_output(capsys, tmp_path):
    output = tmp_path / "test-plan-advice.json"

    exit_code = adapter.main(
        [
            "--json",
            "test-plan-advice",
            "--provider",
            "github_models",
            "--changed-file",
            "scripts/llm_provider_adapter.py",
            "--module",
            "validation.runner",
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"check": "test-plan-advice"' in captured.out
    assert '"workflow_gate": "passed"' not in captured.out
    assert '"advised_plan_count": 3' in captured.out
    assert '"llm_invoked": false' in captured.out
    assert output.exists()



def test_invoke_provider_json_posts_github_models_chat_request(monkeypatch):
    config = adapter.load_config()
    captured = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return json.dumps(
                {
                    "id": "resp-1",
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "summary": "use focused validation",
                                        "suggested_plan_keys": ["l0"],
                                        "confidence": 0.8,
                                    }
                                )
                            }
                        }
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
                }
            ).encode("utf-8")

    def fake_urlopen(request, timeout=45):
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setenv("GITHUB_TOKEN", "gh-test-token")
    monkeypatch.setattr(adapter.urllib.request, "urlopen", fake_urlopen)

    payload = adapter.invoke_provider_json(
        "github_models",
        config,
        purpose="test_plan_advice",
        messages=[{"role": "user", "content": "{}"}],
    )

    assert captured["url"].endswith("/inference/chat/completions")
    assert captured["headers"]["Authorization"] == "Bearer gh-test-token"
    assert captured["body"]["model"] == "deepseek/deepseek-r1"
    assert captured["body"]["response_format"] == {"type": "json_object"}
    assert payload["payload"]["suggested_plan_keys"] == ["l0"]
    assert payload["credential_source"] == "GITHUB_TOKEN"


def test_invoke_provider_json_retries_schema_invalid_once(monkeypatch):
    config = adapter.load_config()
    calls = 0

    class FakeResponse:
        def __init__(self, content: str):
            self.content = content

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return json.dumps(
                {"choices": [{"message": {"content": self.content}}], "usage": {"total_tokens": 1}}
            ).encode("utf-8")

    def fake_urlopen(request, timeout=45):
        nonlocal calls
        calls += 1
        if calls == 1:
            return FakeResponse("not json")
        return FakeResponse('{"summary":"fixed","suggested_plan_keys":["l0"]}')

    monkeypatch.setenv("GITHUB_TOKEN", "gh-test-token")
    monkeypatch.setattr(adapter.urllib.request, "urlopen", fake_urlopen)

    payload = adapter.invoke_provider_json(
        "github_models",
        config,
        purpose="test_plan_advice",
        messages=[{"role": "user", "content": "{}"}],
    )

    assert calls == 2
    assert payload["payload"]["summary"] == "fixed"


def test_test_plan_advice_can_invoke_llm_but_keeps_deterministic_gate(monkeypatch):
    def fake_invoke(provider, config, *, purpose, messages, max_tokens=900, timeout_seconds=45):
        return {
            "provider": provider,
            "model": "deepseek/deepseek-r1",
            "purpose": purpose,
            "payload": {
                "summary": "extra check",
                "rationale": "changed workflow script",
                "risk": "medium",
                "suggested_plan_keys": ["validation_catalog_integrity", "unknown"],
                "confidence": 0.7,
            },
            "credential_source": "GITHUB_TOKEN",
            "usage": {"prompt_tokens": 100, "completion_tokens": 40, "total_tokens": 140},
        }

    monkeypatch.setattr(adapter, "invoke_provider_json", fake_invoke)

    payload = adapter.build_test_plan_advice(
        "github_models",
        adapter.load_config(),
        changed_files=["scripts/llm_provider_adapter.py"],
        module="validation.runner",
        invoke_llm=True,
    )

    assert payload["deterministic_gate"]["workflow_gate"] == "ready"
    assert payload["llm_invocation_evidence"]["invoked"] is True
    assert payload["llm_invocation_evidence"]["usage_summary"]["total_units"] == 140
    assert payload["llm_advice"]["suggested_plan_keys"] == ["validation_catalog_integrity"]
    assert payload["llm_advice"]["ignored_plan_key_count"] == 1


def test_test_plan_advice_live_llm_error_falls_back_without_blocking(monkeypatch):
    def fake_invoke(*args, **kwargs):
        raise adapter.ProviderAdapterError("status=429 retry_after=10 token=ghp_secret")

    monkeypatch.setattr(adapter, "invoke_provider_json", fake_invoke)

    payload = adapter.build_test_plan_advice(
        "github_models",
        adapter.load_config(),
        plan_keys=["l0"],
        invoke_llm=True,
    )

    assert payload["deterministic_gate"]["workflow_gate"] == "ready"
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert payload["llm_invocation_evidence"]["reason"] == "test_plan_advice_live_provider_failed_fallback"
    assert "ghp_secret" not in payload["llm_invocation_evidence"]["error"]
    assert [item["provider"] for item in payload["llm_invocation_evidence"]["provider_chain"]] == [
        "github_models",
        "deepseek_api",
    ]


def test_test_plan_advice_falls_back_from_github_models_to_deepseek_api(monkeypatch):
    calls: list[str] = []

    def fake_invoke(provider, config, *, purpose, messages, max_tokens=900, timeout_seconds=45):
        calls.append(provider)
        if provider == "github_models":
            raise adapter.ProviderAdapterError("github_models inference request failed status=429")
        return {
            "provider": provider,
            "model": "deepseek-v4-pro",
            "purpose": purpose,
            "payload": {
                "summary": "deepseek fallback",
                "rationale": "github throttled",
                "risk": "medium",
                "suggested_plan_keys": ["l0"],
                "confidence": 0.8,
            },
            "credential_source": "env:DEEPSEEK_API_KEY",
            "usage": {"prompt_tokens": 10, "completion_tokens": 8, "total_tokens": 18},
        }

    monkeypatch.setattr(adapter, "invoke_provider_json", fake_invoke)

    payload = adapter.build_test_plan_advice(
        "github_models",
        adapter.load_config(),
        plan_keys=["l0"],
        invoke_llm=True,
    )

    assert calls == ["github_models", "deepseek_api"]
    assert payload["effective_provider"] == "deepseek_api"
    assert payload["llm_invocation_evidence"]["invoked"] is True
    assert payload["llm_invocation_evidence"]["fallback_used"] is True
    assert payload["llm_invocation_evidence"]["provider_chain"][0]["status"] == "failed"
    assert payload["llm_invocation_evidence"]["provider_chain"][1]["status"] == "invoked"
    assert payload["llm_advice"]["suggested_plan_keys"] == ["l0"]
    assert payload["advice_consumption"]["advice_consumed"] is True


def test_test_plan_public_artifact_keeps_safe_fallback_reason(monkeypatch, tmp_path):
    def fake_invoke(*args, **kwargs):
        raise adapter.ProviderAdapterError("github_models inference request failed status=429 token=ghp_secret")

    monkeypatch.setattr(adapter, "invoke_provider_json", fake_invoke)
    output = tmp_path / "test-plan-advice.json"

    exit_code = adapter.main(
        [
            "--json",
            "test-plan-advice",
            "--provider",
            "github_models",
            "--changed-file",
            "scripts/llm_provider_adapter.py",
            "--module",
            "validation.runner",
            "--invoke-llm",
            "--output",
            str(output),
        ]
    )
    artifact = json.loads(output.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert artifact["public_artifact"] is True
    assert artifact["schema_version"] == adapter.TEST_PLAN_ADVICE_SCHEMA_VERSION
    assert artifact["llm_invocation_evidence"]["invoked"] is False
    assert artifact["llm_invocation_evidence"]["reason"] == "test_plan_advice_live_provider_failed_fallback"
    assert "status=429" in artifact["llm_invocation_evidence"]["error"]
    assert "ghp_secret" not in json.dumps(artifact)
    assert artifact["llm_invoked"] is False
    assert "provider_chain" in artifact["llm_invocation_evidence"]


def test_test_plan_advice_includes_compact_code_intelligence_refs(tmp_path):
    code_refs = tmp_path / "code-intelligence-summary.json"
    code_refs.write_text(
        json.dumps(
            {
                "schema_version": "aistock_code_intelligence_summary_v1",
                "context_ref": "tmp/validation/code-intelligence/1/codegraph-context.md",
                "affected_tests_ref": "tmp/validation/code-intelligence/1/affected-tests.json",
                "affected_tests_count": 2,
                "understand_anything_summary_ref": "tmp/validation/code-intelligence/1/ua-validation-summary.md",
                "understand_anything": {"status": "available", "freshness": "base_current"},
                "context": {"large_payload": "x" * 1000},
            }
        ),
        encoding="utf-8",
    )

    payload = adapter.build_test_plan_advice(
        "deterministic",
        adapter.load_config(),
        changed_files=["scripts/llm_provider_adapter.py"],
        module="validation.runner",
        code_intelligence_refs=adapter.code_intelligence_refs_from_file(code_refs),
    )

    refs = payload["code_intelligence_refs"]
    assert refs["context_ref"].endswith("codegraph-context.md")
    assert refs["affected_tests_count"] == 2
    assert refs["understand_anything_summary_ref"].endswith("ua-validation-summary.md")
    assert "context" not in refs
    assert payload["llm_invocation_evidence"]["input_policy"] == "plan_key_intent_catalog_plus_code_intelligence_refs_only"


def test_compact_code_intelligence_refs_extracts_nested_verify_client_artifact() -> None:
    compact = adapter._compact_code_intelligence_refs(  # noqa: SLF001 - regression covers public artifact contract.
        {
            "artifacts": {
                "context_ref": "tmp/issue_workflow/BUG/codegraph-context.md",
                "affected_tests_ref": "tmp/issue_workflow/BUG/affected-tests.json",
                "ua_summary_ref": "tmp/validation/code-intelligence/BUG/ua-validation-summary.md",
            },
            "freshness": {
                "effective_freshness": "fresh",
                "effective_source": "live_status_current_head",
                "latest_artifact_ref": "tmp/validation/code-intelligence/latest/codegraph-freshness.json",
                "stale_metadata_warning": True,
            },
            "affected_tests": {
                "suggested_tests_count": 3,
                "quality": "partial_codegraph_plus_repo_fallback",
            },
            "understand_anything": {"status": "available", "freshness": "base_current"},
            "context": {"large_raw_payload": "must_not_leak"},
        }
    )

    assert compact["context_ref"].endswith("codegraph-context.md")
    assert compact["affected_tests_ref"].endswith("affected-tests.json")
    assert compact["affected_tests_count"] == 3
    assert compact["latest_freshness"] == "fresh"
    assert compact["latest_freshness_source"] == "live_status_current_head"
    assert compact["stale_metadata_warning"] is True
    assert compact["understand_anything_freshness"] == "base_current"
    assert "context" not in compact




def _discovery_pack(**overrides):
    payload = {
        "schema_version": "aistock_discovery_input_pack_v1",
        "run_id": "local-test",
        "commit": "abc123",
        "branch": "feature/test",
        "module": "validation",
        "changed_files": ["scripts/llm_provider_adapter.py"],
        "changed_files_count": 1,
        "input_quality": {"changed_files_status": "ok", "noise_filtered": True},
        "recent_failures": [],
        "recent_bug_clusters": [],
        "allowed_plan_keys": ["l0", "validation_catalog_integrity", "validation_center_backend", "workflow_discovery_root_clean_guard"],
        "stop_conditions": ["no_production_db_write", "allowlisted_plans_only"],
        "production_gates": {
            "production_ddl_gate": "noop",
            "production_frontend_dependency_gate": "noop",
            "production_backend_dependency_gate": "noop",
        },
    }
    payload.update(overrides)
    return payload


def test_nightly_discovery_hypothesis_deterministic_fallback_selects_allowlisted_plan():
    payload = adapter.build_nightly_discovery_hypotheses(
        "deterministic",
        adapter.load_config(),
        discovery_input_pack=_discovery_pack(),
        codegraph_freshness="fresh",
    )

    assert payload["schema_version"] == adapter.DISCOVERY_HYPOTHESIS_SCHEMA_VERSION
    assert payload["warning_only"] is True
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert payload["hypotheses"]
    assert payload["selected_plan_keys"] == ["validation_catalog_integrity"]
    assert "workflow_discovery_root_clean_guard" in payload["discovery_input_pack"]["allowed_plan_keys"]
    assert payload["deterministic_gate"]["shell_commands_allowed"] is False
    assert payload["deterministic_gate"]["production_actions_allowed"] is False


def test_nightly_discovery_hypothesis_invokes_deepseek_and_preserves_hypotheses(monkeypatch):
    def fake_raw_chat(*args, **kwargs):
        return (
            {
                "id": "deepseek-hypothesis",
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "summary": "validation workflow changed",
                                    "rationale": "run backend catalog checks",
                                    "hypotheses": [
                                        {
                                            "id": "H-LLM-1",
                                            "module": "validation.runner",
                                            "risk": "P1",
                                            "why_now": "workflow script changed",
                                            "expected_failure_modes": ["catalog drift"],
                                            "recommended_plan_keys": [
                                                "validation_catalog_integrity",
                                                "not_a_plan",
                                            ],
                                            "evidence_to_collect": ["catalog summary"],
                                            "stop_conditions": ["allowlisted_plans_only"],
                                        }
                                    ],
                                    "confidence": "0.74",
                                }
                            )
                        }
                    }
                ],
                "usage": {"prompt_tokens": 12, "completion_tokens": 8, "total_tokens": 20},
            },
            "deepseek-v4-pro",
            "env:DEEPSEEK_API_KEY",
            "https://api.deepseek.com/v1/chat/completions",
        )

    monkeypatch.setattr(adapter, "_invoke_provider_raw_chat", fake_raw_chat)

    payload = adapter.build_nightly_discovery_hypotheses(
        "deepseek_api",
        adapter.load_config(),
        discovery_input_pack=_discovery_pack(),
        codegraph_freshness="fresh",
        invoke_llm=True,
    )

    assert payload["effective_provider"] == "deepseek_api"
    assert payload["llm_gate"] == "ready"
    assert payload["llm_invocation_evidence"]["invoked"] is True
    assert payload["hypotheses"][0]["id"] == "H-LLM-1"
    assert payload["selected_plan_keys"] == ["validation_catalog_integrity"]
    assert {item["plan_key"] for item in payload["rejected_plan_keys"]} == {"not_a_plan"}
    assert payload["token_budget_used"] == 20


def test_nightly_discovery_hypothesis_rejects_non_allowlist_plan():
    payload = adapter.build_nightly_discovery_hypotheses(
        "deterministic",
        adapter.load_config(),
        discovery_input_pack=_discovery_pack(allowed_plan_keys=["l0"]),
        allowed_plan_keys=["l0"],
        codegraph_freshness="fresh",
    )

    assert payload["selected_plan_keys"] == ["l0"]
    assert all(plan_key in {"l0"} for plan_key in payload["selected_plan_keys"])
    assert payload["deterministic_gate"]["allowlist_enforced"] is True


def test_nightly_discovery_hypothesis_prefers_rotation_selected_plans():
    payload = adapter.build_nightly_discovery_hypotheses(
        "deterministic",
        adapter.load_config(),
        discovery_input_pack=_discovery_pack(
            changed_files=[],
            changed_files_count=0,
            allowed_plan_keys=[
                "l0",
                "validation_semantic_drift_discovery_readonly",
                "code_intelligence_discovery_affected_tests_quality",
                "workflow_discovery_root_clean_guard",
            ],
            rotation={
                "schema_version": "aistock_nightly_discovery_rotation_v1",
                "focus_key": "code_intelligence_llm",
                "focus_label": "CodeGraph / Understand Anything / LLM prompt quality",
                "focus_modules": ["code_intelligence"],
                "selected_plan_keys": [
                    "validation_semantic_drift_discovery_readonly",
                    "code_intelligence_discovery_affected_tests_quality",
                    "workflow_discovery_root_clean_guard",
                ],
                "readonly_only": True,
                "no_candidate_reason": "readonly_rotation_found_no_anomaly_yet",
            },
            discovery_statistics={
                "schema_version": "aistock_nightly_discovery_statistics_v1",
                "planned_plan_count": 2,
                "candidate_count": 0,
                "issue_payload_ready_count": 0,
            },
        ),
        codegraph_freshness="fresh",
    )

    assert payload["rotation"]["focus_key"] == "code_intelligence_llm"
    assert payload["selected_plan_keys"] == [
        "validation_semantic_drift_discovery_readonly",
        "code_intelligence_discovery_affected_tests_quality",
        "workflow_discovery_root_clean_guard",
    ]
    assert payload["hypotheses"][0]["why_now"].startswith("weekly rotation focus")
    assert payload["discovery_statistics"]["planned_plan_count"] == 2


def test_nightly_discovery_hypothesis_rejects_shell_command_fields(monkeypatch):
    def fake_raw_chat(*args, **kwargs):
        return (
            {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "hypotheses": [
                                        {
                                            "id": "H-unsafe",
                                            "recommended_plan_keys": ["l0"],
                                            "command": "python -m nox -s l0",
                                        }
                                    ]
                                }
                            )
                        }
                    }
                ]
            },
            "deepseek-v4-pro",
            "env:DEEPSEEK_API_KEY",
            "https://api.deepseek.com/v1/chat/completions",
        )

    monkeypatch.setattr(adapter, "_invoke_provider_raw_chat", fake_raw_chat)

    payload = adapter.build_nightly_discovery_hypotheses(
        "deepseek_api",
        adapter.load_config(),
        discovery_input_pack=_discovery_pack(allowed_plan_keys=["l0"]),
        codegraph_freshness="fresh",
        invoke_llm=True,
    )

    assert payload["llm_gate"] == "degraded"
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert "provider advice must not contain shell command fields" in payload["llm_invocation_evidence"]["error"]
    assert payload["selected_plan_keys"] == ["l0"]


def test_nightly_discovery_hypothesis_cli_uses_compact_success_output(capsys, tmp_path):
    input_pack = tmp_path / "discovery-input-pack.json"
    output = tmp_path / "llm-hypotheses.json"
    selected = tmp_path / "selected-plans.json"
    input_pack.write_text(json.dumps(_discovery_pack()), encoding="utf-8")

    exit_code = adapter.main(
        [
            "--json",
            "nightly-discovery-hypothesis",
            "--provider",
            "deterministic",
            "--input-pack",
            str(input_pack),
            "--codegraph-freshness",
            "fresh",
            "--allowed-plan-key",
            "l0,validation_catalog_integrity",
            "--output",
            str(output),
            "--selected-plans-output",
            str(selected),
        ]
    )
    captured = capsys.readouterr()
    artifact = json.loads(output.read_text(encoding="utf-8"))
    selected_payload = json.loads(selected.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert '"check": "nightly-discovery-hypothesis"' in captured.out
    assert '"hypothesis_count":' in captured.out
    assert '"llm_invoked": false' in captured.out
    assert artifact["schema_version"] == adapter.DISCOVERY_HYPOTHESIS_SCHEMA_VERSION
    assert artifact["public_artifact"] is True
    assert selected_payload["selected_plan_keys"] == ["validation_catalog_integrity"]
    assert "rotation" in selected_payload
    assert "code_intelligence_refs" in selected_payload


def test_nightly_discovery_selected_plans_artifact_keeps_code_intelligence_refs(capsys, tmp_path):
    input_pack = tmp_path / "discovery-input-pack.json"
    refs = tmp_path / "code-intelligence-summary.json"
    selected = tmp_path / "selected-plans.json"
    input_pack.write_text(json.dumps(_discovery_pack()), encoding="utf-8")
    refs.write_text(
        json.dumps(
            {
                "artifacts": {
                    "context_ref": "tmp/issue_workflow/NIGHTLY/codegraph-context.md",
                    "affected_tests_ref": "tmp/issue_workflow/NIGHTLY/affected-tests.json",
                    "ua_summary_ref": "tmp/validation/code-intelligence/NIGHTLY/ua-validation-summary.md",
                },
                "freshness": {
                    "effective_freshness": "fresh",
                    "latest_artifact_ref": "tmp/validation/code-intelligence/latest/codegraph-freshness.json",
                    "stale_metadata_warning": True,
                },
                "understand_anything": {"status": "available", "freshness": "base_current"},
            }
        ),
        encoding="utf-8",
    )

    exit_code = adapter.main(
        [
            "--json",
            "nightly-discovery-hypothesis",
            "--provider",
            "deterministic",
            "--input-pack",
            str(input_pack),
            "--codegraph-freshness",
            "fresh",
            "--code-intelligence-json",
            str(refs),
            "--selected-plans-output",
            str(selected),
        ]
    )
    capsys.readouterr()
    selected_payload = json.loads(selected.read_text(encoding="utf-8"))

    assert exit_code == 0
    compact_refs = selected_payload["code_intelligence_refs"]
    assert compact_refs["context_ref"].endswith("codegraph-context.md")
    assert compact_refs["latest_freshness"] == "fresh"
    assert compact_refs["stale_metadata_warning"] is True
    assert selected_payload["codegraph"]["warning_only"] is False

def test_nightly_scheduler_advice_uses_fixed_baseline_without_changes_or_failures():
    payload = adapter.build_nightly_scheduler_advice("deterministic", adapter.load_config(), codegraph_freshness="fresh")

    assert payload["schema_version"] == adapter.NIGHTLY_SCHEDULER_ADVICE_SCHEMA_VERSION
    assert payload["deterministic_gate"]["workflow_gate"] == "ready"
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert [item["plan_key"] for item in payload["queue"]] == ["l0"]
    assert payload["queue"][0]["priority"] == "baseline"
    assert payload["queue"][0]["allowed"] is True


def test_nightly_scheduler_advice_qe_ui_failure_recommends_l3_and_safe_runner_fallback():
    payload = adapter.build_nightly_scheduler_advice(
        "github_models",
        adapter.load_config(),
        recent_failure_modules=["qe_ui"],
        codegraph_freshness="fresh",
        resource_budget_seconds=1200,
    )
    queue = {item["plan_key"]: item for item in payload["queue"]}

    assert "qe_archive_l3" in queue
    assert queue["qe_archive_l3"]["allowed"] is False
    assert "runner_not_enabled" in queue["qe_archive_l3"]["deferred_reason"]
    assert queue["qe_archive_backend"]["allowed"] is True
    assert payload["deterministic_gate"]["production_actions_allowed"] is False


def test_nightly_scheduler_advice_defers_over_resource_budget():
    payload = adapter.build_nightly_scheduler_advice(
        "deterministic",
        adapter.load_config(),
        recent_failure_modules=["research_assistant"],
        codegraph_freshness="fresh",
        resource_budget_seconds=200,
    )

    item = payload["queue"][0]
    assert item["plan_key"] == "research_assistant_backend"
    assert item["allowed"] is False
    assert item["deferred_reason"] == "resource_budget_exceeded"
    assert payload["deterministic_gate"]["workflow_gate"] == "ready"


def test_nightly_scheduler_advice_codegraph_missing_is_warning_only():
    payload = adapter.build_nightly_scheduler_advice(
        "deterministic",
        adapter.load_config(),
        changed_files=["scripts/llm_provider_adapter.py"],
        codegraph_freshness="missing",
    )

    assert payload["deterministic_gate"]["workflow_gate"] == "warning"
    assert payload["codegraph"]["warning_only"] is True
    assert payload["deterministic_gate"]["allowed_plan_count"] >= 1


def test_nightly_scheduler_advice_includes_codegraph_and_ua_refs():
    payload = adapter.build_nightly_scheduler_advice(
        "deterministic",
        adapter.load_config(),
        changed_files=["scripts/nightly_adaptive_scheduler.py"],
        codegraph_freshness="fresh",
        code_intelligence_refs={
            "context_ref": "tmp/validation/code-intelligence/1/codegraph-context.md",
            "affected_tests_ref": "tmp/validation/code-intelligence/1/affected-tests.json",
            "affected_tests_count": 1,
            "understand_anything_summary_ref": "tmp/validation/code-intelligence/1/ua-validation-summary.md",
            "understand_anything_status": "available",
        },
    )

    refs = payload["code_intelligence_refs"]
    assert refs["context_ref"].endswith("codegraph-context.md")
    assert refs["affected_tests_ref"].endswith("affected-tests.json")
    assert refs["understand_anything_summary_ref"].endswith("ua-validation-summary.md")
    assert payload["test_plan_advice_gate"]["workflow_gate"] in {"ready", "blocked"}
    assert payload["llm_invocation_evidence"]["input_policy"] == "changed_files_recent_failures_codegraph_ua_refs_catalog_only"


def test_nightly_scheduler_advice_rejects_live_trading_or_business_state_plans():
    payload = adapter.build_nightly_scheduler_advice(
        "deterministic",
        adapter.load_config(),
        recent_failure_plan_keys=["miniqmt_sim_trading_hours_l5"],
        codegraph_freshness="fresh",
        resource_budget_seconds=9000,
    )

    item = payload["queue"][0]
    assert item["allowed"] is False
    assert "writes_business_state" in item["deferred_reason"]
    assert "requires_confirmation" in item["deferred_reason"]
    assert payload["deterministic_gate"]["workflow_gate"] == "blocked"


def test_nightly_scheduler_advice_rejects_shell_command_fields():
    with pytest.raises(adapter.ProviderAdapterError):
        adapter.validate_nightly_scheduler_advice(
            {
                "schema_version": adapter.NIGHTLY_SCHEDULER_ADVICE_SCHEMA_VERSION,
                "queue": [{"plan_key": "l0", "command": "python -m nox -s l0"}],
                "deterministic_gate": {
                    "shell_commands_allowed": False,
                    "production_actions_allowed": False,
                },
            }
        )


def test_nightly_scheduler_advice_normalizes_deepseek_queue_shape(monkeypatch):
    def fake_raw_chat(*args, **kwargs):
        return (
            {
                "id": "deepseek-queue-shape",
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "summary": "run validation plans",
                                    "queue": [
                                        {"plan_key": "validation_catalog_integrity", "reason": "workflow changed"},
                                        {"planKey": "validation_center_backend"},
                                    ],
                                    "confidence": "0.82",
                                }
                            )
                        }
                    }
                ],
                "usage": {"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16},
            },
            "deepseek-v4-pro",
            "env:DEEPSEEK_API_KEY",
            "https://api.deepseek.com/v1/chat/completions",
        )

    monkeypatch.setattr(adapter, "_invoke_provider_raw_chat", fake_raw_chat)

    payload = adapter.build_nightly_scheduler_advice(
        "deepseek_api",
        adapter.load_config(),
        changed_files=["scripts/llm_provider_adapter.py"],
        recent_failure_modules=["validation.runner"],
        codegraph_freshness="fresh",
        resource_budget_seconds=900,
        invoke_llm=True,
    )

    assert payload["effective_provider"] == "deepseek_api"
    assert payload["llm_gate"] == "ready"
    assert payload["llm_invocation_evidence"]["invoked"] is True
    assert payload["llm_advice"]["suggested_plan_keys"] == [
        "validation_catalog_integrity",
        "validation_center_backend",
    ]
    assert payload["advice_consumption"]["advice_consumed"] is True


def test_nightly_scheduler_advice_normalization_rejects_shell_commands(monkeypatch):
    def fake_raw_chat(*args, **kwargs):
        return (
            {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "summary": "unsafe",
                                    "queue": [{"plan_key": "l0", "command": "python -m nox -s l0"}],
                                }
                            )
                        }
                    }
                ]
            },
            "deepseek-v4-pro",
            "env:DEEPSEEK_API_KEY",
            "https://api.deepseek.com/v1/chat/completions",
        )

    monkeypatch.setattr(adapter, "_invoke_provider_raw_chat", fake_raw_chat)

    payload = adapter.build_nightly_scheduler_advice(
        "deepseek_api",
        adapter.load_config(),
        changed_files=["scripts/llm_provider_adapter.py"],
        codegraph_freshness="fresh",
        invoke_llm=True,
    )

    assert payload["llm_gate"] == "degraded"
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert "provider advice must not contain shell command fields" in payload["llm_invocation_evidence"]["error"]
    assert payload["advised_plan_keys"] == []


def test_nightly_scheduler_advice_cli_uses_compact_success_output(capsys, tmp_path):
    output = tmp_path / "nightly-scheduler-advice.json"

    exit_code = adapter.main(
        [
            "--json",
            "nightly-scheduler-advice",
            "--provider",
            "github_models",
            "--changed-file",
            "scripts/llm_provider_adapter.py",
            "--recent-failure-module",
            "validation.runner",
            "--codegraph-freshness",
            "fresh",
            "--resource-budget-seconds",
            "900",
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"check": "nightly-scheduler-advice"' in captured.out
    assert '"queue_count":' in captured.out
    assert '"llm_invoked": false' in captured.out
    assert output.exists()


def test_nightly_scheduler_public_artifact_keeps_safe_fallback_reason(monkeypatch, tmp_path):
    def fake_invoke(*args, **kwargs):
        raise adapter.ProviderAdapterError("github_models inference request failed status=429 token=ghp_secret")

    monkeypatch.setattr(adapter, "invoke_provider_json", fake_invoke)
    output = tmp_path / "nightly-scheduler-advice.json"

    exit_code = adapter.main(
        [
            "--json",
            "nightly-scheduler-advice",
            "--provider",
            "github_models",
            "--changed-file",
            "scripts/llm_provider_adapter.py",
            "--recent-failure-module",
            "validation.runner",
            "--codegraph-freshness",
            "fresh",
            "--invoke-llm",
            "--output",
            str(output),
        ]
    )
    artifact = json.loads(output.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert artifact["public_artifact"] is True
    assert artifact["schema_version"] == adapter.NIGHTLY_SCHEDULER_ADVICE_SCHEMA_VERSION
    assert artifact["llm_invocation_evidence"]["invoked"] is False
    assert artifact["llm_invocation_evidence"]["reason"] == "nightly_scheduler_advice_live_provider_failed_fallback"
    assert "status=429" in artifact["llm_invocation_evidence"]["error"]
    assert "ghp_secret" not in json.dumps(artifact)
    assert artifact["advised_plan_keys"] == []
    assert artifact["executed_plan_keys"]


def test_prompt_evaluation_has_20_fixtures_and_compact_metrics():
    payload = adapter.build_prompt_evaluation("deterministic", adapter.load_config())

    assert payload["schema_version"] == adapter.PROMPT_EVALUATION_SCHEMA_VERSION
    assert payload["metrics"]["case_count"] >= 20
    assert payload["metrics"]["actionability_precision"] == 1.0
    assert payload["metrics"]["dedupe_hit_rate"] == 1.0
    assert payload["metrics"]["plan_recommendation_accuracy"] == 1.0
    assert payload["metrics"]["issue_body_completeness"] == 1.0
    assert payload["metrics"]["average_prompt_tokens"] > 0
    assert payload["metrics"]["average_completion_tokens"] is None
    assert payload["policy_gate"]["workflow_gate"] == "passed"
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert "triage_failure" in payload["prompt_pack_versions"]


def _write_false_positive_prompt_eval_cases(tmp_path):
    payload = json.loads(adapter.DEFAULT_EVALUATION_CASES.read_text(encoding="utf-8-sig"))
    case = payload["cases"][0]
    case["failure_event"]["error_signature"] = "AssertionError: synthetic false positive fixture"
    case["expected"].update(
        {
            "actionable": False,
            "expected_action": "skip",
            "expected_plan_keys": [],
            "issue_body_required_sections": [],
            "false_positive": True,
        }
    )
    cases_path = tmp_path / "false-positive-fixtures.json"
    cases_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return cases_path


def test_prompt_evaluation_blocks_validation_llm_changes_when_false_positive_rate_is_high(tmp_path):
    payload = adapter.build_prompt_evaluation(
        "deterministic",
        adapter.load_config(),
        cases_path=_write_false_positive_prompt_eval_cases(tmp_path),
        changed_files=["prompt_packs/validation_llm/triage_failure.prompt.yml"],
        false_positive_threshold=0.0,
    )

    assert payload["policy_gate"]["workflow_gate"] == "blocked"
    assert payload["policy_gate"]["auto_file_enabled"] is False
    assert payload["policy_gate"]["blocking"] == ["false_positive_auto_file_rate"]


def test_prompt_evaluation_warns_but_does_not_block_unrelated_business_changes(tmp_path):
    payload = adapter.build_prompt_evaluation(
        "deterministic",
        adapter.load_config(),
        cases_path=_write_false_positive_prompt_eval_cases(tmp_path),
        changed_files=["backend/services/example.py"],
        false_positive_threshold=0.0,
    )

    assert payload["policy_gate"]["workflow_gate"] == "warning"
    assert payload["policy_gate"]["blocking_relevant_change"] is False


def test_prompt_evaluation_cli_uses_compact_success_output(capsys, tmp_path):
    output = tmp_path / "prompt-evaluation.json"

    exit_code = adapter.main(
        [
            "--json",
            "prompt-evaluation",
            "--provider",
            "github_models",
            "--changed-file",
            "prompt_packs/validation_llm/triage_failure.prompt.yml",
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"check": "prompt-evaluation"' in captured.out
    assert '"case_count": 20' in captured.out
    assert '"llm_invoked": false' in captured.out
    assert output.exists()


def test_guarded_rollout_config_defaults_preserve_deterministic_auto_file():
    config = adapter.load_config()
    adapter.validate_config(config)

    rollout = config["guarded_rollout"]
    assert rollout["default_mode"] == "warning_only"
    assert rollout["deterministic_auto_file_preserved"] is True
    assert "validation.runner" in rollout["module_allowlist"]


def test_guarded_rollout_gate_requires_opt_in_and_allowlisted_module():
    sections = list(adapter.EVALUATION_ISSUE_BODY_SECTIONS)

    warning = adapter.build_guarded_rollout_gate(
        "deterministic",
        adapter.load_config(),
        module="validation.runner",
        issue_sections=sections,
        deterministic_issue_allowed=True,
    )
    assert warning["workflow_gate"] == "warning"
    assert warning["auto_file_allowed"] is False
    assert "llm_auto_file_not_in_opt_in_mode" in warning["rejection_reasons"]

    opted_in = adapter.build_guarded_rollout_gate(
        "deterministic",
        adapter.load_config(),
        mode="opt_in_auto_file",
        opt_in=True,
        module="validation.runner",
        issue_sections=sections,
        deterministic_issue_allowed=True,
    )
    assert opted_in["workflow_gate"] == "ready"
    assert opted_in["auto_file_allowed"] is True
    assert opted_in["llm_invocation_evidence"]["invoked"] is False


def test_guarded_rollout_gate_kill_switch_and_allowlist_are_safe():
    sections = list(adapter.EVALUATION_ISSUE_BODY_SECTIONS)

    off = adapter.build_guarded_rollout_gate(
        "deterministic",
        adapter.load_config(),
        mode="off",
        opt_in=True,
        module="validation.runner",
        issue_sections=sections,
    )
    assert off["workflow_gate"] == "off"
    assert off["auto_file_allowed"] is False
    assert off["llm_enhancement_allowed"] is False
    assert off["deterministic_issue_creation_unaffected"] is True
    assert off["fallback"] == "deterministic_issue_workflow"

    not_allowlisted = adapter.build_guarded_rollout_gate(
        "deterministic",
        adapter.load_config(),
        mode="opt_in_auto_file",
        opt_in=True,
        module="unknown_business_module",
        issue_sections=sections,
    )
    assert not_allowlisted["auto_file_allowed"] is False
    assert "module_not_allowlisted" in not_allowlisted["rejection_reasons"]


def test_guarded_rollout_gate_false_positive_threshold_keeps_deterministic_fallback():
    sections = list(adapter.EVALUATION_ISSUE_BODY_SECTIONS)

    gate = adapter.build_guarded_rollout_gate(
        "deterministic",
        adapter.load_config(),
        mode="opt_in_auto_file",
        opt_in=True,
        module="validation.runner",
        issue_sections=sections,
        false_positive_rate=0.2,
        false_positive_threshold=0.1,
    )

    assert gate["workflow_gate"] == "warning"
    assert gate["auto_file_allowed"] is False
    assert gate["llm_enhancement_allowed"] is False
    assert gate["deterministic_issue_creation_unaffected"] is True
    assert gate["fallback"] == "deterministic_issue_workflow"
    assert "false_positive_threshold_exceeded" in gate["rejection_reasons"]


def test_guarded_rollout_gate_cli_uses_compact_success_output(capsys, tmp_path):
    output = tmp_path / "guarded-rollout.json"

    exit_code = adapter.main(
        [
            "--json",
            "guarded-rollout-gate",
            "--provider",
            "github_models",
            "--mode",
            "opt_in_auto_file",
            "--opt-in",
            "--module",
            "validation.runner",
            "--issue-section",
            ",".join(adapter.EVALUATION_ISSUE_BODY_SECTIONS),
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"check": "guarded-rollout-gate"' in captured.out
    assert '"auto_file_allowed": true' in captured.out
    assert '"llm_enhancement_allowed": true' in captured.out
    assert '"llm_invoked": false' in captured.out
    assert output.exists()
