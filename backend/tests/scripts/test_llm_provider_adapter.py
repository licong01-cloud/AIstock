from __future__ import annotations

import pytest

from scripts import llm_provider_adapter as adapter


def test_llm_triage_config_defaults_are_safe():
    config = adapter.load_config()

    adapter.validate_config(config)

    assert config["default_provider"] == "github_models"
    assert config["providers"]["deepseek_api"]["enabled"] is False
    assert config["providers"]["deepseek_api"]["model"] == "deepseek-v4-pro"
    selector = config["providers"]["github_models"]["model_selector"]
    assert selector["required_model_family"] == "deepseek-r1"
    assert selector["preferred_models"] == ["deepseek/deepseek-r1"]
    assert selector["allow_lower_tier_fallback"] is False


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
