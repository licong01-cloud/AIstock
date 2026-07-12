from __future__ import annotations

import ast
from pathlib import Path

import pytest
from fastapi import HTTPException

from backend.config_manager_compat import ConfigManager
from backend.execution_algos.adaptive_is.reasons import QuoteContractError, QuoteContractReasonCode
from backend.miniqmt_quote_contract_config import (
    QUOTE_INGRESS_ENV_DEFAULTS,
    QUOTE_INGRESS_ENV_METADATA,
    QuoteContractPolicy,
    QuoteIngressRuntimeConfig,
)
from backend.routers import config_env


def _explicit_quote_policy() -> dict[str, object]:
    return {
        "quote_contract": {
            "schema_version": "miniqmt_quote_contract_policy_v2",
            "control_revision": "B0_QUOTE_V2",
            "required_capabilities": [
                "CALENDAR",
                "DEPTH_UNIT_SHARES",
                "EXCHANGE_TIMESTAMP",
                "FIVE_LEVEL_DEPTH",
                "RAW_PRICE_BASIS",
                "TRADABILITY",
            ],
            "max_receive_age_ms": 1500,
            "max_source_lag_ms": 1500,
            "max_exchange_age_ms": 2000,
            "max_negative_skew_ms": 100,
            "max_clock_age_divergence_ms": 25,
            "max_dependency_group_skew_ms": 250,
            "auction_mode": "OBSERVE_ONLY",
        }
    }


def test_clock_age_divergence_is_required_and_hashed_without_default() -> None:
    policy = QuoteContractPolicy.from_execution_policy(_explicit_quote_policy())
    assert policy.canonical_payload()["max_clock_age_divergence_ms"] == 25
    changed = _explicit_quote_policy()
    changed["quote_contract"]["max_clock_age_divergence_ms"] = 26  # type: ignore[index]
    assert QuoteContractPolicy.from_execution_policy(changed).policy_sha256 != policy.policy_sha256
    missing = _explicit_quote_policy()
    missing["quote_contract"].pop("max_clock_age_divergence_ms")  # type: ignore[index]
    with pytest.raises(QuoteContractError) as exc_info:
        QuoteContractPolicy.from_execution_policy(missing)
    assert exc_info.value.reason_code == QuoteContractReasonCode.POLICY_SCHEMA_INVALID


def test_config_manager_registers_and_roundtrips_every_quote_ingress_key_without_dropping_unknown_config(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text('TCA_EXISTING_CONFIG="must-survive"\nOTHER_SUBSYSTEM_KEY="also-survives"\n', encoding="utf-8")
    manager = ConfigManager(env_file)

    assert set(QUOTE_INGRESS_ENV_METADATA).issubset(manager.default_config)
    assert manager.write_env({"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"})
    reread = manager.read_env()

    assert reread["TCA_EXISTING_CONFIG"] == "must-survive"
    assert reread["OTHER_SUBSYSTEM_KEY"] == "also-survives"
    for key, expected in QUOTE_INGRESS_ENV_DEFAULTS.items():
        assert reread[key] == expected


def test_config_manager_exposes_quote_metadata_and_validates_new_file_defaults(tmp_path) -> None:
    manager = ConfigManager(tmp_path / ".env")
    defaults = manager.read_env()
    info = manager.get_config_info()
    defaults["DEEPSEEK_API_KEY"] = "x" * 20

    assert defaults["MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED"] == "false"
    assert info["MINIQMT_QUOTE_INGRESS_OWNER_MODE"]["options"] == ["simulation_scheduler"]
    assert manager.validate_config(defaults) == (True, "配置验证通过")
    assert manager.validate_config({"DEEPSEEK_API_KEY": "too-short"}) == (False, "DeepSeek API Key格式不正确（长度太短）")


def test_config_manager_reads_dependency_free_env_metadata_not_execution_algorithm_package() -> None:
    tree = ast.parse(Path("backend/config_manager_compat.py").read_text(encoding="utf-8"))
    imported_modules = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module]

    assert "backend.miniqmt_quote_contract_env" in imported_modules
    assert not any(module.startswith("backend.execution_algos") for module in imported_modules)


def test_config_manager_env_read_failure_is_loud_not_defaulted(tmp_path) -> None:
    manager = ConfigManager(tmp_path)

    with pytest.raises(RuntimeError, match="CONFIG_ENV_READ_FAILED"):
        manager.read_env()


def test_config_save_reports_reload_failure_instead_of_false_success(monkeypatch: pytest.MonkeyPatch) -> None:
    class _ReloadFailureConfigManager:
        def validate_config(self, config: dict[str, str]) -> tuple[bool, str]:
            return True, "配置验证通过"

        def write_env(self, config: dict[str, str]) -> bool:
            return True

        def reload_config(self) -> None:
            raise RuntimeError("CONFIG_ENV_RELOAD_FAILED: unit-test")

    monkeypatch.setattr(config_env, "config_manager", _ReloadFailureConfigManager())

    with pytest.raises(HTTPException) as exc_info:
        config_env.save_env_config({"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": False})
    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "CONFIG_ENV_RELOAD_FAILED: unit-test"


def test_config_save_reports_write_failure_instead_of_false_success(monkeypatch: pytest.MonkeyPatch) -> None:
    class _WriteFailureConfigManager:
        def validate_config(self, config: dict[str, str]) -> tuple[bool, str]:
            return True, "配置验证通过"

        def write_env(self, config: dict[str, str]) -> bool:
            raise RuntimeError("CONFIG_ENV_WRITE_FAILED: unit-test")

        def reload_config(self) -> None:
            raise AssertionError("reload must not run after write failure")

    monkeypatch.setattr(config_env, "config_manager", _WriteFailureConfigManager())

    with pytest.raises(HTTPException) as exc_info:
        config_env.save_env_config({"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": False})
    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "CONFIG_ENV_WRITE_FAILED: unit-test"


def test_config_router_returns_metadata_and_normalizes_nested_config_values(monkeypatch: pytest.MonkeyPatch) -> None:
    class _CaptureConfigManager:
        written: dict[str, str] | None = None

        def get_config_info(self) -> dict[str, object]:
            return {"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": {"value": "false"}}

        def validate_config(self, config: dict[str, str]) -> tuple[bool, str]:
            return True, "配置验证通过"

        def write_env(self, config: dict[str, str]) -> bool:
            self.written = config
            return True

        def reload_config(self) -> None:
            return None

    manager = _CaptureConfigManager()
    monkeypatch.setattr(config_env, "config_manager", manager)

    assert config_env.get_env_config() == {"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": {"value": "false"}}
    assert config_env.save_env_config({"config": {"enabled": True, "optional": None, "count": 3}}) == {
        "ok": True,
        "message": "配置验证通过",
    }
    assert manager.written == {"enabled": "true", "optional": "", "count": "3"}


@pytest.mark.parametrize(
    "payload, validation_result, write_result, status_code",
    [
        (None, (True, "配置验证通过"), True, 400),
        ({"config": "not-an-object"}, (True, "配置验证通过"), True, 400),
        ({"value": 1}, (False, "invalid policy"), True, 400),
        ({"value": 1}, (True, "配置验证通过"), False, 500),
    ],
)
def test_config_router_rejects_invalid_or_unsaved_payloads(
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
    validation_result: tuple[bool, str],
    write_result: bool,
    status_code: int,
) -> None:
    class _OutcomeConfigManager:
        def validate_config(self, config: dict[str, str]) -> tuple[bool, str]:
            return validation_result

        def write_env(self, config: dict[str, str]) -> bool:
            return write_result

        def reload_config(self) -> None:
            return None

    monkeypatch.setattr(config_env, "config_manager", _OutcomeConfigManager())

    with pytest.raises(HTTPException) as exc_info:
        config_env.save_env_config(payload)  # type: ignore[arg-type]
    assert exc_info.value.status_code == status_code


def test_process_config_uses_documented_defaults_but_rejects_invalid_lifecycle_values() -> None:
    config = QuoteIngressRuntimeConfig.from_mapping({})
    assert config.enabled is False
    assert config.owner_mode == "simulation_scheduler"
    assert config.max_symbols == 128
    assert config.restart_max_attempts == 3

    with pytest.raises(QuoteContractError) as exc_info:
        QuoteIngressRuntimeConfig.from_mapping({"MINIQMT_QUOTE_INGRESS_RESTART_MAX_BACKOFF_MS": "1"})
    assert exc_info.value.reason_code == QuoteContractReasonCode.POLICY_SCHEMA_INVALID


def test_process_config_constructor_cannot_bypass_registered_schema() -> None:
    with pytest.raises(QuoteContractError) as exc_info:
        QuoteIngressRuntimeConfig(
            enabled=True,
            owner_mode="manual_approval",
            max_symbols=0,
            drain_budget=128,
            heartbeat_timeout_ms=10_000,
            restart_backoff_ms=1_000,
            restart_max_backoff_ms=30_000,
            loud_interval_seconds=30,
            evidence_outbox_max_events=4_096,
            evidence_flush_batch_size=128,
        )
    assert exc_info.value.reason_code == QuoteContractReasonCode.POLICY_SCHEMA_INVALID


def test_process_config_rejects_unknown_quote_namespace_key_but_ignores_unrelated_environment() -> None:
    config = QuoteIngressRuntimeConfig.from_mapping({"PATH": "unrelated"})
    assert config.enabled is False
    with pytest.raises(QuoteContractError) as exc_info:
        QuoteIngressRuntimeConfig.from_mapping({"MINIQMT_QUOTE_INGRESS_MAX_SYMOBLS": "128"})
    assert exc_info.value.reason_code == QuoteContractReasonCode.POLICY_SCHEMA_INVALID


@pytest.mark.parametrize(
    "override",
    [
        {"MINIQMT_QUOTE_INGRESS_OWNER_MODE": "manual_approval"},
        {"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "yes"},
        {"MINIQMT_QUOTE_INGRESS_MAX_SYMBOLS": "0"},
        {"MINIQMT_QUOTE_EVIDENCE_FLUSH_BATCH_SIZE": "4097"},
    ],
)
def test_process_config_rejects_unregistered_or_unsafe_lifecycle_semantics(override: dict[str, str]) -> None:
    with pytest.raises(QuoteContractError) as exc_info:
        QuoteIngressRuntimeConfig.from_mapping(override)
    assert exc_info.value.reason_code == QuoteContractReasonCode.POLICY_SCHEMA_INVALID


def test_immutable_quote_policy_requires_all_thresholds_and_has_canonical_hash() -> None:
    first = QuoteContractPolicy.from_execution_policy(_explicit_quote_policy())
    reordered = _explicit_quote_policy()
    reordered["quote_contract"]["required_capabilities"] = list(reversed(reordered["quote_contract"]["required_capabilities"]))  # type: ignore[index]
    second = QuoteContractPolicy.from_execution_policy(reordered)

    assert first.policy_sha256 == second.policy_sha256
    first.assert_policy_sha256(first.policy_sha256)

    incomplete = _explicit_quote_policy()
    del incomplete["quote_contract"]["max_exchange_age_ms"]  # type: ignore[index]
    with pytest.raises(QuoteContractError) as exc_info:
        QuoteContractPolicy.from_execution_policy(incomplete)
    assert exc_info.value.reason_code == QuoteContractReasonCode.POLICY_SCHEMA_INVALID


@pytest.mark.parametrize(
    "override",
    [
        {"schema_version": "bad-schema"},
        {"control_revision": "LEGACY_B0"},
        {"required_capabilities": frozenset()},
        {"max_receive_age_ms": -1},
        {"auction_mode": "ACTION"},
    ],
)
def test_immutable_quote_policy_constructor_cannot_bypass_schema(override: dict[str, object]) -> None:
    valid = QuoteContractPolicy.from_execution_policy(_explicit_quote_policy())
    fields: dict[str, object] = {
        "schema_version": valid.schema_version,
        "control_revision": valid.control_revision,
        "required_capabilities": valid.required_capabilities,
        "max_receive_age_ms": valid.max_receive_age_ms,
        "max_source_lag_ms": valid.max_source_lag_ms,
        "max_exchange_age_ms": valid.max_exchange_age_ms,
        "max_negative_skew_ms": valid.max_negative_skew_ms,
        "max_clock_age_divergence_ms": valid.max_clock_age_divergence_ms,
        "max_dependency_group_skew_ms": valid.max_dependency_group_skew_ms,
        "auction_mode": valid.auction_mode,
    }
    fields.update(override)
    with pytest.raises(QuoteContractError) as exc_info:
        QuoteContractPolicy(**fields)  # type: ignore[arg-type]
    assert exc_info.value.reason_code == QuoteContractReasonCode.POLICY_SCHEMA_INVALID


def test_config_manager_rejects_invalid_quote_process_values_before_save(tmp_path) -> None:
    manager = ConfigManager(tmp_path / ".env")
    config = manager.read_env()
    config["DEEPSEEK_API_KEY"] = "x" * 20
    config["MINIQMT_QUOTE_INGRESS_OWNER_MODE"] = "manual_approval"
    config["MINIQMT_QUOTE_INGRESS_MAX_SYMBOLS"] = "0"

    ok, message = manager.validate_config(config)

    assert ok is False
    assert message.startswith("ADAPTIVE_IS_QUOTE_POLICY_SCHEMA_INVALID:")


def test_config_manager_rejects_new_unknown_keys_but_preserves_existing_unknown_keys(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text('EXISTING_EXTERNAL_KEY="keep"\n', encoding="utf-8")
    manager = ConfigManager(env_file)
    config = manager.get_config_info()
    payload = {key: str(item["value"]) for key, item in config.items()}
    payload["NEW_UNREGISTERED_KEY"] = "must-reject"

    ok, message = manager.validate_config(payload)

    assert ok is False
    assert message.startswith("CONFIG_ENV_UNKNOWN_KEY:")
    assert manager.write_env({"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"})
    assert manager.read_env()["EXISTING_EXTERNAL_KEY"] == "keep"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda policy: policy.update({"quote_contract": []}),
        lambda policy: policy["quote_contract"].update({"unregistered": True}),
        lambda policy: policy["quote_contract"].update({"required_capabilities": "FIVE_LEVEL_DEPTH"}),
        lambda policy: policy["quote_contract"].update(
            {"required_capabilities": [*policy["quote_contract"]["required_capabilities"], "CALENDAR"]}
        ),
        lambda policy: policy["quote_contract"].update({"schema_version": "v1"}),
        lambda policy: policy["quote_contract"].update({"control_revision": "LEGACY_B0"}),
        lambda policy: policy["quote_contract"].update({"auction_mode": "ACTION"}),
        lambda policy: policy["quote_contract"].update({"max_negative_skew_ms": -1}),
        lambda policy: policy["quote_contract"].pop("max_clock_age_divergence_ms"),
    ],
)
def test_immutable_quote_policy_rejects_unknown_or_implicit_semantics(mutate) -> None:
    policy = _explicit_quote_policy()
    mutate(policy)
    with pytest.raises(QuoteContractError) as exc_info:
        QuoteContractPolicy.from_execution_policy(policy)
    assert exc_info.value.reason_code == QuoteContractReasonCode.POLICY_SCHEMA_INVALID


def test_policy_hash_mismatch_is_loud() -> None:
    policy = QuoteContractPolicy.from_execution_policy(_explicit_quote_policy())
    with pytest.raises(QuoteContractError) as exc_info:
        policy.assert_policy_sha256("not-the-canonical-hash")
    assert exc_info.value.reason_code == QuoteContractReasonCode.POLICY_SCHEMA_INVALID
