from __future__ import annotations

import json

import pytest

from backend.services.qmt_strategy_ledger.tca_models import canonical_json_sha256
from backend.services.qmt_strategy_ledger.tca_read_service import (
    AccountPseudonymizer,
    TcaActiveReadVersion,
    TcaKeysetCursorCodec,
    TcaReadError,
    TcaReadRuntimeConfig,
)


def _active_version_payload() -> dict[str, str]:
    payload = {
        "calculator_version": "calculator-v1",
        "formula_version": "formula-v1",
        "schema_version": "schema-v1",
        "query_version": "query-v1",
        "benchmark_policy_version": "benchmark-v1",
        "mark_policy_version": "mark-v1",
        "fee_policy_version": "fee-v1",
        "trade_provenance_policy_version": "trade-v1",
    }
    return {**payload, "config_sha256": canonical_json_sha256(payload)}


def test_active_read_version_requires_complete_hashed_tuple() -> None:
    payload = _active_version_payload()
    version = TcaActiveReadVersion.from_mapping(payload)

    assert version.as_mapping()["calculator_version"] == "calculator-v1"
    assert version.config_sha256 == payload["config_sha256"]

    with pytest.raises(TcaReadError) as excinfo:
        TcaActiveReadVersion.from_mapping({**payload, "query_version": ""})

    assert excinfo.value.reason_code == "ADAPTIVE_IS_TCA_ACTIVE_READ_VERSION_INVALID"
    assert excinfo.value.http_status == 503


def test_read_runtime_config_has_no_implicit_version_or_identity_fallback() -> None:
    config = TcaReadRuntimeConfig.from_environ({})

    assert config.eod_observation_enabled is False
    with pytest.raises(TcaReadError, match="implicit TCA reads") as version_error:
        config.require_active_read_version()
    with pytest.raises(TcaReadError, match="HMAC key") as identity_error:
        config.require_pseudonymizer()

    assert version_error.value.reason_code == "ADAPTIVE_IS_TCA_ACTIVE_READ_VERSION_MISSING"
    assert identity_error.value.reason_code == "ADAPTIVE_IS_TCA_EXPORT_IDENTITY_UNAVAILABLE"


def test_read_runtime_config_validates_identity_pair_and_pseudonymizes_stably() -> None:
    payload = _active_version_payload()
    config = TcaReadRuntimeConfig.from_environ(
        {
            "MINIQMT_TCA_ACTIVE_READ_VERSION": json.dumps(payload),
            "AISTOCK_TCA_EXPORT_HMAC_KEY": "test-secret",
            "AISTOCK_TCA_EXPORT_HMAC_KEY_VERSION": "v20260711",
            "MINIQMT_TCA_EOD_OBSERVATION_ENABLED": "false",
        }
    )

    pseudonymizer = config.require_pseudonymizer()
    assert pseudonymizer.pseudonymize("account-a") == pseudonymizer.pseudonymize("account-a")
    assert pseudonymizer.pseudonymize("account-a") != pseudonymizer.pseudonymize("account-b")
    assert "account-a" not in pseudonymizer.pseudonymize("account-a")

    with pytest.raises(TcaReadError) as excinfo:
        TcaReadRuntimeConfig.from_environ({"AISTOCK_TCA_EXPORT_HMAC_KEY": "only-key"})
    assert excinfo.value.reason_code == "ADAPTIVE_IS_TCA_EXPORT_IDENTITY_CONFIG_INVALID"


def test_signed_cursor_is_filter_bound_and_tamper_loud() -> None:
    codec = TcaKeysetCursorCodec(AccountPseudonymizer(key=b"test-secret", key_version="v1"))
    filter_sha = "a" * 64
    cursor = codec.encode(last_key=("2026-07-13", "parent-1", 2), filter_sha256=filter_sha)

    assert codec.decode(cursor=cursor, expected_filter_sha256=filter_sha) == ("2026-07-13", "parent-1", 2)

    with pytest.raises(TcaReadError) as wrong_filter:
        codec.decode(cursor=cursor, expected_filter_sha256="b" * 64)
    with pytest.raises(TcaReadError) as tampered:
        codec.decode(cursor=f"{cursor}x", expected_filter_sha256=filter_sha)

    assert wrong_filter.value.reason_code == "ADAPTIVE_IS_TCA_CURSOR_INVALID"
    assert tampered.value.reason_code == "ADAPTIVE_IS_TCA_CURSOR_INVALID"
