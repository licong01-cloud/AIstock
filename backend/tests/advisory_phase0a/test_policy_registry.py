from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.services.advisory_phase0a.policy import (
    POLICY_REGISTRY_ROOT,
    REASON_POLICY_REGISTRY_EFFECTIVE_RANGE_INVALID,
    REASON_POLICY_REGISTRY_HASH_MISMATCH,
    REASON_POLICY_REGISTRY_NOT_FROZEN,
    REASON_POLICY_REGISTRY_PROHIBITED_FIELD,
    PolicyRegistryValidationError,
    canonical_json_sha256,
    load_frozen_policy_registry,
    validate_frozen_policy_registry_payload,
)


POLICY_REGISTRY_ID = "advisory_phase0a"
POLICY_VERSION = "v1"
POLICY_PATH = POLICY_REGISTRY_ROOT / POLICY_REGISTRY_ID / f"{POLICY_VERSION}.json"


def _payload() -> dict[str, object]:
    return json.loads(POLICY_PATH.read_text(encoding="utf-8"))


def _rehash(payload: dict[str, object]) -> None:
    payload["registry_content_hash"] = canonical_json_sha256(
        {key: value for key, value in payload.items() if key != "registry_content_hash"}
    )


def test_repo_tracked_policy_registry_is_frozen_and_loadable() -> None:
    policy = load_frozen_policy_registry(
        policy_registry_id=POLICY_REGISTRY_ID,
        policy_version=POLICY_VERSION,
    )

    assert policy.is_frozen is True
    assert policy.registry_content_hash == "68538d81784294f9b6a6d09c46df274438fb1f34ce0ff5d6da68cb3dbdf86d64"
    assert policy.minimum_trading_day_gap == 20
    assert policy.calendar_hash is None
    assert policy.embargo_policy["calendar_snapshot_required"] is True


def test_policy_registry_content_hash_tampering_fails_closed() -> None:
    payload = _payload()
    payload["cost_policy"]["reference_notional"] = 200000.0  # type: ignore[index]

    with pytest.raises(PolicyRegistryValidationError, match=REASON_POLICY_REGISTRY_HASH_MISMATCH):
        validate_frozen_policy_registry_payload(payload)


def test_policy_registry_missing_required_field_fails_closed() -> None:
    payload = _payload()
    payload.pop("effective_from_trade_date")

    with pytest.raises(PolicyRegistryValidationError, match=REASON_POLICY_REGISTRY_NOT_FROZEN):
        validate_frozen_policy_registry_payload(payload)


def test_policy_registry_empty_identity_fails_closed_even_with_recomputed_hash() -> None:
    payload = _payload()
    payload["policy_registry_id"] = ""
    _rehash(payload)

    with pytest.raises(PolicyRegistryValidationError, match=REASON_POLICY_REGISTRY_NOT_FROZEN):
        validate_frozen_policy_registry_payload(payload)


def test_policy_registry_effective_range_must_cover_registry_range() -> None:
    payload = _payload()
    payload["effective_from_trade_date"] = "2026-07-10"
    _rehash(payload)

    with pytest.raises(PolicyRegistryValidationError, match=REASON_POLICY_REGISTRY_EFFECTIVE_RANGE_INVALID):
        validate_frozen_policy_registry_payload(payload)


def test_policy_registry_rejects_approval_and_role_fields() -> None:
    payload = _payload()
    payload["approved_by"] = "operator"

    with pytest.raises(PolicyRegistryValidationError, match=REASON_POLICY_REGISTRY_PROHIBITED_FIELD):
        validate_frozen_policy_registry_payload(payload)


def test_policy_registry_root_does_not_accept_scratch_files(tmp_path: Path) -> None:
    with pytest.raises(PolicyRegistryValidationError, match=REASON_POLICY_REGISTRY_NOT_FROZEN):
        load_frozen_policy_registry(
            policy_registry_id=POLICY_REGISTRY_ID,
            policy_version=POLICY_VERSION,
            registry_root=tmp_path,
        )
