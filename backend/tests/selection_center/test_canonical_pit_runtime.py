from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_UNIVERSE_KEY,
    LEGACY_PIT_RULE_VERSION,
    LEGACY_PIT_UNIVERSE_KEY,
    PitAuthorityStatus,
    PitConsumerBinding,
    canonical_rule_parameters_digest,
    legacy_rule_parameters_digest,
)
from backend.services.selection_center.canonical_pit_runtime import (
    CANONICAL_PIT_POINTER_PROFILE_SCHEMA,
    CANONICAL_PIT_RUNTIME_LEASE_KEY,
    CANONICAL_PIT_RUNTIME_PROFILE_KEY,
    CanonicalPitGenerationDriftError,
    CanonicalPitRuntimeError,
    SelectionPitRuntimeLease,
    freeze_canonical_pit_runtime_binding,
    inherit_canonical_pit_runtime_binding,
    migrate_runtime_config_to_canonical_pointer,
    require_canonical_pit_generation_current,
    validate_canonical_pit_runtime_profile,
)
from backend.services.selection_center.runtime_profile import (
    normalize_selection_runtime_config,
    parse_selection_runtime_profile,
    runtime_profile_config_sha256,
)


SHA_A = "a" * 64
SHA_B = "b" * 64


def _binding(*, generation: int = 0, active: bool = False) -> PitConsumerBinding:
    return PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=(
            PitAuthorityStatus.ACTIVE_CANONICAL if active else PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION
        ),
        universe_key=CANONICAL_PIT_UNIVERSE_KEY if active else LEGACY_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION if active else LEGACY_PIT_RULE_VERSION,
        rule_parameters_digest=(canonical_rule_parameters_digest() if active else legacy_rule_parameters_digest()),
        activation_generation=generation,
        activation_envelope_digest=SHA_B if active else None,
        expected_source_commit="source-commit" if active else None,
        state_source_digest=SHA_A,
        coverage_start=date(2018, 8, 1),
        coverage_end=date(2026, 7, 31),
    )


class _Resolver:
    def __init__(self, binding: PitConsumerBinding) -> None:
        self.binding = binding
        self.calls = 0

    def resolve_live_binding(self) -> PitConsumerBinding:
        self.calls += 1
        return self.binding


def _legacy_config() -> dict:
    return {
        "runtime_profile": {
            "risk_policy": {
                "enabled": True,
                "providers": ["st_pit"],
                "st_universe_key": LEGACY_PIT_UNIVERSE_KEY,
            }
        }
    }


def test_explicit_migration_returns_new_pointer_profile_without_rewriting_legacy() -> None:
    legacy = _legacy_config()
    legacy_hash = runtime_profile_config_sha256(legacy)

    migrated = migrate_runtime_config_to_canonical_pointer(legacy)

    assert legacy["runtime_profile"]["risk_policy"]["st_universe_key"] == LEGACY_PIT_UNIVERSE_KEY
    assert CANONICAL_PIT_RUNTIME_PROFILE_KEY not in legacy
    assert migrated[CANONICAL_PIT_RUNTIME_PROFILE_KEY] == {
        "schema_version": CANONICAL_PIT_POINTER_PROFILE_SCHEMA,
        "authority_id": CANONICAL_PIT_AUTHORITY_ID,
    }
    assert "st_universe_key" not in migrated["runtime_profile"]["risk_policy"]
    assert validate_canonical_pit_runtime_profile(migrated) == CANONICAL_PIT_POINTER_PROFILE_SCHEMA
    assert runtime_profile_config_sha256(migrated) != legacy_hash


def test_pointer_profile_rejects_mixed_legacy_key_and_unknown_fields() -> None:
    mixed = _legacy_config()
    mixed[CANONICAL_PIT_RUNTIME_PROFILE_KEY] = {
        "schema_version": CANONICAL_PIT_POINTER_PROFILE_SCHEMA,
        "authority_id": CANONICAL_PIT_AUTHORITY_ID,
    }
    with pytest.raises(CanonicalPitRuntimeError, match="cannot be mixed"):
        validate_canonical_pit_runtime_profile(mixed)

    unknown = migrate_runtime_config_to_canonical_pointer(_legacy_config())
    unknown[CANONICAL_PIT_RUNTIME_PROFILE_KEY]["latest"] = True
    with pytest.raises(CanonicalPitRuntimeError, match="unknown fields"):
        validate_canonical_pit_runtime_profile(unknown)

    with pytest.raises(CanonicalPitRuntimeError, match="must be an object"):
        validate_canonical_pit_runtime_profile({CANONICAL_PIT_RUNTIME_PROFILE_KEY: None})


def test_selection_admission_freezes_one_pointer_generation_and_downstream_inherits_it() -> None:
    resolver = _Resolver(_binding(generation=1, active=True))
    migrated = migrate_runtime_config_to_canonical_pointer(_legacy_config())

    frozen, lease = freeze_canonical_pit_runtime_binding(
        migrated,
        trade_date=date(2026, 7, 31),
        authority_resolver=resolver,  # type: ignore[arg-type]
    )
    inherited, inherited_lease = inherit_canonical_pit_runtime_binding(
        frozen,
        trade_date=date(2026, 7, 31),
    )

    assert resolver.calls == 1
    assert inherited == frozen
    assert inherited_lease == lease
    assert lease.activation_generation == 1
    assert lease.universe_key == CANONICAL_PIT_UNIVERSE_KEY
    assert frozen[CANONICAL_PIT_RUNTIME_LEASE_KEY] == lease.as_dict()


def test_external_supplied_lease_must_match_the_live_pointer() -> None:
    migrated = migrate_runtime_config_to_canonical_pointer(_legacy_config())
    stale, _lease = freeze_canonical_pit_runtime_binding(
        migrated,
        trade_date=date(2026, 7, 31),
        authority_resolver=_Resolver(_binding(generation=1, active=True)),  # type: ignore[arg-type]
    )

    with pytest.raises(CanonicalPitRuntimeError, match="differs from live pointer"):
        freeze_canonical_pit_runtime_binding(
            stale,
            trade_date=date(2026, 7, 31),
            authority_resolver=_Resolver(_binding(generation=2, active=True)),  # type: ignore[arg-type]
        )


def test_runtime_profile_hash_excludes_lease_but_includes_pointer_migration() -> None:
    migrated = migrate_runtime_config_to_canonical_pointer(_legacy_config())
    frozen, _lease = freeze_canonical_pit_runtime_binding(
        migrated,
        trade_date=date(2026, 7, 31),
        authority_resolver=_Resolver(_binding(generation=1, active=True)),  # type: ignore[arg-type]
    )

    assert runtime_profile_config_sha256(frozen) == runtime_profile_config_sha256(migrated)
    normalized = normalize_selection_runtime_config(frozen)
    assert "st_universe_key" not in normalized["runtime_profile"]["risk_policy"]
    parsed = parse_selection_runtime_profile(normalized)
    assert parsed.risk_policy.canonical_pit_runtime_lease == frozen[CANONICAL_PIT_RUNTIME_LEASE_KEY]
    assert "canonical_pit_runtime_lease" not in parsed.risk_policy.model_dump(mode="json")

    invalid = dict(frozen)
    invalid[CANONICAL_PIT_RUNTIME_LEASE_KEY] = {
        **frozen[CANONICAL_PIT_RUNTIME_LEASE_KEY],
        "activation_generation": "1",
    }
    with pytest.raises(CanonicalPitRuntimeError, match="activation_generation must be an integer"):
        runtime_profile_config_sha256(invalid)


def test_generation_drift_is_typed_and_never_silently_switches() -> None:
    migrated = migrate_runtime_config_to_canonical_pointer(_legacy_config())
    frozen, _lease = freeze_canonical_pit_runtime_binding(
        migrated,
        trade_date=date(2026, 7, 31),
        authority_resolver=_Resolver(_binding(generation=1, active=True)),  # type: ignore[arg-type]
    )
    drifted = _Resolver(_binding(generation=2, active=True))

    with pytest.raises(CanonicalPitGenerationDriftError) as exc_info:
        require_canonical_pit_generation_current(
            frozen,
            authority_resolver=drifted,  # type: ignore[arg-type]
        )

    assert exc_info.value.error_code == "CANONICAL_PIT_GENERATION_DRIFT"
    assert exc_info.value.context["expected_generation"] == 1
    assert exc_info.value.context["actual_generation"] == 2
    assert exc_info.value.context["identity_changed"] is True


@pytest.mark.parametrize(
    "identity_change",
    [
        {"activation_envelope_digest": "c" * 64},
        {"expected_source_commit": "other-source-commit"},
        {"state_source_digest": "c" * 64},
        {"coverage_start": date(2018, 8, 2)},
        {"coverage_end": date(2026, 7, 30)},
    ],
)
def test_generation_check_rejects_same_generation_identity_drift(identity_change: dict) -> None:
    migrated = migrate_runtime_config_to_canonical_pointer(_legacy_config())
    frozen, _lease = freeze_canonical_pit_runtime_binding(
        migrated,
        trade_date=date(2026, 7, 30),
        authority_resolver=_Resolver(_binding(generation=1, active=True)),  # type: ignore[arg-type]
    )
    changed = replace(_binding(generation=1, active=True), **identity_change)

    with pytest.raises(CanonicalPitGenerationDriftError) as exc_info:
        require_canonical_pit_generation_current(
            frozen,
            authority_resolver=_Resolver(changed),  # type: ignore[arg-type]
        )

    assert exc_info.value.context["expected_generation"] == 1
    assert exc_info.value.context["actual_generation"] == 1
    assert exc_info.value.context["identity_changed"] is True


def test_lease_rejects_unknown_fields_and_dates_outside_coverage() -> None:
    raw = SelectionPitRuntimeLease.from_binding(_binding(generation=1, active=True)).as_dict()
    raw["latest"] = True
    with pytest.raises(CanonicalPitRuntimeError, match="unknown fields"):
        SelectionPitRuntimeLease.from_mapping(raw)

    lease = SelectionPitRuntimeLease.from_binding(_binding(generation=1, active=True))
    with pytest.raises(CanonicalPitRuntimeError, match="does not cover"):
        lease.require_trade_date(date(2026, 8, 1))

    missing_source = replace(_binding(generation=1, active=True), state_source_digest=None)
    with pytest.raises(CanonicalPitRuntimeError, match="state_source_digest"):
        SelectionPitRuntimeLease.from_binding(missing_source)


@pytest.mark.parametrize("invalid_generation", [True, 1.0, "1", None])
def test_lease_rejects_coerced_activation_generation(invalid_generation: object) -> None:
    raw = SelectionPitRuntimeLease.from_binding(_binding(generation=1, active=True)).as_dict()
    raw["activation_generation"] = invalid_generation

    with pytest.raises(CanonicalPitRuntimeError, match="activation_generation must be an integer"):
        SelectionPitRuntimeLease.from_mapping(raw)


def test_lease_rejects_non_json_identity_and_coverage_types() -> None:
    raw = SelectionPitRuntimeLease.from_binding(_binding(generation=1, active=True)).as_dict()
    raw["authority_id"] = 1
    with pytest.raises(CanonicalPitRuntimeError, match="authority_id must be a non-empty string"):
        SelectionPitRuntimeLease.from_mapping(raw)

    raw = SelectionPitRuntimeLease.from_binding(_binding(generation=1, active=True)).as_dict()
    raw["coverage_start"] = date(2018, 8, 1)
    with pytest.raises(CanonicalPitRuntimeError, match="coverage must use ISO date strings"):
        SelectionPitRuntimeLease.from_mapping(raw)


def test_inherited_lease_requires_explicit_pointer_profile() -> None:
    raw = SelectionPitRuntimeLease.from_binding(_binding(generation=1, active=True)).as_dict()
    legacy = _legacy_config()
    legacy[CANONICAL_PIT_RUNTIME_LEASE_KEY] = raw

    with pytest.raises(CanonicalPitRuntimeError, match="legacy runtime profile cannot inherit"):
        inherit_canonical_pit_runtime_binding(legacy, trade_date=date(2026, 7, 31))

    with pytest.raises(CanonicalPitRuntimeError, match="requires the explicit pointer profile"):
        require_canonical_pit_generation_current(
            legacy,
            authority_resolver=_Resolver(_binding(generation=1, active=True)),  # type: ignore[arg-type]
        )
