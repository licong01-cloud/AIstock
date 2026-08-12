from __future__ import annotations

from datetime import date

import pytest

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SNAPSHOT_PREFIX,
    CANONICAL_PIT_UNIVERSE_KEY,
    CanonicalPitContractError,
    PitAuthorityStatus,
    PitConsumerBinding,
    canonical_rule_parameters_digest,
    require_canonical_consumer_binding,
)


def _binding(**overrides):
    values = {
        "authority_id": CANONICAL_PIT_AUTHORITY_ID,
        "authority_status": PitAuthorityStatus.ACTIVE_CANONICAL,
        "universe_key": CANONICAL_PIT_UNIVERSE_KEY,
        "rule_version": CANONICAL_PIT_RULE_VERSION,
        "rule_parameters_digest": canonical_rule_parameters_digest(),
    }
    values.update(overrides)
    return PitConsumerBinding(**values)


def test_all_normal_consumers_share_one_active_authority() -> None:
    for consumer in ("qe", "training", "selection", "paper_v2", "simulation"):
        assert require_canonical_consumer_binding(_binding(), consumer=consumer).universe_key == CANONICAL_PIT_UNIVERSE_KEY


def test_dataset_consumers_require_frozen_snapshot_identity() -> None:
    binding = _binding(
        universe_key=f"{CANONICAL_PIT_SNAPSHOT_PREFIX}release_20260731",
        snapshot_digest="a" * 64,
        cutoff=date(2026, 7, 31),
        release_id="release-20260731",
    )
    assert require_canonical_consumer_binding(binding, consumer="qe", immutable_snapshot_required=True) == binding
    with pytest.raises(CanonicalPitContractError, match="immutable canonical PIT snapshot"):
        require_canonical_consumer_binding(_binding(), consumer="qe", immutable_snapshot_required=True)


def test_legacy_universe_is_only_valid_for_explicit_reproduction() -> None:
    archived = _binding(
        authority_status=PitAuthorityStatus.ARCHIVED_NONCANONICAL,
        universe_key="shsz_st_pit_active_v1",
        rule_version="st_pub_next_trade_restore_active_l_v1",
        rule_parameters_digest="b" * 64,
        snapshot_digest="c" * 64,
        cutoff=date(2026, 6, 30),
        release_id="legacy-release",
        reproduction_mode=True,
    )
    assert require_canonical_consumer_binding(archived, consumer="reproduction") == archived
    with pytest.raises(CanonicalPitContractError, match="cannot drive training or trading"):
        require_canonical_consumer_binding(archived, consumer="selection")
    with pytest.raises(CanonicalPitContractError, match="not an allowlisted archived universe"):
        require_canonical_consumer_binding(
            _binding(
                authority_status=PitAuthorityStatus.ARCHIVED_NONCANONICAL,
                universe_key="unregistered_legacy",
                snapshot_digest="c" * 64,
                cutoff=date(2026, 6, 30),
                release_id="legacy-release",
                reproduction_mode=True,
            ),
            consumer="reproduction",
        )


def test_silent_rule_or_parameter_drift_is_rejected() -> None:
    with pytest.raises(CanonicalPitContractError, match="non-canonical PIT binding"):
        require_canonical_consumer_binding(_binding(rule_parameters_digest="d" * 64), consumer="training")
