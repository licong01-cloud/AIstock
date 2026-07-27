"""BUG-869: snapshot-level day-bound universe policy binding set regressions.

The universe policy hash carried by each observation is the per-decision-day
universe input membership identity, which legitimately differs across trading
days.  The retrospective snapshot stores one deterministic, domain-tagged
composite over the frozen (decision date, universe policy hash) bindings:

1. different days may carry different hashes; the same day must carry exactly
   one hash across all selected observations;
2. every observation row keeps its own per-day value as row-level authority -
   the composite never replaces it and there is no single-value passthrough;
3. the snapshot writer recomputes the composite from the frozen selected
   rows and compares it against the build request, so missing members,
   extra days, same-day conflicts, or row-level tampering fail closed;
4. calendar and symbol normalization policies stay single-valued;
5. the formal (fixture) path keeps its original single-value contract.
"""

from __future__ import annotations

from datetime import date

import pytest

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_phase1.dataset_build import (
    SNAPSHOT_UNIVERSE_POLICY_SET_SCHEMA_VERSION,
    SnapshotUniversePolicySetError,
    build_snapshot_universe_policy_set_hash,
)
from backend.services.advisory_phase1.snapshot_writer import (
    REASON_SOURCE_SNAPSHOT_CONFLICT,
    SnapshotWriterError,
    _retrospective_universe_policy_set_hash,
)

_DAY_1 = date(2026, 7, 6)
_DAY_2 = date(2026, 7, 7)
_DAY_3 = date(2026, 7, 8)
_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64


def _expected(*members: tuple[date, str]) -> str:
    return canonical_json_sha256(
        {
            "schema_version": SNAPSHOT_UNIVERSE_POLICY_SET_SCHEMA_VERSION,
            "members": [
                {
                    "decision_as_of_trade_date": day.isoformat(),
                    "universe_policy_hash": value,
                }
                for day, value in sorted(members)
            ],
        }
    )


def test_multi_day_universe_hashes_are_accepted() -> None:
    result = build_snapshot_universe_policy_set_hash(
        [(_DAY_1, _HASH_A), (_DAY_2, _HASH_B), (_DAY_3, _HASH_A)]
    )
    assert result == _expected((_DAY_1, _HASH_A), (_DAY_2, _HASH_B), (_DAY_3, _HASH_A))


def test_member_order_does_not_change_the_composite() -> None:
    forward = build_snapshot_universe_policy_set_hash(
        [(_DAY_1, _HASH_A), (_DAY_2, _HASH_B), (_DAY_3, _HASH_C)]
    )
    shuffled = build_snapshot_universe_policy_set_hash(
        [(_DAY_3, _HASH_C), (_DAY_1, _HASH_A), (_DAY_2, _HASH_B)]
    )
    duplicated = build_snapshot_universe_policy_set_hash(
        [(_DAY_2, _HASH_B), (_DAY_1, _HASH_A), (_DAY_2, _HASH_B), (_DAY_3, _HASH_C)]
    )
    assert forward == shuffled == duplicated


def test_same_day_conflicting_hashes_fail_closed() -> None:
    with pytest.raises(SnapshotUniversePolicySetError, match="conflicting"):
        build_snapshot_universe_policy_set_hash(
            [(_DAY_1, _HASH_A), (_DAY_1, _HASH_B)]
        )


def test_day_binding_changes_change_the_composite() -> None:
    base = build_snapshot_universe_policy_set_hash([(_DAY_1, _HASH_A), (_DAY_2, _HASH_B)])
    remapped = build_snapshot_universe_policy_set_hash(
        [(_DAY_1, _HASH_B), (_DAY_2, _HASH_A)]
    )
    moved = build_snapshot_universe_policy_set_hash([(_DAY_1, _HASH_A), (_DAY_3, _HASH_B)])
    assert base != remapped
    assert base != moved


def test_missing_or_extra_members_change_the_composite() -> None:
    full = build_snapshot_universe_policy_set_hash([(_DAY_1, _HASH_A), (_DAY_2, _HASH_B)])
    missing = build_snapshot_universe_policy_set_hash([(_DAY_1, _HASH_A)])
    extra = build_snapshot_universe_policy_set_hash(
        [(_DAY_1, _HASH_A), (_DAY_2, _HASH_B), (_DAY_3, _HASH_C)]
    )
    assert full != missing
    assert full != extra


def test_empty_binding_set_fails_closed() -> None:
    with pytest.raises(SnapshotUniversePolicySetError, match="cannot be empty"):
        build_snapshot_universe_policy_set_hash([])


def test_single_day_still_uses_the_composite_algorithm() -> None:
    # No dual semantics: a one-day range stores the domain-tagged composite,
    # never the raw per-day hash.
    result = build_snapshot_universe_policy_set_hash([(_DAY_1, _HASH_A)])
    assert result != _HASH_A
    assert result == _expected((_DAY_1, _HASH_A))


def test_malformed_member_hash_fails_closed() -> None:
    with pytest.raises(ValueError, match="sha256"):
        build_snapshot_universe_policy_set_hash([(_DAY_1, "not-a-hash")])


def _row(version_id: str, universe_policy_hash: str) -> dict[str, str]:
    return {
        "observation_version_id": version_id,
        "universe_policy_hash": universe_policy_hash,
    }


def test_writer_readback_recomputes_the_same_composite() -> None:
    rows = [
        _row("ov_1", _HASH_A),
        _row("ov_2", _HASH_B),
        _row("ov_3", _HASH_A),
        # An unselected historical revision of the same signal does not join
        # the binding set.
        _row("ov_1_old", _HASH_C),
    ]
    dates = {
        "ov_1": _DAY_1,
        "ov_2": _DAY_2,
        "ov_3": _DAY_3,
        "ov_1_old": _DAY_1,
    }
    recomputed = _retrospective_universe_policy_set_hash(
        selected_version_ids={"ov_1", "ov_2", "ov_3"},
        observation_rows=rows,
        observation_dates=dates,
    )
    assert recomputed == build_snapshot_universe_policy_set_hash(
        [(_DAY_1, _HASH_A), (_DAY_2, _HASH_B), (_DAY_3, _HASH_A)]
    )


def test_writer_readback_missing_selected_member_fails_closed() -> None:
    with pytest.raises(SnapshotWriterError, match="missing") as captured:
        _retrospective_universe_policy_set_hash(
            selected_version_ids={"ov_1", "ov_2"},
            observation_rows=[_row("ov_1", _HASH_A)],
            observation_dates={"ov_1": _DAY_1},
        )
    assert captured.value.reason_code == REASON_SOURCE_SNAPSHOT_CONFLICT


def test_writer_readback_same_day_conflict_fails_closed() -> None:
    with pytest.raises(SnapshotWriterError, match="same-day") as captured:
        _retrospective_universe_policy_set_hash(
            selected_version_ids={"ov_1", "ov_2"},
            observation_rows=[_row("ov_1", _HASH_A), _row("ov_2", _HASH_B)],
            observation_dates={"ov_1": _DAY_1, "ov_2": _DAY_1},
        )
    assert captured.value.reason_code == REASON_SOURCE_SNAPSHOT_CONFLICT


def test_writer_readback_tampered_row_hash_changes_the_composite() -> None:
    dates = {"ov_1": _DAY_1, "ov_2": _DAY_2}
    honest = _retrospective_universe_policy_set_hash(
        selected_version_ids={"ov_1", "ov_2"},
        observation_rows=[_row("ov_1", _HASH_A), _row("ov_2", _HASH_B)],
        observation_dates=dates,
    )
    tampered = _retrospective_universe_policy_set_hash(
        selected_version_ids={"ov_1", "ov_2"},
        observation_rows=[_row("ov_1", _HASH_A), _row("ov_2", _HASH_C)],
        observation_dates=dates,
    )
    assert honest != tampered
