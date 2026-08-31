from datetime import date

import pytest

from scripts.simulation_history_cleanup_core import (
    CleanupRequest,
    CleanupSafetyError,
    canonical_plan_sha256,
    classify_miniqmt_runtime,
)


KEEP = ("pkg_local", "pkg_miniqmt")
CUTOFF = date(2026, 8, 13)


def test_request_requires_explicit_two_package_keep_set() -> None:
    with pytest.raises(CleanupSafetyError, match="at least two"):
        CleanupRequest.build(["pkg_local"], CUTOFF)
    assert CleanupRequest.build(["pkg_miniqmt", "pkg_local", "pkg_local"], CUTOFF).keep_package_ids == KEEP


def test_plan_digest_is_canonical_and_excludes_its_own_field() -> None:
    left = {"b": [2, 1], "a": {"x": "中"}}
    right = {"a": {"x": "中"}, "b": [2, 1], "plan_sha256": "ignored"}
    assert canonical_plan_sha256(left) == canonical_plan_sha256(right)


@pytest.mark.parametrize(
    ("binding_id", "package_id", "expected"),
    [
        (None, None, "purge_unbound_legacy"),
        ("binding-old", "pkg_old", "purge_obsolete_package"),
        ("binding-current", "pkg_miniqmt", "keep_package"),
    ],
)
def test_classify_legacy_sim_runtime(binding_id: str | None, package_id: str | None, expected: str) -> None:
    assert (
        classify_miniqmt_runtime(
            mode="SIM",
            trade_date=date(2026, 8, 12),
            binding_id=binding_id,
            binding_package_id=package_id,
            keep_package_ids=KEEP,
            cutoff=CUTOFF,
        )
        == expected
    )


def test_classify_never_purges_at_or_after_cutoff() -> None:
    assert (
        classify_miniqmt_runtime(
            mode="SIM",
            trade_date=CUTOFF,
            binding_id=None,
            binding_package_id=None,
            keep_package_ids=KEEP,
            cutoff=CUTOFF,
        )
        == "keep_cutoff"
    )


def test_classify_fails_closed_for_unknown_binding_and_non_sim() -> None:
    with pytest.raises(CleanupSafetyError, match="unknown binding"):
        classify_miniqmt_runtime(
            mode="SIM",
            trade_date=date(2026, 8, 12),
            binding_id="missing",
            binding_package_id=None,
            keep_package_ids=KEEP,
            cutoff=CUTOFF,
        )
    with pytest.raises(CleanupSafetyError, match="non-SIM"):
        classify_miniqmt_runtime(
            mode="LIVE",
            trade_date=date(2026, 8, 12),
            binding_id=None,
            binding_package_id=None,
            keep_package_ids=KEEP,
            cutoff=CUTOFF,
        )
