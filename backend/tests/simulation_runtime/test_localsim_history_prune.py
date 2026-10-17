from datetime import date

import pytest

from scripts.localsim_history_prune_core import (
    ANCHOR_STATUS,
    DELETABLE_STATUSES,
    LocalSimPruneRequest,
    LocalSimPruneSafetyError,
    canonical_sha256,
)


def test_request_requires_explicit_package_and_anchor_identities() -> None:
    request = LocalSimPruneRequest.build("simacct_current", "pkg_current", "simrun_current")
    assert request.simulation_account_id == "simacct_current"
    assert request.package_id == "pkg_current"
    assert request.anchor_run_id == "simrun_current"
    with pytest.raises(LocalSimPruneSafetyError, match="pkg_"):
        LocalSimPruneRequest.build("simacct_current", "current", "simrun_current")
    with pytest.raises(LocalSimPruneSafetyError, match="simrun_"):
        LocalSimPruneRequest.build("simacct_current", "pkg_current", "current")
    with pytest.raises(LocalSimPruneSafetyError, match="simacct_"):
        LocalSimPruneRequest.build("current", "pkg_current", "simrun_current")


def test_prune_status_contract_preserves_success_and_active_states() -> None:
    assert ANCHOR_STATUS == "SUCCEEDED"
    assert DELETABLE_STATUSES == ("CANCELLED", "FAILED_RETRYABLE", "FAILED_TERMINAL")
    assert "SUCCEEDED" not in DELETABLE_STATUSES
    assert "INTRADAY_RUNNING" not in DELETABLE_STATUSES


def test_plan_digest_is_canonical_and_date_stable() -> None:
    left = {"anchor": {"trade_date": date(2026, 8, 28)}, "runs": ["b", "a"]}
    right = {"runs": ["b", "a"], "anchor": {"trade_date": "2026-08-28"}}
    assert canonical_sha256(left) == canonical_sha256(right)
    assert canonical_sha256({"runs": ["a", "b"]}) != canonical_sha256({"runs": ["b", "a"]})
