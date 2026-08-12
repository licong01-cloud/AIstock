from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.contracts import Scope
from backend.services.dataset_release.reconciler import (
    MonthlyDatasetReconciler,
    ReconcileError,
)


NOW = datetime(2026, 8, 11, 9, 0, tzinfo=UTC)


def _previous_month_end(value: datetime) -> date:
    first = date(value.year, value.month, 1)
    return first - timedelta(days=1)


def test_reconcile_is_disabled_by_default_and_has_no_store_side_effect(
    tmp_path,
    dataset_profile,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    reconciler = MonthlyDatasetReconciler(
        profile=dataset_profile,
        store=store,
        cutoff_resolver=_previous_month_end,
        now=lambda: NOW,
    )

    report = reconciler.run_once(owner_identity="fixture-owner")

    assert report.state == "DISABLED"
    assert store.list_submissions() == []
    assert store._many("SELECT * FROM reconcile_leases", ()) == []


def test_reconcile_submits_three_candidate_only_months_and_same_cycle_deduplicates(
    tmp_path,
    dataset_profile,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    anchors: list[datetime] = []

    def resolver(value: datetime) -> date:
        anchors.append(value)
        return _previous_month_end(value)

    reconciler = MonthlyDatasetReconciler(
        profile=dataset_profile,
        store=store,
        cutoff_resolver=resolver,
        enabled=True,
        now=lambda: NOW,
    )

    first = reconciler.run_once(owner_identity="fixture-owner", cycle_id="2026-08-11")
    replay = reconciler.run_once(owner_identity="fixture-other", cycle_id="2026-08-11")

    assert first.state == "COMPLETED"
    assert [item.cutoff for item in first.items] == [
        "2026-07-31",
        "2026-06-30",
        "2026-05-31",
    ]
    assert all(item.disposition == "SUBMITTED" for item in first.items)
    assert all(item.disposition == "ACTIVE_REUSED" for item in replay.items)
    assert len(store.list_submissions(limit=20)) == 3
    assert len(anchors) == 6
    lease = store._many("SELECT * FROM reconcile_leases", ())[0]
    assert lease["state"] == "FREE"
    assert lease["fence_counter"] == 2
    for row in store.list_submissions(limit=20):
        payload = reconciler.cas.get_json(str(row["request_ref"]))
        request = payload["request"]
        assert request["candidate_only"] is True
        assert request["activation"] == "not_requested"
        assert request["operation"] == "SOURCE_REVISION_PROBE"


def test_reconcile_singleton_busy_and_expired_lease_takeover_are_fenced(
    tmp_path,
    dataset_profile,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    first = MonthlyDatasetReconciler(
        profile=dataset_profile,
        store=store,
        cutoff_resolver=_previous_month_end,
        enabled=True,
        now=lambda: NOW,
    )
    held = first._claim("first-owner", "cycle-one", NOW)
    assert held is not None and held.fence == 1

    busy = MonthlyDatasetReconciler(
        profile=dataset_profile,
        store=store,
        cutoff_resolver=_previous_month_end,
        enabled=True,
        now=lambda: NOW + timedelta(seconds=1),
    ).run_once(owner_identity="second-owner", cycle_id="cycle-two")
    assert busy.state == "LEASE_BUSY"
    assert store.list_submissions() == []

    takeover = MonthlyDatasetReconciler(
        profile=dataset_profile,
        store=store,
        cutoff_resolver=_previous_month_end,
        enabled=True,
        now=lambda: NOW + timedelta(seconds=301),
    ).run_once(owner_identity="second-owner", cycle_id="cycle-two")
    assert takeover.state == "COMPLETED"
    assert takeover.fence == 2
    durable = store._many("SELECT * FROM reconcile_leases", ())[0]
    assert durable["state"] == "FREE" and durable["fence_counter"] == 2


def test_reconcile_calendar_failure_releases_singleton_without_submission(
    tmp_path,
    dataset_profile,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    reconciler = MonthlyDatasetReconciler(
        profile=dataset_profile,
        store=store,
        cutoff_resolver=lambda _value: (_ for _ in ()).throw(ReconcileError("calendar unavailable")),
        enabled=True,
        now=lambda: NOW,
    )

    with pytest.raises(ReconcileError, match="calendar unavailable"):
        reconciler.run_once(owner_identity="fixture-owner")

    assert store.list_submissions() == []
    lease = store._many("SELECT * FROM reconcile_leases", ())[0]
    assert lease["state"] == "FREE"


def test_multiple_active_submissions_for_one_logical_key_are_invariant_failure(
    tmp_path,
    dataset_profile,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    reconciler = MonthlyDatasetReconciler(
        profile=dataset_profile,
        store=store,
        cutoff_resolver=_previous_month_end,
        enabled=True,
        now=lambda: NOW,
    )
    logical = reconciler._logical(date(2026, 7, 31), Scope.FULL).key
    for suffix in ("one", "two"):
        store.submit(
            principal="fixture",
            route="fixture",
            idempotency_key=suffix,
            request_hash=f"request-{suffix}",
            logical_request_key=logical,
            request_ref=f"request-ref-{suffix}",
        )

    with pytest.raises(ReconcileError, match="multiple active submissions"):
        reconciler._active_submission(logical)

    report = reconciler.run_once(owner_identity="fixture-owner")
    assert report.state == "PARTIAL_FAILURE"
    assert report.items[0].disposition == "FAILED"
    assert report.items[0].detail == "ReconcileError"
    assert len(store.list_submissions(limit=20)) == 4
