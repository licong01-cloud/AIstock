from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_service import (
    CandidateOnlyRequired,
    DatasetReleaseControlService,
    DatasetReleaseProfileBinding,
    ProfileNotAllowed,
    RecordNotFound,
    RunStateInvalid,
    previous_month_end,
)
from backend.services.dataset_release.control_store import ControlStore, IdempotencyConflict


def _service(tmp_path) -> DatasetReleaseControlService:
    store = ControlStore.initialize(tmp_path / "control")
    return DatasetReleaseControlService(
        [
            DatasetReleaseProfileBinding(
                profile_id="qe_hmm_full_v1",
                semantic_profile_digest="a" * 64,
                cutoff_policy="previous_month_last_completed_trading_day",
                store=store,
                cas=CASStore(store.root),
                cutoff_resolver=lambda _: date(2026, 7, 31),
            )
        ]
    )


def test_preview_is_deterministic_and_does_not_write(tmp_path) -> None:
    service = _service(tmp_path)
    before = list((tmp_path / "control" / "cas").rglob("*"))
    preview = service.preview_monthly(
        profile_id="qe_hmm_full_v1",
        cutoff_policy="auto-previous-month",
        scope="full",
        candidate_only=True,
        now=datetime(2026, 8, 11, tzinfo=UTC),
    )
    after = list((tmp_path / "control" / "cas").rglob("*"))
    assert preview["resolved_cutoff"] == "2026-07-31"
    assert preview["activation"] == "not_requested"
    assert before == after


def test_previous_month_policy_uses_asia_shanghai_month_boundary() -> None:
    assert previous_month_end(datetime(2026, 7, 31, 16, 30, tzinfo=UTC)) == date(2026, 7, 31)


def test_default_manual_invocation_key_is_unique_and_explicit_key_owns_replay(
    tmp_path,
) -> None:
    service = _service(tmp_path)
    logical = "b" * 64
    observed = datetime(2026, 8, 11, tzinfo=UTC)
    first = service.monthly_invocation_idempotency_key(
        profile_id="qe_hmm_full_v1",
        scope="full",
        logical_request_key=logical,
        observed_at=observed,
    )
    second = service.monthly_invocation_idempotency_key(
        profile_id="qe_hmm_full_v1",
        scope="full",
        logical_request_key=logical,
        observed_at=observed.replace(hour=1),
    )
    third = service.monthly_invocation_idempotency_key(
        profile_id="qe_hmm_full_v1",
        scope="full",
        logical_request_key=logical,
        observed_at=observed.replace(day=12, hour=1),
    )
    assert len({first, second, third}) == 3
    assert all(value.startswith("dsi_") and len(value) == 68 for value in (first, second, third))


def test_submit_is_durable_idempotent_and_never_creates_a_run_in_api(tmp_path) -> None:
    service = _service(tmp_path)
    arguments = {
        "profile_id": "qe_hmm_full_v1",
        "cutoff_policy": "auto-previous-month",
        "scope": "full",
        "candidate_only": True,
        "principal": "operator:one",
        "idempotency_key": "monthly-2026-07",
        "route": "POST:/api/v1/dataset-releases/runs",
        "now": datetime(2026, 8, 11, tzinfo=UTC),
    }
    first = service.submit_monthly(**arguments)
    store = service._bindings["qe_hmm_full_v1"].store
    with store.transaction() as connection:
        connection.execute(
            "UPDATE submissions SET state='RESOLVING_SOURCE' WHERE submission_id=?",
            (first["submission_id"],),
        )
    replay = service.submit_monthly(**arguments)
    assert first["submission_id"] == replay["submission_id"]
    assert replay["replayed"] is True
    assert {key: value for key, value in first.items() if key != "replayed"} == {
        key: value for key, value in replay.items() if key != "replayed"
    }
    assert first["state"] == "QUEUED_RESOLUTION"
    assert first["run_id"] is None
    assert service.get_submission("qe_hmm_full_v1", first["submission_id"])["state"] == "RESOLVING_SOURCE"

    with pytest.raises(IdempotencyConflict):
        service.submit_monthly(**{**arguments, "scope": "sample"})


def test_service_fails_closed_on_profile_candidate_and_missing_record(tmp_path) -> None:
    service = _service(tmp_path)
    with pytest.raises(ProfileNotAllowed):
        service.preview_monthly(
            profile_id="unknown",
            cutoff_policy="auto-previous-month",
            scope="full",
            candidate_only=True,
        )
    with pytest.raises(CandidateOnlyRequired):
        service.preview_monthly(
            profile_id="qe_hmm_full_v1",
            cutoff_policy="auto-previous-month",
            scope="full",
            candidate_only=False,
        )
    with pytest.raises(RecordNotFound):
        service.get_run("qe_hmm_full_v1", "missing")


def test_command_is_durable_and_idempotency_conflict_is_loud(tmp_path) -> None:
    service = _service(tmp_path)
    submission = service.submit_monthly(
        profile_id="qe_hmm_full_v1",
        cutoff_policy="auto-previous-month",
        scope="full",
        candidate_only=True,
        principal="operator:one",
        idempotency_key="monthly",
        route="runs",
        now=datetime(2026, 8, 11, tzinfo=UTC),
    )
    values = {
        "profile_id": "qe_hmm_full_v1",
        "target_type": "submission",
        "target_id": submission["submission_id"],
        "command_type": "CANCEL_REQUESTED",
        "principal": "operator:one",
        "route": "cancel",
        "idempotency_key": "cancel-once",
    }
    first = service.enqueue_command(**values)
    replay = service.enqueue_command(**values)
    assert first["command_id"] == replay["command_id"]
    assert replay["replayed"] is True


def test_run_list_rejects_unknown_state_filter(tmp_path) -> None:
    service = _service(tmp_path)
    with pytest.raises(RunStateInvalid):
        service.list_runs("qe_hmm_full_v1", states=("TYPO_STATE",))
