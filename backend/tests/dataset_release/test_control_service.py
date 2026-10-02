from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

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
from backend.services.dataset_release.profile import load_initial_migration_plan
from backend.services.dataset_release.sector_data_candidate_source import (
    sector_candidate_scope_key,
)


ROOT = Path(__file__).resolve().parents[3]
INITIAL_PLAN_PATH = ROOT / "configs" / "datasets" / "migrations" / "pit_v2_initial_20260731_v1.yaml"


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


def _initial_service(tmp_path) -> DatasetReleaseControlService:
    store = ControlStore.initialize(tmp_path / "control")
    plan = load_initial_migration_plan(INITIAL_PLAN_PATH)
    return DatasetReleaseControlService(
        [
            DatasetReleaseProfileBinding(
                profile_id="qe_hmm_full_v2",
                semantic_profile_digest="b" * 64,
                cutoff_policy="previous_month_last_completed_trading_day",
                store=store,
                cas=CASStore(store.root),
                cutoff_resolver=lambda _: date(2099, 12, 31),
                initial_migration_plans={plan.plan_id: plan},
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


def test_submit_records_preview_status_without_changing_worker_request_schema(tmp_path) -> None:
    service = _service(tmp_path)
    observed_at = datetime(2026, 8, 11, tzinfo=UTC)
    preview = service.preview_monthly(
        profile_id="qe_hmm_full_v1",
        cutoff_policy="auto-previous-month",
        scope="full",
        candidate_only=True,
        now=observed_at,
    )
    submitted = service.submit_monthly(
        profile_id="qe_hmm_full_v1",
        cutoff_policy="auto-previous-month",
        scope="full",
        candidate_only=True,
        principal="operator:one",
        idempotency_key="monthly-preview-token",
        route="POST:/api/v1/dataset-releases/runs",
        now=observed_at,
        preview_token=preview["preview_token"],
    )
    binding = service._bindings["qe_hmm_full_v1"]
    row = binding.store.get_submission(submitted["submission_id"])
    request = binding.cas.get_json_bounded(row["request_ref"], max_bytes=2 * 1024**2)["request"]

    assert submitted["preview_token_status"] == "valid"
    assert "supplied_preview_token" not in request

    drifted = service.submit_monthly(
        profile_id="qe_hmm_full_v1",
        cutoff_policy="auto-previous-month",
        scope="full",
        candidate_only=True,
        principal="operator:one",
        idempotency_key="monthly-preview-token-drifted",
        route="POST:/api/v1/dataset-releases/runs",
        now=observed_at,
        preview_token=f"{preview['preview_token']}drift",
    )
    drifted_row = binding.store.get_submission(drifted["submission_id"])
    drifted_request = binding.cas.get_json_bounded(
        drifted_row["request_ref"], max_bytes=2 * 1024**2
    )["request"]
    drifted_event_types = [
        event["type"]
        for event in binding.store.list_events(submission_id=drifted["submission_id"])
    ]

    assert drifted["preview_token_status"] == "stale_or_mismatch"
    assert "supplied_preview_token" not in drifted_request
    assert drifted_event_types == ["SUBMISSION_QUEUED", "PREVIEW_TOKEN_STALE_OR_MISMATCH"]


def test_initial_migration_submission_binds_fixed_plan_not_wall_clock_cutoff(tmp_path) -> None:
    service = _initial_service(tmp_path)
    result = service.submit_initial_migration(
        profile_id="qe_hmm_full_v2",
        plan_id="pit_v2_initial_20260731_v1",
        scope="sample",
        candidate_only=True,
        principal="operator:one",
        idempotency_key="initial-pit-v2-sample",
        route="cli:initial-migration",
        now=datetime(2027, 3, 15, tzinfo=UTC),
    )
    binding = service._bindings["qe_hmm_full_v2"]
    row = binding.store.get_submission(result["submission_id"])
    request = binding.cas.get_json_bounded(row["request_ref"], max_bytes=2 * 1024**2)["request"]

    assert result["fixed_cutoff"] == "2026-07-31"
    assert result["plan_digest"] == request["plan_digest"]
    assert request["cutoff_policy"] == "fixed-allowlisted-plan"
    assert request["resolved_cutoff"] == "2026-07-31"
    assert request["sample_instruments"] == [
        "000001.SZ",
        "300379.SZ",
        "300741.SZ",
        "600462.SH",
        "600930.SH",
        "688981.SH",
    ]
    assert sector_candidate_scope_key(request["sample_instruments"]) == (
        "sample-554c8193b6f6de4c859b0f16881b1f34e6eb11c41a68f416b829801b941301d2"
    )
    assert sector_candidate_scope_key(request["sample_instruments"]) != sector_candidate_scope_key(
        ["000001.SZ", "300379.SZ", "600462.SH", "600930.SH", "688981.SH"]
    )
    assert {
        (item["instrument"], item["oracle"])
        for item in request["event_windows"]
    } >= {
        ("300741.SZ", "p3a_classification_index_authority_alignment_boundary")
    }
    assert binding.store._many("SELECT * FROM runs", ()) == []


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
