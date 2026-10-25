from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.contracts import (
    Scope,
    SubmissionIdentity,
)
from backend.services.dataset_release.control_store import (
    ControlStore,
)
from backend.services.dataset_release.control_service import (
    DatasetReleaseControlService,
    DatasetReleaseProfileBinding,
)
from backend.services.dataset_release.profile import load_dataset_profile, load_initial_migration_plan
from backend.services.dataset_release.resolution import (
    ResolutionService,
)
from backend.services.dataset_release.resolution_processor import (
    MonthlyResolutionProcessor,
    ResolutionProcessorError,
    ResolutionRequestInvalid,
    ResolutionSourceDriftWaiting,
    SupervisedResolutionSourceStage,
)
from backend.services.dataset_release.resource_budget import ResourceAdmissionClass
from backend.services.dataset_release.worker import (
    WORKER_ERROR_RECEIPT_SCHEMA,
    ProcessorDisposition,
)


class _FrozenFixtureStage:
    def __init__(self, frozen: Any) -> None:
        self.frozen = frozen

    def freeze(self, context, **_kwargs):
        context.checkpoint()
        return self.frozen


ROOT = Path(__file__).resolve().parents[3]
V2_PROFILE_PATH = ROOT / "configs" / "datasets" / "qe_backtest_monthly_v2.yaml"
INITIAL_PLAN_PATH = ROOT / "configs" / "datasets" / "migrations" / "pit_v2_initial_20260731_v1.yaml"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def test_production_resolution_processor_uses_supervised_source_stage(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    processor = MonthlyResolutionProcessor(
        dataset_profile,
        store,
        CASStore(store.root),
    )
    assert isinstance(processor.source_stage, SupervisedResolutionSourceStage)


def test_monthly_control_request_is_accepted_by_resolution_processor(tmp_path) -> None:
    profile = load_dataset_profile(V2_PROFILE_PATH)
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = DatasetReleaseControlService(
        (
            DatasetReleaseProfileBinding(
                profile_id=profile.profile,
                semantic_profile_digest=profile.semantic_profile_digest,
                cutoff_policy=profile.cutoff_policy,
                store=store,
                cas=cas,
                cutoff_resolver=lambda _: date(2026, 7, 31),
            ),
        )
    )
    observed_at = datetime(2026, 8, 11, tzinfo=UTC)
    preview = service.preview_monthly(
        profile_id=profile.profile,
        cutoff_policy="auto-previous-month",
        scope="full",
        candidate_only=True,
        now=observed_at,
    )
    submitted = service.submit_monthly(
        profile_id=profile.profile,
        cutoff_policy="auto-previous-month",
        scope="full",
        candidate_only=True,
        principal="operator",
        idempotency_key="monthly-control-processor-contract",
        route="cli:monthly",
        now=observed_at,
        preview_token=preview["preview_token"],
    )

    request = MonthlyResolutionProcessor(
        profile,
        store,
        cas,
        source_authority=object(),
    )._read_request(store.get_submission(submitted["submission_id"]))

    assert request.profile == profile.profile
    assert request.cutoff == date(2026, 7, 31)
    assert request.logical_request_key == submitted["logical_request_key"]


def test_resolution_resource_spec_ignores_legacy_waiting_pressure_rung(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    submission = store.submit(
        principal="operator",
        route="runs",
        idempotency_key="pressure-rung",
        request_hash="request-pressure-rung",
        logical_request_key="logical-pressure-rung",
        request_ref="request-ref",
    )
    claim = ResolutionService(store, cas).claim(
        submission_id=submission["submission_id"],
        owner_identity="worker",
        ttl_seconds=60,
    )
    error_ref = cas.put_json(
        {
            "schema_version": WORKER_ERROR_RECEIPT_SCHEMA,
            "kind": "resolution",
            "target_id": submission["submission_id"],
            "disposition": "WAITING",
            "context": {"pressure_rung": 1, "data_scope_changed": False},
        }
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE resolution_attempts SET state='RELEASED_WAITING',error_ref=? WHERE resolution_attempt_id=?",
            (error_ref.sha256, claim.attempt_id),
        )
    processor = MonthlyResolutionProcessor(
        dataset_profile,
        store,
        cas,
        source_authority=object(),
    )
    processor._predicted_new_bytes = lambda _submission: 0
    processor._read_request = lambda _submission: SimpleNamespace(scope=Scope.FULL)
    assert processor.resource_spec(submission).pressure_rung == 0


def test_supervised_source_stage_uses_versioned_timeout_and_rung(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    stage = SupervisedResolutionSourceStage(dataset_profile, store, cas)
    observed: dict[str, Any] = {}

    class Context:
        claim = SimpleNamespace(attempt_id="attempt-1", attempt_fence=7)

        def run_supervised(self, command, **kwargs):
            observed["command"] = list(command)
            observed.update(kwargs)
            return SimpleNamespace(
                returncode=1,
                active_processes=0,
                runtime="windows",
            )

    with pytest.raises(ResolutionProcessorError, match="source stage failed"):
        stage.freeze(
            Context(),
            cutoff=date(2026, 7, 31),
            baseline_reuse_ref=None,
            baseline_partitions=(),
            predicted_new_bytes=1,
            pressure_rung=2,
            sample_instruments=("000001.SZ", "600462.SH"),
        )

    assert observed["timeout_seconds"] == float(dataset_profile.stage_timeouts_seconds["source_freeze"])
    command = observed["command"]
    assert command[command.index("--pressure-rung") + 1] == "2"
    assert command[command.index("--stage-timeout-seconds") + 1] == str(
        dataset_profile.stage_timeouts_seconds["source_freeze"]
    )
    assert [command[index + 1] for index, value in enumerate(command) if value == "--sample-instrument"] == [
        "000001.SZ",
        "600462.SH",
    ]


def test_supervised_source_stage_preserves_typed_drift_as_waitable(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    stage = SupervisedResolutionSourceStage(dataset_profile, store, CASStore(store.root))

    class Context:
        claim = SimpleNamespace(attempt_id="attempt-1", attempt_fence=7)

        def run_supervised(self, command, **_kwargs):
            result = Path(command[command.index("--result-path") + 1])
            result.parent.mkdir(parents=True, exist_ok=True)
            result.write_text(
                json.dumps(
                    {
                        "schema_version": "dataset_release_source_stage_error_v1",
                        "error_code": "BLOCKED_SOURCE_SNAPSHOT_DRIFT",
                        "exception_type": "SourceSnapshotDriftBlocked",
                        "message_sha256": "a" * 64,
                        "context_ref": None,
                        "safety": {
                            "database_writes": 0,
                            "provider_database_writes": 0,
                            "production_writes": 0,
                            "production_deletes": 0,
                            "production_pointer_changes": 0,
                            "service_process_controls": 0,
                            "candidate_writes": 0,
                        },
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=2, active_processes=0, runtime="windows")

    with pytest.raises(ResolutionSourceDriftWaiting) as captured:
        stage.freeze(
            Context(),
            cutoff=date(2026, 7, 31),
            baseline_reuse_ref=None,
            baseline_partitions=(),
            predicted_new_bytes=1,
            pressure_rung=2,
            sample_instruments=("000001.SZ",),
        )

    assert captured.value.code == "BLOCKED_SOURCE_SNAPSHOT_DRIFT"
    assert captured.value.context["source_stage_message_sha256"] == "a" * 64


def test_supervised_source_stage_rejects_spoofed_drift_envelope(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    stage = SupervisedResolutionSourceStage(dataset_profile, store, CASStore(store.root))

    class Context:
        claim = SimpleNamespace(attempt_id="attempt-1", attempt_fence=7)

        def run_supervised(self, command, **_kwargs):
            result = Path(command[command.index("--result-path") + 1])
            result.parent.mkdir(parents=True, exist_ok=True)
            result.write_text(
                json.dumps(
                    {
                        "schema_version": "dataset_release_source_stage_error_v1",
                        "error_code": "BLOCKED_SOURCE_SNAPSHOT_DRIFT",
                        "exception_type": "RuntimeError",
                        "message_sha256": "a" * 64,
                        "context_ref": None,
                        "safety": {
                            "database_writes": 0,
                            "provider_database_writes": 0,
                            "production_writes": 0,
                            "production_deletes": 0,
                            "production_pointer_changes": 0,
                            "service_process_controls": 0,
                            "candidate_writes": 0,
                        },
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=2, active_processes=0, runtime="windows")

    with pytest.raises(ResolutionProcessorError, match="source stage failed"):
        stage.freeze(
            Context(),
            cutoff=date(2026, 7, 31),
            baseline_reuse_ref=None,
            baseline_partitions=(),
            predicted_new_bytes=1,
            pressure_rung=0,
            sample_instruments=("000001.SZ",),
        )


def test_resolution_processor_turns_typed_drift_into_waiting_without_scope_change(
    dataset_profile,
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")

    class DriftStage:
        def freeze(self, _context, **_kwargs):
            raise ResolutionSourceDriftWaiting(
                "transient drift",
                context={
                    "source_stage_error_code": "BLOCKED_SOURCE_SNAPSHOT_DRIFT",
                    "source_stage_exception_type": "SourceSnapshotDriftBlocked",
                    "source_stage_message_sha256": "b" * 64,
                },
            )

    processor = MonthlyResolutionProcessor(
        dataset_profile,
        store,
        CASStore(store.root),
        source_stage=DriftStage(),
    )
    processor._read_request = lambda _record: SimpleNamespace(
        cutoff=date(2026, 7, 31),
        sample_instruments=("000001.SZ",),
        scope=Scope.SAMPLE,
    )
    processor._bounded_catalog = lambda _request: []
    processor._select_candidate = lambda _request, _candidates: None
    processor._candidate_source_evidence = lambda _candidate: None
    processor._source_reuse_baseline = lambda _request: None
    processor._predicted_new_bytes = lambda _record: 0
    context = SimpleNamespace(
        kind="resolution",
        store=store,
        record={},
        pressure_rung=2,
        checkpoint=lambda: None,
    )

    result = processor.process(context)

    assert result.disposition is ProcessorDisposition.WAITING
    assert result.error_code == "BLOCKED_SOURCE_SNAPSHOT_DRIFT"
    assert result.context["pressure_rung"] == 2
    assert result.context["data_scope_changed"] is False


@pytest.mark.parametrize("resolution_path", ("reattest", "build"))
def test_processor_passes_artifact_ready_authority_to_every_action_plan_path(
    dataset_profile,
    tmp_path,
    monkeypatch,
    resolution_path: str,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    artifact_ready_ref = cas.put_json({"fixture": "artifact-ready-authority"})
    frozen = SimpleNamespace(artifact_ready_contract_ref=artifact_ready_ref)
    processor = MonthlyResolutionProcessor(
        dataset_profile,
        store,
        cas,
        source_stage=_FrozenFixtureStage(frozen),
    )
    request = SimpleNamespace(
        cutoff=date(2026, 7, 31),
        sample_instruments=("000001.SZ",),
        scope=Scope.SAMPLE,
    )
    candidate = object() if resolution_path == "reattest" else None
    probe = object()
    action_plan = object()
    captured: dict[str, Any] = {}

    monkeypatch.setattr(processor, "_read_request", lambda _record: request)
    monkeypatch.setattr(processor, "_bounded_catalog", lambda _request: [])
    monkeypatch.setattr(processor, "_select_candidate", lambda _request, _rows: candidate)
    monkeypatch.setattr(processor, "_candidate_source_evidence", lambda _candidate: None)
    monkeypatch.setattr(processor, "_source_reuse_baseline", lambda _request: None)
    monkeypatch.setattr(processor, "_predicted_new_bytes", lambda _record: 0)
    monkeypatch.setattr(processor, "_record_probe", lambda *_args: probe)
    monkeypatch.setattr(processor, "_source_snapshot_catalog_spec", lambda *_args: object())
    monkeypatch.setattr(processor, "_fresh_attestation", lambda *_args: None)
    if resolution_path == "reattest":
        monkeypatch.setattr(processor, "_should_reattest", lambda *_args: True)
        monkeypatch.setattr(
            processor,
            "_reattest_plan",
            lambda *_args: (action_plan, SimpleNamespace(key="validation", target_key="target")),
        )
    else:
        monkeypatch.setattr(processor, "_build_plan", lambda *_args: action_plan)
        monkeypatch.setattr(processor, "_build_inputs", lambda **_kwargs: {"fixture": "inputs"})

    def capture_action_plan(_service, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(ResolutionService, "resolve_action_plan", capture_action_plan)
    context = SimpleNamespace(
        kind="resolution",
        store=store,
        record={},
        target_id="submission",
        claim=object(),
        pressure_rung=0,
        checkpoint=lambda: None,
    )

    result = processor.process(context)

    assert result.disposition is ProcessorDisposition.DURABLE_SUCCESS
    assert captured["artifact_ready_contract_ref"] == artifact_ready_ref


def test_resolution_reader_revalidates_fixed_initial_plan_and_sample_scope(tmp_path) -> None:
    profile = load_dataset_profile(V2_PROFILE_PATH)
    plan = load_initial_migration_plan(INITIAL_PLAN_PATH)
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    service = DatasetReleaseControlService(
        (
            DatasetReleaseProfileBinding(
                profile_id=profile.profile,
                semantic_profile_digest=profile.semantic_profile_digest,
                cutoff_policy=profile.cutoff_policy,
                store=store,
                cas=cas,
                cutoff_resolver=lambda _: date(2099, 12, 31),
                initial_migration_plans={plan.plan_id: plan},
            ),
        )
    )
    submitted = service.submit_initial_migration(
        profile_id=profile.profile,
        plan_id=plan.plan_id,
        scope="sample",
        candidate_only=True,
        principal="operator",
        idempotency_key="initial-plan-reader",
        route="cli:initial-migration",
        now=datetime(2027, 3, 15, tzinfo=UTC),
    )
    processor = MonthlyResolutionProcessor(profile, store, cas, source_authority=object())
    request = processor._read_request(store.get_submission(submitted["submission_id"]))

    assert request.is_initial_migration is True
    assert request.cutoff == date(2026, 7, 31)
    assert request.sample_instruments == plan.sample_instruments
    processor._predicted_new_bytes = lambda _submission: 0
    assert (
        processor.resource_spec(store.get_submission(submitted["submission_id"])).admission_class
        is ResourceAdmissionClass.RESOLUTION_LIGHT
    )

    valid_outer = cas.get_json_bounded(
        store.get_submission(submitted["submission_id"])["request_ref"],
        max_bytes=2 * 1024**2,
    )
    tampered = {**valid_outer["request"], "plan_digest": "c" * 64, "logical_request_key": "d" * 64}
    rejected = ResolutionService(store, cas).submit(
        identity=SubmissionIdentity(
            principal="operator",
            route="cli:initial-migration",
            idempotency_key="initial-plan-reader-tampered",
        ),
        logical_request_key="d" * 64,
        request_payload=tampered,
    )
    with pytest.raises(ResolutionRequestInvalid, match="checked-in plan"):
        processor._read_request(store.get_submission(rejected["submission_id"]))

    tampered_logical = {**valid_outer["request"], "logical_request_key": "e" * 64}
    rejected_logical = ResolutionService(store, cas).submit(
        identity=SubmissionIdentity(
            principal="operator",
            route="cli:initial-migration",
            idempotency_key="initial-plan-reader-logical-tampered",
        ),
        logical_request_key="e" * 64,
        request_payload=tampered_logical,
    )
    with pytest.raises(ResolutionRequestInvalid, match="checked-in plan"):
        processor._read_request(store.get_submission(rejected_logical["submission_id"]))
