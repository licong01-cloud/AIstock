from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from backend.services.dataset_release import monthly_unified as monthly_subject
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_unified import (
    AUTHORIZATION_SCHEMA,
    CONSUMER_READBACK_SCHEMA,
    RELEASE_CLOSURE_SCHEMA,
    ActionAuthorizationStore,
    ActiveProfileSnapshot,
    MonthlyReleaseError,
    MonthlyOperationStore,
    MonthlyReleaseAuthorizationError,
    MonthlyReleaseConflict,
    MonthlyReleaseRequest,
    MonthlyReleaseRequestInvalid,
    MonthlyReleaseService,
    REQUIRED_CONSUMERS,
    REQUIRED_NODES,
    SOURCE_GATES,
    STAGES,
    TELEMETRY_COUNT_FIELDS,
    build_stage_receipt,
)
from backend.services.dataset_release.monthly_worker import MonthlyReleaseWorker


HEX = "a" * 64


def _canonical(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) + b"\n")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _active(path: Path, *, cutoff: str = "2026-08-31") -> None:
    baseline = path.parent / "baseline"
    baseline.mkdir(exist_ok=True)
    manifest = baseline / "qe_dataset_manifest.json"
    _canonical(manifest, {"dataset_manifest_sha256": HEX})
    _canonical(
        path,
        {
            "schema_version": "aistock_active_dataset_profile_v3",
            "generation": "20260920-v14-unified",
            "release_id": "qe_hmm_full_v2_20260831",
            "cutoff": cutoff,
            "controller_paths": {"candidate_root": str(baseline)},
            "components": {
                "dataset_manifest_sha256": HEX,
                "dataset_manifest_file_sha256": _sha(manifest),
            },
        },
    )


def test_active_profile_rejects_linked_candidate_ancestor(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    active = real_parent / "active.json"
    _active(active)
    linked_parent = tmp_path / "linked"
    try:
        linked_parent.symlink_to(real_parent, target_is_directory=True)
    except OSError:
        pytest.skip("filesystem does not allow directory symlinks")
    with pytest.raises(MonthlyReleaseError, match="link or junction"):
        ActiveProfileSnapshot.read(linked_parent / "active.json")


def test_active_profile_checks_every_existing_path_segment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    active = tmp_path / "plain" / "active.json"
    active.parent.mkdir()
    _active(active)
    blocked = active.parent
    original = monthly_subject._is_link_or_junction
    monkeypatch.setattr(
        monthly_subject,
        "_is_link_or_junction",
        lambda path: path == blocked or original(path),
    )
    with pytest.raises(MonthlyReleaseError, match="link or junction"):
        ActiveProfileSnapshot.read(active)


class Pipeline:
    def __init__(
        self,
        *,
        manifest: str = "b" * 64,
        fail_once_at: str | None = None,
        corrupt_profile_at_close: bool = False,
    ) -> None:
        self.manifest = manifest
        self.fail_once_at = fail_once_at
        self.corrupt_profile_at_close = corrupt_profile_at_close
        self.calls: list[str] = []

    def run_stage(
        self,
        *,
        stage: str,
        operation_id: str,
        attempt: int,
        request: Mapping[str, Any],
        plan: Mapping[str, Any],
        prior_receipts: Mapping[str, Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        del request, prior_receipts
        self.calls.append(stage)
        if stage == self.fail_once_at:
            self.fail_once_at = None
            raise RuntimeError("planned interruption")
        if stage == "BUILD":
            _canonical(
                Path(str(plan["profile_candidate"])),
                {
                    "schema_version": "aistock_active_dataset_profile_v4",
                    "generation": plan["generation"],
                    "release_id": plan["release_id"],
                    "cutoff": plan["target_cutoff"],
                    "components": {
                        "dataset_manifest_sha256": self.manifest,
                        "release_closure_sha256": HEX,
                        "derived_asset_registry_sha256": HEX,
                    },
                },
            )
        ref = {"id": f"receipts/{stage.lower()}.json", "sha256": HEX, "size": 1}
        output_refs = [ref]
        scope: dict[str, Any] = {"dataset_manifest_sha256": self.manifest}
        if stage == "SOURCE":
            scope = {
                "gates": list(SOURCE_GATES),
                "change_count": 1,
                "source_as_of": "2026-10-01T00:00:00+00:00",
                "snapshot_group_id": "pg-exported-snapshot-test",
                "consistent_input_set_complete": True,
                "snapshot_identity_refs": [ref],
                "repair_overlap_check_refs": [ref],
                "change_scope_refs": [ref],
                "source_gate_refs": [ref],
                "producer_contract_refs": [ref],
                "component_actions": {
                    "day": "INCREMENTAL",
                    "minute": "REUSE",
                    "factor": "REUSE",
                    "index": "REUSE",
                    "suspend": "REUSE",
                    "benchmark": "REUSE",
                    "stock_pools": "REUSE",
                    "sector_context": "REUSE",
                },
            }
        elif stage == "BUILD":
            scope["dataset_manifest_ref"] = ref
        elif stage == "DERIVE":
            scope.update(
                {
                    "derived_assets": [ref],
                    "derived_asset_registry_sha256": HEX,
                    "source_dataset_manifest_sha256": self.manifest,
                    "hmm_fit_count": 0,
                    "training_started": False,
                }
            )
        elif stage == "LOCAL_VALIDATE":
            closure = {
                "schema_version": RELEASE_CLOSURE_SCHEMA,
                "dataset_manifest_ref": ref,
                "derived_asset_refs": [ref],
                "consumer_contract_refs": [ref],
                "source_readiness_refs": [ref],
                "component_validation_refs": [ref],
                "lineage_ref": ref,
            }
            closure["canonical_sha256"] = hashlib.sha256(
                canonical_json_bytes(closure)
            ).hexdigest()
            scope.update(
                {
                    "pool_gap_counts": {
                        name: 0
                        for name in (
                            "stock_universe",
                            "csi300",
                            "csi500",
                            "csi1000",
                            "star50",
                            "star100",
                        )
                    },
                    "dataset_identity_complete": True,
                    "release_closure": closure,
                    "release_closure_ref": ref,
                }
            )
        elif stage == "DEPLOY":
            scope.update(
                {
                    "nodes": list(REQUIRED_NODES),
                    "node_manifest_sha256": {
                        node: self.manifest for node in REQUIRED_NODES
                    },
                    "node_registration_refs": {node: ref for node in REQUIRED_NODES},
                }
            )
        elif stage == "CONSUMER_VALIDATE":
            readback = {
                "schema_version": CONSUMER_READBACK_SCHEMA,
                "dataset_manifest_sha256": self.manifest,
            }
            scope.update(
                {
                    "consumers": list(REQUIRED_CONSUMERS),
                    "consumer_readbacks": {
                        name: {**readback, "consumer_id": name}
                        for name in REQUIRED_CONSUMERS
                    },
                    "consumer_readback_refs": {
                        name: ref for name in REQUIRED_CONSUMERS
                    },
                }
            )
            if self.corrupt_profile_at_close:
                Path(str(plan["profile_candidate"])).write_text("{}\n", encoding="utf-8")
        return build_stage_receipt(
            operation_id=operation_id,
            attempt_id=f"{operation_id}:{attempt}",
            stage=stage,
            producer_id=f"test.{stage.lower()}",
            producer_version="1",
            scope=scope,
            input_refs=[],
            output_refs=output_refs,
            counts={field: 0 for field in TELEMETRY_COUNT_FIELDS},
            started_at="2026-09-21T00:00:00+00:00",
            finished_at="2026-09-21T00:00:01+00:00",
        )


def _service(tmp_path: Path, pipeline: Pipeline) -> MonthlyReleaseService:
    active = tmp_path / "active.json"
    _active(active)
    store = MonthlyOperationStore((tmp_path / "state").absolute())
    return MonthlyReleaseService(
        store,
        active_profile=active,
        pipeline=pipeline,
        candidate_root_factory=lambda request: (
            tmp_path / f"candidate-{request.target_cutoff:%Y%m%d}"
        ).absolute(),
        profile_candidate_factory=lambda request: (
            tmp_path / f"profile-{request.target_cutoff:%Y%m%d}.json"
        ).absolute(),
        node_root_factory=lambda request: {
            "wsl2-5080": f"/data/{request.target_cutoff:%Y%m%d}",
            "rdagent-node1": f"/node/{request.target_cutoff:%Y%m%d}",
        },
        generation_factory=lambda request: f"{request.target_cutoff:%Y%m%d}-v2",
        revision_factory=lambda request: f"{request.target_cutoff:%Y%m%d}-unified",
        allowed_cutoff_resolver=lambda: date(2026, 9, 30),
    )


def _request(*, key: str = "monthly-202609") -> MonthlyReleaseRequest:
    return MonthlyReleaseRequest(
        target_cutoff=date(2026, 9, 30),
        product_profile="qe_hmm_full_v2",
        idempotency_key=key,
    )


def _authorization(
    path: Path,
    *,
    authorization_id: str,
    operation_id: str,
    action: str,
    principal: str,
    cutoff: str,
    predecessor: str,
    target: str,
) -> None:
    value = {
        "schema_version": AUTHORIZATION_SCHEMA,
        "authorization_id": authorization_id,
        "operation_id": operation_id,
        "action": action,
        "principal": principal,
        "target_cutoff": cutoff,
        "predecessor_profile_sha256": predecessor,
        "target_profile_sha256": target,
        "status": "APPROVED",
        "approved_at": "2026-09-21T00:00:00+00:00",
    }
    value["canonical_sha256"] = hashlib.sha256(canonical_json_bytes(value)).hexdigest()
    _canonical(path, value)


def test_request_is_idempotent_even_after_active_profile_advances(tmp_path: Path) -> None:
    service = _service(tmp_path, Pipeline())
    first = service.submit(_request())
    _active(service.active_profile, cutoff="2026-09-30")
    second = service.submit(_request())
    assert second["operation_id"] == first["operation_id"]


def test_same_idempotency_key_rejects_different_request(tmp_path: Path) -> None:
    service = _service(tmp_path, Pipeline())
    service.submit(_request())
    changed = MonthlyReleaseRequest(
        target_cutoff=date(2026, 10, 30),
        product_profile="qe_hmm_full_v2",
        idempotency_key="monthly-202609",
    )
    with pytest.raises(MonthlyReleaseConflict, match="idempotency"):
        service.submit(changed)


def test_unclosed_or_nonofficial_month_cutoff_is_rejected_before_state_write(tmp_path: Path) -> None:
    service = _service(tmp_path, Pipeline())
    service.allowed_cutoff_resolver = lambda: date(2026, 9, 29)
    with pytest.raises(MonthlyReleaseRequestInvalid, match="official last trading day"):
        service.submit(_request())
    assert not service.store.operations.exists()


def test_one_product_has_one_operation_and_one_target_cutoff(tmp_path: Path) -> None:
    service = _service(tmp_path, Pipeline())
    first = service.submit(_request())
    with pytest.raises(MonthlyReleaseConflict, match="target cutoff"):
        service.submit(_request(key="different-key"))
    service.allowed_cutoff_resolver = lambda: date(2026, 10, 30)
    with pytest.raises(MonthlyReleaseConflict, match="another monthly operation"):
        service.submit(
            MonthlyReleaseRequest(
                target_cutoff=date(2026, 10, 30),
                product_profile="qe_hmm_full_v2",
                idempotency_key="october-key",
            )
        )
    assert service.status(first["operation_id"])["status"] == "PLANNED"


def test_resume_reuses_completed_stage_checkpoints(tmp_path: Path) -> None:
    pipeline = Pipeline(fail_once_at="LOCAL_VALIDATE")
    service = _service(tmp_path, pipeline)
    state = service.submit(_request())
    operation_id = state["operation_id"]
    assert service.run(operation_id)["status"] == "FAILED"
    assert pipeline.calls == ["SOURCE", "BUILD", "DERIVE", "LOCAL_VALIDATE"]
    service.resume(operation_id)
    assert service.run(operation_id)["status"] == "READY_TO_ACTIVATE"
    assert pipeline.calls.count("SOURCE") == 1
    assert pipeline.calls.count("BUILD") == 1
    assert pipeline.calls.count("DERIVE") == 1
    assert pipeline.calls.count("LOCAL_VALIDATE") == 2


def test_cancel_is_observed_at_stage_boundary(tmp_path: Path) -> None:
    service = _service(tmp_path, Pipeline())
    operation_id = service.submit(_request())["operation_id"]
    service.cancel(operation_id)
    assert service.run(operation_id)["status"] == "CANCELLED"


def test_activation_requires_exact_authorization_and_supports_safe_rollback(tmp_path: Path) -> None:
    service = _service(tmp_path, Pipeline())
    operation_id = service.submit(_request())["operation_id"]
    assert service.run(operation_id)["status"] == "READY_TO_ACTIVATE"
    auth_root = (tmp_path / "authorizations").absolute()
    auth_root.mkdir()
    auth_store = ActionAuthorizationStore(auth_root)
    with pytest.raises(MonthlyReleaseAuthorizationError):
        service.activate(
            operation_id,
            authorization_store=auth_store,
            authorization_ref="anything",
            principal="operator",
            verify_after=lambda ready: {},
        )

    plan = service.store.read_plan(operation_id)
    candidate_profile = Path(plan["profile_candidate"])
    activate_id = "dsauth_" + "1" * 32
    _authorization(
        auth_root / f"{activate_id}.json",
        authorization_id=activate_id,
        operation_id=operation_id,
        action="ACTIVATE",
        principal="operator",
        cutoff=plan["target_cutoff"],
        predecessor=plan["predecessor"]["profile_sha256"],
        target=_sha(candidate_profile),
    )
    activated = service.activate(
        operation_id,
        authorization_store=auth_store,
        authorization_ref=activate_id,
        principal="operator",
        verify_after=lambda ready: {
            "status": "PASS",
            "dataset_manifest_sha256": ready["dataset_manifest_sha256"],
        },
    )
    assert activated["status"] == "ACTIVATED_VERIFIED"

    rollback_id = "dsauth_" + "2" * 32
    previous = service.store.operation_root(operation_id) / "receipts" / "previous-profile.json"
    _authorization(
        auth_root / f"{rollback_id}.json",
        authorization_id=rollback_id,
        operation_id=operation_id,
        action="ROLLBACK",
        principal="operator",
        cutoff=plan["target_cutoff"],
        predecessor=_sha(service.active_profile),
        target=_sha(previous),
    )
    receipt = service.rollback(
        operation_id,
        authorization_store=auth_store,
        authorization_ref=rollback_id,
        principal="operator",
        verify_after=lambda predecessor: {"profile_sha256": _sha(service.active_profile)},
    )
    assert receipt["referenced_releases_deleted"] is False
    assert _sha(service.active_profile) == plan["predecessor"]["profile_sha256"]


def test_activation_rejects_profile_mutation_after_ready(tmp_path: Path) -> None:
    service = _service(tmp_path, Pipeline())
    operation_id = service.submit(_request())["operation_id"]
    assert service.run(operation_id)["status"] == "READY_TO_ACTIVATE"
    plan = service.store.read_plan(operation_id)
    Path(plan["profile_candidate"]).write_text("{}\n", encoding="utf-8")
    auth_root = (tmp_path / "authorizations").absolute()
    auth_root.mkdir()
    with pytest.raises(MonthlyReleaseConflict, match="changed after the READY"):
        service.activate(
            operation_id,
            authorization_store=ActionAuthorizationStore(auth_root),
            authorization_ref="dsauth_" + "1" * 32,
            principal="operator",
            verify_after=lambda ready: ready,
        )


def test_activation_readback_failure_retries_without_second_profile_switch(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path, Pipeline())
    operation_id = service.submit(_request())["operation_id"]
    assert service.run(operation_id)["status"] == "READY_TO_ACTIVATE"
    plan = service.store.read_plan(operation_id)
    auth_root = (tmp_path / "authorizations").absolute()
    auth_root.mkdir()
    auth_store = ActionAuthorizationStore(auth_root)
    authorization_id = "dsauth_" + "6" * 32
    candidate_sha = _sha(Path(plan["profile_candidate"]))
    _authorization(
        auth_root / f"{authorization_id}.json",
        authorization_id=authorization_id,
        operation_id=operation_id,
        action="ACTIVATE",
        principal="operator",
        cutoff=plan["target_cutoff"],
        predecessor=plan["predecessor"]["profile_sha256"],
        target=candidate_sha,
    )

    failed = service.activate(
        operation_id,
        authorization_store=auth_store,
        authorization_ref=authorization_id,
        principal="operator",
        verify_after=lambda _ready: {"status": "FAIL"},
    )
    assert failed["status"] == "ACTIVATED_VERIFY_FAILED"
    assert _sha(service.active_profile) == candidate_sha

    recovered = service.activate(
        operation_id,
        authorization_store=auth_store,
        authorization_ref=authorization_id,
        principal="operator",
        verify_after=lambda ready: {
            "status": "PASS",
            "dataset_manifest_sha256": ready["dataset_manifest_sha256"],
        },
    )
    activation = json.loads(
        (
            service.store.operation_root(operation_id)
            / "receipts"
            / "activation.json"
        ).read_text(encoding="utf-8")
    )
    assert recovered["status"] == "ACTIVATED_VERIFIED"
    assert activation["apply_state"] == "ALREADY_APPLIED"
    assert activation["readback_state"] == "PASS"
    assert _sha(service.active_profile) == candidate_sha


def test_rollback_readback_failure_is_durable_and_retryable(tmp_path: Path) -> None:
    service = _service(tmp_path, Pipeline())
    operation_id = service.submit(_request())["operation_id"]
    assert service.run(operation_id)["status"] == "READY_TO_ACTIVATE"
    plan = service.store.read_plan(operation_id)
    auth_root = (tmp_path / "authorizations").absolute()
    auth_root.mkdir()
    auth_store = ActionAuthorizationStore(auth_root)
    activate_id = "dsauth_" + "3" * 32
    _authorization(
        auth_root / f"{activate_id}.json",
        authorization_id=activate_id,
        operation_id=operation_id,
        action="ACTIVATE",
        principal="operator",
        cutoff=plan["target_cutoff"],
        predecessor=plan["predecessor"]["profile_sha256"],
        target=_sha(Path(plan["profile_candidate"])),
    )
    service.activate(
        operation_id,
        authorization_store=auth_store,
        authorization_ref=activate_id,
        principal="operator",
        verify_after=lambda ready: {
            "status": "PASS",
            "dataset_manifest_sha256": ready["dataset_manifest_sha256"],
        },
    )
    previous = service.store.operation_root(operation_id) / "receipts" / "previous-profile.json"
    rollback_id = "dsauth_" + "4" * 32
    _authorization(
        auth_root / f"{rollback_id}.json",
        authorization_id=rollback_id,
        operation_id=operation_id,
        action="ROLLBACK",
        principal="operator",
        cutoff=plan["target_cutoff"],
        predecessor=_sha(service.active_profile),
        target=_sha(previous),
    )
    failed = service.rollback(
        operation_id,
        authorization_store=auth_store,
        authorization_ref=rollback_id,
        principal="operator",
        verify_after=lambda _predecessor: {"profile_sha256": "0" * 64},
    )
    assert failed["status"] == "ROLLBACK_VERIFY_FAILED"
    assert _sha(service.active_profile) == _sha(previous)

    receipt = service.rollback(
        operation_id,
        authorization_store=auth_store,
        authorization_ref=rollback_id,
        principal="operator",
        verify_after=lambda _predecessor: {"profile_sha256": _sha(service.active_profile)},
    )
    assert receipt["apply_state"] == "ALREADY_APPLIED"
    assert receipt["readback_state"] == "PASS"
    assert service.status(operation_id)["status"] == "ROLLBACK_VERIFIED"


def test_worker_recovers_ready_auto_activation_after_stage_process_exit(tmp_path: Path) -> None:
    service = _service(tmp_path, Pipeline())
    authorization_id = "dsauth_" + "5" * 32
    request = MonthlyReleaseRequest(
        target_cutoff=date(2026, 9, 30),
        product_profile="qe_hmm_full_v2",
        idempotency_key="monthly-auto-202609",
        activation_mode="activate_when_ready",
        activation_authorization_ref=authorization_id,
    )
    operation_id = service.submit(request, principal="dataset-operator:test")["operation_id"]
    assert service.run(operation_id)["status"] == "READY_TO_ACTIVATE"
    assert operation_id in service.store.pending_operation_ids()
    plan = service.store.read_plan(operation_id)
    auth_root = (tmp_path / "authorizations").absolute()
    auth_root.mkdir()
    _authorization(
        auth_root / f"{authorization_id}.json",
        authorization_id=authorization_id,
        operation_id=operation_id,
        action="ACTIVATE",
        principal="dataset-operator:test",
        cutoff=plan["target_cutoff"],
        predecessor=plan["predecessor"]["profile_sha256"],
        target=_sha(Path(plan["profile_candidate"])),
    )
    result = MonthlyReleaseWorker(
        service,
        authorization_store=ActionAuthorizationStore(auth_root),
        activation_verifier=lambda ready: {
            "status": "PASS",
            "dataset_manifest_sha256": ready["dataset_manifest_sha256"],
        },
    ).run_once()
    assert result is not None
    assert result["status"] == "ACTIVATED_VERIFIED"


def test_complete_run_has_all_six_durable_stage_receipts(tmp_path: Path) -> None:
    service = _service(tmp_path, Pipeline())
    operation_id = service.submit(_request())["operation_id"]
    assert service.run(operation_id)["status"] == "READY_TO_ACTIVATE"
    assert all(service.store.read_checkpoint(operation_id, stage) for stage in STAGES)

    ready = json.loads(
        (
            service.store.operation_root(operation_id) / "receipts" / "ready.json"
        ).read_text(encoding="utf-8")
    )
    assert set(ready["telemetry"]) == set(STAGES)
    assert ready["database_write_performed"] is False
    assert ready["production_ddl_performed"] is False
    assert ready["production_dml_performed"] is False
    assert ready["candidate_write_performed"] is True
    assert ready["candidate_deployed"] is True
    assert ready["training_or_experiment"] is False
    assert ready["runtime_action_performed"] is False
    assert ready["active_profile_write"] is False


def test_checkpoint_binds_explicit_registered_producer_digest(tmp_path: Path) -> None:
    pipeline = Pipeline()
    service = _service(tmp_path, pipeline)
    operation_id = service.submit(_request())["operation_id"]
    request = service.store.read_request(operation_id)
    plan = service.store.read_plan(operation_id)
    producer_digest = "c" * 64
    receipt = pipeline.run_stage(
        stage="SOURCE",
        operation_id=operation_id,
        attempt=1,
        request=request,
        plan=plan,
        prior_receipts={},
    )
    service.store.write_checkpoint(
        operation_id,
        stage="SOURCE",
        attempt=1,
        receipt=receipt,
        request_digest=request["semantic_digest"],
        plan_digest="d" * 64,
        input_set_digest="e" * 64,
        producer_digest=producer_digest,
    )
    assert service.store.read_checkpoint(
        operation_id,
        "SOURCE",
        producer_digest=producer_digest,
    ) is not None
    assert service.store.read_checkpoint(
        operation_id,
        "SOURCE",
        producer_digest="f" * 64,
    ) is None


def test_release_closure_failure_is_durable_instead_of_crashing_worker(tmp_path: Path) -> None:
    service = _service(tmp_path, Pipeline(corrupt_profile_at_close=True))
    operation_id = service.submit(_request())["operation_id"]
    state = service.run(operation_id)
    assert state["status"] == "FAILED"
    assert state["current_stage"] == "RELEASE_CLOSURE"
    assert state["last_error"]["code"] == "MONTHLY_RELEASE_NOT_READY"
