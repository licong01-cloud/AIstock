from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonical_json_text,
)
from backend.services.advisory_phase0b.errors import (
    Phase0BAuditError,
    REASON_SNAPSHOT_CHANGED,
    REASON_WINNER_REGISTRY_CONFLICT,
)
from backend.services.advisory_phase0b.audit_service import Phase0BMetricEngine
from backend.services.advisory_phase0b.service import Phase0BCandidateQualityAuditService
from backend.services.advisory_phase0b.snapshot_reader import (
    Phase0BSnapshotReadResultV1,
    Phase0BTargetProgramBindingV1,
)
from backend.tests.advisory_phase0b.test_audit_service import (
    _full_request,
    _populate_metric_spool,
)
from backend.tests.advisory_phase0b.test_snapshot_reader import _entry, _receipt
from backend.tests.advisory_phase0b.test_spool import _roots


class _Reader:
    def __init__(self) -> None:
        self.read_count = 0
        self.confirm_count = 0

    def read_into_spool(self, *, request: Any, spool: Any) -> Phase0BSnapshotReadResultV1:
        self.read_count += 1
        _populate_metric_spool(spool)
        target = request.audit_targets[0]
        return Phase0BSnapshotReadResultV1(
            first_catalog_receipt=_receipt(),
            target_program_bindings=(
                Phase0BTargetProgramBindingV1(
                    target_hash=str(target.target_hash),
                    range_program_hash="9" * 64,
                ),
            ),
        )

    def confirm_unchanged(self, *, request: Any, first_receipt: Any) -> Any:
        self.confirm_count += 1
        return first_receipt


class _ChangingReader(_Reader):
    def confirm_unchanged(self, *, request: Any, first_receipt: Any) -> Any:
        self.confirm_count += 1
        raise Phase0BAuditError(
            REASON_SNAPSHOT_CHANGED,
            "injected final catalog change",
        )


def _two_target_request() -> Any:
    request = _full_request()
    first_target = request.audit_targets[0]
    second_payload = first_target.model_dump(mode="python")
    second_payload.update(
        {
            "snapshot_id": "snapshot-2",
            "program_id": "program-2",
            "package_id": "package-2",
            "manifest_sha256": "b" * 64,
            "target_hash": None,
        }
    )
    second_target = type(first_target).model_validate(second_payload)
    targets = (first_target, second_target)
    registry_payload = request.multiple_testing_registry.model_dump(mode="python")
    first_style = request.multiple_testing_registry.style_hypothesis_by_target[0]
    second_style_payload = first_style.model_dump(mode="python")
    second_style_payload["target_hash"] = second_target.target_hash
    first_runtime = request.multiple_testing_registry.manifest_runtime_variant_by_target[0]
    second_runtime_payload = first_runtime.model_dump(mode="python")
    second_runtime_payload.update(
        {
            "target_hash": second_target.target_hash,
            "manifest_sha256": second_target.manifest_sha256,
            "runtime_variant_id": "runtime-variant-2",
        }
    )
    registry_payload.update(
        {
            "audit_target_identity_set_hash": canonical_json_sha256(
                tuple(sorted(str(item.target_hash) for item in targets))
            ),
            "style_hypothesis_by_target": (
                first_style,
                type(first_style).model_validate(second_style_payload),
            ),
            "manifest_runtime_variant_by_target": (
                first_runtime,
                type(first_runtime).model_validate(second_runtime_payload),
            ),
            "registry_hash": None,
        }
    )
    registry = type(request.multiple_testing_registry).model_validate(registry_payload)
    request_payload = request.model_dump(mode="python")
    request_payload.update(
        {
            "snapshot_ids": ("snapshot-1", "snapshot-2"),
            "audit_targets": targets,
            "multiple_testing_registry": registry,
            "multiple_testing_registry_hash": registry.registry_hash,
            "request_hash": None,
        }
    )
    return type(request).model_validate(request_payload)


def _two_snapshot_receipt() -> Any:
    receipt = _receipt()
    first_entry = receipt.entries[0]
    second_header = first_entry.header_payload()
    second_header["snapshot_id"] = "snapshot-2"
    second_payload = first_entry.model_dump(mode="python")
    second_payload.update(
        {
            "snapshot_id": "snapshot-2",
            "header_payload_json": canonical_json_text(second_header),
            "header_hash": canonical_json_sha256(second_header),
            "catalog_content_hash": None,
        }
    )
    second_entry = type(first_entry).model_validate(second_payload)
    receipt_payload = receipt.model_dump(mode="python")
    receipt_payload.update(
        {
            "entries": (first_entry, second_entry),
            "catalog_content_set_hash": None,
            "receipt_hash": None,
        }
    )
    return type(receipt).model_validate(receipt_payload)


class _TwoSnapshotReader(_Reader):
    def read_into_spool(self, *, request: Any, spool: Any) -> Phase0BSnapshotReadResultV1:
        self.read_count += 1
        _populate_metric_spool(spool)
        _populate_metric_spool(
            spool,
            snapshot_id="snapshot-2",
            package_id="package-2",
            manifest_sha256="b" * 64,
            range_program_hash="8" * 64,
        )
        bindings = tuple(
            Phase0BTargetProgramBindingV1(
                target_hash=str(target.target_hash),
                range_program_hash="9" * 64
                if target.snapshot_id == "snapshot-1"
                else "8" * 64,
            )
            for target in request.audit_targets
        )
        return Phase0BSnapshotReadResultV1(
            first_catalog_receipt=_two_snapshot_receipt(),
            target_program_bindings=bindings,
        )


class _FailSecondTargetMetricEngine(Phase0BMetricEngine):
    def __init__(self) -> None:
        self.target_count = 0

    def evaluate_target(self, **kwargs: Any) -> Any:
        self.target_count += 1
        if self.target_count == 2:
            raise Phase0BAuditError(
                "ADVISORY_PHASE0B_TEST_SECOND_TARGET_FAILURE",
                "injected second target failure",
            )
        return super().evaluate_target(**kwargs)


def test_service_runs_one_atomic_bundle_and_cleans_exact_spool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root, dataset_root, output_root = _roots(tmp_path)
    request = _full_request()
    reader = _Reader()
    monkeypatch.setattr(
        "backend.services.advisory_phase0b.service.phase0b_producer_code_closure_hash",
        lambda **_kwargs: request.producer_code_closure_hash,
    )

    receipt = Phase0BCandidateQualityAuditService(snapshot_reader=reader).run(  # type: ignore[arg-type]
        request=request,
        repository_root=repository_root,
        dataset_root=dataset_root,
        output_root=output_root,
        source_git_commit="abc123",
        source_state="dirty",
    )

    assert reader.read_count == reader.confirm_count == 1
    assert receipt.request_hash == request.request_hash
    assert (output_root / f"phase0b_report_{receipt.report_semantic_hash}" / "report_receipt.json").is_file()
    assert not (output_root / ".phase0b-tmp").exists()


def test_service_rejects_producer_drift_before_snapshot_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root, dataset_root, output_root = _roots(tmp_path)
    request = _full_request()
    reader = _Reader()
    monkeypatch.setattr(
        "backend.services.advisory_phase0b.service.phase0b_producer_code_closure_hash",
        lambda **_kwargs: "0" * 64,
    )

    with pytest.raises(Phase0BAuditError) as captured:
        Phase0BCandidateQualityAuditService(snapshot_reader=reader).run(  # type: ignore[arg-type]
            request=request,
            repository_root=repository_root,
            dataset_root=dataset_root,
            output_root=output_root,
            source_git_commit="abc123",
            source_state="clean",
        )

    assert captured.value.reason_code == REASON_WINNER_REGISTRY_CONFLICT
    assert reader.read_count == 0
    assert not any(output_root.iterdir())


def test_snapshot_authority_uses_lineage_specific_capability_identity() -> None:
    entry = _entry()
    header = entry.header_payload()
    header.update(
        {
            "lineage_identity_type": "PHASE0A",
            "maturity_coverage_hash": "8" * 64,
            "policy_compatibility_hash": "7" * 64,
        }
    )
    payload = entry.model_dump(mode="python")
    payload.update(
        {
            "lineage_identity_type": "PHASE0A",
            "header_payload_json": canonical_json_text(header),
            "header_hash": canonical_json_sha256(header),
            "catalog_content_hash": None,
        }
    )
    formal_entry = type(entry).model_validate(payload)

    authority = Phase0BCandidateQualityAuditService._snapshot_authority(formal_entry)

    assert authority.capability_identity_hash == "7" * 64


def test_service_final_catalog_change_publishes_no_report_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root, dataset_root, output_root = _roots(tmp_path)
    request = _full_request()
    reader = _ChangingReader()
    monkeypatch.setattr(
        "backend.services.advisory_phase0b.service.phase0b_producer_code_closure_hash",
        lambda **_kwargs: request.producer_code_closure_hash,
    )

    with pytest.raises(Phase0BAuditError) as captured:
        Phase0BCandidateQualityAuditService(snapshot_reader=reader).run(  # type: ignore[arg-type]
            request=request,
            repository_root=repository_root,
            dataset_root=dataset_root,
            output_root=output_root,
            source_git_commit="abc123",
            source_state="clean",
        )

    assert captured.value.reason_code == REASON_SNAPSHOT_CHANGED
    assert reader.read_count == reader.confirm_count == 1
    assert not tuple(output_root.glob("phase0b_report_*"))
    assert not (output_root / ".phase0b-tmp").exists()


def test_service_two_snapshot_request_is_all_or_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root, dataset_root, output_root = _roots(tmp_path)
    request = _two_target_request()
    reader = _TwoSnapshotReader()
    metric_engine = _FailSecondTargetMetricEngine()
    monkeypatch.setattr(
        "backend.services.advisory_phase0b.service.phase0b_producer_code_closure_hash",
        lambda **_kwargs: request.producer_code_closure_hash,
    )

    with pytest.raises(Phase0BAuditError, match="injected second target failure"):
        Phase0BCandidateQualityAuditService(
            snapshot_reader=reader,  # type: ignore[arg-type]
            metric_engine=metric_engine,
        ).run(
            request=request,
            repository_root=repository_root,
            dataset_root=dataset_root,
            output_root=output_root,
            source_git_commit="abc123",
            source_state="clean",
        )

    assert metric_engine.target_count == 2
    assert reader.read_count == 1
    assert reader.confirm_count == 0
    assert not tuple(output_root.glob("phase0b_report_*"))
    assert not (output_root / ".phase0b-tmp").exists()
