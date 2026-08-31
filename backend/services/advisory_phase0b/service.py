from __future__ import annotations

from pathlib import Path

from backend.services.advisory_historical_range.canonical import canonical_json_sha256

from .audit_service import Phase0BMetricEngine
from .contracts import Phase0BCandidateQualityAuditRequestV1
from .errors import (
    Phase0BAuditError,
    REASON_MANIFEST_CONFLICT,
    REASON_TARGET_SET_CONFLICT,
    REASON_WINNER_REGISTRY_CONFLICT,
)
from .producer_closure import phase0b_producer_code_closure_hash
from .report_store import (
    Phase0BAuditReportV1,
    Phase0BReportReceiptV1,
    Phase0BReportStore,
    Phase0BSnapshotReportAuthorityV1,
)
from .snapshot_reader import Phase0BSnapshotCatalogEntryV1, Phase0BSnapshotReader
from .spool import Phase0BBoundedSpool


class Phase0BCandidateQualityAuditService:
    """Run one isolated, read-only, all-or-nothing Phase 0B audit request."""

    def __init__(
        self,
        *,
        snapshot_reader: Phase0BSnapshotReader,
        metric_engine: Phase0BMetricEngine | None = None,
    ) -> None:
        self._snapshot_reader = snapshot_reader
        self._metric_engine = metric_engine or Phase0BMetricEngine()

    def run(
        self,
        *,
        request: Phase0BCandidateQualityAuditRequestV1,
        repository_root: Path,
        dataset_root: Path,
        output_root: Path,
        source_git_commit: str,
        source_state: str,
    ) -> Phase0BReportReceiptV1:
        actual_producer_hash = phase0b_producer_code_closure_hash(
            repository_root=repository_root,
        )
        if request.producer_code_closure_hash != actual_producer_hash:
            raise Phase0BAuditError(
                REASON_WINNER_REGISTRY_CONFLICT,
                "request producer code closure differs from current source",
                context={
                    "expected": request.producer_code_closure_hash,
                    "actual": actual_producer_hash,
                },
            )
        operation_id = f"phase0b-{request.request_hash}"
        with Phase0BBoundedSpool(
            output_root=output_root,
            repository_root=repository_root,
            dataset_root=dataset_root,
            operation_id=operation_id,
        ) as spool:
            read_result = self._snapshot_reader.read_into_spool(request=request, spool=spool)
            binding_by_target = {
                item.target_hash: item for item in read_result.target_program_bindings
            }
            target_hashes = {str(item.target_hash) for item in request.audit_targets}
            if set(binding_by_target) != target_hashes:
                raise Phase0BAuditError(
                    REASON_TARGET_SET_CONFLICT,
                    "snapshot target Program bindings do not exactly cover the audit request",
                    context={
                        "missing": tuple(sorted(target_hashes - set(binding_by_target))),
                        "extra": tuple(sorted(set(binding_by_target) - target_hashes)),
                    },
                )
            target_reports = tuple(
                self._metric_engine.evaluate_target(
                    request=request,
                    target=target,
                    program_binding=binding_by_target[str(target.target_hash)],
                    spool=spool,
                )
                for target in request.audit_targets
            )
            first_receipt = read_result.first_catalog_receipt
            snapshot_content_set_hash = self._snapshot_content_set_hash(first_receipt.entries)
            report_semantic_hash = canonical_json_sha256(
                {
                    "request_hash": request.request_hash,
                    "snapshot_content_set_hash": snapshot_content_set_hash,
                }
            )
            report = Phase0BAuditReportV1(
                request_hash=str(request.request_hash),
                producer_code_closure_hash=request.producer_code_closure_hash,
                metric_registry_hash=request.metric_registry_hash,
                multiple_testing_registry_hash=request.multiple_testing_registry_hash,
                snapshot_content_set_hash=snapshot_content_set_hash,
                catalog_content_set_hash=str(first_receipt.catalog_content_set_hash),
                snapshot_authorities=tuple(
                    self._snapshot_authority(item) for item in first_receipt.entries
                ),
                report_semantic_hash=report_semantic_hash,
                target_reports=target_reports,
            )
            final_receipt = self._snapshot_reader.confirm_unchanged(
                request=request,
                first_receipt=first_receipt,
            )
            return Phase0BReportStore(output_root=output_root).publish(
                report=report,
                final_catalog_receipt=final_receipt,
                source_git_commit=source_git_commit,
                source_state=source_state,
            )

    @staticmethod
    def _snapshot_content_set_hash(
        entries: tuple[Phase0BSnapshotCatalogEntryV1, ...],
    ) -> str:
        values: list[tuple[str, str, str, str]] = []
        for entry in entries:
            header = entry.header_payload()
            values.append(
                (
                    entry.snapshot_id,
                    str(header["snapshot_content_hash"]),
                    str(header["manifest_sha256"]),
                    entry.file_set_hash,
                )
            )
        return canonical_json_sha256(tuple(sorted(values)))

    @staticmethod
    def _snapshot_authority(
        entry: Phase0BSnapshotCatalogEntryV1,
    ) -> Phase0BSnapshotReportAuthorityV1:
        header = entry.header_payload()
        capability_field = (
            "maturity_coverage_hash"
            if entry.lineage_identity_type == "HISTORICAL_RANGE"
            else "policy_compatibility_hash"
        )
        capability_hash = header.get(capability_field)
        if capability_hash is None:
            raise Phase0BAuditError(
                REASON_MANIFEST_CONFLICT,
                "snapshot header lacks a frozen capability identity",
                context={
                    "snapshot_id": entry.snapshot_id,
                    "required_field": capability_field,
                },
            )
        return Phase0BSnapshotReportAuthorityV1(
            snapshot_id=entry.snapshot_id,
            lineage_identity_type=entry.lineage_identity_type,
            catalog_content_hash=str(entry.catalog_content_hash),
            snapshot_content_hash=str(header["snapshot_content_hash"]),
            manifest_sha256=str(header["manifest_sha256"]),
            file_set_hash=entry.file_set_hash,
            snapshot_source_revision_set_hash=str(
                header["snapshot_source_revision_set_hash"]
            ),
            schema_fingerprint=str(header["schema_fingerprint"]),
            capability_identity_hash=str(capability_hash),
        )
