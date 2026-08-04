from __future__ import annotations

import hashlib
import json
import os
import uuid
import ctypes
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonical_json_text,
)
from backend.services.advisory_historical_range.models import require_sha256

from .contracts import MetricStatus, OUTPUT_SCHEMA_VERSION, _FrozenModel
from .errors import Phase0BAuditError, REASON_REPORT_BUNDLE_CONFLICT
from .snapshot_reader import Phase0BSnapshotCatalogReceiptV1


REPORT_RECEIPT_SCHEMA_VERSION = "advisory_phase0b_report_receipt_v1"


class Phase0BMetricResultV1(_FrozenModel):
    metric_definition_id: str = Field(min_length=1, max_length=160)
    metric_definition_hash: str = Field(min_length=64, max_length=64)
    slice_id: str = Field(min_length=1, max_length=320)
    projection: str | None = Field(default=None, min_length=1, max_length=160)
    horizon_trading_days: int | None = Field(default=None, ge=1)
    stage: str | None = Field(default=None, min_length=1, max_length=160)
    depth: int | None = Field(default=None, ge=1)
    regime_definition_id: str | None = Field(default=None, min_length=1, max_length=160)
    regime_value: str | None = Field(default=None, min_length=1, max_length=160)
    status: MetricStatus
    reason_codes: tuple[str, ...]
    decision_date_count: int = Field(ge=0)
    evaluable_date_count: int = Field(ge=0)
    effective_sample_count: int = Field(ge=0)
    missing_decision_date_count: int = Field(ge=0)
    candidate_count: int = Field(ge=0)
    matured_label_count: int = Field(ge=0)
    unavailable_label_count: int = Field(ge=0)
    winner_event_count: int = Field(default=0, ge=0)
    regime_count: int = Field(default=0, ge=0)
    maturity_counts_json: str = "{}"
    observed_value: Decimal | None = None
    conclusion: str | None = None
    conclusion_scope: Literal["DESCRIPTIVE", "INFERENTIAL"] | None = None
    confidence_interval_lower: Decimal | None = None
    confidence_interval_upper: Decimal | None = None
    p_value: Decimal | None = None
    detail_json: str = "{}"
    result_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("metric_definition_hash", "result_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BMetricResultV1":
        detail = json.loads(self.detail_json)
        if not isinstance(detail, dict) or canonical_json_text(detail) != self.detail_json:
            raise ValueError("metric detail must be canonical JSON object text")
        maturity_counts = json.loads(self.maturity_counts_json)
        if (
            not isinstance(maturity_counts, dict)
            or canonical_json_text(maturity_counts) != self.maturity_counts_json
            or any(
                not isinstance(key, str) or not isinstance(value, int) or value < 0
                for key, value in maturity_counts.items()
            )
        ):
            raise ValueError("metric maturity counts must be a canonical non-negative JSON object")
        if self.evaluable_date_count > self.decision_date_count:
            raise ValueError("evaluable dates cannot exceed total decision dates")
        if self.missing_decision_date_count != self.decision_date_count - self.evaluable_date_count:
            raise ValueError("missing decision dates must close total and evaluable dates")
        if self.effective_sample_count != self.evaluable_date_count:
            raise ValueError("Phase 0B effective sample count is the evaluable decision-date count")
        if self.status is MetricStatus.AVAILABLE and self.observed_value is None:
            raise ValueError("AVAILABLE metric requires an observed value")
        if self.status is not MetricStatus.AVAILABLE and any(
            value is not None for value in (self.conclusion, self.conclusion_scope)
        ):
            raise ValueError("non-AVAILABLE metric cannot publish a conclusion")
        if (self.conclusion is None) != (self.conclusion_scope is None):
            raise ValueError("metric conclusion and conclusion scope are nullable together")
        if (self.regime_definition_id is None) != (self.regime_value is None):
            raise ValueError("metric regime definition and value are nullable together")
        if self.evaluable_date_count < 60 and any(
            value is not None
            for value in (
                self.conclusion,
                self.conclusion_scope,
                self.confidence_interval_lower,
                self.confidence_interval_upper,
                self.p_value,
            )
        ):
            raise ValueError("fewer than 60 dates cannot publish inferential fields")
        if self.evaluable_date_count < 252 and (
            self.p_value is not None or self.conclusion_scope == "INFERENTIAL"
        ):
            raise ValueError("fewer than 252 dates cannot publish inferential conclusions")
        reasons = tuple(sorted(set(self.reason_codes)))
        if len(reasons) != len(self.reason_codes) or any(not value.strip() for value in reasons):
            raise ValueError("metric reason codes must be sorted unique non-empty values")
        object.__setattr__(self, "reason_codes", reasons)
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"result_hash"}))
        if self.result_hash is not None and self.result_hash != digest:
            raise ValueError("metric result hash differs from canonical content")
        object.__setattr__(self, "result_hash", digest)
        return self


class Phase0BTargetAuditReportV1(_FrozenModel):
    target_hash: str = Field(min_length=64, max_length=64)
    snapshot_id: str = Field(min_length=1, max_length=160)
    program_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: Literal["single_alpha", "multi_alpha"]
    style_hypothesis: Literal["SHORT_REBOUND", "LONG_TREND", "UNCLASSIFIED"]
    decision_date_count: int = Field(ge=0)
    metric_results: tuple[Phase0BMetricResultV1, ...]
    package_conclusion: Literal[
        "RESEARCH_EVIDENCE_AVAILABLE",
        "RESEARCH_EVIDENCE_UNAVAILABLE",
    ] | None = None
    phase2_phase3_recommendations: tuple[str, ...]
    report_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("target_hash", "manifest_sha256", "report_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BTargetAuditReportV1":
        results = tuple(sorted(self.metric_results, key=lambda item: item.slice_id))
        if len({item.slice_id for item in results}) != len(results):
            raise ValueError("target report metric slices must be unique")
        if self.decision_date_count < 60 and self.package_conclusion is not None:
            raise ValueError("fewer than 60 dates cannot publish a package conclusion")
        recommendations = tuple(sorted(set(self.phase2_phase3_recommendations)))
        if len(recommendations) != len(self.phase2_phase3_recommendations):
            raise ValueError("Phase 2/3 recommendations must be unique")
        object.__setattr__(self, "metric_results", results)
        object.__setattr__(self, "phase2_phase3_recommendations", recommendations)
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"report_hash"}))
        if self.report_hash is not None and self.report_hash != digest:
            raise ValueError("target report hash differs from canonical content")
        object.__setattr__(self, "report_hash", digest)
        return self


class Phase0BSnapshotReportAuthorityV1(_FrozenModel):
    snapshot_id: str = Field(min_length=1, max_length=160)
    lineage_identity_type: Literal["PHASE0A", "HISTORICAL_RANGE"]
    catalog_content_hash: str = Field(min_length=64, max_length=64)
    snapshot_content_hash: str = Field(min_length=64, max_length=64)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    file_set_hash: str = Field(min_length=64, max_length=64)
    snapshot_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    schema_fingerprint: str = Field(min_length=64, max_length=64)
    capability_identity_hash: str = Field(min_length=64, max_length=64)
    authority_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "catalog_content_hash",
        "snapshot_content_hash",
        "manifest_sha256",
        "file_set_hash",
        "snapshot_source_revision_set_hash",
        "schema_fingerprint",
        "capability_identity_hash",
        "authority_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BSnapshotReportAuthorityV1":
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"authority_hash"}))
        if self.authority_hash is not None and self.authority_hash != digest:
            raise ValueError("snapshot report authority hash differs from canonical content")
        object.__setattr__(self, "authority_hash", digest)
        return self


class Phase0BAuditReportV1(_FrozenModel):
    schema_version: Literal[OUTPUT_SCHEMA_VERSION] = OUTPUT_SCHEMA_VERSION
    request_hash: str = Field(min_length=64, max_length=64)
    producer_code_closure_hash: str = Field(min_length=64, max_length=64)
    metric_registry_hash: str = Field(min_length=64, max_length=64)
    multiple_testing_registry_hash: str = Field(min_length=64, max_length=64)
    snapshot_content_set_hash: str = Field(min_length=64, max_length=64)
    catalog_content_set_hash: str = Field(min_length=64, max_length=64)
    snapshot_authorities: tuple[Phase0BSnapshotReportAuthorityV1, ...] = Field(min_length=1)
    report_semantic_hash: str = Field(min_length=64, max_length=64)
    research_scope: Literal["ACADEMIC_RESEARCH_AND_HISTORICAL_ANALYSIS_ONLY"] = (
        "ACADEMIC_RESEARCH_AND_HISTORICAL_ANALYSIS_ONLY"
    )
    execution_prohibited: Literal[True] = True
    disclaimer: Literal["Not investment advice; no trading execution is produced."] = (
        "Not investment advice; no trading execution is produced."
    )
    target_reports: tuple[Phase0BTargetAuditReportV1, ...]
    report_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "request_hash",
        "producer_code_closure_hash",
        "metric_registry_hash",
        "multiple_testing_registry_hash",
        "snapshot_content_set_hash",
        "catalog_content_set_hash",
        "report_semantic_hash",
        "report_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BAuditReportV1":
        authorities = tuple(sorted(self.snapshot_authorities, key=lambda item: item.snapshot_id))
        if len({item.snapshot_id for item in authorities}) != len(authorities):
            raise ValueError("snapshot report authorities must have unique snapshot ids")
        if not self.research_scope.strip() or not self.disclaimer.strip():
            raise ValueError("research scope and disclaimer must be explicit")
        reports = tuple(sorted(self.target_reports, key=lambda item: item.target_hash))
        if len({item.target_hash for item in reports}) != len(reports):
            raise ValueError("audit report targets must be unique")
        expected_semantic_hash = canonical_json_sha256(
            {
                "request_hash": self.request_hash,
                "snapshot_content_set_hash": self.snapshot_content_set_hash,
            }
        )
        if self.report_semantic_hash != expected_semantic_hash:
            raise ValueError("report semantic hash differs from request and snapshot content set")
        object.__setattr__(self, "target_reports", reports)
        object.__setattr__(self, "snapshot_authorities", authorities)
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"report_hash"}))
        if self.report_hash is not None and self.report_hash != digest:
            raise ValueError("audit report hash differs from canonical content")
        object.__setattr__(self, "report_hash", digest)
        return self


class Phase0BReportReceiptV1(_FrozenModel):
    schema_version: Literal[REPORT_RECEIPT_SCHEMA_VERSION] = REPORT_RECEIPT_SCHEMA_VERSION
    report_semantic_hash: str = Field(min_length=64, max_length=64)
    report_hash: str = Field(min_length=64, max_length=64)
    request_hash: str = Field(min_length=64, max_length=64)
    snapshot_content_set_hash: str = Field(min_length=64, max_length=64)
    producer_code_closure_hash: str = Field(min_length=64, max_length=64)
    report_json_sha256: str = Field(min_length=64, max_length=64)
    report_json_size_bytes: int = Field(gt=0)
    report_markdown_sha256: str = Field(min_length=64, max_length=64)
    report_markdown_size_bytes: int = Field(gt=0)
    catalog_receipt_hash: str = Field(min_length=64, max_length=64)
    database_target_receipt_hash: str = Field(min_length=64, max_length=64)
    catalog_valid_through: datetime
    source_git_commit: str = Field(min_length=1, max_length=160)
    source_state: str = Field(min_length=1, max_length=160)
    materialized_at: datetime
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "report_semantic_hash",
        "report_hash",
        "request_hash",
        "snapshot_content_set_hash",
        "producer_code_closure_hash",
        "report_json_sha256",
        "report_markdown_sha256",
        "catalog_receipt_hash",
        "database_target_receipt_hash",
        "receipt_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BReportReceiptV1":
        for field_name in ("catalog_valid_through", "materialized_at"):
            value = getattr(self, field_name)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")
            object.__setattr__(self, field_name, value.astimezone(UTC))
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"receipt_hash"}))
        if self.receipt_hash is not None and self.receipt_hash != digest:
            raise ValueError("report receipt hash differs from canonical content")
        object.__setattr__(self, "receipt_hash", digest)
        return self


def render_report_markdown(report: Phase0BAuditReportV1) -> str:
    lines = [
        "# Advisory Phase 0B Candidate Quality Audit",
        "",
        f"- Request: `{report.request_hash}`",
        f"- Snapshot content set: `{report.snapshot_content_set_hash}`",
        f"- Catalog content set: `{report.catalog_content_set_hash}`",
        f"- Research scope: `{report.research_scope}`",
        f"- Execution prohibited: `{str(report.execution_prohibited).lower()}`",
        f"- Disclaimer: {report.disclaimer}",
        "",
    ]
    for target in report.target_reports:
        lines.extend(
            [
                f"## {target.program_id} / {target.package_id}",
                "",
                f"- Snapshot: `{target.snapshot_id}`",
                f"- Alpha mode: `{target.alpha_mode}`",
                f"- Style hypothesis: `{target.style_hypothesis}`",
                f"- Decision dates: `{target.decision_date_count}`",
                f"- Research evidence status: `{target.package_conclusion or 'NOT_YET_AVAILABLE'}`",
                "",
                "| Metric | Status | Observed | Conclusion | Evaluable/Total Dates | Reason |",
                "|---|---:|---:|---|---:|---|",
            ]
        )
        for metric in target.metric_results:
            observed = "-" if metric.observed_value is None else str(metric.observed_value)
            lines.append(
                f"| `{metric.slice_id}` | {metric.status.value} | {observed} | "
                f"{metric.conclusion or '-'} | "
                f"{metric.evaluable_date_count}/{metric.decision_date_count} | "
                f"{', '.join(metric.reason_codes) or '-'} |"
            )
        lines.extend(("", "Phase 2/3 data requirements:"))
        lines.extend(f"- {item}" for item in target.phase2_phase3_recommendations)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


class Phase0BReportStore:
    def __init__(self, *, output_root: Path) -> None:
        if not output_root.is_absolute():
            raise ValueError("Phase 0B report output root must be absolute")
        self._output_root = output_root.resolve()

    def publish(
        self,
        *,
        report: Phase0BAuditReportV1,
        final_catalog_receipt: Phase0BSnapshotCatalogReceiptV1,
        source_git_commit: str,
        source_state: str,
    ) -> Phase0BReportReceiptV1:
        if final_catalog_receipt.catalog_content_set_hash != report.catalog_content_set_hash:
            raise Phase0BAuditError(
                REASON_REPORT_BUNDLE_CONFLICT,
                "final catalog content differs from the report authority",
            )
        bundle_root = (
            self._output_root / f"phase0b_report_{report.report_semantic_hash}"
        ).resolve()
        if self._output_root not in bundle_root.parents:
            raise Phase0BAuditError(
                REASON_REPORT_BUNDLE_CONFLICT,
                "report bundle path escapes output root",
            )
        report_json = (canonical_json_text(report.model_dump(mode="python")) + "\n").encode("utf-8")
        report_markdown = render_report_markdown(report).encode("utf-8")
        report_json_hash = hashlib.sha256(report_json).hexdigest()
        report_markdown_hash = hashlib.sha256(report_markdown).hexdigest()
        existing = self._existing_complete_receipt(
            bundle_root=bundle_root,
            report=report,
            report_json=report_json,
            report_markdown=report_markdown,
            database_target_receipt_hash=str(
                final_catalog_receipt.database_target.target_receipt_hash
            ),
        )
        if existing is not None:
            return existing
        receipt = Phase0BReportReceiptV1(
            report_semantic_hash=report.report_semantic_hash,
            report_hash=str(report.report_hash),
            request_hash=report.request_hash,
            snapshot_content_set_hash=report.snapshot_content_set_hash,
            producer_code_closure_hash=report.producer_code_closure_hash,
            report_json_sha256=report_json_hash,
            report_json_size_bytes=len(report_json),
            report_markdown_sha256=report_markdown_hash,
            report_markdown_size_bytes=len(report_markdown),
            catalog_receipt_hash=str(final_catalog_receipt.receipt_hash),
            database_target_receipt_hash=str(
                final_catalog_receipt.database_target.target_receipt_hash
            ),
            catalog_valid_through=final_catalog_receipt.observed_at,
            source_git_commit=source_git_commit,
            source_state=source_state,
            materialized_at=datetime.now(UTC),
        )
        receipt_bytes = (canonical_json_text(receipt.model_dump(mode="python")) + "\n").encode("utf-8")
        bundle_root.mkdir(parents=True, exist_ok=True)
        self._publish_exact(path=bundle_root / "report.json", payload=report_json)
        self._publish_exact(path=bundle_root / "report.md", payload=report_markdown)
        try:
            self._publish_exact(path=bundle_root / "report_receipt.json", payload=receipt_bytes)
        except Phase0BAuditError:
            concurrent = self._existing_complete_receipt(
                bundle_root=bundle_root,
                report=report,
                report_json=report_json,
                report_markdown=report_markdown,
                database_target_receipt_hash=str(
                    final_catalog_receipt.database_target.target_receipt_hash
                ),
            )
            if concurrent is None:
                raise
            return concurrent
        self._verify_complete(bundle_root=bundle_root, receipt=receipt)
        return receipt

    @classmethod
    def _existing_complete_receipt(
        cls,
        *,
        bundle_root: Path,
        report: Phase0BAuditReportV1,
        report_json: bytes,
        report_markdown: bytes,
        database_target_receipt_hash: str,
    ) -> Phase0BReportReceiptV1 | None:
        receipt_path = bundle_root / "report_receipt.json"
        if not receipt_path.exists():
            return None
        try:
            persisted = Phase0BReportReceiptV1.model_validate(
                json.loads(receipt_path.read_bytes())
            )
        except Exception as error:
            raise Phase0BAuditError(
                REASON_REPORT_BUNDLE_CONFLICT,
                "existing report receipt cannot be validated",
                context={"error_type": type(error).__name__},
            ) from error
        if (
            persisted.report_semantic_hash != report.report_semantic_hash
            or persisted.report_hash != report.report_hash
            or persisted.request_hash != report.request_hash
            or persisted.snapshot_content_set_hash != report.snapshot_content_set_hash
            or persisted.producer_code_closure_hash != report.producer_code_closure_hash
            or persisted.report_json_sha256 != hashlib.sha256(report_json).hexdigest()
            or persisted.report_markdown_sha256 != hashlib.sha256(report_markdown).hexdigest()
            or persisted.database_target_receipt_hash != database_target_receipt_hash
        ):
            raise Phase0BAuditError(
                REASON_REPORT_BUNDLE_CONFLICT,
                "existing report receipt has different semantic content",
            )
        cls._verify_complete(bundle_root=bundle_root, receipt=persisted)
        return persisted

    @staticmethod
    def _publish_exact(*, path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        staging = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        try:
            with staging.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(staging, path)
            except FileExistsError:
                try:
                    existing = path.read_bytes()
                except OSError as error:
                    raise Phase0BAuditError(
                        REASON_REPORT_BUNDLE_CONFLICT,
                        "existing report path cannot be read",
                        context={"path_name": path.name},
                    ) from error
                if existing != payload:
                    raise Phase0BAuditError(
                        REASON_REPORT_BUNDLE_CONFLICT,
                        "existing report path has different content",
                        context={"path_name": path.name},
                    )
            Phase0BReportStore._flush_directory(path.parent)
        except Phase0BAuditError:
            raise
        except OSError as error:
            raise Phase0BAuditError(
                REASON_REPORT_BUNDLE_CONFLICT,
                "report file could not be published atomically",
                context={"path_name": path.name, "error_type": type(error).__name__},
            ) from error
        finally:
            try:
                staging.unlink(missing_ok=True)
                Phase0BReportStore._flush_directory(path.parent)
            except OSError as error:
                raise Phase0BAuditError(
                    REASON_REPORT_BUNDLE_CONFLICT,
                    "report staging file could not be cleaned",
                    context={"path_name": path.name},
                ) from error

    @staticmethod
    def _flush_directory(directory: Path) -> None:
        try:
            if os.name != "nt":
                descriptor = os.open(str(directory), os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                return
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.CreateFileW(
                str(directory),
                0x40000000,
                0x00000001 | 0x00000002 | 0x00000004,
                None,
                3,
                0x02000000,
                None,
            )
            invalid_handle = ctypes.c_void_p(-1).value
            if handle == invalid_handle:
                raise OSError("cannot open report directory for durable flush")
            try:
                if not kernel32.FlushFileBuffers(handle):
                    raise OSError("cannot flush report directory")
            finally:
                kernel32.CloseHandle(handle)
        except OSError as error:
            raise Phase0BAuditError(
                REASON_REPORT_BUNDLE_CONFLICT,
                "report directory durable flush failed",
                context={"error_type": type(error).__name__},
            ) from error

    @staticmethod
    def _verify_complete(
        *,
        bundle_root: Path,
        receipt: Phase0BReportReceiptV1,
    ) -> None:
        try:
            report_json = (bundle_root / "report.json").read_bytes()
            report_markdown = (bundle_root / "report.md").read_bytes()
            receipt_payload = (bundle_root / "report_receipt.json").read_bytes()
            persisted_receipt = Phase0BReportReceiptV1.model_validate(json.loads(receipt_payload))
        except Exception as error:
            raise Phase0BAuditError(
                REASON_REPORT_BUNDLE_CONFLICT,
                "completed report bundle cannot be read back",
                context={"error_type": type(error).__name__},
            ) from error
        if (
            persisted_receipt != receipt
            or len(report_json) != receipt.report_json_size_bytes
            or hashlib.sha256(report_json).hexdigest() != receipt.report_json_sha256
            or len(report_markdown) != receipt.report_markdown_size_bytes
            or hashlib.sha256(report_markdown).hexdigest() != receipt.report_markdown_sha256
        ):
            raise Phase0BAuditError(
                REASON_REPORT_BUNDLE_CONFLICT,
                "completed report bundle readback differs from receipt",
            )
