from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_phase0b.contracts import MetricStatus
from backend.services.advisory_phase0b.errors import Phase0BAuditError
from backend.services.advisory_phase0b.report_store import (
    Phase0BAuditReportV1,
    Phase0BMetricResultV1,
    Phase0BReportStore,
    Phase0BSnapshotReportAuthorityV1,
    Phase0BTargetAuditReportV1,
)
from backend.tests.advisory_phase0b.test_snapshot_reader import _receipt


_HASH = "a" * 64


def _report() -> Phase0BAuditReportV1:
    catalog_receipt = _receipt()
    entry = catalog_receipt.entries[0]
    header = entry.header_payload()
    semantic_hash = canonical_json_sha256(
        {"request_hash": _HASH, "snapshot_content_set_hash": "b" * 64}
    )
    metric = Phase0BMetricResultV1(
        metric_definition_id="stage-topk-point-estimate-v1",
        metric_definition_hash="c" * 64,
        slice_id="RETURN_NET_EXCESS:h5:selection_effective:k5",
        projection="RETURN_NET_EXCESS",
        horizon_trading_days=5,
        stage="selection_effective",
        depth=5,
        status=MetricStatus.INSUFFICIENT_SAMPLE,
        reason_codes=("ADVISORY_PHASE0B_INSUFFICIENT_DECISION_DATES",),
        decision_date_count=15,
        evaluable_date_count=15,
        effective_sample_count=15,
        missing_decision_date_count=0,
        candidate_count=75,
        matured_label_count=70,
        unavailable_label_count=5,
        observed_value=Decimal("0.0123"),
    )
    target = Phase0BTargetAuditReportV1(
        target_hash="d" * 64,
        snapshot_id="snapshot-1",
        program_id="program-1",
        package_id="package-1",
        manifest_sha256="e" * 64,
        alpha_mode="multi_alpha",
        style_hypothesis="SHORT_REBOUND",
        decision_date_count=15,
        metric_results=(metric,),
        phase2_phase3_recommendations=("Generate a 2/3/5-year PIT SEALED snapshot.",),
    )
    return Phase0BAuditReportV1(
        request_hash=_HASH,
        producer_code_closure_hash="f" * 64,
        metric_registry_hash="1" * 64,
        multiple_testing_registry_hash="2" * 64,
        snapshot_content_set_hash="b" * 64,
        catalog_content_set_hash=str(catalog_receipt.catalog_content_set_hash),
        snapshot_authorities=(
            Phase0BSnapshotReportAuthorityV1(
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
                capability_identity_hash=str(header["maturity_coverage_hash"]),
            ),
        ),
        report_semantic_hash=semantic_hash,
        target_reports=(target,),
    )


def test_report_store_publishes_receipt_last_and_exact_retry_converges(tmp_path: Path) -> None:
    output_root = (tmp_path / "reports").resolve()
    store = Phase0BReportStore(output_root=output_root)
    report = _report()

    first = store.publish(
        report=report,
        final_catalog_receipt=_receipt(),
        source_git_commit="abc123",
        source_state="clean",
    )
    later_catalog_payload = _receipt().model_dump(mode="python")
    later_catalog_payload.update(
        {
            "observed_at": datetime(2026, 8, 1, tzinfo=UTC) + timedelta(minutes=5),
            "receipt_hash": None,
        }
    )
    later_catalog = type(_receipt()).model_validate(later_catalog_payload)
    second = store.publish(
        report=report,
        final_catalog_receipt=later_catalog,
        source_git_commit="different-materialization",
        source_state="dirty",
    )

    bundle = output_root / f"phase0b_report_{report.report_semantic_hash}"
    assert first == second
    assert (bundle / "report.json").exists()
    assert (bundle / "report.md").exists()
    assert (bundle / "report_receipt.json").exists()


def test_report_store_rejects_existing_same_path_different_content(tmp_path: Path) -> None:
    output_root = (tmp_path / "reports").resolve()
    store = Phase0BReportStore(output_root=output_root)
    report = _report()
    store.publish(
        report=report,
        final_catalog_receipt=_receipt(),
        source_git_commit="abc123",
        source_state="clean",
    )
    bundle = output_root / f"phase0b_report_{report.report_semantic_hash}"
    (bundle / "report.json").write_text("conflict", encoding="utf-8")

    with pytest.raises(Phase0BAuditError, match="readback differs"):
        store.publish(
            report=report,
            final_catalog_receipt=_receipt(),
            source_git_commit="abc123",
            source_state="clean",
        )


def test_report_store_failure_before_receipt_is_not_consumable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = (tmp_path / "reports").resolve()
    store = Phase0BReportStore(output_root=output_root)
    report = _report()
    original = store._publish_exact

    def _fail_markdown(*, path: Path, payload: bytes) -> None:
        if path.name == "report.md":
            raise Phase0BAuditError("ADVISORY_PHASE0B_REPORT_BUNDLE_CONFLICT", "injected")
        original(path=path, payload=payload)

    monkeypatch.setattr(store, "_publish_exact", _fail_markdown)
    with pytest.raises(Phase0BAuditError, match="injected"):
        store.publish(
            report=report,
            final_catalog_receipt=_receipt(),
            source_git_commit="abc123",
            source_state="clean",
        )
    bundle = output_root / f"phase0b_report_{report.report_semantic_hash}"
    assert (bundle / "report.json").exists()
    assert not (bundle / "report_receipt.json").exists()


def test_report_store_failure_after_markdown_still_has_no_completion_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = (tmp_path / "reports").resolve()
    store = Phase0BReportStore(output_root=output_root)
    report = _report()
    original = store._publish_exact

    def _fail_receipt(*, path: Path, payload: bytes) -> None:
        if path.name == "report_receipt.json":
            raise Phase0BAuditError("ADVISORY_PHASE0B_REPORT_BUNDLE_CONFLICT", "injected")
        original(path=path, payload=payload)

    monkeypatch.setattr(store, "_publish_exact", _fail_receipt)
    with pytest.raises(Phase0BAuditError, match="injected"):
        store.publish(
            report=report,
            final_catalog_receipt=_receipt(),
            source_git_commit="abc123",
            source_state="clean",
        )
    bundle = output_root / f"phase0b_report_{report.report_semantic_hash}"
    assert (bundle / "report.json").exists()
    assert (bundle / "report.md").exists()
    assert not (bundle / "report_receipt.json").exists()


def test_report_store_concurrent_publishers_converge_to_one_complete_receipt(
    tmp_path: Path,
) -> None:
    output_root = (tmp_path / "reports").resolve()
    store = Phase0BReportStore(output_root=output_root)
    report = _report()

    def publish() -> object:
        return store.publish(
            report=report,
            final_catalog_receipt=_receipt(),
            source_git_commit="abc123",
            source_state="clean",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        receipts = tuple(executor.map(lambda _index: publish(), range(2)))

    bundle = output_root / f"phase0b_report_{report.report_semantic_hash}"
    assert receipts[0] == receipts[1]
    assert len(tuple(bundle.glob("report_receipt.json"))) == 1
