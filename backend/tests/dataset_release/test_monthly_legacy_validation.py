from datetime import date
from dataclasses import replace
import struct
from types import SimpleNamespace

import pytest
import pandas as pd

from backend.tests.dataset_release.test_monthly_legacy_build_stage import _append_and_finalize


@pytest.mark.parametrize("dataset,rows", [("daily_bin", 2), ("minute_bin", 240)])
def test_native_month_validator_compares_actual_tail_to_frozen_csv(tmp_path, dataset, rows):
    from backend.services.dataset_release.monthly_legacy_validation import validate_qlib_month
    invocation, _, receipt = _append_and_finalize(tmp_path, dataset=dataset)
    report = validate_qlib_month(root=invocation.staging_root, materialization=receipt, cas=invocation.cas,
        cutoff=date(2026, 9, 1), predecessor_manifest_sha256=receipt["predecessor_manifest_sha256"],
        source_bundle_sha256="e" * 64)
    assert report["validation_scope"] == "month_delta"
    assert report["validated_rows"] == rows
    assert report["value_comparison_count"] == rows * 12
    assert report["historical_values_read"] == 0
    assert len(report["verified_output_files"]) == 12 * (2 if dataset == "daily_bin" else 1) + 2


def test_native_month_validator_rejects_real_bin_value_drift_not_only_signature(tmp_path):
    from backend.services.dataset_release.monthly_legacy_validation import validate_qlib_month
    invocation, _, receipt = _append_and_finalize(tmp_path)
    path = invocation.staging_root / "daily_bin/qlib/features/000001.sz/close.day.bin"
    with path.open("r+b") as writer:
        writer.seek(8)
        writer.write(struct.pack("<f", 6.001))
    state = path.stat()
    for feature in receipt["native_writer"]["feature_receipts"]:
        if feature["instrument"].upper() == "000001.SZ" and feature["feature"] == "close":
            feature["target_signature"] = [state.st_dev, state.st_ino, state.st_size, state.st_mtime_ns]
    with pytest.raises(Exception, match="frozen CSV"):
        validate_qlib_month(root=invocation.staging_root, materialization=receipt, cas=invocation.cas,
            cutoff=date(2026, 9, 1), predecessor_manifest_sha256=receipt["predecessor_manifest_sha256"], source_bundle_sha256="e" * 64)


@pytest.mark.parametrize("binding", ["source_bundle_sha256", "predecessor_manifest_sha256"])
def test_native_month_validator_rejects_foreign_source_and_prefix(tmp_path, binding):
    from backend.services.dataset_release.monthly_legacy_validation import validate_qlib_month
    invocation, _, receipt = _append_and_finalize(tmp_path)
    expected_prefix = receipt["predecessor_manifest_sha256"]
    receipt[binding] = "f" * 64
    with pytest.raises(Exception, match="identity differs"):
        validate_qlib_month(root=invocation.staging_root, materialization=receipt, cas=invocation.cas,
            cutoff=date(2026, 9, 1), predecessor_manifest_sha256=expected_prefix, source_bundle_sha256="e" * 64)


def test_native_month_validator_rejects_extra_unfrozen_tail(tmp_path):
    from backend.services.dataset_release.monthly_legacy_validation import validate_qlib_month
    invocation, _, receipt = _append_and_finalize(tmp_path)
    path = invocation.staging_root / "daily_bin/qlib/features/000001.sz/close.day.bin"
    with path.open("ab") as writer:
        writer.write(struct.pack("<f", 6.0))
    metadata = path.stat()
    for item in receipt["native_writer"]["feature_receipts"]:
        if item["instrument"].upper() == "000001.SZ" and item["feature"] == "close":
            item["target_signature"] = [metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns]
            item["target_size"] = metadata.st_size
    with pytest.raises(Exception, match="bounds differ"):
        validate_qlib_month(root=invocation.staging_root, materialization=receipt, cas=invocation.cas,
            cutoff=date(2026, 9, 1), predecessor_manifest_sha256=receipt["predecessor_manifest_sha256"], source_bundle_sha256="e" * 64)


def test_formal_native_month_branch_rejects_smoke_from_foreign_attempt(tmp_path):
    from backend.services.dataset_release.build_stage import _validate_native_month
    from backend.services.dataset_release.candidate_consumer_smoke import run_candidate_consumer_smoke
    from backend.tests.dataset_release.test_candidate_consumer_smoke import _spec, _FakeD, _FakeQlib
    invocation, _, receipt = _append_and_finalize(tmp_path)
    spec = _spec(tmp_path)
    frame = pd.read_hdf(spec.index_h5_path, "data")
    frame.index = pd.MultiIndex.from_tuples([(pd.Timestamp("2026-08-31"), "000300.SH"),
        (pd.Timestamp("2026-09-01"), "000300.SH")], names=["datetime", "instrument"])
    frame.to_hdf(spec.index_h5_path, "data", mode="w", format="table", data_columns=True)
    spec = replace(spec, cutoff=date(2026, 9, 1))
    smoke = run_candidate_consumer_smoke(spec, checkpoint=lambda: None, data_api=_FakeD(), qlib_runtime=_FakeQlib())
    invocation.profile.profile = spec.profile
    invocation.profile.index_codes = spec.expected_index_codes
    invocation.profile.stage_timeouts_seconds["consumer"] = spec.stage_timeout_seconds
    invocation.build_inputs["require_production_consumer_smoke"] = False
    invocation.run_id = spec.run_id
    invocation.attempt_id = "new-attempt"
    invocation.attempt_fence = 2
    invocation.release_id = spec.release_id
    invocation.release_digest = spec.release_digest
    invocation.staging_relative_path = spec.staging_relative_path
    invocation.prerequisites["consumer_smoke"] = invocation.cas.put_json(smoke).sha256
    with pytest.raises(Exception, match="fence identity differs"):
        _validate_native_month(invocation, pit=SimpleNamespace(cutoff=spec.cutoff),
            daily_receipt=receipt, minute_receipt={}, factor_receipt={}, index_receipt={},
            ledger=SimpleNamespace(chunk=lambda _: None), checkpoint=lambda: None)
