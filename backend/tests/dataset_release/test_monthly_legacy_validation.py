from datetime import date
from dataclasses import replace
import struct
from types import SimpleNamespace
import csv
import hashlib
import io

import pytest
import pandas as pd
import numpy as np

from backend.tests.dataset_release.test_monthly_legacy_build_stage import _append_and_finalize


def _sealed_quote_correction(tmp_path, *, dataset="daily_bin", extend=False, fill=False):
    from backend.services.dataset_release.monthly_quote_fact_correction import SCHEMA
    invocation, _, receipt = _append_and_finalize(tmp_path, dataset=dataset)
    frequency = "day" if dataset == "daily_bin" else "1min"
    provider = invocation.staging_root / dataset / "qlib"
    path = provider / "features/000001.sz" / f"close.{frequency}.bin"
    prepared = invocation.cas.get_json(receipt["sealed_canonical_rows"]["month_preparation_ref"])
    if fill:
        ref = next(row for row in prepared["csv_refs"] if "000001.SZ" in row["id"])
        csv_path = invocation.staging_root / ref["id"]
        reader = csv.DictReader(io.StringIO(csv_path.read_text(encoding="utf-8")))
        fields, rows = reader.fieldnames, list(reader)
        rows[0]["close"] = "nan"
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
        raw = output.getvalue().encode()
        csv_path.write_bytes(raw)
        ref.update(size=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    if extend:
        calendar = provider / f"calendars/{frequency}.txt"
        raw = calendar.read_bytes() + b"2026-09-02\n"
        calendar.write_bytes(raw)
        receipt["native_writer"]["calendar_sha256"] = hashlib.sha256(raw).hexdigest()
        prepared["calendar_ref"] = {**invocation.cas.put_bytes(raw).as_dict()}
        prepared["cutoff"] = receipt["cutoff"] = "2026-09-02"
        values = np.append(np.fromfile(path, dtype="<f4"), np.float32(7)).astype("<f4")
        path.write_bytes(values.tobytes())
    receipt["sealed_canonical_rows"]["month_preparation_ref"] = invocation.cas.put_json(prepared).as_dict()
    row = {"relative_path": path.relative_to(invocation.staging_root).as_posix(),
           "target_ref": invocation.cas.put_bytes(path.read_bytes()).as_dict()}
    authority = {"schema_version": SCHEMA, "files": [row]}
    receipt["quote_fact_correction"] = {**authority, "authority_ref": invocation.cas.put_json(authority).as_dict()}
    return invocation, receipt, path


@pytest.mark.parametrize("dataset,extend", [("daily_bin", True), ("daily_bin", False), ("minute_bin", False)])
def test_native_month_uses_sealed_quote_fills_and_extended_bounds(tmp_path, dataset, extend):
    from backend.services.dataset_release.monthly_legacy_validation import validate_qlib_month
    invocation, receipt, _ = _sealed_quote_correction(tmp_path, dataset=dataset, extend=extend, fill=True)
    report = validate_qlib_month(root=invocation.staging_root, materialization=receipt, cas=invocation.cas,
        cutoff=date.fromisoformat(receipt["cutoff"]), predecessor_manifest_sha256=receipt["predecessor_manifest_sha256"],
        source_bundle_sha256="e" * 64)
    assert report["status"] == "PASS" and report["historical_values_read"] == 0


@pytest.mark.parametrize("defect", ["finite_csv", "changed_target", "unbound_target"])
def test_native_month_quote_correction_preserves_finite_csv_and_exact_binding(tmp_path, defect):
    from backend.services.dataset_release.monthly_legacy_validation import validate_qlib_month
    invocation, receipt, path = _sealed_quote_correction(tmp_path)
    values = np.fromfile(path, dtype="<f4")
    values[-1] = 9
    path.write_bytes(values.tobytes())
    if defect in {"finite_csv", "unbound_target"}:
        row = receipt["quote_fact_correction"]["files"][0]
        row["target_ref"] = invocation.cas.put_bytes(path.read_bytes()).as_dict()
        if defect == "finite_csv":
            authority = {"schema_version": receipt["quote_fact_correction"]["schema_version"], "files": [row]}
            receipt["quote_fact_correction"]["authority_ref"] = invocation.cas.put_json(authority).as_dict()
    with pytest.raises((ValueError, RuntimeError)):
        validate_qlib_month(root=invocation.staging_root, materialization=receipt, cas=invocation.cas,
            cutoff=date(2026, 9, 1), predecessor_manifest_sha256=receipt["predecessor_manifest_sha256"],
            source_bundle_sha256="e" * 64)


@pytest.mark.parametrize("corrupt", [False, True])
def test_native_factor_metadata_relocation_keeps_content_identity_not_old_inode(tmp_path, corrupt):
    from backend.services.dataset_release.factor_materializer import FACTOR_H5_DATASETS
    from backend.services.dataset_release.monthly_legacy_validation import validate_aggregate_month_receipt
    root = tmp_path / "factor_bundle"
    root.mkdir()
    files, metadata = [], []
    def pin(path):
        state = path.stat()
        return {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "size": state.st_size,
                "target_signature": [state.st_dev, state.st_ino, state.st_size, state.st_mtime_ns]}
    for name in (*FACTOR_H5_DATASETS, "static_factors"):
        path = root / f"{name}.{'parquet' if name == 'static_factors' else 'h5'}"
        path.write_bytes(b"unchanged native writer output")
        files.append({**pin(path), "dataset": name, "schema_version": "aistock_monthly_legacy_factor_append_v1",
            "predecessor_manifest_sha256": "a" * 64, "source_bundle_sha256": "e" * 64, "cutoff": "2026-09-01",
            "validation_scope": "month_delta", "month_output_values_verified": True, "inherited_rows": 0,
            "month_rows": 0, "historical_business_audit_performed": False, "publication_allowed": False})
    for name in ("security_source_identity.json", "moneyflow_alias_coverage_v1.json"):
        path = root / name
        path.write_bytes(b"{}\n")
        metadata.append({"relative_path": name, **pin(path)})
    alias = {"schema_version": "qe_moneyflow_alias_coverage_receipt_v1", "status": "PASS",
        "validation_scope": "month_delta", "month_start": "2026-09-01", "cutoff": "2026-09-01",
        "predecessor_manifest_sha256": "a" * 64, "source_bundle_sha256": "e" * 64,
        "moneyflow_sha256": next(item["sha256"] for item in files if item["dataset"] == "moneyflow"),
        "expected": 0, "resolved": 0, "unknown": 0, "nonfinite": 0, "mismatched": 0}
    receipt = {"schema_version": "aistock_monthly_native_factor_materialization_v1", "status": "PASS",
        "month_start": "2026-09-01", "cutoff": "2026-09-01", "predecessor_manifest_sha256": "a" * 64,
        "source_bundle_sha256": "e" * 64, "historical_business_audit_performed": False,
        "publication_allowed": False, "files": files, "alias_coverage": alias, "metadata_files": metadata}
    path = root / metadata[0]["relative_path"]
    replacement = path.with_suffix(".relocated")
    replacement.write_bytes(b"[]\n" if corrupt else path.read_bytes())
    replacement.replace(path)
    arguments = dict(root=tmp_path, receipt=receipt, component="factor", cutoff=date(2026, 9, 1),
        predecessor_manifest_sha256="a" * 64, source_bundle_sha256="e" * 64)
    if corrupt:
        with pytest.raises(RuntimeError, match="metadata changed"):
            validate_aggregate_month_receipt(**arguments)
    else:
        assert validate_aggregate_month_receipt(**arguments)["status"] == "PASS"


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


@pytest.mark.parametrize('dataset',['daily_bin','minute_bin'])
def test_native_month_validator_reuses_same_content_after_private_path_relocation(tmp_path,dataset):
    from backend.services.dataset_release.monthly_legacy_validation import validate_qlib_month
    invocation, _, receipt = _append_and_finalize(tmp_path,dataset=dataset)
    # target_root is the writer's historical location, not a fact identity.
    # The SOURCE, predecessor, actual file signatures and values stay exact.
    receipt['native_writer']['target_root'] = str(tmp_path/'retained-original-export'/dataset/'qlib')
    frequency = 'day' if dataset=='daily_bin' else '1min'
    for name in [f'calendars/{frequency}.txt','instruments/all.txt']:
        target = invocation.staging_root/dataset/'qlib'/name
        copied = target.with_suffix('.copied')
        copied.write_bytes(target.read_bytes())
        copied.replace(target)
    report = validate_qlib_month(root=invocation.staging_root,materialization=receipt,cas=invocation.cas,
        cutoff=date(2026,9,1),predecessor_manifest_sha256=receipt['predecessor_manifest_sha256'],source_bundle_sha256='e'*64)
    assert report['status']=='PASS' and report['historical_values_read']==0


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
