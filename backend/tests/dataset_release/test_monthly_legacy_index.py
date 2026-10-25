from datetime import date
import hashlib
import struct
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.monthly_legacy_prefix import load_legacy_prefix
from backend.services.dataset_release.monthly_legacy_index import append_legacy_index_context


def _inputs(tmp_path):
    root = tmp_path / "august"
    (root / "index_context").mkdir(parents=True)
    old = pd.DataFrame([{"trade_date": pd.Timestamp("2026-08-31"), "ts_code": definition.daily_code,
                         "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 1000.0, "amount": 20.0}
                        for definition in DOMESTIC_INDEX_DEFINITIONS])
    path = root / "index_context/index_daily.h5"
    old.to_hdf(path, "data", format="fixed")
    raw_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {"release_id": "august", "cutoff_trade_date": "2026-08-31", "components": {
        "index_daily": {"path": "index_context/index_daily.h5", "size": path.stat().st_size, "sha256": raw_hash}}}
    manifest["dataset_manifest_sha256"] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    raw = canonical_json_bytes(manifest) + b"\n"
    (root / "qe_dataset_manifest.json").write_bytes(raw)
    prefix = load_legacy_prefix(root, expected_manifest_sha256=manifest["dataset_manifest_sha256"],
        expected_file_sha256=hashlib.sha256(raw).hexdigest(), expected_cutoff=date(2026, 8, 31), expected_release_id="august")
    class Source:
        def __init__(self):
            self.calls = []
            self.missing = None
        def trading_dates(self, start, end):
            assert start == date(2026, 9, 1)
            return (date(2026, 9, 1), date(2026, 9, 2))
        def database_rows(self, definition, start, end):
            self.calls.append((start, end))
            return [{"ts_code": definition.daily_code, "trade_date": day, "open": 12.0, "high": 13.0,
                     "low": 11.0, "close": 12.5, "pre_close": 10.5, "pct_chg": 1.5, "vol": 30.0, "amount": 40.0}
                    for day in self.trading_dates(start, end) if (definition.daily_code, day) != self.missing]
        def provider_rows(self, *args):
            raise AssertionError("frozen BUILD must never call provider fallback")
    return prefix, Source(), path, old


def test_native_index_appends_month_and_preserves_legacy_raw_unit_contract(tmp_path):
    from backend.services.dataset_release.build_stage import _patch_index_component, _index_receipt_payload
    from backend.services.dataset_release.contracts import Component, ComponentAction
    prefix, source, old_path, old = _inputs(tmp_path)
    before = old_path.read_bytes()
    staging = tmp_path / "september"
    staging.mkdir()
    raw = (prefix.root / "qe_dataset_manifest.json").read_bytes()
    invocation = SimpleNamespace(staging_root=staging, profile=SimpleNamespace(candidate_root=str(tmp_path), indices=DOMESTIC_INDEX_DEFINITIONS),
        build_inputs={"source_bundle_sha256": "e" * 64, "monthly_legacy_predecessor": {
            "predecessor": {"candidate_root": str(prefix.root), "cutoff": "2026-08-31", "release_id": "august", "dataset_manifest_sha256": prefix.manifest_sha256},
            "predecessor_manifest_ref": {"id": "qe_dataset_manifest.json", "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest(), "dataset_manifest_sha256": prefix.manifest_sha256}}},
        plan={"actions": [{"component": Component.DOMESTIC_INDEX_CONTEXT.value, "action": ComponentAction.INCREMENTAL.value}]})
    receipt, adoption = _patch_index_component(invocation, source=source, cutoff=date(2026, 9, 2))
    payload = _index_receipt_payload(receipt, staging=staging)
    assert payload["schema_version"] == "aistock_monthly_native_index_materialization_v1"
    assert adoption["publication_allowed"] is False
    assert old_path.read_bytes() == before
    actual = pd.read_hdf(receipt.h5_path, "data")
    pd.testing.assert_frame_equal(actual.iloc[:len(old)].reset_index(drop=True), old)
    assert len(actual) == 36 and set(actual.ts_code) == {item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS}
    assert actual.iloc[-1]["volume"] == 3000.0 and actual.iloc[-1]["amount"] == 40.0
    csv = pd.read_csv(receipt.csv_root / "000300.SH.csv")
    assert csv.iloc[-1]["amount"] == 40_000.0 and csv.iloc[-1]["prev_close"] == 10.5
    context = pd.read_parquet(receipt.parquet_path)
    assert len(context) == 24 and context["idx_amount_cny"].iloc[-1] == 40_000.0
    assert receipt.details["validation_scope"] == "month_delta"
    assert all(start == date(2026, 9, 1) for start, _ in source.calls)
    from backend.services.dataset_release.monthly_legacy_validation import validate_aggregate_month_receipt
    verified = validate_aggregate_month_receipt(root=staging, receipt=payload, component="index",
        cutoff=date(2026, 9, 2), predecessor_manifest_sha256=prefix.manifest_sha256, source_bundle_sha256="e" * 64)
    assert len(verified["verified_output_files"]) == 14
    receipt.h5_path.write_bytes(b"drifted")
    with pytest.raises(Exception, match="changed"):
        validate_aggregate_month_receipt(root=staging, receipt=payload, component="index",
            cutoff=date(2026, 9, 2), predecessor_manifest_sha256=prefix.manifest_sha256, source_bundle_sha256="e" * 64)


def test_native_index_missing_month_fact_stops_without_provider_fallback(tmp_path):
    from backend.services.dataset_release.monthly_legacy_index import append_legacy_index_context
    prefix, source, _, _ = _inputs(tmp_path)
    source.missing = ("000300.SH", date(2026, 9, 1))
    with pytest.raises(Exception, match="coverage"):
        append_legacy_index_context(prefix, source=source, output_root=tmp_path / "september",
            cutoff=date(2026, 9, 2), source_bundle_sha256="e" * 64)
    assert not (tmp_path / "september/index_daily.h5").exists()


def _row():
    return dict(ts_code="000985.CSI", trade_date="2026-06-04", open=10, high=12, low=9,
                close=11, pre_close=10.5, pct_chg=4.7619, vol=3, amount=4)


def test_exact_history_correction_is_frozen_with_new_month_not_lost(tmp_path):
    prefix, source, original, old = _inputs(tmp_path)
    before = original.read_bytes()
    result = append_legacy_index_context(prefix, source=source, output_root=tmp_path / "sep",
        cutoff=date(2026, 9, 2), source_bundle_sha256="e"*64, historical_corrections=[_row()])
    actual = pd.read_hdf(result.h5_path, "data")
    row = actual[(actual.ts_code == "000985.CSI") & (actual.trade_date == pd.Timestamp("2026-06-04"))]
    assert len(row) == 1 and row.iloc[0].close == 11
    assert row.iloc[0].volume == 300 and row.iloc[0].amount == 4
    pd.testing.assert_frame_equal(actual.iloc[:len(old)], old)
    assert original.read_bytes() == before
    assert result.details["historical_correction_rows"] == 1
    assert result.details["historical_business_audit_performed"] is False


def test_exact_index_bin_patch_preserves_month_tail_and_all_other_values(tmp_path):
    from backend.services.dataset_release.monthly_index_corrections import correct_index_bins
    provider = tmp_path / "provider"
    (provider / "calendars").mkdir(parents=True)
    (provider / "calendars/day.txt").write_text("2026-06-04\n2026-08-31\n2026-09-01\n")
    root = provider / "features/000985.csi"
    root.mkdir(parents=True)
    from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS
    features = []
    for field in QLIB_STOCK_FIELDS:
        path = root / f"{field}.day.bin"
        path.write_bytes(struct.pack("<ff", 2, 99))
        features.append(dict(instrument="000985.CSI", feature=field, start_index=2, end_index=2))
    native = dict(feature_receipts=features)
    corrected = correct_index_bins(provider, [_row()], native=native)
    values = struct.unpack("<4f", (root / "close.day.bin").read_bytes())
    assert values[0] == 0 and values[1] == 11 and pd.isna(values[2]) and values[3] == 99
    assert corrected["historical_index_correction"]["keys"] == 1
    for item in corrected["feature_receipts"]:
        path = root / f"{item['feature']}.day.bin"
        assert item["start_index"] == 0
        assert item["target_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    # Identical replay is harmless; conflicting existing finite values fail.
    correct_index_bins(provider, [_row()], native=corrected)
    with pytest.raises(ValueError, match="finite"):
        correct_index_bins(provider, [{**_row(), "close":12}], native=corrected)


def test_history_correction_uses_existing_monthly_binding_and_rejects_wrong_scope(tmp_path):
    from backend.tests.dataset_release.test_monthly_repair_inputs import _inputs as repair_inputs
    from backend.services.dataset_release.monthly_repair_inputs import validate_monthly_repair_inputs
    prefix, body = repair_inputs(tmp_path)
    body["index_history_repairs"] = dict(schema_version="aistock_monthly_index_history_repair_v1",
        source_dataset="market.index_daily", captured_at="2026-10-20T08:00:00+08:00", rows=[_row()])
    assert validate_monthly_repair_inputs(body, predecessor=prefix, target_cutoff=date(2026, 9, 30)) == body
    body["index_history_repairs"]["rows"] = [{**_row(), "trade_date":"2026-09-01"}]
    with pytest.raises(ValueError, match="inherited"):
        validate_monthly_repair_inputs(body, predecessor=prefix, target_cutoff=date(2026, 9, 30))


def test_history_correction_closes_real_native_writer_and_month_validator(tmp_path):
    from backend.tests.dataset_release.test_monthly_legacy_build_stage import _setup
    from backend.services.dataset_release import build_stage as stages
    from backend.services.dataset_release.monthly_mature_build_runner import MatureMonthlyPhysicalBuildRunner
    from backend.services.dataset_release.monthly_worker import ProducerContext
    from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS
    from backend.services.dataset_release.monthly_legacy_validation import validate_qlib_month
    invocation, source, pit, toolchain, provider, _, _ = _setup(tmp_path)
    (provider / "calendars/day.txt").write_text("2026-06-04\n2026-08-31\n")
    for field in QLIB_STOCK_FIELDS:
        path = provider / f"features/000001.sz/{field}.day.bin"
        path.write_bytes(struct.pack("<f", 1)+path.read_bytes()[4:])
    invocation.profile.index_codes = ("000300.SH", "000985.CSI")
    csv_root = invocation.staging_root / "index_context/index_csv"
    (csv_root / "000985.CSI.csv").write_text((csv_root / "000300.SH.csv").read_text().replace("000300.SH", "000985.CSI"))
    repairs = {"index_history_repairs":dict(schema_version="aistock_monthly_index_history_repair_v1",
        source_dataset="market.index_daily", captured_at="2026-10-20T08:00:00+08:00", rows=[_row()])}
    invocation.build_inputs["monthly_repair_inputs"] = repairs
    prepared = stages._prepare_bin_patch(invocation, source=source, pit=pit, dataset="daily_bin", toolchain=toolchain, checkpoint=lambda:None)
    runner = MatureMonthlyPhysicalBuildRunner(profile=invocation.profile, cas=invocation.cas, project_root=tmp_path,
        qlib_writer=SimpleNamespace(), consumer_smoke=SimpleNamespace(), finalizer=SimpleNamespace())
    context = ProducerContext("BUILD", "dmr_"+"1"*32, 1, {}, {**invocation.build_inputs["monthly_legacy_predecessor"],
        "target_cutoff":pit.cutoff.isoformat(), "monthly_repair_inputs":repairs}, {})
    child = runner._append_inherited_month(context=context, staging_root=invocation.staging_root,
        compiled=SimpleNamespace(source_bundle_sha256="e"*64), operation=prepared["qlib_dump_operations"][0])
    invocation.prerequisites = {"qlib_dump_daily":invocation.cas.put_json(child).sha256}
    receipt = stages._finalize_bin_patch(invocation, dataset="daily_bin", pit=pit, toolchain=toolchain,
        preparation=prepared, checkpoint=lambda:None)
    report = validate_qlib_month(root=invocation.staging_root, cas=invocation.cas, materialization=receipt,
        cutoff=pit.cutoff, predecessor_manifest_sha256=invocation.build_inputs["monthly_legacy_predecessor"]["predecessor"]["dataset_manifest_sha256"], source_bundle_sha256="e"*64)
    assert report["validated_rows"] == 3
    values = struct.unpack("<4f", (invocation.staging_root / "daily_bin/qlib/features/000985.csi/close.day.bin").read_bytes())
    assert values[0] == 0 and values[1] == 11 and pd.isna(values[2]) and values[3] == 100
