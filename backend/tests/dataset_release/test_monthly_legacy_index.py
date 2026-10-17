from datetime import date
import hashlib
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.monthly_legacy_prefix import load_legacy_prefix


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
