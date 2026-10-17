from datetime import date
import hashlib
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_legacy_prefix import load_legacy_prefix


def _candidate(root, value, receipt):
    root.mkdir()
    raw = canonical_json_bytes(receipt) + b"\n"
    (root / "receipt.json").write_bytes(raw)
    manifest = {"cutoff_trade_date": "2026-08-31", "release_id": "qe_hmm_full_v2_20260831",
                "components": {"daily_basic_fields_repair": {"path": "receipt.json", "size": len(raw),
                    "sha256": hashlib.sha256(raw).hexdigest()}}, "revision": value}
    manifest["dataset_manifest_sha256"] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    raw = canonical_json_bytes(manifest) + b"\n"
    (root / "qe_dataset_manifest.json").write_bytes(raw)
    return {"candidate_root": str(root), "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
            "dataset_manifest_file_sha256": hashlib.sha256(raw).hexdigest(),
            "receipt_component": "daily_basic_fields_repair", "receipt_sha256": manifest["components"]["daily_basic_fields_repair"]["sha256"]}


def _inputs(tmp_path):
    catalog = tmp_path / "releases"
    catalog.mkdir()
    baseline = _candidate(catalog / "old", "old", {})
    prefix = load_legacy_prefix(catalog / "old", expected_manifest_sha256=baseline["dataset_manifest_sha256"],
        expected_file_sha256=baseline["dataset_manifest_file_sha256"], expected_cutoff=date(2026, 8, 31),
        expected_release_id="qe_hmm_full_v2_20260831")
    receipt = {"schema_version": "aistock_daily_basic_fields_selective_repair_v1", "status": "PASS",
        "baseline_manifest_identity": prefix.manifest_sha256, "existing_finite_preserved": True,
        "source_null_preserved": True, "h5_filled_cells": {"db_dv_ratio": 1},
        "source_artifacts": [], "database_write": False, "active_profile_write": False}
    fixed = _candidate(catalog / "fixed", "fixed", receipt)
    body = {"schema_version": "aistock_monthly_repair_inputs_v1", "target_cutoff": "2026-09-30",
        "predecessor_manifest_sha256": prefix.manifest_sha256, "factor_prefixes": {"static_factors": fixed},
        "daily_basic_null_repairs": None, "deferred_source_dates": []}
    return prefix, body


def test_explicit_independent_repair_prefix_does_not_replace_logical_predecessor(tmp_path):
    from backend.services.dataset_release.monthly_repair_inputs import validate_monthly_repair_inputs
    prefix, body = _inputs(tmp_path)
    result = validate_monthly_repair_inputs(body, predecessor=prefix, target_cutoff=date(2026, 9, 30))
    assert result == body
    assert prefix.root.name == "old"


@pytest.mark.parametrize("dataset,day,accepted", [("margin_detail", "2026-09-30", True),
    ("moneyflow", "2026-09-30", False), ("margin_detail", "2026-09-29", False)])
def test_only_explicit_user_deferred_financing_tail_can_be_bound(tmp_path, dataset, day, accepted):
    from backend.services.dataset_release.monthly_repair_inputs import validate_monthly_repair_inputs
    prefix, body = _inputs(tmp_path)
    body["deferred_source_dates"] = [{"dataset": dataset, "trade_date": day, "reason_code": "USER_DEFERRED_COLLECTION"}]
    if accepted:
        assert validate_monthly_repair_inputs(body, predecessor=prefix, target_cutoff=date(2026, 9, 30)) == body
    else:
        with pytest.raises(ValueError):
            validate_monthly_repair_inputs(body, predecessor=prefix, target_cutoff=date(2026, 9, 30))


@pytest.mark.parametrize("mutation", ["outside_catalog", "hash_drift", "wrong_month", "unapproved_dataset"])
def test_repair_input_identity_cannot_be_implicitly_selected_or_changed(tmp_path, mutation):
    from backend.services.dataset_release.monthly_repair_inputs import validate_monthly_repair_inputs
    prefix, body = _inputs(tmp_path)
    entry = body["factor_prefixes"]["static_factors"]
    if mutation == "outside_catalog":
        entry["candidate_root"] = str(tmp_path)
    elif mutation == "hash_drift":
        entry["receipt_sha256"] = "0" * 64
    elif mutation == "wrong_month":
        body["target_cutoff"] = "2026-10-30"
    else:
        body["factor_prefixes"]["daily_pv"] = body["factor_prefixes"].pop("static_factors")
    with pytest.raises(ValueError):
        validate_monthly_repair_inputs(body, predecessor=prefix, target_cutoff=date(2026, 9, 30))


def test_exact_indexed_daily_basic_repair_preserves_finite_cells_and_other_history(tmp_path, monkeypatch):
    from backend.services.dataset_release.monthly_repair_inputs import apply_daily_basic_null_repairs
    path = tmp_path / "private.h5"
    index = pd.MultiIndex.from_tuples([(pd.Timestamp("2026-04-20"), "000001.SZ"),
        (pd.Timestamp("2026-04-20"), "000002.SZ"), (pd.Timestamp("2026-04-21"), "000001.SZ")],
        names=("datetime", "instrument"))
    frame = pd.DataFrame({"db_dv_ratio": [np.nan, 7.0, np.nan], "db_circ_mv": [10., 20., 30.]}, index=index).astype("float32")
    frame.to_hdf(path, "data", format="table", data_columns=["datetime", "instrument"])
    import tables
    original = tables.Table.modify_columns
    updated = []
    def value_only(table, *args, **kwargs):
        updated.extend(kwargs["names"])
        assert all(name.startswith("values_block_") for name in kwargs["names"])
        return original(table, *args, **kwargs)
    monkeypatch.setattr(tables.Table, "modify_columns", value_only)
    monkeypatch.setattr(tables.Table, "modify_coordinates", lambda *_a, **_kw: pytest.fail("would rebuild full date/instrument indexes"))
    source = tmp_path / "source.parquet"
    pd.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"], "trade_date": ["20260420"] * 2,
                  "dv_ratio": [1.5, 2.5]}).to_parquet(source)
    ref = {"source_path": str(source), "provider_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
           "trade_date": "2026-04-20", "fields": ["db_dv_ratio"]}
    result = apply_daily_basic_null_repairs(path, entries=[ref])
    actual = pd.read_hdf(path, "data")
    expected = frame.copy()
    expected.iloc[0, 0] = 1.5
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    assert result["filled_cells"] == {"db_dv_ratio": 1}
    assert result["existing_finite_preserved"] is True
    assert result["historical_business_audit_performed"] is False
    assert updated == ["values_block_0"]


def test_null_repair_rejects_unknown_dates_instead_of_manufacturing_rows(tmp_path):
    from backend.services.dataset_release.monthly_repair_inputs import apply_daily_basic_null_repairs
    source = tmp_path / "source.parquet"
    pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260421"], "dv_ratio": [1.5]}).to_parquet(source)
    with pytest.raises(ValueError, match="date"):
        apply_daily_basic_null_repairs(tmp_path / "unavailable.h5", entries=[{
            "source_path": str(source), "provider_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "trade_date": "2026-04-20", "fields": ["db_dv_ratio"]}])


@pytest.mark.parametrize("forbidden", [False, True])
def test_formal_binding_is_idempotent_pre_source_only_and_never_activates(tmp_path, forbidden):
    from backend.tests.dataset_release.test_monthly_unified_v2 import _service, _request, Pipeline
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseConflict
    prefix, body = _inputs(tmp_path)
    service = _service(tmp_path, Pipeline())
    active = json.loads(service.active_profile.read_text())
    active["controller_paths"]["candidate_root"] = str(prefix.root)
    active["components"].update(dataset_manifest_sha256=prefix.manifest_sha256,
        dataset_manifest_file_sha256=prefix.manifest_file_sha256)
    service.active_profile.write_bytes(canonical_json_bytes(active) + b"\n")
    before = service.active_profile.read_bytes()
    operation = service.submit(_request())
    op_id = operation["operation_id"]
    if forbidden:
        service.store.update_state(op_id, status="CHECKING_SOURCE")
        with pytest.raises(MonthlyReleaseConflict):
            service.bind_repair_inputs(op_id, inputs=body, principal="operator:test")
        assert "monthly_repair_inputs" not in service.store.read_plan(op_id)
    else:
        receipt = service.bind_repair_inputs(op_id, inputs=body, principal="operator:test")
        assert service.bind_repair_inputs(op_id, inputs=body, principal="operator:test") == receipt
        plan = service.store.read_plan(op_id)
        assert plan["monthly_repair_inputs"] == body
        assert service.status(op_id)["status"] == "PLANNED"
        assert not any(service.status(op_id)["checkpoints"].values())
        changed = {**body, "factor_prefixes": {}}
        with pytest.raises(MonthlyReleaseConflict):
            service.bind_repair_inputs(op_id, inputs=changed, principal="operator:test")
    assert service.active_profile.read_bytes() == before
