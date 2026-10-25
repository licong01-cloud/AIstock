from __future__ import annotations

from datetime import date
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.factor_materializer import FACTOR_H5_DTYPES, FACTOR_H5_SCHEMAS, SealedFactorChunk
from backend.services.dataset_release.monthly_legacy_prefix import load_legacy_prefix
from backend.services.dataset_release.static_schema import STATIC_COLUMN_DTYPES, STATIC_ORDERED_COLUMNS


def _frame(dataset, day):
    columns = STATIC_ORDERED_COLUMNS if dataset == "static_factors" else FACTOR_H5_SCHEMAS[dataset]
    types = STATIC_COLUMN_DTYPES if dataset == "static_factors" else FACTOR_H5_DTYPES[dataset]
    index = pd.MultiIndex.from_tuples([(pd.Timestamp(day), "000001.SZ")], names=("datetime", "instrument"))
    return pd.DataFrame({name: pd.Series([1 if name == "l2_code_id" else 10.0], index=index, dtype=types[name]) for name in columns})


def test_legacy_rolling_state_uses_real_raw_anchor_and_rebases_price_seed(tmp_path):
    from backend.data_service.security_source_identity import load_security_source_identity_manifest
    from backend.services.dataset_release.monthly_legacy_factor import restore_legacy_factor_state
    from backend.data_service.security_source_identity import DEFAULT_MANIFEST_PATH

    prefix, _, _, _, old_path, _, _ = _inputs(tmp_path, "daily_pv", legacy_layout=True)
    root = old_path.parent
    inventory_path = prefix.root / "factor-inventory.json"
    inventory = json.loads(inventory_path.read_text())
    for dataset in ("moneyflow", "bak_basic", "cyq_perf", "sector_data", "margin_detail"):
        path = root / f"{dataset}.h5"
        _frame(dataset, "2026-08-31").to_hdf(path, "data", format="table", data_columns=["datetime", "instrument"])
        inventory["files"].append({"path": path.relative_to(prefix.root).as_posix(),
                                  "size": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    raw = canonical_json_bytes(inventory) + b"\n"
    inventory_path.write_bytes(raw)
    # Fixture manifest is intentionally rebound before construction, not a live release edit.
    prefix.manifest["components"]["factor_content_manifest"].update(size=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    seen = []
    def raw_anchor(latest):
        seen.extend(latest.index.tolist())
        return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": date(2026, 8, 31), "adj_factor": 2.0}])
    state, changes, boundaries = restore_legacy_factor_state(
        prefix, instruments=("000001.SZ",), before=date(2026, 9, 1), denominators={"000001.SZ": 8.0},
        security_identity=load_security_source_identity_manifest(DEFAULT_MANIFEST_PATH),
        raw_adj_reader=raw_anchor, max_rows=3,
    )
    assert seen == [(pd.Timestamp("2026-08-31"), "000001.SZ")]
    assert state.adj_factor_tail.iloc[0]["adj_factor"] == 2.0
    assert state.price_tail.iloc[0]["close"] == 10.0000000001 / 2
    assert state.price_tail.iloc[0]["volume"] == 200.0
    assert state.price_tail.iloc[0]["amount"] == 10.0
    assert changes == {"000001.SZ": (4.0, 8.0)}
    assert boundaries[0]["source_adj_factor"] == 2.0
    assert state.moneyflow_tail.iloc[0]["mf_net_amt"] == 10.0
    assert state.slow_tails["sector_data"].iloc[0]["l2_code_id"] == 1


@pytest.mark.parametrize("deferred_financing_tail", [False, True])
def test_official_factor_stage_produces_only_month_and_appends_all_native_aggregates(tmp_path, deferred_financing_tail):
    from backend.data_service.security_source_identity import DEFAULT_MANIFEST_PATH, load_security_source_identity_manifest
    from backend.services.dataset_release.build_stage import _patch_factor_component
    from backend.services.dataset_release.canonical_stock_transformer import build_qfq_denominator_authority
    from backend.services.dataset_release.contracts import Component, ComponentAction
    from backend.services.dataset_release.external_ordered_rows import OrderedMappingPartition
    from backend.services.dataset_release.factor_materializer import FACTOR_SOURCE_SCHEMAS
    from backend.services.dataset_release.pit import FrozenPitSnapshot, FrozenPitSpan

    prefix, _, _, _, old_path, _, _ = _inputs(tmp_path, "daily_pv", legacy_layout=True)
    factor = old_path.parent
    inventory_path = prefix.root / "factor-inventory.json"
    inventory = json.loads(inventory_path.read_text())
    for dataset in ("daily_basic", "moneyflow", "bak_basic", "cyq_perf", "sector_data", "margin_detail", "static_factors"):
        path = factor / f"{dataset}.{'parquet' if dataset == 'static_factors' else 'h5'}"
        frame = _frame(dataset, "2026-08-31")
        if dataset == "static_factors":
            frame.to_parquet(path)
        else:
            frame.to_hdf(path, "data", format="table", data_columns=["datetime", "instrument"])
        inventory["files"].append({"path": path.relative_to(prefix.root).as_posix(), "size": path.stat().st_size,
                                  "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    raw = canonical_json_bytes(inventory) + b"\n"
    inventory_path.write_bytes(raw)
    prefix.manifest["components"]["factor_content_manifest"].update(size=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    provider = prefix.root / "components/day"
    (provider / "instruments").mkdir(parents=True)
    (provider / "instruments/all.txt").write_text("000001.SZ\t2026-08-31\t2026-08-31\n")
    (provider / "meta_export.json").write_bytes(b"{}\n")
    prefix.manifest["components"]["day_meta_export"] = {"path": "components/day/meta_export.json", "size": 3, "sha256": hashlib.sha256(b"{}\n").hexdigest()}
    prefix.manifest.pop("dataset_manifest_sha256")
    prefix.manifest["dataset_manifest_sha256"] = hashlib.sha256(canonical_json_bytes(prefix.manifest)).hexdigest()
    raw = canonical_json_bytes(prefix.manifest) + b"\n"
    (prefix.root / "qe_dataset_manifest.json").write_bytes(raw)
    day = date(2026, 9, 30) if deferred_financing_tail else date(2026, 9, 1)
    month_start = day.replace(day=1)
    pit = FrozenPitSnapshot("aistock_equity_pit_canonical_v2", "shsz_a_252td_st_delist_asof_v2", date(2018, 8, 1), day,
        "a" * 64, "b" * 64, "c" * 64, "d" * 64,
        (FrozenPitSpan("000001.SZ", date(2026, 8, 31), day, None, None),))
    facts = {"adj_factor": [{"ts_code": "000001.SZ", "trade_date": date(2026, 8, 31), "adj_factor": 2.0},
                            {"ts_code": "000001.SZ", "trade_date": day, "adj_factor": 8.0}]}
    for dataset, columns in FACTOR_SOURCE_SCHEMAS.items():
        if dataset == "adj_factor":
            continue
        facts[dataset] = [{**dict.fromkeys(columns, 10.0), "trade_date": day, "ts_code": "000001.SZ"}]
    facts["daily_raw"] = [{"ts_code": "000001.SZ", "trade_date": day,
                          "open_li": 6000, "high_li": 6100, "low_li": 5900, "close_li": 6000,
                          "volume_hand": 1, "amount_li": 600000}]
    facts["sector_data"][0]["l2_code_id"] = 1
    if deferred_financing_tail:
        facts["margin_detail"][0]["trade_date"] = date(2026, 9, 29)
    calls = []
    def frames(dataset, partition_key, *, start, end, max_rows, instruments):
        calls.append((dataset, start, end))
        yield pd.DataFrame([row for row in facts[dataset] if start <= row["trade_date"] <= end])
    def ordered(component, dataset, *, date_ranges, instruments):
        return (OrderedMappingPartition(dataset, [row for row in facts[dataset]
            if any(start <= row["trade_date"] <= end for start, end in date_ranges)]),)
    source = SimpleNamespace(iter_factor_frames=frames, ordered_partitions=ordered,
        security_source_identity=load_security_source_identity_manifest(DEFAULT_MANIFEST_PATH),
        qfq_authority=build_qfq_denominator_authority(facts["adj_factor"], pit_snapshot=pit, cutoff=day),
        factor_partition_plan=lambda *, start: ({"partition_key": "2026-09", "start": month_start, "end": day},),
        qfq_source_summary={"source_precedence": "db_then_tushare_missing_keys_conflict_fail_v1", "overlap_mismatch_cells": 0},
        factor_overlay_summary={"source_precedence": "database_then_provider_missing_keys_conflict_fail_v1", "overlap_mismatch_cells": 0, "provider_override_rows": 0})
    staging = tmp_path / "staging"
    staging.mkdir()
    profile = SimpleNamespace(candidate_root=str(tmp_path), static_ordered_columns=STATIC_ORDERED_COLUMNS,
        pressure_ladder={"row_group_rows": (3,)}, resource_policy=SimpleNamespace(validation_read_chunk_rows=100_000))
    invocation = SimpleNamespace(staging_root=staging, pressure_rung=0, profile=profile,
        build_inputs={"source_bundle_sha256": "e" * 64, "monthly_legacy_predecessor": {
            "predecessor": {"candidate_root": str(prefix.root), "cutoff": "2026-08-31", "release_id": "august", "dataset_manifest_sha256": prefix.manifest["dataset_manifest_sha256"]},
            "predecessor_manifest_ref": {"id": "qe_dataset_manifest.json", "sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw), "dataset_manifest_sha256": prefix.manifest["dataset_manifest_sha256"]}}},
        plan={"actions": [{"component": Component.FACTOR_H5_STATIC.value, "action": ComponentAction.INCREMENTAL.value}]})
    old_bytes = old_path.read_bytes()
    receipt, adoption = _patch_factor_component(invocation, source=source, pit=pit, checkpoint=lambda: None)
    assert old_path.read_bytes() == old_bytes
    assert receipt["schema_version"] == "aistock_monthly_native_factor_materialization_v1"
    assert len(receipt["files"]) == 8 and receipt["publication_allowed"] is False
    assert all(start == month_start and end == day for _, start, end in calls)
    actual = pd.read_hdf(staging / "factor_bundle/daily_pv.h5", "data")
    assert len(actual) == 2 and actual.iloc[-1]["close"] == 6.0
    assert actual.iloc[0]["close"] == 10.0000000001 / 2
    assert adoption["historical_business_audit_performed"] is False
    from backend.services.dataset_release.monthly_legacy_validation import validate_aggregate_month_receipt
    verification = validate_aggregate_month_receipt(root=staging, receipt=receipt, component="factor", cutoff=day,
        predecessor_manifest_sha256=prefix.manifest["dataset_manifest_sha256"], source_bundle_sha256="e" * 64)
    assert len(verification["verified_output_files"]) == 10
    assert verification["historical_values_read"] == 0
    assert (staging / "factor_bundle/security_source_identity.json").read_bytes() == DEFAULT_MANIFEST_PATH.read_bytes()
    assert receipt["alias_coverage"]["validation_scope"] == "month_delta"
    assert receipt["alias_coverage"]["month_start"] == "2026-09-01"
    assert not (staging / ".factor-month/factor_source_chunks/.alias-month-readback.h5").exists()
    if deferred_financing_tail:
        margin = pd.read_hdf(staging / "factor_bundle/margin_detail.h5", "data")
        assert margin.index.get_level_values("datetime").max() == pd.Timestamp("2026-09-29")
        assert day not in margin.index.get_level_values("datetime").date
    import copy
    drifted = copy.deepcopy(receipt)
    repair_inputs = {"factor_prefixes": {"static_factors": {"dataset_manifest_sha256": "f" * 64}}}
    drifted["monthly_repair_inputs"] = repair_inputs
    with pytest.raises(Exception, match="QA is incomplete"):
        validate_aggregate_month_receipt(root=staging, receipt=drifted, component="factor", cutoff=day,
            predecessor_manifest_sha256=prefix.manifest["dataset_manifest_sha256"], source_bundle_sha256="e" * 64,
            repair_inputs=repair_inputs)
    receipt["files"][0]["month_output_values_verified"] = False
    with pytest.raises(Exception, match="QA is incomplete"):
        validate_aggregate_month_receipt(root=staging, receipt=receipt, component="factor", cutoff=day,
            predecessor_manifest_sha256=prefix.manifest["dataset_manifest_sha256"], source_bundle_sha256="e" * 64)


def _inputs(tmp_path, dataset, *, legacy_layout=False):
    root = tmp_path / "august"
    factor = root / "components/factors"
    factor.mkdir(parents=True)
    (factor / "meta.json").write_bytes(b"{}\n")
    old = _frame(dataset, "2026-08-31")
    if dataset == "daily_pv":
        old["factor"] = pd.Series(0.5, index=old.index, dtype="float32")
        old["volume"] = pd.Series(100.0, index=old.index, dtype="float32")
    elif dataset == "daily_basic":
        old["db_dv_ratio"] = float("nan")
        old["db_dv_ratio"] = old["db_dv_ratio"].astype("float32")
    if legacy_layout and dataset == "daily_pv":
        old = old.astype("float64")
        old["close"] = 10.0000000001
    if legacy_layout and dataset == "sector_data":
        old = old.loc[:, ["l2_code_id", *[name for name in old.columns if name != "l2_code_id"]]]
    extension = "parquet" if dataset == "static_factors" else "h5"
    old_path = factor / f"{dataset}.{extension}"
    if extension == "h5":
        old.to_hdf(old_path, key="data", format="table", data_columns=["datetime", "instrument"], index=False)
    else:
        old.to_parquet(old_path)
    inventory = {"schema_version": "qe_factor_component_pin_inventory_v1", "files": [
        {"path": old_path.relative_to(root).as_posix(), "size": old_path.stat().st_size, "sha256": hashlib.sha256(old_path.read_bytes()).hexdigest()},
    ]}
    inventory_path = root / "factor-inventory.json"
    inventory_path.write_bytes(canonical_json_bytes(inventory) + b"\n")
    manifest = {"release_id": "august", "cutoff_trade_date": "2026-08-31", "components": {
        key: {"path": path.relative_to(root).as_posix(), "size": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        for key, path in (("factor_meta", factor / "meta.json"), ("factor_content_manifest", inventory_path))
    }}
    manifest["dataset_manifest_sha256"] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    raw = canonical_json_bytes(manifest) + b"\n"
    (root / "qe_dataset_manifest.json").write_bytes(raw)
    prefix = load_legacy_prefix(root, expected_manifest_sha256=manifest["dataset_manifest_sha256"],
                                expected_file_sha256=hashlib.sha256(raw).hexdigest(), expected_cutoff=date(2026, 8, 31), expected_release_id="august")
    source = tmp_path / "source"
    source.mkdir()
    new = _frame(dataset, "2026-09-01")
    if dataset == "daily_pv":
        for name in ("open", "high", "low", "close"):
            new[name] = pd.Series(6.0, index=new.index, dtype="float32")
        new["factor"] = pd.Series(1.0, index=new.index, dtype="float32")
        new["volume"] = pd.Series(100.0, index=new.index, dtype="float32")
    chunk = source / "september.parquet"
    new.to_parquet(chunk, row_group_size=1)
    sealed = SealedFactorChunk(dataset, "2026-09", chunk.name, hashlib.sha256(chunk.read_bytes()).hexdigest(), len(new), tuple(new.columns))
    target = tmp_path / "september/factor_bundle"
    target.mkdir(parents=True)
    return prefix, source, sealed, target, old_path, old, new


def _append(tmp_path, dataset):
    from backend.services.dataset_release.monthly_legacy_factor import append_legacy_factor_aggregate
    prefix, source, sealed, target, old_path, old, new = _inputs(tmp_path, dataset)
    before = old_path.read_bytes()
    receipt = append_legacy_factor_aggregate(prefix, dataset=dataset, source_root=source,
        chunks=(sealed,), target_root=target, target_cutoff=date(2026, 9, 30),
        source_receipt_sha256="e" * 64, qfq_basis_changes={"000001.SZ": (4.0, 8.0)}, max_rows=1)
    assert old_path.read_bytes() == before
    path = target / f"{dataset}.{'parquet' if dataset == 'static_factors' else 'h5'}"
    actual = pd.read_parquet(path) if dataset == "static_factors" else pd.read_hdf(path, "data")
    assert receipt["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert receipt["inherited_rows"] == 1 and receipt["month_rows"] == 1
    assert receipt["historical_business_audit_performed"] is False
    assert receipt["publication_allowed"] is False
    return actual, old, new


def test_native_factor_price_append_reanchors_only_qfq_fields(tmp_path):
    actual, _, new = _append(tmp_path, "daily_pv")
    assert actual.iloc[0]["close"] == 5.0
    assert actual.iloc[0]["factor"] == 0.25
    assert actual.iloc[0]["volume"] == 200.0
    assert actual.iloc[0]["amount"] == 10.0
    pd.testing.assert_frame_equal(actual.iloc[1:], new)


def test_month_writer_combines_exact_null_repairs_with_new_tail_and_keeps_two_identities(tmp_path):
    from backend.services.dataset_release.monthly_legacy_factor import append_legacy_factor_aggregate
    prefix, source, sealed, target, old_path, old, new = _inputs(tmp_path, "daily_basic")
    with pd.HDFStore(old_path, "a") as store:
        store.create_table_index("data", columns=["datetime", "instrument"])
    # Fixture is pinned AFTER building its indexes; the immutable input is
    # never changed by the writer under test.
    inventory_path = prefix.root / "factor-inventory.json"
    inventory = json.loads(inventory_path.read_text())
    inventory["files"][0].update(size=old_path.stat().st_size, sha256=hashlib.sha256(old_path.read_bytes()).hexdigest())
    raw = canonical_json_bytes(inventory) + b"\n"
    inventory_path.write_bytes(raw)
    prefix.manifest["components"]["factor_content_manifest"].update(size=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    provider = source / "approved-fields.parquet"
    pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260831"], "dv_ratio": [1.2345]}).to_parquet(provider)
    before = old_path.read_bytes()
    result = append_legacy_factor_aggregate(prefix, dataset="daily_basic", source_root=source,
        chunks=(sealed,), target_root=target, target_cutoff=date(2026, 9, 30), source_receipt_sha256="e" * 64,
        logical_predecessor_manifest_sha256="a" * 64, exact_null_repairs=[{
            "trade_date": "2026-08-31", "source_path": str(provider), "provider_sha256": hashlib.sha256(provider.read_bytes()).hexdigest(),
            "fields": ["db_dv_ratio"]}])
    actual = pd.read_hdf(target / "daily_basic.h5", "data")
    expected = old.copy()
    expected["db_dv_ratio"] = pd.Series(1.2345, index=old.index, dtype="float32")
    pd.testing.assert_frame_equal(actual, pd.concat([expected, new]))
    assert old_path.read_bytes() == before
    assert result["physical_prefix_manifest_sha256"] == prefix.manifest_sha256
    assert result["predecessor_manifest_sha256"] == "a" * 64
    assert result["exact_field_repair_readback"]["filled_cells"] == {"db_dv_ratio": 1}


@pytest.mark.parametrize("dataset", ["daily_basic", "moneyflow", "sector_data", "static_factors"])
def test_native_factor_append_preserves_legacy_facts_and_legal_nan(tmp_path, dataset):
    actual, old, new = _append(tmp_path, dataset)
    pd.testing.assert_frame_equal(actual, pd.concat([old, new]))


def test_native_factor_rejects_old_dates_in_month_chunk_without_publishing(tmp_path):
    from backend.services.dataset_release.monthly_legacy_factor import append_legacy_factor_aggregate
    prefix, source, sealed, target, _, old, _ = _inputs(tmp_path, "daily_basic")
    chunk = source / sealed.relative_path
    old.to_parquet(chunk, row_group_size=1)
    sealed = SealedFactorChunk(sealed.dataset, sealed.partition_key, sealed.relative_path,
                               hashlib.sha256(chunk.read_bytes()).hexdigest(), len(old), tuple(old.columns))
    with pytest.raises(Exception, match="month"):
        append_legacy_factor_aggregate(prefix, dataset="daily_basic", source_root=source,
            chunks=(sealed,), target_root=target, target_cutoff=date(2026, 9, 30), source_receipt_sha256="e" * 64, max_rows=1)
    assert not (target / "daily_basic.h5").exists()


def test_native_factor_month_readback_detects_writer_drift_before_publish(tmp_path, monkeypatch):
    from backend.services.dataset_release.monthly_legacy_factor import append_legacy_factor_aggregate
    prefix, source, sealed, target, _, _, _ = _inputs(tmp_path, "daily_basic")
    append = pd.HDFStore.append
    def corrupted(store, key, value, *args, **kwargs):
        value = value.copy()
        value["db_turnover_rate"] += 0.001
        return append(store, key, value, *args, **kwargs)
    monkeypatch.setattr(pd.HDFStore, "append", corrupted)
    with pytest.raises(Exception, match="month readback differs"):
        append_legacy_factor_aggregate(prefix, dataset="daily_basic", source_root=source,
            chunks=(sealed,), target_root=target, target_cutoff=date(2026, 9, 30), source_receipt_sha256="e" * 64, max_rows=1)
    assert not (target / "daily_basic.h5").exists()


@pytest.mark.parametrize("dataset", ["daily_pv", "sector_data"])
def test_native_factor_accepts_actual_legacy_precision_and_order_without_rewriting(tmp_path, dataset):
    from backend.services.dataset_release.monthly_legacy_factor import append_legacy_factor_aggregate
    prefix, source, sealed, target, old_path, old, new = _inputs(tmp_path, dataset, legacy_layout=True)
    before = old_path.read_bytes()
    receipt = append_legacy_factor_aggregate(prefix, dataset=dataset, source_root=source,
        chunks=(sealed,), target_root=target, target_cutoff=date(2026, 9, 30), source_receipt_sha256="e" * 64, max_rows=1)
    actual = pd.read_hdf(target / f"{dataset}.h5", "data")
    assert old_path.read_bytes() == before
    assert tuple(actual.columns) == tuple(old.columns)
    assert dict(actual.dtypes) == dict(old.dtypes)
    pd.testing.assert_frame_equal(actual.iloc[:1], old)
    pd.testing.assert_frame_equal(actual.iloc[1:], new.loc[:, list(old.columns)].astype(old.dtypes.to_dict()))
    assert receipt["storage_columns"] == list(old.columns)


def test_indexed_tail_reads_latest_actual_observations_without_scanning_history(tmp_path):
    from backend.services.dataset_release.monthly_legacy_factor import read_legacy_factor_tail
    frames = []
    for day in pd.date_range("2026-08-01", "2026-08-31"):
        frames.append(_frame("daily_basic", day))
    sparse = _frame("daily_basic", "2026-08-03")
    sparse.index = pd.MultiIndex.from_tuples([(pd.Timestamp("2026-08-03"), "000002.SZ")], names=("datetime", "instrument"))
    frame = pd.concat([*frames, sparse]).sort_index()
    path = tmp_path / "daily_basic.h5"
    frame.to_hdf(path, "data", format="table", data_columns=["datetime", "instrument"])
    progress = []
    tail = read_legacy_factor_tail(path, instruments=("000001.SZ", "000002.SZ", "000003.SZ"),
        rows_per_instrument=2, before=date(2026, 9, 1), max_rows=3, progress=progress.append)
    assert len(tail) == 3
    assert list(tail.xs("000001.SZ", level="instrument").index) == list(pd.to_datetime(["2026-08-30", "2026-08-31"]))
    assert tail.xs("000002.SZ", level="instrument").index[0] == pd.Timestamp("2026-08-03")
    assert progress[-1]["physical_rows_read"] < len(frame)
    assert progress[-1]["indexed_tail_requests"] == 2


def test_legacy_price_basis_conversion_preserves_real_float64_precision(tmp_path):
    from backend.services.dataset_release.monthly_legacy_factor import append_legacy_factor_aggregate
    prefix, source, sealed, target, _, old, _ = _inputs(tmp_path, "daily_pv", legacy_layout=True)
    append_legacy_factor_aggregate(prefix, dataset="daily_pv", source_root=source,
        chunks=(sealed,), target_root=target, target_cutoff=date(2026, 9, 30),
        source_receipt_sha256="e" * 64, qfq_basis_changes={"000001.SZ": (4.0, 8.0)}, max_rows=1)
    actual = pd.read_hdf(target / "daily_pv.h5", "data")
    assert actual.iloc[0]["close"] == old.iloc[0]["close"] * 0.5
    assert actual.iloc[0]["close"] != 5.0
    assert actual["close"].dtype == "float64"


def test_factor_writer_rejects_target_inside_frozen_source(tmp_path):
    from backend.services.dataset_release.monthly_legacy_factor import append_legacy_factor_aggregate
    prefix, source, sealed, _, _, _, _ = _inputs(tmp_path, "daily_basic")
    target = source / "output"
    target.mkdir()
    with pytest.raises(Exception, match="overlaps sealed"):
        append_legacy_factor_aggregate(prefix, dataset="daily_basic", source_root=source,
            chunks=(sealed,), target_root=target, target_cutoff=date(2026, 9, 30), source_receipt_sha256="e" * 64)
    assert not (target / "daily_basic.h5").exists()
