from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
import hashlib
import struct
from types import SimpleNamespace

import pytest

import backend.services.dataset_release.build_stage as stages
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.canonical_stock_transformer import build_qfq_denominator_authority
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.contracts import Component, ComponentAction
from backend.services.dataset_release.daily_minute_materializer import QlibDumpToolchain
from backend.services.dataset_release.external_ordered_rows import OrderedMappingPartition
from backend.services.dataset_release.monthly_mature_build_runner import MatureMonthlyPhysicalBuildRunner
from backend.services.dataset_release.monthly_worker import ProducerContext
from backend.services.dataset_release.minute_overlay import canonical_session_times
from backend.services.dataset_release.pit import FrozenPitSnapshot, FrozenPitSpan
from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS


def _setup(tmp_path, *, dataset="daily_bin", metadata=None):
    cutoff = date(2026, 9, 1)
    parent = tmp_path / "august"
    frequency = "day" if dataset == "daily_bin" else "1min"
    meta_key = "day_meta_export" if dataset == "daily_bin" else "minute_meta_export"
    provider = parent / f"components/{frequency}"
    (provider / "features/000001.sz").mkdir(parents=True)
    (provider / "calendars").mkdir()
    (provider / "instruments").mkdir()
    meta_bytes = canonical_json_bytes(metadata or {}) + b"\n"
    (provider / "meta_export.json").write_bytes(meta_bytes)
    old_stamp = "2026-08-31" if frequency == "day" else "2026-08-31 15:00:00"
    (provider / f"calendars/{frequency}.txt").write_text(old_stamp + "\n", encoding="utf-8")
    (provider / "instruments/all.txt").write_text("000001.SZ\t2026-08-31\t2026-08-31\n", encoding="utf-8")
    for field in QLIB_STOCK_FIELDS:
        value = 0.5 if field == "factor" else 100.0 if field == "volume" else 10.0
        (provider / f"features/000001.sz/{field}.{frequency}.bin").write_bytes(struct.pack("<ff", 0, value))
    manifest = {"release_id": "august", "cutoff_trade_date": "2026-08-31", "components": {
        meta_key: {"path": f"components/{frequency}/meta_export.json", "size": len(meta_bytes), "sha256": hashlib.sha256(meta_bytes).hexdigest()},
    }}
    manifest["dataset_manifest_sha256"] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    raw = canonical_json_bytes(manifest) + b"\n"
    (parent / "qe_dataset_manifest.json").write_bytes(raw)
    predecessor = {"candidate_root": str(parent), "cutoff": "2026-08-31", "release_id": "august", "dataset_manifest_sha256": manifest["dataset_manifest_sha256"]}
    manifest_ref = {"id": "qe_dataset_manifest.json", "sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw), "dataset_manifest_sha256": manifest["dataset_manifest_sha256"]}
    pit = FrozenPitSnapshot(
        "aistock_equity_pit_canonical_v2", "shsz_a_252td_st_delist_asof_v2", date(2018, 8, 1), cutoff,
        "a" * 64, "b" * 64, "c" * 64, "d" * 64,
        (FrozenPitSpan("000001.SZ", date(2026, 8, 31), cutoff, None, None),),
    )
    data = {
        "adj_factor": [{"ts_code": "000001.SZ", "trade_date": date(2026, 8, 31), "adj_factor": 2.0}, {"ts_code": "000001.SZ", "trade_date": cutoff, "adj_factor": 8.0}],
        "kline_daily_raw": [{"ts_code": "000001.SZ", "trade_date": cutoff, "open_li": 6000, "high_li": 6100, "low_li": 5900, "close_li": 6000, "volume_hand": 1, "amount_li": 600000}],
        "stk_limit": [{"ts_code": "000001.SZ", "trade_date": cutoff, "up_limit": 6.6, "down_limit": 5.4, "pre_close": 6.0}],
        "suspend_d": [],
    }
    calls = []
    if dataset == "minute_bin":
        data["kline_minute_raw"] = [
            {**{key: value for key, value in data["kline_daily_raw"][0].items() if key != "trade_date"},
             "trade_time": stamp, "freq": "1m"}
            for stamp in canonical_session_times(cutoff)
        ]
    def partitions(component, dataset, *, date_ranges=(), instruments=(), **_kwargs):
        calls.append((dataset, date_ranges, instruments))
        def row_date(row):
            return row["trade_time"].date() if "trade_time" in row else row["trade_date"]
        rows = [row for row in data.get(dataset, ()) if (not instruments or row["ts_code"] in instruments) and
                (not date_ranges or any(start <= row_date(row) <= end for start, end in date_ranges))]
        return (OrderedMappingPartition(dataset, rows),)
    source = SimpleNamespace(
        qfq_authority=build_qfq_denominator_authority(data["adj_factor"], pit_snapshot=pit, cutoff=cutoff),
        trading_days=lambda: (date(2026, 8, 31), cutoff), ordered_partitions=partitions,
    )
    staging = tmp_path / ".staging/september.building"
    staging.mkdir(parents=True)
    index_csv = staging / "index_context/index_csv"
    index_csv.mkdir(parents=True)
    with (index_csv / "000300.SH.csv").open("w", encoding="utf-8", newline="") as writer:
        csv_writer = csv.DictWriter(writer, fieldnames=("date", "symbol", *QLIB_STOCK_FIELDS))
        csv_writer.writeheader()
        csv_writer.writerow({"date": cutoff.isoformat(), "symbol": "000300.SH", **dict.fromkeys(QLIB_STOCK_FIELDS, 100.0), "factor": 1.0, "limit_up": 0.0, "limit_down": 0.0})
    cas = CASStore(ControlStore.initialize(tmp_path / "control").root)
    profile = SimpleNamespace(candidate_root=str(tmp_path), start_date=date(2018, 8, 1), minute_start_date=date(2024, 1, 2),
                              index_codes=("000300.SH",),
                              pressure_ladder={"dump_workers": (1,)}, stage_timeouts_seconds={"qlib_dump": 3600})
    invocation = SimpleNamespace(
        build_inputs={"monthly_legacy_predecessor": {"predecessor": predecessor, "predecessor_manifest_ref": manifest_ref}, "source_bundle_sha256": "e" * 64},
        staging_root=staging, profile=profile, cas=cas, project_root=tmp_path, pressure_rung=0,
        plan={"actions": [{"component": component.value, "action": ComponentAction.INCREMENTAL.value} for component in Component]},
    )
    toolchain = QlibDumpToolchain("Ubuntu", "/conda.sh", "rdagent-gpu", "/dump.py", tmp_path / "dump.py", "a" * 64,
                                 "python", "/guardian.py", tmp_path / "guardian.py", "b" * 64,
                                 "/heartbeat.json", "python", "/runner.py", tmp_path / "runner.py", "c" * 64)
    return invocation, source, pit, toolchain, provider, data, calls


def test_real_month_prepare_generates_native_operation_without_old_csv_lineage(tmp_path):
    invocation, source, pit, toolchain, _, _, calls = _setup(tmp_path)
    prepared = stages._prepare_bin_patch(invocation, source=source, pit=pit, dataset="daily_bin", toolchain=toolchain, checkpoint=lambda: None)
    assert prepared["schema_version"] == "aistock_monthly_legacy_bin_patch_preparation_v1"
    operation = prepared["qlib_dump_operations"][0]
    payload = invocation.cas.get_json(operation["preparation_ref"])
    assert operation["mode"] == "inherited_month"
    assert payload["qfq_basis_changes"]["000001.SZ"] == [4.0, 8.0]
    assert payload["csv_refs"][0]["size"] > 0
    with (invocation.staging_root / payload["csv_refs"][0]["id"]).open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 1 and float(rows[0]["close"]) == 6.0
    assert all(start.year == 2026 and start.month == 9 for name, ranges, _ in calls if name != "adj_factor" for start, _ in ranges)
    assert not (invocation.staging_root / "daily_bin").exists()


def test_month_prepare_appends_all_declared_index_csv_not_only_csi300(tmp_path):
    invocation, source, pit, toolchain, _, _, _ = _setup(tmp_path)
    invocation.profile.index_codes = ("000300.SH", "000905.SH")
    csv_root = invocation.staging_root / "index_context/index_csv"
    original = (csv_root / "000300.SH.csv").read_text(encoding="utf-8")
    (csv_root / "000905.SH.csv").write_text(original.replace("000300.SH", "000905.SH"), encoding="utf-8")
    prepared = stages._prepare_bin_patch(invocation, source=source, pit=pit, dataset="daily_bin", toolchain=toolchain, checkpoint=lambda: None)
    payload = invocation.cas.get_json(prepared["qlib_dump_operations"][0]["preparation_ref"])
    assert {Path(row["id"]).stem for row in payload["csv_refs"]} == {"000001.SZ", "000300.SH", "000905.SH"}


def test_native_month_prepare_keeps_missing_boundary_source_fact_unresolved(tmp_path):
    invocation, source, pit, toolchain, _, data, _ = _setup(tmp_path)
    data["adj_factor"] = data["adj_factor"][1:]
    with pytest.raises(stages.CandidateBuildStageError, match="anchor"):
        stages._prepare_bin_patch(invocation, source=source, pit=pit, dataset="daily_bin", toolchain=toolchain, checkpoint=lambda: None)
    assert not (invocation.staging_root / "daily_bin").exists()


def _append_and_finalize(tmp_path, *, mutate_child=None, dataset="daily_bin", metadata=None, indices=None):
    invocation, source, pit, toolchain, provider, _, _ = _setup(tmp_path, dataset=dataset, metadata=metadata)
    if indices is not None:
        invocation.profile.index_codes = indices
        csv_root = invocation.staging_root / "index_context/index_csv"
        original = (csv_root / "000300.SH.csv").read_text(encoding="utf-8")
        for code in indices:
            if code != "000300.SH":
                (csv_root / f"{code}.csv").write_text(original.replace("000300.SH", code), encoding="utf-8")
    prepared = stages._prepare_bin_patch(invocation, source=source, pit=pit, dataset=dataset, toolchain=toolchain, checkpoint=lambda: None)
    runner = MatureMonthlyPhysicalBuildRunner(
        profile=invocation.profile, cas=invocation.cas, project_root=tmp_path,
        qlib_writer=SimpleNamespace(), consumer_smoke=SimpleNamespace(), finalizer=SimpleNamespace(),
    )
    context = ProducerContext("BUILD", "dmr_" + "1" * 32, 1, {}, {
        **invocation.build_inputs["monthly_legacy_predecessor"], "target_cutoff": pit.cutoff.isoformat(),
    }, {})
    child = runner._append_inherited_month(context=context, staging_root=invocation.staging_root,
                                           compiled=SimpleNamespace(source_bundle_sha256="e" * 64),
                                           operation=prepared["qlib_dump_operations"][0])
    if mutate_child:
        mutate_child(child)
    invocation.prerequisites = {"qlib_dump_" + dataset.removesuffix("_bin"): invocation.cas.put_json(child).sha256}
    receipt = stages._finalize_bin_patch(invocation, dataset=dataset, pit=pit, toolchain=toolchain,
                                        preparation=prepared, checkpoint=lambda: None)
    return invocation, provider, receipt


def test_native_daily_chain_supports_official_csi_provider_codes(tmp_path):
    from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
    from backend.services.dataset_release.monthly_legacy_validation import validate_qlib_month

    codes = tuple(item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS)
    invocation, _, receipt = _append_and_finalize(tmp_path, indices=codes)
    actual = invocation.staging_root / "daily_bin/qlib/features/000985.csi/close.day.bin"
    assert struct.unpack("<ff", actual.read_bytes()) == (1, 100)
    report = validate_qlib_month(
        root=invocation.staging_root, cas=invocation.cas, materialization=receipt,
        cutoff=date(2026, 9, 1),
        predecessor_manifest_sha256=invocation.build_inputs["monthly_legacy_predecessor"]["predecessor"]["dataset_manifest_sha256"],
        source_bundle_sha256="e" * 64,
    )
    assert report["validated_rows"] == 13
    assert report["historical_values_read"] == 0


def test_native_finalize_preserves_end_dates_for_stocks_without_month_append(tmp_path):
    metadata = {"last_end_dates": {"000001.SZ": "2026-08-31", "000005.SZ": "2024-04-11"}}
    invocation, provider, _ = _append_and_finalize(tmp_path, metadata=metadata)
    actual = stages._load_component_json(invocation.staging_root / "daily_bin/qlib/meta_export.json")
    assert actual["last_end_dates"]["000001.SZ"] == "2026-09-01"
    assert actual["last_end_dates"]["000005.SZ"] == "2024-04-11"
    assert stages._load_component_json(provider / "meta_export.json") == metadata


def test_prepare_native_write_and_finalize_keep_real_qfq_and_raw_fields(tmp_path):
    invocation, provider, receipt = _append_and_finalize(tmp_path)
    target = invocation.staging_root / "daily_bin/qlib"
    assert struct.unpack("<fff", (target / "features/000001.sz/close.day.bin").read_bytes()) == (0, 5, 6)
    assert struct.unpack("<fff", (target / "features/000001.sz/factor.day.bin").read_bytes()) == (0, 0.25, 1)
    assert struct.unpack("<fff", (target / "features/000001.sz/volume.day.bin").read_bytes()) == (0, 200, 100)
    assert struct.unpack("<fff", (target / "features/000001.sz/prev_close.day.bin").read_bytes()) == (0, 10, 6)
    assert struct.unpack("<ff", (provider / "features/000001.sz/close.day.bin").read_bytes()) == (0, 10)
    assert (provider / "meta_export.json").read_bytes() == b"{}\n"
    assert stages._load_component_json(target / "meta_export.json")["end"] == "2026-09-01"
    assert receipt["publication_allowed"] is False
    assert receipt["historical_business_audit_performed"] is False
    assert stages._load_component_json(invocation.staging_root / "daily_bin/materialization_receipt.json") == receipt


def test_real_native_minute_chain_has_exactly_240_closing_bars_and_immutable_prefix(tmp_path):
    invocation, provider, receipt = _append_and_finalize(tmp_path, dataset="minute_bin")
    target = invocation.staging_root / "minute_bin/qlib"
    calendar = (target / "calendars/1min.txt").read_text(encoding="utf-8").splitlines()
    assert len(calendar) == 241
    assert calendar[1] == "2026-09-01 09:31:00"
    assert calendar[120] == "2026-09-01 11:30:00"
    assert calendar[121] == "2026-09-01 13:01:00"
    assert calendar[-1] == "2026-09-01 15:00:00"
    values = struct.unpack("<242f", (target / "features/000001.sz/close.1min.bin").read_bytes())
    assert values[:2] == (0.0, 5.0)
    assert values[2:] == (6.0,) * 240
    assert struct.unpack("<ff", (provider / "features/000001.sz/close.1min.bin").read_bytes()) == (0, 10)
    assert receipt["csv"]["rows"] == 240


@pytest.mark.parametrize("fault", ["source", "prefix", "preparation", "target"])
def test_native_finalize_rejects_foreign_receipt_without_writing_signoff(tmp_path, fault):
    def mutate(child):
        if fault == "source":
            child["source_receipt_sha256"] = "f" * 64
        elif fault == "prefix":
            child["predecessor_manifest_sha256"] = "f" * 64
        elif fault == "preparation":
            child["preparation_ref"]["sha256"] = "f" * 64
        else:
            child["receipt"]["target_root"] = str(tmp_path / "other")
    with pytest.raises(stages.CandidateBuildStageError):
        _append_and_finalize(tmp_path, mutate_child=mutate)
    assert not (tmp_path / ".staging/september.building/daily_bin/materialization_receipt.json").exists()
