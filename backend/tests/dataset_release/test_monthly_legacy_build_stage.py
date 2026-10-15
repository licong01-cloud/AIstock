from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
import hashlib
import json
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
from backend.services.dataset_release import monthly_quote_catalog as catalog
from backend.services.dataset_release.monthly_build_resume import _atomic, _signature
from backend.services.dataset_release.monthly_legacy_validation import MonthlyLegacyValidationError, validate_qlib_month


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
    assert receipt["native_writer"]["quote_catalog"]["mode"] == "physical_quote_catalog_v1"
    assert (target / "instruments/all.txt").read_text(encoding="utf-8") == (
        "000001.SZ\t2026-08-31 15:00:00\t2026-09-01 15:00:00\n"
    )


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


def _retained(tmp_path):
    invocation, _, materialization = _append_and_finalize(tmp_path, dataset="minute_bin")
    root, cas = invocation.staging_root, invocation.cas
    provider = root / "minute_bin/qlib"
    # This quote-only stock is absent from the monthly PIT population/CSVs.
    extra = provider / "features/000698.sz"
    extra.mkdir()
    (extra / "close.1min.bin").write_bytes((provider / "features/000001.sz/close.1min.bin").read_bytes())
    old = b"000001.SZ\t2026-08-31\t2026-08-31\n"
    (provider / "instruments/all.txt").write_bytes(old)
    native = materialization["native_writer"]
    native.pop("quote_catalog", None)
    native["instruments_sha256"] = hashlib.sha256(old).hexdigest()
    native["instruments_signature"] = list(_signature(provider / "instruments/all.txt"))
    _atomic(root / "minute_bin/materialization_receipt.json", materialization)
    ref = cas.put_json(materialization).as_dict()
    finalized = dict(finalize_receipt_ref=cas.put_json(dict(minute_receipt_ref=ref,
                    minute_materialization_receipt_file_ref=ref)).as_dict())
    journal = root.parent / "resume.json"
    record = dict(binding_digest="a" * 64,
                  steps={"finalize-bins": cas.put_json(finalized).as_dict()},
                  files={relative: list(_signature(root / relative)) for relative in catalog._FILES if relative != catalog._META})
    _atomic(journal, record)
    resume = SimpleNamespace(root=root, path=journal, record=record,
                             context=SimpleNamespace(checkpoint=lambda: None))
    prices = {p: (p.read_bytes(), _signature(p)) for p in provider.glob("features/*/*.bin")}
    return invocation, resume, finalized, prices


def test_retained_catalog_repair_preserves_prices_and_is_idempotent(tmp_path):
    invocation, resume, finalized, prices = _retained(tmp_path)
    repaired = catalog.refresh_retained_quote_catalog(resume, finalized=finalized, cas=invocation.cas)
    actual = (resume.root / catalog._CATALOG).read_text(encoding="utf-8")
    assert "000698.SZ\t2026-08-31 15:00:00\t2026-09-01 15:00:00" in actual
    assert all((p.read_bytes(), _signature(p)) == value for p, value in prices.items())
    assert catalog.refresh_retained_quote_catalog(resume, finalized=repaired, cas=invocation.cas) == repaired
    assert invocation.cas.get_json(finalized["finalize_receipt_ref"]) != invocation.cas.get_json(repaired["finalize_receipt_ref"])
    assert json.loads(resume.path.read_text()) == resume.record
    updated = invocation.cas.get_json(repaired["finalize_receipt_ref"])
    materialization = invocation.cas.get_json(updated["minute_receipt_ref"])
    assert "quote_catalog" not in materialization["native_writer"]  # Original writer proof stays honest.
    result = validate_qlib_month(root=resume.root, materialization=materialization, cas=invocation.cas,
        cutoff=date(2026, 9, 1), predecessor_manifest_sha256=materialization["predecessor_manifest_sha256"],
        source_bundle_sha256="e" * 64)
    assert result["status"] == "PASS"


@pytest.mark.parametrize("interrupted_replace", [1, 2, 3])
def test_retained_catalog_recovers_each_interrupted_small_file_commit(tmp_path, monkeypatch, interrupted_replace):
    invocation, resume, finalized, prices = _retained(tmp_path)
    original, count = catalog._replace, 0

    def interrupted(path, payload):
        nonlocal count
        original(path, payload)
        count += 1
        if count == interrupted_replace:
            raise OSError("simulated interruption after atomic replacement")

    monkeypatch.setattr(catalog, "_replace", interrupted)
    with pytest.raises(OSError, match="interruption"):
        catalog.refresh_retained_quote_catalog(resume, finalized=finalized, cas=invocation.cas)
    monkeypatch.setattr(catalog, "_replace", original)
    record = catalog.recover_quote_catalog(root=resume.root, journal=resume.path, record=resume.record, cas=invocation.cas)
    assert "000698.SZ" in (resume.root / catalog._CATALOG).read_text()
    assert json.loads(resume.path.read_text()) == record
    assert not catalog._pending(resume.root).exists()
    assert all((p.read_bytes(), _signature(p)) == value for p, value in prices.items())


@pytest.mark.parametrize("fault", ["catalog", "input", "mode"])
def test_monthly_validation_rejects_catalog_and_provenance_drift(tmp_path, fault):
    invocation, _, materialization = _append_and_finalize(tmp_path, dataset="minute_bin")
    authority = materialization["native_writer"]["quote_catalog"]
    if fault == "catalog":
        (invocation.staging_root / catalog._CATALOG).write_bytes(b"000001.SZ\t2026-09-01\t2026-09-01\n")
    elif fault == "input":
        authority["input_instruments_sha256"] = "f" * 64
    else:
        authority["mode"] = "pit_universe_spans"
    with pytest.raises(MonthlyLegacyValidationError, match="catalog"):
        validate_qlib_month(root=invocation.staging_root, materialization=materialization,
            cas=invocation.cas, cutoff=date(2026, 9, 1),
            predecessor_manifest_sha256=materialization["predecessor_manifest_sha256"], source_bundle_sha256="e" * 64)


def _retained_prev_close(tmp_path, dataset="daily_bin", repair_day="2026-08-31"):
    from backend.services.dataset_release import monthly_prev_close_repair as correction
    invocation, provider, materialization = _append_and_finalize(tmp_path, dataset=dataset)
    root, cas = invocation.staging_root, invocation.cas
    prefix, freq = dataset.removesuffix("_bin"), "day" if dataset == "daily_bin" else "1min"
    finalized = {"finalize_receipt_ref": cas.put_json({f"{prefix}_receipt_ref": cas.put_json(materialization).as_dict()}).as_dict()}
    journal = root.parent / "resume.json"
    files = {p.relative_to(root).as_posix(): list(_signature(p)) for p in (root / dataset).rglob("*") if p.is_file()}
    record = {"binding_digest": "a" * 64, "steps": {"finalize-bins": cas.put_json(finalized).as_dict()}, "files": files}
    _atomic(journal, record)
    facts = [{"ts_code": "000001.SZ", "trade_date": repair_day, "pre_close": "11.0", "up_limit": "12.1", "down_limit": "9.9"}]
    authority = cas.put_json({"schema_version": correction.SCHEMA, "facts": facts}).as_dict()
    binding = {"authority_ref": authority, "target_cutoff": "2026-09-01", "operation_id": "dmr_" + "1" * 32,
               "predecessor_manifest_sha256": materialization["predecessor_manifest_sha256"]}
    plan = {"target_cutoff": "2026-09-01", correction.BINDING: binding,
            "predecessor": {"dataset_manifest_sha256": materialization["predecessor_manifest_sha256"]}}
    resume = SimpleNamespace(root=root, path=journal, record=record,
                             context=SimpleNamespace(operation_id=binding["operation_id"], checkpoint=lambda: None))
    return invocation, provider, materialization, finalized, resume, plan, freq


@pytest.mark.parametrize("dataset", ["daily_bin", "minute_bin"])
@pytest.mark.parametrize("repair_day", ["2026-08-31", "2026-09-01"])
def test_exact_prev_close_correction_keeps_prices_source_and_append_proof(tmp_path, dataset, repair_day):
    from backend.services.dataset_release import monthly_prev_close_repair as correction
    invocation, predecessor, original, finalized, resume, plan, freq = _retained_prev_close(tmp_path, dataset, repair_day)
    before = {p: p.read_bytes() for p in resume.root.rglob("*.bin") if not p.name.startswith("prev_close.")}
    inherited = (predecessor / f"features/000001.sz/prev_close.{freq}.bin").read_bytes()
    updated = correction.apply_prev_close_corrections(resume, finalized=finalized, cas=invocation.cas, plan=plan)
    inner = invocation.cas.get_json(updated["finalize_receipt_ref"])
    actual = invocation.cas.get_json(inner[f"{dataset.removesuffix('_bin')}_receipt_ref"])
    assert actual["native_writer"] == original["native_writer"]
    assert actual["prev_close_correction"]["counts"]["repaired_stock_dates"] == 1
    assert all(p.read_bytes() == raw for p, raw in before.items())
    assert (predecessor / f"features/000001.sz/prev_close.{freq}.bin").read_bytes() == inherited
    assert correction.apply_prev_close_corrections(resume, finalized=updated, cas=invocation.cas, plan=plan) == updated
    report = validate_qlib_month(root=resume.root, materialization=actual, cas=invocation.cas,
        cutoff=date(2026, 9, 1), predecessor_manifest_sha256=original["predecessor_manifest_sha256"], source_bundle_sha256="e" * 64)
    assert report["status"] == "PASS" and report["historical_values_read"] == 0
    target = resume.root / dataset / f"qlib/features/000001.sz/prev_close.{freq}.bin"
    target.write_bytes(target.read_bytes() + b"bad!")
    with pytest.raises(RuntimeError, match="corrected bytes"):
        validate_qlib_month(root=resume.root, materialization=actual, cas=invocation.cas,
            cutoff=date(2026, 9, 1), predecessor_manifest_sha256=original["predecessor_manifest_sha256"], source_bundle_sha256="e" * 64)


@pytest.mark.parametrize("interrupted_replace", [1, 2])
def test_prev_close_correction_recovers_exact_files_without_reexport(tmp_path, monkeypatch, interrupted_replace):
    from backend.services.dataset_release import monthly_prev_close_repair as correction
    invocation, _, _, finalized, resume, plan, _ = _retained_prev_close(tmp_path)
    replace, calls = correction._replace, []
    def interrupted(path, payload):
        replace(path, payload)
        calls.append(path)
        if len(calls) == interrupted_replace:
            raise OSError("simulated interruption")
    monkeypatch.setattr(correction, "_replace", interrupted)
    with pytest.raises(OSError, match="interruption"):
        correction.apply_prev_close_corrections(resume, finalized=finalized, cas=invocation.cas, plan=plan)
    monkeypatch.setattr(correction, "_replace", replace)
    record = correction.recover_prev_close(root=resume.root, journal=resume.path, record=resume.record, cas=invocation.cas)
    assert record == json.loads(resume.path.read_bytes())
    assert not correction._pending(resume.root).exists()
    target = resume.root / "daily_bin/qlib/features/000001.sz/prev_close.day.bin"
    assert struct.unpack("<fff", target.read_bytes()) == (0, 11, 6)


@pytest.mark.parametrize("fault", [None, "unclosed", "source_hash", "nonfinite", "scope"])
def test_prev_close_correction_requires_closed_exact_provider_sources(tmp_path, fault):
    from backend.services.dataset_release import monthly_prev_close_repair as correction
    root = tmp_path / "data_repair_staging"
    root.mkdir()
    def pin(name, value):
        path = root / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return str(path), hashlib.sha256(path.read_bytes()).hexdigest()
    row = {"ts_code": "000001.SZ", "trade_date": "20260831", "pre_close": "11" if fault != "nonfinite" else "nan"}
    rows_hash = hashlib.sha256(canonical_json_bytes([row])).hexdigest()
    source = {"schema_version": "local_data_stk_limit_pre_close_repair_v1", "provider": "tushare.stk_limit", "units": "CNY_per_share",
              "trade_date": "2026-08-31", "source_rows": [row], "patches": [row], "source_rows_sha256": rows_hash,
              "target_database": "aistock"}
    source_path, source_hash = pin("source.json", source)
    plan = {"schema_version": "local_data_stk_limit_history_production_plan_v1", "target_database": "aistock", "target_table": "market.stk_limit",
            "changed_columns": ["pre_close"] if fault != "scope" else ["up_limit"],
            "daily": [{"source_plan_path": source_path, "source_plan_sha256": source_hash if fault != "source_hash" else "0" * 64,
                       "source_rows_sha256": rows_hash, "keys": 1, "trade_date": "2026-08-31"}]}
    plan_path, plan_hash = pin("plan.json", plan)
    receipt = {"schema_version": "local_data_stk_limit_history_production_apply_v1", "plan_sha256": plan_hash,
               "production_apply": "PASS", "production_readback": "PASS_INDEPENDENT_FRESH_CONNECTION" if fault != "unclosed" else "pending",
               "non_target_columns_unchanged": True, "verified_keys": 1}
    receipt_path, receipt_hash = pin("receipt.json", receipt)
    inputs = {"plan_path": plan_path, "plan_sha256": plan_hash, "receipt_path": receipt_path, "receipt_sha256": receipt_hash}
    if fault:
        with pytest.raises(ValueError):
            correction.capture_approved_corrections(inputs, repair_root=root, cutoff=date(2026, 9, 30))
    else:
        actual = correction.capture_approved_corrections(inputs, repair_root=root, cutoff=date(2026, 9, 30))
        assert actual["facts"] == [{**row, "trade_date": "2026-08-31"}] and actual["database_write"] is False
        single_receipt = {"schema_version": "local_data_stk_limit_pre_close_production_readback_v1",
            "source_plan_sha256": source_hash, "source_rows_sha256": rows_hash, "database": "aistock",
            "production_database_written": True, "other_columns_unchanged": True, "other_same_day_keys_unchanged": True,
            "trade_date": "2026-08-31", "readback_keys": 1, "expected_keys": 1}
        single_path, single_hash = pin("single-receipt.json", single_receipt)
        single = {"plan_path": source_path, "plan_sha256": source_hash, "receipt_path": single_path, "receipt_sha256": single_hash}
        group = correction.capture_approved_corrections({"repairs": [inputs, single]}, repair_root=root, cutoff=date(2026, 9, 30))
        assert group["facts"] == actual["facts"] and len(group["sources"]) == 2  # One fact, not double accumulation.
        with pytest.raises(ValueError, match="nested"):
            correction.capture_approved_corrections({"repairs": [{"repairs": []}]}, repair_root=root, cutoff=date(2026, 9, 30))
