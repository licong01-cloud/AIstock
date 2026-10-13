from __future__ import annotations

from copy import deepcopy
from datetime import date
import hashlib
import json
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes, digest_named_fields
from backend.services.dataset_release.errors import SourceManifestError
from backend.services.dataset_release.shared_sector_context import (
    build_release_sw_l2_code_map_payload, validate_release_sw_l2_code_map,
)
from backend.services.dataset_release import sw_l2_quote_policy as policy
from backend.services.dataset_release.monthly_sector_mapping import (
    build_bound_sector_enricher,
    frozen_sector_mapping_binding,
    load_predecessor_sector_mapping,
    sector_mapping_catalog_receipt,
)


EXTRAS = ("801217.SI", "801768.SI", "801786.SI")


@pytest.fixture
def predecessor(tmp_path, monkeypatch):
    catalog = set(policy._NEVER_QUOTED) | set(policy._LAST_PUBLISHED)
    for i in range(1000):
        if len(catalog) == 131:
            break
        if f"801{i:03d}.SI" not in EXTRAS:
            catalog.add(f"801{i:03d}.SI")
    codes = {code: i for i, code in enumerate(sorted(catalog))}
    monkeypatch.setattr(policy, "SW2021_L2_TAXONOMY_DIGEST", policy.taxonomy_digest(codes))
    payload = build_release_sw_l2_code_map_payload(
        code_to_id=codes, member_backed_codes=tuple(codes),
        authority_id="frozen-release", authority_sha256="a" * 64,
    )
    path = tmp_path / "components/sector_context_candidate_v1/sector_code_map.json"
    path.parent.mkdir(parents=True)
    raw = canonical_json_bytes(payload) + b"\n"
    path.write_bytes(raw)
    return SimpleNamespace(
        root=tmp_path, manifest_sha256="b" * 64,
        manifest={"components": {"sector_code_map": {
            "path": path.relative_to(tmp_path).as_posix(), "size": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }}},
    )


def _facts(binding):
    rows = [{"index_code": item["canonical_l2_code"], "level": "L2", "src": "SW2021", "is_pub": "1"}
            for item in binding["code_map"]["entries"]]
    rows += [{"index_code": code, "level": "L2", "src": "SW2021", "is_pub": "0"} for code in EXTRAS]
    last = binding["code_map"]["entries"][-1]
    members = [{"ts_code": "000001.SZ", "l2_code": last["canonical_l2_code"],
                "in_date": "2020-01-01", "out_date": None}]
    return rows, members, last


def test_134_raw_catalog_keeps_131_pinned_ids_and_receipts(predecessor):
    binding = load_predecessor_sector_mapping(predecessor)
    rows, members, last = _facts(binding)
    enricher = build_bound_sector_enricher(rows, members, binding=binding)
    assert len(rows) == 134 and len(enricher.code_map) == 131
    assert enricher.enrich({"ts_code": "000001.SZ", "trade_date": date(2026, 9, 1)})["l2_code_id"] == last["l2_code_id"]
    receipt = sector_mapping_catalog_receipt(rows, members, binding=binding)
    assert receipt["raw_catalog_count"] == 134
    assert receipt["canonical_catalog_count"] == 131
    assert receipt["unassigned_unpublished_codes"] == list(EXTRAS)
    assert receipt["binding"] == binding
    assert receipt["raw_catalog_sha256"] == digest_named_fields(
        "aistock_monthly_sw_l2_raw_catalog_v1", {"rows": sorted(rows, key=lambda row: row["index_code"])}
    )


@pytest.mark.parametrize("kind", ["published", "unknown_publication", "member", "missing", "duplicate", "wrong_source"])
def test_new_required_or_unproven_catalog_never_silently_filtered(predecessor, kind):
    binding = load_predecessor_sector_mapping(predecessor)
    rows, members, _ = _facts(binding)
    if kind == "published":
        rows[-1]["is_pub"] = "1"
    elif kind == "unknown_publication":
        rows[-1].pop("is_pub")
    elif kind == "member":
        members[0]["l2_code"] = EXTRAS[0]
    elif kind == "missing":
        rows.pop(0)
    elif kind == "duplicate":
        rows.append(deepcopy(rows[0]))
    else:
        rows[-1]["src"] = "SW2014"
    with pytest.raises(SourceManifestError):
        build_bound_sector_enricher(rows, members, binding=binding)


def test_map_pin_tampering_and_link_fail_before_source_rows(predecessor):
    path = predecessor.root / predecessor.manifest["components"]["sector_code_map"]["path"]
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(SourceManifestError, match="bytes"):
        load_predecessor_sector_mapping(predecessor)


def test_predecessor_map_link_is_not_accepted(predecessor):
    path = predecessor.root / predecessor.manifest["components"]["sector_code_map"]["path"]
    original = path.with_name("original.json")
    path.rename(original)
    try:
        path.symlink_to(original)
    except OSError:
        pytest.skip("symlink privilege unavailable")
    with pytest.raises(ValueError, match="link|reparse"):
        load_predecessor_sector_mapping(predecessor)


def test_frozen_binding_roundtrip_is_self_contained(predecessor):
    binding = load_predecessor_sector_mapping(predecessor)
    cas = SimpleNamespace(get_json_bounded=lambda ref, **kwargs: {"monthly_sector_mapping": binding})
    snapshot = SimpleNamespace(source_manifest_ref="source-ref")
    assert frozen_sector_mapping_binding(cas, snapshot) == binding
    rows, members, last = _facts(binding)
    assert build_bound_sector_enricher(rows, members, binding=frozen_sector_mapping_binding(cas, snapshot)).code_map[last["canonical_l2_code"]] == last["l2_code_id"]
    binding["code_map"]["entries"][-1]["l2_code_id"] -= 1
    with pytest.raises(SourceManifestError):
        frozen_sector_mapping_binding(cas, snapshot)


def test_bound_source_publication_receipt_readback_rejects_cross_mapping(predecessor):
    from backend.services.dataset_release.source_authority import (
        _validate_monthly_sector_publication_receipt, MONTHLY_SECTOR_PUBLICATION_SCHEMA,
        SourceAuditIncomplete,
    )

    binding = load_predecessor_sector_mapping(predecessor)
    rows, members, _ = _facts(binding)
    enricher = build_bound_sector_enricher(rows, members, binding=binding)
    classify = [{"identity": "sw_index_classify:all", "content_digest": "c" * 64}]
    member = [{"identity": "sw_index_member:all", "content_digest": "d" * 64}]
    receipt = {
        **enricher.receipt(classify_partitions=classify, member_partitions=member),
        "schema_version": MONTHLY_SECTOR_PUBLICATION_SCHEMA,
        "publication_policy": "classification_published_snapshot_v1",
        "mapping_policy": "immutable_predecessor_shared_ids_v1",
        "profile": "qe_hmm_full_v2", "cutoff": "2026-09-30",
        "monthly_catalog_lineage": sector_mapping_catalog_receipt(rows, members, binding=binding),
    }
    args = dict(expected_profile="qe_hmm_full_v2", expected_cutoff=date(2026, 9, 30),
                classify_partitions=classify, member_partitions=member, mapping_binding=binding)
    _validate_monthly_sector_publication_receipt(receipt, **args)
    quote = policy.build_quote_availability_payload(
        code_map=validate_release_sw_l2_code_map(binding["code_map"]),
        required_start=date(2024, 7, 1), cutoff=date(2026, 9, 30),
    )
    assert len(quote["entries"]) == 131
    for field, value in [("mapping_policy", "sorted_unique_sw_l2_zero_based_v1"),
                         ("code_map_digest", "e" * 64), ("monthly_catalog_lineage", None)]:
        with pytest.raises(SourceAuditIncomplete):
            _validate_monthly_sector_publication_receipt({**receipt, field: value}, **args)


def test_monthly_adapter_binds_map_before_freeze(predecessor, monkeypatch):
    from backend.services.dataset_release import monthly_postgres_source as source
    from backend.services.dataset_release import monthly_build_bridge as bridge
    from backend.services.dataset_release.monthly_snapshot import MonthlySnapshotIdentity

    binding = load_predecessor_sector_mapping(predecessor)
    monkeypatch.setattr(bridge, "load_monthly_predecessor_prefix", lambda **kwargs: predecessor)
    monkeypatch.setattr(source.PostgresMonthlySourceAdapter, "_require_pit_coverage", lambda *args: None)
    from backend.services.dataset_release import monthly_industry_history
    monkeypatch.setattr(monthly_industry_history, "capture_industry_history", lambda *args, **kwargs: None)
    seen = []

    def authority(*args, **kwargs):
        seen.append(kwargs["monthly_sector_mapping"])
        return SimpleNamespace()

    monkeypatch.setattr(source, "MonthlyObservedSourceAuthority", authority)
    def preflight(*args, **kwargs):
        raise RuntimeError("stop before database payload")
    monkeypatch.setattr(source, "_preflight_refresh_readiness", preflight)
    adapter = source.PostgresMonthlySourceAdapter(
        profile=SimpleNamespace(profile="qe_hmm_full_v2"), cas=SimpleNamespace(root=predecessor.root),
        artifact_root=predecessor.root, source_catalog=SimpleNamespace(root=predecessor.root),
    )
    context = SimpleNamespace(plan={"predecessor": {"cutoff": "2026-08-31"}, "target_cutoff": "2026-09-30"}, operation_id="dmr_test")
    with pytest.raises(RuntimeError, match="stop before database payload"):
        adapter.read(None, MonthlySnapshotIdentity("1-AA-1", "2026-10-06T00:00:00+00:00", "repair"), context)
    assert seen == [binding]


def test_versioned_history_does_not_backcast_or_change_current_ids(predecessor):
    import pandas as pd
    from backend.services.dataset_release.monthly_industry_history import IndustryHistoryProjection

    binding = load_predecessor_sector_mapping(predecessor)
    rows = [{"canonical_symbol": "000001.SZ", "taxonomy_version": version,
             "published_l2_code": code, "effective_from": start, "effective_to": end,
             "known_from": None, "knowledge_time_quality": "FIRST_PUBLICATION_UNVERIFIED",
             "source_sha256": "a" * 64, "source_row_sha256": "b" * 64}
            for version, code, start, end in [
                ("SW2014", "801192.SI", "2018-08-01", "2021-07-29"),
                ("SW2021", "801011.SI", "2021-07-30", "2026-08-31")]]
    projection = IndustryHistoryProjection.build(rows, current_map=binding["code_map"],
        authority_sha256="c" * 64)
    index = pd.MultiIndex.from_tuples([(pd.Timestamp(day), "000001.SZ") for day in
        ("2021-07-29", "2021-07-30", "2026-09-01")], names=["datetime", "instrument"])
    frame = pd.DataFrame({"l2_code_id": [-1, -1, 9], "unrelated": [1.0, 2.0, 3.0]}, index=index)
    actual = projection.project(frame)
    expected_id = next(entry["l2_code_id"] for entry in binding["code_map"]["entries"] if entry["canonical_l2_code"] == "801011.SI")
    assert actual["l2_code_id"].tolist() == [131, expected_id, 9]
    pd.testing.assert_series_equal(actual["unrelated"], frame["unrelated"])
    assert projection.payload["known_from_inferred"] is False
    assert projection.payload["entries"][131]["taxonomy_version"] == "SW2014"


@pytest.mark.parametrize("change", ["overlap", "future_taxonomy", "unknown_code", "invented_known_from"])
def test_versioned_history_rejects_ambiguous_or_invented_authority(predecessor, change):
    from backend.services.dataset_release.monthly_industry_history import IndustryHistoryProjection
    binding = load_predecessor_sector_mapping(predecessor)
    row = {"canonical_symbol": "000001.SZ", "taxonomy_version": "SW2021",
           "published_l2_code": "801011.SI", "effective_from": "2021-07-30",
           "effective_to": "2026-08-31", "known_from": None,
           "knowledge_time_quality": "FIRST_PUBLICATION_UNVERIFIED",
           "source_sha256": "a" * 64, "source_row_sha256": "b" * 64}
    rows = [row]
    if change == "overlap":
        rows.append(dict(row))
    elif change == "future_taxonomy":
        row["effective_from"] = "2018-08-01"
    elif change == "unknown_code":
        row["published_l2_code"] = "999999.SI"
    else:
        row["known_from"] = row["effective_from"]
    with pytest.raises(SourceManifestError):
        IndustryHistoryProjection.build(rows, current_map=binding["code_map"], authority_sha256="c" * 64)


def test_history_materialization_rebinds_reference_facts_and_preserves_prices(predecessor, tmp_path):
    import numpy as np
    import pandas as pd
    from backend.services.dataset_release.monthly_industry_history import IndustryHistoryProjection, materialize_history_files
    binding = load_predecessor_sector_mapping(predecessor)
    entries = binding["code_map"]["entries"]
    current_id, other_id = entries[0]["l2_code_id"], entries[1]["l2_code_id"]
    rows = []
    for symbol in ("000001.SZ", "000002.SZ"):
        for version, code, start, end in [
            ("SW2014", "801192.SI", "2018-08-01", "2021-07-29"),
            ("SW2021", entries[0]["canonical_l2_code"], "2021-07-30", "2026-08-31")]:
            rows.append({"canonical_symbol": symbol, "taxonomy_version": version, "published_l2_code": code,
                         "effective_from": start, "effective_to": end, "known_from": None,
                         "knowledge_time_quality": "FIRST_PUBLICATION_UNVERIFIED",
                         "source_sha256": "a" * 64, "source_row_sha256": "b" * 64})
    history = IndustryHistoryProjection.build(rows, current_map=binding["code_map"], authority_sha256="c" * 64)
    root = tmp_path / "private.building"
    factor = root / "factor_bundle"
    factor.mkdir(parents=True)
    index = pd.MultiIndex.from_tuples([(pd.Timestamp(day), symbol) for day in
        ("2018-08-01", "2024-07-01", "2026-09-01") for symbol in ("000001.SZ", "000002.SZ")], names=["datetime", "instrument"])
    sector = pd.DataFrame({"l2_code_id": np.array([other_id, current_id, current_id, current_id], dtype=np.int16),
                           "sw2_close": np.array([300, 200, 210, 210], dtype=np.float32)}, index=index[2:])
    static = pd.DataFrame({"l2_code_id": np.array([-1, -1, other_id, current_id, current_id, current_id], dtype=np.int16),
                           "sw2_close": np.array([np.nan, np.nan, 300, 200, 210, 210], dtype=np.float32),
                           "unrelated": np.arange(6, dtype=np.float32)}, index=index)
    price = pd.DataFrame({"close": np.arange(6, dtype=np.float32)}, index=index)
    for name, frame in (("sector_data", sector), ("daily_pv", price)):
        frame.to_hdf(factor / f"{name}.h5", "data", format="table", data_columns=["datetime", "instrument"])
    static.to_parquet(factor / "static_factors.parquet")
    signatures = {path.name: path.stat().st_mtime_ns for path in factor.iterdir()}
    result = materialize_history_files(root, history)
    outputs = {name: root.parent / pin["scratch_relative_path"] for name, pin in result["files"].items()}
    actual_sector = pd.read_hdf(outputs["sector_data.h5"], "data").sort_index()
    actual_static = pd.read_parquet(outputs["static_factors.parquet"])
    assert actual_sector.index.equals(index)
    assert actual_sector["l2_code_id"].tolist() == [131, 131, current_id, current_id, current_id, current_id]
    assert actual_sector.loc[(pd.Timestamp("2024-07-01"), "000001.SZ"), "sw2_close"] == 200
    assert actual_sector.loc[(pd.Timestamp("2018-08-01"), "000001.SZ"), "sw2_close"] != actual_sector.loc[(pd.Timestamp("2018-08-01"), "000001.SZ"), "sw2_close"]
    pd.testing.assert_series_equal(actual_static["unrelated"], static["unrelated"])
    pd.testing.assert_frame_equal(actual_static.loc[pd.Timestamp("2026-09-01")], static.loc[pd.Timestamp("2026-09-01")])
    assert result["classification_only_rows_added"] == 2
    assert {path.name: path.stat().st_mtime_ns for path in factor.iterdir()} == signatures


def test_history_supplement_is_bound_to_original_source_and_current_release(predecessor):
    import hashlib
    from backend.services.dataset_release.canonical import canonical_json_bytes
    from backend.services.dataset_release.monthly_industry_history import load_frozen_history
    plan = {"operation_id": "dmr_" + "1" * 32, "target_cutoff": "2026-09-30",
            "predecessor": {"dataset_manifest_sha256": "a" * 64}}
    binding = {"schema_version": "aistock_monthly_industry_history_binding_v1", "operation_id": plan["operation_id"],
               "target_cutoff": plan["target_cutoff"], "predecessor_manifest_sha256": "a" * 64,
               "source_bundle_sha256": "b" * 64, "projection": {"schema_version": "untrusted"}}
    plan.update(monthly_industry_history=binding,
                monthly_industry_history_ref={"sha256": hashlib.sha256(canonical_json_bytes(binding) + b"\n").hexdigest()})
    with pytest.raises(SourceManifestError, match="sealed SOURCE"):
        load_frozen_history(None, context_plan=plan, build_inputs={"source_bundle_sha256": "c" * 64})
    with pytest.raises(SourceManifestError, match="schema"):
        load_frozen_history(None, context_plan=plan, build_inputs={"source_bundle_sha256": "b" * 64})
    binding["target_cutoff"] = "2026-10-31"
    with pytest.raises(SourceManifestError, match="bytes"):
        load_frozen_history(None, context_plan=plan, build_inputs={"source_bundle_sha256": "b" * 64})


def test_unchanged_frozen_history_is_reused_without_historical_rescan(predecessor, tmp_path, monkeypatch):
    from backend.services.dataset_release import monthly_industry_history as subject, monthly_build_bridge
    from backend.services.dataset_release.cas_store import CASStore
    from backend.services.dataset_release.control_store import ControlStore
    binding = load_predecessor_sector_mapping(predecessor)
    row = {"canonical_symbol": "000001.SZ", "taxonomy_version": "SW2014", "published_l2_code": "801192.SI",
           "effective_from": "2018-08-01", "effective_to": "2021-07-29", "known_from": None,
           "knowledge_time_quality": "FIRST_PUBLICATION_UNVERIFIED", "source_sha256": "a" * 64, "source_row_sha256": "b" * 64}
    history = subject.IndustryHistoryProjection.build([row], current_map=binding["code_map"], authority_sha256="c" * 64)
    path = predecessor.root / "components/sector_context_candidate_v1/industry_l2_history_authority.json"
    path.write_bytes(canonical_json_bytes({**history.payload, "projection_sha256": history.sha256, "factor_projection_receipt_ref": {}}) + b"\n")
    pin = {"path": path.relative_to(predecessor.root).as_posix(), "size": path.stat().st_size,
           "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    monkeypatch.setattr(subject, "load_frozen_history", lambda *args, **kwargs: history)
    monkeypatch.setattr(monthly_build_bridge, "load_monthly_predecessor_prefix", lambda **kwargs: SimpleNamespace(
        root=predecessor.root, manifest={"components": {"industry_history": pin}}))
    def unexpected(*args, **kwargs):
        raise AssertionError("A normal subsequent month must not export or audit unchanged industry history")
    monkeypatch.setattr(subject, "materialize_history_files", unexpected)
    ControlStore.initialize(tmp_path / "control")
    cas = CASStore(tmp_path / "control")
    from backend.services.dataset_release.factor_materializer import FACTOR_H5_DATASETS
    factor = cas.put_json({"files": [{"dataset": name, "inherited_rows": 100, "month_rows": 10}
                                    for name in (*FACTOR_H5_DATASETS, "static_factors")]})
    prepare = {"prepare_receipt_ref": cas.put_json({"refs": {"factor_receipt": factor.as_dict()}}).as_dict()}
    saved = []
    resume = SimpleNamespace(path=tmp_path / "resume.json", get=lambda key: None,
                             save=lambda key, reference, **kwargs: saved.append((key, reference)))
    actual = subject.apply_retained_history(resume, context=SimpleNamespace(plan={}),
        compiled=SimpleNamespace(physical_plan={}), prepare=prepare, cas=cas, profile=object())
    assert actual != prepare
    proof = cas.get_json_bounded(saved[-1][1], max_bytes=1024 * 1024)
    assert proof["historical_projection_reused"] is True and proof["sector_row_count"] == 110
    assert proof["database_write"] is False
    assert [entry[0] for entry in saved] == ["prepare", "industry-history"]


def test_history_reuses_same_official_text_code_id_without_taxonomy_backcast(predecessor):
    from backend.services.dataset_release.monthly_industry_history import IndustryHistoryProjection
    binding = load_predecessor_sector_mapping(predecessor)
    entry = binding["code_map"]["entries"][0]
    row = {"canonical_symbol": "000001.SZ", "taxonomy_version": "SW2014", "published_l2_code": entry["canonical_l2_code"],
           "effective_from": "2018-08-01", "effective_to": "2021-07-29", "known_from": None,
           "knowledge_time_quality": "FIRST_PUBLICATION_UNVERIFIED", "source_sha256": "a" * 64, "source_row_sha256": "b" * 64}
    history = IndustryHistoryProjection.build([row], current_map=binding["code_map"], authority_sha256="c" * 64)
    assert history.resolve("000001.SZ", date(2018, 8, 1)) == entry["l2_code_id"]
    assert history.resolve("000001.SZ", date(2021, 7, 30)) == -1
    assert history.payload["effective_intervals"][0]["taxonomy_version"] == "SW2014"
    assert len(history.payload["entries"]) == 132
    assert len({item["l2_code_id"] for item in history.payload["entries"]}) == 131


@pytest.mark.parametrize("interruption", [None, "backup", "publish", "journal"])
def test_native_factor_receipt_survives_real_history_repair_and_retry(predecessor, tmp_path, monkeypatch, interruption):
    import json
    from pathlib import Path
    import pandas as pd
    from backend.services.dataset_release import monthly_industry_history as subject, monthly_build_bridge, monthly_build_resume
    from backend.services.dataset_release.cas_store import CASStore
    from backend.services.dataset_release.control_store import ControlStore
    from backend.services.dataset_release.factor_materializer import FACTOR_H5_DATASETS
    from backend.services.dataset_release.monthly_build_resume import _signature

    mapping = load_predecessor_sector_mapping(predecessor)["code_map"]
    current_id = mapping["entries"][0]["l2_code_id"]
    row = {"canonical_symbol": "000001.SZ", "taxonomy_version": "SW2014", "published_l2_code": "801192.SI",
           "effective_from": "2018-08-01", "effective_to": "2021-07-29", "known_from": None,
           "knowledge_time_quality": "FIRST_PUBLICATION_UNVERIFIED", "source_sha256": "a" * 64, "source_row_sha256": "b" * 64}
    history = subject.IndustryHistoryProjection.build([row], current_map=mapping, authority_sha256="c" * 64)
    monkeypatch.setattr(subject, "load_frozen_history", lambda *args, **kwargs: history)
    monkeypatch.setattr(monthly_build_bridge, "load_monthly_predecessor_prefix", lambda **kwargs: None)
    root = tmp_path / "retained"
    factor_root = root / "factor_bundle"
    factor_root.mkdir(parents=True)
    index = pd.MultiIndex.from_tuples([(pd.Timestamp("2018-08-01"), "000001.SZ"),
                                      (pd.Timestamp("2026-09-01"), "000001.SZ")], names=["datetime", "instrument"])
    for name in FACTOR_H5_DATASETS:
        frame = pd.DataFrame({"untouched": [1., 2.]}, index=index)
        if name == "sector_data":
            frame = pd.DataFrame({"l2_code_id": [current_id]}, index=index[1:])
        frame.to_hdf(factor_root / f"{name}.h5", key="data", format="table")
    pd.DataFrame({"l2_code_id": [-1, current_id], "untouched": [1., 2.]}, index=index).to_parquet(factor_root / "static_factors.parquet")
    entries = []
    for name in (*FACTOR_H5_DATASETS, "static_factors"):
        path = factor_root / f"{name}.{'parquet' if name == 'static_factors' else 'h5'}"
        entries.append({"dataset": name, "inherited_rows": 0 if name == "sector_data" else 1, "month_rows": 1,
                        "size": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "target_signature": list(_signature(path))})
    original_factor = {"schema_version": "aistock_monthly_native_factor_materialization_v1", "files": entries}
    # Native monthly materialization seals its receipt in CAS, not a legacy
    # factor_checkpoint.json in the physical bundle.
    original_bytes = {path.name: path.read_bytes() for path in factor_root.iterdir()}
    ControlStore.initialize(tmp_path / "control")
    cas = CASStore(tmp_path / "control")
    prepare = {"prepare_receipt_ref": cas.put_json({"refs": {"factor_receipt": cas.put_json(original_factor).as_dict()}}).as_dict()}
    record = {"binding": {}, "binding_digest": "d" * 64,
              "files": {f"factor_bundle/{path.name}": list(_signature(path)) for path in factor_root.iterdir()},
              "steps": {"prepare": cas.put_json(prepare).as_dict(), "dump": {"unchanged": True}, "finalize": {"unchanged": True}}}
    resume = SimpleNamespace(root=root, path=tmp_path / "resume.json", record=record)
    resume.get = lambda name: cas.get_json_bounded(resume.record["steps"][name], max_bytes=16 * 1024 * 1024) if name in resume.record["steps"] else None
    context = SimpleNamespace(plan={}, checkpoint=lambda: None, progress=lambda value: None)
    def apply():
        return subject.apply_retained_history(resume, context=context, compiled=SimpleNamespace(physical_plan={}),
                                              prepare=prepare, cas=cas, profile=object())
    if interruption is None:
        actual = apply()
    else:
        rename = Path.rename
        atomic = monthly_build_resume._atomic
        def interrupted_rename(path, target):
            result = rename(path, target)
            name = "sector_data.h5.before" if interruption == "backup" else "sector_data.h5"
            if Path(target).name == name:
                raise OSError("simulated industry commit interruption")
            return result
        def interrupted_atomic(path, value):
            atomic(path, value)
            if path == resume.path:
                raise OSError("simulated industry commit interruption")
        with monkeypatch.context() as fault:
            if interruption == "journal":
                fault.setattr(monthly_build_resume, "_atomic", interrupted_atomic)
            else:
                fault.setattr(Path, "rename", interrupted_rename)
            with pytest.raises(OSError, match="simulated industry commit interruption"):
                apply()
        if resume.path.exists():
            resume.record = json.loads(resume.path.read_bytes())
        resume.record = subject.recover_history_commit(root=root, journal=resume.path, record=resume.record,
                                                       expected_projection=history.sha256)
        actual = resume.get("prepare")
    nested = cas.get_json_bounded(actual["prepare_receipt_ref"], max_bytes=16 * 1024 * 1024)
    repaired = cas.get_json_bounded(nested["refs"]["factor_receipt"], max_bytes=16 * 1024 * 1024)
    by_name = subject.factor_file_receipts(repaired)
    assert by_name["sector_data.h5"]["inherited_rows"] == 1
    assert by_name["sector_data.h5"]["month_rows"] == 1
    assert by_name["static_factors.parquet"]["sha256"] != entries[-1]["sha256"]
    proof = resume.get("industry-history")
    assert proof["status"] == "PASS" and proof["sector_row_count"] == 2
    assert proof["database_write"] is False
    scratch = root.parent / f".{root.name}.industry-history-{history.sha256}"
    assert not (factor_root / "factor_checkpoint.json").exists()
    pending = json.loads((root.parent / f".{root.name}.industry-history-commit.json").read_bytes())
    assert set(pending["files"]) == {"sector_data.h5", "static_factors.parquet"}
    assert set(resume.record["files"]) == set(record["files"])
    for name in ("sector_data.h5", "static_factors.parquet"):
        assert (scratch / (name + ".before")).read_bytes() == original_bytes[name]
    for name in FACTOR_H5_DATASETS:
        if name != "sector_data":
            assert (factor_root / f"{name}.h5").read_bytes() == original_bytes[f"{name}.h5"]
    assert resume.record["steps"]["dump"] == record["steps"]["dump"]
    signatures = {path.name: _signature(path) for path in factor_root.iterdir()}
    assert subject.apply_retained_history(resume, context=context, compiled=SimpleNamespace(physical_plan={}),
                                         prepare=actual, cas=cas, profile=object()) == actual
    assert signatures == {path.name: _signature(path) for path in factor_root.iterdir()}

    # Exercise the downstream BUILD proof binding using the actual repair receipt.
    # External price/consumer QA is independent of this receipt-shape regression.
    from backend.services.dataset_release import build_stage, candidate_consumer_smoke, monthly_legacy_validation
    monkeypatch.setattr(monthly_build_bridge, "load_monthly_predecessor_prefix", lambda **kwargs: predecessor)
    report = {"verified_output_files": {}, "physical_month_ranges": {}}
    monkeypatch.setattr(monthly_legacy_validation, "validate_qlib_month", lambda **kwargs: report)
    monkeypatch.setattr(monthly_legacy_validation, "validate_aggregate_month_receipt", lambda **kwargs: report)
    monkeypatch.setattr(candidate_consumer_smoke, "validate_candidate_consumer_smoke_receipt", lambda *args, **kwargs: {})
    monkeypatch.setattr(build_stage, "_prerequisite_json", lambda *args: {})
    invocation = SimpleNamespace(cas=cas, staging_root=root, run_id="unit", attempt_id="unit", attempt_fence="unit",
        release_id="unit", release_digest="e" * 64, staging_relative_path=root.name,
        build_inputs={"monthly_legacy_predecessor": {}, "source_bundle_sha256": "f" * 64},
        plan={"industry_history_projection_sha256": history.sha256},
        profile=SimpleNamespace(profile="qe_hmm_full_v2", index_codes=(), stage_timeouts_seconds={"consumer": 600}))
    def validate(factor):
        return build_stage._validate_native_month(invocation,
            pit=SimpleNamespace(cutoff=date(2026, 9, 30), spans_sha256="e" * 64), factor_receipt=factor,
            index_receipt={}, daily_receipt={}, minute_receipt={}, ledger=SimpleNamespace(chunk=lambda value: None),
            checkpoint=lambda: None)
    assert validate(repaired)["validation_status"] == "PASS"
    bad = {**repaired, "industry_history_projection_ref": cas.put_json({**proof, "sector_row_count": 99}).as_dict()}
    with pytest.raises(build_stage.CandidateBuildStageError, match="row count"):
        validate(bad)


def test_history_extends_after_highest_shared_id_without_densifying_catalog(predecessor):
    from backend.services.dataset_release.monthly_industry_history import IndustryHistoryProjection
    mapping = load_predecessor_sector_mapping(predecessor)["code_map"]
    codes = {item["canonical_l2_code"]: item["l2_code_id"] for item in mapping["entries"]}
    codes[max(codes, key=codes.get)] = 133
    mapping = build_release_sw_l2_code_map_payload(code_to_id=codes, member_backed_codes=tuple(codes),
        authority_id="frozen-release", authority_sha256="a" * 64)
    row = {"canonical_symbol": "000001.SZ", "taxonomy_version": "SW2014", "published_l2_code": "801192.SI",
           "effective_from": "2018-08-01", "effective_to": "2021-07-29", "known_from": None,
           "knowledge_time_quality": "FIRST_PUBLICATION_UNVERIFIED", "source_sha256": "a" * 64, "source_row_sha256": "b" * 64}
    history = IndustryHistoryProjection.build([row], current_map=mapping, authority_sha256="c" * 64)
    assert history.resolve("000001.SZ", date(2018, 8, 1)) == 134
    assert 130 not in {item["l2_code_id"] for item in history.payload["entries"]}


def test_private_source_seals_raw_134_and_enriches_with_pinned_ids(predecessor, tmp_path, monkeypatch):
    from contextlib import contextmanager
    from backend.services.dataset_release import monthly_preparation_source as preparation
    from backend.services.dataset_release.cas_store import CASStore
    from backend.services.dataset_release.control_store import ControlStore
    from backend.services.dataset_release.source_authority import (
        MonthlySourceAuthority, PRODUCTION_QUERY_SPECS, SourceTableSchema, MONTHLY_SECTOR_SOURCE_POLICY,
    )
    from backend.services.dataset_release.sealed_source_reader import CASSealedPartitionReader

    binding = load_predecessor_sector_mapping(predecessor)
    classify, members, last = _facts(binding)
    queries = {key: PRODUCTION_QUERY_SPECS[key] for key in ("sw_index_classify", "sw_index_member", "sector_data")}
    monkeypatch.setattr(preparation, "PRODUCTION_QUERY_SPECS", queries)
    cutoff = date(2026, 9, 30)
    schemas = {key: SourceTableSchema(query.table_identity, query.required_columns) for key, query in queries.items()}
    payloads = {"sw_index_classify": classify, "sw_index_member": members, "sector_data": [{
        "ts_code": "000001.SZ", "trade_date": cutoff.isoformat(),
        **{name: 1000 for name in queries["sector_data"].value_columns},
    }]}
    class Session:
        snapshot_tokens = ("postgres:1-ABC-1",)
        def describe(self, key):
            return schemas[key]
        def stream(self, key, params, **kwargs):
            for row in sorted(payloads[key], key=lambda r: tuple(str(r[name]) for name in queries[key].key_columns)):
                yield {"row_key": json.dumps([row[name] for name in queries[key].key_columns]), "row_payload": json.dumps(row)}
    @contextmanager
    def sessions(_policy):
        yield Session()
    ControlStore.initialize(tmp_path / "control")
    cas = CASStore(tmp_path / "control")
    profile = SimpleNamespace(profile="qe_hmm_full_v2", start_date=date(2026, 9, 1),
                              resource_policy=SimpleNamespace(validation_read_chunk_rows=1000),
                              pressure_ladder={"date_chunk_months": (1,), "minute_batch": (100,)})
    authority = MonthlySourceAuthority(profile, cas, session_factory=sessions,
                                       sector_source_policy=MONTHLY_SECTOR_SOURCE_POLICY, monthly_sector_mapping=binding)
    pit = SimpleNamespace(spans_sha256="c" * 64, canonical_bytes=lambda: b'{"pit":true}')
    control = SimpleNamespace(schemas=schemas, audit=SimpleNamespace(partition_digest=lambda *a: "b" * 64,
                                                                    as_receipt=lambda **kw: {}),
                              pit_snapshot=pit, pit_partitions=(), snapshot_tokens=("postgres:1-ABC-1",),
                              consistency_digest="a" * 64, writer_ledger_digest="d" * 64)
    monkeypatch.setattr(authority, "_capture_control_snapshot", lambda **kwargs: control)
    monkeypatch.setattr(authority, "_partition_requests", lambda *a, **kw: [("2026-09-01_2026-09-30", {"start": profile.start_date, "end": cutoff})])
    monkeypatch.setattr(authority, "_freeze_writer_ledger", lambda *a, **kw: ("d" * 64, {}))
    frozen = preparation.freeze_preparation_source(authority, operation_id="dmr_" + "1" * 32,
                                                  cutoff=cutoff, blocking_datasets=("margin_detail",))
    assert frozen_sector_mapping_binding(cas, frozen) == binding
    reader = CASSealedPartitionReader(cas, [item.as_build_input() for item in frozen.partitions], max_partition_rows=1000)
    with reader.iter_rows("sw_index_classify", "2026-09-01_2026-09-30") as stream:
        assert len(list(stream)) == 134
    with reader.iter_rows("sector_data", "2026-09-01_2026-09-30") as stream:
        assert list(stream)[0]["l2_code_id"] == last["l2_code_id"]
