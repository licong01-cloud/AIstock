"""Native monthly release consumption, without manufacturing legacy state."""

from datetime import date
import hashlib
import json
import shutil

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import rotation_l1_input_bundle as reader
from backend.services.hmm_risk import monthly_release_input as monthly_reader
from backend.services.hmm_risk.contracts import canonical_sha256


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8", newline="\n")


def _ref(root, relative, *, field="path"):
    path = root / relative
    return {field: relative, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "size": path.stat().st_size}


def _seal(root):
    """Fixture producer: a native manifest/closure DAG, not a direct-v3 state."""
    files = sorted(
        p
        for p in root.rglob("*")
        if p.is_file()
        and p.name
        not in {
            "qe_dataset_manifest.json",
            "release_closure_receipt.json",
            "consumer.json",
            "readiness.json",
            "validation.json",
            "lineage.json",
        }
    )
    manifest = {
        "schema_version": "qe_dataset_manifest_v1",
        "release_id": "qe_hmm_full_v2_20260930",
        "revision": "20260930-monthly-v2",
        "cutoff_trade_date": "2026-09-30",
        "components": {str(i): _ref(root, p.relative_to(root).as_posix()) for i, p in enumerate(files)},
    }
    manifest["dataset_manifest_sha256"] = canonical_sha256(manifest)
    _write(root / "qe_dataset_manifest.json", manifest)
    groups = {"factor": [], "index": [], "sector_context": [], "manifest": [_ref(root, "qe_dataset_manifest.json")]}
    for ref in manifest["components"].values():
        for group, prefix in [
            ("factor", "factor_h5_static_candidate_v2"),
            ("index", "index_context"),
            ("sector_context", "sector_context_candidate_v1"),
        ]:
            if ref["path"].startswith("components/" + prefix + "/"):
                groups[group].append(ref)
    _write(
        root / "consumer.json",
        {
            "schema_version": "aistock_monthly_consumer_contract_v1",
            "consumer_id": "hmm_file_only",
            "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
            "status": "READY_FOR_NODE_READBACK",
            "required_components": sorted(groups),
            "component_refs": groups,
        },
    )
    for filename, schema in [
        ("readiness.json", "aistock_monthly_source_readiness_v1"),
        ("validation.json", "aistock_monthly_component_validation_v1"),
    ]:
        _write(
            root / filename,
            {
                "schema_version": schema,
                "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
                "status": "PASS",
                "unexplained_gap_count": 0,
                "hash_mismatch_count": 0,
                "duplicate_path_count": 0,
                "unpinned_core_file_count": 0,
            },
        )
    _write(
        root / "lineage.json",
        {
            "schema_version": "aistock_monthly_release_lineage_v1",
            "status": "SEALED_SUCCESSOR",
            **{k: manifest[k] for k in ("release_id", "revision", "cutoff_trade_date", "dataset_manifest_sha256")},
        },
    )
    closure = {
        "schema_version": "aistock_release_closure_v1",
        "dataset_manifest_ref": _ref(root, "qe_dataset_manifest.json", field="id"),
        "consumer_contract_refs": [_ref(root, "consumer.json", field="id")],
        "source_readiness_refs": [_ref(root, "readiness.json", field="id")],
        "component_validation_refs": [_ref(root, "validation.json", field="id")],
        "lineage_ref": _ref(root, "lineage.json", field="id"),
        "derived_asset_refs": [],
    }
    closure["canonical_sha256"] = canonical_sha256(closure)
    _write(root / "release_closure_receipt.json", closure)
    return {
        "generation": "monthly-test-generation",
        "release_id": manifest["release_id"],
        "revision": manifest["revision"],
        "cutoff": manifest["cutoff_trade_date"],
        "manifest_sha256": manifest["dataset_manifest_sha256"],
        "manifest_file_sha256": _ref(root, "qe_dataset_manifest.json")["sha256"],
    }


@pytest.fixture
def monthly(tmp_path, monkeypatch):
    root = tmp_path / "monthly-release"
    day = root / "components/daily_bin_candidate"
    factor = root / "components/factor_h5_static_candidate_v2"
    index = root / "components/index_context"
    suspend = root / "components/suspend_d_daily_candidate_v2"
    days = ["2020-07-29", "2020-07-30", "2026-03-31", "2026-08-31", "2026-09-30"]
    (day / "calendars").mkdir(parents=True)
    (day / "calendars/day.txt").write_text("\n".join(days) + "\n", encoding="utf-8")
    stock = "000001.SZ\t2018-08-01\t2026-09-30\n"
    benchmark = "".join(f"{code}\t2018-08-01\t2026-09-30\n" for code in reader.DIRECT_V2_INDEX_CODES)
    (day / "instruments").mkdir()
    (day / "instruments/all.txt").write_text(stock + benchmark, encoding="utf-8")
    (root / "stock_pools").mkdir()
    (root / "stock_pools/stock_universe.txt").write_text(stock, encoding="utf-8")
    _write(
        day / "meta_export.json",
        {
            "snapshot_id": "daily_bin_candidate",
            "start": "2018-08-01",
            "end": "2026-09-30",
            "universe_key": reader.DIRECT_V2_UNIVERSE_KEY,
            "rule_version": "shsz_a_252td_st_delist_asof_v2",
        },
    )
    factor.mkdir(parents=True)
    mi = pd.MultiIndex.from_product(
        [pd.to_datetime(["2018-08-01", *days]), ["000001.SZ"]], names=["datetime", "instrument"]
    )
    for name, columns in [("daily_basic", reader._DAILY_BASIC_COLUMNS), ("moneyflow", reader._MONEYFLOW_COLUMNS)]:
        pd.DataFrame(np.ones((len(mi), len(columns)), dtype="<f4"), index=mi, columns=columns).to_hdf(
            factor / (name + ".h5"), key="data", format="table", data_columns=["datetime", "instrument"]
        )
    _write(
        factor / "meta.json",
        {
            "schema_version": reader.DIRECT_V2_FACTOR_SCHEMA_VERSION,
            "start": "2018-08-01",
            "end": "2026-09-30",
            "universe_key": reader.DIRECT_V2_UNIVERSE_KEY,
        },
    )
    security = factor / "security_source_identity.json"
    _write(
        security,
        {
            "schema_version": "aistock_security_source_identity_v1",
            "manifest_version": "test",
            "default_resolution": "canonical_same_code",
            "rows": [],
        },
    )
    provider = tmp_path / "provider.json"
    _write(provider, {"external_authority": "fixture"})
    index.mkdir(parents=True)
    rows = [
        {
            "trade_date": pd.Timestamp(d),
            "ts_code": c,
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.0 + i,
            "volume": 100.0,
            "amount": 1000.0,
        }
        for i, d in enumerate(days)
        for c in reader.DIRECT_V2_INDEX_CODES
    ]
    pd.DataFrame(rows).astype({"trade_date": object}).to_hdf(index / "index_daily.h5", key="data", format="fixed")
    _write(
        index / "meta.json",
        {
            "schema_version": "qe_index_context_v1",
            "start": "2018-08-01",
            "end": "2026-09-30",
            "codes": list(reader.DIRECT_V2_INDEX_CODES),
        },
    )
    suspend.mkdir(parents=True)
    (suspend / "suspend_d.parquet").write_bytes(b"suspension-read-by-existing-kernel")
    _write(
        suspend / "meta.json",
        {
            "schema_version": reader.DIRECT_V2_SUSPEND_SCHEMA_VERSION,
            "start": "2018-08-01",
            "end": "2026-09-30",
            "universe_key": reader.DIRECT_V2_UNIVERSE_KEY,
        },
    )
    _write(root / "components/sector_context_candidate_v1/sector_code_map.json", {"shared": "fixture"})
    monkeypatch.setattr(reader, "_preflight_qlib_feature_inventory", lambda *a, **k: {"passed": True})
    return root, security, provider, _seal(root)


def _load(monthly, **kwargs):
    root, security, provider, binding = monthly
    return reader.load_rotation_l1_direct_v2_source_assets(
        root,
        security_identity_manifest=security,
        provider_absence_manifest=provider,
        frozen_release_binding=binding,
        **kwargs,
    )


def _active_profile(monthly, monkeypatch):
    root, _, _, binding = monthly
    path = root.parent / "active.json"
    _write(
        path,
        {
            "schema_version": "aistock_active_dataset_profile_v4",
            "generation": binding["generation"],
            "release_id": binding["release_id"],
            "cutoff": binding["cutoff"],
            "controller_paths": {"candidate_root": root.as_posix()},
            "node_bindings": {"wsl": {"candidate_root": "/mnt/x/this-release"}},
            "components": {
                "dataset_manifest_sha256": binding["manifest_sha256"],
                "dataset_manifest_file_sha256": binding["manifest_file_sha256"],
                "release_closure_sha256": _ref(root, "release_closure_receipt.json")["sha256"],
            },
        },
    )
    monkeypatch.setenv(reader.ACTIVE_DATASET_PROFILE_ENV, str(path))
    return path


def _rewrite_receipt(root, name, change):
    path = root / name
    value = json.loads(path.read_bytes())
    change(value)
    _write(path, value)
    closure_path = root / "release_closure_receipt.json"
    closure = json.loads(closure_path.read_bytes())
    for group in ("consumer_contract_refs", "source_readiness_refs", "component_validation_refs"):
        closure[group] = [_ref(root, name, field="id") if ref["id"] == name else ref for ref in closure[group]]
    if closure["lineage_ref"]["id"] == name:
        closure["lineage_ref"] = _ref(root, name, field="id")
    closure.pop("canonical_sha256")
    closure["canonical_sha256"] = canonical_sha256(closure)
    _write(closure_path, closure)


def _successor(monthly, name="successor"):
    parent, _, provider, previous = monthly
    root = parent.parent / name
    shutil.copytree(parent, root)
    manifest = json.loads((root / "qe_dataset_manifest.json").read_bytes())
    predecessor = manifest["dataset_manifest_sha256"]
    amendment = "reports/suspension-amendment.json"
    _write(root / amendment, {
        "schema_version": "aistock_monthly_suspension_amendment_v1",
        "predecessor_manifest_sha256": predecessor,
    })
    # This is deliberately outside the HMM read set. Inheritance must verify
    # the whole physical delta, rather than just trust PASS or consumed files.
    _write(root / "reports/changed-fact.json", {"source": name})
    refs = [dict(value) for value in manifest["components"].values()]
    refs = [ref for ref in refs if ref["path"] not in {amendment, "reports/changed-fact.json"}]
    refs += [_ref(root, amendment), _ref(root, "reports/changed-fact.json")]
    manifest["components"] = {str(i):ref for i, ref in enumerate(refs)}
    manifest["deployment_content_sha256"] = canonical_sha256(manifest["components"])
    manifest["revision"] += "-" + name
    manifest["source_contract"] = {"suspension_amendment": {
        "predecessor_manifest_sha256": predecessor, "receipt_sha256": _ref(root, amendment)["sha256"],
    }}
    manifest.pop("dataset_manifest_sha256")
    manifest["dataset_manifest_sha256"] = canonical_sha256(manifest)
    _write(root / "qe_dataset_manifest.json", manifest)
    identity = manifest["dataset_manifest_sha256"]
    inherited = {"path":str(parent / "release_closure_receipt.json"),
        "sha256":_ref(parent, "release_closure_receipt.json")["sha256"]}
    consumer = json.loads((root / "consumer.json").read_bytes())
    consumer["dataset_manifest_sha256"] = identity
    consumer["component_refs"]["manifest"] = [_ref(root, "qe_dataset_manifest.json")]
    _write(root / "consumer.json", consumer)
    _write(root / "readiness.json", {
        "schema_version":"aistock_monthly_source_readiness_v1", "dataset_manifest_sha256":identity,
        "status":"PASS", "unexplained_gap_count":0, "inherited_unchanged_readiness":inherited,
        "suspension_amendment_ref":_ref(root, amendment),
    })
    _write(root / "validation.json", {
        "schema_version":"aistock_monthly_component_validation_v1", "dataset_manifest_sha256":identity,
        "status":"PASS", "validation_scope":"EXACT_REVIEWED_SUSPENSION_KEYS_AND_INHERITED_PINS",
        "historical_content_revalidated":False, "inherited_unchanged_readiness":inherited,
        "deployment_content_sha256":manifest["deployment_content_sha256"], "manifest_component_count":len(refs),
    })
    _write(root / "lineage.json", {
        "schema_version":"aistock_monthly_release_lineage_v1", "status":"SEALED_SUCCESSOR",
        **{k:manifest[k] for k in ("dataset_manifest_sha256","release_id","revision","cutoff_trade_date")},
        "predecessor_dataset_manifest_sha256":predecessor,
    })
    closure = json.loads((root / "release_closure_receipt.json").read_bytes())
    closure["dataset_manifest_ref"] = _ref(root, "qe_dataset_manifest.json", field="id")
    for group, filename in [("consumer_contract_refs","consumer.json"),
        ("source_readiness_refs","readiness.json"),("component_validation_refs","validation.json")]:
        closure[group] = [_ref(root, filename, field="id")]
    closure["lineage_ref"] = _ref(root, "lineage.json", field="id")
    closure.pop("canonical_sha256")
    closure["canonical_sha256"] = canonical_sha256(closure)
    _write(root / "release_closure_receipt.json", closure)
    binding = {**previous, "revision":manifest["revision"], "manifest_sha256":identity,
        "manifest_file_sha256":_ref(root, "qe_dataset_manifest.json")["sha256"]}
    return root, root/"components/factor_h5_static_candidate_v2/security_source_identity.json", provider, binding


def test_monthly_accepts_verified_multilevel_inheritance_without_inventing_counters(monthly):
    successor = _successor(_successor(monthly), name="second-successor")
    before = (successor[0]/"validation.json").read_bytes()
    assets = _load(successor)
    assert assets["release_identity"]["dataset_manifest_sha256"] == successor[3]["manifest_sha256"]
    assert (successor[0]/"validation.json").read_bytes() == before
    assert not any(k in json.loads(before) for k in monthly_reader._ZERO_COUNTERS)


@pytest.mark.parametrize("receipt,key,value", [
    ("validation.json","validation_scope","UNKNOWN"),
    ("validation.json","hash_mismatch_count",0),  # partial counters are not zero counters
    ("validation.json","hash_mismatch_count",1),
    ("validation.json","manifest_component_count",1),
    ("lineage.json","predecessor_dataset_manifest_sha256","0"*64),
    ("readiness.json","inherited_unchanged_readiness",{}),
    ("validation.json","inherited_unchanged_readiness",{"path":"relative","sha256":"0"*64}),
])
def test_monthly_inheritance_rejects_rehashed_conflicts(monthly, receipt, key, value):
    successor = _successor(monthly)
    _rewrite_receipt(successor[0], receipt, lambda obj:obj.update({key:value}))
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(successor)


def test_monthly_inheritance_hashes_changed_files_outside_hmm_read_set(monthly):
    successor = _successor(monthly)
    (successor[0]/"reports/changed-fact.json").write_bytes(b"drift")
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(successor)


def test_monthly_inheritance_requires_full_terminal_validation(monthly):
    _rewrite_receipt(monthly[0], "validation.json", lambda obj:obj.pop("hash_mismatch_count"))
    successor = _successor(monthly)
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(successor)


def test_monthly_inheritance_never_masks_changed_predecessor_closure(monthly):
    successor = _successor(monthly)
    _rewrite_receipt(monthly[0], "readiness.json", lambda obj:obj.update(status="BLOCKED"))
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(successor)


def test_monthly_inheritance_depth_and_cycle_are_bounded(monthly):
    root = monthly[0].resolve()
    manifest = json.loads((root/"qe_dataset_manifest.json").read_bytes())
    pins = monthly_reader._manifest_pins(manifest)
    for ancestry in ((root/"release_closure_receipt.json",), tuple(range(8))):
        with pytest.raises(reader.RotationL1InputBundleError):
            monthly_reader._closure(monthly_reader._Files(root), manifest, pins, ancestry=ancestry)


def test_monthly_l2_consumes_native_closure_without_state_or_l1(monthly):
    assets = _load(monthly, data_window_end=date(2026, 3, 31))
    assert assets["release_cutoff"] == date(2026, 9, 30)
    assert assets["data_window_end"] == date(2026, 3, 31)
    assert max(assets["benchmark"]) == date(2026, 3, 31)
    assert assets["sector_index_close"] is None
    assert assets["instrument_universe_path"].name == "stock_universe.txt"
    assert not (monthly[0] / "direct_monthly_state.json").exists()
    assert assets["release_identity"]["frozen_release_generation"] == monthly[3]["generation"]
    identity, universe, rule, revision = reader._validated_release_identity(assets["release_identity"])
    assert identity == assets["release_identity"]
    assert universe == reader.DIRECT_V2_UNIVERSE_KEY
    assert rule == "shsz_a_252td_st_delist_asof_v2"
    assert revision == reader.MONTHLY_SOURCE_REVISION


@pytest.mark.parametrize("field", ["manifest_sha256", "manifest_file_sha256", "revision", "cutoff", "release_id"])
def test_monthly_rejects_different_explicit_identity(monthly, field):
    monthly[3][field] = "0" * 64 if "sha256" in field else "wrong"
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(monthly)


@pytest.mark.parametrize(
    "path",
    [
        "components/factor_h5_static_candidate_v2/moneyflow.h5",
        "components/daily_bin_candidate/calendars/day.txt",
        "components/index_context/index_daily.h5",
        "components/suspend_d_daily_candidate_v2/suspend_d.parquet",
        "stock_pools/stock_universe.txt",
    ],
)
def test_monthly_consumed_file_hash_drift_fails_closed(monthly, path):
    with (monthly[0] / path).open("ab") as f:
        f.write(b"drift")
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(monthly)


def test_monthly_rejects_unknown_or_rehashed_broken_closure(monthly):
    path = monthly[0] / "release_closure_receipt.json"
    obj = json.loads(path.read_bytes())
    obj["schema_version"] = "unknown"
    obj.pop("canonical_sha256")
    obj["canonical_sha256"] = canonical_sha256(obj)
    _write(path, obj)
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(monthly)


def test_monthly_never_hides_broken_legacy_state_with_closure(monthly):
    _write(monthly[0] / "direct_monthly_state.json", {"schema_version": "unknown"})
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(monthly)


def test_monthly_still_rejects_legacy_g2a_l1_contract(monthly, monkeypatch):
    root, security, provider, _ = monthly
    _active_profile(monthly, monkeypatch)
    with pytest.raises(reader.RotationL1InputBundleError):
        reader.load_rotation_l1_g2a_direct_v2_source_assets(
            root, security_identity_manifest=security, provider_absence_manifest=provider
        )


def test_monthly_active_profile_and_explicit_frozen_paths_are_separate(monthly, monkeypatch):
    root, security, provider, _ = monthly
    monkeypatch.setenv(reader.ACTIVE_DATASET_PROFILE_ENV, str(root / "not-present.json"))
    assert _load(monthly)["release_identity"]["frozen_release_generation"] == monthly[3]["generation"]
    _active_profile(monthly, monkeypatch)
    assets = reader.load_rotation_l1_direct_v2_source_assets(
        root, security_identity_manifest=security, provider_absence_manifest=provider, data_window_end=date(2026, 3, 31)
    )
    assert assets["release_identity"]["active_profile_generation"] == monthly[3]["generation"]
    assert "frozen_release_generation" not in assets["release_identity"]
    reader._validated_release_identity(assets["release_identity"])


@pytest.mark.parametrize("field", ["dataset_manifest_sha256", "dataset_manifest_file_sha256", "release_closure_sha256"])
def test_monthly_active_profile_rejects_pin_drift(monthly, monkeypatch, field):
    root, security, provider, _ = monthly
    profile_path = _active_profile(monthly, monkeypatch)
    profile = json.loads(profile_path.read_bytes())
    profile["components"][field] = "0" * 64
    _write(profile_path, profile)
    with pytest.raises(reader.RotationL1InputBundleError):
        reader.load_rotation_l1_direct_v2_source_assets(
            root, security_identity_manifest=security, provider_absence_manifest=provider
        )


@pytest.mark.parametrize(
    "receipt,key,value",
    [
        ("readiness.json", "status", "BLOCKED"),
        ("readiness.json", "unexplained_gap_count", 1),
        ("validation.json", "hash_mismatch_count", 1),
        ("lineage.json", "release_id", "another-release"),
        ("consumer.json", "dataset_manifest_sha256", "0" * 64),
    ],
)
def test_monthly_rehashed_receipts_do_not_bypass_semantic_checks(monthly, receipt, key, value):
    _rewrite_receipt(monthly[0], receipt, lambda obj: obj.update({key: value}))
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(monthly)


def test_monthly_consumer_must_pin_consumed_factor_file(monthly):
    def omit(obj):
        obj["component_refs"]["factor"] = [
            r for r in obj["component_refs"]["factor"] if not r["path"].endswith("moneyflow.h5")
        ]

    _rewrite_receipt(monthly[0], "consumer.json", omit)
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(monthly)


@pytest.mark.parametrize(
    "bad_path", ["../outside", "/outside", "components/../outside", "C:/outside", "components\\outside"]
)
def test_monthly_rejects_escaping_manifest_reference(monthly, bad_path):
    root = monthly[0]
    path = root / "qe_dataset_manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["components"]["bad"] = {"path": bad_path, "sha256": "0" * 64, "size": 0}
    manifest.pop("dataset_manifest_sha256")
    manifest["dataset_manifest_sha256"] = canonical_sha256(manifest)
    _write(path, manifest)
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(monthly)


def test_monthly_pit_membership_can_span_normal_quote_gaps(monthly, monkeypatch):
    root = monthly[0]
    provider = root / "components/daily_bin_candidate/instruments/all.txt"
    provider.write_text("000001.SZ\t2026-03-31\t2026-09-30\n", encoding="utf-8")
    monthly[3].update(_seal(root))
    seen = {}

    def inventory(_root, *, calendar, spans):
        seen.update(spans)
        return {"passed": True}

    monkeypatch.setattr(reader, "_preflight_qlib_feature_inventory", inventory)
    assets = _load(monthly)
    assert seen["000001.SZ"][0][0] == date(2026, 3, 31)
    assert reader._parse_instrument_spans(assets["instrument_universe_path"])["000001.SZ"][0][0] == date(2018, 8, 1)


@pytest.mark.parametrize("bad_symbol", ["000300.SH", "000985.CSI", "000099.SZ"])
def test_monthly_pit_rejects_index_or_unknown_provider_stock(monthly, bad_symbol):
    root = monthly[0]
    with (root / "stock_pools/stock_universe.txt").open("a", encoding="utf-8") as handle:
        handle.write(f"{bad_symbol}\t2018-08-01\t2026-09-30\n")
    monthly[3].update(_seal(root))
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(monthly)


def test_monthly_file_change_during_bounded_read_fails_closed(monthly, monkeypatch):
    original = reader._load_direct_v2_index_context

    def mutate(*args, **kwargs):
        result = original(*args, **kwargs)
        with (monthly[0] / "components/factor_h5_static_candidate_v2/moneyflow.h5").open("ab") as handle:
            handle.write(b"changed-during-read")
        return result

    monkeypatch.setattr(reader, "_load_direct_v2_index_context", mutate)
    with pytest.raises(reader.RotationL1InputBundleError, match="changed during readback"):
        _load(monthly)


def test_monthly_bounded_index_read_never_loads_later_numeric_values(monthly):
    root = monthly[0]
    path = root / "components/index_context/index_daily.h5"
    frame = pd.read_hdf(path)
    frame.loc[frame["trade_date"] > pd.Timestamp("2026-03-31"), "close"] = np.nan
    frame.astype({"trade_date": object}).to_hdf(path, key="data", format="fixed")
    monthly[3].update(_seal(root))
    assets = _load(monthly, data_window_end=date(2026, 3, 31))
    assert max(assets["benchmark_close"]) == date(2026, 3, 31)
    with pytest.raises(reader.RotationL1InputBundleError):
        _load(monthly)


def test_monthly_malformed_release_identity_hash_fails_readback(monthly):
    identity = _load(monthly)["release_identity"]
    identity["metadata_sha256"]["dataset_manifest"] = "invalid"
    with pytest.raises(reader.RotationL1InputBundleError):
        reader._validated_release_identity(identity)


def test_monthly_duplicate_manifest_path_is_ambiguous(monthly):
    root = monthly[0]
    path = root / "qe_dataset_manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["components"]["duplicate"] = next(iter(manifest["components"].values()))
    manifest.pop("dataset_manifest_sha256")
    manifest["dataset_manifest_sha256"] = canonical_sha256(manifest)
    _write(path, manifest)
    with pytest.raises(reader.RotationL1InputBundleError, match="duplicate file paths"):
        _load(monthly)


def test_monthly_indirect_component_ancestor_fails_closed(monthly, monkeypatch):
    target = monthly[0] / "components/factor_h5_static_candidate_v2"
    original = reader._is_indirect_path
    monkeypatch.setattr(reader, "_is_indirect_path", lambda path: path == target or original(path))
    with pytest.raises(reader.RotationL1InputBundleError, match="indirect"):
        _load(monthly)
