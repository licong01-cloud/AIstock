"""Sparse shared IDs and immutable historical/release classification boundary."""

from copy import deepcopy
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.hmm_risk import release_industry_binding as h
from backend.services.hmm_risk import rotation_l1_input_bundle as reader
from backend.services.hmm_risk.contracts import StateModelSetError


class Files:
    def __init__(self):
        self.changed = False

    def digest(self, path):
        return {"receipt": "1", "code_map": "2", "membership": "3"}[path] * 64

    def unchanged(self):
        h.require(not self.changed, "file changed during read")


@pytest.fixture
def source(monkeypatch, tmp_path):
    prior = SimpleNamespace(
        transition_dates=lambda s: (date(2026, 9, 1),), receipt=SimpleNamespace(receipt_hash="9" * 64)
    )
    old = object()
    baseline = SimpleNamespace(
        classification_resolver=prior,
        index_membership_resolver=prior,
        authority_bundle=SimpleNamespace(preflight_report={"window_start": "2018-08-01", "window_end": "2026-09-30"}),
        constituents={"801010.SI": {"l1_code": "801010.SI", "l2_codes": ["801783.SI"]}},
        classification_lookup={("L1", "110000"): {"index_code": "801010.SI", "name": "L1"}},
        resolve=lambda *a: old,
    )
    code_map = SimpleNamespace(id_to_code={133: "801783.SI"}, member_backed_codes=("801783.SI",))
    files = Files()
    frame = pd.DataFrame(
        [["000001.SZ", "2024-07-01", "2026-10-15", 133], ["000001.SZ", "2026-10-16", "2026-10-30", 133]],
        columns=["instrument", "start_date", "end_date", "l2_code_id"],
    )
    metadata = {
        "membership": dict(
            current_snapshot_backfill=False,
            default_industry_assignment=False,
            silent_symbol_exclusion=False,
            span_count=2,
            symbol_count=1,
        )
    }
    monkeypatch.setattr(reader, "_industry_adapter", lambda *a, **k: baseline)
    monkeypatch.setattr(
        h, "_inputs", lambda *a: (files, {k: k for k in ("receipt", "code_map", "membership")}, metadata)
    )
    monkeypatch.setattr(h, "load_release_sw_l2_code_map", lambda *a: code_map)
    monkeypatch.setattr(h.pd, "read_parquet", lambda *a: frame.copy())
    original = dict(
        research_basis={"active_mode": "historical_replay"},
        l2_projection={
            "rows": [dict(canonical_l2_code="801783.SI", taxonomy_l1_code="110000", canonical_l2_name="L2")]
        },
    )
    binding = h.build_binding(original, tmp_path / "release", {"cutoff": "2026-10-30"})
    return binding, files, frame, metadata, baseline, old


def test_shared_sparse_id_resolves_to_text_while_old_history_is_identical(source):
    binding, files, _, _, baseline, old = source
    original = deepcopy(binding["baseline"])
    adapter = h.ReleaseIndustryAdapter(binding, forbidden_roots=())
    assert adapter.resolve("000001.SZ", date(2026, 9, 30)) is old
    projection = adapter.resolve("000001.SZ", date(2026, 10, 8))
    assert projection.l2_code == "801783.SI" and projection.classification_receipt_hash == binding["receipt_sha256"]
    assert projection.non_as_known_taxonomy and projection.index_membership_row_hashes == ()
    assert binding["baseline"] == original and baseline.authority_bundle.preflight_report["window_end"] == "2026-09-30"
    assert date(2026, 10, 16) in adapter.classification_resolver.transition_dates("000001.SZ")
    assert adapter.resolve("000001.SZ", date(2026, 11, 2)).status == "unavailable"
    assert (
        adapter.resolve("000002.SZ", date(2026, 10, 8)).reason_code == "classification:release_membership_unavailable"
    )
    files.changed = True
    with pytest.raises(StateModelSetError):
        adapter.unchanged()


def test_row_order_is_canonical_but_overlap_and_unknown_id_fail(source):
    binding, _, frame, _, _, _ = source
    expected = h.ReleaseIndustryAdapter(binding, forbidden_roots=()).resolve("000001.SZ", date(2026, 10, 8))
    frame.iloc[:] = frame.iloc[::-1].to_numpy()
    frame["instrument"] = " 000001.sz "
    assert h.ReleaseIndustryAdapter(binding, forbidden_roots=()).resolve("000001.SZ", date(2026, 10, 8)) == expected
    frame.loc[0, "l2_code_id"] = 999
    with pytest.raises(ValueError, match="unknown l2_code_id"):
        h.ReleaseIndustryAdapter(binding, forbidden_roots=())
    frame.loc[0, "l2_code_id"] = 133
    frame.loc[1, "end_date"] = "2026-10-16"
    with pytest.raises(ValueError, match="overlap"):
        h.ReleaseIndustryAdapter(binding, forbidden_roots=())


def test_hash_dates_policy_and_live_basis_cannot_be_relabelled(source):
    binding, _, _, metadata, _, _ = source
    for field, value in (
        ("extension_start", "2026-09-30"),
        ("extension_end", "2026-11-30"),
        ("membership_sha256", "0" * 64),
    ):
        bad = deepcopy(binding)
        bad[field] = value
        bad = h.receipt({k: v for k, v in bad.items() if k != "receipt_sha256"})
        with pytest.raises(StateModelSetError):
            h.ReleaseIndustryAdapter(bad, forbidden_roots=())
    bad = deepcopy(binding)
    bad["baseline"]["research_basis"]["active_mode"] = "forward"
    bad = h.receipt({k: v for k, v in bad.items() if k != "receipt_sha256"})
    with pytest.raises(StateModelSetError, match="live use forbidden"):
        h.ReleaseIndustryAdapter(bad, forbidden_roots=())
    metadata["membership"]["current_snapshot_backfill"] = True
    with pytest.raises(StateModelSetError, match="unsupported membership policy"):
        h.ReleaseIndustryAdapter(binding, forbidden_roots=())


def test_dispatch_requires_matching_file_release_and_does_not_expand_old_callers(tmp_path):
    authority = dict(
        schema_version=h.SCHEMA, candidate_root=str(tmp_path / "a"), release_binding={"cutoff": "2026-10-30"}
    )
    for kwargs in (
        {},
        {"candidate_root": tmp_path / "b", "release_binding": authority["release_binding"]},
        {"candidate_root": tmp_path / "a", "release_binding": {"cutoff": "2026-09-30"}},
    ):
        with pytest.raises(reader.RotationL1InputBundleError):
            reader._industry_adapter(authority, forbidden_roots=(), **kwargs)


def test_real_pins_reject_cross_release_missing_component_or_changed_bytes(tmp_path):
    import hashlib
    import json

    root = tmp_path / "release"
    component = root / h.PREFIX
    component.mkdir(parents=True)
    paths = {
        name: component / name
        for name in ("component_receipt.json", "sector_code_map.json", "sector_membership_spans.parquet")
    }
    paths["sector_code_map.json"].write_bytes(b"{}")
    paths["sector_membership_spans.parquet"].write_bytes(b"fixture")

    def sha(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    paths["component_receipt.json"].write_text(
        json.dumps(
            dict(
                membership={"sha256": sha(paths["sector_membership_spans.parquet"]), "end": "2026-10-30"},
                sector_code_map={"sha256": sha(paths["sector_code_map.json"])},
            )
        ),
        encoding="utf-8",
    )
    manifest = dict(
        release_id="release",
        revision="revision",
        cutoff_trade_date="2026-10-30",
        components={
            name: dict(path=h.PREFIX + name, sha256=sha(path), size=path.stat().st_size) for name, path in paths.items()
        },
    )
    manifest["dataset_manifest_sha256"] = h.canonical_sha256(manifest)
    manifest_path = root / "qe_dataset_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    binding = dict(
        release_id="release",
        revision="revision",
        generation="generation",
        cutoff="2026-10-30",
        manifest_sha256=manifest["dataset_manifest_sha256"],
        manifest_file_sha256=sha(manifest_path),
    )
    files, _, _ = h._inputs(root, binding)
    files.unchanged()
    wrong = {**binding, "release_id": "another"}
    with pytest.raises(StateModelSetError, match="release identity differs"):
        h._inputs(root, wrong)
    paths["sector_code_map.json"].write_bytes(b"bad")
    with pytest.raises(reader.RotationL1InputBundleError):
        h._inputs(root, binding)
    paths["sector_code_map.json"].unlink()
    with pytest.raises(reader.RotationL1InputBundleError):
        h._inputs(root, binding)
