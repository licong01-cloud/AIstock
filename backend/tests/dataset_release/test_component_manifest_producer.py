from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pytest

from backend.services.dataset_release import component_manifest_producer as producer_module
from backend.services.dataset_release.canonical_stock_transformer import (
    build_qfq_denominator_authority,
)
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.component_artifact_manifest import (
    load_component_artifact_manifest,
)
from backend.services.dataset_release.component_manifest_producer import (
    ComponentManifestProductionError,
    produce_component_artifact_manifest,
    snapshot_candidate_artifacts,
)
from backend.services.dataset_release.contracts import Component
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.copy_on_write import tree_merkle
from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.services.dataset_release.publisher import artifact_tree_digest


def _pit():
    cutoff = date(2026, 7, 31)
    return freeze_pit_snapshot(
        [
            {
                "ts_code": "000001.SZ",
                "eligible_start": date(2026, 7, 1),
                "eligible_end": cutoff,
                "entry_reason": None,
                "exit_reason": None,
            }
        ],
        universe_key="fixture-pit",
        rule_version="fixture-v1",
        scope_start=date(2026, 7, 1),
        cutoff=cutoff,
        state_identity="fixture",
        source_fingerprint_sha256="1" * 64,
        parameter_hash="2" * 64,
    )


def _source(component: Component) -> list[dict]:
    dataset = {
        Component.DAILY_BIN: "kline_daily_raw",
        Component.MINUTE_BIN: "kline_minute_raw",
        Component.FACTOR_H5_STATIC: "daily_basic",
        Component.DOMESTIC_INDEX_CONTEXT: "index_daily",
    }[component]
    return [
        {
            "identity": f"{dataset}:2026-07-01_2026-07-31",
            "dataset": dataset,
            "partition_key": "2026-07-01_2026-07-31",
            "row_count": 1,
            "content_digest": "3" * 64,
            "schema_digest": "4" * 64,
            "source_table_schema_digest": "5" * 64,
            "source_code_membership_digest": None,
            "min_key": ["000001.SZ", "2026-07-31"],
            "max_key": ["000001.SZ", "2026-07-31"],
            "monthly_content_leaves": [],
        }
    ]


def test_component_manifest_producer_seals_exact_candidate_file_graph(tmp_path, dataset_profile, monkeypatch) -> None:
    candidate = tmp_path / "candidate"
    files = {
        "daily_bin/qlib/features/000001.sz/close.day.bin": b"daily",
        "daily_bin/qlib/calendars/day.txt": b"2026-07-31\n",
        "daily_bin/csv_deltas/202607/000001.sz.csv": b"sealed-delta",
        "daily_bin/csv_deltas/202607/manifest.json": b"sealed-delta-manifest",
        "daily_bin/csv_overrides/revision/000001.sz.csv": b"sealed-override",
        "daily_bin/csv_overrides/revision/manifest.json": b"sealed-override-manifest",
        "minute_bin/qlib/features/000001.sz/close.1min.bin": b"minute",
        "factor_bundle/daily_pv.h5": b"factor",
        "factor_bundle/partitions/daily_pv/2026-07.parquet": b"sealed-factor-month",
        "index_context/index_csv/000985.CSI.csv": b"index-csi",
    }
    for relative, payload in files.items():
        path = candidate / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    pit = _pit()
    qfq = build_qfq_denominator_authority(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": pit.cutoff,
                "adj_factor": 1.25,
            }
        ],
        pit_snapshot=pit,
        cutoff=pit.cutoff,
    )
    content_reads = 0
    original_hash = producer_module._hash_file_once

    def counted_hash(path):
        nonlocal content_reads
        content_reads += 1
        return original_hash(path)

    monkeypatch.setattr(producer_module, "_hash_file_once", counted_hash)
    snapshot = snapshot_candidate_artifacts(candidate)
    artifact_root = snapshot.artifact_root
    assert artifact_root == artifact_tree_digest(candidate)
    assert snapshot.receipt() == {
        "schema_version": "dataset_release_candidate_artifact_snapshot_v1",
        "artifact_root": artifact_root,
        "file_count": len(files),
        "total_bytes": sum(len(value) for value in files.values()),
        "content_read_passes": 1,
        "identity_readbacks": 2,
    }
    identity_readbacks = 0
    original_verify = producer_module._verify_candidate_artifact_snapshot

    def counted_verify(root, value):
        nonlocal identity_readbacks
        identity_readbacks += 1
        return original_verify(root, value)

    monkeypatch.setattr(
        producer_module,
        "_verify_candidate_artifact_snapshot",
        counted_verify,
    )
    reference = produce_component_artifact_manifest(
        cas,
        candidate_root=candidate,
        profile=dataset_profile,
        scope="full",
        cutoff=pit.cutoff,
        candidate_identity="6" * 64,
        artifact_root=artifact_root,
        producer_fingerprint="7" * 64,
        artifact_fingerprint="8" * 64,
        validation_fingerprint="9" * 64,
        source_content_root="a" * 64,
        artifact_ready_content_root="b" * 64,
        pit_snapshot=pit,
        source_partitions={component: _source(component) for component in Component},
        qfq_authority=qfq,
        artifact_snapshot=snapshot,
    )
    assert content_reads == snapshot.file_count
    assert identity_readbacks == 2

    loaded = load_component_artifact_manifest(cas, reference)
    for component, relative in (
        (Component.DAILY_BIN, "daily_bin"),
        (Component.MINUTE_BIN, "minute_bin"),
        (Component.FACTOR_H5_STATIC, "factor_bundle"),
        (Component.DOMESTIC_INDEX_CONTEXT, "index_context"),
    ):
        assert loaded.component(component).filesystem_tree_merkle == tree_merkle(candidate / relative)[1]
    daily = loaded.component(Component.DAILY_BIN)
    assert daily.adj_writer_targets("000001.SZ") == (
        "qlib/calendars/day.txt",
        "qlib/features/000001.sz/close.day.bin",
    )
    assert "csv_deltas/202607/000001.sz.csv" in daily.instrument_file_targets["000001.SZ"]
    assert not any(
        path.startswith(("csv_deltas/", "csv_overrides/")) for path in daily.append_rules[0].replace_existing_targets
    )
    replace, create = daily.append_rules[0].targets_for_instruments([], create_for_instruments=["000002.SZ"])
    assert replace == ("qlib/calendars/day.txt",)
    assert len(create) == 13
    factor = loaded.component(Component.FACTOR_H5_STATIC)
    assert factor.adj_writer_targets("000001.SZ") == ("daily_pv.h5",)
    index_files = loaded.component(Component.DOMESTIC_INDEX_CONTEXT).instrument_file_targets
    assert index_files["000985.CSI"] == ("index_csv/000985.csi.csv",)


@pytest.mark.parametrize("mutation", ["same_size_content", "stat_only"])
def test_component_snapshot_rejects_same_size_tamper_or_stat_drift(
    tmp_path,
    mutation,
) -> None:
    candidate = tmp_path / "candidate"
    target = candidate / "daily_bin" / "fixture.bin"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"before")
    snapshot = snapshot_candidate_artifacts(candidate)
    before = target.stat()
    if mutation == "same_size_content":
        target.write_bytes(b"after!")
    os.utime(
        target,
        ns=(before.st_atime_ns, before.st_mtime_ns + 10_000_000),
    )

    with pytest.raises(
        ComponentManifestProductionError,
        match="stat identity changed",
    ):
        producer_module._verify_candidate_artifact_snapshot(candidate, snapshot)


def test_component_snapshot_rejects_reparse_or_symlink_entry(
    tmp_path,
    monkeypatch,
) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside")
    linked = candidate / "linked.bin"
    try:
        linked.symlink_to(outside)
    except OSError:
        linked.write_bytes(b"plain-fixture")
        original_lstat = producer_module.os.lstat

        class _ReparseStat:
            def __init__(self, value) -> None:
                self.st_mode = value.st_mode
                self.st_file_attributes = int(getattr(value, "st_file_attributes", 0)) | 0x0400

        def fake_lstat(path):
            value = original_lstat(path)
            return _ReparseStat(value) if Path(path) == linked else value

        monkeypatch.setattr(producer_module.os, "lstat", fake_lstat)

    with pytest.raises(
        ComponentManifestProductionError,
        match="link/reparse",
    ):
        snapshot_candidate_artifacts(candidate)
