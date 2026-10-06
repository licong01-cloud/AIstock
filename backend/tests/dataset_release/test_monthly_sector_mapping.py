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
