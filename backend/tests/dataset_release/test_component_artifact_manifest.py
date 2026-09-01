from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import date

import pytest

from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.canonical_lineage import (
    CANONICAL_LINEAGE_CAPABILITY,
    planned_lineage_paths,
)
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.component_artifact_manifest import (
    COMPONENT_ARTIFACT_COMPONENT_INDEX_SCHEMA_V2,
    COMPONENT_ARTIFACT_MANIFEST_STORAGE_SCHEMA_V2,
    COMPONENT_ARTIFACT_MANIFEST_SCHEMA,
    COMPONENT_ARTIFACT_SECTION_SHARD_SCHEMA_V2,
    MAX_COMPONENT_SECTION_ROWS,
    MAX_COMPONENT_MANIFEST_BYTES,
    TARGET_COMPONENT_SECTION_SHARD_BYTES,
    ComponentArtifactManifestError,
    _seal_component_artifact_manifest_v1,
    _seal_v2_section,
    load_component_artifact_manifest,
    seal_component_artifact_manifest,
)
from backend.services.dataset_release.contracts import Component
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.copy_on_write import tree_merkle
from backend.services.dataset_release.mixed_planner import (
    CurrentAdjSeriesAuthority,
    CurrentComponentAuthority,
    MixedPlannerContext,
    build_mixed_action_plan,
    pit_span_digest_by_code,
)
from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _month(month: str, suffix: str) -> dict:
    body = {
        "schema_version": "dataset_release_source_month_content_leaf_v1",
        "month": month,
        "row_count": 2,
        "min_key": ["000001.SZ", f"{month}-01"],
        "max_key": ["000001.SZ", f"{month}-28"],
        "merkle_root": _digest(f"merkle:{suffix}"),
        "content_digest": _digest(f"content:{suffix}"),
    }
    return {
        **body,
        "leaf_identity": digest_named_fields("dataset_release_source_month_content_leaf_v1", body),
    }


def _complete_component(component: Component) -> dict:
    dataset = {
        Component.DAILY_BIN: "kline_daily_raw",
        Component.MINUTE_BIN: "kline_minute_raw",
        Component.FACTOR_H5_STATIC: "daily_basic",
        Component.DOMESTIC_INDEX_CONTEXT: "index_daily",
    }[component]
    partition = "2026-05-01_2026-06-30"
    source_identity = f"{dataset}:{partition}"
    output = "artifact.bin"
    return {
        "status": "COMPLETE",
        "component": component.value,
        "component_root_relative_path": component.value,
        "source_partitions": [
            {
                "identity": source_identity,
                "dataset": dataset,
                "partition_key": partition,
                "row_count": 4,
                "content_digest": _digest(f"source:{component.value}"),
                "schema_digest": _digest(f"schema:{component.value}"),
                "source_table_schema_digest": _digest(f"table-schema:{component.value}"),
                "source_code_membership_digest": None,
                "min_key": ["000001.SZ", "2026-05-01"],
                "max_key": ["000001.SZ", "2026-06-30"],
                "monthly_content_leaves": [
                    _month("2026-05", f"{component.value}:may"),
                    _month("2026-06", f"{component.value}:jun"),
                ],
            }
        ],
        "artifact_partitions": [
            {
                "partition_key": "all",
                "source_partition_identities": [source_identity],
                "dependency_edges": [f"{dataset}->{component.value}"],
                "instruments": ["000001.SZ"],
                "start": "2018-08-01",
                "end": "2026-06-30",
                "files": [
                    {
                        "relative_path": output,
                        "size_bytes": 17,
                        "sha256": _digest(f"file:{component.value}"),
                        "instrument": "000001.SZ",
                    }
                ],
            }
        ],
        "append_rules": [
            {
                "rule_id": "monthly-tail",
                "datasets": [dataset],
                "replace_existing_targets": [output],
                "create_new_targets": [],
                "create_target_templates": ["new/{instrument}.bin"],
                "writer_targets_by_instrument": {"000001.SZ": [output]},
                "writer_target_policy": "explicit_by_instrument_v1",
                "dependency_edges": [f"{dataset}.monthly_tail->{component.value}"],
            }
        ],
        "pit_mutation_rule": {
            "rule_id": "pit-change",
            "datasets": ["stock_universe_pit_spans"],
            "replace_existing_targets": [output],
            "create_new_targets": [],
            "create_target_templates": ["new/{instrument}.bin"],
            "writer_targets_by_instrument": {"000001.SZ": [output]},
            "writer_target_policy": "explicit_by_instrument_v1",
            "dependency_edges": [f"pit_span->{component.value}"],
        },
        "pit_instruments": ["000001.SZ"],
        "pit_span_digest_by_code": {"000001.SZ": _digest("pit:000001.SZ")},
        "adj_series": {
            "complete": True,
            "qfq_denominator_by_code": {"000001.SZ": "1.25"},
            "ordered_adj_digest_by_code": {"000001.SZ": _digest("ordered")},
            "adj_row_count_by_code": {"000001.SZ": 2000},
            "monthly_ordered_adj_by_code": {},
            "writer_targets_by_code": {"000001.SZ": [output]},
            "shared_writer_targets": [],
            "writer_target_policy": "explicit_by_instrument_v1",
        },
    }


def manifest_payload() -> dict:
    return {
        "schema_version": COMPONENT_ARTIFACT_MANIFEST_SCHEMA,
        "profile": "qe_hmm_full_v1",
        "scope": "full",
        "cutoff": "2026-06-30",
        "candidate_identity": _digest("candidate"),
        "artifact_root": _digest("artifact"),
        "semantic_profile_digest": _digest("semantic"),
        "producer_fingerprint": _digest("producer"),
        "artifact_fingerprint": _digest("artifact-contract"),
        "validation_fingerprint": _digest("validation"),
        "source_content_root": _digest("source-root"),
        "artifact_ready_content_root": _digest("artifact-ready"),
        "pit_snapshot_digest": _digest("pit"),
        "components": {component.value: _complete_component(component) for component in Component},
    }


def test_component_manifest_seals_complete_partition_file_and_mutation_identity(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    reference = seal_component_artifact_manifest(cas, manifest_payload())

    loaded = load_component_artifact_manifest(cas, reference)

    daily = loaded.component(Component.DAILY_BIN)
    assert daily.complete
    assert len(daily.source_partitions[0].monthly_content_leaves) == 2
    assert daily.artifact_partitions[0].files[0].relative_path == ("artifact.bin")
    replace, create = daily.append_rules[0].targets_for_instruments([], create_for_instruments=["000002.SZ"])
    assert replace == ("artifact.bin",)
    assert create == ("new/000002.sz.bin",)
    assert loaded.manifest_root == cas.get_json(reference)["manifest_root"]


def test_component_manifest_rejects_incomplete_reference_and_tampered_identity(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    reference = seal_component_artifact_manifest(cas, manifest_payload())
    with pytest.raises(ComponentArtifactManifestError, match="complete CAS"):
        load_component_artifact_manifest(cas, reference.sha256)

    value = cas.get_json(reference)
    value["components"]["daily_bin"]["component_manifest_root"] = _digest("tampered")
    tampered = cas.put_json(value)
    with pytest.raises(ComponentArtifactManifestError, match="index root"):
        load_component_artifact_manifest(cas, tampered)


def test_component_filesystem_tree_merkle_matches_cow_component_root(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    component_root = tmp_path / "component"
    component_root.mkdir()
    payload = b"component-bytes-v1"
    (component_root / "artifact.bin").write_bytes(payload)
    value = manifest_payload()
    daily_file = value["components"]["daily_bin"]["artifact_partitions"][0]["files"][0]
    daily_file["size_bytes"] = len(payload)
    daily_file["sha256"] = hashlib.sha256(payload).hexdigest()

    loaded = load_component_artifact_manifest(cas, seal_component_artifact_manifest(cas, value))

    assert loaded.component(Component.DAILY_BIN).filesystem_tree_merkle == (tree_merkle(component_root)[1])


def test_component_manifest_allows_explicit_per_component_unavailable_evidence(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    value = manifest_payload()
    value["components"]["factor_h5_static"] = {
        "status": "UNAVAILABLE",
        "reason_code": "LEGACY_COMPONENT_MANIFEST_MISSING",
    }
    loaded = load_component_artifact_manifest(cas, seal_component_artifact_manifest(cas, value))
    assert not loaded.component(Component.FACTOR_H5_STATIC).complete
    assert loaded.component(Component.DAILY_BIN).complete


def test_component_manifest_accepts_csi_index_artifact_instrument_only(
    tmp_path,
) -> None:
    value = manifest_payload()
    index = value["components"]["domestic_index_context"]
    index["artifact_partitions"][0]["instruments"] = ["000985.CSI"]
    index["artifact_partitions"][0]["files"][0]["instrument"] = "000985.CSI"
    index["append_rules"][0]["writer_targets_by_instrument"] = {"000985.CSI": ["artifact.bin"]}
    store = ControlStore.initialize(tmp_path / "control")
    loaded = load_component_artifact_manifest(
        CASStore(store.root),
        seal_component_artifact_manifest(CASStore(store.root), value),
    )
    assert loaded.component(Component.DOMESTIC_INDEX_CONTEXT).artifact_partitions[0].instruments == ("000985.CSI",)


def test_component_manifest_v2_shards_and_compacts_production_scale_index(
    tmp_path,
) -> None:
    value = manifest_payload()
    codes = [f"{number:06d}.SZ" for number in range(1, 6001)]
    shared_sha = _digest("scale-file-content")
    component = Component.DAILY_BIN
    entry = value["components"][component.value]
    files = [
        {
            "relative_path": (f"qlib/features/{code.casefold()}/{field}.day.bin"),
            "size_bytes": 64,
            "sha256": shared_sha,
            "instrument": code,
        }
        for code in codes
        for field in QLIB_STOCK_FIELDS
    ]
    files.append(
        {
            "relative_path": "csv_deltas/202606/000001.sz.csv",
            "size_bytes": 64,
            "sha256": shared_sha,
            "instrument": "000001.SZ",
        }
    )
    targets_by_code = {
        code: [f"qlib/features/{code.casefold()}/{field}.day.bin" for field in QLIB_STOCK_FIELDS] for code in codes
    }
    entry["artifact_partitions"][0]["files"] = files
    entry["artifact_partitions"][0]["instruments"] = codes
    entry["pit_instruments"] = codes
    entry["pit_span_digest_by_code"] = {code: _digest(f"pit:{code}") for code in codes}
    entry["append_rules"][0]["replace_existing_targets"] = []
    entry["append_rules"][0]["writer_targets_by_instrument"] = targets_by_code
    entry["append_rules"][0]["writer_target_policy"] = "artifact_file_instrument_index_v1"
    entry["pit_mutation_rule"]["replace_existing_targets"] = []
    entry["pit_mutation_rule"]["writer_targets_by_instrument"] = targets_by_code
    entry["pit_mutation_rule"]["writer_target_policy"] = "artifact_file_instrument_index_v1"
    entry["adj_series"] = {
        "complete": True,
        "qfq_denominator_by_code": {code: "1.25" for code in codes},
        "ordered_adj_digest_by_code": {code: _digest(f"adj:{code}") for code in codes},
        "adj_row_count_by_code": {code: 2000 for code in codes},
        "monthly_ordered_adj_by_code": {},
        "writer_targets_by_code": targets_by_code,
        "shared_writer_targets": [],
        "writer_target_policy": "artifact_file_instrument_index_v1",
    }
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)

    reference = seal_component_artifact_manifest(cas, value)
    index = cas.get_json(reference)
    loaded = load_component_artifact_manifest(cas, reference)

    assert reference.size < 1024 * 1024
    assert index["schema_version"] == COMPONENT_ARTIFACT_MANIFEST_STORAGE_SCHEMA_V2
    component_ref = index["components"][component.value]["component_index_ref"]
    assert 0 < component_ref["size"] <= MAX_COMPONENT_MANIFEST_BYTES
    component_index = cas.get_json(component_ref)
    for refs in component_index["sections"].values():
        for shard in refs:
            assert shard["row_count"] <= MAX_COMPONENT_SECTION_ROWS
            assert 0 < shard["section_shard_ref"]["size"] <= TARGET_COMPONENT_SECTION_SHARD_BYTES
    evidence = loaded.component(component)
    assert len(evidence.instrument_file_targets) == 6000
    assert sum(len(paths) for paths in evidence.instrument_file_targets.values()) == 6000 * len(QLIB_STOCK_FIELDS) + 1
    assert evidence.append_rules[0].writer_targets_by_instrument == {}
    assert evidence.pit_mutation_rule is not None
    assert evidence.pit_mutation_rule.writer_targets_by_instrument == {}
    assert evidence.adj_series is not None
    assert evidence.adj_series.writer_targets_by_code == {}
    replace, _create = evidence.append_rules[0].targets_for_instruments(
        ["000001.SZ"],
        instrument_file_targets=evidence.instrument_file_targets,
    )
    assert len(replace) == len(QLIB_STOCK_FIELDS)
    assert not any(path.startswith("csv_deltas/") for path in replace)
    assert evidence.adj_writer_targets("000001.SZ") == replace


def test_public_loader_keeps_legacy_v1_component_shards_read_only_compatible(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)

    reference = _seal_component_artifact_manifest_v1(cas, manifest_payload())
    loaded = load_component_artifact_manifest(cas, reference)

    assert cas.get_json(reference)["schema_version"] == COMPONENT_ARTIFACT_MANIFEST_SCHEMA
    assert loaded.component(Component.DAILY_BIN).complete
    assert loaded.manifest_root == cas.get_json(reference)["manifest_root"]


@pytest.mark.parametrize(
    "immutable",
    [
        "csv_deltas/202606/000001.sz.csv",
        "csv_overrides/revision/000001.sz.csv",
        "csv_lineage/events/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/event.json",
    ],
)
def test_v2_compaction_rejects_immutable_lineage_as_explicit_fallback_target(
    tmp_path,
    immutable: str,
) -> None:
    value = manifest_payload()
    daily = value["components"][Component.DAILY_BIN.value]
    daily["artifact_partitions"][0]["files"].append(
        {
            "relative_path": immutable,
            "size_bytes": 19,
            "sha256": _digest("immutable-lineage"),
            "instrument": "000001.SZ",
        }
    )
    daily["append_rules"][0]["replace_existing_targets"] = []
    daily["append_rules"][0]["writer_targets_by_instrument"] = {"000001.SZ": [immutable]}
    daily["append_rules"][0]["writer_target_policy"] = "artifact_file_instrument_index_v1"
    store = ControlStore.initialize(tmp_path / "control")

    with pytest.raises(ComponentArtifactManifestError, match="mutable files"):
        seal_component_artifact_manifest(CASStore(store.root), value)


def test_v2_adj_section_shards_6000_codes_by_36_months_under_hard_bound(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    shared_digest = _digest("virtual-adj-series")
    monthly = {}
    for offset in range(36):
        absolute_month = (2023 * 12 + 7) + offset
        year, month_index = divmod(absolute_month, 12)
        month = f"{year:04d}-{month_index + 1:02d}"
        monthly[month] = {
            "ordered_digest": shared_digest,
            "row_count": 20,
            "min_date": f"{month}-01",
            "max_date": f"{month}-28",
        }

    def virtual_rows():
        for number in range(1, 6001):
            instrument = f"{number:06d}.SZ"
            yield {
                "instrument": instrument,
                "qfq_denominator": "1.25",
                "ordered_adj_digest": shared_digest,
                "adj_row_count": 720,
                "has_monthly_authority": True,
                "monthly_ordered_adj": monthly,
                "writer_targets": [],
            }

    refs, row_count = _seal_v2_section(
        cas,
        profile="qe_hmm_full_v1",
        candidate_identity=_digest("candidate"),
        artifact_root=_digest("artifact"),
        component=Component.DAILY_BIN,
        section="adj_authority",
        values=virtual_rows(),
    )

    sizes = [entry["section_shard_ref"]["size"] for entry in refs]
    assert row_count == 6000
    assert sum(sizes) > MAX_COMPONENT_MANIFEST_BYTES
    assert max(sizes) <= TARGET_COMPONENT_SECTION_SHARD_BYTES
    assert max(entry["row_count"] for entry in refs) <= MAX_COMPONENT_SECTION_ROWS
    assert all(cas.get_json(entry["section_shard_ref"])["row_count"] == entry["row_count"] for entry in refs)


def _reseal_v2_nested_index(cas: CASStore, reference, mutation: str):
    top = deepcopy(cas.get_json(reference))
    entry = top["components"][Component.DAILY_BIN.value]
    component_index = deepcopy(cas.get_json(entry["component_index_ref"]))
    refs = component_index["sections"]["artifact_files"]
    assert len(refs) == 2
    if mutation == "missing":
        refs.pop()
    elif mutation == "reorder":
        refs.reverse()
    elif mutation == "duplicate_boundary":
        refs[1]["first_key"] = refs[0]["first_key"]
    elif mutation == "ordinal_bool":
        refs[1]["ordinal"] = True
    elif mutation == "leaf_semantic":
        shard = deepcopy(cas.get_json(refs[0]["section_shard_ref"]))
        shard["rows"][0]["value"]["file"]["sha256"] = _digest("tampered-leaf")
        shard_body = dict(shard)
        shard_body.pop("shard_root")
        shard_root = digest_named_fields(COMPONENT_ARTIFACT_SECTION_SHARD_SCHEMA_V2, shard_body)
        shard_ref = cas.put_json({**shard_body, "shard_root": shard_root})
        refs[0]["section_shard_ref"] = shard_ref.as_dict()
        refs[0]["shard_root"] = shard_root
    else:  # pragma: no cover - test helper contract
        raise AssertionError(mutation)
    component_body = dict(component_index)
    component_body.pop("component_index_root")
    component_root = digest_named_fields(COMPONENT_ARTIFACT_COMPONENT_INDEX_SCHEMA_V2, component_body)
    component_ref = cas.put_json({**component_body, "component_index_root": component_root})
    entry["component_index_ref"] = component_ref.as_dict()
    entry["component_index_root"] = component_root
    top_body = dict(top)
    top_body.pop("manifest_root")
    top_root = digest_named_fields(COMPONENT_ARTIFACT_MANIFEST_STORAGE_SCHEMA_V2, top_body)
    return cas.put_json({**top_body, "manifest_root": top_root})


def _manifest_with_two_artifact_file_shards() -> dict:
    value = manifest_payload()
    daily = value["components"][Component.DAILY_BIN.value]
    daily["artifact_partitions"][0]["files"].extend(
        {
            "relative_path": f"extra/{number:03d}.bin",
            "size_bytes": 17,
            "sha256": _digest(f"extra:{number}"),
            "instrument": "000001.SZ",
        }
        for number in range(128)
    )
    return value


@pytest.mark.parametrize(
    "mutation",
    ["missing", "reorder", "duplicate_boundary", "ordinal_bool"],
)
def test_v2_section_index_missing_reordered_or_duplicate_boundary_fails_closed(
    tmp_path,
    mutation: str,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    reference = seal_component_artifact_manifest(cas, _manifest_with_two_artifact_file_shards())
    tampered = _reseal_v2_nested_index(cas, reference, mutation)

    with pytest.raises(ComponentArtifactManifestError, match="section"):
        load_component_artifact_manifest(cas, tampered)


def test_v2_resealed_parent_chain_still_rejects_tampered_leaf_semantics(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    reference = seal_component_artifact_manifest(cas, _manifest_with_two_artifact_file_shards())
    tampered = _reseal_v2_nested_index(cas, reference, "leaf_semantic")

    with pytest.raises(
        ComponentArtifactManifestError,
        match="artifact file derived identity differs",
    ):
        load_component_artifact_manifest(cas, tampered)


def _planner_fixture(
    cas: CASStore,
    value: dict | None = None,
    *,
    legacy_v1: bool = False,
):
    payload = value or manifest_payload()
    baseline = load_component_artifact_manifest(
        cas,
        (
            _seal_component_artifact_manifest_v1(cas, payload)
            if legacy_v1
            else seal_component_artifact_manifest(cas, payload)
        ),
    )
    current = {
        component: CurrentComponentAuthority(
            partitions=tuple(item.as_dict() for item in baseline.component(component).source_partitions),
            adj_series=(
                CurrentAdjSeriesAuthority(
                    qfq_denominator_by_code=dict(baseline.component(component).adj_series.qfq_denominator_by_code),
                    ordered_adj_digest_by_code=dict(
                        baseline.component(component).adj_series.ordered_adj_digest_by_code
                    ),
                    adj_row_count_by_code=dict(baseline.component(component).adj_series.adj_row_count_by_code),
                    monthly_ordered_adj_by_code={},
                )
                if baseline.component(component).complete and baseline.component(component).adj_series is not None
                else None
            ),
        )
        for component in Component
        if baseline.component(component).complete
    }
    context = MixedPlannerContext(
        source_release_id="20260630-qe_hmm_full_v1-full-candidate",
        source_release_digest=_digest("release"),
        source_attestation_key=_digest("attestation"),
        dataset_start=date(2018, 8, 1),
        cutoff=date(2026, 7, 31),
        current_pit_snapshot_digest=baseline.pit_snapshot_digest,
        current_pit_instruments=("000001.SZ",),
        current_pit_span_digest_by_code={"000001.SZ": _digest("pit:000001.SZ")},
    )
    return baseline, current, context


def _actions(plan):
    return {item.component: item for item in plan.actions}


def test_mixed_planner_same_cutoff_reuses_every_complete_component(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, context = _planner_fixture(CASStore(store.root))

    actions = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )

    assert {item.action.value for item in actions.values()} == {"REUSE"}
    assert all(item.frozen_reuse is not None for item in actions.values())


def test_mixed_planner_first_rule_limit_coverage_is_exact_code_selective(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    payload = _two_code_payload()
    for component in (Component.DAILY_BIN, Component.MINUTE_BIN):
        payload["components"][component.value]["append_rules"][0]["datasets"].append("stk_limit")
    baseline, current, old_context = _planner_fixture(CASStore(store.root), payload)
    coverage = {
        "identity": "stk_limit_rule_coverage:2024-07-01_2024-07-31",
        "dataset": "stk_limit_rule_coverage",
        "partition_key": "2024-07-01_2024-07-31",
        "row_count": 1,
        "content_digest": _digest("limit-coverage"),
        "schema_digest": _digest("limit-coverage-schema"),
        "source_table_schema_digest": _digest("stk-limit-table-schema"),
        "source_code_membership_digest": _digest("stk-limit-membership"),
        "min_key": ["000001.SZ", "2024-07-22"],
        "max_key": ["000001.SZ", "2024-07-22"],
        "monthly_content_leaves": [_month("2024-07", "limit-coverage")],
        "affected_instruments": ["000001.SZ"],
    }
    for component in (Component.DAILY_BIN, Component.MINUTE_BIN):
        authority = current[component]
        current[component] = CurrentComponentAuthority(
            partitions=(*authority.partitions, coverage),
            adj_series=authority.adj_series,
        )
    context = MixedPlannerContext(
        source_release_id=old_context.source_release_id,
        source_release_digest=old_context.source_release_digest,
        source_attestation_key=old_context.source_attestation_key,
        dataset_start=old_context.dataset_start,
        cutoff=old_context.cutoff,
        current_pit_snapshot_digest=baseline.pit_snapshot_digest,
        current_pit_instruments=("000001.SZ", "000002.SZ"),
        current_pit_span_digest_by_code={
            "000001.SZ": _digest("pit:000001.SZ"),
            "000002.SZ": _digest("pit:000002.SZ"),
        },
    )

    actions = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )

    for component in (Component.DAILY_BIN, Component.MINUTE_BIN):
        action = actions[component]
        assert action.action.value == "SELECTIVE_REBUILD"
        assert action.frozen_reuse is not None
        scope = next(
            item
            for item in action.frozen_reuse.invalidation_scopes
            if item.get("source_partition") == coverage["identity"]
        )
        assert scope["months"] == ["2024-07"]
        assert scope["affected_instruments"] == ["000001.SZ"]
        assert action.estimated_work["source_rows"] == 1
        override_targets = {
            path for path in action.frozen_reuse.create_new_targets if path.startswith("csv_overrides/")
        }
        assert any(path.endswith("/000001.sz.csv") for path in override_targets)
        assert not any(path.endswith("/000002.sz.csv") for path in override_targets)
    assert actions[Component.FACTOR_H5_STATIC].action.value == "REUSE"
    assert actions[Component.DOMESTIC_INDEX_CONTEXT].action.value == "REUSE"

    coverage["affected_instruments"] = ["000003.SZ"]
    blocked = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )
    assert blocked[Component.DAILY_BIN].action.value == "FULL_REBUILD"
    assert blocked[Component.MINUTE_BIN].action.value == "FULL_REBUILD"


def test_legacy_v1_manifest_drives_all_components_through_planner_reuse(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, context = _planner_fixture(CASStore(store.root), legacy_v1=True)

    actions = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )

    assert set(actions) == set(Component)
    assert {item.action.value for item in actions.values()} == {"REUSE"}


def _assert_canonical_lineage_targets(
    frozen,
    *,
    event_instruments: tuple[str, ...],
    anchor_instruments: tuple[str, ...],
    non_lineage_targets: set[str],
) -> None:
    lineage = frozen.canonical_lineage
    assert lineage is not None
    assert lineage["capability"] == CANONICAL_LINEAGE_CAPABILITY
    event_key = lineage["event_key"]
    anchor_key = lineage["anchor_key"]
    assert isinstance(event_key, str)
    assert isinstance(anchor_key, str)
    expected_lineage = set(
        planned_lineage_paths(
            event_key=event_key,
            instruments=event_instruments,
        )
    )
    expected_lineage.update(
        planned_lineage_paths(
            event_key=anchor_key,
            instruments=anchor_instruments,
            anchor=True,
        )
    )
    actual_lineage = {path for path in frozen.create_new_targets if path.startswith("csv_lineage/")}
    assert actual_lineage == expected_lineage
    assert set(frozen.create_new_targets) == expected_lineage | non_lineage_targets


def test_mixed_planner_june_to_july_tail_consumes_exact_monthly_prefix(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, context = _planner_fixture(CASStore(store.root))
    daily = deepcopy(current[Component.DAILY_BIN].partitions[0])
    daily["partition_key"] = "2026-05-01_2026-07-31"
    daily["identity"] = "kline_daily_raw:2026-05-01_2026-07-31"
    daily["row_count"] = 6
    daily["content_digest"] = _digest("daily-july-tail")
    daily["monthly_content_leaves"].append(_month("2026-07", "daily_bin:july"))
    current[Component.DAILY_BIN] = CurrentComponentAuthority((daily,), current[Component.DAILY_BIN].adj_series)

    actions = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )

    daily_action = actions[Component.DAILY_BIN]
    assert daily_action.action.value == "INCREMENTAL"
    assert daily_action.frozen_reuse is not None
    assert daily_action.frozen_reuse.replace_existing_targets == ("artifact.bin",)
    _assert_canonical_lineage_targets(
        daily_action.frozen_reuse,
        event_instruments=("000001.SZ",),
        anchor_instruments=("000001.SZ",),
        non_lineage_targets={
            "csv_deltas/202607/000001.sz.csv",
            "csv_deltas/202607/manifest.json",
        },
    )
    assert daily_action.frozen_reuse.invalidation_scopes[0]["new_months"] == ["2026-07"]
    assert actions[Component.MINUTE_BIN].action.value == "REUSE"


def _open_pit(cutoff: date):
    return freeze_pit_snapshot(
        [
            {
                "ts_code": "000001.SZ",
                "eligible_start": date(2018, 8, 1),
                "eligible_end": date(2099, 12, 31),
                "entry_reason": None,
                "exit_reason": None,
            }
        ],
        universe_key="fixture",
        rule_version="v1",
        scope_start=date(2018, 8, 1),
        cutoff=cutoff,
        state_identity="fixture",
        source_fingerprint_sha256="1" * 64,
        parameter_hash="2" * 64,
    )


def test_pit_cutoff_extension_is_not_a_same_instrument_historical_revision(
    tmp_path,
) -> None:
    old_pit = _open_pit(date(2026, 6, 30))
    current_pit = _open_pit(date(2026, 7, 31))
    assert pit_span_digest_by_code(old_pit) == pit_span_digest_by_code(current_pit)

    value = manifest_payload()
    value["pit_snapshot_digest"] = old_pit.spans_sha256
    for component in (
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    ):
        value["components"][component.value]["pit_span_digest_by_code"] = pit_span_digest_by_code(old_pit)
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, old_context = _planner_fixture(CASStore(store.root), value)
    daily = deepcopy(current[Component.DAILY_BIN].partitions[0])
    daily["partition_key"] = "2026-05-01_2026-07-31"
    daily["identity"] = "kline_daily_raw:2026-05-01_2026-07-31"
    daily["row_count"] = 6
    daily["content_digest"] = _digest("daily-natural-cutoff-tail")
    daily["monthly_content_leaves"].append(_month("2026-07", "daily-natural-cutoff-tail"))
    current[Component.DAILY_BIN] = CurrentComponentAuthority((daily,), current[Component.DAILY_BIN].adj_series)
    context = MixedPlannerContext(
        source_release_id=old_context.source_release_id,
        source_release_digest=old_context.source_release_digest,
        source_attestation_key=old_context.source_attestation_key,
        dataset_start=old_context.dataset_start,
        cutoff=current_pit.cutoff,
        current_pit_snapshot_digest=current_pit.spans_sha256,
        current_pit_instruments=("000001.SZ",),
        current_pit_span_digest_by_code=pit_span_digest_by_code(current_pit),
    )

    action = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )[Component.DAILY_BIN]
    assert action.action.value == "INCREMENTAL"
    assert action.frozen_reuse is not None
    assert any(scope["kind"] == "pit_cutoff_extension" for scope in action.frozen_reuse.invalidation_scopes)
    assert not any(scope["kind"] == "pit_span_change" for scope in action.frozen_reuse.invalidation_scopes)


def _baseline_with_csv_lineage() -> dict:
    value = manifest_payload()
    daily = value["components"][Component.DAILY_BIN.value]
    files = daily["artifact_partitions"][0]["files"]
    files.extend(
        [
            {
                "relative_path": "csv_deltas/202606/000001.sz.csv",
                "size_bytes": 19,
                "sha256": _digest("old-delta-csv"),
                "instrument": "000001.SZ",
            },
            {
                "relative_path": "csv_deltas/202606/manifest.json",
                "size_bytes": 23,
                "sha256": _digest("old-delta-manifest"),
                "instrument": None,
            },
        ]
    )
    for rule_name in ("append_rules",):
        rule = daily[rule_name][0]
        rule["replace_existing_targets"].append("csv_deltas/202606/manifest.json")
        rule["writer_targets_by_instrument"]["000001.SZ"].append("csv_deltas/202606/000001.sz.csv")
    daily["pit_mutation_rule"]["replace_existing_targets"].append("csv_deltas/202606/manifest.json")
    daily["pit_mutation_rule"]["writer_targets_by_instrument"]["000001.SZ"].append("csv_deltas/202606/000001.sz.csv")
    daily["adj_series"]["shared_writer_targets"].append("csv_deltas/202606/manifest.json")
    daily["adj_series"]["writer_targets_by_code"]["000001.SZ"].append("csv_deltas/202606/000001.sz.csv")
    return value


def test_second_month_tail_never_rewrites_prior_delta_lineage(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, context = _planner_fixture(CASStore(store.root), _baseline_with_csv_lineage())
    daily = deepcopy(current[Component.DAILY_BIN].partitions[0])
    daily["partition_key"] = "2026-05-01_2026-07-31"
    daily["identity"] = "kline_daily_raw:2026-05-01_2026-07-31"
    daily["row_count"] = 6
    daily["content_digest"] = _digest("second-month-tail")
    daily["monthly_content_leaves"].append(_month("2026-07", "second-month"))
    current[Component.DAILY_BIN] = CurrentComponentAuthority((daily,), current[Component.DAILY_BIN].adj_series)

    action = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )[Component.DAILY_BIN]
    assert action.action.value == "INCREMENTAL"
    assert action.frozen_reuse is not None
    assert not any(path.startswith("csv_deltas/202606/") for path in action.frozen_reuse.replace_existing_targets)


def test_historical_revision_after_delta_keeps_old_lineage_immutable(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, old_context = _planner_fixture(CASStore(store.root), _baseline_with_csv_lineage())
    daily = deepcopy(current[Component.DAILY_BIN].partitions[0])
    daily["content_digest"] = _digest("historical-after-delta")
    daily["monthly_content_leaves"][1] = _month("2026-06", "historical-after-delta")
    current[Component.DAILY_BIN] = CurrentComponentAuthority((daily,), current[Component.DAILY_BIN].adj_series)
    context = MixedPlannerContext(
        source_release_id=old_context.source_release_id,
        source_release_digest=old_context.source_release_digest,
        source_attestation_key=old_context.source_attestation_key,
        dataset_start=old_context.dataset_start,
        cutoff=date(2026, 6, 30),
        current_pit_snapshot_digest=baseline.pit_snapshot_digest,
        current_pit_instruments=("000001.SZ",),
        current_pit_span_digest_by_code={"000001.SZ": _digest("pit:000001.SZ")},
    )

    action = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )[Component.DAILY_BIN]
    assert action.action.value == "SELECTIVE_REBUILD"
    assert action.frozen_reuse is not None
    assert not any(
        path.startswith(("csv_deltas/", "csv_overrides/")) for path in action.frozen_reuse.replace_existing_targets
    )


def test_mixed_planner_tail_without_matching_monthly_prefix_fails_only_component(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, context = _planner_fixture(CASStore(store.root))
    daily = deepcopy(current[Component.DAILY_BIN].partitions[0])
    daily["partition_key"] = "2026-05-01_2026-07-31"
    daily["identity"] = "kline_daily_raw:2026-05-01_2026-07-31"
    daily["content_digest"] = _digest("tail-with-revised-prefix")
    daily["monthly_content_leaves"][1] = _month("2026-06", "daily_bin:revised-june")
    daily["monthly_content_leaves"].append(_month("2026-07", "daily_bin:july"))
    current[Component.DAILY_BIN] = CurrentComponentAuthority((daily,), current[Component.DAILY_BIN].adj_series)

    actions = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )
    assert actions[Component.DAILY_BIN].action.value == "FULL_REBUILD"
    assert actions[Component.MINUTE_BIN].action.value == "REUSE"


def test_mixed_planner_new_ipo_has_exact_create_targets_and_keeps_index_reuse(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, context = _planner_fixture(CASStore(store.root))
    daily = deepcopy(current[Component.DAILY_BIN].partitions[0])
    daily["partition_key"] = "2026-05-01_2026-07-31"
    daily["identity"] = "kline_daily_raw:2026-05-01_2026-07-31"
    daily["row_count"] = 6
    daily["content_digest"] = _digest("daily-tail-with-new-ipo")
    daily["monthly_content_leaves"].append(_month("2026-07", "daily-tail-with-new-ipo"))
    current[Component.DAILY_BIN] = CurrentComponentAuthority((daily,), current[Component.DAILY_BIN].adj_series)
    context = MixedPlannerContext(
        source_release_id=context.source_release_id,
        source_release_digest=context.source_release_digest,
        source_attestation_key=context.source_attestation_key,
        dataset_start=context.dataset_start,
        cutoff=context.cutoff,
        current_pit_snapshot_digest=_digest("pit-with-new-ipo"),
        current_pit_instruments=("000001.SZ", "000002.SZ"),
        current_pit_span_digest_by_code={
            "000001.SZ": _digest("pit:000001.SZ"),
            "000002.SZ": _digest("pit:000002.SZ"),
        },
    )
    for component in (
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    ):
        existing = current[component].adj_series
        assert existing is not None
        current[component] = CurrentComponentAuthority(
            current[component].partitions,
            CurrentAdjSeriesAuthority(
                qfq_denominator_by_code={
                    **dict(existing.qfq_denominator_by_code),
                    "000002.SZ": "1.1",
                },
                ordered_adj_digest_by_code={
                    **dict(existing.ordered_adj_digest_by_code),
                    "000002.SZ": _digest("ordered:000002.SZ"),
                },
                adj_row_count_by_code={
                    **dict(existing.adj_row_count_by_code),
                    "000002.SZ": 20,
                },
                monthly_ordered_adj_by_code={},
            ),
        )

    actions = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )

    factor_action = actions[Component.FACTOR_H5_STATIC]
    assert factor_action.action.value == "INCREMENTAL"
    assert factor_action.frozen_reuse is not None
    assert factor_action.frozen_reuse.create_new_targets == ("new/000002.sz.bin",)
    minute_action = actions[Component.MINUTE_BIN]
    assert minute_action.action.value == "INCREMENTAL"
    assert minute_action.frozen_reuse is not None
    _assert_canonical_lineage_targets(
        minute_action.frozen_reuse,
        event_instruments=("000002.SZ",),
        anchor_instruments=("000001.SZ",),
        non_lineage_targets={
            "csv/000002.sz.csv",
            "new/000002.sz.bin",
        },
    )
    daily_action = actions[Component.DAILY_BIN]
    assert daily_action.action.value == "INCREMENTAL"
    assert daily_action.frozen_reuse is not None
    _assert_canonical_lineage_targets(
        daily_action.frozen_reuse,
        event_instruments=("000001.SZ", "000002.SZ"),
        anchor_instruments=("000001.SZ",),
        non_lineage_targets={
            "csv/000002.sz.csv",
            "csv_deltas/202607/000001.sz.csv",
            "csv_deltas/202607/manifest.json",
            "new/000002.sz.bin",
        },
    )
    pit_scope = next(
        scope for scope in daily_action.frozen_reuse.invalidation_scopes if scope["kind"] == "pit_span_change"
    )
    assert pit_scope["new_instruments"] == ["000002.SZ"]
    assert pit_scope["changed_instruments"] == []
    assert actions[Component.DOMESTIC_INDEX_CONTEXT].action.value == "REUSE"


def test_mixed_planner_same_instrument_pit_span_revision_is_selective(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, old_context = _planner_fixture(CASStore(store.root))
    context = MixedPlannerContext(
        source_release_id=old_context.source_release_id,
        source_release_digest=old_context.source_release_digest,
        source_attestation_key=old_context.source_attestation_key,
        dataset_start=old_context.dataset_start,
        cutoff=old_context.cutoff,
        current_pit_snapshot_digest=_digest("pit-span-revised"),
        current_pit_instruments=("000001.SZ",),
        current_pit_span_digest_by_code={"000001.SZ": _digest("pit:000001.SZ:revised")},
    )

    actions = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )

    for component in (
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    ):
        action = actions[component]
        assert action.action.value == "SELECTIVE_REBUILD"
        assert action.frozen_reuse is not None
        assert action.frozen_reuse.replace_existing_targets == ("artifact.bin",)
        pit_scope = next(
            scope for scope in action.frozen_reuse.invalidation_scopes if scope["kind"] == "pit_span_change"
        )
        assert pit_scope["changed_instruments"] == ["000001.SZ"]
    assert actions[Component.DOMESTIC_INDEX_CONTEXT].action.value == "REUSE"


def _two_code_payload() -> dict:
    value = manifest_payload()
    for component in (
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    ):
        entry = value["components"][component.value]
        entry["pit_instruments"].append("000002.SZ")
        entry["pit_span_digest_by_code"]["000002.SZ"] = _digest("pit:000002.SZ")
        entry["pit_mutation_rule"]["writer_targets_by_instrument"]["000002.SZ"] = ["artifact.bin"]
        entry["adj_series"]["qfq_denominator_by_code"]["000002.SZ"] = "1.1"
        entry["adj_series"]["ordered_adj_digest_by_code"]["000002.SZ"] = _digest("ordered:000002.SZ")
        entry["adj_series"]["adj_row_count_by_code"]["000002.SZ"] = 20
        entry["adj_series"]["writer_targets_by_code"]["000002.SZ"] = ["artifact.bin"]
    return value


def test_mixed_planner_removed_pit_instrument_fails_related_components_full(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, old_context = _planner_fixture(CASStore(store.root), _two_code_payload())
    for component in (
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    ):
        existing = current[component].adj_series
        assert existing is not None
        current[component] = CurrentComponentAuthority(
            current[component].partitions,
            CurrentAdjSeriesAuthority(
                qfq_denominator_by_code={"000001.SZ": "1.25"},
                ordered_adj_digest_by_code={"000001.SZ": _digest("ordered")},
                adj_row_count_by_code={"000001.SZ": 2000},
                monthly_ordered_adj_by_code={},
            ),
        )
    context = MixedPlannerContext(
        source_release_id=old_context.source_release_id,
        source_release_digest=old_context.source_release_digest,
        source_attestation_key=old_context.source_attestation_key,
        dataset_start=old_context.dataset_start,
        cutoff=old_context.cutoff,
        current_pit_snapshot_digest=_digest("pit-after-removal"),
        current_pit_instruments=("000001.SZ",),
        current_pit_span_digest_by_code={"000001.SZ": _digest("pit:000001.SZ")},
    )
    actions = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )
    assert {
        actions[component].action.value
        for component in (
            Component.DAILY_BIN,
            Component.MINUTE_BIN,
            Component.FACTOR_H5_STATIC,
        )
    } == {"FULL_REBUILD"}
    assert actions[Component.DOMESTIC_INDEX_CONTEXT].action.value == "REUSE"


def test_mixed_planner_historical_revision_selects_exact_dependency_target(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, context = _planner_fixture(CASStore(store.root))
    daily = deepcopy(current[Component.DAILY_BIN].partitions[0])
    daily["content_digest"] = _digest("historical-daily-revision")
    daily["monthly_content_leaves"][1] = _month("2026-06", "daily_bin:historical-revision")
    current[Component.DAILY_BIN] = CurrentComponentAuthority((daily,), current[Component.DAILY_BIN].adj_series)

    actions = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )

    daily_action = actions[Component.DAILY_BIN]
    assert daily_action.action.value == "SELECTIVE_REBUILD"
    assert daily_action.frozen_reuse is not None
    assert daily_action.frozen_reuse.replace_existing_targets == ("artifact.bin",)
    assert daily_action.frozen_reuse.invalidation_scopes[0]["months"] == ["2026-06"]
    assert daily_action.frozen_reuse.invalidation_scopes[0]["affected_instruments"] == ["000001.SZ"]
    assert actions[Component.MINUTE_BIN].action.value == "REUSE"


def test_index_context_tail_plus_historical_revision_explicitly_uses_full(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, context = _planner_fixture(CASStore(store.root))
    historical = deepcopy(current[Component.DOMESTIC_INDEX_CONTEXT].partitions[0])
    historical["content_digest"] = _digest("index-historical-revision")
    historical["monthly_content_leaves"][1] = _month("2026-06", "index-historical-revision")
    tail = deepcopy(historical)
    tail["partition_key"] = "2026-07-01_2026-07-31"
    tail["identity"] = "index_daily:2026-07-01_2026-07-31"
    tail["row_count"] = 2
    tail["content_digest"] = _digest("index-july-tail")
    tail["min_key"] = ["000001.SZ", "2026-07-01"]
    tail["max_key"] = ["000001.SZ", "2026-07-31"]
    tail["monthly_content_leaves"] = [_month("2026-07", "index-july-tail")]
    current[Component.DOMESTIC_INDEX_CONTEXT] = CurrentComponentAuthority(
        (
            historical,
            tail,
        ),
        None,
    )

    action = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )[Component.DOMESTIC_INDEX_CONTEXT]
    assert action.action.value == "FULL_REBUILD"
    assert action.frozen_reuse is None
    assert "index tail plus historical revision" in action.reason


def _adj_baseline_payload() -> dict:
    value = manifest_payload()
    for component in (
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    ):
        entry = value["components"][component.value]
        old_identity = entry["source_partitions"][0]["identity"]
        entry["source_partitions"][0]["dataset"] = "adj_factor"
        entry["source_partitions"][0]["identity"] = "adj_factor:2026-05-01_2026-06-30"
        entry["artifact_partitions"][0]["source_partition_identities"] = ["adj_factor:2026-05-01_2026-06-30"]
        entry["artifact_partitions"][0]["dependency_edges"] = [f"adj_factor->{component.value}"]
        entry["append_rules"][0]["datasets"] = ["adj_factor"]
        assert old_identity != entry["source_partitions"][0]["identity"]
    return value


def test_mixed_planner_qfq_denominator_change_is_code_full_history_selective(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, context = _planner_fixture(CASStore(store.root), _adj_baseline_payload())
    for component in (
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    ):
        partition = deepcopy(current[component].partitions[0])
        partition["content_digest"] = _digest(f"adj-revised:{component.value}")
        partition["monthly_content_leaves"][1] = _month("2026-06", f"adj-revised:{component.value}:june")
        old_adj = current[component].adj_series
        assert old_adj is not None
        current[component] = CurrentComponentAuthority(
            (partition,),
            CurrentAdjSeriesAuthority(
                qfq_denominator_by_code={"000001.SZ": "1.5"},
                ordered_adj_digest_by_code={"000001.SZ": _digest(f"ordered-revised:{component.value}")},
                adj_row_count_by_code={"000001.SZ": 2000},
                monthly_ordered_adj_by_code={},
            ),
        )

    actions = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )

    for component in (Component.DAILY_BIN, Component.MINUTE_BIN):
        action = actions[component]
        assert action.action.value == "SELECTIVE_REBUILD"
        assert action.frozen_reuse is not None
        assert any(
            scope["kind"] == "qfq_denominator_change"
            and scope["start"] == "2018-08-01"
            and scope["end"] == "2026-07-31"
            for scope in action.frozen_reuse.invalidation_scopes
        )
    assert actions[Component.FACTOR_H5_STATIC].action.value == "SELECTIVE_REBUILD"


def test_mixed_planner_qfq_historical_numerator_uses_month_and_factor_window(
    tmp_path,
) -> None:
    value = _adj_baseline_payload()
    baseline_month = {
        "ordered_digest": _digest("adj:2026-06:baseline"),
        "row_count": 20,
        "min_date": "2026-06-01",
        "max_date": "2026-06-30",
    }
    for component in (
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    ):
        value["components"][component.value]["adj_series"]["monthly_ordered_adj_by_code"] = {
            "000001.SZ": {"2026-06": baseline_month}
        }
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, context = _planner_fixture(CASStore(store.root), value)
    for component in (
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    ):
        partition = deepcopy(current[component].partitions[0])
        partition["content_digest"] = _digest(f"adj-history:{component.value}")
        partition["monthly_content_leaves"][1] = _month("2026-06", f"adj-history:{component.value}:june")
        current[component] = CurrentComponentAuthority(
            (partition,),
            CurrentAdjSeriesAuthority(
                qfq_denominator_by_code={"000001.SZ": "1.25"},
                ordered_adj_digest_by_code={"000001.SZ": _digest(f"ordered-history:{component.value}")},
                adj_row_count_by_code={"000001.SZ": 2000},
                monthly_ordered_adj_by_code={
                    "000001.SZ": {
                        "2026-06": {
                            **baseline_month,
                            "ordered_digest": _digest(f"adj:2026-06:current:{component.value}"),
                        }
                    }
                },
            ),
        )

    actions = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )
    for component in (
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    ):
        action = actions[component]
        assert action.action.value == "SELECTIVE_REBUILD"
        assert action.frozen_reuse is not None
        scope = next(
            item
            for item in action.frozen_reuse.invalidation_scopes
            if item["kind"] == "qfq_historical_numerator_revision"
        )
        assert scope["months"] == ["2026-06"]
        assert scope["downstream_observations"] == (10 if component is Component.FACTOR_H5_STATIC else 0)
    factor_targets = actions[Component.FACTOR_H5_STATIC].frozen_reuse.create_new_targets
    assert "partitions/daily_pv/2026-06.parquet" in factor_targets
    assert "partitions/daily_pv/2026-07.parquet" in factor_targets


def test_mixed_planner_missing_current_qfq_authority_fails_related_only(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    baseline, current, context = _planner_fixture(CASStore(store.root), _adj_baseline_payload())
    for component in (
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    ):
        partition = deepcopy(current[component].partitions[0])
        partition["content_digest"] = _digest(f"adj-change:{component.value}")
        partition["monthly_content_leaves"][1] = _month("2026-06", f"adj-change:{component.value}")
        current[component] = CurrentComponentAuthority((partition,), None)

    actions = _actions(
        build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=True,
        )
    )
    assert {
        actions[component].action.value
        for component in (
            Component.DAILY_BIN,
            Component.MINUTE_BIN,
            Component.FACTOR_H5_STATIC,
        )
    } == {"FULL_REBUILD"}
    assert actions[Component.DOMESTIC_INDEX_CONTEXT].action.value == "REUSE"
