from __future__ import annotations

from dataclasses import replace
from datetime import date
import json

import pytest

from backend.services.dataset_release.monthly_component_preparation import (
    ComponentInputIdentity,
    ComponentPreparationError,
    preparation_plan,
    seal_prepared_component,
    validate_prepared_component,
)
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.canonical import digest_named_fields


OPERATION = "dmr_" + "1" * 32
CUTOFF = date(2026, 9, 30)


def identity(component="day", **changes):
    value = ComponentInputIdentity(
        component=component,
        cutoff=CUTOFF,
        predecessor_profile_sha256="a" * 64,
        effective_source_sha256="b" * 64,
        pit_sha256="c" * 64,
        qfq_sha256="d" * 64,
        producer_sha256="e" * 64,
        schema_sha256="f" * 64,
        build_parameters_sha256="0" * 64,
        validation_policy_sha256="2" * 64,
    )
    return replace(value, **changes)


def seal(tmp_path, *, current=None):
    root = tmp_path / "component"
    root.mkdir()
    (root / "feature.bin").write_bytes(b"real-writer-output-placeholder-for-contract-test")
    (root / "validation.json").write_bytes(canonical_json_bytes({"result": "PASS"}) + b"\n")
    selected = current or identity()
    ref = seal_prepared_component(
        preparation_root=tmp_path,
        component_root=root,
        operation_id=OPERATION,
        source_snapshot_id="postgres:1-AA-1",
        identity=selected,
        output_paths=("feature.bin",),
        validation_paths=("validation.json",),
    )
    return root, ref, selected


def test_margin_defers_factor_only_without_claiming_completeness():
    plan = preparation_plan(operation_id=OPERATION, cutoff=CUTOFF, blocking_datasets=("margin_detail",))
    assert plan["components"]["factor"]["status"] == "DEFERRED"
    assert plan["components"]["factor"]["blocking_datasets"] == ["margin_detail"]
    assert all(row["status"] == "ELIGIBLE" for name, row in plan["components"].items() if name != "factor")
    assert plan["publication_allowed"] is False
    assert plan["consistent_input_set_complete"] is False
    assert plan["prepared_component_count"] == 0


@pytest.mark.parametrize("dataset", ["adj_factor", "kline_daily_raw"])
def test_price_dependencies_propagate_to_minute_and_benchmark(dataset):
    plan = preparation_plan(operation_id=OPERATION, cutoff=CUTOFF, blocking_datasets=(dataset,))
    deferred = {name for name, row in plan["components"].items() if row["status"] == "DEFERRED"}
    assert deferred == {"day", "minute", "factor", "benchmark"}


@pytest.mark.parametrize("dataset", ["trading_calendar", "stock_universe_pit", "unknown_source"])
def test_control_or_unknown_blocker_cannot_be_skipped(dataset):
    plan = preparation_plan(operation_id=OPERATION, cutoff=CUTOFF, blocking_datasets=(dataset,))
    assert all(row["status"] == "DEFERRED" for row in plan["components"].values())


def test_moneyflow_audit_alias_also_defers_sector_aggregation():
    plan = preparation_plan(operation_id=OPERATION, cutoff=CUTOFF, blocking_datasets=("stock_moneyflow_ts",))
    assert {name for name, row in plan["components"].items() if row["status"] == "DEFERRED"} == {
        "factor",
        "sector_context",
    }


def test_index_dependency_is_also_present_for_daily_provider_benchmark():
    plan = preparation_plan(operation_id=OPERATION, cutoff=CUTOFF, blocking_datasets=("index_daily",))
    assert {name for name, row in plan["components"].items() if row["status"] == "DEFERRED"} == {
        "day",
        "index",
        "benchmark",
    }


def test_plan_is_order_independent_and_invalid_operation_is_rejected():
    first = preparation_plan(operation_id=OPERATION, cutoff=CUTOFF, blocking_datasets=("adj_factor", "margin_detail"))
    second = preparation_plan(
        operation_id=OPERATION, cutoff=CUTOFF, blocking_datasets=("margin_detail", "adj_factor", "margin_detail")
    )
    assert first == second
    with pytest.raises(ComponentPreparationError, match="operation"):
        preparation_plan(operation_id="../escape", cutoff=CUTOFF, blocking_datasets=())


def test_same_component_identity_can_be_read_after_a_different_final_snapshot(tmp_path):
    root, ref, current = seal(tmp_path)
    result = validate_prepared_component(
        preparation_root=tmp_path,
        component_root=root,
        receipt_sha256=ref["sha256"],
        operation_id=OPERATION,
        expected_identity=current,
    )
    assert result["status"] == "PREPARED_UNPUBLISHED"
    assert result["publication_allowed"] is False
    assert "source_content_root" not in current.payload()


@pytest.mark.parametrize(
    "field",
    [
        "predecessor_profile_sha256",
        "effective_source_sha256",
        "pit_sha256",
        "qfq_sha256",
        "producer_sha256",
        "schema_sha256",
        "build_parameters_sha256",
        "validation_policy_sha256",
    ],
)
def test_any_dependency_drift_invalidates_preparation(tmp_path, field):
    root, ref, current = seal(tmp_path)
    with pytest.raises(ComponentPreparationError, match="identity"):
        validate_prepared_component(
            preparation_root=tmp_path,
            component_root=root,
            receipt_sha256=ref["sha256"],
            operation_id=OPERATION,
            expected_identity=replace(current, **{field: "9" * 64}),
        )


@pytest.mark.parametrize("file_name", ["feature.bin", "validation.json"])
def test_output_or_validation_evidence_drift_is_rejected(tmp_path, file_name):
    root, ref, current = seal(tmp_path)
    (root / file_name).write_bytes(b"tampered")
    with pytest.raises(ComponentPreparationError, match="bytes"):
        validate_prepared_component(
            preparation_root=tmp_path,
            component_root=root,
            receipt_sha256=ref["sha256"],
            operation_id=OPERATION,
            expected_identity=current,
        )


def test_duplicate_escape_missing_evidence_and_receipt_overwrite_are_rejected(tmp_path):
    root, _, current = seal(tmp_path)
    base = dict(
        preparation_root=tmp_path,
        component_root=root,
        operation_id=OPERATION,
        source_snapshot_id="postgres:1-AA-1",
        identity=current,
    )
    for paths in [("feature.bin", "feature.bin"), ("../feature.bin",), ()]:
        with pytest.raises(ComponentPreparationError):
            seal_prepared_component(**base, output_paths=paths, validation_paths=("validation.json",))
    with pytest.raises(ComponentPreparationError):
        seal_prepared_component(**base, output_paths=("feature.bin",), validation_paths=())
    with pytest.raises(FileExistsError):
        seal_prepared_component(**base, output_paths=("feature.bin",), validation_paths=("validation.json",))


def test_other_operation_and_malformed_receipt_are_rejected(tmp_path):
    root, ref, current = seal(tmp_path)
    with pytest.raises(ComponentPreparationError, match="operation"):
        validate_prepared_component(
            preparation_root=tmp_path,
            component_root=root,
            receipt_sha256=ref["sha256"],
            operation_id="dmr_" + "3" * 32,
            expected_identity=current,
        )
    path = root / "prepared-component.json"
    value = json.loads(path.read_bytes())
    value["extra"] = True
    path.write_bytes(canonical_json_bytes(value) + b"\n")
    with pytest.raises(ComponentPreparationError):
        validate_prepared_component(
            preparation_root=tmp_path,
            component_root=root,
            receipt_sha256=ref["sha256"],
            operation_id=OPERATION,
            expected_identity=current,
        )


def test_file_link_rejected_without_resolving_away_its_identity(tmp_path, monkeypatch):
    from backend.services.dataset_release import monthly_component_preparation as prep

    root, ref, current = seal(tmp_path)
    original = prep._is_link
    monkeypatch.setattr(prep, "_is_link", lambda path: path.name == "feature.bin" or original(path))
    with pytest.raises(ComponentPreparationError, match="link"):
        validate_prepared_component(
            preparation_root=tmp_path,
            component_root=root,
            receipt_sha256=ref["sha256"],
            operation_id=OPERATION,
            expected_identity=current,
        )


@pytest.mark.parametrize("field", ["cutoff", "component"])
def test_preparation_cannot_cross_cutoff_or_component(tmp_path, field):
    root, ref, current = seal(tmp_path)
    change = date(2026, 10, 30) if field == "cutoff" else "minute"
    with pytest.raises(ComponentPreparationError, match="identity"):
        validate_prepared_component(
            preparation_root=tmp_path,
            component_root=root,
            receipt_sha256=ref["sha256"],
            operation_id=OPERATION,
            expected_identity=replace(current, **{field: change}),
        )


def test_dependency_contract_contains_every_formal_producer_dependency():
    from backend.services.dataset_release.monthly_component_preparation import component_dependencies
    from backend.services.dataset_release.artifact_ready_source import _COMPONENT_DATASETS
    from backend.services.dataset_release.contracts import Component
    from backend.services.dataset_release.source_authority import PRODUCTION_QUERY_SPECS

    dependencies = component_dependencies()
    for name, component in {
        "day": Component.DAILY_BIN,
        "minute": Component.MINUTE_BIN,
        "factor": Component.FACTOR_H5_STATIC,
        "index": Component.DOMESTIC_INDEX_CONTEXT,
    }.items():
        assert set(_COMPONENT_DATASETS[component]).issubset(dependencies[name])
        assert {query.query_id for query in PRODUCTION_QUERY_SPECS.values() if component in query.components}.issubset(
            dependencies[name]
        )


@pytest.mark.parametrize(
    "mutation", ["schema", "ready", "missing_validation", "extra", "duplicate", "bad_size", "bad_digest"]
)
def test_matching_file_sha_does_not_bypass_receipt_contract(tmp_path, mutation):
    import hashlib
    from backend.services.dataset_release.monthly_component_preparation import RECEIPT_SCHEMA

    root, _, current = seal(tmp_path)
    path = root / "prepared-component.json"
    value = json.loads(path.read_bytes())
    if mutation == "schema":
        value["schema_version"] = "aistock_monthly_source_snapshot_identity_v1"
    elif mutation == "ready":
        value["publication_allowed"] = True
    elif mutation == "missing_validation":
        value["validation_refs"] = []
    elif mutation == "extra":
        value["extra"] = True
    elif mutation == "duplicate":
        value["output_refs"].append(dict(value["output_refs"][0]))
    elif mutation == "bad_size":
        value["output_refs"][0]["size"] = True
    else:
        value["canonical_digest"] = "7" * 64
    if mutation != "bad_digest":
        value["canonical_digest"] = digest_named_fields(
            RECEIPT_SCHEMA, {k: v for k, v in value.items() if k != "canonical_digest"}
        )
    raw = canonical_json_bytes(value) + b"\n"
    path.write_bytes(raw)
    with pytest.raises(ComponentPreparationError):
        validate_prepared_component(
            preparation_root=tmp_path,
            component_root=root,
            receipt_sha256=hashlib.sha256(raw).hexdigest(),
            operation_id=OPERATION,
            expected_identity=current,
        )


def test_price_identity_requires_qfq_and_strict_hashes():
    with pytest.raises(ComponentPreparationError, match="QFQ"):
        identity(qfq_sha256=None)
    with pytest.raises(ComponentPreparationError, match="digest"):
        identity(pit_sha256="not-a-hash")
    with pytest.raises(ComponentPreparationError, match="canonical"):
        identity(producer_sha256="E" * 64)
    assert identity("stock_pools", qfq_sha256=None).qfq_sha256 is None


def test_actual_uppercase_exchange_file_is_preserved_but_case_aliases_are_rejected(tmp_path):
    root = tmp_path / "index"
    root.mkdir()
    (root / "000300.SH.csv").write_bytes(b"frozen official index CSV")
    (root / "validation.json").write_bytes(b"domain validation")
    current = identity("index", qfq_sha256=None)
    reference = seal_prepared_component(
        preparation_root=tmp_path,
        component_root=root,
        operation_id=OPERATION,
        source_snapshot_id="postgres:1-AA-1",
        identity=current,
        output_paths=("000300.SH.csv",),
        validation_paths=("validation.json",),
    )
    receipt = validate_prepared_component(
        preparation_root=tmp_path,
        component_root=root,
        operation_id=OPERATION,
        expected_identity=current,
        receipt_sha256=reference["sha256"],
    )
    assert receipt["output_refs"][0]["path"] == "000300.SH.csv"
    with pytest.raises(ComponentPreparationError, match="duplicated"):
        seal_prepared_component(
            preparation_root=tmp_path,
            component_root=root,
            operation_id=OPERATION,
            source_snapshot_id="postgres:1-AA-1",
            identity=current,
            output_paths=("000300.SH.csv", "000300.sh.csv"),
            validation_paths=("validation.json",),
        )


def test_hardlinked_preparation_output_is_not_a_private_checkpoint(tmp_path):
    import os

    root, ref, current = seal(tmp_path)
    os.link(root / "feature.bin", tmp_path / "other-owner-feature.bin")
    with pytest.raises(ComponentPreparationError, match="hardlinked"):
        validate_prepared_component(
            preparation_root=tmp_path,
            component_root=root,
            operation_id=OPERATION,
            expected_identity=current,
            receipt_sha256=ref["sha256"],
        )


def test_preparation_schema_is_rejected_by_formal_artifact_ready_loader(tmp_path):
    from types import SimpleNamespace
    from backend.services.dataset_release.artifact_ready_source import (
        load_artifact_ready_contract,
        ArtifactReadySourceError,
    )
    from backend.services.dataset_release.cas_store import CASStore
    from backend.services.dataset_release.control_store import ControlStore

    root = tmp_path / "cas"
    ControlStore.initialize(root)
    cas = CASStore(root)
    value = preparation_plan(operation_id=OPERATION, cutoff=CUTOFF, blocking_datasets=("margin_detail",))
    ref = cas.put_json(value)
    with pytest.raises(ArtifactReadySourceError):
        load_artifact_ready_contract(
            cas,
            SimpleNamespace(profile="qe_hmm_full_v2"),
            ref,
            expected_source_content_root="a" * 64,
            expected_pit_snapshot_digest="b" * 64,
        )


def test_real_preflight_keeps_source_blocked_and_exposes_honest_dependency_plan():
    from contextlib import contextmanager
    from types import SimpleNamespace
    from backend.services.dataset_release.monthly_postgres_source import _preflight_refresh_readiness
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseSourceBlocked
    from backend.services.dataset_release.source_authority import SourceAuditIncomplete

    class Ledger:
        def partition_digest(self, dataset, start, cutoff):
            if dataset == "margin_detail":
                raise SourceAuditIncomplete(
                    "delay",
                    context={
                        "dataset": dataset,
                        "unusable_sample": [cutoff.isoformat()],
                        "unusable_count": 1,
                    },
                )
            return "a" * 64

    @contextmanager
    def session_factory(_policy):
        yield object()

    authority = SimpleNamespace(
        _freeze_refresh_audit=lambda *_args, **_kwargs: Ledger(),
        _database_query_specs=lambda: [
            SimpleNamespace(audit_dataset="margin_detail", date_expression="trade_date", start_policy="daily"),
            SimpleNamespace(audit_dataset="kline_daily_raw", date_expression="trade_date", start_policy="daily"),
        ],
    )
    profile = SimpleNamespace(resource_policy=None, start_date=date(2018, 8, 1), minute_start_date=date(2024, 7, 1))
    with pytest.raises(MonthlyReleaseSourceBlocked) as error:
        _preflight_refresh_readiness(authority, session_factory, profile=profile, cutoff=CUTOFF, operation_id=OPERATION)
    result = error.value.context
    assert result["source_payload_materialized"] is False
    assert result["component_preparation_execution"] == "NOT_STARTED"
    assert result["component_preparation_plan"]["deferred_component_count"] == 1
    assert result["component_preparation_plan"]["eligible_component_count"] == 7
    assert result["component_preparation_plan"]["prepared_component_count"] == 0
