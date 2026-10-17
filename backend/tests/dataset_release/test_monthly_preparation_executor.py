from contextlib import contextmanager
from dataclasses import replace
from datetime import date
from pathlib import Path
import os
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.dataset_release.artifact_ready_build_source import (
    ArtifactReadyBuildSource,
    ArtifactReadyPreparationBuildSource,
)
from backend.services.dataset_release.build_stage import BuildStageInvocation
from backend.services.dataset_release.canonical import canonical_json_bytes, digest_named_fields
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.contracts import Component, ComponentAction
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.monthly_component_preparation import ComponentPreparationError
from backend.services.dataset_release.monthly_preparation_executor import (
    MonthlyPrivatePhysicalPreparationExecutor,
    adopt_prepared_physical_components,
)
from backend.services.dataset_release.monthly_worker import ProducerContext


@pytest.fixture
def physical(tmp_path):
    ControlStore.initialize(tmp_path / "control")
    cas = CASStore(tmp_path / "control")
    catalog = tmp_path / "candidates"
    catalog.mkdir()
    cutoff = date(2026, 9, 30)
    op = "dmr_" + "1" * 32
    profile = SimpleNamespace(
        candidate_root=str(catalog),
        profile="qe_hmm_full_v2",
        indices=DOMESTIC_INDEX_DEFINITIONS,
        stage_timeouts_seconds={"full_build": 3600},
        semantic_profile_digest="a" * 64,
        qlib_stock_schema_digest="b" * 64,
        qlib_toolchain=SimpleNamespace(build_verified=lambda _: None, digest="c" * 64),
        resource_policy_digest="d" * 64,
        pressure_ladder={
            name: (100,)
            for name in (
                "row_group_rows",
                "h5_batch",
                "minute_batch",
                "date_chunk_months",
                "dump_workers",
            )
        },
    )
    # Source parsing/hash proof has dedicated real-CAS reader tests. This
    # fixture exercises real production index materialization, not a fake PASS
    # runner. Any unexpected Qlib/provider/consumer invocation fails the test.
    source = ArtifactReadyPreparationBuildSource.__new__(ArtifactReadyPreparationBuildSource)
    source.profile = profile
    source.cutoff = cutoff
    source.pit_snapshot = SimpleNamespace(
        cutoff=cutoff,
        spans_sha256="e" * 64,
        spans=(SimpleNamespace(ts_code="000001.SZ", eligible_start=cutoff, eligible_end=cutoff),),
    )
    source.contract = {"operation_id": op}
    source.contract_ref = cas.put_json({"private_test_graph": True})
    source.component_manifests = {
        Component.DOMESTIC_INDEX_CONTEXT: {
            "component_effective_content_root": "f" * 64,
            "effective_partitions": [{"schema_digest": "1" * 64}],
        }
    }
    source.trading_days = lambda: (cutoff,)
    source.index_rows = lambda: iter(
        {
            "ts_code": item.daily_code,
            "trade_date": cutoff,
            "open": 9.0,
            "high": 11.0,
            "low": 8.0,
            "close": 10.0,
            "pre_close": 9.5,
            "pct_chg": 5.0,
            "vol": 123.0,
            "amount": 456.0,
        }
        for item in DOMESTIC_INDEX_DEFINITIONS
    )
    calls = []

    class Forbidden:
        def execute(self, **kwargs):
            raise AssertionError("unexpected Qlib writer or release consumer")

    @contextmanager
    def scope(context):
        assert context.stage == "BUILD"
        calls.append(context.attempt)
        yield SimpleNamespace(qlib_writer=Forbidden(), consumer_smoke=Forbidden())

    executor = MonthlyPrivatePhysicalPreparationExecutor(profile, cas, Path(__file__).resolve().parents[3], scope)
    context = ProducerContext(
        "SOURCE",
        op,
        1,
        {},
        {
            "target_cutoff": cutoff.isoformat(),
            "release_id": "qe_hmm_full_v2_20260930",
            "predecessor": {"profile_sha256": "2" * 64},
        },
        {},
    )
    return executor, context, source, catalog, calls


def execute(physical, *, context=None, checkpoint=lambda: None):
    executor, first, source, _, _ = physical
    return executor.execute(
        context=context or first, source=source, source_snapshot_id="postgres:1-ABC-1", checkpoint=checkpoint
    )


def test_real_index_is_sealed_and_next_attempt_does_not_recompute(physical):
    first = execute(physical)
    executor, context, source, catalog, calls = physical
    assert first["prepared_component_count"] == 1
    assert first["publication_allowed"] is False
    assert first["consistent_input_set_complete"] is False
    record = next(iter(first["prepared_components"].values()))
    component = catalog / record["component_root"]
    frame = pd.read_hdf(component / "index_daily.h5", "data")
    assert len(frame) == 12
    assert frame["idx_volume_share_equiv"].eq(12300).all()
    assert frame["idx_amount_cny"].eq(456000).all()
    receipt_before = (component / "prepared-component.json").read_bytes()
    # Unrelated global source provenance changes are irrelevant to index's
    # validated effective root. They must not cause a second computation.
    source.source_content_root = "9" * 64
    second = execute(physical, context=replace(context, attempt=2))
    assert second == first
    assert calls == [1]
    assert (component / "prepared-component.json").read_bytes() == receipt_before
    assert not (catalog / ".staging" / f"{context.operation_id}-preparation-attempt-2").exists()
    assert not list(catalog.rglob("qe_dataset_manifest.json"))


def test_related_effective_input_drift_rebuilds_only_new_private_output(physical):
    first = execute(physical)
    _, context, source, catalog, calls = physical
    source.component_manifests[Component.DOMESTIC_INDEX_CONTEXT]["component_effective_content_root"] = "3" * 64
    second = execute(physical, context=replace(context, attempt=2))
    before = next(iter(first["prepared_components"].values()))
    after = next(iter(second["prepared_components"].values()))
    assert before["identity_digest"] != after["identity_digest"]
    assert before["component_root"] != after["component_root"]
    assert (catalog / before["component_root"] / "prepared-component.json").is_file()
    assert calls == [1, 2]


def test_pinned_output_tamper_fails_before_writer(physical):
    first = execute(physical)
    _, context, _, catalog, calls = physical
    record = next(iter(first["prepared_components"].values()))
    path = catalog / record["component_root"] / "index_daily.h5"
    with path.open("ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(ComponentPreparationError, match="bytes differ"):
        execute(physical, context=replace(context, attempt=2))
    assert calls == [1]


@pytest.mark.parametrize("change", ["operation", "cutoff", "stage", "attempt", "snapshot"])
def test_bad_binding_does_not_create_output(physical, change):
    executor, context, source, catalog, calls = physical
    if change == "operation":
        context = replace(context, operation_id="dmr_" + "2" * 32)
    elif change == "cutoff":
        context = replace(context, plan={**context.plan, "target_cutoff": "2026-08-31"})
    elif change == "stage":
        context = replace(context, stage="BUILD")
    elif change == "attempt":
        context = replace(context, attempt=True)
    with pytest.raises(ComponentPreparationError, match="binding differs"):
        executor.execute(
            context=context, source=source, source_snapshot_id="" if change == "snapshot" else "postgres:test"
        )
    assert calls == []
    assert list(catalog.iterdir()) == []


def test_record_cannot_repoint_to_another_staging_directory(physical):
    execute(physical)
    _, context, _, catalog, calls = physical
    record_path = catalog / ".staging" / "preparation-records" / context.operation_id / "attempt-1-index.json"
    import json

    value = json.loads(record_path.read_bytes())
    value["component_root"] = ".staging/other/index_context"
    body = {key: item for key, item in value.items() if key != "canonical_digest"}
    value["canonical_digest"] = digest_named_fields(value["schema_version"], body)
    record_path.write_bytes(canonical_json_bytes(value) + b"\n")
    with pytest.raises(ComponentPreparationError, match="root binding differs"):
        execute(physical, context=replace(context, attempt=2))
    assert calls == [1]


def test_cancel_does_not_seal_a_component(physical):
    _, _, _, catalog, calls = physical

    def cancel():
        raise RuntimeError("cancelled")

    with pytest.raises(RuntimeError, match="cancelled"):
        execute(physical, checkpoint=cancel)
    assert not list(catalog.rglob("prepared-component.json"))
    assert calls == []


def full_adoption_fixture(physical):
    executor, context, private, catalog, _ = physical
    source = ArtifactReadyBuildSource.__new__(ArtifactReadyBuildSource)
    source.__dict__.update(private.__dict__)
    source.contract = {"artifact_ready_content_root": "4" * 64}
    source.source_content_root = "5" * 64
    source.qfq_authority = SimpleNamespace(digest="6" * 64)
    source.component_manifests = {
        component: {
            "component_effective_content_root": "f" * 64,
            "effective_partitions": [{"schema_digest": "1" * 64}],
        }
        for component in Component
    }
    staging = catalog / ".staging" / "full-build"
    staging.mkdir()
    invocation = BuildStageInvocation(
        "prepare",
        context.operation_id,
        "full",
        2,
        0,
        3600,
        context.plan["release_id"],
        "7" * 64,
        ".staging/full-build",
        executor.project_root,
        catalog,
        staging,
        executor.profile,
        executor.cas,
        {
            "preparation_predecessor_profile_sha256": context.plan["predecessor"]["profile_sha256"],
            "build_inputs": {"cutoff": private.cutoff.isoformat()},
            "actions": [
                {"component": component.value, "action": ComponentAction.FULL_REBUILD.value} for component in Component
            ],
        },
        {},
    )
    return invocation, source


def test_full_input_adopts_only_pinned_files_as_private_copies(physical):
    prepared = execute(physical)
    invocation, source = full_adoption_fixture(physical)
    adopted = adopt_prepared_physical_components(invocation, source=source)
    assert set(adopted) == {Component.DOMESTIC_INDEX_CONTEXT}
    assert adopted[Component.DOMESTIC_INDEX_CONTEXT]["full_candidate_validation_required"] is True
    assert adopted[Component.DOMESTIC_INDEX_CONTEXT]["publication_allowed"] is False
    old = invocation.candidate_root / next(iter(prepared["prepared_components"].values()))["component_root"]
    new = invocation.staging_root / "index_context"
    assert (old / "index_daily.h5").read_bytes() == (new / "index_daily.h5").read_bytes()
    assert (old / "index_daily.h5").stat().st_ino != (new / "index_daily.h5").stat().st_ino
    assert not (new / "private-domain-validation.json").exists()
    assert not (new / "prepared-component.json").exists()
    assert not list(invocation.staging_root.rglob("qe_dataset_manifest.json"))


def test_private_graph_cannot_adopt_into_final_build(physical):
    execute(physical)
    invocation, _ = full_adoption_fixture(physical)
    with pytest.raises(ComponentPreparationError, match="adoption binding differs"):
        adopt_prepared_physical_components(invocation, source=physical[2])
    assert list(invocation.staging_root.iterdir()) == []


def test_changed_final_effective_input_uses_normal_build_not_old_preparation(physical):
    execute(physical)
    invocation, source = full_adoption_fixture(physical)
    source.component_manifests[Component.DOMESTIC_INDEX_CONTEXT]["component_effective_content_root"] = "8" * 64
    assert adopt_prepared_physical_components(invocation, source=source) == {}
    assert list(invocation.staging_root.iterdir()) == []


def test_non_full_action_keeps_its_existing_lineage_plan(physical):
    execute(physical)
    invocation, source = full_adoption_fixture(physical)
    for item in invocation.plan["actions"]:
        item["action"] = ComponentAction.INCREMENTAL.value
    assert adopt_prepared_physical_components(invocation, source=source) == {}
    assert list(invocation.staging_root.iterdir()) == []


def test_completed_index_survives_later_stage_failure_and_is_not_recomputed(physical, monkeypatch):
    import backend.services.dataset_release.monthly_preparation_executor as module

    actual = module.run_preparation_build_stage

    def later_failure(invocation, **kwargs):
        if invocation.stage == "finalize-bins":
            raise RuntimeError("later stage failed")
        return actual(invocation, **kwargs)

    monkeypatch.setattr(module, "run_preparation_build_stage", later_failure)
    with pytest.raises(RuntimeError, match="later stage failed"):
        execute(physical)
    _, context, _, catalog, calls = physical
    completed = catalog / ".staging" / "preparation-records" / context.operation_id / "attempt-1-index.json"
    assert completed.is_file()
    result = execute(physical, context=replace(context, attempt=2))
    assert result["prepared_component_count"] == 1
    assert calls == [1]
    assert not list(catalog.rglob("qe_dataset_manifest.json"))


def test_private_index_csv_dependency_reuses_the_real_completed_index(physical):
    from backend.services.dataset_release.monthly_preparation_executor import reuse_private_index_dependency

    prepared = execute(physical)
    invocation, _ = full_adoption_fixture(physical)
    private = physical[2]
    invocation = replace(
        invocation,
        plan={
            **invocation.plan,
            "target_cutoff": private.cutoff.isoformat(),
        },
    )
    proof = reuse_private_index_dependency(invocation, source=private)
    assert proof["publication_allowed"] is False
    old = invocation.candidate_root / next(iter(prepared["prepared_components"].values()))["component_root"]
    new = invocation.staging_root / "index_context"
    assert (old / "index_csv" / "000300.SH.csv").read_bytes() == (new / "index_csv" / "000300.SH.csv").read_bytes()
    assert not (new / "prepared-component.json").exists()


@pytest.mark.skipif(os.getenv("AISTOCK_MONTHLY_REAL_QLIB_TEST") != "1", reason="requires existing WSL Qlib toolchain")
def test_real_wsl_private_daily_minute_dump_finalize_and_recovery(physical, monkeypatch):
    """Actual official dump processes, never a fabricated binary writer.

    SQL/CAS parsing has separate boundary tests. This deliberately supplies
    one-day raw facts at that unit boundary while running production stock
    transforms, CSV preparation, guarded WSL dump, finalizer and recovery.
    """
    from datetime import datetime
    from pathlib import PureWindowsPath
    import numpy as np
    from backend.services.dataset_release import build_stage as stages
    from backend.services.dataset_release.canonical_stock_transformer import (
        MINUTE_SESSION_TIMES, build_qfq_denominator_authority,
    )
    from backend.services.dataset_release.monthly_supervised_scope import ResourceSupervisedMonthlyBuildScopeFactory
    from backend.services.dataset_release.pit import freeze_pit_snapshot
    from backend.services.dataset_release.profile import load_dataset_profile
    from backend.services.dataset_release.monthly_production import _supervisor_factory

    old, context, source, catalog, _calls = physical
    profile = load_dataset_profile(old.project_root / "configs/datasets/qe_backtest_monthly_v2.yaml")
    profile = replace(profile, candidate_root=PureWindowsPath(str(catalog)), start_date=source.cutoff,
                      minute_start_date=source.cutoff)
    source.profile = profile
    source.pit_snapshot = freeze_pit_snapshot([
        {"ts_code": "000001.SZ", "eligible_start": source.cutoff, "eligible_end": source.cutoff,
         "entry_reason": None, "exit_reason": None},
    ], universe_key=profile.universe_key, rule_version=profile.universe_rule_version,
       scope_start=source.cutoff, cutoff=source.cutoff, state_identity="explicit-unit-test",
       source_fingerprint_sha256="a" * 64, parameter_hash="b" * 64)
    adj = [{"ts_code": "000001.SZ", "trade_date": source.cutoff, "adj_factor": 2.0}]
    source.qfq_authority = build_qfq_denominator_authority(adj, pit_snapshot=source.pit_snapshot, cutoff=source.cutoff)
    for component in (Component.DAILY_BIN, Component.MINUTE_BIN):
        source.component_manifests[component] = {
            "component_effective_content_root": "f" * 64, "effective_partitions": [{"schema_digest": "1" * 64}],
        }
    source.trading_days = lambda: (source.cutoff,)
    raw = {"ts_code": "000001.SZ", "open_li": 9500.0, "high_li": 10000.0, "low_li": 9000.0,
           "close_li": 10000.0, "volume_hand": 100.0, "amount_li": 123000.0}
    facts = {
        "kline_daily_raw": [{**raw, "trade_date": source.cutoff}],
        "adj_factor": adj,
        "stk_limit": [{"ts_code": "000001.SZ", "trade_date": source.cutoff, "pre_close": 9.5, "up_limit": 11.0, "down_limit": 9.0}],
        "suspend_d": [],
        "kline_minute_raw": [{**raw, "trade_time": datetime.combine(source.cutoff, label).isoformat(sep=" ") + "+08:00",
                              "freq": "1m"} for label in MINUTE_SESSION_TIMES],
    }
    monkeypatch.setattr(stages, "_merged_rows", lambda _source, _component, dataset, **_kwargs: iter(facts[dataset]))
    toolchain = profile.qlib_toolchain.build_verified(old.project_root)
    scopes = []
    production_supervisor = _supervisor_factory(profile=profile, artifact_root=old.cas.root)

    def supervisor(ctx):
        scopes.append(ctx.attempt)
        return production_supervisor(ctx)

    scope = ResourceSupervisedMonthlyBuildScopeFactory(profile=profile, project_root=old.project_root,
                                                       toolchain=toolchain, supervisor_factory=supervisor)
    executor = MonthlyPrivatePhysicalPreparationExecutor(profile, old.cas, old.project_root, scope)
    first = executor.execute(context=context, source=source, source_snapshot_id="postgres:unit-test")
    assert first["prepared_component_count"] == 3
    for component, freq, expected in ((Component.DAILY_BIN, "day", 1), (Component.MINUTE_BIN, "1min", 240)):
        record = first["prepared_components"][component.value]
        directory = catalog / record["component_root"] / "qlib"
        close = np.fromfile(directory / "features/000001.sz" / f"close.{freq}.bin", dtype="<f4")
        assert len(close) == expected + 1 and np.isfinite(close[1:]).all() and np.allclose(close[1:], 10.0)
        assert (catalog / record["component_root"] / "prepared-component.json").is_file()
    second = executor.execute(context=replace(context, attempt=2), source=source, source_snapshot_id="postgres:second")
    assert second == first and scopes == [1]
    assert not list(catalog.rglob("qe_dataset_manifest.json"))
