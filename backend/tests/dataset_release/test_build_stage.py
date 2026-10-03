from __future__ import annotations

from types import SimpleNamespace
from datetime import date
from dataclasses import replace

import pytest

from backend.services.dataset_release.build_stage import (
    CandidateBuildStageError,
    _actions,
    run_build_stage,
)
from backend.services.dataset_release.contracts import Component, ComponentAction


def _complete_actions() -> list[dict[str, str]]:
    return [{"component": component.value, "action": ComponentAction.REUSE.value} for component in Component]


def test_action_plan_requires_one_action_for_every_component() -> None:
    assert _actions({"actions": _complete_actions()}) == {component: ComponentAction.REUSE for component in Component}

    with pytest.raises(CandidateBuildStageError, match="incomplete"):
        _actions({"actions": _complete_actions()[:-1]})

    with pytest.raises(CandidateBuildStageError, match="duplicated"):
        _actions({"actions": [*_complete_actions(), _complete_actions()[0]]})


def test_run_build_stage_rejects_unknown_stage_before_any_write() -> None:
    with pytest.raises(CandidateBuildStageError, match="unsupported build stage"):
        run_build_stage(SimpleNamespace(stage="publish"))


def test_legacy_full_prepare_does_not_import_unenabled_preparation_helpers(tmp_path, monkeypatch):
    import builtins
    from backend.services.dataset_release import build_stage

    original_import = builtins.__import__
    def reject_preparation_import(name, *args, **kwargs):
        if "monthly_preparation" in name:
            pytest.fail("legacy full BUILD must not import unenabled preparation helpers")
        return original_import(name, *args, **kwargs)

    class ReachedNormalToolchain(Exception):
        pass

    def stop_at_toolchain(root):
        raise ReachedNormalToolchain

    monkeypatch.setattr(builtins, "__import__", reject_preparation_import)
    monkeypatch.setattr(build_stage, "_build_source", lambda invocation: (object(), object()))
    invocation = SimpleNamespace(
        plan={"actions": _complete_actions()}, staging_root=tmp_path / "full",
        profile=SimpleNamespace(qlib_toolchain=SimpleNamespace(build_verified=stop_at_toolchain)),
        project_root=tmp_path,
    )
    with pytest.raises(ReachedNormalToolchain):
        build_stage._prepare(invocation, ledger=object(), checkpoint=lambda: None)


def test_private_index_materializes_real_h5_without_factor_or_bins(tmp_path):
    import pandas as pd
    from backend.services.dataset_release.artifact_ready_build_source import ArtifactReadyPreparationBuildSource
    from backend.services.dataset_release.build_stage import (
        BuildStageInvocation,
        PRIVATE_STAGE_SCHEMA,
        run_preparation_build_stage,
    )
    from backend.services.dataset_release.canonical import digest_named_fields
    from backend.services.dataset_release.cas_store import CASStore
    from backend.services.dataset_release.control_store import ControlStore
    from backend.services.dataset_release.decision import DECISION_SCHEMA_VERSION
    from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS

    control = tmp_path / "control"
    ControlStore.initialize(control)
    cas = CASStore(control)
    catalog = tmp_path / "candidates"
    (catalog / ".staging").mkdir(parents=True)
    cutoff = date(2026, 9, 30)
    operation = f"dmr_{'1' * 32}"
    profile = SimpleNamespace(
        profile="qe_hmm_full_v2",
        indices=DOMESTIC_INDEX_DEFINITIONS,
        stage_timeouts_seconds={"full_build": 3600},
        qlib_toolchain=SimpleNamespace(build_verified=lambda _: None),
        resource_policy_digest="a" * 64,
        pressure_ladder={
            key: (100,)
            for key in (
                "row_group_rows",
                "h5_batch",
                "minute_batch",
                "date_chunk_months",
                "dump_workers",
            )
        },
    )
    source = ArtifactReadyPreparationBuildSource.__new__(ArtifactReadyPreparationBuildSource)
    source.profile = profile
    source.cutoff = cutoff
    source.pit_snapshot = SimpleNamespace(
        cutoff=cutoff,
        spans_sha256="a" * 64,
        spans=(SimpleNamespace(ts_code="000001.SZ", eligible_start=cutoff, eligible_end=cutoff),),
    )
    source.contract = {"operation_id": operation}
    source.contract_ref = cas.put_json({"fixture_private_artifacts": True})
    source.component_manifests = {Component.DOMESTIC_INDEX_CONTEXT: {}}
    source.trading_days = lambda: (cutoff,)
    source.index_rows = lambda: iter(
        {
            "ts_code": definition.daily_code,
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
        for definition in DOMESTIC_INDEX_DEFINITIONS
    )
    actions = [{"component": component.value, "action": ComponentAction.FULL_REBUILD.value} for component in Component]
    plan = {
        "target_cutoff": cutoff.isoformat(),
        "actions": actions,
        "action_plan_digest": digest_named_fields(
            DECISION_SCHEMA_VERSION, {"actions": sorted(actions, key=lambda row: row["component"])}
        ),
        "build_inputs": {},
    }
    invocation = BuildStageInvocation(
        "prepare",
        operation,
        f"{operation}-attempt-1",
        1,
        0,
        3600,
        "qe_hmm_full_v2_20260930",
        "b" * 64,
        ".staging/private",
        tmp_path,
        catalog,
        catalog / ".staging" / "private",
        profile,
        cas,
        plan,
        {},
    )
    result = run_preparation_build_stage(invocation, source=source)
    assert result["schema_version"] == PRIVATE_STAGE_SCHEMA
    assert result["publication_allowed"] is False
    assert result["status"] == "MATERIALIZED_UNPUBLISHED"
    assert result["components"] == [Component.DOMESTIC_INDEX_CONTEXT.value]
    frame = pd.read_hdf(invocation.staging_root / "index_context" / "index_daily.h5", "data")
    assert len(frame) == 12
    assert set(frame.index.get_level_values("instrument")) == {
        definition.daily_code for definition in DOMESTIC_INDEX_DEFINITIONS
    }
    assert frame["idx_volume_share_equiv"].eq(12300).all()
    assert frame["idx_amount_cny"].eq(456000).all()
    assert not (invocation.staging_root / "daily_bin").exists()
    assert not (invocation.staging_root / "minute_bin").exists()
    assert not (invocation.staging_root / "factor_bundle").exists()
    final = run_preparation_build_stage(
        replace(invocation, stage="finalize-bins", prerequisites={"prepare": cas.put_json(result).sha256}),
        source=source,
    )
    assert final["publication_allowed"] is False
    with pytest.raises(CandidateBuildStageError, match="already exists"):
        run_preparation_build_stage(invocation, source=source)
    with pytest.raises(CandidateBuildStageError, match="binding differs"):
        run_preparation_build_stage(replace(invocation, stage="validate"), source=source)
