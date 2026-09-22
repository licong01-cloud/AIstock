from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.monthly_build_bridge import CompiledMonthlyBuild
from backend.services.dataset_release.monthly_candidate_finalizer import (
    MonthlyCandidateFinalizerError,
    SharedReleaseComponents,
    UnifiedMonthlyCandidateFinalizer,
)
from backend.services.dataset_release.factor_materializer import FACTOR_H5_DATASETS
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.monthly_consumer_layout import publish_consumer_layout
from backend.services.dataset_release.monthly_official_adapters import (
    BuildExecution,
    validate_build_execution,
)
from backend.services.dataset_release.monthly_worker import ProducerContext


def _context(root: Path) -> ProducerContext:
    return ProducerContext(
        stage="BUILD",
        operation_id="dmr_" + "1" * 32,
        attempt=2,
        request={},
        plan={
            "release_id": "qe_hmm_full_v2_20260930",
            "revision": "20261001-monthly-v2",
            "target_cutoff": "2026-09-30",
            "candidate_root": str(root / "final"),
        },
        prior_receipts={},
    )


def _compiled(tmp_path: Path, ref) -> CompiledMonthlyBuild:  # type: ignore[no-untyped-def]
    bundle = tmp_path / "bundle.json"
    bundle.write_text("{}\n", encoding="utf-8")
    return CompiledMonthlyBuild(
        source_bundle_path=bundle,
        source_bundle_sha256=hashlib.sha256(bundle.read_bytes()).hexdigest(),
        source_stage_receipt_ref=ref,
        monthly_actions={},
        physical_plan={
            "release_digest": "1" * 64,
            "build_inputs": {"profile": "qe_hmm_full_v2", "scope": "full"},
        },
    )


class _Shared:
    def __init__(self, *, fabricate: bool = False, copy_alias: bool = False) -> None:
        self.fabricate = fabricate
        self.copy_alias = copy_alias

    def execute(self, *, context, staging_root, compiled, validation_result):  # type: ignore[no-untyped-def]
        del context, compiled, validation_result
        calendar = staging_root / "daily_bin" / "qlib" / "calendars" / "day.txt"
        instruments = staging_root / "daily_bin" / "qlib" / "instruments" / "all.txt"
        pool_root = staging_root / "stock_pools"
        suspend = staging_root / "components" / "suspend" / "suspend.parquet"
        sector = staging_root / "components" / "sector_context" / "membership.parquet"
        benchmark = staging_root / "benchmark" / "instruments.txt"
        for path, value in (
            (calendar, "2018-08-01\n2026-09-30\n"),
            (instruments, "SZ000001\t2018-08-01\t2026-09-30\n"),
            (suspend, "suspend"),
            (sector, "sector"),
            (benchmark, "SH000300\n"),
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(value, encoding="utf-8")
        (instruments.parent / "index.txt").write_text(
            "".join(
                f"{item.daily_code}\t{item.required_from.isoformat()}\t2026-09-30\n"
                for item in DOMESTIC_INDEX_DEFINITIONS
            ),
            encoding="utf-8",
        )
        minute = staging_root / "minute_bin" / "qlib"
        (minute / "calendars").mkdir(parents=True)
        (minute / "instruments").mkdir()
        (minute / "calendars" / "1min.txt").write_text(
            "2018-08-01\n2026-09-30\n", encoding="utf-8"
        )
        (minute / "instruments" / "all.txt").write_text(
            "SZ000001\t2018-08-01\t2026-09-30\n", encoding="utf-8"
        )
        factor = staging_root / "factor_bundle"
        for dataset in FACTOR_H5_DATASETS:
            (factor / f"{dataset}.h5").write_bytes(dataset.encode("ascii"))
        (factor / "static_factors.parquet").write_bytes(b"static")
        index = staging_root / "index_context"
        index.mkdir()
        (index / "index_daily.h5").write_bytes(b"index")
        pool_paths: dict[str, Path] = {}
        for name in ("stock_universe", "csi300", "csi500", "csi1000", "star50", "star100"):
            filename = "stock_universe.txt" if name == "stock_universe" else f"index_pool__{name}.txt"
            path = pool_root / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("SH000001\t2026-09-30\t2026-09-30\n", encoding="utf-8")
            pool_paths[name] = path

        def pin(path: Path) -> dict[str, object]:
            return {
                "path": path.relative_to(staging_root).as_posix(),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size": path.stat().st_size,
            }

        st_pit = {
            "schema_version": "qe_st_pit_manifest_v1",
            "snapshot_id": "pit-20260930",
            "cutoff_trade_date": "2026-09-30",
            "universe_key": "aistock_equity_pit_canonical_v2",
            "rule_version": "shsz_a_252td_st_delist_asof_v2",
            "selection_universe": pin(pool_paths["stock_universe"]),
            "index_membership_sidecars": {
                name: pin(path) for name, path in pool_paths.items()
            },
        }
        layout = publish_consumer_layout(
            root=staging_root,
            profile=SimpleNamespace(
                start_date=date(2018, 8, 1),
                universe_key="aistock_equity_pit_canonical_v2",
            ),  # type: ignore[arg-type]
            cutoff=date(2026, 9, 30),
            release_id="qe_hmm_full_v2_20260930",
            st_pit_manifest=st_pit,
            validation_authority={
                "validation_ref": {
                    "sha256": "a" * 64,
                    "size": 1,
                    "relative_path": f"cas/sha256/aa/{'a' * 64}",
                },
                "component_artifact_manifest_ref": {
                    "sha256": "b" * 64,
                    "size": 1,
                    "relative_path": f"cas/sha256/bb/{'b' * 64}",
                },
            },
        )
        if self.copy_alias:
            alias = (
                staging_root
                / "components"
                / "daily_bin_candidate"
                / "calendars"
                / "day.txt"
            )
            payload = alias.read_bytes()
            alias.unlink()
            alias.write_bytes(payload)
        return SharedReleaseComponents(
            required_files=tuple({
                calendar,
                instruments,
                *pool_paths.values(),
                suspend,
                sector,
                benchmark,
                *layout.required_files,
            }),
            qlib_calendar_path=layout.day_calendar_path,
            qlib_instruments_path=layout.day_instruments_path,
            st_pit_manifest=st_pit,
            source_contract={"no_fabrication": not self.fabricate, "database_fallback": False},
            source_rows_read=12,
            computed_rows=6,
        )


def _setup(tmp_path: Path):  # type: ignore[no-untyped-def]
    staging = tmp_path / ".staging" / "candidate"
    staging.mkdir(parents=True)
    physical = staging / "factor_bundle" / "factor.h5"
    physical.parent.mkdir()
    physical.write_bytes(b"factor")
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    stage_refs = {
        name: cas.put_json({"stage": name})
        for name in (
            "prepare",
            "finalize_bins",
            "consumer_smoke",
            "consumer_smoke_resource",
            "validate",
        )
    }
    source_ref = cas.put_json({"stage": "SOURCE"})
    validation = {
        "validation_status": "PASS",
        "required_validation_failures": 0,
        "artifact_root": "3" * 64,
        "manifest_root": "4" * 64,
        "validation_ref": cas.put_json({"status": "PASS"}).as_dict(),
        "component_artifact_manifest_ref": cas.put_json({"components": []}).as_dict(),
    }
    return staging, cas, stage_refs, source_ref, validation


def test_finalizer_seals_all_release_files_and_manifest_identity(tmp_path: Path) -> None:
    staging, _cas, stage_refs, source_ref, validation = _setup(tmp_path)
    result = UnifiedMonthlyCandidateFinalizer(_Shared()).execute(
        context=_context(tmp_path),
        staging_root=staging,
        compiled=_compiled(tmp_path, source_ref),
        validation_result=validation,
        stage_refs=stage_refs,
    )

    raw = result.manifest_path.read_bytes()
    manifest = json.loads(raw)
    unsigned = dict(manifest)
    unsigned.pop("dataset_manifest_sha256")
    assert raw == canonical_json_bytes(manifest) + b"\n"
    assert manifest["dataset_manifest_sha256"] == hashlib.sha256(
        canonical_json_bytes(unsigned)
    ).hexdigest()
    assert manifest["cutoff_trade_date"] == "2026-09-30"
    assert manifest["st_pit_snapshot_id"] == "pit-20260930"
    baseline = json.loads(
        (staging / "reports" / "monthly_incremental_baseline_authority.json").read_text(
            encoding="utf-8"
        )
    )
    assert baseline["schema_version"] == "aistock_monthly_incremental_baseline_authority_v1"
    assert baseline["release_digest"] == "1" * 64
    assert len(manifest["components"]) == len(result.component_artifacts)
    assert all(path.is_file() for path in result.component_artifacts)
    assert result.database_read_performed is False
    assert result.database_write_performed is False
    assert result.workload.source_rows_read == 12
    validate_build_execution(
        BuildExecution(
            manifest_path=result.manifest_path,
            component_artifacts=result.component_artifacts,
        ),
        context=_context(tmp_path),
    )


def test_finalizer_rejects_missing_supervised_resource_evidence(tmp_path: Path) -> None:
    staging, _cas, stage_refs, source_ref, validation = _setup(tmp_path)
    stage_refs.pop("consumer_smoke_resource")

    with pytest.raises(MonthlyCandidateFinalizerError, match="stage evidence"):
        UnifiedMonthlyCandidateFinalizer(_Shared()).execute(
            context=_context(tmp_path),
            staging_root=staging,
            compiled=_compiled(tmp_path, source_ref),
            validation_result=validation,
            stage_refs=stage_refs,
        )


def test_finalizer_rejects_fabricated_shared_component_contract(tmp_path: Path) -> None:
    staging, _cas, stage_refs, source_ref, validation = _setup(tmp_path)

    with pytest.raises(MonthlyCandidateFinalizerError, match="fabrication"):
        UnifiedMonthlyCandidateFinalizer(_Shared(fabricate=True)).execute(
            context=_context(tmp_path),
            staging_root=staging,
            compiled=_compiled(tmp_path, source_ref),
            validation_result=validation,
            stage_refs=stage_refs,
        )


def test_finalizer_rejects_copied_consumer_alias(tmp_path: Path) -> None:
    staging, _cas, stage_refs, source_ref, validation = _setup(tmp_path)

    with pytest.raises(MonthlyCandidateFinalizerError, match="not a hardlink"):
        UnifiedMonthlyCandidateFinalizer(_Shared(copy_alias=True)).execute(
            context=_context(tmp_path),
            staging_root=staging,
            compiled=_compiled(tmp_path, source_ref),
            validation_result=validation,
            stage_refs=stage_refs,
        )


def test_finalizer_is_create_exclusive(tmp_path: Path) -> None:
    staging, _cas, stage_refs, source_ref, validation = _setup(tmp_path)
    finalizer = UnifiedMonthlyCandidateFinalizer(_Shared())
    finalizer.execute(
        context=_context(tmp_path),
        staging_root=staging,
        compiled=_compiled(tmp_path, source_ref),
        validation_result=validation,
        stage_refs=stage_refs,
    )

    with pytest.raises(MonthlyCandidateFinalizerError, match="already exists"):
        finalizer.execute(
            context=_context(tmp_path),
            staging_root=staging,
            compiled=_compiled(tmp_path, source_ref),
            validation_result=validation,
            stage_refs=stage_refs,
        )
