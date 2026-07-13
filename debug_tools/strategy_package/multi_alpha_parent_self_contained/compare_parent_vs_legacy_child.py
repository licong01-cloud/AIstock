#!/usr/bin/env python
"""Compare parent self-contained MULTI_ALPHA runtime against a legacy child oracle.

This debug tool is read-only and intentionally lives under debug_tools so the
production provider never imports the legacy child-based oracle path.
"""

# ruff: noqa: E402 - debug tool bootstraps repo root before backend imports.

from __future__ import annotations

import argparse
import json
import sys
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from backend.db.pg_pool import get_conn
from backend.services.strategy_package.live_inference import QEExperimentRuntimeAssetResolver
from backend.services.strategy_package.models import RuntimeAssetManifest
from backend.services.strategy_package.multi_alpha_live import (
    MultiAlphaLivePredictionProvider,
    MultiAlphaWeightService,
)
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.selection_artifact import InMemorySelectionScoreArtifactRepository
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError

TOLERANCE = 1e-12
DEFAULT_ENV_FILE = Path(r"F:\Dev\AIstock\.env")


@contextmanager
def _read_only_conn() -> Iterator[Any]:
    """Production DB guard: parity oracle must not write package state."""

    with get_conn(autocommit=True) as conn:
        conn.set_session(readonly=True, autocommit=True)
        yield conn


@dataclass(frozen=True)
class CompareResult:
    package_id: str
    trade_date: str
    tolerance: float
    row_count: int
    topk: int
    max_abs_combined_score_diff: float
    max_abs_leg_normalized_diff: dict[str, float]
    weights: dict[str, float]
    parent_runtime_source: str
    parent_model_params_origin: str
    scratch_parent_runtime_view: dict[str, Any]


@dataclass(frozen=True)
class ScratchParentRuntimeView:
    manifest: Any
    metadata: dict[str, Any]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-id", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--backend", default="wsl", choices=["wsl", "local"])
    parser.add_argument("--top-k", type=int, default=None)
    parser.add_argument("--read-only", action="store_true", required=True)
    parser.add_argument("--tolerance", type=float, default=TOLERANCE)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.env_file:
        load_dotenv(args.env_file)
    try:
        trade_date = date.fromisoformat(args.trade_date)
    except ValueError as exc:
        raise SystemExit(f"invalid --trade-date {args.trade_date!r}: {exc}") from exc

    result = compare_parent_vs_legacy_child(
        package_id=args.package_id,
        trade_date=trade_date,
        backend=args.backend,
        top_k=args.top_k,
        tolerance=args.tolerance,
    )
    payload = {
        "ok": True,
        "schema_version": "multi_alpha_parent_vs_legacy_child_compare_v1",
        **result.__dict__,
    }
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


def compare_parent_vs_legacy_child(
    *,
    package_id: str,
    trade_date: date,
    backend: str,
    top_k: int | None,
    tolerance: float,
) -> CompareResult:
    repository = StrategyPackageRepository(conn_factory=_read_only_conn)
    parent_record = repository.get(package_id)
    parent_manifest = parent_record.current_manifest()
    scratch_view = _scratch_parent_runtime_view_from_legacy_children(
        repository=repository,
        parent_manifest=parent_manifest,
        package_id=package_id,
    )
    runtime_topk = top_k or _manifest_topk(parent_manifest)
    runtime_config = {
        "runtime_profile": {"selection": {"top_k": runtime_topk}},
        "selection_artifact_config": {
            "multi_alpha_live_inference_enabled": True,
            "component_coverage_threshold": runtime_topk,
            "inference_backend": backend,
        },
    }

    parent_artifact = _run_parent_path(
        repository=_SingleRecordRepository(package_id=package_id, manifest=scratch_view.manifest),
        package_id=package_id,
        trade_date=trade_date,
        runtime_config=runtime_config,
        backend=backend,
    )
    legacy_artifact = _run_legacy_child_oracle(
        repository=repository,
        parent_manifest=parent_manifest,
        package_id=package_id,
        trade_date=trade_date,
        runtime_config=runtime_config,
        backend=backend,
    )
    _assert_same_ranked_rows(parent_artifact.scores_json, legacy_artifact.scores_json, tolerance=tolerance)
    leg_diffs = _leg_normalized_diffs(parent_artifact.scores_json, legacy_artifact.scores_json, tolerance=tolerance)
    _assert_same_weights(parent_artifact.metadata, legacy_artifact.metadata, tolerance=tolerance)
    return CompareResult(
        package_id=package_id,
        trade_date=trade_date.isoformat(),
        tolerance=tolerance,
        row_count=len(parent_artifact.scores_json),
        topk=runtime_topk,
        max_abs_combined_score_diff=_max_score_diff(parent_artifact.scores_json, legacy_artifact.scores_json),
        max_abs_leg_normalized_diff=leg_diffs,
        weights={str(k): float(v) for k, v in parent_artifact.metadata.get("weights", {}).items()},
        parent_runtime_source=str(parent_artifact.metadata.get("runtime_source")),
        parent_model_params_origin=str(parent_artifact.metadata.get("model_params_origin")),
        scratch_parent_runtime_view=scratch_view.metadata,
    )


def _run_parent_path(
    *,
    repository: StrategyPackageRepository,
    package_id: str,
    trade_date: date,
    runtime_config: Mapping[str, Any],
    backend: str,
):
    from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactService

    service = StrategyPackageSelectionArtifactService(
        package_repository=repository,
        artifact_repository=InMemorySelectionScoreArtifactRepository(),
        runtime_asset_resolver=QEExperimentRuntimeAssetResolver(),
    )
    artifact = service.generate_from_live_inference(
        package_id=package_id,
        trade_date=trade_date,
        runtime_config=dict(runtime_config),
        include_reference_price=False,
    )
    if artifact.metadata.get("runtime_source") != "parent_package_asset":
        raise StrategyPackageValidationError(
            "parent path did not use parent package assets",
            context={
                "reason_code": "multi_alpha_parent_parity_parent_runtime_source_mismatch",
                "package_id": package_id,
                "runtime_source": artifact.metadata.get("runtime_source"),
                "backend": backend,
            },
        )
    return artifact


def _run_legacy_child_oracle(
    *,
    repository: StrategyPackageRepository,
    parent_manifest: Any,
    package_id: str,
    trade_date: date,
    runtime_config: Mapping[str, Any],
    backend: str,
):
    child_manifest = _legacy_parent_manifest_with_child_slices(repository=repository, parent_manifest=parent_manifest)
    oracle_repo = _SingleRecordRepository(package_id=package_id, manifest=child_manifest)
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    resolver = _LegacyChildRuntimeResolver(repository=repository)
    provider = _OracleProvider(backend=backend)
    return MultiAlphaLivePredictionProvider(
        package_repository=oracle_repo,
        artifact_repository=artifact_repo,
        runtime_asset_resolver=resolver,
        live_inference_provider=provider,
        weight_service=MultiAlphaWeightService(),
    ).generate_artifacts(
        package_id=package_id,
        trade_dates=[trade_date],
        data_source="DB_HISTORICAL",
        runtime_config=dict(runtime_config),
        include_reference_price=False,
        inference_backend=backend,
    )[0]


def _legacy_parent_manifest_with_child_slices(*, repository: StrategyPackageRepository, parent_manifest: Any):
    manifest = parent_manifest.model_copy(deep=True)
    evidence = deepcopy(manifest.source_evidence)
    legs = evidence.get("multi_alpha", {}).get("legs") or []
    updated_components = []
    updated_models = []
    updated_factors = []
    updated_legs = []
    child_runtime_assets: list[tuple[str, RuntimeAssetManifest]] = []
    for component in manifest.alpha_components:
        leg = next((item for item in legs if item.get("leg_id") == component.alpha_id), None)
        if not leg:
            raise DataUnavailableError(
                "legacy child oracle cannot find leg evidence",
                context={"reason_code": "multi_alpha_legacy_oracle_leg_missing", "leg_id": component.alpha_id},
            )
        child_package_id = str(leg.get("child_package_id") or "").strip()
        if not child_package_id:
            raise DataUnavailableError(
                "legacy child oracle requires child_package_id in legacy parent manifest",
                context={"reason_code": "multi_alpha_legacy_oracle_child_missing", "leg_id": component.alpha_id},
            )
        child = repository.get(child_package_id)
        child_manifest = child.current_manifest()
        child_runtime = _required_child_runtime_assets(
            child_manifest=child_manifest,
            child_package_id=child_package_id,
            leg_id=component.alpha_id,
        )
        child_runtime_assets.append((child_package_id, child_runtime))
        updated_leg = dict(leg)
        updated_leg["runtime_assets"] = child_runtime.model_dump(mode="json")
        updated_legs.append(updated_leg)
        child_component = child_manifest.alpha_components[0]
        updated_components.append(
            component.model_copy(
                update={
                    "model_id": child_component.model_id,
                    "lineage": component.lineage.model_copy(
                        update={"factor_artifact_refs": list(child_component.lineage.factor_artifact_refs or child_component.factor_ids)}
                    ),
                }
            )
        )
        child_models = child_manifest.model_asset if isinstance(child_manifest.model_asset, list) else [child_manifest.model_asset]
        updated_models.extend(child_models)
        updated_factors.extend(child_manifest.factor_set)
    evidence.setdefault("multi_alpha", {})["legs"] = updated_legs
    return manifest.model_copy(
        update={
            "alpha_components": updated_components,
            "model_asset": updated_models,
            "factor_set": updated_factors,
            "runtime_assets": _parent_runtime_assets_from_child_leg_assets(
                package_id=manifest.package_id,
                child_runtime_assets=child_runtime_assets,
            ),
            "source_evidence": evidence,
        }
    )


def _scratch_parent_runtime_view_from_legacy_children(
    *,
    repository: StrategyPackageRepository,
    parent_manifest: Any,
    package_id: str,
) -> ScratchParentRuntimeView:
    """Build an in-memory complete parent view for the legacy package.

    The existing production parent package predates parent-owned Alpha158 schema
    freeze. This view copies only runtime schema mappings from legacy child
    manifests into memory so the parent-self-contained production path can be
    compared honestly without mutating production DB rows or manifests.
    """

    manifest = parent_manifest.model_copy(deep=True)
    evidence = deepcopy(manifest.source_evidence)
    legs = evidence.get("multi_alpha", {}).get("legs") if isinstance(evidence, dict) else None
    if not isinstance(legs, list):
        raise DataUnavailableError(
            "scratch parent runtime view requires legacy child leg evidence",
            context={"reason_code": "multi_alpha_parent_parity_legacy_legs_missing", "package_id": package_id},
        )

    child_runtime_assets: list[tuple[str, RuntimeAssetManifest]] = []
    updated_legs: list[dict[str, Any]] = []
    for leg in legs:
        if not isinstance(leg, Mapping):
            raise DataUnavailableError(
                "scratch parent runtime view found invalid legacy leg evidence",
                context={"reason_code": "multi_alpha_parent_parity_legacy_leg_invalid", "package_id": package_id},
            )
        leg_id = str(leg.get("leg_id") or "").strip()
        child_package_id = str(leg.get("child_package_id") or "").strip()
        if not leg_id or not child_package_id:
            raise DataUnavailableError(
                "scratch parent runtime view requires leg_id and child_package_id",
                context={
                    "reason_code": "multi_alpha_parent_parity_legacy_child_missing",
                    "package_id": package_id,
                    "leg_id": leg_id or None,
                    "child_package_id": child_package_id or None,
                },
            )
        child = repository.get(child_package_id)
        child_runtime = _required_child_runtime_assets(
            child_manifest=child.current_manifest(),
            child_package_id=child_package_id,
            leg_id=leg_id,
        )
        child_runtime_assets.append((child_package_id, child_runtime))
        updated_leg = dict(leg)
        updated_leg["runtime_assets"] = child_runtime.model_dump(mode="json")
        updated_legs.append(updated_leg)

    parent_runtime_assets = _parent_runtime_assets_from_child_leg_assets(
        package_id=package_id,
        child_runtime_assets=child_runtime_assets,
    )
    evidence.setdefault("multi_alpha", {})["legs"] = updated_legs
    scratch_manifest = manifest.model_copy(
        update={
            "runtime_assets": parent_runtime_assets,
            "source_evidence": evidence,
        }
    )
    return ScratchParentRuntimeView(
        manifest=scratch_manifest,
        metadata={
            "scratch_only": True,
            "writes_db": False,
            "mutates_manifest": False,
            "reason": "legacy_parent_missing_runtime_assets_factor_schema",
            "parent_manifest_sha256_preserved": parent_manifest.manifest_sha256,
            "parent_runtime_assets_before": parent_manifest.runtime_assets.model_dump(mode="json")
            if parent_manifest.runtime_assets is not None
            else None,
            "parent_runtime_assets_after": parent_runtime_assets.model_dump(mode="json"),
            "leg_runtime_assets_from_children": [
                {
                    "child_package_id": child_package_id,
                    "runtime_assets": runtime_assets.model_dump(mode="json"),
                }
                for child_package_id, runtime_assets in child_runtime_assets
            ],
        },
    )


def _required_child_runtime_assets(
    *,
    child_manifest: Any,
    child_package_id: str,
    leg_id: str,
) -> RuntimeAssetManifest:
    runtime_assets = child_manifest.runtime_assets
    if runtime_assets is None:
        raise DataUnavailableError(
            "legacy child package has no runtime_assets for scratch parent view",
            context={
                "reason_code": "multi_alpha_parent_parity_child_runtime_assets_missing",
                "child_package_id": child_package_id,
                "leg_id": leg_id,
            },
        )
    alpha158 = runtime_assets.alpha158
    if alpha158.enabled and (not alpha158.asset_ref or not alpha158.sha256 or not alpha158.aliases):
        raise DataUnavailableError(
            "legacy child Alpha158 runtime asset is incomplete for scratch parent view",
            context={
                "reason_code": "multi_alpha_parent_parity_child_alpha158_incomplete",
                "child_package_id": child_package_id,
                "leg_id": leg_id,
                "asset_ref": alpha158.asset_ref,
                "sha256": alpha158.sha256,
                "alias_count": len(alpha158.aliases or []),
            },
        )
    return runtime_assets


def _parent_runtime_assets_from_child_leg_assets(
    *,
    package_id: str,
    child_runtime_assets: Sequence[tuple[str, RuntimeAssetManifest]],
) -> RuntimeAssetManifest:
    enabled: list[tuple[str, RuntimeAssetManifest]] = [
        (child_package_id, runtime_assets)
        for child_package_id, runtime_assets in child_runtime_assets
        if runtime_assets.alpha158.enabled
    ]
    if not enabled:
        return RuntimeAssetManifest()

    first_child, first_runtime = enabled[0]
    first_alpha158 = first_runtime.alpha158
    for child_package_id, runtime_assets in enabled[1:]:
        alpha158 = runtime_assets.alpha158
        if (
            alpha158.asset_ref != first_alpha158.asset_ref
            or str(alpha158.sha256 or "").strip().lower() != str(first_alpha158.sha256 or "").strip().lower()
            or list(alpha158.aliases) != list(first_alpha158.aliases)
        ):
            raise DataUnavailableError(
                "scratch parent view cannot merge divergent child Alpha158 schemas",
                context={
                    "reason_code": "multi_alpha_parent_parity_child_alpha158_mismatch",
                    "package_id": package_id,
                    "first_child_package_id": first_child,
                    "first_sha256": first_alpha158.sha256,
                    "second_child_package_id": child_package_id,
                    "second_sha256": alpha158.sha256,
                },
            )
    return RuntimeAssetManifest(alpha158=first_alpha158)


class _SingleRecordRepository:
    def __init__(self, *, package_id: str, manifest: Any) -> None:
        self.package_id = package_id
        self.manifest = manifest

    def get(self, package_id: str):  # noqa: ANN201
        if package_id != self.package_id:
            raise AssertionError(f"oracle repository got unexpected package_id={package_id}")
        return type("Record", (), {"current_manifest": lambda _self: self.manifest})()


class _LegacyChildRuntimeResolver(QEExperimentRuntimeAssetResolver):
    def __init__(self, *, repository: StrategyPackageRepository) -> None:
        super().__init__()
        self.repository = repository

    def load_source_for_strategy_package_leg(self, **kwargs):  # noqa: ANN001, ANN201
        manifest = kwargs["manifest"]
        leg_id = kwargs["leg_id"]
        evidence = manifest.source_evidence.get("multi_alpha", {}) if isinstance(manifest.source_evidence, dict) else {}
        leg = next((item for item in evidence.get("legs", []) if item.get("leg_id") == leg_id), None)
        if not leg:
            raise DataUnavailableError(
                "legacy child oracle cannot find leg evidence",
                context={"reason_code": "multi_alpha_legacy_oracle_leg_missing", "leg_id": leg_id},
            )
        child = self.repository.get(leg["child_package_id"])
        child_manifest = child.current_manifest()
        return self.load_source_for_strategy_package(
            source_type=child.source_type,
            source_id=child.source_id,
            loop_id=child.loop_id,
            run_id=(leg.get("seed_run_ids") or [child.run_id])[0],
            manifest=child_manifest,
            package_id=child.package_id,
        )


class _OracleProvider:
    def __init__(self, *, backend: str) -> None:
        if backend == "wsl":
            from backend.services.strategy_package.live_inference import WslStrategyPackageInferenceProvider

            self._provider = WslStrategyPackageInferenceProvider()
            self.backend_name = "wsl_legacy_child_oracle"
        else:
            from backend.services.strategy_package.live_inference import LocalStrategyPackageInferenceProvider

            self._provider = LocalStrategyPackageInferenceProvider()
            self.backend_name = "local_legacy_child_oracle"

    def run(self, **kwargs):  # noqa: ANN001, ANN201
        return self._provider.run(**kwargs)


def _manifest_topk(manifest: Any) -> int:
    daily = (manifest.backtest_context or {}).get("daily_strategy") if isinstance(manifest.backtest_context, dict) else None
    if isinstance(daily, dict) and daily.get("topk"):
        return int(daily["topk"])
    raise StrategyPackageValidationError(
        "manifest daily_strategy.topk is required for parity compare",
        context={"reason_code": "multi_alpha_parent_parity_topk_missing", "package_id": manifest.package_id},
    )


def _assert_same_ranked_rows(parent_rows: Sequence[Mapping[str, Any]], legacy_rows: Sequence[Mapping[str, Any]], *, tolerance: float) -> None:
    if len(parent_rows) != len(legacy_rows):
        _fail_compare("row_count", parent_rows, legacy_rows, tolerance=tolerance)
    for idx, (parent, legacy) in enumerate(zip(parent_rows, legacy_rows, strict=True)):
        if parent.get("symbol") != legacy.get("symbol") or int(parent.get("rank")) != int(legacy.get("rank")):
            raise StrategyPackageValidationError(
                "parent and legacy child ranked rows differ",
                context={
                    "reason_code": "multi_alpha_parent_parity_rank_mismatch",
                    "row_index": idx,
                    "parent": {"symbol": parent.get("symbol"), "rank": parent.get("rank")},
                    "legacy": {"symbol": legacy.get("symbol"), "rank": legacy.get("rank")},
                },
            )
        diff = abs(float(parent.get("score")) - float(legacy.get("score")))
        if diff > tolerance:
            raise StrategyPackageValidationError(
                "parent and legacy child combined scores differ",
                context={
                    "reason_code": "multi_alpha_parent_parity_score_mismatch",
                    "row_index": idx,
                    "symbol": parent.get("symbol"),
                    "parent_score": parent.get("score"),
                    "legacy_score": legacy.get("score"),
                    "abs_diff": diff,
                    "tolerance": tolerance,
                },
            )


def _leg_normalized_diffs(parent_rows: Sequence[Mapping[str, Any]], legacy_rows: Sequence[Mapping[str, Any]], *, tolerance: float) -> dict[str, float]:
    diffs: dict[str, float] = {}
    for parent, legacy in zip(parent_rows, legacy_rows, strict=True):
        parent_components = parent.get("component_scores") or {}
        legacy_components = legacy.get("component_scores") or {}
        if set(parent_components) != set(legacy_components):
            raise StrategyPackageValidationError(
                "parent and legacy child component score legs differ",
                context={
                    "reason_code": "multi_alpha_parent_parity_component_set_mismatch",
                    "parent_legs": sorted(parent_components),
                    "legacy_legs": sorted(legacy_components),
                },
            )
        for leg_id in parent_components:
            diff = abs(
                float(parent_components[leg_id]["normalized_score"])
                - float(legacy_components[leg_id]["normalized_score"])
            )
            diffs[leg_id] = max(diffs.get(leg_id, 0.0), diff)
            if diff > tolerance:
                raise StrategyPackageValidationError(
                    "parent and legacy child per-leg normalized scores differ",
                    context={
                        "reason_code": "multi_alpha_parent_parity_leg_score_mismatch",
                        "symbol": parent.get("symbol"),
                        "leg_id": leg_id,
                        "abs_diff": diff,
                        "tolerance": tolerance,
                    },
                )
    return diffs


def _assert_same_weights(parent_metadata: Mapping[str, Any], legacy_metadata: Mapping[str, Any], *, tolerance: float) -> None:
    parent_weights = {str(key): float(value) for key, value in (parent_metadata.get("weights") or {}).items()}
    legacy_weights = {str(key): float(value) for key, value in (legacy_metadata.get("weights") or {}).items()}
    if set(parent_weights) != set(legacy_weights):
        raise StrategyPackageValidationError(
            "parent and legacy child weight legs differ",
            context={
                "reason_code": "multi_alpha_parent_parity_weight_set_mismatch",
                "parent_legs": sorted(parent_weights),
                "legacy_legs": sorted(legacy_weights),
            },
        )
    for leg_id, parent_weight in parent_weights.items():
        legacy_weight = legacy_weights[leg_id]
        diff = abs(parent_weight - legacy_weight)
        if diff > tolerance:
            raise StrategyPackageValidationError(
                "parent and legacy child weights differ",
                context={
                    "reason_code": "multi_alpha_parent_parity_weight_mismatch",
                    "leg_id": leg_id,
                    "parent_weight": parent_weight,
                    "legacy_weight": legacy_weight,
                    "abs_diff": diff,
                    "tolerance": tolerance,
                },
            )


def _max_score_diff(parent_rows: Sequence[Mapping[str, Any]], legacy_rows: Sequence[Mapping[str, Any]]) -> float:
    if not parent_rows:
        return 0.0
    return max(abs(float(parent.get("score")) - float(legacy.get("score"))) for parent, legacy in zip(parent_rows, legacy_rows, strict=True))


def _fail_compare(kind: str, parent_rows: Sequence[Mapping[str, Any]], legacy_rows: Sequence[Mapping[str, Any]], *, tolerance: float) -> None:
    raise StrategyPackageValidationError(
        "parent and legacy child parity compare failed",
        context={
            "reason_code": "multi_alpha_parent_parity_mismatch",
            "mismatch_kind": kind,
            "parent_row_count": len(parent_rows),
            "legacy_row_count": len(legacy_rows),
            "tolerance": tolerance,
        },
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # fail-fast with explicit context for Tier2 reruns
        context = getattr(exc, "context", {}) or {}
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "context": context,
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), file=sys.stderr)
        raise SystemExit(1) from exc
