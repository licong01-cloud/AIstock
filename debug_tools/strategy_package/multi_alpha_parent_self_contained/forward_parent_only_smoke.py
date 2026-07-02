#!/usr/bin/env python
"""Forward smoke for parent-only MULTI_ALPHA promotion.

This debug tool reads existing combine/QE evidence, writes only scratch local
package assets, and persists the promoted package only in memory. It is meant
to prove that future parent-only promotion creates a complete self-contained
parent package without writing production DB rows.
"""

# ruff: noqa: E402 - debug tool bootstraps repo root before backend imports.

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from backend.db.pg_pool import get_conn
from backend.services.multi_alpha.combine_backtest import (
    InMemoryCombineBacktestRepository,
    MultiAlphaCombineBacktestRepository,
)
from backend.services.qe_archive.multi_alpha_provenance import MultiAlphaProvenanceResolver
from backend.services.qe_archive.repository import QEArchiveRepository
from backend.services.strategy_package.frozen_runtime_self_check import FrozenRuntimeSelfCheckResult, FrozenRuntimeSelfCheckService
from backend.services.strategy_package.live_inference import QEExperimentRuntimeAssetResolver
from backend.services.strategy_package.models import AlphaMode, StrategyPackageManifest
from backend.services.strategy_package.multi_alpha_promotion import (
    MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION,
    MultiAlphaPackagePromotionService,
)
from backend.services.strategy_package.package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from backend.services.strategy_package.package_asset_freeze import PackageAssetFreezeService, StrategyPackageAssetSource
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.strategy_package.qe_source_resolver import QEExperimentSourceResolver
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    StrategyPackageSelectionArtifactService,
)
from backend.services.trading_core.errors import StrategyPackageValidationError


DEFAULT_RUN_ID = "macb_7738e811293948eb_20240702_20260310_20260625T184334308696Z"
DEFAULT_TOLERANCE_NOTE = "forward smoke asserts structural completeness and non-empty parent-self-contained signal"
DEFAULT_ENV_FILE = Path(r"F:\Dev\AIstock\.env")


@dataclass
class RecordingSelfCheck:
    delegate: FrozenRuntimeSelfCheckService
    results: list[dict[str, Any]]

    def assert_manifest_self_contained(self, manifest: StrategyPackageManifest) -> FrozenRuntimeSelfCheckResult:
        result = self.delegate.assert_manifest_self_contained(manifest)
        self.results.append(
            {
                "package_id": manifest.package_id,
                "alpha_mode": manifest.alpha_mode.value,
                "manifest_sha256": manifest.manifest_sha256,
                "context": result.to_context(),
            }
        )
        return result


@contextmanager
def _read_only_conn() -> Iterator[Any]:
    """Production DB guard: any accidental write must fail at the session layer."""

    with get_conn(autocommit=True) as conn:
        conn.set_session(readonly=True, autocommit=True)
        yield conn


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--weighting-scheme", default="ic_weighted")
    parser.add_argument("--scheme-result-id", default=None)
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--backend", default="wsl", choices=["wsl", "local"])
    parser.add_argument("--top-k", type=int, default=None)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--read-only-production-db", action="store_true", required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.env_file:
        load_dotenv(args.env_file)
    payload = run_forward_smoke(
        run_id=args.run_id,
        weighting_scheme=args.weighting_scheme,
        scheme_result_id=args.scheme_result_id,
        trade_date=date.fromisoformat(args.trade_date) if args.trade_date else None,
        backend=args.backend,
        top_k=args.top_k,
    )
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


def run_forward_smoke(
    *,
    run_id: str,
    weighting_scheme: str,
    scheme_result_id: str | None,
    trade_date: date | None,
    backend: str,
    top_k: int | None,
) -> dict[str, Any]:
    bundle = MultiAlphaCombineBacktestRepository(connection_provider=_read_only_conn).get_run(run_id)
    if bundle is None:
        raise StrategyPackageValidationError(
            "combine run missing for forward parent-only smoke",
            context={"reason_code": "multi_alpha_forward_smoke_combine_run_missing", "run_id": run_id},
        )
    run = dict(bundle["run"])
    if str(run.get("status") or "").lower() != "succeeded":
        raise StrategyPackageValidationError(
            "combine run must be succeeded for forward parent-only smoke",
            context={"reason_code": "multi_alpha_forward_smoke_combine_run_not_succeeded", "run_id": run_id, "status": run.get("status")},
        )

    smoke_trade_date = trade_date or _parse_date(run.get("oos_start")) or date(2024, 7, 2)
    topk = top_k or _topk_from_run(run)
    scratch_parent = REPO_ROOT / "rdagent_assets" / "strategy_package_runtime" / "_forward_parent_smoke"
    scratch_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="multi_alpha_forward_parent_smoke_", dir=scratch_parent) as tmp:
        scratch_root = Path(tmp)
        package_asset_store = LocalPackageAssetStore(scratch_root / "package_assets")
        runtime_cache_root = scratch_root / "runtime_cache"

        combine_repo = _scratch_combine_repository(bundle)
        package_repo = InMemoryStrategyPackageRepository()
        real_self_check = FrozenRuntimeSelfCheckService(
            asset_store=package_asset_store,
            cache_root=runtime_cache_root / "self_check",
            model_probe_backend=backend,
        )
        recording_self_check = RecordingSelfCheck(delegate=real_self_check, results=[])
        asset_source = StrategyPackageAssetSource(
            conn_factory=_read_only_conn,
            local_workspace_roots=_local_workspace_roots(),
        )
        asset_freezer = PackageAssetFreezeService(
            asset_store=package_asset_store,
            source=asset_source,
        )

        result = MultiAlphaPackagePromotionService(
            combine_repository=combine_repo,
            package_repository=package_repo,
            provenance_resolver=MultiAlphaProvenanceResolver(
                repository=QEArchiveRepository(connection_provider=_read_only_conn)
            ),
            source_resolver=QEExperimentSourceResolver(conn_factory=_read_only_conn),
            asset_freezer=asset_freezer,
            frozen_runtime_self_check=recording_self_check,
            prediction_ref_roots=_prediction_ref_roots(),
        ).promote_from_combine_run(
            combine_backtest_run_id=run_id,
            weighting_scheme=weighting_scheme,
            scheme_result_id=scheme_result_id,
            topk=topk,
            secondary_topk=[],
            package_name=f"scratch_forward_parent_only_{run_id[-12:]}",
            weight_policy=_weight_policy(run),
            confirmation=MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION,
        )

        parent = result.package
        manifest = parent.current_manifest()
        _assert_parent_only(package_repo=package_repo, manifest=manifest, result_components=result.components)
        asset_rows = package_repo.list_package_assets(parent.package_id)
        _assert_forward_assets(manifest=manifest, asset_rows=asset_rows)

        artifact = StrategyPackageSelectionArtifactService(
            package_repository=package_repo,
            artifact_repository=InMemorySelectionScoreArtifactRepository(),
            runtime_asset_resolver=QEExperimentRuntimeAssetResolver(
                cache_root=runtime_cache_root / "runtime_signal",
                asset_store=package_asset_store,
            ),
        ).generate_from_live_inference(
            package_id=parent.package_id,
            trade_date=smoke_trade_date,
            runtime_config=_runtime_config(topk=topk, backend=backend),
            include_reference_price=False,
        )
        _assert_signal_artifact(artifact.metadata)

        return {
            "ok": True,
            "schema_version": "multi_alpha_forward_parent_only_smoke_v1",
            "scope": {
                "scratch_only": True,
                "production_db": "read_only_session",
                "writes_production_db": False,
                "writes_dev_db": False,
                "mutates_existing_package": False,
                "scratch_package_asset_store": str(scratch_root / "package_assets"),
                "note": DEFAULT_TOLERANCE_NOTE,
            },
            "input": {
                "combine_backtest_run_id": run_id,
                "weighting_scheme": weighting_scheme,
                "scheme_result_id": scheme_result_id,
                "trade_date": smoke_trade_date.isoformat(),
                "topk": topk,
                "backend": backend,
            },
            "promotion": {
                "package_id": parent.package_id,
                "alpha_mode": parent.alpha_mode.value,
                "package_status": parent.package_status.value,
                "manifest_sha256": parent.manifest_sha256,
                "record_count": len(package_repo.records),
                "multi_alpha_parent_count": len(
                    [record for record in package_repo.records.values() if record.alpha_mode == AlphaMode.MULTI_ALPHA]
                ),
                "single_alpha_child_count": len(
                    [record for record in package_repo.records.values() if record.alpha_mode == AlphaMode.SINGLE_ALPHA]
                ),
                "component_edge_count": len(package_repo.components),
                "result_component_count": len(result.components),
                "auto_component_materialization": result.auto_component_materialization,
            },
            "manifest": _manifest_summary(manifest),
            "asset_ledger": _asset_ledger_summary(asset_rows),
            "self_check": {
                "pass": True,
                "calls": recording_self_check.results,
                "parent_combined_signal_smoke": _parent_self_check_smoke(recording_self_check.results, parent.package_id),
            },
            "runtime_signal": {
                "pass": True,
                "score_count": artifact.score_count,
                "universe_count": artifact.universe_count,
                "top_score_symbol": artifact.top_score_symbol,
                "runtime_source": artifact.metadata.get("runtime_source"),
                "model_params_origin": artifact.metadata.get("model_params_origin"),
                "component_candidate_universe_size": artifact.metadata.get("component_candidate_universe_size"),
                "weights": artifact.metadata.get("weights"),
                "component_artifacts": artifact.metadata.get("component_artifacts"),
            },
        }


def _scratch_combine_repository(bundle: Mapping[str, Any]) -> InMemoryCombineBacktestRepository:
    repo = InMemoryCombineBacktestRepository()
    run = dict(bundle["run"])
    run_id = str(run["id"])
    repo.runs[run_id] = run
    repo.scheme_results.extend(dict(row) for row in bundle.get("scheme_results") or [])
    repo.loo.extend(dict(row) for row in bundle.get("loo") or [])
    return repo


def _weight_policy(run: Mapping[str, Any]) -> dict[str, Any]:
    config = run.get("backtest_config_json") if isinstance(run.get("backtest_config_json"), Mapping) else {}
    label_horizon = int(config.get("label_horizon") or 20)
    return {
        "mode": "frozen_backtest_terminal_weights",
        "metric": "rank_ic",
        "lookback_trading_days": 252,
        "min_periods": 60,
        "label_horizon": label_horizon,
        "label_maturity_lag_days": label_horizon,
        "clip_negative_to_zero": True,
    }


def _runtime_config(*, topk: int, backend: str) -> dict[str, Any]:
    return {
        "runtime_profile": {"selection": {"top_k": topk}},
        "selection_artifact_config": {
            "multi_alpha_live_inference_enabled": True,
            "component_coverage_threshold": topk,
            "inference_backend": backend,
        },
    }


def _topk_from_run(run: Mapping[str, Any]) -> int:
    config = run.get("backtest_config_json") if isinstance(run.get("backtest_config_json"), Mapping) else {}
    value = config.get("topk") or config.get("top_k") or 25
    return int(value)


def _local_workspace_roots() -> list[Path]:
    roots = []
    for raw in (
        os.getenv("AISTOCK_QE_WORKSPACE_ROOT"),
        os.getenv("AISTOCK_QE_LOCAL_WORKSPACE_ROOT"),
        r"F:\Dev\AIstock\rdagent_assets",
        r"F:\Dev\AIstock_worktrees",
    ):
        if raw:
            roots.append(Path(raw))
    return roots


def _prediction_ref_roots() -> list[Path]:
    return [
        Path(r"F:\Dev\AIstock\rdagent_assets\multi_alpha_combine_backtests"),
        Path("rdagent_assets/multi_alpha_combine_backtests"),
    ]


def _assert_parent_only(
    *,
    package_repo: InMemoryStrategyPackageRepository,
    manifest: StrategyPackageManifest,
    result_components: Sequence[Any],
) -> None:
    single_children = [record.package_id for record in package_repo.records.values() if record.alpha_mode == AlphaMode.SINGLE_ALPHA]
    if manifest.alpha_mode != AlphaMode.MULTI_ALPHA or len(package_repo.records) != 1 or single_children or package_repo.components or result_components:
        raise StrategyPackageValidationError(
            "forward smoke expected exactly one parent MULTI_ALPHA package and no child edges",
            context={
                "reason_code": "multi_alpha_forward_smoke_parent_only_failed",
                "package_id": manifest.package_id,
                "alpha_mode": manifest.alpha_mode.value,
                "record_count": len(package_repo.records),
                "single_alpha_child_package_ids": single_children,
                "component_edge_count": len(package_repo.components),
                "result_component_count": len(result_components),
            },
        )


def _assert_forward_assets(
    *,
    manifest: StrategyPackageManifest,
    asset_rows: Sequence[StrategyPackageAssetRecord],
) -> None:
    runtime_assets = manifest.runtime_assets
    alpha158 = runtime_assets.alpha158 if runtime_assets is not None else None
    if alpha158 is None or not alpha158.enabled or not alpha158.asset_ref or not alpha158.sha256:
        raise StrategyPackageValidationError(
            "forward parent package is missing frozen Alpha158 runtime assets",
            context={"reason_code": "multi_alpha_forward_smoke_alpha158_missing", "package_id": manifest.package_id},
        )
    if not any(row.asset_type == StrategyPackageAssetType.FACTOR_SCHEMA and row.asset_ref == alpha158.asset_ref for row in asset_rows):
        raise StrategyPackageValidationError(
            "forward parent package asset ledger is missing Alpha158 factor_schema row",
            context={
                "reason_code": "multi_alpha_forward_smoke_factor_schema_ledger_missing",
                "package_id": manifest.package_id,
                "alpha158_asset_ref": alpha158.asset_ref,
            },
        )
    model_assets = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    incomplete = [
        {
            "model_id": model.model_id,
            "model_code_required": model.model_code_required,
            "model_code_asset_count": len(model.model_code_assets or []),
        }
        for model in model_assets
        if model.model_code_required and not model.model_code_assets
    ]
    if incomplete:
        raise StrategyPackageValidationError(
            "forward parent package model_code assets are incomplete",
            context={
                "reason_code": "multi_alpha_forward_smoke_model_code_missing",
                "package_id": manifest.package_id,
                "models": incomplete,
            },
        )
    if not any(row.asset_type == StrategyPackageAssetType.MODEL_CODE for row in asset_rows):
        raise StrategyPackageValidationError(
            "forward parent package did not freeze any model_code asset; BUG-573 path was not exercised",
            context={
                "reason_code": "multi_alpha_forward_smoke_model_code_not_exercised",
                "package_id": manifest.package_id,
                "model_count": len(model_assets),
            },
        )


def _assert_signal_artifact(metadata: Mapping[str, Any]) -> None:
    if metadata.get("runtime_source") != "parent_package_asset" or metadata.get("model_params_origin") != "package_asset":
        raise StrategyPackageValidationError(
            "forward runtime signal did not use parent package-owned assets",
            context={
                "reason_code": "multi_alpha_forward_smoke_runtime_source_mismatch",
                "runtime_source": metadata.get("runtime_source"),
                "model_params_origin": metadata.get("model_params_origin"),
            },
        )


def _manifest_summary(manifest: StrategyPackageManifest) -> dict[str, Any]:
    runtime_assets = manifest.runtime_assets
    alpha158 = runtime_assets.alpha158 if runtime_assets is not None else None
    model_assets = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    return {
        "package_id": manifest.package_id,
        "manifest_sha256": manifest.manifest_sha256,
        "alpha_mode": manifest.alpha_mode.value,
        "runtime_assets_alpha158_enabled": bool(alpha158 and alpha158.enabled),
        "runtime_assets_alpha158_sha256": alpha158.sha256 if alpha158 else None,
        "runtime_assets_alpha158_alias_count": len(alpha158.aliases or []) if alpha158 else 0,
        "leg_count": len(manifest.alpha_components),
        "factor_count": len(manifest.factor_set),
        "model_count": len(model_assets),
        "models": [
            {
                "model_id": model.model_id,
                "sha256": model.sha256,
                "model_code_required": model.model_code_required,
                "model_code_asset_count": len(model.model_code_assets or []),
                "model_code_assets": [
                    {
                        "module_name": asset.module_name,
                        "relative_path": asset.relative_path,
                        "sha256": asset.sha256,
                    }
                    for asset in model.model_code_assets
                ],
            }
            for model in model_assets
        ],
        "legs": [
            {
                "leg_id": component.alpha_id,
                "model_id": component.model_id,
                "factor_ref_count": len(component.lineage.factor_artifact_refs or []),
                "model_artifact_ref": component.lineage.model_artifact_ref,
            }
            for component in manifest.alpha_components
        ],
        "source_evidence_has_child_refs": any(
            "child_package_id" in leg or "child_manifest_sha256" in leg
            for leg in (manifest.source_evidence.get("multi_alpha", {}).get("legs", []) if isinstance(manifest.source_evidence, dict) else [])
        ),
    }


def _asset_ledger_summary(asset_rows: Sequence[StrategyPackageAssetRecord]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for row in asset_rows:
        counts[row.asset_type.value] = counts.get(row.asset_type.value, 0) + 1
    return {
        "total_count": len(asset_rows),
        "counts_by_type": counts,
        "factor_schema_count": counts.get(StrategyPackageAssetType.FACTOR_SCHEMA.value, 0),
        "model_code_count": counts.get(StrategyPackageAssetType.MODEL_CODE.value, 0),
    }


def _parent_self_check_smoke(results: Sequence[Mapping[str, Any]], package_id: str) -> dict[str, Any] | None:
    for item in results:
        context = item.get("context") if isinstance(item.get("context"), Mapping) else {}
        if item.get("package_id") == package_id and context.get("combined_signal_smoke"):
            return dict(context["combined_signal_smoke"])
    return None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "context": getattr(exc, "context", {}) or {},
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, default=str), file=sys.stderr)
        raise SystemExit(1) from exc
