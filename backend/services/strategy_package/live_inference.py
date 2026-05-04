"""Authoritative live/latest-data inference for StrategyPackage selection.

This module builds a transient inference workspace from a frozen StrategyPackage
and its QE source assets. It never reads QE backtest ``pred.pkl`` as a current
selection signal; scores must be produced by recomputing factors from the
current DB data window and applying the saved QE model.
"""

from __future__ import annotations

import ast
import asyncio
import io
import json
import math
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from threading import Thread
from typing import Any, Callable, Iterator

import pandas as pd
import psycopg2.extras
import yaml

from backend.db.pg_pool import get_conn
from backend.infra.wsl_qlib_runner import win_to_wsl_path
from backend.services.quantevolver.node_execution import resolve_default_qe_node_id
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceClient,
    QEWorkspaceFileNotFound,
)
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError

from .workspace_policy import ensure_not_forbidden_worker_workspace_path

ConnFactory = Callable[[], Iterator[Any]]

AUTHORITATIVE_SELECTION_SOURCE_TYPE = "live_qe_model_inference_v1"
AUTHORITATIVE_SELECTION_SCOPE = "authoritative_selection"
DIAGNOSTIC_BACKTEST_SOURCE_TYPE = "qe_mlruns_pred_pkl_v1"
DIAGNOSTIC_BACKTEST_SCOPE = "diagnostic_backtest_only"


@dataclass(frozen=True)
class QEExperimentRuntimeSource:
    experiment_id: str
    db_workspace_path: Path
    asset_workspace_path: Path
    factor_names: list[str]
    custom_params: dict[str, Any]
    data_split: dict[str, Any]
    qe_task_id: str | None = None
    qe_loop_id: str | None = None
    execution_node_id: str | None = None


@dataclass(frozen=True)
class StaticLoaderFeatureResolution:
    factors: list[str]
    configs: list[str]
    missing_configs: list[str]
    unreadable_configs: list[dict[str, str]]


@dataclass(frozen=True)
class FactorOrderResolution:
    alpha158_factors: list[str]
    dynamic_factors: list[str]
    factor_order: list[str]
    dynamic_factor_source: str
    static_loader_schema_available: bool
    static_loader_configs: list[str]
    static_loader_missing_configs: list[str]
    static_loader_unreadable_configs: list[dict[str, str]]
    warnings: list[str]


@dataclass(frozen=True)
class PreparedInferenceWorkspace:
    workspace_path: Path
    manifest_path: Path
    factor_order_path: Path
    factor_entry_path: Path
    model_params_path: Path
    source_workspace_path: Path
    factor_source_dir: Path
    factor_order: list[str]
    alpha158_factors: list[str]
    dynamic_factors: list[str]
    model_source_path: Path
    model_candidate_count: int


@dataclass(frozen=True)
class LiveInferenceResult:
    scores: list[dict[str, Any]]
    metadata: dict[str, Any]


def _parse_jsonish(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        if not value.strip():
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise StrategyPackageValidationError(
                "invalid JSON payload in QE experiment runtime source",
                context={"value_preview": value[:200]},
            ) from exc
    return value


def _safe_name(value: str) -> str:
    text = re.sub(r"\W+", "_", value.strip())
    if not text or text[0].isdigit():
        text = f"factor_{text}"
    return text


def _date_to_datetime(value: date) -> datetime:
    return datetime.combine(value, datetime.min.time())


def _run_async_blocking(factory: Callable[[], Any]) -> Any:
    """Run a coroutine factory from sync service code, including event-loop threads."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())

    result: dict[str, Any] = {}

    def _target() -> None:
        try:
            result["value"] = asyncio.run(factory())
        except BaseException as exc:  # pragma: no cover - re-raised below
            result["error"] = exc

    thread = Thread(target=_target, daemon=True)
    thread.start()
    thread.join()
    if "error" in result:
        raise result["error"]
    return result.get("value")


def _safe_cache_component(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
    return text or "unknown"


def _remote_relpath(value: str) -> str:
    text = str(value or "").strip().replace("\\", "/")
    if not text:
        raise StrategyPackageValidationError("remote workspace file path is empty")
    if ":" in text:
        raise StrategyPackageValidationError(
            "absolute or drive-qualified QE workspace paths are not allowed",
            context={"path": value},
        )
    pure = PurePosixPath(text)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        raise StrategyPackageValidationError(
            "QE workspace file path must be a safe relative path",
            context={"path": value},
        )
    return str(pure)


def _score_rows_from_frame(df_scores: pd.DataFrame, expected_date: date) -> list[dict[str, Any]]:
    if df_scores is None or df_scores.empty:
        raise DataUnavailableError(
            "live QE model inference produced no score rows",
            context={"expected_trade_date": expected_date.isoformat()},
        )
    if isinstance(df_scores, pd.Series):
        df_scores = df_scores.to_frame(name="score")
    if "score" not in df_scores.columns:
        if len(df_scores.columns) == 1:
            df_scores = df_scores.rename(columns={df_scores.columns[0]: "score"})
        else:
            raise StrategyPackageValidationError(
                "live QE model inference output is missing score column",
                context={"columns": [str(item) for item in df_scores.columns]},
            )
    if not isinstance(df_scores.index, pd.MultiIndex):
        raise StrategyPackageValidationError(
            "live QE model inference output must use MultiIndex(datetime, instrument)",
            context={"index_type": type(df_scores.index).__name__},
        )
    names = list(df_scores.index.names)
    if "datetime" not in names or "instrument" not in names:
        raise StrategyPackageValidationError(
            "live QE model inference output index must contain datetime and instrument",
            context={"index_names": names},
        )

    actual_dates = sorted(set(pd.to_datetime(df_scores.index.get_level_values("datetime")).date))
    if actual_dates != [expected_date]:
        raise DataUnavailableError(
            "live QE model inference did not score the requested trade_date exactly",
            context={
                "expected_trade_date": expected_date.isoformat(),
                "actual_dates": [item.isoformat() for item in actual_dates],
            },
        )

    day = df_scores.reset_index()
    day["symbol"] = day["instrument"].astype(str)
    day["score"] = pd.to_numeric(day["score"], errors="coerce")
    if not day["score"].map(lambda value: pd.notna(value) and math.isfinite(float(value))).all():
        raise StrategyPackageValidationError(
            "live QE model inference output contains invalid scores",
            context={"row_count": int(len(day))},
        )
    day = day.sort_values(["score", "symbol"], ascending=[False, True]).reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    for rank, item in enumerate(day.itertuples(index=False), start=1):
        rows.append({"symbol": str(item.symbol), "score": float(item.score), "rank": rank})
    if not rows:
        raise DataUnavailableError(
            "live QE model inference produced an empty ranked universe",
            context={"expected_trade_date": expected_date.isoformat()},
        )
    return rows


class QEExperimentRuntimeAssetResolver:
    """Resolve QE source assets required for live model inference."""

    def __init__(
        self,
        *,
        conn_factory: ConnFactory | None = None,
        cache_root: Path | str | None = None,
    ) -> None:
        self._conn_factory = conn_factory or get_conn
        self.cache_root = Path(cache_root or Path("rdagent_assets") / "strategy_package_runtime")

    def load_source(self, experiment_id: str) -> QEExperimentRuntimeSource:
        experiment_id = str(experiment_id or "").strip()
        if not experiment_id:
            raise StrategyPackageValidationError("QE experiment_id is required for live inference")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT experiment_id, status, qe_task_id, qe_loop_id,
                           factor_names, custom_params, data_split
                    FROM qe_experiments
                    WHERE experiment_id = %s
                    """,
                    (experiment_id,),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "QE experiment does not exist for live inference",
                context={"experiment_id": experiment_id},
            )
        if str(row["status"]).lower() != "completed":
            raise StrategyPackageValidationError(
                "QE experiment must be completed before live inference",
                context={"experiment_id": experiment_id, "status": row["status"]},
            )
        factor_names = _parse_jsonish(row.get("factor_names")) or []
        if not isinstance(factor_names, list) or not factor_names:
            raise StrategyPackageValidationError(
                "QE experiment has no factor_names for live inference",
                context={"experiment_id": experiment_id},
            )
        custom_params = _parse_jsonish(row.get("custom_params")) or {}
        data_split = _parse_jsonish(row.get("data_split")) or {}
        if not isinstance(custom_params, dict):
            raise StrategyPackageValidationError("QE experiment custom_params must be an object")
        if not isinstance(data_split, dict):
            raise StrategyPackageValidationError("QE experiment data_split must be an object")

        qe_task_id = str(row.get("qe_task_id") or experiment_id).strip()
        qe_loop_id = str(row.get("qe_loop_id") or "").strip()
        if not qe_task_id or not qe_loop_id:
            raise DataUnavailableError(
                "QE experiment is missing qe_task_id/qe_loop_id for node API runtime asset resolution",
                context={"experiment_id": experiment_id, "qe_task_id": qe_task_id or None, "qe_loop_id": qe_loop_id or None},
            )
        execution_node_id = str(
            custom_params.get("execution_node_id")
            or custom_params.get("node_id")
            or resolve_default_qe_node_id()
        ).strip()
        asset_workspace = self._materialize_runtime_source_from_node(
            experiment_id=experiment_id,
            qe_task_id=qe_task_id,
            qe_loop_id=qe_loop_id,
            execution_node_id=execution_node_id,
            factor_names=[str(item) for item in factor_names],
            custom_params=custom_params,
            data_split=data_split,
        )
        return QEExperimentRuntimeSource(
            experiment_id=experiment_id,
            db_workspace_path=Path(),
            asset_workspace_path=asset_workspace,
            factor_names=[str(item) for item in factor_names],
            custom_params=custom_params,
            data_split=data_split,
            qe_task_id=qe_task_id,
            qe_loop_id=qe_loop_id,
            execution_node_id=execution_node_id,
        )

    def prepare_workspace(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        source: QEExperimentRuntimeSource,
        runtime_config: dict[str, Any] | None = None,
        path_converter: Callable[[str], str] | None = None,
    ) -> PreparedInferenceWorkspace:
        config = runtime_config or {}
        artifact_config = config.get("selection_artifact_config") or config.get("selection_artifact") or {}
        if artifact_config and not isinstance(artifact_config, dict):
            raise StrategyPackageValidationError("selection_artifact_config must be an object")

        source_conf = self._resolve_conf_path(source)
        factor_source_dir = self._resolve_factor_source_dir(source)
        factor_order_resolution = self._build_factor_order(
            source=source,
            conf_path=source_conf,
        )
        factor_files = self._resolve_factor_files(
            factor_source_dir,
            factor_order_resolution.dynamic_factors,
        )
        model_source_path, model_candidate_count = self._resolve_model_params_path(source, artifact_config)

        cache_key = manifest_sha256[:16] if manifest_sha256 else "unfrozen_manifest"
        workspace_path = self.cache_root / package_id / cache_key
        self._reset_cache_dir(workspace_path)
        (workspace_path / "model").mkdir(parents=True, exist_ok=True)

        model_dest = workspace_path / "model" / "params.pkl"
        shutil.copy2(model_source_path, model_dest)

        factor_order_path = workspace_path / "factor_order.json"
        factor_order_path.write_text(
            json.dumps(
                {
                    "package_id": package_id,
                    "source_experiment_id": source.experiment_id,
                    "total_factors": len(factor_order_resolution.factor_order),
                    "alpha158_count": len(factor_order_resolution.alpha158_factors),
                    "dynamic_count": len(factor_order_resolution.dynamic_factors),
                    "factor_order": factor_order_resolution.factor_order,
                    "alpha158_factors": factor_order_resolution.alpha158_factors,
                    "dynamic_factors": factor_order_resolution.dynamic_factors,
                    "dynamic_factor_source": factor_order_resolution.dynamic_factor_source,
                    "qe_experiment_factor_name_count": len(source.factor_names),
                    "static_loader_schema_available": factor_order_resolution.static_loader_schema_available,
                    "static_loader_configs": factor_order_resolution.static_loader_configs,
                    "static_loader_missing_configs": factor_order_resolution.static_loader_missing_configs,
                    "static_loader_unreadable_configs": factor_order_resolution.static_loader_unreadable_configs,
                    "schema_alignment_basis": factor_order_resolution.dynamic_factor_source,
                    "warnings": factor_order_resolution.warnings,
                    "source": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                    "is_aligned": True,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        factor_entry_path = workspace_path / "strategy_package_factor_entry.py"
        factor_entry_path.write_text(
            self._build_factor_entry_source(
                factor_files=factor_files,
                path_converter=path_converter,
            ),
            encoding="utf-8",
        )

        manifest_path = workspace_path / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "task_id": package_id,
                    "source": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                    "primary_assets": {
                        "model_weight_relpath": "model/params.pkl",
                        "factor_entry_relpath": "strategy_package_factor_entry.py",
                    },
                    "assets": {
                        "model_weight": "model/params.pkl",
                        "factor_entry": "strategy_package_factor_entry.py",
                        "factor_order": "factor_order.json",
                        "factors_count": len(factor_order_resolution.factor_order),
                    },
                    "diagnostics": {
                        "qe_experiment_id": source.experiment_id,
                        "source_workspace_path": str(source.asset_workspace_path),
                        "source_workspace_type": "aistock_node_api_cache",
                        "qe_task_id": source.qe_task_id,
                        "qe_loop_id": source.qe_loop_id,
                        "execution_node_id": source.execution_node_id,
                        "factor_source_dir": str(factor_source_dir),
                        "model_source_path": str(model_source_path),
                        "model_candidate_count": model_candidate_count,
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        return PreparedInferenceWorkspace(
            workspace_path=workspace_path,
            manifest_path=manifest_path,
            factor_order_path=factor_order_path,
            factor_entry_path=factor_entry_path,
            model_params_path=model_dest,
            source_workspace_path=source.asset_workspace_path,
            factor_source_dir=factor_source_dir,
            factor_order=factor_order_resolution.factor_order,
            alpha158_factors=factor_order_resolution.alpha158_factors,
            dynamic_factors=factor_order_resolution.dynamic_factors,
            model_source_path=model_source_path,
            model_candidate_count=model_candidate_count,
        )

    def _materialize_runtime_source_from_node(
        self,
        *,
        experiment_id: str,
        qe_task_id: str,
        qe_loop_id: str,
        execution_node_id: str,
        factor_names: list[str],
        custom_params: dict[str, Any],
        data_split: dict[str, Any],
    ) -> Path:
        source_dir = (
            self.cache_root
            / "_qe_node_sources"
            / _safe_cache_component(experiment_id)
            / _safe_cache_component(execution_node_id)
            / _safe_cache_component(qe_task_id)
            / _safe_cache_component(qe_loop_id)
        )
        self._reset_cache_dir(source_dir)

        async def _download() -> Path:
            async with QEWorkspaceClient.for_node(execution_node_id) as client:
                await self._download_workspace_file(client, qe_task_id, qe_loop_id, "conf.yaml", source_dir / "conf.yaml")

                temp_source = QEExperimentRuntimeSource(
                    experiment_id=experiment_id,
                    db_workspace_path=Path(),
                    asset_workspace_path=source_dir,
                    factor_names=factor_names,
                    custom_params=custom_params,
                    data_split=data_split,
                    qe_task_id=qe_task_id,
                    qe_loop_id=qe_loop_id,
                    execution_node_id=execution_node_id,
                )
                conf_path = source_dir / "conf.yaml"
                static_paths = self._find_static_loader_configs(
                    yaml.safe_load(conf_path.read_text(encoding="utf-8")) or {}
                )
                seen_static_relpaths: set[str] = set()
                for raw_path in static_paths:
                    if isinstance(raw_path, str) and raw_path.strip():
                        rel_path = _remote_relpath(raw_path)
                        if rel_path in seen_static_relpaths:
                            continue
                        seen_static_relpaths.add(rel_path)
                        try:
                            await self._download_workspace_file(
                                client,
                                qe_task_id,
                                qe_loop_id,
                                rel_path,
                                source_dir / rel_path,
                            )
                        except QEWorkspaceFileNotFound:
                            # Historical QE workspaces may keep conf.yaml and factors but not the schema parquet.
                            # Factor order can still be recovered from qe_experiments.factor_names below.
                            continue

                static_loader = self._extract_static_loader_feature_names(
                    source=temp_source,
                    conf_path=conf_path,
                )
                if static_loader.unreadable_configs:
                    raise DataUnavailableError(
                        "QE StaticDataLoader feature-order artifact is unreadable for live inference",
                        context={
                            "experiment_id": experiment_id,
                            "qe_task_id": qe_task_id,
                            "qe_loop_id": qe_loop_id,
                            "unreadable_configs": static_loader.unreadable_configs,
                        },
                    )
                required_factor_names = sorted(set(factor_names) | set(static_loader.factors))
                for factor_name in required_factor_names:
                    rel_path = _remote_relpath(f"factors/{factor_name}.py")
                    await self._download_workspace_file(
                        client,
                        qe_task_id,
                        qe_loop_id,
                        rel_path,
                        source_dir / rel_path,
                    )

                params_tar = await client.download_mlruns_params(qe_task_id, qe_loop_id)
                if not params_tar:
                    raise DataUnavailableError(
                        "QE node API returned an empty mlruns params archive",
                        context={"experiment_id": experiment_id, "qe_task_id": qe_task_id, "qe_loop_id": qe_loop_id},
                    )
                self._extract_mlruns_params_archive(params_tar, source_dir)
            return source_dir

        try:
            return _run_async_blocking(_download)
        except StrategyPackageValidationError:
            raise
        except DataUnavailableError:
            raise
        except Exception as exc:
            raise DataUnavailableError(
                "failed to materialize QE runtime assets through the node API",
                context={
                    "experiment_id": experiment_id,
                    "qe_task_id": qe_task_id,
                    "qe_loop_id": qe_loop_id,
                    "execution_node_id": execution_node_id,
                    "cache_dir": str(source_dir),
                    "error": str(exc),
                },
            ) from exc

    def _reset_cache_dir(self, path: Path) -> None:
        cache_root = self.cache_root.resolve(strict=False)
        target = path.resolve(strict=False)
        if target == cache_root or cache_root not in target.parents:
            raise StrategyPackageValidationError(
                "refusing to reset a path outside the StrategyPackage runtime cache",
                context={"path": str(path), "cache_root": str(self.cache_root)},
            )
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    async def _download_workspace_file(
        self,
        client: QEWorkspaceClient,
        task_id: str,
        loop_id: str,
        rel_path: str,
        target_path: Path,
    ) -> None:
        rel_path = _remote_relpath(rel_path)
        target = target_path.resolve(strict=False)
        cache_root = self.cache_root.resolve(strict=False)
        if cache_root not in target.parents:
            raise StrategyPackageValidationError(
                "refusing to write QE runtime asset outside the StrategyPackage runtime cache",
                context={"target_path": str(target_path), "cache_root": str(self.cache_root)},
            )
        data = await client.download_workspace_file_bytes(task_id, loop_id, rel_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(data)

    def _extract_mlruns_params_archive(self, payload: bytes, dest_dir: Path) -> None:
        dest_root = dest_dir.resolve(strict=False)
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
            members = archive.getmembers()
            for member in members:
                if member.issym() or member.islnk():
                    raise StrategyPackageValidationError(
                        "QE mlruns params archive must not contain links",
                        context={"member": member.name},
                    )
                target = (dest_dir / member.name).resolve(strict=False)
                if target != dest_root and dest_root not in target.parents:
                    raise StrategyPackageValidationError(
                        "QE mlruns params archive contains an unsafe path",
                        context={"member": member.name},
                    )
            try:
                archive.extractall(dest_dir, filter="data")
            except TypeError:  # pragma: no cover - compatibility with older Python/tarfile.
                archive.extractall(dest_dir)
        if not any(dest_dir.glob("**/artifacts/params.pkl")):
            raise DataUnavailableError(
                "QE mlruns params archive does not contain artifacts/params.pkl",
                context={"dest_dir": str(dest_dir)},
            )

    def _resolve_conf_path(self, source: QEExperimentRuntimeSource) -> Path:
        candidates = [source.asset_workspace_path / "conf.yaml"]
        for path in candidates:
            if path.exists() and path.is_file():
                return path
        raise DataUnavailableError(
            "QE conf.yaml is missing for live inference",
            context={"experiment_id": source.experiment_id, "checked_paths": [str(item) for item in candidates]},
        )

    def _build_factor_order(
        self,
        *,
        source: QEExperimentRuntimeSource,
        conf_path: Path,
    ) -> FactorOrderResolution:
        disable_alpha158 = bool(source.custom_params.get("disable_alpha158"))
        alpha158_factors = [] if disable_alpha158 else self._extract_alpha158_aliases(conf_path)
        static_loader = self._extract_static_loader_feature_names(
            source=source,
            conf_path=conf_path,
        )
        warnings: list[str] = []
        if static_loader.unreadable_configs:
            raise DataUnavailableError(
                "QE StaticDataLoader feature-order artifact is unreadable for live inference",
                context={
                    "experiment_id": source.experiment_id,
                    "conf_path": str(conf_path),
                    "unreadable_configs": static_loader.unreadable_configs,
                },
            )

        if static_loader.missing_configs:
            if not source.factor_names:
                raise DataUnavailableError(
                    "QE StaticDataLoader feature-order artifact is unavailable for live inference",
                    context={
                        "experiment_id": source.experiment_id,
                        "conf_path": str(conf_path),
                        "missing_configs": static_loader.missing_configs,
                    },
                )
            dynamic_factors = list(source.factor_names)
            dynamic_factor_source = "qe_experiments.factor_names_after_missing_static_loader"
            warnings.append(
                "StaticDataLoader schema artifact is missing; recovered dynamic factor order from qe_experiments.factor_names."
            )
        elif static_loader.factors:
            dynamic_factors = static_loader.factors
            dynamic_factor_source = "qe_static_dataloader"
        elif static_loader.configs:
            dynamic_factors = list(source.factor_names)
            dynamic_factor_source = "qe_experiments.factor_names_after_empty_static_loader"
            warnings.append(
                "StaticDataLoader schema artifact exposed no feature columns; recovered dynamic factor order from qe_experiments.factor_names."
            )
        else:
            dynamic_factors = list(source.factor_names)
            dynamic_factor_source = "qe_experiments.factor_names"
        factor_order = [*alpha158_factors, *dynamic_factors]
        if not factor_order:
            raise StrategyPackageValidationError(
                "live inference factor_order is empty",
                context={"experiment_id": source.experiment_id},
            )
        duplicates = sorted({item for item in factor_order if factor_order.count(item) > 1})
        if duplicates:
            raise StrategyPackageValidationError(
                "live inference factor_order contains duplicates",
                context={"experiment_id": source.experiment_id, "duplicates": duplicates},
            )
        return FactorOrderResolution(
            alpha158_factors=alpha158_factors,
            dynamic_factors=dynamic_factors,
            factor_order=factor_order,
            dynamic_factor_source=dynamic_factor_source,
            static_loader_schema_available=bool(static_loader.configs)
            and not static_loader.missing_configs
            and not static_loader.unreadable_configs
            and bool(static_loader.factors),
            static_loader_configs=static_loader.configs,
            static_loader_missing_configs=static_loader.missing_configs,
            static_loader_unreadable_configs=static_loader.unreadable_configs,
            warnings=warnings,
        )

    def _extract_static_loader_feature_names(
        self,
        *,
        source: QEExperimentRuntimeSource,
        conf_path: Path,
    ) -> StaticLoaderFeatureResolution:
        try:
            conf = yaml.safe_load(conf_path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            raise StrategyPackageValidationError(
                "failed to parse QE conf.yaml for static factor order",
                context={"conf_path": str(conf_path), "error": str(exc)},
            ) from exc

        configs = self._find_static_loader_configs(conf)
        if not configs:
            return StaticLoaderFeatureResolution(
                factors=[],
                configs=[],
                missing_configs=[],
                unreadable_configs=[],
            )

        factors: list[str] = []
        config_paths: list[str] = []
        seen_config_paths: set[str] = set()
        missing_paths: list[str] = []
        unreadable_paths: list[dict[str, str]] = []
        for config in configs:
            if not isinstance(config, str) or not config.strip():
                continue
            config_text = config.strip()
            if config_text in seen_config_paths:
                continue
            seen_config_paths.add(config_text)
            config_paths.append(config_text)
            path = Path(config_text)
            candidates = [path] if path.is_absolute() else [
                source.asset_workspace_path / path,
                conf_path.parent / path,
            ]
            for candidate in candidates:
                ensure_not_forbidden_worker_workspace_path(
                    candidate,
                    purpose="live inference StaticDataLoader config",
                )
            resolved = next((candidate for candidate in candidates if candidate.exists() and candidate.is_file()), None)
            if resolved is None:
                missing_paths.append(config_text)
                continue
            try:
                factors.extend(self._read_static_feature_columns(resolved))
            except Exception as exc:
                unreadable_paths.append({"path": str(resolved), "error": str(exc)})

        if not factors:
            return StaticLoaderFeatureResolution(
                factors=[],
                configs=config_paths,
                missing_configs=missing_paths,
                unreadable_configs=unreadable_paths,
            )
        unique: list[str] = []
        seen: set[str] = set()
        for factor in factors:
            if factor not in seen:
                unique.append(factor)
                seen.add(factor)
        return StaticLoaderFeatureResolution(
            factors=unique,
            configs=config_paths,
            missing_configs=missing_paths,
            unreadable_configs=unreadable_paths,
        )

    def _find_static_loader_configs(self, node: Any) -> list[Any]:
        configs: list[Any] = []
        if isinstance(node, dict):
            if node.get("class") == "qlib.data.dataset.loader.StaticDataLoader":
                configs.append((node.get("kwargs") or {}).get("config"))
            for value in node.values():
                configs.extend(self._find_static_loader_configs(value))
        elif isinstance(node, list):
            for value in node:
                configs.extend(self._find_static_loader_configs(value))
        return configs

    def _read_static_feature_columns(self, path: Path) -> list[str]:
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            try:
                import pyarrow.parquet as pq  # type: ignore

                names = list(pq.ParquetFile(path).schema_arrow.names)
            except Exception as exc:
                raise StrategyPackageValidationError(
                    "failed to read parquet feature schema for live inference",
                    context={"path": str(path), "error": str(exc)},
                ) from exc
            factors: list[str] = []
            for name in names:
                parsed: Any = name
                if isinstance(name, str) and name.startswith("("):
                    try:
                        parsed = ast.literal_eval(name)
                    except (SyntaxError, ValueError):
                        parsed = name
                if isinstance(parsed, tuple) and len(parsed) >= 2 and parsed[0] == "feature":
                    factors.append(str(parsed[1]))
                elif isinstance(parsed, str) and parsed not in {"datetime", "instrument"}:
                    factors.append(parsed)
            return factors

        raise StrategyPackageValidationError(
            "unsupported StaticDataLoader feature-order artifact format for live inference",
            context={"path": str(path), "suffix": suffix},
        )

    def _extract_alpha158_aliases(self, conf_path: Path) -> list[str]:
        try:
            conf = yaml.safe_load(conf_path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            raise StrategyPackageValidationError(
                "failed to parse QE conf.yaml for alpha158 factors",
                context={"conf_path": str(conf_path), "error": str(exc)},
            ) from exc

        aliases = self._find_alpha158_aliases(conf)
        if not aliases:
            raise DataUnavailableError(
                "QE conf.yaml does not expose Alpha158 aliases required by live inference",
                context={"conf_path": str(conf_path)},
            )
        return aliases

    def _find_alpha158_aliases(self, node: Any) -> list[str]:
        if isinstance(node, dict):
            if node.get("class") == "qlib.contrib.data.loader.Alpha158DL":
                try:
                    feature = node["kwargs"]["config"]["feature"]
                    aliases = feature[1]
                except Exception:
                    aliases = None
                if isinstance(aliases, list) and all(isinstance(item, str) for item in aliases):
                    return [str(item) for item in aliases]
            for value in node.values():
                found = self._find_alpha158_aliases(value)
                if found:
                    return found
        elif isinstance(node, list):
            for value in node:
                found = self._find_alpha158_aliases(value)
                if found:
                    return found
        return []

    def _resolve_factor_source_dir(self, source: QEExperimentRuntimeSource) -> Path:
        candidates = [source.asset_workspace_path / "factors"]
        for candidate in candidates:
            if candidate.exists() and candidate.is_dir():
                return candidate
        raise DataUnavailableError(
            "QE factor source directory is missing for live inference",
            context={"experiment_id": source.experiment_id, "checked_paths": [str(item) for item in candidates]},
        )

    def _resolve_factor_files(self, factor_source_dir: Path, factor_names: list[str]) -> dict[str, Path]:
        files: dict[str, Path] = {}
        missing: list[str] = []
        for factor_name in factor_names:
            path = factor_source_dir / f"{factor_name}.py"
            if not path.exists() or not path.is_file():
                missing.append(factor_name)
            else:
                files[factor_name] = path
        if missing:
            raise DataUnavailableError(
                "QE factor source files are missing for live inference",
                context={
                    "factor_source_dir": str(factor_source_dir),
                    "missing_factors": missing,
                    "missing_count": len(missing),
                },
            )
        return files

    def _resolve_model_params_path(
        self,
        source: QEExperimentRuntimeSource,
        artifact_config: dict[str, Any],
    ) -> tuple[Path, int]:
        explicit = artifact_config.get("model_params_path")
        if explicit:
            path = Path(str(explicit))
            ensure_not_forbidden_worker_workspace_path(path, purpose="live inference explicit model_params_path")
            if not path.exists() or not path.is_file():
                raise DataUnavailableError(
                    "explicit model_params_path does not exist for live inference",
                    context={"model_params_path": str(path), "experiment_id": source.experiment_id},
                )
            return path, 1

        candidates: list[Path] = []
        root = source.asset_workspace_path
        if root.exists():
            candidates.extend(root.glob("**/artifacts/params.pkl"))
        unique: dict[str, Path] = {}
        for candidate in candidates:
            if candidate.exists() and candidate.is_file():
                unique[str(candidate).lower()] = candidate
        candidates = list(unique.values())
        if not candidates:
            raise DataUnavailableError(
                "QE model params.pkl is missing for live inference",
                context={
                    "experiment_id": source.experiment_id,
                    "asset_workspace_path": str(source.asset_workspace_path),
                },
            )
        candidates.sort(key=lambda item: (item.stat().st_mtime, str(item).lower()), reverse=True)
        return candidates[0], len(candidates)

    def _build_factor_entry_source(
        self,
        *,
        factor_files: dict[str, Path],
        path_converter: Callable[[str], str] | None,
    ) -> str:
        entries = {
            factor_name: path_converter(str(path)) if path_converter else str(path)
            for factor_name, path in factor_files.items()
        }
        lines = [
            "from __future__ import annotations",
            "",
            "import os",
            "import runpy",
            "import tempfile",
            "from pathlib import Path",
            "",
            "import pandas as pd",
            "",
            f"_FACTOR_FILES = {json.dumps(entries, ensure_ascii=False, indent=2)}",
            "_DATA_FILES = (",
            "    'daily_pv.h5', 'static_factors.parquet', 'daily_basic.h5',",
            "    'moneyflow.h5', 'sector_data.h5', 'bak_basic.h5',",
            "    'cyq_perf.h5', 'margin_detail.h5',",
            ")",
            "",
            "def _link_data_file(src: Path, dst: Path) -> None:",
            "    if dst.exists() or dst.is_symlink():",
            "        dst.unlink()",
            "    try:",
            "        os.link(src, dst)",
            "        return",
            "    except OSError:",
            "        pass",
            "    try:",
            "        os.symlink(src, dst)",
            "        return",
            "    except OSError:",
            "        raise RuntimeError(f'failed to link inference data file {src} -> {dst}')",
            "",
            "def _ensure_h5_aliases(base_dir: Path) -> None:",
            "    pv_path = base_dir / 'daily_pv.h5'",
            "    clean_pv_path = base_dir / 'daily_pv_clean.h5'",
            "    if pv_path.exists() and not clean_pv_path.exists():",
            "        pv = pd.read_hdf(pv_path)",
            "        dollar_cols = [col for col in pv.columns if str(col).startswith('$')]",
            "        if dollar_cols:",
            "            pv = pv[[col for col in pv.columns if col not in dollar_cols]]",
            "            pv.to_hdf(clean_pv_path, key='data', mode='w')",
            "    static_path = base_dir / 'static_factors.parquet'",
            "    if not static_path.exists():",
            "        return",
            "    aliases = [",
            "        'daily_basic.h5', 'moneyflow.h5', 'sector_data.h5',",
            "        'bak_basic.h5', 'cyq_perf.h5', 'margin_detail.h5',",
            "    ]",
            "    if all((base_dir / name).exists() for name in aliases):",
            "        return",
            "    df = pd.read_parquet(static_path)",
            "    for name in aliases:",
            "        out = base_dir / name",
            "        if not out.exists():",
            "            df.to_hdf(out, key='data', mode='w')",
            "",
            "def _run_factor(factor_name: str):",
            "    outer_dir = Path.cwd()",
            "    _ensure_h5_aliases(outer_dir)",
            "    source = Path(_FACTOR_FILES[factor_name])",
            "    if not source.exists():",
            "        raise FileNotFoundError(f'factor source missing: {source}')",
            "    with tempfile.TemporaryDirectory(prefix=f'sp_factor_{factor_name}_') as tmp:",
            "        work_root = Path(tmp)",
            "        factor_dir = work_root / f'_factor_{factor_name}'",
            "        factor_dir.mkdir(parents=True, exist_ok=True)",
            "        factor_py = factor_dir / 'factor.py'",
            "        factor_py.write_text(source.read_text(encoding='utf-8'), encoding='utf-8')",
            "        for filename in _DATA_FILES:",
            "            src = outer_dir / filename",
            "            if filename == 'daily_pv.h5' and (outer_dir / 'daily_pv_clean.h5').exists():",
            "                src = outer_dir / 'daily_pv_clean.h5'",
            "            if not src.exists():",
            "                continue",
            "            _link_data_file(src, work_root / filename)",
            "            _link_data_file(src, factor_dir / filename)",
            "        old_cwd = os.getcwd()",
            "        try:",
            "            os.chdir(factor_dir)",
            "            runpy.run_path(str(factor_py), run_name='__main__')",
            "            result_path = factor_dir / 'result.h5'",
            "            if not result_path.exists():",
            "                raise FileNotFoundError(f'factor result.h5 missing for {factor_name}')",
            "            result = pd.read_hdf(result_path)",
            "        finally:",
            "            os.chdir(old_cwd)",
            "    if isinstance(result, pd.Series):",
            "        result = result.to_frame(name=factor_name)",
            "    if not isinstance(result, pd.DataFrame):",
            "        raise TypeError(f'factor {factor_name} returned {type(result).__name__}, expected DataFrame')",
            "    if len(result.columns) == 1 and factor_name not in result.columns:",
            "        result = result.rename(columns={result.columns[0]: factor_name})",
            "    if result.empty:",
            "        raise ValueError(f'factor {factor_name} returned empty result')",
            "    return result",
            "",
        ]
        for idx, factor_name in enumerate(factor_files.keys(), start=1):
            lines.extend(
                [
                    f"def calculate_{idx:03d}_{_safe_name(factor_name)}():",
                    f"    return _run_factor({factor_name!r})",
                    "",
                ]
            )
        return "\n".join(lines)


class LocalStrategyPackageInferenceProvider:
    """Run the live inference engine in the current Python process."""

    def run(
        self,
        *,
        workspace: PreparedInferenceWorkspace,
        trade_date: date,
        cutoff_date: date | None = None,
    ) -> LiveInferenceResult:
        try:
            from backend.inference_engine import InferenceEngine
        except Exception as exc:
            raise DataUnavailableError(
                "local live inference engine is unavailable",
                context={"error": str(exc)},
            ) from exc
        old_strict = os.environ.get("AISTOCK_STRICT_INFERENCE")
        os.environ["AISTOCK_STRICT_INFERENCE"] = "1"
        try:
            df_scores = InferenceEngine().run_inference(
                strategy_id="",
                version_tag="strategy_package_live",
                trade_date=_date_to_datetime(trade_date),
                cutoff_date=_date_to_datetime(cutoff_date) if cutoff_date else None,
                experiment_id="strategy_package_live",
                workspace_path=str(workspace.workspace_path),
            )
        except Exception as exc:
            raise DataUnavailableError(
                "local live QE model inference failed",
                context={"workspace_path": str(workspace.workspace_path), "error": str(exc)},
            ) from exc
        finally:
            if old_strict is None:
                os.environ.pop("AISTOCK_STRICT_INFERENCE", None)
            else:
                os.environ["AISTOCK_STRICT_INFERENCE"] = old_strict
        return LiveInferenceResult(
            scores=_score_rows_from_frame(df_scores, cutoff_date or trade_date),
            metadata={"inference_backend": "local"},
        )


class WslStrategyPackageInferenceProvider:
    """Run live inference inside the WSL Qlib environment."""

    def __init__(
        self,
        *,
        distro: str | None = None,
        conda_sh: str | None = None,
        conda_env: str | None = None,
        repo_root: Path | str | None = None,
        timeout_seconds: int = 3600,
    ) -> None:
        self.distro = distro or os.getenv("QLIB_WSL_DISTRO") or "Ubuntu"
        self.conda_sh = conda_sh or os.getenv("QLIB_WSL_CONDA_SH") or "~/miniconda3/etc/profile.d/conda.sh"
        self.conda_env = conda_env or os.getenv("QLIB_WSL_CONDA_ENV") or "rdagent-gpu"
        self.repo_root = Path(repo_root or Path.cwd()).resolve()
        self.timeout_seconds = timeout_seconds

    def run(
        self,
        *,
        workspace: PreparedInferenceWorkspace,
        trade_date: date,
        cutoff_date: date | None = None,
    ) -> LiveInferenceResult:
        with tempfile.TemporaryDirectory(prefix="sp_live_inference_") as tmp:
            output_path = Path(tmp) / "scores.json"
            args = [
                "scripts/strategy_package_live_inference.py",
                "--runtime-workspace",
                win_to_wsl_path(str(workspace.workspace_path)),
                "--trade-date",
                trade_date.isoformat(),
                "--output-path",
                win_to_wsl_path(str(output_path)),
            ]
            if cutoff_date:
                args.extend(["--cutoff-date", cutoff_date.isoformat()])
            env_exports = self._build_env_exports()
            command = (
                f"source {self.conda_sh} && "
                f"conda activate {self.conda_env} && "
                f"cd {win_to_wsl_path(str(self.repo_root))} && "
                f"{env_exports} "
                + "python "
                + " ".join(self._quote(arg) for arg in args)
            )
            completed = subprocess.run(
                ["wsl", "-d", self.distro, "bash", "-lc", command],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.timeout_seconds,
                check=False,
            )
            if completed.returncode != 0:
                raise DataUnavailableError(
                    "WSL live QE model inference failed",
                    context={
                        "returncode": completed.returncode,
                        "stdout_tail": completed.stdout[-4000:],
                        "stderr_tail": completed.stderr[-4000:],
                        "workspace_path": str(workspace.workspace_path),
                    },
                )
            if not output_path.exists():
                raise DataUnavailableError(
                    "WSL live QE model inference did not write output JSON",
                    context={
                        "stdout_tail": completed.stdout[-4000:],
                        "stderr_tail": completed.stderr[-4000:],
                        "output_path": str(output_path),
                    },
                )
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        scores = payload.get("scores")
        if not isinstance(scores, list) or not scores:
            raise DataUnavailableError(
                "WSL live QE model inference output contains no scores",
                context={"payload_keys": sorted(payload.keys())},
            )
        metadata = dict(payload.get("metadata") or {})
        metadata.update({"inference_backend": "wsl", "wsl_distro": self.distro, "wsl_conda_env": self.conda_env})
        return LiveInferenceResult(scores=scores, metadata=metadata)

    def _build_env_exports(self) -> str:
        keys = [
            "TDX_DB_HOST",
            "TDX_DB_PORT",
            "TDX_DB_NAME",
            "TDX_DB_USER",
            "TDX_DB_PASSWORD",
            "AISTOCK_PG_STATEMENT_TIMEOUT_MS",
        ]
        exports = ["PYTHONIOENCODING=utf-8", "PYTHONDONTWRITEBYTECODE=1", "AISTOCK_STRICT_INFERENCE=1"]
        for key in keys:
            value = os.getenv(key)
            if value is not None:
                exports.append(f"{key}={self._quote(value)}")
        return " ".join(exports)

    @staticmethod
    def _quote(value: str) -> str:
        return "'" + str(value).replace("'", "'\"'\"'") + "'"
