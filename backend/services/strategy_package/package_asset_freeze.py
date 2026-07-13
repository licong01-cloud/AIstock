"""Freeze StrategyPackage runtime assets into package-owned storage."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import io
import json
import os
import re
import sys
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from threading import Thread
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import quote

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.model_store import ModelStoreService, PredictionArtifactStore
from backend.services.model_store.artifact_store import PredictionStoreError
from backend.services.quantevolver.node_execution import resolve_default_qe_node_id
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    DataUnavailableError,
    PackageAssetInvalidError,
    RuntimeConfigInvalidError,
    StrategyPackageValidationError,
)

from .manifest import freeze_manifest
from .models import (
    Alpha158SchemaAsset,
    FactorAsset,
    ModelAsset,
    ModelCodeAsset,
    RuntimeAssetManifest,
    StrategyPackageManifest,
)
from .package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from .package_asset_store import LocalPackageAssetStore, PackageAssetStore
from .runtime_schema import (
    alpha158_schema_bytes,
    alpha158_schema_payload,
    load_conf_yaml_bytes,
    model_code_module_from_pt_uri,
    pt_model_uri_from_conf,
)


@dataclass(frozen=True)
class PackageAssetBytes:
    data: bytes
    source_uri: str | None = None
    local_path: Path | None = None
    source_root: Path | None = None
    locator: QERuntimeAssetLocator | None = None


@dataclass(frozen=True)
class PackageAssetFreezeResult:
    manifest: StrategyPackageManifest
    assets: list[StrategyPackageAssetRecord]


ModelParamsReader = Callable[[StrategyPackageManifest], PackageAssetBytes]
FactorCodeReader = Callable[[FactorAsset, StrategyPackageManifest], PackageAssetBytes]
ConfYamlReader = Callable[[StrategyPackageManifest], PackageAssetBytes]
QEWorkspaceClientFactory = Callable[[str], QEWorkspaceClient]


@dataclass(frozen=True)
class QERuntimeAssetLocator:
    """Resolved QE coordinate used only to recover frozen package assets."""

    experiment_id: str | None = None
    qe_task_id: str | None = None
    qe_loop_id: str | None = None
    node_id: str | None = None
    source: str = "manifest"


class StrategyPackageAssetSource:
    """Resolve source bytes for a package freeze using auditable source attempts."""

    def __init__(
        self,
        *,
        model_store: ModelStoreService | Any | None = None,
        artifact_store: PredictionArtifactStore | None = None,
        conn_factory: Any = get_conn,
        workspace_client_factory: QEWorkspaceClientFactory | None = None,
        local_workspace_roots: Sequence[str | Path] | None = None,
    ) -> None:
        self.model_store = model_store or ModelStoreService()
        self.artifact_store = artifact_store or getattr(self.model_store, "artifact_store", None) or PredictionArtifactStore()
        self._conn_factory = conn_factory
        self._workspace_client_factory = workspace_client_factory or QEWorkspaceClient.for_node
        self._local_workspace_roots = _local_workspace_roots(local_workspace_roots)

    def model_params_bytes(self, manifest: StrategyPackageManifest) -> PackageAssetBytes:
        attempts: list[dict[str, Any]] = []
        experiment_candidates = _model_experiment_candidates(manifest)
        run_candidates = _model_run_candidates(manifest)
        if not experiment_candidates and not run_candidates:
            attempts.append(
                {
                    "source": "central_store",
                    "method": "model_store_lookup",
                    "error": "no experiment_id or run_id candidate resolved from manifest",
                    "strategy_package_source": manifest.source.model_dump(mode="json"),
                }
            )
        for experiment_id in experiment_candidates:
            try:
                pointer = self.model_store.get_pointer(experiment_id=experiment_id)
                store_uri = str(pointer.get("mlflow_artifact_uri") or "").strip()
                if store_uri:
                    path = self.artifact_store.resolve_artifact_path(
                        store_uri,
                        artifact_type="model_params",
                        artifact_name="params.pkl",
                    )
                    return PackageAssetBytes(
                        data=_read_non_empty(path, reason_code="strategy_package_model_params_missing"),
                        source_uri=f"{store_uri}/model_params",
                    )
                attempts.append(
                    {
                        "source": "central_store",
                        "method": "experiment_id_pointer",
                        "experiment_id": experiment_id,
                        "pointer_status": pointer.get("pointer_status"),
                        "error": "mlflow_artifact_uri missing",
                    }
                )
            except Exception as exc:
                attempts.append(
                    {
                        "source": "central_store",
                        "method": "experiment_id_pointer",
                        "experiment_id": experiment_id,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        for run_id in run_candidates:
            try:
                path = self.model_store.pull_params_path(run_id=run_id)
                return PackageAssetBytes(
                    data=_read_non_empty(path, reason_code="strategy_package_model_params_missing"),
                    source_uri=f"aistock-prediction-store://runs/{quote(run_id, safe='')}/model_params",
                )
            except Exception as exc:
                attempts.append(
                    {
                        "source": "central_store",
                        "method": "run_id_pointer",
                        "run_id": run_id,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        fallback = self._model_params_from_qe_sources(manifest, attempts)
        if fallback is not None:
            return fallback

        raise DataUnavailableError(
            "strategy package model params.pkl is missing",
            context={
                "reason_code": "strategy_package_model_params_missing",
                "package_id": manifest.package_id,
                "model_asset": _model_asset_summary(manifest.model_asset),
                "source": manifest.source.model_dump(mode="json"),
                "attempts": attempts,
                "attempted_sources": attempts,
            },
        )

    def factor_code_bytes(self, factor: FactorAsset, manifest: StrategyPackageManifest) -> PackageAssetBytes:
        factor_name = str(factor.factor_name or factor.factor_id or "").strip()
        if not factor_name:
            raise StrategyPackageValidationError(
                "strategy package factor name is required for asset freeze",
                context={"reason_code": "strategy_package_factor_code_missing", "package_id": manifest.package_id},
            )
        attempts: list[dict[str, Any]] = []
        central_ambiguous: dict[str, Any] | None = None
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT id, factor_name, source, code_text, is_available
                        FROM aistock_factor_catalog
                        WHERE factor_name = %s
                          AND code_text IS NOT NULL
                          AND length(trim(code_text)) > 0
                        ORDER BY is_available DESC NULLS LAST, id ASC
                        """,
                        (factor_name,),
                    )
                    rows = [dict(row) for row in cur.fetchall()]
        except Exception as exc:
            attempts.append(
                {
                    "source": "central_store",
                    "method": "factor_catalog",
                    "factor_name": factor_name,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            rows = []
        if not rows:
            attempts.append(
                {
                    "source": "central_store",
                    "method": "factor_catalog",
                    "factor_name": factor_name,
                    "error": "factor code_text missing",
                }
            )
        else:
            by_hash: dict[str, dict[str, Any]] = {}
            for row in rows:
                code = str(row.get("code_text") or "")
                digest = hashlib.sha256(code.encode("utf-8")).hexdigest()
                by_hash.setdefault(digest, row)
            if len(by_hash) == 1:
                row = next(iter(by_hash.values()))
                return PackageAssetBytes(
                    data=str(row.get("code_text") or "").encode("utf-8"),
                    source_uri=f"aistock_factor_catalog:{row.get('id')}:code_text",
                )
            central_ambiguous = {
                "source": "central_store",
                "method": "factor_catalog",
                "factor_name": factor_name,
                "error": "factor code is ambiguous",
                "candidate_count": len(rows),
                "distinct_code_sha256": sorted(by_hash),
                "candidate_sources": [
                    {"id": row.get("id"), "source": row.get("source"), "is_available": row.get("is_available")}
                    for row in rows
                ],
            }
            attempts.append(central_ambiguous)

        fallback = self._factor_code_from_qe_sources(factor_name=factor_name, manifest=manifest, attempts=attempts)
        if fallback is not None:
            return fallback

        if central_ambiguous is not None:
            raise StrategyPackageValidationError(
                "strategy package factor code is ambiguous and QE workspace fallback did not recover it",
                context={
                    "reason_code": "strategy_package_factor_code_ambiguous",
                    "package_id": manifest.package_id,
                    "factor_name": factor_name,
                    "strategy_package_source": manifest.source.model_dump(mode="json"),
                    "central_attempt": central_ambiguous,
                    "attempts": attempts,
                    "attempted_sources": attempts,
                },
            )

        raise DataUnavailableError(
            "strategy package factor code is missing",
            context={
                "reason_code": "strategy_package_factor_code_missing",
                "package_id": manifest.package_id,
                "factor_name": factor_name,
                "source": manifest.source.model_dump(mode="json"),
                "attempts": attempts,
                "attempted_sources": attempts,
            },
        )

    def _model_params_from_qe_sources(
        self,
        manifest: StrategyPackageManifest,
        attempts: list[dict[str, Any]],
    ) -> PackageAssetBytes | None:
        locators = self._runtime_asset_locators(manifest, attempts=attempts)
        node_locators = [locator for locator in locators if locator.qe_task_id and locator.qe_loop_id and locator.node_id]
        if not node_locators:
            attempts.append(
                {
                    "source": "qe_node",
                    "method": "download_mlruns_params",
                    "error": "no QE node locator resolved",
                    "locators": [_locator_payload(locator) for locator in locators],
                }
            )
        for locator in node_locators:
            try:
                data = self._download_node_model_params(locator)
                return PackageAssetBytes(
                    data=data,
                    source_uri=(
                        f"qe-workspace://node/{quote(locator.node_id, safe='')}"
                        f"/tasks/{quote(locator.qe_task_id, safe='')}"
                        f"/loops/{quote(locator.qe_loop_id, safe='')}/mlruns/artifacts/params.pkl"
                    ),
                )
            except ArtifactGenerationFailedError as exc:
                attempts.append(
                    {
                        "source": "qe_node",
                        "method": "download_mlruns_params",
                        "experiment_id": locator.experiment_id,
                        "qe_task_id": locator.qe_task_id,
                        "qe_loop_id": locator.qe_loop_id,
                        "node_id": locator.node_id,
                        "locator_source": locator.source,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                exc.context.setdefault("attempted_sources", attempts)
                raise
            except Exception as exc:
                attempts.append(
                    {
                        "source": "qe_node",
                        "method": "download_mlruns_params",
                        "experiment_id": locator.experiment_id,
                        "qe_task_id": locator.qe_task_id,
                        "qe_loop_id": locator.qe_loop_id,
                        "node_id": locator.node_id,
                        "locator_source": locator.source,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        local_roots = self._resolved_local_workspace_roots(attempts)
        local_candidates = list(_local_model_param_candidates(locators, local_roots))
        if not local_candidates:
            attempts.append(
                {
                    "source": "wsl_workspace",
                    "method": "local_workspace_glob",
                    "error": "no local workspace candidate paths resolved",
                    "workspace_roots": [str(root) for root in local_roots],
                    "locators": [_locator_payload(locator) for locator in locators],
                }
            )
            return None
        for path in local_candidates:
            try:
                return PackageAssetBytes(
                    data=_read_non_empty(path, reason_code="strategy_package_model_params_missing"),
                    source_uri=path.resolve(strict=False).as_uri(),
                )
            except Exception as exc:
                attempts.append(
                    {
                        "source": "wsl_workspace",
                        "method": "local_workspace_read",
                        "path": str(path),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
        return None

    def _factor_code_from_qe_sources(
        self,
        *,
        factor_name: str,
        manifest: StrategyPackageManifest,
        attempts: list[dict[str, Any]],
    ) -> PackageAssetBytes | None:
        locators = self._runtime_asset_locators(manifest, attempts=attempts)
        safe_factor_name = _safe_factor_file_name(factor_name)
        rel_path = _remote_relpath(f"factors/{safe_factor_name}.py")
        node_locators = [locator for locator in locators if locator.qe_task_id and locator.qe_loop_id and locator.node_id]
        if not node_locators:
            attempts.append(
                {
                    "source": "qe_node",
                    "method": "download_workspace_file_bytes",
                    "file_path": rel_path,
                    "factor_name": factor_name,
                    "error": "no QE node locator resolved",
                    "locators": [_locator_payload(locator) for locator in locators],
                }
            )
        for locator in node_locators:
            try:
                data = self._download_node_workspace_file(locator, rel_path=rel_path)
                return PackageAssetBytes(
                    data=_ensure_non_empty_bytes(
                        data,
                        reason_code="strategy_package_factor_code_missing",
                        context={"factor_name": factor_name, "locator": _locator_payload(locator)},
                    ),
                    source_uri=(
                        f"qe-workspace://node/{quote(locator.node_id, safe='')}"
                        f"/tasks/{quote(locator.qe_task_id, safe='')}"
                        f"/loops/{quote(locator.qe_loop_id, safe='')}/{quote(rel_path, safe='/')}"
                    ),
                )
            except Exception as exc:
                attempts.append(
                    {
                        "source": "qe_node",
                        "method": "download_workspace_file_bytes",
                        "file_path": rel_path,
                        "factor_name": factor_name,
                        "experiment_id": locator.experiment_id,
                        "qe_task_id": locator.qe_task_id,
                        "qe_loop_id": locator.qe_loop_id,
                        "node_id": locator.node_id,
                        "locator_source": locator.source,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        local_roots = self._resolved_local_workspace_roots(attempts)
        local_candidates = list(_local_factor_code_candidates(factor_name, locators, local_roots))
        if not local_candidates:
            attempts.append(
                {
                    "source": "wsl_workspace",
                    "method": "local_workspace_factor_glob",
                    "factor_name": factor_name,
                    "error": "no local workspace factor candidate paths resolved",
                    "workspace_roots": [str(root) for root in local_roots],
                    "locators": [_locator_payload(locator) for locator in locators],
                }
            )
            return None
        for path in local_candidates:
            try:
                return PackageAssetBytes(
                    data=_read_non_empty(path, reason_code="strategy_package_factor_code_missing"),
                    source_uri=path.resolve(strict=False).as_uri(),
                )
            except Exception as exc:
                attempts.append(
                    {
                        "source": "wsl_workspace",
                        "method": "local_workspace_factor_read",
                        "factor_name": factor_name,
                        "path": str(path),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
        return None

    def _runtime_asset_locators(
        self,
        manifest: StrategyPackageManifest,
        *,
        attempts: list[dict[str, Any]],
    ) -> list[QERuntimeAssetLocator]:
        raw_locators = _manifest_runtime_locators(manifest)
        try:
            raw_locators.extend(self._db_runtime_locators(manifest))
        except Exception as exc:
            attempts.append(
                {
                    "source": "source_coordinate_db",
                    "method": "resolve_runtime_asset_locators",
                    "error": f"{type(exc).__name__}: {exc}",
                    "strategy_package_source": manifest.source.model_dump(mode="json"),
                }
            )
        locators = _dedupe_locators(_expand_default_node_locators(raw_locators))
        locators = _dedupe_locators(self._expand_compute_node_locators(locators, attempts=attempts))
        if not locators:
            attempts.append(
                {
                    "source": "source_coordinates",
                    "method": "resolve_runtime_asset_locators",
                    "error": "no qe_task_id/qe_loop_id/experiment_id locator could be resolved",
                    "package_id": manifest.package_id,
                    "strategy_package_source": manifest.source.model_dump(mode="json"),
                }
            )
        return locators

    def _expand_compute_node_locators(
        self,
        locators: Sequence[QERuntimeAssetLocator],
        *,
        attempts: list[dict[str, Any]],
    ) -> list[QERuntimeAssetLocator]:
        node_ids = self._workspace_recovery_node_ids(attempts)
        if not node_ids:
            return list(locators)
        expanded = list(locators)
        for locator in locators:
            if not locator.qe_task_id or not locator.qe_loop_id:
                continue
            for node_id in node_ids:
                if locator.node_id == node_id:
                    continue
                expanded.append(
                    QERuntimeAssetLocator(
                        experiment_id=locator.experiment_id,
                        qe_task_id=locator.qe_task_id,
                        qe_loop_id=locator.qe_loop_id,
                        node_id=node_id,
                        source=f"{locator.source}.compute_node_catalog",
                    )
                )
        return expanded

    def _workspace_recovery_node_ids(self, attempts: list[dict[str, Any]]) -> list[str]:
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT node_id
                        FROM infra.compute_nodes
                        WHERE COALESCE(api_base_url, '') <> ''
                          AND lower(COALESCE(status, '')) <> 'offline'
                        ORDER BY
                          CASE WHEN node_id = %s THEN 0 ELSE 1 END,
                          node_id
                        """,
                        (_safe_default_qe_node_id() or "",),
                    )
                    rows = [dict(row) for row in cur.fetchall()]
        except Exception as exc:
            attempts.append(
                {
                    "source": "qe_node",
                    "method": "compute_node_catalog",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            return []
        return list(dict.fromkeys(str(row.get("node_id") or "").strip() for row in rows if str(row.get("node_id") or "").strip()))

    def _db_runtime_locators(self, manifest: StrategyPackageManifest) -> list[QERuntimeAssetLocator]:
        source_type = manifest.source.source_type.value
        locators: list[QERuntimeAssetLocator] = []
        if source_type == "candidate_strategy_package":
            locators.extend(self._candidate_runtime_locators(manifest.source.source_id))
            return locators
        if source_type == "qe_evolution_loop":
            locators.extend(
                self._load_locators_by_task_loop(
                    task_id=manifest.source.source_id,
                    loop_id=manifest.source.loop_id,
                    source="strategy_pkg.package.source",
                )
            )
            if manifest.source.run_id:
                locators.extend(self._load_locators_by_experiment_id(manifest.source.run_id, source="strategy_pkg.package.run_id"))
            return locators
        for experiment_id in _model_experiment_candidates(manifest):
            locators.extend(self._load_locators_by_experiment_id(experiment_id, source="strategy_pkg.package.source"))
        return locators

    def _candidate_runtime_locators(self, candidate_id: str) -> list[QERuntimeAssetLocator]:
        candidate_id = str(candidate_id or "").strip()
        if not candidate_id:
            return []
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT candidate_id, source_type, source_id, source_task_id,
                           source_loop_id, source_experiment_id, status
                    FROM strategy_pkg.candidate_strategy_package
                    WHERE candidate_id = %s
                    """,
                    (candidate_id,),
                )
                row = cur.fetchone()
        if not row:
            return []
        data = dict(row)
        locators: list[QERuntimeAssetLocator] = []
        candidate_source_type = str(data.get("source_type") or "").strip()
        source_id = str(data.get("source_id") or "").strip()
        task_id = str(data.get("source_task_id") or "").strip() or None
        loop_id = _short_loop_id(str(data.get("source_loop_id") or source_id).strip(), task_id=task_id)
        experiment_id = str(data.get("source_experiment_id") or "").strip() or None
        if candidate_source_type == "qe_experiment" and not experiment_id:
            experiment_id = source_id or None
        if experiment_id:
            locators.extend(self._load_locators_by_experiment_id(experiment_id, source="candidate_strategy_package"))
        if task_id and loop_id:
            locators.extend(
                self._load_locators_by_task_loop(
                    task_id=task_id,
                    loop_id=loop_id,
                    source="candidate_strategy_package",
                )
            )
        locators.append(
            QERuntimeAssetLocator(
                experiment_id=experiment_id,
                qe_task_id=task_id,
                qe_loop_id=loop_id,
                source="candidate_strategy_package.snapshot",
            )
        )
        return locators

    def _resolved_local_workspace_roots(self, attempts: list[dict[str, Any]]) -> list[Path]:
        roots = list(self._local_workspace_roots)
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT node_id, workspace_base, qlib_rdagent_root
                        FROM infra.compute_nodes
                        WHERE COALESCE(workspace_base, '') <> ''
                           OR COALESCE(qlib_rdagent_root, '') <> ''
                        ORDER BY node_id
                        """
                    )
                    rows = [dict(row) for row in cur.fetchall()]
        except Exception as exc:
            attempts.append(
                {
                    "source": "wsl_workspace",
                    "method": "compute_node_workspace_catalog",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            return _dedupe_paths(roots)

        for row in rows:
            for key in ("workspace_base", "qlib_rdagent_root"):
                path_text = str(row.get(key) or "").strip()
                if not path_text:
                    continue
                roots.append(Path(path_text))
                translated = _windows_path_from_wsl_mount(path_text)
                if translated is not None:
                    roots.append(translated)
                if key == "qlib_rdagent_root":
                    roots.append(Path(path_text) / "qe_workspace")
                    if translated is not None:
                        roots.append(translated / "qe_workspace")
        return _dedupe_paths(roots)

    def _load_locators_by_experiment_id(self, experiment_id: str, *, source: str) -> list[QERuntimeAssetLocator]:
        experiment_id = str(experiment_id or "").strip()
        if not experiment_id:
            return []
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT experiment_id, qe_task_id, qe_loop_id, loop_index,
                           custom_params, result_metrics
                    FROM qe_experiments
                    WHERE experiment_id = %s
                    ORDER BY completed_at DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (experiment_id,),
                )
                row = cur.fetchone()
        if not row:
            return []
        payload = dict(row)
        task_id = str(payload.get("qe_task_id") or "").strip() or None
        loop_id = _short_loop_id(str(payload.get("qe_loop_id") or "").strip(), task_id=task_id)
        node_id = self._node_id_for_locator(
            experiment_id=experiment_id,
            task_id=task_id,
            loop_id=loop_id,
            payload=payload,
        )
        return [
            QERuntimeAssetLocator(
                experiment_id=experiment_id,
                qe_task_id=task_id,
                qe_loop_id=loop_id,
                node_id=node_id,
                source=source,
            )
        ]

    def _load_locators_by_task_loop(
        self,
        *,
        task_id: str | None,
        loop_id: str | None,
        source: str,
    ) -> list[QERuntimeAssetLocator]:
        task_id = str(task_id or "").strip() or None
        loop_id = _short_loop_id(str(loop_id or "").strip(), task_id=task_id)
        if not task_id or not loop_id:
            return []
        loop_index = _loop_index(loop_id)
        task_prefixed_loop_id = f"{task_id}_{loop_id}" if task_id and loop_id and not loop_id.startswith(task_id) else loop_id
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT e.experiment_id, e.qe_task_id, e.qe_loop_id, e.loop_index,
                           e.custom_params, e.result_metrics, l.node_id
                    FROM qe_experiments e
                    LEFT JOIN qe_evolution_loops l
                      ON l.experiment_id = e.experiment_id
                      OR (
                          l.task_id = e.qe_task_id
                          AND (
                              l.loop_id = e.qe_loop_id
                              OR l.loop_id = CONCAT(e.qe_task_id, '_', e.qe_loop_id)
                              OR l.loop_index = e.loop_index
                          )
                      )
                    WHERE e.qe_task_id = %s
                      AND (
                          e.qe_loop_id = %s
                          OR e.qe_loop_id = %s
                          OR (%s IS NOT NULL AND e.loop_index = %s)
                      )
                    ORDER BY e.completed_at DESC NULLS LAST, e.created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (task_id, loop_id, task_prefixed_loop_id, loop_index, loop_index),
                )
                row = cur.fetchone()
        if not row:
            return [
                QERuntimeAssetLocator(
                    experiment_id=_experiment_id_from_task_loop(task_id, loop_id),
                    qe_task_id=task_id,
                    qe_loop_id=loop_id,
                    source=f"{source}.task_loop_only",
                )
            ]
        payload = dict(row)
        experiment_id = str(payload.get("experiment_id") or "").strip() or _experiment_id_from_task_loop(task_id, loop_id)
        node_id = str(payload.get("node_id") or "").strip() or self._node_id_for_locator(
            experiment_id=experiment_id,
            task_id=task_id,
            loop_id=loop_id,
            payload=payload,
        )
        return [
            QERuntimeAssetLocator(
                experiment_id=experiment_id,
                qe_task_id=task_id,
                qe_loop_id=_short_loop_id(str(payload.get("qe_loop_id") or loop_id), task_id=task_id),
                node_id=node_id or None,
                source=source,
            )
        ]

    def _node_id_for_locator(
        self,
        *,
        experiment_id: str | None,
        task_id: str | None,
        loop_id: str | None,
        payload: Mapping[str, Any] | None = None,
    ) -> str | None:
        payload = payload or {}
        for value in (
            _jsonish_mapping(payload.get("custom_params")).get("execution_node_id"),
            _jsonish_mapping(payload.get("custom_params")).get("node_id"),
            _jsonish_mapping(payload.get("result_metrics")).get("execution_node_id"),
            _jsonish_mapping(_jsonish_mapping(payload.get("result_metrics")).get("execution_trace")).get("node_id"),
        ):
            text = str(value or "").strip()
            if text:
                return text
        if task_id and loop_id:
            task_prefixed_loop_id = f"{task_id}_{loop_id}" if not loop_id.startswith(task_id) else loop_id
            loop_index = _loop_index(loop_id)
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT node_id
                        FROM qe_evolution_loops
                        WHERE task_id = %s
                          AND (
                              loop_id = %s
                              OR loop_id = %s
                              OR (%s IS NOT NULL AND loop_index = %s)
                              OR (%s IS NOT NULL AND experiment_id = %s)
                          )
                        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                        LIMIT 1
                        """,
                        (task_id, loop_id, task_prefixed_loop_id, loop_index, loop_index, experiment_id, experiment_id),
                    )
                    row = cur.fetchone()
            if row and str(row.get("node_id") or "").strip():
                return str(row["node_id"]).strip()
        return None

    def _download_node_model_params(self, locator: QERuntimeAssetLocator) -> bytes:
        async def _download() -> bytes:
            async with self._workspace_client_factory(str(locator.node_id)) as client:
                payload = await client.download_mlruns_params(str(locator.qe_task_id), str(locator.qe_loop_id))
                if not payload:
                    raise DataUnavailableError(
                        "QE node API returned an empty mlruns params archive",
                        context={
                            "reason_code": "strategy_package_model_params_missing",
                            "locator": _locator_payload(locator),
                        },
                    )
                return _params_from_mlruns_archive(payload, locator=locator)

        return _run_async_blocking(_download)

    def _download_node_workspace_file(self, locator: QERuntimeAssetLocator, *, rel_path: str) -> bytes:
        async def _download() -> bytes:
            async with self._workspace_client_factory(str(locator.node_id)) as client:
                return await client.download_workspace_file_bytes(str(locator.qe_task_id), str(locator.qe_loop_id), rel_path)

        return _run_async_blocking(_download)

    def workspace_file_bytes(self, manifest: StrategyPackageManifest, rel_path: str) -> PackageAssetBytes:
        attempts: list[dict[str, Any]] = []
        rel_path = _remote_relpath(rel_path)
        locators = self._runtime_asset_locators(manifest, attempts=attempts)
        node_locators = [locator for locator in locators if locator.qe_task_id and locator.qe_loop_id and locator.node_id]
        for locator in node_locators:
            try:
                data = self._download_node_workspace_file(locator, rel_path=rel_path)
                return PackageAssetBytes(
                    data=_ensure_non_empty_bytes(
                        data,
                        reason_code="strategy_package_workspace_file_missing",
                        context={"rel_path": rel_path, "locator": _locator_payload(locator)},
                    ),
                    source_uri=(
                        f"qe-workspace://node/{quote(locator.node_id or '', safe='')}"
                        f"/tasks/{quote(locator.qe_task_id or '', safe='')}"
                        f"/loops/{quote(locator.qe_loop_id or '', safe='')}/{quote(rel_path, safe='/')}"
                    ),
                    locator=locator,
                )
            except Exception as exc:
                attempts.append(
                    {
                        "source": "qe_node",
                        "method": "download_workspace_file_bytes",
                        "rel_path": rel_path,
                        "locator": _locator_payload(locator),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        local_roots = self._resolved_local_workspace_roots(attempts)
        for base_dir in _local_candidate_dirs(locators, local_roots):
            path = base_dir / rel_path
            try:
                if path.exists() and path.is_file():
                    return PackageAssetBytes(
                        data=_read_non_empty(path, reason_code="strategy_package_workspace_file_missing"),
                        source_uri=path.resolve(strict=False).as_uri(),
                        local_path=path,
                        source_root=base_dir,
                        locator=_locator_for_local_candidate(base_dir, locators),
                    )
            except Exception as exc:
                attempts.append(
                    {
                        "source": "wsl_workspace",
                        "method": "local_workspace_file_read",
                        "rel_path": rel_path,
                        "path": str(path),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
        raise DataUnavailableError(
            "strategy package QE workspace file is missing",
            context={
                "reason_code": "strategy_package_workspace_file_missing",
                "package_id": manifest.package_id,
                "rel_path": rel_path,
                "source": manifest.source.model_dump(mode="json"),
                "attempts": attempts,
                "attempted_sources": attempts,
            },
        )

    def conf_yaml_bytes(self, manifest: StrategyPackageManifest) -> PackageAssetBytes:
        try:
            return self.workspace_file_bytes(manifest, "conf.yaml")
        except DataUnavailableError as exc:
            raise DataUnavailableError(
                "strategy package QE conf.yaml is missing",
                context={
                    **exc.context,
                    "reason_code": "strategy_package_conf_yaml_missing",
                    "package_id": manifest.package_id,
                },
            ) from exc


class PackageAssetFreezeService:
    """Materialize MODEL_WEIGHT and FACTOR_CODE rows before package persistence."""

    def __init__(
        self,
        *,
        asset_store: PackageAssetStore | None = None,
        source: StrategyPackageAssetSource | None = None,
        model_params_reader: ModelParamsReader | None = None,
        factor_code_reader: FactorCodeReader | None = None,
        conf_yaml_reader: ConfYamlReader | None = None,
    ) -> None:
        self.asset_store = asset_store or LocalPackageAssetStore()
        self.source = source or StrategyPackageAssetSource()
        self._model_params_reader = model_params_reader
        self._factor_code_reader = factor_code_reader
        self._conf_yaml_reader = conf_yaml_reader

    def freeze_manifest_assets(self, manifest: StrategyPackageManifest) -> PackageAssetFreezeResult:
        factor_assets: list[FactorAsset] = []
        ledger: list[StrategyPackageAssetRecord] = []
        runtime_assets, schema_record = self._freeze_alpha158_schema(manifest)
        if schema_record is not None:
            ledger.append(schema_record)
        for factor in manifest.factor_set:
            frozen_factor, record = self._freeze_factor(factor, manifest)
            factor_assets.append(frozen_factor)
            ledger.append(record)

        model_input = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
        model_assets: list[ModelAsset] = []
        for model in model_input:
            frozen_model, records = self._freeze_model(model, manifest)
            model_assets.append(frozen_model)
            ledger.extend(records)

        model_value: ModelAsset | list[ModelAsset] = model_assets if isinstance(manifest.model_asset, list) else model_assets[0]
        frozen_manifest = freeze_manifest(
            manifest.model_copy(
                update={
                    "factor_set": factor_assets,
                    "model_asset": model_value,
                    "runtime_assets": runtime_assets,
                    "manifest_sha256": None,
                }
            )
        )
        return PackageAssetFreezeResult(manifest=frozen_manifest, assets=ledger)

    def _freeze_alpha158_schema(
        self,
        manifest: StrategyPackageManifest,
    ) -> tuple[RuntimeAssetManifest | None, StrategyPackageAssetRecord | None]:
        if _is_multi_alpha_parent_manifest(manifest):
            runtime_assets = manifest.runtime_assets
            alpha158 = runtime_assets.alpha158 if runtime_assets is not None else None
            if alpha158 is None or not alpha158.enabled:
                return runtime_assets, None
            data = self._read_existing_asset(
                asset_ref=alpha158.asset_ref,
                expected_sha256=alpha158.sha256,
                package_id=manifest.package_id,
                logical_name="alpha158_schema",
                asset_type=StrategyPackageAssetType.FACTOR_SCHEMA,
            )
            size_bytes = alpha158.size_bytes if alpha158.size_bytes is not None else len(data)
            frozen_alpha158 = alpha158.model_copy(update={"size_bytes": size_bytes})
            frozen_runtime_assets = runtime_assets.model_copy(update={"alpha158": frozen_alpha158})
            return frozen_runtime_assets, self._asset_record(
                manifest=manifest,
                asset_type=StrategyPackageAssetType.FACTOR_SCHEMA,
                asset_ref=alpha158.asset_ref,
                sha256=alpha158.sha256,
                size_bytes=size_bytes,
                logical_name="alpha158_schema",
                source_uri=alpha158.source_uri,
            )
        source = self._conf_yaml_bytes(manifest)
        conf = load_conf_yaml_bytes(source.data, source_uri=source.source_uri)
        payload = alpha158_schema_payload(conf)
        if payload is None:
            return RuntimeAssetManifest(alpha158=Alpha158SchemaAsset(enabled=False)), None

        data = alpha158_schema_bytes(payload)
        blob = self.asset_store.put(data, kind=StrategyPackageAssetType.FACTOR_SCHEMA.value)
        logical_name = "alpha158_schema"
        asset_ref = _logical_asset_ref(blob.uri, asset_type=StrategyPackageAssetType.FACTOR_SCHEMA, logical_name=logical_name)
        alpha158 = Alpha158SchemaAsset(
            enabled=True,
            aliases=list(payload["aliases"]),
            alias_count=int(payload["alias_count"]),
            loader_class=str(payload["loader_class"]),
            asset_ref=asset_ref,
            sha256=blob.sha256,
            size_bytes=blob.size_bytes,
            source_uri=source.source_uri,
        )
        return RuntimeAssetManifest(alpha158=alpha158), self._asset_record(
            manifest=manifest,
            asset_type=StrategyPackageAssetType.FACTOR_SCHEMA,
            asset_ref=asset_ref,
            sha256=blob.sha256,
            size_bytes=blob.size_bytes,
            logical_name=logical_name,
            source_uri=source.source_uri,
        )

    def _freeze_factor(
        self,
        factor: FactorAsset,
        manifest: StrategyPackageManifest,
    ) -> tuple[FactorAsset, StrategyPackageAssetRecord]:
        logical_name = str(factor.factor_name or factor.factor_id)
        if factor.asset_ref and factor.sha256:
            data = self._read_existing_asset(
                asset_ref=factor.asset_ref,
                expected_sha256=factor.sha256,
                package_id=manifest.package_id,
                logical_name=logical_name,
                asset_type=StrategyPackageAssetType.FACTOR_CODE,
            )
            size_bytes = factor.size_bytes if factor.size_bytes is not None else len(data)
            frozen = factor.model_copy(update={"size_bytes": size_bytes})
            return frozen, self._asset_record(
                manifest=manifest,
                asset_type=StrategyPackageAssetType.FACTOR_CODE,
                asset_ref=factor.asset_ref,
                sha256=factor.sha256,
                size_bytes=size_bytes,
                logical_name=logical_name,
                source_uri=factor.source_uri,
            )

        source = (
            self._factor_code_reader(factor, manifest)
            if self._factor_code_reader is not None
            else self.source.factor_code_bytes(factor, manifest)
        )
        blob = self.asset_store.put(source.data, kind=StrategyPackageAssetType.FACTOR_CODE.value)
        asset_ref = _logical_asset_ref(blob.uri, asset_type=StrategyPackageAssetType.FACTOR_CODE, logical_name=logical_name)
        frozen = factor.model_copy(
            update={
                "asset_ref": asset_ref,
                "sha256": blob.sha256,
                "size_bytes": blob.size_bytes,
                "source_uri": source.source_uri,
            }
        )
        return frozen, self._asset_record(
            manifest=manifest,
            asset_type=StrategyPackageAssetType.FACTOR_CODE,
            asset_ref=asset_ref,
            sha256=blob.sha256,
            size_bytes=blob.size_bytes,
            logical_name=logical_name,
            source_uri=source.source_uri,
        )

    def _freeze_model(
        self,
        model: ModelAsset,
        manifest: StrategyPackageManifest,
    ) -> tuple[ModelAsset, list[StrategyPackageAssetRecord]]:
        logical_name = str(model.model_id)
        if model.asset_ref and model.sha256:
            data = self._read_existing_asset(
                asset_ref=model.asset_ref,
                expected_sha256=model.sha256,
                package_id=manifest.package_id,
                logical_name=logical_name,
                asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
            )
            size_bytes = model.size_bytes if model.size_bytes is not None else len(data)
            frozen = model.model_copy(update={"size_bytes": size_bytes})
            weight_record = self._asset_record(
                manifest=manifest,
                asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
                asset_ref=model.asset_ref,
                sha256=model.sha256,
                size_bytes=size_bytes,
                logical_name=logical_name,
                source_uri=model.source_uri,
            )
            code_required, code_assets, code_records = self._freeze_model_code_assets(
                frozen,
                manifest,
                model_weight_data=data,
            )
            return (
                frozen.model_copy(
                    update={
                        "model_code_required": code_required,
                        "model_code_assets": code_assets,
                    }
                ),
                [weight_record, *code_records],
            )

        source = (
            self._model_params_reader(manifest)
            if self._model_params_reader is not None
            else self.source.model_params_bytes(manifest)
        )
        blob = self.asset_store.put(source.data, kind=StrategyPackageAssetType.MODEL_WEIGHT.value)
        asset_ref = _logical_asset_ref(blob.uri, asset_type=StrategyPackageAssetType.MODEL_WEIGHT, logical_name=logical_name)
        frozen = model.model_copy(
            update={
                "asset_ref": asset_ref,
                "sha256": blob.sha256,
                "size_bytes": blob.size_bytes,
                "source_uri": source.source_uri,
            }
        )
        weight_record = self._asset_record(
            manifest=manifest,
            asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
            asset_ref=asset_ref,
            sha256=blob.sha256,
            size_bytes=blob.size_bytes,
            logical_name=logical_name,
            source_uri=source.source_uri,
        )
        code_required, code_assets, code_records = self._freeze_model_code_assets(
            frozen,
            manifest,
            model_weight_data=source.data,
        )
        return (
            frozen.model_copy(
                update={
                    "model_code_required": code_required,
                    "model_code_assets": code_assets,
                }
            ),
            [weight_record, *code_records],
        )

    def _freeze_model_code_assets(
        self,
        model: ModelAsset,
        manifest: StrategyPackageManifest,
        *,
        model_weight_data: bytes,
    ) -> tuple[bool, list[ModelCodeAsset], list[StrategyPackageAssetRecord]]:
        existing = list(model.model_code_assets or [])
        conf_missing: DataUnavailableError | None = None
        conf_module_name: str | None = None
        try:
            conf_source = self._conf_yaml_bytes(manifest)
            conf = load_conf_yaml_bytes(conf_source.data, source_uri=conf_source.source_uri)
            conf_module_name = model_code_module_from_pt_uri(pt_model_uri_from_conf(conf))
        except DataUnavailableError as exc:
            conf_missing = exc

        module_names: list[str] = []
        if conf_module_name:
            module_names.append(conf_module_name)
        pickle_refs = pickled_model_code_references_from_params_bytes(model_weight_data)
        for ref in pickle_refs:
            if ref.module_name not in module_names:
                module_names.append(ref.module_name)

        if not module_names:
            if existing:
                return model.model_code_required, existing, [
                    self._record_for_existing_model_code_asset(asset, manifest=manifest) for asset in existing
                ]
            if _is_multi_alpha_parent_manifest(manifest) and not model.model_code_required:
                return False, [], []
            if conf_missing is not None:
                raise conf_missing
            return False, [], []

        existing_closure = self._model_code_assets_from_existing_closure(
            existing,
            manifest=manifest,
            module_names=module_names,
        )
        if existing_closure is not None:
            return True, existing_closure[0], existing_closure[1]

        discovered: list[dict[str, Any]] = []
        seen_sources: set[str] = set()
        for module_name in module_names:
            for source in self._discover_model_code_sources(manifest, root_module=module_name):
                if source["relative_path"] in seen_sources:
                    continue
                seen_sources.add(source["relative_path"])
                discovered.append(source)
        existing_by_path = {asset.relative_path: asset for asset in existing}
        assets: list[ModelCodeAsset] = []
        records: list[StrategyPackageAssetRecord] = []
        for source in discovered:
            existing_asset = existing_by_path.get(source["relative_path"])
            if existing_asset is not None:
                self._read_existing_asset(
                    asset_ref=existing_asset.asset_ref,
                    expected_sha256=existing_asset.sha256,
                    package_id=manifest.package_id,
                    logical_name=existing_asset.relative_path,
                    asset_type=StrategyPackageAssetType.MODEL_CODE,
                )
                assets.append(existing_asset)
                records.append(self._record_for_existing_model_code_asset(existing_asset, manifest=manifest))
                continue
            blob = self.asset_store.put(source["data"], kind=StrategyPackageAssetType.MODEL_CODE.value)
            asset_ref = _logical_asset_ref(
                blob.uri,
                asset_type=StrategyPackageAssetType.MODEL_CODE,
                logical_name=source["relative_path"],
            )
            asset = ModelCodeAsset(
                module_name=source["module_name"],
                relative_path=source["relative_path"],
                asset_ref=asset_ref,
                sha256=blob.sha256,
                size_bytes=blob.size_bytes,
                source_uri=source["source_uri"],
                required=source["required"],
            )
            assets.append(asset)
            records.append(
                self._asset_record(
                    manifest=manifest,
                    asset_type=StrategyPackageAssetType.MODEL_CODE,
                    asset_ref=asset_ref,
                    sha256=blob.sha256,
                    size_bytes=blob.size_bytes,
                    logical_name=source["relative_path"],
                    source_uri=source["source_uri"],
                )
            )
        return True, assets, records

    def _model_code_assets_from_existing_closure(
        self,
        existing: Sequence[ModelCodeAsset],
        *,
        manifest: StrategyPackageManifest,
        module_names: Sequence[str],
    ) -> tuple[list[ModelCodeAsset], list[StrategyPackageAssetRecord]] | None:
        """Reuse an already-frozen model-code closure when refreezing a parent.

        Multi-alpha parent freeze merges leg manifests that were already frozen
        from their QE workspaces. The parent manifest itself intentionally has
        no QE source coordinates, so a second parent freeze must validate the
        inherited immutable code assets instead of rediscovering them from QE.
        """

        if not existing:
            return None
        existing_by_path = {asset.relative_path: asset for asset in existing}
        ordered: list[ModelCodeAsset] = []
        records: list[StrategyPackageAssetRecord] = []
        seen: set[str] = set()
        queue: list[tuple[str, str, str]] = []
        for module_name in module_names:
            root_rel = _module_relpath(module_name)
            if root_rel not in existing_by_path:
                return None
            queue.append((root_rel, module_name, module_name))

        while queue:
            rel_path, module_name, root_module = queue.pop(0)
            if rel_path in seen:
                continue
            asset = existing_by_path.get(rel_path)
            if asset is None:
                return None
            _validate_model_code_relpath(rel_path)
            data = self._read_existing_asset(
                asset_ref=asset.asset_ref,
                expected_sha256=asset.sha256,
                package_id=manifest.package_id,
                logical_name=asset.relative_path,
                asset_type=StrategyPackageAssetType.MODEL_CODE,
            )
            seen.add(rel_path)
            ordered.append(asset)
            records.append(
                self._asset_record(
                    manifest=manifest,
                    asset_type=StrategyPackageAssetType.MODEL_CODE,
                    asset_ref=asset.asset_ref,
                    sha256=asset.sha256,
                    size_bytes=asset.size_bytes if asset.size_bytes is not None else len(data),
                    logical_name=asset.relative_path,
                    source_uri=asset.source_uri,
                )
            )
            for child_rel, child_module in _local_python_import_relpaths(
                data,
                root_module=root_module,
                source_path=rel_path,
            ):
                if child_rel not in seen:
                    queue.append((child_rel, child_module, root_module))
        return ordered, records

    def _conf_yaml_bytes(self, manifest: StrategyPackageManifest) -> PackageAssetBytes:
        return self._conf_yaml_reader(manifest) if self._conf_yaml_reader is not None else self.source.conf_yaml_bytes(manifest)

    def _discover_model_code_sources(
        self,
        manifest: StrategyPackageManifest,
        *,
        root_module: str,
    ) -> list[dict[str, Any]]:
        root_rel = _module_relpath(root_module)
        root = self._required_workspace_code_file(
            manifest,
            rel_path=root_rel,
            module_name=root_module,
            required=True,
        )
        ordered: list[dict[str, Any]] = [root]
        seen = {root_rel}
        queue = [root]
        while queue:
            current = queue.pop(0)
            for rel_path, module_name in _local_python_import_relpaths(
                current["data"],
                root_module=root_module,
                source_path=current["relative_path"],
            ):
                if rel_path in seen:
                    continue
                seen.add(rel_path)
                helper = self._required_workspace_code_file(
                    manifest,
                    rel_path=rel_path,
                    module_name=module_name,
                    required=True,
                )
                ordered.append(helper)
                queue.append(helper)
        return ordered

    def _required_workspace_code_file(
        self,
        manifest: StrategyPackageManifest,
        *,
        rel_path: str,
        module_name: str,
        required: bool,
    ) -> dict[str, Any]:
        try:
            source = self.source.workspace_file_bytes(manifest, rel_path)
        except DataUnavailableError as exc:
            raise DataUnavailableError(
                "strategy package custom model code is missing",
                context={
                    **exc.context,
                    "reason_code": "strategy_package_model_code_missing",
                    "package_id": manifest.package_id,
                    "module_name": module_name,
                    "relative_path": rel_path,
                },
            ) from exc
        _validate_model_code_relpath(rel_path)
        return {
            "module_name": module_name,
            "relative_path": rel_path,
            "data": source.data,
            "source_uri": source.source_uri,
            "required": required,
        }

    def _record_for_existing_model_code_asset(
        self,
        asset: ModelCodeAsset,
        *,
        manifest: StrategyPackageManifest,
    ) -> StrategyPackageAssetRecord:
        data = self._read_existing_asset(
            asset_ref=asset.asset_ref,
            expected_sha256=asset.sha256,
            package_id=manifest.package_id,
            logical_name=asset.relative_path,
            asset_type=StrategyPackageAssetType.MODEL_CODE,
        )
        return self._asset_record(
            manifest=manifest,
            asset_type=StrategyPackageAssetType.MODEL_CODE,
            asset_ref=asset.asset_ref,
            sha256=asset.sha256,
            size_bytes=asset.size_bytes if asset.size_bytes is not None else len(data),
            logical_name=asset.relative_path,
            source_uri=asset.source_uri,
        )

    def _read_existing_asset(
        self,
        *,
        asset_ref: str,
        expected_sha256: str,
        package_id: str,
        logical_name: str,
        asset_type: StrategyPackageAssetType,
    ) -> bytes:
        data = self.asset_store.get(asset_ref)
        actual = hashlib.sha256(data).hexdigest()
        if actual != str(expected_sha256).strip().lower():
            raise PackageAssetInvalidError(
                "strategy package asset sha256 mismatch",
                context={
                    "reason_code": "strategy_package_asset_sha_mismatch",
                    "package_id": package_id,
                    "asset_type": asset_type.value,
                    "logical_name": logical_name,
                    "asset_ref": asset_ref,
                    "expected_sha256": expected_sha256,
                    "actual_sha256": actual,
                },
            )
        return data

    def _asset_record(
        self,
        *,
        manifest: StrategyPackageManifest,
        asset_type: StrategyPackageAssetType,
        asset_ref: str,
        sha256: str,
        size_bytes: int,
        logical_name: str,
        source_uri: str | None,
    ) -> StrategyPackageAssetRecord:
        return StrategyPackageAssetRecord(
            package_id=manifest.package_id,
            asset_type=asset_type,
            asset_ref=asset_ref,
            asset_sha256=str(sha256).strip().lower(),
            asset_size_bytes=size_bytes,
            source_uri=source_uri,
            metadata={
                "schema_version": "strategy_package_asset_freeze_v1",
                "logical_name": logical_name,
                "source_type": manifest.source.source_type.value,
                "source_id": manifest.source.source_id,
                "loop_id": manifest.source.loop_id,
                "run_id": manifest.source.run_id,
            },
        )


def manifest_has_frozen_runtime_assets(manifest: StrategyPackageManifest) -> bool:
    factors = list(getattr(manifest, "factor_set", None) or [])
    if not factors or any(not factor.asset_ref or not factor.sha256 for factor in factors):
        return False
    model_asset = getattr(manifest, "model_asset", None)
    models = model_asset if isinstance(model_asset, list) else [model_asset]
    if not models or any(model is None or not model.asset_ref or not model.sha256 for model in models):
        return False
    for model in models:
        code_assets = list(model.model_code_assets or [])
        if model.model_code_required and not code_assets:
            return False
        if any(not asset.asset_ref or not asset.sha256 for asset in code_assets):
            return False
    runtime_assets = getattr(manifest, "runtime_assets", None)
    if runtime_assets is not None and runtime_assets.alpha158.enabled:
        alpha158 = runtime_assets.alpha158
        return bool(alpha158.asset_ref and alpha158.sha256 and alpha158.aliases)
    return True


def _is_multi_alpha_parent_manifest(manifest: StrategyPackageManifest) -> bool:
    return (
        getattr(getattr(manifest, "alpha_mode", None), "value", None) == "multi_alpha"
        and getattr(getattr(manifest.source, "source_type", None), "value", None) == "multi_alpha_combine_run"
    )


def _model_run_candidates(manifest: StrategyPackageManifest) -> list[str]:
    candidates: list[str] = []
    for value in (manifest.source.run_id, manifest.source.source_id):
        if str(value or "").strip():
            candidates.append(str(value).strip())
    evidence = manifest.source_evidence or {}
    experiment_id = str(evidence.get("experiment_id") or "").strip()
    if experiment_id:
        candidates.append(experiment_id)
    task_id = str(evidence.get("qe_task_id") or "").strip()
    loop_id = str(evidence.get("qe_loop_id") or manifest.source.loop_id or "").strip()
    loop_index = _loop_index(loop_id)
    if task_id and loop_index is not None:
        candidates.append(f"{task_id}_L{loop_index}")
    if task_id and loop_id:
        candidates.append(f"{task_id}_{loop_id}")
    return list(dict.fromkeys(candidates))


def _model_experiment_candidates(manifest: StrategyPackageManifest) -> list[str]:
    candidates: list[str] = []
    evidence = manifest.source_evidence or {}
    for value in (evidence.get("experiment_id"), manifest.source.source_id):
        if str(value or "").strip():
            candidates.append(str(value).strip())
    return list(dict.fromkeys(candidates))


def _loop_index(loop_id: str) -> int | None:
    text = str(loop_id or "").strip()
    if text.lower().startswith("loop") and text[4:].isdigit():
        return int(text[4:])
    marker = "_L"
    if marker in text:
        suffix = text.rsplit(marker, 1)[-1]
        if suffix.isdigit():
            return int(suffix)
    return None


def _read_non_empty(path: str | Path, *, reason_code: str) -> bytes:
    try:
        data = Path(path).read_bytes()
    except PredictionStoreError:
        raise
    except Exception as exc:
        raise DataUnavailableError(
            "strategy package source asset cannot be read",
            context={"reason_code": reason_code, "path": str(path), "error": f"{type(exc).__name__}: {exc}"},
        ) from exc
    if not data:
        raise DataUnavailableError(
            "strategy package source asset is empty",
            context={"reason_code": reason_code, "path": str(path)},
        )
    return data


def _logical_asset_ref(base_uri: str, *, asset_type: StrategyPackageAssetType, logical_name: str) -> str:
    return (
        f"{base_uri}?kind={quote(asset_type.value, safe='')}"
        f"&logical_name={quote(str(logical_name), safe='')}"
    )


def _model_asset_summary(value: ModelAsset | Sequence[ModelAsset]) -> list[Mapping[str, Any]]:
    items = value if isinstance(value, list) else [value]
    return [
        {
            "model_id": item.model_id,
            "model_type": item.model_type,
            "asset_ref": item.asset_ref,
            "sha256": item.sha256,
        }
        for item in items
    ]


def _manifest_runtime_locators(manifest: StrategyPackageManifest) -> list[QERuntimeAssetLocator]:
    evidence = manifest.source_evidence if isinstance(manifest.source_evidence, Mapping) else {}
    locators: list[QERuntimeAssetLocator] = []
    locators.extend(_locators_from_mapping(evidence, source="manifest.source_evidence"))

    component_entries = []
    if isinstance(evidence, Mapping):
        for key in ("multi_alpha_parent_leg_asset", "multi_alpha_component"):
            component = evidence.get(key)
            if isinstance(component, Mapping):
                component_entries.append((key, component))
    for evidence_key, component in component_entries:
        primary = component.get("primary_qe_source")
        if isinstance(primary, Mapping):
            locators.extend(_locators_from_mapping(primary, source=f"manifest.{evidence_key}.primary_qe_source"))
        seed_provenance = component.get("seed_provenance")
        if isinstance(seed_provenance, list):
            for index, item in enumerate(seed_provenance):
                if isinstance(item, Mapping):
                    locators.extend(_locators_from_mapping(item, source=f"manifest.{evidence_key}.seed_provenance[{index}]"))

    source = manifest.source
    source_type = source.source_type.value
    if source_type == "qe_evolution_loop":
        task_id = str(source.source_id or "").strip() or None
        locators.append(
            QERuntimeAssetLocator(
                experiment_id=_text_or_none(source.run_id) or _experiment_id_from_task_loop(task_id, source.loop_id),
                qe_task_id=task_id,
                qe_loop_id=_short_loop_id(source.loop_id, task_id=task_id),
                source="manifest.source.qe_evolution_loop",
            )
        )
    elif source_type == "qe_experiment":
        experiment_id = _text_or_none(source.run_id) or _text_or_none(source.source_id)
        locators.append(
            QERuntimeAssetLocator(
                experiment_id=experiment_id,
                qe_task_id=_text_or_none(evidence.get("qe_task_id")),
                qe_loop_id=_short_loop_id(_text_or_none(evidence.get("qe_loop_id")) or source.loop_id, task_id=evidence.get("qe_task_id")),
                source="manifest.source.qe_experiment",
            )
        )
    return _dedupe_locators(locators)


def _locators_from_mapping(payload: Mapping[str, Any], *, source: str) -> list[QERuntimeAssetLocator]:
    task_id = _first_text(
        payload,
        "qe_task_id",
        "source_task_id",
        "task_id",
    )
    loop_id = _short_loop_id(
        _first_text(payload, "qe_loop_id", "source_loop_id", "loop_id"),
        task_id=task_id,
    )
    experiment_id = _first_text(
        payload,
        "experiment_id",
        "source_experiment_id",
        "run_id",
        "source_run_id",
    )
    node_id = _first_text(payload, "node_id", "execution_node_id")
    if not any((task_id, loop_id, experiment_id, node_id)):
        return []
    return [
        QERuntimeAssetLocator(
            experiment_id=experiment_id or _experiment_id_from_task_loop(task_id, loop_id),
            qe_task_id=task_id,
            qe_loop_id=loop_id,
            node_id=node_id,
            source=source,
        )
    ]


def _first_text(payload: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = _text_or_none(payload.get(key))
        if value:
            return value
    return None


def _dedupe_locators(locators: Iterable[QERuntimeAssetLocator]) -> list[QERuntimeAssetLocator]:
    ordered: list[QERuntimeAssetLocator] = []
    seen: set[tuple[str | None, str | None, str | None, str | None]] = set()
    for locator in locators:
        normalized = QERuntimeAssetLocator(
            experiment_id=_text_or_none(locator.experiment_id),
            qe_task_id=_text_or_none(locator.qe_task_id),
            qe_loop_id=_short_loop_id(locator.qe_loop_id, task_id=locator.qe_task_id),
            node_id=_text_or_none(locator.node_id),
            source=locator.source,
        )
        if not any((normalized.experiment_id, normalized.qe_task_id, normalized.qe_loop_id, normalized.node_id)):
            continue
        key = (normalized.experiment_id, normalized.qe_task_id, normalized.qe_loop_id, normalized.node_id)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(normalized)
    return ordered


def _expand_default_node_locators(locators: Iterable[QERuntimeAssetLocator]) -> list[QERuntimeAssetLocator]:
    expanded: list[QERuntimeAssetLocator] = []
    default_node_id = _safe_default_qe_node_id()
    for locator in locators:
        expanded.append(locator)
        if locator.qe_task_id and locator.qe_loop_id and not locator.node_id and default_node_id:
            expanded.append(
                QERuntimeAssetLocator(
                    experiment_id=locator.experiment_id,
                    qe_task_id=locator.qe_task_id,
                    qe_loop_id=locator.qe_loop_id,
                    node_id=default_node_id,
                    source=f"{locator.source}.default_node",
                )
            )
    return expanded


def _safe_default_qe_node_id() -> str | None:
    try:
        return _text_or_none(resolve_default_qe_node_id())
    except Exception:
        return None


def _local_workspace_roots(explicit_roots: Sequence[str | Path] | None) -> list[Path]:
    raw: list[str | Path] = []
    raw.extend(explicit_roots or [])
    for env_key in ("AISTOCK_QE_WORKSPACE_ROOTS", "QE_WORKSPACE_ROOTS"):
        value = os.getenv(env_key)
        if value:
            raw.extend(part for part in re.split(r"[;]", value) if part.strip())
    for env_key in ("QE_WORKSPACE_WIN", "QE_EXPERIMENTS_ROOT", "AISTOCK_QE_EXPERIMENTS_ROOT"):
        value = os.getenv(env_key)
        if value:
            raw.append(value)
    rdagent_root = os.getenv("QLIB_RDAGENT_ROOT_WIN")
    if rdagent_root:
        raw.append(Path(rdagent_root) / "qe_workspace")

    roots: list[Path] = []
    for item in raw:
        try:
            path = Path(item).expanduser()
        except Exception:
            continue
        roots.append(path)
    return _dedupe_paths(roots)


def _dedupe_paths(raw: Iterable[str | Path]) -> list[Path]:
    roots: list[Path] = []
    seen: set[str] = set()
    for item in raw:
        try:
            path = Path(item).expanduser()
        except Exception:
            continue
        key = str(path.resolve(strict=False)).lower()
        if key in seen:
            continue
        seen.add(key)
        roots.append(path)
    return roots


def _windows_path_from_wsl_mount(path_text: str) -> Path | None:
    text = str(path_text or "").strip().replace("\\", "/")
    match = re.match(r"^/mnt/(?P<drive>[a-zA-Z])/(?P<rest>.*)$", text)
    if not match:
        return None
    drive = match.group("drive").upper()
    rest = match.group("rest").replace("/", "\\")
    return Path(f"{drive}:\\{rest}")


def _local_model_param_candidates(
    locators: Sequence[QERuntimeAssetLocator],
    roots: Sequence[Path],
) -> Iterable[Path]:
    for base_dir in _local_candidate_dirs(locators, roots):
        if not base_dir.exists() or not base_dir.is_dir():
            continue
        direct = base_dir / "mlruns"
        search_root = direct if direct.exists() and direct.is_dir() else base_dir
        matches = [item for item in search_root.glob("**/artifacts/params.pkl") if item.is_file()]
        matches.sort(key=lambda item: (item.stat().st_mtime, str(item).lower()), reverse=True)
        yield from matches


def _local_factor_code_candidates(
    factor_name: str,
    locators: Sequence[QERuntimeAssetLocator],
    roots: Sequence[Path],
) -> Iterable[Path]:
    safe_name = _safe_factor_file_name(factor_name)
    for base_dir in _local_candidate_dirs(locators, roots):
        for rel in (Path("factors") / f"{safe_name}.py", Path(f"{safe_name}.py")):
            path = base_dir / rel
            if path.exists() and path.is_file():
                yield path


def _local_candidate_dirs(
    locators: Sequence[QERuntimeAssetLocator],
    roots: Sequence[Path],
) -> Iterable[Path]:
    seen: set[str] = set()
    for root in roots:
        for locator in locators:
            task_id = _text_or_none(locator.qe_task_id)
            loop_id = _short_loop_id(locator.qe_loop_id, task_id=task_id)
            experiment_id = _text_or_none(locator.experiment_id) or _experiment_id_from_task_loop(task_id, loop_id)
            candidates: list[Path] = []
            if task_id and loop_id:
                candidates.extend(
                    [
                        root / task_id / loop_id,
                        root / task_id / f"{task_id}_{loop_id}",
                    ]
                )
            if experiment_id:
                candidates.append(root / experiment_id)
            for candidate in candidates:
                key = str(candidate.resolve(strict=False)).lower()
                if key in seen:
                    continue
                seen.add(key)
                yield candidate


def _locator_for_local_candidate(
    base_dir: Path,
    locators: Sequence[QERuntimeAssetLocator],
) -> QERuntimeAssetLocator | None:
    base_name = base_dir.name.lower()
    for locator in locators:
        task_id = _text_or_none(locator.qe_task_id)
        loop_id = _short_loop_id(locator.qe_loop_id, task_id=task_id)
        experiment_id = _text_or_none(locator.experiment_id) or _experiment_id_from_task_loop(task_id, loop_id)
        keys = {str(loop_id or "").lower(), str(experiment_id or "").lower()}
        if task_id and loop_id:
            keys.add(f"{task_id}_{loop_id}".lower())
        if base_name in keys:
            return locator
    return locators[0] if locators else None


def _locator_payload(locator: QERuntimeAssetLocator) -> dict[str, Any]:
    return {
        "experiment_id": locator.experiment_id,
        "qe_task_id": locator.qe_task_id,
        "qe_loop_id": locator.qe_loop_id,
        "node_id": locator.node_id,
        "source": locator.source,
    }


def _short_loop_id(loop_id: str | None, *, task_id: str | None = None) -> str | None:
    text = str(loop_id or "").strip()
    if not text:
        return None
    task = str(task_id or "").strip()
    if task and text.startswith(f"{task}_"):
        text = text[len(task) + 1 :]
    match = re.search(r"(?:^|_)L(?P<index>\d+)$", text)
    if match:
        return f"Loop{int(match.group('index'))}"
    if text.lower().startswith("loop") and text[4:].isdigit():
        return f"Loop{int(text[4:])}"
    return text


def _experiment_id_from_task_loop(task_id: str | None, loop_id: str | None) -> str | None:
    task = str(task_id or "").strip()
    short_loop = _short_loop_id(loop_id, task_id=task)
    index = _loop_index(short_loop or "")
    if task and index is not None:
        return f"{task}_L{index}"
    if task and short_loop:
        return f"{task}_{short_loop}"
    return None


def _jsonish_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, Mapping) else {}
    return {}


def _params_from_mlruns_archive(payload: bytes, *, locator: QERuntimeAssetLocator) -> bytes:
    data = _ensure_non_empty_bytes(
        payload,
        reason_code="strategy_package_model_params_missing",
        context={"locator": _locator_payload(locator), "phase": "mlruns_archive_download"},
    )
    try:
        archive = tarfile.open(fileobj=io.BytesIO(data), mode="r:gz")
    except tarfile.TarError as exc:
        raise ArtifactGenerationFailedError(
            "QE mlruns params archive is not a readable tar.gz",
            context={"reason_code": "strategy_package_mlruns_archive_invalid", "locator": _locator_payload(locator)},
        ) from exc
    with archive:
        matches: list[tarfile.TarInfo] = []
        for member in archive.getmembers():
            _validate_tar_member(member, locator=locator)
            normalized = member.name.replace("\\", "/").lstrip("./")
            if member.isfile() and normalized.endswith("/artifacts/params.pkl"):
                matches.append(member)
        if not matches:
            raise DataUnavailableError(
                "QE mlruns params archive does not contain artifacts/params.pkl",
                context={"reason_code": "strategy_package_model_params_missing", "locator": _locator_payload(locator)},
            )
        matches.sort(key=lambda item: (item.mtime or 0, item.name), reverse=True)
        handle = archive.extractfile(matches[0])
        if handle is None:
            raise DataUnavailableError(
                "QE mlruns params archive params.pkl member cannot be read",
                context={
                    "reason_code": "strategy_package_model_params_missing",
                    "locator": _locator_payload(locator),
                    "member": matches[0].name,
                },
            )
        return _ensure_non_empty_bytes(
            handle.read(),
            reason_code="strategy_package_model_params_missing",
            context={"locator": _locator_payload(locator), "member": matches[0].name},
        )


_THIRD_PARTY_MODULE_PREFIXES = {
    "catboost",
    "lightgbm",
    "numpy",
    "pandas",
    "qlib",
    "sklearn",
    "torch",
    "xgboost",
}
_STDLIB_MODULE_NAMES = set(getattr(sys, "stdlib_module_names", ()))
_LOCAL_PICKLED_MODEL_MODULES = frozenset({"model"})


@dataclass(frozen=True, order=True)
class PickledModelCodeReference:
    module_name: str
    class_name: str | None = None

    @property
    def qualified_name(self) -> str:
        return f"{self.module_name}.{self.class_name}" if self.class_name else self.module_name


def pickled_model_code_references_from_params_bytes(
    data: bytes,
    module_names: Iterable[str] | None = None,
) -> list[PickledModelCodeReference]:
    """Return local model-code references embedded in pickle/torch params bytes."""

    modules = {str(item).strip() for item in (module_names or _LOCAL_PICKLED_MODEL_MODULES) if str(item).strip()}
    if not data or not modules:
        return []
    found: dict[tuple[str, str | None], PickledModelCodeReference] = {}
    for payload in _pickle_payloads_from_params_bytes(data):
        for ref in _pickle_payload_model_refs(payload, modules):
            found[(ref.module_name, ref.class_name)] = ref
    return sorted(found.values(), key=lambda item: (item.module_name, item.class_name or ""))


def _pickle_payloads_from_params_bytes(data: bytes) -> list[bytes]:
    payloads = [bytes(data)]
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for name in archive.namelist():
                if name.endswith(".pkl"):
                    payloads.append(archive.read(name))
    except zipfile.BadZipFile:
        pass
    return payloads


def _pickle_arg_text(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        text = text[1:-1]
    return text


def _pickle_payload_model_refs(
    payload: bytes,
    module_names: set[str],
) -> list[PickledModelCodeReference]:
    import pickletools

    found: dict[tuple[str, str | None], PickledModelCodeReference] = {}
    memo: dict[int, Any] = {}
    stack: list[Any] = []
    mark = object()
    try:
        for opcode, arg, _pos in pickletools.genops(payload):
            name = opcode.name
            if name in {"STRING", "BINSTRING", "SHORT_BINSTRING", "UNICODE", "BINUNICODE", "SHORT_BINUNICODE"}:
                stack.append(_pickle_arg_text(arg))
                continue
            if name == "GLOBAL":
                parts = _pickle_arg_text(arg).split(" ", 1)
                module = parts[0] if parts else ""
                class_name = parts[1] if len(parts) > 1 and parts[1] else None
                _record_pickle_ref(found, module, class_name, module_names)
                stack.append(f"{module}.{class_name}" if class_name else module)
                continue
            if name == "STACK_GLOBAL":
                class_name = stack.pop() if stack else None
                module = stack.pop() if stack else None
                module_text = module if isinstance(module, str) else ""
                class_text = class_name if isinstance(class_name, str) else None
                _record_pickle_ref(found, module_text, class_text, module_names)
                stack.append(f"{module_text}.{class_text}" if class_text else module_text)
                continue
            if name == "MEMOIZE":
                if stack:
                    memo[len(memo)] = stack[-1]
                continue
            if name in {"BINPUT", "LONG_BINPUT", "PUT"}:
                if stack:
                    memo[int(arg)] = stack[-1]
                continue
            if name in {"BINGET", "LONG_BINGET", "GET"}:
                stack.append(memo.get(int(arg)))
                continue
            if name == "MARK":
                stack.append(mark)
                continue
            if name == "POP":
                if stack:
                    stack.pop()
                continue
            if name == "POP_MARK":
                while stack and stack[-1] is not mark:
                    stack.pop()
                if stack and stack[-1] is mark:
                    stack.pop()
                continue
        return sorted(found.values(), key=lambda item: (item.module_name, item.class_name or ""))
    except Exception:
        return _fallback_pickle_payload_model_refs(payload, module_names)


def _record_pickle_ref(
    found: dict[tuple[str, str | None], PickledModelCodeReference],
    module: str,
    class_name: str | None,
    module_names: set[str],
) -> None:
    module_text = str(module or "").strip()
    if module_text not in module_names:
        return
    class_text = str(class_name or "").strip() or None
    found[(module_text, class_text)] = PickledModelCodeReference(module_text, class_text)


def _fallback_pickle_payload_model_refs(
    payload: bytes,
    module_names: set[str],
) -> list[PickledModelCodeReference]:
    found: dict[tuple[str, str | None], PickledModelCodeReference] = {}
    for module in module_names:
        encoded = module.encode("utf-8")
        if (
            b"c" + encoded + b"\n" in payload
            or bytes([0x8C, len(encoded)]) + encoded in payload
            or b"X" + len(encoded).to_bytes(4, "little") + encoded in payload
            or b"U" + bytes([len(encoded)]) + encoded in payload
        ):
            _record_pickle_ref(found, module, None, module_names)
    return sorted(found.values(), key=lambda item: (item.module_name, item.class_name or ""))


def _module_relpath(module_name: str) -> str:
    parts = [part for part in str(module_name or "").strip().split(".") if part]
    if not parts:
        raise StrategyPackageValidationError(
            "custom model module name is required",
            context={"reason_code": "strategy_package_model_code_module_invalid", "module_name": module_name},
        )
    return str(PurePosixPath(*parts).with_suffix(".py"))


def _validate_model_code_relpath(rel_path: str) -> None:
    pure = PurePosixPath(str(rel_path or "").replace("\\", "/"))
    if (
        not str(pure)
        or pure.is_absolute()
        or any(part in {"", ".", ".."} for part in pure.parts)
        or pure.suffix != ".py"
    ):
        raise StrategyPackageValidationError(
            "model code asset path must be a safe relative Python file path",
            context={"reason_code": "strategy_package_model_code_path_invalid", "relative_path": rel_path},
        )


def _local_python_import_relpaths(
    payload: bytes,
    *,
    root_module: str,
    source_path: str,
) -> list[tuple[str, str]]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise StrategyPackageValidationError(
            "model code asset is not UTF-8 Python source",
            context={
                "reason_code": "strategy_package_model_code_parse_failed",
                "module_name": root_module,
                "relative_path": source_path,
                "error": str(exc),
            },
        ) from exc
    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        raise StrategyPackageValidationError(
            "model code asset cannot be parsed for import closure",
            context={
                "reason_code": "strategy_package_model_code_parse_failed",
                "module_name": root_module,
                "relative_path": source_path,
                "line": exc.lineno,
                "offset": exc.offset,
                "error": str(exc),
            },
        ) from exc
    root_package = root_module.split(".", 1)[0]
    found: list[tuple[str, str]] = []
    seen: set[str] = set()
    for node in ast.walk(tree):
        modules: list[str] = []
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.module:
                modules.append(f"{root_package}.{node.module}")
            elif node.module:
                modules.append(node.module)
        for module in modules:
            normalized = str(module or "").strip()
            if not normalized:
                continue
            prefix = normalized.split(".", 1)[0]
            if prefix == root_package and normalized == root_module:
                continue
            if prefix in _THIRD_PARTY_MODULE_PREFIXES or prefix in _STDLIB_MODULE_NAMES:
                continue
            rel_path = _module_relpath(normalized)
            if rel_path in seen:
                continue
            seen.add(rel_path)
            found.append((rel_path, normalized))
    return found


def _validate_tar_member(member: tarfile.TarInfo, *, locator: QERuntimeAssetLocator) -> None:
    name = member.name.replace("\\", "/")
    if member.issym() or member.islnk():
        raise ArtifactGenerationFailedError(
            "QE mlruns params archive must not contain links",
            context={
                "reason_code": "strategy_package_mlruns_archive_unsafe",
                "locator": _locator_payload(locator),
                "member": member.name,
            },
        )
    if ":" in name:
        raise ArtifactGenerationFailedError(
            "QE mlruns params archive contains a drive-qualified path",
            context={
                "reason_code": "strategy_package_mlruns_archive_unsafe",
                "locator": _locator_payload(locator),
                "member": member.name,
            },
        )
    pure = PurePosixPath(name)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        raise ArtifactGenerationFailedError(
            "QE mlruns params archive contains an unsafe path",
            context={
                "reason_code": "strategy_package_mlruns_archive_unsafe",
                "locator": _locator_payload(locator),
                "member": member.name,
            },
        )


def _run_async_blocking(factory: Callable[[], Any]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())

    result: dict[str, Any] = {}

    def _target() -> None:
        try:
            result["value"] = asyncio.run(factory())
        except BaseException as exc:  # pragma: no cover - re-raised below.
            result["error"] = exc

    thread = Thread(target=_target, daemon=True)
    thread.start()
    thread.join()
    if "error" in result:
        raise result["error"]
    return result.get("value")


def _remote_relpath(value: str) -> str:
    text = str(value or "").strip().replace("\\", "/")
    if not text:
        raise RuntimeConfigInvalidError("remote workspace file path is empty")
    if ":" in text:
        raise RuntimeConfigInvalidError(
            "absolute or drive-qualified QE workspace paths are not allowed",
            context={"path": value},
        )
    pure = PurePosixPath(text)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        raise RuntimeConfigInvalidError(
            "QE workspace file path must be a safe relative path",
            context={"path": value},
        )
    return str(pure)


def _ensure_non_empty_bytes(
    data: bytes | bytearray | memoryview | None,
    *,
    reason_code: str,
    context: Mapping[str, Any] | None = None,
) -> bytes:
    payload = bytes(data or b"")
    if not payload:
        raise DataUnavailableError(
            "strategy package source asset is empty",
            context={"reason_code": reason_code, **dict(context or {})},
        )
    return payload


def _safe_factor_file_name(factor_name: str) -> str:
    text = str(factor_name or "").strip()
    if not text or text in {".", ".."} or any(sep in text for sep in ("/", "\\", ":")):
        raise RuntimeConfigInvalidError(
            "strategy package factor_name must be a safe file stem for QE workspace lookup",
            context={"factor_name": factor_name},
        )
    return text


def _text_or_none(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None
