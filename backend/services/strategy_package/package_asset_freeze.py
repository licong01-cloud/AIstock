"""Freeze StrategyPackage runtime assets into package-owned storage."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import quote

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.model_store import ModelStoreService, PredictionArtifactStore
from backend.services.model_store.artifact_store import PredictionStoreError
from backend.services.trading_core.errors import (
    DataUnavailableError,
    PackageAssetInvalidError,
    StrategyPackageValidationError,
)

from .manifest import freeze_manifest
from .models import FactorAsset, ModelAsset, StrategyPackageManifest
from .package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from .package_asset_store import LocalPackageAssetStore, PackageAssetStore


@dataclass(frozen=True)
class PackageAssetBytes:
    data: bytes
    source_uri: str | None = None


@dataclass(frozen=True)
class PackageAssetFreezeResult:
    manifest: StrategyPackageManifest
    assets: list[StrategyPackageAssetRecord]


ModelParamsReader = Callable[[StrategyPackageManifest], PackageAssetBytes]
FactorCodeReader = Callable[[FactorAsset, StrategyPackageManifest], PackageAssetBytes]


class StrategyPackageAssetSource:
    """Resolve source bytes for a package freeze without touching QE workspaces."""

    def __init__(
        self,
        *,
        model_store: ModelStoreService | Any | None = None,
        artifact_store: PredictionArtifactStore | None = None,
        conn_factory: Any = get_conn,
    ) -> None:
        self.model_store = model_store or ModelStoreService()
        self.artifact_store = artifact_store or getattr(self.model_store, "artifact_store", None) or PredictionArtifactStore()
        self._conn_factory = conn_factory

    def model_params_bytes(self, manifest: StrategyPackageManifest) -> PackageAssetBytes:
        attempts: list[dict[str, Any]] = []
        for experiment_id in _model_experiment_candidates(manifest):
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
                        "method": "experiment_id_pointer",
                        "experiment_id": experiment_id,
                        "pointer_status": pointer.get("pointer_status"),
                        "error": "mlflow_artifact_uri missing",
                    }
                )
            except Exception as exc:
                attempts.append(
                    {
                        "method": "experiment_id_pointer",
                        "experiment_id": experiment_id,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        for run_id in _model_run_candidates(manifest):
            try:
                path = self.model_store.pull_params_path(run_id=run_id)
                return PackageAssetBytes(
                    data=_read_non_empty(path, reason_code="strategy_package_model_params_missing"),
                    source_uri=f"aistock-prediction-store://runs/{quote(run_id, safe='')}/model_params",
                )
            except Exception as exc:
                attempts.append({"method": "run_id_pointer", "run_id": run_id, "error": f"{type(exc).__name__}: {exc}"})

        raise DataUnavailableError(
            "strategy package model params.pkl is missing",
            context={
                "reason_code": "strategy_package_model_params_missing",
                "package_id": manifest.package_id,
                "model_asset": _model_asset_summary(manifest.model_asset),
                "source": manifest.source.model_dump(mode="json"),
                "attempts": attempts,
            },
        )

    def factor_code_bytes(self, factor: FactorAsset, manifest: StrategyPackageManifest) -> PackageAssetBytes:
        factor_name = str(factor.factor_name or factor.factor_id or "").strip()
        if not factor_name:
            raise StrategyPackageValidationError(
                "strategy package factor name is required for asset freeze",
                context={"reason_code": "strategy_package_factor_code_missing", "package_id": manifest.package_id},
            )
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
            raise DataUnavailableError(
                "strategy package factor code lookup failed",
                context={
                    "reason_code": "strategy_package_factor_code_missing",
                    "package_id": manifest.package_id,
                    "factor_name": factor_name,
                    "error": f"{type(exc).__name__}: {exc}",
                },
            ) from exc
        if not rows:
            raise DataUnavailableError(
                "strategy package factor code is missing",
                context={
                    "reason_code": "strategy_package_factor_code_missing",
                    "package_id": manifest.package_id,
                    "factor_name": factor_name,
                    "source": manifest.source.model_dump(mode="json"),
                },
            )
        by_hash: dict[str, dict[str, Any]] = {}
        for row in rows:
            code = str(row.get("code_text") or "")
            digest = hashlib.sha256(code.encode("utf-8")).hexdigest()
            by_hash.setdefault(digest, row)
        if len(by_hash) > 1:
            raise StrategyPackageValidationError(
                "strategy package factor code is ambiguous",
                context={
                    "reason_code": "strategy_package_factor_code_ambiguous",
                    "package_id": manifest.package_id,
                    "factor_name": factor_name,
                    "candidate_count": len(rows),
                    "distinct_code_sha256": sorted(by_hash),
                    "candidate_sources": [
                        {"id": row.get("id"), "source": row.get("source"), "is_available": row.get("is_available")}
                        for row in rows
                    ],
                },
            )
        row = next(iter(by_hash.values()))
        return PackageAssetBytes(
            data=str(row.get("code_text") or "").encode("utf-8"),
            source_uri=f"aistock_factor_catalog:{row.get('id')}:code_text",
        )


class PackageAssetFreezeService:
    """Materialize MODEL_WEIGHT and FACTOR_CODE rows before package persistence."""

    def __init__(
        self,
        *,
        asset_store: PackageAssetStore | None = None,
        source: StrategyPackageAssetSource | None = None,
        model_params_reader: ModelParamsReader | None = None,
        factor_code_reader: FactorCodeReader | None = None,
    ) -> None:
        self.asset_store = asset_store or LocalPackageAssetStore()
        self.source = source or StrategyPackageAssetSource()
        self._model_params_reader = model_params_reader
        self._factor_code_reader = factor_code_reader

    def freeze_manifest_assets(self, manifest: StrategyPackageManifest) -> PackageAssetFreezeResult:
        factor_assets: list[FactorAsset] = []
        ledger: list[StrategyPackageAssetRecord] = []
        for factor in manifest.factor_set:
            frozen_factor, record = self._freeze_factor(factor, manifest)
            factor_assets.append(frozen_factor)
            ledger.append(record)

        model_input = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
        model_assets: list[ModelAsset] = []
        for model in model_input:
            frozen_model, record = self._freeze_model(model, manifest)
            model_assets.append(frozen_model)
            ledger.append(record)

        model_value: ModelAsset | list[ModelAsset] = model_assets if isinstance(manifest.model_asset, list) else model_assets[0]
        frozen_manifest = freeze_manifest(
            manifest.model_copy(
                update={
                    "factor_set": factor_assets,
                    "model_asset": model_value,
                    "manifest_sha256": None,
                }
            )
        )
        return PackageAssetFreezeResult(manifest=frozen_manifest, assets=ledger)

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
    ) -> tuple[ModelAsset, StrategyPackageAssetRecord]:
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
            return frozen, self._asset_record(
                manifest=manifest,
                asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
                asset_ref=model.asset_ref,
                sha256=model.sha256,
                size_bytes=size_bytes,
                logical_name=logical_name,
                source_uri=model.source_uri,
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
        return frozen, self._asset_record(
            manifest=manifest,
            asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
            asset_ref=asset_ref,
            sha256=blob.sha256,
            size_bytes=blob.size_bytes,
            logical_name=logical_name,
            source_uri=source.source_uri,
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
    return bool(models) and all(model is not None and model.asset_ref and model.sha256 for model in models)


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
