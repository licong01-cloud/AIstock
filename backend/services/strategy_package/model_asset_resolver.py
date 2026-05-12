"""Runtime asset resolver for Strategy Package manifests.

The resolver may copy external model assets into an AIstock-owned cache, but it
never moves or mutates the original QE assets. Missing runtime assets are
reported as explicit data errors so paper trading cannot start with a hidden
fallback.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from backend.services.trading_core.execution_algo_capabilities import required_runtime_asset_keys
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError

from .manifest import freeze_manifest
from .models import StrategyPackageManifest
from .workspace_policy import ensure_not_forbidden_worker_workspace_path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MODEL_CACHE_ROOT = PROJECT_ROOT / "rdagent_assets" / "model_cache" / "execution"

AssetNamespaceLookup = Mapping[str, str | None] | Callable[[str], str | None]


@dataclass(frozen=True)
class ResolvedModelAsset:
    """Resolved model file information."""

    algo_code: str
    config_key: str
    original_path: str
    resolved_path: Path
    copied: bool


class ModelAssetResolver:
    """Resolve execution model files into AIstock-accessible local paths."""

    def __init__(
        self,
        cache_root: Path | str | None = None,
        *,
        asset_namespace_lookup: AssetNamespaceLookup | None = None,
    ) -> None:
        root = cache_root or os.getenv("AISTOCK_MODEL_CACHE_DIR") or DEFAULT_MODEL_CACHE_ROOT
        self.cache_root = Path(root)
        self._asset_namespace_lookup = asset_namespace_lookup
        ensure_not_forbidden_worker_workspace_path(self.cache_root, purpose="StrategyPackage model asset cache root")

    def resolve_manifest_assets(
        self,
        manifest: StrategyPackageManifest,
        *,
        copy_missing: bool = True,
    ) -> StrategyPackageManifest:
        """Return a manifest whose execution assets are accessible locally.

        The returned manifest is re-frozen because any rewritten runtime asset
        path must be covered by the manifest hash.
        """

        algo_code = manifest.minute_execution_policy.algo_code.upper()
        asset_keys = required_runtime_asset_keys(algo_code)
        if not asset_keys:
            return manifest

        config = dict(manifest.minute_execution_policy.algo_config)
        cache_status: dict[str, str] = {}
        original_paths: dict[str, str] = {}
        for key in asset_keys:
            resolved = self.resolve_runtime_asset(
                manifest=manifest,
                config_key=key,
                copy_missing=copy_missing,
            )
            original_key = f"original_{key}"
            original_path = str(config.get(original_key) or resolved.original_path)
            config[original_key] = original_path
            config[key] = str(resolved.resolved_path)
            cache_status[key] = "copied" if resolved.copied else "local"
            original_paths[key] = original_path

        if len(asset_keys) == 1 and asset_keys[0] == "model_path":
            config["original_model_path"] = original_paths["model_path"]
            config["model_asset_cache_status"] = cache_status["model_path"]
        config["runtime_asset_cache_status"] = cache_status

        updated_policy = manifest.minute_execution_policy.model_copy(
            update={"algo_config": config}
        )
        updated = manifest.model_copy(
            update={"minute_execution_policy": updated_policy, "manifest_sha256": None}
        )
        return freeze_manifest(updated)

    def resolve_v24_plan_model(
        self,
        manifest: StrategyPackageManifest,
        *,
        copy_missing: bool = True,
    ) -> ResolvedModelAsset:
        return self.resolve_runtime_asset(
            manifest=manifest,
            config_key="model_path",
            copy_missing=copy_missing,
        )

    def resolve_runtime_asset(
        self,
        *,
        manifest: StrategyPackageManifest,
        config_key: str,
        copy_missing: bool = True,
    ) -> ResolvedModelAsset:
        algo_code = manifest.minute_execution_policy.algo_code.upper()
        config = manifest.minute_execution_policy.algo_config
        asset_namespace = self._asset_namespace_for(algo_code, config)
        original_path = str(
            config.get(f"original_{config_key}")
            or (config.get("original_model_path") if config_key == "model_path" else None)
            or config.get(config_key)
            or ""
        ).strip()
        if not original_path:
            raise DataUnavailableError(
                f"{algo_code} requires {config_key} before asset resolution",
                context={
                    "package_id": manifest.package_id,
                    "algo_code": algo_code,
                    "config_key": config_key,
                },
            )
        self._ensure_not_worker_path(
            original_path,
            package_id=manifest.package_id,
            algo_code=algo_code,
            config_key=config_key,
        )

        local_path = Path(original_path)
        if self._is_existing_file(local_path):
            self._ensure_positive_file(local_path, original_path, manifest.package_id)
            return ResolvedModelAsset(
                algo_code=algo_code,
                config_key=config_key,
                original_path=original_path,
                resolved_path=local_path,
                copied=False,
            )

        destination = self._cache_destination(asset_namespace, original_path)
        if self._is_existing_file(destination):
            self._validate_cached_asset(
                destination,
                original_path,
                manifest.package_id,
                algo_code=algo_code,
                asset_namespace=asset_namespace,
                config_key=config_key,
            )
            return ResolvedModelAsset(
                algo_code=algo_code,
                config_key=config_key,
                original_path=original_path,
                resolved_path=destination,
                copied=True,
            )

        if not copy_missing:
            raise DataUnavailableError(
                f"{algo_code} {config_key} is not accessible and cache copy is disabled",
                context={
                    "package_id": manifest.package_id,
                    "algo_code": algo_code,
                    "asset_namespace": asset_namespace,
                    "config_key": config_key,
                    "asset_path": original_path,
                },
            )

        source = self._find_existing_source(original_path, algo_code=asset_namespace)
        if source is None:
            raise DataUnavailableError(
                f"{algo_code} {config_key} is not accessible from AIstock backend",
                context={
                    "package_id": manifest.package_id,
                    "algo_code": algo_code,
                    "asset_namespace": asset_namespace,
                    "config_key": config_key,
                    "asset_path": original_path,
                    "cache_path": str(destination),
                    "attempted_paths": [
                        str(path) for path in self._candidate_paths(original_path, algo_code=asset_namespace)
                    ],
                },
            )

        copied_path = self._copy_to_cache(
            source=source,
            destination=destination,
            original_path=original_path,
            package_id=manifest.package_id,
            algo_code=algo_code,
            asset_namespace=asset_namespace,
            config_key=config_key,
        )
        return ResolvedModelAsset(
            algo_code=algo_code,
            config_key=config_key,
            original_path=original_path,
            resolved_path=copied_path,
            copied=True,
        )

    def _copy_to_cache(
        self,
        *,
        source: Path,
        destination: Path,
        original_path: str,
        package_id: str,
        algo_code: str,
        asset_namespace: str,
        config_key: str,
    ) -> Path:
        self._ensure_positive_file(source, original_path, package_id)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        self._ensure_positive_file(destination, original_path, package_id)
        self._write_sidecar(
            destination,
            {
                "algo_code": asset_namespace,
                "requested_algo_code": algo_code,
                "asset_namespace": asset_namespace,
                "config_key": config_key,
                "original_path": original_path,
                "resolved_source_path": str(source),
                "cached_path": str(destination),
                "source_size": source.stat().st_size,
                "cached_size": destination.stat().st_size,
                "copied_at": datetime.now(UTC).isoformat(),
            },
        )
        return destination

    def _validate_cached_asset(
        self,
        destination: Path,
        original_path: str,
        package_id: str,
        *,
        algo_code: str,
        asset_namespace: str,
        config_key: str,
    ) -> None:
        self._ensure_positive_file(destination, original_path, package_id)
        sidecar = self._sidecar_path(destination)
        if not sidecar.exists():
            raise DataUnavailableError(
                "cached execution model is missing sidecar metadata",
                context={
                    "package_id": package_id,
                    "algo_code": algo_code,
                    "asset_namespace": asset_namespace,
                    "config_key": config_key,
                    "asset_path": original_path,
                    "cache_path": str(destination),
                    "sidecar_path": str(sidecar),
                },
            )
        try:
            metadata = json.loads(sidecar.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise DataUnavailableError(
                "cached execution model sidecar metadata is invalid",
                context={
                    "package_id": package_id,
                    "algo_code": algo_code,
                    "asset_namespace": asset_namespace,
                    "config_key": config_key,
                    "asset_path": original_path,
                    "sidecar_path": str(sidecar),
                },
            ) from exc
        if metadata.get("original_path") != original_path:
            raise DataUnavailableError(
                "cached execution model sidecar does not match original path",
                context={
                    "package_id": package_id,
                    "algo_code": algo_code,
                    "asset_namespace": asset_namespace,
                    "config_key": config_key,
                    "asset_path": original_path,
                    "cache_path": str(destination),
                    "sidecar_original_path": metadata.get("original_path"),
                },
            )
        sidecar_algo_code = metadata.get("algo_code")
        sidecar_namespace = metadata.get("asset_namespace") or sidecar_algo_code
        if sidecar_namespace != asset_namespace:
            raise DataUnavailableError(
                "cached execution model sidecar does not match asset namespace",
                context={
                    "package_id": package_id,
                    "algo_code": algo_code,
                    "asset_namespace": asset_namespace,
                    "config_key": config_key,
                    "asset_path": original_path,
                    "cache_path": str(destination),
                    "sidecar_algo_code": sidecar_algo_code,
                    "sidecar_asset_namespace": metadata.get("asset_namespace"),
                },
            )
        cached_size = metadata.get("cached_size")
        if cached_size is not None and int(cached_size) != destination.stat().st_size:
            raise DataUnavailableError(
                "cached execution model size does not match sidecar metadata",
                context={
                    "package_id": package_id,
                    "algo_code": algo_code,
                    "asset_namespace": asset_namespace,
                    "config_key": config_key,
                    "asset_path": original_path,
                    "cache_path": str(destination),
                    "sidecar_cached_size": cached_size,
                    "actual_cached_size": destination.stat().st_size,
                },
            )

    def _ensure_positive_file(self, path: Path, original_path: str, package_id: str) -> None:
        if not path.exists() or not path.is_file():
            raise DataUnavailableError(
                "execution model asset is not a file",
                context={
                    "package_id": package_id,
                    "asset_path": original_path,
                    "resolved_path": str(path),
                },
            )
        if path.stat().st_size <= 0:
            raise DataUnavailableError(
                "execution model asset is empty",
                context={
                    "package_id": package_id,
                    "asset_path": original_path,
                    "resolved_path": str(path),
                },
            )

    def _find_existing_source(self, original_path: str, *, algo_code: str) -> Path | None:
        for candidate in self._candidate_paths(original_path, algo_code=algo_code):
            if self._is_existing_file(candidate):
                return candidate
        return None

    @staticmethod
    def _is_existing_file(path: Path) -> bool:
        try:
            return path.exists() and path.is_file()
        except OSError:
            return False

    def _candidate_paths(self, original_path: str, *, algo_code: str | None = None) -> list[Path]:
        ensure_not_forbidden_worker_workspace_path(original_path, purpose="StrategyPackage model asset source path")
        candidates = [Path(original_path)]
        filename = Path(original_path).name
        normalized_algo = str(algo_code or "").strip().upper()
        if filename and normalized_algo:
            # Legacy hand-copied model assets live under the AIstock cache by
            # original filename; use them only as a source for hashed cache copy.
            candidates.append(self.cache_root / normalized_algo / filename)
        return self._dedupe_candidate_paths(candidates)

    @staticmethod
    def _dedupe_candidate_paths(candidates: list[Path]) -> list[Path]:
        seen: set[str] = set()
        deduped: list[Path] = []
        for candidate in candidates:
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(candidate)
        return deduped

    @staticmethod
    def _ensure_not_worker_path(
        original_path: str,
        *,
        package_id: str,
        algo_code: str,
        config_key: str,
    ) -> None:
        try:
            ensure_not_forbidden_worker_workspace_path(
                original_path,
                purpose="StrategyPackage model asset source path",
            )
        except StrategyPackageValidationError as exc:
            raise DataUnavailableError(
                f"{algo_code} {config_key} points to a worker workspace path",
                context={
                    "package_id": package_id,
                    "algo_code": algo_code,
                    "config_key": config_key,
                    "asset_path": original_path,
                    "policy": "direct worker workspace paths are forbidden",
                },
            ) from exc

    def _cache_destination(self, algo_code: str, original_path: str) -> Path:
        digest = hashlib.sha256(original_path.encode("utf-8")).hexdigest()[:16]
        suffix = Path(original_path).suffix or ".pt"
        stem = Path(original_path).stem or "model"
        safe_stem = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in stem)
        return self.cache_root / algo_code / f"{safe_stem}_{digest}{suffix}"

    def _asset_namespace_for(self, algo_code: str, config: Mapping[str, Any]) -> str:
        namespace = config.get("asset_namespace")
        if namespace is None and self._asset_namespace_lookup is not None:
            lookup = self._asset_namespace_lookup
            namespace = lookup(algo_code) if callable(lookup) else lookup.get(algo_code)
        normalized = str(namespace or algo_code).strip().upper()
        if not normalized:
            normalized = algo_code
        if any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in normalized):
            raise DataUnavailableError(
                "execution model asset namespace is invalid",
                context={
                    "algo_code": algo_code,
                    "asset_namespace": namespace,
                    "policy": "asset_namespace must be an algorithm-style cache directory name",
                },
            )
        return normalized

    def asset_namespace_for(self, algo_code: str, config: Mapping[str, Any]) -> str:
        """Return the validated cache namespace used for an execution algo."""

        return self._asset_namespace_for(algo_code, config)

    @staticmethod
    def _sidecar_path(destination: Path) -> Path:
        return destination.with_suffix(destination.suffix + ".json")

    def _write_sidecar(self, destination: Path, payload: dict[str, Any]) -> None:
        sidecar = self._sidecar_path(destination)
        sidecar.write_text(
            json.dumps(payload, ensure_ascii=True, sort_keys=True, indent=2),
            encoding="utf-8",
        )
