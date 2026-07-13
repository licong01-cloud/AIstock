"""P1 multi-alpha StrategyPackage structure and artifact integrity services."""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from backend.services.model_store import ModelStoreService, PredictionArtifactStore
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, StrategyPackageValidationError

from .frozen_runtime_self_check import FrozenRuntimeSelfCheckService
from .manifest import freeze_manifest
from .models import (
    AlphaMode,
    PackageStatus,
    StrategyPackageComponentInput,
    StrategyPackageComponentRecord,
    StrategyPackageManifest,
)
from .package_asset_freeze import PackageAssetFreezeService, manifest_has_frozen_runtime_assets
from .repository import StrategyPackageRecord, StrategyPackageRepository

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ArtifactResolver = Callable[[str], bytes | str | Path]


class StrategyPackageComponentService:
    """Service layer for depth-1 multi-alpha package edges and artifact checks."""

    def __init__(
        self,
        *,
        repository: StrategyPackageRepository | Any | None = None,
        model_store: ModelStoreService | Any | None = None,
        artifact_store: PredictionArtifactStore | None = None,
        asset_freezer: PackageAssetFreezeService | Any | None = None,
        frozen_runtime_self_check: FrozenRuntimeSelfCheckService | Any | None = None,
    ) -> None:
        self.repository = repository or StrategyPackageRepository()
        self.model_store = model_store or ModelStoreService()
        self.artifact_store = artifact_store or PredictionArtifactStore()
        self.asset_freezer = asset_freezer or PackageAssetFreezeService()
        self.frozen_runtime_self_check = frozen_runtime_self_check or FrozenRuntimeSelfCheckService(
            asset_store=getattr(self.asset_freezer, "asset_store", None)
        )

    def build_display_name(
        self,
        *,
        alpha_mode: AlphaMode,
        signal_domain: str,
        custom_name: str,
        data_vintage: date | None = None,
        component_count: int | None = None,
        version_suffix: str | None = None,
    ) -> str:
        domain = _name_part(signal_domain, field_name="signal_domain", max_len=16)
        custom = _name_part(custom_name, field_name="custom_name", max_len=16)
        vintage = (data_vintage or datetime.now(timezone.utc).date()).strftime("%Y%m%d")
        if alpha_mode == AlphaMode.SINGLE_ALPHA:
            mode = "单A"
        else:
            if component_count is None or component_count < 2:
                raise StrategyPackageValidationError(
                    "multi-alpha display_name requires component_count >= 2",
                    context={"component_count": component_count},
                )
            mode = f"组合×{component_count}"
        parts = [mode, domain, custom, vintage]
        if version_suffix:
            parts.append(_name_part(version_suffix, field_name="version_suffix", max_len=8))
        name = "·".join(parts)
        if len(name) > 64:
            raise StrategyPackageValidationError(
                "strategy package display_name exceeds 64 characters",
                context={"display_name": name, "length": len(name)},
            )
        return name

    def create_multi_alpha_package(
        self,
        *,
        manifest: StrategyPackageManifest,
        components: Iterable[StrategyPackageComponentInput | dict[str, Any]],
    ) -> tuple[StrategyPackageRecord, list[StrategyPackageComponentRecord]]:
        component_inputs = [
            item if isinstance(item, StrategyPackageComponentInput) else StrategyPackageComponentInput.model_validate(item)
            for item in components
        ]
        if manifest.alpha_mode != AlphaMode.MULTI_ALPHA:
            raise StrategyPackageValidationError(
                "parent package must have alpha_mode=multi_alpha",
                context={"package_id": manifest.package_id, "alpha_mode": manifest.alpha_mode.value},
            )
        if len(component_inputs) < 2:
            raise StrategyPackageValidationError(
                "multi-alpha package requires at least two single-alpha components",
                context={"package_id": manifest.package_id, "component_count": len(component_inputs)},
            )
        frozen = freeze_manifest(manifest)
        asset_records = None
        if _manifest_has_runtime_assets(frozen):
            frozen_assets = self.asset_freezer.freeze_manifest_assets(frozen)
            frozen = frozen_assets.manifest
            asset_records = frozen_assets.assets
        edges = self._build_component_records(
            parent_package_id=frozen.package_id,
            parent_alpha_mode=frozen.alpha_mode,
            component_inputs=component_inputs,
        )
        parent = (
            self.repository.save_manifest_with_assets(frozen, asset_records)
            if asset_records is not None
            else self.repository.save_manifest(frozen)
        )
        saved = self.repository.save_components(parent.package_id, edges)
        return parent, saved

    def get_components(self, package_id: str) -> dict[str, Any]:
        parent = self.repository.get(package_id)
        components = self.repository.list_components(package_id)
        return {
            "package_id": parent.package_id,
            "alpha_mode": parent.alpha_mode.value,
            "component_count": len(components),
            "components": [component.model_dump(mode="json") for component in components],
        }

    def get_prediction_ref(self, package_id: str) -> dict[str, Any]:
        record = self.repository.get(package_id)
        return {
            "package_id": record.package_id,
            "alpha_mode": record.alpha_mode.value,
            "prediction_ref_uri": record.prediction_ref_uri,
            "prediction_ref_sha256": record.prediction_ref_sha256,
            "model_artifact_uri": record.model_artifact_uri,
            "model_artifact_sha256": record.model_artifact_sha256,
            "has_prediction_ref": bool(record.prediction_ref_uri and record.prediction_ref_sha256),
            "has_model_artifact": bool(record.model_artifact_uri and record.model_artifact_sha256),
            "p1_1_post_restart_validation_required": record.prediction_ref_uri is None,
        }

    def bind_prediction_ref_from_run(self, *, package_id: str, run_id: str | None = None) -> StrategyPackageRecord:
        record = self.repository.get(package_id)
        if record.alpha_mode != AlphaMode.SINGLE_ALPHA:
            raise StrategyPackageValidationError(
                "prediction_ref binding is only valid for single-alpha packages in P1",
                context={"package_id": package_id, "alpha_mode": record.alpha_mode.value},
            )
        run_key = run_id or record.run_id
        if not run_key:
            raise StrategyPackageValidationError(
                "strategy package has no run_id to bind prediction_ref",
                context={"package_id": package_id},
            )
        pointer = self.model_store.get_pointer(run_id=run_key)
        manifest = pointer.get("prediction_store_manifest")
        if not isinstance(manifest, dict):
            raise DataUnavailableError(
                "prediction-store manifest missing for strategy package run",
                context={"package_id": package_id, "run_id": run_key, "pointer_status": pointer.get("pointer_status")},
            )
        prediction = _artifact_item(manifest, "prediction")
        model_params = _artifact_item(manifest, "model_params", required=False)
        prediction_uri = str(prediction.get("uri") or "").strip()
        prediction_sha = _required_sha(prediction.get("sha256"), field_name="prediction.sha256")
        if not prediction_uri:
            raise DataUnavailableError(
                "prediction-store manifest prediction artifact has no uri",
                context={"package_id": package_id, "run_id": run_key},
            )
        return self.repository.update_artifact_refs(
            package_id,
            prediction_ref_uri=prediction_uri,
            prediction_ref_sha256=prediction_sha,
            model_artifact_uri=str(model_params.get("uri") or "").strip() if model_params else None,
            model_artifact_sha256=_required_sha(model_params.get("sha256"), field_name="model_params.sha256") if model_params else None,
        )

    def verify_artifact_on_use(
        self,
        *,
        package_id: str,
        artifact_kind: str,
        resolver: ArtifactResolver | None = None,
    ) -> dict[str, Any]:
        record = self.repository.get(package_id)
        kind = str(artifact_kind or "").strip().lower()
        if kind in {"prediction", "pred", "pred.pkl"}:
            uri = record.prediction_ref_uri
            expected = record.prediction_ref_sha256
        elif kind in {"model", "model_params", "params", "params.pkl"}:
            uri = record.model_artifact_uri
            expected = record.model_artifact_sha256
        else:
            raise StrategyPackageValidationError(
                "unsupported strategy package artifact_kind",
                context={"package_id": package_id, "artifact_kind": artifact_kind},
            )
        if not uri or not expected:
            raise DataUnavailableError(
                "strategy package artifact pointer is missing",
                context={"package_id": package_id, "artifact_kind": kind, "uri": uri, "expected_sha256": expected},
            )
        payload = resolver(uri) if resolver else self.artifact_store.resolve_artifact_path(uri)
        actual = _sha256_of_payload(payload)
        if actual != expected:
            raise StrategyPackageValidationError(
                "strategy package artifact sha256 mismatch",
                context={
                    "package_id": package_id,
                    "artifact_kind": kind,
                    "artifact_uri": uri,
                    "expected_sha256": expected,
                    "actual_sha256": actual,
                },
            )
        return {"package_id": package_id, "artifact_kind": kind, "artifact_uri": uri, "sha256": actual, "verified": True}

    def assert_child_can_retire(self, package_id: str) -> None:
        parents = [component.parent_package_id for component in self.repository.list_component_parents(package_id)]
        active_parents = [
            parent_id
            for parent_id in parents
            if self.repository.get(parent_id).package_status != PackageStatus.RETIRED
        ]
        if active_parents:
            raise InvalidStateTransitionError(
                "referenced single-alpha child package cannot be retired",
                context={"package_id": package_id, "active_parent_package_ids": sorted(active_parents)},
            )

    def _build_component_records(
        self,
        *,
        parent_package_id: str,
        parent_alpha_mode: AlphaMode,
        component_inputs: list[StrategyPackageComponentInput],
    ) -> list[StrategyPackageComponentRecord]:
        if parent_alpha_mode != AlphaMode.MULTI_ALPHA:
            raise StrategyPackageValidationError(
                "component parent must have alpha_mode=multi_alpha",
                context={"package_id": parent_package_id, "alpha_mode": parent_alpha_mode.value},
            )
        seen_child_ids: set[str] = set()
        seen_positions: set[int] = set()
        records: list[StrategyPackageComponentRecord] = []
        for item in component_inputs:
            if item.child_package_id in seen_child_ids:
                raise StrategyPackageValidationError(
                    "duplicate child package in multi-alpha components",
                    context={"parent_package_id": parent_package_id, "child_package_id": item.child_package_id},
                )
            if item.position in seen_positions:
                raise StrategyPackageValidationError(
                    "duplicate component position in multi-alpha components",
                    context={"parent_package_id": parent_package_id, "position": item.position},
                )
            child = self.repository.get(item.child_package_id)
            if child.alpha_mode != AlphaMode.SINGLE_ALPHA:
                raise StrategyPackageValidationError(
                    "multi-alpha component child must have alpha_mode=single_alpha",
                    context={
                        "parent_package_id": parent_package_id,
                        "child_package_id": child.package_id,
                        "child_alpha_mode": child.alpha_mode.value,
                    },
                )
            if child.package_status == PackageStatus.RETIRED:
                raise StrategyPackageValidationError(
                    "multi-alpha component child package is retired",
                    context={"parent_package_id": parent_package_id, "child_package_id": child.package_id},
                )
            child_manifest = child.current_manifest()
            if manifest_has_frozen_runtime_assets(child_manifest):
                self.frozen_runtime_self_check.assert_manifest_self_contained(child_manifest)
            records.append(
                StrategyPackageComponentRecord(
                    parent_package_id=parent_package_id,
                    child_package_id=child.package_id,
                    child_manifest_sha256=child.manifest_sha256,
                    component_weight=item.component_weight,
                    score_normalization=item.score_normalization,
                    position=item.position,
                )
            )
            seen_child_ids.add(item.child_package_id)
            seen_positions.add(item.position)
        return records


def _name_part(value: str, *, field_name: str, max_len: int) -> str:
    text = str(value or "").strip()
    if not text:
        raise StrategyPackageValidationError(f"{field_name} is required for strategy package display_name")
    if "·" in text:
        raise StrategyPackageValidationError(f"{field_name} must not contain the separator '·'", context={field_name: text})
    meaningless = text.lower()
    if meaningless.startswith(("pkg_", "qear_", "qe_")) or _SHA256_RE.match(meaningless):
        raise StrategyPackageValidationError(
            f"{field_name} must be human-readable, not a bare id/hash",
            context={field_name: text},
        )
    if len(text) > max_len:
        raise StrategyPackageValidationError(
            f"{field_name} is too long",
            context={field_name: text, "max_len": max_len, "length": len(text)},
        )
    return text


def _artifact_item(manifest: dict[str, Any], artifact_type: str, *, required: bool = True) -> dict[str, Any] | None:
    for item in manifest.get("artifacts", []):
        if isinstance(item, dict) and str(item.get("artifact_type") or "").strip() == artifact_type:
            return item
    if required:
        raise DataUnavailableError(
            "prediction-store manifest missing required artifact",
            context={"artifact_type": artifact_type, "manifest_uri": manifest.get("uri")},
        )
    return None


def _required_sha(value: Any, *, field_name: str) -> str:
    text = str(value or "").strip().lower()
    if not _SHA256_RE.match(text):
        raise DataUnavailableError(
            "prediction-store artifact sha256 is missing or invalid",
            context={"field": field_name, "value": value},
        )
    return text


def _sha256_of_payload(payload: bytes | str | Path) -> str:
    if isinstance(payload, bytes):
        return hashlib.sha256(payload).hexdigest()
    path = Path(payload)
    if not path.exists():
        raise DataUnavailableError("strategy package artifact file does not exist", context={"path": str(path)})
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _manifest_has_runtime_assets(manifest: StrategyPackageManifest) -> bool:
    return manifest_has_frozen_runtime_assets(manifest)
