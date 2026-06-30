"""Backfill package-owned runtime assets for existing StrategyPackages."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import Any, Iterable, Mapping, Sequence

from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    StrategyPackageValidationError,
    TradingCoreError,
)

from .manifest import freeze_manifest
from .models import AlphaMode, FactorAsset, ModelAsset, StrategyPackageComponentRecord, StrategyPackageManifest
from .package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from .package_asset_freeze import PackageAssetFreezeService, manifest_has_frozen_runtime_assets
from .repository import StrategyPackageRecord, StrategyPackageRepository


STATUS_SKIPPED_ALREADY_FROZEN = "skipped_already_frozen"
STATUS_PLANNED_FREEZE = "planned_freeze"
STATUS_APPLIED = "applied"
STATUS_UNRECOVERABLE = "unrecoverable"

REASON_MANIFEST_DRIFT = "strategy_package_manifest_hash_drift"
REASON_COMPONENTS_MISSING = "strategy_package_asset_backfill_components_missing"
REASON_CHILD_UNRECOVERABLE = "strategy_package_asset_backfill_child_unrecoverable"
REASON_COMPONENT_LOOKUP_FAILED = "strategy_package_asset_backfill_component_lookup_failed"
REASON_COMPONENT_CYCLE = "strategy_package_asset_backfill_component_cycle"
REASON_PARENT_EVIDENCE_MISSING = "strategy_package_asset_backfill_parent_evidence_missing"
REASON_CHILD_FACTOR_CONFLICT = "strategy_package_asset_backfill_child_factor_conflict"
REASON_CHILD_MODEL_CONFLICT = "strategy_package_asset_backfill_child_model_conflict"
REASON_UNEXPECTED = "strategy_package_asset_backfill_unexpected_error"


@dataclass(frozen=True)
class PackageAssetBackfillItem:
    package_id: str
    package_name: str | None
    alpha_mode: str | None
    old_manifest_sha256: str | None
    status: str
    reason_code: str | None = None
    context: dict[str, Any] = field(default_factory=dict)
    new_manifest_sha256: str | None = None
    asset_count: int = 0
    frozen_manifest: StrategyPackageManifest | None = field(default=None, repr=False)
    assets: list[StrategyPackageAssetRecord] = field(default_factory=list, repr=False)

    def to_report(self) -> dict[str, Any]:
        payload = {
            "package_id": self.package_id,
            "package_name": self.package_name,
            "alpha_mode": self.alpha_mode,
            "old_manifest_sha256": self.old_manifest_sha256,
            "new_manifest_sha256": self.new_manifest_sha256,
            "status": self.status,
            "asset_count": self.asset_count,
        }
        if self.reason_code:
            payload["reason_code"] = self.reason_code
        if self.context:
            payload["context"] = self.context
        return payload


@dataclass(frozen=True)
class PackageAssetBackfillPlan:
    items: list[PackageAssetBackfillItem]
    mode: str = "dry_run"

    def to_report(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for item in self.items:
            counts[item.status] = counts.get(item.status, 0) + 1
        total_assets = sum(item.asset_count for item in self.items if item.status in {STATUS_PLANNED_FREEZE, STATUS_APPLIED})
        failures = [item.to_report() for item in self.items if item.status == STATUS_UNRECOVERABLE]
        resolvable = len([item for item in self.items if item.status != STATUS_UNRECOVERABLE])
        return {
            "mode": self.mode,
            "total_scanned": len(self.items),
            "counts": counts,
            "asset_count": total_assets,
            "source_resolution": {
                "resolved_count": resolvable,
                "unrecoverable_count": len(failures),
                "resolution_rate": (resolvable / len(self.items)) if self.items else 1.0,
            },
            "items": [item.to_report() for item in self.items],
            "unrecoverable": failures,
        }


class PackageAssetBackfillService:
    """Plan and apply frozen-runtime-asset backfills without silent skips."""

    def __init__(
        self,
        *,
        repository: StrategyPackageRepository | Any | None = None,
        asset_freezer: PackageAssetFreezeService | None = None,
    ) -> None:
        self.repository = repository or StrategyPackageRepository()
        self.asset_freezer = asset_freezer or PackageAssetFreezeService()

    def build_plan(
        self,
        *,
        limit: int = 500,
        package_ids: Iterable[str] | None = None,
        package_id_prefix: str | None = None,
    ) -> PackageAssetBackfillPlan:
        if limit <= 0:
            raise StrategyPackageValidationError(
                "limit must be positive",
                context={"reason_code": "strategy_package_asset_backfill_limit_invalid", "limit": limit},
            )
        requested_ids = {str(item).strip() for item in package_ids or [] if str(item).strip()}
        prefix = str(package_id_prefix or "").strip() or None
        ordered: list[PackageAssetBackfillItem] = []
        memo: dict[str, PackageAssetBackfillItem] = {}

        for drift in self._manifest_drift_items(limit=limit):
            if requested_ids and drift.package_id not in requested_ids:
                continue
            if prefix and not drift.package_id.startswith(prefix):
                continue
            memo[drift.package_id] = drift
            ordered.append(drift)

        if requested_ids:
            for package_id in sorted(requested_ids):
                if package_id in memo:
                    continue
                if prefix and not package_id.startswith(prefix):
                    continue
                try:
                    record = self.repository.get(package_id)
                except Exception as exc:  # noqa: BLE001 - requested ids must produce explicit item-level context.
                    reason_code = (
                        "strategy_package_asset_backfill_requested_package_missing"
                        if isinstance(exc, DataUnavailableError)
                        else _reason_code(exc, default="strategy_package_asset_backfill_requested_package_failed")
                    )
                    item = PackageAssetBackfillItem(
                        package_id=package_id,
                        package_name=None,
                        alpha_mode=None,
                        old_manifest_sha256=None,
                        status=STATUS_UNRECOVERABLE,
                        reason_code=reason_code,
                        context=_error_context(exc, package_id=package_id),
                    )
                    memo[package_id] = item
                    ordered.append(item)
                    continue
                self._plan_record(record, memo=memo, ordered=ordered, stack=())
            return PackageAssetBackfillPlan(items=ordered, mode="dry_run")

        records = self._records_to_scan(limit=limit)
        for record in records:
            if record.package_id in memo:
                continue
            if prefix and not record.package_id.startswith(prefix):
                continue
            self._plan_record(record, memo=memo, ordered=ordered, stack=())
        return PackageAssetBackfillPlan(items=ordered, mode="dry_run")

    def apply_plan(
        self,
        plan: PackageAssetBackfillPlan,
        *,
        operator: str,
    ) -> PackageAssetBackfillPlan:
        applied_items: list[PackageAssetBackfillItem] = []
        blocked_after_failure = False
        for item in plan.items:
            if item.status != STATUS_PLANNED_FREEZE:
                applied_items.append(item)
                continue
            if blocked_after_failure:
                applied_items.append(
                    replace(
                        item,
                        status=STATUS_UNRECOVERABLE,
                        reason_code="strategy_package_asset_backfill_apply_blocked_after_failure",
                        context={
                            "package_id": item.package_id,
                            "message": "apply stopped after an earlier package failed; rerun from a fresh dry-run plan",
                        },
                    )
                )
                continue
            if item.frozen_manifest is None or not item.assets:
                applied_items.append(
                    replace(
                        item,
                        status=STATUS_UNRECOVERABLE,
                        reason_code="strategy_package_asset_backfill_plan_incomplete",
                        context={"package_id": item.package_id, "asset_count": len(item.assets)},
                    )
                )
                blocked_after_failure = True
                continue
            try:
                updated = self.repository.backfill_frozen_manifest_assets(
                    item.package_id,
                    frozen_manifest=item.frozen_manifest,
                    assets=item.assets,
                    operator=operator,
                    expected_old_manifest_sha256=str(item.old_manifest_sha256 or ""),
                )
            except Exception as exc:  # noqa: BLE001 - per-item failures must be reported, not swallowed.
                applied_items.append(
                    replace(
                        item,
                        status=STATUS_UNRECOVERABLE,
                        reason_code=_reason_code(exc, default="strategy_package_asset_backfill_apply_failed"),
                        context=_error_context(exc, package_id=item.package_id),
                    )
                )
                blocked_after_failure = True
                continue
            applied_items.append(
                replace(
                    item,
                    status=STATUS_APPLIED,
                    new_manifest_sha256=updated.manifest_sha256,
                    reason_code=None,
                    context={"operator": operator, "asset_count": item.asset_count},
                    frozen_manifest=updated.current_manifest(),
                )
            )
        return PackageAssetBackfillPlan(items=applied_items, mode="apply")

    def _records_to_scan(
        self,
        *,
        limit: int,
    ) -> list[StrategyPackageRecord]:
        return list(self.repository.list(limit=limit))

    def _manifest_drift_items(self, *, limit: int) -> list[PackageAssetBackfillItem]:
        if not hasattr(self.repository, "validate_manifest_integrity"):
            return []
        try:
            report = self.repository.validate_manifest_integrity(limit=limit)
        except Exception:
            raise
        items: list[PackageAssetBackfillItem] = []
        for drift in report.get("drifted") or []:
            package_id = str(drift.get("package_id") or "").strip()
            if not package_id:
                continue
            items.append(
                PackageAssetBackfillItem(
                    package_id=package_id,
                    package_name=drift.get("package_name"),
                    alpha_mode=None,
                    old_manifest_sha256=drift.get("stored_sha256"),
                    new_manifest_sha256=drift.get("computed_sha256"),
                    status=STATUS_UNRECOVERABLE,
                    reason_code=REASON_MANIFEST_DRIFT,
                    context={"drift": _jsonable(drift)},
                )
            )
        return items

    def _plan_record(
        self,
        record: StrategyPackageRecord,
        *,
        memo: dict[str, PackageAssetBackfillItem],
        ordered: list[PackageAssetBackfillItem],
        stack: tuple[str, ...],
    ) -> PackageAssetBackfillItem:
        if record.package_id in memo:
            return memo[record.package_id]
        if record.package_id in stack:
            item = self._unrecoverable_item(
                record,
                reason_code=REASON_COMPONENT_CYCLE,
                context={"component_stack": [*stack, record.package_id]},
            )
            memo[record.package_id] = item
            ordered.append(item)
            return item

        item = self._plan_record_uncached(record, memo=memo, ordered=ordered, stack=stack)
        memo[record.package_id] = item
        ordered.append(item)
        return item

    def _plan_record_uncached(
        self,
        record: StrategyPackageRecord,
        *,
        memo: dict[str, PackageAssetBackfillItem],
        ordered: list[PackageAssetBackfillItem],
        stack: tuple[str, ...],
    ) -> PackageAssetBackfillItem:
        manifest = record.current_manifest()
        if record.alpha_mode == AlphaMode.MULTI_ALPHA and _has_multi_alpha_evidence(manifest):
            try:
                components = list(self.repository.list_components(record.package_id))
            except Exception as exc:
                return self._unrecoverable_item(
                    record,
                    reason_code=_reason_code(exc, default=REASON_COMPONENT_LOOKUP_FAILED),
                    context=_error_context(exc, package_id=record.package_id),
                )
            if components:
                return self._plan_multi_alpha_parent(
                    record,
                    components=components,
                    memo=memo,
                    ordered=ordered,
                    stack=stack,
                )
            if not manifest_has_frozen_runtime_assets(manifest):
                return self._unrecoverable_item(
                    record,
                    reason_code=REASON_COMPONENTS_MISSING,
                    context={
                        "package_id": record.package_id,
                        "alpha_mode": record.alpha_mode.value,
                        "message": "multi-alpha parent has no component edges to derive frozen child assets",
                    },
                )
        return self._plan_freeze(record, desired_manifest=manifest)

    def _plan_multi_alpha_parent(
        self,
        record: StrategyPackageRecord,
        *,
        components: Sequence[StrategyPackageComponentRecord],
        memo: dict[str, PackageAssetBackfillItem],
        ordered: list[PackageAssetBackfillItem],
        stack: tuple[str, ...],
    ) -> PackageAssetBackfillItem:
        child_manifests: dict[str, StrategyPackageManifest] = {}
        child_failures: list[dict[str, Any]] = []
        for component in components:
            try:
                child = self.repository.get(component.child_package_id)
            except Exception as exc:
                child_failures.append(
                    {
                        "child_package_id": component.child_package_id,
                        "reason_code": _reason_code(exc, default="strategy_package_asset_backfill_child_missing"),
                        "context": _error_context(exc, package_id=component.child_package_id),
                    }
                )
                continue
            child_item = self._plan_record(child, memo=memo, ordered=ordered, stack=(*stack, record.package_id))
            if child_item.status == STATUS_UNRECOVERABLE or child_item.frozen_manifest is None:
                child_failures.append(
                    {
                        "child_package_id": child.package_id,
                        "reason_code": child_item.reason_code,
                        "context": child_item.context,
                    }
                )
                continue
            child_manifests[child.package_id] = child_item.frozen_manifest
        if child_failures:
            return self._unrecoverable_item(
                record,
                reason_code=REASON_CHILD_UNRECOVERABLE,
                context={"child_failures": child_failures},
            )

        try:
            desired = self._desired_parent_manifest(record.current_manifest(), components, child_manifests)
        except Exception as exc:
            return self._unrecoverable_item(
                record,
                reason_code=_reason_code(exc, default=REASON_PARENT_EVIDENCE_MISSING),
                context=_error_context(exc, package_id=record.package_id),
            )
        return self._plan_freeze(record, desired_manifest=desired)

    def _desired_parent_manifest(
        self,
        manifest: StrategyPackageManifest,
        components: Sequence[StrategyPackageComponentRecord],
        child_manifests: Mapping[str, StrategyPackageManifest],
    ) -> StrategyPackageManifest:
        ordered_children = [child_manifests[component.child_package_id] for component in components]
        child_sha_by_id = {
            component.child_package_id: child_manifests[component.child_package_id].manifest_sha256
            for component in components
        }
        source_evidence = _patch_multi_alpha_child_shas(
            manifest,
            child_sha_by_id=child_sha_by_id,
            component_child_ids=[component.child_package_id for component in components],
        )
        return manifest.model_copy(
            update={
                "factor_set": _merge_factor_assets(ordered_children, parent_package_id=manifest.package_id),
                "model_asset": _merge_model_assets(ordered_children, parent_package_id=manifest.package_id),
                "source_evidence": source_evidence,
                "manifest_sha256": None,
            }
        )

    def _plan_freeze(
        self,
        record: StrategyPackageRecord,
        *,
        desired_manifest: StrategyPackageManifest,
    ) -> PackageAssetBackfillItem:
        desired = desired_manifest.model_copy(update={"manifest_sha256": None, "package_status": record.package_status})
        try:
            frozen_candidate = freeze_manifest(desired)
            if (
                manifest_has_frozen_runtime_assets(frozen_candidate)
                and frozen_candidate.manifest_sha256 == record.manifest_sha256
                and self._ledger_covers(frozen_candidate)
            ):
                return PackageAssetBackfillItem(
                    package_id=record.package_id,
                    package_name=record.package_name,
                    alpha_mode=record.alpha_mode.value,
                    old_manifest_sha256=record.manifest_sha256,
                    new_manifest_sha256=record.manifest_sha256,
                    status=STATUS_SKIPPED_ALREADY_FROZEN,
                    asset_count=len(_manifest_asset_keys(frozen_candidate)),
                    frozen_manifest=frozen_candidate,
                )
            frozen_assets = self.asset_freezer.freeze_manifest_assets(desired)
            if (
                frozen_assets.manifest.manifest_sha256 == record.manifest_sha256
                and self._ledger_covers(frozen_assets.manifest)
            ):
                return PackageAssetBackfillItem(
                    package_id=record.package_id,
                    package_name=record.package_name,
                    alpha_mode=record.alpha_mode.value,
                    old_manifest_sha256=record.manifest_sha256,
                    new_manifest_sha256=record.manifest_sha256,
                    status=STATUS_SKIPPED_ALREADY_FROZEN,
                    asset_count=len(frozen_assets.assets),
                    frozen_manifest=frozen_assets.manifest,
                    assets=frozen_assets.assets,
                )
        except Exception as exc:  # noqa: BLE001 - per-package source failures are part of the report.
            return self._unrecoverable_item(
                record,
                reason_code=_reason_code(exc),
                context=_error_context(exc, package_id=record.package_id),
            )

        return PackageAssetBackfillItem(
            package_id=record.package_id,
            package_name=record.package_name,
            alpha_mode=record.alpha_mode.value,
            old_manifest_sha256=record.manifest_sha256,
            new_manifest_sha256=frozen_assets.manifest.manifest_sha256,
            status=STATUS_PLANNED_FREEZE,
            asset_count=len(frozen_assets.assets),
            frozen_manifest=frozen_assets.manifest,
            assets=frozen_assets.assets,
            context={
                "old_manifest_sha256": record.manifest_sha256,
                "new_manifest_sha256": frozen_assets.manifest.manifest_sha256,
                "asset_refs": _asset_key_payload(frozen_assets.assets),
            },
        )

    def _ledger_covers(self, manifest: StrategyPackageManifest) -> bool:
        expected = _manifest_asset_keys(manifest)
        if not expected:
            return False
        rows = self.repository.list_package_assets(manifest.package_id)
        actual = {(row.asset_type, row.asset_ref, row.asset_sha256) for row in rows}
        return all(item in actual for item in expected)

    @staticmethod
    def _unrecoverable_item(
        record: StrategyPackageRecord,
        *,
        reason_code: str,
        context: dict[str, Any],
    ) -> PackageAssetBackfillItem:
        return PackageAssetBackfillItem(
            package_id=record.package_id,
            package_name=record.package_name,
            alpha_mode=record.alpha_mode.value,
            old_manifest_sha256=record.manifest_sha256,
            new_manifest_sha256=None,
            status=STATUS_UNRECOVERABLE,
            reason_code=reason_code,
            context=context,
            frozen_manifest=None,
        )


def _manifest_asset_keys(
    manifest: StrategyPackageManifest,
) -> set[tuple[StrategyPackageAssetType, str, str | None]]:
    keys: set[tuple[StrategyPackageAssetType, str, str | None]] = set()
    for factor in manifest.factor_set:
        if factor.asset_ref and factor.sha256:
            keys.add((StrategyPackageAssetType.FACTOR_CODE, factor.asset_ref, factor.sha256))
    model_assets = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    for asset in model_assets:
        if asset.asset_ref and asset.sha256:
            keys.add((StrategyPackageAssetType.MODEL_WEIGHT, asset.asset_ref, asset.sha256))
    return keys


def _merge_factor_assets(
    manifests: Sequence[StrategyPackageManifest],
    *,
    parent_package_id: str,
) -> list[FactorAsset]:
    merged: dict[str, FactorAsset] = {}
    for manifest in manifests:
        for factor in manifest.factor_set:
            existing = merged.get(factor.factor_id)
            if existing and _asset_identity(existing) != _asset_identity(factor):
                raise StrategyPackageValidationError(
                    "multi-alpha child factor assets conflict during backfill",
                    context={
                        "reason_code": REASON_CHILD_FACTOR_CONFLICT,
                        "parent_package_id": parent_package_id,
                        "factor_id": factor.factor_id,
                        "existing": _asset_identity(existing),
                        "incoming": _asset_identity(factor),
                    },
                )
            merged.setdefault(factor.factor_id, factor)
    return [merged[key] for key in sorted(merged)]


def _merge_model_assets(
    manifests: Sequence[StrategyPackageManifest],
    *,
    parent_package_id: str,
) -> list[ModelAsset]:
    merged: dict[str, ModelAsset] = {}
    for manifest in manifests:
        assets = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
        for asset in assets:
            existing = merged.get(asset.model_id)
            if existing and _asset_identity(existing) != _asset_identity(asset):
                raise StrategyPackageValidationError(
                    "multi-alpha child model assets conflict during backfill",
                    context={
                        "reason_code": REASON_CHILD_MODEL_CONFLICT,
                        "parent_package_id": parent_package_id,
                        "model_id": asset.model_id,
                        "existing": _asset_identity(existing),
                        "incoming": _asset_identity(asset),
                    },
                )
            merged.setdefault(asset.model_id, asset)
    return [merged[key] for key in sorted(merged)]


def _patch_multi_alpha_child_shas(
    manifest: StrategyPackageManifest,
    *,
    child_sha_by_id: Mapping[str, str | None],
    component_child_ids: Sequence[str],
) -> dict[str, Any]:
    evidence = deepcopy(manifest.source_evidence or {})
    multi_alpha = evidence.get("multi_alpha")
    if not isinstance(multi_alpha, dict):
        raise StrategyPackageValidationError(
            "multi-alpha parent source_evidence.multi_alpha is required for backfill",
            context={"reason_code": REASON_PARENT_EVIDENCE_MISSING, "package_id": manifest.package_id},
        )
    legs = multi_alpha.get("legs")
    if not isinstance(legs, list):
        raise StrategyPackageValidationError(
            "multi-alpha parent source_evidence.multi_alpha.legs is required for backfill",
            context={"reason_code": REASON_PARENT_EVIDENCE_MISSING, "package_id": manifest.package_id},
        )
    seen: set[str] = set()
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        child_package_id = str(leg.get("child_package_id") or "").strip()
        if child_package_id in child_sha_by_id:
            new_sha = child_sha_by_id[child_package_id]
            if not new_sha:
                raise StrategyPackageValidationError(
                    "multi-alpha child manifest sha is unavailable during backfill",
                    context={
                        "reason_code": REASON_PARENT_EVIDENCE_MISSING,
                        "package_id": manifest.package_id,
                        "child_package_id": child_package_id,
                    },
                )
            leg["child_manifest_sha256"] = new_sha
            seen.add(child_package_id)
    missing = sorted(set(component_child_ids).difference(seen))
    if missing:
        raise StrategyPackageValidationError(
            "multi-alpha parent evidence is missing component child entries",
            context={
                "reason_code": REASON_PARENT_EVIDENCE_MISSING,
                "package_id": manifest.package_id,
                "missing_child_package_ids": missing,
            },
        )
    evidence["multi_alpha"] = multi_alpha
    return evidence


def _has_multi_alpha_evidence(manifest: StrategyPackageManifest) -> bool:
    evidence = manifest.source_evidence or {}
    multi_alpha = evidence.get("multi_alpha") if isinstance(evidence, Mapping) else None
    if not isinstance(multi_alpha, Mapping):
        return False
    return isinstance(multi_alpha.get("legs"), list)


def _asset_identity(asset: Any) -> dict[str, Any]:
    return {
        "asset_ref": getattr(asset, "asset_ref", None),
        "sha256": getattr(asset, "sha256", None),
        "size_bytes": getattr(asset, "size_bytes", None),
        "source_uri": getattr(asset, "source_uri", None),
    }


def _asset_key_payload(assets: Sequence[StrategyPackageAssetRecord]) -> list[dict[str, str | None]]:
    return [
        {
            "asset_type": asset.asset_type.value,
            "asset_ref": asset.asset_ref,
            "asset_sha256": asset.asset_sha256,
        }
        for asset in assets
    ]


def _reason_code(exc: BaseException, *, default: str = REASON_UNEXPECTED) -> str:
    context = getattr(exc, "context", None)
    if isinstance(context, Mapping) and str(context.get("reason_code") or "").strip():
        return str(context["reason_code"])
    if isinstance(exc, DataUnavailableError):
        return "strategy_package_asset_backfill_source_missing"
    if isinstance(exc, InvalidStateTransitionError):
        return "strategy_package_asset_backfill_invalid_state"
    if isinstance(exc, StrategyPackageValidationError):
        return "strategy_package_asset_backfill_validation_failed"
    return default


def _error_context(exc: BaseException, *, package_id: str) -> dict[str, Any]:
    context = getattr(exc, "context", None)
    return {
        "package_id": package_id,
        "error_type": type(exc).__name__,
        "error": str(exc),
        "context": _jsonable(context if isinstance(context, Mapping) else {}),
        "error_code": getattr(exc, "error_code", TradingCoreError.error_code),
    }


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "value"):
        return value.value
    return value
