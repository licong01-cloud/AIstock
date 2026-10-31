"""Recover original seed assets into a new, non-admitted package version.

This is an asset operation, never promotion, training or execution admission.
All source reads and staging stores are injected; persistence is a separate step.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlparse

from backend.services.trading_core.errors import StrategyPackageValidationError
from .manifest import compute_manifest_sha256, freeze_manifest
from .models import AlphaMode, PackageStatus, StrategyPackageManifest
from .multi_alpha_promotion import _merge_model_assets
from .package_asset_freeze import PackageAssetFreezeService
from .runtime_schema import alpha158_schema_bytes, alpha158_schema_payload, load_conf_yaml_bytes


RECOVERY_SCHEMA = "multi_alpha_original_seed_asset_recovery_v1"


def _fail(reason: str, **context: Any) -> None:
    raise StrategyPackageValidationError(reason, context={"reason_code": reason, **context})


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def semantic_payload(manifest: StrategyPackageManifest) -> dict[str, Any]:
    """Everything except the expressly replaceable asset closure and identity."""
    payload = manifest.model_dump(mode="json")
    for key in (
        "package_id",
        "package_name",
        "package_version",
        "package_status",
        "manifest_sha256",
        "model_asset",
        "asset_checks",
    ):
        payload.pop(key, None)
    evidence = payload["source_evidence"]
    evidence.pop("seed_asset_recovery", None)
    evidence.pop("runtime_asset_admission", None)
    evidence["multi_alpha"].pop("signal_admission", None)
    for leg in evidence["multi_alpha"]["legs"]:
        leg.pop("seed_assets", None)
    return payload


@dataclass
class SeedRecoveryPlan:
    parent_id: str
    parent_sha256: str
    semantic_sha256: str
    receipts: list[dict[str, Any]] = field(default_factory=list)
    manifest: StrategyPackageManifest | None = None
    assets: list[Any] = field(default_factory=list)

    @property
    def ready(self) -> bool:
        return bool(self.manifest and self.receipts and all(r["status"] == "verified" for r in self.receipts))

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": RECOVERY_SCHEMA,
            "parent_id": self.parent_id,
            "parent_sha256": self.parent_sha256,
            "semantic_sha256": self.semantic_sha256,
            "ready": self.ready,
            "seed_count": len(self.receipts),
            "receipts": self.receipts,
            "manifest": self.manifest.model_dump(mode="json") if self.manifest else None,
            "production_assets_written": False,
            "activated": False,
        }


class SeedAssetRecoveryService:
    def __init__(self, *, promotion: Any, source_store: Any, staging_store: Any, self_check: Any) -> None:
        if source_store is staging_store or (
            getattr(source_store, "root", None) is not None
            and Path(source_store.root).resolve() == Path(staging_store.root).resolve()
        ):
            _fail("seed_recovery_requires_isolated_staging")
        self.promotion = promotion
        self.source = promotion.asset_freezer.source
        self.source_store = source_store
        self.store = staging_store
        self.self_check = self_check

    def audit(
        self,
        parent: StrategyPackageManifest,
        *,
        version: str,
        on_receipt: Callable[[dict[str, Any]], None] | None = None,
    ) -> SeedRecoveryPlan:
        if compute_manifest_sha256(parent) != parent.manifest_sha256:
            _fail("seed_recovery_parent_hash_mismatch")
        if parent.alpha_mode != AlphaMode.MULTI_ALPHA or version == parent.package_version or not version.strip():
            _fail("seed_recovery_requires_new_multi_alpha_version")
        multi = parent.source_evidence.get("multi_alpha", {})
        legs = multi.get("legs", [])
        if not legs or {x["leg_id"] for x in legs} != {x.alpha_id for x in parent.alpha_components}:
            _fail("seed_recovery_invalid_leg_roster")
        for leg in legs:
            seeds = leg.get("seed_run_ids", [])
            if (
                not seeds
                or len(seeds) != len(set(seeds))
                or leg.get("seed_assets")
                or leg.get("ensemble_method") != "mean_by_trade_date_instrument"
            ):
                _fail("seed_recovery_invalid_seed_roster", leg_id=leg["leg_id"])
        plan = SeedRecoveryPlan(parent.package_id, str(parent.manifest_sha256), _digest(semantic_payload(parent)))
        manifests: list[StrategyPackageManifest] = []
        bindings: dict[str, list[dict[str, Any]]] = {}
        # Copy only already pinned parent assets into isolated CAS, never into production.
        parent_assets = [*parent.factor_set]
        if parent.runtime_assets and parent.runtime_assets.alpha158.enabled:
            parent_assets.append(parent.runtime_assets.alpha158)
        for asset in parent_assets:
            data = self.source_store.get(asset.asset_ref)
            if hashlib.sha256(data).hexdigest() != asset.sha256 or len(data) != asset.size_bytes:
                _fail("seed_recovery_parent_asset_mismatch", asset_ref=asset.asset_ref)
            self.store.put(data, kind="original_parent_asset", sha256=asset.sha256)
        for leg in legs:
            bindings[leg["leg_id"]] = []
            for index, seed in enumerate(leg["seed_run_ids"]):
                row, frozen = self._audit_seed(parent, leg, index, seed)
                plan.receipts.append(row)
                if frozen is not None:
                    manifests.append(frozen)
                    model = frozen.model_asset
                    bindings[leg["leg_id"]].append(
                        {
                            "seed_run_id": seed,
                            "model_id": model.model_id,
                            "asset_ref": model.asset_ref,
                            "sha256": model.sha256,
                            "factor_artifact_refs": list(leg["factor_artifact_refs"]),
                            "runtime_assets": frozen.runtime_assets.model_dump(mode="json"),
                        }
                    )
                if on_receipt:
                    on_receipt(deepcopy(row))
        if any(r["status"] != "verified" for r in plan.receipts):
            return plan
        evidence = deepcopy(parent.source_evidence)
        evidence.pop("runtime_asset_admission", None)
        evidence["multi_alpha"].pop("signal_admission", None)
        for leg in evidence["multi_alpha"]["legs"]:
            leg["seed_assets"] = bindings[leg["leg_id"]]
        evidence["seed_asset_recovery"] = {
            "schema_version": RECOVERY_SCHEMA,
            "parent_package_id": parent.package_id,
            "parent_manifest_sha256": parent.manifest_sha256,
            "semantic_sha256": plan.semantic_sha256,
            "receipt_sha256": _digest(plan.receipts),
            "seed_count": len(plan.receipts),
            "asset_only": True,
            "execution_admitted": False,
            "historical_metrics_revalidated": False,
        }
        identity = _digest([parent.package_id, parent.manifest_sha256, version])[:24]
        candidate = freeze_manifest(
            parent.model_copy(
                update={
                    "package_id": f"pkg_sr_{identity}",
                    "package_version": version,
                    "package_name": f"{parent.package_name} [original seed asset recovery {version}]",
                    "package_status": PackageStatus.DRAFT,
                    "source_evidence": evidence,
                    "model_asset": _merge_model_assets(manifests),
                    "asset_checks": [],
                    "manifest_sha256": None,
                }
            )
        )
        if _digest(semantic_payload(candidate)) != plan.semantic_sha256:
            _fail("seed_recovery_semantic_drift")
        freezer = PackageAssetFreezeService(asset_store=self.store, source=self.source)
        result = freezer.freeze_manifest_assets(candidate)
        self.self_check.assert_manifest_self_contained(result.manifest)
        plan.manifest, plan.assets = result.manifest, result.assets
        return plan

    def _audit_seed(
        self, parent: StrategyPackageManifest, leg: dict[str, Any], index: int, seed: str
    ) -> tuple[dict[str, Any], StrategyPackageManifest | None]:
        row: dict[str, Any] = {
            "leg_id": leg["leg_id"],
            "seed_run_id": seed,
            "seed_index": index,
            "status": "blocked",
            "checks": {},
        }
        checks = row["checks"]

        def check(name: str, action: Callable[[], Any]) -> Any:
            try:
                result = action()
                checks[name] = {"status": "verified"}
                return result
            except Exception as exc:
                checks[name] = {
                    "status": "blocked",
                    "error_type": type(exc).__name__,
                    "reason": getattr(exc, "context", {}).get("reason_code", type(exc).__name__),
                    "context": getattr(exc, "context", {}),
                    "message": str(exc),
                }
                return None

        sources = check(
            "provenance",
            lambda: self.promotion._resolve_leg_seed_sources(
                leg_id=leg["leg_id"], seed_run_ids=(seed,), run_id=parent.source.source_id
            ),
        )
        if not sources:
            return row, None
        provenance = sources[0]
        row["provenance"] = json.loads(json.dumps(provenance.to_meta(), default=str))
        original = next((p for p in leg.get("seed_provenance", []) if p.get("seed_ref") == seed), None)
        if not original or any(
            original.get(k) != getattr(provenance, k)
            for k in ("source_task_id", "source_loop_id", "source_experiment_id")
        ):
            checks["provenance"] = {"status": "blocked", "reason": "seed_recovery_provenance_drift"}
            return row, None
        manifest = check(
            "source_manifest",
            lambda: self.promotion._build_parent_leg_asset_manifest(
                leg_id=leg["leg_id"], seed_run_ids=(seed,), seed_sources=sources, run_id=parent.source.source_id
            ),
        )
        if manifest is None:
            return row, None
        source_weight = check("weight", lambda: self.source.model_params_bytes(manifest))
        if source_weight:
            sha = hashlib.sha256(source_weight.data).hexdigest()
            checks["weight"].update(sha256=sha, size_bytes=len(source_weight.data), source_uri=source_weight.source_uri)
            primary = next(m for m in parent.model_asset if m.model_id == leg["model_id"])
            if index == 0 and sha != primary.sha256:
                checks["weight"].update(status="blocked", reason="seed_recovery_primary_weight_drift")

            def weight_provenance() -> None:
                uri = urlparse(str(source_weight.source_uri or ""))
                parts = uri.path.strip("/").split("/")
                allowed = {seed, provenance.source_run_id, provenance.source_experiment_id}
                if (
                    uri.scheme != "aistock-prediction-store"
                    or uri.netloc != "runs"
                    or len(parts) != 2
                    or unquote(parts[0]) not in allowed
                    or parts[1] != "model_params"
                ):
                    _fail("seed_recovery_weight_source_not_bound")
                stored = self.source.artifact_store.load_manifest(unquote(parts[0]))
                entries = [a for a in stored.get("artifacts", []) if a.get("artifact_type") == "model_params"]
                metadata = stored.get("metadata", {})
                if (
                    len(entries) != 1
                    or entries[0].get("sha256") != sha
                    or entries[0].get("size_bytes") != len(source_weight.data)
                    or metadata.get("task_id") != provenance.source_task_id
                    or metadata.get("loop_index") != provenance.source_loop_index
                ):
                    _fail("seed_recovery_weight_archive_identity_mismatch")
                checks["weight_archive"] = {
                    "status": "verified",
                    "run_key": parts[0],
                    "manifest_sha256": _digest(stored),
                    "weight_sha256": sha,
                    "task_id": metadata.get("task_id"),
                    "loop_index": metadata.get("loop_index"),
                }

            check("weight_provenance", weight_provenance)
        conf = check("training_config", lambda: self.source.conf_yaml_bytes(manifest))
        conf_dict = (
            check("config_parse", lambda: load_conf_yaml_bytes(conf.data, source_uri=conf.source_uri)) if conf else None
        )
        if conf:
            checks["training_config"].update(sha256=hashlib.sha256(conf.data).hexdigest(), source_uri=conf.source_uri)

        def factors() -> StrategyPackageManifest:
            if list(manifest.alpha_components[0].lineage.factor_artifact_refs) != list(leg["factor_artifact_refs"]):
                _fail("seed_recovery_factor_order_drift")
            pinned = {f.factor_id: f for f in parent.factor_set}
            selected = [pinned[f.factor_id] for f in manifest.factor_set]
            if {f.factor_id for f in selected} != set(leg["factor_artifact_refs"]):
                _fail("seed_recovery_factor_set_drift")
            checks["factor_bindings"] = {
                "status": "verified",
                "factors": [{"factor_id": f.factor_id, "sha256": f.sha256, "asset_ref": f.asset_ref} for f in selected],
            }
            return manifest.model_copy(update={"factor_set": selected})

        pinned_manifest = check("factor_references", factors)

        def schema() -> None:
            if conf_dict is None:
                _fail("seed_recovery_schema_requires_original_config")
            payload = alpha158_schema_payload(conf_dict)
            expected = leg["runtime_assets"]["alpha158"]
            if bool(payload) != bool(expected["enabled"]) or (
                payload and hashlib.sha256(alpha158_schema_bytes(payload)).hexdigest() != expected["sha256"]
            ):
                _fail("seed_recovery_schema_drift")

        check("schema", schema)
        freezer = PackageAssetFreezeService(
            asset_store=self.store,
            source=self.source,
            model_params_reader=lambda _: source_weight,
            conf_yaml_reader=lambda _: conf,
        )
        processor = None
        if source_weight:
            # Even when config is absent, attempt exact-weight paired original dataset recovery.
            handler = (conf_dict or {}).get("task", {}).get("dataset", {}).get("kwargs", {}).get("handler", {})
            required = conf_dict is None or bool(handler.get("kwargs", {}).get("infer_processors"))
            if required:
                processor = check(
                    "preprocessor", lambda: self.source.fitted_preprocessor_bytes(manifest, model_weight_sha256=sha)
                )
                if processor:
                    checks["preprocessor"].update(
                        sha256=hashlib.sha256(processor.data).hexdigest(),
                        size_bytes=len(processor.data),
                        source_uri=processor.source_uri,
                    )
            else:
                checks["preprocessor"] = {"status": "verified", "requirement": "none_in_original_config"}
            code_freezer = PackageAssetFreezeService(asset_store=self.store, source=self.source)
            code = check(
                "model_code",
                lambda: code_freezer._freeze_model_code_assets(
                    manifest.model_asset,
                    manifest,
                    model_weight_data=source_weight.data,
                    processor_data=processor.data if processor else None,
                ),
            )
            if code:
                checks["model_code"].update(required=code[0], assets=[a.model_dump(mode="json") for a in code[1]])
        if any(c["status"] != "verified" for c in checks.values()):
            return row, None
        frozen = check("freeze", lambda: freezer.freeze_manifest_assets(pinned_manifest))
        if frozen is None:
            return row, None

        def unchanged_closure() -> None:
            model = frozen.manifest.model_asset
            if model.sha256 != checks["weight"]["sha256"]:
                _fail("seed_recovery_source_changed_during_freeze")
            if model.preprocessor_asset and model.preprocessor_asset.sha256 != checks["preprocessor"].get("sha256"):
                _fail("seed_recovery_preprocessor_changed_during_freeze")
            expected_code = [(a["relative_path"], a["sha256"]) for a in checks["model_code"]["assets"]]
            if [(a.relative_path, a.sha256) for a in model.model_code_assets] != expected_code:
                _fail("seed_recovery_code_changed_during_freeze")

        check("unchanged_asset_closure", unchanged_closure)
        if checks["unchanged_asset_closure"]["status"] != "verified":
            return row, None
        checked = check("runtime_self_check", lambda: self.self_check.assert_manifest_self_contained(frozen.manifest))
        if checked is None:
            return row, None
        checks["runtime_self_check"]["receipt"] = checked.to_context()
        model = frozen.manifest.model_asset
        if index:
            suffix = hashlib.sha256(f"{leg['leg_id']}:{seed}".encode()).hexdigest()[:16]
            model = model.model_copy(update={"model_id": f"{model.model_id}__seed_{suffix}"})
        row["status"] = "verified"
        return row, freeze_manifest(frozen.manifest.model_copy(update={"model_asset": model, "manifest_sha256": None}))

    def persist(self, plan: SeedRecoveryPlan, *, repository: Any, current_parent: StrategyPackageManifest) -> Any:
        if not plan.ready or plan.manifest is None:
            _fail("seed_recovery_incomplete", seed_count=len(plan.receipts))
        if (
            current_parent.package_id != plan.parent_id
            or current_parent.manifest_sha256 != plan.parent_sha256
            or compute_manifest_sha256(current_parent) != plan.parent_sha256
        ):
            _fail("seed_recovery_parent_changed")
        if (
            plan.manifest.package_id == plan.parent_id
            or plan.manifest.package_status != PackageStatus.DRAFT
            or _digest(semantic_payload(plan.manifest)) != plan.semantic_sha256
            or _digest(semantic_payload(current_parent)) != plan.semantic_sha256
            or compute_manifest_sha256(plan.manifest) != plan.manifest.manifest_sha256
            or plan.manifest.source_evidence["seed_asset_recovery"]["receipt_sha256"] != _digest(plan.receipts)
        ):
            _fail("seed_recovery_invalid_candidate")
        existing = repository.find_by_source_version(
            source_type=plan.manifest.source.source_type.value,
            source_id=plan.manifest.source.source_id,
            loop_id=plan.manifest.source.loop_id,
            package_version=plan.manifest.package_version,
        )
        if existing and (
            existing.package_id != plan.manifest.package_id or existing.manifest_sha256 != plan.manifest.manifest_sha256
        ):
            _fail("seed_recovery_version_conflict")
        for asset in plan.assets:
            self.store.verify(asset.asset_ref, sha256=asset.asset_sha256, size_bytes=asset.asset_size_bytes)
        record = repository.save_manifest_with_assets(plan.manifest, plan.assets)
        readback = repository.get(record.package_id)
        if readback.manifest_sha256 != plan.manifest.manifest_sha256 or readback.package_status != PackageStatus.DRAFT:
            _fail("seed_recovery_readback_mismatch")
        return readback
