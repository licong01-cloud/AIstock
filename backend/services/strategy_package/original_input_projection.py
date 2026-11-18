"""Carry an original, explicit model-input exclusion into a new frozen parent.

Raw factor assets remain protected in the parent. Only the per-model runtime
projection follows the original request; absent metadata never infers a drop.
"""
from __future__ import annotations

from .manifest import freeze_manifest
from .models import FactorAsset
from .original_context_bridge import require

KEY = "original_model_input_projection"


def project_original_inputs(manifest):
    evidence = dict(manifest.source_evidence or {})
    processing = (evidence.get("custom_params") or {}).get("feature_processing")
    if processing is None:
        return manifest
    require(isinstance(processing, dict) and not set(processing) - {"exclude_from_model", "clip_outlier"},
            "original model-input processing is not a supported frozen projection")
    excluded = processing.get("exclude_from_model", [])
    require(isinstance(excluded, list) and all(isinstance(x, str) for x in excluded)
            and len(set(excluded)) == len(excluded), "original model-input exclusions are invalid")
    if not excluded:
        return manifest
    require(getattr(manifest.model_asset, "preprocessor_asset", None) is not None,
            "original model-input projection requires its frozen fitted preprocessor")
    factors = list(manifest.factor_set)
    names = [f.factor_name or f.factor_id for f in factors]
    require(len(set(names)) == len(names) and set(excluded) <= set(names),
            "original model-input exclusions are not in the raw factor schema")
    retained = [f for f in factors if (f.factor_name or f.factor_id) not in excluded]
    require(bool(retained), "original model-input projection cannot exclude every dynamic factor")
    retained_names = [f.factor_name or f.factor_id for f in retained]
    retained_ids = {f.factor_id for f in retained}
    components = []
    for component in manifest.alpha_components:
        refs = list(component.lineage.factor_artifact_refs)
        require(set(excluded) <= set(refs), "original component lineage does not prove the excluded input names")
        components.append(component.model_copy(update={
            "factor_ids": [x for x in component.factor_ids if x in retained_ids],
            "lineage": component.lineage.model_copy(update={"factor_artifact_refs": [x for x in refs if x not in excluded]}),
        }))
    evidence[KEY] = {"schema_version": "strategy_original_model_input_projection_v1",
                     "source_model_manifest_sha256": manifest.manifest_sha256,
                     "preprocessor_sha256": manifest.model_asset.preprocessor_asset.sha256,
                     "original_feature_processing": processing, "raw_factor_names": names,
                     "model_factor_names": retained_names,
                     "raw_factor_assets": [f.model_dump(mode="json") for f in factors],
                     "weights_or_processors_changed": False, "exclusion_inferred_from_missing_columns": False}
    panel = (evidence.get("custom_params") or {}).get("observation_panel")
    if panel is not None:
        evidence[KEY]["observation_panel"] = dict(panel)
    return freeze_manifest(manifest.model_copy(update={
        "factor_set": retained, "alpha_components": components, "source_evidence": evidence,
        "manifest_sha256": None,
    }))


def retain_raw_projection_assets(manifest, leg_plans):
    """Include unused raw recipe assets in the protected parent asset ledger."""
    factors = {f.factor_id: f for f in manifest.factor_set}
    for leg in leg_plans:
        for seed in leg.seed_assets:
            projection = seed.get(KEY)
            if projection is None:
                continue
            for raw in projection["raw_factor_assets"]:
                asset = FactorAsset.model_validate(raw)
                old = factors.get(asset.factor_id)
                require(old is None or old.sha256 == asset.sha256,
                        "original raw factor asset conflicts between seed projections")
                factors.setdefault(asset.factor_id, asset)
    return manifest.model_copy(update={"factor_set": list(factors.values())})
