"""Bounded discovery of real frozen model manifests, never test-return tables."""
from __future__ import annotations

import json
from pathlib import Path
import re

from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import canonical_sha, file_sha


def _read(path):
    path = Path(path)
    if path.stat().st_size > 64*1024*1024:
        raise ValueError(f"model metadata exceeds inventory budget: {path.name}")
    return json.loads(path.read_text(encoding="utf-8"))


def _role(name):
    if "exit" in name:
        return "EXIT"
    if "generic_" in name or "population_transfer" in name:
        return "FIXED_5TD_ENTRY"
    if any(v in name for v in ("economic", "entry_value", "entry_timing", "entry_information", "value_anchor", "context_price", "price_research_campaign")):
        return "REVIEW_CLOCK_ENTRY"
    if "price_range" in name or "price_envelope" in name:
        return "OPEN_DISTRIBUTION"
    if "outcome" in name:
        return "OUTCOME_HOLDING"
    return "RANKING_ADMISSION"


def _metadata_dates(value, prefix=""):
    found = {}
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"models", "trees", "diagnostics", "metrics", "nodes", "test_predictions"}:
                continue
            name = f"{prefix}.{key}".strip(".")
            if isinstance(item, str) and re.fullmatch(r"20\d{2}-\d{2}-\d{2}", item):
                found[name] = item
            elif isinstance(item, dict):
                found.update(_metadata_dates(item, name))
            elif isinstance(item, list) and item and all(isinstance(v, str) and re.fullmatch(r"20\d{2}-\d{2}-\d{2}", v) for v in item):
                found[name] = {"first": min(item), "last": max(item), "count": len(item)}
    return found


def manifest_candidates(root):
    """At most three declared layout levels; never descend raw data/provider trees."""
    root = Path(root)
    groups = []
    model_first = root/"advisory_model_first"
    for leaf in ("bundles", "outcome_bundles", "price_range_bundles", "meta_label_bundles"):
        base = model_first/leaf
        if base.is_dir():
            groups.extend((leaf, child) for child in sorted(base.iterdir()) if child.is_dir())
    for base in sorted(root.iterdir()):
        if not base.is_dir() or not base.name.startswith("advisory_") or base.name == "advisory_model_first":
            continue
        if any(v in base.name for v in ("alpha_audit", "alpha_generator", "qe_alpha_mve", "handoff", "preflight", "dataset", "source_preparation")):
            continue
        groups.append((base.name, base))
        groups.extend((base.name, child) for child in sorted(base.iterdir()) if child.is_dir()
            and child.name not in {"inputs", "data", "finance", "evidence", "reports", "registry"})
        for container in sorted(base.iterdir()):
            if container.is_dir() and container.name.endswith("_bundles"):
                groups.extend((base.name, child) for child in sorted(container.iterdir()) if child.is_dir())
    candidates = []
    for name, directory in groups:
        for relative in ("manifest.json", "trained/manifest.json", "training/manifest.json"):
            path = directory/relative
            if path.is_file():
                candidates.append((name, path))
    return candidates


def inspect_manifest(name, path):
    path = Path(path)
    manifest = _read(path)
    files = manifest.get("files", {})
    if not isinstance(files, dict):
        raise ValueError("manifest files are not a mapping")
    weights, errors = [], []
    for relative, descriptor in files.items():
        suffix = Path(relative).suffix.lower()
        if suffix not in {".json", ".txt", ".pkl", ".pickle", ".bin", ".pt", ".pth"}:
            continue
        if suffix == ".json" and not any(v in relative.lower() for v in ("model", "metadata", "calibration")):
            continue
        member = path.parent/relative
        if path.parent.resolve() not in member.resolve().parents:
            errors.append({"file": relative, "reason": "MANIFEST_PATH_ESCAPE"})
            continue
        if not member.is_file():
            errors.append({"file": relative, "reason": "ARTIFACT_UNAVAILABLE"})
            continue
        digest = file_sha(member)
        expected = descriptor.get("sha256") if isinstance(descriptor, dict) else None
        expected_size = descriptor.get("size_bytes") if isinstance(descriptor, dict) else None
        if expected != digest or (expected_size is not None and expected_size != member.stat().st_size):
            errors.append({"file": relative, "reason": "ARTIFACT_IDENTITY_MISMATCH"})
            continue
        entry = dict(file=relative, sha256=digest, size_bytes=member.stat().st_size)
        entry["artifact_kind"] = "PREDICTOR_WEIGHT" if suffix != ".json" else "AUXILIARY_METADATA"
        if member.suffix == ".json":
            body = _read(member)
            entry["keys"] = sorted(body) if isinstance(body, dict) else []
            entry["model_names"] = sorted(body.get("models", {})) if isinstance(body, dict) and isinstance(body.get("models"), dict) else []
            entry["fold_names"] = sorted(body.get("folds", {})) if isinstance(body, dict) and isinstance(body.get("folds"), dict) else []
            entry["dates"] = _metadata_dates(body)
            if (entry["model_names"] or entry["fold_names"]
                    or (isinstance(body, dict) and ({"coefficients", "intercept"}.issubset(body)
                        or {"forest", "calibration", "recipe"}.issubset(body)
                        or {"matched_anchor", "candidate_transfer"}.issubset(body)))):
                entry["artifact_kind"] = "SERIALIZED_PREDICTOR"
            elif "calibration" in relative and "request" not in relative:
                entry["artifact_kind"] = "CALIBRATOR_NEEDS_PARENT"
            if Path(relative).name == "fresh_hmm_models.json":
                entry["artifact_kind"] = "AUXILIARY_HMM_MODELS"
        weights.append(entry)
    metadata = {}
    for relative in ("training_request.json", "request.json", "split.json", "label_policy.json", "feature_schema.json"):
        member = path.parent/relative
        if member.is_file():
            body = _read(member)
            metadata[relative] = dict(sha256=file_sha(member), dates=_metadata_dates(body),
                identity={k: body[k] for k in ("schema_version", "package_id", "policy_sha256", "style_profile_hash", "feature_schema_hash", "label_policy_version") if isinstance(body, dict) and k in body})
    plan = path.parent.parent/"preregistered/plan.json" if path.parent.name == "trained" else None
    if plan is not None and plan.is_file():
        body = _read(plan)
        metadata["preregistered/plan.json"] = dict(sha256=file_sha(plan), dates=_metadata_dates(body),
            identity={k: body[k] for k in ("schema_version", "experiment_id", "objective_contract", "policy_sha256") if k in body})
    identity = {k: manifest[k] for k in ("schema_version", "package_id", "plan_sha256", "feature_schema_hash", "style_profile_hash", "label_policy_version", "calibration_state", "model_names", "status") if k in manifest}
    # Shared files alone are not a candidate identity: original policy/plan is retained.
    model_id = canonical_sha(dict(files=[(w["file"], w["sha256"]) for w in weights], identity=identity))
    predictors = [w for w in weights if w["artifact_kind"] in {"PREDICTOR_WEIGHT", "SERIALIZED_PREDICTOR"}]
    return dict(model_id=model_id, role=_role(name), family=name, manifest_ref=str(path),
        manifest_sha256=file_sha(path), identity=identity, weights=weights, metadata=metadata,
        predictor_artifact_count=len(predictors), derived_calibrator_present=any(w["artifact_kind"] == "CALIBRATOR_NEEDS_PARENT" for w in weights),
        inventory_status="VERIFIED_ARTIFACTS" if weights and not errors else "REQUIRES_INSPECTION",
        errors=errors, inference_applicability="NOT_YET_CLASSIFIED")


def inventory_frozen_models(root):
    items = [inspect_manifest(name, path) for name, path in manifest_candidates(root)]
    unique = {}
    for item in items:
        key = item["model_id"]
        if key not in unique:
            unique[key] = item
            item["duplicate_manifest_refs"] = []
        else:
            unique[key]["duplicate_manifest_refs"].append(item["manifest_ref"])
    return dict(manifest_count=len(items), unique_inventory_unit_count=len(unique),
        model_count_claimed=False, items=list(unique.values()), sealed_read=False, returns_read=False, physical_fit_count=0)
