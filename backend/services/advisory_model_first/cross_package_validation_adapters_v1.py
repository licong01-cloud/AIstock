"""Only frozen inference adapters; original model policy and encoders stay intact."""
from __future__ import annotations

from dataclasses import dataclass
import ast
import importlib
import importlib.util
import json
from pathlib import Path
import sys

from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import canonical_sha, file_sha


MODULE_PREFIX = "backend.services.advisory_model_first."
# Actual current source, not guessed family aliases or calls to train/pipelines.
FIXED5 = {
    "advisory_generic_price_5td_v1_20261006": ("generic_price_5td_models_v1", "GenericPrice5TDFitV1", "generic_price_5td_inference_v1", "query_price_nodes_v1"),
    "advisory_generic_minute_price_5td_v1_20261006": ("generic_minute_price_5td_models_v1", "GenericMinutePrice5TDFitV1", "generic_minute_price_5td_inference_v1", "query_minute_price_nodes_v1"),
    "advisory_generic_volume_path_price_5td_v1_20261006": ("generic_volume_path_price_5td_model_v1", "GenericVolumePathPrice5TDFitV1", "generic_volume_path_price_5td_model_v1", "query_volume_path_nodes_v1"),
    "advisory_generic_joint_distribution_price_5td_v1_20261007": ("generic_joint_distribution_price_5td_model_v1", "GenericJointDistributionPrice5TDFitV1", "generic_joint_distribution_price_5td_model_v1", "query_joint_price_nodes_v1"),
    "advisory_generic_ordered_path_price_5td_v1_20261007": ("generic_ordered_path_price_5td_model_v1", "GenericOrderedPathPrice5TDFitV1", "generic_ordered_path_price_5td_model_v1", "query_ordered_path_nodes_v1"),
    "advisory_generic_return_volume_price_5td_v1_20261007": ("generic_return_volume_price_5td_model_v1", "GenericReturnVolumePrice5TDFitV1", "generic_return_volume_price_5td_model_v1", "query_return_volume_nodes_v1"),
    "advisory_generic_moneyflow_price_5td_v1_20261008": ("generic_moneyflow_price_5td_models_v1", "GenericMoneyflowPrice5TDFitV1", "generic_moneyflow_price_5td_models_v1", "query_moneyflow_nodes_v1"),
}


@dataclass(frozen=True)
class FrozenInferenceUnit:
    model_id: str
    arm: str
    fitted: object
    query: object
    manifest_sha256: str
    weight_sha256: str
    family: str


def _checked_member(item, filename):
    manifest_path = Path(item["manifest_ref"])
    if file_sha(manifest_path) != item["manifest_sha256"]:
        raise ValueError("frozen inventory manifest changed")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    path = manifest_path.parent/filename
    descriptor = manifest["files"][filename]
    if (path.resolve().parent != manifest_path.parent.resolve() or file_sha(path) != descriptor["sha256"]
            or path.stat().st_size != descriptor["size_bytes"]):
        raise ValueError("frozen inference member differs")
    return json.loads(path.read_text(encoding="utf-8")), descriptor["sha256"]


def load_selection_context_source(descriptor):
    """Reuse the original two pure modules, not an old pipeline or a refit.

    Explicit SHA-bound source paths are research inputs. Dependencies either
    match whole files or the two unchanged numeric helper function ASTs.
    """
    if descriptor.get("family") != "advisory_generic_selection_context_price_5td_v1_20261007":
        raise ValueError("original source family differs")
    for member in descriptor["dependencies"]:
        path = Path(member["current_path"])
        if file_sha(path) != member["sha256"]:
            raise ValueError("original pure source dependency changed")
        if "function_sha256" in member:
            definitions = {node.name: canonical_sha(ast.dump(node, include_attributes=False))
                for node in ast.parse(path.read_text(encoding="utf-8-sig")).body
                if isinstance(node, ast.FunctionDef)}
            if any(definitions.get(k) != v for k, v in member["function_sha256"].items()):
                raise ValueError("original numeric helper semantics differ")
    names = ("generic_selection_context_price_5td_contracts_v1", "generic_selection_context_price_5td_model_v1")
    if [m["module"] for m in descriptor["modules"]] != list(names):
        raise ValueError("only the original selection-context pure modules are allowed")
    for member in descriptor["modules"]:
        path = Path(member["path"])
        if (not path.is_absolute() or path.drive.upper() == "C:" or path.suffix != ".py"
                or path.stat().st_size > 65536 or file_sha(path) != member["sha256"]):
            raise ValueError("original pure source path/hash differs")
        name = MODULE_PREFIX+member["module"]
        if name in sys.modules:
            if file_sha(sys.modules[name].__file__) != member["sha256"]:
                raise ValueError("original pure module collides with another source")
            continue
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
            if file_sha(path) != member["sha256"]:
                raise ValueError("original source changed while loading")
        except BaseException:
            sys.modules.pop(name, None)
            raise
    return sys.modules[MODULE_PREFIX+names[-1]]


def load_fixed5_units(item, *, original_source=None):
    """Identity/encoding validation is correctness, not a new package alpha gate."""
    family = item["family"]
    if family == "advisory_population_transfer_price_5td_v1_20261008":
        module = importlib.import_module(MODULE_PREFIX+"generic_population_price_5td_model_v1")
        body, digest = _checked_member(item, "models.json")
        if set(body) != {"matched_anchor", "candidate_transfer"}:
            raise ValueError("population frozen arms differ")
        result = []
        for arm in sorted(body):
            model_root = Path(item["manifest_ref"]).parent.parent/"fits"/arm/"trained"
            header = json.loads((model_root/"manifest.json").read_text(encoding="utf-8"))
            if (header["stage_sha256"] != body[arm]["stage_sha256"] or header["files"] != body[arm]["files"]
                    or canonical_sha({k: v for k, v in header.items() if k != "stage_sha256"}) != header["stage_sha256"]):
                raise ValueError("population frozen arm descriptor differs")
            payload, model_digest = _checked_member(dict(manifest_ref=str(model_root/"manifest.json"),
                manifest_sha256=file_sha(model_root/"manifest.json")), "model.json")
            fitted = module.model_from_payload_v1(payload)
            if fitted.model_sha256 != body[arm]["model_sha256"] or fitted.recipe["arm"] != arm:
                raise ValueError("population frozen model/arm differs")
            result.append(FrozenInferenceUnit(item["model_id"], arm, fitted,
                module.query_population_price_nodes_v1, item["manifest_sha256"], model_digest, family))
        return result
    if family == "advisory_generic_selection_context_price_5td_v1_20261007" and original_source:
        module = load_selection_context_source(original_source)
        body, digest = _checked_member(item, "model.json")
        body["intervals_bps"] = tuple(tuple(interval) for interval in body["intervals_bps"])
        fitted = module.GenericSelectionContextPrice5TDFitV1(**body)
        module.validate_fit(fitted)
        return [FrozenInferenceUnit(item["model_id"], "candidate", fitted,
            module.query_selection_context_nodes_v1, item["manifest_sha256"], digest, family)]
    if family not in FIXED5:
        raise LookupError(f"no current pure-inference source registered for {family}")
    module_name, constructor, query_module, query_name = FIXED5[family]
    module = importlib.import_module(MODULE_PREFIX+module_name)
    query = getattr(importlib.import_module(MODULE_PREFIX+query_module), query_name)
    body, digest = _checked_member(item, "model.json")
    if "intervals_bps" in body:
        body["intervals_bps"] = tuple(tuple(interval) for interval in body["intervals_bps"])
    if "forest" in body:
        body["forest"] = tuple(body["forest"])
    fitted = getattr(module, constructor)(**body)
    validate = getattr(module, "validate_fit", None) or getattr(module, "validate_fit_v1")
    validate(fitted)
    arms = ["matched", "candidate"] if family in {
        "advisory_generic_price_5td_v1_20261006", "advisory_generic_minute_price_5td_v1_20261006",
        "advisory_generic_volume_path_price_5td_v1_20261006"} else ["candidate"]
    return [FrozenInferenceUnit(item["model_id"], arm, fitted, query, item["manifest_sha256"], digest, family) for arm in arms]


def query_frozen_fixed5(unit, *, d_features, scenario_gap_bps):
    kwargs = dict(fitted=unit.fitted, features=d_features, scenario_gap_bps=scenario_gap_bps)
    if unit.family in {"advisory_generic_price_5td_v1_20261006", "advisory_generic_minute_price_5td_v1_20261006",
                       "advisory_generic_volume_path_price_5td_v1_20261006"}:
        kwargs["arm"] = unit.arm
    return unit.query(**kwargs)


def inspect_fixed5_loadability(inventory):
    items = []
    for item in inventory["items"]:
        if item["role"] != "FIXED_5TD_ENTRY":
            continue
        try:
            units = load_fixed5_units(item)
            items.append(dict(model_id=item["model_id"], family=item["family"], status="FROZEN_MODEL_LOADABLE",
                units=[dict(arm=u.arm, weight_sha256=u.weight_sha256, model_sha256=u.fitted.model_sha256) for u in units],
                query_qualified=False, input_ready=False, physical_fit_count=0))
        except (LookupError, AttributeError, ModuleNotFoundError) as exc:
            items.append(dict(model_id=item["model_id"], family=item["family"], status="SOURCE_UNAVAILABLE",
                reason=str(exc), physical_fit_count=0))
        except (ValueError, TypeError, KeyError, OSError) as exc:
            items.append(dict(model_id=item["model_id"], family=item["family"], status="ARTIFACT_REQUIRES_INSPECTION",
                reason=str(exc), physical_fit_count=0))
    return dict(items=items, item_identity_sha256=canonical_sha(items), inference_ran=False, outcomes_read=False, physical_fit_count=0)
