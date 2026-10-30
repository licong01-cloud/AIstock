"""Read-only dependency projection from existing admitted, frozen packages."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
import hashlib
import json
import re
from typing import Mapping, Sequence

from .catalog import RULE_VERSION, SCOPE_VERSION, canonical_dataset
from .contracts import digest, effective_partition
from .dependency_source import DependencyUnknown, factor_fields, factor_window, field_sources
from .frozen_metadata import dataset_sequence_length


@dataclass(frozen=True)
class ConsumerDependencies:
    datasets: tuple[str, ...]
    fields: tuple[str, ...]
    window_trading_days: int
    identity: str
    sequence_lengths: tuple[tuple[str, int], ...] = ()


def field_datasets():
    return field_sources()


def _asset(asset, store):
    if not asset.asset_ref or not asset.sha256:
        raise DependencyUnknown("frozen input asset identity unavailable")
    payload = store.get(asset.asset_ref)
    if hashlib.sha256(payload).hexdigest() != asset.sha256:
        raise DependencyUnknown("frozen input asset identity differs")
    return payload


def _sequence_lengths(manifest, store):
    from backend.services.strategy_package.frozen_runtime_self_check import runtime_asset_admission_status
    from backend.services.strategy_package.models import AlphaMode
    models = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    if manifest.alpha_mode == AlphaMode.MULTI_ALPHA:
        # Every declared seed must be available; a primary seed never certifies
        # the parent. Native asset slicing performs the existing identity check.
        from backend.services.strategy_package.multi_alpha_live import (
            _multi_alpha_evidence, _parent_leg_runtime_slices, _seed_leg_slices,
        )
        legs = _parent_leg_runtime_slices(manifest, evidence=_multi_alpha_evidence(manifest), package_id=manifest.package_id)
        models = [seed.model_asset for leg in legs for seed in _seed_leg_slices(leg)]
    admitted, _ = runtime_asset_admission_status(manifest)
    kind = ((manifest.source_evidence or {}).get("runtime_asset_admission", {}).get("self_check_summary", {}).get("model_kind")) if admitted else None
    lengths = []
    for model in models:
        if model.preprocessor_asset is None:
            # Exact existing native loader contract, independent of model names.
            # Explicit sequence types without a paired asset remain unknown.
            if str(model.model_type or "").lower() in {"generalptnn", "lstm", "tcn"}:
                raise DependencyUnknown("model input sequence window is not deterministically declared")
            length = 1
        else:
            raw = _asset(model.preprocessor_asset, store)
            if kind in {"lgb", "xgb", "linear", "catboost"}:
                length = 1
            else:
                try:
                    length = dataset_sequence_length(raw)
                except ValueError as exc:
                    raise DependencyUnknown(f"model sequence window unavailable: {model.model_id}: {exc}") from exc
        lengths.append((model.model_id, length))
    if not lengths:
        raise DependencyUnknown("model input sequence window is empty")
    return tuple(lengths)


def package_dependencies(manifest, asset_store) -> ConsumerDependencies:
    """Never execute factor/model code to discover dependencies."""
    from backend.services.strategy_package.advisory_input_projection import (
        project_advisory_inputs, get_strategy_package_inference_required_window,
    )
    projection = project_advisory_inputs(manifest)
    mapping = field_datasets()
    fields = {"open", "high", "low", "close", "volume", "amount", "factor"}
    identities, code_windows = [], []
    for factor in manifest.factor_set:
        if not factor.asset_ref or not factor.sha256:
            raise DependencyUnknown(f"factor input definition unavailable: {factor.factor_id}")
        payload = _asset(factor, asset_store)
        try:
            found = factor_fields(payload.decode("utf-8-sig"), mapping)
            code_windows.append(factor_window(payload.decode("utf-8-sig")))
        except (ValueError, UnicodeError) as exc:
            raise DependencyUnknown(f"factor dependencies cannot be resolved deterministically: {factor.factor_id}: {exc}") from exc
        fields.update(found)
        identities.append(factor.sha256)
    alpha = manifest.runtime_assets.alpha158 if manifest.runtime_assets else None
    if alpha and alpha.enabled:
        try:
            schema = json.loads(_asset(alpha, asset_store))
            if schema["aliases"] != list(alpha.aliases):
                raise ValueError("Alpha158 alias order differs")
            features = schema["loader_node"]["kwargs"]["config"]["feature"]
            if not isinstance(features, list) or len(features) != 2 or features[1] != schema["aliases"]:
                raise ValueError("Alpha158 loader alias order differs")
            expressions = features[0]
            if (not isinstance(expressions, list) or len(expressions) != len(schema["aliases"])
                    or not expressions or not all(isinstance(expr, str) and expr.strip() for expr in expressions)):
                raise ValueError("Alpha158 expressions unavailable")
            for expression in expressions:
                for name in re.findall(r"\$([A-Za-z_][A-Za-z_0-9]*)", expression):
                    if name not in mapping:
                        raise ValueError("unknown Alpha158 source field: " + name)
                    fields.add(name)
            identities.append(alpha.sha256)
        except (KeyError, TypeError, ValueError) as exc:
            raise DependencyUnknown("frozen Alpha158 source schema unavailable") from exc
    if not manifest.factor_set and not (manifest.runtime_assets and manifest.runtime_assets.alpha158.enabled):
        raise DependencyUnknown("admitted package has no resolvable input definitions")
    windows = [leg.required_window for leg in projection.legs]
    if not windows:
        raise DependencyUnknown("admitted package input window is unresolved")
    datasets = tuple(sorted({dataset for field in fields for dataset in mapping[field]}))
    lengths = _sequence_lengths(manifest, asset_store)
    window = max(max(windows), max(code_windows, default=1), *(get_strategy_package_inference_required_window(leg.factor_order)
                                 for leg in projection.legs)) + max(n for _, n in lengths) - 1 + 5
    identity = digest({"manifest": manifest.manifest_sha256, "assets": identities,
                       "fields": sorted(fields), "datasets": datasets, "window": window,
                       "sequence_lengths": lengths, "rule": "frozen_dependency_projection_v2"})
    return ConsumerDependencies(datasets, tuple(sorted(fields)), window, identity, lengths)


def project_readiness(dependencies: ConsumerDependencies | None, dates: Sequence[date], partitions: Mapping,
                      *, pit_binding: Mapping | None, reason: str | None = None,
                      input_scope: Mapping | None = None, members=None):
    refs, blockers = [], []
    if dependencies is None or not dependencies.datasets or not dates or not pit_binding:
        return {"consumer_ready": False, "prediction_ready": False, "state": "UNKNOWN",
                "reason": reason or "dependency_window_or_global_pit_unresolved", "partition_refs": []}
    for day in dates:
        for dataset in dependencies.datasets:
            row = partitions.get((day, canonical_dataset(dataset)))
            effective = effective_partition(row, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)
            projection = None
            if not effective["source_complete"] and input_scope is not None and members is not None:
                from .advisory_scope import partition_projection
                projection = partition_projection(row, input_scope, members)
            if not effective["source_complete"] and projection is None:
                blockers.append({"dataset": dataset, "fact_date": str(day), "state": effective["state"]})
            else:
                refs.append({"dataset": dataset, "fact_date": str(day), **{key: row.get(key) for key in
                    ("data_revision", "validation_id", "scope_version", "rule_version")},
                    **({"consumer_projection": projection} if projection is not None else {})})
    scope_fields = {"input_scope": dict(input_scope)} if input_scope is not None else {}
    quality = {"outside_scope_gap_count": sum(len(r.get("consumer_projection", {}).get("outside_scope", [])) for r in refs),
               "missing_chip_key_count": sum(len(r.get("consumer_projection", {}).get("missing_as_nan", [])) for r in refs),
               "source_completeness_unchanged": True} if input_scope is not None else None
    binding = digest({"dependencies": asdict(dependencies), "dates": dates, "partitions": refs,
                      "pit": pit_binding, **scope_fields})
    return {"consumer_ready": not blockers, "prediction_ready": not blockers,
            "state": "READY" if not blockers else "WAITING", "blockers": blockers,
            "partition_refs": refs, "input_binding_sha256": binding,
            "dependencies": asdict(dependencies), "global_pit": dict(pit_binding),
            "historical_first_availability_claimed": False, "prediction_started": False, **scope_fields,
            **({"input_quality": quality} if quality is not None else {})}
