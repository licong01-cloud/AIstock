"""Read-only dependency projection from existing admitted, frozen packages."""
from __future__ import annotations

import ast
from dataclasses import asdict, dataclass
from datetime import date
import hashlib
from typing import Mapping, Sequence

from .catalog import RULE_VERSION, SCOPE_VERSION, canonical_dataset
from .contracts import digest, effective_partition


class DependencyUnknown(ValueError):
    pass


@dataclass(frozen=True)
class ConsumerDependencies:
    datasets: tuple[str, ...]
    fields: tuple[str, ...]
    window_trading_days: int
    identity: str


def field_datasets():
    # These are the existing shared source-to-factor names, not a second mapping.
    from backend.data_service import qe_data_service as data
    from backend.data_service.moneyflow_contract import MONEYFLOW_FIELD_MAP
    result = {field: "kline_daily_raw" for field in ("open", "high", "low", "close", "volume", "amount", "vwap")}
    result["factor"] = "adj_factor"
    for dataset, fields in (("daily_basic", data.DAILY_BASIC_FIELD_MAP.values()),
        ("moneyflow_ts", MONEYFLOW_FIELD_MAP.values()), ("bak_basic", data.BAK_BASIC_FIELD_MAP.values()),
        ("cyq_perf", data.CYQ_PERF_FIELD_MAP.values()), ("margin_detail", data.MARGIN_DETAIL_FIELD_MAP.values()),
        ("sector_data", data.SECTOR_DATA_COLUMNS)):
        result.update({field: dataset for field in fields})
    return result


def package_dependencies(manifest, asset_store) -> ConsumerDependencies:
    """Never execute factor/model code to discover dependencies."""
    from backend.services.strategy_package.advisory_input_projection import (
        project_advisory_inputs, get_strategy_package_inference_required_window,
    )
    projection = project_advisory_inputs(manifest)
    mapping = field_datasets()
    fields = {"open", "high", "low", "close", "volume", "amount", "factor"}
    identities = []
    for factor in manifest.factor_set:
        if not factor.asset_ref or not factor.sha256:
            raise DependencyUnknown(f"factor input definition unavailable: {factor.factor_id}")
        payload = asset_store.get(factor.asset_ref)
        if hashlib.sha256(payload).hexdigest() != factor.sha256:
            raise DependencyUnknown(f"frozen factor identity differs: {factor.factor_id}")
        try:
            tree = ast.parse(payload.decode("utf-8-sig"))
        except (SyntaxError, UnicodeError) as exc:
            raise DependencyUnknown(f"factor input definition unreadable: {factor.factor_id}") from exc
        found, derived = set(), set()
        frames = {"data", "df", "df_factor", "df_pv"}
        for node in ast.walk(tree):
            if (isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Store)
                    and isinstance(node.value, ast.Name) and node.value.id in frames
                    and isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str)):
                derived.add(node.slice.value.lstrip("$"))
        dynamic = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id in frames:
                if node.attr in mapping:
                    found.add(node.attr)
            if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name) and node.value.id in frames:
                if not isinstance(node.ctx, ast.Load):
                    continue
                keys = node.slice.elts if isinstance(node.slice, (ast.List, ast.Tuple)) else [node.slice]
                for key in keys:
                    if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                        dynamic = True
                        continue
                    field = key.value.lstrip("$")
                    if field in mapping:
                        found.add(field)
                    elif field not in derived:
                        dynamic = True
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name) and node.func.value.id in frames
                    and node.func.attr in {"get", "filter", "__getitem__"}):
                if (not node.args or not isinstance(node.args[0], ast.Constant)
                        or node.args[0].value not in mapping):
                    dynamic = True
                else:
                    found.add(node.args[0].value)
        if not found or dynamic:
            raise DependencyUnknown(f"factor dependencies cannot be resolved deterministically: {factor.factor_id}")
        fields.update(found)
        identities.append(factor.sha256)
    if not manifest.factor_set and not (manifest.runtime_assets and manifest.runtime_assets.alpha158.enabled):
        raise DependencyUnknown("admitted package has no resolvable input definitions")
    windows = [leg.required_window for leg in projection.legs]
    if not windows:
        raise DependencyUnknown("admitted package input window is unresolved")
    datasets = tuple(sorted({mapping[field] for field in fields}))
    # Never certify a factor-only window for a sequence model whose admitted
    # sequence length is not exposed by the existing immutable contract. The
    # actual input session still supports such consumers; readiness is UNKNOWN.
    stateless = {"lgbmodel", "lightgbm", "lgb", "xgbmodel", "xgboost", "linear", "linearmodel", "catboost", "catboostmodel"}
    if any(str(model.model_type or "").lower() not in stateless for model in manifest.model_asset):
        raise DependencyUnknown("model input sequence window is not deterministically declared")
    window = max(max(windows)+5, *(get_strategy_package_inference_required_window(leg.factor_order)
                                 for leg in projection.legs))
    identity = digest({"manifest": manifest.manifest_sha256, "assets": identities,
                       "fields": sorted(fields), "datasets": datasets, "window": window})
    return ConsumerDependencies(datasets, tuple(sorted(fields)), window, identity)


def project_readiness(dependencies: ConsumerDependencies | None, dates: Sequence[date], partitions: Mapping,
                      *, pit_binding: Mapping | None, reason: str | None = None):
    refs, blockers = [], []
    if dependencies is None or not dependencies.datasets or not dates or not pit_binding:
        return {"consumer_ready": False, "prediction_ready": False, "state": "UNKNOWN",
                "reason": reason or "dependency_window_or_global_pit_unresolved", "partition_refs": []}
    for day in dates:
        for dataset in dependencies.datasets:
            row = partitions.get((day, canonical_dataset(dataset)))
            effective = effective_partition(row, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)
            if not effective["source_complete"]:
                blockers.append({"dataset": dataset, "fact_date": str(day), "state": effective["state"]})
            else:
                refs.append({"dataset": dataset, "fact_date": str(day), **{key: row.get(key) for key in
                    ("data_revision", "validation_id", "scope_version", "rule_version")}})
    binding = digest({"dependencies": asdict(dependencies), "dates": dates, "partitions": refs, "pit": pit_binding})
    return {"consumer_ready": not blockers, "prediction_ready": not blockers,
            "state": "READY" if not blockers else "WAITING", "blockers": blockers,
            "partition_refs": refs, "input_binding_sha256": binding,
            "dependencies": asdict(dependencies), "global_pit": dict(pit_binding),
            "historical_first_availability_claimed": False, "prediction_started": False}
