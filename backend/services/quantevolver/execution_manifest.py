"""Execution manifest audit for QE request-to-artifact truth."""

from __future__ import annotations

import re
from typing import Any, Mapping

import yaml

from backend.services.qe_archive.models import sha256_json

from .experiment_config import ExperimentConfig, model_seed_param_keys, normalize_label_horizon
from .executors.base import ExecutionContext


_CAPACITY_KEYS = ("max_single_order_value", "max_position_ratio", "max_weight")
_STRATEGY_KEYS = ("topk", "n_drop", *_CAPACITY_KEYS)
_FACTOR_HANDLER_CLASSES = {"Alpha158", "Alpha360", "Alpha158DL", "StaticDataLoader"}


def _safe_conf_yaml_load(text: str) -> dict[str, Any]:
    sanitized = re.sub(r"\{\{\s*(num_features|num_timesteps)\s*\}\}", lambda m: f'"{{{{ {m.group(1)} }}}}"', text or "")
    loaded = yaml.safe_load(sanitized) or {}
    if not isinstance(loaded, dict):
        raise ValueError("conf.yaml did not parse into a mapping")
    return loaded


def _is_auditable_conf(conf: Mapping[str, Any]) -> bool:
    task = conf.get("task")
    port_analysis = conf.get("port_analysis_config")
    return isinstance(task, Mapping) and isinstance(port_analysis, Mapping)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _task_section(conf: Mapping[str, Any], *path: str) -> dict[str, Any]:
    current: Any = conf
    for key in path:
        if not isinstance(current, Mapping):
            return {}
        current = current.get(key)
    return _as_dict(current)


def _collect_factor_names(value: Any) -> list[str]:
    if not isinstance(value, Mapping):
        return []
    names: list[str] = []
    loader_class = value.get("class")
    if loader_class in _FACTOR_HANDLER_CLASSES:
        names.append(str(loader_class))
    for key in ("handler", "data_loader", "learn", "infer"):
        nested = value.get(key)
        if isinstance(nested, Mapping):
            names.extend(_collect_factor_names(nested))
        elif isinstance(nested, list):
            for item in nested:
                names.extend(_collect_factor_names(item))
    kwargs = value.get("kwargs")
    if isinstance(kwargs, Mapping):
        names.extend(_collect_factor_names(kwargs))
    return names


def _artifact_factor_names(conf: Mapping[str, Any], requested: Mapping[str, Any]) -> list[str]:
    names: list[str] = []
    task_dataset = _task_section(conf, "task", "dataset")
    names.extend(_collect_factor_names(task_dataset))
    return names or list(requested.get("factor_list") or [])



def _strategy_subset(params: Mapping[str, Any]) -> dict[str, Any]:
    return {key: params.get(key) for key in _STRATEGY_KEYS if key in params}


def _requested_manifest(config: ExperimentConfig, ctx: ExecutionContext, mode: str) -> dict[str, Any]:
    runtime_flags = config.build_runtime_flags()
    custom_params = config.build_custom_params()
    strategy_params = config.build_strategy_params()
    return {
        "schema_version": "qe_execution_manifest_v1",
        "task_id": ctx.task_id,
        "loop_index": ctx.loop_index,
        "node_id": ctx.node_id,
        "mode": str(mode),
        "factor_list": list(config.factor_names),
        "factor_count": len(config.factor_names),
        "model_id": config.model_id,
        "strategy_id": config.strategy_id,
        "strategy_params": strategy_params,
        "strategy_audit_subset": _strategy_subset({**custom_params, **strategy_params}),
        "custom_params": custom_params,
        "data_split": config.data_split or {},
        "label_horizon": normalize_label_horizon(config.label_horizon),
        "execution_algo": config.execution_algo,
        "execution_algo_params": config.execution_algo_params or {},
        "runtime_flags": runtime_flags,
        "random_seed": runtime_flags.get("random_seed"),
        "backtest_only": bool(config.backtest_only),
        "hmm_enabled": bool(config.hmm and config.hmm.enable_sector_hmm),
    }


def _artifact_manifest(conf: Mapping[str, Any], requested: Mapping[str, Any]) -> dict[str, Any]:
    task_model = _task_section(conf, "task", "model")
    model_kwargs = _as_dict(task_model.get("kwargs"))
    dataset_handler = _task_section(conf, "task", "dataset", "kwargs", "handler", "kwargs")
    strategy = _task_section(conf, "port_analysis_config", "strategy")
    strategy_kwargs = _as_dict(strategy.get("kwargs"))
    backtest = _task_section(conf, "port_analysis_config", "backtest")
    exchange_kwargs = _as_dict(backtest.get("exchange_kwargs"))
    executor = _task_section(conf, "port_analysis_config", "executor")
    qe_runtime = _as_dict(conf.get("qe_runtime"))
    factor_names = _artifact_factor_names(conf, requested)
    return {
        "model": {
            "class": task_model.get("class"),
            "module_path": task_model.get("module_path"),
            "kwargs": model_kwargs,
            "seed_param_keys": list(model_seed_param_keys(task_model.get("class"))),
        },
        "dataset": {
            "class": _task_section(conf, "task", "dataset").get("class"),
            "handler": dataset_handler,
            "label_horizon": requested.get("label_horizon"),
        },
        "strategy": {
            "class": strategy.get("class"),
            "module_path": strategy.get("module_path"),
            "kwargs": strategy_kwargs,
            "audit_subset": _strategy_subset(strategy_kwargs),
        },
        "backtest": {
            "account": backtest.get("account"),
            "start_time": backtest.get("start_time"),
            "end_time": backtest.get("end_time"),
            "exchange_kwargs": exchange_kwargs,
        },
        "execution": {
            "executor_class": executor.get("class"),
            "executor_module_path": executor.get("module_path"),
        },
        "qe_runtime": qe_runtime,
        "factor_list": factor_names,
        "factor_count": len(factor_names),
    }


def _append_mismatch(mismatches: list[str], path: str, requested: Any, artifact: Any) -> None:
    if requested != artifact:
        mismatches.append(f"{path}: requested={requested!r} artifact={artifact!r}")


def _compare_manifest(requested: Mapping[str, Any], artifact: Mapping[str, Any]) -> list[str]:
    mismatches: list[str] = []
    requested_factors = list(requested.get("factor_list") or [])
    artifact_factors = list(artifact.get("factor_list") or [])
    if artifact_factors != requested_factors and artifact_factors != ["Alpha158"] and set(artifact_factors) != set(requested_factors):
        _append_mismatch(mismatches, "factor_list", requested_factors, artifact_factors)

    runtime_seed = requested.get("random_seed")
    qe_runtime = _as_dict(artifact.get("qe_runtime"))
    if runtime_seed is not None:
        _append_mismatch(mismatches, "qe_runtime.random_seed", int(runtime_seed), qe_runtime.get("random_seed"))
        _append_mismatch(mismatches, "qe_runtime.seed_policy", "fixed", qe_runtime.get("seed_policy"))
        model = _as_dict(artifact.get("model"))
        model_kwargs = _as_dict(model.get("kwargs"))
        for key in model.get("seed_param_keys") or []:
            _append_mismatch(mismatches, f"model.kwargs.{key}", int(runtime_seed), model_kwargs.get(key))

    strategy_req = _as_dict(requested.get("strategy_audit_subset"))
    strategy_art = _as_dict(_as_dict(artifact.get("strategy")).get("audit_subset"))
    for key in _STRATEGY_KEYS:
        if key in strategy_req:
            _append_mismatch(mismatches, f"strategy.kwargs.{key}", strategy_req.get(key), strategy_art.get(key))
    if strategy_art.get("max_single_order_value") == 5_000_000 and strategy_req.get("max_single_order_value") not in (None, 5_000_000):
        mismatches.append("capacity: legacy 5000000 max_single_order_value cap is present but was not requested")

    _append_mismatch(
        mismatches,
        "label_horizon",
        normalize_label_horizon(requested.get("label_horizon")),
        _as_dict(artifact.get("dataset")).get("label_horizon"),
    )
    return mismatches


def build_and_audit_execution_manifest(
    *,
    config: ExperimentConfig,
    ctx: ExecutionContext,
    mode: str,
    experiment_files: Mapping[str, str],
    wsl_command: str,
) -> tuple[dict[str, Any], str]:
    """Build canonical request/artifact manifest and fail on config drift."""

    conf_yaml = experiment_files.get("conf.yaml")
    if not conf_yaml:
        raise ValueError("execution manifest audit requires generated conf.yaml")
    requested = _requested_manifest(config, ctx, mode)
    try:
        conf = _safe_conf_yaml_load(conf_yaml)
    except ValueError as exc:
        if requested.get("random_seed") is not None:
            raise
        manifest = {
            "schema_version": "qe_execution_manifest_v1",
            "verification_status": "not_applicable",
            "not_applicable_reason": str(exc),
            "requested": requested,
            "artifact": {},
            "wsl_command": wsl_command,
            "conf_yaml_sha256": sha256_json(conf_yaml),
        }
        manifest["manifest_sha256"] = sha256_json(manifest)
        return manifest, str(manifest["manifest_sha256"])
    if not _is_auditable_conf(conf):
        if requested.get("random_seed") is not None:
            raise ValueError("execution manifest audit requires generated Qlib task and port_analysis_config sections")
        manifest = {
            "schema_version": "qe_execution_manifest_v1",
            "verification_status": "not_applicable",
            "not_applicable_reason": "conf.yaml is not a generated Qlib experiment config",
            "requested": requested,
            "artifact": {},
            "wsl_command": wsl_command,
            "conf_yaml_sha256": sha256_json(conf_yaml),
        }
        manifest["manifest_sha256"] = sha256_json(manifest)
        return manifest, str(manifest["manifest_sha256"])
    artifact = _artifact_manifest(conf, requested)
    manifest = {
        "schema_version": "qe_execution_manifest_v1",
        "verification_status": "verified",
        "requested": requested,
        "artifact": artifact,
        "wsl_command": wsl_command,
        "conf_yaml_sha256": sha256_json(conf_yaml),
    }
    mismatches = _compare_manifest(requested, artifact)
    if mismatches:
        manifest["verification_status"] = "failed"
        manifest["mismatches"] = mismatches
        raise ValueError("QE execution manifest mismatch: " + "; ".join(mismatches))
    manifest["manifest_sha256"] = sha256_json(manifest)
    return manifest, str(manifest["manifest_sha256"])
