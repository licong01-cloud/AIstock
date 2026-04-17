"""
QE Unified Engine — ExperimentConfig builder functions.

One builder per call path. Each builder:
  1. Extracts raw values from the path-specific data source.
  2. Resolves HMM snapshot_id -> model_path via HMMTrainingService (when needed).
  3. Returns a fully-constructed ExperimentConfig.

Callers then do:
    cfg = build_config_from_*(...)
    compose_res = composer.compose_experiment_in_memory(
        factor_names=cfg.factor_names,
        model_id=cfg.model_id,
        strategy_id=cfg.strategy_id,
        data_split=cfg.data_split,
        custom_params=cfg.build_custom_params(),
        experiment_name=cfg.experiment_name,
        skip_db_save=True,
        execution_algo=cfg.execution_algo,
        execution_algo_params=cfg.execution_algo_params,
        strategy_params=cfg.build_strategy_params(),
        node_id=cfg.node_id,
    )
"""
from __future__ import annotations

import json
import logging
from typing import Any

from .experiment_config import (
    ExperimentConfig, HmmConfig,
    AlphaGroup, MetaModelConfig, MultiAlphaConfig,
)

logger = logging.getLogger("aistock.quantevolver.experiment_config_builders")


# ── Internal helpers ───────────────────────────────────────────────────────────

def _resolve_hmm_snapshot(hmm_model_version_id: str) -> str:
    """Resolve a HMM snapshot ID to its model_path. Raises ValueError if not found."""
    from ..hmm_training_service import HMMTrainingService
    svc = HMMTrainingService()
    snapshot = svc.get_snapshot(hmm_model_version_id)
    if snapshot is None:
        raise ValueError(f"HMM snapshot {hmm_model_version_id!r} does not exist")
    return snapshot["model_path"]


def _parse_json_field(value: Any) -> Any:
    """Parse a field that may arrive as a JSON string or already be a dict/list."""
    if isinstance(value, str):
        return json.loads(value)
    return value


def _build_hmm_config(
    enable_sector_hmm: bool,
    hmm_model_version_id: str | None,
    sector_hmm_model_path: str | None,
    hmm_signal_preset: str | None,
    hmm_signal_presets: dict[str, Any] | None = None,
) -> HmmConfig | None:
    """Construct HmmConfig, resolving model_path if not already provided."""
    if not enable_sector_hmm:
        return None
    if not hmm_model_version_id:
        raise ValueError("enable_sector_hmm=True requires hmm_model_version_id")
    if not sector_hmm_model_path:
        sector_hmm_model_path = _resolve_hmm_snapshot(hmm_model_version_id)
    return HmmConfig(
        enable_sector_hmm=True,
        hmm_model_version_id=hmm_model_version_id,
        sector_hmm_model_path=sector_hmm_model_path,
        hmm_signal_preset=hmm_signal_preset,
        hmm_signal_presets=hmm_signal_presets,
    )


def _build_multi_alpha_config(raw: Any) -> MultiAlphaConfig | None:
    """Parse multi_alpha_config from DB JSON field (may be dict, JSON str, or None).

    Returns None if input is empty. Raises ValueError on invalid structure
    (handled by MultiAlphaConfig pydantic validators).
    """
    if not raw:
        return None
    parsed = _parse_json_field(raw)
    if not isinstance(parsed, dict):
        raise ValueError(
            f"multi_alpha_config must be dict, got {type(parsed).__name__}"
        )
    # Build nested pydantic models from dict
    groups = [AlphaGroup(**g) for g in parsed.get("alpha_groups", [])]
    meta_raw = parsed.get("meta_model") or {}
    meta = MetaModelConfig(**meta_raw) if isinstance(meta_raw, dict) else MetaModelConfig()
    return MultiAlphaConfig(
        alpha_groups=groups,
        meta_model=meta,
        execution_mode=parsed.get("execution_mode", "serial"),
        auto_selected=bool(parsed.get("auto_selected", False)),
    )


def _build_unfilled_handler_params(raw: Any) -> dict[str, Any]:
    """Normalise unfilled_handler_params from DB (may be JSON string or dict)."""
    if not raw:
        return {}
    parsed = _parse_json_field(raw)
    if not isinstance(parsed, dict):
        raise ValueError(
            f"unfilled_handler_params must be a dict, got {type(parsed).__name__}: {parsed!r}"
        )
    return parsed


# ── Path 1: run_experiment (routers/quantevolver.py:3255) ─────────────────────

def build_config_from_exp_record(
    exp_record: dict[str, Any],
    experiment_name: str | None = None,
) -> ExperimentConfig:
    """Build ExperimentConfig from a qe_experiments DB row (Path 1: run_experiment).

    Mirrors the extraction logic in quantevolver.py:3275-3314.
    """
    factor_names: list[str] = _parse_json_field(exp_record.get("factor_names") or [])
    data_split = _parse_json_field(exp_record.get("data_split"))
    custom_params: dict[str, Any] = dict(
        _parse_json_field(exp_record.get("custom_params") or {})
    )

    execution_algo: str | None = custom_params.pop("execution_algo", None)
    execution_algo_params: dict[str, Any] = custom_params.pop("execution_algo_params", None) or {}

    # HMM keys live inside custom_params in Path 1
    enable_sector_hmm: bool = bool(custom_params.pop("enable_sector_hmm", False))
    sector_hmm_model_path: str | None = custom_params.pop("sector_hmm_model_path", None)
    hmm_model_version_id: str | None = custom_params.pop("hmm_model_version_id", None)
    hmm_signal_preset: str | None = custom_params.pop("hmm_signal_preset", None)
    hmm_signal_presets: dict[str, Any] | None = custom_params.pop("hmm_signal_presets", None)

    # Path 1 stores sector_hmm_model_path directly (already resolved upstream)
    hmm = _build_hmm_config(
        enable_sector_hmm=enable_sector_hmm,
        hmm_model_version_id=hmm_model_version_id,
        sector_hmm_model_path=sector_hmm_model_path,
        hmm_signal_preset=hmm_signal_preset,
        hmm_signal_presets=hmm_signal_presets,
    )

    strategy_params = _parse_json_field(exp_record.get("strategy_params") or {})

    # ── Multi-Alpha (Phase 3) ────────────────────────────────────────────
    # 从 DB 读取 alpha_mode 和 multi_alpha_config (新增列，老记录为 NULL/默认)
    alpha_mode = exp_record.get("alpha_mode") or "single"
    multi_alpha_cfg = _build_multi_alpha_config(exp_record.get("multi_alpha_config"))

    return ExperimentConfig(
        factor_names=factor_names,
        model_id=exp_record.get("model_id") or "",
        strategy_id=exp_record.get("strategy_id"),
        data_split=data_split,
        hmm=hmm,
        execution_algo=execution_algo,
        execution_algo_params=execution_algo_params or None,
        strategy_params=strategy_params or None,
        extra_params=custom_params or None,
        experiment_name=experiment_name or exp_record.get("experiment_name"),
        alpha_mode=alpha_mode,
        multi_alpha_config=multi_alpha_cfg,
    )


# ── Path 2: submit_next_loop (qe_evolution_service.py:947) ────────────────────

def build_config_from_evolution_loop(
    config: dict[str, Any],
    task: dict[str, Any],
    experiment_name: str | None = None,
) -> ExperimentConfig:
    """Build ExperimentConfig for a standard auto-evolution loop (Path 2).

    config — reviewer validated_config or base_experiment config dict.
    task   — qe_evolution_tasks DB row.

    NOTE: Path 2 does not inject HMM (known gap in existing code). HmmConfig
    is left None until Path 2 is updated to carry HMM fields on the task record.
    """
    model_params_base: dict[str, Any] = dict(config.get("model_params") or {})

    effective_strategy_params: dict[str, Any] = dict(
        _parse_json_field(task.get("strategy_params") or {})
    )
    effective_execution_algo: str | None = task.get("execution_algo")
    effective_execution_algo_params: dict[str, Any] = dict(
        _parse_json_field(task.get("execution_algo_params") or {})
    )

    unfilled_handler: str | None = task.get("unfilled_handler")
    unfilled_handler_params = _build_unfilled_handler_params(
        task.get("unfilled_handler_params")
    )

    # Multi-Alpha 透传 (Path 2)
    alpha_mode = config.get("alpha_mode") or task.get("alpha_mode") or "single"
    multi_alpha_cfg = _build_multi_alpha_config(
        config.get("multi_alpha_config") or task.get("multi_alpha_config")
    )

    return ExperimentConfig(
        factor_names=config.get("factor_list") or [],
        model_id=config.get("model_id") or "",
        strategy_id=task.get("strategy_id") or config.get("strategy_id"),
        data_split=config.get("data_split"),
        stock_pool=task.get("stock_pool"),
        label_type=task.get("label_type"),
        hmm=None,  # Path 2 does not inject HMM (known gap)
        execution_algo=effective_execution_algo,
        execution_algo_params=effective_execution_algo_params or None,
        unfilled_handler=unfilled_handler,
        unfilled_handler_params=unfilled_handler_params or None,
        strategy_params=effective_strategy_params or None,
        model_params_base=model_params_base or None,
        node_id=task.get("node_id"),
        experiment_name=experiment_name,
        alpha_mode=alpha_mode,
        multi_alpha_config=multi_alpha_cfg,
    )


# ── Path 3: submit_strategy_evo_loop (qe_evolution_service.py:2695) ───────────

def build_config_from_strategy_evo_loop(
    base_config: dict[str, Any],
    loop_config: dict[str, Any],
    task: dict[str, Any],
    experiment_name: str | None = None,
) -> ExperimentConfig:
    """Build ExperimentConfig for a strategy backtest loop (Path 3, --backtest-only).

    base_config — merged config dict built from source loop + strategy overrides.
    loop_config — single entry from strategy_evo_config["loops"].
    task        — qe_evolution_tasks DB row.
    """
    model_params_base: dict[str, Any] = dict(base_config.get("model_params") or {})
    strategy_params: dict[str, Any] = dict(loop_config.get("strategy_params") or {})

    execution_algo: str | None = loop_config.get("execution_algo")
    execution_algo_params: dict[str, Any] = dict(
        loop_config.get("execution_algo_params") or {}
    )

    enable_sector_hmm: bool = bool(loop_config.get("enable_sector_hmm", False))
    hmm_model_version_id: str | None = loop_config.get("hmm_model_version_id")
    hmm_signal_preset: str | None = loop_config.get("hmm_signal_preset")
    hmm = _build_hmm_config(
        enable_sector_hmm=enable_sector_hmm,
        hmm_model_version_id=hmm_model_version_id,
        sector_hmm_model_path=None,  # always resolve from version_id
        hmm_signal_preset=hmm_signal_preset,
    )

    sector_blacklist: list[str] | None = loop_config.get("sector_blacklist") or None

    unfilled_handler: str | None = (
        loop_config.get("unfilled_handler") or task.get("unfilled_handler")
    )
    unfilled_handler_params = _build_unfilled_handler_params(
        loop_config.get("unfilled_handler_params") or task.get("unfilled_handler_params")
    )

    # Multi-Alpha 透传 (Path 3) — 策略演进不改因子/模型配置，原样保留
    alpha_mode = base_config.get("alpha_mode") or loop_config.get("alpha_mode") or "single"
    multi_alpha_cfg = _build_multi_alpha_config(
        base_config.get("multi_alpha_config") or loop_config.get("multi_alpha_config")
    )

    return ExperimentConfig(
        factor_names=base_config.get("factor_list") or [],
        model_id=base_config.get("model_id") or "",
        strategy_id=base_config.get("strategy_id"),
        data_split=base_config.get("data_split"),
        stock_pool=task.get("stock_pool"),
        label_type=task.get("label_type"),
        sector_blacklist=sector_blacklist,
        hmm=hmm,
        execution_algo=execution_algo,
        execution_algo_params=execution_algo_params or None,
        unfilled_handler=unfilled_handler,
        unfilled_handler_params=unfilled_handler_params or None,
        strategy_params=strategy_params or None,
        model_params_base=model_params_base or None,
        node_id=task.get("node_id"),
        experiment_name=experiment_name,
        alpha_mode=alpha_mode,
        multi_alpha_config=multi_alpha_cfg,
    )


# ── Path 4: submit_custom_evo_loop (qe_evolution_service.py:3301) ─────────────

def build_config_from_custom_evo_loop(
    loop_config: dict[str, Any],
    task: dict[str, Any],
    experiment_name: str | None = None,
) -> ExperimentConfig:
    """Build ExperimentConfig for a custom config loop (Path 4).

    loop_config — single entry from custom_evo_config["loops"].
    task        — qe_evolution_tasks DB row.

    This is the most complete path and is the canonical reference for
    build_custom_params() ordering.
    """
    factor_keys: list[str] = loop_config.get("factor_keys") or []
    factor_names: list[str] = [k.split("||")[0] for k in factor_keys]

    strategy_params: dict[str, Any] = dict(
        _parse_json_field(loop_config.get("strategy_params") or {})
    )
    execution_algo: str | None = loop_config.get("execution_algo")
    execution_algo_params: dict[str, Any] = dict(
        _parse_json_field(loop_config.get("execution_algo_params") or {})
    )
    data_split = loop_config.get("data_split")
    label_type: str | None = loop_config.get("label_type")

    enable_sector_hmm: bool = bool(loop_config.get("enable_sector_hmm", False))
    hmm_model_version_id: str | None = loop_config.get("hmm_model_version_id")
    hmm_signal_preset: str | None = loop_config.get("hmm_signal_preset")
    hmm = _build_hmm_config(
        enable_sector_hmm=enable_sector_hmm,
        hmm_model_version_id=hmm_model_version_id,
        sector_hmm_model_path=None,  # always resolve from version_id
        hmm_signal_preset=hmm_signal_preset,
    )

    sector_blacklist: list[str] | None = loop_config.get("sector_blacklist") or None
    stock_pool: str | None = loop_config.get("stock_pool")

    unfilled_handler: str | None = loop_config.get("unfilled_handler")
    unfilled_handler_params = _build_unfilled_handler_params(
        loop_config.get("unfilled_handler_params")
    )

    # backtest-only mode
    backtest_only: bool = bool(loop_config.get("backtest_only", False))
    model_source_task_id: str | None = loop_config.get("model_source_task_id")
    model_source_loop_index: int | None = loop_config.get("model_source_loop_index")

    # Multi-Alpha 支持 (Path 4 — 统一引擎路径)
    # strategy_evo_config.loops[i] 可嵌入 multi_alpha_config 字段
    alpha_mode = loop_config.get("alpha_mode") or "single"
    multi_alpha_cfg = _build_multi_alpha_config(loop_config.get("multi_alpha_config"))
    # 如果 multi-alpha 模式，factor_names 由各组聚合（仅作展示用，实际由 engine 处理）
    if alpha_mode == "multi" and multi_alpha_cfg:
        _all_factor_names = []
        _seen = set()
        for g in multi_alpha_cfg.alpha_groups:
            for fn in g.factor_names:
                if fn not in _seen:
                    _all_factor_names.append(fn)
                    _seen.add(fn)
        if _all_factor_names:
            factor_names = _all_factor_names

    return ExperimentConfig(
        factor_names=factor_names,
        model_id=loop_config.get("model_id") or "",
        strategy_id=loop_config.get("strategy_id"),
        data_split=data_split,
        stock_pool=stock_pool,
        label_type=label_type,
        sector_blacklist=sector_blacklist,
        hmm=hmm,
        execution_algo=execution_algo,
        execution_algo_params=execution_algo_params or None,
        unfilled_handler=unfilled_handler,
        unfilled_handler_params=unfilled_handler_params or None,
        strategy_params=strategy_params or None,
        backtest_only=backtest_only,
        model_source_task_id=model_source_task_id,
        model_source_loop_index=model_source_loop_index,
        node_id=task.get("node_id"),
        experiment_name=experiment_name,
        alpha_mode=alpha_mode,
        multi_alpha_config=multi_alpha_cfg,
    )


# ── Path 5: retry_loop (qe_evolution_service.py retry_loop) ─────────────────

def build_config_from_retry_loop(
    config: dict[str, Any],
    task: dict[str, Any],
    experiment_name: str | None = None,
) -> ExperimentConfig:
    """Build ExperimentConfig for retry_loop (backtest-only mode).

    config — loop_row["config_json"] dict with keys:
             factor_list, model_id, strategy_id, model_params, data_split.
    task   — qe_evolution_tasks DB row.

    model_params contains the merged custom_params from the original submission,
    including HMM settings if any.  Task-level overrides (stock_pool, label_type,
    strategy_params, unfilled_handler, execution_algo) are merged on top.
    """
    # model_params is the original custom_params snapshot
    model_params_base: dict[str, Any] = dict(config.get("model_params") or {})

    # Task-level strategy params overlay
    effective_strategy_params: dict[str, Any] = dict(
        _parse_json_field(task.get("strategy_params") or {})
    )
    effective_execution_algo: str | None = task.get("execution_algo")
    effective_execution_algo_params: dict[str, Any] = dict(
        _parse_json_field(task.get("execution_algo_params") or {})
    )

    # HMM: extract from model_params_base (where the original submission stored them)
    enable_sector_hmm: bool = bool(model_params_base.pop("enable_sector_hmm", False))
    hmm_model_version_id: str | None = model_params_base.pop("hmm_model_version_id", None)
    sector_hmm_model_path: str | None = model_params_base.pop("sector_hmm_model_path", None)
    hmm_signal_preset: str | None = model_params_base.pop("hmm_signal_preset", None)
    hmm = _build_hmm_config(
        enable_sector_hmm=enable_sector_hmm,
        hmm_model_version_id=hmm_model_version_id,
        sector_hmm_model_path=sector_hmm_model_path,
        hmm_signal_preset=hmm_signal_preset,
    )

    unfilled_handler: str | None = task.get("unfilled_handler")
    unfilled_handler_params = _build_unfilled_handler_params(
        task.get("unfilled_handler_params")
    )

    # Multi-Alpha 透传 (Path 5 — 重试) — 从原始 snapshot 恢复
    alpha_mode = config.get("alpha_mode") or "single"
    multi_alpha_cfg = _build_multi_alpha_config(config.get("multi_alpha_config"))

    return ExperimentConfig(
        factor_names=config.get("factor_list") or [],
        model_id=config.get("model_id") or "",
        strategy_id=task.get("strategy_id") or config.get("strategy_id"),
        data_split=config.get("data_split"),
        stock_pool=task.get("stock_pool"),
        label_type=task.get("label_type"),
        hmm=hmm,
        execution_algo=effective_execution_algo,
        execution_algo_params=effective_execution_algo_params or None,
        unfilled_handler=unfilled_handler,
        unfilled_handler_params=unfilled_handler_params or None,
        strategy_params=effective_strategy_params or None,
        model_params_base=model_params_base or None,
        node_id=task.get("node_id"),
        experiment_name=experiment_name,
        alpha_mode=alpha_mode,
        multi_alpha_config=multi_alpha_cfg,
    )


# ── Path 6: Multi-Alpha dedicated builder (Phase 3) ─────────────────────

def build_config_from_multi_alpha(
    multi_alpha_config,
    data_split=None,
    strategy_id=None,
    strategy_params=None,
    stock_pool=None,
    label_type=None,
    sector_blacklist=None,
    hmm_config=None,
    execution_algo=None,
    execution_algo_params=None,
    unfilled_handler=None,
    unfilled_handler_params=None,
    node_id=None,
    experiment_name=None,
):
    """Path 6 — 专门构建 Multi-Alpha 模式的 ExperimentConfig。

    由 Multi-Alpha API endpoints 和 QE Multi-Alpha engine 使用。
    factor_names 自动从各 AlphaGroup 聚合；model_id 由各组各自指定。
    """
    if isinstance(multi_alpha_config, dict):
        multi_alpha_config = _build_multi_alpha_config(multi_alpha_config)
    if multi_alpha_config is None:
        raise ValueError("multi_alpha_config is required for Path 6")

    all_factor_names = []
    seen = set()
    for g in multi_alpha_config.alpha_groups:
        for fn in g.factor_names:
            if fn not in seen:
                all_factor_names.append(fn)
                seen.add(fn)

    placeholder_model_id = multi_alpha_config.alpha_groups[0].model_id

    return ExperimentConfig(
        factor_names=all_factor_names,
        model_id=placeholder_model_id,
        strategy_id=strategy_id,
        data_split=data_split,
        stock_pool=stock_pool,
        label_type=label_type,
        sector_blacklist=sector_blacklist,
        hmm=hmm_config,
        execution_algo=execution_algo,
        execution_algo_params=execution_algo_params,
        unfilled_handler=unfilled_handler,
        unfilled_handler_params=unfilled_handler_params,
        strategy_params=strategy_params,
        node_id=node_id,
        experiment_name=experiment_name,
        alpha_mode="multi",
        multi_alpha_config=multi_alpha_config,
    )
