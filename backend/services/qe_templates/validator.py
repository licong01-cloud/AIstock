"""Side-effect-free validation for QE execution templates."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import re
from typing import Any

from backend.services.quantevolver.experiment_config import QE_RUNTIME_METADATA_KEYS
from backend.services.quantevolver.seed_contract import ensure_template_fixed_seeds

QE_STOCK_POOL_DATE_OUT_OF_WINDOW = "QE_STOCK_POOL_DATE_OUT_OF_WINDOW"
_FILTERED_POOL_DATE_RE = re.compile(r"filtered_pool[_-](\d{8})")

# Keep this local instead of importing ConfigComposer so the template validator
# stays side-effect-free and safe for the thin MCP server preflight.
_QE_DEFAULT_DATA_SPLIT = {
    "train_start": "2018-08-01",
    "train_end": "2022-12-31",
    "valid_start": "2023-01-01",
    "valid_end": "2024-06-30",
    "test_start": "2024-07-01",
    "test_end": "2026-04-28",
    "backtest_end": "2026-04-27",
}


def normalize_template_config(template_kind: str, config_json: Mapping[str, Any]) -> dict[str, Any]:
    """Return the canonical template config persisted and later materialized."""

    return ensure_template_fixed_seeds(template_kind, dict(config_json or {}))


def _parse_date(value: Any, *, context: str) -> datetime:
    if not value:
        raise ValueError(f"{context} is required")
    text = str(value)
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ValueError(f"{context} must be YYYY-MM-DD or YYYYMMDD, got {value!r}")


def _stock_pool_filtered_date(stock_pool: Any) -> datetime | None:
    if not stock_pool:
        return None
    match = _FILTERED_POOL_DATE_RE.search(str(stock_pool))
    if not match:
        return None
    return _parse_date(match.group(1), context="stock_pool filtered_pool date")


def _effective_test_end(data_split: Any) -> datetime:
    split = dict(data_split) if isinstance(data_split, Mapping) else {}
    return _parse_date(
        split.get("test_end") or _QE_DEFAULT_DATA_SPLIT["test_end"],
        context="data_split.test_end",
    )


def _validate_stock_pool_date(
    stock_pool: Any,
    *,
    data_split: Any,
    context: str,
) -> str | None:
    pool_date = _stock_pool_filtered_date(stock_pool)
    if pool_date is None:
        return None
    test_end = _effective_test_end(data_split)
    if pool_date <= test_end:
        return None
    return (
        f"{QE_STOCK_POOL_DATE_OUT_OF_WINDOW}: {context} stock_pool={stock_pool!r} "
        f"uses filtered_pool date {pool_date:%Y-%m-%d} after data_split.test_end={test_end:%Y-%m-%d}"
    )


def validate_qe_historical_stock_pool_window(config: Mapping[str, Any], *, context: str) -> list[str]:
    """Return stock-pool PIT window errors for QE historical template payloads."""

    errors: list[str] = []
    data_split = config.get("data_split")

    for source_name, candidate in (
        ("stock_pool", config.get("stock_pool")),
        ("custom_params.stock_pool", (config.get("custom_params") or {}).get("stock_pool") if isinstance(config.get("custom_params"), Mapping) else None),
        ("strategy_params.stock_pool", (config.get("strategy_params") or {}).get("stock_pool") if isinstance(config.get("strategy_params"), Mapping) else None),
    ):
        error = _validate_stock_pool_date(candidate, data_split=data_split, context=f"{context}.{source_name}")
        if error:
            errors.append(error)
    return errors


def validate_template_payload(template_kind: str, config_json: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    config = dict(config_json or {})
    try:
        config = normalize_template_config(template_kind, config)
    except ValueError as exc:
        errors.append(str(exc))
    errors.extend(validate_qe_historical_stock_pool_window(config, context="template"))
    if template_kind == "single_experiment":
        if config.get("alpha_mode") == "multi":
            errors.append("QE MCP v1 does not support multi-alpha experiment templates")
        if not config.get("factor_names"):
            errors.append("single_experiment config requires factor_names")
        if not config.get("model_id"):
            errors.append("single_experiment config requires model_id")
    elif template_kind == "custom_evo":
        loops = config.get("loops")
        if not isinstance(loops, list) or not loops:
            errors.append("custom_evo config requires non-empty loops")
        else:
            for idx, loop in enumerate(loops, start=1):
                if not isinstance(loop, Mapping):
                    errors.append(f"Loop {idx} must be an object")
                    continue
                if not loop.get("factor_keys"):
                    errors.append(f"Loop {idx} requires factor_keys")
                if not loop.get("model_id"):
                    errors.append(f"Loop {idx} requires model_id")
                strategy_params = loop.get("strategy_params")
                if isinstance(strategy_params, Mapping):
                    reserved = sorted(set(strategy_params).intersection(QE_RUNTIME_METADATA_KEYS))
                    if reserved:
                        warnings.append(
                            f"Loop {idx}: runtime metadata {reserved} belongs in runtime_flags, "
                            "not strategy_params; materialization will hoist it before execution"
                        )
                node_id = str(loop.get("node_id") or config.get("node_id") or "")
                model_id = str(loop.get("model_id") or "").lower()
                if node_id and node_id not in {"local", "wsl", "wsl2-5080"} and any(token in model_id for token in ("lstm", "gru", "transformer", "alstm")):
                    warnings.append(f"Loop {idx}: remote node should run CPU model only; treat this as a soft limit")
                loop_config = dict(loop)
                if "data_split" not in loop_config and isinstance(config.get("data_split"), Mapping):
                    loop_config["data_split"] = config.get("data_split")
                errors.extend(validate_qe_historical_stock_pool_window(loop_config, context=f"custom_evo.loops[{idx}]"))
    else:
        errors.append(f"unsupported template_kind: {template_kind}")
    return {"valid": not errors, "errors": errors, "warnings": warnings}
