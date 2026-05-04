"""QE runtime-contract helpers for minute-backed experiments.

The StrategyPackage and QE warehouse paths need an explicit, reproducible
runtime contract.  The execution algorithm remains the authority; frequency
fields are derived compatibility metadata and must not silently convert
historical daily/unknown runs into minute runs.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


QE_MINUTE_RUNTIME_CONTRACT_VERSION = "qe_minute_runtime_contract_v1"
QE_DEFAULT_MINUTE_EXECUTION_ALGO = "TWAP"
QE_DAILY_EXECUTION_ALGOS = {"CLOSE_PRICE"}
QE_RUNTIME_CONTRACT_KEYS = {
    "runtime_mode",
    "bar_freq",
    "backtest_freq",
    "execution_algo",
    "execution_algo_params",
    "runtime_contract_version",
    "runtime_contract_source",
}


def parse_json_mapping(value: Any) -> dict[str, Any]:
    """Return a JSON-like mapping as a dict; non-object inputs become {}."""

    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, Mapping):
            return dict(parsed)
    return {}


def normalize_qe_execution_algo(value: Any) -> str | None:
    """Normalize a QE execution algorithm code without constraining future algos."""

    text = str(value or "").strip().upper()
    if not text or text in {"NONE", "NULL"}:
        return None
    if text == "DEFAULT":
        return QE_DEFAULT_MINUTE_EXECUTION_ALGO
    return text


def normalize_qe_minute_freq(value: Any) -> str | None:
    """Normalize explicit minute frequency aliases; return None for missing."""

    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"1min", "1m", "minute", "min"}:
        return "1min"
    if text in {"5min", "5m"}:
        return "5min"
    if text in {"day", "daily", "1d"}:
        return "day"
    return text


def is_qe_minute_execution_algo(value: Any) -> bool:
    """Return True when an execution algo is explicit evidence of minute QE."""

    algo = normalize_qe_execution_algo(value)
    return bool(algo and algo not in QE_DAILY_EXECUTION_ALGOS)


def _first_present(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def build_qe_minute_runtime_contract(
    *,
    config: Mapping[str, Any] | None = None,
    custom_params: Mapping[str, Any] | None = None,
    execution_algo: Any = None,
    execution_algo_params: Any = None,
    source: str,
    allow_default_execution_algo: bool = False,
    require_minute: bool = False,
) -> dict[str, Any] | None:
    """Build the minute runtime contract, or None when minute evidence is absent.

    ``allow_default_execution_algo`` is only for new generation paths where the
    current QE system default is known.  Historical backfills should leave it
    false and require explicit execution-algo evidence in loop config/task data.
    """

    cfg = parse_json_mapping(config)
    params = parse_json_mapping(custom_params)
    execution_section = parse_json_mapping(cfg.get("execution"))
    data_context = parse_json_mapping(cfg.get("data_context"))

    raw_algo = _first_present(
        execution_algo,
        params.get("execution_algo"),
        cfg.get("execution_algo"),
        execution_section.get("execution_algo"),
        execution_section.get("algo_code"),
    )
    algo = normalize_qe_execution_algo(raw_algo)
    if algo is None and allow_default_execution_algo:
        algo = QE_DEFAULT_MINUTE_EXECUTION_ALGO

    if not algo:
        if require_minute:
            raise ValueError("QE minute runtime contract requires execution_algo evidence")
        return None
    if algo in QE_DAILY_EXECUTION_ALGOS:
        if require_minute:
            raise ValueError(
                f"QE execution_algo={algo} is a daily legacy algo and cannot declare a minute contract"
            )
        return None

    raw_freq = _first_present(
        params.get("backtest_freq"),
        params.get("freq"),
        params.get("qlib_freq"),
        cfg.get("backtest_freq"),
        cfg.get("freq"),
        data_context.get("freq"),
    )
    freq = normalize_qe_minute_freq(raw_freq) or "1min"
    if freq == "day":
        if require_minute:
            raise ValueError(
                f"QE execution_algo={algo} requires minute backtest_freq, got day"
            )
        return None
    if freq not in {"1min", "5min"}:
        if require_minute:
            raise ValueError(
                f"QE minute runtime contract got unsupported backtest_freq={raw_freq!r}"
            )
        return None

    algo_params = _first_present(
        execution_algo_params,
        params.get("execution_algo_params"),
        execution_section.get("execution_algo_params"),
        execution_section.get("algo_config"),
        cfg.get("execution_algo_params"),
    )
    algo_params_dict = parse_json_mapping(algo_params)

    return {
        "runtime_mode": "minute",
        "bar_freq": "1m" if freq == "1min" else "5m",
        "backtest_freq": freq,
        "execution_algo": algo,
        "execution_algo_params": algo_params_dict,
        "runtime_contract_version": QE_MINUTE_RUNTIME_CONTRACT_VERSION,
        "runtime_contract_source": source,
    }


def merge_qe_minute_runtime_contract(
    custom_params: Mapping[str, Any] | None,
    *,
    config: Mapping[str, Any] | None = None,
    execution_algo: Any = None,
    execution_algo_params: Any = None,
    source: str,
    allow_default_execution_algo: bool = False,
    require_minute: bool = False,
) -> dict[str, Any]:
    """Merge the derived minute contract into a custom_params snapshot."""

    merged = parse_json_mapping(custom_params)
    contract = build_qe_minute_runtime_contract(
        config=config,
        custom_params=merged,
        execution_algo=execution_algo,
        execution_algo_params=execution_algo_params,
        source=source,
        allow_default_execution_algo=allow_default_execution_algo,
        require_minute=require_minute,
    )
    if contract:
        merged.update(contract)
    return merged


def runtime_contract_missing(custom_params: Mapping[str, Any] | None) -> bool:
    """Return True when a QE experiment row lacks the package-critical fields."""

    params = parse_json_mapping(custom_params)
    return not params.get("backtest_freq") or not params.get("execution_algo")
