"""Dependency-free process configuration metadata for Phase 1 quote ingress."""

from __future__ import annotations

from typing import Any, Mapping


QUOTE_INGRESS_ENV_METADATA: dict[str, dict[str, Any]] = {
    "MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": {
        "value": "false",
        "description": "启用 MiniQMT Adaptive IS Phase 1 quote ingress（默认关闭）",
        "required": False,
        "type": "boolean",
    },
    "MINIQMT_QUOTE_INGRESS_OWNER_MODE": {
        "value": "simulation_scheduler",
        "description": "MiniQMT quote ingress 唯一 owner 模式",
        "required": False,
        "type": "select",
        "options": ["simulation_scheduler"],
    },
    "MINIQMT_QUOTE_INGRESS_MAX_SYMBOLS": {
        "value": "128",
        "description": "MiniQMT quote ingress 最大 active symbol 数",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_INGRESS_DRAIN_BUDGET": {
        "value": "128",
        "description": "MiniQMT quote ingress 单轮 drain budget",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_INGRESS_HEARTBEAT_TIMEOUT_MS": {
        "value": "10000",
        "description": "MiniQMT quote ingress writer heartbeat 超时毫秒数",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_INGRESS_RESTART_BACKOFF_MS": {
        "value": "1000",
        "description": "MiniQMT quote ingress restart 初始退避毫秒数",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_INGRESS_RESTART_MAX_BACKOFF_MS": {
        "value": "30000",
        "description": "MiniQMT quote ingress restart 最大退避毫秒数",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_INGRESS_RESTART_MAX_ATTEMPTS": {
        "value": "3",
        "description": "MiniQMT quote ingress 单个 lifecycle epoch 的自动重启上限",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_INGRESS_LOUD_INTERVAL_SECONDS": {
        "value": "30",
        "description": "MiniQMT quote ingress 同类 loud 事件最小输出间隔秒数",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_EVIDENCE_OUTBOX_MAX_EVENTS": {
        "value": "4096",
        "description": "MiniQMT market-data evidence outbox 最大事件数",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_EVIDENCE_FLUSH_BATCH_SIZE": {
        "value": "128",
        "description": "MiniQMT market-data evidence flush batch 大小",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_EVIDENCE_RETRY_MAX_ATTEMPTS": {
        "value": "5",
        "description": "MiniQMT market-data evidence transient persistence 最大重试次数",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_EVIDENCE_RETRY_INITIAL_BACKOFF_MS": {
        "value": "100",
        "description": "MiniQMT market-data evidence transient persistence 初始退避毫秒数",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_EVIDENCE_RETRY_MAX_BACKOFF_MS": {
        "value": "5000",
        "description": "MiniQMT market-data evidence transient persistence 最大退避毫秒数",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_EVIDENCE_CADENCE_SECONDS": {
        "value": "30",
        "description": "MiniQMT market-data evidence cadence aggregate 窗口秒数",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_MARK_HISTORY_MAX_SAMPLES": {
        "value": "65536",
        "description": "MiniQMT markout first-quote selector 全进程历史样本上限",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_EVIDENCE_RECEIPT_RESERVE_EVENTS": {
        "value": "256",
        "description": "MiniQMT 已发生 broker receipt evidence 的独立保留槽容量",
        "required": False,
        "type": "text",
    },
    "MINIQMT_QUOTE_MARK_HISTORY_MAX_LAG_MS": {
        "value": "10000",
        "description": "MiniQMT markout history 支持的显式最大 selector lag 毫秒数",
        "required": False,
        "type": "text",
    },
}

QUOTE_INGRESS_ENV_DEFAULTS = {key: str(value["value"]) for key, value in QUOTE_INGRESS_ENV_METADATA.items()}


class QuoteIngressEnvValidationError(ValueError):
    """Dependency-free loud process-config error used by ConfigManager."""


def parse_quote_ingress_env_values(values: Mapping[str, Any]) -> dict[str, Any]:
    """Validate registered quote-ingress process values without algorithm imports.

    The caller may pass a complete process environment. Unknown unrelated keys
    are ignored, while unknown keys in the quote-ingress namespace are rejected
    so typos cannot be silently accepted.
    """

    if not isinstance(values, Mapping):
        raise QuoteIngressEnvValidationError("quote ingress process config must be a mapping")
    normalized_input = {str(key): value for key, value in values.items()}
    quote_prefixes = ("MINIQMT_ADAPTIVE_IS_QUOTE_", "MINIQMT_QUOTE_")
    unknown_quote_keys = sorted(
        key for key in normalized_input if key.startswith(quote_prefixes) and key not in QUOTE_INGRESS_ENV_METADATA
    )
    if unknown_quote_keys:
        raise QuoteIngressEnvValidationError(f"unknown quote ingress process config keys: {unknown_quote_keys}")
    merged = {**QUOTE_INGRESS_ENV_DEFAULTS, **normalized_input}
    owner_mode = str(merged["MINIQMT_QUOTE_INGRESS_OWNER_MODE"]).strip()
    if owner_mode != "simulation_scheduler":
        raise QuoteIngressEnvValidationError("MINIQMT_QUOTE_INGRESS_OWNER_MODE must be simulation_scheduler")
    parsed = {
        "enabled": _strict_bool(
            merged["MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED"],
            key="MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED",
        ),
        "owner_mode": owner_mode,
        "max_symbols": _positive_int(merged["MINIQMT_QUOTE_INGRESS_MAX_SYMBOLS"], key="MINIQMT_QUOTE_INGRESS_MAX_SYMBOLS"),
        "drain_budget": _positive_int(merged["MINIQMT_QUOTE_INGRESS_DRAIN_BUDGET"], key="MINIQMT_QUOTE_INGRESS_DRAIN_BUDGET"),
        "heartbeat_timeout_ms": _positive_int(
            merged["MINIQMT_QUOTE_INGRESS_HEARTBEAT_TIMEOUT_MS"],
            key="MINIQMT_QUOTE_INGRESS_HEARTBEAT_TIMEOUT_MS",
        ),
        "restart_backoff_ms": _positive_int(
            merged["MINIQMT_QUOTE_INGRESS_RESTART_BACKOFF_MS"],
            key="MINIQMT_QUOTE_INGRESS_RESTART_BACKOFF_MS",
        ),
        "restart_max_backoff_ms": _positive_int(
            merged["MINIQMT_QUOTE_INGRESS_RESTART_MAX_BACKOFF_MS"],
            key="MINIQMT_QUOTE_INGRESS_RESTART_MAX_BACKOFF_MS",
        ),
        "restart_max_attempts": _positive_int(
            merged["MINIQMT_QUOTE_INGRESS_RESTART_MAX_ATTEMPTS"],
            key="MINIQMT_QUOTE_INGRESS_RESTART_MAX_ATTEMPTS",
        ),
        "loud_interval_seconds": _positive_int(
            merged["MINIQMT_QUOTE_INGRESS_LOUD_INTERVAL_SECONDS"],
            key="MINIQMT_QUOTE_INGRESS_LOUD_INTERVAL_SECONDS",
        ),
        "evidence_outbox_max_events": _positive_int(
            merged["MINIQMT_QUOTE_EVIDENCE_OUTBOX_MAX_EVENTS"],
            key="MINIQMT_QUOTE_EVIDENCE_OUTBOX_MAX_EVENTS",
        ),
        "evidence_flush_batch_size": _positive_int(
            merged["MINIQMT_QUOTE_EVIDENCE_FLUSH_BATCH_SIZE"],
            key="MINIQMT_QUOTE_EVIDENCE_FLUSH_BATCH_SIZE",
        ),
        "evidence_retry_max_attempts": _positive_int(
            merged["MINIQMT_QUOTE_EVIDENCE_RETRY_MAX_ATTEMPTS"],
            key="MINIQMT_QUOTE_EVIDENCE_RETRY_MAX_ATTEMPTS",
        ),
        "evidence_retry_initial_backoff_ms": _positive_int(
            merged["MINIQMT_QUOTE_EVIDENCE_RETRY_INITIAL_BACKOFF_MS"],
            key="MINIQMT_QUOTE_EVIDENCE_RETRY_INITIAL_BACKOFF_MS",
        ),
        "evidence_retry_max_backoff_ms": _positive_int(
            merged["MINIQMT_QUOTE_EVIDENCE_RETRY_MAX_BACKOFF_MS"],
            key="MINIQMT_QUOTE_EVIDENCE_RETRY_MAX_BACKOFF_MS",
        ),
        "evidence_cadence_seconds": _positive_int(
            merged["MINIQMT_QUOTE_EVIDENCE_CADENCE_SECONDS"],
            key="MINIQMT_QUOTE_EVIDENCE_CADENCE_SECONDS",
        ),
        "mark_history_max_samples": _positive_int(
            merged["MINIQMT_QUOTE_MARK_HISTORY_MAX_SAMPLES"],
            key="MINIQMT_QUOTE_MARK_HISTORY_MAX_SAMPLES",
        ),
        "evidence_receipt_reserve_events": _positive_int(
            merged["MINIQMT_QUOTE_EVIDENCE_RECEIPT_RESERVE_EVENTS"],
            key="MINIQMT_QUOTE_EVIDENCE_RECEIPT_RESERVE_EVENTS",
        ),
        "mark_history_max_lag_ms": _positive_int(
            merged["MINIQMT_QUOTE_MARK_HISTORY_MAX_LAG_MS"],
            key="MINIQMT_QUOTE_MARK_HISTORY_MAX_LAG_MS",
        ),
    }
    if parsed["restart_max_backoff_ms"] < parsed["restart_backoff_ms"]:
        raise QuoteIngressEnvValidationError("restart max backoff cannot be smaller than restart backoff")
    if parsed["evidence_flush_batch_size"] > parsed["evidence_outbox_max_events"]:
        raise QuoteIngressEnvValidationError("evidence flush batch cannot exceed evidence outbox capacity")
    if parsed["evidence_retry_max_backoff_ms"] < parsed["evidence_retry_initial_backoff_ms"]:
        raise QuoteIngressEnvValidationError("evidence retry max backoff cannot be smaller than initial backoff")
    return parsed


def _strict_bool(value: Any, *, key: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    raise QuoteIngressEnvValidationError(f"{key} must be exactly true or false")


def _positive_int(value: Any, *, key: str) -> int:
    if isinstance(value, bool):
        raise QuoteIngressEnvValidationError(f"{key} cannot be boolean")
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise QuoteIngressEnvValidationError(f"{key} must be an integer") from exc
    if parsed <= 0:
        raise QuoteIngressEnvValidationError(f"{key} must be positive")
    return parsed


__all__ = [
    "QUOTE_INGRESS_ENV_DEFAULTS",
    "QUOTE_INGRESS_ENV_METADATA",
    "QuoteIngressEnvValidationError",
    "parse_quote_ingress_env_values",
]
