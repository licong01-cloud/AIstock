"""Fail-fast domain errors for Trading Core v2.

These errors are intentionally explicit: trading code must never hide missing
data, unsupported rules, or incomplete implementations behind default results.
"""

from __future__ import annotations

from typing import Any


class TradingCoreError(RuntimeError):
    """Base class for fail-fast trading errors."""

    error_code = "TRADING_CORE_ERROR"

    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.context = context or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.error_code,
            "message": self.message,
            "context": self.context,
        }


class StrategyPackageValidationError(TradingCoreError):
    error_code = "STRATEGY_PACKAGE_VALIDATION_ERROR"


class DataUnavailableError(TradingCoreError):
    error_code = "DATA_UNAVAILABLE"


class UnsupportedFeatureError(TradingCoreError):
    error_code = "UNSUPPORTED_FEATURE"


class InvalidStateTransitionError(TradingCoreError):
    error_code = "INVALID_STATE_TRANSITION"


class ExecutionAlgoError(TradingCoreError):
    error_code = "EXECUTION_ALGO_ERROR"


class RiskRuleError(TradingCoreError):
    error_code = "RISK_RULE_ERROR"


class SessionConfigError(StrategyPackageValidationError):
    error_code = "SESSION_CONFIG_INVALID"


class SessionSourceUnsupportedError(UnsupportedFeatureError):
    error_code = "SESSION_SOURCE_UNSUPPORTED"


class SessionAlreadyRunningError(InvalidStateTransitionError):
    error_code = "SESSION_ALREADY_RUNNING"


class SessionLockTimeoutError(InvalidStateTransitionError):
    error_code = "SESSION_LOCK_TIMEOUT"


class AlgoModeUnsupportedError(UnsupportedFeatureError):
    error_code = "ALGO_MODE_UNSUPPORTED"


class AlgoRealtimeUnsupportedError(AlgoModeUnsupportedError):
    error_code = "ALGO_REALTIME_UNSUPPORTED"
