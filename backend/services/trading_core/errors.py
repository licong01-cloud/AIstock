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


class PackageAssetInvalidError(StrategyPackageValidationError):
    error_code = "PACKAGE_ASSET_INVALID"


class RuntimeConfigInvalidError(TradingCoreError):
    error_code = "RUNTIME_CONFIG_INVALID"


class TradingCalendarUnavailableError(TradingCoreError):
    error_code = "TRADING_CALENDAR_UNAVAILABLE"


class MarketDataUnavailableError(TradingCoreError):
    error_code = "MARKET_DATA_UNAVAILABLE"


class HMMRuntimeUnavailableError(TradingCoreError):
    error_code = "HMM_RUNTIME_UNAVAILABLE"


class ArtifactGenerationFailedError(TradingCoreError):
    error_code = "ARTIFACT_GENERATION_FAILED"


class LiveApprovalRequiredError(TradingCoreError):
    error_code = "LIVE_APPROVAL_REQUIRED"


class BrokerUnavailableError(TradingCoreError):
    error_code = "BROKER_UNAVAILABLE"


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


class BrokerBackendError(TradingCoreError):
    """Base class for adapter-side BrokerBackend failures.

    Strategy Engine design 2026-05-08 §3.6.1 / §10.1 (R-Q9). Engine itself does
    not raise these — the BrokerBackend implementation does, and the adapter
    propagates them up (no silent retry; feedback_no_silent_errors).
    """

    error_code = "BROKER_BACKEND_ERROR"


class BrokerSubmitError(BrokerBackendError):
    """submit_order_intent failed before reaching the backend (validation)."""

    error_code = "BROKER_SUBMIT_ERROR"


class BrokerRejectedError(BrokerBackendError):
    """Backend rejected the order (capital limit / suspended / limit-up etc.)."""

    error_code = "BROKER_REJECTED"


class BrokerConnectivityError(BrokerBackendError):
    """Backend session lost (e.g. miniQMT crash / xtquant disconnect).

    The adapter MUST surface; never silently retry.
    """

    error_code = "BROKER_CONNECTIVITY_ERROR"


class BrokerMarketSourceMismatchError(TradingCoreError):
    """MinuteDataSource not in ALLOWED_MARKET_SOURCES[backend_id].

    Strategy Engine design 2026-05-08 §3.6.4 (R-Q9 D3): market data channel is
    strongly bound to the broker backend. Cross-pairing (e.g. local_sim with
    MINIQMT_REALTIME, or minqmt_sim with TDX_REALTIME) is fail-fast at adapter
    init / portfolio bootstrap / live_session bootstrap; never silently fall
    back.
    """

    error_code = "BROKER_MARKET_SOURCE_MISMATCH"
