"""Portfolio-level autonomous auto-run for Paper v2 broker backends."""

from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from backend.services.trading_core.errors import RuntimeConfigInvalidError, TradingCoreError

from .market_data import MinuteDataSource
from .models import (
    PaperPortfolio,
    PaperSessionMode,
    PortfolioStatus,
)
from .repository import PaperTradingV2Repository

AUTO_RUN_TZ = ZoneInfo(os.getenv("PAPER_V2_AUTO_RUN_TIMEZONE", "Asia/Shanghai"))
AUTO_RUN_SCHEMA_VERSION = "paper_v2_auto_run_v1"
DEFAULT_TRADE_WINDOW_POLICY: dict[str, Any] = {
    "prepare_start": "08:50",
    "submit_windows": [
        {"start": "09:25", "end": "11:30"},
        {"start": "13:00", "end": "14:55"},
    ],
    "final_submit_cutoff": "14:55",
    "allow_intraday_start": True,
    "after_cutoff_policy": "fail_day_without_submit",
}

AUTO_RUN_BROKER_DEFAULTS: dict[str, dict[str, str]] = {
    "local_sim": {
        "live_data_source": MinuteDataSource.TDX_REALTIME.value,
        "authority_source": "LOCAL_SIM_LEDGER",
        "account_binding_mode": "virtual_portfolio",
    },
    "minqmt_sim": {
        "live_data_source": MinuteDataSource.MINIQMT_REALTIME.value,
        "authority_source": "MINIQMT_QUERY",
        "account_binding_mode": "exclusive_account_phase1",
    },
}


def auto_run_live_source_for_broker(broker_backend: str) -> MinuteDataSource:
    defaults = AUTO_RUN_BROKER_DEFAULTS.get(str(broker_backend or "").strip().lower())
    if defaults is None:
        raise RuntimeConfigInvalidError(
            "Paper v2 auto-run broker backend is not supported",
            context={
                "broker_backend": broker_backend,
                "supported_broker_backends": sorted(AUTO_RUN_BROKER_DEFAULTS),
            },
        )
    return MinuteDataSource(defaults["live_data_source"])


def canonical_auto_run_json(config: dict[str, Any]) -> str:
    return json.dumps(config, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compute_auto_run_config_sha256(config: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_auto_run_json(config).encode("utf-8")).hexdigest()


def normalize_auto_run_config(
    config: dict[str, Any] | None = None,
    *,
    package_id: str | None = None,
    broker_account_id: str | None = None,
    broker_backend: str = "minqmt_sim",
    broker_mode: str = "SIM",
    initial_cash: float | None = None,
    top_k: int | None = None,
    hmm: dict[str, Any] | None = None,
    industry_blacklist: list[str] | None = None,
    fee_policy: dict[str, Any] | None = None,
    trade_window_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return canonical portfolio runtime config without StrategyPackage gates."""

    raw = deepcopy(config or {})
    runtime_profile = deepcopy(raw.get("runtime_profile") or {})
    selection = deepcopy(runtime_profile.get("selection") or {})
    if top_k is not None:
        selection["top_k"] = int(top_k)
    selection.setdefault("daily_strategy_id", "score_weighted_topk_v2")
    selection.setdefault("daily_strategy_params", {})
    runtime_profile["selection"] = selection

    hmm_payload = deepcopy(runtime_profile.get("hmm") or {})
    if hmm is not None:
        hmm_payload.update(hmm)
    hmm_payload.setdefault("enabled", False)
    hmm_payload.setdefault("auto_compute", True)
    hmm_payload.setdefault("manual_snapshot_required", False)
    runtime_profile["hmm"] = hmm_payload

    tradability = deepcopy(runtime_profile.get("tradability") or {})
    tradability.setdefault("exclude_suspended", True)
    runtime_profile["tradability"] = tradability
    runtime_profile["industry_blacklist"] = list(industry_blacklist if industry_blacklist is not None else runtime_profile.get("industry_blacklist") or [])

    broker = deepcopy(raw.get("broker") or {})
    raw_broker_backend = str(broker.get("broker_backend") or "").strip().lower()
    effective_broker_backend = str(broker_backend or raw_broker_backend or "minqmt_sim").strip().lower()
    if raw_broker_backend and raw_broker_backend != effective_broker_backend:
        raise RuntimeConfigInvalidError(
            "Paper v2 auto-run config broker_backend does not match the portfolio broker backend",
            context={
                "config_broker_backend": raw_broker_backend,
                "portfolio_broker_backend": effective_broker_backend,
            },
        )
    broker_defaults = AUTO_RUN_BROKER_DEFAULTS.get(effective_broker_backend)
    if broker_defaults is None:
        raise RuntimeConfigInvalidError(
            "Paper v2 auto-run broker backend is not supported",
            context={
                "broker_backend": effective_broker_backend,
                "supported_broker_backends": sorted(AUTO_RUN_BROKER_DEFAULTS),
            },
        )
    raw_live_data_source = str(broker.get("live_data_source") or "").strip()
    expected_live_data_source = broker_defaults["live_data_source"]
    if raw_live_data_source and raw_live_data_source != expected_live_data_source:
        raise RuntimeConfigInvalidError(
            "Paper v2 auto-run config live_data_source does not match the portfolio broker backend",
            context={
                "broker_backend": effective_broker_backend,
                "config_live_data_source": raw_live_data_source,
                "expected_live_data_source": expected_live_data_source,
            },
        )
    broker["broker_backend"] = effective_broker_backend
    broker["broker_mode"] = str(broker.get("broker_mode") or broker_mode).upper()
    if broker_account_id is not None:
        broker["account_id"] = str(broker_account_id)
    broker["live_data_source"] = expected_live_data_source
    broker.setdefault("authority_source", broker_defaults["authority_source"])
    broker.setdefault("account_binding_mode", broker_defaults["account_binding_mode"])
    broker.setdefault("strategy_name_template", "paper_{portfolio_id_short}")
    broker.setdefault("order_remark_schema", "aistock_paper_v2_json_v1")

    session_policy = deepcopy(raw.get("session_policy") or {})
    session_policy.setdefault("mode", PaperSessionMode.LIVE_ONLY.value)
    session_policy.setdefault("create_on_enable", True)
    session_policy.setdefault("recover_on_backend_start", True)
    session_policy.setdefault("manual_tick_only", False)
    session_policy.setdefault("duplicate_trade_date_policy", "reconcile_no_duplicate_submit")
    session_policy.setdefault("missing_session_policy", "auto_create_live_only")

    calendar_policy = deepcopy(raw.get("calendar_policy") or {})
    calendar_policy.setdefault("timezone", os.getenv("PAPER_V2_AUTO_RUN_TIMEZONE", "Asia/Shanghai"))
    calendar_policy.setdefault("calendar_service", "TradingCalendarStatusService")
    calendar_policy.setdefault("non_trading_day_policy", "wait_next_trading_day")
    calendar_policy.setdefault("missing_calendar_row_policy", "fail_fast")

    selection_artifact_config = deepcopy(raw.get("selection_artifact_config") or {})
    selection_artifact_config.setdefault("signal_data_source", MinuteDataSource.DB_HISTORICAL.value)
    selection_artifact_config.setdefault("auto_generate", True)
    selection_artifact_config.setdefault("inference_backend", "wsl")
    selection_artifact_config.setdefault("pit_mode", "PREVIOUS_TRADING_DAY_CLOSE")
    selection_artifact_config.setdefault("include_reference_price", True)
    selection_artifact_config.setdefault("artifact_reuse", "same_trade_date_config_hash")

    retry_policy = deepcopy(raw.get("retry_policy") or {})
    retry_policy.setdefault("broker_connect_retry_interval_seconds", 30)
    retry_policy.setdefault("data_ready_retry_interval_seconds", 60)
    retry_policy.setdefault("hmm_compute_retry_interval_seconds", 60)
    retry_policy.setdefault("max_retry_until", "14:55")
    retry_policy.setdefault(
        "retryable_error_codes",
        [
            "MINIQMT_NOT_CONNECTED",
            "MINIQMT_QUERY_TIMEOUT",
            "DATA_REFRESH_NOT_READY",
            "HMM_DAILY_CACHE_BUILDING",
        ],
    )

    reconciliation_policy = deepcopy(raw.get("reconciliation_policy") or {})
    reconciliation_policy.setdefault("post_submit_query_delay_seconds", 3)
    reconciliation_policy.setdefault("query_orders", True)
    reconciliation_policy.setdefault("query_trades", True)
    reconciliation_policy.setdefault("query_positions", True)
    reconciliation_policy.setdefault("already_reconciled_policy", "no_duplicate_orders")
    reconciliation_policy.setdefault("partial_submit_policy", "broker_authoritative_reconcile")

    ui_policy = deepcopy(raw.get("ui_policy") or {})
    ui_policy.setdefault("show_next_plan", True)
    ui_policy.setdefault("show_compact_error", True)
    ui_policy.setdefault("show_copyable_diagnostic", True)

    normalized_trade_window = deepcopy(DEFAULT_TRADE_WINDOW_POLICY)
    normalized_trade_window.update(deepcopy(raw.get("trade_window_policy") or {}))
    if trade_window_policy:
        normalized_trade_window.update(deepcopy(trade_window_policy))

    normalized = {
        "schema_version": AUTO_RUN_SCHEMA_VERSION,
        "enabled": bool(raw.get("enabled", True)),
        "package_id": package_id or raw.get("package_id"),
        "broker": broker,
        "session_policy": session_policy,
        "calendar_policy": calendar_policy,
        "trade_window_policy": normalized_trade_window,
        "selection_artifact_config": selection_artifact_config,
        "runtime_profile": runtime_profile,
        "retry_policy": retry_policy,
        "reconciliation_policy": reconciliation_policy,
        "ui_policy": ui_policy,
    }
    if initial_cash is not None:
        normalized["initial_cash"] = float(initial_cash)
    if fee_policy:
        normalized["fee_policy"] = fee_policy
    return normalized


class AutoRunCoordinator:
    """Recover enabled portfolios into scheduler-tickable live sessions."""

    def __init__(
        self,
        *,
        repository: PaperTradingV2Repository | Any | None = None,
        session_service: Any | None = None,
    ) -> None:
        self.repository = repository or PaperTradingV2Repository()
        if session_service is None:
            from .session import PaperTradingSessionService

            session_service = PaperTradingSessionService(repository=self.repository)
        self.session_service = session_service

    @staticmethod
    def env_enabled() -> bool:
        raw = (os.getenv("PAPER_V2_AUTO_RUN_ENABLED") or "true").strip().lower()
        return raw not in {"0", "false", "no", "off"}

    @staticmethod
    def bootstrap_missing_session_enabled() -> bool:
        raw = (os.getenv("PAPER_V2_AUTO_RUN_BOOTSTRAP_MISSING_SESSION") or "true").strip().lower()
        return raw not in {"0", "false", "no", "off"}

    @staticmethod
    def default_limit() -> int:
        raw = (os.getenv("PAPER_V2_AUTO_RUN_MAX_SESSIONS_PER_TICK") or "50").strip()
        try:
            value = int(raw)
        except ValueError:
            return 50
        return max(1, min(value, 500))

    def recover_enabled_portfolios(
        self,
        *,
        limit: int | None = None,
        as_of_time: datetime | None = None,
    ) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.env_enabled():
            return {
                "enabled": False,
                "started_at": started.isoformat(),
                "recovered": [],
                "skipped": [],
                "errors": [],
            }
        max_items = limit or self.default_limit()
        portfolios = self.repository.list_auto_run_portfolios(limit=max_items)
        result: dict[str, Any] = {
            "enabled": True,
            "started_at": started.isoformat(),
            "portfolio_count": len(portfolios),
            "recovered": [],
            "skipped": [],
            "errors": [],
        }
        for portfolio in portfolios:
            try:
                action = self._recover_portfolio(portfolio, as_of_time=as_of_time)
                result[action["bucket"]].append(action["payload"])
            except TradingCoreError as exc:
                payload = exc.to_dict()
                payload["context"] = {**payload.get("context", {}), "portfolio_id": portfolio.portfolio_id}
                result["errors"].append(payload)
            except Exception as exc:  # pragma: no cover - defensive scheduler guard
                result["errors"].append(
                    {
                        "error_code": "PAPER_V2_AUTO_RUN_RECOVERY_ERROR",
                        "message": "paper v2 auto-run recovery crashed",
                        "context": {
                            "portfolio_id": portfolio.portfolio_id,
                            "reason": f"{type(exc).__name__}: {exc}",
                        },
                    }
                )
        result["completed_at"] = datetime.now(UTC).isoformat()
        return result

    def _recover_portfolio(self, portfolio: PaperPortfolio, *, as_of_time: datetime | None) -> dict[str, Any]:
        config = normalize_auto_run_config(
            portfolio.auto_run_config,
            package_id=portfolio.package_id,
            broker_backend=portfolio.broker_backend,
        )
        active_sessions = self.repository.list_active_sessions(portfolio.portfolio_id)
        if active_sessions:
            return {
                "bucket": "skipped",
                "payload": {
                    "portfolio_id": portfolio.portfolio_id,
                    "reason": "active_session_exists",
                    "session_ids": [item.session_id for item in active_sessions],
                },
            }
        if not self.bootstrap_missing_session_enabled():
            return {
                "bucket": "skipped",
                "payload": {"portfolio_id": portfolio.portfolio_id, "reason": "bootstrap_missing_session_disabled"},
            }
        policy = config.get("session_policy") or {}
        if policy.get("missing_session_policy") not in {None, "auto_create_live_only"}:
            return {
                "bucket": "skipped",
                "payload": {
                    "portfolio_id": portfolio.portfolio_id,
                    "reason": "unsupported_missing_session_policy",
                    "missing_session_policy": policy.get("missing_session_policy"),
                },
            }
        if portfolio.status == PortfolioStatus.RUNNING:
            portfolio = self.repository.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.READY)
        if portfolio.status not in {PortfolioStatus.READY, PortfolioStatus.PAUSED}:
            return {
                "bucket": "skipped",
                "payload": {"portfolio_id": portfolio.portfolio_id, "reason": "portfolio_not_operable", "status": portfolio.status.value},
            }
        if portfolio.status == PortfolioStatus.PAUSED:
            portfolio = self.repository.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.READY)
        session_config = deepcopy(config)
        session_config["auto_run_config"] = deepcopy(config)
        session_config.setdefault("paper_v2_session", {})["manual_tick_only"] = bool(policy.get("manual_tick_only", False))
        local_today = self._local_date(as_of_time)
        live_data_source = auto_run_live_source_for_broker(portfolio.broker_backend)
        session = self.session_service.create_session(
            portfolio_id=portfolio.portfolio_id,
            mode=PaperSessionMode.LIVE_ONLY,
            start_date=max(portfolio.start_date, local_today),
            live_data_source=live_data_source,
            runtime_config=session_config,
            created_by="auto_run_coordinator",
            as_of_time=as_of_time,
        )
        return {
            "bucket": "recovered",
            "payload": {
                "portfolio_id": portfolio.portfolio_id,
                "session_id": session.session_id,
                "status": session.status.value,
                "mode": session.mode.value,
            },
        }

    @staticmethod
    def _local_date(as_of_time: datetime | None = None):
        current = as_of_time or datetime.now(AUTO_RUN_TZ)
        if current.tzinfo is not None:
            current = current.astimezone(AUTO_RUN_TZ)
        return current.date()

    def status(self) -> dict[str, Any]:
        try:
            enabled_count = len(self.repository.list_auto_run_portfolios(limit=self.default_limit()))
        except Exception:
            enabled_count = None
        return {
            "env_enabled": self.env_enabled(),
            "bootstrap_missing_session": self.bootstrap_missing_session_enabled(),
            "max_sessions_per_tick": self.default_limit(),
            "enabled_portfolio_count": enabled_count,
        }


__all__ = [
    "AUTO_RUN_SCHEMA_VERSION",
    "DEFAULT_TRADE_WINDOW_POLICY",
    "auto_run_live_source_for_broker",
    "AutoRunCoordinator",
    "canonical_auto_run_json",
    "compute_auto_run_config_sha256",
    "normalize_auto_run_config",
]
