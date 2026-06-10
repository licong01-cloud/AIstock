"""Shared Paper v2 point-in-time selection cutoff handling."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from backend.services.trading_core.errors import RuntimeConfigInvalidError, TradingCalendarUnavailableError, TradingCoreError

PREVIOUS_TRADING_DAY_CLOSE = "PREVIOUS_TRADING_DAY_CLOSE"


def ensure_previous_trading_day_selection_cutoff(
    runtime_config: dict[str, Any],
    *,
    trade_date: date,
    calendar_provider: Any,
    config_error_type: type[TradingCoreError] = RuntimeConfigInvalidError,
) -> date | None:
    """Mutate auto-generated selection config to use the previous close.

    Trading-day Paper v2 readiness and day-run flows cannot ask DB_HISTORICAL
    for same-day live inference data before the post-close sync has happened.
    When the operator enables authoritative selection artifact generation but
    omits a cutoff, force the same PIT policy used by live sessions.
    """

    artifact_config = runtime_config.get("selection_artifact_config")
    if artifact_config is None:
        artifact_config = runtime_config.get("selection_artifact")
    if artifact_config is None:
        return None
    if not isinstance(artifact_config, dict):
        raise config_error_type(
            "selection_artifact_config must be an object",
            context={"selection_artifact_config_type": type(artifact_config).__name__},
        )
    raw_cutoff = artifact_config.get("cutoff_date")
    if raw_cutoff:
        cutoff_date = _parse_cutoff_date(raw_cutoff, trade_date=trade_date, config_error_type=config_error_type)
        _record_session_cutoff(
            runtime_config,
            cutoff_date=cutoff_date,
            policy=artifact_config.get("pit_mode"),
            config_error_type=config_error_type,
        )
        return cutoff_date
    if not bool(artifact_config.get("auto_generate")):
        return None

    lookup_start = trade_date - timedelta(days=31)
    previous_days = calendar_provider.list_trading_days(lookup_start, trade_date - timedelta(days=1))
    if not previous_days:
        raise TradingCalendarUnavailableError(
            "trading calendar has no previous trading day for Paper v2 selection cutoff",
            context={"trade_date": trade_date.isoformat(), "lookup_start": lookup_start.isoformat()},
        )
    cutoff_date = previous_days[-1]
    artifact_config["cutoff_date"] = cutoff_date.isoformat()
    artifact_config.setdefault("pit_mode", PREVIOUS_TRADING_DAY_CLOSE)
    _record_session_cutoff(
        runtime_config,
        cutoff_date=cutoff_date,
        policy=PREVIOUS_TRADING_DAY_CLOSE,
        config_error_type=config_error_type,
    )
    return cutoff_date


def _parse_cutoff_date(
    raw_cutoff: Any,
    *,
    trade_date: date,
    config_error_type: type[TradingCoreError],
) -> date:
    try:
        cutoff_date = date.fromisoformat(str(raw_cutoff))
    except ValueError as exc:
        raise config_error_type(
            "selection_artifact_config.cutoff_date must be YYYY-MM-DD",
            context={"cutoff_date": raw_cutoff},
        ) from exc
    if cutoff_date > trade_date:
        raise config_error_type(
            "selection_artifact_config.cutoff_date cannot be after trade_date",
            context={"trade_date": trade_date.isoformat(), "cutoff_date": cutoff_date.isoformat()},
        )
    return cutoff_date


def _record_session_cutoff(
    runtime_config: dict[str, Any],
    *,
    cutoff_date: date,
    policy: Any,
    config_error_type: type[TradingCoreError],
) -> None:
    session_config = runtime_config.setdefault("paper_v2_session", {})
    if not isinstance(session_config, dict):
        raise config_error_type(
            "paper_v2_session must be an object when recording selection cutoff",
            context={"paper_v2_session_type": type(session_config).__name__},
        )
    session_config["selection_cutoff_date"] = cutoff_date.isoformat()
    if policy:
        session_config["selection_cutoff_policy"] = str(policy)
