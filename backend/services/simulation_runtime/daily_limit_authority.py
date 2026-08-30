"""Broker-specific daily-limit authority contracts for simulation plans."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from backend.services.simulation_data.daily_context import (
    DAILY_LIMIT_AUTHORITY_BY_BROKER_V2,
    DAILY_LIMIT_RESOLVER_BY_BROKER_V2,
    DailyLimitAuthorityV2,
    DailyLimitResolverV2,
    DailyTradingContextV1,
    DailyTradingContextV2,
    SimulationBrokerBackend,
)


class DailyLimitAuthorityContractError(ValueError):
    """Raised when a plan attempts a cross-broker or ambiguous authority."""

    code = "DAILY_LIMIT_AUTHORITY_BROKER_MATRIX_INVALID"


def allowed_daily_limit_authorities(
    broker_backend: SimulationBrokerBackend | str,
) -> frozenset[DailyLimitAuthorityV2]:
    backend = _broker_backend(broker_backend)
    return DAILY_LIMIT_AUTHORITY_BY_BROKER_V2[backend]


def required_daily_limit_resolver(
    broker_backend: SimulationBrokerBackend | str,
) -> DailyLimitResolverV2:
    backend = _broker_backend(broker_backend)
    return DAILY_LIMIT_RESOLVER_BY_BROKER_V2[backend]


def assert_daily_limit_authorities_for_broker(
    *,
    broker_backend: SimulationBrokerBackend | str,
    authorities: set[DailyLimitAuthorityV2 | str] | frozenset[DailyLimitAuthorityV2 | str],
) -> frozenset[DailyLimitAuthorityV2]:
    backend = _broker_backend(broker_backend)
    if not authorities:
        raise DailyLimitAuthorityContractError("daily limit authority set must not be empty")
    try:
        normalized = frozenset(DailyLimitAuthorityV2(value) for value in authorities)
    except (TypeError, ValueError) as exc:
        raise DailyLimitAuthorityContractError("daily limit authority set contains an unknown value") from exc
    forbidden = normalized.difference(DAILY_LIMIT_AUTHORITY_BY_BROKER_V2[backend])
    if forbidden:
        names = ",".join(sorted(value.value for value in forbidden))
        raise DailyLimitAuthorityContractError(f"daily limit authority is not permitted for {backend.value}: {names}")
    return normalized


def parse_daily_trading_context(
    payload: Mapping[str, Any],
) -> DailyTradingContextV1 | DailyTradingContextV2:
    """Read a frozen V1 or V2 carrier without inference, upgrade, or requery."""

    if not isinstance(payload, Mapping):
        raise DailyLimitAuthorityContractError("daily trading context carrier must be a mapping")
    schema_version = payload.get("schema_version")
    raw = dict(payload)
    if schema_version == "daily_trading_context_v1":
        return DailyTradingContextV1.model_validate(raw)
    if schema_version == "daily_trading_context_v2":
        return DailyTradingContextV2.model_validate(raw)
    raise DailyLimitAuthorityContractError(
        "daily trading context carrier requires an explicit supported schema_version"
    )


def _broker_backend(value: SimulationBrokerBackend | str) -> SimulationBrokerBackend:
    try:
        return SimulationBrokerBackend(value)
    except (TypeError, ValueError) as exc:
        raise DailyLimitAuthorityContractError("simulation broker backend is invalid") from exc


__all__ = [
    "DailyLimitAuthorityContractError",
    "allowed_daily_limit_authorities",
    "assert_daily_limit_authorities_for_broker",
    "parse_daily_trading_context",
    "required_daily_limit_resolver",
]
