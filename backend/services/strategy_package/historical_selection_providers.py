"""Explicit read-only providers for historical StrategyPackage computation."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable

from backend.services.selection_center.industry_provider import DbSwIndustryLookupProvider
from backend.services.selection_center.risk_policy import (
    AnnouncementRiskDecisionProvider,
    StPitRiskDecisionProvider,
    StockRiskPolicyService,
)
from backend.services.selection_center.tradability import DbSuspendLookupProvider, TradabilityFilter

from .selection_computation import StrategyPackageSelectionReadOnlyProvidersV1


def build_historical_range_read_only_providers(
    *,
    conn_factory: Callable[[], Any],
) -> StrategyPackageSelectionReadOnlyProvidersV1:
    """Build providers with explicit DB ownership and no current PIT readiness predicate."""

    if conn_factory is None:
        raise ValueError("conn_factory is required")
    read_only_conn_factory = historical_read_only_connection_factory(conn_factory)
    return StrategyPackageSelectionReadOnlyProvidersV1(
        risk_policy=StockRiskPolicyService(
            providers={
                "st_pit": StPitRiskDecisionProvider(
                    conn_factory=read_only_conn_factory,
                    require_ready_state=False,
                ),
                "announcement_risk": AnnouncementRiskDecisionProvider(),
            }
        ),
        tradability=TradabilityFilter(
            suspend_provider=DbSuspendLookupProvider(read_only_conn_factory),
            industry_provider=DbSwIndustryLookupProvider(read_only_conn_factory),
        ),
    )


def historical_read_only_connection_factory(conn_factory: Callable[[], Any]) -> Callable[[], Any]:
    @contextmanager
    def open_read_only():
        with conn_factory() as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            yield conn

    return open_read_only
