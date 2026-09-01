"""Composition root for successor LocalSIM product services."""

from __future__ import annotations

from backend.services.strategy_package.repository import StrategyPackageRepository

from .localsim_control import LocalSimControlPlaneService
from .localsim_cutover_readiness import LocalSimCutoverReadiness
from .localsim_economic_query import LocalSimEconomicQueryService
from .localsim_product_authority import LocalSimProductAuthority
from .localsim_product_control import LocalSimProductControlPlaneService
from .localsim_product_control import LocalSimHistoricalSourceAuthority
from .localsim_query import LocalSimQueryService
from .localsim_replay_runtime import (
    LocalSimHistoricalReplayDayExecutor,
    LocalSimReplayLifecycleOwner,
    LocalSimReplaySafeBoundaryAuthority,
)
from .localsim_runtime_profile_repository import LocalSimRuntimeProfileRepository
from .localsim_runtime_profile_service import LocalSimRuntimeProfileService
from .successor_repository import LocalSimSuccessorRepository


def _dependencies() -> tuple[
    LocalSimSuccessorRepository,
    LocalSimRuntimeProfileRepository,
    LocalSimProductAuthority,
    LocalSimCutoverReadiness,
]:
    successor_repository = LocalSimSuccessorRepository()
    profile_repository = LocalSimRuntimeProfileRepository()
    package_repository = StrategyPackageRepository()
    authority = LocalSimProductAuthority(
        profile_repository=profile_repository,
        package_repository=package_repository,
    )
    return successor_repository, profile_repository, authority, LocalSimCutoverReadiness()


def build_localsim_product_service() -> LocalSimProductControlPlaneService:
    successor_repository, _, authority, readiness = _dependencies()
    return LocalSimProductControlPlaneService(
        repository=successor_repository,
        control=LocalSimControlPlaneService(
            repository=successor_repository,
            package_lifecycle_reader=authority.package_repository,
        ),
        authority=authority,
        readiness=readiness,
    )


def build_localsim_profile_service() -> LocalSimRuntimeProfileService:
    _, profile_repository, authority, _ = _dependencies()
    return LocalSimRuntimeProfileService(repository=profile_repository, authority=authority)


def build_localsim_query_service() -> LocalSimQueryService:
    successor_repository, profile_repository, _, _ = _dependencies()
    return LocalSimQueryService(repository=successor_repository, profile_repository=profile_repository)


def build_localsim_economic_query_service() -> LocalSimEconomicQueryService:
    successor_repository, _, _, _ = _dependencies()
    return LocalSimEconomicQueryService(repository=successor_repository)


def build_localsim_replay_lifecycle_owner(
    *, lifecycle_scheduler: object, trading_calendar_service: object
) -> LocalSimReplayLifecycleOwner:
    successor_repository, _, authority, readiness = _dependencies()
    control = LocalSimControlPlaneService(
        repository=successor_repository,
        package_lifecycle_reader=authority.package_repository,
    )
    return LocalSimReplayLifecycleOwner(
        repository=successor_repository,
        control=control,
        product_authority=authority,
        historical_source_authority=LocalSimHistoricalSourceAuthority(),
        historical_day_executor=LocalSimHistoricalReplayDayExecutor(
            lifecycle_scheduler=lifecycle_scheduler,
        ),
        safe_boundary_authority=LocalSimReplaySafeBoundaryAuthority(
            readiness=readiness,
            lifecycle_scheduler=lifecycle_scheduler,
            trading_calendar_service=trading_calendar_service,
        ),
    )
