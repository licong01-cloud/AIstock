from __future__ import annotations

from datetime import date

import pytest

from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import (
    PaperTradingV2PortfolioService,
)
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.tests.paper_trading_v2.test_day_runner import (
    make_paper_enabled_manifest,
    save_manifest_with_default_execution_policy,
)


def _service_and_portfolio(*, broker_backend: str = "minqmt_sim"):
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
    service = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    )
    portfolio = service.create_portfolio(
        package_id=manifest.package_id,
        portfolio_name=f"{broker_backend} policy template test",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=(
            MinuteDataSource.MINIQMT_REALTIME if broker_backend == "minqmt_sim" else MinuteDataSource.TDX_REALTIME
        ),
        broker_backend=broker_backend,  # type: ignore[arg-type]
    )
    return service, manifest, portfolio


def test_activate_vnpy_style_template_rejects_local_sim_portfolio() -> None:
    service, _manifest, portfolio = _service_and_portfolio(broker_backend="local_sim")

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        service.activate_execution_policy(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            policy_id="vnpy_asset:SNIPER_MINIQMT",
            activated_by="unit_test",
            reason="local sim must not select MiniQMT asset",
        )

    assert exc_info.value.context["required_broker_backend"] == "minqmt_sim"
