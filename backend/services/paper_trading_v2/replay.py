"""Historical minute replay orchestration for Paper Trading v2."""

from __future__ import annotations

from datetime import date
from typing import Any

from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.market_data import MinuteDataSource, TradeCalendarProvider
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.trading_core.errors import (
    InvalidStateTransitionError,
    RuntimeConfigInvalidError,
    UnsupportedFeatureError,
)

from .models import PaperReplayDayResult, PaperReplayResult
from .repository import PaperTradingV2Repository


class PaperTradingHistoricalReplay:
    """Run Paper v2 day-runner over a historical date range.

    This is intentionally not the QE/offline backtest path. It reuses the
    Paper v2 portfolio, runner, OMS/execution/ledger persistence, and fails on
    the same missing-data conditions as a normal Paper v2 day run.
    """

    def __init__(
        self,
        *,
        repository: PaperTradingV2Repository | Any | None = None,
        package_repository: StrategyPackageRepository | Any | None = None,
        calendar_provider: TradeCalendarProvider | Any | None = None,
        day_runner: PaperTradingDayRunner | None = None,
    ) -> None:
        self.repository = repository or PaperTradingV2Repository()
        self.package_repository = package_repository
        self.calendar_provider = calendar_provider or TradeCalendarProvider()
        self.day_runner = day_runner or PaperTradingDayRunner(
            repository=self.repository,
            package_repository=self.package_repository,
            calendar_provider=self.calendar_provider,
        )

    def run(
        self,
        *,
        portfolio_id: str,
        start_date: date,
        end_date: date,
        runtime_config: dict[str, Any] | None = None,
        rerun_policy: str = "reject_existing",
        confirm_reset: bool = False,
        confirm_text: str | None = None,
    ) -> PaperReplayResult:
        if rerun_policy not in {"reject_existing", "reset_portfolio"}:
            raise UnsupportedFeatureError(
                "paper v2 replay rerun policy is not implemented",
                context={"portfolio_id": portfolio_id, "rerun_policy": rerun_policy},
            )
        portfolio = self.repository.get_portfolio(portfolio_id)
        if portfolio.data_source != MinuteDataSource.DB_HISTORICAL:
            raise RuntimeConfigInvalidError(
                "historical replay requires DB_HISTORICAL data_source",
                context={"portfolio_id": portfolio_id, "data_source": portfolio.data_source.value},
            )
        if start_date < portfolio.start_date:
            raise InvalidStateTransitionError(
                "historical replay start_date cannot be before portfolio start_date",
                context={
                    "portfolio_id": portfolio_id,
                    "start_date": start_date.isoformat(),
                    "portfolio_start_date": portfolio.start_date.isoformat(),
                },
            )
        trading_days = self.calendar_provider.list_trading_days(start_date, end_date)
        existing_runs = self._existing_runs(portfolio_id=portfolio_id, trading_days=trading_days)
        reset_audit: dict[str, Any] | None = None
        if existing_runs and rerun_policy == "reject_existing":
            raise InvalidStateTransitionError(
                "historical replay range already has paper v2 runs",
                context={
                    "portfolio_id": portfolio_id,
                    "rerun_policy": rerun_policy,
                    "existing_runs": existing_runs,
                },
            )
        if rerun_policy == "reset_portfolio":
            if not confirm_reset or confirm_text != portfolio_id:
                raise RuntimeConfigInvalidError(
                    "reset_portfolio replay requires explicit confirmation text matching portfolio_id",
                    context={
                        "portfolio_id": portfolio_id,
                        "confirm_reset": confirm_reset,
                        "confirm_text_matches": confirm_text == portfolio_id,
                    },
                )
            deleted_counts = self.repository.reset_portfolio_runs(
                portfolio_id=portfolio_id,
                start_date=None,
                end_date=None,
            )
            reset_audit = self.repository.save_reset_audit(
                portfolio_id=portfolio_id,
                rerun_policy=rerun_policy,
                start_date=start_date,
                end_date=end_date,
                confirm_text=confirm_text or "",
                deleted_counts=deleted_counts,
                status="RESET_COMPLETED",
                context={"existing_runs": existing_runs},
            )
        day_results: list[PaperReplayDayResult] = []
        base_config = dict(runtime_config or {})
        for trade_date in trading_days:
            config = {
                **base_config,
                "paper_v2_replay": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "mode": "historical_minute_replay",
                },
            }
            result = self.day_runner.run_day(
                portfolio_id=portfolio_id,
                trade_date=trade_date,
                runtime_config=config,
            )
            day_results.append(
                PaperReplayDayResult(
                    trade_date=trade_date,
                    run_id=result.run.run_id,
                    status=result.run.status,
                    nav=result.account_snapshot.nav,
                    order_count=len(result.orders),
                    fill_count=len(result.fills),
                    position_count=len(result.positions),
                )
            )
        return PaperReplayResult(
            portfolio_id=portfolio_id,
            start_date=start_date,
            end_date=end_date,
            data_source=MinuteDataSource.DB_HISTORICAL,
            trading_days=trading_days,
            day_results=day_results,
            reset_audit=reset_audit,
        )

    def _existing_runs(self, *, portfolio_id: str, trading_days: list[date]) -> list[dict[str, Any]]:
        existing_runs = []
        for trade_date in trading_days:
            existing = self.repository.get_run_by_portfolio_date(portfolio_id, trade_date)
            if existing is not None:
                existing_runs.append(
                    {
                        "trade_date": trade_date.isoformat(),
                        "run_id": existing.run_id,
                        "status": existing.status.value,
                    }
                )
        return existing_runs
