"""Read-only realtime data source for HMM research analysis."""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Optional, Protocol, Tuple

import pandas as pd

from .base import HMMDataSourceInterface
from .db_repository import HMMDataRepository
from .exceptions import DataNotFoundError, DataSourceError, DateRangeError, HorizonError


class RealtimePredictionProvider(Protocol):
    """Candidate-scoped prediction provider supplied by the evolution layer."""

    def get_predictions(
        self,
        *,
        candidate_id: str,
        start_date: date,
        end_date: date,
        as_of_date: date,
    ) -> pd.DataFrame:
        """Return candidate-bound predictions for a completed data window."""
        ...


class RealtimeDataSource(HMMDataSourceInterface):
    """Expose completed market data without guessing a prediction relation."""

    def __init__(
        self,
        snapshot_id: str = "latest",
        lag_days: int = 1,
        max_query_days: int = 730,
        *,
        candidate_id: str | None = None,
        as_of_date: date | None = None,
        repository: HMMDataRepository | None = None,
        prediction_provider: RealtimePredictionProvider | None = None,
    ) -> None:
        """Create a realtime analysis source.

        ``snapshot_id`` remains as a compatibility alias.  Predictions require
        an explicit candidate identity and provider; ``latest`` is never
        resolved implicitly because that could mix unrelated model outputs.
        ``lag_days`` counts completed trading days, not calendar days.
        """
        if lag_days < 1:
            raise ValueError("lag_days must be >= 1 completed trading day")
        if max_query_days < 1:
            raise ValueError("max_query_days must be >= 1")

        self.candidate_id = str(candidate_id or snapshot_id or "").strip()
        self.snapshot_id = self.candidate_id
        self.lag_days = lag_days
        self.max_query_days = max_query_days
        self.as_of_date = as_of_date or date.today()
        self.repository = repository or HMMDataRepository()
        self.prediction_provider = prediction_provider
        self._latest_available_date: Optional[date] = None

    @property
    def mode(self) -> str:
        return "realtime"

    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """Return predictions from an explicit candidate-scoped provider."""
        await self._validate_query_range(start_date, end_date)
        if not self.candidate_id or self.candidate_id == "latest":
            raise DataSourceError(
                "Realtime predictions require an explicit candidate_id; "
                "implicit 'latest' resolution is forbidden"
            )
        if self.prediction_provider is None:
            raise DataSourceError(
                "Realtime prediction provider is not configured for "
                f"candidate_id={self.candidate_id}"
            )

        completed_date = (await self.get_available_date_range())[1]
        try:
            df = await asyncio.to_thread(
                self.prediction_provider.get_predictions,
                candidate_id=self.candidate_id,
                start_date=start_date,
                end_date=end_date,
                as_of_date=completed_date,
            )
        except Exception as exc:
            raise DataSourceError(
                f"Candidate-scoped prediction query failed: {exc}"
            ) from exc

        required = {"trade_date", "symbol", "score"}
        if not isinstance(df, pd.DataFrame) or not required.issubset(df.columns):
            columns = list(df.columns) if isinstance(df, pd.DataFrame) else []
            raise DataSourceError(
                "Prediction provider returned an invalid schema: "
                f"required={sorted(required)}, actual={columns}"
            )
        if df.empty:
            raise DataNotFoundError(
                f"No predictions found for candidate={self.candidate_id}, "
                f"date range [{start_date}, {end_date}]"
            )
        return df.copy()

    async def get_labels(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int = 10,
    ) -> pd.DataFrame:
        """Return realized returns whose label date is already completed."""
        if not 1 <= horizon_days <= 30:
            raise HorizonError(
                f"horizon_days must be between 1 and 30, got {horizon_days}"
            )
        await self._validate_query_range(start_date, end_date)
        completed_date = (await self.get_available_date_range())[1]
        try:
            df = await asyncio.to_thread(
                self.repository.get_realized_returns,
                start_date=start_date,
                end_date=end_date,
                horizon_days=horizon_days,
                as_of_date=completed_date,
            )
        except DataSourceError:
            raise
        except Exception as exc:
            raise DataSourceError(f"Failed to query realized returns: {exc}") from exc
        if df.empty:
            raise DataNotFoundError(
                f"No realized returns found for date range [{start_date}, {end_date}] "
                f"with horizon_days={horizon_days}"
            )
        return df.copy()

    async def get_sector_mapping(self, trade_date: date) -> dict[str, str]:
        """Return the canonical PIT SW L2 mapping."""
        try:
            return await asyncio.to_thread(
                self.repository.get_sector_mapping,
                trade_date,
            )
        except DataSourceError:
            raise
        except Exception as exc:
            raise DataSourceError(f"Failed to query sector mapping: {exc}") from exc

    async def get_available_date_range(self) -> Tuple[date, date]:
        """Return bounds ending at the requested completed trading-day lag."""
        try:
            min_date, max_date = await asyncio.to_thread(
                self.repository.get_available_date_range,
                lag_trading_days=self.lag_days,
                as_of_date=self.as_of_date,
            )
        except DataSourceError:
            raise
        except Exception as exc:
            raise DataSourceError(f"Failed to query available date range: {exc}") from exc
        self._latest_available_date = max_date
        return min_date, max_date

    async def _validate_query_range(self, start_date: date, end_date: date) -> None:
        is_valid, error_msg = await self.validate_date_range(start_date, end_date)
        if not is_valid:
            raise DateRangeError(error_msg)
        query_days = (end_date - start_date).days + 1
        if query_days > self.max_query_days:
            raise DateRangeError(
                f"Query span ({query_days} days) exceeds "
                f"max_query_days ({self.max_query_days})"
            )
