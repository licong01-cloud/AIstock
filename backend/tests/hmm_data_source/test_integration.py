"""Controlled HMM data-source integration smoke.

The integration lane is opt-in and read-only.  It never writes DB rows, runs
DDL, starts services, or guesses a historical QE task.  Operators must provide
an authoritative loop reference and reproducible as-of date.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from backend.services.hmm_data_source import BacktestDataSource, DataSourceConfig
from backend.services.hmm_data_source.realtime_source import RealtimeDataSource


def test_data_source_config_requires_explicit_realtime_candidate():
    backtest = DataSourceConfig(
        mode="backtest",
        base_loop_ref="qe_example/Loop1",
    )
    realtime = DataSourceConfig(
        mode="realtime",
        candidate_id="candidate-readonly-smoke",
    )

    assert backtest.mode == "backtest"
    assert backtest.artifact_source_preference == "prediction_store_first"
    assert backtest.label_horizon_days == 10
    assert realtime.candidate_id == "candidate-readonly-smoke"


@pytest.mark.integration
def test_real_db_transaction_is_readonly(hmm_readonly_conn_factory):
    """Prove that the external receipt cannot execute writes by transaction mode."""

    with hmm_readonly_conn_factory() as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            transaction_read_only = cur.fetchone()[0]

    assert transaction_read_only == "on"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_qe_prediction_store_readonly(
    hmm_readonly_integration_config,
    tmp_path,
):
    """Read one manifest-backed QE artifact without creating another copy."""
    config = hmm_readonly_integration_config
    async with BacktestDataSource(
        base_loop_ref=config.qe_loop_ref,
        cache_dir=str(tmp_path / "qe-cache"),
        artifact_source_preference="prediction_store_only",
    ) as source:
        start_date, end_date = await source.get_available_date_range()
        requested_start = max(start_date, end_date - timedelta(days=10))
        frame = await source.get_predictions(requested_start, end_date)
        source_info = source.get_artifact_source_info()["pred.pkl"]
        cache_created = source.cache_manager.is_cached(config.qe_loop_ref, "pred.pkl")

    assert not frame.empty
    assert {"trade_date", "symbol", "score"}.issubset(frame.columns)
    assert frame["trade_date"].min() >= requested_start
    assert frame["trade_date"].max() <= end_date
    assert source_info["source"] == "prediction_store"
    assert source_info["zero_copy"] is True
    assert cache_created is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_market_repository_readonly(
    hmm_readonly_integration_config,
    hmm_readonly_repository,
):
    """Exercise canonical trading-calendar and PIT sector SELECT paths only."""
    config = hmm_readonly_integration_config
    source = RealtimeDataSource(
        candidate_id="candidate-readonly-smoke",
        as_of_date=config.as_of_date,
        repository=hmm_readonly_repository,
    )

    start_date, completed_date = await source.get_available_date_range()
    mapping = await source.get_sector_mapping(completed_date)

    assert start_date <= completed_date <= config.as_of_date
    assert mapping
    assert all(isinstance(symbol, str) for symbol in mapping)
    assert all(isinstance(sector, str) for sector in mapping.values())


@pytest.mark.integration
@pytest.mark.asyncio
async def test_backtest_and_realtime_share_canonical_sector_mapping(
    hmm_readonly_integration_config,
    hmm_readonly_repository,
    tmp_path,
):
    """Compare both adapters against the same PIT mapping date without writes."""
    config = hmm_readonly_integration_config
    realtime = RealtimeDataSource(
        candidate_id="candidate-readonly-smoke",
        as_of_date=config.as_of_date,
        repository=hmm_readonly_repository,
    )
    _, completed_date = await realtime.get_available_date_range()
    async with BacktestDataSource(
        base_loop_ref=config.qe_loop_ref,
        cache_dir=str(tmp_path / "mapping-cache"),
        repository=hmm_readonly_repository,
    ) as backtest:
        backtest_mapping = await backtest.get_sector_mapping(completed_date)
    realtime_mapping = await realtime.get_sector_mapping(completed_date)

    assert backtest_mapping == realtime_mapping
