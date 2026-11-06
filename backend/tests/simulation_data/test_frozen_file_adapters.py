import pytest

from backend.data_service.frozen_dataset import FrozenDataset, FrozenDatasetError, freeze_dataset_binding
from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.simulation_data.frozen_file_adapters import (
    FileMinuteMarketDataProvider, FileTradabilityFilter, file_reference_prices,
)
from backend.services.simulation_runtime.localsim_file_inputs import historical_suspension_provider
from backend.services.simulation_execution.localsim.suspension import REFERENCE_KEY, validate_suspension_reference
from backend.services.trading_core.errors import DataUnavailableError
from backend.tests.data_service.test_frozen_dataset import DAYS, HALT, SYMBOL, make_dataset


def test_native_minute_input_and_day_clocks_use_files_without_sql(tmp_path, monkeypatch):
    import psycopg2
    monkeypatch.setattr(psycopg2, "connect", lambda *_, **__: pytest.fail("file minute opened SQL"))
    dataset = FrozenDataset(freeze_dataset_binding(make_dataset(tmp_path)), consumer_scope="LOCALSIM", run_id="day")
    provider = FileMinuteMarketDataProvider(dataset)
    market = provider.load_symbol_input(symbol=SYMBOL, trade_date=DAYS[1], source=MinuteDataSource.DATASET_FILES,
                                        min_bars=2, require_suspend_status=True)
    assert market.source is MinuteDataSource.DATASET_FILES
    assert market.minute_bars[0].volume == 1000. and market.minute_bars[0].close == 10.5
    assert market.market_context["prev_close"] == 10.5
    assert market.market_context["file_binding_hash"] == dataset.identity
    assert len(market.minute_bars) == 2 and not market.minute_bars[0].is_suspended
    with pytest.raises(DataUnavailableError, match="min_bars|insufficient"):
        provider.load_symbol_input(symbol=SYMBOL, trade_date=DAYS[1], source=MinuteDataSource.DATASET_FILES, min_bars=3)
    with pytest.raises(DataUnavailableError, match="cannot be derived"):
        provider.load_symbol_input(symbol=HALT, trade_date=DAYS[1], source=MinuteDataSource.DATASET_FILES)


def test_native_halt_reference_is_raw_close_at_c_and_uses_public_proof(tmp_path, monkeypatch):
    import psycopg2
    monkeypatch.setattr(psycopg2, "connect", lambda *_, **__: pytest.fail("file halt opened SQL"))
    dataset = FrozenDataset(freeze_dataset_binding(make_dataset(tmp_path)), consumer_scope="LOCALSIM", run_id="halt")
    status = historical_suspension_provider(dataset).load_statuses(
        symbols=[HALT, SYMBOL], trade_date=DAYS[1], valuation_symbols=[HALT])
    assert not status[HALT]["is_tradable"] and status[SYMBOL]["is_tradable"]
    evidence = validate_suspension_reference(status[HALT][REFERENCE_KEY], symbol=HALT, trade_date=DAYS[1])
    assert evidence["reference_close"] == 10.5 and evidence["cutoff_date"] == str(DAYS[0])
    assert evidence["file_binding_hash"] == dataset.identity
    prices = file_reference_prices(dataset, symbols=[HALT], as_of_date=DAYS[1])
    assert prices[HALT] == 10.5
    assert prices.observations[HALT]["state"] == "SUSPENSION_LAST_OBSERVED_CLOSE"
    broken = {**evidence, "reference_source": "market.kline_daily_raw"}
    with pytest.raises(DataUnavailableError):
        validate_suspension_reference(broken, symbol=HALT, trade_date=DAYS[1])


def test_industry_policy_gaps_and_bin_generation_drift_are_explicit(tmp_path):
    dataset = FrozenDataset(freeze_dataset_binding(make_dataset(tmp_path)), consumer_scope="LOCALSIM", run_id="gap")
    with pytest.raises(DataUnavailableError) as error:
        FileTradabilityFilter(dataset).industry_provider.require_policy_coverage(["银行"])
    assert error.value.context["reason_code"] == "FROZEN_DATASET_INDUSTRY_POLICY_UNCOVERED"
    path = tmp_path / "minute/features/000001.sz/close.1min.bin"
    path.touch()
    with pytest.raises(FrozenDatasetError, match="generation"):
        dataset.raw_minute_rows(SYMBOL, DAYS[1])
