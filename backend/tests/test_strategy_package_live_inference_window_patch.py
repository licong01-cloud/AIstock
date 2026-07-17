from __future__ import annotations

from datetime import date

from backend.data_service import qe_data_service, timescaledb_adapter
from scripts import strategy_package_live_inference as live_runner


def test_strategy_package_window_patch_accepts_uncapped_inference_contract(monkeypatch) -> None:
    calls: list[tuple[list[str], date, date, bool]] = []
    expected = object()

    def build_static_factors(
        universe: list[str],
        start_date: date,
        end_date: date,
        *,
        asof_fill_slow_static: bool,
    ) -> object:
        calls.append((universe, start_date, end_date, asof_fill_slow_static))
        return expected

    original_fetch = timescaledb_adapter.fetch_fundamental_data_ts
    original_window = live_runner.inference_engine_module.get_required_data_window
    monkeypatch.setattr(qe_data_service, "build_static_factors", build_static_factors)

    try:
        live_runner._patch_strategy_package_data_window()
        actual = timescaledb_adapter.fetch_fundamental_data_ts(
            ["000001.SZ"],
            date(2025, 6, 27),
            date(2026, 7, 15),
            max_natural_days=None,
        )
    finally:
        timescaledb_adapter.fetch_fundamental_data_ts = original_fetch
        live_runner.inference_engine_module.get_required_data_window = original_window

    assert actual is expected
    assert calls == [(["000001.SZ"], date(2025, 6, 27), date(2026, 7, 15), True)]
