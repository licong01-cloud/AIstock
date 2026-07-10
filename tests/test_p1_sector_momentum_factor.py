import ast
from pathlib import Path

import pandas as pd
import pytest


def _factor_code() -> str:
    script_path = Path(__file__).parents[1] / "scripts" / "p1_new_factors.py"
    module = ast.parse(script_path.read_text(encoding="utf-8"))
    for node in module.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(target, ast.Name) and target.id == "FACTORS" for target in node.targets):
            factors = ast.literal_eval(node.value)
            return factors["m_stock_vs_industry_mom_20d"]["code"]
    raise AssertionError("FACTORS definition not found")


def _write_inputs(root: Path, *, conflict: bool = False) -> tuple[pd.DatetimeIndex, list[str]]:
    dates = pd.date_range("2024-01-02", periods=45, freq="B")
    instruments = ["000001.SZ", "000002.SZ"]
    index = pd.MultiIndex.from_product(
        [dates, instruments], names=["datetime", "instrument"]
    )

    close_values: list[float] = []
    sector_values: list[float] = []
    l2_values: list[int] = []
    for day, _date in enumerate(dates):
        for instrument in instruments:
            is_switching_stock = instrument == "000001.SZ"
            l2_code_id = 10 if is_switching_stock and day < 25 else 20
            sector_close = 100.0 + day if l2_code_id == 10 else 200.0 + 2.0 * day
            if conflict and day == 30 and is_switching_stock:
                sector_close += 1.0

            l2_values.append(l2_code_id)
            sector_values.append(sector_close)
            growth = 1.01 if is_switching_stock else 1.005
            close_values.append((50.0 if is_switching_stock else 80.0) * growth**day)

    pd.DataFrame({"close": close_values}, index=index).to_hdf(
        root / "daily_pv.h5", key="data", mode="w"
    )
    pd.DataFrame(
        {"sw2_close": sector_values, "l2_code_id": l2_values}, index=index
    ).to_hdf(root / "sector_data.h5", key="data", mode="w")
    return dates, instruments


def test_stock_vs_industry_momentum_uses_current_sector_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dates, _ = _write_inputs(tmp_path)
    monkeypatch.chdir(tmp_path)

    exec(compile(_factor_code(), "m_stock_vs_industry_mom_20d", "exec"), {})

    result = pd.read_hdf(tmp_path / "result.h5")
    observed = result.loc[(dates[30], "000001.SZ"), "m_stock_vs_industry_mom_20d"]
    stock_return = 1.01**20 - 1.0
    current_sector_return = (200.0 + 2.0 * 30) / (200.0 + 2.0 * 10) - 1.0

    assert observed == pytest.approx(stock_return - current_sector_return)


def test_stock_vs_industry_momentum_rejects_conflicting_sector_day_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_inputs(tmp_path, conflict=True)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="conflicting sw2_close values"):
        exec(compile(_factor_code(), "m_stock_vs_industry_mom_20d", "exec"), {})
