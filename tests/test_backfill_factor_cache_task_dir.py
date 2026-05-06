from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd


def test_backfill_factor_cache_eligible_index_uses_tasks_dir_constant() -> None:
    source_path = Path("scripts/backfill_factor_cache.py")
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(source_path))

    loaded_names = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }

    assert "TASK_DIR" not in loaded_names
    assert 'TASKS_DIR / f"{task_id}.eligible_index.parquet"' in source


def test_backfill_factor_cache_supports_realtime_loader_style_code(tmp_path) -> None:
    from scripts.backfill_factor_cache import _execute_factor_subprocess

    dates = pd.to_datetime(["2026-04-29", "2026-04-30"])
    index = pd.MultiIndex.from_product(
        [dates, ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    daily = pd.DataFrame(
        {
            "open": [10.0, 11.0],
            "close": [11.0, 12.0],
            "high": [11.5, 12.5],
            "low": [9.5, 10.5],
            "volume": [100.0, 200.0],
        },
        index=index,
    )

    factor_data_dir = tmp_path / "factor_data"
    factor_data_dir.mkdir()
    daily.to_hdf(factor_data_dir / "daily_pv.h5", key="data", mode="w")

    cache_path = tmp_path / "cache.parquet"
    code_text = '''
import pandas as pd


def calculate_LoaderCompatFactor(instruments, start_date, end_date):
    df = _REALTIME_LOADER.load(
        instruments=instruments,
        start_date=start_date,
        end_date=end_date,
        fields=["close", "volume"],
        adjust="qfq",
    )
    result = pd.DataFrame(index=df.index)
    result["LoaderCompatFactor"] = df["close"] + df["volume"]
    return result
'''

    meta = _execute_factor_subprocess(
        factor_name="LoaderCompatFactor",
        code_text=code_text,
        factor_data_dir=str(factor_data_dir),
        work_dir=str(tmp_path / "work"),
        cache_parquet_path=str(cache_path),
        start_date="2026-04-29",
        end_date="2026-04-30",
        timeout=30,
    )

    cached = pd.read_parquet(cache_path)
    assert meta["num_rows"] == 2
    assert list(cached["value"]) == [111.0, 212.0]
