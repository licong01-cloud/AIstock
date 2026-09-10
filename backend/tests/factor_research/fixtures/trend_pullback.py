"""Functional research case, not an approved alpha or a production factor."""
import argparse
import json
from pathlib import Path

import pandas as pd


def calculate(close):
    wide = close.unstack("instrument").sort_index()
    trend = wide / wide.shift(60) - 1
    deviation = wide / wide.rolling(10, min_periods=10).mean() - 1
    value = (-deviation * (trend > 0)).where(trend.notna())
    return value.stack(future_stack=True).reindex(close.index).to_frame("m_p1_trend_pullback_case")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--instruments", required=True)
    args = parser.parse_args()
    names = json.loads(args.instruments)
    frame = pd.read_hdf(args.data_dir / "daily_pv.h5", key="data", columns=["close"],
                        where=[f"datetime >= '{args.start_date}'", f"datetime <= '{args.end_date}'",
                               f"instrument in {names!r}"])
    result = calculate(frame["close"])
    result.to_hdf(args.output, key="data")
    print(f"candidate_rows={len(result)} stocks={result.index.get_level_values('instrument').nunique()}")


if __name__ == "__main__":
    main()
