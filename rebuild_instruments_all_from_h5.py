r"""Rebuild instruments/all.txt for a Qlib snapshot based on daily_pv.h5.

Usage (in WSL or Windows PowerShell):

    # WSL example
    #   conda activate rdagent-gpu
    #   cd /mnt/c/Users/lc999/NewAIstock/AIstock
    #   python rebuild_instruments_all_from_h5.py \
    #       --snapshot-id qlib_export_20251206

    # Windows PowerShell example
    #   conda activate aistock
    #   cd C:/Users/lc999/NewAIstock/AIstock
    #   python rebuild_instruments_all_from_h5.py --snapshot-id qlib_export_20251206

This script:
- Loads daily_pv.h5 from the given snapshot directory.
- Groups data by instrument and computes start/end trading dates.
- Writes instruments/all.txt as TAB-separated values:
    instrument<TAB>start_date<TAB>end_date

Only daily_pv.h5 is used as the truth source; other files are not modified.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def rebuild_all_txt(snapshot_dir: Path) -> None:
    daily_path = snapshot_dir / "daily_pv.h5"
    inst_dir = snapshot_dir / "instruments"
    all_path = inst_dir / "all.txt"

    if not daily_path.exists():
        raise FileNotFoundError(f"daily_pv.h5 not found at: {daily_path}")

    print(f"Loading daily data from: {daily_path}")
    df = pd.read_hdf(daily_path, key="data")

    if df.empty:
        raise ValueError("daily_pv.h5 is empty; cannot rebuild instruments/all.txt")

    if list(df.index.names) != ["datetime", "instrument"]:
        raise ValueError(f"Unexpected index names: {df.index.names}, expected ['datetime', 'instrument']")

    df_reset = df.reset_index()[["datetime", "instrument"]]
    df_reset["datetime"] = pd.to_datetime(df_reset["datetime"], utc=False)

    groups = df_reset.groupby("instrument")["datetime"]
    lines: list[str] = []
    for inst, s in groups:
        s_sorted = s.sort_values()
        start_dt = s_sorted.iloc[0].strftime("%Y-%m-%d")
        end_dt = s_sorted.iloc[-1].strftime("%Y-%m-%d")
        # Qlib expects TAB-separated instrument file
        lines.append(f"{inst}\t{start_dt}\t{end_dt}")

    inst_dir.mkdir(parents=True, exist_ok=True)
    all_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Rebuilt instruments/all.txt with {len(lines)} instruments:")
    print(f"  {all_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild instruments/all.txt from daily_pv.h5 for a snapshot")
    parser.add_argument(
        "--snapshot-id",
        required=True,
        help="Snapshot ID directory name under qlib_snapshots, e.g. qlib_export_20251206",
    )
    parser.add_argument(
        "--root",
        default=str(Path(__file__).parent / "qlib_snapshots"),
        help="Root directory containing snapshot folders (default: ./qlib_snapshots)",
    )

    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    snapshot_dir = root / args.snapshot_id

    if not snapshot_dir.exists():
        raise FileNotFoundError(f"Snapshot directory not found: {snapshot_dir}")

    rebuild_all_txt(snapshot_dir)


if __name__ == "__main__":
    main()
