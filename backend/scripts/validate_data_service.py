"""Simple validation script for the AIstock data service layer.

Usage (from project root):

    python -m backend.scripts.validate_data_service

This script will:
- Call get_realtime_snapshot for a small universe and report row count;
- Call get_history_window (1d) and report row count and data source logs;
- Call get_intraday_window (1m) and report row count.

It is read-only and does not place any orders.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from backend.data_service import api as data_api


UNIVERSE = ["000001.SZ", "000002.SZ", "600000.SH", "600519.SH"]


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def validate_realtime_snapshot() -> None:
    _print_header("VALIDATE get_realtime_snapshot (xtquant -> TDX)")
    try:
        df = data_api.get_realtime_snapshot(UNIVERSE, fields=[
            "datetime",
            "close",
            "open",
            "high",
            "low",
            "volume",
            "amount",
        ])
    except Exception as exc:
        print("get_realtime_snapshot FAILED:", repr(exc))
        return

    print("rows:", len(df))
    print(df.head())


def validate_history_window() -> None:
    _print_header("VALIDATE get_history_window (1d)")
    # 取最近 60 根日线
    try:
        df = data_api.get_history_window(
            UNIVERSE,
            bars=60,
            fields=["open", "high", "low", "close", "volume", "amount"],
            freq="1d",
        )
    except Exception as exc:
        print("get_history_window FAILED:", repr(exc))
        return

    print("rows:", len(df))
    print("index names:", df.index.names)
    print(df.head())


def validate_intraday_window() -> None:
    _print_header("VALIDATE get_intraday_window (1m)")
    try:
        df = data_api.get_intraday_window(
            UNIVERSE,
            bars=120,
            fields=["open", "high", "low", "close", "volume", "amount"],
            freq="1m",
        )
    except Exception as exc:
        print("get_intraday_window FAILED:", repr(exc))
        return

    print("rows:", len(df))
    print("index names:", df.index.names)
    print(df.head())


def main() -> None:
    print("Validating data service for universe:", UNIVERSE)
    validate_realtime_snapshot()
    validate_history_window()
    validate_intraday_window()


if __name__ == "__main__":
    main()
