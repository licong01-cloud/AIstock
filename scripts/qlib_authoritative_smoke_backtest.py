from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd


MINUTE_FIELDS = [
    "$open",
    "$high",
    "$low",
    "$close",
    "$volume",
    "$amount",
    "$factor",
    "$up_limit_price",
    "$down_limit_price",
    "$prev_close",
    "$limit_up",
    "$limit_down",
]


def split_codes(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip().upper() for part in value.replace(";", ",").split(",") if part.strip()]


def parse_instruments(provider_uri: Path) -> list[str]:
    path = provider_uri / "instruments" / "all.txt"
    if not path.exists():
        raise FileNotFoundError(f"missing Qlib instrument file: {path}")
    instruments: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        parts = raw.strip().split()
        if parts:
            instruments.append(parts[0].upper())
    if not instruments:
        raise RuntimeError(f"no instruments found in {path}")
    return instruments


def flatten_qlib(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).lstrip("$") for col in out.columns]
    idx = out.index
    if not isinstance(idx, pd.MultiIndex) or idx.nlevels != 2:
        raise RuntimeError(f"expected two-level Qlib index, got {type(idx).__name__}")
    frame = idx.to_frame(index=False)
    names = list(idx.names)
    if "datetime" in names and "instrument" in names:
        dt_col = names.index("datetime")
        inst_col = names.index("instrument")
    else:
        parsed0 = pd.to_datetime(frame.iloc[:, 0], errors="coerce")
        parsed1 = pd.to_datetime(frame.iloc[:, 1], errors="coerce")
        dt_col, inst_col = (0, 1) if parsed0.notna().sum() >= parsed1.notna().sum() else (1, 0)
    out.index = pd.MultiIndex.from_frame(
        pd.DataFrame(
            {
                "datetime": pd.to_datetime(frame.iloc[:, dt_col]),
                "instrument": frame.iloc[:, inst_col].astype(str).str.upper(),
            }
        ),
        names=["datetime", "instrument"],
    )
    return out.sort_index()


def summarize_portfolio(portfolio_metric_dict: dict) -> dict:
    if not portfolio_metric_dict:
        return {"report_found": False}
    for _, value in portfolio_metric_dict.items():
        if isinstance(value, tuple) and value:
            report = value[0]
        else:
            report = value
        if isinstance(report, pd.DataFrame) and not report.empty:
            summary = {
                "report_found": True,
                "rows": int(len(report)),
                "columns": list(map(str, report.columns)),
            }
            if "account" in report.columns:
                summary["first_account"] = float(pd.to_numeric(report["account"], errors="coerce").iloc[0])
                summary["last_account"] = float(pd.to_numeric(report["account"], errors="coerce").iloc[-1])
            if "return" in report.columns:
                ret = pd.to_numeric(report["return"], errors="coerce")
                summary["return_sum"] = float(ret.sum())
                summary["return_nan"] = int(ret.isna().sum())
            return summary
    return {"report_found": False}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Small Qlib NestedExecutor 1min smoke backtest for authoritative stock minute bins.")
    parser.add_argument("--minute-provider-uri", required=True)
    parser.add_argument("--day-provider-uri", default="/home/lc999/data/qlib_bin")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--codes", default=None)
    parser.add_argument("--num-stocks", type=int, default=3)
    parser.add_argument("--topk", type=int, default=2)
    parser.add_argument("--drop", type=int, default=1)
    parser.add_argument("--account", type=float, default=1_000_000)
    parser.add_argument("--output", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    import qlib
    from qlib.backtest import backtest as qlib_backtest
    from qlib.backtest.executor import NestedExecutor, SimulatorExecutor
    from qlib.config import C
    from qlib.contrib.strategy.rule_strategy import TWAPStrategy
    from qlib.contrib.strategy.signal_strategy import TopkDropoutStrategy
    from qlib.data import D

    minute_provider = Path(args.minute_provider_uri)
    requested = split_codes(args.codes)
    available = parse_instruments(minute_provider)
    codes = [code for code in requested if code in available]
    if not codes:
        codes = available[: args.num_stocks]
    if len(codes) < 2:
        raise RuntimeError(f"need at least two instruments for smoke backtest, got {codes}")

    C["kernels"] = 1
    qlib.init(
        provider_uri={"day": args.day_provider_uri, "1min": str(minute_provider)},
        region="cn",
        dataset_cache=None,
        expression_cache=None,
    )

    minute = flatten_qlib(
        D.features(
            codes,
            MINUTE_FIELDS,
            start_time=f"{args.start} 09:30:00",
            end_time=f"{args.end} 15:00:00",
            freq="1min",
        )
    )
    minute_nan = {col: int(minute[col].isna().sum()) for col in minute.columns}
    bad_minute_nan = {k: v for k, v in minute_nan.items() if v > 0}
    dates = sorted(pd.to_datetime(minute.index.get_level_values("datetime")).normalize().unique())
    if len(dates) < 2:
        raise RuntimeError(f"not enough minute dates for smoke backtest: {dates}")

    signal_rows = []
    for date_value in dates:
        # Deterministic PIT signal. It does not use future returns; it only
        # proves Qlib can consume the exported minute provider in a backtest.
        for rank, code in enumerate(codes):
            signal_rows.append((pd.Timestamp(date_value), code, float(len(codes) - rank)))
    signal = pd.DataFrame(signal_rows, columns=["datetime", "instrument", "score"]).set_index(["datetime", "instrument"])

    portfolio_metric_dict, indicator_dict = qlib_backtest(
        start_time=args.start,
        end_time=args.end,
        strategy=TopkDropoutStrategy(signal=signal, topk=args.topk, n_drop=args.drop),
        executor=NestedExecutor(
            time_per_step="day",
            inner_executor=SimulatorExecutor(time_per_step="1min", generate_portfolio_metrics=False),
            inner_strategy=TWAPStrategy(),
            generate_portfolio_metrics=True,
        ),
        benchmark=None,
        account=args.account,
        exchange_kwargs={
            "freq": "1min",
            "limit_threshold": ("$limit_up", "$limit_down"),
            "deal_price": "close",
            "open_cost": 0.000095,
            "close_cost": 0.000595,
            "min_cost": 5,
            "trade_unit": 100,
            "subscribe_fields": MINUTE_FIELDS,
        },
    )

    summary = summarize_portfolio(portfolio_metric_dict)
    result = {
        "ok": not bad_minute_nan and summary.get("report_found", False),
        "codes": codes,
        "start": args.start,
        "end": args.end,
        "minute_rows": int(len(minute)),
        "minute_dates": [pd.Timestamp(d).strftime("%Y-%m-%d") for d in dates],
        "minute_nan": minute_nan,
        "portfolio_summary": summary,
        "indicator_keys": list(indicator_dict.keys()) if isinstance(indicator_dict, dict) else [],
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
