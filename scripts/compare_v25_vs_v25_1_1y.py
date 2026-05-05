"""Qlib comparison backtest: V25 vs V25.1 on identical alpha.

Same alpha (5-day momentum reversal), same TopkDropoutStrategy outer, same
account, fees, minute bin, and universe; only the inner execution algorithm
differs:
  * Run A: TailTWAPWithV25TwoStageStrategy (V25 baseline)
  * Run B: TailTWAPWithV25_1SmallCapStrategy (V25.1 board+cost-aware)

Outputs a side-by-side comparison of total return, annualized return, Sharpe,
max drawdown, turnover, total commission, and per-side fill breakdown.

Designed to run from the AIstock project root with explicit local provider and
model paths. Example arguments:
  --minute-provider-uri F:/Dev/AIstock/qlib_bin/minute_candidate
  --day-provider-uri F:/Dev/AIstock/qlib_bin/day_candidate
  --early-model F:/Dev/AIstock/rdagent_assets/model_cache/execution/V25_TWO_STAGE/v25_early_net_joint_fixed.pt
  --late-model F:/Dev/AIstock/rdagent_assets/model_cache/execution/V25_TWO_STAGE/v25_late_net_joint_fixed.pt
  --output .codex_tmp/compare_v25_vs_v25_1_1y.json

If 1-year minute data is not yet available, run a shorter window and the
metrics still highlight relative behavior.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("compare_v25_vs_v25_1_1y")


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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def flatten_qlib_minute(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).lstrip("$") for col in out.columns]
    return out.sort_index()


def build_5d_reversal_signal(
    minute_df: pd.DataFrame,
    backtest_start: pd.Timestamp,
    backtest_end: pd.Timestamp,
) -> pd.DataFrame:
    """Daily 5-day reversal alpha: signal = -ret_5d (rebound bet).

    Computed from minute close: per (instrument, date) take last close and
    aggregate to daily; then 5-day percentage change per instrument.
    """

    df = minute_df.reset_index()
    df.columns = [str(c) for c in df.columns]
    if "datetime" not in df.columns or "instrument" not in df.columns:
        raise RuntimeError(f"minute df missing index columns: {df.columns.tolist()}")
    df["date"] = pd.to_datetime(df["datetime"]).dt.normalize()
    daily = (
        df.groupby(["instrument", "date"], as_index=False)
        .agg(close=("close", "last"))
        .sort_values(["instrument", "date"])
    )
    daily["ret_5d"] = daily.groupby("instrument")["close"].pct_change(5)
    daily["score"] = -daily["ret_5d"]
    daily = daily.dropna(subset=["score"])
    daily = daily[
        (daily["date"] >= backtest_start) & (daily["date"] <= backtest_end)
    ]
    if daily.empty:
        raise RuntimeError(
            f"no signal rows after dropping NaN for window {backtest_start}~{backtest_end}"
        )
    signal = daily.set_index(["date", "instrument"])["score"]
    signal.index.names = ["datetime", "instrument"]
    return signal.to_frame("score")


def summarize_account_history(report: pd.DataFrame, init_cash: float) -> dict[str, Any]:
    if report is None or report.empty:
        return {"empty": True}
    out: dict[str, Any] = {"empty": False, "rows": int(len(report))}
    cols = report.columns
    if "account" in cols:
        nav = pd.to_numeric(report["account"], errors="coerce").dropna()
        if not nav.empty:
            out["init_nav"] = float(nav.iloc[0])
            out["final_nav"] = float(nav.iloc[-1])
            total_ret = nav.iloc[-1] / init_cash - 1.0
            out["total_return"] = float(total_ret)
            n_days = len(nav)
            if n_days > 0:
                ann = (1.0 + total_ret) ** (252.0 / max(n_days, 1)) - 1.0
                out["ann_return"] = float(ann)
            daily_ret = nav.pct_change().dropna()
            if not daily_ret.empty and daily_ret.std() > 0:
                ann_vol = daily_ret.std() * np.sqrt(252.0)
                out["ann_vol"] = float(ann_vol)
                out["sharpe"] = float(out.get("ann_return", 0.0) / ann_vol) if ann_vol > 0 else 0.0
                peak = nav.cummax()
                drawdown = (nav - peak) / peak
                out["max_drawdown"] = float(drawdown.min())
    if "return" in cols:
        ret = pd.to_numeric(report["return"], errors="coerce")
        out["return_sum"] = float(ret.sum())
    if "turnover" in cols:
        turnover = pd.to_numeric(report["turnover"], errors="coerce").fillna(0.0)
        out["total_turnover"] = float(turnover.sum())
    if "cost" in cols:
        cost = pd.to_numeric(report["cost"], errors="coerce").fillna(0.0)
        out["total_cost"] = float(cost.sum())
    return out


def summarize_indicator(indicator_dict: dict) -> dict[str, Any]:
    if not isinstance(indicator_dict, dict):
        return {}
    out: dict[str, Any] = {}
    for freq, value in indicator_dict.items():
        if not isinstance(value, tuple) or len(value) == 0:
            continue
        ind = value[0]
        if isinstance(ind, pd.DataFrame) and not ind.empty:
            sample = {}
            if "ffr" in ind.columns:
                ffr = pd.to_numeric(ind["ffr"], errors="coerce").dropna()
                if not ffr.empty:
                    sample["fill_rate_mean"] = float(ffr.mean())
                    sample["fill_rate_min"] = float(ffr.min())
            if "deal_amount" in ind.columns:
                da = pd.to_numeric(ind["deal_amount"], errors="coerce").fillna(0.0)
                sample["deal_amount_sum"] = float(da.sum())
            if "trade_value" in ind.columns:
                tv = pd.to_numeric(ind["trade_value"], errors="coerce").fillna(0.0)
                sample["trade_value_sum"] = float(tv.sum())
            sample["n_rows"] = int(len(ind))
            out[str(freq)] = sample
    return out


def collect_no_fill_reasons(strategy) -> dict:
    reasons = getattr(strategy, "_v25_no_fill_reasons", None)
    if not reasons:
        return {}
    return dict(Counter(reasons.values()))


# ---------------------------------------------------------------------------
# Single backtest runner
# ---------------------------------------------------------------------------


def run_one(
    *,
    label: str,
    strategy_inner,
    signal: pd.DataFrame,
    start: str,
    end: str,
    topk: int,
    drop: int,
    account: float,
    open_cost: float,
    close_cost: float,
    min_cost: float,
    trade_unit: int | None,
    board_lot_exchange: bool,
    benchmark: str | None,
) -> dict[str, Any]:
    from qlib.backtest import backtest as qlib_backtest
    from qlib.backtest.executor import NestedExecutor, SimulatorExecutor
    from qlib.contrib.strategy.signal_strategy import TopkDropoutStrategy
    if board_lot_exchange:
        from qe_board_lot_exchange import install_board_lot_exchange_patch

        install_board_lot_exchange_patch()

    logger.info("============================================================")
    logger.info("[%s] starting backtest %s ~ %s", label, start, end)
    logger.info("============================================================")

    outer = TopkDropoutStrategy(signal=signal, topk=topk, n_drop=drop)
    portfolio_metric_dict, indicator_dict = qlib_backtest(
        start_time=start,
        end_time=end,
        strategy=outer,
        executor=NestedExecutor(
            time_per_step="day",
            inner_executor=SimulatorExecutor(
                time_per_step="1min",
                generate_portfolio_metrics=False,
                verbose=False,
            ),
            inner_strategy=strategy_inner,
            generate_portfolio_metrics=True,
        ),
        benchmark=benchmark,
        account=account,
        exchange_kwargs={
            "freq": "1min",
            "limit_threshold": ("$limit_up", "$limit_down"),
            "deal_price": "close",
            "open_cost": open_cost,
            "close_cost": close_cost,
            "min_cost": min_cost,
            "trade_unit": trade_unit,
            "subscribe_fields": MINUTE_FIELDS,
        },
    )

    # Report DataFrame extraction.
    report_df: pd.DataFrame | None = None
    for value in portfolio_metric_dict.values():
        if isinstance(value, tuple) and value:
            cand = value[0]
            if isinstance(cand, pd.DataFrame) and not cand.empty:
                report_df = cand
                break
    summary = summarize_account_history(report_df, init_cash=account)
    indicators = summarize_indicator(indicator_dict)
    no_fill_counts = collect_no_fill_reasons(strategy_inner)
    return {
        "label": label,
        "summary": summary,
        "indicators": indicators,
        "no_fill_counts": no_fill_counts,
    }


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--minute-provider-uri", required=True)
    p.add_argument("--day-provider-uri", default="/home/lc999/data/qlib_bin")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--early-model", required=True)
    p.add_argument("--late-model", required=True)
    p.add_argument("--device", default="cpu")
    p.add_argument("--codes", default=None,
                   help="optional comma-separated subset of instruments")
    p.add_argument("--num-stocks", type=int, default=0,
                   help="if >0 and --codes is empty, take first N from instruments file")
    p.add_argument("--topk", type=int, default=50)
    p.add_argument("--drop", type=int, default=5)
    p.add_argument("--account", type=float, default=10_000_000)
    p.add_argument("--open-cost", type=float, default=0.000095,
                   help="A-share open commission rate (default 0.95 bps)")
    p.add_argument("--close-cost", type=float, default=0.000595,
                   help="A-share close commission + stamp duty (default 5.95 bps)")
    p.add_argument("--min-cost", type=float, default=5.0,
                   help="A-share minimum commission per fill (default 5 RMB)")
    p.add_argument("--benchmark", default=None)
    p.add_argument("--lookback-warmup-days", type=int, default=10,
                   help="extra days before --start to compute 5-day reversal")
    # V25.1 specific knobs.
    p.add_argument("--v25-1-tolerance-bps", type=float, default=10.0)
    p.add_argument("--v25-1-max-buckets", type=int, default=30)
    p.add_argument("--output", required=True)
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    import qlib
    from qlib.config import C
    from qlib.data import D

    minute_provider = Path(args.minute_provider_uri)
    if not minute_provider.exists():
        raise FileNotFoundError(f"minute provider missing: {minute_provider}")
    available = parse_instruments(minute_provider)
    if args.codes:
        requested = [c.strip().upper() for c in args.codes.split(",") if c.strip()]
        codes = [c for c in requested if c in available]
        if not codes:
            raise RuntimeError(f"none of --codes match instrument file: {requested[:5]}...")
    elif args.num_stocks > 0:
        codes = available[: args.num_stocks]
    else:
        codes = available
    logger.info("Universe size: %d (first 5: %s)", len(codes), codes[:5])

    C["kernels"] = 1
    qlib.init(
        provider_uri={"day": args.day_provider_uri, "1min": str(minute_provider)},
        region="cn",
        dataset_cache=None,
        expression_cache=None,
    )

    # Load minute data with warm-up days for the 5-day reversal.
    warmup_start = (
        pd.Timestamp(args.start) - pd.Timedelta(days=int(args.lookback_warmup_days) + 5)
    ).strftime("%Y-%m-%d")
    logger.info("Loading minute data %s ~ %s ...", warmup_start, args.end)
    minute_df = flatten_qlib_minute(
        D.features(
            codes,
            MINUTE_FIELDS,
            start_time=f"{warmup_start} 09:30:00",
            end_time=f"{args.end} 15:00:00",
            freq="1min",
        )
    )
    logger.info("Minute rows: %s", f"{len(minute_df):,}")

    # Build signal from minute data so we don't depend on a separate day provider.
    signal = build_5d_reversal_signal(
        minute_df,
        backtest_start=pd.Timestamp(args.start),
        backtest_end=pd.Timestamp(args.end),
    )
    logger.info("Signal rows: %s", f"{len(signal):,}")

    # Build inner strategies.
    from tail_twap_v25_strategy import TailTWAPWithV25TwoStageStrategy
    from tail_twap_v25_1_strategy import TailTWAPWithV25_1SmallCapStrategy

    common_kwargs = dict(
        early_model_path=args.early_model,
        late_model_path=args.late_model,
        device=args.device,
    )
    v25_inner = TailTWAPWithV25TwoStageStrategy(**common_kwargs)
    v25_1_inner = TailTWAPWithV25_1SmallCapStrategy(
        v25_1_min_cost=args.min_cost,
        v25_1_commission_rate=max(args.open_cost, args.close_cost),
        v25_1_tolerance_bps=args.v25_1_tolerance_bps,
        v25_1_max_buckets=args.v25_1_max_buckets,
        **common_kwargs,
    )

    common_run = dict(
        signal=signal,
        start=args.start,
        end=args.end,
        topk=args.topk,
        drop=args.drop,
        account=args.account,
        open_cost=args.open_cost,
        close_cost=args.close_cost,
        min_cost=args.min_cost,
        benchmark=args.benchmark,
    )
    result_v25 = run_one(
        label="V25",
        strategy_inner=v25_inner,
        trade_unit=100,
        board_lot_exchange=False,
        **common_run,
    )
    result_v25_1 = run_one(
        label="V25.1",
        strategy_inner=v25_1_inner,
        trade_unit=None,
        board_lot_exchange=True,
        **common_run,
    )

    # Side-by-side comparison.
    logger.info("\n%s", "=" * 80)
    logger.info("V25 vs V25.1 — SAME ALPHA (5-day reversal), SAME UNIVERSE, SAME ACCOUNT")
    logger.info("=" * 80)
    rows = [
        ("total_return", "summary", "total_return"),
        ("ann_return", "summary", "ann_return"),
        ("sharpe", "summary", "sharpe"),
        ("max_drawdown", "summary", "max_drawdown"),
        ("ann_vol", "summary", "ann_vol"),
        ("total_turnover", "summary", "total_turnover"),
        ("total_cost", "summary", "total_cost"),
    ]
    fmt_pct_keys = {"total_return", "ann_return", "max_drawdown", "ann_vol"}
    logger.info("%-22s %-16s %-16s %-12s", "metric", "V25", "V25.1", "diff (B-A)")
    for label, section, key in rows:
        a = result_v25[section].get(key)
        b = result_v25_1[section].get(key)
        if a is None or b is None:
            logger.info("%-22s %-16s %-16s %-12s", label, str(a), str(b), "-")
            continue
        if label in fmt_pct_keys:
            logger.info(
                "%-22s %-16s %-16s %-12s",
                label,
                f"{a*100:+.2f}%",
                f"{b*100:+.2f}%",
                f"{(b-a)*100:+.2f} pp",
            )
        else:
            logger.info(
                "%-22s %-16.4f %-16.4f %-12.4f",
                label,
                a,
                b,
                b - a,
            )

    logger.info("\n--- Indicators (1min frequency) ---")
    for label, res in [("V25", result_v25), ("V25.1", result_v25_1)]:
        ind = res["indicators"].get("1min", {})
        logger.info(
            "[%s] fill_rate_mean=%.4f deal_amount_sum=%.2f n_rows=%d",
            label,
            ind.get("fill_rate_mean", float("nan")),
            ind.get("deal_amount_sum", 0.0),
            ind.get("n_rows", 0),
        )

    logger.info("\n--- No-fill reason breakdown ---")
    for label, res in [("V25", result_v25), ("V25.1", result_v25_1)]:
        nf = res["no_fill_counts"]
        if nf:
            top = sorted(nf.items(), key=lambda kv: kv[1], reverse=True)
            logger.info("[%s] %s", label, ", ".join(f"{k}={v}" for k, v in top))
        else:
            logger.info("[%s] (no recorded no-fill reasons)", label)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "config": {
            "minute_provider_uri": str(minute_provider),
            "day_provider_uri": args.day_provider_uri,
            "start": args.start,
            "end": args.end,
            "topk": args.topk,
            "drop": args.drop,
            "account": args.account,
            "open_cost": args.open_cost,
            "close_cost": args.close_cost,
            "min_cost": args.min_cost,
            "v25_trade_unit": 100,
            "v25_1_trade_unit": None,
            "v25_1_board_lot_exchange": True,
            "v25_1_tolerance_bps": args.v25_1_tolerance_bps,
            "v25_1_max_buckets": args.v25_1_max_buckets,
            "n_codes": len(codes),
            "n_signal_rows": int(len(signal)),
        },
        "v25": result_v25,
        "v25_1": result_v25_1,
    }
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("\nWrote comparison report to %s", output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
