"""Read-only, paired actual-account evaluation for the five continuous arms."""

import math

import numpy as np
import pandas as pd

ARMS = ("B", "M0", "M1", "X0", "X1")


def _require(value, code):
    if not value:
        raise ValueError(code)


def evaluate(accounts, actions, calendar, benchmarks, *, signal_diagnostics=None):
    dates = pd.DatetimeIndex(pd.to_datetime(calendar))
    _require(
        dates.is_unique
        and dates.is_monotonic_increasing
        and len(dates) == 125
        and str(dates[0].date()) == "2026-04-01"
        and str(dates[-1].date()) == "2026-09-30",
        "QE_L2_EVAL_CALENDAR",
    )
    keys = {(seed, arm) for seed in (42, 2024, 2026) for arm in ARMS}
    _require(set(accounts) == keys and set(actions) == keys, "QE_L2_EVAL_ACCOUNT_POPULATION")
    _require(set(benchmarks) == {"000300.SH", "000985.CSI"}, "QE_L2_EVAL_BENCHMARKS")
    benchmark_series = {k: pd.Series(v, dtype=float).rename_axis("datetime") for k, v in benchmarks.items()}
    for series in benchmark_series.values():
        series.index = pd.to_datetime(series.index)
        _require(
            series.index.is_unique and series.reindex(dates).notna().all() and np.isfinite(series.reindex(dates)).all(),
            "QE_L2_EVAL_BENCHMARK_GAP",
        )
    spans = {
        "full": ("2026-04-01", "2026-09-30"),
        "2026Q2": ("2026-04-01", "2026-06-30"),
        "2026Q3": ("2026-07-01", "2026-09-30"),
    }
    for month in range(4, 10):
        start = pd.Timestamp(2026, month, 1)
        spans[f"2026-{month:02d}"] = (str(start.date()), str((start + pd.offsets.MonthEnd(0)).date()))
    output = {}
    targets = {}
    for key in sorted(keys):
        frame = accounts[key].copy()
        frame.index = pd.to_datetime(frame.index)
        pd.testing.assert_index_equal(frame.index, dates, check_names=False)
        _require(
            frame.index.is_unique and set(["account", "cash", "trade_cost", "trade_notional"]).issubset(frame),
            "QE_L2_EVAL_LEDGER_SCHEMA",
        )
        _require(
            np.isfinite(frame[["account", "cash", "trade_cost", "trade_notional"]].to_numpy()).all()
            and frame.account.gt(0).all()
            and frame.cash.ge(-1e-6).all()
            and frame.trade_cost.ge(0).all()
            and frame.trade_notional.ge(0).all(),
            "QE_L2_EVAL_LEDGER_INVALID",
        )
        _require((frame.cash <= frame.account + 1e-6).all(), "QE_L2_EVAL_LEVERAGE_UNAPPROVED")
        prior = frame.account.shift(1)
        prior.iloc[0] = 100_000_000
        net = frame.account / prior - 1
        action_rows = actions[key]
        _require(len(action_rows) == 125, "QE_L2_EVAL_ACTION_GRID")
        target = pd.Series({r["trade_date"]: r["target_exposure"] for r in action_rows}, dtype=float)
        target.index = pd.to_datetime(target.index)
        _require(len(target) == 125 and target.index.is_unique, "QE_L2_EVAL_ACTION_DUPLICATE")
        target = target.reindex(dates)
        _require(
            target.notna().all() and np.isfinite(target).all() and target.between(0, 1).all(),
            "QE_L2_EVAL_ACTION_INVALID",
        )
        previous = {
            str(day.date()): str((dates[i - 1] if i else pd.Timestamp("2026-03-31")).date())
            for i, day in enumerate(dates)
        }
        for action in action_rows:
            _require(
                action["seed"] == key[0]
                and action["arm"] == key[1]
                and action["as_of_date"] == previous[action["trade_date"]]
                and abs(sum(action["target_weights"].values()) - action["target_exposure"]) < 1e-10,
                "QE_L2_EVAL_ACTION_IDENTITY",
            )
        targets[key] = target
        periods = {}
        for name, (start, end) in spans.items():
            part = net.loc[start:end]
            curve = np.r_[1.0, (1 + part).cumprod().to_numpy()]
            sigma = float(part.std(ddof=1))
            actual = (1 - frame.cash / frame.account).loc[start:end]
            value = {
                "days": len(part),
                "net_return": float(curve[-1] - 1),
                "max_drawdown": float((curve / np.maximum.accumulate(curve) - 1).min()),
                "CAGR": float(curve[-1] ** (252 / len(part)) - 1),
                "Sharpe": float(part.mean() / sigma * math.sqrt(252)) if sigma > 0 else None,
                "fees_currency": float(frame.trade_cost.loc[start:end].sum()),
                "mean_daily_turnover": float((frame.trade_notional / prior).loc[start:end].mean()),
                "mean_cash_fraction": float((frame.cash / frame.account).loc[start:end].mean()),
                "mean_target_exposure": float(target.loc[start:end].mean()),
                "mean_actual_exposure": float(actual.mean()),
                "mean_absolute_exposure_gap": float((actual - target.loc[start:end]).abs().mean()),
            }
            for symbol, series in benchmark_series.items():
                market = float((1 + series.reindex(part.index)).prod() - 1)
                value[symbol + "_return"] = market
                value["excess_vs_" + symbol] = value["net_return"] - market
            periods[name] = value
        output[key] = periods
    pairs = []
    for seed in (42, 2024, 2026):
        for left, right in [
            ("M1", "M0"),
            ("M0", "B"),
            ("M1", "B"),
            ("M0", "X0"),
            ("M1", "X1"),
            ("X0", "B"),
            ("X1", "B"),
        ]:
            if right in ("X0", "X1"):
                _require(
                    float((targets[seed, left] - targets[seed, right]).abs().max()) < 1e-10,
                    "QE_L2_EVAL_REFERENCE_EXPOSURE_MISMATCH",
                )
            for name in spans:
                a, b = output[seed, left][name], output[seed, right][name]
                pairs.append(
                    {
                        "seed": seed,
                        "comparison": left + "-" + right,
                        "period": name,
                        "net_increment": a["net_return"] - b["net_return"],
                        "max_drawdown_change": a["max_drawdown"] - b["max_drawdown"],
                        "fee_currency_change": a["fees_currency"] - b["fees_currency"],
                        "turnover_change": a["mean_daily_turnover"] - b["mean_daily_turnover"],
                        "actual_exposure_change": a["mean_actual_exposure"] - b["mean_actual_exposure"],
                        "foregone_return_vs_reference": b["net_return"] - a["net_return"],
                    }
                )
    if signal_diagnostics is not None:
        for name in ["IC", "RankIC"]:
            _require(name in signal_diagnostics, "QE_L2_EVAL_SIGNAL_SCHEMA")
            for day, value in signal_diagnostics[name].items():
                _require(day < "2026-09-01" or value is None, "QE_L2_EVAL_IMMATURE_LABEL_IMPUTED")
                _require(value is None or math.isfinite(value), "QE_L2_EVAL_SIGNAL_INVALID")
    return {
        "contract": "QE-L2-CONTINUOUS-RISK-V1",
        "account_count": 15,
        "accounts": [{"seed": seed, "arm": arm, "periods": rows} for (seed, arm), rows in sorted(output.items())],
        "paired_results": pairs,
        "signal_diagnostics": signal_diagnostics,
        "maturity_note": "Shared AF signal diagnostics only on mature labels; September h20 is NA, never imputed zero.",
        "limits": [
            "Seeds share market history and are not independent market samples.",
            "Target matching is not fill/actual exposure matching.",
            "Historical retrospective classification; research feedback, not untouched holdout.",
            "Account net already includes fees; fees are never deducted twice.",
        ],
        "automatic_promotion": False,
        "new_fits": 0,
        "new_backtests": 0,
    }
