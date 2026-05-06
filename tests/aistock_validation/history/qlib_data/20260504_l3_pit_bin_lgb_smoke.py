from __future__ import annotations

import json
import math
import os
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import qlib
from qlib.backtest import backtest
from qlib.config import REG_CN
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
from qlib.contrib.strategy.signal_strategy import TopkDropoutStrategy
from qlib.data import D
from qlib.data.dataset import DatasetH

PROVIDER_URI = "/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_pit_smoke_lgb_202001_202112_220"
OUTPUT_JSON = Path("/mnt/f/Dev/AIstock/tests/aistock_validation/history/qlib_data/20260504_l3_pit-bin-lgb-smoke-result.json")
OUTPUT_PRED = Path("/mnt/f/Dev/AIstock/tests/aistock_validation/history/qlib_data/20260504_l3_pit-bin-lgb-smoke-pred.pkl")

START = "2020-01-02"
END = "2021-12-31"
TRAIN = ("2020-01-02", "2020-12-31")
VALID = ("2021-01-04", "2021-06-30")
TEST = ("2021-07-01", "2021-12-31")
# Qlib's day executor needs the next calendar point to close each step, so the
# backtest stops one trading day before the exported calendar end.
BACKTEST = ("2021-07-01", "2021-12-30")


def _jsonify(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return str(pd.Timestamp(value))
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        if math.isnan(float(value)) or math.isinf(float(value)):
            return None
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, pd.Series):
        return {str(k): _jsonify(v) for k, v in value.to_dict().items()}
    if isinstance(value, pd.DataFrame):
        return value.reset_index().tail(5).to_dict(orient="records")
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonify(v) for v in value]
    return value


def _safe_float(value: Any) -> float | None:
    try:
        v = float(value)
    except Exception:
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _portfolio_summary(portfolio_metric: Any) -> dict[str, Any]:
    summary: dict[str, Any] = {"raw_type": str(type(portfolio_metric))}
    try:
        if isinstance(portfolio_metric, tuple) and portfolio_metric:
            # Common qlib shape: (report_df, positions_dict)
            report = portfolio_metric[0]
        elif isinstance(portfolio_metric, dict):
            first_key = next(iter(portfolio_metric))
            summary["dict_keys"] = [str(k) for k in portfolio_metric.keys()]
            value = portfolio_metric[first_key]
            report = value[0] if isinstance(value, tuple) else value
        else:
            report = portfolio_metric
        if isinstance(report, pd.DataFrame):
            summary["report_rows"] = len(report)
            summary["report_columns"] = [str(c) for c in report.columns]
            if len(report) > 0:
                cols = report.columns
                if "return" in cols:
                    ret = report["return"].dropna()
                    summary["mean_daily_return"] = _safe_float(ret.mean())
                    summary["std_daily_return"] = _safe_float(ret.std())
                    summary["cum_return"] = _safe_float((1 + ret).prod() - 1)
                    summary["annualized_return_rough"] = _safe_float((1 + ret.mean()) ** 252 - 1)
                if "account" in cols:
                    summary["final_account"] = _safe_float(report["account"].dropna().iloc[-1])
                summary["head"] = _jsonify(report.head(3))
                summary["tail"] = _jsonify(report.tail(3))
    except Exception as exc:
        summary["summary_error"] = f"{type(exc).__name__}: {exc}"
    return summary


def _indicator_summary(indicator: Any) -> dict[str, Any]:
    summary: dict[str, Any] = {"raw_type": str(type(indicator))}
    try:
        items = indicator.items() if isinstance(indicator, dict) else [("value", indicator)]
        out: dict[str, Any] = {}
        for key, value in items:
            item: dict[str, Any] = {"raw_type": str(type(value))}
            frames: list[pd.DataFrame] = []
            if isinstance(value, pd.DataFrame):
                frames = [value]
            elif isinstance(value, pd.Series):
                item["series_rows"] = len(value)
                item["series_tail"] = _jsonify(value.tail(5))
            elif isinstance(value, (list, tuple)):
                item["tuple_len"] = len(value)
                frames = [v for v in value if isinstance(v, pd.DataFrame)]
                item["element_types"] = [str(type(v)) for v in value]
            if frames:
                frame = frames[0]
                item["frame_rows"] = len(frame)
                item["frame_columns"] = [str(c) for c in frame.columns]
                item["frame_tail"] = _jsonify(frame.tail(5))
            out[str(key)] = item
        summary["items"] = out
    except Exception as exc:
        summary["summary_error"] = f"{type(exc).__name__}: {exc}"
    return summary


def main() -> int:
    os.environ.setdefault("QLIB_SKIP_CACHE", "1")
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        "status": "started",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "provider_uri": PROVIDER_URI,
        "segments": {"train": TRAIN, "valid": VALID, "test": TEST, "backtest": BACKTEST},
        "model": "qlib.contrib.model.gbdt.LGBModel",
        "handler": "qlib.contrib.data.handler.Alpha158",
        "purpose": "PIT daily Bin small candidate LGB train/backtest usability smoke",
    }
    try:
        qlib.init(provider_uri=PROVIDER_URI, region=REG_CN, clear_mem_cache=True)
        market = "all"
        inst = D.instruments(market)
        result["instrument_counts"] = {
            "train_start": len(D.list_instruments(inst, start_time=TRAIN[0], end_time=TRAIN[0], as_list=True)),
            "valid_start": len(D.list_instruments(inst, start_time=VALID[0], end_time=VALID[0], as_list=True)),
            "test_start": len(D.list_instruments(inst, start_time=TEST[0], end_time=TEST[0], as_list=True)),
            "test_end": len(D.list_instruments(inst, start_time=TEST[1], end_time=TEST[1], as_list=True)),
        }
        sample_features = D.features(inst, ["$close", "$volume", "$factor", "$limit_up", "$limit_down"], start_time=TEST[0], end_time="2021-07-05", freq="day")
        result["qlib_feature_smoke"] = {
            "rows": int(len(sample_features)),
            "columns": [str(c) for c in sample_features.columns],
            "unique_instruments": int(sample_features.index.get_level_values("instrument").nunique()),
            "nan_by_column": {str(k): int(v) for k, v in sample_features.isna().sum().to_dict().items()},
        }

        handler = Alpha158(
            instruments=market,
            start_time=START,
            end_time=END,
            fit_start_time=TRAIN[0],
            fit_end_time=TRAIN[1],
            freq="day",
        )
        dataset = DatasetH(
            handler=handler,
            segments={"train": TRAIN, "valid": VALID, "test": TEST},
        )
        train_data = dataset.prepare("train", col_set=["feature", "label"])
        valid_data = dataset.prepare("valid", col_set=["feature", "label"])
        test_data = dataset.prepare("test", col_set=["feature", "label"])
        result["dataset_shapes"] = {
            "train": list(train_data.shape),
            "valid": list(valid_data.shape),
            "test": list(test_data.shape),
            "train_instruments": int(train_data.index.get_level_values("instrument").nunique()),
            "test_instruments": int(test_data.index.get_level_values("instrument").nunique()),
        }
        result["dataset_nan_ratio"] = {
            "train_total_nan_ratio": _safe_float(train_data.isna().sum().sum() / max(1, train_data.size)),
            "valid_total_nan_ratio": _safe_float(valid_data.isna().sum().sum() / max(1, valid_data.size)),
            "test_total_nan_ratio": _safe_float(test_data.isna().sum().sum() / max(1, test_data.size)),
        }

        model = LGBModel(
            loss="mse",
            num_boost_round=120,
            early_stopping_rounds=20,
            learning_rate=0.05,
            num_leaves=32,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            n_jobs=4,
            verbosity=-1,
            seed=20260504,
        )
        model.fit(dataset)
        pred = model.predict(dataset, segment="test")
        if isinstance(pred, pd.DataFrame):
            pred_series = pred.iloc[:, 0]
        else:
            pred_series = pred
        pred_series = pred_series.dropna().sort_index()
        pred_series.to_pickle(OUTPUT_PRED)
        result["prediction"] = {
            "path": str(OUTPUT_PRED),
            "rows": int(len(pred_series)),
            "days": int(pred_series.index.get_level_values("datetime").nunique()),
            "instruments": int(pred_series.index.get_level_values("instrument").nunique()),
            "min": _safe_float(pred_series.min()),
            "max": _safe_float(pred_series.max()),
            "mean": _safe_float(pred_series.mean()),
            "std": _safe_float(pred_series.std()),
        }

        label_df = dataset.prepare("test", col_set="label")
        label = label_df.iloc[:, 0] if isinstance(label_df, pd.DataFrame) else label_df
        aligned = pd.concat([pred_series.rename("score"), label.rename("label")], axis=1).dropna()
        by_day = aligned.groupby(level="datetime")
        ic = by_day.apply(lambda x: x["score"].corr(x["label"], method="pearson") if len(x) >= 20 else np.nan).dropna()
        rank_ic = by_day.apply(lambda x: x["score"].corr(x["label"], method="spearman") if len(x) >= 20 else np.nan).dropna()
        result["test_ic"] = {
            "aligned_rows": int(len(aligned)),
            "days": int(aligned.index.get_level_values("datetime").nunique()),
            "ic_mean": _safe_float(ic.mean()),
            "ic_std": _safe_float(ic.std()),
            "rank_ic_mean": _safe_float(rank_ic.mean()),
            "rank_ic_std": _safe_float(rank_ic.std()),
        }

        strategy = TopkDropoutStrategy(
            signal=pred_series,
            topk=20,
            n_drop=5,
            only_tradable=True,
            forbid_all_trade_at_limit=True,
        )
        executor = {
            "class": "SimulatorExecutor",
            "module_path": "qlib.backtest.executor",
            "kwargs": {"time_per_step": "day", "generate_portfolio_metrics": True},
        }
        portfolio_metric, indicator = backtest(
            start_time=BACKTEST[0],
            end_time=BACKTEST[1],
            strategy=strategy,
            executor=executor,
            benchmark=None,
            account=100000000.0,
            exchange_kwargs={
                "freq": "day",
                "codes": market,
                "deal_price": "close",
                "limit_threshold": ("$limit_up", "$limit_down"),
                "open_cost": 0.0015,
                "close_cost": 0.0025,
                "min_cost": 5.0,
            },
        )
        result["backtest"] = {
            "status": "ok",
            "portfolio_summary": _portfolio_summary(portfolio_metric),
            "indicator_summary": _indicator_summary(indicator),
        }
        result["status"] = "ok"
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["traceback"] = traceback.format_exc()
    OUTPUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=_jsonify), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=_jsonify))
    return 0 if result.get("status") == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
