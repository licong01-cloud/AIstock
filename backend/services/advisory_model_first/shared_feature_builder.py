from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    IDENTITY_COLUMNS,
    MODEL_FEATURE_COLUMNS,
    OPTIONAL_FEATURE_COLUMNS,
    REQUIRED_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID


@dataclass(frozen=True)
class FeatureBuildResult:
    features: pd.DataFrame
    coverage: pd.DataFrame


def build_advisory_feature_matrix(
    *,
    candidates: pd.DataFrame,
    candidate_daily: pd.DataFrame,
    candidate_static: pd.DataFrame,
    market_daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    hmm_states: pd.DataFrame,
) -> FeatureBuildResult:
    panel = candidate_daily.join(candidate_static, how="left").sort_index()
    panel_features = _build_instrument_features(panel)
    panel_features.index = panel_features.index.set_names(["decision_as_of_trade_date", "instrument"])
    sector_features = _build_sector_features(candidate_static, benchmark_daily)
    benchmark_features = _build_benchmark_features(benchmark_daily)
    market_features = _build_market_features(market_daily)

    rows = candidates.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(
        rows.get("decision_as_of_trade_date", rows["trade_date"])
    ).dt.normalize()
    rows["target_trade_date"] = pd.to_datetime(rows["target_trade_date"]).dt.normalize()
    rows["instrument"] = rows["instrument"].astype(str).str.upper()
    rows = rows.set_index(["decision_as_of_trade_date", "instrument"], drop=False).sort_index()
    rows = rows.join(panel_features, how="left")
    decision_index = pd.DatetimeIndex(rows.index.get_level_values("decision_as_of_trade_date"))
    for column in benchmark_features.columns:
        rows[column] = benchmark_features[column].reindex(decision_index).to_numpy()
    for column in market_features.columns:
        rows[column] = market_features[column].reindex(decision_index).to_numpy()

    rows["parent_combined_score"] = pd.to_numeric(rows["combined_score"], errors="coerce")
    denominator = (pd.to_numeric(rows["candidate_group_size"], errors="coerce") - 1).clip(lower=1)
    rows["parent_rank_pct"] = 1.0 - (
        pd.to_numeric(rows["selection_effective_rank"], errors="coerce") - 1
    ) / denominator
    rows["lstm_raw_score"] = rows[f"raw__{LSTM_LEG_ID}"]
    rows["lstm_norm_score"] = rows[f"norm__{LSTM_LEG_ID}"]
    rows["lstm_leg_rank"] = rows[f"rank__{LSTM_LEG_ID}"]
    rows["lstm_weight"] = rows[f"weight__{LSTM_LEG_ID}"]
    rows["fund_raw_score"] = rows[f"raw__{FUND_LEG_ID}"]
    rows["fund_norm_score"] = rows[f"norm__{FUND_LEG_ID}"]
    rows["fund_leg_rank"] = rows[f"rank__{FUND_LEG_ID}"]
    rows["fund_weight"] = rows[f"weight__{FUND_LEG_ID}"]
    rows["leg_norm_score_gap"] = rows["lstm_norm_score"] - rows["fund_norm_score"]
    rows["leg_rank_gap"] = rows["lstm_leg_rank"] - rows["fund_leg_rank"]
    rows["leg_direction_agreement"] = (
        np.sign(rows["lstm_norm_score"]) == np.sign(rows["fund_norm_score"])
    ).astype("int8")
    rows["weight_concentration"] = rows["lstm_weight"] ** 2 + rows["fund_weight"] ** 2

    sector_index = pd.MultiIndex.from_arrays(
        [
            decision_index,
            pd.to_numeric(rows["l2_code_id"], errors="coerce"),
        ],
        names=["decision_as_of_trade_date", "l2_code_id"],
    )
    for column in sector_features.columns:
        rows[column] = sector_features[column].reindex(sector_index).to_numpy()
    hmm = _normalize_hmm_states(hmm_states)
    for column in hmm.columns:
        rows[column] = hmm[column].reindex(sector_index).to_numpy()
    suspended = {
        (pd.Timestamp(item.trade_date).normalize(), str(item.instrument).upper())
        for item in suspend_rows.itertuples(index=False)
    }
    rows["decision_is_suspended"] = [
        int((date, instrument) in suspended) for date, instrument in rows.index
    ]
    rows["decision_limit_up"] = pd.to_numeric(rows["limit_up"], errors="coerce")
    rows["decision_limit_down"] = pd.to_numeric(rows["limit_down"], errors="coerce")
    adjusted_limit_up = pd.to_numeric(rows["up_limit_price"], errors="coerce") * pd.to_numeric(
        rows["factor"], errors="coerce"
    )
    adjusted_limit_down = pd.to_numeric(rows["down_limit_price"], errors="coerce") * pd.to_numeric(
        rows["factor"], errors="coerce"
    )
    current_close = pd.to_numeric(rows["close"], errors="coerce")
    rows["distance_to_limit_up"] = adjusted_limit_up / current_close - 1.0
    rows["distance_to_limit_down"] = adjusted_limit_down / current_close - 1.0

    for column in OPTIONAL_FEATURE_COLUMNS:
        rows[f"{column}__missing"] = rows[column].isna().astype("int8")
    numeric_columns = rows.select_dtypes(include=[np.number]).columns
    rows.loc[:, numeric_columns] = rows.loc[:, numeric_columns].replace([np.inf, -np.inf], np.nan)
    required_missing = rows[list(REQUIRED_FEATURE_COLUMNS)].isna().any(axis=1)
    required_missing_by_column = rows[list(REQUIRED_FEATURE_COLUMNS)].isna()
    by_date = required_missing.groupby(level="decision_as_of_trade_date").any()
    valid_dates = by_date.index[~by_date]
    coverage = pd.DataFrame(
        {
            "decision_as_of_trade_date": by_date.index,
            "candidate_count": rows.groupby(level="decision_as_of_trade_date").size().reindex(by_date.index).to_numpy(),
            "required_missing_row_count": required_missing.groupby(level="decision_as_of_trade_date").sum().to_numpy(),
            "required_missing_columns": [
                sorted(
                    required_missing_by_column.loc[
                        required_missing_by_column.index.get_level_values("decision_as_of_trade_date") == decision
                    ].columns[
                        required_missing_by_column.loc[
                            required_missing_by_column.index.get_level_values("decision_as_of_trade_date") == decision
                        ].any(axis=0)
                    ].tolist()
                )
                for decision in by_date.index
            ],
            "status": np.where(by_date.to_numpy(), "unavailable", "available"),
        }
    )
    output_columns = [*IDENTITY_COLUMNS, *MODEL_FEATURE_COLUMNS]
    missing_output = sorted(set(output_columns) - set(rows.columns))
    if missing_output:
        raise AdvisoryModelFirstError(
            "feature builder did not produce the frozen schema",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_columns": missing_output},
        )
    features = rows.loc[rows.index.get_level_values("decision_as_of_trade_date").isin(valid_dates), output_columns]
    return FeatureBuildResult(features=features.reset_index(drop=True), coverage=coverage)


def _build_instrument_features(panel: pd.DataFrame) -> pd.DataFrame:
    required = {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "factor",
        "up_limit_price",
        "down_limit_price",
        "limit_up",
        "limit_down",
    }
    if not required.issubset(panel.columns):
        raise AdvisoryModelFirstError(
            "candidate market panel is missing required Qlib fields",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_columns": sorted(required - set(panel.columns))},
        )
    out = panel.copy()
    close = _number(out["close"])
    high = _number(out["high"])
    low = _number(out["low"])
    open_price = _number(out["open"])
    volume = _number(out["volume"])
    amount = _number(out["amount"])
    grouped_close = close.groupby(level="instrument", group_keys=False)
    for horizon in (1, 3, 5, 10, 20):
        out[f"ret_{horizon}"] = grouped_close.pct_change(horizon, fill_method=None)
    for horizon in (20, 60):
        rolling_high = _rolling(high, horizon, "max")
        out[f"drawdown_{horizon}"] = close / rolling_high - 1.0
    previous_close = grouped_close.shift(1)
    true_range = pd.concat(
        [(high - low), (high - previous_close).abs(), (low - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    out["atr14_close"] = _rolling(true_range, 14, "mean") / close
    out["intraday_range"] = (high - low) / close
    out["open_gap"] = open_price / previous_close - 1.0
    for horizon in (5, 20):
        out[f"volume_ratio_{horizon}"] = volume / _rolling(volume, horizon, "mean").where(lambda value: value > 0)
        out[f"amount_ratio_{horizon}"] = amount / _rolling(amount, horizon, "mean").where(lambda value: value > 0)
    out["turnover_rate"] = _number(out.get("db_turnover_rate"))
    out["quoted_volume_ratio"] = _number(out.get("db_volume_ratio"))

    main_net_amount = (
        _number(out.get("mf_lg_buy_amt"))
        + _number(out.get("mf_elg_buy_amt"))
        - _number(out.get("mf_lg_sell_amt"))
        - _number(out.get("mf_elg_sell_amt"))
    )
    elg_net_amount = _number(out.get("mf_elg_buy_amt")) - _number(out.get("mf_elg_sell_amt"))
    out["main_net_amt_ratio"] = main_net_amount / amount.where(amount > 0)
    out["elg_net_amt_ratio"] = elg_net_amount / amount.where(amount > 0)
    for horizon in (5, 20):
        rolling_amount = _rolling(amount, horizon, "sum").where(lambda value: value > 0)
        out[f"main_net_amt_ratio_{horizon}"] = _rolling(main_net_amount, horizon, "sum") / rolling_amount
        out[f"elg_net_amt_ratio_{horizon}"] = _rolling(elg_net_amount, horizon, "sum") / rolling_amount
    pe = _number(out.get("db_pe_ttm"))
    pb = _number(out.get("db_pb"))
    out["value_pe_inv"] = 1.0 / pe.where(pe > 0)
    out["value_pb_inv"] = 1.0 / pb.where(pb > 0)
    out["size_log_mv"] = np.log1p(_number(out.get("db_circ_mv")).clip(lower=0))
    out["revenue_yoy"] = _number(out.get("bb_rev_yoy"))
    out["profit_yoy"] = _number(out.get("bb_profit_yoy"))
    out["gross_margin"] = _number(out.get("bb_gpr"))
    out["net_margin"] = _number(out.get("bb_npr"))
    out["chip_winner_rate"] = _number(out.get("cp_winner_rate"))
    out["chip_cost_spread"] = (
        _number(out.get("cp_cost_95pct")) - _number(out.get("cp_cost_5pct"))
    ) / close.where(close > 0)
    out["chip_cost_position"] = (close - _number(out.get("cp_cost_50pct"))) / close.where(close > 0)
    margin = _number(out.get("md_rzye"))
    out["margin_balance_log"] = np.log1p(margin.clip(lower=0))
    out["margin_balance_change_5"] = margin.groupby(level="instrument", group_keys=False).pct_change(
        5, fill_method=None
    )
    out["l2_code_id"] = _number(out.get("l2_code_id"))
    return out


def _build_sector_features(static: pd.DataFrame, benchmark_daily: pd.DataFrame) -> pd.DataFrame:
    required = {"l2_code_id", "sw2_close", "sw2_amount", "sw2_mf_net_amt"}
    if not required.issubset(static.columns):
        raise AdvisoryModelFirstError(
            "static factors are missing sector fields",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_columns": sorted(required - set(static.columns))},
        )
    reset = static[list(required)].reset_index().dropna(subset=["l2_code_id"])
    value_columns = ["sw2_close", "sw2_amount", "sw2_mf_net_amt"]
    divergent = reset.groupby(["datetime", "l2_code_id"])[value_columns].nunique(dropna=True).max(axis=1)
    if (divergent > 1).any():
        example = divergent[divergent > 1].index[0]
        raise AdvisoryModelFirstError(
            "sector fields disagree inside the same date and L2 identity",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"datetime": str(example[0]), "l2_code_id": int(example[1])},
        )
    sector = reset.groupby(["datetime", "l2_code_id"], sort=True)[value_columns].first().sort_index()
    close = _number(sector["sw2_close"])
    amount = _number(sector["sw2_amount"])
    benchmark = _benchmark_close(benchmark_daily)
    benchmark_returns = {
        horizon: benchmark.pct_change(horizon, fill_method=None) for horizon in (1, 5, 20)
    }
    for horizon in (1, 5, 20):
        sector[f"sector_ret_{horizon}"] = close.groupby(level="l2_code_id", group_keys=False).pct_change(
            horizon, fill_method=None
        )
    sector["sector_excess_5"] = sector["sector_ret_5"] - sector.index.get_level_values("datetime").map(
        benchmark_returns[5]
    )
    sector["sector_excess_20"] = sector["sector_ret_20"] - sector.index.get_level_values("datetime").map(
        benchmark_returns[20]
    )
    sector["sector_amount_ratio_20"] = amount / _rolling_by_level(amount, "l2_code_id", 20, "mean").where(
        lambda value: value > 0
    )
    sector["sector_net_amt_ratio"] = _number(sector["sw2_mf_net_amt"]) / amount.where(amount > 0)
    keep = [
        "sector_ret_1",
        "sector_ret_5",
        "sector_ret_20",
        "sector_excess_5",
        "sector_excess_20",
        "sector_amount_ratio_20",
        "sector_net_amt_ratio",
    ]
    sector.index = sector.index.set_names(["decision_as_of_trade_date", "l2_code_id"])
    return sector[keep]


def _build_benchmark_features(benchmark_daily: pd.DataFrame) -> pd.DataFrame:
    close = _benchmark_close(benchmark_daily)
    result = pd.DataFrame(index=close.index)
    for horizon in (1, 5, 20):
        result[f"csi300_ret_{horizon}"] = close.pct_change(horizon, fill_method=None)
    result.index.name = "decision_as_of_trade_date"
    return result


def _build_market_features(market_daily: pd.DataFrame) -> pd.DataFrame:
    required = {"close", "limit_up"}
    if not required.issubset(market_daily.columns):
        raise AdvisoryModelFirstError(
            "market breadth input is missing required fields",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_columns": sorted(required - set(market_daily.columns))},
        )
    close = _number(market_daily["close"])
    returns = close.groupby(level="instrument", group_keys=False).pct_change(1, fill_method=None)
    valid = returns.notna()
    counts = valid.groupby(level="datetime").sum()
    up = returns.gt(0).where(valid).groupby(level="datetime").mean()
    limit_up = _number(market_daily["limit_up"]).where(valid).groupby(level="datetime").mean()
    volatility = returns.where(valid).groupby(level="datetime").std(ddof=1)
    result = pd.DataFrame(
        {
            "market_up_ratio": up.where(counts >= 100),
            "market_limit_up_ratio": limit_up.where(counts >= 100),
            "market_cross_section_vol": volatility.where(counts >= 100),
        }
    )
    result.index.name = "decision_as_of_trade_date"
    return result


def _normalize_hmm_states(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "hmm_bull_posterior",
                "hmm_state",
                "hmm_state_duration",
                "hmm_observation_completeness",
            ],
            index=pd.MultiIndex.from_arrays([[], []], names=["decision_as_of_trade_date", "l2_code_id"]),
        )
    required = {
        "decision_as_of_trade_date",
        "l2_code_id",
        "hmm_bull_posterior",
        "hmm_state",
        "hmm_state_duration",
        "hmm_observation_completeness",
    }
    if not required.issubset(frame.columns):
        raise AdvisoryModelFirstError(
            "fresh HMM state frame has an invalid schema",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_columns": sorted(required - set(frame.columns))},
        )
    result = frame[list(required)].copy()
    result["decision_as_of_trade_date"] = pd.to_datetime(result["decision_as_of_trade_date"]).dt.normalize()
    return result.set_index(["decision_as_of_trade_date", "l2_code_id"]).sort_index()


def _benchmark_close(frame: pd.DataFrame) -> pd.Series:
    if "close" not in frame.columns:
        raise AdvisoryModelFirstError(
            "CSI300 daily input is missing close",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
        )
    reset = frame.reset_index()
    if reset["datetime"].duplicated().any():
        raise AdvisoryModelFirstError(
            "CSI300 daily input contains duplicate dates",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
        )
    return pd.Series(
        pd.to_numeric(reset["close"], errors="coerce").to_numpy(),
        index=pd.DatetimeIndex(reset["datetime"]).normalize(),
        name="close",
    ).sort_index()


def _rolling(series: pd.Series, window: int, operation: str) -> pd.Series:
    return _rolling_by_level(series, "instrument", window, operation)


def _rolling_by_level(series: pd.Series, level: str, window: int, operation: str) -> pd.Series:
    rolling = series.groupby(level=level, group_keys=False).rolling(window, min_periods=window)
    value = getattr(rolling, operation)()
    return value.droplevel(0).reindex(series.index)


def _number(value: pd.Series | None) -> pd.Series:
    if value is None:
        return pd.Series(dtype=float)
    return pd.to_numeric(value, errors="coerce").astype(float)
