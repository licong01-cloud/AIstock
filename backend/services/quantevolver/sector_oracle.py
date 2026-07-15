"""QE-only four-cell sector oracle and soft-gating research evaluator.

The oracle branches intentionally use future returns and are therefore upper
bounds, never deployable signals. Results quantify the current trial and never
emit GO/STOP or research-direction decisions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd

from .long_trend_evaluation import moving_block_bootstrap_mean, newey_west_mean_test


ORACLE_CLASSIFICATION = "QE_ONLY_FUTURE_INFORMATION_CEILING"
REALITY_CLASSIFICATION = "QE_ONLY_REALITY_DIAGNOSTIC"
_CELLS = (
    ("reality", "reality"),
    ("oracle", "reality"),
    ("reality", "oracle"),
    ("oracle", "oracle"),
)


@dataclass(frozen=True)
class SectorOracleConfig:
    horizon: int
    sector_top_m: int
    stock_top_k: int
    round_trip_cost_bps: float
    barriers: tuple[float, ...] = (0.30, 0.50, 0.70)
    bootstrap_samples: int = 500
    bootstrap_seed: int = 20260716

    def __post_init__(self) -> None:
        for field, value in (
            ("horizon", self.horizon),
            ("sector_top_m", self.sector_top_m),
            ("stock_top_k", self.stock_top_k),
            ("bootstrap_samples", self.bootstrap_samples),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field} must be a positive integer, got {value!r}")
        if not np.isfinite(self.round_trip_cost_bps) or self.round_trip_cost_bps < 0:
            raise ValueError("round_trip_cost_bps must be a finite non-negative number")
        if not self.barriers or any(
            not np.isfinite(value) or value <= 0 for value in self.barriers
        ):
            raise ValueError("barriers must contain positive finite values")


@dataclass(frozen=True)
class SectorOracleResult:
    config: SectorOracleConfig
    daily: pd.DataFrame
    selections: pd.DataFrame
    summaries: list[dict[str, Any]]
    eligibility: dict[str, Any]


def _validate_observations(
    observations: pd.DataFrame,
    *,
    config: SectorOracleConfig,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not isinstance(observations, pd.DataFrame) or observations.empty:
        raise ValueError("sector oracle requires non-empty F-014 signal observations")
    horizon = config.horizon
    required = {
        "signal_date",
        "instrument",
        "score",
        "l2_code_id",
        "entry_close_qfq",
        "entry_suspension_diagnostic",
        f"return_{horizon}",
        f"maturity_{horizon}",
    }
    missing = sorted(required - set(observations.columns))
    if missing:
        raise ValueError(f"sector oracle observations are missing columns: {missing}")
    frame = observations.loc[:, sorted(required)].copy()
    frame["signal_date"] = pd.to_datetime(frame["signal_date"], errors="coerce").dt.normalize()
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    frame["forward_return"] = pd.to_numeric(frame[f"return_{horizon}"], errors="coerce")
    frame["entry_close_qfq"] = pd.to_numeric(frame["entry_close_qfq"], errors="coerce")
    frame["l2_code_id"] = pd.to_numeric(frame["l2_code_id"], errors="coerce")
    frame["entry_suspension_diagnostic"] = frame["entry_suspension_diagnostic"].astype("boolean")
    frame["maturity"] = frame[f"maturity_{horizon}"].astype("string")
    identity_invalid = (
        frame["signal_date"].isna()
        | frame["instrument"].isna()
        | frame.duplicated(["signal_date", "instrument"])
    )
    if bool(identity_invalid.any()):
        raise ValueError(
            "sector oracle signal identity contains invalid or duplicate date/instrument rows"
        )

    criteria = {
        "mature": frame["maturity"].isin(("matured", "mature")),
        "finite_score": np.isfinite(frame["score"]),
        "finite_forward_return": np.isfinite(frame["forward_return"]),
        "valid_entry_close": np.isfinite(frame["entry_close_qfq"])
        & frame["entry_close_qfq"].gt(0.0),
        "pit_sector_present": frame["l2_code_id"].notna() & frame["l2_code_id"].ge(0),
        "not_suspended_on_entry": ~frame["entry_suspension_diagnostic"].fillna(True),
    }
    eligible = pd.Series(True, index=frame.index)
    for mask in criteria.values():
        eligible &= mask
    audit = {
        "input_rows": int(len(frame)),
        "eligible_rows": int(eligible.sum()),
        "excluded_rows": int((~eligible).sum()),
        "criterion_pass_counts": {key: int(mask.sum()) for key, mask in criteria.items()},
        "tradability_contract": (
            "finite_T_plus_1_close_and_not_suspended; daily limit state is not treated as "
            "authoritative non-fill evidence"
        ),
        "pit_sector_contract": "signal_date_sw_l2_l2_code_id",
    }
    selected = frame.loc[eligible].copy()
    if selected.empty:
        raise ValueError("sector oracle has no eligible mature observations for this horizon")
    selected["l2_code_id"] = selected["l2_code_id"].astype("int64")
    return selected, audit


def _rank_percentile(values: pd.Series) -> pd.Series:
    return values.rank(method="average", pct=True)


def _sector_panel(
    day: pd.DataFrame,
    *,
    external_reality_scores: pd.DataFrame | None = None,
) -> pd.DataFrame:
    enriched = day.copy()
    enriched["stock_reality_rank_pct"] = _rank_percentile(enriched["score"])
    panel = (
        enriched.groupby("l2_code_id", sort=True)
        .agg(
            reality_sector_score=("stock_reality_rank_pct", "mean"),
            oracle_sector_score=("forward_return", "mean"),
            member_count=("instrument", "size"),
        )
        .reset_index()
    )
    panel = panel.rename(
        columns={"reality_sector_score": "stock_aggregate_reality_sector_score"}
    )
    if external_reality_scores is None:
        panel["reality_sector_score"] = panel["stock_aggregate_reality_sector_score"]
    else:
        panel = panel.merge(
            external_reality_scores.loc[:, ["l2_code_id", "sector_score"]],
            on="l2_code_id",
            how="left",
            validate="one_to_one",
        )
        panel = panel.rename(columns={"sector_score": "reality_sector_score"})
    panel["reality_sector_rank_pct"] = _rank_percentile(panel["reality_sector_score"])
    panel["oracle_sector_rank_pct"] = _rank_percentile(panel["oracle_sector_score"])
    return panel


def _select_stocks(
    day: pd.DataFrame,
    panel: pd.DataFrame,
    *,
    sector_source: str,
    stock_source: str,
    mode: str,
    sector_top_m: int,
    stock_top_k: int,
) -> tuple[pd.DataFrame, tuple[int, ...]]:
    sector_score = f"{sector_source}_sector_score"
    sector_rank_pct = f"{sector_source}_sector_rank_pct"
    stock_score = "score" if stock_source == "reality" else "forward_return"
    ordered_sectors = panel.sort_values(
        [sector_score, "l2_code_id"],
        ascending=[False, True],
        kind="mergesort",
    )
    ordered_sectors = ordered_sectors.loc[np.isfinite(ordered_sectors[sector_score])]
    hard_sectors = tuple(
        int(value) for value in ordered_sectors.head(sector_top_m)["l2_code_id"].tolist()
    )
    candidates = day.merge(
        panel.loc[:, ["l2_code_id", sector_score, sector_rank_pct]],
        on="l2_code_id",
        how="inner",
        validate="many_to_one",
    )
    if mode == "hard":
        candidates = candidates.loc[candidates["l2_code_id"].isin(hard_sectors)].copy()
        candidates["selection_score"] = candidates[stock_score]
    elif mode == "soft":
        candidates = candidates.loc[np.isfinite(candidates[sector_rank_pct])].copy()
        candidates["stock_rank_pct"] = _rank_percentile(candidates[stock_score])
        candidates["selection_score"] = (
            candidates["stock_rank_pct"] * candidates[sector_rank_pct]
        )
    else:
        raise ValueError(f"unsupported sector oracle selection mode {mode!r}")
    chosen = candidates.sort_values(
        ["selection_score", "instrument"],
        ascending=[False, True],
        kind="mergesort",
    ).head(stock_top_k)
    return chosen, hard_sectors


def _one_layer_select(day: pd.DataFrame, *, stock_source: str, stock_top_k: int) -> pd.DataFrame:
    stock_score = "score" if stock_source == "reality" else "forward_return"
    chosen = day.sort_values(
        [stock_score, "instrument"],
        ascending=[False, True],
        kind="mergesort",
    ).head(stock_top_k).copy()
    chosen["selection_score"] = chosen[stock_score]
    return chosen


def _selection_hhi(chosen: pd.DataFrame) -> float | None:
    if chosen.empty:
        return None
    shares = chosen["l2_code_id"].value_counts(normalize=True)
    return float(np.square(shares.to_numpy(dtype="float64")).sum())


def _barrier_recall(day: pd.DataFrame, chosen: pd.DataFrame, barrier: float) -> float | None:
    winners = set(day.loc[day["forward_return"] >= barrier, "instrument"].astype(str))
    if not winners:
        return None
    captured = winners & set(chosen["instrument"].astype(str))
    return float(len(captured) / len(winners))


def _daily_record(
    *,
    signal_date: pd.Timestamp,
    cell: str,
    mode: str,
    eligible_day: pd.DataFrame,
    chosen: pd.DataFrame,
    oracle_top_sectors: tuple[int, ...],
    hard_sectors: tuple[int, ...],
    previous_instruments: set[str] | None,
    config: SectorOracleConfig,
) -> tuple[dict[str, Any], set[str]]:
    instruments = set(chosen["instrument"].astype(str))
    if previous_instruments is None:
        turnover = 1.0 if instruments else 0.0
    elif not instruments and not previous_instruments:
        turnover = 0.0
    else:
        denominator = max(len(instruments), len(previous_instruments), 1)
        turnover = 1.0 - len(instruments & previous_instruments) / denominator
    gross_return = float(chosen["forward_return"].mean()) if not chosen.empty else None
    cost = turnover * config.round_trip_cost_bps / 10_000.0
    selected_sectors = set(int(value) for value in chosen["l2_code_id"].unique())
    sector_recall = (
        float(len(selected_sectors & set(oracle_top_sectors)) / len(oracle_top_sectors))
        if oracle_top_sectors
        else None
    )
    record: dict[str, Any] = {
        "signal_date": signal_date,
        "cell": cell,
        "mode": mode,
        "selected_count": int(len(chosen)),
        "selected_sector_count": int(len(selected_sectors)),
        "hard_sector_ids": list(hard_sectors),
        "gross_forward_return": gross_return,
        "turnover_proxy": float(turnover),
        "cost_proxy": float(cost),
        "net_forward_return_proxy": gross_return - cost if gross_return is not None else None,
        "sector_recall_at_m": sector_recall,
        "sector_hhi": _selection_hhi(chosen),
    }
    for barrier in config.barriers:
        record[f"recall_barrier_{int(round(barrier * 100))}"] = _barrier_recall(
            day=eligible_day,
            chosen=chosen,
            barrier=barrier,
        )
    return record, instruments


def _summary_statistics(
    values: Iterable[float],
    *,
    config: SectorOracleConfig,
    seed_offset: int,
) -> dict[str, Any]:
    clean = [float(value) for value in values if value is not None and np.isfinite(value)]
    return {
        "newey_west": newey_west_mean_test(clean, lag=max(config.horizon - 1, 0)),
        "moving_block_bootstrap": moving_block_bootstrap_mean(
            clean,
            block_length=max(config.horizon, 1),
            samples=config.bootstrap_samples,
            seed=config.bootstrap_seed + seed_offset,
        ),
    }


def _normalize_reality_sector_scores(
    value: pd.DataFrame | None,
) -> tuple[pd.DataFrame | None, dict[str, Any]]:
    if value is None:
        return None, {
            "reality_sector_score_source": "daily_mean_stock_score_percentile",
            "external_sector_score_rows": 0,
        }
    if not isinstance(value, pd.DataFrame) or value.empty:
        raise ValueError("external reality sector scores must be a non-empty DataFrame")
    required = {"signal_date", "l2_code_id", "sector_score"}
    missing = sorted(required - set(value.columns))
    if missing:
        raise ValueError(f"external reality sector scores are missing columns: {missing}")
    frame = value.loc[:, sorted(required)].copy()
    frame["signal_date"] = pd.to_datetime(frame["signal_date"], errors="coerce").dt.normalize()
    frame["l2_code_id"] = pd.to_numeric(frame["l2_code_id"], errors="coerce")
    frame["sector_score"] = pd.to_numeric(frame["sector_score"], errors="coerce")
    invalid_identity = (
        frame["signal_date"].isna()
        | frame["l2_code_id"].isna()
        | frame.duplicated(["signal_date", "l2_code_id"])
    )
    if bool(invalid_identity.any()):
        raise ValueError("external reality sector scores contain invalid or duplicate identities")
    frame["l2_code_id"] = frame["l2_code_id"].astype("int64")
    negative_sector_id = frame["l2_code_id"].lt(0)
    finite = np.isfinite(frame["sector_score"]) & ~negative_sector_id
    return frame.loc[finite].copy(), {
        "reality_sector_score_source": "external_qe_sector_model",
        "external_sector_score_rows": int(len(frame)),
        "finite_external_sector_score_rows": int(finite.sum()),
        "negative_sector_score_identity_rows": int(negative_sector_id.sum()),
    }


def compute_sector_oracle_grid(
    observations: pd.DataFrame,
    *,
    config: SectorOracleConfig,
    reality_sector_scores: pd.DataFrame | None = None,
    reality_sector_score_name: str = "daily_mean_stock_score_percentile",
) -> SectorOracleResult:
    eligible, eligibility = _validate_observations(observations, config=config)
    normalized_sector_scores, score_audit = _normalize_reality_sector_scores(
        reality_sector_scores
    )
    if normalized_sector_scores is not None:
        score_audit["reality_sector_score_source"] = str(reality_sector_score_name)
        eligible_sector_keys = eligible.loc[:, ["signal_date", "l2_code_id"]].drop_duplicates()
        score_keys = normalized_sector_scores.loc[:, ["signal_date", "l2_code_id"]]
        coverage = eligible_sector_keys.merge(
            score_keys.assign(_covered=True),
            on=["signal_date", "l2_code_id"],
            how="left",
            validate="one_to_one",
        )
        score_audit["eligible_sector_key_count"] = int(len(coverage))
        covered = coverage["_covered"].eq(True)
        score_audit["covered_eligible_sector_key_count"] = int(covered.sum())
        score_audit["eligible_sector_score_coverage"] = float(covered.mean())
    eligibility.update(score_audit)
    daily_records: list[dict[str, Any]] = []
    selection_records: list[pd.DataFrame] = []
    previous: dict[tuple[str, str], set[str]] = {}

    for signal_date, day in eligible.groupby("signal_date", sort=True):
        day = day.sort_values("instrument", kind="mergesort").reset_index(drop=True)
        external_day = None
        if normalized_sector_scores is not None:
            external_day = normalized_sector_scores.loc[
                normalized_sector_scores["signal_date"].eq(signal_date)
            ]
        panel = _sector_panel(day, external_reality_scores=external_day)
        oracle_top_sectors = tuple(
            int(value)
            for value in panel.sort_values(
                ["oracle_sector_score", "l2_code_id"],
                ascending=[False, True],
                kind="mergesort",
            )
            .head(config.sector_top_m)["l2_code_id"]
            .tolist()
        )

        one_layer_specs = (
            ("one_layer_reality", "reality"),
            ("one_layer_oracle", "oracle"),
        )
        for cell, stock_source in one_layer_specs:
            chosen = _one_layer_select(day, stock_source=stock_source, stock_top_k=config.stock_top_k)
            key = (cell, "one_layer")
            record, selected = _daily_record(
                signal_date=pd.Timestamp(signal_date),
                cell=cell,
                mode="one_layer",
                eligible_day=day,
                chosen=chosen,
                oracle_top_sectors=oracle_top_sectors,
                hard_sectors=(),
                previous_instruments=previous.get(key),
                config=config,
            )
            previous[key] = selected
            daily_records.append(record)
            detail = chosen.loc[
                :, ["instrument", "l2_code_id", "score", "forward_return", "selection_score"]
            ].copy()
            detail.insert(0, "signal_date", signal_date)
            detail["cell"] = cell
            detail["mode"] = "one_layer"
            detail["selection_rank"] = np.arange(1, len(detail) + 1)
            selection_records.append(detail)

        for sector_source, stock_source in _CELLS:
            cell = f"{sector_source}_sector__{stock_source}_stock"
            for mode in ("hard", "soft"):
                chosen, hard_sectors = _select_stocks(
                    day,
                    panel,
                    sector_source=sector_source,
                    stock_source=stock_source,
                    mode=mode,
                    sector_top_m=config.sector_top_m,
                    stock_top_k=config.stock_top_k,
                )
                key = (cell, mode)
                record, selected = _daily_record(
                    signal_date=pd.Timestamp(signal_date),
                    cell=cell,
                    mode=mode,
                    eligible_day=day,
                    chosen=chosen,
                    oracle_top_sectors=oracle_top_sectors,
                    hard_sectors=hard_sectors,
                    previous_instruments=previous.get(key),
                    config=config,
                )
                previous[key] = selected
                daily_records.append(record)
                detail = chosen.loc[
                    :, ["instrument", "l2_code_id", "score", "forward_return", "selection_score"]
                ].copy()
                detail.insert(0, "signal_date", signal_date)
                detail["cell"] = cell
                detail["mode"] = mode
                detail["selection_rank"] = np.arange(1, len(detail) + 1)
                selection_records.append(detail)

    daily = pd.DataFrame.from_records(daily_records).sort_values(
        ["cell", "mode", "signal_date"], kind="mergesort"
    )
    selections = pd.concat(selection_records, ignore_index=True, sort=False)
    baseline = daily.loc[
        (daily["cell"] == "reality_sector__reality_stock") & (daily["mode"] == "hard"),
        ["signal_date", "net_forward_return_proxy"],
    ].rename(columns={"net_forward_return_proxy": "baseline_net_forward_return_proxy"})
    daily = daily.merge(baseline, on="signal_date", how="left", validate="many_to_one")
    daily["incremental_net_return_vs_hard_reality_reality"] = (
        daily["net_forward_return_proxy"] - daily["baseline_net_forward_return_proxy"]
    )

    summaries: list[dict[str, Any]] = []
    for sequence, ((cell, mode), group) in enumerate(
        daily.groupby(["cell", "mode"], sort=True), start=1
    ):
        contains_oracle = "oracle" in cell
        summary = {
            "cell": cell,
            "mode": mode,
            "classification": ORACLE_CLASSIFICATION if contains_oracle else REALITY_CLASSIFICATION,
            "signal_day_count": int(group["signal_date"].nunique()),
            "mean_gross_forward_return": float(group["gross_forward_return"].mean()),
            "mean_net_forward_return_proxy": float(group["net_forward_return_proxy"].mean()),
            "mean_turnover_proxy": float(group["turnover_proxy"].mean()),
            "mean_cost_proxy": float(group["cost_proxy"].mean()),
            "mean_sector_recall_at_m": float(group["sector_recall_at_m"].mean()),
            "mean_sector_hhi": float(group["sector_hhi"].mean()),
            "mean_incremental_net_return_vs_hard_reality_reality": float(
                group["incremental_net_return_vs_hard_reality_reality"].mean()
            ),
            "net_forward_return_statistics": _summary_statistics(
                group["net_forward_return_proxy"], config=config, seed_offset=sequence * 2
            ),
            "incremental_statistics": _summary_statistics(
                group["incremental_net_return_vs_hard_reality_reality"],
                config=config,
                seed_offset=sequence * 2 + 1,
            ),
            "research_decision": None,
            "research_note": (
                "quantifies this QE trial only; no GO/STOP and no direction elimination"
            ),
            "reality_sector_score_source": eligibility["reality_sector_score_source"],
        }
        for barrier in config.barriers:
            column = f"recall_barrier_{int(round(barrier * 100))}"
            summary[f"mean_{column}"] = float(group[column].mean())
        summaries.append(summary)

    return SectorOracleResult(
        config=config,
        daily=daily,
        selections=selections,
        summaries=summaries,
        eligibility=eligibility,
    )
