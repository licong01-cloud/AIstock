from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd


RELATION_NAME = "dynamic_residual_flow_relation_v1"
CONTRACT_VERSION = "qe_dynamic_relation_prior_v1"
REPO_ROOT = Path(__file__).resolve().parents[3]
CHANNEL_NAMES = ("residual_return", "flow_state", "leadership_state")
REQUIRED_PANEL_COLUMNS = CHANNEL_NAMES
DEFAULT_LAGS = (1, 5, 10, 20)
TOPOLOGY_COLUMNS = (
    "source_l2_code_id",
    "target_l2_code_id",
    "channel",
    "lag_days",
    "fit_corr",
    "first_half_corr",
    "second_half_corr",
    "stability_score",
    "selection_score",
    "topology_rank",
)


def _normalized_date(value: str | pd.Timestamp) -> pd.Timestamp:
    parsed = pd.Timestamp(value)
    if parsed.tz is not None:
        parsed = parsed.tz_localize(None)
    return parsed.normalize()


def _validated_panel(panel: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(panel.index, pd.MultiIndex):
        raise ValueError("sector panel index must be a MultiIndex")
    if list(panel.index.names) != ["datetime", "l2_code_id"]:
        raise ValueError("sector panel index names must be datetime,l2_code_id")
    if panel.index.has_duplicates:
        raise ValueError("sector panel index must be unique")
    missing = [column for column in REQUIRED_PANEL_COLUMNS if column not in panel]
    if missing:
        raise ValueError("sector panel missing required columns: " + ",".join(missing))

    dates = pd.DatetimeIndex(panel.index.get_level_values("datetime"))
    if dates.tz is not None:
        raise ValueError("sector panel datetime must be timezone-naive")
    raw_l2 = pd.Series(
        panel.index.get_level_values("l2_code_id"), index=panel.index
    )
    numeric_l2 = pd.to_numeric(raw_l2, errors="coerce")
    invalid_l2 = (
        numeric_l2.isna()
        | ~np.isfinite(numeric_l2)
        | ~np.equal(numeric_l2, np.floor(numeric_l2))
        | numeric_l2.le(0)
    )
    if invalid_l2.any():
        raise ValueError("l2_code_id must be a positive integer category")

    result = panel.loc[:, list(REQUIRED_PANEL_COLUMNS)].copy()
    for column in REQUIRED_PANEL_COLUMNS:
        raw = result[column]
        numeric = pd.to_numeric(raw, errors="coerce")
        if (raw.notna() & numeric.isna()).any():
            raise ValueError(f"{column} contains non-numeric values")
        if not np.isfinite(numeric.dropna().to_numpy(dtype=np.float64)).all():
            raise ValueError(f"{column} contains non-finite values")
        result[column] = numeric.astype("float64")
    if result["residual_return"].abs().gt(0.5).any():
        raise ValueError("residual_return must use decimal-return units")
    if result["flow_state"].abs().gt(2.0 + 1e-9).any():
        raise ValueError("flow_state must use a bounded normalized ratio")
    if result["leadership_state"].abs().gt(0.5 + 1e-9).any():
        raise ValueError("leadership_state must be normalized to [-0.5,0.5]")

    result.index = pd.MultiIndex.from_arrays(
        [dates.normalize(), numeric_l2.astype("int32")],
        names=["datetime", "l2_code_id"],
    )
    if result.index.has_duplicates:
        raise ValueError("sector panel becomes non-unique after index normalization")
    return result.sort_index()


def build_channel_panels(panel: pd.DataFrame) -> dict[str, pd.DataFrame]:
    validated = _validated_panel(panel)
    return {
        channel: validated[channel].unstack("l2_code_id").sort_index()
        for channel in CHANNEL_NAMES
    }


def _correlation(
    left: pd.Series,
    right: pd.Series,
    *,
    min_observations: int,
) -> float | None:
    left_values = left.to_numpy(dtype=np.float64, copy=False)
    right_values = right.to_numpy(dtype=np.float64, copy=False)
    valid = np.isfinite(left_values) & np.isfinite(right_values)
    if int(valid.sum()) < min_observations:
        return None
    left_valid = left_values[valid]
    right_valid = right_values[valid]
    if left_valid.std(ddof=0) <= 1e-12 or right_valid.std(ddof=0) <= 1e-12:
        return None
    value = float(np.corrcoef(left_valid, right_valid)[0, 1])
    return value if np.isfinite(value) else None


def _stable_score(
    full_corr: float,
    first_half_corr: float,
    second_half_corr: float,
) -> tuple[float, float]:
    signs = np.sign([full_corr, first_half_corr, second_half_corr])
    if 0 in signs or not np.all(signs == signs[0]):
        return 0.0, 0.0
    stability = min(abs(first_half_corr), abs(second_half_corr))
    return stability, abs(full_corr) * stability


def fit_frozen_topology(
    panel: pd.DataFrame,
    *,
    fit_start: str | pd.Timestamp,
    fit_end: str | pd.Timestamp,
    lags: tuple[int, ...] = DEFAULT_LAGS,
    top_k: int = 5,
    min_observations: int = 80,
) -> pd.DataFrame:
    if not lags or any(int(lag) < 1 for lag in lags):
        raise ValueError("lags must contain positive integers")
    if len(set(int(lag) for lag in lags)) != len(lags):
        raise ValueError("lags must be unique")
    if top_k < 1:
        raise ValueError("top_k must be positive")
    if min_observations < 20:
        raise ValueError("min_observations must be at least 20")

    start = _normalized_date(fit_start)
    end = _normalized_date(fit_end)
    if start > end:
        raise ValueError("fit_start must not be after fit_end")
    channels = build_channel_panels(panel)
    residual = channels["residual_return"]
    fit_dates = residual.index[(residual.index >= start) & (residual.index <= end)]
    if len(fit_dates) < min_observations:
        raise ValueError("fit window has insufficient trading dates")
    split_at = len(fit_dates) // 2
    first_dates = fit_dates[:split_at]
    second_dates = fit_dates[split_at:]
    half_minimum = max(10, min_observations // 2)
    sectors = list(residual.columns)

    candidates: list[dict[str, object]] = []
    for channel_name in CHANNEL_NAMES:
        source_panel = channels[channel_name]
        for lag in sorted(int(value) for value in lags):
            lagged = source_panel.shift(lag)
            for source in sectors:
                source_values = lagged[source]
                for target in sectors:
                    if source == target:
                        continue
                    target_values = residual[target]
                    full_corr = _correlation(
                        source_values.loc[fit_dates],
                        target_values.loc[fit_dates],
                        min_observations=min_observations,
                    )
                    first_corr = _correlation(
                        source_values.loc[first_dates],
                        target_values.loc[first_dates],
                        min_observations=half_minimum,
                    )
                    second_corr = _correlation(
                        source_values.loc[second_dates],
                        target_values.loc[second_dates],
                        min_observations=half_minimum,
                    )
                    if full_corr is None or first_corr is None or second_corr is None:
                        continue
                    stability, selection = _stable_score(
                        full_corr, first_corr, second_corr
                    )
                    if selection <= 0:
                        continue
                    candidates.append(
                        {
                            "source_l2_code_id": int(source),
                            "target_l2_code_id": int(target),
                            "channel": channel_name,
                            "lag_days": lag,
                            "fit_corr": full_corr,
                            "first_half_corr": first_corr,
                            "second_half_corr": second_corr,
                            "stability_score": stability,
                            "selection_score": selection,
                        }
                    )
    if not candidates:
        raise ValueError("no stable relation candidates in fit window")

    frame = pd.DataFrame(candidates)
    frame = frame.sort_values(
        [
            "target_l2_code_id",
            "channel",
            "source_l2_code_id",
            "selection_score",
            "lag_days",
        ],
        ascending=[True, True, True, False, True],
        kind="mergesort",
    )
    frame = frame.drop_duplicates(
        ["source_l2_code_id", "target_l2_code_id", "channel"], keep="first"
    )
    frame = frame.sort_values(
        [
            "target_l2_code_id",
            "channel",
            "selection_score",
            "source_l2_code_id",
            "lag_days",
        ],
        ascending=[True, True, False, True, True],
        kind="mergesort",
    )
    frame = frame.groupby(
        ["target_l2_code_id", "channel"], sort=True, group_keys=False
    ).head(top_k)
    frame["topology_rank"] = (
        frame.groupby(["target_l2_code_id", "channel"], sort=True).cumcount() + 1
    )
    return frame.loc[:, TOPOLOGY_COLUMNS].reset_index(drop=True)


def _validated_topology(topology: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in TOPOLOGY_COLUMNS if column not in topology]
    if missing:
        raise ValueError("topology missing required columns: " + ",".join(missing))
    result = topology.loc[:, TOPOLOGY_COLUMNS].copy()
    if result.empty:
        raise ValueError("topology must not be empty")
    if result.duplicated(
        ["source_l2_code_id", "target_l2_code_id", "channel"]
    ).any():
        raise ValueError("topology must freeze one lag per directed channel edge")
    if not set(result["channel"]).issubset(CHANNEL_NAMES):
        raise ValueError("topology contains unsupported channel")
    for column in (
        "source_l2_code_id",
        "target_l2_code_id",
        "lag_days",
        "topology_rank",
    ):
        numeric = pd.to_numeric(result[column], errors="coerce")
        invalid = (
            numeric.isna()
            | ~np.isfinite(numeric)
            | ~np.equal(numeric, np.floor(numeric))
            | numeric.le(0)
        )
        if invalid.any():
            raise ValueError(f"topology {column} must be a positive integer")
        result[column] = numeric.astype("int32")
    for column in (
        "fit_corr",
        "first_half_corr",
        "second_half_corr",
        "stability_score",
        "selection_score",
    ):
        numeric = pd.to_numeric(result[column], errors="coerce")
        if numeric.isna().any() or not np.isfinite(numeric).all():
            raise ValueError(f"topology {column} must be finite")
        result[column] = numeric.astype("float64")
    for column in ("fit_corr", "first_half_corr", "second_half_corr"):
        if result[column].abs().gt(1.0 + 1e-9).any():
            raise ValueError(f"topology {column} must be within [-1,1]")
    for column in ("stability_score", "selection_score"):
        if result[column].lt(0).any() or result[column].gt(1.0 + 1e-9).any():
            raise ValueError(f"topology {column} must be within [0,1]")
    if (result["lag_days"] < 1).any():
        raise ValueError("topology lag_days must be positive")
    return result.sort_values(
        ["target_l2_code_id", "channel", "topology_rank"], kind="mergesort"
    ).reset_index(drop=True)


def materialize_dynamic_weights(
    panel: pd.DataFrame,
    topology: pd.DataFrame,
    *,
    evaluation_start: str | pd.Timestamp,
    evaluation_end: str | pd.Timestamp,
    rolling_window: int = 120,
    min_observations: int = 80,
) -> pd.DataFrame:
    if rolling_window < 20:
        raise ValueError("rolling_window must be at least 20")
    if min_observations < 20 or min_observations > rolling_window:
        raise ValueError("min_observations must be within [20, rolling_window]")
    start = _normalized_date(evaluation_start)
    end = _normalized_date(evaluation_end)
    if start > end:
        raise ValueError("evaluation_start must not be after evaluation_end")

    frozen = _validated_topology(topology)
    channels = build_channel_panels(panel)
    residual = channels["residual_return"]
    evaluation_dates = residual.index[
        (residual.index >= start) & (residual.index <= end)
    ]
    if evaluation_dates.empty:
        raise ValueError("evaluation window has no sector observations")
    short_window = max(20, rolling_window // 2)
    short_minimum = min(short_window, max(10, min_observations // 2))

    rows: list[pd.DataFrame] = []
    for edge in frozen.itertuples(index=False):
        source = int(edge.source_l2_code_id)
        target = int(edge.target_l2_code_id)
        if source not in channels[edge.channel] or target not in residual:
            raise ValueError("topology sector is absent from the evaluation panel")
        source_values = channels[edge.channel][source].shift(int(edge.lag_days))
        target_values = residual[target]
        long_weight = (
            source_values.rolling(
                rolling_window, min_periods=min_observations
            ).corr(target_values)
        ).shift(1)
        short_weight = (
            source_values.rolling(
                short_window, min_periods=short_minimum
            ).corr(target_values)
        ).shift(1)
        selected = pd.DataFrame(
            {
                "weight": long_weight.reindex(evaluation_dates),
                "short_weight": short_weight.reindex(evaluation_dates),
            },
            index=evaluation_dates,
        ).dropna()
        if selected.empty:
            continue
        same_sign = np.sign(selected["weight"]) == np.sign(selected["short_weight"])
        selected["stability_score"] = np.where(
            same_sign,
            np.minimum(selected["weight"].abs(), selected["short_weight"].abs()),
            0.0,
        )
        selected["effective_weight"] = (
            selected["weight"] * selected["stability_score"]
        )
        selected["source_l2_code_id"] = source
        selected["target_l2_code_id"] = target
        selected["channel"] = edge.channel
        selected["lag_days"] = int(edge.lag_days)
        selected.index.name = "datetime"
        rows.append(selected.reset_index())
    if not rows:
        raise ValueError("dynamic relation weights are empty")
    result = pd.concat(rows, ignore_index=True)
    result = result.sort_values(
        [
            "datetime",
            "target_l2_code_id",
            "channel",
            "source_l2_code_id",
            "lag_days",
        ],
        kind="mergesort",
    ).reset_index(drop=True)
    frozen_edges = set(
        frozen[
            ["source_l2_code_id", "target_l2_code_id", "channel", "lag_days"]
        ].itertuples(index=False, name=None)
    )
    materialized_edges = set(
        result[
            ["source_l2_code_id", "target_l2_code_id", "channel", "lag_days"]
        ].itertuples(index=False, name=None)
    )
    if materialized_edges != frozen_edges:
        raise ValueError("dynamic weights do not cover every frozen topology edge")
    numeric_weights = result[
        ["weight", "short_weight", "stability_score", "effective_weight"]
    ].to_numpy(dtype=np.float64, copy=False)
    if not np.isfinite(numeric_weights).all():
        raise ValueError("dynamic weights must be finite")
    return result


def _require_external_output(path: Path) -> Path:
    resolved = path.resolve()
    if resolved.is_relative_to(REPO_ROOT):
        raise ValueError("output paths must be outside the repository/worktree")
    return resolved


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_relation_artifacts(
    *,
    panel_path: Path,
    topology_path: Path,
    weights_path: Path,
    receipt_path: Path,
    fit_start: str,
    fit_end: str,
    evaluation_start: str,
    evaluation_end: str,
    lags: tuple[int, ...] = DEFAULT_LAGS,
    top_k: int = 5,
    min_observations: int = 80,
    rolling_window: int = 120,
) -> None:
    fit_end_date = _normalized_date(fit_end)
    evaluation_start_date = _normalized_date(evaluation_start)
    if fit_end_date >= evaluation_start_date:
        raise ValueError("fit_end must precede evaluation_start")
    panel_path = panel_path.resolve()
    if panel_path.is_relative_to(REPO_ROOT):
        raise ValueError("panel_path must be outside the repository/worktree")
    if not panel_path.is_file():
        raise ValueError("panel_path must identify an existing Parquet file")
    outputs = [
        _require_external_output(topology_path),
        _require_external_output(weights_path),
        _require_external_output(receipt_path),
    ]
    if len(set([panel_path, *outputs])) != len(outputs) + 1:
        raise ValueError("panel, topology, weights, and receipt paths must be distinct")
    panel = pd.read_parquet(panel_path)
    topology = fit_frozen_topology(
        panel,
        fit_start=fit_start,
        fit_end=fit_end,
        lags=lags,
        top_k=top_k,
        min_observations=min_observations,
    )
    weights = materialize_dynamic_weights(
        panel,
        topology,
        evaluation_start=evaluation_start,
        evaluation_end=evaluation_end,
        rolling_window=rolling_window,
        min_observations=min_observations,
    )
    for output in outputs:
        output.parent.mkdir(parents=True, exist_ok=True)
    topology.to_parquet(outputs[0], index=False)
    weights.to_parquet(outputs[1], index=False)
    receipt = pd.DataFrame(
        [
            {
                "contract_version": CONTRACT_VERSION,
                "relation_name": RELATION_NAME,
                "role": "RELATION_PRIOR",
                "panel_sha256": _sha256(panel_path),
                "topology_sha256": _sha256(outputs[0]),
                "weights_sha256": _sha256(outputs[1]),
                "fit_start": str(_normalized_date(fit_start).date()),
                "fit_end": str(fit_end_date.date()),
                "evaluation_start": str(evaluation_start_date.date()),
                "evaluation_end": str(_normalized_date(evaluation_end).date()),
                "lags": ",".join(str(value) for value in sorted(lags)),
                "top_k": top_k,
                "min_observations": min_observations,
                "rolling_window": rolling_window,
                "topology_rows": len(topology),
                "weight_rows": len(weights),
            }
        ]
    )
    receipt.to_parquet(outputs[2], index=False)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a frozen-topology, historical-only QE relation prior."
    )
    parser.add_argument("--panel-path", required=True, type=Path)
    parser.add_argument("--topology-path", required=True, type=Path)
    parser.add_argument("--weights-path", required=True, type=Path)
    parser.add_argument("--receipt-path", required=True, type=Path)
    parser.add_argument("--fit-start", required=True)
    parser.add_argument("--fit-end", required=True)
    parser.add_argument("--evaluation-start", required=True)
    parser.add_argument("--evaluation-end", required=True)
    parser.add_argument("--lags", default="1,5,10,20")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--min-observations", type=int, default=80)
    parser.add_argument("--rolling-window", type=int, default=120)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        lags = tuple(int(value.strip()) for value in args.lags.split(","))
    except ValueError as exc:
        raise ValueError("--lags must be a comma-separated integer list") from exc
    build_relation_artifacts(
        panel_path=args.panel_path,
        topology_path=args.topology_path,
        weights_path=args.weights_path,
        receipt_path=args.receipt_path,
        fit_start=args.fit_start,
        fit_end=args.fit_end,
        evaluation_start=args.evaluation_start,
        evaluation_end=args.evaluation_end,
        lags=lags,
        top_k=args.top_k,
        min_observations=args.min_observations,
        rolling_window=args.rolling_window,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
