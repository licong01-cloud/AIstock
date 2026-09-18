#!/usr/bin/env python3
"""Precompute sector-HMM coefficients for QE/Paper selection.

The script runs inside WSL and reads a JSON payload from stdin. It supports two
model JSON schemas used by AIstock:
- legacy 4/7-feature sector HMM snapshots with optional zscore_mean/zscore_std;
- horizon-v2 snapshots produced by scripts/hmm_horizon_v2_train.py with
  feature_names + preprocess metadata.

It prints the coefficient JSON to stdout and writes the same JSON to
--output-path when provided. Business failures are fail-fast: empty sector decode
or empty daily coefficients are treated as errors, never as a successful neutral
fallback.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np


FROZEN_INPUT_SCHEMA = "qe_hmm_frozen_input_v1"
FROZEN_CODE_MAP_SCHEMA = "aistock_release_sw_l2_code_map_v1"
FROZEN_FILE_KEYS = (
    "sector_data_h5",
    "index_daily_h5",
    "sector_code_map_json",
    "market_context_parquet",
    "sector_membership_spans_parquet",
)
FROZEN_MARKET_VOLUME_DEFINITION = "sum_market_sw_daily_vol_all_rows_v1"

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(PROJECT_ROOT))


def forward_filter_posteriors(hmm: Any, obs: np.ndarray) -> np.ndarray:
    """Causal forward-filter posterior probabilities; no future observations are used."""
    from hmmlearn import _hmmc

    log_frameprob = hmm._compute_log_likelihood(obs)
    _, fwd_lattice = _hmmc.forward_log(hmm.startprob_, hmm.transmat_, log_frameprob)
    row_max = np.max(fwd_lattice, axis=1, keepdims=True)
    row_max = np.where(np.isfinite(row_max), row_max, 0.0)
    posteriors = np.exp(fwd_lattice - row_max)
    denom = posteriors.sum(axis=1, keepdims=True)
    n_states = posteriors.shape[1]
    posteriors = np.divide(
        posteriors,
        denom,
        out=np.full_like(posteriors, 1.0 / max(n_states, 1)),
        where=denom > 0,
    )
    return np.nan_to_num(
        posteriors,
        nan=1.0 / max(n_states, 1),
        posinf=1.0 / max(n_states, 1),
        neginf=0.0,
    )


def forward_filter_states(hmm: Any, obs: np.ndarray) -> np.ndarray:
    """Causal forward-filter state decoding; no future observations are used."""
    return forward_filter_posteriors(hmm, obs).argmax(axis=1)


def confidence_from_posterior(prob: np.ndarray, scale: float) -> float:
    prob = np.nan_to_num(prob, nan=0.0, posinf=0.0, neginf=0.0)
    total = float(prob.sum())
    if total <= 0:
        return 0.0
    prob = prob / total
    ordered = np.sort(prob)
    margin = float(ordered[-1] - ordered[-2]) if len(ordered) > 1 else 1.0
    return float(np.clip(margin / max(scale, 1e-6), 0.0, 1.0))


def utility_from_state_stats(
    prob: np.ndarray,
    state_stats: dict[str, Any],
    method: str,
    horizon_weights: dict[int, float],
) -> float:
    if method in {"er", "er_winsor", "er_median"}:
        field_prefix = {"er": "mu", "er_winsor": "winsor_mu", "er_median": "median"}[method]
        total = 0.0
        for horizon, weight in horizon_weights.items():
            vals = np.asarray(
                [state_stats[str(s)].get(f"{field_prefix}_{horizon}d", 0.0) for s in range(len(prob))],
                dtype=np.float64,
            )
            total += weight * float(np.dot(prob, vals))
        return total
    if method in {"pup", "pup_z", "pup_rank", "additive_pup"}:
        total = 0.0
        for horizon, weight in horizon_weights.items():
            vals = np.asarray(
                [state_stats[str(s)].get(f"pup_{horizon}d", 0.5) for s in range(len(prob))],
                dtype=np.float64,
            )
            total += weight * float(np.dot(prob, vals))
        return 2.0 * (total - 0.5)
    raise ValueError(f"Unknown dynamic HMM method: {method}")


def robust_z_by_date(raw_by_sector: dict[str, float]) -> dict[str, float]:
    values = np.asarray(
        [v for v in raw_by_sector.values() if math.isfinite(v)],
        dtype=np.float64,
    )
    if len(values) < 3:
        return {k: 0.0 for k in raw_by_sector}
    med = float(np.median(values))
    q25, q75 = np.quantile(values, [0.25, 0.75])
    iqr = float(q75 - q25)
    scale = iqr / 1.349 if iqr > 1e-12 else float(np.std(values))
    if scale < 1e-12:
        return {k: 0.0 for k in raw_by_sector}
    return {k: float(np.clip((v - med) / scale, -3.0, 3.0)) for k, v in raw_by_sector.items()}


def rank_signal_by_date(raw_by_sector: dict[str, float]) -> dict[str, float]:
    valid = [(k, v) for k, v in raw_by_sector.items() if math.isfinite(v)]
    if len(valid) < 2:
        return {k: 0.0 for k in raw_by_sector}
    ordered = sorted(valid, key=lambda item: item[1])
    denom = max(len(ordered) - 1, 1)
    ranks = {k: (idx / denom) * 2.0 - 1.0 for idx, (k, _) in enumerate(ordered)}
    return {k: float(ranks.get(k, 0.0)) for k in raw_by_sector}


def build_stock_sector_maps_by_date(
    membership_rows: list[dict[str, Any]],
    trade_dates: list[date],
) -> dict[str, dict[str, str]]:
    """Build exact point-in-time stock-sector maps; conflicting memberships fail closed."""

    output: dict[str, dict[str, str]] = {}
    for trade_date in sorted(set(trade_dates)):
        day_map: dict[str, str] = {}
        for row in membership_rows:
            symbol = str(row.get("ts_code") or "").strip().upper()
            sector = str(row.get("l2_code") or "").strip()
            in_date = row.get("in_date")
            out_date = row.get("out_date")
            if (
                not symbol
                or not sector
                or not isinstance(in_date, date)
                or in_date > trade_date
                or (isinstance(out_date, date) and out_date < trade_date)
            ):
                continue
            existing = day_map.get(symbol)
            if existing is not None and existing != sector:
                raise ValueError(
                    "conflicting stock-sector memberships for "
                    f"trade_date={trade_date.isoformat()} symbol={symbol}: {existing} != {sector}"
                )
            day_map[symbol] = sector
        if not day_map:
            raise ValueError(f"empty point-in-time stock-sector map for {trade_date.isoformat()}")
        output[trade_date.isoformat()] = day_map
    return output


def build_input_data_max_dates_by_date(
    *,
    trade_dates: list[date],
    sector_dates: list[date],
    index_dates: list[date],
    market_volume_dates: list[date],
) -> dict[str, dict[str, str]]:
    """Close the causal source watermark used by every daily coefficient."""

    sources = {
        "sector_data": sorted(set(sector_dates)),
        "index_daily": sorted(set(index_dates)),
        "sw_daily": sorted(set(market_volume_dates)),
    }
    output: dict[str, dict[str, str]] = {}
    for trade_date in sorted(set(trade_dates)):
        day: dict[str, str] = {}
        for source, available_dates in sources.items():
            eligible = [value for value in available_dates if value <= trade_date]
            if not eligible:
                raise ValueError(f"{source} has no input watermark on or before {trade_date.isoformat()}")
            day[source] = eligible[-1].isoformat()
        day["sw_index_member_effective_as_of"] = trade_date.isoformat()
        output[trade_date.isoformat()] = day
    return output


def restore_hmm(info: dict[str, Any]) -> Any:
    """Rebuild GaussianHMM from both legacy and horizon-v2 JSON payloads."""
    from hmmlearn.hmm import GaussianHMM

    n_states = int(info["n_states"])
    cov_type = info.get("covariance_type", "diag")
    hmm = GaussianHMM(n_components=n_states, covariance_type=cov_type)
    hmm.startprob_ = np.asarray(
        info.get("startprob") or np.full(n_states, 1.0 / n_states),
        dtype=np.float64,
    )
    hmm.transmat_ = np.asarray(info["transmat"], dtype=np.float64)
    hmm.means_ = np.asarray(info["means"], dtype=np.float64)
    covars = np.asarray(info["covars"], dtype=np.float64)
    if cov_type == "diag":
        if covars.ndim == 3:
            covars = np.asarray([np.diag(covars[i]) for i in range(covars.shape[0])], dtype=np.float64)
        covars = np.maximum(covars, 1e-6)
    elif cov_type == "full":
        for i in range(covars.shape[0]):
            covars[i] = (covars[i] + covars[i].T) / 2
            covars[i] += np.eye(covars[i].shape[0]) * 1e-6
    hmm.covars_ = covars
    return hmm


def build_legacy_observations(
    rows_by_date: dict[date, dict[str, Any]],
    sorted_dates: list[date],
    csi300_pct: dict[date, float],
    market_vol: dict[date, float],
    rolling_window: int,
    n_features: int,
) -> tuple[np.ndarray, list[date]]:
    rows: list[list[float]] = []
    dates_out: list[date] = []
    win = max(rolling_window, 2)
    for i, td in enumerate(sorted_dates):
        rec = rows_by_date[td]
        csi_pct = csi300_pct.get(td)
        mvol = market_vol.get(td)
        if csi_pct is None or mvol is None:
            continue

        pct = _require_finite_float(rec["sw2_pct_change"], field="sw2_pct_change", trade_date=td)
        vol = _require_finite_float(rec["sw2_vol"], field="sw2_vol", trade_date=td)
        amount = _require_finite_float(rec["sw2_amount"], field="sw2_amount", trade_date=td)
        mf_net = _require_finite_float(rec["sw2_mf_net_amt"], field="sw2_mf_net_amt", trade_date=td)
        mf_buy_elg = _require_finite_float(
            rec["sw2_mf_buy_elg_amt"], field="sw2_mf_buy_elg_amt", trade_date=td
        )
        mf_sell_elg = _require_finite_float(
            rec["sw2_mf_sell_elg_amt"], field="sw2_mf_sell_elg_amt", trade_date=td
        )

        daily_ret = pct / 100.0
        csi_window: list[float] = []
        ret_window: list[float] = []
        for j in range(max(0, i - win + 1), i + 1):
            d2 = sorted_dates[j]
            c2 = csi300_pct.get(d2)
            if c2 is not None and d2 in rows_by_date:
                ret2 = (
                    _require_finite_float(
                        rows_by_date[d2]["sw2_pct_change"],
                        field="sw2_pct_change",
                        trade_date=d2,
                    )
                    / 100.0
                )
                csi_window.append(ret2 - c2 / 100.0)
                ret_window.append(ret2)

        excess_nd = float(np.mean(csi_window)) if csi_window else 0.0
        vol_ratio = vol / mvol if mvol > 0 else 0.0
        limit_up_ratio = 0.0
        volatility = float(np.std(ret_window)) if len(ret_window) > 1 else 0.0
        mf_net_ratio = mf_net / amount if amount > 0 else 0.0
        elg_ratio = (mf_buy_elg - mf_sell_elg) / amount if amount > 0 else 0.0

        if n_features >= 7:
            row = [daily_ret, excess_nd, vol_ratio, limit_up_ratio, volatility, mf_net_ratio, elg_ratio]
        else:
            row = [daily_ret, excess_nd, vol_ratio, limit_up_ratio]
        if any(np.isnan(v) or np.isinf(v) for v in row):
            continue
        rows.append(row)
        dates_out.append(td)
    return np.asarray(rows, dtype=np.float64), dates_out


def build_horizon_v2_observations(
    rows: list[dict[str, Any]],
    csi300_pct: dict[date, float],
    market_vol: dict[date, float],
    preprocess: dict[str, Any],
) -> tuple[np.ndarray, list[date]]:
    from hmm_horizon_v2_train import build_observations, preprocess_apply

    csi300_decimal = {td: pct / 100.0 for td, pct in csi300_pct.items()}
    obs, dates_out, _ = build_observations(rows, csi300_decimal, market_vol)
    if len(obs):
        obs = preprocess_apply(obs, preprocess)
    return obs, dates_out


def parse_stdin() -> dict[str, Any]:
    raw = sys.stdin.read().lstrip("\ufeff")
    if not raw.strip():
        print("ERROR: no stdin JSON payload received", file=sys.stderr)
        sys.exit(1)
    return json.loads(raw)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_finite_float(
    value: Any,
    *,
    field: str,
    trade_date: Any | None = None,
    sector_code: str | None = None,
) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "invalid frozen sector value: "
            f"field={field} trade_date={trade_date} sector={sector_code} value={value!r}"
        ) from exc
    if not math.isfinite(parsed):
        raise ValueError(
            "non-finite frozen sector value: "
            f"field={field} trade_date={trade_date} sector={sector_code} value={value!r}"
        )
    return parsed


def build_static_daily_coefficients(
    sector_date_labels: dict[str, dict[str, str]],
    *,
    expected_sector_codes: list[str],
    expected_dates: list[str],
    preset_coeffs: dict[str, float],
) -> dict[str, dict[str, float]]:
    """Build one exact sector/date coefficient grid without neutral fallback."""

    expected_sectors = set(expected_sector_codes)
    actual_sectors = set(sector_date_labels)
    if actual_sectors != expected_sectors:
        raise ValueError(
            "decoded HMM sector set differs from frozen code map: "
            f"missing={sorted(expected_sectors - actual_sectors)[:10]} "
            f"unexpected={sorted(actual_sectors - expected_sectors)[:10]}"
        )
    expected_date_set = set(expected_dates)
    for code in sorted(expected_sectors):
        actual_dates = set(sector_date_labels[code])
        missing_dates = sorted(expected_date_set - actual_dates)
        unexpected_dates = sorted(actual_dates - expected_date_set)
        if missing_dates or unexpected_dates:
            raise ValueError(
                "missing decoded state dates for frozen HMM sector: "
                f"sector={code} missing={missing_dates[:5]} unexpected={unexpected_dates[:5]}"
            )

    daily_coefficients: dict[str, dict[str, float]] = {}
    for trade_date in expected_dates:
        day: dict[str, float] = {}
        for code in sorted(expected_sectors):
            label = sector_date_labels[code][trade_date]
            if label not in preset_coeffs:
                raise ValueError(
                    "decoded HMM state has no preset coefficient: "
                    f"sector={code} trade_date={trade_date} state={label!r}"
                )
            coefficient = float(preset_coeffs[label])
            if not math.isfinite(coefficient):
                raise ValueError(
                    "non-finite HMM preset coefficient: "
                    f"sector={code} trade_date={trade_date} state={label!r}"
                )
            day[code] = coefficient
        daily_coefficients[trade_date] = day
    return daily_coefficients


def _resolve_frozen_files(bundle: dict[str, Any]) -> tuple[Path, dict[str, Path], dict[str, str]]:
    if bundle.get("schema_version") != FROZEN_INPUT_SCHEMA:
        raise ValueError(
            f"invalid frozen HMM input schema: {bundle.get('schema_version')!r} != {FROZEN_INPUT_SCHEMA!r}"
        )
    root_value = str(bundle.get("dataset_root") or "").strip()
    if not root_value:
        raise ValueError("frozen HMM input requires dataset_root")
    root = Path(root_value).expanduser().resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"frozen HMM dataset_root is not a directory: {root}")

    raw_files = bundle.get("files")
    if not isinstance(raw_files, dict):
        raise ValueError("frozen HMM input requires files object")
    paths: dict[str, Path] = {}
    hashes: dict[str, str] = {}
    for key in FROZEN_FILE_KEYS:
        spec = raw_files.get(key)
        if not isinstance(spec, dict):
            raise ValueError(f"frozen HMM input is missing file spec: {key}")
        relative_path = str(spec.get("relative_path") or "").strip()
        expected_sha256 = str(spec.get("sha256") or "").strip().lower()
        if not relative_path or len(expected_sha256) != 64:
            raise ValueError(f"frozen HMM file spec is incomplete: {key}")
        candidate = root.joinpath(relative_path).resolve(strict=True)
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"frozen HMM file escapes dataset_root: {key}") from exc
        if not candidate.is_file() or candidate.is_symlink():
            raise ValueError(f"frozen HMM input must be a regular non-symlink file: {key}")
        actual_sha256 = _sha256_file(candidate)
        if actual_sha256 != expected_sha256:
            raise ValueError(
                f"frozen HMM input sha256 mismatch: {key} expected={expected_sha256} actual={actual_sha256}"
            )
        paths[key] = candidate
        hashes[key] = actual_sha256
    return root, paths, hashes


def _select_factor_hdf_window(
    path: Path,
    *,
    start: date,
    end: date,
    columns: list[str],
) -> Any:
    import pandas as pd

    with pd.HDFStore(path, "r") as store:
        if "/data" not in store.keys():
            raise ValueError(f"frozen HMM H5 has no /data key: {path}")
        storer = store.get_storer("data")
        if not bool(getattr(storer, "is_table", False)):
            raise ValueError(f"frozen HMM factor H5 must use queryable table format: {path}")
        frame = store.select(
            "data",
            where=[
                f'datetime >= Timestamp("{start.isoformat()}")',
                f'datetime <= Timestamp("{end.isoformat()}")',
            ],
            columns=columns,
        ).reset_index()
    if frame.empty:
        raise ValueError(f"frozen HMM H5 has no rows in {start.isoformat()}..{end.isoformat()}: {path}")
    required = {"datetime", "instrument", *columns}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"frozen HMM H5 missing columns {missing}: {path}")
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="raise").dt.date
    frame["instrument"] = frame["instrument"].astype(str).str.strip().str.upper()
    if (frame["instrument"] == "").any():
        raise ValueError(f"frozen HMM H5 contains blank instrument: {path}")
    return frame


def _load_release_l2_code_map(path: Path) -> tuple[dict[int, str], dict[str, str], str]:
    """Load one hash-pinned release-wide ID-to-canonical-code bijection."""

    from backend.services.dataset_release.shared_sector_context import (
        load_release_sw_l2_code_map,
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != FROZEN_CODE_MAP_SCHEMA:
        raise ValueError("invalid frozen SW L2 code-map schema")
    if not isinstance(payload.get("mapping_authority"), dict):
        raise ValueError("frozen SW L2 code map requires mapping_authority")
    try:
        code_map = load_release_sw_l2_code_map(path, require_member_backed=False)
    except ValueError as exc:
        if "code-map digest differs" in str(exc):
            raise ValueError("frozen SW L2 code-map digest mismatch") from exc
        raise
    return (
        dict(code_map.id_to_code),
        dict(code_map.mapping_authority),
        code_map.code_map_digest,
    )


def _load_release_membership_maps(
    path: Path,
    *,
    sector_code_by_id: dict[int, str],
    trade_dates: list[date],
) -> dict[str, dict[str, str]]:
    """Load the shared PIT membership authority without inferring it from factor rows."""

    import pandas as pd

    required_columns = {"instrument", "start_date", "end_date", "l2_code_id"}
    frame = pd.read_parquet(path)
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise ValueError(f"frozen sector membership spans missing columns: {missing_columns}")
    frame = frame.loc[:, sorted(required_columns)].copy()
    if frame.empty:
        raise ValueError("frozen sector membership spans are empty")
    frame["instrument"] = frame["instrument"].astype(str).str.strip().str.upper()
    if (frame["instrument"] == "").any():
        raise ValueError("frozen sector membership spans contain blank instruments")
    frame["start_date"] = pd.to_datetime(frame["start_date"], errors="raise").dt.date
    frame["end_date"] = pd.to_datetime(frame["end_date"], errors="raise").dt.date
    if (frame["start_date"] > frame["end_date"]).any():
        raise ValueError("frozen sector membership span starts after its end date")
    numeric_ids = pd.to_numeric(frame["l2_code_id"], errors="raise")
    if (~np.isfinite(numeric_ids) | (numeric_ids != np.floor(numeric_ids))).any():
        raise ValueError("frozen sector membership spans contain non-integral l2_code_id values")
    frame["l2_code_id"] = numeric_ids.astype("int64")
    unknown_ids = sorted(set(frame["l2_code_id"]) - set(sector_code_by_id))
    if unknown_ids:
        raise ValueError(f"frozen sector membership spans contain unmapped l2_code_id values: {unknown_ids[:10]}")
    duplicate_columns = ["instrument", "start_date", "end_date", "l2_code_id"]
    if frame.duplicated(duplicate_columns, keep=False).any():
        raise ValueError("frozen sector membership spans contain duplicate rows")
    for instrument, spans in frame.sort_values(["instrument", "start_date", "end_date"]).groupby(
        "instrument", sort=False
    ):
        prior_end: date | None = None
        for row in spans.itertuples(index=False):
            if prior_end is not None and row.start_date <= prior_end:
                raise ValueError(
                    "frozen sector membership spans overlap: "
                    f"instrument={instrument} start_date={row.start_date.isoformat()}"
                )
            prior_end = row.end_date

    membership_rows = [
        {
            "ts_code": row.instrument,
            "l2_code": sector_code_by_id[int(row.l2_code_id)],
            "in_date": row.start_date,
            "out_date": row.end_date,
        }
        for row in frame.itertuples(index=False)
    ]
    return build_stock_sector_maps_by_date(membership_rows, trade_dates)


def build_stock_sector_membership_spans(
    stock_sector_maps_by_date: dict[str, dict[str, str]],
) -> dict[str, list[dict[str, str]]]:
    """Compress exhaustive daily PIT membership into deterministic spans."""

    if not stock_sector_maps_by_date:
        raise ValueError("empty frozen stock-sector membership maps")
    active: dict[str, tuple[str, str, str]] = {}
    completed: dict[str, list[dict[str, str]]] = {}
    previous_date: str | None = None
    for trade_date in sorted(stock_sector_maps_by_date):
        day_map = stock_sector_maps_by_date[trade_date]
        if not day_map:
            raise ValueError(f"empty frozen stock-sector membership map: {trade_date}")
        current_symbols = set(day_map)
        for symbol in sorted(set(active) - current_symbols):
            start_date, _last_date, sector_code = active.pop(symbol)
            completed.setdefault(symbol, []).append(
                {"start_date": start_date, "end_date": str(previous_date), "sector_code": sector_code}
            )
        for symbol in sorted(current_symbols):
            sector_code = str(day_map[symbol]).strip()
            if not sector_code:
                raise ValueError(f"blank frozen sector code: trade_date={trade_date} instrument={symbol}")
            prior = active.get(symbol)
            if prior is None:
                active[symbol] = (trade_date, trade_date, sector_code)
            elif prior[2] == sector_code:
                active[symbol] = (prior[0], trade_date, sector_code)
            else:
                completed.setdefault(symbol, []).append(
                    {
                        "start_date": prior[0],
                        "end_date": prior[1],
                        "sector_code": prior[2],
                    }
                )
                active[symbol] = (trade_date, trade_date, sector_code)
        previous_date = trade_date
    for symbol in sorted(active):
        start_date, last_date, sector_code = active[symbol]
        completed.setdefault(symbol, []).append(
            {"start_date": start_date, "end_date": last_date, "sector_code": sector_code}
        )
    return {symbol: completed[symbol] for symbol in sorted(completed)}


def resolve_coefficient_membership_maps(
    all_maps_by_date: dict[str, dict[str, str]],
    daily_coefficients: dict[str, dict[str, float]],
    *,
    output_trade_date: str | None,
    as_of_trade_date: str | None,
    backtest_end: str,
) -> dict[str, dict[str, str]]:
    """Bind range coefficients to same-day PIT maps or daily output to its as-of map."""

    coefficient_dates = sorted(daily_coefficients)
    if output_trade_date:
        source_date = str(as_of_trade_date or backtest_end)
        source_map = all_maps_by_date.get(source_date)
        if not isinstance(source_map, dict) or not source_map:
            raise ValueError(f"frozen stock-sector membership missing daily as-of date: {source_date}")
        selected = {str(output_trade_date): source_map}
    else:
        missing_dates = sorted(set(coefficient_dates) - set(all_maps_by_date))
        if missing_dates:
            raise ValueError(f"frozen stock-sector membership missing coefficient dates: {missing_dates[:5]}")
        selected = {value: all_maps_by_date[value] for value in coefficient_dates}

    for trade_date_value, day_map in selected.items():
        missing_codes = sorted(set(day_map.values()) - set(daily_coefficients[trade_date_value]))
        if missing_codes:
            raise ValueError(
                "frozen membership references sectors without daily coefficients: "
                f"trade_date={trade_date_value} codes={missing_codes[:10]}"
            )
    return selected


def load_frozen_coefficient_inputs(
    bundle: dict[str, Any],
    *,
    history_start: date,
    test_start: date,
    backtest_end: date,
) -> dict[str, Any]:
    """Load all QE coefficient inputs from one hash-pinned frozen release."""

    import pandas as pd

    root, paths, file_hashes = _resolve_frozen_files(bundle)
    (
        sector_code_by_id,
        sector_code_map_authority,
        actual_code_map_digest,
    ) = _load_release_l2_code_map(
        paths["sector_code_map_json"],
    )

    sector_columns = [
        "l2_code_id",
        "sw2_pct_change",
        "sw2_vol",
        "sw2_amount",
        "sw2_mf_net_amt",
        "sw2_mf_buy_elg_amt",
        "sw2_mf_sell_elg_amt",
    ]
    sector_frame = _select_factor_hdf_window(
        paths["sector_data_h5"],
        start=history_start,
        end=backtest_end,
        columns=sector_columns,
    )
    numeric_ids = pd.to_numeric(sector_frame["l2_code_id"], errors="raise")
    if (~np.isfinite(numeric_ids) | (numeric_ids != np.floor(numeric_ids))).any():
        raise ValueError("frozen sector_data contains non-integral l2_code_id values")
    sector_frame["l2_code_id"] = numeric_ids.astype("int64")
    invalid_ids = sorted(set(sector_frame["l2_code_id"]) - set(sector_code_by_id))
    if invalid_ids:
        raise ValueError(f"frozen sector_data contains unmapped l2_code_id values: {invalid_ids[:10]}")
    sector_frame["sector_code"] = sector_frame["l2_code_id"].map(sector_code_by_id)

    metric_columns = sector_columns[1:]
    conflicts = sector_frame.groupby(["datetime", "sector_code"], sort=False)[metric_columns].nunique(dropna=True).gt(1)
    if bool(conflicts.to_numpy().any()):
        first = conflicts.stack().loc[lambda values: values].index[0]
        raise ValueError(
            "frozen sector_data has conflicting per-sector daily values: "
            f"trade_date={first[0]} sector={first[1]} field={first[2]}"
        )

    representatives = sector_frame.sort_values(["datetime", "sector_code", "instrument"]).drop_duplicates(
        ["datetime", "sector_code"], keep="first"
    )
    sector_data: dict[str, dict[date, dict[str, Any]]] = {}
    sector_rows: dict[str, list[dict[str, Any]]] = {}
    for row in representatives.itertuples(index=False):
        record = {
            "trade_date": row.datetime,
            "l2_name": row.sector_code,
            "sw2_pct_change": row.sw2_pct_change,
            "sw2_vol": row.sw2_vol,
            "sw2_amount": row.sw2_amount,
            "sw2_mf_net_amt": row.sw2_mf_net_amt,
            "sw2_mf_buy_elg_amt": row.sw2_mf_buy_elg_amt,
            "sw2_mf_sell_elg_amt": row.sw2_mf_sell_elg_amt,
        }
        sector_data.setdefault(row.sector_code, {})[row.datetime] = record
        sector_rows.setdefault(row.sector_code, []).append(
            {
                "trade_date": row.datetime,
                "l2_name": row.sector_code,
                "pct_change": _require_finite_float(
                    row.sw2_pct_change,
                    field="sw2_pct_change",
                    trade_date=row.datetime,
                    sector_code=row.sector_code,
                ),
                "vol": _require_finite_float(
                    row.sw2_vol,
                    field="sw2_vol",
                    trade_date=row.datetime,
                    sector_code=row.sector_code,
                ),
                "amount": _require_finite_float(
                    row.sw2_amount,
                    field="sw2_amount",
                    trade_date=row.datetime,
                    sector_code=row.sector_code,
                ),
                "mf_net_amt": _require_finite_float(
                    row.sw2_mf_net_amt,
                    field="sw2_mf_net_amt",
                    trade_date=row.datetime,
                    sector_code=row.sector_code,
                ),
                "mf_buy_elg_amt": _require_finite_float(
                    row.sw2_mf_buy_elg_amt,
                    field="sw2_mf_buy_elg_amt",
                    trade_date=row.datetime,
                    sector_code=row.sector_code,
                ),
                "mf_sell_elg_amt": _require_finite_float(
                    row.sw2_mf_sell_elg_amt,
                    field="sw2_mf_sell_elg_amt",
                    trade_date=row.datetime,
                    sector_code=row.sector_code,
                ),
            }
        )

    if bundle.get("market_volume_definition") != FROZEN_MARKET_VOLUME_DEFINITION:
        raise ValueError("invalid frozen HMM market-volume definition")
    market_context = pd.read_parquet(
        paths["market_context_parquet"],
        columns=["trade_date", "sw_daily_total_vol"],
    )
    market_context["trade_date"] = pd.to_datetime(market_context["trade_date"], errors="raise").dt.date
    market_context["sw_daily_total_vol"] = pd.to_numeric(market_context["sw_daily_total_vol"], errors="raise")
    market_context = market_context.loc[
        (market_context["trade_date"] >= history_start) & (market_context["trade_date"] <= backtest_end)
    ].sort_values("trade_date")
    if market_context.empty or market_context["trade_date"].duplicated().any():
        raise ValueError("frozen HMM market context is empty or has duplicated dates")
    if (~np.isfinite(market_context["sw_daily_total_vol"]) | (market_context["sw_daily_total_vol"] <= 0)).any():
        raise ValueError("frozen HMM market context contains invalid market volume")
    market_vol = {row.trade_date: float(row.sw_daily_total_vol) for row in market_context.itertuples(index=False)}

    index_frame = pd.read_hdf(paths["index_daily_h5"], "data")
    required_index_columns = {"trade_date", "ts_code", "close"}
    if not required_index_columns.issubset(index_frame.columns):
        raise ValueError("frozen index_daily.h5 is missing trade_date/ts_code/close")
    index_frame["trade_date"] = pd.to_datetime(index_frame["trade_date"], errors="raise").dt.date
    index_frame["ts_code"] = index_frame["ts_code"].astype(str).str.strip().str.upper()
    csi300_frame = index_frame.loc[
        (index_frame["ts_code"] == "000300.SH")
        & (index_frame["trade_date"] >= history_start)
        & (index_frame["trade_date"] <= backtest_end),
        ["trade_date", "close"],
    ].sort_values("trade_date")
    if csi300_frame.empty or csi300_frame["trade_date"].duplicated().any():
        raise ValueError("frozen index_daily.h5 has empty or duplicated 000300.SH rows")
    csi300_frame["close"] = pd.to_numeric(csi300_frame["close"], errors="raise")
    csi300_frame["pct_chg"] = csi300_frame["close"].pct_change() * 100.0
    csi300 = {
        row.trade_date: float(row.pct_chg) for row in csi300_frame.itertuples(index=False) if pd.notna(row.pct_chg)
    }

    expected_trade_dates = sorted(
        value for value in set(csi300_frame["trade_date"]) if test_start <= value <= backtest_end
    )
    expected_dates = [value.isoformat() for value in expected_trade_dates]
    maps_by_date = _load_release_membership_maps(
        paths["sector_membership_spans_parquet"],
        sector_code_by_id=sector_code_by_id,
        trade_dates=expected_trade_dates,
    )
    missing_membership_dates = sorted(set(expected_dates) - set(maps_by_date))
    if missing_membership_dates:
        raise ValueError(f"frozen sector membership has no rows for trading dates: {missing_membership_dates[:5]}")
    missing_market_dates = sorted(set(expected_dates) - {value.isoformat() for value in market_vol})
    if missing_market_dates:
        raise ValueError(f"frozen HMM market context is missing trading dates: {missing_market_dates[:5]}")
    active_sector_codes = sorted({code for day_map in maps_by_date.values() for code in day_map.values()})
    missing_sector_data = sorted(set(active_sector_codes) - set(sector_data))
    if missing_sector_data:
        raise ValueError(
            "frozen membership references sectors without sector_data: "
            f"{missing_sector_data[:10]}"
        )

    return {
        "dataset_root": str(root),
        "dataset_identity": bundle.get("dataset_identity"),
        "file_sha256": file_hashes,
        "sector_data": sector_data,
        "sector_rows": sector_rows,
        "sector_dates": sorted(set(sector_frame["datetime"])),
        "csi300": csi300,
        "index_dates": sorted(csi300),
        "market_vol": market_vol,
        "market_volume_dates": sorted(market_vol),
        "stock_sector_membership_spans": build_stock_sector_membership_spans(maps_by_date),
        "stock_sector_maps_by_date": maps_by_date,
        "sector_code_by_id": dict(sorted(sector_code_by_id.items())),
        "ordered_sector_codes": sorted(set(sector_code_by_id.values())),
        "active_sector_codes": active_sector_codes,
        "sector_code_map_authority": sector_code_map_authority,
        "sector_code_map_digest": actual_code_map_digest,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", default=None)
    args, _ = parser.parse_known_args()

    params = parse_stdin()
    model_path = params["model_path"]
    test_start = params["test_start"]
    backtest_end = params["backtest_end"]
    preset_coeffs = params.get("preset_coeffs") or {
        "trending": 1.05,
        "neutral": 1.00,
        "fading": 0.96,
    }
    preset_key = params.get("preset_key")
    output_trade_date = params.get("output_trade_date")
    as_of_trade_date = params.get("as_of_trade_date")
    config_json = params.get("config_json") if isinstance(params.get("config_json"), dict) else {}

    frozen_input_bundle = params.get("frozen_input_bundle")
    if not isinstance(frozen_input_bundle, dict):
        print(
            "ERROR: frozen_input_bundle is required; market-table fallback is forbidden",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"HMM precompute: model={model_path}", file=sys.stderr)
    print(f"  date range: {test_start} ~ {backtest_end}", file=sys.stderr)
    print(f"  preset: {preset_key}, coeffs: {preset_coeffs}", file=sys.stderr)
    print("  data source: hash-pinned frozen QE release", file=sys.stderr)

    expected_model_sha256 = str(params.get("model_sha256") or "").strip().lower()
    if len(expected_model_sha256) != 64:
        print("ERROR: frozen HMM precompute requires model_sha256", file=sys.stderr)
        sys.exit(1)
    actual_model_sha256 = _sha256_file(Path(model_path))
    if actual_model_sha256 != expected_model_sha256:
        print(
            f"ERROR: HMM model sha256 mismatch: expected={expected_model_sha256} actual={actual_model_sha256}",
            file=sys.stderr,
        )
        sys.exit(1)

    with open(model_path, "r", encoding="utf-8") as f:
        models = json.load(f)
    if not models:
        print(f"ERROR: empty HMM model file: {model_path}", file=sys.stderr)
        sys.exit(1)
    print(f"  loaded {len(models)} sector models", file=sys.stderr)

    first = next(iter(models.values()))
    n_features = len(first.get("means", [[]])[0]) if first.get("means") else 4
    rolling_window = int(first.get("rolling_window", 5))
    has_horizon_v2_features = bool(first.get("feature_names") and first.get("preprocess"))
    uses_dynamic_coefficients = bool(first.get("state_validation_stats") and not first.get("state_labels"))
    dynamic_method = str(config_json.get("method") or params.get("dynamic_method") or "").strip()
    horizon_weights = {int(k): float(v) for k, v in (config_json.get("horizon_weights") or {}).items()}
    if uses_dynamic_coefficients:
        if dynamic_method not in {"er", "er_winsor", "er_median", "pup", "pup_z", "pup_rank", "additive_pup"}:
            print(
                "ERROR: dynamic HMM model requires explicit supported config_json.method",
                file=sys.stderr,
            )
            sys.exit(1)
        if not horizon_weights:
            print(
                "ERROR: dynamic HMM model requires config_json.horizon_weights",
                file=sys.stderr,
            )
            sys.exit(1)
        missing_dynamic_keys = [
            key for key in ("coefficient_lambda", "coefficient_bounds", "confidence_scale") if key not in config_json
        ]
        if missing_dynamic_keys:
            print(
                "ERROR: dynamic HMM model requires config_json keys: " + ",".join(missing_dynamic_keys),
                file=sys.stderr,
            )
            sys.exit(1)
    coefficient_lambda = float(config_json.get("coefficient_lambda", 0.0))
    coefficient_bounds = config_json.get("coefficient_bounds") or [0.0, float("inf")]
    if uses_dynamic_coefficients and (not isinstance(coefficient_bounds, list) or len(coefficient_bounds) != 2):
        print("ERROR: dynamic HMM model requires two coefficient_bounds values", file=sys.stderr)
        sys.exit(1)
    coeff_min = float(coefficient_bounds[0])
    coeff_max = float(coefficient_bounds[1])
    confidence_scale = float(config_json.get("confidence_scale", 1.0))
    neutral_band = float(config_json.get("neutral_band", 0.0))
    confidence_floor = float(config_json.get("confidence_floor", 0.0))
    has_zscore = "zscore_mean" in first
    zscore_mean = np.asarray(first["zscore_mean"], dtype=np.float64) if has_zscore else None
    zscore_std = np.asarray(first["zscore_std"], dtype=np.float64) if has_zscore else None
    print(
        f"  features={n_features}, rolling_window={rolling_window}, "
        f"zscore={has_zscore}, horizon_v2_schema={has_horizon_v2_features}, "
        f"dynamic_coefficients={uses_dynamic_coefficients}",
        file=sys.stderr,
    )

    hmm_objs: dict[str, tuple[Any, dict[str, str] | None, dict[str, Any]]] = {}
    restore_failures: list[str] = []
    for code, info in models.items():
        try:
            labels = info.get("state_labels")
            if uses_dynamic_coefficients:
                stats = info.get("state_validation_stats")
                if not isinstance(stats, dict) or not stats:
                    raise KeyError("state_validation_stats")
                labels = None
            elif not isinstance(labels, dict) or not labels:
                raise KeyError("state_labels")
            hmm_objs[code] = (restore_hmm(info), labels, info)
        except Exception as exc:
            restore_failures.append(f"{code}: {type(exc).__name__}: {exc}")
    if restore_failures:
        print(
            "ERROR: frozen HMM model restoration incomplete: "
            f"count={len(restore_failures)} first={restore_failures[:3]}",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"  restored {len(hmm_objs)}/{len(models)} HMM models", file=sys.stderr)

    start_d = date.fromisoformat(test_start)
    end_d = date.fromisoformat(backtest_end)
    history_start = start_d - timedelta(days=int(3.0 * 365 + 30))

    try:
        frozen_loaded = load_frozen_coefficient_inputs(
            frozen_input_bundle,
            history_start=history_start,
            test_start=start_d,
            backtest_end=end_d,
        )
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"ERROR: frozen HMM input rejected: {exc}", file=sys.stderr)
        sys.exit(1)
    sector_data = frozen_loaded["sector_data"]
    sector_rows = frozen_loaded["sector_rows"]
    csi300 = frozen_loaded["csi300"]
    market_vol = frozen_loaded["market_vol"]
    print(
        f"  loaded frozen sectors={len(sector_data)}, CSI300={len(csi300)}, "
        f"market_vol={len(market_vol)}, "
        f"membership_spans={len(frozen_loaded['stock_sector_membership_spans'])}",
        file=sys.stderr,
    )
    mapped_sector_codes = {
        sector_code
        for day_map in frozen_loaded["stock_sector_maps_by_date"].values()
        for sector_code in day_map.values()
    }
    missing_models = sorted(mapped_sector_codes - set(hmm_objs))
    if missing_models:
        print(
            f"ERROR: frozen membership references sectors without restored HMM models: {missing_models[:10]}",
            file=sys.stderr,
        )
        sys.exit(1)
    frozen_sector_codes = set(frozen_loaded["ordered_sector_codes"])
    active_sector_codes = set(frozen_loaded["active_sector_codes"])
    restored_sector_codes = set(hmm_objs)
    if restored_sector_codes != frozen_sector_codes:
        print(
            "ERROR: restored HMM sector set differs from frozen code map: "
            f"missing_models={sorted(frozen_sector_codes - restored_sector_codes)[:10]} "
            f"unexpected_models={sorted(restored_sector_codes - frozen_sector_codes)[:10]}",
            file=sys.stderr,
        )
        sys.exit(1)
    inactive_sector_codes = frozen_sector_codes - active_sector_codes
    expected_coefficient_dates = [
        value.isoformat()
        for value in frozen_loaded["index_dates"]
        if start_d <= value <= end_d
    ]
    if not expected_coefficient_dates:
        print("ERROR: frozen index calendar has no coefficient dates", file=sys.stderr)
        sys.exit(1)

    print("  decoding sector states...", file=sys.stderr)
    sector_date_labels: dict[str, dict[str, str]] = {}
    dynamic_signal_by_date: dict[str, dict[str, float]] = {}
    dynamic_confidence_by_date: dict[str, dict[str, float]] = {}
    for idx, code in enumerate(sorted(active_sector_codes)):
        hmm, labels, info = hmm_objs[code]
        if code not in sector_data:
            print(f"ERROR: frozen sector data missing HMM sector: {code}", file=sys.stderr)
            sys.exit(1)
        if has_horizon_v2_features and info.get("preprocess"):
            obs, dates_out = build_horizon_v2_observations(sector_rows[code], csi300, market_vol, info["preprocess"])
        else:
            sorted_dates = sorted(sector_data[code].keys())
            obs, dates_out = build_legacy_observations(
                sector_data[code], sorted_dates, csi300, market_vol, rolling_window, n_features
            )
            if len(obs) and zscore_mean is not None:
                obs = (obs - zscore_mean) / zscore_std

        if len(obs) < 20:
            print(
                f"ERROR: insufficient frozen observations for HMM sector {code}: {len(obs)}",
                file=sys.stderr,
            )
            sys.exit(1)
        expected_features = int(hmm.means_.shape[1])
        if obs.shape[1] != expected_features:
            print(
                f"ERROR: feature dimension mismatch {code}: obs={obs.shape[1]}, model={expected_features}",
                file=sys.stderr,
            )
            sys.exit(1)
        try:
            posteriors = forward_filter_posteriors(hmm, obs)
        except Exception as exc:
            print(f"ERROR: forward filter failed {code}: {exc}", file=sys.stderr)
            sys.exit(1)

        if uses_dynamic_coefficients:
            state_stats = info.get("state_validation_stats")
            if not isinstance(state_stats, dict) or not state_stats:
                print(f"  WARNING: missing state_validation_stats {code}", file=sys.stderr)
                continue
            for i, td in enumerate(dates_out):
                if start_d <= td <= end_d:
                    prob = posteriors[i]
                    try:
                        raw_signal = utility_from_state_stats(
                            prob,
                            state_stats,
                            dynamic_method,
                            horizon_weights,
                        )
                    except Exception as exc:
                        print(f"  WARNING: dynamic signal failed {code}: {exc}", file=sys.stderr)
                        continue
                    if not math.isfinite(raw_signal):
                        raw_signal = 0.0
                    d = td.isoformat()
                    dynamic_signal_by_date.setdefault(d, {})[code] = float(raw_signal)
                    dynamic_confidence_by_date.setdefault(d, {})[code] = confidence_from_posterior(
                        prob,
                        confidence_scale,
                    )
        else:
            assert labels is not None
            states = posteriors.argmax(axis=1)
            by_date = {}
            for i, td in enumerate(dates_out):
                if start_d <= td <= end_d:
                    state_key = str(int(states[i]))
                    if state_key not in labels:
                        print(
                            "ERROR: decoded HMM state is absent from state_labels: "
                            f"sector={code} trade_date={td.isoformat()} state={state_key}",
                            file=sys.stderr,
                        )
                        sys.exit(1)
                    by_date[td.isoformat()] = labels[state_key]
            if by_date:
                sector_date_labels[code] = by_date
        if (idx + 1) % 20 == 0:
            print(f"  processed {idx + 1}/{len(active_sector_codes)} active sectors", file=sys.stderr)

    daily_coefficients: dict[str, dict[str, float]] = {}
    if uses_dynamic_coefficients:
        dynamic_sector_count = len({code for by_sector in dynamic_signal_by_date.values() for code in by_sector})
        print(f"  decoded dynamic sectors={dynamic_sector_count}", file=sys.stderr)
        if not dynamic_signal_by_date:
            print("ERROR: no dynamic HMM signals decoded; refusing empty coefficient output", file=sys.stderr)
            sys.exit(1)
        for d in sorted(dynamic_signal_by_date):
            raw_map = dynamic_signal_by_date[d]
            if dynamic_method in {"er", "er_winsor", "er_median", "pup_z"}:
                normalized_map = robust_z_by_date(raw_map)
            elif dynamic_method == "pup_rank":
                normalized_map = rank_signal_by_date(raw_map)
            else:
                normalized_map = raw_map
            daily_coefficients[d] = {}
            for code, raw_signal in raw_map.items():
                confidence = dynamic_confidence_by_date.get(d, {}).get(code, 0.0)
                if dynamic_method in {"er", "er_winsor", "er_median", "pup_z", "pup_rank"}:
                    normalized_signal = normalized_map.get(code, 0.0)
                    if not math.isfinite(normalized_signal):
                        normalized_signal = 0.0
                    coeff = 1.0 + coefficient_lambda * confidence * normalized_signal
                elif dynamic_method == "pup":
                    normalized_signal = float(np.clip(raw_signal, -1.0, 1.0))
                    if not math.isfinite(normalized_signal):
                        normalized_signal = 0.0
                    coeff = 1.0 + coefficient_lambda * confidence * normalized_signal
                else:
                    normalized_signal = float(np.clip(raw_signal, -1.0, 1.0))
                    if not math.isfinite(normalized_signal):
                        normalized_signal = 0.0
                    coeff = 1.0
                if confidence < confidence_floor or abs(normalized_signal) < neutral_band:
                    coeff = 1.0
                coeff = float(np.clip(coeff, coeff_min, coeff_max))
                if not math.isfinite(coeff):
                    print(f"  WARNING: non-finite dynamic coefficient {code} {d}", file=sys.stderr)
                    continue
                daily_coefficients[d][code] = round(coeff, 8)
    else:
        print(f"  decoded sectors={len(sector_date_labels)}", file=sys.stderr)
        if not sector_date_labels:
            print("ERROR: no HMM sectors decoded; refusing empty coefficient output", file=sys.stderr)
            sys.exit(1)

        try:
            daily_coefficients = build_static_daily_coefficients(
                sector_date_labels,
                expected_sector_codes=frozen_loaded["active_sector_codes"],
                expected_dates=expected_coefficient_dates,
                preset_coeffs=preset_coeffs,
            )
        except ValueError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            sys.exit(1)
    if not daily_coefficients:
        print("ERROR: no daily HMM coefficients generated", file=sys.stderr)
        sys.exit(1)
    actual_dates = set(daily_coefficients)
    expected_dates = set(expected_coefficient_dates)
    if actual_dates != expected_dates:
        print(
            "ERROR: generated HMM coefficient dates differ from frozen calendar: "
            f"missing={sorted(expected_dates - actual_dates)[:5]} "
            f"unexpected={sorted(actual_dates - expected_dates)[:5]}",
            file=sys.stderr,
        )
        sys.exit(1)
    for trade_date in expected_coefficient_dates:
        actual_codes = set(daily_coefficients[trade_date])
        if actual_codes != active_sector_codes:
            print(
                "ERROR: generated HMM coefficient sectors differ from frozen active membership: "
                f"trade_date={trade_date} "
                f"missing={sorted(active_sector_codes - actual_codes)[:10]} "
                f"unexpected={sorted(actual_codes - active_sector_codes)[:10]}",
                file=sys.stderr,
            )
            sys.exit(1)
        if any(
            not math.isfinite(float(value))
            for value in daily_coefficients[trade_date].values()
        ):
            print(
                f"ERROR: generated HMM coefficient grid contains non-finite values: {trade_date}",
                file=sys.stderr,
            )
            sys.exit(1)

    if output_trade_date:
        source_trade_date = as_of_trade_date or backtest_end
        source_coefficients = daily_coefficients.get(source_trade_date)
        if not isinstance(source_coefficients, dict) or not source_coefficients:
            available = sorted(daily_coefficients.keys())[-5:]
            print(
                f"ERROR: coefficients missing for as_of_trade_date={source_trade_date}; available_tail={available}",
                file=sys.stderr,
            )
            sys.exit(1)
        daily_coefficients = {str(output_trade_date): source_coefficients}

    try:
        coefficient_dates = sorted(daily_coefficients)
        stock_sector_map_by_date = resolve_coefficient_membership_maps(
            frozen_loaded["stock_sector_maps_by_date"],
            daily_coefficients,
            output_trade_date=str(output_trade_date) if output_trade_date else None,
            as_of_trade_date=str(as_of_trade_date) if as_of_trade_date else None,
            backtest_end=backtest_end,
        )
        stock_sector_membership_spans = build_stock_sector_membership_spans(stock_sector_map_by_date)
        input_data_max_dates_by_date = build_input_data_max_dates_by_date(
            trade_dates=[date.fromisoformat(value) for value in coefficient_dates],
            sector_dates=frozen_loaded["sector_dates"],
            index_dates=frozen_loaded["index_dates"],
            market_volume_dates=frozen_loaded["market_volume_dates"],
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    result: dict[str, Any] = {
        "model_path": model_path,
        "preset_key": preset_key,
        "preset_coeffs": preset_coeffs,
        "test_start": test_start,
        "backtest_end": backtest_end,
        "sector_count": (
            len({code for by_sector in dynamic_signal_by_date.values() for code in by_sector})
            if uses_dynamic_coefficients
            else len(sector_date_labels)
        ),
        "dynamic_coefficients": uses_dynamic_coefficients,
        "daily_coefficients": daily_coefficients,
        "stock_sector_membership_spans": stock_sector_membership_spans,
        "input_data_max_dates_by_date": input_data_max_dates_by_date,
    }
    result.update(
        {
            "data_source": "frozen_qe_release",
            "dataset_root": frozen_loaded["dataset_root"],
            "dataset_identity": frozen_loaded["dataset_identity"],
            "input_file_sha256": frozen_loaded["file_sha256"],
            "model_sha256": expected_model_sha256,
            "sector_code_map_schema": FROZEN_CODE_MAP_SCHEMA,
            "sector_code_map_authority": frozen_loaded["sector_code_map_authority"],
            "sector_code_map_digest": frozen_loaded["sector_code_map_digest"],
            "catalog_sector_count": len(frozen_sector_codes),
            "active_sector_count": len(active_sector_codes),
            "inactive_catalog_sector_codes": sorted(inactive_sector_codes),
            "coefficient_sector_scope": "membership_active_union",
        }
    )
    if output_trade_date:
        result["stock_sector_map"] = stock_sector_map_by_date[str(output_trade_date)]
        result.update(
            {
                "generation_mode": params.get("generation_mode") or "daily_asof_prediction_v1",
                "as_of_trade_date": as_of_trade_date or backtest_end,
                "effective_trade_date": str(output_trade_date),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "snapshot_id": params.get("snapshot_id"),
                "config_id": params.get("config_id"),
                "input_data_max_dates": params.get("input_data_max_dates"),
            }
        )

    result_json = json.dumps(result, ensure_ascii=False)
    print(result_json)
    if args.output_path:
        os.makedirs(os.path.dirname(args.output_path) or ".", exist_ok=True)
        with open(args.output_path, "w", encoding="utf-8") as f:
            f.write(result_json)
        print(f"  wrote result: {args.output_path}", file=sys.stderr)
    print("HMM precompute completed", file=sys.stderr)


if __name__ == "__main__":
    main()
