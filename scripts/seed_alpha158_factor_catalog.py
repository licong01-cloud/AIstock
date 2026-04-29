#!/usr/bin/env python3
"""Seed the 20 QE Alpha158 baseline factors as first-class catalog factors.

This script intentionally does not compute factor values, metrics, ratings, or
correlations.  It registers the factors with executable QE code so the existing
UI workflows can compute values, official metrics, classifications, ratings,
and correlations later.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent


AISTOCK_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(AISTOCK_ROOT))

ALPHA158_SOURCE = "alpha158"
CATALOG_VERSION = "alpha158_qe20_v1"
CATALOG_SOURCE = "qlib_alpha158_qe20"


ALPHA158_FACTORS = [
    {
        "name": "RESI5",
        "expression": "Resi($close, 5)/$close",
        "description": "Alpha158 residual price deviation over 5 days divided by close.",
        "category": "TECH",
    },
    {
        "name": "WVMA5",
        "expression": "Std(Abs($close/Ref($close, 1)-1)*$volume, 5)/(Mean(Abs($close/Ref($close, 1)-1)*$volume, 5)+1e-12)",
        "description": "Alpha158 volume-weighted volatility over 5 days.",
        "category": "VOL",
    },
    {
        "name": "RSQR5",
        "expression": "Rsquare($close, 5)",
        "description": "Alpha158 linear trend R-squared of close over 5 days.",
        "category": "TECH",
    },
    {
        "name": "KLEN",
        "expression": "($high-$low)/$open",
        "description": "Alpha158 candlestick total range normalized by open.",
        "category": "VOL",
    },
    {
        "name": "RSQR10",
        "expression": "Rsquare($close, 10)",
        "description": "Alpha158 linear trend R-squared of close over 10 days.",
        "category": "TECH",
    },
    {
        "name": "CORR5",
        "expression": "Corr($close, Log($volume+1), 5)",
        "description": "Alpha158 rolling correlation between close and log volume over 5 days.",
        "category": "PV",
    },
    {
        "name": "CORD5",
        "expression": "Corr($close/Ref($close,1), Log($volume/Ref($volume, 1)+1), 5)",
        "description": "Alpha158 rolling correlation between close ratio and volume ratio over 5 days.",
        "category": "PV",
    },
    {
        "name": "CORR10",
        "expression": "Corr($close, Log($volume+1), 10)",
        "description": "Alpha158 rolling correlation between close and log volume over 10 days.",
        "category": "PV",
    },
    {
        "name": "ROC60",
        "expression": "Ref($close, 60)/$close",
        "description": "Alpha158 60-day reverse close ratio.",
        "category": "MOM",
    },
    {
        "name": "RESI10",
        "expression": "Resi($close, 10)/$close",
        "description": "Alpha158 residual price deviation over 10 days divided by close.",
        "category": "TECH",
    },
    {
        "name": "VSTD5",
        "expression": "Std($volume, 5)/($volume+1e-12)",
        "description": "Alpha158 5-day volume standard deviation normalized by current volume.",
        "category": "VOL",
    },
    {
        "name": "RSQR60",
        "expression": "Rsquare($close, 60)",
        "description": "Alpha158 linear trend R-squared of close over 60 days.",
        "category": "TECH",
    },
    {
        "name": "CORR60",
        "expression": "Corr($close, Log($volume+1), 60)",
        "description": "Alpha158 rolling correlation between close and log volume over 60 days.",
        "category": "PV",
    },
    {
        "name": "WVMA60",
        "expression": "Std(Abs($close/Ref($close, 1)-1)*$volume, 60)/(Mean(Abs($close/Ref($close, 1)-1)*$volume, 60)+1e-12)",
        "description": "Alpha158 volume-weighted volatility over 60 days.",
        "category": "VOL",
    },
    {
        "name": "STD5",
        "expression": "Std($close, 5)/$close",
        "description": "Alpha158 5-day close standard deviation normalized by close.",
        "category": "VOL",
    },
    {
        "name": "RSQR20",
        "expression": "Rsquare($close, 20)",
        "description": "Alpha158 linear trend R-squared of close over 20 days.",
        "category": "TECH",
    },
    {
        "name": "CORD60",
        "expression": "Corr($close/Ref($close,1), Log($volume/Ref($volume, 1)+1), 60)",
        "description": "Alpha158 rolling correlation between close ratio and volume ratio over 60 days.",
        "category": "PV",
    },
    {
        "name": "CORD10",
        "expression": "Corr($close/Ref($close,1), Log($volume/Ref($volume, 1)+1), 10)",
        "description": "Alpha158 rolling correlation between close ratio and volume ratio over 10 days.",
        "category": "PV",
    },
    {
        "name": "CORR20",
        "expression": "Corr($close, Log($volume+1), 20)",
        "description": "Alpha158 rolling correlation between close and log volume over 20 days.",
        "category": "PV",
    },
    {
        "name": "KLOW",
        "expression": "(Less($open, $close)-$low)/$open",
        "description": "Alpha158 lower candlestick shadow normalized by open.",
        "category": "TECH",
    },
]


QE_CODE_TEMPLATE = r'''
import numpy as np
import pandas as pd

FACTOR_NAME = "__FACTOR_NAME__"


def _safe_divide(a, b):
    return a / b.replace(0, np.nan)


def _load_ohlcv(instruments, start_date, end_date):
    df = _REALTIME_LOADER.load(
        instruments=instruments,
        start_date=start_date,
        end_date=end_date,
        fields=["open", "close", "high", "low", "volume"],
        adjust="qfq",
    )
    return df.sort_index()


def _wide(df, column):
    values = df[column].unstack("instrument").sort_index()
    values.index.name = "datetime"
    values.columns.name = "instrument"
    return values.astype("float64")


def _to_result(values, factor_name):
    series = values.replace([np.inf, -np.inf], np.nan).stack()
    series.index.names = ["datetime", "instrument"]
    result = series.to_frame(factor_name)
    result = result.dropna()
    result = result[np.isfinite(result[factor_name])]
    return result


def _rolling_residual(values, window):
    def _resi(arr):
        if len(arr) < window or not np.isfinite(arr).all():
            return np.nan
        x = np.arange(len(arr), dtype="float64")
        x_centered = x - x.mean()
        denom = np.dot(x_centered, x_centered)
        if denom <= 1e-12:
            return np.nan
        y = arr.astype("float64")
        slope = np.dot(x_centered, y - y.mean()) / denom
        intercept = y.mean() - slope * x.mean()
        return y[-1] - (slope * x[-1] + intercept)

    return values.rolling(window, min_periods=window).apply(_resi, raw=True)


def _rolling_rsquare(values, window):
    def _r2(arr):
        if len(arr) < window or not np.isfinite(arr).all():
            return np.nan
        x = np.arange(len(arr), dtype="float64")
        x_centered = x - x.mean()
        denom = np.dot(x_centered, x_centered)
        if denom <= 1e-12:
            return np.nan
        y = arr.astype("float64")
        y_centered = y - y.mean()
        ss_tot = np.dot(y_centered, y_centered)
        if ss_tot <= 1e-12:
            return 0.0
        slope = np.dot(x_centered, y_centered) / denom
        intercept = y.mean() - slope * x.mean()
        pred = slope * x + intercept
        ss_res = np.sum((y - pred) ** 2)
        return 1.0 - ss_res / (ss_tot + 1e-12)

    return values.rolling(window, min_periods=window).apply(_r2, raw=True)


def _rolling_corr(left, right, window):
    left = left.replace([np.inf, -np.inf], np.nan)
    right = right.replace([np.inf, -np.inf], np.nan)
    return left.rolling(window, min_periods=window).corr(right)


def _compute_alpha158(df):
    open_ = _wide(df, "open")
    close = _wide(df, "close")
    high = _wide(df, "high")
    low = _wide(df, "low")
    volume = _wide(df, "volume")

    if FACTOR_NAME == "RESI5":
        return _safe_divide(_rolling_residual(close, 5), close)
    if FACTOR_NAME == "WVMA5":
        weighted_abs_ret = (close / close.shift(1) - 1).abs() * volume
        return _safe_divide(
            weighted_abs_ret.rolling(5, min_periods=5).std(),
            weighted_abs_ret.rolling(5, min_periods=5).mean() + 1e-12,
        )
    if FACTOR_NAME == "RSQR5":
        return _rolling_rsquare(close, 5)
    if FACTOR_NAME == "KLEN":
        return _safe_divide(high - low, open_)
    if FACTOR_NAME == "RSQR10":
        return _rolling_rsquare(close, 10)
    if FACTOR_NAME == "CORR5":
        return _rolling_corr(close, np.log(volume + 1), 5)
    if FACTOR_NAME == "CORD5":
        close_ratio = close / close.shift(1)
        volume_ratio_log = np.log(volume / volume.shift(1) + 1)
        return _rolling_corr(close_ratio, volume_ratio_log, 5)
    if FACTOR_NAME == "CORR10":
        return _rolling_corr(close, np.log(volume + 1), 10)
    if FACTOR_NAME == "ROC60":
        return _safe_divide(close.shift(60), close)
    if FACTOR_NAME == "RESI10":
        return _safe_divide(_rolling_residual(close, 10), close)
    if FACTOR_NAME == "VSTD5":
        return _safe_divide(volume.rolling(5, min_periods=5).std(), volume + 1e-12)
    if FACTOR_NAME == "RSQR60":
        return _rolling_rsquare(close, 60)
    if FACTOR_NAME == "CORR60":
        return _rolling_corr(close, np.log(volume + 1), 60)
    if FACTOR_NAME == "WVMA60":
        weighted_abs_ret = (close / close.shift(1) - 1).abs() * volume
        return _safe_divide(
            weighted_abs_ret.rolling(60, min_periods=60).std(),
            weighted_abs_ret.rolling(60, min_periods=60).mean() + 1e-12,
        )
    if FACTOR_NAME == "STD5":
        return _safe_divide(close.rolling(5, min_periods=5).std(), close)
    if FACTOR_NAME == "RSQR20":
        return _rolling_rsquare(close, 20)
    if FACTOR_NAME == "CORD60":
        close_ratio = close / close.shift(1)
        volume_ratio_log = np.log(volume / volume.shift(1) + 1)
        return _rolling_corr(close_ratio, volume_ratio_log, 60)
    if FACTOR_NAME == "CORD10":
        close_ratio = close / close.shift(1)
        volume_ratio_log = np.log(volume / volume.shift(1) + 1)
        return _rolling_corr(close_ratio, volume_ratio_log, 10)
    if FACTOR_NAME == "CORR20":
        return _rolling_corr(close, np.log(volume + 1), 20)
    if FACTOR_NAME == "KLOW":
        return _safe_divide(np.minimum(open_, close) - low, open_)
    raise ValueError(f"Unsupported Alpha158 factor: {FACTOR_NAME}")


def calculate___FACTOR_NAME__(instruments: list, start_date: str, end_date: str) -> pd.DataFrame:
    df = _load_ohlcv(instruments, start_date, end_date)
    values = _compute_alpha158(df)
    return _to_result(values, FACTOR_NAME)
'''


def load_env() -> None:
    env_file = AISTOCK_ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def render_qe_code(factor_name: str) -> str:
    return dedent(QE_CODE_TEMPLATE).strip().replace("__FACTOR_NAME__", factor_name) + "\n"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def write_factor_files() -> dict[str, dict[str, str]]:
    source_dir = AISTOCK_ROOT / "rdagent_assets" / "alpha158_factors"
    qe_dir = AISTOCK_ROOT / "rdagent_assets" / "qe_factors"
    source_dir.mkdir(parents=True, exist_ok=True)
    qe_dir.mkdir(parents=True, exist_ok=True)

    paths: dict[str, dict[str, str]] = {}
    for meta in ALPHA158_FACTORS:
        name = meta["name"]
        code = render_qe_code(name)
        source_path = source_dir / f"{name}.py"
        qe_path = qe_dir / f"{name}.py"
        source_path.write_text(code, encoding="utf-8")
        qe_path.write_text(code, encoding="utf-8")
        paths[name] = {
            "code": code,
            "asset_path": source_path.relative_to(AISTOCK_ROOT).as_posix(),
            "qe_code_path": qe_path.relative_to(AISTOCK_ROOT).as_posix(),
        }
    return paths


def seed_database(paths: dict[str, dict[str, str]], create_classification_stubs: bool) -> None:
    from backend.db.pg_pool import get_conn

    now_utc = datetime.now(timezone.utc).isoformat()
    factor_rows = []
    meta_rows = []
    for item in ALPHA158_FACTORS:
        name = item["name"]
        path_info = paths[name]
        raw_payload = {
            "seed_script": "scripts/seed_alpha158_factor_catalog.py",
            "baseline_set": "qe_alpha158_20",
            "qlib_alias": name,
        }
        tags = {
            "family": "alpha158",
            "baseline_set": "qe_alpha158_20",
            "source": "qlib.contrib.data.loader.Alpha158DL",
        }
        factor_rows.append(
            (
                name,
                ALPHA158_SOURCE,
                CATALOG_VERSION,
                now_utc,
                CATALOG_SOURCE,
                item["expression"],
                "cn",
                json.dumps(tags),
                item["description"],
                item["expression"],
                "day",
                "pit_close",
                "dropna",
                json.dumps(raw_payload),
                path_info["asset_path"],
                path_info["code"],
                path_info["qe_code_path"],
                path_info["code"],
                "price_volume",
                "pv",
                "SUCCESS",
                now_utc,
                sha256_text(item["expression"]),
                True,
            )
        )
        meta_rows.append(
            (
                name,
                CATALOG_VERSION,
                now_utc,
                item["expression"],
                item["description"],
                json.dumps({"fields": ["open", "close", "high", "low", "volume"]}),
                "day",
                "pit_close",
                "dropna",
                "qlib.contrib.data.loader",
                "Alpha158DL",
                CATALOG_VERSION,
                json.dumps(raw_payload),
            )
        )

    with get_conn() as conn:
        with conn.cursor() as cur:
            for row in meta_rows:
                cur.execute(
                    """
                    INSERT INTO aistock_alpha158_meta (
                        factor_name, lib_version, generated_at_utc, expression,
                        description_cn, variables, freq, align, nan_policy,
                        impl_module, impl_func, impl_version, raw_payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (factor_name) DO UPDATE SET
                        lib_version = EXCLUDED.lib_version,
                        generated_at_utc = EXCLUDED.generated_at_utc,
                        expression = EXCLUDED.expression,
                        description_cn = EXCLUDED.description_cn,
                        variables = EXCLUDED.variables,
                        freq = EXCLUDED.freq,
                        align = EXCLUDED.align,
                        nan_policy = EXCLUDED.nan_policy,
                        impl_module = EXCLUDED.impl_module,
                        impl_func = EXCLUDED.impl_func,
                        impl_version = EXCLUDED.impl_version,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    row,
                )

            for row in factor_rows:
                cur.execute(
                    """
                    INSERT INTO aistock_factor_catalog (
                        factor_name, source, catalog_version, generated_at_utc,
                        catalog_source, expression, region, tags, description_cn,
                        formula_hint, freq, align, nan_policy, raw_payload,
                        asset_path, code_text, qe_code_path, realtime_code_text,
                        factor_type, data_source, transformation_status,
                        last_transformation_at, dedup_hash, is_available
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s,
                        %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s,
                        %s, %s::timestamptz, %s, %s
                    )
                    ON CONFLICT (factor_name, source) DO UPDATE SET
                        catalog_version = EXCLUDED.catalog_version,
                        generated_at_utc = EXCLUDED.generated_at_utc,
                        catalog_source = EXCLUDED.catalog_source,
                        expression = EXCLUDED.expression,
                        region = EXCLUDED.region,
                        tags = EXCLUDED.tags,
                        description_cn = EXCLUDED.description_cn,
                        formula_hint = EXCLUDED.formula_hint,
                        freq = EXCLUDED.freq,
                        align = EXCLUDED.align,
                        nan_policy = EXCLUDED.nan_policy,
                        raw_payload = EXCLUDED.raw_payload,
                        asset_path = EXCLUDED.asset_path,
                        code_text = EXCLUDED.code_text,
                        qe_code_path = EXCLUDED.qe_code_path,
                        realtime_code_text = EXCLUDED.realtime_code_text,
                        factor_type = EXCLUDED.factor_type,
                        data_source = EXCLUDED.data_source,
                        transformation_status = EXCLUDED.transformation_status,
                        last_transformation_at = EXCLUDED.last_transformation_at,
                        dedup_hash = EXCLUDED.dedup_hash,
                        is_available = TRUE
                    RETURNING id, factor_name
                    """,
                    row,
                )
                factor_catalog_id, factor_name = cur.fetchone()

                if create_classification_stubs:
                    category = next(
                        item["category"] for item in ALPHA158_FACTORS if item["name"] == factor_name
                    )
                    cur.execute(
                        """
                        INSERT INTO qe_factor_classification (
                            factor_name, factor_source, category,
                            classification_reason, description, factor_dimension,
                            data_source_group, update_freq, analyzed_by,
                            factor_catalog_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (factor_name, factor_source) DO UPDATE SET
                            factor_catalog_id = EXCLUDED.factor_catalog_id,
                            description = COALESCE(qe_factor_classification.description, EXCLUDED.description),
                            factor_dimension = COALESCE(qe_factor_classification.factor_dimension, EXCLUDED.factor_dimension),
                            data_source_group = COALESCE(qe_factor_classification.data_source_group, EXCLUDED.data_source_group),
                            update_freq = COALESCE(qe_factor_classification.update_freq, EXCLUDED.update_freq)
                        """,
                        (
                            factor_name,
                            ALPHA158_SOURCE,
                            category,
                            "Seeded Alpha158 baseline catalog stub; rerun Factor Analyst in UI for official analysis.",
                            next(
                                item["description"]
                                for item in ALPHA158_FACTORS
                                if item["name"] == factor_name
                            ),
                            "price_volume",
                            "price_volume",
                            "daily",
                            "alpha158_seed",
                            factor_catalog_id,
                        ),
                    )


def verify() -> dict[str, int]:
    from backend.db.pg_pool import get_conn

    names = [item["name"] for item in ALPHA158_FACTORS]
    placeholders = ",".join(["%s"] * len(names))
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM aistock_factor_catalog
                WHERE source = %s
                  AND factor_name IN ({placeholders})
                  AND is_available = TRUE
                  AND transformation_status = 'SUCCESS'
                  AND qe_code_path IS NOT NULL
                """,
                [ALPHA158_SOURCE] + names,
            )
            catalog_count = int(cur.fetchone()[0])
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM aistock_alpha158_meta
                WHERE factor_name IN ({placeholders})
                """,
                names,
            )
            meta_count = int(cur.fetchone()[0])
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM qe_factor_classification
                WHERE factor_source = %s
                  AND factor_name IN ({placeholders})
                """,
                [ALPHA158_SOURCE] + names,
            )
            classification_count = int(cur.fetchone()[0])
    return {
        "catalog_count": catalog_count,
        "meta_count": meta_count,
        "classification_count": classification_count,
        "expected": len(names),
    }


def main() -> int:
    load_env()
    paths = write_factor_files()
    seed_database(paths, create_classification_stubs=True)
    result = verify()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result["catalog_count"] != result["expected"] or result["meta_count"] != result["expected"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
