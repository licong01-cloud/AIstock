"""Register retrained HMM utility-score mapping candidates for QE loops.

The source score panels were produced by the sector-factor HMM retraining
diagnostic.  This script converts selected HMM utility scores into precomputed
daily sector coefficient artifacts and registers hidden snapshots that custom
QE loops can reference by snapshot_id.

No model is retrained here and no entries are added to the default QE HMM
selector.  The registered artifacts are intentionally precomputed-only because
the QE runtime consumes coefficient JSON files, not diagnostic score panels.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras


ROOT = Path(__file__).resolve().parents[1]
MODELS_ROOT = ROOT / "backend" / "data" / "hmm_models"
TMP_ROOT = ROOT / ".codex_tmp" / "hmm_registry_updates"

MODEL_TYPE = "sector_hmm_experimental_utility_mapping_20260504"
TARGET_DATE_FOLDER = "2026-05-04"
TEST_START = "2024-07-01"
TEST_END = "2026-04-28"
BACKTEST_END = "2026-04-27"
RUNTIME_PRESET = "preset_A"

BASE_CONFIG_ID = "b99c907b-873a-4173-a4ee-5eab266f8c49"
BASE_SNAPSHOT_ID = "bbec3863-fb67-445f-938e-66f092d18696"
BASE_DISPLAY_NAME = "HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore"
BASE_DATE_FOLDER = "2026-04-27"
BASE_MODEL = MODELS_ROOT / BASE_CONFIG_ID / BASE_DATE_FOLDER / "models.json"
BASE_COEFF = (
    MODELS_ROOT
    / BASE_CONFIG_ID
    / BASE_DATE_FOLDER
    / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
)

STAGE3_RANKED = ROOT / ".codex_tmp" / "hmm_coeffmap_stage3_20260504" / "mapping_summary_ranked.csv"


@dataclass(frozen=True)
class CandidateSpec:
    key: str
    display_suffix: str
    source_dir: str
    candidate: str
    n_states: int
    covariance_type: str
    score_column: str
    transform: str
    range_name: str
    coefficient_low: float
    coefficient_high: float
    description: str
    qe_label: str


CANDIDATES: list[CandidateSpec] = [
    CandidateSpec(
        key="fpb_n3_csz_aggressive",
        display_suffix="UTIL_FPB_N3_CSZ_AGG_0p95_1p08",
        source_dir=".codex_tmp/hmm_sector_factor_stage2_diag3_20260504",
        candidate="flow_plus_breadth",
        n_states=3,
        covariance_type="diag",
        score_column="utility_raw_score",
        transform="cs_zscore_clip2",
        range_name="aggressive_0p95_1p08",
        coefficient_low=0.95,
        coefficient_high=1.08,
        description=(
            "Retrained flow-plus-breadth HMM; same-date cross-sectional "
            "z-score utility mapping with aggressive 0.95-1.08 coefficients."
        ),
        qe_label="HMM utility FPB n3 cs-z aggressive high-churn",
    ),
    CandidateSpec(
        key="fpb_n3_valz_aggressive",
        display_suffix="UTIL_FPB_N3_VALZ_AGG_0p95_1p08",
        source_dir=".codex_tmp/hmm_sector_factor_stage2_diag3_20260504",
        candidate="flow_plus_breadth",
        n_states=3,
        covariance_type="diag",
        score_column="utility_raw_score",
        transform="val_zscore_clip2",
        range_name="aggressive_0p95_1p08",
        coefficient_low=0.95,
        coefficient_high=1.08,
        description=(
            "Retrained flow-plus-breadth HMM; validation-window z-score "
            "utility mapping with aggressive 0.95-1.08 coefficients."
        ),
        qe_label="HMM utility FPB n3 val-z aggressive",
    ),
    CandidateSpec(
        key="fpb_n3_valz_conservative",
        display_suffix="UTIL_FPB_N3_VALZ_CONS_0p98_1p03",
        source_dir=".codex_tmp/hmm_sector_factor_stage2_diag3_20260504",
        candidate="flow_plus_breadth",
        n_states=3,
        covariance_type="diag",
        score_column="utility_raw_score",
        transform="val_zscore_clip2",
        range_name="conservative_0p98_1p03",
        coefficient_low=0.98,
        coefficient_high=1.03,
        description=(
            "Retrained flow-plus-breadth HMM; validation-window z-score "
            "utility mapping with conservative 0.98-1.03 coefficients."
        ),
        qe_label="HMM utility FPB n3 val-z conservative comparator",
    ),
    CandidateSpec(
        key="flowcore_n2_valz_aggressive",
        display_suffix="UTIL_FLOWCORE_N2_VALZ_AGG_0p95_1p08",
        source_dir=".codex_tmp/hmm_sector_factor_stage2_diag2_20260504",
        candidate="flow_core",
        n_states=2,
        covariance_type="diag",
        score_column="utility_raw_score",
        transform="val_zscore_clip2",
        range_name="aggressive_0p95_1p08",
        coefficient_low=0.95,
        coefficient_high=1.08,
        description=(
            "Retrained flow-core HMM; lower-state validation z-score "
            "aggressive mapping selected as a lower-change-rate comparator."
        ),
        qe_label="HMM utility flow-core n2 val-z aggressive",
    ),
    CandidateSpec(
        key="volcompress_n4_valz_aggressive",
        display_suffix="UTIL_VOLCOMP_N4_VALZ_AGG_0p95_1p08",
        source_dir=".codex_tmp/hmm_sector_factor_stage2_diag4_20260504",
        candidate="vol_compress",
        n_states=4,
        covariance_type="diag",
        score_column="utility_raw_score",
        transform="val_zscore_clip2",
        range_name="aggressive_0p95_1p08",
        coefficient_low=0.95,
        coefficient_high=1.08,
        description=(
            "Retrained volatility-compression HMM; defensive sector-factor "
            "feature set with validation z-score aggressive mapping."
        ),
        qe_label="HMM utility vol-compress n4 val-z aggressive",
    ),
]


def json_default(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, np.generic):
        return str(obj.item())
    return str(obj)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=json_default),
        encoding="utf-8",
    )


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON object expected: {path}")
    return payload


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def windows_to_wsl(path: Path) -> str:
    text = str(path.resolve()).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    return text


def load_dotenv_if_present() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(env_path)
        return
    except Exception:
        pass
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def db_connect() -> psycopg2.extensions.connection:
    load_dotenv_if_present()
    password = os.getenv("TDX_DB_PASSWORD", "")
    if not password:
        raise RuntimeError("TDX_DB_PASSWORD is required; refusing to embed DB secrets")
    return psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=password,
        application_name="AIstock-HMM-utility-mapping-registry-20260504",
        options="-c client_encoding=utf8",
    )


def safe_config_dir(config_id: str) -> Path:
    target = (MODELS_ROOT / config_id).resolve()
    root = MODELS_ROOT.resolve()
    if target == root or root not in target.parents or target.name != config_id:
        raise RuntimeError(f"unsafe generated HMM config dir: {target}")
    return target


def validate_base_coeff_payload(payload: dict[str, Any]) -> dict[str, Any]:
    daily = payload.get("daily_coefficients")
    stock_map = payload.get("stock_sector_map")
    if not isinstance(daily, dict) or not daily:
        raise RuntimeError(f"daily_coefficients missing in {BASE_COEFF}")
    if not isinstance(stock_map, dict) or not stock_map:
        raise RuntimeError(f"stock_sector_map missing in {BASE_COEFF}")
    first_day = daily.get(TEST_START)
    last_day = daily.get(BACKTEST_END)
    if not isinstance(first_day, dict) or not isinstance(last_day, dict):
        raise RuntimeError(f"source coefficients do not cover {TEST_START} and {BACKTEST_END}")
    sectors = sorted(str(k) for k in first_day.keys())
    if not sectors or sorted(str(k) for k in last_day.keys()) != sectors:
        raise RuntimeError("source coefficient sector set is inconsistent")
    dates = sorted(str(d) for d in daily if TEST_START <= str(d) <= BACKTEST_END)
    if not dates or dates[0] != TEST_START or dates[-1] != BACKTEST_END:
        raise RuntimeError(f"source coefficient date coverage is incomplete: {dates[:1]}..{dates[-1:]}")
    return {
        "daily": daily,
        "stock_sector_map": stock_map,
        "sectors": sectors,
        "dates": dates,
        "sector_count": len(sectors),
        "date_count": len(dates),
    }


def centered_to_coeff(centered: pd.Series | np.ndarray, low: float, high: float) -> np.ndarray:
    arr = np.asarray(centered, dtype=np.float64)
    arr = np.clip(arr, -1.0, 1.0)
    return np.where(arr >= 0, 1.0 + arr * (high - 1.0), 1.0 + arr * (1.0 - low))


def score_centered(frame: pd.DataFrame, score_col: str, transform: str) -> pd.Series:
    score = frame[score_col].astype(float)
    if transform == "cs_zscore_clip2":
        by_date = score.groupby(frame["trade_date"])
        mean = by_date.transform("mean")
        std = by_date.transform("std").replace(0, np.nan)
        return (((score - mean) / std).clip(-2.0, 2.0) / 2.0).fillna(0.0)
    if transform == "val_zscore_clip2":
        validation = frame.loc[frame["split"] == "validation", score_col].astype(float)
        mean = float(validation.mean()) if len(validation) else float(score.mean())
        std = float(validation.std(ddof=0)) if len(validation) else float(score.std(ddof=0))
        if not math.isfinite(std) or std < 1e-12:
            std = 1.0
        return (((score - mean) / std).clip(-2.0, 2.0) / 2.0).fillna(0.0)
    raise ValueError(f"Unsupported transform for QE registration: {transform}")


def load_source_run_context(source_dir: Path, candidate: str) -> dict[str, Any]:
    run_config_path = source_dir / "run_config.json"
    run_config = read_json(run_config_path) if run_config_path.exists() else {}
    args = dict(run_config.get("args") or {})
    for secret_key in ("db_password", "db_user", "db_host", "db_name"):
        args.pop(secret_key, None)
    candidate_specs = run_config.get("candidate_specs") or []
    candidate_spec = next(
        (item for item in candidate_specs if isinstance(item, dict) and item.get("name") == candidate),
        {},
    )
    return {
        "source_dir": str(source_dir),
        "args": args,
        "base_features": run_config.get("base_features"),
        "candidate_spec": candidate_spec,
        "score_methods": run_config.get("score_methods"),
        "horizons": run_config.get("horizons"),
        "horizon_weights": run_config.get("horizon_weights"),
    }


def load_stage3_metrics(spec: CandidateSpec) -> dict[str, Any]:
    if not STAGE3_RANKED.exists():
        return {}
    df = pd.read_csv(STAGE3_RANKED)
    row = df[
        (df["source_dir"] == spec.source_dir)
        & (df["candidate"] == spec.candidate)
        & (df["n_states"] == spec.n_states)
        & (df["covariance_type"] == spec.covariance_type)
        & (df["score_column"] == spec.score_column)
        & (df["transform"] == spec.transform)
        & (df["range_name"] == spec.range_name)
    ]
    if row.empty:
        return {}
    item = row.iloc[0].to_dict()
    return {
        k: (v.item() if isinstance(v, np.generic) else v)
        for k, v in item.items()
        if not (isinstance(v, float) and math.isnan(v))
    }


def build_daily_coefficients(
    spec: CandidateSpec,
    base_stats: dict[str, Any],
) -> tuple[dict[str, dict[str, float]], dict[str, Any]]:
    source_dir = ROOT / spec.source_dir
    panel_path = source_dir / "models" / spec.candidate / "score_panel.csv"
    if not panel_path.is_file():
        raise RuntimeError(f"score panel missing: {panel_path}")
    frame = pd.read_csv(panel_path)
    required = {"trade_date", "sector_code", "split", spec.score_column}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"{panel_path} missing columns: {missing}")

    frame = frame.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y-%m-%d")
    frame["sector_code"] = frame["sector_code"].astype(str)
    frame = frame[(frame["trade_date"] >= TEST_START) & (frame["trade_date"] <= BACKTEST_END)].copy()
    centered = score_centered(frame, spec.score_column, spec.transform)
    coeff = centered_to_coeff(centered, spec.coefficient_low, spec.coefficient_high)
    frame["mapped_coefficient"] = coeff

    duplicate_pairs = int(frame.duplicated(["trade_date", "sector_code"]).sum())
    if duplicate_pairs:
        raise RuntimeError(f"duplicate score rows in {panel_path}: {duplicate_pairs}")

    mapped = {
        (str(row.trade_date), str(row.sector_code)): float(row.mapped_coefficient)
        for row in frame[["trade_date", "sector_code", "mapped_coefficient"]].itertuples(index=False)
    }

    daily: dict[str, dict[str, float]] = {}
    missing_pairs = 0
    non_finite_pairs = 0
    active_pairs = 0
    for trade_date in base_stats["dates"]:
        out_day: dict[str, float] = {}
        for sector_code in base_stats["sectors"]:
            value = mapped.get((trade_date, sector_code))
            if value is None:
                missing_pairs += 1
                value = 1.0
            if not math.isfinite(value):
                non_finite_pairs += 1
                value = 1.0
            if abs(value - 1.0) > 0.001:
                active_pairs += 1
            out_day[sector_code] = round(float(value), 10)
        daily[trade_date] = out_day

    expected_pairs = len(base_stats["dates"]) * len(base_stats["sectors"])
    stats = {
        "source_panel_path": str(panel_path.resolve()),
        "source_panel_rows": int(len(frame)),
        "source_panel_dates": int(frame["trade_date"].nunique()),
        "source_panel_sectors": int(frame["sector_code"].nunique()),
        "expected_sector_date_pairs": expected_pairs,
        "mapped_sector_date_pairs": int(len(mapped)),
        "missing_sector_date_pairs_filled_neutral": missing_pairs,
        "non_finite_pairs_filled_neutral": non_finite_pairs,
        "active_sector_date_pairs": active_pairs,
        "active_rate": active_pairs / expected_pairs if expected_pairs else 0.0,
        "coefficient_min": float(min(v for day in daily.values() for v in day.values())),
        "coefficient_max": float(max(v for day in daily.values() for v in day.values())),
        "unique_coefficients_count": len({v for day in daily.values() for v in day.values()}),
        "neutral_fill_policy": "missing score-panel sector/date pairs are filled with 1.0 and counted",
    }
    return daily, stats


def load_base_config(cur: psycopg2.extensions.cursor, config_id: str) -> dict[str, Any]:
    cur.execute("SELECT config_json FROM model_train_configs WHERE config_id = %s", (config_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"source config not found: {config_id}")
    config_json = row["config_json"]
    if isinstance(config_json, str):
        return json.loads(config_json)
    return dict(config_json or {})


def backup_current_registry(cur: psycopg2.extensions.cursor) -> Path:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    backup_path = TMP_ROOT / f"hmm_utility_mapping_registry_before_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    cur.execute(
        """
        SELECT * FROM model_train_configs
        WHERE model_type LIKE 'sector_hmm%%'
        ORDER BY created_at, config_id
        """
    )
    configs = [dict(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT s.* FROM model_train_snapshots s
        JOIN model_train_configs c ON c.config_id = s.config_id
        WHERE c.model_type LIKE 'sector_hmm%%'
        ORDER BY s.trained_at, s.snapshot_id
        """
    )
    snapshots = [dict(row) for row in cur.fetchall()]
    write_json(
        backup_path,
        {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "operation": "register retrained HMM utility mapping QE candidates",
            "configs": configs,
            "snapshots": snapshots,
        },
    )
    return backup_path


def build_candidate_assets(
    spec: CandidateSpec,
    source_payload: dict[str, Any],
    base_stats: dict[str, Any],
    base_config_json: dict[str, Any],
) -> dict[str, Any]:
    config_id = str(uuid.uuid4())
    snapshot_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    display_name = f"HMM_EXP_{spec.display_suffix}__qe20260504"

    dest_dir = MODELS_ROOT / config_id / TARGET_DATE_FOLDER
    dest_dir.mkdir(parents=True, exist_ok=False)
    dest_model = dest_dir / "models.json"
    dest_coeff = dest_dir / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
    dest_meta = dest_dir / "metadata.json"
    dest_training = dest_dir / "training_result.json"
    shutil.copy2(BASE_MODEL, dest_model)

    daily, coeff_stats = build_daily_coefficients(spec, base_stats)
    run_context = load_source_run_context(ROOT / spec.source_dir, spec.candidate)
    stage3_metrics = load_stage3_metrics(spec)
    now = datetime.now(timezone.utc).isoformat()
    model_path_win = str(dest_model.resolve())
    coeff_path_win = str(dest_coeff.resolve())

    coeff_payload = deepcopy(source_payload)
    coeff_payload.update(
        {
            "version": "retrained_hmm_utility_mapping_qe_candidate_v1",
            "candidate": spec.key,
            "candidate_display_name": display_name,
            "qe_label": spec.qe_label,
            "source_config_id": BASE_CONFIG_ID,
            "source_snapshot_id": BASE_SNAPSHOT_ID,
            "source_display_name": BASE_DISPLAY_NAME,
            "source_coefficients_path": str(BASE_COEFF.resolve()),
            "model_path": model_path_win,
            "model_path_wsl": windows_to_wsl(dest_model),
            "coefficients_path": coeff_path_win,
            "coefficients_path_wsl": windows_to_wsl(dest_coeff),
            "preset_key": RUNTIME_PRESET,
            "runtime_preset_alias": RUNTIME_PRESET,
            "test_start": TEST_START,
            "test_end": TEST_END,
            "backtest_end": BACKTEST_END,
            "sector_count": base_stats["sector_count"],
            "date_count": len(daily),
            "daily_coefficients": daily,
            "stock_sector_map": base_stats["stock_sector_map"],
            "dynamic_coefficients": False,
            "strict_no_leakage": True,
            "precomputed_only": True,
            "runtime_generation_supported": False,
            "registered_for_qe": True,
            "registered_at": now,
            "utility_mapping": {
                "source": "retrained_hmm_sector_factor_score_panel",
                "source_dir": spec.source_dir,
                "candidate": spec.candidate,
                "n_states": spec.n_states,
                "covariance_type": spec.covariance_type,
                "score_column": spec.score_column,
                "transform": spec.transform,
                "range_name": spec.range_name,
                "coefficient_low": spec.coefficient_low,
                "coefficient_high": spec.coefficient_high,
                "description": spec.description,
                "coefficient_stats": coeff_stats,
                "stage3_metrics": stage3_metrics,
            },
        }
    )
    coeff_payload["preset_coeffs"] = {
        "mapping": spec.range_name,
        "low": spec.coefficient_low,
        "neutral": 1.0,
        "high": spec.coefficient_high,
    }
    write_json(dest_coeff, coeff_payload)

    model_sha = file_sha256(dest_model)
    coeff_sha = file_sha256(dest_coeff)
    artifact_sha = {"models_json": model_sha, "coefficients_json": coeff_sha}
    config_json = {
        "version": "retrained_hmm_utility_mapping_qe_candidate_v1",
        "version_role": "sector_factor_retrained_hmm_utility_mapping",
        "ui_label": display_name,
        "qe_label": spec.qe_label,
        "description": spec.description,
        "registered_by": "scripts/register_hmm_utility_mapping_qe_candidates_20260504.py",
        "registered_at": now,
        "base_config_id": BASE_CONFIG_ID,
        "base_snapshot_id": BASE_SNAPSHOT_ID,
        "base_display_name": BASE_DISPLAY_NAME,
        "candidate_name": spec.key,
        "test_start": TEST_START,
        "test_end": TEST_END,
        "coefficient_start": TEST_START,
        "coefficient_end": BACKTEST_END,
        "qe_default_supported": True,
        "precomputed_only": True,
        "runtime_generation_supported": False,
        "strict_no_leakage": True,
        "runtime_preset": RUNTIME_PRESET,
        "coefficient_windows": [
            {
                "role": "qe_default_window_20260504_retrained_hmm_utility_mapping",
                "preset": RUNTIME_PRESET,
                "test_start": TEST_START,
                "backtest_end": BACKTEST_END,
                "strict_no_leakage": True,
            }
        ],
        "signal_presets": {
            RUNTIME_PRESET: {
                "label": spec.qe_label,
                "description": "Precomputed daily sector coefficients for the QE default split only.",
                "coefficients": {
                    "precomputed_daily": True,
                    "source_score_column": spec.score_column,
                    "transform": spec.transform,
                    "range": [spec.coefficient_low, spec.coefficient_high],
                },
            }
        },
        "utility_mapping": {
            "source_dir": spec.source_dir,
            "candidate": spec.candidate,
            "n_states": spec.n_states,
            "covariance_type": spec.covariance_type,
            "score_column": spec.score_column,
            "transform": spec.transform,
            "range_name": spec.range_name,
            "coefficient_low": spec.coefficient_low,
            "coefficient_high": spec.coefficient_high,
        },
        "coefficient_stats": coeff_stats,
        "stage3_metrics": stage3_metrics,
        "retrained_hmm_context": run_context,
        "inherited_runtime_artifact_context": {
            key: base_config_json.get(key)
            for key in [
                "train_start",
                "train_end",
                "val_start",
                "val_end",
                "sector_level",
                "rolling_window",
                "cooldown_days",
                "min_trading_days",
            ]
            if key in base_config_json
        },
        "artifact_sha256": artifact_sha,
    }
    metrics_json = {
        "version": "retrained_hmm_utility_mapping_qe_candidate_v1",
        "display_name": display_name,
        "snapshot_display_name": display_name + "__default_window",
        "variant_name": spec.key,
        "qe_label": spec.qe_label,
        "base_snapshot_id": BASE_SNAPSHOT_ID,
        "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
        "test_end": TEST_END,
        "strict_no_leakage": True,
        "precomputed_only": True,
        "runtime_preset": RUNTIME_PRESET,
        "sector_count": base_stats["sector_count"],
        "date_count": len(daily),
        "stock_sector_map_count": len(base_stats["stock_sector_map"]),
        "coefficient_stats": coeff_stats,
        "stage3_metrics": stage3_metrics,
        "validation_status": "registered_hidden__needs_qe_shadow_loop",
        "hypothesis": spec.description,
    }
    metadata = {
        "db_registered": True,
        "display_name": display_name,
        "variant_name": spec.key,
        "qe_label": spec.qe_label,
        "model_type": MODEL_TYPE,
        "model_path": model_path_win,
        "model_path_wsl": windows_to_wsl(dest_model),
        "coefficients_path": coeff_path_win,
        "coefficients_path_wsl": windows_to_wsl(dest_coeff),
        "source_coefficients_path": str(BASE_COEFF.resolve()),
        "base_snapshot_id": BASE_SNAPSHOT_ID,
        "registered_at": now,
        "strict_no_leakage": True,
        "coefficient_stats": coeff_stats,
        "stage3_metrics": stage3_metrics,
        "artifact_sha256": artifact_sha,
    }
    write_json(dest_meta, metadata)
    write_json(
        dest_training,
        {
            "display_name": display_name,
            "variant_name": spec.key,
            "qe_label": spec.qe_label,
            "model_path": model_path_win,
            "coefficients_path": coeff_path_win,
            "runtime_preset": RUNTIME_PRESET,
            "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
            "source": "retrained HMM diagnostic score panel converted to QE coefficient artifact",
            "base_snapshot_id": BASE_SNAPSHOT_ID,
            "strict_no_leakage": True,
            "note": "Precomputed QE artifact from retrained HMM utility scores; no runtime HMM retraining was run by this registry script.",
        },
    )
    return {
        "config_id": config_id,
        "snapshot_id": snapshot_id,
        "job_id": job_id,
        "display_name": display_name,
        "variant_name": spec.key,
        "qe_label": spec.qe_label,
        "config_json": config_json,
        "metrics_json": metrics_json,
        "model_path": model_path_win,
        "sector_count": base_stats["sector_count"],
        "asset_dir": str(dest_dir.resolve()),
        "coefficients_path": coeff_path_win,
        "coefficient_stats": coeff_stats,
        "stage3_metrics": stage3_metrics,
        "artifact_sha256": artifact_sha,
    }


def fetch_existing(cur: psycopg2.extensions.cursor, display_names: list[str]) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT c.config_id, c.model_type, c.display_name, s.snapshot_id, s.model_path, s.status, s.metrics_json
        FROM model_train_configs c
        JOIN model_train_snapshots s ON s.config_id = c.config_id
        WHERE c.display_name = ANY(%s)
        ORDER BY c.display_name, s.trained_at DESC
        """,
        (display_names,),
    )
    return [dict(row) for row in cur.fetchall()]


def register_candidates(dry_run: bool = False, reuse_existing: bool = True) -> dict[str, Any]:
    if not BASE_MODEL.is_file():
        raise RuntimeError(f"source model missing: {BASE_MODEL}")
    if not BASE_COEFF.is_file():
        raise RuntimeError(f"source coefficient artifact missing: {BASE_COEFF}")

    source_payload = read_json(BASE_COEFF)
    base_stats = validate_base_coeff_payload(source_payload)
    prepared: list[dict[str, Any]] = []
    backup_path: Path | None = None

    display_names = [f"HMM_EXP_{spec.display_suffix}__qe20260504" for spec in CANDIDATES]
    conn = db_connect()
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            existing = fetch_existing(cur, display_names)
            existing_names = {row["display_name"] for row in existing}
            if existing:
                if existing_names == set(display_names) and reuse_existing:
                    conn.rollback()
                    return {
                        "dry_run": dry_run,
                        "reused_existing": True,
                        "model_type": MODEL_TYPE,
                        "split": {
                            "test_start": TEST_START,
                            "test_end": TEST_END,
                            "backtest_end": BACKTEST_END,
                            "preset": RUNTIME_PRESET,
                        },
                        "registered": existing,
                    }
                raise RuntimeError(
                    "partial or duplicate candidate display names already exist; aborting: "
                    + json.dumps(existing, ensure_ascii=False, default=json_default)
                )

            base_config_json = load_base_config(cur, BASE_CONFIG_ID)
            backup_path = backup_current_registry(cur)
            for spec in CANDIDATES:
                prepared.append(build_candidate_assets(spec, source_payload, base_stats, base_config_json))

            if not dry_run:
                for item in prepared:
                    cur.execute(
                        """
                        INSERT INTO model_train_configs
                            (config_id, model_type, display_name, config_json, cron_enabled)
                        VALUES (%s, %s, %s, %s, false)
                        """,
                        (
                            item["config_id"],
                            MODEL_TYPE,
                            item["display_name"],
                            psycopg2.extras.Json(item["config_json"]),
                        ),
                    )
                    cur.execute(
                        """
                        INSERT INTO model_train_snapshots
                            (snapshot_id, config_id, model_path, sector_count, status, metrics_json)
                        VALUES (%s, %s, %s, %s, 'completed', %s)
                        """,
                        (
                            item["snapshot_id"],
                            item["config_id"],
                            item["model_path"],
                            item["sector_count"],
                            psycopg2.extras.Json(item["metrics_json"]),
                        ),
                    )
                    cur.execute(
                        """
                        INSERT INTO model_train_jobs
                            (job_id, config_id, snapshot_id, status, started_at, completed_at)
                        VALUES (%s, %s, %s, 'completed', NOW(), NOW())
                        """,
                        (item["job_id"], item["config_id"], item["snapshot_id"]),
                    )
        if dry_run:
            conn.rollback()
            for item in prepared:
                cfg_dir = safe_config_dir(item["config_id"])
                if cfg_dir.exists():
                    shutil.rmtree(cfg_dir)
        else:
            conn.commit()
    except Exception:
        conn.rollback()
        for item in prepared:
            cfg_dir = safe_config_dir(item["config_id"])
            if cfg_dir.exists():
                shutil.rmtree(cfg_dir)
        raise
    finally:
        conn.close()

    result = {
        "backup_path": str(backup_path.resolve()) if backup_path else None,
        "dry_run": dry_run,
        "reused_existing": False,
        "model_type": MODEL_TYPE,
        "source": {
            "base_config_id": BASE_CONFIG_ID,
            "base_snapshot_id": BASE_SNAPSHOT_ID,
            "base_display_name": BASE_DISPLAY_NAME,
            "model_path": str(BASE_MODEL.resolve()),
            "coefficients_path": str(BASE_COEFF.resolve()),
        },
        "split": {
            "test_start": TEST_START,
            "test_end": TEST_END,
            "backtest_end": BACKTEST_END,
            "preset": RUNTIME_PRESET,
        },
        "registered": [
            {
                "display_name": item["display_name"],
                "variant_name": item["variant_name"],
                "qe_label": item["qe_label"],
                "config_id": item["config_id"],
                "snapshot_id": item["snapshot_id"],
                "model_path": item["model_path"],
                "coefficients_path": item["coefficients_path"],
                "sector_count": item["sector_count"],
                "coefficient_stats": item["coefficient_stats"],
                "stage3_metrics": item["stage3_metrics"],
                "artifact_sha256": item["artifact_sha256"],
            }
            for item in prepared
        ],
    }
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    result_path = TMP_ROOT / f"hmm_utility_mapping_registry_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    write_json(result_path, result)
    result["result_path"] = str(result_path.resolve())
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Build assets, then rollback DB and remove generated assets.")
    parser.add_argument("--no-reuse-existing", action="store_true", help="Abort instead of returning already registered candidates.")
    args = parser.parse_args()
    result = register_candidates(dry_run=args.dry_run, reuse_existing=not args.no_reuse_existing)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=json_default))


if __name__ == "__main__":
    main()
