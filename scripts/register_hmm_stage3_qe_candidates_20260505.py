"""Register Stage-3 retrained HMM candidates as QE-selectable snapshots.

The source score panels were produced by real HMM retraining runs where the
sector-factor columns entered the GaussianHMM observation matrix before fit.
This script only converts those completed retrain outputs into the precomputed
coefficient artifacts that QE consumes at backtest time.

It is idempotent by display_name, does not overwrite existing assets, and writes
new artifacts under backend/data/hmm_models/<config_id>/2026-05-05/.
"""

from __future__ import annotations

import argparse
import csv
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

TARGET_DATE_FOLDER = "2026-05-05"
TEST_START = "2024-07-01"
TEST_END = "2026-04-28"
BACKTEST_END = "2026-04-27"
RUNTIME_PRESET = "preset_A"
MODEL_TYPE = "sector_hmm"

BASE_CONFIG_ID = "b99c907b-873a-4173-a4ee-5eab266f8c49"
BASE_SNAPSHOT_ID = "bbec3863-fb67-445f-938e-66f092d18696"
BASE_DISPLAY_NAME = "HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore"
LOOP10_CONFIG_ID = "ce4952c1-4b0d-46a7-81f2-ae1d4a249555"
LOOP10_SNAPSHOT_ID = "6ea64754-003d-48d8-ad9e-d0e7857716c8"
LOOP10_DISPLAY_NAME = "HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504"

BASE_DATE_FOLDER = "2026-04-27"
BASE_COEFF = (
    MODELS_ROOT
    / BASE_CONFIG_ID
    / BASE_DATE_FOLDER
    / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
)


@dataclass(frozen=True)
class CandidateSpec:
    key: str
    display_name: str
    source_dir: str
    candidate: str
    n_states: int
    covariance_type: str
    score_column: str
    score_method: str
    transform: str
    range_name: str
    coefficient_low: float
    coefficient_high: float
    description: str
    qe_label: str


CANDIDATES: list[CandidateSpec] = [
    CandidateSpec(
        key="stage3_fbt_robust_n2_tf_penalty_0p96_1p00",
        display_name="HMM_STAGE3_FBT_ROBUST_N2_TF_PEN_0p96__qe20260505",
        source_dir=".codex_tmp/hmm_sector_factor_stage3_best_final_20260505",
        candidate="stage3_flow_breadth_tier_robust",
        n_states=2,
        covariance_type="diag",
        score_column="hmm_score",
        score_method="trend_fade",
        transform="direct_clip1",
        range_name="penalty_only_0p96_1p00",
        coefficient_low=0.96,
        coefficient_high=1.00,
        description=(
            "Stage-3 best retrained HMM: flow + breadth + flow-tier emissions, "
            "robust train-window zscore, trend-fade score, penalty-only mapping."
        ),
        qe_label="Stage3 FBT robust n2 trend-fade penalty-only",
    ),
    CandidateSpec(
        key="stage3_fbt_robust_n2_tf_sym_0p96_1p04",
        display_name="HMM_STAGE3_FBT_ROBUST_N2_TF_SYM_0p96_1p04__qe20260505",
        source_dir=".codex_tmp/hmm_sector_factor_stage3_best_final_20260505",
        candidate="stage3_flow_breadth_tier_robust",
        n_states=2,
        covariance_type="diag",
        score_column="hmm_score",
        score_method="trend_fade",
        transform="direct_clip1",
        range_name="symmetric_conservative_0p96_1p04",
        coefficient_low=0.96,
        coefficient_high=1.04,
        description=(
            "Stage-3 best retrained HMM with conservative symmetric "
            "sector rotation adjustment from trend-fade score."
        ),
        qe_label="Stage3 FBT robust n2 trend-fade symmetric",
    ),
    CandidateSpec(
        key="stage3_fbt_robust_n2_tf_agg_0p95_1p08",
        display_name="HMM_STAGE3_FBT_ROBUST_N2_TF_AGG_0p95_1p08__qe20260505",
        source_dir=".codex_tmp/hmm_sector_factor_stage3_best_final_20260505",
        candidate="stage3_flow_breadth_tier_robust",
        n_states=2,
        covariance_type="diag",
        score_column="hmm_score",
        score_method="trend_fade",
        transform="direct_clip1",
        range_name="symmetric_aggressive_0p95_1p08",
        coefficient_low=0.95,
        coefficient_high=1.08,
        description=(
            "Stage-3 best retrained HMM with stronger symmetric mapping; "
            "included to test whether the new trend-fade signal can tolerate amplitude."
        ),
        qe_label="Stage3 FBT robust n2 trend-fade aggressive",
    ),
    CandidateSpec(
        key="stage3_turnover_light_n3_util_penalty_0p96_1p00",
        display_name="HMM_STAGE3_TURNOVER_LIGHT_N3_UTIL_PEN_0p96__qe20260505",
        source_dir=".codex_tmp/hmm_sector_factor_stage3_diag3_20260505",
        candidate="stage3_flow_breadth_turnover_light",
        n_states=3,
        covariance_type="diag",
        score_column="utility_raw_score",
        score_method="utility_raw",
        transform="val_zscore_clip2",
        range_name="penalty_only_0p96_1p00",
        coefficient_low=0.96,
        coefficient_high=1.00,
        description=(
            "Stage-3 secondary retrained HMM: flow + breadth + light turnover "
            "context, utility_raw validation-zscore, penalty-only mapping."
        ),
        qe_label="Stage3 turnover-light n3 utility penalty-only",
    ),
]


def json_default(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, np.generic):
        return str(obj.item())
    return str(obj)


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON object expected: {path}")
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=json_default),
        encoding="utf-8",
    )


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def windows_to_wsl(path: Path) -> str:
    text = str(path.resolve()).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    return text


def load_dotenv_if_present() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8-sig", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
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
        application_name="AIstock-HMM-stage3-qe-registry-20260505",
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
        raise RuntimeError(f"base coefficients do not cover {TEST_START} and {BACKTEST_END}")
    sectors = sorted(str(k) for k in first_day)
    if not sectors or sorted(str(k) for k in last_day) != sectors:
        raise RuntimeError("base coefficient sector set is inconsistent")
    dates = sorted(str(day) for day in daily if TEST_START <= str(day) <= BACKTEST_END)
    if not dates or dates[0] != TEST_START or dates[-1] != BACKTEST_END:
        raise RuntimeError(f"base coefficient date coverage is incomplete: {dates[:1]}..{dates[-1:]}")
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
    if transform == "direct_clip1":
        return score.clip(-1.0, 1.0).fillna(0.0)
    if transform == "val_zscore_clip2":
        validation = frame.loc[frame["split"] == "validation", score_col].astype(float)
        mean = float(validation.mean()) if len(validation) else float(score.mean())
        std = float(validation.std(ddof=0)) if len(validation) else float(score.std(ddof=0))
        if not math.isfinite(std) or std < 1e-12:
            std = 1.0
        return (((score - mean) / std).clip(-2.0, 2.0) / 2.0).fillna(0.0)
    raise ValueError(f"Unsupported transform: {transform}")


def load_summary_row(source_dir: Path, spec: CandidateSpec) -> dict[str, Any]:
    summary_path = source_dir / "summary.csv"
    if not summary_path.is_file():
        return {}
    with summary_path.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("candidate") != spec.candidate:
                continue
            if str(row.get("n_states")) != str(spec.n_states):
                continue
            if row.get("covariance_type") != spec.covariance_type:
                continue
            if row.get("score_column") != spec.score_column:
                continue
            if row.get("score_method") and row.get("score_method") != spec.score_method:
                continue
            return {
                key: coerce_csv_scalar(value)
                for key, value in row.items()
                if value not in (None, "")
            }
    return {}


def coerce_csv_scalar(value: str) -> Any:
    text = str(value).strip()
    if text.lower() == "true":
        return True
    if text.lower() == "false":
        return False
    try:
        if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
            return int(text)
        return float(text)
    except ValueError:
        return text


def load_source_context(source_dir: Path, spec: CandidateSpec) -> dict[str, Any]:
    run_config_path = source_dir / "run_config.json"
    run_config = read_json(run_config_path) if run_config_path.is_file() else {}
    args = dict(run_config.get("args") or {})
    for secret_key in ("db_password", "db_user", "db_host", "db_name"):
        args.pop(secret_key, None)
    diagnostics_path = source_dir / "models" / spec.candidate / "model_diagnostics.json"
    diagnostics = read_json(diagnostics_path)
    meta = diagnostics.get("meta") if isinstance(diagnostics.get("meta"), dict) else {}
    summary = diagnostics.get("summary") if isinstance(diagnostics.get("summary"), dict) else {}
    return {
        "source_dir": str(source_dir),
        "run_args": args,
        "base_features": run_config.get("base_features"),
        "candidate_specs": run_config.get("candidate_specs"),
        "score_methods": run_config.get("score_methods"),
        "horizons": run_config.get("horizons"),
        "horizon_weights": run_config.get("horizon_weights"),
        "diagnostics_meta": meta,
        "diagnostics_summary": summary,
        "summary_row": load_summary_row(source_dir, spec),
    }


def load_retrained_models(source_dir: Path, spec: CandidateSpec) -> dict[str, Any]:
    diagnostics_path = source_dir / "models" / spec.candidate / "model_diagnostics.json"
    diagnostics = read_json(diagnostics_path)
    models = diagnostics.get("models")
    if not isinstance(models, dict) or not models:
        raise RuntimeError(f"model diagnostics does not contain models: {diagnostics_path}")
    if len(models) < 100:
        raise RuntimeError(f"too few HMM sector models in {diagnostics_path}: {len(models)}")
    return {
        str(sector): payload
        for sector, payload in sorted(models.items())
        if isinstance(payload, dict)
    }


def build_daily_coefficients(
    spec: CandidateSpec,
    base_stats: dict[str, Any],
) -> tuple[dict[str, dict[str, float]], dict[str, Any]]:
    source_dir = ROOT / spec.source_dir
    panel_path = source_dir / "models" / spec.candidate / "score_panel.csv"
    if not panel_path.is_file():
        raise RuntimeError(f"score panel missing: {panel_path}")
    required = {"trade_date", "sector_code", "split", spec.score_column}
    # The panel is bounded to four columns and one fixed QE window before any mapping.
    frame = pd.read_table(panel_path, sep=",", usecols=lambda col: col in required)
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"{panel_path} missing columns: {missing}")

    frame = frame.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y-%m-%d")
    frame["sector_code"] = frame["sector_code"].astype(str)
    frame = frame[(frame["trade_date"] >= TEST_START) & (frame["trade_date"] <= BACKTEST_END)].copy()
    duplicate_pairs = int(frame.duplicated(["trade_date", "sector_code"]).sum())
    if duplicate_pairs:
        raise RuntimeError(f"duplicate score rows in {panel_path}: {duplicate_pairs}")

    centered = score_centered(frame, spec.score_column, spec.transform)
    coeff = centered_to_coeff(centered, spec.coefficient_low, spec.coefficient_high)
    frame["mapped_coefficient"] = coeff
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
    coeff_values = [value for day in daily.values() for value in day.values()]
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
        "coefficient_min": float(min(coeff_values)),
        "coefficient_max": float(max(coeff_values)),
        "unique_coefficients_count": len({value for value in coeff_values}),
        "neutral_fill_policy": "known 2026-03-13 missing sector rows are filled with 1.0 and counted",
    }
    return daily, stats


def load_config_json(cur: psycopg2.extensions.cursor, config_id: str) -> dict[str, Any]:
    cur.execute("SELECT config_json FROM model_train_configs WHERE config_id = %s", (config_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"config not found: {config_id}")
    payload = row["config_json"]
    if isinstance(payload, str):
        return json.loads(payload)
    return dict(payload or {})


def backup_current_registry(cur: psycopg2.extensions.cursor) -> Path:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    backup_path = TMP_ROOT / f"hmm_stage3_qe_registry_before_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
            "operation": "register Stage-3 retrained HMM QE candidates",
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
    loop10_config_json: dict[str, Any],
) -> dict[str, Any]:
    config_id = str(uuid.uuid4())
    snapshot_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    source_dir = ROOT / spec.source_dir
    dest_dir = MODELS_ROOT / config_id / TARGET_DATE_FOLDER
    dest_dir.mkdir(parents=True, exist_ok=False)

    dest_model = dest_dir / "models.json"
    dest_coeff = dest_dir / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
    dest_meta = dest_dir / "metadata.json"
    dest_training = dest_dir / "training_result.json"
    dest_diagnostics = dest_dir / "model_diagnostics.json"

    retrained_models = load_retrained_models(source_dir, spec)
    write_json(dest_model, retrained_models)
    shutil.copy2(source_dir / "models" / spec.candidate / "model_diagnostics.json", dest_diagnostics)

    daily, coeff_stats = build_daily_coefficients(spec, base_stats)
    source_context = load_source_context(source_dir, spec)
    now = datetime.now(timezone.utc).isoformat()
    model_path_win = str(dest_model.resolve())
    coeff_path_win = str(dest_coeff.resolve())

    coeff_payload = deepcopy(source_payload)
    coeff_payload.update(
        {
            "version": "stage3_retrained_hmm_qe_candidate_v1",
            "candidate": spec.key,
            "candidate_display_name": spec.display_name,
            "qe_label": spec.qe_label,
            "source_config_id": BASE_CONFIG_ID,
            "source_snapshot_id": BASE_SNAPSHOT_ID,
            "source_display_name": BASE_DISPLAY_NAME,
            "loop10_reference_config_id": LOOP10_CONFIG_ID,
            "loop10_reference_snapshot_id": LOOP10_SNAPSHOT_ID,
            "loop10_reference_display_name": LOOP10_DISPLAY_NAME,
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
            "stage3_mapping": {
                "source": "stage3_retrained_hmm_score_panel",
                "source_dir": spec.source_dir,
                "candidate": spec.candidate,
                "n_states": spec.n_states,
                "covariance_type": spec.covariance_type,
                "score_column": spec.score_column,
                "score_method": spec.score_method,
                "transform": spec.transform,
                "range_name": spec.range_name,
                "coefficient_low": spec.coefficient_low,
                "coefficient_high": spec.coefficient_high,
                "description": spec.description,
                "coefficient_stats": coeff_stats,
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
    diagnostics_sha = file_sha256(dest_diagnostics)
    artifact_sha = {
        "models_json": model_sha,
        "coefficients_json": coeff_sha,
        "model_diagnostics_json": diagnostics_sha,
    }
    config_json = {
        "version": "stage3_retrained_hmm_qe_candidate_v1",
        "version_role": "stage3_retrained_sector_factor_hmm",
        "ui_label": spec.display_name,
        "qe_label": spec.qe_label,
        "description": spec.description,
        "registered_by": "scripts/register_hmm_stage3_qe_candidates_20260505.py",
        "registered_at": now,
        "base_config_id": BASE_CONFIG_ID,
        "base_snapshot_id": BASE_SNAPSHOT_ID,
        "base_display_name": BASE_DISPLAY_NAME,
        "loop10_reference_config_id": LOOP10_CONFIG_ID,
        "loop10_reference_snapshot_id": LOOP10_SNAPSHOT_ID,
        "loop10_reference_display_name": LOOP10_DISPLAY_NAME,
        "candidate_name": spec.key,
        "hmm_candidate": spec.candidate,
        "n_states": spec.n_states,
        "covariance_type": spec.covariance_type,
        "score_column": spec.score_column,
        "score_method": spec.score_method,
        "transform": spec.transform,
        "range_name": spec.range_name,
        "coefficient_low": spec.coefficient_low,
        "coefficient_high": spec.coefficient_high,
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
                "role": "qe_default_window_20260505_stage3_retrained_hmm",
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
                    "score_method": spec.score_method,
                    "transform": spec.transform,
                    "range": [spec.coefficient_low, spec.coefficient_high],
                },
            }
        },
        "coefficient_stats": coeff_stats,
        "retrained_hmm_context": source_context,
        "inherited_runtime_artifact_context": {
            "baseline": {
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
            "loop10_reference": {
                key: loop10_config_json.get(key)
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
                if key in loop10_config_json
            },
        },
        "artifact_sha256": artifact_sha,
    }
    metrics_json = {
        "version": "stage3_retrained_hmm_qe_candidate_v1",
        "display_name": spec.display_name,
        "snapshot_display_name": spec.display_name + "__default_window",
        "variant_name": spec.key,
        "qe_label": spec.qe_label,
        "base_snapshot_id": BASE_SNAPSHOT_ID,
        "loop10_reference_snapshot_id": LOOP10_SNAPSHOT_ID,
        "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
        "test_end": TEST_END,
        "strict_no_leakage": True,
        "precomputed_only": True,
        "runtime_preset": RUNTIME_PRESET,
        "sector_count": base_stats["sector_count"],
        "date_count": len(daily),
        "stock_sector_map_count": len(base_stats["stock_sector_map"]),
        "coefficient_stats": coeff_stats,
        "summary_row": source_context.get("summary_row"),
        "validation_status": "registered_selectable__needs_qe_shadow_loop",
        "hypothesis": spec.description,
    }
    metadata = {
        "db_registered": True,
        "display_name": spec.display_name,
        "variant_name": spec.key,
        "qe_label": spec.qe_label,
        "model_type": MODEL_TYPE,
        "model_path": model_path_win,
        "model_path_wsl": windows_to_wsl(dest_model),
        "coefficients_path": coeff_path_win,
        "coefficients_path_wsl": windows_to_wsl(dest_coeff),
        "diagnostics_path": str(dest_diagnostics.resolve()),
        "source_coefficients_path": str(BASE_COEFF.resolve()),
        "base_snapshot_id": BASE_SNAPSHOT_ID,
        "loop10_reference_snapshot_id": LOOP10_SNAPSHOT_ID,
        "registered_at": now,
        "strict_no_leakage": True,
        "coefficient_stats": coeff_stats,
        "artifact_sha256": artifact_sha,
    }
    write_json(dest_meta, metadata)
    write_json(
        dest_training,
        {
            "display_name": spec.display_name,
            "variant_name": spec.key,
            "qe_label": spec.qe_label,
            "model_path": model_path_win,
            "coefficients_path": coeff_path_win,
            "runtime_preset": RUNTIME_PRESET,
            "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
            "source": "Stage-3 retrained HMM diagnostic score panel converted to QE coefficient artifact",
            "base_snapshot_id": BASE_SNAPSHOT_ID,
            "loop10_reference_snapshot_id": LOOP10_SNAPSHOT_ID,
            "strict_no_leakage": True,
            "note": "The HMM was retrained before registration; this registry script only packages completed retrain outputs for QE.",
        },
    )
    return {
        "config_id": config_id,
        "snapshot_id": snapshot_id,
        "job_id": job_id,
        "display_name": spec.display_name,
        "variant_name": spec.key,
        "config_json": config_json,
        "metrics_json": metrics_json,
        "model_path": model_path_win,
        "sector_count": base_stats["sector_count"],
        "asset_dir": str(dest_dir.resolve()),
        "coefficients_path": coeff_path_win,
        "coefficient_stats": coeff_stats,
        "summary_row": source_context.get("summary_row"),
        "artifact_sha256": artifact_sha,
    }


def fetch_existing(cur: psycopg2.extensions.cursor, display_names: list[str]) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT c.config_id, c.model_type, c.display_name, s.snapshot_id, s.model_path, s.sector_count
        FROM model_train_configs c
        LEFT JOIN model_train_snapshots s ON s.config_id = c.config_id
        WHERE c.display_name = ANY(%s)
        ORDER BY c.display_name, s.trained_at DESC NULLS LAST
        """,
        (display_names,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        display_name = row.get("display_name")
        if display_name in seen:
            continue
        seen.add(display_name)
        out.append(
            {
                "display_name": display_name,
                "config_id": row.get("config_id"),
                "snapshot_id": row.get("snapshot_id"),
                "model_path": row.get("model_path"),
                "sector_count": row.get("sector_count"),
                "reused_existing": True,
            }
        )
    return out


def register_candidates(dry_run: bool = False) -> dict[str, Any]:
    if not BASE_COEFF.is_file():
        raise RuntimeError(f"base coefficient artifact missing: {BASE_COEFF}")

    source_payload = read_json(BASE_COEFF)
    base_stats = validate_base_coeff_payload(source_payload)
    display_names = [spec.display_name for spec in CANDIDATES]
    prepared: list[dict[str, Any]] = []
    backup_path: Path | None = None

    conn = db_connect()
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            reused = fetch_existing(cur, display_names)
            reused_names = {row["display_name"] for row in reused}
            pending = [spec for spec in CANDIDATES if spec.display_name not in reused_names]
            if pending:
                base_config_json = load_config_json(cur, BASE_CONFIG_ID)
                loop10_config_json = load_config_json(cur, LOOP10_CONFIG_ID)
                backup_path = backup_current_registry(cur)
                for spec in pending:
                    prepared.append(
                        build_candidate_assets(
                            spec,
                            source_payload,
                            base_stats,
                            base_config_json,
                            loop10_config_json,
                        )
                    )
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

    registered = [
        {
            "display_name": item["display_name"],
            "variant_name": item["variant_name"],
            "config_id": item["config_id"],
            "snapshot_id": item["snapshot_id"],
            "model_path": item["model_path"],
            "coefficients_path": item["coefficients_path"],
            "sector_count": item["sector_count"],
            "coefficient_stats": item["coefficient_stats"],
            "summary_row": item["summary_row"],
            "artifact_sha256": item["artifact_sha256"],
            "reused_existing": False,
        }
        for item in prepared
    ] + reused
    result = {
        "backup_path": str(backup_path.resolve()) if backup_path else None,
        "dry_run": dry_run,
        "model_type": MODEL_TYPE,
        "source": {
            "base_config_id": BASE_CONFIG_ID,
            "base_snapshot_id": BASE_SNAPSHOT_ID,
            "base_display_name": BASE_DISPLAY_NAME,
            "loop10_reference_config_id": LOOP10_CONFIG_ID,
            "loop10_reference_snapshot_id": LOOP10_SNAPSHOT_ID,
            "loop10_reference_display_name": LOOP10_DISPLAY_NAME,
            "base_coefficients_path": str(BASE_COEFF.resolve()),
        },
        "split": {
            "test_start": TEST_START,
            "test_end": TEST_END,
            "backtest_end": BACKTEST_END,
            "preset": RUNTIME_PRESET,
        },
        "registered": registered,
    }
    if not dry_run:
        result_path = TMP_ROOT / f"hmm_stage3_qe_registry_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        write_json(result_path, result)
        result["result_path"] = str(result_path.resolve())
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Build artifacts and roll back DB/file changes")
    args = parser.parse_args()
    result = register_candidates(dry_run=args.dry_run)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=json_default))


if __name__ == "__main__":
    main()
