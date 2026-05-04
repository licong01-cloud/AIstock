"""Register hidden HMM sector-factor gate candidates for QE custom loops.

The generated snapshots are precomputed-only artifacts. They do not retrain HMM
models and they do not add new entries to the default QE HMM selector
(`model_type='sector_hmm'`). They are registered under an experimental
model_type so QE custom tasks can reference them by snapshot_id while the UI
selector remains limited to the retained Loop2 and Loop10 baselines.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import uuid
from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras


ROOT = Path(__file__).resolve().parents[1]
MODELS_ROOT = ROOT / "backend" / "data" / "hmm_models"
TMP_ROOT = ROOT / ".codex_tmp" / "hmm_registry_updates"
GROUP_SCORE_CSV = (
    ROOT
    / ".codex_tmp"
    / "hmm_offline_diag"
    / "qe_20260502_131502_9b54"
    / "sector_factor_overlay"
    / "sector_factor_overlay_group_scores.csv"
)

MODEL_TYPE = "sector_hmm_experimental_stacking_20260504"
TARGET_DATE_FOLDER = "2026-05-04"
TEST_START = "2024-07-01"
TEST_END = "2026-04-28"
BACKTEST_END = "2026-04-27"
RUNTIME_PRESET = "preset_A"
SOURCE_QE_TASK = "qe_20260502_131502_9b54"
SCORE_GROUP = "turnover_flow_core"
HIGH_CONFIRM = 0.70
LOW_CONFIRM = 0.30
RISK_ONLY_PENALTY = 0.98

BASES: list[dict[str, Any]] = [
    {
        "base_key": "L2",
        "base_config_id": "b99c907b-873a-4173-a4ee-5eab266f8c49",
        "base_snapshot_id": "bbec3863-fb67-445f-938e-66f092d18696",
        "base_display_name": "HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore",
        "date_folder": "2026-04-27",
        "description": "Loop2 repaired old-covfix baseline with 0.96 penalty and 1.05 boost.",
    },
    {
        "base_key": "L10",
        "base_config_id": "ce4952c1-4b0d-46a7-81f2-ae1d4a249555",
        "base_snapshot_id": "6ea64754-003d-48d8-ad9e-d0e7857716c8",
        "base_display_name": "HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504",
        "date_folder": "2026-05-04",
        "description": "Loop10 current retained best, penalty-only 0.96/1.00.",
    },
]

VARIANTS: list[dict[str, Any]] = [
    {
        "variant": "boost_confirm",
        "display_suffix": "sf_boost_confirm_tfcore_c70",
        "description": "Keep HMM boosts only when sector factor rank is >= 0.70; penalties stay unchanged.",
    },
    {
        "variant": "penalty_confirm",
        "display_suffix": "sf_penalty_confirm_tfcore_c30",
        "description": "Keep HMM penalties only when sector factor rank is <= 0.30; boosts stay unchanged.",
    },
    {
        "variant": "both_confirm",
        "display_suffix": "sf_both_confirm_tfcore_c70c30",
        "description": "Gate both HMM boosts and penalties by sector-factor confirmation.",
    },
    {
        "variant": "risk_only_overlay",
        "display_suffix": "sf_risk_only_tfcore_p098_c30",
        "description": "Keep base HMM and add a 0.98 risk penalty to low-ranked sectors; never add boosts.",
    },
]


def json_default(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def write_json(path: Path, payload: dict[str, Any]) -> None:
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
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


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
        application_name="AIstock-HMM-sector-factor-gate-registry-20260504",
        options="-c client_encoding=utf8",
    )


def safe_config_dir(config_id: str) -> Path:
    target = (MODELS_ROOT / config_id).resolve()
    root = MODELS_ROOT.resolve()
    if target == root or root not in target.parents or target.name != config_id:
        raise RuntimeError(f"unsafe generated HMM config dir: {target}")
    return target


def source_paths(base: dict[str, Any]) -> tuple[Path, Path]:
    model = MODELS_ROOT / base["base_config_id"] / base["date_folder"] / "models.json"
    coeff = (
        MODELS_ROOT
        / base["base_config_id"]
        / base["date_folder"]
        / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
    )
    return model, coeff


def validate_coeff_payload(payload: dict[str, Any], source: Path) -> dict[str, Any]:
    daily = payload.get("daily_coefficients")
    stock_map = payload.get("stock_sector_map")
    if not isinstance(daily, dict) or not daily:
        raise RuntimeError(f"daily_coefficients missing in {source}")
    if not isinstance(stock_map, dict) or not stock_map:
        raise RuntimeError(f"stock_sector_map missing in {source}")
    first_day = daily.get(TEST_START)
    last_day = daily.get(BACKTEST_END)
    if not isinstance(first_day, dict) or not isinstance(last_day, dict):
        raise RuntimeError(f"source coefficients do not cover {TEST_START} and {BACKTEST_END}: {source}")
    sector_count = len(first_day)
    if sector_count <= 0 or len(last_day) != sector_count:
        raise RuntimeError(f"source coefficient sector count is inconsistent: {source}")
    return {
        "daily": daily,
        "stock_sector_map": stock_map,
        "sector_count": sector_count,
        "date_count": len([d for d in daily if TEST_START <= str(d) <= BACKTEST_END]),
    }


def load_score_ranks() -> dict[tuple[str, str], float]:
    if not GROUP_SCORE_CSV.is_file():
        raise RuntimeError(f"sector-factor group score file missing: {GROUP_SCORE_CSV}")
    ranks: dict[tuple[str, str], float] = {}
    with GROUP_SCORE_CSV.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("group") != SCORE_GROUP:
                continue
            trade_date = str(row.get("trade_date") or "")[:10]
            if trade_date < TEST_START or trade_date > BACKTEST_END:
                continue
            sector_code = str(row.get("sector_code") or "").strip()
            if not trade_date or not sector_code:
                continue
            ranks[(trade_date, sector_code)] = float(row["score_rank"])
    if not ranks:
        raise RuntimeError(f"no sector-factor ranks loaded for group={SCORE_GROUP}")
    return ranks


def apply_variant(
    base_daily: dict[str, Any],
    ranks: dict[tuple[str, str], float],
    variant: str,
) -> tuple[dict[str, dict[str, float]], dict[str, Any]]:
    output: dict[str, dict[str, float]] = {}
    stats = {
        "missing_rank_count": 0,
        "boost_neutralized": 0,
        "penalty_neutralized": 0,
        "risk_overlay_added": 0,
        "changed_sector_date_count": 0,
        "base_non_neutral_count": 0,
        "candidate_non_neutral_count": 0,
    }
    for trade_date in sorted(base_daily):
        if not (TEST_START <= str(trade_date) <= BACKTEST_END):
            continue
        day = base_daily[trade_date]
        if not isinstance(day, dict) or not day:
            raise RuntimeError(f"empty source coefficients for {trade_date}")
        out_day: dict[str, float] = {}
        for sector_code, raw_coeff in sorted(day.items()):
            sec = str(sector_code)
            cf = float(raw_coeff)
            rank = ranks.get((str(trade_date), sec))
            if rank is None:
                stats["missing_rank_count"] += 1

            new_cf = cf
            if variant == "boost_confirm":
                if cf > 1.0 and (rank is None or rank < HIGH_CONFIRM):
                    new_cf = 1.0
                    stats["boost_neutralized"] += 1
            elif variant == "penalty_confirm":
                if cf < 1.0 and (rank is None or rank > LOW_CONFIRM):
                    new_cf = 1.0
                    stats["penalty_neutralized"] += 1
            elif variant == "both_confirm":
                if cf > 1.0 and (rank is None or rank < HIGH_CONFIRM):
                    new_cf = 1.0
                    stats["boost_neutralized"] += 1
                elif cf < 1.0 and (rank is None or rank > LOW_CONFIRM):
                    new_cf = 1.0
                    stats["penalty_neutralized"] += 1
            elif variant == "risk_only_overlay":
                if rank is not None and rank <= LOW_CONFIRM and cf >= RISK_ONLY_PENALTY:
                    new_cf = RISK_ONLY_PENALTY
                    if abs(new_cf - cf) > 1e-12:
                        stats["risk_overlay_added"] += 1
            else:
                raise RuntimeError(f"unknown variant: {variant}")

            if abs(cf - 1.0) > 1e-12:
                stats["base_non_neutral_count"] += 1
            if abs(new_cf - 1.0) > 1e-12:
                stats["candidate_non_neutral_count"] += 1
            if abs(new_cf - cf) > 1e-12:
                stats["changed_sector_date_count"] += 1
            out_day[sec] = float(new_cf)
        output[str(trade_date)] = out_day

    if not output or TEST_START not in output or BACKTEST_END not in output:
        raise RuntimeError(f"candidate coefficient coverage must include {TEST_START}..{BACKTEST_END}")
    stats["date_count"] = len(output)
    stats["unique_coefficients"] = sorted({v for day in output.values() for v in day.values()})
    stats["same_as_base"] = stats["changed_sector_date_count"] == 0
    return output, stats


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
    backup_path = TMP_ROOT / f"hmm_sector_factor_gate_registry_before_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
            "operation": "register HMM sector-factor gate QE candidates",
            "configs": configs,
            "snapshots": snapshots,
        },
    )
    return backup_path


def build_candidate_assets(
    base: dict[str, Any],
    variant: dict[str, Any],
    source_model: Path,
    source_coeff: Path,
    source_payload: dict[str, Any],
    source_stats: dict[str, Any],
    base_config_json: dict[str, Any],
    ranks: dict[tuple[str, str], float],
) -> dict[str, Any]:
    config_id = str(uuid.uuid4())
    snapshot_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    base_key = str(base["base_key"])
    variant_name = str(variant["variant"])
    short_name = f"{base_key}_{variant['display_suffix']}"
    display_name = f"HMM_EXP_{short_name}__qe20260504"

    dest_dir = MODELS_ROOT / config_id / TARGET_DATE_FOLDER
    dest_dir.mkdir(parents=True, exist_ok=False)
    dest_model = dest_dir / "models.json"
    dest_coeff = dest_dir / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
    dest_meta = dest_dir / "metadata.json"
    dest_training = dest_dir / "training_result.json"
    shutil.copy2(source_model, dest_model)

    daily, overlay_stats = apply_variant(source_stats["daily"], ranks, variant_name)
    now = datetime.now(timezone.utc).isoformat()
    model_path_win = str(dest_model.resolve())
    coeff_path_win = str(dest_coeff.resolve())
    coeff_payload = deepcopy(source_payload)
    coeff_payload.update(
        {
            "version": "sector_factor_gate_overlay_qe_candidate_v1",
            "candidate": short_name,
            "candidate_display_name": display_name,
            "source_qe_task": SOURCE_QE_TASK,
            "source_config_id": base["base_config_id"],
            "source_snapshot_id": base["base_snapshot_id"],
            "source_display_name": base["base_display_name"],
            "source_coefficients_path": str(source_coeff.resolve()),
            "model_path": model_path_win,
            "model_path_wsl": windows_to_wsl(dest_model),
            "coefficients_path": coeff_path_win,
            "coefficients_path_wsl": windows_to_wsl(dest_coeff),
            "preset_key": RUNTIME_PRESET,
            "runtime_preset_alias": RUNTIME_PRESET,
            "test_start": TEST_START,
            "test_end": TEST_END,
            "backtest_end": BACKTEST_END,
            "sector_count": source_stats["sector_count"],
            "date_count": len(daily),
            "daily_coefficients": daily,
            "stock_sector_map": source_stats["stock_sector_map"],
            "dynamic_coefficients": False,
            "strict_no_leakage": True,
            "precomputed_only": True,
            "runtime_generation_supported": False,
            "registered_for_qe": True,
            "registered_at": now,
            "overlay": {
                "type": "sector_factor_gate_confirmation",
                "variant": variant_name,
                "score_group": SCORE_GROUP,
                "high_confirm": HIGH_CONFIRM,
                "low_confirm": LOW_CONFIRM,
                "risk_only_penalty": RISK_ONLY_PENALTY,
                "stats": overlay_stats,
            },
        }
    )
    write_json(dest_coeff, coeff_payload)

    model_sha = file_sha256(dest_model)
    coeff_sha = file_sha256(dest_coeff)
    config_json = {
        "version": "sector_factor_gate_overlay_qe_candidate_v1",
        "version_role": "sector_factor_gate_confirmation_overlay",
        "ui_label": display_name,
        "description": f"{base['description']} {variant['description']}",
        "registered_by": "scripts/register_hmm_sector_factor_gate_candidates_20260504.py",
        "registered_at": now,
        "source_qe_task": SOURCE_QE_TASK,
        "base_config_id": base["base_config_id"],
        "base_snapshot_id": base["base_snapshot_id"],
        "base_display_name": base["base_display_name"],
        "base_key": base_key,
        "candidate_name": short_name,
        "overlay_variant": variant_name,
        "score_group": SCORE_GROUP,
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
                "role": "qe_default_window_20260504_sector_factor_gate",
                "preset": RUNTIME_PRESET,
                "test_start": TEST_START,
                "backtest_end": BACKTEST_END,
                "strict_no_leakage": True,
            }
        ],
        "signal_presets": {
            RUNTIME_PRESET: {
                "label": "sector-factor gate preset_A only",
                "description": "Precomputed daily sector coefficients for the QE default split only.",
                "coefficients": {"precomputed_daily": True},
            }
        },
        "overlay_parameters": {
            "score_group": SCORE_GROUP,
            "high_confirm": HIGH_CONFIRM,
            "low_confirm": LOW_CONFIRM,
            "risk_only_penalty": RISK_ONLY_PENALTY,
        },
        "overlay_stats": overlay_stats,
        "unique_coefficients": overlay_stats["unique_coefficients"],
        "artifact_sha256": {"models_json": model_sha, "coefficients_json": coeff_sha},
        "inherited_hmm_training_context": {
            key: base_config_json.get(key)
            for key in [
                "train_start",
                "train_end",
                "val_start",
                "val_end",
                "n_states",
                "covariance_type",
                "obs_features",
                "sector_level",
                "rolling_window",
                "cooldown_days",
                "min_trading_days",
                "zscore",
            ]
            if key in base_config_json
        },
    }
    metrics_json = {
        "version": "sector_factor_gate_overlay_qe_candidate_v1",
        "display_name": display_name,
        "snapshot_display_name": display_name + "__default_window",
        "variant_name": short_name,
        "overlay_variant": variant_name,
        "score_group": SCORE_GROUP,
        "source_qe_task": SOURCE_QE_TASK,
        "base_snapshot_id": base["base_snapshot_id"],
        "base_display_name": base["base_display_name"],
        "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
        "test_end": TEST_END,
        "strict_no_leakage": True,
        "precomputed_only": True,
        "runtime_preset": RUNTIME_PRESET,
        "sector_count": source_stats["sector_count"],
        "date_count": len(daily),
        "stock_sector_map_count": len(source_stats["stock_sector_map"]),
        "overlay_stats": overlay_stats,
        "unique_coefficients": overlay_stats["unique_coefficients"],
        "validation_status": "registered_hidden__needs_qe_shadow_loop",
        "hypothesis": variant["description"],
    }
    metadata = {
        "db_registered": True,
        "display_name": display_name,
        "variant_name": short_name,
        "model_type": MODEL_TYPE,
        "model_path": model_path_win,
        "model_path_wsl": windows_to_wsl(dest_model),
        "coefficients_path": coeff_path_win,
        "coefficients_path_wsl": windows_to_wsl(dest_coeff),
        "source_coefficients_path": str(source_coeff.resolve()),
        "base_snapshot_id": base["base_snapshot_id"],
        "registered_at": now,
        "strict_no_leakage": True,
        "overlay_stats": overlay_stats,
        "artifact_sha256": {"models_json": model_sha, "coefficients_json": coeff_sha},
    }
    write_json(dest_meta, metadata)
    write_json(
        dest_training,
        {
            "display_name": display_name,
            "variant_name": short_name,
            "model_path": model_path_win,
            "coefficients_path": coeff_path_win,
            "runtime_preset": RUNTIME_PRESET,
            "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
            "source_qe_task": SOURCE_QE_TASK,
            "base_snapshot_id": base["base_snapshot_id"],
            "strict_no_leakage": True,
            "note": "Synthetic registry snapshot for QE testing; no HMM retraining was run.",
        },
    )
    return {
        "config_id": config_id,
        "snapshot_id": snapshot_id,
        "job_id": job_id,
        "display_name": display_name,
        "variant_name": short_name,
        "config_json": config_json,
        "metrics_json": metrics_json,
        "model_path": model_path_win,
        "sector_count": source_stats["sector_count"],
        "asset_dir": str(dest_dir.resolve()),
        "coefficients_path": coeff_path_win,
        "overlay_stats": overlay_stats,
        "artifact_sha256": {"models_json": model_sha, "coefficients_json": coeff_sha},
    }


def register_candidates(dry_run: bool = False) -> dict[str, Any]:
    ranks = load_score_ranks()
    prepared: list[dict[str, Any]] = []
    backup_path: Path | None = None

    conn = db_connect()
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            display_names = [
                f"HMM_EXP_{base['base_key']}_{variant['display_suffix']}__qe20260504"
                for base in BASES
                for variant in VARIANTS
            ]
            cur.execute(
                "SELECT config_id, model_type, display_name FROM model_train_configs WHERE display_name = ANY(%s)",
                (display_names,),
            )
            existing = [dict(row) for row in cur.fetchall()]
            if existing:
                raise RuntimeError(
                    "candidate display names already exist; aborting to avoid duplicate QE choices: "
                    + json.dumps(existing, ensure_ascii=False, default=json_default)
                )

            backup_path = backup_current_registry(cur)
            for base in BASES:
                source_model, source_coeff = source_paths(base)
                if not source_model.is_file():
                    raise RuntimeError(f"source model missing: {source_model}")
                if not source_coeff.is_file():
                    raise RuntimeError(f"source coefficient artifact missing: {source_coeff}")
                source_payload = read_json(source_coeff)
                source_stats = validate_coeff_payload(source_payload, source_coeff)
                base_config_json = load_base_config(cur, str(base["base_config_id"]))
                for variant in VARIANTS:
                    prepared.append(
                        build_candidate_assets(
                            base,
                            variant,
                            source_model,
                            source_coeff,
                            source_payload,
                            source_stats,
                            base_config_json,
                            ranks,
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

    result = {
        "backup_path": str(backup_path.resolve()) if backup_path else None,
        "dry_run": dry_run,
        "model_type": MODEL_TYPE,
        "source_qe_task": SOURCE_QE_TASK,
        "score_group": SCORE_GROUP,
        "thresholds": {
            "high_confirm": HIGH_CONFIRM,
            "low_confirm": LOW_CONFIRM,
            "risk_only_penalty": RISK_ONLY_PENALTY,
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
                "config_id": item["config_id"],
                "snapshot_id": item["snapshot_id"],
                "model_path": item["model_path"],
                "coefficients_path": item["coefficients_path"],
                "sector_count": item["sector_count"],
                "overlay_stats": item["overlay_stats"],
                "artifact_sha256": item["artifact_sha256"],
            }
            for item in prepared
        ],
    }
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    result_path = TMP_ROOT / f"hmm_sector_factor_gate_registry_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    write_json(result_path, result)
    result["result_path"] = str(result_path.resolve())
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Build assets, then rollback DB and remove generated assets.")
    args = parser.parse_args()
    print(json.dumps(register_candidates(dry_run=args.dry_run), ensure_ascii=False, indent=2, default=json_default))


if __name__ == "__main__":
    main()
