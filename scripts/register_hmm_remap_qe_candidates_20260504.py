"""Register old-covfix HMM coefficient remap candidates for QE selection.

This script creates synthetic, precomputed-only HMM snapshots from the retained
old-covfix model. It does not retrain or overwrite protected HMM assets; it
copies the baseline ``models.json`` into new UUID directories and remaps the
daily coefficients by interpreting source coefficients as states:

- coeff < 1.0: fading
- coeff > 1.0: trending
- coeff == 1.0: neutral

Database credentials are read from TDX_DB_* environment variables.
"""

from __future__ import annotations

import argparse
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

SOURCE_CONFIG_ID = "b99c907b-873a-4173-a4ee-5eab266f8c49"
SOURCE_SNAPSHOT_ID = "bbec3863-fb67-445f-938e-66f092d18696"
SOURCE_DISPLAY_NAME = "HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore"
SOURCE_DATE_FOLDER = "2026-04-27"

TARGET_DATE_FOLDER = "2026-05-04"
TEST_START = "2024-07-01"
TEST_END = "2026-04-28"
BACKTEST_END = "2026-04-27"
RUNTIME_PRESET = "preset_A"
SOURCE_QE_TASK = "qe_20260502_231229_0565"

SOURCE_MODEL = MODELS_ROOT / SOURCE_CONFIG_ID / SOURCE_DATE_FOLDER / "models.json"
SOURCE_COEFF = (
    MODELS_ROOT
    / SOURCE_CONFIG_ID
    / SOURCE_DATE_FOLDER
    / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
)

CANDIDATES: list[dict[str, Any]] = [
    {
        "display_name": "HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504",
        "variant_name": "old_covfix_penalty_only_f096_b000",
        "fading": 0.96,
        "neutral": 1.00,
        "trending": 1.00,
        "hypothesis": "Keep old-covfix risk-off penalty and remove trend boost.",
    },
    {
        "display_name": "HMM_TEST_old_covfix_boost_only_p105__qe20260504",
        "variant_name": "old_covfix_boost_only_p105",
        "fading": 1.00,
        "neutral": 1.00,
        "trending": 1.05,
        "hypothesis": "Keep old-covfix trend boost and remove risk-off penalty.",
    },
    {
        "display_name": "HMM_TEST_old_covfix_penalty094_boost103__qe20260504",
        "variant_name": "old_covfix_penalty094_boost103",
        "fading": 0.94,
        "neutral": 1.00,
        "trending": 1.03,
        "hypothesis": "Use stronger downside penalty and softer trend boost than old covfix.",
    },
    {
        "display_name": "HMM_TEST_old_covfix_penalty095_boost104__qe20260504",
        "variant_name": "old_covfix_penalty095_boost104",
        "fading": 0.95,
        "neutral": 1.00,
        "trending": 1.04,
        "hypothesis": "Use balanced downside penalty and trend boost near old covfix.",
    },
    {
        "display_name": "HMM_TEST_old_covfix_penalty095_boost106__qe20260504",
        "variant_name": "old_covfix_penalty095_boost106",
        "fading": 0.95,
        "neutral": 1.00,
        "trending": 1.06,
        "hypothesis": "Test whether a slightly stronger trend boost improves the current best baseline.",
    },
]


def json_default(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON object expected: {path}")
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=json_default),
        encoding="utf-8",
    )


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


def db_connect() -> psycopg2.extensions.connection:
    password = os.getenv("TDX_DB_PASSWORD", "")
    if not password:
        raise RuntimeError("TDX_DB_PASSWORD is required; refusing to embed local DB secrets in this script")
    return psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=password,
        application_name="AIstock-HMM-remap-registry-20260504",
        options="-c client_encoding=utf8",
    )


def safe_config_dir(config_id: str) -> Path:
    target = (MODELS_ROOT / config_id).resolve()
    root = MODELS_ROOT.resolve()
    if target == root or root not in target.parents or target.name != config_id:
        raise RuntimeError(f"unsafe generated HMM config dir: {target}")
    return target


def classify_source_coeff(value: Any) -> str:
    coeff = float(value)
    if coeff < 1.0:
        return "fading"
    if coeff > 1.0:
        return "trending"
    return "neutral"


def remap_daily_coefficients(
    source_daily: dict[str, Any],
    spec: dict[str, Any],
) -> tuple[dict[str, dict[str, float]], dict[str, int]]:
    output: dict[str, dict[str, float]] = {}
    state_counts = {"fading": 0, "neutral": 0, "trending": 0}
    for trade_date in sorted(source_daily):
        if not (TEST_START <= str(trade_date) <= BACKTEST_END):
            continue
        day = source_daily[trade_date]
        if not isinstance(day, dict) or not day:
            raise RuntimeError(f"empty source coefficients for {trade_date}")
        remapped_day: dict[str, float] = {}
        for sector_code, source_coeff in sorted(day.items()):
            state = classify_source_coeff(source_coeff)
            state_counts[state] += 1
            remapped_day[str(sector_code)] = float(spec[state])
        output[str(trade_date)] = remapped_day
    if not output or TEST_START not in output or BACKTEST_END not in output:
        raise RuntimeError(f"source coefficient coverage must include {TEST_START}..{BACKTEST_END}")
    return output, state_counts


def validate_coeff_payload(payload: dict[str, Any]) -> dict[str, Any]:
    daily = payload.get("daily_coefficients")
    stock_map = payload.get("stock_sector_map")
    if not isinstance(daily, dict) or not daily:
        raise RuntimeError(f"daily_coefficients missing in {SOURCE_COEFF}")
    if not isinstance(stock_map, dict) or not stock_map:
        raise RuntimeError(f"stock_sector_map missing in {SOURCE_COEFF}")
    first_day = daily.get(TEST_START)
    last_day = daily.get(BACKTEST_END)
    if not isinstance(first_day, dict) or not isinstance(last_day, dict):
        raise RuntimeError(f"source coefficients do not cover {TEST_START} and {BACKTEST_END}")
    sector_count = len(first_day)
    if sector_count <= 0 or len(last_day) != sector_count:
        raise RuntimeError("source coefficient sector count is inconsistent")
    return {
        "daily": daily,
        "stock_sector_map": stock_map,
        "sector_count": sector_count,
        "date_count": len([d for d in daily if TEST_START <= str(d) <= BACKTEST_END]),
    }


def build_candidate_assets(
    spec: dict[str, Any],
    source_payload: dict[str, Any],
    source_stats: dict[str, Any],
    base_config_json: dict[str, Any],
) -> dict[str, Any]:
    config_id = str(uuid.uuid4())
    snapshot_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    dest_dir = MODELS_ROOT / config_id / TARGET_DATE_FOLDER
    dest_dir.mkdir(parents=True, exist_ok=False)

    dest_model = dest_dir / "models.json"
    dest_coeff = dest_dir / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
    dest_meta = dest_dir / "metadata.json"
    dest_training = dest_dir / "training_result.json"

    shutil.copy2(SOURCE_MODEL, dest_model)

    daily, state_counts = remap_daily_coefficients(source_stats["daily"], spec)
    coeff_values = sorted({v for day in daily.values() for v in day.values()})
    now = datetime.now(timezone.utc).isoformat()
    model_path_win = str(dest_model.resolve())
    coeff_path_win = str(dest_coeff.resolve())

    coeff_payload = deepcopy(source_payload)
    coeff_payload.update(
        {
            "version": "old_covfix_synthetic_remap_qe_candidate_v1",
            "candidate": spec["variant_name"],
            "candidate_display_name": spec["display_name"],
            "source_qe_task": SOURCE_QE_TASK,
            "source_config_id": SOURCE_CONFIG_ID,
            "source_snapshot_id": SOURCE_SNAPSHOT_ID,
            "source_display_name": SOURCE_DISPLAY_NAME,
            "source_coefficients_path": str(SOURCE_COEFF.resolve()),
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
            "source_state_mapping": {
                "fading_source": "source coefficient < 1.0",
                "neutral_source": "source coefficient == 1.0",
                "trending_source": "source coefficient > 1.0",
                "fading": spec["fading"],
                "neutral": spec["neutral"],
                "trending": spec["trending"],
            },
        }
    )
    coeff_payload["preset_coeffs"] = {
        "trending": spec["trending"],
        "neutral": spec["neutral"],
        "fading": spec["fading"],
    }
    write_json(dest_coeff, coeff_payload)

    model_sha = file_sha256(dest_model)
    coeff_sha = file_sha256(dest_coeff)
    config_json = {
        "version": "old_covfix_synthetic_remap_qe_candidate_v1",
        "version_role": "old_covfix_coefficient_remap_ablation",
        "ui_label": spec["display_name"],
        "description": spec["hypothesis"],
        "registered_by": "scripts/register_hmm_remap_qe_candidates_20260504.py",
        "registered_at": now,
        "source_qe_task": SOURCE_QE_TASK,
        "base_config_id": SOURCE_CONFIG_ID,
        "base_snapshot_id": SOURCE_SNAPSHOT_ID,
        "base_display_name": SOURCE_DISPLAY_NAME,
        "candidate_name": spec["variant_name"],
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
                "role": "qe_default_window_20260504_old_covfix_remap",
                "preset": RUNTIME_PRESET,
                "test_start": TEST_START,
                "backtest_end": BACKTEST_END,
                "strict_no_leakage": True,
            }
        ],
        "signal_presets": {
            RUNTIME_PRESET: {
                "label": "old covfix remap preset_A only",
                "description": "Synthetic precomputed coefficients for QE default split only.",
                "coefficients": {
                    "trending": spec["trending"],
                    "neutral": spec["neutral"],
                    "fading": spec["fading"],
                },
            }
        },
        "source_state_counts": state_counts,
        "unique_coefficients": coeff_values,
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
        "version": "old_covfix_synthetic_remap_qe_candidate_v1",
        "display_name": spec["display_name"],
        "snapshot_display_name": spec["display_name"] + "__default_window",
        "variant_name": spec["variant_name"],
        "source_qe_task": SOURCE_QE_TASK,
        "base_snapshot_id": SOURCE_SNAPSHOT_ID,
        "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
        "test_end": TEST_END,
        "strict_no_leakage": True,
        "precomputed_only": True,
        "runtime_preset": RUNTIME_PRESET,
        "sector_count": source_stats["sector_count"],
        "date_count": len(daily),
        "stock_sector_map_count": len(source_stats["stock_sector_map"]),
        "source_state_counts": state_counts,
        "unique_coefficients": coeff_values,
        "validation_status": "registered_selectable__needs_qe_shadow_loop",
        "hypothesis": spec["hypothesis"],
    }
    metadata = {
        "db_registered": True,
        "display_name": spec["display_name"],
        "variant_name": spec["variant_name"],
        "model_path": model_path_win,
        "model_path_wsl": windows_to_wsl(dest_model),
        "coefficients_path": coeff_path_win,
        "coefficients_path_wsl": windows_to_wsl(dest_coeff),
        "source_coefficients_path": str(SOURCE_COEFF.resolve()),
        "base_snapshot_id": SOURCE_SNAPSHOT_ID,
        "registered_at": now,
        "strict_no_leakage": True,
        "artifact_sha256": {"models_json": model_sha, "coefficients_json": coeff_sha},
    }
    write_json(dest_meta, metadata)
    write_json(
        dest_training,
        {
            "display_name": spec["display_name"],
            "variant_name": spec["variant_name"],
            "model_path": model_path_win,
            "coefficients_path": coeff_path_win,
            "runtime_preset": RUNTIME_PRESET,
            "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
            "source_qe_task": SOURCE_QE_TASK,
            "base_snapshot_id": SOURCE_SNAPSHOT_ID,
            "strict_no_leakage": True,
            "note": "Synthetic registry snapshot for QE testing; no HMM retraining was run.",
        },
    )
    return {
        "config_id": config_id,
        "snapshot_id": snapshot_id,
        "job_id": job_id,
        "display_name": spec["display_name"],
        "variant_name": spec["variant_name"],
        "config_json": config_json,
        "metrics_json": metrics_json,
        "model_path": model_path_win,
        "sector_count": source_stats["sector_count"],
        "asset_dir": str(dest_dir.resolve()),
        "coefficients_path": coeff_path_win,
        "artifact_sha256": {"models_json": model_sha, "coefficients_json": coeff_sha},
    }


def load_base_config(cur: psycopg2.extensions.cursor) -> dict[str, Any]:
    cur.execute("SELECT config_json FROM model_train_configs WHERE config_id = %s", (SOURCE_CONFIG_ID,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"source config not found: {SOURCE_CONFIG_ID}")
    config_json = row["config_json"]
    if isinstance(config_json, str):
        return json.loads(config_json)
    return dict(config_json or {})


def backup_current_registry(cur: psycopg2.extensions.cursor) -> Path:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    backup_path = TMP_ROOT / f"hmm_remap_registry_before_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
    cur.execute(
        """
        SELECT j.* FROM model_train_jobs j
        JOIN model_train_configs c ON c.config_id = j.config_id
        WHERE c.model_type LIKE 'sector_hmm%%'
        ORDER BY j.started_at, j.job_id
        """
    )
    jobs = [dict(row) for row in cur.fetchall()]
    write_json(
        backup_path,
        {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "operation": "register old-covfix synthetic remap QE candidates",
            "configs": configs,
            "snapshots": snapshots,
            "jobs": jobs,
        },
    )
    return backup_path


def register_candidates(dry_run: bool = False) -> dict[str, Any]:
    if not SOURCE_MODEL.is_file():
        raise RuntimeError(f"source model missing: {SOURCE_MODEL}")
    if not SOURCE_COEFF.is_file():
        raise RuntimeError(f"source coefficient artifact missing: {SOURCE_COEFF}")

    source_payload = read_json(SOURCE_COEFF)
    source_stats = validate_coeff_payload(source_payload)
    prepared: list[dict[str, Any]] = []
    backup_path: Path | None = None

    conn = db_connect()
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            display_names = [spec["display_name"] for spec in CANDIDATES]
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

            base_config_json = load_base_config(cur)
            backup_path = backup_current_registry(cur)
            for spec in CANDIDATES:
                prepared.append(build_candidate_assets(spec, source_payload, source_stats, base_config_json))

            if not dry_run:
                for item in prepared:
                    cur.execute(
                        """
                        INSERT INTO model_train_configs
                            (config_id, model_type, display_name, config_json, cron_enabled)
                        VALUES (%s, 'sector_hmm', %s, %s, false)
                        """,
                        (
                            item["config_id"],
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
        "source": {
            "config_id": SOURCE_CONFIG_ID,
            "snapshot_id": SOURCE_SNAPSHOT_ID,
            "display_name": SOURCE_DISPLAY_NAME,
            "model_path": str(SOURCE_MODEL.resolve()),
            "coefficients_path": str(SOURCE_COEFF.resolve()),
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
                "artifact_sha256": item["artifact_sha256"],
            }
            for item in prepared
        ],
    }
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    result_path = TMP_ROOT / f"hmm_remap_registry_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    write_json(result_path, result)
    result["result_path"] = str(result_path.resolve())
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Build and validate assets, then rollback DB and remove assets.")
    args = parser.parse_args()
    print(json.dumps(register_candidates(dry_run=args.dry_run), ensure_ascii=False, indent=2, default=json_default))


if __name__ == "__main__":
    main()
