"""Register Loop10-centered FPB bottom-penalty HMM candidates for QE.

The candidates were selected by script-level TopK attribution against the
retained Loop10 baseline.  This script copies the Loop10 model artifact and
attaches precomputed virtual coefficient maps so the versions can be selected
directly in QE.  It does not retrain HMM models and it does not overwrite
existing assets.
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
SOURCE_CONFIG_ID = "ce4952c1-4b0d-46a7-81f2-ae1d4a249555"
SOURCE_SNAPSHOT_ID = "6ea64754-003d-48d8-ad9e-d0e7857716c8"
SOURCE_DISPLAY_NAME = "HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504"
SOURCE_DATE_FOLDER = "2026-05-04"
TARGET_DATE_FOLDER = "2026-05-05"
TEST_START = "2024-07-01"
TEST_END = "2026-04-28"
BACKTEST_END = "2026-04-27"
RUNTIME_PRESET = "preset_A"
SOURCE_QE_TASK = "qe_20260504_184036_3a3c"
SOURCE_MODEL = MODELS_ROOT / SOURCE_CONFIG_ID / SOURCE_DATE_FOLDER / "models.json"
SOURCE_COEFF = (
    MODELS_ROOT
    / SOURCE_CONFIG_ID
    / SOURCE_DATE_FOLDER
    / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
)
VIRTUAL_COEFF_ROOT = (
    ROOT
    / ".codex_tmp"
    / "hmm_loop10_virtual_candidate_screen"
    / SOURCE_QE_TASK
    / "candidate_coefficients"
)

CANDIDATES: list[dict[str, Any]] = [
    {
        "display_name": "HMM_TEST_L10_FPBVALZ_BOTTOM15_PENALTY_0p98__qe20260505",
        "variant_name": "l10_fpbvalz_bottom15_penalty_0p98",
        "virtual_coeff_filename": "VIRT_L10_FPB_VALZ_BOTTOMP15_PENALTY_0p98.json",
        "hypothesis": "Loop10 baseline plus sparse penalty for the bottom 15% FPB_VALZ utility sectors; no boost.",
    },
    {
        "display_name": "HMM_TEST_L10_FPBVALZ_BOTTOM20_PENALTY_0p98__qe20260505",
        "variant_name": "l10_fpbvalz_bottom20_penalty_0p98",
        "virtual_coeff_filename": "VIRT_L10_FPB_VALZ_BOTTOMP20_PENALTY_0p98.json",
        "hypothesis": "Loop10 baseline plus sparse penalty for the bottom 20% FPB_VALZ utility sectors; top script-level candidate.",
    },
    {
        "display_name": "HMM_TEST_L10_FPBVALZ_BOTTOM25_PENALTY_0p98__qe20260505",
        "variant_name": "l10_fpbvalz_bottom25_penalty_0p98",
        "virtual_coeff_filename": "VIRT_L10_FPB_VALZ_BOTTOMP25_PENALTY_0p98.json",
        "hypothesis": "Loop10 baseline plus sparse penalty for the bottom 25% FPB_VALZ utility sectors; sensitivity check.",
    },
]


def load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def json_default(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
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
    load_dotenv(ROOT / ".env")
    password = os.getenv("TDX_DB_PASSWORD", "")
    if not password:
        raise RuntimeError("TDX_DB_PASSWORD is required; load it from .env or the environment first")
    return psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=password,
        application_name="AIstock-HMM-loop10-bottom-penalty-registry-20260505",
        options="-c client_encoding=utf8",
    )


def safe_config_dir(config_id: str) -> Path:
    target = (MODELS_ROOT / config_id).resolve()
    root = MODELS_ROOT.resolve()
    if target == root or root not in target.parents or target.name != config_id:
        raise RuntimeError(f"unsafe generated HMM config dir: {target}")
    return target


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
    backup_path = TMP_ROOT / f"hmm_loop10_bottom_penalty_registry_before_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
            "operation": "register Loop10 FPB bottom-penalty QE candidates",
            "configs": configs,
            "snapshots": snapshots,
        },
    )
    return backup_path


def validate_source_payload(source_payload: dict[str, Any]) -> dict[str, Any]:
    daily = source_payload.get("daily_coefficients")
    stock_map = source_payload.get("stock_sector_map")
    if not isinstance(daily, dict) or not daily:
        raise RuntimeError(f"daily_coefficients missing in {SOURCE_COEFF}")
    if not isinstance(stock_map, dict) or not stock_map:
        raise RuntimeError(f"stock_sector_map missing in {SOURCE_COEFF}")
    if TEST_START not in daily or BACKTEST_END not in daily:
        raise RuntimeError(f"source coefficients do not cover {TEST_START}..{BACKTEST_END}")
    return {
        "daily": daily,
        "stock_sector_map": stock_map,
        "sector_count": len(daily[TEST_START]),
        "date_count": len([d for d in daily if TEST_START <= str(d) <= BACKTEST_END]),
    }


def validate_virtual_coefficients(path: Path, source_stats: dict[str, Any]) -> dict[str, dict[str, float]]:
    payload = read_json(path)
    daily = payload.get("daily_coefficients")
    if not isinstance(daily, dict) or not daily:
        raise RuntimeError(f"virtual daily_coefficients missing: {path}")
    if TEST_START not in daily or BACKTEST_END not in daily:
        raise RuntimeError(f"virtual coefficients do not cover {TEST_START}..{BACKTEST_END}: {path}")
    output: dict[str, dict[str, float]] = {}
    for trade_date in sorted(daily):
        if not (TEST_START <= str(trade_date) <= BACKTEST_END):
            continue
        day = daily[trade_date]
        if not isinstance(day, dict) or not day:
            raise RuntimeError(f"empty virtual coefficients for {trade_date}: {path}")
        output[str(trade_date)] = {str(k): float(v) for k, v in day.items()}
    if len(output) != source_stats["date_count"]:
        raise RuntimeError(f"virtual date count mismatch for {path}: {len(output)} != {source_stats['date_count']}")
    return output


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

    virtual_path = VIRTUAL_COEFF_ROOT / spec["virtual_coeff_filename"]
    daily = validate_virtual_coefficients(virtual_path, source_stats)
    coeff_values = [value for day in daily.values() for value in day.values()]
    unique_coeffs = sorted({round(float(v), 10) for v in coeff_values})
    now = datetime.now(timezone.utc).isoformat()
    model_path_win = str(dest_model.resolve())
    coeff_path_win = str(dest_coeff.resolve())

    coeff_payload = deepcopy(source_payload)
    coeff_payload.update(
        {
            "version": "loop10_fpbvalz_bottom_penalty_qe_candidate_v1",
            "candidate": spec["variant_name"],
            "candidate_display_name": spec["display_name"],
            "source_qe_task": SOURCE_QE_TASK,
            "source_config_id": SOURCE_CONFIG_ID,
            "source_snapshot_id": SOURCE_SNAPSHOT_ID,
            "source_display_name": SOURCE_DISPLAY_NAME,
            "source_coefficients_path": str(SOURCE_COEFF.resolve()),
            "virtual_coefficients_path": str(virtual_path.resolve()),
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
        }
    )
    write_json(dest_coeff, coeff_payload)

    model_sha = file_sha256(dest_model)
    coeff_sha = file_sha256(dest_coeff)
    config_json = {
        "version": "loop10_fpbvalz_bottom_penalty_qe_candidate_v1",
        "version_role": "loop10_centered_sparse_sector_penalty",
        "ui_label": spec["display_name"],
        "description": spec["hypothesis"],
        "registered_by": "scripts/register_hmm_loop10_bottom_penalty_candidates_20260505.py",
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
                "role": "qe_default_window_20260505_loop10_bottom_penalty",
                "preset": RUNTIME_PRESET,
                "test_start": TEST_START,
                "backtest_end": BACKTEST_END,
                "strict_no_leakage": True,
            }
        ],
        "signal_presets": {
            RUNTIME_PRESET: {
                "label": "Loop10 FPB_VALZ sparse bottom penalty preset_A only",
                "description": "Precomputed coefficients for QE default split only.",
            }
        },
        "unique_coefficients": unique_coeffs,
        "coefficient_min": min(coeff_values),
        "coefficient_max": max(coeff_values),
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
        "version": "loop10_fpbvalz_bottom_penalty_qe_candidate_v1",
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
        "unique_coefficients": unique_coeffs,
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
        "virtual_coefficients_path": str(virtual_path.resolve()),
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
            "note": "Precomputed Loop10-centered sparse penalty registry snapshot for QE testing; no HMM retraining was run.",
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


def existing_registered(cur: psycopg2.extensions.cursor, display_names: list[str]) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT c.config_id, c.display_name, s.snapshot_id, s.model_path, s.sector_count
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
        name = row.get("display_name")
        if name in seen:
            continue
        seen.add(name)
        out.append(
            {
                "display_name": name,
                "config_id": row.get("config_id"),
                "snapshot_id": row.get("snapshot_id"),
                "model_path": row.get("model_path"),
                "sector_count": row.get("sector_count"),
                "reused_existing": True,
            }
        )
    return out


def register_candidates(dry_run: bool = False, candidate_specs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    if not SOURCE_MODEL.is_file():
        raise RuntimeError(f"source model missing: {SOURCE_MODEL}")
    if not SOURCE_COEFF.is_file():
        raise RuntimeError(f"source coefficient artifact missing: {SOURCE_COEFF}")

    source_payload = read_json(SOURCE_COEFF)
    source_stats = validate_source_payload(source_payload)
    specs = candidate_specs or CANDIDATES
    display_names = [spec["display_name"] for spec in specs]
    prepared: list[dict[str, Any]] = []
    reused: list[dict[str, Any]] = []
    backup_path: Path | None = None

    conn = db_connect()
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            reused = existing_registered(cur, display_names)
            reused_names = {row["display_name"] for row in reused}
            pending = [spec for spec in specs if spec["display_name"] not in reused_names]
            if pending:
                base_config_json = load_base_config(cur)
                backup_path = backup_current_registry(cur)
                for spec in pending:
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

    registered = [
        {
            "display_name": item["display_name"],
            "variant_name": item["variant_name"],
            "config_id": item["config_id"],
            "snapshot_id": item["snapshot_id"],
            "model_path": item["model_path"],
            "coefficients_path": item["coefficients_path"],
            "sector_count": item["sector_count"],
            "artifact_sha256": item["artifact_sha256"],
            "reused_existing": False,
        }
        for item in prepared
    ] + reused
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
        "registered": registered,
    }
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    result_path = TMP_ROOT / f"hmm_loop10_bottom_penalty_registry_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    write_json(result_path, result)
    result["result_path"] = str(result_path.resolve())
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Build and validate assets, then rollback DB and remove new assets.")
    parser.add_argument(
        "--candidates-json",
        type=Path,
        default=None,
        help="Optional JSON list of candidate specs overriding the built-in stage-1 candidates.",
    )
    args = parser.parse_args()
    candidate_specs = None
    if args.candidates_json:
        raw_specs = json.loads(args.candidates_json.read_text(encoding="utf-8-sig"))
        candidate_specs = raw_specs.get("candidates") if isinstance(raw_specs, dict) else raw_specs
        if not isinstance(candidate_specs, list) or not candidate_specs:
            raise RuntimeError("--candidates-json must contain a non-empty list or {'candidates': [...]}")
    print(
        json.dumps(
            register_candidates(dry_run=args.dry_run, candidate_specs=candidate_specs),
            ensure_ascii=False,
            indent=2,
            default=json_default,
        )
    )


if __name__ == "__main__":
    main()
