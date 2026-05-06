"""Register Stage3 sparse-penalty HMM coefficient candidates for QE.

The source HMM training has already been completed in the Stage3 diagnostics.
This script registers selected sparse coefficient maps as precomputed HMM
snapshots so QE can validate them via backtest-only loops. It copies the
retained Loop10 model artifact, attaches the selected sparse daily coefficient
map, and writes hidden registry rows. It does not retrain any model.
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
SOURCE_QE_TASK = "qe_20260505_123035_bf80"
HIDDEN_MODEL_TYPE = "sector_hmm_experimental_stage3_sparse_20260505"

SOURCE_MODEL = MODELS_ROOT / SOURCE_CONFIG_ID / SOURCE_DATE_FOLDER / "models.json"
SOURCE_COEFF = (
    MODELS_ROOT
    / SOURCE_CONFIG_ID
    / SOURCE_DATE_FOLDER
    / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
)
SPARSE_COEFF_ROOT = (
    ROOT
    / ".codex_tmp"
    / "hmm_stage3_sparse_penalty_screen_20260505"
    / SOURCE_QE_TASK
    / "candidate_coefficients"
)

CANDIDATES: list[dict[str, Any]] = [
    {
        "display_name": "HMM_TEST_STAGE3_SPARSE_TL_B05_PEN_0p995__qe20260505",
        "variant_name": "stage3_sparse_turnover_light_b05_pen_0p995",
        "virtual_coeff_filename": "SPARSE_turnover_light_n3_util_low_B05_PEN_0p995_stage3_only.json",
        "hypothesis": "Sparse Stage3 turnover-light penalty on only the worst 5% sectors; very low changed-day count.",
    },
    {
        "display_name": "HMM_TEST_STAGE3_SPARSE_TL_B10_PEN_0p995__qe20260505",
        "variant_name": "stage3_sparse_turnover_light_b10_pen_0p995",
        "virtual_coeff_filename": "SPARSE_turnover_light_n3_util_low_B10_PEN_0p995_stage3_only.json",
        "hypothesis": "Sparse Stage3 turnover-light penalty on the worst 10% sectors; top script-level candidate with positive 20d check.",
    },
    {
        "display_name": "HMM_TEST_STAGE3_SPARSE_FB_B05_PEN_0p995__qe20260505",
        "variant_name": "stage3_sparse_flow_breadth_b05_pen_0p995",
        "virtual_coeff_filename": "SPARSE_flow_breadth_n2_util_low_B05_PEN_0p995_stage3_only.json",
        "hypothesis": "Sparse Stage3 flow-breadth penalty on the worst 5% sectors; independent source cross-check.",
    },
    {
        "display_name": "HMM_TEST_STAGE3_SPARSE_TL_B15_PEN_0p995__qe20260505",
        "variant_name": "stage3_sparse_turnover_light_b15_pen_0p995",
        "virtual_coeff_filename": "SPARSE_turnover_light_n3_util_low_B15_PEN_0p995_stage3_only.json",
        "hypothesis": "Broader sparse Stage3 turnover-light penalty on the worst 15% sectors.",
    },
    {
        "display_name": "HMM_TEST_STAGE3_SPARSE_FB_B20_PEN_0p995__qe20260505",
        "variant_name": "stage3_sparse_flow_breadth_b20_pen_0p995",
        "virtual_coeff_filename": "SPARSE_flow_breadth_n2_util_low_B20_PEN_0p995_stage3_only.json",
        "hypothesis": "Broader sparse Stage3 flow-breadth penalty on the worst 20% sectors; stress tests changed-day breadth.",
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
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


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


def safe_config_dir(config_id: str) -> Path:
    target = (MODELS_ROOT / config_id).resolve()
    root = MODELS_ROOT.resolve()
    if target == root or root not in target.parents or target.name != config_id:
        raise RuntimeError(f"unsafe HMM config dir: {target}")
    return target


def db_connect() -> psycopg2.extensions.connection:
    load_dotenv(ROOT / ".env")
    password = os.getenv("TDX_DB_PASSWORD", "")
    if not password:
        raise RuntimeError("TDX_DB_PASSWORD is required")
    return psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=password,
        application_name="AIstock-HMM-stage3-sparse-registry-20260505",
        options="-c client_encoding=utf8",
    )


def load_base_config(cur: psycopg2.extensions.cursor) -> dict[str, Any]:
    cur.execute("SELECT config_json FROM model_train_configs WHERE config_id = %s", (SOURCE_CONFIG_ID,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"source config not found: {SOURCE_CONFIG_ID}")
    payload = row["config_json"]
    return json.loads(payload) if isinstance(payload, str) else dict(payload or {})


def backup_current_registry(cur: psycopg2.extensions.cursor) -> Path:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    path = TMP_ROOT / f"hmm_stage3_sparse_registry_before_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
        path,
        {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "operation": "register Stage3 sparse HMM QE candidates",
            "configs": configs,
            "snapshots": snapshots,
        },
    )
    return path


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
        "stock_sector_map": stock_map,
        "sector_count": len(daily[TEST_START]),
        "date_count": len([d for d in daily if TEST_START <= str(d) <= BACKTEST_END]),
    }


def validate_sparse_coefficients(path: Path, source_stats: dict[str, Any]) -> dict[str, dict[str, float]]:
    payload = read_json(path)
    daily = payload.get("daily_coefficients")
    if not isinstance(daily, dict) or not daily:
        raise RuntimeError(f"sparse daily_coefficients missing: {path}")
    if TEST_START not in daily or BACKTEST_END not in daily:
        raise RuntimeError(f"sparse coefficients do not cover {TEST_START}..{BACKTEST_END}: {path}")
    out: dict[str, dict[str, float]] = {}
    for trade_date in sorted(daily):
        if not (TEST_START <= str(trade_date) <= BACKTEST_END):
            continue
        day = daily[trade_date]
        if not isinstance(day, dict) or not day:
            raise RuntimeError(f"empty sparse coefficients for {trade_date}: {path}")
        out[str(trade_date)] = {str(k): float(v) for k, v in day.items()}
    if len(out) != source_stats["date_count"]:
        raise RuntimeError(f"sparse date count mismatch: {len(out)} != {source_stats['date_count']}")
    return out


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

    sparse_path = SPARSE_COEFF_ROOT / spec["virtual_coeff_filename"]
    daily = validate_sparse_coefficients(sparse_path, source_stats)
    coeff_values = [value for day in daily.values() for value in day.values()]
    unique_coeffs = sorted({round(float(v), 10) for v in coeff_values})
    now = datetime.now(timezone.utc).isoformat()

    coeff_payload = deepcopy(source_payload)
    coeff_payload.update(
        {
            "version": "stage3_sparse_penalty_qe_candidate_v1",
            "candidate": spec["variant_name"],
            "candidate_display_name": spec["display_name"],
            "source_qe_task": SOURCE_QE_TASK,
            "source_config_id": SOURCE_CONFIG_ID,
            "source_snapshot_id": SOURCE_SNAPSHOT_ID,
            "source_display_name": SOURCE_DISPLAY_NAME,
            "source_coefficients_path": str(SOURCE_COEFF.resolve()),
            "sparse_coefficients_path": str(sparse_path.resolve()),
            "model_path": str(dest_model.resolve()),
            "model_path_wsl": windows_to_wsl(dest_model),
            "coefficients_path": str(dest_coeff.resolve()),
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
        "version": "stage3_sparse_penalty_qe_candidate_v1",
        "version_role": "stage3_retrained_sparse_sector_penalty",
        "ui_label": spec["display_name"],
        "description": spec["hypothesis"],
        "registered_by": "scripts/register_hmm_stage3_sparse_qe_candidates_20260505.py",
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
                "role": "qe_default_window_20260505_stage3_sparse",
                "preset": RUNTIME_PRESET,
                "test_start": TEST_START,
                "backtest_end": BACKTEST_END,
                "strict_no_leakage": True,
            }
        ],
        "signal_presets": {
            RUNTIME_PRESET: {
                "label": "Stage3 sparse penalty preset_A only",
                "description": "Precomputed sparse coefficients for QE default split only.",
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
        "version": "stage3_sparse_penalty_qe_candidate_v1",
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
        "validation_status": "registered_hidden__needs_qe_backtest_only",
        "hypothesis": spec["hypothesis"],
    }
    metadata = {
        "db_registered": True,
        "display_name": spec["display_name"],
        "variant_name": spec["variant_name"],
        "model_path": str(dest_model.resolve()),
        "model_path_wsl": windows_to_wsl(dest_model),
        "coefficients_path": str(dest_coeff.resolve()),
        "coefficients_path_wsl": windows_to_wsl(dest_coeff),
        "sparse_coefficients_path": str(sparse_path.resolve()),
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
            "model_path": str(dest_model.resolve()),
            "coefficients_path": str(dest_coeff.resolve()),
            "runtime_preset": RUNTIME_PRESET,
            "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
            "source_qe_task": SOURCE_QE_TASK,
            "base_snapshot_id": SOURCE_SNAPSHOT_ID,
            "strict_no_leakage": True,
            "note": "Precomputed Stage3 sparse penalty registry snapshot for QE testing; no HMM retraining was run here.",
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
        "model_path": str(dest_model.resolve()),
        "sector_count": source_stats["sector_count"],
        "asset_dir": str(dest_dir.resolve()),
        "coefficients_path": str(dest_coeff.resolve()),
        "artifact_sha256": {"models_json": model_sha, "coefficients_json": coeff_sha},
    }


def existing_registered(cur: psycopg2.extensions.cursor, display_names: list[str]) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT c.config_id, c.display_name, c.model_type, s.snapshot_id, s.model_path, s.sector_count
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
                "model_type": row.get("model_type"),
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
                            VALUES (%s, %s, %s, %s, false)
                            """,
                            (
                                item["config_id"],
                                HIDDEN_MODEL_TYPE,
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
            "model_type": HIDDEN_MODEL_TYPE,
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
    result_path = TMP_ROOT / f"hmm_stage3_sparse_registry_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    write_json(result_path, result)
    result["result_path"] = str(result_path.resolve())
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--candidates-json", type=Path, default=None)
    args = parser.parse_args()
    specs = None
    if args.candidates_json:
        raw = json.loads(args.candidates_json.read_text(encoding="utf-8-sig"))
        specs = raw.get("candidates") if isinstance(raw, dict) else raw
        if not isinstance(specs, list) or not specs:
            raise RuntimeError("--candidates-json must contain a non-empty list or {'candidates': [...]}")
    print(json.dumps(register_candidates(dry_run=args.dry_run, candidate_specs=specs), ensure_ascii=False, indent=2, default=json_default))


if __name__ == "__main__":
    main()
