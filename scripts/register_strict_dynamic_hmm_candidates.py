#!/usr/bin/env python3
"""Register strict no-leak dynamic HMM candidates for QE default backtests.

The registered snapshots are intended to be selectable by QE.  They keep the
existing baseline HMM and replace only earlier strict-default candidates with
the same display names.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras


KEEP_BASELINE_DISPLAY = "HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore"
TRAIN_START = "2021-01-04"
TRAIN_END = "2024-02-29"
VAL_START = "2024-03-01"
VAL_END = "2024-05-30"
TEST_START = "2024-07-01"
QE_TEST_END = "2026-04-28"
BACKTEST_END = "2026-04-27"
MAX_FORWARD_HORIZON = 20
DATE_FOLDER = "2026-04-29"
RUNTIME_PRESET = "preset_A"
OUTPUT_ROOT_REL = Path(".codex_tmp") / "hmm_dynamic_strict_default_20260429"


@dataclass(frozen=True)
class CandidateSpec:
    variant_name: str
    display_name: str
    role: str
    confidence_scale: float


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def wsl_to_windows_path(path: Path | str) -> str:
    text = str(path).replace("\\", "/")
    if text.startswith("/mnt/") and len(text) > 6 and text[6] == "/":
        return f"{text[5].upper()}:{text[6:]}".replace("/", "\\")
    return str(path)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def connect_db(args: argparse.Namespace):
    return psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password or os.getenv("TDX_DB_PASSWORD", ""),
    )


def candidate_specs() -> list[CandidateSpec]:
    return [
        CandidateSpec(
            variant_name="strict_default_pup_w20_50_clip_0p9800_1p0150_conf_0p075",
            display_name="HMM_DYNAMIC_PUP_w20_50_conf_0p075_STRICT_DEFAULT__n3_diag",
            role="primary_strict_default_candidate",
            confidence_scale=0.075,
        ),
        CandidateSpec(
            variant_name="strict_default_pup_w20_50_clip_0p9800_1p0150_conf_0p10",
            display_name="HMM_DYNAMIC_PUP_w20_50_conf_0p10_STRICT_DEFAULT__n3_diag",
            role="backup_strict_default_candidate",
            confidence_scale=0.10,
        ),
    ]


def find_source_dir(root: Path, variant_name: str) -> Path:
    base = root / OUTPUT_ROOT_REL / "models"
    coeff_name = f"coefficients_{variant_name}_{TEST_START}_{BACKTEST_END}.json"
    matches = sorted(
        [path for path in base.glob(f"offline_{variant_name}_*") if (path / coeff_name).exists()],
        key=lambda p: p.stat().st_mtime,
    )
    if not matches:
        raise FileNotFoundError(f"missing strict candidate directory for {variant_name}")
    source = matches[-1]
    for filename in ("models.json", "metadata.json", coeff_name):
        if not (source / filename).exists():
            raise FileNotFoundError(f"missing {filename} under {source}")
    return source


def load_summary_metrics(root: Path) -> dict[str, dict[str, Any]]:
    path = root / OUTPUT_ROOT_REL / "summary.csv"
    if not path.exists():
        return {}
    metrics: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            name = row.get("name") or ""
            if name == "NO_HMM_BASELINE":
                key = name
            else:
                key = name
            clean: dict[str, Any] = {}
            for col, value in row.items():
                if value is None or value == "":
                    clean[col] = None
                    continue
                try:
                    clean[col] = float(value)
                except ValueError:
                    clean[col] = value
            metrics[key] = clean
    return metrics


def verify_embargo(conn) -> dict[str, Any]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT trade_date
        FROM market.index_daily
        WHERE ts_code = '000300.SH' AND trade_date > %s
        ORDER BY trade_date
        LIMIT %s
        """,
        (VAL_END, MAX_FORWARD_HORIZON),
    )
    forward_dates = [row["trade_date"] for row in cur.fetchall()]
    cur.close()
    if len(forward_dates) < MAX_FORWARD_HORIZON:
        raise RuntimeError("not enough trading days after validation end to verify embargo")
    validation_label_end = forward_dates[-1]
    if not (validation_label_end < date.fromisoformat(TEST_START)):
        raise RuntimeError(
            f"strict embargo failed: validation_label_end={validation_label_end} "
            f"test_start={TEST_START}"
        )
    return {
        "max_forward_horizon": MAX_FORWARD_HORIZON,
        "validation_label_end": validation_label_end.isoformat(),
        "test_start": TEST_START,
        "status": "passed",
    }


def fetch_stock_sector_map_as_of(conn) -> dict[str, str]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT ts_code, l2_code
        FROM market.sw_index_member
        WHERE in_date <= %s AND (out_date IS NULL OR out_date >= %s)
        """,
        (TEST_START, TEST_START),
    )
    mapping = {
        str(row["ts_code"]): str(row["l2_code"])
        for row in cur.fetchall()
        if row["ts_code"] and row["l2_code"]
    }
    cur.close()
    if not mapping:
        raise RuntimeError("stock_sector_map is empty; refusing to register strict HMM candidates")
    return mapping


def safe_config_asset_dir(models_root: Path, config_id: str) -> Path:
    target = (models_root / config_id).resolve()
    root = models_root.resolve()
    if target == root or root not in target.parents:
        raise RuntimeError(f"unsafe model asset delete target: {target}")
    return target


def prepare_candidate_assets(
    *,
    root: Path,
    spec: CandidateSpec,
    source_dir: Path,
    config_id: str,
    stock_sector_map: dict[str, str],
    embargo: dict[str, Any],
    summary_metrics: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    models_root = root / "backend" / "data" / "hmm_models"
    dest_dir = models_root / config_id / DATE_FOLDER
    dest_dir.mkdir(parents=True, exist_ok=False)

    source_model = source_dir / "models.json"
    source_meta = source_dir / "metadata.json"
    source_coeff = source_dir / f"coefficients_{spec.variant_name}_{TEST_START}_{BACKTEST_END}.json"
    dest_model = dest_dir / "models.json"
    dest_meta = dest_dir / "metadata.json"
    dest_coeff = dest_dir / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
    dest_training = dest_dir / "training_result.json"

    shutil.copy2(source_model, dest_model)
    metadata = read_json(source_meta)
    coeff_payload = read_json(source_coeff)
    model_path_win = wsl_to_windows_path(dest_model)
    coeff_path_win = wsl_to_windows_path(dest_coeff)
    sector_count = len(read_json(dest_model))

    coeff_payload.update(
        {
            "version": "dynamic_pup_strict_default_v1",
            "model_path": model_path_win,
            "model_path_wsl": str(dest_model).replace("\\", "/"),
            "preset_key": RUNTIME_PRESET,
            "runtime_preset_alias": RUNTIME_PRESET,
            "original_variant_name": spec.variant_name,
            "db_display_name": spec.display_name,
            "test_start": TEST_START,
            "qe_test_end": QE_TEST_END,
            "backtest_end": BACKTEST_END,
            "sector_count": sector_count,
            "stock_sector_map": stock_sector_map,
            "stock_sector_map_policy": "static_as_of_qe_test_start_no_future",
            "stock_sector_map_as_of": TEST_START,
            "strict_no_leakage": True,
            "embargo_check": embargo,
            "registered_for_qe": True,
        }
    )
    write_json(dest_coeff, coeff_payload)

    metadata.update(
        {
            "db_registered": True,
            "db_display_name": spec.display_name,
            "runtime_preset": RUNTIME_PRESET,
            "model_path": model_path_win,
            "model_path_wsl": str(dest_model).replace("\\", "/"),
            "coefficients_path": coeff_path_win,
            "coefficients_path_wsl": str(dest_coeff).replace("\\", "/"),
            "strict_no_leakage": True,
            "embargo_check": embargo,
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    write_json(dest_meta, metadata)

    validation = summary_metrics.get(spec.variant_name, {})
    training_result = {
        "display_name": spec.display_name,
        "variant_name": spec.variant_name,
        "role": spec.role,
        "source_dir": str(source_dir),
        "model_path": model_path_win,
        "coefficients_path": coeff_path_win,
        "runtime_preset": RUNTIME_PRESET,
        "train_period": f"{TRAIN_START} ~ {TRAIN_END}",
        "val_period": f"{VAL_START} ~ {VAL_END}",
        "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
        "qe_test_end": QE_TEST_END,
        "strict_no_leakage": True,
        "embargo_check": embargo,
        "qlib_script_validation": validation,
    }
    write_json(dest_training, training_result)

    config_json = {
        "version": "dynamic_pup_strict_default_v1",
        "version_role": spec.role,
        "ui_label": spec.display_name,
        "description": (
            "Strict no-leak dynamic HMM PUP candidate for QE default backtest. "
            "Validation 20D forward labels end before test_start; no fixed trending boost."
        ),
        "script": "scripts/hmm_dynamic_strict_default_train.py",
        "registered_by": "scripts/register_strict_dynamic_hmm_candidates.py",
        "train_start": TRAIN_START,
        "train_end": TRAIN_END,
        "val_start": VAL_START,
        "val_end": VAL_END,
        "validation_label_end": embargo["validation_label_end"],
        "validation_forward_label_max_horizon": MAX_FORWARD_HORIZON,
        "test_start": TEST_START,
        "qe_test_end": QE_TEST_END,
        "coefficient_start": TEST_START,
        "coefficient_end": BACKTEST_END,
        "qe_default_supported": True,
        "strict_no_leakage": True,
        "embargo_check": embargo,
        "n_states": 3,
        "covariance_type": "diag",
        "method": "pup",
        "horizon_weights": {"5": 0.20, "10": 0.30, "20": 0.50},
        "coefficient_lambda": 0.06,
        "coefficient_bounds": [0.98, 1.015],
        "confidence_scale": spec.confidence_scale,
        "random_state": 44,
        "precomputed_only": True,
        "runtime_generation_supported": False,
        "runtime_preset": RUNTIME_PRESET,
        "coefficient_windows": [
            {
                "preset": RUNTIME_PRESET,
                "test_start": TEST_START,
                "backtest_end": BACKTEST_END,
                "role": "qe_default_window",
                "strict_no_leakage": True,
            }
        ],
        "stock_sector_map_policy": "static_as_of_qe_test_start_no_future",
        "stock_sector_map_as_of": TEST_START,
        "signal_presets": {
            RUNTIME_PRESET: {
                "label": f"Strict dynamic PUP conf={spec.confidence_scale:g}",
                "description": (
                    "Strict default-window dynamic daily sector coefficients. "
                    "Use QE default split test_start=2024-07-01, test_end=2026-04-28 "
                    "so backtest_end=2026-04-27."
                ),
                "coefficients": {"1": {"fading": 1.0, "neutral": 1.0, "trending": 1.0}},
            }
        },
    }
    metrics_json = {
        "version": "dynamic_pup_strict_default_v1",
        "display_name": spec.display_name,
        "variant_name": spec.variant_name,
        "role": spec.role,
        "train_period": f"{TRAIN_START} ~ {TRAIN_END}",
        "val_period": f"{VAL_START} ~ {VAL_END}",
        "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
        "strict_no_leakage": True,
        "embargo_check": embargo,
        "sector_count": sector_count,
        "runtime_preset": RUNTIME_PRESET,
        "qlib_script_validation": validation,
    }
    return {
        "config_json": config_json,
        "metrics_json": metrics_json,
        "model_path": model_path_win,
        "sector_count": sector_count,
        "asset_dir": str(dest_dir),
        "coefficients_path": str(dest_coeff),
    }


def register(args: argparse.Namespace) -> dict[str, Any]:
    root = repo_root()
    models_root = root / "backend" / "data" / "hmm_models"
    specs = candidate_specs()
    display_names = [spec.display_name for spec in specs]
    summary_metrics = load_summary_metrics(root)

    conn = connect_db(args)
    conn.autocommit = False
    embargo = verify_embargo(conn)
    stock_sector_map = fetch_stock_sector_map_as_of(conn)

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT c.config_id, c.display_name, s.snapshot_id
        FROM model_train_configs c
        LEFT JOIN model_train_snapshots s ON s.config_id = c.config_id
        WHERE c.model_type = 'sector_hmm'
        ORDER BY c.display_name
        """
    )
    existing = cur.fetchall()
    if not any(row["display_name"] == KEEP_BASELINE_DISPLAY for row in existing):
        raise RuntimeError(f"baseline to keep not found: {KEEP_BASELINE_DISPLAY}")
    delete_config_ids = sorted(
        {str(row["config_id"]) for row in existing if row["display_name"] in display_names}
    )
    delete_snapshot_ids = sorted(
        {str(row["snapshot_id"]) for row in existing if row["display_name"] in display_names and row["snapshot_id"]}
    )
    delete_asset_dirs = [safe_config_asset_dir(models_root, config_id) for config_id in delete_config_ids]

    prepared: list[dict[str, Any]] = []
    for spec in specs:
        source_dir = find_source_dir(root, spec.variant_name)
        config_id = str(uuid.uuid4())
        snapshot_id = str(uuid.uuid4())
        job_id = str(uuid.uuid4())
        assets = prepare_candidate_assets(
            root=root,
            spec=spec,
            source_dir=source_dir,
            config_id=config_id,
            stock_sector_map=stock_sector_map,
            embargo=embargo,
            summary_metrics=summary_metrics,
        )
        prepared.append(
            {
                "spec": spec,
                "source_dir": str(source_dir),
                "config_id": config_id,
                "snapshot_id": snapshot_id,
                "job_id": job_id,
                **assets,
            }
        )

    try:
        if delete_snapshot_ids:
            cur.execute("UPDATE model_train_jobs SET snapshot_id = NULL WHERE snapshot_id = ANY(%s)", (delete_snapshot_ids,))
        if delete_config_ids:
            cur.execute("DELETE FROM model_train_daily_coefficient_jobs WHERE config_id = ANY(%s)", (delete_config_ids,))
            cur.execute("DELETE FROM model_train_jobs WHERE config_id = ANY(%s)", (delete_config_ids,))
            cur.execute("DELETE FROM model_train_snapshots WHERE config_id = ANY(%s)", (delete_config_ids,))
            cur.execute("DELETE FROM model_train_configs WHERE config_id = ANY(%s)", (delete_config_ids,))

        for item in prepared:
            cur.execute(
                """
                INSERT INTO model_train_configs (config_id, model_type, display_name, config_json)
                VALUES (%s, 'sector_hmm', %s, %s)
                """,
                (
                    item["config_id"],
                    item["spec"].display_name,
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
        conn.commit()
    except Exception:
        conn.rollback()
        for item in prepared:
            config_dir = Path(item["asset_dir"]).parent
            if config_dir.exists() and models_root.resolve() in config_dir.resolve().parents:
                shutil.rmtree(config_dir)
        raise
    finally:
        cur.close()
        conn.close()

    deleted_dirs: list[str] = []
    for path in delete_asset_dirs:
        if path.exists():
            shutil.rmtree(path)
            deleted_dirs.append(str(path))

    return {
        "kept_baseline": KEEP_BASELINE_DISPLAY,
        "strict_split": {
            "train_start": TRAIN_START,
            "train_end": TRAIN_END,
            "val_start": VAL_START,
            "val_end": VAL_END,
            "test_start": TEST_START,
            "qe_test_end": QE_TEST_END,
            "backtest_end": BACKTEST_END,
        },
        "embargo_check": embargo,
        "stock_sector_map_policy": "static_as_of_qe_test_start_no_future",
        "stock_sector_map_as_of": TEST_START,
        "stock_sector_map_count": len(stock_sector_map),
        "deleted_config_ids": delete_config_ids,
        "deleted_asset_dirs": deleted_dirs,
        "registered": [
            {
                "display_name": item["spec"].display_name,
                "variant_name": item["spec"].variant_name,
                "config_id": item["config_id"],
                "snapshot_id": item["snapshot_id"],
                "model_path": item["model_path"],
                "coefficients_path": item["coefficients_path"],
            }
            for item in prepared
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Register strict default-window dynamic HMM candidates")
    parser.add_argument("--db-host", default=os.getenv("TDX_DB_HOST", "127.0.0.1"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("TDX_DB_PORT", "5432")))
    parser.add_argument("--db-name", default=os.getenv("TDX_DB_NAME", "aistock"))
    parser.add_argument("--db-user", default=os.getenv("TDX_DB_USER", "postgres"))
    parser.add_argument("--db-password", default=os.getenv("TDX_DB_PASSWORD", ""))
    return parser


def main() -> None:
    result = register(build_parser().parse_args())
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
