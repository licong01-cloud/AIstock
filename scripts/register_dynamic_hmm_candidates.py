#!/usr/bin/env python3
"""Register selected offline dynamic HMM candidates as DB snapshots.

This script is intentionally narrow:
- keep exactly one existing DB baseline HMM;
- remove the other existing sector_hmm DB configs and their model asset dirs;
- register two pre-trained offline dynamic candidates as new sector_hmm configs;
- do not start QE experiments.
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
TEST_START = "2025-03-11"
TEST_END = "2026-03-03"
DATE_FOLDER = "2026-04-29"
RUNTIME_PRESET = "preset_A"


@dataclass(frozen=True)
class CandidateSpec:
    variant_name: str
    display_name: str
    source_dir: Path
    role: str
    confidence_scale: float


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def windows_to_wsl_path(path: str) -> str:
    text = str(path).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    return text


def wsl_to_windows_path(path: Path) -> str:
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


def fetch_stock_sector_map(conn) -> dict[str, str]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT ts_code, l2_code
        FROM market.sw_index_member
        WHERE in_date <= %s AND (out_date IS NULL OR out_date >= %s)
        """,
        (TEST_END, TEST_START),
    )
    mapping = {str(row["ts_code"]): str(row["l2_code"]) for row in cur.fetchall() if row["ts_code"] and row["l2_code"]}
    cur.close()
    if not mapping:
        raise RuntimeError("stock_sector_map is empty; refusing to register HMM candidates")
    return mapping


def load_summary_metrics(root: Path) -> dict[str, dict[str, Any]]:
    path = root / ".codex_tmp" / "hmm_db_vs_dynamic_1y_20260429" / "summary.csv"
    if not path.exists():
        return {}
    metrics: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            name = row.get("name") or ""
            if name.startswith("OFFLINE_DYNAMIC::"):
                variant = name.split("::", 1)[1]
                clean: dict[str, Any] = {}
                for key, value in row.items():
                    if value is None or value == "":
                        clean[key] = None
                        continue
                    try:
                        clean[key] = float(value)
                    except ValueError:
                        clean[key] = value
                metrics[variant] = clean
    return metrics


def find_source_dir(root: Path, variant_name: str) -> Path:
    base = root / ".codex_tmp" / "hmm_dynamic_tuning_pass8_20260429" / "models"
    coeff_name = f"coefficients_{variant_name}_{TEST_START}_{TEST_END}.json"
    matches = sorted(
        [path for path in base.glob(f"offline_{variant_name}_*") if (path / coeff_name).exists()],
        key=lambda p: p.stat().st_mtime,
    )
    if not matches:
        raise FileNotFoundError(f"missing offline candidate directory for {variant_name}")
    source = matches[-1]
    for filename in ("models.json", "metadata.json", coeff_name):
        if not (source / filename).exists():
            raise FileNotFoundError(f"missing {filename} under {source}")
    return source


def make_candidates(root: Path) -> list[CandidateSpec]:
    specs = [
        (
            "p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075",
            "HMM_DYNAMIC_PUP_w20_50_conf_0p075_PIT1Y__n3_diag",
            "primary_candidate",
            0.075,
        ),
        (
            "p8_pup_w20_50_clip_0p9800_1p0150_conf_0p10",
            "HMM_DYNAMIC_PUP_w20_50_conf_0p10_PIT1Y__n3_diag",
            "robust_backup_candidate",
            0.10,
        ),
    ]
    return [
        CandidateSpec(
            variant_name=variant,
            display_name=display,
            source_dir=find_source_dir(root, variant),
            role=role,
            confidence_scale=conf,
        )
        for variant, display, role, conf in specs
    ]


def safe_config_asset_dir(models_root: Path, config_id: str) -> Path:
    target = (models_root / config_id).resolve()
    root = models_root.resolve()
    if target == root or root not in target.parents:
        raise RuntimeError(f"unsafe model asset delete target: {target}")
    if target.name != config_id:
        raise RuntimeError(f"unsafe model asset delete target name mismatch: {target}")
    return target


def prepare_candidate_assets(
    *,
    root: Path,
    spec: CandidateSpec,
    config_id: str,
    stock_sector_map: dict[str, str],
    summary_metrics: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    models_root = root / "backend" / "data" / "hmm_models"
    dest_dir = models_root / config_id / DATE_FOLDER
    dest_dir.mkdir(parents=True, exist_ok=False)

    source_model = spec.source_dir / "models.json"
    source_meta = spec.source_dir / "metadata.json"
    source_coeff = spec.source_dir / f"coefficients_{spec.variant_name}_{TEST_START}_{TEST_END}.json"
    dest_model = dest_dir / "models.json"
    dest_meta = dest_dir / "metadata.json"
    dest_coeff = dest_dir / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{TEST_END}.json"
    dest_training = dest_dir / "training_result.json"

    shutil.copy2(source_model, dest_model)
    metadata = read_json(source_meta)
    coeff_payload = read_json(source_coeff)
    model_path_win = wsl_to_windows_path(dest_model)

    coeff_payload.update(
        {
            "model_path": model_path_win,
            "model_path_wsl": str(dest_model),
            "preset_key": RUNTIME_PRESET,
            "runtime_preset_alias": RUNTIME_PRESET,
            "original_variant_name": spec.variant_name,
            "db_display_name": spec.display_name,
            "test_start": TEST_START,
            "backtest_end": TEST_END,
            "sector_count": len(coeff_payload.get("daily_coefficients", {}))
            and len(next(iter(coeff_payload.get("daily_coefficients", {}).values()))),
            "stock_sector_map": stock_sector_map,
            "registered_for_qe": True,
            "registration_note": "Dynamic PUP coefficients are precomputed; QE date window must match an available coefficient artifact.",
        }
    )
    write_json(dest_coeff, coeff_payload)

    metadata.update(
        {
            "db_registered": True,
            "db_display_name": spec.display_name,
            "runtime_preset": RUNTIME_PRESET,
            "model_path": model_path_win,
            "model_path_wsl": str(dest_model),
            "coefficients_path": wsl_to_windows_path(dest_coeff),
            "coefficients_path_wsl": str(dest_coeff),
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    write_json(dest_meta, metadata)

    validation = summary_metrics.get(spec.variant_name, {})
    training_result = {
        "display_name": spec.display_name,
        "variant_name": spec.variant_name,
        "role": spec.role,
        "source_dir": str(spec.source_dir),
        "model_path": model_path_win,
        "coefficients_path": wsl_to_windows_path(dest_coeff),
        "runtime_preset": RUNTIME_PRESET,
        "train_period": f"{metadata.get('train_start')} ~ {metadata.get('train_end')}",
        "val_period": f"{metadata.get('val_start')} ~ {metadata.get('val_end')}",
        "coefficient_period": f"{TEST_START} ~ {TEST_END}",
        "pit_status": "PIT-compatible for 2025-03-11 ~ 2026-03-03 script validation",
        "qlib_1y_script_validation": validation,
    }
    write_json(dest_training, training_result)

    config_json = {
        "version": "dynamic_pup_pit1y_v1",
        "version_role": spec.role,
        "ui_label": spec.display_name,
        "description": (
            "Offline dynamic HMM PUP candidate. Uses precomputed sector coefficients calibrated "
            "to 5D/10D/20D validation outcomes; no fixed trending boost."
        ),
        "script": "scripts/hmm_dynamic_tuning_pass8_experiments.py",
        "registered_by": "scripts/register_dynamic_hmm_candidates.py",
        "train_start": metadata.get("train_start"),
        "train_end": metadata.get("train_end"),
        "val_start": metadata.get("val_start"),
        "val_end": metadata.get("val_end"),
        "coefficient_start": TEST_START,
        "coefficient_end": TEST_END,
        "n_states": 3,
        "covariance_type": "diag",
        "method": "pup",
        "horizon_weights": {"5": 0.20, "10": 0.30, "20": 0.50},
        "coefficient_lambda": 0.06,
        "coefficient_bounds": [0.98, 1.015],
        "confidence_scale": spec.confidence_scale,
        "random_state": 44,
        "precomputed_only": True,
        "runtime_preset": RUNTIME_PRESET,
        "signal_presets": {
            RUNTIME_PRESET: {
                "label": f"动态PUP conf={spec.confidence_scale:g}",
                "description": (
                    "动态日度行业系数已预计算在 coefficients_preset_A_2025-03-11_2026-03-03.json；"
                    "如QE窗口不同，需要先生成对应窗口系数。"
                ),
                "coefficients": {
                    "1": {"fading": 1.0, "neutral": 1.0, "trending": 1.0}
                },
            }
        },
    }
    metrics_json = {
        "version": "dynamic_pup_pit1y_v1",
        "display_name": spec.display_name,
        "variant_name": spec.variant_name,
        "role": spec.role,
        "train_period": f"{metadata.get('train_start')} ~ {metadata.get('train_end')}",
        "val_period": f"{metadata.get('val_start')} ~ {metadata.get('val_end')}",
        "coefficient_period": f"{TEST_START} ~ {TEST_END}",
        "pit_status": "PIT-compatible",
        "sector_count": len(read_json(dest_model)),
        "runtime_preset": RUNTIME_PRESET,
        "qlib_1y_script_validation": validation,
    }
    return {
        "config_json": config_json,
        "metrics_json": metrics_json,
        "model_path": model_path_win,
        "sector_count": len(read_json(dest_model)),
        "asset_dir": str(dest_dir),
        "coefficients_path": str(dest_coeff),
    }


def register_and_cleanup(args: argparse.Namespace) -> dict[str, Any]:
    root = repo_root()
    models_root = root / "backend" / "data" / "hmm_models"
    candidates = make_candidates(root)
    summary_metrics = load_summary_metrics(root)
    target_displays = {spec.display_name for spec in candidates}

    conn = connect_db(args)
    conn.autocommit = False
    stock_sector_map = fetch_stock_sector_map(conn)

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT c.config_id, c.display_name, s.snapshot_id, s.model_path
        FROM model_train_configs c
        LEFT JOIN model_train_snapshots s ON s.config_id = c.config_id
        WHERE c.model_type = 'sector_hmm'
        ORDER BY c.display_name, s.trained_at
        """
    )
    existing = cur.fetchall()
    keep_rows = [row for row in existing if row["display_name"] == KEEP_BASELINE_DISPLAY]
    if not keep_rows:
        raise RuntimeError(f"baseline to keep not found: {KEEP_BASELINE_DISPLAY}")

    delete_config_ids = sorted({str(row["config_id"]) for row in existing if row["display_name"] != KEEP_BASELINE_DISPLAY})
    delete_asset_dirs = [safe_config_asset_dir(models_root, config_id) for config_id in delete_config_ids]

    prepared: list[dict[str, Any]] = []
    for spec in candidates:
        config_id = str(uuid.uuid4())
        snapshot_id = str(uuid.uuid4())
        job_id = str(uuid.uuid4())
        assets = prepare_candidate_assets(
            root=root,
            spec=spec,
            config_id=config_id,
            stock_sector_map=stock_sector_map,
            summary_metrics=summary_metrics,
        )
        prepared.append(
            {
                "spec": spec,
                "config_id": config_id,
                "snapshot_id": snapshot_id,
                "job_id": job_id,
                **assets,
            }
        )

    try:
        if delete_config_ids:
            cur.execute(
                "DELETE FROM model_train_daily_coefficient_jobs WHERE config_id = ANY(%s)",
                (delete_config_ids,),
            )
            cur.execute("DELETE FROM model_train_jobs WHERE config_id = ANY(%s)", (delete_config_ids,))
            cur.execute("DELETE FROM model_train_snapshots WHERE config_id = ANY(%s)", (delete_config_ids,))
            cur.execute("DELETE FROM model_train_configs WHERE config_id = ANY(%s)", (delete_config_ids,))

        for item in prepared:
            spec = item["spec"]
            if spec.display_name in target_displays:
                cur.execute(
                    """
                    INSERT INTO model_train_configs (config_id, model_type, display_name, config_json)
                    VALUES (%s, 'sector_hmm', %s, %s)
                    """,
                    (
                        item["config_id"],
                        spec.display_name,
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
            config_dir_resolved = config_dir.resolve()
            models_root_resolved = models_root.resolve()
            if (
                config_dir.exists()
                and config_dir_resolved != models_root_resolved
                and models_root_resolved in config_dir_resolved.parents
            ):
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
    parser = argparse.ArgumentParser(description="Register two dynamic HMM candidates and keep one DB baseline")
    parser.add_argument("--db-host", default=os.getenv("TDX_DB_HOST", "127.0.0.1"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("TDX_DB_PORT", "5432")))
    parser.add_argument("--db-name", default=os.getenv("TDX_DB_NAME", "aistock"))
    parser.add_argument("--db-user", default=os.getenv("TDX_DB_USER", "postgres"))
    parser.add_argument("--db-password", default=os.getenv("TDX_DB_PASSWORD", ""))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = register_and_cleanup(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
