#!/usr/bin/env python3
"""Register the best bounded regime-HMM candidate as a hidden QE snapshot.

This script packages the selected offline regime-HMM candidate into a
precomputed-only snapshot so QE can reference it by snapshot_id without adding
it to the default public HMM selector.

It does not retrain any model. It copies the offline candidate coefficient map
and writes a synthetic ``models.json`` wrapper that preserves the source HMM
metadata while making the snapshot visible to the QE runtime and UI.
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

MODEL_TYPE = "sector_hmm_experimental_regime_bounded_20260510"
TARGET_DATE_FOLDER = "2026-05-10"
RUNTIME_PRESET = "preset_A"
TEST_START = "2024-07-01"
TEST_END = "2026-04-28"
BACKTEST_END = "2026-04-27"

SOURCE_SCREEN_DIR = ROOT / ".codex_tmp" / "hmm_regime_bounded_candidate_screen_20260509_run2"
SOURCE_OFFLINE_DIR = ROOT / ".codex_tmp" / "hmm_sector_rotation_redefine_20260509_oriented_full"
SOURCE_MODEL_META = SOURCE_OFFLINE_DIR / "model_meta.json"
SOURCE_ANALYSIS_REPORT = ROOT / "docs" / "analysis" / "hmm_regime_bounded_screen_20260509.md"
SOURCE_CANDIDATE_COEFF = (
    SOURCE_SCREEN_DIR
    / "candidate_coefficients"
    / "REGHMM_REGIMELINEAR_BOTH_T20_B15_BOOST0p01_PEN0p005.json"
)

DISPLAY_NAME = "HMM_EXP_REGHMM_REGIMELINEAR_BOTH_T20_B15_BOOST0p01_PEN0p005__qe20260510"
VARIANT_NAME = "regime_linear_both_t20_b15_boost0p01_pen0p005"
SOURCE_MODEL_KEY = "ROT_REGIME_LINEAR_v1__INV"
SELECTION_REASONS = ["recent_holdout", "robust_full"]
SCREEN_METRICS = {
    "holdout_10d": 0.013528,
    "full_10d": 0.011337,
    "train_pre_10d": 0.002847,
    "avg_entered_per_day": 0.615385,
    "changed_days": 82,
}


def json_default(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
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
    candidate_paths = [ROOT / ".env"]
    production_env = ROOT.parent.parent / "AIstock" / ".env"
    if production_env not in candidate_paths:
        candidate_paths.append(production_env)
    for env_path in candidate_paths:
        if not env_path.exists():
            continue
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
        return


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
        application_name="AIstock-HMM-regime-bounded-registry-20260510",
        options="-c client_encoding=utf8",
    )


def safe_config_dir(config_id: str) -> Path:
    target = (MODELS_ROOT / config_id).resolve()
    root = MODELS_ROOT.resolve()
    if target == root or root not in target.parents or target.name != config_id:
        raise RuntimeError(f"unsafe generated HMM config dir: {target}")
    return target


def backup_current_registry(cur: psycopg2.extensions.cursor) -> Path:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    path = TMP_ROOT / f"hmm_regime_bounded_registry_before_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
            "operation": "register bounded regime-HMM QE candidate",
            "configs": configs,
            "snapshots": snapshots,
        },
    )
    return path


def validate_candidate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    daily = payload.get("daily_coefficients")
    stock_map = payload.get("stock_sector_map")
    source_metadata = payload.get("source_metadata")
    if not isinstance(daily, dict) or not daily:
        raise RuntimeError(f"daily_coefficients missing in {SOURCE_CANDIDATE_COEFF}")
    if not isinstance(stock_map, dict) or not stock_map:
        raise RuntimeError(f"stock_sector_map missing in {SOURCE_CANDIDATE_COEFF}")
    if not isinstance(source_metadata, dict):
        raise RuntimeError(f"source_metadata missing in {SOURCE_CANDIDATE_COEFF}")
    if payload.get("candidate") != "REGHMM_REGIMELINEAR_BOTH_T20_B15_BOOST0p01_PEN0p005":
        raise RuntimeError(f"unexpected candidate name in {SOURCE_CANDIDATE_COEFF}")
    if source_metadata.get("source") != SOURCE_MODEL_KEY:
        raise RuntimeError(f"unexpected source model key in {SOURCE_CANDIDATE_COEFF}")
    if not daily.get(TEST_START) or not daily.get(BACKTEST_END):
        raise RuntimeError(f"source coefficients do not cover {TEST_START}..{BACKTEST_END}")
    return {
        "daily": daily,
        "stock_sector_map": stock_map,
        "source_metadata": source_metadata,
        "sector_count": len(daily[TEST_START]),
        "date_count": len([day for day in daily if TEST_START <= str(day) <= BACKTEST_END]),
    }


def build_candidate_assets(
    *,
    source_model_meta: dict[str, Any],
    source_candidate_payload: dict[str, Any],
    source_stats: dict[str, Any],
) -> dict[str, Any]:
    config_id = str(uuid.uuid4())
    snapshot_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    dest_dir = MODELS_ROOT / config_id / TARGET_DATE_FOLDER
    dest_dir.mkdir(parents=True, exist_ok=False)
    dest_model = dest_dir / "models.json"
    dest_coeff = dest_dir / f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
    dest_meta = dest_dir / "metadata.json"
    dest_training = dest_dir / "training_result.json"

    source_model_entry = source_model_meta.get(SOURCE_MODEL_KEY)
    if not isinstance(source_model_entry, dict):
        raise RuntimeError(f"source model key missing in {SOURCE_MODEL_META}: {SOURCE_MODEL_KEY}")

    # Keep the original model_meta structure, but add a registry wrapper so the
    # snapshot can be traced back to the bounded screen result.
    model_payload = deepcopy(source_model_meta)
    model_payload["registry"] = {
        "version": "sector_hmm_experimental_regime_bounded_20260510",
        "model_type": MODEL_TYPE,
        "display_name": DISPLAY_NAME,
        "candidate_name": source_candidate_payload["candidate"],
        "candidate_variant": VARIANT_NAME,
        "source_model_key": SOURCE_MODEL_KEY,
        "source_offline_dir": str(SOURCE_OFFLINE_DIR.resolve()),
        "source_screen_dir": str(SOURCE_SCREEN_DIR.resolve()),
        "source_analysis_report": str(SOURCE_ANALYSIS_REPORT.resolve()),
        "source_candidate_coefficients_path": str(SOURCE_CANDIDATE_COEFF.resolve()),
        "selection_status": source_candidate_payload.get("selection_status"),
        "selection_reasons": list(source_candidate_payload.get("selection_reasons") or []),
        "screen_metrics": SCREEN_METRICS,
        "registered_at": now,
        "note": "Synthetic registry wrapper for QE selection; no additional retraining was run.",
    }
    write_json(dest_model, model_payload)

    coeff_payload = deepcopy(source_candidate_payload)
    coeff_payload.update(
        {
            "generated_by": "scripts/register_hmm_regime_bounded_qe_candidate_20260510.py",
            "registered_for_qe": True,
            "selection_status": "registered_hidden__needs_qe_backtest_only",
            "selection_reasons": list(SELECTION_REASONS),
            "preset_key": RUNTIME_PRESET,
            "runtime_preset_alias": RUNTIME_PRESET,
            "test_start": TEST_START,
            "test_end": TEST_END,
            "backtest_end": BACKTEST_END,
            "model_path": str(dest_model.resolve()),
            "model_path_wsl": windows_to_wsl(dest_model),
            "coefficients_path": str(dest_coeff.resolve()),
            "coefficients_path_wsl": windows_to_wsl(dest_coeff),
            "source_offline_dir": str(SOURCE_OFFLINE_DIR.resolve()),
            "source_screen_dir": str(SOURCE_SCREEN_DIR.resolve()),
            "source_model_meta_path": str(SOURCE_MODEL_META.resolve()),
            "source_analysis_report": str(SOURCE_ANALYSIS_REPORT.resolve()),
            "source_model_key": SOURCE_MODEL_KEY,
            "screen_metrics": SCREEN_METRICS,
            "registered_at": now,
        }
    )
    source_metadata = dict(coeff_payload.get("source_metadata") or {})
    source_metadata.update(
        {
            "model_type": MODEL_TYPE,
            "display_name": DISPLAY_NAME,
            "candidate_variant": VARIANT_NAME,
            "screen_metrics": SCREEN_METRICS,
            "source_model_key": SOURCE_MODEL_KEY,
            "source_screen_dir": str(SOURCE_SCREEN_DIR.resolve()),
            "source_analysis_report": str(SOURCE_ANALYSIS_REPORT.resolve()),
        }
    )
    coeff_payload["source_metadata"] = source_metadata
    write_json(dest_coeff, coeff_payload)

    model_sha = file_sha256(dest_model)
    coeff_sha = file_sha256(dest_coeff)

    model_context = {
        key: source_model_entry.get("candidate", {}).get(key)
        for key in (
            "scope",
            "features",
            "daily_features",
            "target",
            "n_states",
            "min_self_trans",
            "alpha_smooth",
            "covariance_type",
            "preprocess",
            "ridge_alpha",
        )
        if source_model_entry.get("candidate", {}).get(key) is not None
    }
    config_json = {
        "version": "sector_hmm_experimental_regime_bounded_20260510",
        "version_role": "redefined_regime_hmm_bounded_candidate",
        "ui_label": DISPLAY_NAME,
        "description": (
            "Redefined regime-HMM bounded candidate selected from the offline "
            "screen; recent-holdout positive and robust across the full window."
        ),
        "registered_by": "scripts/register_hmm_regime_bounded_qe_candidate_20260510.py",
        "registered_at": now,
        "source_offline_dir": str(SOURCE_OFFLINE_DIR.resolve()),
        "source_screen_dir": str(SOURCE_SCREEN_DIR.resolve()),
        "source_analysis_report": str(SOURCE_ANALYSIS_REPORT.resolve()),
        "source_model_meta_path": str(SOURCE_MODEL_META.resolve()),
        "source_candidate_coefficients_path": str(SOURCE_CANDIDATE_COEFF.resolve()),
        "source_model_key": SOURCE_MODEL_KEY,
        "source_selection_status": source_candidate_payload.get("selection_status"),
        "source_selection_reasons": list(SELECTION_REASONS),
        "candidate_name": source_candidate_payload.get("candidate"),
        "candidate_variant": VARIANT_NAME,
        "screen_metrics": SCREEN_METRICS,
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
                "role": "qe_default_window_20260510_regime_bounded",
                "preset": RUNTIME_PRESET,
                "test_start": TEST_START,
                "backtest_end": BACKTEST_END,
                "strict_no_leakage": True,
            }
        ],
        "signal_presets": {
            RUNTIME_PRESET: {
                "label": "regime-bounded preset_A only",
                "description": "Precomputed regime-HMM daily coefficients for the QE default split only.",
                "coefficients": {"precomputed_daily": True},
            }
        },
        "inherited_hmm_training_context": model_context,
        "artifact_sha256": {"models_json": model_sha, "coefficients_json": coeff_sha},
    }
    metrics_json = {
        "version": "sector_hmm_experimental_regime_bounded_20260510",
        "display_name": DISPLAY_NAME,
        "snapshot_display_name": DISPLAY_NAME + "__default_window",
        "variant_name": VARIANT_NAME,
        "source_model_key": SOURCE_MODEL_KEY,
        "source_selection_status": source_candidate_payload.get("selection_status"),
        "source_selection_reasons": list(SELECTION_REASONS),
        "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
        "test_end": TEST_END,
        "strict_no_leakage": True,
        "precomputed_only": True,
        "runtime_preset": RUNTIME_PRESET,
        "sector_count": source_stats["sector_count"],
        "date_count": source_stats["date_count"],
        "stock_sector_map_count": len(source_stats["stock_sector_map"]),
        "screen_metrics": SCREEN_METRICS,
        "validation_status": "registered_hidden__needs_qe_backtest_only",
        "hypothesis": (
            "The balanced regime-linear bounded map should keep the recent-holdout "
            "edge while staying stable over the earlier full window."
        ),
    }
    metadata = {
        "db_registered": True,
        "display_name": DISPLAY_NAME,
        "variant_name": VARIANT_NAME,
        "model_type": MODEL_TYPE,
        "model_path": str(dest_model.resolve()),
        "model_path_wsl": windows_to_wsl(dest_model),
        "coefficients_path": str(dest_coeff.resolve()),
        "coefficients_path_wsl": windows_to_wsl(dest_coeff),
        "source_model_meta_path": str(SOURCE_MODEL_META.resolve()),
        "source_candidate_coefficients_path": str(SOURCE_CANDIDATE_COEFF.resolve()),
        "source_model_key": SOURCE_MODEL_KEY,
        "registered_at": now,
        "strict_no_leakage": True,
        "screen_metrics": SCREEN_METRICS,
        "artifact_sha256": {"models_json": model_sha, "coefficients_json": coeff_sha},
    }
    write_json(dest_meta, metadata)
    write_json(
        dest_training,
        {
            "display_name": DISPLAY_NAME,
            "variant_name": VARIANT_NAME,
            "model_path": str(dest_model.resolve()),
            "coefficients_path": str(dest_coeff.resolve()),
            "runtime_preset": RUNTIME_PRESET,
            "coefficient_period": f"{TEST_START} ~ {BACKTEST_END}",
            "source_model_key": SOURCE_MODEL_KEY,
            "source_selection_reasons": list(SELECTION_REASONS),
            "screen_metrics": SCREEN_METRICS,
            "strict_no_leakage": True,
            "note": "Synthetic registry snapshot for QE testing; no HMM retraining was run here.",
        },
    )

    return {
        "config_id": config_id,
        "snapshot_id": snapshot_id,
        "job_id": job_id,
        "display_name": DISPLAY_NAME,
        "variant_name": VARIANT_NAME,
        "config_json": config_json,
        "metrics_json": metrics_json,
        "model_path": str(dest_model.resolve()),
        "sector_count": source_stats["sector_count"],
        "asset_dir": str(dest_dir.resolve()),
        "coefficients_path": str(dest_coeff.resolve()),
        "artifact_sha256": {"models_json": model_sha, "coefficients_json": coeff_sha},
    }


def register_candidates(dry_run: bool = False) -> dict[str, Any]:
    if not SOURCE_MODEL_META.is_file():
        raise RuntimeError(f"source model metadata missing: {SOURCE_MODEL_META}")
    if not SOURCE_CANDIDATE_COEFF.is_file():
        raise RuntimeError(f"source candidate coefficient artifact missing: {SOURCE_CANDIDATE_COEFF}")
    if not SOURCE_ANALYSIS_REPORT.is_file():
        raise RuntimeError(f"source analysis report missing: {SOURCE_ANALYSIS_REPORT}")

    source_model_meta = read_json(SOURCE_MODEL_META)
    source_candidate_payload = read_json(SOURCE_CANDIDATE_COEFF)
    source_stats = validate_candidate_payload(source_candidate_payload)

    prepared: list[dict[str, Any]] = []
    backup_path: Path | None = None

    conn = db_connect()
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT config_id, model_type, display_name
                FROM model_train_configs
                WHERE display_name = %s
                """,
                (DISPLAY_NAME,),
            )
            existing = [dict(row) for row in cur.fetchall()]
            if existing:
                raise RuntimeError(
                    "candidate display name already exists; aborting to avoid duplicate QE choice: "
                    + json.dumps(existing, ensure_ascii=False, default=json_default)
                )

            backup_path = backup_current_registry(cur)
            prepared.append(
                build_candidate_assets(
                    source_model_meta=source_model_meta,
                    source_candidate_payload=source_candidate_payload,
                    source_stats=source_stats,
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
        "source_offline_dir": str(SOURCE_OFFLINE_DIR.resolve()),
        "source_screen_dir": str(SOURCE_SCREEN_DIR.resolve()),
        "source_model_meta_path": str(SOURCE_MODEL_META.resolve()),
        "source_candidate_coefficients_path": str(SOURCE_CANDIDATE_COEFF.resolve()),
        "source_analysis_report": str(SOURCE_ANALYSIS_REPORT.resolve()),
        "source_model_key": SOURCE_MODEL_KEY,
        "selection_reasons": list(SELECTION_REASONS),
        "screen_metrics": SCREEN_METRICS,
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
    result_path = TMP_ROOT / f"hmm_regime_bounded_registry_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    write_json(result_path, result)
    result["result_path"] = str(result_path.resolve())
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Build assets, then rollback DB and remove generated files.")
    args = parser.parse_args()
    print(json.dumps(register_candidates(dry_run=args.dry_run), ensure_ascii=False, indent=2, default=json_default))


if __name__ == "__main__":
    main()
