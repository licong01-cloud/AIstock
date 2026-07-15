#!/usr/bin/env python3
"""Build and run a real QE-only SW2 sector-rotation model suite."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.services.quantevolver.long_trend_evaluation_contract import canonical_sha256
from backend.services.quantevolver.sector_rotation_model import (
    REAL_SECTOR_MODEL_CLASSIFICATION,
    SectorModelConfig,
    aggregate_factor_to_sector,
    build_sector_base,
    engineer_sector_panel,
    train_sector_model_suite,
)


STATIC_COLUMNS = [
    "datetime",
    "instrument",
    "l2_code_id",
    "sw2_close",
    "sw2_amount",
    "sw2_vol",
    "sw2_mf_net_amt",
    "sw2_mf_net_vol",
    "sw2_total_mv",
    "sw2_pb",
    "sw2_pe",
]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _finite_or_none(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _finite_or_none(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite_or_none(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, Path):
        return str(value)
    if value is pd.NA:
        return None
    return value


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(
            _finite_or_none(value),
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        ),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.stem + ".tmp.parquet")
    frame.to_parquet(temporary, index=False)
    os.replace(temporary, path)


def _file_receipt(path: Path, *, include_sha256: bool = True) -> dict[str, Any]:
    stat = path.stat()
    receipt: dict[str, Any] = {
        "path": str(path),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }
    if include_sha256:
        receipt["sha256"] = _sha256_file(path)
    return receipt


def _assert_same_file_identity(path: Path, receipt: dict[str, Any]) -> None:
    stat = path.stat()
    if int(stat.st_size) != receipt["size"] or int(stat.st_mtime_ns) != receipt["mtime_ns"]:
        raise RuntimeError(f"input artifact changed during sector-model run: {path}")


def _source_sha256() -> str:
    root = Path(__file__).resolve().parents[1]
    sources = (
        Path(__file__).resolve(),
        root / "backend/services/quantevolver/sector_rotation_model.py",
    )
    return canonical_sha256(
        {str(path.relative_to(root)): _sha256_file(path) for path in sources}
    )


def _model_params(model: Any) -> dict[str, Any]:
    params = model.get_params(deep=False)
    return {str(key): _finite_or_none(value) for key, value in params.items()}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--static-factors", required=True)
    parser.add_argument("--factor-cache-root", required=True)
    parser.add_argument("--factor", action="append", required=True)
    parser.add_argument("--horizon", action="append", type=int, required=True)
    parser.add_argument("--seed", action="append", type=int, required=True)
    parser.add_argument(
        "--model-kind",
        action="append",
        choices=("lgbm_regression", "lambdarank"),
        required=True,
    )
    parser.add_argument("--train-start", default="2018-08-01")
    parser.add_argument("--train-end", default="2022-12-31")
    parser.add_argument("--valid-start", default="2023-01-01")
    parser.add_argument("--valid-end", default="2024-06-30")
    parser.add_argument("--test-start", default="2024-07-01")
    parser.add_argument("--test-end", default="2026-06-29")
    parser.add_argument("--top-m", type=int, default=5)
    parser.add_argument("--output-root", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    static_path = Path(args.static_factors).expanduser().resolve()
    factor_root = Path(args.factor_cache_root).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    if not static_path.is_file():
        raise RuntimeError(f"static factor artifact does not exist: {static_path}")
    if not factor_root.is_dir():
        raise RuntimeError(f"factor cache root does not exist: {factor_root}")
    if len(set(args.factor)) != len(args.factor):
        raise ValueError("duplicate --factor arguments are not allowed")
    if len(set(args.horizon)) != len(args.horizon):
        raise ValueError("duplicate --horizon arguments are not allowed")
    if len(set(args.seed)) != len(args.seed):
        raise ValueError("duplicate --seed arguments are not allowed")
    if len(set(args.model_kind)) != len(args.model_kind):
        raise ValueError("duplicate --model-kind arguments are not allowed")

    source_sha = _source_sha256()
    static_receipt = _file_receipt(static_path)
    factor_receipts: dict[str, dict[str, Any]] = {}
    factor_paths: dict[str, Path] = {}
    for factor_name in args.factor:
        factor_path = (factor_root / f"{factor_name}.parquet").resolve()
        if not factor_path.is_file():
            raise RuntimeError(f"factor cache artifact does not exist: {factor_path}")
        factor_paths[factor_name] = factor_path
        factor_receipts[factor_name] = _file_receipt(factor_path)

    print(
        json.dumps(
            {
                "event": "sector_model_static_read_started",
                "path": str(static_path),
                "factor_count": len(args.factor),
                "horizons": args.horizon,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    static_frame = pd.read_parquet(static_path, columns=STATIC_COLUMNS)
    base_result = build_sector_base(static_frame)
    del static_frame
    gc.collect()
    _assert_same_file_identity(static_path, static_receipt)

    factor_aggregates: dict[str, pd.DataFrame] = {}
    factor_audits: dict[str, Any] = {}
    for index, factor_name in enumerate(args.factor, start=1):
        factor_path = factor_paths[factor_name]
        print(
            json.dumps(
                {
                    "event": "sector_model_factor_aggregate_started",
                    "factor": factor_name,
                    "index": index,
                    "total": len(args.factor),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        factor_frame = pd.read_parquet(factor_path)
        aggregate, audit = aggregate_factor_to_sector(
            factor_frame,
            base_result.membership,
            factor_name=factor_name,
        )
        factor_aggregates[factor_name] = aggregate
        factor_audits[factor_name] = audit
        del factor_frame
        gc.collect()
        _assert_same_file_identity(factor_path, factor_receipts[factor_name])
        print(
            json.dumps(
                {
                    "event": "sector_model_factor_aggregate_completed",
                    "factor": factor_name,
                    "matched_rows": audit["matched_rows"],
                    "sector_days": audit["matched_sector_days"],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    common_identity = {
        "source_sha256": source_sha,
        "static_factors": static_receipt,
        "factor_caches": factor_receipts,
        "factors": list(args.factor),
        "seeds": [int(value) for value in args.seed],
        "model_kinds": list(args.model_kind),
        "split": {
            "train": [args.train_start, args.train_end],
            "valid": [args.valid_start, args.valid_end],
            "test": [args.test_start, args.test_end],
        },
        "top_m": int(args.top_m),
    }

    completed: list[dict[str, Any]] = []
    for horizon in args.horizon:
        config = SectorModelConfig(
            horizon=int(horizon),
            train_start=args.train_start,
            train_end=args.train_end,
            valid_start=args.valid_start,
            valid_end=args.valid_end,
            test_start=args.test_start,
            test_end=args.test_end,
            top_m=int(args.top_m),
        )
        evaluation_id = "qesr_" + canonical_sha256(
            {**common_identity, "horizon": int(horizon)}
        )
        destination = output_root / evaluation_id
        summary_path = destination / "summary.json"
        if summary_path.is_file():
            existing = json.loads(summary_path.read_text(encoding="utf-8"))
            if existing.get("evaluation_id") != evaluation_id:
                raise RuntimeError(f"immutable sector-model identity conflict: {summary_path}")
            completed.append(existing)
            continue

        print(
            json.dumps(
                {
                    "event": "sector_model_horizon_started",
                    "evaluation_id": evaluation_id,
                    "horizon": int(horizon),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        panel, feature_columns, panel_audit = engineer_sector_panel(
            base_result.sector_base,
            factor_aggregates,
            horizon=int(horizon),
        )
        result = train_sector_model_suite(
            panel,
            feature_columns,
            config=config,
            seeds=args.seed,
            model_kinds=args.model_kind,
        )

        destination.mkdir(parents=True, exist_ok=True)
        panel_path = destination / "sector_feature_panel.parquet"
        predictions_path = destination / "component_predictions.parquet"
        ensemble_path = destination / "reality_sector_scores.parquet"
        importance_path = destination / "feature_importance.parquet"
        _atomic_parquet(panel_path, panel)
        _atomic_parquet(predictions_path, result.predictions)
        _atomic_parquet(ensemble_path, result.ensemble_scores)
        _atomic_parquet(importance_path, result.feature_importance)

        model_outputs: dict[str, Any] = {}
        for model_key, model in result.models.items():
            final_path = destination / "models" / f"{model_key}.txt"
            temporary = final_path.with_name(final_path.name + ".tmp")
            final_path.parent.mkdir(parents=True, exist_ok=True)
            model.booster_.save_model(str(temporary))
            os.replace(temporary, final_path)
            model_outputs[model_key] = {
                **_file_receipt(final_path),
                "params": _model_params(model),
            }

        _assert_same_file_identity(static_path, static_receipt)
        for factor_name, factor_path in factor_paths.items():
            _assert_same_file_identity(factor_path, factor_receipts[factor_name])
        payload = {
            "schema_version": "qe_sector_rotation_model_artifact_v1",
            "classification": REAL_SECTOR_MODEL_CLASSIFICATION,
            "evaluation_id": evaluation_id,
            "source_sha256": source_sha,
            "config": {
                **common_identity,
                "horizon": int(horizon),
                "target_contract": result.data_audit["target_contract"],
            },
            "base_audit": base_result.audit,
            "factor_audits": factor_audits,
            "panel_audit": panel_audit,
            "training_audit": result.data_audit,
            "feature_columns": list(result.feature_columns),
            "metrics": result.metrics,
            "outputs": {
                "sector_feature_panel": _file_receipt(panel_path),
                "component_predictions": _file_receipt(predictions_path),
                "reality_sector_scores": _file_receipt(ensemble_path),
                "feature_importance": _file_receipt(importance_path),
                "models": model_outputs,
            },
            "research_decision": None,
            "research_note": (
                "real QE-only sector model evidence; missing or negative evidence is analyzed "
                "and does not eliminate the research direction"
            ),
        }
        _atomic_json(summary_path, payload)
        completed.append(payload)
        print(
            json.dumps(
                {
                    "event": "sector_model_horizon_completed",
                    "evaluation_id": evaluation_id,
                    "horizon": int(horizon),
                    "summary": str(summary_path),
                    "ensemble_scores": str(ensemble_path),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    batch_path = output_root / (
        "batch_" + canonical_sha256(common_identity) + ".json"
    )
    _atomic_json(
        batch_path,
        {
            "schema_version": "qe_sector_rotation_model_batch_v1",
            "classification": REAL_SECTOR_MODEL_CLASSIFICATION,
            "source_sha256": source_sha,
            "evaluations": [
                {
                    "evaluation_id": item["evaluation_id"],
                    "horizon": item["config"]["horizon"],
                    "summary": str(output_root / item["evaluation_id"] / "summary.json"),
                }
                for item in completed
            ],
            "research_decision": None,
        },
    )
    print(
        json.dumps(
            {"event": "sector_model_batch_completed", "batch_summary": str(batch_path)},
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
