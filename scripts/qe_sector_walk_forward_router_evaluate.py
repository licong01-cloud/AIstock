#!/usr/bin/env python3
"""Evaluate a causal horizon-specific QE sector routing probe."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.services.quantevolver.long_trend_evaluation_contract import canonical_sha256
from backend.services.quantevolver.sector_regime_router import (
    SectorWalkForwardRouterConfig,
    build_observable_sector_state,
    compute_sector_walk_forward_router,
)


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
    if isinstance(value, np.integer):
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


def _verified_parquet(value: str) -> tuple[Path, str]:
    path = Path(value).expanduser().resolve()
    if not path.is_file():
        raise RuntimeError(f"walk-forward router input does not exist: {path}")
    return path, _sha256_file(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--regression-scores", required=True)
    parser.add_argument("--breadth-scores", required=True)
    parser.add_argument("--momentum-scores")
    parser.add_argument("--sector-overlay-oracle-daily", required=True)
    parser.add_argument("--horizon", type=int, required=True)
    parser.add_argument("--top-m", type=int, default=5)
    parser.add_argument("--min-train-days", type=int, default=80)
    parser.add_argument("--ridge-alpha", type=float, default=10.0)
    parser.add_argument("--route-threshold", type=float, default=0.0)
    parser.add_argument("--bootstrap-samples", type=int, default=500)
    parser.add_argument("--bootstrap-seed", type=int, default=20260716)
    parser.add_argument("--output-root", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_values = {
        "regression_scores": args.regression_scores,
        "breadth_scores": args.breadth_scores,
        "sector_overlay_oracle_daily": args.sector_overlay_oracle_daily,
    }
    if args.momentum_scores:
        input_values["momentum_scores"] = args.momentum_scores
    inputs = {
        name: _verified_parquet(value) for name, value in input_values.items()
    }
    source_paths = (
        Path(__file__).resolve(),
        Path(__file__).resolve().parents[1]
        / "backend/services/quantevolver/sector_regime_router.py",
    )
    source_sha = canonical_sha256(
        {path.name: _sha256_file(path) for path in source_paths}
    )
    config = SectorWalkForwardRouterConfig(
        horizon=args.horizon,
        top_m=args.top_m,
        min_train_days=args.min_train_days,
        ridge_alpha=args.ridge_alpha,
        route_threshold=args.route_threshold,
        bootstrap_samples=args.bootstrap_samples,
        bootstrap_seed=args.bootstrap_seed,
    )
    identity = {
        "source_sha256": source_sha,
        "input_sha256": {name: sha for name, (_, sha) in inputs.items()},
        "config": {
            "horizon": config.horizon,
            "top_m": config.top_m,
            "min_train_days": config.min_train_days,
            "ridge_alpha": config.ridge_alpha,
            "route_threshold": config.route_threshold,
            "bootstrap_samples": config.bootstrap_samples,
            "bootstrap_seed": config.bootstrap_seed,
        },
    }
    evaluation_id = "qesrwf_" + canonical_sha256(identity)
    destination = Path(args.output_root).expanduser().resolve() / evaluation_id
    score_frames = {
        "regression": pd.read_parquet(inputs["regression_scores"][0]),
        "breadth": pd.read_parquet(inputs["breadth_scores"][0]),
    }
    if "momentum_scores" in inputs:
        score_frames["momentum"] = pd.read_parquet(inputs["momentum_scores"][0])
    observable_state = build_observable_sector_state(
        score_frames,
        top_m=config.top_m,
    )
    result = compute_sector_walk_forward_router(
        observable_state,
        pd.read_parquet(inputs["sector_overlay_oracle_daily"][0]),
        config=config,
    )
    observable_path = destination / "observable_state.parquet"
    daily_path = destination / "daily.parquet"
    coefficients_path = destination / "coefficients.parquet"
    _atomic_parquet(observable_path, observable_state)
    _atomic_parquet(daily_path, result.daily)
    _atomic_parquet(coefficients_path, result.coefficients)
    payload = {
        "schema_version": "qe_sector_walk_forward_router_artifact_v1",
        "classification": "REAL_QE_WALK_FORWARD_SECTOR_ROUTER",
        "evaluation_id": evaluation_id,
        "identity": identity,
        "inputs": {
            name: {"path": str(path), "sha256": sha}
            for name, (path, sha) in inputs.items()
        },
        "audit": result.audit,
        "score_families": sorted(score_frames),
        "metrics": result.metrics,
        "outputs": {
            "observable_state": {
                "path": str(observable_path),
                "rows": int(len(observable_state)),
                "sha256": _sha256_file(observable_path),
            },
            "daily": {
                "path": str(daily_path),
                "rows": int(len(result.daily)),
                "sha256": _sha256_file(daily_path),
            },
            "coefficients": {
                "path": str(coefficients_path),
                "rows": int(len(result.coefficients)),
                "sha256": _sha256_file(coefficients_path),
            },
        },
        "research_decision": None,
        "research_note": (
            "causal online expanding QE-only probe; current result does not "
            "eliminate any sector routing direction"
        ),
    }
    summary_path = destination / "summary.json"
    _atomic_json(summary_path, payload)
    print(
        json.dumps(
            {
                "event": "sector_walk_forward_router_completed",
                "evaluation_id": evaluation_id,
                "summary": str(summary_path),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
