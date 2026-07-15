#!/usr/bin/env python3
"""Evaluate an observable QE-only sector agreement router."""

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
    SECTOR_ROUTER_CLASSIFICATION,
    SectorAgreementRouterConfig,
    compute_sector_agreement_router,
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


def _verified_parquet(value: str) -> tuple[Path, str]:
    path = Path(value).expanduser().resolve()
    if not path.is_file():
        raise RuntimeError(f"router input does not exist: {path}")
    return path, _sha256_file(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--regression-scores", required=True)
    parser.add_argument("--breadth-scores", required=True)
    parser.add_argument("--sector-overlay-oracle-daily", required=True)
    parser.add_argument("--horizon", type=int, required=True)
    parser.add_argument("--lookback", type=int, default=126)
    parser.add_argument("--min-periods", type=int, default=60)
    parser.add_argument("--agreement-quantile", type=float, default=0.75)
    parser.add_argument("--bootstrap-samples", type=int, default=500)
    parser.add_argument("--bootstrap-seed", type=int, default=20260716)
    parser.add_argument("--output-root", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    regression_path, regression_sha = _verified_parquet(args.regression_scores)
    breadth_path, breadth_sha = _verified_parquet(args.breadth_scores)
    overlay_oracle_path, overlay_oracle_sha = _verified_parquet(
        args.sector_overlay_oracle_daily
    )
    source_paths = (
        Path(__file__).resolve(),
        Path(__file__).resolve().parents[1]
        / "backend/services/quantevolver/sector_regime_router.py",
    )
    source_sha = canonical_sha256(
        {path.name: _sha256_file(path) for path in source_paths}
    )
    config = SectorAgreementRouterConfig(
        horizon=args.horizon,
        lookback=args.lookback,
        min_periods=args.min_periods,
        agreement_quantile=args.agreement_quantile,
        bootstrap_samples=args.bootstrap_samples,
        bootstrap_seed=args.bootstrap_seed,
    )
    identity = {
        "source_sha256": source_sha,
        "regression_scores_sha256": regression_sha,
        "breadth_scores_sha256": breadth_sha,
        "sector_overlay_oracle_daily_sha256": overlay_oracle_sha,
        "config": {
            "horizon": config.horizon,
            "lookback": config.lookback,
            "min_periods": config.min_periods,
            "agreement_quantile": config.agreement_quantile,
            "bootstrap_samples": config.bootstrap_samples,
            "bootstrap_seed": config.bootstrap_seed,
        },
    }
    evaluation_id = "qesrtr_" + canonical_sha256(identity)
    destination = Path(args.output_root).expanduser().resolve() / evaluation_id
    result = compute_sector_agreement_router(
        pd.read_parquet(regression_path),
        pd.read_parquet(breadth_path),
        pd.read_parquet(overlay_oracle_path),
        config=config,
    )
    daily_path = destination / "daily.parquet"
    _atomic_parquet(daily_path, result.daily)
    payload = {
        "schema_version": "qe_sector_agreement_router_artifact_v1",
        "classification": SECTOR_ROUTER_CLASSIFICATION,
        "evaluation_id": evaluation_id,
        "identity": identity,
        "inputs": {
            "regression_scores": {"path": str(regression_path), "sha256": regression_sha},
            "breadth_scores": {"path": str(breadth_path), "sha256": breadth_sha},
            "sector_overlay_oracle_daily": {
                "path": str(overlay_oracle_path),
                "sha256": overlay_oracle_sha,
            },
        },
        "audit": result.audit,
        "metrics": result.metrics,
        "outputs": {
            "daily": {
                "path": str(daily_path),
                "rows": int(len(result.daily)),
                "sha256": _sha256_file(daily_path),
            }
        },
        "research_decision": None,
        "research_note": (
            "observable QE-only routing trial; no research direction is eliminated"
        ),
    }
    summary_path = destination / "summary.json"
    _atomic_json(summary_path, payload)
    print(
        json.dumps(
            {
                "event": "sector_agreement_router_completed",
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
