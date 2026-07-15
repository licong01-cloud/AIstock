#!/usr/bin/env python3
"""Run F-018 four-cell sector oracle analysis on F-014 QE artifacts."""

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
from backend.services.quantevolver.sector_oracle import (
    ORACLE_CLASSIFICATION,
    SectorOracleConfig,
    compute_sector_oracle_grid,
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid JSON artifact {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON artifact must contain an object: {path}")
    return value


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
    safe = frame.copy(deep=False)
    for column in safe.select_dtypes(include=["object"]).columns:
        sample = safe[column].dropna().head(100)
        if any(isinstance(value, (dict, list, tuple, set)) for value in sample):
            safe[column] = safe[column].map(
                lambda value: json.dumps(
                    _finite_or_none(value), ensure_ascii=False, sort_keys=True, allow_nan=False
                )
                if isinstance(value, (dict, list, tuple, set))
                else value
            )
    safe.to_parquet(temporary, index=False)
    os.replace(temporary, path)


def _source_sha256() -> str:
    root = Path(__file__).resolve().parents[1]
    sources = (Path(__file__).resolve(), root / "backend/services/quantevolver/sector_oracle.py")
    return canonical_sha256(
        {str(path.relative_to(root)): _sha256_file(path) for path in sources}
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--f014-summary", required=True)
    parser.add_argument("--horizon", type=int, action="append", required=True)
    parser.add_argument("--sector-top-m", type=int, action="append", default=[])
    parser.add_argument("--stock-top-k", type=int, default=50)
    parser.add_argument("--round-trip-cost-bps", type=float, required=True)
    parser.add_argument("--bootstrap-samples", type=int, default=500)
    parser.add_argument("--bootstrap-seed", type=int, default=20260716)
    parser.add_argument(
        "--reality-sector-scores",
        help=(
            "Optional QE-only Parquet with signal_date, l2_code_id and sector_score. "
            "Missing scores remain missing and never fall back to stock-score aggregation."
        ),
    )
    parser.add_argument(
        "--reality-sector-score-name",
        default="external_qe_sector_model",
        help="Auditable name of the external sector-score producer.",
    )
    parser.add_argument("--output-root")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary_path = Path(args.f014_summary).expanduser().resolve()
    f014 = _read_json(summary_path)
    if f014.get("schema_version") != "qe_long_trend_evaluation_artifact_v1":
        raise RuntimeError(f"unsupported F-014 summary schema: {summary_path}")
    observation_meta = f014.get("outputs", {}).get("signal_observations", {})
    observation_path = Path(str(observation_meta.get("path") or "")).expanduser().resolve()
    expected_sha = str(observation_meta.get("sha256") or "")
    if not observation_path.is_file() or _sha256_file(observation_path) != expected_sha:
        raise RuntimeError(
            f"F-014 signal observation artifact is missing or hash-mismatched: {observation_path}"
        )
    observations = pd.read_parquet(observation_path)
    reality_sector_scores = None
    reality_sector_score_input: dict[str, Any] | None = None
    if args.reality_sector_scores:
        score_path = Path(args.reality_sector_scores).expanduser().resolve()
        if not score_path.is_file():
            raise RuntimeError(f"reality sector score artifact does not exist: {score_path}")
        score_sha256 = _sha256_file(score_path)
        reality_sector_scores = pd.read_parquet(score_path)
        reality_sector_score_input = {
            "path": str(score_path),
            "sha256": score_sha256,
            "rows": int(len(reality_sector_scores)),
            "score_name": str(args.reality_sector_score_name),
        }
    source_sha = _source_sha256()
    output_root = (
        Path(args.output_root).expanduser().resolve()
        if args.output_root
        else summary_path.parent / "sector_oracle"
    )
    sector_top_m_values = args.sector_top_m or [3, 5, 10]
    results: list[dict[str, Any]] = []

    for horizon in args.horizon:
        for sector_top_m in sector_top_m_values:
            config = SectorOracleConfig(
                horizon=horizon,
                sector_top_m=sector_top_m,
                stock_top_k=args.stock_top_k,
                round_trip_cost_bps=args.round_trip_cost_bps,
                bootstrap_samples=args.bootstrap_samples,
                bootstrap_seed=args.bootstrap_seed,
            )
            config_payload = {
                "horizon": config.horizon,
                "sector_top_m": config.sector_top_m,
                "stock_top_k": config.stock_top_k,
                "round_trip_cost_bps": config.round_trip_cost_bps,
                "barriers": list(config.barriers),
                "bootstrap_samples": config.bootstrap_samples,
                "bootstrap_seed": config.bootstrap_seed,
                "reality_sector_score_input": reality_sector_score_input,
            }
            evaluation_id = "qeso_" + canonical_sha256(
                {
                    "f014_evaluation_id": f014["evaluation_id"],
                    "observation_sha256": expected_sha,
                    "source_sha256": source_sha,
                    "config": config_payload,
                }
            )
            destination = output_root / evaluation_id
            summary_output = destination / "summary.json"
            if summary_output.is_file():
                existing = _read_json(summary_output)
                if existing.get("evaluation_id") != evaluation_id:
                    raise RuntimeError(f"immutable oracle identity conflict: {summary_output}")
                results.append(existing)
                continue

            print(
                json.dumps(
                    {
                        "event": "sector_oracle_started",
                        "evaluation_id": evaluation_id,
                        "horizon": horizon,
                        "sector_top_m": sector_top_m,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            result = compute_sector_oracle_grid(
                observations,
                config=config,
                reality_sector_scores=reality_sector_scores,
                reality_sector_score_name=str(args.reality_sector_score_name),
            )
            daily_path = destination / "daily.parquet"
            selections_path = destination / "selections.parquet"
            _atomic_parquet(daily_path, result.daily)
            _atomic_parquet(selections_path, result.selections)
            payload = {
                "schema_version": "qe_sector_oracle_artifact_v1",
                "evaluation_id": evaluation_id,
                "classification": ORACLE_CLASSIFICATION,
                "f014_evaluation_id": f014["evaluation_id"],
                "run_id": f014["run_id"],
                "source_sha256": source_sha,
                "config": config_payload,
                "eligibility": result.eligibility,
                "summaries": result.summaries,
                "inputs": {
                    "f014_signal_observations": {
                        "path": str(observation_path),
                        "sha256": expected_sha,
                        "rows": int(len(observations)),
                    },
                    "reality_sector_scores": reality_sector_score_input,
                },
                "outputs": {
                    "daily": {
                        "path": str(daily_path),
                        "rows": int(len(result.daily)),
                        "sha256": _sha256_file(daily_path),
                    },
                    "selections": {
                        "path": str(selections_path),
                        "rows": int(len(result.selections)),
                        "sha256": _sha256_file(selections_path),
                    },
                },
                "research_decision": None,
                "research_note": (
                    "future-information ceiling and reality diagnostics for this QE trial; "
                    "no GO/STOP and no research-direction elimination"
                ),
            }
            if _sha256_file(observation_path) != expected_sha:
                raise RuntimeError(
                    "F-014 signal observation artifact changed during sector-oracle evaluation"
                )
            if reality_sector_score_input is not None:
                score_path = Path(reality_sector_score_input["path"])
                if _sha256_file(score_path) != reality_sector_score_input["sha256"]:
                    raise RuntimeError(
                        "reality sector score artifact changed during sector-oracle evaluation"
                    )
            _atomic_json(summary_output, payload)
            results.append(payload)
            print(
                json.dumps(
                    {
                        "event": "sector_oracle_completed",
                        "evaluation_id": evaluation_id,
                        "summary": str(summary_output),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

    batch_path = output_root / "batch_summary.json"
    _atomic_json(
        batch_path,
        {
            "schema_version": "qe_sector_oracle_batch_v1",
            "f014_evaluation_id": f014["evaluation_id"],
            "source_sha256": source_sha,
            "results": [
                {
                    "evaluation_id": item["evaluation_id"],
                    "config": item["config"],
                    "summary_path": str(output_root / item["evaluation_id"] / "summary.json"),
                }
                for item in results
            ],
        },
    )
    print(
        json.dumps(
            {"event": "sector_oracle_batch_completed", "batch_summary": str(batch_path)},
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
