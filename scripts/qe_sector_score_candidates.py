#!/usr/bin/env python3
"""Derive auditable real sector-score candidates from a QE sector-model artifact."""

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


_HEURISTIC_FEATURES: dict[str, tuple[str, ...]] = {
    "price_momentum_20_60": (
        "sector_return_20d",
        "sector_return_60d",
    ),
    "price_volume_flow_composite": (
        "sector_return_20d",
        "sector_return_60d",
        "sector_amount_ratio_20d",
        "sector_flow_amount_ratio_20d",
    ),
    "sector_factor_composite": (
        "Industry_Momentum__mean_rank",
        "IndustryMomentumExcessReturnCross__mean_rank",
        "m_sw2_net_vol_momentum__mean_rank",
        "m_sector_flow_price_divergence_10d_20d__mean_rank",
        "m_sector_breadth_persistence_10d_20d__mean_rank",
    ),
    "breadth_leadership_composite": (
        "m_sector_breadth_persistence_10d_20d__mean_rank",
        "industry_stock_momentum_diff_10d__mean_rank",
        "Price_Deviation_Historical_High__mean_rank",
        "m_drawdown_from_high__mean_rank",
    ),
}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
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
    frame.to_parquet(temporary, index=False)
    os.replace(temporary, path)


def _verified_output(summary: dict[str, Any], key: str) -> Path:
    metadata = summary.get("outputs", {}).get(key, {})
    path = Path(str(metadata.get("path") or "")).expanduser().resolve()
    expected = str(metadata.get("sha256") or "")
    if not path.is_file() or _sha256_file(path) != expected:
        raise RuntimeError(f"sector-model output is missing or hash-mismatched: {key}")
    return path


def _daily_metrics(frame: pd.DataFrame, *, top_m: int) -> dict[str, Any]:
    mature = frame.loc[
        np.isfinite(frame["sector_score"]) & np.isfinite(frame["target_return"])
    ]
    records: list[dict[str, float]] = []
    for _, day in mature.groupby("signal_date", sort=True):
        if len(day) < 3:
            continue
        predicted = set(day.nlargest(top_m, "sector_score")["l2_code_id"])
        actual = set(day.nlargest(top_m, "target_return")["l2_code_id"])
        records.append(
            {
                "rank_ic": float(
                    day["sector_score"].corr(day["target_return"], method="spearman")
                ),
                "recall_at_m": float(len(predicted & actual) / max(len(actual), 1)),
            }
        )
    daily = pd.DataFrame.from_records(records)
    return {
        "row_count": int(len(mature)),
        "date_count": int(len(daily)),
        "rank_ic_mean": float(daily["rank_ic"].mean()) if len(daily) else None,
        "rank_ic_std": float(daily["rank_ic"].std(ddof=1)) if len(daily) > 1 else None,
        "recall_at_m_mean": float(daily["recall_at_m"].mean()) if len(daily) else None,
    }


def _slice_metrics(frame: pd.DataFrame, *, top_m: int) -> dict[str, Any]:
    slices = {
        "all_test_mature": pd.Series(True, index=frame.index),
        "2024H2": frame["signal_date"].between("2024-07-01", "2024-12-31"),
        "2025": frame["signal_date"].between("2025-01-01", "2025-12-31"),
        "2026H1": frame["signal_date"].between("2026-01-01", "2026-06-30"),
    }
    return {
        name: _daily_metrics(frame.loc[mask], top_m=top_m)
        for name, mask in slices.items()
    }


def _component_candidate(
    components: pd.DataFrame,
    *,
    model_kind: str | None,
) -> pd.DataFrame:
    selected = components if model_kind is None else components.loc[
        components["model_kind"].eq(model_kind)
    ]
    return (
        selected.groupby(["signal_date", "l2_code_id"], sort=True)
        .agg(
            sector_score=("daily_score_rank", "mean"),
            component_count=("daily_score_rank", "count"),
        )
        .reset_index()
    )


def _heuristic_candidate(
    panel: pd.DataFrame,
    *,
    columns: tuple[str, ...],
    test_start: str,
    test_end: str,
) -> pd.DataFrame:
    selected = panel.loc[
        panel["datetime"].between(test_start, test_end),
        ["datetime", "l2_code_id", *columns],
    ].copy()
    ranked_columns: list[str] = []
    for column in columns:
        rank_column = f"__rank__{column}"
        selected[rank_column] = selected.groupby("datetime", sort=False)[column].rank(
            method="average", pct=True
        )
        ranked_columns.append(rank_column)
    selected["sector_score"] = selected[ranked_columns].mean(axis=1, skipna=True)
    selected["component_count"] = selected[ranked_columns].notna().sum(axis=1)
    return selected.rename(columns={"datetime": "signal_date"}).loc[
        :, ["signal_date", "l2_code_id", "sector_score", "component_count"]
    ]


def _equal_rank_blend(*frames: pd.DataFrame) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    rank_columns: list[str] = []
    for index, frame in enumerate(frames, start=1):
        column = f"component_rank_{index}"
        component = frame.loc[:, ["signal_date", "l2_code_id", "sector_score"]].copy()
        component[column] = component.groupby("signal_date", sort=False)[
            "sector_score"
        ].rank(method="average", pct=True)
        component = component.drop(columns=["sector_score"])
        merged = (
            component
            if merged is None
            else merged.merge(
                component,
                on=["signal_date", "l2_code_id"],
                how="outer",
                validate="one_to_one",
            )
        )
        rank_columns.append(column)
    if merged is None:
        raise ValueError("equal-rank sector blend requires at least one component")
    merged["sector_score"] = merged[rank_columns].mean(axis=1, skipna=True)
    merged["component_count"] = merged[rank_columns].notna().sum(axis=1)
    return merged.loc[
        :, ["signal_date", "l2_code_id", "sector_score", "component_count"]
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sector-model-summary", required=True)
    parser.add_argument("--output-root")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary_path = Path(args.sector_model_summary).expanduser().resolve()
    summary = _read_json(summary_path)
    if summary.get("schema_version") != "qe_sector_rotation_model_artifact_v1":
        raise RuntimeError(f"unsupported sector-model summary schema: {summary_path}")
    panel_path = _verified_output(summary, "sector_feature_panel")
    component_path = _verified_output(summary, "component_predictions")
    panel = pd.read_parquet(panel_path)
    components = pd.read_parquet(component_path)
    required_component = {
        "signal_date",
        "l2_code_id",
        "model_kind",
        "seed",
        "daily_score_rank",
    }
    missing_component = sorted(required_component - set(components.columns))
    if missing_component:
        raise RuntimeError(f"component predictions are missing: {missing_component}")
    required_panel = {"datetime", "l2_code_id", "target_return"}
    missing_panel = sorted(required_panel - set(panel.columns))
    if missing_panel:
        raise RuntimeError(f"sector feature panel is missing: {missing_panel}")

    config = summary["config"]
    test_start, test_end = config["split"]["test"]
    top_m = int(config["top_m"])
    candidate_frames: dict[str, pd.DataFrame] = {
        "all_model_seed_ensemble": _component_candidate(components, model_kind=None),
        "lgbm_regression_seed_ensemble": _component_candidate(
            components, model_kind="lgbm_regression"
        ),
        "lambdarank_seed_ensemble": _component_candidate(
            components, model_kind="lambdarank"
        ),
    }
    not_computable: dict[str, Any] = {}
    for candidate_name, columns in _HEURISTIC_FEATURES.items():
        missing = sorted(set(columns) - set(panel.columns))
        if missing:
            not_computable[candidate_name] = {
                "status": "NOT_COMPUTABLE",
                "missing_feature_columns": missing,
                "data_action": "restore_or_compute_missing_sector_feature_columns",
            }
            continue
        candidate_frames[candidate_name] = _heuristic_candidate(
            panel,
            columns=columns,
            test_start=str(test_start),
            test_end=str(test_end),
        )
    blend_specs = {
        "regression_breadth_equal_rank": (
            "lgbm_regression_seed_ensemble",
            "breadth_leadership_composite",
        ),
        "lambdarank_breadth_equal_rank": (
            "lambdarank_seed_ensemble",
            "breadth_leadership_composite",
        ),
        "regression_lambdarank_breadth_equal_rank": (
            "lgbm_regression_seed_ensemble",
            "lambdarank_seed_ensemble",
            "breadth_leadership_composite",
        ),
        "regression_breadth_momentum_equal_rank": (
            "lgbm_regression_seed_ensemble",
            "breadth_leadership_composite",
            "price_momentum_20_60",
        ),
    }
    for candidate_name, component_names in blend_specs.items():
        missing = [name for name in component_names if name not in candidate_frames]
        if missing:
            not_computable[candidate_name] = {
                "status": "NOT_COMPUTABLE",
                "missing_candidate_components": missing,
                "data_action": "restore_missing_sector_score_candidate_components",
            }
            continue
        candidate_frames[candidate_name] = _equal_rank_blend(
            *(candidate_frames[name] for name in component_names)
        )

    output_root = (
        Path(args.output_root).expanduser().resolve()
        if args.output_root
        else summary_path.parent / "score_candidates"
    )
    source_sha = _sha256_file(Path(__file__).resolve())
    evaluation_id = "qesc_" + canonical_sha256(
        {
            "source_sha256": source_sha,
            "sector_model_evaluation_id": summary["evaluation_id"],
            "panel_sha256": summary["outputs"]["sector_feature_panel"]["sha256"],
            "component_sha256": summary["outputs"]["component_predictions"]["sha256"],
            "heuristic_features": _HEURISTIC_FEATURES,
        }
    )
    destination = output_root / evaluation_id
    target = panel.loc[:, ["datetime", "l2_code_id", "target_return"]].rename(
        columns={"datetime": "signal_date"}
    )
    candidate_summaries: list[dict[str, Any]] = []
    outputs: dict[str, Any] = {}
    for candidate_name, scores in candidate_frames.items():
        score_path = destination / f"{candidate_name}.parquet"
        _atomic_parquet(score_path, scores)
        evaluation = scores.merge(
            target,
            on=["signal_date", "l2_code_id"],
            how="left",
            validate="one_to_one",
        )
        candidate_summaries.append(
            {
                "candidate_name": candidate_name,
                "metrics": _slice_metrics(evaluation, top_m=top_m),
                "score_rows": int(len(scores)),
                "finite_score_rows": int(np.isfinite(scores["sector_score"]).sum()),
                "component_count_distribution": {
                    str(key): int(value)
                    for key, value in scores["component_count"].value_counts().sort_index().items()
                },
                "research_decision": None,
                "research_note": "current QE trial evidence; no direction elimination",
            }
        )
        outputs[candidate_name] = {
            "path": str(score_path),
            "rows": int(len(scores)),
            "sha256": _sha256_file(score_path),
        }

    payload = {
        "schema_version": "qe_sector_score_candidates_v1",
        "evaluation_id": evaluation_id,
        "sector_model_evaluation_id": summary["evaluation_id"],
        "horizon": int(config["horizon"]),
        "source_sha256": source_sha,
        "candidate_summaries": candidate_summaries,
        "not_computable": not_computable,
        "outputs": outputs,
        "research_decision": None,
        "research_note": (
            "all candidate results remain QE research evidence and do not eliminate a direction"
        ),
    }
    summary_output = destination / "summary.json"
    _atomic_json(summary_output, payload)
    print(
        json.dumps(
            {
                "event": "sector_score_candidates_completed",
                "evaluation_id": evaluation_id,
                "summary": str(summary_output),
                "candidate_count": len(candidate_summaries),
                "not_computable_count": len(not_computable),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
