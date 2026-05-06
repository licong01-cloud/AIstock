"""Screen sparse penalty maps from retrained Stage-3 HMM score panels.

The Stage-3 QE run showed that continuous sector coefficients were too broad.
This diagnostic keeps the retrained HMM outputs fixed, converts their sector
scores into sparse penalty-only coefficient maps, and replays TopK
enter/drop attribution versus the retained Loop10 baseline before any QE run.

It is intentionally read-only: no registry writes, no model asset writes, and
no QE task submission.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.diagnostics.hmm_loop10_centered_attribution import (  # noqa: E402
    DEFAULT_HMM_DIAG_DIR,
    HOLDOUT_START,
    LOOP10_SNAPSHOT_ID,
    coefficient_delta_summary,
    load_coefficients,
    pairwise_topk_attribution,
    summarize_pairwise_periods,
)
from scripts.diagnostics.hmm_qe_candidate_attribution import BASELINE_COEFFICIENTS  # noqa: E402
from scripts.diagnostics.hmm_sector_factor_overlay_diagnostic import (  # noqa: E402
    enrich_db_forward_returns,
    find_base_artifacts,
    label_to_series,
    load_pickle,
    load_stock_sector_map,
    pred_to_series,
)


DEFAULT_TASK_ID = "qe_20260505_123035_bf80"
DEFAULT_OUTPUT_DIR = Path(".codex_tmp/hmm_stage3_sparse_penalty_screen_20260505")


@dataclass(frozen=True)
class ScoreSource:
    name: str
    panel_path: Path
    score_col: str
    bad_side: str
    note: str


SCORE_SOURCES: tuple[ScoreSource, ...] = (
    ScoreSource(
        name="fbt_robust_n2_hmm_low",
        panel_path=Path(
            ".codex_tmp/hmm_sector_factor_stage3_best_final_20260505/"
            "models/stage3_flow_breadth_tier_robust/score_panel.csv"
        ),
        score_col="hmm_score",
        bad_side="low",
        note="Stage3 best retrained HMM, trend-fade score; low score is bad.",
    ),
    ScoreSource(
        name="fbt_robust_n2_pfading_high",
        panel_path=Path(
            ".codex_tmp/hmm_sector_factor_stage3_best_final_20260505/"
            "models/stage3_flow_breadth_tier_robust/score_panel.csv"
        ),
        score_col="p_fading",
        bad_side="high",
        note="Stage3 best retrained HMM, posterior fading probability.",
    ),
    ScoreSource(
        name="flow_dynamic_n2_util_low",
        panel_path=Path(
            ".codex_tmp/hmm_sector_factor_stage3_diag2_20260505/"
            "models/stage3_flow_dynamic_breadth/score_panel.csv"
        ),
        score_col="utility_raw_score",
        bad_side="low",
        note="Retrained flow dynamic breadth HMM, utility score.",
    ),
    ScoreSource(
        name="flow_breadth_n2_util_low",
        panel_path=Path(
            ".codex_tmp/hmm_sector_factor_stage3_diag2_20260505/"
            "models/flow_plus_breadth/score_panel.csv"
        ),
        score_col="utility_raw_score",
        bad_side="low",
        note="Retrained flow plus breadth HMM, utility score.",
    ),
    ScoreSource(
        name="turnover_light_n3_util_low",
        panel_path=Path(
            ".codex_tmp/hmm_sector_factor_stage3_diag3_20260505/"
            "models/stage3_flow_breadth_turnover_light/score_panel.csv"
        ),
        score_col="utility_raw_score",
        bad_side="low",
        note="Retrained 3-state turnover-light HMM, utility score.",
    ),
)


def load_loop10_coefficients() -> dict[str, dict[str, float]]:
    for item in BASELINE_COEFFICIENTS:
        if item["snapshot_id"] == LOOP10_SNAPSHOT_ID:
            return load_coefficients(ROOT / item["coefficients_path"])
    raise RuntimeError(f"Loop10 coefficient path not found for {LOOP10_SNAPSHOT_ID}")


def read_score_panel(path: Path, score_col: str) -> pd.DataFrame:
    full_path = ROOT / path
    if not full_path.is_file():
        raise FileNotFoundError(f"score panel not found: {full_path}")
    usecols = ["trade_date", "sector_code", score_col]
    # Keep the file read column-bounded; score panels are sector-date level.
    read_csv = getattr(pd, "read_csv")
    frame = read_csv(full_path, usecols=usecols)
    frame = frame.dropna(subset=["trade_date", "sector_code", score_col])
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y-%m-%d")
    frame["sector_code"] = frame["sector_code"].astype(str)
    frame[score_col] = pd.to_numeric(frame[score_col], errors="coerce")
    frame = frame.dropna(subset=[score_col])
    return frame


def badness_from_score(frame: pd.DataFrame, source: ScoreSource) -> pd.DataFrame:
    out = frame[["trade_date", "sector_code", source.score_col]].copy()
    score = out[source.score_col].astype(float)
    if source.bad_side == "low":
        out["badness"] = -score
    elif source.bad_side == "high":
        out["badness"] = score
    else:
        raise ValueError(f"unsupported bad_side: {source.bad_side}")
    return out[["trade_date", "sector_code", "badness"]]


def candidate_name(source: ScoreSource, pct: float, penalty: float, confirm: str) -> str:
    pct_tag = f"B{int(round(pct * 100)):02d}"
    pen_tag = str(penalty).replace(".", "p")
    return f"SPARSE_{source.name}_{pct_tag}_PEN_{pen_tag}_{confirm}"


def iter_grid(
    *,
    bottom_pcts: Iterable[float],
    penalties: Iterable[float],
    confirms: Iterable[str],
) -> Iterable[tuple[float, float, str]]:
    for pct in bottom_pcts:
        for penalty in penalties:
            for confirm in confirms:
                yield pct, penalty, confirm


def build_sparse_coefficients(
    *,
    base_coeffs: dict[str, dict[str, float]],
    badness: pd.DataFrame,
    pct: float,
    penalty: float,
    confirm: str,
) -> dict[str, dict[str, float]]:
    """Add sparse penalties on top of Loop10 without ever boosting sectors."""

    by_date = {
        str(dt): day.set_index("sector_code")["badness"].astype(float)
        for dt, day in badness.groupby("trade_date", sort=False)
    }
    out: dict[str, dict[str, float]] = {}
    for trade_date, base_row in base_coeffs.items():
        row = {str(sector): float(value) for sector, value in base_row.items()}
        scores = by_date.get(trade_date)
        if scores is None or scores.empty:
            out[trade_date] = row
            continue

        scores = scores.reindex(row.keys()).dropna()
        if scores.empty:
            out[trade_date] = row
            continue

        n_bad = max(1, int(np.ceil(len(scores) * pct)))
        selected = set(scores.sort_values(ascending=False, kind="mergesort").head(n_bad).index)
        if confirm == "loop10_only":
            selected = {sector for sector in selected if row.get(str(sector), 1.0) < 1.0 - 1e-12}
        elif confirm == "stage3_only":
            pass
        elif confirm == "loop10_or_stage3":
            # Same output as stage3_only for added penalties, but the name is
            # kept for report readability when comparing future rule families.
            pass
        else:
            raise ValueError(f"unsupported confirm mode: {confirm}")

        for sector in selected:
            row[str(sector)] = min(float(row.get(str(sector), 1.0)), float(penalty))
        out[trade_date] = row
    return out


def write_coefficients(path: Path, candidate: str, coeffs: dict[str, dict[str, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": "stage3_sparse_penalty_screen_v1",
        "candidate": candidate,
        "baseline_snapshot_id": LOOP10_SNAPSHOT_ID,
        "registered_for_qe": False,
        "daily_coefficients": coeffs,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def score_for_ranking(summary: pd.DataFrame) -> pd.DataFrame:
    out = summary.copy()
    for col in ("net_mean_db_ret_5d", "net_mean_db_ret_10d", "avg_entered_per_day"):
        if col not in out.columns:
            out[col] = np.nan
    out["screen_score"] = (
        out["net_mean_db_ret_10d"].fillna(-1.0)
        + 0.5 * out["net_mean_db_ret_5d"].fillna(-1.0)
        - 0.0005 * out["avg_entered_per_day"].fillna(0.0)
    )
    return out


def write_report(path: Path, summary: pd.DataFrame, coeff_summary: pd.DataFrame, sources: list[dict[str, Any]]) -> None:
    holdout = summary[summary["period"] == "holdout"].copy()
    holdout = score_for_ranking(holdout).sort_values("screen_score", ascending=False)
    coeff = coeff_summary.sort_values("mean_abs_delta_vs_loop10", ascending=True)

    def table(df: pd.DataFrame, cols: list[str], n: int = 20) -> str:
        if df.empty:
            return "(no rows)"
        existing = [col for col in cols if col in df.columns]
        return "```text\n" + df[existing].head(n).to_string(index=False) + "\n```"

    lines = [
        "# Stage3 Sparse Penalty Screen",
        "",
        f"- Baseline: `LOOP10_BASE__penalty_only_f096` / `{LOOP10_SNAPSHOT_ID}`",
        f"- Holdout split: `{HOLDOUT_START}`",
        "- Candidate maps are script-level only and are not registered for QE.",
        "- All candidates use retrained Stage-3 HMM score panels but sparse penalty-only coefficient mapping.",
        "",
        "## Sources",
        "",
        table(pd.DataFrame(sources), ["name", "score_col", "bad_side", "rows", "dates", "sectors", "note"], 20),
        "",
        "## Top Holdout TopK Replacement Candidates vs Loop10",
        "",
        table(
            holdout,
            [
                "candidate",
                "changed_days",
                "avg_entered_per_day",
                "net_mean_label_10d",
                "net_mean_db_ret_5d",
                "net_mean_db_ret_10d",
                "net_mean_db_ret_20d",
                "positive_net_label_day_ratio",
                "screen_score",
            ],
            30,
        ),
        "",
        "## Coefficient Delta vs Loop10",
        "",
        table(
            coeff,
            [
                "candidate",
                "mean_abs_delta_vs_loop10",
                "candidate_lt_loop10_share",
                "candidate_penalty_share",
                "candidate_min",
                "candidate_max",
            ],
            30,
        ),
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", default=DEFAULT_TASK_ID)
    parser.add_argument("--hmm-diag-dir", type=Path, default=DEFAULT_HMM_DIAG_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--topk", type=int, default=50)
    parser.add_argument("--bottom-pcts", nargs="+", type=float, default=[0.05, 0.10, 0.15, 0.20])
    parser.add_argument("--penalties", nargs="+", type=float, default=[0.995, 0.99, 0.985, 0.98, 0.96])
    parser.add_argument("--confirm-modes", nargs="+", default=["stage3_only", "loop10_only"])
    parser.add_argument("--max-candidates", type=int, default=0, help="Optional cap for smoke tests.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_coeffs = load_loop10_coefficients()
    pred_path, label_path = find_base_artifacts(args.hmm_diag_dir)
    pred_ser = pred_to_series(load_pickle(pred_path))
    label_ser = label_to_series(load_pickle(label_path))
    stock_sector_map = load_stock_sector_map(args.hmm_diag_dir)

    out_dir = args.output_dir / args.task_id
    coeff_dir = out_dir / "candidate_coefficients"
    if coeff_dir.exists():
        for old_path in coeff_dir.glob("SPARSE_*.json"):
            old_path.unlink()
    coeff_dir.mkdir(parents=True, exist_ok=True)

    source_stats: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    rep_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    coeff_rows: list[dict[str, Any]] = []
    tested = 0

    for source in SCORE_SOURCES:
        frame = read_score_panel(source.panel_path, source.score_col)
        badness = badness_from_score(frame, source)
        source_stats.append(
            {
                "name": source.name,
                "score_col": source.score_col,
                "bad_side": source.bad_side,
                "rows": int(len(frame)),
                "dates": int(frame["trade_date"].nunique()),
                "sectors": int(frame["sector_code"].nunique()),
                "note": source.note,
            }
        )
        for pct, penalty, confirm in iter_grid(
            bottom_pcts=args.bottom_pcts,
            penalties=args.penalties,
            confirms=args.confirm_modes,
        ):
            name = candidate_name(source, pct, penalty, confirm)
            coeffs = build_sparse_coefficients(
                base_coeffs=base_coeffs,
                badness=badness,
                pct=pct,
                penalty=penalty,
                confirm=confirm,
            )
            write_coefficients(coeff_dir / f"{name}.json", name, coeffs)
            rep, daily = pairwise_topk_attribution(
                pred_ser,
                label_ser,
                stock_sector_map,
                base_coeffs,
                coeffs,
                name,
                args.topk,
            )
            enriched = enrich_db_forward_returns(rep, [5, 10, 20]) if not rep.empty else rep
            rep_frames.append(enriched)
            daily_frames.append(daily)
            summary_rows.extend(summarize_pairwise_periods(enriched, daily, name))
            coeff_rows.append(coefficient_delta_summary(base_coeffs, coeffs, name))
            tested += 1
            if args.max_candidates and tested >= args.max_candidates:
                break
        if args.max_candidates and tested >= args.max_candidates:
            break

    summary = pd.DataFrame(summary_rows)
    concat_frames = getattr(pd, "concat")
    replacements = concat_frames(rep_frames, ignore_index=True) if rep_frames else pd.DataFrame()
    daily = concat_frames(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    coeff_summary = pd.DataFrame(coeff_rows)

    out_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "summary": out_dir / "sparse_topk_summary_vs_loop10.csv",
        "summary_ranked": out_dir / "sparse_topk_holdout_ranked.csv",
        "replacements": out_dir / "sparse_topk_replacements_vs_loop10.csv",
        "daily": out_dir / "sparse_topk_daily_vs_loop10.csv",
        "coefficient_delta": out_dir / "sparse_coefficient_delta_vs_loop10.csv",
        "sources": out_dir / "sparse_sources.json",
        "report": out_dir / "sparse_penalty_screen_report.md",
        "coeff_dir": coeff_dir,
    }
    summary.to_csv(files["summary"], index=False, encoding="utf-8-sig")
    ranked = score_for_ranking(summary[summary["period"] == "holdout"].copy()).sort_values(
        "screen_score", ascending=False
    )
    ranked.to_csv(files["summary_ranked"], index=False, encoding="utf-8-sig")
    replacements.to_csv(files["replacements"], index=False, encoding="utf-8-sig")
    daily.to_csv(files["daily"], index=False, encoding="utf-8-sig")
    coeff_summary.to_csv(files["coefficient_delta"], index=False, encoding="utf-8-sig")
    files["sources"].write_text(json.dumps(source_stats, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(files["report"], summary, coeff_summary, source_stats)

    print(
        json.dumps(
            {
                "task_id": args.task_id,
                "tested_candidates": tested,
                "output_dir": str(out_dir),
                "files": {key: str(value) for key, value in files.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
