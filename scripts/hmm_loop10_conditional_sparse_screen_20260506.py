"""Screen Loop10-centered conditional sparse HMM maps before QE.

This read-only diagnostic is the next step after the Stage3 sparse QE run:
keep Loop10 as the anchor, use retrained sector-factor HMM score panels only as
conditional gates, and avoid broad continuous coefficient replacement.

No model registry rows are written and no QE task is submitted.
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
    pairwise_topk_attribution,
    summarize_pairwise_periods,
)
from scripts.diagnostics.hmm_sector_factor_overlay_diagnostic import (  # noqa: E402
    enrich_db_forward_returns,
    find_base_artifacts,
    label_to_series,
    load_pickle,
    load_stock_sector_map,
    pred_to_series,
)
from scripts.hmm_stage3_sparse_penalty_screen_20260505 import (  # noqa: E402
    SCORE_SOURCES,
    ScoreSource,
    badness_from_score,
    load_loop10_coefficients,
    read_score_panel,
    write_coefficients,
)


DEFAULT_TASK_ID = "qe_20260505_210355_155f"
DEFAULT_OUTPUT_DIR = Path(".codex_tmp/hmm_loop10_conditional_sparse_screen_20260506")


@dataclass(frozen=True)
class CandidateSpec:
    name: str
    family: str
    flags: pd.DataFrame
    penalty: float | None = None
    note: str = ""


def pct_tag(value: float) -> str:
    return f"P{int(round(value * 100)):02d}"


def pen_tag(value: float) -> str:
    return str(value).replace(".", "p")


def source_alias(source: ScoreSource) -> str:
    aliases = {
        "turnover_light_n3_util_low": "TL",
        "flow_breadth_n2_util_low": "FB",
        "flow_dynamic_n2_util_low": "FD",
        "fbt_robust_n2_hmm_low": "FBT",
        "fbt_robust_n2_pfading_high": "PFAD",
    }
    return aliases.get(source.name, source.name.upper())


def select_bad_flags(
    badness: pd.DataFrame,
    *,
    pct: float,
    persist_days: int = 1,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for trade_date, day in badness.groupby("trade_date", sort=False):
        ordered = day.sort_values("badness", ascending=False, kind="mergesort")
        n_bad = max(1, int(np.ceil(len(ordered) * pct)))
        selected = ordered.head(n_bad)[["trade_date", "sector_code"]].copy()
        selected["selected"] = True
        rows.append(selected)
    if not rows:
        return pd.DataFrame(columns=["trade_date", "sector_code"])
    flags = pd.concat(rows, ignore_index=True)
    if persist_days <= 1:
        return flags[["trade_date", "sector_code"]].drop_duplicates()

    all_dates = sorted(pd.to_datetime(badness["trade_date"].unique()))
    sectors = sorted(badness["sector_code"].astype(str).unique())
    grid = pd.MultiIndex.from_product(
        [all_dates, sectors], names=["trade_date_ts", "sector_code"]
    ).to_frame(index=False)
    grid["trade_date"] = grid["trade_date_ts"].dt.strftime("%Y-%m-%d")
    merged = grid.merge(flags, on=["trade_date", "sector_code"], how="left")
    merged["selected"] = merged["selected"].eq(True).astype(int)
    merged = merged.sort_values(["sector_code", "trade_date_ts"])
    merged["rolling_selected"] = (
        merged.groupby("sector_code")["selected"]
        .transform(lambda s: s.rolling(persist_days, min_periods=persist_days).sum())
        .fillna(0)
    )
    out = merged.loc[merged["rolling_selected"] >= persist_days, ["trade_date", "sector_code"]]
    return out.drop_duplicates()


def select_good_flags(badness: pd.DataFrame, *, pct: float) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for _trade_date, day in badness.groupby("trade_date", sort=False):
        ordered = day.sort_values("badness", ascending=True, kind="mergesort")
        n_good = max(1, int(np.ceil(len(ordered) * pct)))
        rows.append(ordered.head(n_good)[["trade_date", "sector_code"]].copy())
    if not rows:
        return pd.DataFrame(columns=["trade_date", "sector_code"])
    return pd.concat(rows, ignore_index=True).drop_duplicates()


def vote_flags(flag_sets: Iterable[pd.DataFrame], *, vote_threshold: int) -> pd.DataFrame:
    frames = []
    for frame in flag_sets:
        if frame.empty:
            continue
        cur = frame[["trade_date", "sector_code"]].copy()
        cur["vote"] = 1
        frames.append(cur)
    if not frames:
        return pd.DataFrame(columns=["trade_date", "sector_code"])
    votes = pd.concat(frames, ignore_index=True)
    agg = votes.groupby(["trade_date", "sector_code"], as_index=False)["vote"].sum()
    return agg.loc[agg["vote"] >= vote_threshold, ["trade_date", "sector_code"]]


def flags_by_date(flags: pd.DataFrame) -> dict[str, set[str]]:
    if flags.empty:
        return {}
    out: dict[str, set[str]] = {}
    for trade_date, day in flags.groupby("trade_date", sort=False):
        out[str(trade_date)] = {str(x) for x in day["sector_code"]}
    return out


def build_add_penalty(
    base_coeffs: dict[str, dict[str, float]],
    flags: pd.DataFrame,
    penalty: float,
) -> dict[str, dict[str, float]]:
    selected_by_date = flags_by_date(flags)
    out: dict[str, dict[str, float]] = {}
    for trade_date, base_row in base_coeffs.items():
        row = {str(sec): float(value) for sec, value in base_row.items()}
        for sector in selected_by_date.get(trade_date, set()):
            row[sector] = min(float(row.get(sector, 1.0)), float(penalty))
        out[trade_date] = row
    return out


def build_confirm_loop10(
    base_coeffs: dict[str, dict[str, float]],
    flags: pd.DataFrame,
) -> dict[str, dict[str, float]]:
    selected_by_date = flags_by_date(flags)
    out: dict[str, dict[str, float]] = {}
    for trade_date, base_row in base_coeffs.items():
        selected = selected_by_date.get(trade_date, set())
        row: dict[str, float] = {}
        for sector, value in base_row.items():
            base = float(value)
            row[str(sector)] = base if base >= 1.0 - 1e-12 or str(sector) in selected else 1.0
        out[trade_date] = row
    return out


def build_relax_good(
    base_coeffs: dict[str, dict[str, float]],
    good_flags: pd.DataFrame,
) -> dict[str, dict[str, float]]:
    good_by_date = flags_by_date(good_flags)
    out: dict[str, dict[str, float]] = {}
    for trade_date, base_row in base_coeffs.items():
        good = good_by_date.get(trade_date, set())
        row: dict[str, float] = {}
        for sector, value in base_row.items():
            base = float(value)
            row[str(sector)] = 1.0 if base < 1.0 - 1e-12 and str(sector) in good else base
        out[trade_date] = row
    return out


def build_tighten_existing(
    base_coeffs: dict[str, dict[str, float]],
    flags: pd.DataFrame,
    penalty: float,
) -> dict[str, dict[str, float]]:
    selected_by_date = flags_by_date(flags)
    out: dict[str, dict[str, float]] = {}
    for trade_date, base_row in base_coeffs.items():
        selected = selected_by_date.get(trade_date, set())
        row: dict[str, float] = {}
        for sector, value in base_row.items():
            base = float(value)
            row[str(sector)] = min(base, penalty) if base < 1.0 - 1e-12 and str(sector) in selected else base
        out[trade_date] = row
    return out


def screen_score(summary: pd.DataFrame, coeff_summary: pd.DataFrame) -> pd.DataFrame:
    out = summary.copy()
    coeff = coeff_summary[["candidate", "mean_abs_delta_vs_loop10", "candidate_lt_loop10_share", "candidate_gt_loop10_share"]]
    out = out.merge(coeff, on="candidate", how="left")
    for col in (
        "net_mean_db_ret_5d",
        "net_mean_db_ret_10d",
        "net_mean_db_ret_20d",
        "positive_net_label_day_ratio",
        "avg_entered_per_day",
        "changed_days",
        "mean_abs_delta_vs_loop10",
        "candidate_lt_loop10_share",
        "candidate_gt_loop10_share",
    ):
        if col not in out.columns:
            out[col] = np.nan
    coverage = (out["changed_days"].fillna(0).clip(lower=0, upper=60) / 60.0).replace(0, 0.2)
    turnover_penalty = 0.0004 * out["avg_entered_per_day"].fillna(0)
    churn_penalty = 0.05 * out["mean_abs_delta_vs_loop10"].fillna(0)
    relax_penalty = 0.001 * out["candidate_gt_loop10_share"].fillna(0)
    raw = (
        out["net_mean_db_ret_10d"].fillna(-1.0)
        + 0.45 * out["net_mean_db_ret_5d"].fillna(-1.0)
        + 0.15 * out["net_mean_db_ret_20d"].fillna(-1.0)
        + 0.003 * out["positive_net_label_day_ratio"].fillna(0)
        - turnover_penalty
        - churn_penalty
        - relax_penalty
    )
    out["raw_screen_score"] = raw
    out["robust_screen_score"] = np.where(raw > 0, raw * coverage, raw)
    return out


def write_report(
    path: Path,
    *,
    summary: pd.DataFrame,
    coeff_summary: pd.DataFrame,
    metadata: pd.DataFrame,
    source_stats: list[dict[str, Any]],
    tested_candidates: int,
) -> None:
    holdout = summary[summary["period"] == "holdout"].copy()
    holdout = screen_score(holdout, coeff_summary).sort_values("robust_screen_score", ascending=False)
    top = holdout.merge(metadata, on="candidate", how="left")

    def table(df: pd.DataFrame, cols: list[str], n: int = 30) -> str:
        if df.empty:
            return "```text\n(no rows)\n```"
        existing = [col for col in cols if col in df.columns]
        return "```text\n" + df[existing].head(n).to_string(index=False) + "\n```"

    lines = [
        "# Loop10 Conditional Sparse HMM Screen",
        "",
        f"- Baseline: `LOOP10_BASE__penalty_only_f096` / `{LOOP10_SNAPSHOT_ID}`",
        f"- Holdout split: `{HOLDOUT_START}`",
        f"- Tested candidates: `{tested_candidates}`",
        "- Candidate maps are script-level only; no HMM registry write and no QE submission.",
        "- Intent: keep Loop10 as anchor and only allow sparse/conditional HMM sector adjustments.",
        "",
        "## Source Score Panels",
        "",
        table(pd.DataFrame(source_stats), ["name", "score_col", "bad_side", "rows", "dates", "sectors", "note"], 20),
        "",
        "## Top Holdout Candidates vs Loop10",
        "",
        table(
            top,
            [
                "candidate",
                "family",
                "source",
                "pct",
                "penalty",
                "changed_days",
                "avg_entered_per_day",
                "net_mean_db_ret_5d",
                "net_mean_db_ret_10d",
                "net_mean_db_ret_20d",
                "positive_net_label_day_ratio",
                "mean_abs_delta_vs_loop10",
                "candidate_lt_loop10_share",
                "candidate_gt_loop10_share",
                "raw_screen_score",
                "robust_screen_score",
            ],
            40,
        ),
        "",
        "## Coefficient Delta",
        "",
        table(
            coeff_summary.sort_values("mean_abs_delta_vs_loop10"),
            [
                "candidate",
                "mean_abs_delta_vs_loop10",
                "candidate_lt_loop10_share",
                "candidate_gt_loop10_share",
                "candidate_penalty_share",
                "candidate_boost_share",
                "candidate_min",
                "candidate_max",
            ],
            40,
        ),
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", default=DEFAULT_TASK_ID)
    parser.add_argument("--hmm-diag-dir", type=Path, default=DEFAULT_HMM_DIAG_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--topk", type=int, default=50)
    parser.add_argument("--pcts", nargs="+", type=float, default=[0.05, 0.10, 0.15, 0.20])
    parser.add_argument("--penalties", nargs="+", type=float, default=[0.998, 0.9975, 0.995, 0.9925, 0.99])
    parser.add_argument("--tighten-penalties", nargs="+", type=float, default=[0.955, 0.95])
    parser.add_argument("--vote-thresholds", nargs="+", type=int, default=[2, 3])
    parser.add_argument("--persist-days", nargs="+", type=int, default=[2, 3])
    parser.add_argument("--max-candidates", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_coeffs = load_loop10_coefficients()
    pred_path, label_path = find_base_artifacts(args.hmm_diag_dir)
    pred_ser = pred_to_series(load_pickle(pred_path))
    label_ser = label_to_series(load_pickle(label_path))
    stock_sector_map = load_stock_sector_map(args.hmm_diag_dir)

    source_badness: dict[str, pd.DataFrame] = {}
    source_stats: list[dict[str, Any]] = []
    for source in SCORE_SOURCES:
        frame = read_score_panel(source.panel_path, source.score_col)
        badness = badness_from_score(frame, source)
        alias = source_alias(source)
        source_badness[alias] = badness
        source_stats.append(
            {
                "name": alias,
                "score_col": source.score_col,
                "bad_side": source.bad_side,
                "rows": int(len(frame)),
                "dates": int(frame["trade_date"].nunique()),
                "sectors": int(frame["sector_code"].nunique()),
                "note": source.note,
            }
        )

    candidate_specs: list[CandidateSpec] = []
    metadata_rows: list[dict[str, Any]] = []

    def add_spec(spec: CandidateSpec, meta: dict[str, Any]) -> None:
        if args.max_candidates and len(candidate_specs) >= args.max_candidates:
            return
        candidate_specs.append(spec)
        metadata_rows.append({"candidate": spec.name, "family": spec.family, "note": spec.note, **meta})

    primary_sources = ["TL", "FB", "FBT"]
    for alias in primary_sources:
        badness = source_badness[alias]
        for pct in args.pcts:
            flags = select_bad_flags(badness, pct=pct)
            for penalty in args.penalties:
                name = f"L10_ADD_{alias}_{pct_tag(pct)}_PEN_{pen_tag(penalty)}"
                add_spec(
                    CandidateSpec(name, "add_sparse_penalty", flags, penalty, f"Add sparse {alias} bad-sector penalties."),
                    {"source": alias, "pct": pct, "penalty": penalty, "persist_days": 1, "vote_threshold": None},
                )
            for tighten in args.tighten_penalties:
                name = f"L10_TIGHTEN_{alias}_{pct_tag(pct)}_PEN_{pen_tag(tighten)}"
                add_spec(
                    CandidateSpec(name, "tighten_existing_loop10", flags, tighten, f"Only deepen existing Loop10 penalties if {alias} confirms bad."),
                    {"source": alias, "pct": pct, "penalty": tighten, "persist_days": 1, "vote_threshold": None},
                )
            confirm_name = f"L10_CONFIRM_ONLY_{alias}_{pct_tag(pct)}"
            add_spec(
                CandidateSpec(confirm_name, "confirm_loop10_penalty", flags, None, f"Keep Loop10 penalties only when {alias} confirms bad."),
                {"source": alias, "pct": pct, "penalty": None, "persist_days": 1, "vote_threshold": None},
            )
            good_flags = select_good_flags(badness, pct=pct)
            relax_name = f"L10_RELAX_GOOD_{alias}_{pct_tag(pct)}"
            add_spec(
                CandidateSpec(relax_name, "relax_loop10_good", good_flags, None, f"Relax Loop10 penalties when {alias} strongly disagrees."),
                {"source": alias, "pct": pct, "penalty": None, "persist_days": 1, "vote_threshold": None},
            )
            for persist in args.persist_days:
                persistent_flags = select_bad_flags(badness, pct=pct, persist_days=persist)
                for penalty in args.penalties:
                    name = f"L10_ADD_{alias}_{pct_tag(pct)}_PERSIST{persist}_PEN_{pen_tag(penalty)}"
                    add_spec(
                        CandidateSpec(name, "persistent_add_sparse_penalty", persistent_flags, penalty, f"Add penalties only after {persist} consecutive {alias} bad signals."),
                        {"source": alias, "pct": pct, "penalty": penalty, "persist_days": persist, "vote_threshold": None},
                    )

    for pct in args.pcts:
        per_source = [select_bad_flags(source_badness[alias], pct=pct) for alias in source_badness]
        for threshold in args.vote_thresholds:
            flags = vote_flags(per_source, vote_threshold=threshold)
            for penalty in args.penalties:
                name = f"L10_ADD_VOTE{threshold}_{pct_tag(pct)}_PEN_{pen_tag(penalty)}"
                add_spec(
                    CandidateSpec(name, "vote_add_sparse_penalty", flags, penalty, f"Add penalty when at least {threshold} HMM sources agree."),
                    {"source": "ALL", "pct": pct, "penalty": penalty, "persist_days": None, "vote_threshold": threshold},
                )
            confirm_name = f"L10_CONFIRM_ONLY_VOTE{threshold}_{pct_tag(pct)}"
            add_spec(
                CandidateSpec(confirm_name, "vote_confirm_loop10_penalty", flags, None, f"Keep Loop10 penalties only when {threshold}+ HMM sources agree."),
                {"source": "ALL", "pct": pct, "penalty": None, "persist_days": None, "vote_threshold": threshold},
            )

    out_dir = args.output_dir / args.task_id
    coeff_dir = out_dir / "candidate_coefficients"
    if coeff_dir.exists():
        for old_path in coeff_dir.glob("L10_*.json"):
            old_path.unlink()
    coeff_dir.mkdir(parents=True, exist_ok=True)

    rep_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    coeff_rows: list[dict[str, Any]] = []
    built_candidates: dict[str, dict[str, dict[str, float]]] = {}

    for spec in candidate_specs:
        if spec.family in {"add_sparse_penalty", "persistent_add_sparse_penalty", "vote_add_sparse_penalty"}:
            coeffs = build_add_penalty(base_coeffs, spec.flags, float(spec.penalty))
        elif spec.family in {"confirm_loop10_penalty", "vote_confirm_loop10_penalty"}:
            coeffs = build_confirm_loop10(base_coeffs, spec.flags)
        elif spec.family == "relax_loop10_good":
            coeffs = build_relax_good(base_coeffs, spec.flags)
        elif spec.family == "tighten_existing_loop10":
            coeffs = build_tighten_existing(base_coeffs, spec.flags, float(spec.penalty))
        else:
            raise ValueError(f"unsupported candidate family: {spec.family}")

        built_candidates[spec.name] = coeffs
        write_coefficients(coeff_dir / f"{spec.name}.json", spec.name, coeffs)
        rep, day = pairwise_topk_attribution(
            pred_ser,
            label_ser,
            stock_sector_map,
            base_coeffs,
            coeffs,
            spec.name,
            args.topk,
        )
        rep_frames.append(rep)
        daily_frames.append(day)
        coeff_rows.append(coefficient_delta_summary(base_coeffs, coeffs, spec.name))

    replacements = pd.concat(rep_frames, ignore_index=True) if rep_frames else pd.DataFrame()
    daily = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    enriched = enrich_db_forward_returns(replacements, [5, 10, 20]) if not replacements.empty else replacements

    summary_rows: list[dict[str, Any]] = []
    for candidate in built_candidates:
        rep = enriched[enriched["candidate"] == candidate] if not enriched.empty else enriched
        day = daily[daily["candidate"] == candidate] if not daily.empty else daily
        summary_rows.extend(summarize_pairwise_periods(rep, day, candidate))

    summary = pd.DataFrame(summary_rows)
    coeff_summary = pd.DataFrame(coeff_rows)
    metadata = pd.DataFrame(metadata_rows)
    holdout_ranked = screen_score(summary[summary["period"] == "holdout"].copy(), coeff_summary).merge(
        metadata, on="candidate", how="left"
    ).sort_values("robust_screen_score", ascending=False)

    out_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "summary": out_dir / "conditional_sparse_topk_summary_vs_loop10.csv",
        "holdout_ranked": out_dir / "conditional_sparse_holdout_ranked.csv",
        "replacements": out_dir / "conditional_sparse_replacements_vs_loop10.csv",
        "daily": out_dir / "conditional_sparse_daily_vs_loop10.csv",
        "coefficient_delta": out_dir / "conditional_sparse_coefficient_delta_vs_loop10.csv",
        "metadata": out_dir / "conditional_sparse_candidate_metadata.csv",
        "sources": out_dir / "conditional_sparse_sources.json",
        "report": out_dir / "conditional_sparse_screen_report.md",
        "coeff_dir": coeff_dir,
    }
    summary.to_csv(files["summary"], index=False, encoding="utf-8-sig")
    holdout_ranked.to_csv(files["holdout_ranked"], index=False, encoding="utf-8-sig")
    enriched.to_csv(files["replacements"], index=False, encoding="utf-8-sig")
    daily.to_csv(files["daily"], index=False, encoding="utf-8-sig")
    coeff_summary.to_csv(files["coefficient_delta"], index=False, encoding="utf-8-sig")
    metadata.to_csv(files["metadata"], index=False, encoding="utf-8-sig")
    files["sources"].write_text(json.dumps(source_stats, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(
        files["report"],
        summary=summary,
        coeff_summary=coeff_summary,
        metadata=metadata,
        source_stats=source_stats,
        tested_candidates=len(candidate_specs),
    )

    print(
        json.dumps(
            {
                "task_id": args.task_id,
                "tested_candidates": len(candidate_specs),
                "output_dir": str(out_dir),
                "top_candidates": holdout_ranked.head(10)[
                    [
                        "candidate",
                        "family",
                        "source",
                        "pct",
                        "penalty",
                        "changed_days",
                        "avg_entered_per_day",
                        "net_mean_db_ret_10d",
                        "robust_screen_score",
                    ]
                ].to_dict(orient="records"),
                "files": {key: str(value) for key, value in files.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
