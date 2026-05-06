"""Screen Loop10-centered virtual HMM coefficient candidates before QE.

The script creates lightweight, unregistered coefficient maps from existing
Loop10 and sector-utility HMM artifacts, then reuses TopK attribution to decide
which variants deserve an expensive remote QE run.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.diagnostics.hmm_loop10_centered_attribution import (  # noqa: E402
    DEFAULT_HMM_DIAG_DIR,
    DEFAULT_REGISTRY,
    HOLDOUT_START,
    LOOP10_SNAPSHOT_ID,
    coefficient_delta_summary,
    load_coefficients,
    pairwise_topk_attribution,
    summarize_pairwise_periods,
)
from scripts.diagnostics.hmm_qe_candidate_attribution import (  # noqa: E402
    BASELINE_COEFFICIENTS,
    load_candidate_specs,
)
from scripts.diagnostics.hmm_sector_factor_overlay_diagnostic import (  # noqa: E402
    enrich_db_forward_returns,
    find_base_artifacts,
    label_to_series,
    load_pickle,
    load_stock_sector_map,
    pred_to_series,
)


DEFAULT_TASK_ID = "qe_20260504_184036_3a3c"
DEFAULT_OUTPUT_DIR = Path(".codex_tmp/hmm_loop10_virtual_candidate_screen")


def find_spec(
    specs: list[dict[str, Any]],
    *,
    snapshot_id: str | None = None,
    variant_name: str | None = None,
    label_contains: str | None = None,
) -> dict[str, Any]:
    for spec in specs:
        if snapshot_id and str(spec.get("snapshot_id")) == snapshot_id:
            return spec
        if variant_name and str(spec.get("variant_name")) == variant_name:
            return spec
        if label_contains and label_contains in str(spec.get("label")):
            return spec
    raise KeyError(
        "candidate spec not found: "
        f"snapshot_id={snapshot_id!r} variant_name={variant_name!r} label_contains={label_contains!r}"
    )


def all_date_sectors(*panels: dict[str, dict[str, float]]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for panel in panels:
        for date, row in panel.items():
            out.setdefault(date, set()).update(str(k) for k in row)
    return out


def build_from_two(
    base: dict[str, dict[str, float]],
    util: dict[str, dict[str, float]],
    mapper: Callable[[float, float, str, str], float],
) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for date, sectors in all_date_sectors(base, util).items():
        row: dict[str, float] = {}
        for sector in sectors:
            base_coeff = float(base.get(date, {}).get(sector, 1.0))
            util_coeff = float(util.get(date, {}).get(sector, 1.0))
            row[sector] = float(mapper(base_coeff, util_coeff, date, sector))
        out[date] = row
    return out


def risk_only(base: dict[str, dict[str, float]], util: dict[str, dict[str, float]], penalty: float) -> dict[str, dict[str, float]]:
    return build_from_two(
        base,
        util,
        lambda b, u, _d, _s: min(b, penalty if u < 1.0 else 1.0),
    )


def confirm_only(base: dict[str, dict[str, float]], util: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    return build_from_two(
        base,
        util,
        lambda b, u, _d, _s: b if (b < 1.0 and u < 1.0) else 1.0,
    )


def blend_clip(
    base: dict[str, dict[str, float]],
    util: dict[str, dict[str, float]],
    alpha: float,
    low: float,
    high: float,
) -> dict[str, dict[str, float]]:
    return build_from_two(
        base,
        util,
        lambda b, u, _d, _s: min(high, max(low, b + alpha * (u - 1.0))),
    )


def bottom_pct_penalty(
    base: dict[str, dict[str, float]],
    util: dict[str, dict[str, float]],
    pct: float,
    penalty: float,
) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for date, sectors in all_date_sectors(base, util).items():
        util_row = {sec: float(util.get(date, {}).get(sec, 1.0)) for sec in sectors}
        ordered = sorted(util_row, key=lambda sec: (util_row[sec], sec))
        n_bottom = max(1, int(np.ceil(len(ordered) * pct)))
        bottom = set(ordered[:n_bottom])
        row: dict[str, float] = {}
        for sector in sectors:
            b = float(base.get(date, {}).get(sector, 1.0))
            row[sector] = min(b, penalty) if sector in bottom else b
        out[date] = row
    return out


def write_coefficients(path: Path, candidate: str, coeffs: dict[str, dict[str, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": "loop10_virtual_candidate_screen_20260504",
        "candidate": candidate,
        "baseline_snapshot_id": LOOP10_SNAPSHOT_ID,
        "registered_for_qe": False,
        "daily_coefficients": coeffs,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_report(path: Path, summary: pd.DataFrame, coeff_summary: pd.DataFrame) -> None:
    holdout = summary[summary["period"] == "holdout"].copy()
    if not holdout.empty:
        holdout = holdout.sort_values("net_mean_db_ret_10d", ascending=False)
    coeff_view = coeff_summary.sort_values("mean_abs_delta_vs_loop10", ascending=True)

    def table(df: pd.DataFrame, cols: list[str]) -> str:
        if df.empty:
            return "(no rows)"
        existing = [c for c in cols if c in df.columns]
        return "```text\n" + df[existing].to_string(index=False) + "\n```"

    lines = [
        "# Loop10 Virtual HMM Candidate Screen",
        "",
        f"- Baseline: `LOOP10_BASE__penalty_only_f096` / `{LOOP10_SNAPSHOT_ID}`",
        f"- Holdout split: `{HOLDOUT_START}`",
        "- These candidates are not registered for QE; they are script-level filters only.",
        "",
        "## Holdout TopK Replacement Attribution vs Loop10",
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
            ],
        ),
        "",
        "## Coefficient Delta vs Loop10",
        "",
        table(
            coeff_view,
            [
                "candidate",
                "mean_abs_delta_vs_loop10",
                "candidate_gt_loop10_share",
                "candidate_lt_loop10_share",
                "candidate_penalty_share",
                "candidate_boost_share",
                "candidate_min",
                "candidate_max",
            ],
        ),
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", default=DEFAULT_TASK_ID)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--hmm-diag-dir", type=Path, default=DEFAULT_HMM_DIAG_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--topk", type=int, default=50)
    args = parser.parse_args()

    specs = load_candidate_specs(args.registry)
    base_spec = find_spec(specs, snapshot_id=LOOP10_SNAPSHOT_ID)
    fpb_valz = find_spec(specs, label_contains="FPB_N3_VALZ_AGG")
    fpb_cons = find_spec(specs, label_contains="FPB_N3_VALZ_CONS")
    volcomp = find_spec(specs, label_contains="VOLCOMP_N4_VALZ_AGG")

    base_coeffs = load_coefficients(Path(base_spec["coefficients_path"]))
    fpb_valz_coeffs = load_coefficients(Path(fpb_valz["coefficients_path"]))
    fpb_cons_coeffs = load_coefficients(Path(fpb_cons["coefficients_path"]))
    volcomp_coeffs = load_coefficients(Path(volcomp["coefficients_path"]))

    virtual_candidates: dict[str, dict[str, dict[str, float]]] = {
        "VIRT_L10_FPB_VALZ_CONFIRM_ONLY": confirm_only(base_coeffs, fpb_valz_coeffs),
        "VIRT_L10_FPB_CONS_CONFIRM_ONLY": confirm_only(base_coeffs, fpb_cons_coeffs),
    }
    for source_name, source_coeffs in (
        ("FPB_VALZ", fpb_valz_coeffs),
        ("VOLCOMP", volcomp_coeffs),
    ):
        for pct in (0.10, 0.15, 0.20, 0.25):
            pct_tag = f"P{int(round(pct * 100)):02d}"
            for penalty in (0.99, 0.985, 0.98):
                pen_tag = str(penalty).replace(".", "p")
                virtual_candidates[f"VIRT_L10_{source_name}_BOTTOM{pct_tag}_PENALTY_{pen_tag}"] = bottom_pct_penalty(
                    base_coeffs,
                    source_coeffs,
                    pct,
                    penalty,
                )
    for penalty in (0.995, 0.99, 0.985, 0.98):
        pen_tag = str(penalty).replace(".", "p")
        virtual_candidates[f"VIRT_L10_VOLCOMP_RISKONLY_{pen_tag}"] = risk_only(base_coeffs, volcomp_coeffs, penalty)
    for source_name, source_coeffs in (
        ("FPB_VALZ", fpb_valz_coeffs),
        ("VOLCOMP", volcomp_coeffs),
    ):
        for alpha in (0.1, 0.2):
            alpha_tag = str(alpha).replace(".", "p")
            virtual_candidates[f"VIRT_L10_{source_name}_BLEND_A{alpha_tag}_CLIP_0p96_1p01"] = blend_clip(
                base_coeffs,
                source_coeffs,
                alpha,
                0.96,
                1.01,
            )

    pred_path, label_path = find_base_artifacts(args.hmm_diag_dir)
    pred_ser = pred_to_series(load_pickle(pred_path))
    label_ser = label_to_series(load_pickle(label_path))
    stock_sector_map = load_stock_sector_map(args.hmm_diag_dir)

    out_dir = args.output_dir / args.task_id
    coeff_dir = out_dir / "candidate_coefficients"
    if coeff_dir.exists():
        # Avoid stale virtual maps from previous grids being mistaken as selected candidates.
        for old_path in coeff_dir.glob("VIRT_L10_*.json"):
            old_path.unlink()
    summary_rows: list[dict[str, Any]] = []
    rep_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    coeff_rows: list[dict[str, Any]] = []

    for candidate, coeffs in virtual_candidates.items():
        write_coefficients(coeff_dir / f"{candidate}.json", candidate, coeffs)
        rep, day = pairwise_topk_attribution(
            pred_ser,
            label_ser,
            stock_sector_map,
            base_coeffs,
            coeffs,
            candidate,
            args.topk,
        )
        enriched = enrich_db_forward_returns(rep, [5, 10, 20]) if not rep.empty else rep
        rep_frames.append(enriched)
        daily_frames.append(day)
        summary_rows.extend(summarize_pairwise_periods(enriched, day, candidate))
        coeff_rows.append(coefficient_delta_summary(base_coeffs, coeffs, candidate))

    summary = pd.DataFrame(summary_rows)
    replacements = pd.concat(rep_frames, ignore_index=True) if rep_frames else pd.DataFrame()
    daily = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    coeff_summary = pd.DataFrame(coeff_rows)

    out_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "summary": out_dir / "virtual_topk_summary_vs_loop10.csv",
        "replacements": out_dir / "virtual_topk_replacements_vs_loop10.csv",
        "daily": out_dir / "virtual_topk_daily_vs_loop10.csv",
        "coefficient_delta": out_dir / "virtual_coefficient_delta_vs_loop10.csv",
        "report": out_dir / "virtual_candidate_screen_report.md",
        "coeff_dir": coeff_dir,
    }
    summary.to_csv(files["summary"], index=False, encoding="utf-8-sig")
    replacements.to_csv(files["replacements"], index=False, encoding="utf-8-sig")
    daily.to_csv(files["daily"], index=False, encoding="utf-8-sig")
    coeff_summary.to_csv(files["coefficient_delta"], index=False, encoding="utf-8-sig")
    write_report(files["report"], summary, coeff_summary)

    print(
        json.dumps(
            {
                "task_id": args.task_id,
                "output_dir": str(out_dir),
                "files": {key: str(value) for key, value in files.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
