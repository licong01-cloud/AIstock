"""Register Loop10 conditional-sparse HMM candidates as hidden QE snapshots.

The candidate coefficient maps are produced by
``scripts/hmm_loop10_conditional_sparse_screen_20260506.py``.  This wrapper
reuses the existing protected-asset registration implementation, but points it
at the 2026-05-06 conditional-sparse coefficient directory and stores the
configs under a hidden model_type so the normal QE HMM selector stays clean.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import register_hmm_stage3_sparse_qe_candidates_20260505 as base


ROOT = Path(__file__).resolve().parents[1]
SOURCE_QE_TASK = "qe_20260505_210355_155f"
HIDDEN_MODEL_TYPE = "sector_hmm_experimental_l10_conditional_sparse_20260506"
TARGET_DATE_FOLDER = "2026-05-06"
CONDITIONAL_COEFF_ROOT = (
    ROOT
    / ".codex_tmp"
    / "hmm_loop10_conditional_sparse_screen_20260506"
    / SOURCE_QE_TASK
    / "candidate_coefficients"
)

CANDIDATES: list[dict[str, Any]] = [
    {
        "display_name": "HMM_TEST_L10_TIGHTEN_FBT_P15_PEN_0p955__qe20260506",
        "variant_name": "l10_tighten_fbt_p15_pen_0p955",
        "virtual_coeff_filename": "L10_TIGHTEN_FBT_P15_PEN_0p955.json",
        "hypothesis": "Loop10 anchored candidate: only deepen existing Loop10 penalties to 0.955 when Stage3 FBT confirms the bottom 15% bad sectors.",
    },
    {
        "display_name": "HMM_TEST_L10_TIGHTEN_TL_P15_PEN_0p95__qe20260506",
        "variant_name": "l10_tighten_tl_p15_pen_0p95",
        "virtual_coeff_filename": "L10_TIGHTEN_TL_P15_PEN_0p95.json",
        "hypothesis": "Loop10 anchored candidate: only deepen existing Loop10 penalties to 0.95 when Stage3 turnover-light confirms the bottom 15% bad sectors.",
    },
    {
        "display_name": "HMM_TEST_L10_TIGHTEN_FB_P15_PEN_0p95__qe20260506",
        "variant_name": "l10_tighten_fb_p15_pen_0p95",
        "virtual_coeff_filename": "L10_TIGHTEN_FB_P15_PEN_0p95.json",
        "hypothesis": "Loop10 anchored candidate: only deepen existing Loop10 penalties to 0.95 when Stage3 flow-breadth confirms the bottom 15% bad sectors.",
    },
]


def json_default(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def configure_base_module() -> None:
    base.SOURCE_QE_TASK = SOURCE_QE_TASK
    base.HIDDEN_MODEL_TYPE = HIDDEN_MODEL_TYPE
    base.TARGET_DATE_FOLDER = TARGET_DATE_FOLDER
    base.SPARSE_COEFF_ROOT = CONDITIONAL_COEFF_ROOT


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--candidates-json", type=Path, default=None)
    parser.add_argument("--coeff-root", type=Path, default=CONDITIONAL_COEFF_ROOT)
    parser.add_argument("--source-qe-task", default=SOURCE_QE_TASK)
    args = parser.parse_args()

    candidates = CANDIDATES
    if args.candidates_json:
        raw = json.loads(args.candidates_json.read_text(encoding="utf-8-sig"))
        candidates = raw.get("candidates") if isinstance(raw, dict) else raw
        if not isinstance(candidates, list) or not candidates:
            raise RuntimeError("--candidates-json must contain a non-empty list or {'candidates': [...]}")

    configure_base_module()
    base.SOURCE_QE_TASK = args.source_qe_task
    base.SPARSE_COEFF_ROOT = args.coeff_root

    missing = [
        str(args.coeff_root / c["virtual_coeff_filename"])
        for c in candidates
        if not (args.coeff_root / c["virtual_coeff_filename"]).is_file()
    ]
    if missing:
        raise RuntimeError("missing conditional-sparse coefficient files: " + "; ".join(missing))

    result = base.register_candidates(dry_run=args.dry_run, candidate_specs=candidates)
    result["wrapper"] = {
        "registered_by": "scripts/register_hmm_loop10_conditional_sparse_qe_candidates_20260506.py",
        "hidden_model_type": HIDDEN_MODEL_TYPE,
        "conditional_coeff_root": str(args.coeff_root.resolve()),
        "source_qe_task": args.source_qe_task,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=json_default))


if __name__ == "__main__":
    main()
