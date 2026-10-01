"""Candidate-only daily_basic source-history repair (no database writes)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from backend.services.dataset_release.daily_basic_history_repair import repair_daily_basic_history
from backend.services.dataset_release.index_sources import independent_postgres_connection_factory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("baseline", "candidate", "manifest", "generation", "revision", "diagnostic", "dotenv"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--resume-cloned", action="store_true", help="Resume only a verified, unmodified baseline clone")
    args = parser.parse_args()
    load_dotenv(args.dotenv, override=False)
    result = repair_daily_basic_history(
        baseline_root=Path(args.baseline), candidate_root=Path(args.candidate), expected_manifest=args.manifest,
        generation=args.generation, revision=args.revision,
        diagnostic=json.loads(Path(args.diagnostic).read_text(encoding="utf-8-sig")),
        connection_factory=independent_postgres_connection_factory,
        resume_cloned=args.resume_cloned,
        progress=lambda value: print(json.dumps(value, ensure_ascii=True), flush=True),
    )
    print(json.dumps(result, ensure_ascii=True), flush=True)


if __name__ == "__main__":
    main()
