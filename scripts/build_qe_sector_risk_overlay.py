"""Build an immutable QE-only sector-risk runtime artifact."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.quantevolver.backtest_base_data_memory_cache import (  # noqa: E402
    BacktestBaseDataMemoryCache,
)
from backend.services.quantevolver.sector_risk_overlay import (  # noqa: E402
    build_sector_risk_runtime,
    canonical_json_sha256,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_identity(path: Path) -> dict[str, object]:
    stat = path.stat()
    return {
        "name": path.name,
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "sha256": _sha256(path),
    }


def build(args: argparse.Namespace) -> dict[str, object]:
    root = Path(args.factor_data_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    history_start = (pd.Timestamp(args.start_date) - pd.Timedelta(days=args.history_calendar_days)).date().isoformat()
    cache = BacktestBaseDataMemoryCache.load_once(
        root,
        history_start,
        args.end_date,
        allowed_files=("daily_pv.h5", "sector_data.h5"),
    )
    result = build_sector_risk_runtime(
        cache.get("daily_pv.h5", columns=["close"]),
        cache.get(
            "sector_data.h5",
            columns=["sw2_close", "sw2_amount", "sw2_mf_net_amt", "l2_code_id"],
        ),
        output_start=args.start_date,
        output_end=args.end_date,
        dataset_identity=args.dataset_identity,
        minimum_mapped_rate=args.minimum_mapped_rate,
    )
    runtime_path = output_dir / "sector_risk_overlay.parquet"
    sector_path = output_dir / "sector_risk_sector_daily.parquet"
    manifest_path = output_dir / "sector_risk_overlay_manifest.json"
    result.runtime.to_parquet(runtime_path, index=False)
    result.sector_daily.to_parquet(sector_path, index=False)
    manifest: dict[str, object] = dict(result.summary)
    manifest.update(
        {
            "factor_data_root": str(root),
            "history_start": history_start,
            "source_files": {
                name: _file_identity(root / name) for name in ("daily_pv.h5", "sector_data.h5")
            },
            "artifacts": {
                "runtime": _file_identity(runtime_path),
                "sector_daily": _file_identity(sector_path),
            },
        }
    )
    manifest["manifest_payload_sha256"] = canonical_json_sha256(manifest)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {"manifest": str(manifest_path), **manifest}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--factor-data-dir", required=True)
    parser.add_argument("--dataset-identity", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--history-calendar-days", type=int, default=365)
    parser.add_argument("--minimum-mapped-rate", type=float, default=0.80)
    args = parser.parse_args()
    if args.history_calendar_days < 120:
        parser.error("--history-calendar-days must be >= 120")
    if not 0.0 <= args.minimum_mapped_rate <= 1.0:
        parser.error("--minimum-mapped-rate must be in [0, 1]")
    print(json.dumps(build(args), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
