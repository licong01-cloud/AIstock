from __future__ import annotations

"""HMM inference entry (skeleton).

Implements the public interface for regime detection used by Quant Analyst,
following quant_analyst_design.md §9.6/§9.10.

Concrete HMM logic will be added later; for now all programmatic entrypoints
raise NotImplementedError when invoked.
"""

from dataclasses import dataclass, field
from typing import Any, Dict
import argparse
from datetime import datetime


@dataclass
class HMMPredictionConfig:
    """Configuration for HMM inference for a single symbol."""

    model_name: str = "HMM_DAILY"
    extra: Dict[str, Any] = field(default_factory=dict)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HMM inference (skeleton)")
    parser.add_argument("--symbol", required=True, help="ts_code, e.g. 600000.SH")
    parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="as-of datetime in ISO format, e.g. 2024-01-01T15:00:00",
    )
    parser.add_argument(
        "--freq",
        type=str,
        default="1d",
        help="frequency for HMM regime detection (default: 1d)",
    )
    return parser.parse_args()


def infer_hmm_for_symbol(
    ts_code: str,
    as_of_time: datetime,
    freq: str,
    config: HMMPredictionConfig,
) -> Any:
    """Run HMM inference for a single symbol.

    The real implementation will return an HMMOutput instance as defined in
    quant_analyst_service; this skeleton only defines the interface.
    """

    raise NotImplementedError(
        "infer_hmm_for_symbol is not implemented yet; "
        "implement HMM inference according to quant_analyst_design.md §9.6/§9.10."
    )


def main() -> None:
    args = _parse_args()

    as_of = datetime.fromisoformat(args.as_of) if args.as_of else datetime.utcnow()
    cfg = HMMPredictionConfig()
    infer_hmm_for_symbol(
        ts_code=args.symbol,
        as_of_time=as_of,
        freq=args.freq,
        config=cfg,
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry only
    main()
