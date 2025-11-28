from __future__ import annotations

"""ARIMA inference entry (skeleton).

This module defines the public interface for ARIMA-based forecasts used by
Quant Analyst, as described in quant_analyst_design.md §9.5 and §9.10.

Concrete ARIMA logic is intentionally left unimplemented at this stage to
avoid introducing any provisional or simplified behaviour.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List
import argparse
from datetime import datetime


@dataclass
class ARIMAPredictionConfig:
    """Configuration for ARIMA inference for a single symbol.

    The exact fields (orders, seasonal orders, etc.) will be aligned with
    ARIMAConfig and persisted configuration when the implementation is
    added.
    """

    model_name: str = "ARIMA_DAILY"
    extra: Dict[str, Any] = field(default_factory=dict)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ARIMA inference (skeleton)")
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
        help="frequency for ARIMA forecasts (default: 1d)",
    )
    parser.add_argument(
        "--horizon",
        action="append",
        default=None,
        help="forecast horizons like next_1d, next_3d; may be repeated",
    )
    return parser.parse_args()


def infer_arima_for_symbol(
    ts_code: str,
    as_of_time: datetime,
    freq: str,
    horizons: List[str],
    config: ARIMAPredictionConfig,
) -> Any:
    """Run ARIMA inference for a single symbol.

    The real implementation will return an ARIMAOutput instance as defined in
    quant_analyst_service; here we only define the call surface and raise
    NotImplementedError when used.
    """

    raise NotImplementedError(
        "infer_arima_for_symbol is not implemented yet; "
        "implement ARIMA inference according to quant_analyst_design.md §9.5/§9.10."
    )


def main() -> None:
    args = _parse_args()

    as_of = datetime.fromisoformat(args.as_of) if args.as_of else datetime.utcnow()
    horizons: List[str] = args.horizon or ["next_1d"]
    cfg = ARIMAPredictionConfig()
    infer_arima_for_symbol(
        ts_code=args.symbol,
        as_of_time=as_of,
        freq=args.freq,
        horizons=horizons,
        config=cfg,
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry only
    main()
