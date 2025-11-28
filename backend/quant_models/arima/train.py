from __future__ import annotations

"""ARIMA training entry (skeleton).

Planned according to quant_analyst_design.md §9.10:
- Read configuration from app.model_config / app.model_schedule (via CLI
  parameters provided by backend.model_scheduler.scheduler).
- Train per-symbol or shared ARIMA models on daily data from TimescaleDB.
- Record runs in app.model_train_run.

This file currently only defines the structure and raises NotImplementedError
so that no simplified training logic is accidentally introduced.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple
import argparse
import json


@dataclass
class ARIMAConfig:
    """Configuration for ARIMA training.

    The concrete fields are aligned with quant_analyst_design.md §9.10 and
    app.model_config.model_name/Config JSON:
    - model_name: logical name, e.g. "ARIMA_DAILY"；
    - freq: 主频率，目前固定为 "1d"；
    - history_years: 训练窗口长度（单位：年）；
    - universe_name: Universe 配置名称，如 "ALL_EQ_CLEAN"；
    - order: ARIMA(p,d,q)；
    - seasonal_order: 可选季节项 (P,D,Q,s)。
    """
    model_name: str = "ARIMA_DAILY"
    freq: str = "1d"
    history_years: float = 3.0
    universe_name: str = "ALL_EQ_CLEAN"
    order: Tuple[int, int, int] = (1, 1, 1)
    seasonal_order: Optional[Tuple[int, int, int, int]] = None
    extra: Dict[str, Any] = field(default_factory=dict)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ARIMA training (skeleton)")
    parser.add_argument(
        "--config-json",
        type=str,
        default=None,
        help=(
            "JSON string representing ARIMAConfig-like payload; in real "
            "implementation this will come from app.model_schedule.config_json"
        ),
    )
    return parser.parse_args()


def train_arima_for_universe(config: ARIMAConfig) -> None:
    """Train ARIMA models for the configured universe.

    This function is the programmatic entrypoint to be used by the CLI and
    by potential higher-level orchestration. It is intentionally left
    unimplemented until the full ARIMA training procedure (including
    parameter selection and backtesting) is finalized.
    """

    raise NotImplementedError(
        "train_arima_for_universe is not implemented yet; "
        "implement ARIMA training according to quant_analyst_design.md §9.10."
    )


def main() -> None:
    args = _parse_args()

    cfg_payload: Dict[str, Any] = {}
    if args.config_json:
        try:
            cfg_payload = json.loads(args.config_json)
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"invalid --config-json payload: {exc}") from exc

    config = ARIMAConfig(extra=cfg_payload)
    # The following call is expected to be replaced by the real
    # implementation when ARIMA training is ready.
    train_arima_for_universe(config)


if __name__ == "__main__":  # pragma: no cover - CLI entry only
    main()
