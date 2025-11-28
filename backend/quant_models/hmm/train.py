from __future__ import annotations

"""HMM training entry (skeleton).

Designed following quant_analyst_design.md §9.10:
- Train a shared HMM model on filtered daily return series;
- Write training metadata to app.model_train_run.

Currently only defines configuration and entrypoints; actual algorithms will
be implemented after the design is fully validated and data is ready.
"""

from dataclasses import dataclass, field
from typing import Any, Dict
import argparse
import json


@dataclass
class HMMConfig:
    """Configuration for HMM training.

    Fields are aligned with quant_analyst_design.md §9.10 and will be used
    together with app.model_config entries:
    - model_name: logical model name, e.g. "HMM_DAILY"；
    - freq: 主频率，目前固定为 "1d"；
    - history_years: 训练窗口长度（年）；
    - universe_name: Universe 配置名称，如 "ALL_EQ_CLEAN"；
    - n_states: HMM 隐状态数（如 3/4）。
    """
    model_name: str = "HMM_DAILY"
    freq: str = "1d"
    history_years: float = 3.0
    universe_name: str = "ALL_EQ_CLEAN"
    n_states: int = 3
    extra: Dict[str, Any] = field(default_factory=dict)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HMM training (skeleton)")
    parser.add_argument(
        "--config-json",
        type=str,
        default=None,
        help="JSON string representing HMMConfig-like payload",
    )
    return parser.parse_args()


def train_hmm_shared(config: HMMConfig) -> None:
    """Train a shared HMM model for the configured universe.

    Real implementation will follow quant_analyst_design.md §9.10 and write
    metadata to app.model_train_run. For now, this function only raises
    NotImplementedError.
    """

    raise NotImplementedError(
        "train_hmm_shared is not implemented yet; "
        "implement shared HMM training according to quant_analyst_design.md §9.10."
    )


def main() -> None:
    args = _parse_args()

    cfg_payload: Dict[str, Any] = {}
    if args.config_json:
        try:
            cfg_payload = json.loads(args.config_json)
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"invalid --config-json payload: {exc}") from exc

    config = HMMConfig(extra=cfg_payload)
    train_hmm_shared(config)


if __name__ == "__main__":  # pragma: no cover - CLI entry only
    main()
