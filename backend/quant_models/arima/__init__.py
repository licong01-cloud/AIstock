from __future__ import annotations

"""ARIMA model package (training and inference skeleton).

This package is planned according to quant_analyst_design.md §9.10 and will
host ARIMA training and inference logic for the new backend. At this stage it
only defines module structure and public symbols; concrete algorithms are
implemented later together with full backtesting and validation.
"""

from .train import ARIMAConfig
from .infer import infer_arima_for_symbol

__all__ = [
    "ARIMAConfig",
    "infer_arima_for_symbol",
]
