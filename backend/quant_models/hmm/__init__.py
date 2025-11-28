from __future__ import annotations

"""HMM model package (training and inference skeleton).

Planned according to quant_analyst_design.md §9.10. This package will contain
hidden Markov model training and inference logic used for regime detection in
Quant Analyst.
"""

from .train import HMMConfig
from .infer import infer_hmm_for_symbol

__all__ = [
    "HMMConfig",
    "infer_hmm_for_symbol",
]
