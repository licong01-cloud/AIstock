"""HMM evolution research foundation.

This package is intentionally isolated from Selection, Paper Trading, QMT and
production model configuration.  Phase 1 services may inspect QE assets and
write only to the dedicated ``hmm_evolution`` schema.
"""

from .errors import HMMEvolutionError

__all__ = ["HMMEvolutionError"]
