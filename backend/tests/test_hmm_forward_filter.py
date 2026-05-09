from __future__ import annotations

from datetime import date

import numpy as np
from hmmlearn.hmm import GaussianHMM

from backend.quant_models.hmm.sector_hmm import SectorHMMInference


def test_decode_state_uses_hmmlearn_forward_log_probability_inputs() -> None:
    """hmmlearn._hmmc.forward_log expects probability matrices, not log matrices."""

    hmm = GaussianHMM(n_components=2, covariance_type="diag")
    hmm.startprob_ = np.array([0.5, 0.5], dtype=np.float64)
    hmm.transmat_ = np.array([[0.90, 0.10], [0.05, 0.95]], dtype=np.float64)
    hmm.means_ = np.array([[0.0], [3.0]], dtype=np.float64)
    hmm.covars_ = np.array([[0.05], [0.05]], dtype=np.float64)

    inference = SectorHMMInference.__new__(SectorHMMInference)
    inference._hmm_models = {"801010.SI": hmm}
    inference._state_labels = {"801010.SI": {"0": "cold", "1": "hot"}}
    inference._build_obs_up_to = lambda sector_code, trade_date: np.array(
        [[2.8], [3.1], [3.2]], dtype=np.float64
    )

    assert inference._decode_state("801010.SI", date(2026, 5, 8)) == "hot"
