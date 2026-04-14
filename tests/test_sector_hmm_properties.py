"""Property-based tests for SectorHMMTrainer.

Feature: p4-p1-p5-strategy-enhancement
Uses hypothesis to verify correctness properties of the sector HMM trainer.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict
from unittest.mock import patch, MagicMock

import numpy as np
import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Helpers – generate fake DB data matching the dict-based return types
# ---------------------------------------------------------------------------

def _make_trading_dates(n_days: int, start: date = date(2022, 1, 4)):
    """Return a sorted list of *n_days* consecutive weekday dates."""
    dates = []
    current = start
    while len(dates) < n_days:
        if current.weekday() < 5:  # Mon-Fri
            dates.append(current)
        current += timedelta(days=1)
    return dates


@st.composite
def sector_hmm_data(draw):
    """Hypothesis strategy that generates aligned sector / CSI300 / volume / limit-up data.

    Returns a dict with keys:
        n_days, trading_dates, sector_daily, csi300_daily, market_volume, limit_up
    """
    n_days = draw(st.integers(min_value=50, max_value=150))
    trading_dates = _make_trading_dates(n_days)

    # --- sector daily data (keyed by date) ---
    sector_daily: Dict[date, Dict[str, float]] = {}
    for td in trading_dates:
        pct = draw(st.floats(min_value=-10.0, max_value=10.0,
                             allow_nan=False, allow_infinity=False))
        vol = draw(st.floats(min_value=1.0, max_value=1e9,
                             allow_nan=False, allow_infinity=False))
        sector_daily[td] = {
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 100.0,
            "vol": vol,
            "amount": vol * 10.0,
            "pct_change": pct,
        }

    # --- CSI 300 daily data (keyed by date, same dates) ---
    csi300_daily: Dict[date, Dict[str, float]] = {}
    for td in trading_dates:
        csi_pct = draw(st.floats(min_value=-10.0, max_value=10.0,
                                 allow_nan=False, allow_infinity=False))
        csi300_daily[td] = {
            "close": 4000.0,
            "pct_change": csi_pct,
        }

    # --- market volume (keyed by date) ---
    # Total market volume must be > 0 and >= sector volume
    market_volume: Dict[date, float] = {}
    for td in trading_dates:
        sec_vol = sector_daily[td]["vol"]
        extra = draw(st.floats(min_value=1.0, max_value=1e10,
                               allow_nan=False, allow_infinity=False))
        market_volume[td] = sec_vol + extra

    # --- limit-up data (keyed by date) ---
    limit_up: Dict[date, Dict[str, float]] = {}
    for td in trading_dates:
        total_stocks = draw(st.integers(min_value=1, max_value=500))
        lu_cnt = draw(st.integers(min_value=0, max_value=total_stocks))
        limit_up[td] = {
            "limit_up": float(lu_cnt),
            "total": float(total_stocks),
        }

    return {
        "n_days": n_days,
        "trading_dates": trading_dates,
        "sector_daily": sector_daily,
        "csi300_daily": csi300_daily,
        "market_volume": market_volume,
        "limit_up": limit_up,
    }


# ---------------------------------------------------------------------------
# Property 5: HMM 观测矩阵结构
# Feature: p4-p1-p5-strategy-enhancement, Property 5: HMM 观测矩阵结构
# Validates: Requirements 4.2
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(data=sector_hmm_data())
def test_property_5_hmm_observation_matrix_structure(data):
    """**Validates: Requirements 4.2**

    For any Shenwan L1 sector, _build_observation_matrix() must return a matrix
    with exactly 4 columns, row count <= number of trading days, and no NaN.
    """
    from backend.quant_models.hmm.sector_hmm import SectorHMMTrainer, SectorHMMConfig

    config = SectorHMMConfig()
    # Use a no-op db_conn_factory; we will mock the private query methods
    trainer = SectorHMMTrainer(config=config, db_conn_factory=lambda: MagicMock())

    sector_code = "801010.SI"

    # Patch the four private query methods to return hypothesis-generated data
    with patch.object(trainer, "_query_sector_daily", return_value=data["sector_daily"]), \
         patch.object(trainer, "_query_csi300_daily", return_value=data["csi300_daily"]), \
         patch.object(trainer, "_query_market_volume", return_value=data["market_volume"]), \
         patch.object(trainer, "_query_limit_up", return_value=data["limit_up"]):

        obs = trainer._build_observation_matrix(sector_code)

    # --- Assertions ---
    # 1. Result must be a 2-D numpy array
    assert isinstance(obs, np.ndarray), "Observation matrix must be a numpy array"
    assert obs.ndim == 2, f"Expected 2-D array, got {obs.ndim}-D"

    # 2. Exactly 4 columns
    assert obs.shape[1] == 4, f"Expected 4 columns, got {obs.shape[1]}"

    # 3. Row count must not exceed the number of trading days generated
    assert obs.shape[0] <= data["n_days"], (
        f"Row count {obs.shape[0]} exceeds trading day count {data['n_days']}"
    )

    # 4. No NaN values
    assert not np.any(np.isnan(obs)), "Observation matrix must not contain NaN"


# ---------------------------------------------------------------------------
# Helpers – generate valid HMM parameters
# ---------------------------------------------------------------------------

@st.composite
def hmm_model_params(draw):
    """Hypothesis strategy that generates a dict of random HMM model parameters.

    Generates 1-5 sectors, each with:
        - transmat: 2x2 transition matrix (rows sum to 1)
        - means: 2x4 means vectors
        - covars: 2x4x4 positive semi-definite covariance matrices
        - state_labels: mapping of state index to "trending"/"fading"
    """
    n_sectors = draw(st.integers(min_value=1, max_value=5))
    models: Dict[str, Any] = {}

    for i in range(n_sectors):
        sector_code = f"80{1010 + i}.SI"

        # --- Transition matrix: 2x2, rows sum to 1 ---
        row0_p = draw(st.floats(min_value=0.01, max_value=0.99,
                                allow_nan=False, allow_infinity=False))
        row1_p = draw(st.floats(min_value=0.01, max_value=0.99,
                                allow_nan=False, allow_infinity=False))
        transmat = [[row0_p, 1.0 - row0_p],
                     [row1_p, 1.0 - row1_p]]

        # --- Means: 2x4 ---
        means = []
        for _ in range(2):
            row = [draw(st.floats(min_value=-1.0, max_value=1.0,
                                  allow_nan=False, allow_infinity=False))
                   for _ in range(4)]
            means.append(row)

        # --- Covars: 2x4x4 positive semi-definite ---
        covars = []
        for _ in range(2):
            # Generate a random 4x4 matrix, then make it PSD via A @ A^T + eps*I
            raw = np.array([
                [draw(st.floats(min_value=-2.0, max_value=2.0,
                                allow_nan=False, allow_infinity=False))
                 for _ in range(4)]
                for _ in range(4)
            ])
            psd = (raw @ raw.T) + 0.01 * np.eye(4)
            covars.append(psd.tolist())

        # --- State labels ---
        label_order = draw(st.sampled_from([
            {"0": "trending", "1": "fading"},
            {"0": "fading", "1": "trending"},
        ]))

        models[sector_code] = {
            "sector_code": sector_code,
            "sector_name": f"行业{i}",
            "n_states": 2,
            "transmat": transmat,
            "means": means,
            "covars": covars,
            "state_labels": label_order,
            "trained_at": "2025-01-15T10:00:00",
            "training_days": 730,
        }

    return models


# ---------------------------------------------------------------------------
# Property 6: HMM 模型持久化往返
# Feature: p4-p1-p5-strategy-enhancement, Property 6: HMM 模型持久化往返
# Validates: Requirements 4.4
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(models=hmm_model_params())
def test_property_6_hmm_model_persistence_round_trip(models, tmp_path_factory):
    """**Validates: Requirements 4.4**

    For any set of trained HMM model parameters, save_models() followed by
    load_models() must produce numerically equivalent results: element-wise
    difference in transmat, means, and covars must be < 1e-10.
    """
    from backend.quant_models.hmm.sector_hmm import SectorHMMTrainer

    # Use a unique temp file for each example
    tmp_dir = tmp_path_factory.mktemp("hmm_persist")
    path = str(tmp_dir / "test_models.json")

    # Save then load
    SectorHMMTrainer.save_models(models, path)
    loaded = SectorHMMTrainer.load_models(path)

    # --- Assertions ---
    # 1. Same set of sector codes
    assert set(loaded.keys()) == set(models.keys()), (
        f"Sector codes mismatch: saved={set(models.keys())}, loaded={set(loaded.keys())}"
    )

    for sector_code in models:
        orig = models[sector_code]
        recv = loaded[sector_code]

        # 2. Transition matrix round-trip
        orig_transmat = np.array(orig["transmat"], dtype=np.float64)
        recv_transmat = np.array(recv["transmat"], dtype=np.float64)
        assert orig_transmat.shape == recv_transmat.shape, (
            f"[{sector_code}] transmat shape mismatch"
        )
        diff_transmat = np.max(np.abs(orig_transmat - recv_transmat))
        assert diff_transmat < 1e-10, (
            f"[{sector_code}] transmat max diff = {diff_transmat}"
        )

        # 3. Means round-trip
        orig_means = np.array(orig["means"], dtype=np.float64)
        recv_means = np.array(recv["means"], dtype=np.float64)
        assert orig_means.shape == recv_means.shape, (
            f"[{sector_code}] means shape mismatch"
        )
        diff_means = np.max(np.abs(orig_means - recv_means))
        assert diff_means < 1e-10, (
            f"[{sector_code}] means max diff = {diff_means}"
        )

        # 4. Covariance matrices round-trip
        orig_covars = np.array(orig["covars"], dtype=np.float64)
        recv_covars = np.array(recv["covars"], dtype=np.float64)
        assert orig_covars.shape == recv_covars.shape, (
            f"[{sector_code}] covars shape mismatch"
        )
        diff_covars = np.max(np.abs(orig_covars - recv_covars))
        assert diff_covars < 1e-10, (
            f"[{sector_code}] covars max diff = {diff_covars}"
        )

        # 5. State labels preserved exactly
        assert recv["state_labels"] == orig["state_labels"], (
            f"[{sector_code}] state_labels mismatch: "
            f"orig={orig['state_labels']}, loaded={recv['state_labels']}"
        )


# ---------------------------------------------------------------------------
# Property 7: HMM 状态标记一致性
# Feature: p4-p1-p5-strategy-enhancement, Property 7: HMM 状态标记一致性
# Validates: Requirements 4.6
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(
    mean0=st.lists(
        st.floats(min_value=-1.0, max_value=1.0, allow_nan=False, allow_infinity=False),
        min_size=4, max_size=4,
    ),
    mean1=st.lists(
        st.floats(min_value=-1.0, max_value=1.0, allow_nan=False, allow_infinity=False),
        min_size=4, max_size=4,
    ),
)
def test_property_7_hmm_state_labeling_consistency(mean0, mean1):
    """**Validates: Requirements 4.6**

    For any 2-state HMM model, the state labeled "trending" must have a
    strictly greater daily return component (column 0) than the state
    labeled "fading".
    """
    from backend.quant_models.hmm.sector_hmm import SectorHMMTrainer

    # Ensure the two daily return components are different to avoid ambiguity
    assume(mean0[0] != mean1[0])

    # Build a mock HMM model with means_ attribute
    mock_model = MagicMock()
    mock_model.means_ = np.array([mean0, mean1], dtype=np.float64)  # shape (2, 4)

    labels = SectorHMMTrainer._label_states(mock_model)

    # Find which state index is "trending" and which is "fading"
    trending_idx = [k for k, v in labels.items() if v == "trending"]
    fading_idx = [k for k, v in labels.items() if v == "fading"]

    assert len(trending_idx) == 1, f"Expected exactly one trending state, got {trending_idx}"
    assert len(fading_idx) == 1, f"Expected exactly one fading state, got {fading_idx}"

    trending_daily_return = mock_model.means_[trending_idx[0], 0]
    fading_daily_return = mock_model.means_[fading_idx[0], 0]

    assert trending_daily_return > fading_daily_return, (
        f"Trending state daily return ({trending_daily_return}) must be strictly "
        f"greater than fading state daily return ({fading_daily_return})"
    )


# ---------------------------------------------------------------------------
# Property 8: HMM 推断输出有效性
# Feature: p4-p1-p5-strategy-enhancement, Property 8: HMM 推断输出有效性
# Validates: Requirements 5.1, 5.2, 5.3
# ---------------------------------------------------------------------------

@st.composite
def obs_matrix_for_sectors(draw, n_sectors: int):
    """Generate a dict mapping sector codes to random (T, 4) observation matrices.

    Each matrix has between 10 and 50 rows (enough for Viterbi decoding)
    with finite, non-NaN values.
    """
    matrices: Dict[str, Any] = {}
    for i in range(n_sectors):
        sector_code = f"80{1010 + i}.SI"
        n_rows = draw(st.integers(min_value=10, max_value=50))
        rows = []
        for _ in range(n_rows):
            row = [
                draw(st.floats(min_value=-0.1, max_value=0.1,
                               allow_nan=False, allow_infinity=False))
                for _ in range(4)
            ]
            rows.append(row)
        matrices[sector_code] = np.array(rows, dtype=np.float64)
    return matrices


@settings(max_examples=50, deadline=None)
@given(models=hmm_model_params())
def test_property_8_hmm_inference_output_validity(models, tmp_path_factory):
    """**Validates: Requirements 5.1, 5.2, 5.3**

    For any set of trained HMM model parameters and any trade date,
    get_sector_coefficients() must return a dict where:
    - Every value is one of {0.5, 1.0, 1.5}
    - Every trained sector code appears as a key
    """
    from backend.quant_models.hmm.sector_hmm import (
        SectorHMMInference, SectorHMMTrainer, SectorHMMConfig,
    )

    # Persist models to a temp file
    tmp_dir = tmp_path_factory.mktemp("hmm_infer")
    model_path = str(tmp_dir / "models.json")
    SectorHMMTrainer.save_models(models, model_path)

    # Create inference instance (reconstructs real GaussianHMM objects)
    config = SectorHMMConfig()
    inference = SectorHMMInference(
        model_path=model_path,
        config=config,
        db_conn_factory=lambda: MagicMock(),
    )

    # Build per-sector observation matrices to feed into Viterbi decode.
    # We generate a random (T, 4) matrix for each sector so that
    # hmm.decode() can run without DB access.
    sector_codes = list(models.keys())
    n_sectors = len(sector_codes)

    # Create random observation matrices (enough rows for Viterbi)
    obs_by_sector: Dict[str, np.ndarray] = {}
    rng = np.random.RandomState(42)
    for sc in sector_codes:
        n_rows = rng.randint(10, 51)
        obs_by_sector[sc] = rng.randn(n_rows, 4) * 0.05  # small values

    # Mock _build_obs_up_to to return our pre-built matrices
    def mock_build_obs(sector_code, trade_date):
        return obs_by_sector[sector_code]

    trade_date = date(2025, 6, 15)

    with patch.object(inference, "_build_obs_up_to", side_effect=mock_build_obs):
        coefficients = inference.get_sector_coefficients(trade_date)

    # --- Assertions ---
    valid_coeffs = {config.fading_coeff, config.neutral_coeff, config.trending_coeff}
    # i.e. {0.5, 1.0, 1.5}

    # 1. All trained sector codes must appear as keys
    for sc in sector_codes:
        assert sc in coefficients, (
            f"Trained sector {sc} missing from coefficients dict. "
            f"Keys: {list(coefficients.keys())}"
        )

    # 2. Every value must be one of {0.5, 1.0, 1.5}
    for sc, coeff in coefficients.items():
        assert coeff in valid_coeffs, (
            f"Sector {sc} has coefficient {coeff}, "
            f"expected one of {valid_coeffs}"
        )


# ---------------------------------------------------------------------------
# Property 9: HMM 冷却期行为
# Feature: p4-p1-p5-strategy-enhancement, Property 9: HMM 冷却期行为
# Validates: Requirements 5.4
# ---------------------------------------------------------------------------

@settings(max_examples=100, deadline=None)
@given(
    cooldown_days=st.integers(min_value=2, max_value=10),
    initial_state=st.sampled_from(["trending", "fading"]),
    days_before_first_switch=st.integers(min_value=1, max_value=20),
    extra_days_after_cooldown=st.integers(min_value=1, max_value=5),
)
def test_property_9_hmm_cooldown_behavior(
    cooldown_days,
    initial_state,
    days_before_first_switch,
    extra_days_after_cooldown,
    tmp_path_factory,
):
    """**Validates: Requirements 5.4**

    Feature: p4-p1-p5-strategy-enhancement, Property 9: HMM 冷却期行为

    For any sector, after a state switch occurs, if the decoded state tries
    to switch again within cooldown_days calendar days, the heat coefficient
    must remain at the post-first-switch value (i.e. the second switch is
    blocked by cooldown). After the cooldown expires, the second switch
    takes effect.

    The implementation uses ``(trade_date - last_switch_date).days < cooldown_days``
    to decide if cooldown is active. Since Phase C starts 1 calendar day
    after the first switch, the number of Phase C days that are blocked is
    ``cooldown_days - 1`` (because the gap starts at 1, not 0).

    Test sequence:
    1. Establish initial_state for enough days that cooldown from cache
       creation expires (days_before_first_switch >= cooldown_days).
    2. Switch to switched_state → takes effect immediately (first switch).
    3. On the very next day, Viterbi returns initial_state again (attempt
       second switch back). During cooldown, the coefficient must stay at
       the first-switch value.
    4. After cooldown expires, the second switch takes effect.
    """
    from backend.quant_models.hmm.sector_hmm import (
        SectorHMMInference, SectorHMMTrainer, SectorHMMConfig,
    )

    # Ensure enough days before first switch so cooldown from cache creation
    # has expired, allowing the first switch to take effect immediately.
    assume(days_before_first_switch >= cooldown_days)

    switched_state = "fading" if initial_state == "trending" else "trending"

    config = SectorHMMConfig(cooldown_days=cooldown_days)

    def _coeff_for(state: str) -> float:
        if state == "trending":
            return config.trending_coeff
        elif state == "fading":
            return config.fading_coeff
        return config.neutral_coeff

    initial_coeff = _coeff_for(initial_state)
    switched_coeff = _coeff_for(switched_state)

    # Build a minimal single-sector model and persist it
    sector_code = "801010.SI"
    models = {
        sector_code: {
            "sector_code": sector_code,
            "sector_name": "测试行业",
            "n_states": 2,
            "transmat": [[0.9, 0.1], [0.1, 0.9]],
            "means": [[0.01, 0.0, 0.0, 0.0], [-0.01, 0.0, 0.0, 0.0]],
            "covars": [
                np.eye(4).tolist(),
                np.eye(4).tolist(),
            ],
            "state_labels": {"0": "trending", "1": "fading"},
            "trained_at": "2025-01-01T00:00:00",
            "training_days": 500,
        }
    }

    tmp_dir = tmp_path_factory.mktemp("hmm_cooldown")
    model_path = str(tmp_dir / "models.json")
    SectorHMMTrainer.save_models(models, model_path)

    inference = SectorHMMInference(
        model_path=model_path,
        config=config,
        db_conn_factory=lambda: MagicMock(),
    )

    # Date layout (all consecutive calendar days):
    #   Phase A: days_before_first_switch days of initial_state
    #   Phase B: 1 day of switched_state (the first switch)
    #   Phase C: (cooldown_days - 1) + extra_days_after_cooldown days of
    #            initial_state (attempt to switch back)
    #
    # Phase C blocked days = cooldown_days - 1 because the gap between
    # the first switch date and the first Phase C date is 1 calendar day,
    # and _check_cooldown uses strict < comparison.
    phase_a_len = days_before_first_switch
    phase_b_len = 1  # the first switch day
    blocked_days = cooldown_days - 1  # days in Phase C blocked by cooldown
    phase_c_len = blocked_days + extra_days_after_cooldown
    total_days = phase_a_len + phase_b_len + phase_c_len

    base_date = date(2025, 1, 1)
    all_dates = [base_date + timedelta(days=i) for i in range(total_days)]

    first_switch_idx = phase_a_len  # index of the first switch day
    second_switch_idx = phase_a_len + phase_b_len  # index where switch-back starts

    # Build the state sequence
    state_by_date = {}
    for i, d in enumerate(all_dates):
        if i < first_switch_idx:
            state_by_date[d] = initial_state       # Phase A
        elif i < second_switch_idx:
            state_by_date[d] = switched_state       # Phase B (first switch)
        else:
            state_by_date[d] = initial_state        # Phase C (attempt switch back)

    # Mock _decode_state to return our controlled state sequence
    def mock_decode_state(sc, trade_date):
        assert sc == sector_code
        return state_by_date[trade_date]

    with patch.object(inference, "_decode_state", side_effect=mock_decode_state):
        coefficients_over_time = []
        for d in all_dates:
            coeffs = inference.get_sector_coefficients(d)
            coefficients_over_time.append((d, coeffs[sector_code]))

    # --- Assertions ---

    # Phase A: initial_state → coefficient should be initial_coeff
    for i in range(phase_a_len):
        d, coeff = coefficients_over_time[i]
        assert coeff == initial_coeff, (
            f"Phase A day {i} ({d}): expected {initial_coeff}, got {coeff}"
        )

    # Phase B: first switch takes effect (cooldown from creation has expired)
    d, coeff = coefficients_over_time[first_switch_idx]
    assert coeff == switched_coeff, (
        f"First switch day ({d}): expected {switched_coeff}, got {coeff}"
    )

    # Phase C (blocked): attempt to switch back to initial_state.
    # For the first `blocked_days` days of Phase C, the cooldown is active
    # because (date - first_switch_date).days < cooldown_days.
    # The coefficient must stay at switched_coeff.
    for i in range(second_switch_idx, second_switch_idx + blocked_days):
        if i >= total_days:
            break
        d, coeff = coefficients_over_time[i]
        first_switch_date = all_dates[first_switch_idx]
        gap = (d - first_switch_date).days
        assert coeff == switched_coeff, (
            f"Cooldown day {i - second_switch_idx} ({d}): gap={gap}, "
            f"expected {switched_coeff} (blocked by cooldown), got {coeff}. "
            f"cooldown_days={cooldown_days}"
        )

    # Phase C (unblocked): after cooldown expires, the switch-back takes effect
    for i in range(second_switch_idx + blocked_days, total_days):
        d, coeff = coefficients_over_time[i]
        first_switch_date = all_dates[first_switch_idx]
        gap = (d - first_switch_date).days
        assert coeff == initial_coeff, (
            f"Post-cooldown day {i - second_switch_idx} ({d}): gap={gap}, "
            f"expected {initial_coeff} (switch-back took effect), got {coeff}. "
            f"cooldown_days={cooldown_days}"
        )
