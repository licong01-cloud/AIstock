from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pytest

from backend.services.hmm_risk import jump_model as subject


def _component(values: np.ndarray, *, ordinals: tuple[int, ...] | None = None) -> subject.PreparedComponent:
    rows = np.asarray(values, dtype=np.float64)
    dates = tuple(date(2026, 1, 1) + timedelta(days=index) for index in range(len(rows)))
    sequence = subject.SequenceData(
        key="market",
        dates=dates,
        ordinals=ordinals or tuple(range(len(rows))),
        values=rows,
    )
    preprocessor = subject.Preprocessor(
        feature_names=("daily_return", "volatility_3d"),
        lower=(-1.0, -1.0),
        upper=(1.0, 1.0),
        mean=(0.0, 0.0),
        std=(1.0, 1.0),
        valid_row_count=len(rows),
        valid_identity_sha256="a" * 64,
    )
    return subject.PreparedComponent(
        component="market",
        level="market",
        feature_names=preprocessor.feature_names,
        expected_sector_count=1,
        minimum_daily_count=1,
        canonical_codes=("000300.SH",),
        sequences=(sequence,),
        preprocessor=preprocessor,
        unavailable_items=(),
        valid_row_count=len(rows),
        valid_identity_sha256="b" * 64,
    )


def test_current_jump_profile_is_deterministic() -> None:
    component = _component(
        np.asarray(
            [
                [-1.2, 1.1],
                [-1.0, 0.9],
                [-0.8, 1.0],
                [0.8, -1.0],
                [1.0, -0.9],
                [1.2, -1.1],
            ]
        )
    )

    first = subject.fit_jump_model(component, state_count=2, jump_penalty=4.0, seed=42)
    second = subject.fit_jump_model(component, state_count=2, jump_penalty=4.0, seed=42)

    np.testing.assert_array_equal(first.centers, second.centers)
    for left, right in zip(first.paths, second.paths, strict=True):
        np.testing.assert_array_equal(left, right)
    assert first.objective == second.objective


@pytest.mark.parametrize(
    ("state_count", "jump_penalty", "seed"),
    [(3, 4.0, 42), (2, 2.0, 42), (2, 4.0, 43)],
)
def test_fit_rejects_profiles_outside_the_current_contract(
    state_count: int,
    jump_penalty: float,
    seed: int,
) -> None:
    with pytest.raises(subject.JumpModelError) as caught:
        subject.fit_jump_model(
            _component(np.asarray([[-1.0, 1.0], [1.0, -1.0]])),
            state_count=state_count,
            jump_penalty=jump_penalty,
            seed=seed,
        )
    assert caught.value.reason_code == subject.REASON_INPUT_IDENTITY


def test_causal_states_are_prefix_invariant() -> None:
    values = np.asarray([[-1.0, 1.0], [-0.8, 0.9], [0.7, -0.8], [1.0, -1.0]])
    centers = np.asarray([[-1.0, 1.0], [1.0, -1.0]])
    full = subject.causal_states(_component(values), centers, 4.0)[0]
    prefix = subject.causal_states(_component(values[:3]), centers, 4.0)[0]
    np.testing.assert_array_equal(full[:3], prefix)


def test_causal_states_reset_at_calendar_gaps() -> None:
    values = np.asarray([[-1.0, 1.0], [-0.9, 0.9], [1.0, -1.0]])
    centers = np.asarray([[-1.0, 1.0], [1.0, -1.0]])
    with_gap = subject.causal_states(_component(values, ordinals=(0, 1, 5)), centers, 4.0)[0]
    isolated = subject.causal_states(_component(values[2:], ordinals=(5,)), centers, 4.0)[0]
    assert with_gap[-1] == isolated[0]


def test_non_finite_inference_fails_closed() -> None:
    centers = np.asarray([[-1.0, 1.0], [1.0, -1.0]])
    with pytest.raises(subject.JumpModelError) as caught:
        subject.causal_states(_component(np.asarray([[np.nan, 1.0]])), centers, 4.0)
    assert caught.value.reason_code == subject.REASON_OBJECTIVE_NON_FINITE


def test_preprocessor_payload_retains_active_receipt_identity() -> None:
    payload = _component(np.asarray([[-1.0, 1.0], [1.0, -1.0]])).preprocessor.payload()
    assert payload["schema_version"] == "hmm_risk_jump_level_global_preprocess_v1"
    assert payload["dtype"] == "float64_le"
    assert payload["ddof"] == 0
