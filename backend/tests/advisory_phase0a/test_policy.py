from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from backend.services.advisory_phase0a.models import (
    AvailabilityStatus,
    FormalOOSStatus,
    OOSClassification,
    OOSClassificationInput,
)
from backend.services.advisory_phase0a.policy import (
    CanonicalJSONError,
    REASON_CANDIDATE_AUTHORITY_MISSING,
    REASON_RETROSPECTIVE_ONLY,
    canonical_json_sha256,
    canonical_json_text,
    classify_oos,
    coalesce_oos_intervals,
    embargo_formal_start,
    effective_cutoff,
)


def test_canonical_json_normalizes_decimals_keys_and_negative_zero() -> None:
    left = {
        "score": Decimal("1.2"),
        "entry_price": Decimal("10.1234567"),
        "nested": {"return": -0.0},
    }
    right = {
        "nested": {"return": Decimal("0")},
        "entry_price": Decimal("10.1234567"),
        "score": 1.2,
    }

    assert canonical_json_sha256(left) == canonical_json_sha256(right)
    assert canonical_json_text(left) == (
        '{"entry_price":"10.123457","nested":{"return":"0"},'
        '"score":"1.200000000000"}'
    )


def test_canonical_json_rejects_non_finite_values() -> None:
    with pytest.raises(CanonicalJSONError, match="NaN"):
        canonical_json_sha256({"score": float("nan")})


def test_effective_cutoff_requires_every_mandatory_input() -> None:
    cutoff, reasons = effective_cutoff({"asset": date(2026, 1, 5), "hmm": None})

    assert cutoff is None
    assert "ADVISORY_PHASE0A_CUTOFF_MISSING_HMM" in reasons


def test_embargo_uses_trading_days_and_selects_the_day_after_gap() -> None:
    formal_start, reasons = embargo_formal_start(
        effective_cutoff=date(2026, 1, 2),
        trading_days=[date(2026, 1, day) for day in range(5, 31)],
        minimum_trading_day_gap=20,
    )

    assert formal_start == date(2026, 1, 25)
    assert reasons == []


def test_oos_classifier_preserves_formal_availability_invariant() -> None:
    formal = classify_oos(
        OOSClassificationInput(
            decision_date=date(2026, 1, 5),
            formal_start_date=date(2026, 1, 1),
            effective_cutoff=date(2026, 1, 5),
            mandatory_closure_complete=True,
            historical_semantics_available=True,
            point_in_time_source_available=True,
            candidate_authority_formal=True,
        )
    )
    retrospective = classify_oos(
        OOSClassificationInput(
            decision_date=date(2026, 1, 5),
            effective_cutoff=date(2026, 1, 5),
            mandatory_closure_complete=True,
            research_replay_eligible=True,
        )
    )
    unavailable = classify_oos(OOSClassificationInput(decision_date=date(2026, 1, 5)))

    assert (formal.formal_oos_status, formal.availability_status) == (
        FormalOOSStatus.FORMAL_OOS,
        AvailabilityStatus.AVAILABLE,
    )
    assert retrospective.formal_oos_status == FormalOOSStatus.RETROSPECTIVE_RESEARCH_ONLY
    assert retrospective.availability_status == AvailabilityStatus.UNAVAILABLE
    assert REASON_RETROSPECTIVE_ONLY in retrospective.phase0a_reason_codes
    assert unavailable.formal_oos_status == FormalOOSStatus.NONE
    assert REASON_CANDIDATE_AUTHORITY_MISSING in unavailable.phase0a_reason_codes

    with pytest.raises(ValueError, match="invariant"):
        OOSClassification(
            decision_date=date(2026, 1, 5),
            formal_oos_status=FormalOOSStatus.FORMAL_OOS,
            availability_status=AvailabilityStatus.UNAVAILABLE,
        )


def test_oos_interval_coalescing_keeps_different_cutoffs_separate() -> None:
    first = classify_oos(
        OOSClassificationInput(
            decision_date=date(2026, 1, 5),
            effective_cutoff=date(2026, 1, 5),
            mandatory_closure_complete=True,
            historical_semantics_available=True,
            point_in_time_source_available=True,
            candidate_authority_formal=True,
        )
    )
    second = first.model_copy(update={"decision_date": date(2026, 1, 6)})
    third = first.model_copy(update={"decision_date": date(2026, 1, 7), "effective_cutoff": date(2026, 1, 6)})

    intervals = coalesce_oos_intervals([third, first, second])

    assert [(item.start_date, item.end_date) for item in intervals] == [
        (date(2026, 1, 5), date(2026, 1, 6)),
        (date(2026, 1, 7), date(2026, 1, 7)),
    ]
