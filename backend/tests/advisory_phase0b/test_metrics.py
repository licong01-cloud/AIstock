from __future__ import annotations

from decimal import Decimal

from backend.services.advisory_phase0b.metrics import (
    CandidateOutcome,
    benjamini_yekutieli_adjusted,
    equal_weight_by_decision_date,
    fixed_k_portfolio,
    hansen_spa_p_value,
    ndcg_at_k,
    precision_at_k,
    random5_symbols,
    recall_at_k,
    spearman,
    stage_overlap,
    stationary_bootstrap_indices,
    stationary_bootstrap_mean_interval,
)
from backend.services.advisory_phase1.label_policy import Projection


def _outcome(
    symbol: str,
    rank: int,
    value: str | None,
    *,
    maturity: str = "MATURED",
) -> CandidateOutcome:
    return CandidateOutcome(
        symbol=symbol,
        rank=rank,
        value=Decimal(value) if value is not None else None,
        maturity_status=maturity,
        outcome_event_status="TERMINAL",
        benchmark_net_total_return=Decimal("0.02"),
    )


def test_fixed_k_cash_policy_is_projection_specific() -> None:
    absolute = fixed_k_portfolio(
        candidates=(_outcome("A", 1, "0.10"), _outcome("B", 2, "0.20")),
        k=5,
        projection=Projection.RETURN_NET_ABSOLUTE,
        benchmark_net_total_return=Decimal("0.02"),
    )
    excess = fixed_k_portfolio(
        candidates=(_outcome("A", 1, "0.10"), _outcome("B", 2, "0.20")),
        k=5,
        projection=Projection.RETURN_NET_EXCESS,
        benchmark_net_total_return=Decimal("0.02"),
    )

    assert absolute.value == Decimal("0.060000000000")
    assert excess.value == Decimal("0.048000000000")
    assert absolute.cash_slot_count == excess.cash_slot_count == 3


def test_selected_unmature_label_is_not_cash_or_replaced() -> None:
    result = fixed_k_portfolio(
        candidates=(
            _outcome("A", 1, "0.10"),
            _outcome("B", 2, None, maturity="PENDING"),
            _outcome("C", 6, "0.90"),
        ),
        k=5,
        projection=Projection.RETURN_NET_ABSOLUTE,
        benchmark_net_total_return=Decimal("0.02"),
    )

    assert result.value is None
    assert result.qualified_count == 1
    assert result.cash_slot_count == 3
    assert result.reason_code == "ADVISORY_PHASE0B_SELECTED_LABEL_UNAVAILABLE"


def test_decision_dates_are_equal_weighted_not_rows() -> None:
    value = equal_weight_by_decision_date(
        {
            "2026-07-01": (Decimal("1"),),
            "2026-07-02": (Decimal("0"), Decimal("0"), Decimal("0")),
        }
    )

    assert value == Decimal("0.500000000000")


def test_random5_and_stationary_bootstrap_golden_vectors() -> None:
    assert random5_symbols(
        seed=bytes.fromhex("ab" * 32),
        replicate_no=7,
        symbols=("000001.SZ", "000002.SZ", "600000.SH", "600001.SH", "300001.SZ", "688001.SH"),
    ) == ("600000.SH", "000001.SZ", "300001.SZ", "688001.SH", "000002.SZ")
    assert stationary_bootstrap_indices(
        sample_size=6,
        replicates=2,
        registry_hash="cd" * 32,
    ) == ((1, 2, 3, 3, 4, 5), (0, 1, 4, 5, 1, 2))


def test_precision_ndcg_recall_and_stage_overlap_keep_fixed_denominators() -> None:
    selected = (_outcome("A", 1, "0.8"), _outcome("B", 2, "0.2"))

    assert precision_at_k(
        selected=selected,
        k=5,
        is_winner={"A": True, "B": False},
    ) == Decimal("0.200000000000")
    assert ndcg_at_k(
        selected=selected,
        candidate_gains={"A": Decimal("0.8"), "B": Decimal("0.2")},
        k=5,
    ) == Decimal("1.000000000000")
    recall = recall_at_k(
        selected_symbols=("A", "B"),
        denominator_winners=("A", "C"),
        k=5,
    )
    assert recall.recall == Decimal("0.500000000000")
    assert recall.denominator_winner_count == 2
    assert stage_overlap(left=("A", "B"), right=("B", "C"), k=5) == (
        1,
        Decimal("0.333333333333"),
    )


def test_undefined_spearman_and_bootstrap_interval_are_explicit() -> None:
    assert spearman((Decimal("1"), Decimal("1")), (Decimal("1"), Decimal("2"))) is None
    assert spearman(
        (Decimal("1"), Decimal("2"), Decimal("3")),
        (Decimal("3"), Decimal("2"), Decimal("1")),
    ) == Decimal("-1.000000000000")
    interval = stationary_bootstrap_mean_interval(
        values=(Decimal("0.1"), Decimal("0.2"), Decimal("0.3"), Decimal("0.4"), Decimal("0.5")),
        registry_hash="ef" * 32,
        replicates=40,
    )
    assert interval.replicate_count == 40
    assert interval.lower <= interval.upper


def test_spa_and_by_are_deterministic_and_bounded() -> None:
    spa = hansen_spa_p_value(
        performance_by_model={
            "candidate": tuple(Decimal(index) / Decimal("1000") for index in range(1, 21)),
            "random": tuple(Decimal("0") for _ in range(20)),
        },
        registry_hash="12" * 32,
        replicates=80,
    )
    assert Decimal(0) <= spa <= Decimal(1)
    assert spa == hansen_spa_p_value(
        performance_by_model={
            "candidate": tuple(Decimal(index) / Decimal("1000") for index in range(1, 21)),
            "random": tuple(Decimal("0") for _ in range(20)),
        },
        registry_hash="12" * 32,
        replicates=80,
    )
    assert benjamini_yekutieli_adjusted(
        {"a": Decimal("0.01"), "b": Decimal("0.04"), "c": Decimal("0.20")}
    ) == {
        "a": Decimal("0.055000000000"),
        "b": Decimal("0.110000000000"),
        "c": Decimal("0.366666666667"),
    }
