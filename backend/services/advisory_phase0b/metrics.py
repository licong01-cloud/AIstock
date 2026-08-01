from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Literal

from backend.services.advisory_phase1.label_policy import Projection

from .errors import Phase0BAuditError, REASON_METRIC_REGISTRY_CONFLICT


DECIMAL_QUANTUM = Decimal("0.000000000001")


def quantize_metric(value: Decimal) -> Decimal:
    if not value.is_finite():
        raise Phase0BAuditError(
            REASON_METRIC_REGISTRY_CONFLICT,
            "Phase 0B metric received a non-finite Decimal",
        )
    result = value.quantize(DECIMAL_QUANTUM, rounding=ROUND_HALF_EVEN)
    return Decimal(0).quantize(DECIMAL_QUANTUM) if result == 0 else result


@dataclass(frozen=True)
class CandidateOutcome:
    symbol: str
    rank: int
    value: Decimal | None
    maturity_status: str
    outcome_event_status: str
    benchmark_net_total_return: Decimal | None = None

    @property
    def evaluable(self) -> bool:
        return (
            self.maturity_status == "MATURED"
            and self.outcome_event_status == "TERMINAL"
            and self.value is not None
        )


@dataclass(frozen=True)
class FixedPortfolioResult:
    value: Decimal | None
    qualified_count: int
    cash_slot_count: int
    reason_code: str | None


def fixed_k_portfolio(
    *,
    candidates: Sequence[CandidateOutcome],
    k: int,
    projection: Projection,
    benchmark_net_total_return: Decimal | None,
) -> FixedPortfolioResult:
    if k <= 0:
        raise ValueError("fixed K must be positive")
    selected = tuple(sorted((item for item in candidates if item.rank <= k), key=lambda item: item.rank))
    if len(selected) > k or len({item.rank for item in selected}) != len(selected):
        raise Phase0BAuditError(
            REASON_METRIC_REGISTRY_CONFLICT,
            "candidate ranks do not close to one fixed-K portfolio",
        )
    unavailable = tuple(item for item in selected if not item.evaluable)
    if unavailable:
        return FixedPortfolioResult(
            value=None,
            qualified_count=len(selected) - len(unavailable),
            cash_slot_count=k - len(selected),
            reason_code="ADVISORY_PHASE0B_SELECTED_LABEL_UNAVAILABLE",
        )
    if projection in {Projection.GAP_1D, Projection.BARRIER, Projection.SURVIVAL}:
        return FixedPortfolioResult(
            value=None,
            qualified_count=len(selected),
            cash_slot_count=k - len(selected),
            reason_code="ADVISORY_PHASE0B_PROJECTION_NOT_APPLICABLE",
        )
    cash_slots = k - len(selected)
    if projection is Projection.RETURN_NET_EXCESS:
        if cash_slots and benchmark_net_total_return is None:
            return FixedPortfolioResult(
                value=None,
                qualified_count=len(selected),
                cash_slot_count=cash_slots,
                reason_code="ADVISORY_PHASE0B_BENCHMARK_UNAVAILABLE",
            )
        cash_contribution = -(benchmark_net_total_return or Decimal(0))
    else:
        cash_contribution = Decimal(0)
    total = sum((item.value or Decimal(0) for item in selected), Decimal(0))
    total += cash_contribution * cash_slots
    return FixedPortfolioResult(
        value=quantize_metric(total / Decimal(k)),
        qualified_count=len(selected),
        cash_slot_count=cash_slots,
        reason_code=None,
    )


def equal_weight_by_decision_date(values: Mapping[str, Sequence[Decimal]]) -> Decimal | None:
    daily_values: list[Decimal] = []
    for decision_date in sorted(values):
        observations = tuple(values[decision_date])
        if not observations:
            continue
        daily_values.append(sum(observations, Decimal(0)) / Decimal(len(observations)))
    if not daily_values:
        return None
    return quantize_metric(sum(daily_values, Decimal(0)) / Decimal(len(daily_values)))


def random5_symbols(
    *,
    seed: bytes,
    replicate_no: int,
    symbols: Sequence[str],
) -> tuple[str, ...]:
    if replicate_no < 0:
        raise ValueError("replicate number cannot be negative")
    unique = tuple(sorted(set(symbols)))
    if len(unique) != len(symbols):
        raise Phase0BAuditError(
            REASON_METRIC_REGISTRY_CONFLICT,
            "Random5 candidate symbols must be unique",
        )
    counter = replicate_no.to_bytes(8, "big", signed=False)
    ranked = sorted(
        unique,
        key=lambda symbol: (
            hashlib.sha256(seed + counter + symbol.encode("utf-8")).digest(),
            symbol,
        ),
    )
    return tuple(ranked[:5])


def precision_at_k(
    *,
    selected: Sequence[CandidateOutcome],
    k: int,
    is_winner: Mapping[str, bool],
) -> Decimal | None:
    fixed = tuple(item for item in sorted(selected, key=lambda item: item.rank) if item.rank <= k)
    if any(not item.evaluable or item.symbol not in is_winner for item in fixed):
        return None
    wins = sum(1 for item in fixed if is_winner[item.symbol])
    return quantize_metric(Decimal(wins) / Decimal(k))


def ndcg_at_k(
    *,
    selected: Sequence[CandidateOutcome],
    candidate_gains: Mapping[str, Decimal | None],
    k: int,
) -> Decimal | None:
    ranked = tuple(item for item in sorted(selected, key=lambda item: item.rank) if item.rank <= k)
    if any(not item.evaluable or candidate_gains.get(item.symbol) is None for item in ranked):
        return None
    available_gains = tuple(value for value in candidate_gains.values() if value is not None)
    if len(available_gains) != len(candidate_gains):
        return None

    def _dcg(values: Sequence[Decimal]) -> float:
        return sum(float(value) / math.log2(index + 2) for index, value in enumerate(values))

    selected_values = [max(candidate_gains[item.symbol] or Decimal(0), Decimal(0)) for item in ranked]
    selected_values.extend(Decimal(0) for _ in range(k - len(selected_values)))
    ideal_values = sorted((max(value, Decimal(0)) for value in available_gains), reverse=True)[:k]
    ideal_values.extend(Decimal(0) for _ in range(k - len(ideal_values)))
    ideal = _dcg(ideal_values)
    if ideal == 0:
        return quantize_metric(Decimal(0))
    return quantize_metric(Decimal(str(_dcg(selected_values) / ideal)))


@dataclass(frozen=True)
class RecallResult:
    recall: Decimal | None
    selected_winner_count: int
    denominator_winner_count: int
    no_winner: bool


def recall_at_k(
    *,
    selected_symbols: Sequence[str],
    denominator_winners: Iterable[str],
    k: int,
) -> RecallResult:
    denominator = frozenset(denominator_winners)
    if not denominator:
        return RecallResult(
            recall=None,
            selected_winner_count=0,
            denominator_winner_count=0,
            no_winner=True,
        )
    selected = frozenset(selected_symbols[:k])
    numerator = len(selected & denominator)
    return RecallResult(
        recall=quantize_metric(Decimal(numerator) / Decimal(len(denominator))),
        selected_winner_count=numerator,
        denominator_winner_count=len(denominator),
        no_winner=False,
    )


def average_rank(values: Sequence[Decimal]) -> tuple[float, ...]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    result = [0.0] * len(values)
    offset = 0
    while offset < len(indexed):
        end = offset + 1
        while end < len(indexed) and indexed[end][1] == indexed[offset][1]:
            end += 1
        rank = (offset + 1 + end) / 2.0
        for original_index, _value in indexed[offset:end]:
            result[original_index] = rank
        offset = end
    return tuple(result)


def spearman(values_x: Sequence[Decimal], values_y: Sequence[Decimal]) -> Decimal | None:
    if len(values_x) != len(values_y) or len(values_x) < 2:
        return None
    ranks_x = average_rank(values_x)
    ranks_y = average_rank(values_y)
    mean_x = sum(ranks_x) / len(ranks_x)
    mean_y = sum(ranks_y) / len(ranks_y)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(ranks_x, ranks_y, strict=True))
    denominator_x = sum((x - mean_x) ** 2 for x in ranks_x)
    denominator_y = sum((y - mean_y) ** 2 for y in ranks_y)
    if denominator_x == 0 or denominator_y == 0:
        return None
    return quantize_metric(Decimal(str(numerator / math.sqrt(denominator_x * denominator_y))))


def nearest_rank(values: Sequence[Decimal], probability: Decimal) -> Decimal:
    if not values or probability <= 0 or probability > 1:
        raise ValueError("nearest-rank quantile requires values and probability in (0, 1]")
    ordered = sorted(values)
    rank = max(1, math.ceil(float(probability * Decimal(len(ordered)))))
    return ordered[rank - 1]


def stationary_bootstrap_indices(
    *,
    sample_size: int,
    replicates: int,
    registry_hash: str,
) -> tuple[tuple[int, ...], ...]:
    return tuple(
        iter_stationary_bootstrap_indices(
            sample_size=sample_size,
            replicates=replicates,
            registry_hash=registry_hash,
        )
    )


def iter_stationary_bootstrap_indices(
    *,
    sample_size: int,
    replicates: int,
    registry_hash: str,
) -> Iterator[tuple[int, ...]]:
    if sample_size <= 0 or replicates <= 0:
        raise ValueError("stationary bootstrap requires positive sizes")
    try:
        bytes.fromhex(registry_hash)
    except ValueError as error:
        raise ValueError("stationary bootstrap registry hash must be lowercase sha256 hex") from error
    if len(registry_hash) != 64 or registry_hash.lower() != registry_hash:
        raise ValueError("stationary bootstrap registry hash must be lowercase sha256 hex")
    seed = hashlib.sha256(registry_hash.encode("ascii")).digest()[:4]
    block_length = min(60, max(5, round(sample_size ** (1 / 3))))
    restart_threshold = (1 << 64) // block_length
    for replicate in range(replicates):
        indices: list[int] = []
        previous = 0
        for position in range(sample_size):
            prefix = seed + replicate.to_bytes(8, "big") + position.to_bytes(8, "big")
            restart_draw = int.from_bytes(
                hashlib.sha256(prefix + b"restart").digest()[:8],
                "big",
            )
            if position == 0 or restart_draw < restart_threshold:
                index_draw = int.from_bytes(
                    hashlib.sha256(prefix + b"index").digest()[:8],
                    "big",
                )
                previous = index_draw % sample_size
            else:
                previous = (previous + 1) % sample_size
            indices.append(previous)
        yield tuple(indices)


@dataclass(frozen=True)
class BootstrapInterval:
    lower: Decimal
    upper: Decimal
    replicate_count: int


def stationary_bootstrap_mean_interval(
    *,
    values: Sequence[Decimal],
    registry_hash: str,
    replicates: int = 5000,
) -> BootstrapInterval:
    means = tuple(
        quantize_metric(sum((values[index] for index in sample), Decimal(0)) / Decimal(len(sample)))
        for sample in iter_stationary_bootstrap_indices(
            sample_size=len(values),
            replicates=replicates,
            registry_hash=registry_hash,
        )
    )
    return BootstrapInterval(
        lower=nearest_rank(means, Decimal("0.025")),
        upper=nearest_rank(means, Decimal("0.975")),
        replicate_count=replicates,
    )


def hansen_spa_p_value(
    *,
    performance_by_model: Mapping[str, Sequence[Decimal]],
    registry_hash: str,
    replicates: int = 5000,
) -> Decimal:
    if not performance_by_model:
        raise ValueError("SPA requires at least one registered comparison")
    ordered = tuple(sorted(performance_by_model.items()))
    sample_size = len(ordered[0][1])
    if sample_size < 2 or any(len(values) != sample_size for _name, values in ordered):
        raise ValueError("SPA comparisons require equal decision-date samples")
    means = {
        name: sum(values, Decimal(0)) / Decimal(sample_size)
        for name, values in ordered
    }
    bootstrap_means: dict[str, list[float]] = {name: [] for name, _values in ordered}
    for sample in iter_stationary_bootstrap_indices(
        sample_size=sample_size,
        replicates=replicates,
        registry_hash=registry_hash,
    ):
        for name, values in ordered:
            bootstrap_means[name].append(
                float(
                    sum((values[index] for index in sample), Decimal(0))
                    / Decimal(sample_size)
                )
            )
    scales: dict[str, float] = {}
    for name, values in ordered:
        raw_means = bootstrap_means[name]
        center = sum(raw_means) / len(raw_means)
        variance = sum((value - center) ** 2 for value in raw_means) / max(1, len(raw_means) - 1)
        scales[name] = max(math.sqrt(variance) * math.sqrt(sample_size), 1e-15)
    observed = max(
        math.sqrt(sample_size) * float(means[name]) / scales[name]
        for name, _values in ordered
    )
    loglog = math.log(max(math.log(sample_size), 1.0000001))
    threshold_multiplier = -math.sqrt(max(0.0, 2.0 * loglog))
    recentered: dict[str, float] = {}
    for name, _values in ordered:
        statistic = math.sqrt(sample_size) * float(means[name]) / scales[name]
        recentered[name] = float(means[name]) if statistic <= threshold_multiplier else 0.0
    exceedances = 0
    for replicate in range(replicates):
        bootstrap_statistic = max(
            math.sqrt(sample_size)
            * (bootstrap_means[name][replicate] - float(means[name]) + recentered[name])
            / scales[name]
            for name, _values in ordered
        )
        if bootstrap_statistic >= observed:
            exceedances += 1
    return quantize_metric(Decimal(exceedances + 1) / Decimal(replicates + 1))


def benjamini_yekutieli_adjusted(
    p_values: Mapping[str, Decimal],
) -> dict[str, Decimal]:
    if not p_values:
        return {}
    ordered = sorted(p_values.items(), key=lambda item: (item[1], item[0]))
    if any(value < 0 or value > 1 for _name, value in ordered):
        raise ValueError("BY adjustment requires p-values in [0, 1]")
    count = len(ordered)
    harmonic = sum(Decimal(1) / Decimal(index) for index in range(1, count + 1))
    adjusted = [Decimal(0)] * count
    running = Decimal(1)
    for offset in range(count - 1, -1, -1):
        rank = offset + 1
        raw = ordered[offset][1] * Decimal(count) * harmonic / Decimal(rank)
        running = min(running, raw, Decimal(1))
        adjusted[offset] = quantize_metric(running)
    return {name: adjusted[index] for index, (name, _value) in enumerate(ordered)}


def stage_overlap(
    *,
    left: Sequence[str],
    right: Sequence[str],
    k: int,
) -> tuple[int, Decimal]:
    left_set = frozenset(left[:k])
    right_set = frozenset(right[:k])
    overlap = len(left_set & right_set)
    union = left_set | right_set
    jaccard = Decimal(1) if not union else Decimal(overlap) / Decimal(len(union))
    return overlap, quantize_metric(jaccard)


MetricDirection = Literal["HIGHER_IS_BETTER", "LOWER_IS_BETTER"]
