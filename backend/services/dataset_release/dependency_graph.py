from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence

from .contracts import Component, ComponentAction
from .errors import DependencyGraphError


DEPENDENCY_GRAPH_VERSION = "qe_dataset_dependency_graph_v1"


class RevisionKind(str, Enum):
    DAILY = "daily"
    INDEX = "index"
    QFQ_DENOMINATOR = "qfq_denominator"
    QFQ_NUMERATOR = "qfq_numerator"
    STK_LIMIT = "stk_limit"
    SUSPEND = "suspend"
    DAILY_REFERENCE = "daily_reference"
    PIT_SPAN = "pit_span"
    MONEYFLOW = "moneyflow"
    PRICE = "price"
    SLOW_STATIC = "slow_static"
    SECTOR_INTERVAL = "sector_interval"
    STATIC_SCHEMA = "static_schema"
    SCHEMA = "schema"
    UNIT = "unit"
    FORMULA = "formula"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RevisionEvent:
    kind: RevisionKind
    dataset: str
    instruments: tuple[str, ...] = ()
    start: date | None = None
    end: date | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Invalidation:
    component: Component
    action: ComponentAction
    instruments: tuple[str, ...]
    start: date | None
    end: date | None
    dependency_edge: str
    reason: str


def _extend_observations(
    value: date,
    observations: int,
    trading_dates: Sequence[date],
) -> date:
    if not trading_dates:
        raise DependencyGraphError("trading dates are required for rolling invalidation")
    ordered = sorted(set(trading_dates))
    try:
        position = next(index for index, item in enumerate(ordered) if item >= value)
    except StopIteration:
        return ordered[-1]
    return ordered[min(len(ordered) - 1, position + observations)]


class DatasetDependencyGraph:
    """Versioned fail-closed invalidation graph for selective monthly updates."""

    def __init__(
        self,
        *,
        dataset_start: date,
        cutoff: date,
        trading_dates: Sequence[date],
    ) -> None:
        if cutoff < dataset_start:
            raise DependencyGraphError("cutoff precedes dataset start")
        self.dataset_start = dataset_start
        self.cutoff = cutoff
        self.trading_dates = tuple(sorted(set(trading_dates)))

    def _selective(
        self,
        component: Component,
        event: RevisionEvent,
        *,
        start: date | None = None,
        end: date | None = None,
        edge: str,
        reason: str,
    ) -> Invalidation:
        return Invalidation(
            component=component,
            action=ComponentAction.SELECTIVE_REBUILD,
            instruments=tuple(sorted(set(event.instruments))),
            start=start if start is not None else event.start,
            end=end if end is not None else event.end,
            dependency_edge=edge,
            reason=reason,
        )

    def _full(self, component: Component, event: RevisionEvent, reason: str) -> Invalidation:
        return Invalidation(
            component=component,
            action=ComponentAction.FULL_REBUILD,
            instruments=(),
            start=self.dataset_start,
            end=self.cutoff,
            dependency_edge=f"{event.kind.value}->{component.value}",
            reason=reason,
        )

    def propagate(self, event: RevisionEvent) -> tuple[Invalidation, ...]:
        if event.kind is RevisionKind.QFQ_DENOMINATOR:
            if not event.instruments:
                raise DependencyGraphError("QFQ denominator revision requires instruments")
            return tuple(
                self._selective(
                    component,
                    event,
                    start=self.dataset_start,
                    end=self.cutoff,
                    edge=f"adj_factor.denominator->{component.value}",
                    reason="QFQ denominator changed; rebuild affected instrument history",
                )
                for component in (
                    Component.DAILY_BIN,
                    Component.MINUTE_BIN,
                    Component.FACTOR_H5_STATIC,
                )
            )
        if event.kind is RevisionKind.QFQ_NUMERATOR:
            if event.start is None or not event.instruments:
                raise DependencyGraphError("QFQ numerator revision requires date and instruments")
            rolling_end = _extend_observations(
                event.end or event.start,
                10,
                self.trading_dates,
            )
            return (
                self._selective(
                    Component.DAILY_BIN,
                    event,
                    edge="adj_factor.numerator->daily_bin",
                    reason="QFQ numerator changed on exact dates",
                ),
                self._selective(
                    Component.MINUTE_BIN,
                    event,
                    edge="adj_factor.numerator->minute_bin",
                    reason="QFQ numerator changed on exact dates",
                ),
                self._selective(
                    Component.FACTOR_H5_STATIC,
                    event,
                    end=rolling_end,
                    edge="adj_factor.numerator->price_rolling_10",
                    reason="QFQ numerator revision propagates through 10 observations",
                ),
            )
        if event.kind is RevisionKind.PIT_SPAN:
            if event.start is None or event.end is None or not event.instruments:
                raise DependencyGraphError("PIT revision requires instrument/date range")
            return tuple(
                self._selective(
                    component,
                    event,
                    edge=f"pit_span->{component.value}",
                    reason="frozen PIT membership changed",
                )
                for component in (
                    Component.DAILY_BIN,
                    Component.MINUTE_BIN,
                    Component.FACTOR_H5_STATIC,
                )
            )
        if event.kind is RevisionKind.STK_LIMIT:
            return tuple(
                self._selective(
                    component,
                    event,
                    edge=f"stk_limit->{component.value}",
                    reason="12-field Qlib limit/pre-close reference input changed",
                )
                for component in (Component.DAILY_BIN, Component.MINUTE_BIN)
            )
        if event.kind is RevisionKind.SUSPEND:
            return tuple(
                self._selective(
                    component,
                    event,
                    edge=f"suspend->{component.value}",
                    reason="suspension-aware stock-bar authority changed",
                )
                for component in (Component.DAILY_BIN, Component.MINUTE_BIN)
            )
        if event.kind is RevisionKind.DAILY_REFERENCE:
            return tuple(
                self._selective(
                    component,
                    event,
                    edge=f"daily_reference->{component.value}",
                    reason="daily price/reference authority changed",
                )
                for component in (
                    Component.DAILY_BIN,
                    Component.MINUTE_BIN,
                    Component.FACTOR_H5_STATIC,
                )
            )
        if event.kind is RevisionKind.MONEYFLOW:
            if event.start is None:
                raise DependencyGraphError("moneyflow revision requires start date")
            return (
                self._selective(
                    Component.FACTOR_H5_STATIC,
                    event,
                    end=_extend_observations(
                        event.end or event.start,
                        20,
                        self.trading_dates,
                    ),
                    edge="moneyflow->rolling_5_20",
                    reason="moneyflow change propagates through 20 observations",
                ),
            )
        if event.kind is RevisionKind.PRICE:
            if event.start is None:
                raise DependencyGraphError("price revision requires start date")
            return (
                self._selective(
                    Component.FACTOR_H5_STATIC,
                    event,
                    end=_extend_observations(
                        event.end or event.start,
                        10,
                        self.trading_dates,
                    ),
                    edge="price->PriceStrength_10D",
                    reason="price change propagates through 10 observations",
                ),
            )
        if event.kind is RevisionKind.SLOW_STATIC:
            if event.start is None:
                raise DependencyGraphError("slow-static revision requires start date")
            next_observation = event.metadata.get("next_observation")
            if not isinstance(next_observation, date):
                raise DependencyGraphError("slow-static revision requires next_observation")
            return (
                self._selective(
                    Component.FACTOR_H5_STATIC,
                    event,
                    end=next_observation,
                    edge="slow_static->forward_fill",
                    reason="forward-fill changes through the next true observation",
                ),
            )
        if event.kind is RevisionKind.SECTOR_INTERVAL:
            return (
                self._selective(
                    Component.FACTOR_H5_STATIC,
                    event,
                    edge="sector_interval->static_l2",
                    reason="sector interval changed",
                ),
            )
        if event.kind is RevisionKind.INDEX:
            return (
                self._selective(
                    Component.DOMESTIC_INDEX_CONTEXT,
                    event,
                    edge="index_daily->domestic_index_context",
                    reason="index source partition changed",
                ),
                self._selective(
                    Component.DAILY_BIN,
                    event,
                    edge="index_daily->daily_bin.index_features",
                    reason="daily Qlib embeds the domestic-index CSV features",
                ),
            )
        if event.kind is RevisionKind.DAILY:
            return (
                self._selective(
                    Component.DAILY_BIN,
                    event,
                    edge="daily->daily_bin",
                    reason="daily source partition changed",
                ),
                self._selective(
                    Component.FACTOR_H5_STATIC,
                    event,
                    edge="daily->factor_h5_static",
                    reason="daily source feeds factor bundle",
                ),
            )
        if event.kind is RevisionKind.STATIC_SCHEMA:
            return (
                self._full(
                    Component.FACTOR_H5_STATIC,
                    event,
                    "static authority schema changed; re-attestation is insufficient",
                ),
            )
        if event.kind in {RevisionKind.SCHEMA, RevisionKind.UNIT, RevisionKind.FORMULA}:
            requested = event.metadata.get("components") or [item.value for item in Component]
            return tuple(
                self._full(Component(str(component)), event, "semantic/artifact contract changed")
                for component in requested
            )
        if event.kind is RevisionKind.UNKNOWN:
            requested = event.metadata.get("components") or [item.value for item in Component]
            return tuple(
                self._full(
                    Component(str(component)),
                    event,
                    "undefined dependency edge; fail closed to component full rebuild",
                )
                for component in requested
            )
        raise DependencyGraphError(f"unhandled revision kind: {event.kind}")

    def plan(self, events: Iterable[RevisionEvent]) -> tuple[Invalidation, ...]:
        values = [item for event in events for item in self.propagate(event)]
        return tuple(
            sorted(
                values,
                key=lambda item: (
                    item.component.value,
                    item.start or self.dataset_start,
                    item.end or self.cutoff,
                    item.dependency_edge,
                ),
            )
        )
