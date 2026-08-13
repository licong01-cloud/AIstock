from __future__ import annotations

from datetime import date

from backend.services.dataset_release.contracts import Component, ComponentAction
from backend.services.dataset_release.dependency_graph import (
    DatasetDependencyGraph,
    RevisionEvent,
    RevisionKind,
)


def _graph() -> DatasetDependencyGraph:
    trading_dates = [item.date() for item in __import__("pandas").bdate_range("2026-01-01", "2026-07-31")]
    return DatasetDependencyGraph(
        dataset_start=date(2018, 8, 1),
        cutoff=date(2026, 7, 31),
        trading_dates=trading_dates,
    )


def test_qfq_denominator_change_rebuilds_affected_instrument_history() -> None:
    result = _graph().propagate(
        RevisionEvent(
            RevisionKind.QFQ_DENOMINATOR,
            "market.adj_factor",
            instruments=("000001.SZ",),
            start=date(2026, 7, 1),
            end=date(2026, 7, 31),
        )
    )
    assert {item.component for item in result} == {
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    }
    assert all(item.action is ComponentAction.SELECTIVE_REBUILD for item in result)
    assert all(item.start == date(2018, 8, 1) for item in result)
    assert all(item.instruments == ("000001.SZ",) for item in result)


def test_qfq_numerator_change_has_exact_and_downstream_scopes() -> None:
    changed = date(2026, 7, 1)
    result = _graph().propagate(
        RevisionEvent(
            RevisionKind.QFQ_NUMERATOR,
            "market.adj_factor",
            instruments=("000001.SZ",),
            start=changed,
            end=changed,
        )
    )
    daily = next(item for item in result if item.component is Component.DAILY_BIN)
    factor = next(item for item in result if item.component is Component.FACTOR_H5_STATIC)
    assert daily.start == daily.end == changed
    assert factor.end is not None and factor.end > changed
    assert "10 observations" in factor.reason


def test_pit_span_change_invalidates_only_affected_stock_range() -> None:
    result = _graph().propagate(
        RevisionEvent(
            RevisionKind.PIT_SPAN,
            "market.stock_universe_pit_spans",
            instruments=("000007.SZ",),
            start=date(2024, 7, 2),
            end=date(2025, 1, 3),
        )
    )
    assert len(result) == 3
    assert all(item.instruments == ("000007.SZ",) for item in result)
    assert all(item.start == date(2024, 7, 2) for item in result)


def test_stk_limit_and_suspend_revision_invalidate_daily_and_minute() -> None:
    for kind, dataset in (
        (RevisionKind.STK_LIMIT, "market.stk_limit"),
        (RevisionKind.SUSPEND, "market.suspend_d"),
    ):
        result = _graph().propagate(
            RevisionEvent(
                kind,
                dataset,
                instruments=("000001.SZ",),
                start=date(2026, 7, 1),
                end=date(2026, 7, 1),
            )
        )
        assert {item.component for item in result} == {
            Component.DAILY_BIN,
            Component.MINUTE_BIN,
        }


def test_daily_reference_revision_matches_all_actual_consumers() -> None:
    result = _graph().propagate(
        RevisionEvent(
            RevisionKind.DAILY_REFERENCE,
            "market.kline_daily",
            instruments=("000001.SZ",),
            start=date(2026, 7, 1),
            end=date(2026, 7, 1),
        )
    )
    assert {item.component for item in result} == {
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    }


def test_unknown_edge_fails_closed_to_component_full_rebuild() -> None:
    result = _graph().propagate(
        RevisionEvent(
            RevisionKind.UNKNOWN,
            "new_source",
            metadata={"components": [Component.MINUTE_BIN.value]},
        )
    )
    assert result[0].component is Component.MINUTE_BIN
    assert result[0].action is ComponentAction.FULL_REBUILD
    assert result[0].start == date(2018, 8, 1)
