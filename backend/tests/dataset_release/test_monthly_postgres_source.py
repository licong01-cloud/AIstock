from __future__ import annotations

from datetime import date

from backend.services.dataset_release.monthly_postgres_source import _source_diffs


def _partition(
    dataset: str,
    key: str,
    *,
    content: str,
    schema: str = "schema-v1",
    rows: int = 1,
) -> dict[str, object]:
    return {
        "dataset": dataset,
        "partition_key": key,
        "schema_digest": schema,
        "content_digest": content,
        "row_count": rows,
    }


def test_source_diff_classifies_tail_history_schema_removal_and_pit() -> None:
    baseline = {
        "partitions": [
            _partition("kline_daily_raw", "2026-08-01_2026-08-31", content="old"),
            _partition("adj_factor", "2026-08-01_2026-08-31", content="same"),
            _partition("daily_basic", "2026-08-01_2026-08-31", content="gone"),
        ]
    }
    current = {
        "partitions": [
            _partition("kline_daily_raw", "2026-08-01_2026-08-31", content="repaired"),
            _partition("kline_daily_raw", "2026-09-01_2026-09-30", content="tail"),
            _partition(
                "adj_factor",
                "2026-08-01_2026-08-31",
                content="same",
                schema="schema-v2",
            ),
        ]
    }

    observed = _source_diffs(
        baseline=baseline,
        current=current,
        predecessor_cutoff=date(2026, 8, 31),
        target_cutoff=date(2026, 9, 30),
        pit_changed=True,
    )
    identities = {(item.dataset, item.kind, item.start, item.end) for item in observed}
    assert (
        "kline_daily_raw",
        "HISTORICAL_REPAIR",
        date(2026, 8, 1),
        date(2026, 8, 31),
    ) in identities
    assert (
        "kline_daily_raw",
        "TAIL_APPEND",
        date(2026, 9, 1),
        date(2026, 9, 30),
    ) in identities
    assert ("adj_factor", "SCHEMA_CHANGE", date(2026, 8, 1), date(2026, 8, 31)) in identities
    assert ("daily_basic", "SCHEMA_CHANGE", date(2026, 8, 1), date(2026, 8, 31)) in identities
    assert (
        "stock_universe_pit",
        "PIT_REVISION",
        date(2026, 9, 1),
        date(2026, 9, 30),
    ) in identities


def test_first_unified_source_forces_complete_evidenced_migration() -> None:
    observed = _source_diffs(
        baseline=None,
        current={
            "partitions": [
                _partition("kline_daily_raw", "2026-09-01_2026-09-30", content="tail")
            ]
        },
        predecessor_cutoff=date(2026, 8, 31),
        target_cutoff=date(2026, 9, 30),
        pit_changed=True,
    )
    forced = {item.dataset for item in observed if item.kind == "SCHEMA_CHANGE"}
    assert {
        "kline_daily_raw",
        "kline_minute_raw",
        "adj_factor",
        "daily_basic",
        "moneyflow",
        "suspend_d",
        "stk_limit",
        "index_daily",
        "stock_universe_pit",
        "industry_classification",
    }.issubset(forced)
