from __future__ import annotations

import datetime as dt

from scripts.build_stock_universe_pit_spans import SpanRow, _classify_st_event, _validate


def test_classify_st_event_restore_vs_still_risky() -> None:
    assert _classify_st_event("*ST", None, None) == ("st_negative", False)
    assert _classify_st_event("撤销ST", None, None) == ("st_restore", False)
    assert _classify_st_event("撤销ST", None, None, "*ST示例") == ("st_negative", False)
    assert _classify_st_event("撤销叠加*ST", "重整完成", "其他风险警示保持不变") == ("st_negative", False)
    assert _classify_st_event("撤消*ST并实行ST", None, None) == ("st_negative", False)
    assert _classify_st_event("退市整理期", None, None) == ("delist_event", True)
    assert _classify_st_event("退市整理期", None, None, terminal_as_negative=True) == ("st_negative", False)


def test_validate_rejects_overlapping_spans() -> None:
    spans = [
        SpanRow(
            universe_key="u",
            ts_code="000001.SZ",
            eligible_start=dt.date(2020, 1, 1),
            eligible_end=dt.date(2020, 1, 10),
            entry_reason="ipo_365d",
            exit_reason="st_negative",
            base_list_date=dt.date(2010, 1, 1),
            ipo_eligible_date=dt.date(2011, 1, 1),
        ),
        SpanRow(
            universe_key="u",
            ts_code="000001.SZ",
            eligible_start=dt.date(2020, 1, 10),
            eligible_end=dt.date(2020, 1, 20),
            entry_reason="st_restore",
            exit_reason="generation_end",
            base_list_date=dt.date(2010, 1, 1),
            ipo_eligible_date=dt.date(2011, 1, 1),
        ),
    ]

    result = _validate(spans, [])

    assert result["overlap_error_count"] == 1
