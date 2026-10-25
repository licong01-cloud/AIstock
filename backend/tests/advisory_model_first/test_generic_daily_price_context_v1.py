"""Hand-model projection checks only; no training or production configuration."""
from copy import deepcopy
from dataclasses import asdict, replace
from contextlib import contextmanager
from datetime import date, datetime, timezone
import json
from types import SimpleNamespace as NS

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import generic_daily_price_context_v1 as raw
from backend.services.advisory_model_first import generic_price_set_consumer_v1 as consumer
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_generic_price_set_consumer_v1 import packet
from backend.services.advisory_model_first.entry_price_daily_service import EntryWorkBudget


def context():
    return dict(reference_cny=100., raw_legal_low_cny=99., raw_legal_high_cny=102., raw_tick_cny=1.,
                target_raw_price_multiplier=1., visible_through="2026-06-10", coordinate_source="D_VISIBLE_FIXTURE")


def test_original_nodes_and_package_independence(tmp_path):
    loaded, fitted, rows, coordinates, source, price_set, _ = packet(tmp_path)
    source_before, rows_before = deepcopy(source), rows.copy(deep=True)
    expected = price_set(fitted=fitted, d_features=rows.iloc[0].drop(list(consumer.ROSTER)).to_dict(), arm="candidate",
        **{key: coordinates["000001.SZ"][key] for key in consumer.PRICES})
    result = raw.project_generic_raw_price_sets_v1(loaded=loaded, candidate_rows=rows,
        raw_price_contexts={"000001.SZ": context()}, source_context=source)
    assert result["advice"][0]["intervals_raw_cny"] == expected["intervals_cny"]
    assert result["advice"][0]["status"] == expected["status"]
    alternate = {**source, "package_id": "new", "universe_identity": {"mode": "index_union", "indices": ["000300.SH", "000905.SH"]}}
    other = raw.project_generic_raw_price_sets_v1(loaded=loaded, candidate_rows=rows,
        raw_price_contexts={"000001.SZ": context()}, source_context=alternate)
    assert result["advice"] == other["advice"] and result["input_sha256"] != other["input_sha256"]
    assert not any(result[name] for name in ("deployable", "economic_confirmation", "outcomes_read", "database_write", "model_activation"))
    pd.testing.assert_frame_equal(rows, rows_before)
    assert source == source_before


def test_ex_right_nonfinite_adjusted_tick_preserves_raw_cents(tmp_path):
    loaded, _, rows, _, source, _, _ = packet(tmp_path)
    coordinate = context()
    coordinate.update(raw_legal_low_cny=29., raw_legal_high_cny=30., raw_tick_cny=.01, target_raw_price_multiplier=.3)
    result = raw.project_generic_raw_price_sets_v1(loaded=loaded, candidate_rows=rows,
        raw_price_contexts={"000001.SZ": coordinate}, source_context=source)
    item = result["advice"][0]
    assert item["legal_node_count"] == 101 and item["intervals_raw_cny"] == ((29., 30.),)
    assert item["intervals_cny"][0] == pytest.approx((29/.3, 30/.3))
    assert item["coordinate_source"] == "D_VISIBLE_FIXTURE" and item["holding_sessions"] == 5


def test_normal_unknown_and_empty_roster_are_not_rejections(tmp_path):
    loaded, _, rows, _, source, _, _ = packet(tmp_path)
    result = raw.project_generic_raw_price_sets_v1(loaded=loaded, candidate_rows=rows,
        raw_price_contexts={}, source_context=source)
    assert result["candidate_count"] == 1 and result["advice"][0]["status"] == "UNKNOWN_PRICE_CONTEXT"
    empty = raw.project_generic_raw_price_sets_v1(loaded=loaded, candidate_rows=rows.iloc[:0],
        raw_price_contexts={}, source_context=source)
    assert empty["status"] == "NO_CANDIDATES" and empty["advice"] == []


def test_price_support_hole_is_not_filled_by_a_min_max_band(tmp_path):
    _, fitted, rows, _, source, _, _ = packet(tmp_path)
    module = consumer.FAMILIES["DAILY_5TD"][0]
    fitted = replace(fitted, intervals_bps=((-500., -200.), (0., 500.)), model_sha256="")
    fitted = replace(fitted, model_sha256=module.fitted_identity(fitted))
    stage = publish_stage(study_root=tmp_path/"holes", stage="trained", plan_sha256="a"*64, parent_sha256="b"*64,
                          artifacts={"model.json": json.dumps(asdict(fitted), allow_nan=False).encode()})
    loaded = consumer.load_generic_price_set_model_v1(model_family="DAILY_5TD",
        trained_manifest_ref=evidence_reference_for_file(stage/"manifest.json", role="test_trained_manifest"))
    coordinate = context()
    coordinate.update(raw_legal_low_cny=95., raw_legal_high_cny=105.)
    item = raw.project_generic_raw_price_sets_v1(loaded=loaded, candidate_rows=rows,
        raw_price_contexts={"000001.SZ": coordinate}, source_context=source)["advice"][0]
    assert item["intervals_raw_cny"] == ((95., 98.), (100., 103.)) and item["unknown_node_count"] == 1


@pytest.mark.parametrize("change", [{"visible_through": "2026-06-11"}, {"target_raw_price_multiplier": 0.},
    {"raw_tick_cny": np.inf}, {"raw_legal_low_cny": 103.}, {"raw_tick_cny": .00001}])
def test_contradictory_or_overbudget_coordinate_fails(tmp_path, change):
    loaded, _, rows, _, source, _, _ = packet(tmp_path)
    coordinate = context()
    coordinate.update(change)
    with pytest.raises(AdvisoryModelFirstError):
        raw.project_generic_raw_price_sets_v1(loaded=loaded, candidate_rows=rows,
            raw_price_contexts={"000001.SZ": coordinate}, source_context=source)


@pytest.fixture
def legal():
    d, t, symbol = date(2026, 6, 10), date(2026, 6, 11), "000001.SZ"
    state = NS(sql=[], base=[(0, symbol, 100000, date(2020, 1, 1), 99, 1.)],
        st=[(0, symbol, None, None, None, "ready", False, date(2018, 1, 1), d)], actions=[])
    class Cursor:
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            pass
        def execute(self, sql, params):
            state.sql.append((sql, params))
            self.rows = state.base if "legal_base" in sql else state.st if "legal_st" in sql else state.actions
        def fetchall(self):
            return self.rows
    class Session:
        budget = EntryWorkBudget(30.)
        @contextmanager
        def connection(self):
            yield NS(cursor=Cursor)
    roster = pd.DataFrame([dict(decision_as_of_trade_date=d.isoformat(), target_trade_date=t.isoformat(),
        instrument=symbol, selection_effective_rank=1, candidate_group_size=1)])
    state.packet = dict(decision_date=d, target_date=t, candidates=roster)
    state.reader = raw.GenericDailyPriceContextV1(read_session=Session(), now=lambda: datetime(2026, 7, 1, tzinfo=timezone.utc))
    return state


def test_d_visible_context_no_future_quotes_and_current_coverage_not_vintage(legal):
    result = legal.reader.load_batch(packets=[legal.packet])[0]
    value = result["contexts"]["000001.SZ"]
    assert (value["raw_legal_low_cny"], value["raw_legal_high_cny"]) == (90., 110.)
    assert value["visible_through"] == "2026-06-10" and result["receipt"]["query_count"] == 3
    assert not result["receipt"]["historical_vintage_proven"] and result["receipt"]["source_evidence"] == "CURRENT_DATABASE_NON_VINTAGE"
    base, st, actions = [value[0] for value in legal.sql]
    assert "price.trade_date=request.decision" in base and "adjustment.trade_date=request.decision" in base
    assert "COALESCE(source_pub_date,source_imp_date)<=request.decision" in st and "action_date<=request.target" in st
    assert "action.imp_ann_date<=request.decision THEN action.stk_div" in actions
    assert "action.ann_date<=request.decision OR action.imp_ann_date<=request.decision" in actions
    assert not any(term in " ".join([base, st, actions]) for term in ("dataset_date_refresh_audit", "price.trade_date=request.target"))


@pytest.mark.parametrize("case", ["dirty", "missing_close", "pending", "no_limit", "missing_tax"])
def test_normal_coordinate_unknown_retains_candidate(legal, case):
    symbol = "000001.SZ"
    if case == "dirty":
        legal.st[0] = (*legal.st[0][:6], True, *legal.st[0][7:])
    elif case == "missing_close":
        legal.base[0] = (*legal.base[0][:2], None, *legal.base[0][3:])
    elif case in {"pending", "missing_tax"}:
        legal.actions = [(0, symbol, date(2025, 12, 31), date(2026, 6, 1), "实施" if case == "missing_tax" else "预案",
            0., 0., 0., 1. if case == "missing_tax" else None, None,
            date(2026, 6, 1) if case == "missing_tax" else None)]
    else:
        symbol = "688001.SH"
        legal.packet["candidates"]["instrument"] = symbol
        legal.base[0] = (0, symbol, 100000, legal.packet["target_date"], 1, 1.)
        legal.st[0] = (0, symbol, *legal.st[0][2:])
    result = legal.reader.load_batch(packets=[legal.packet])[0]
    assert result["receipt"]["candidate_count"] == 1 and not result["contexts"]
    assert result["receipt"]["unavailable"][0]["instrument"] == symbol


def test_last_d_known_st_and_visible_dividend_coordinates(legal):
    legal.st[0] = (0, "000001.SZ", "st_negative", date(2026, 6, 1), date(2026, 5, 29), *legal.st[0][5:])
    legal.actions = [(0, "000001.SZ", date(2025, 12, 31), date(2026, 6, 1), "实施",
        .1, .1, 0., 1., 1., date(2026, 6, 1))]
    value = legal.reader.load_batch(packets=[legal.packet])[0]["contexts"]["000001.SZ"]
    expected, _ = raw._target_raw_price_multiplier(symbol="000001.SZ", decision_raw_close=100.,
        decision_adjustment_factor=1., rows=[legal.actions[0][1:]], decision_as_of_trade_date=legal.packet["decision_date"])
    assert value["target_raw_price_multiplier"] == expected and "MAIN_ST_5PCT" in value["coordinate_source"]
    assert value["raw_legal_low_cny"] == pytest.approx(round(100*expected*.95, 2))


@pytest.mark.parametrize("case", ["foreign", "future_st", "future_action", "conflicting_action"])
def test_future_or_conflicting_coordinate_objects_fail_closed(legal, case):
    if case == "foreign":
        legal.base[0] = (1, *legal.base[0][1:])
    elif case == "future_st":
        legal.st[0] = (0, "000001.SZ", "st_negative", date(2026, 6, 11), date(2026, 6, 11), *legal.st[0][5:])
    else:
        legal.actions = [(0, "000001.SZ", date(2025, 12, 31), date(2026, 6, 1), "实施",
            0., 0., 0., 1., 1., date(2026, 6, 11) if case == "future_action" else date(2026, 6, 1))]
        if case == "conflicting_action":
            legal.actions.append((*legal.actions[0][:8], 2., 2., date(2026, 6, 2)))
    with pytest.raises(AdvisoryModelFirstError):
        legal.reader.load_batch(packets=[legal.packet])


def test_unclosed_d_does_not_read_price_coordinates(legal):
    legal.reader._now = lambda: datetime(2026, 6, 10, 2, tzinfo=timezone.utc)
    result = legal.reader.load_batch(packets=[legal.packet])[0]
    assert not legal.sql and result["receipt"]["status"] == "DEFERRED_D_NOT_CLOSED"
