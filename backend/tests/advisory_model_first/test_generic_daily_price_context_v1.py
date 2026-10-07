"""Hand-model projection checks only; no training or production configuration."""
from copy import deepcopy
from dataclasses import asdict, replace
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import generic_daily_price_context_v1 as raw
from backend.services.advisory_model_first import generic_price_set_consumer_v1 as consumer
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_generic_price_set_consumer_v1 import packet


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
