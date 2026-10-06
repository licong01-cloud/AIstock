"""One hand model packet, no fitting; source identity and business-price contracts."""
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import date
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import generic_price_set_consumer_v1 as consumer
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def packet(tmp_path, family="DAILY_5TD"):
    module, cls, price_set = consumer.FAMILIES[family]
    names = tuple(module.FEATURES) + (() if family == "DAILY_5TD" else tuple(module.MINUTE_FEATURES))
    recipe = dict(medians=[0.] * len(names), policy_sha256=module.POLICY_SHA256,
                  label_contract=module.POLICY["label_contract"], parameters=module.PARAMETERS)
    if family == "DAILY_5TD":
        recipe["features"] = list(names)
        dimensions = (19, 18)
    elif family != "JOINT_DISTRIBUTION_5TD":
        dimensions = (35, 19) if family == "MINUTE_5TD" else (39, 33)
        recipe.update(schema=module.SCHEMA, schema_sha256=module.SCHEMA_SHA256,
                      daily_features=list(module.FEATURES), minute_features=list(module.MINUTE_FEATURES),
                      candidate_dimensions=dimensions[0], matched_dimensions=dimensions[1])
    if family == "JOINT_DISTRIBUTION_5TD":
        recipe.update(schema=module.SCHEMA, schema_sha256=module.SCHEMA_SHA256,
            input_schema_sha256=module.INPUT_SCHEMA_SHA256, path_quantile=.1,
            candidate_dimensions=39, intervals_bps=[[-500, 500]])
        leaf = dict(feature=[-2], threshold=[-2.], left=[-1], right=[-1], members={"0": [0]})
        fitted = cls(recipe, tuple(deepcopy(leaf) for _ in range(128)),
                     dict(terminal=[1.04], path=[.95], keys=[["fixture"]]), {}, "")
    else:
        def tree(dim, initial):
            return dict(kind="gbdt", features=dim, initial=initial, learning_rate=.05,
                        trees=[dict(left=[-1], right=[-1], feature=[-2], threshold=[-2.], value=[0.])])
        models = {arm + "_" + head: tree(dimensions[index], 1.04 if head == "mean" else .95)
                  for index, arm in enumerate(("candidate", "matched")) for head in ("mean", "path")}
        fitted = cls(recipe, models, ((-500., 500.),), {}, "")
    fitted = replace(fitted, model_sha256=module.fitted_identity(fitted))
    stage = publish_stage(study_root=tmp_path / family, stage="trained", plan_sha256="a"*64,
        parent_sha256="b"*64, artifacts={"model.json": json.dumps(asdict(fitted), allow_nan=False).encode()})
    ref = evidence_reference_for_file(stage / "manifest.json", role="test_trained_manifest")
    loaded = consumer.load_generic_price_set_model_v1(model_family=family, trained_manifest_ref=ref)
    values = dict.fromkeys(names, 0.)
    values.update(atr14_close=.01, close_location_in_day=.5, volume_ratio_5_to_20=1.)
    rows = pd.DataFrame([{**values, "decision_as_of_trade_date": date(2026, 6, 10),
                         "target_trade_date": date(2026, 6, 11), "instrument": "000001.SZ",
                         "selection_effective_rank": 1, "candidate_group_size": 50}])
    context = dict(reference_cny=100., legal_low_cny=99., legal_high_cny=102., tick_cny=1.,
                   visible_through="2026-06-10", price_basis="D_ANCHORED_CNY")
    source = dict(package_id="old", run_id=None, list_version_id=None, universe_identity={"mode": "stock_universe"},
                  source_evidence="FIXTURE_NOT_NATIVE", feature_visible_through="2026-06-10")
    return loaded, fitted, rows, {"000001.SZ": context}, source, price_set, ref


@pytest.mark.parametrize("family", tuple(consumer.FAMILIES))
def test_four_family_exact_price_parity_and_package_pool_rank_are_not_inputs(tmp_path, family):
    loaded, fitted, rows, contexts, source, price_set, _ = packet(tmp_path, family)
    original = rows.copy(deep=True), deepcopy(contexts), deepcopy(source)
    result = consumer.project_generic_price_sets_v1(loaded=loaded, candidate_rows=rows,
        price_contexts=contexts, source_context=source)
    args = dict(fitted=fitted, d_features=rows.iloc[0].drop(list(consumer.ROSTER)).to_dict(),
                **{name: contexts["000001.SZ"][name] for name in consumer.PRICES})
    if family != "JOINT_DISTRIBUTION_5TD":
        args["arm"] = "candidate"
    expected = price_set(**args)
    assert result["advice"][0]["intervals_cny"] == expected["intervals_cny"]
    assert result["advice"][0]["status"] == expected["status"]
    changed = rows.copy(deep=True)
    changed["selection_effective_rank"] = 42
    alternate = {**source, "package_id": "new", "universe_identity": {"mode": "index_union", "indices": ["000300.SH", "000905.SH"]}}
    again = consumer.project_generic_price_sets_v1(loaded=loaded, candidate_rows=changed,
        price_contexts=contexts, source_context=alternate)
    assert again["advice"][0]["intervals_cny"] == expected["intervals_cny"]
    assert again["advice"][0]["selection_effective_rank"] == 42 and again["advice"][0]["holding_sessions"] == 5
    assert result["input_sha256"] != again["input_sha256"] and not again["economic_confirmation"]
    assert again["fit_count"] == 0 and not any(again[key] for key in ("outcomes_read", "database_write", "model_activation", "qualification_rechecked", "deployable"))
    pd.testing.assert_frame_equal(rows, original[0])
    assert contexts == original[1] and source == original[2]


def test_complete_roster_unknown_and_empty_population_do_not_query_or_fill(tmp_path):
    loaded, _, rows, contexts, source, _, _ = packet(tmp_path)
    rows.loc[0, "ret_5"] = np.nan
    result = consumer.project_generic_price_sets_v1(loaded=loaded, candidate_rows=rows, price_contexts={}, source_context=source)
    assert result["candidate_count"] == 1 and result["advice"][0]["status"] == "UNKNOWN_PRICE_CONTEXT"
    assert result["advice"][0]["input_unknown_fields"] == ["ret_5"]
    result = consumer.project_generic_price_sets_v1(loaded=loaded, candidate_rows=rows, price_contexts=contexts,
        source_context={**source, "feature_visible_through": None})
    assert result["advice"][0]["status"] == "UNKNOWN_FEATURE_CLOCK"
    empty = consumer.project_generic_price_sets_v1(loaded=loaded, candidate_rows=rows.iloc[:0], price_contexts={}, source_context=source)
    assert empty["status"] == "NO_CANDIDATES" and empty["advice"] == []


def test_entire_batch_validates_future_and_bad_values_before_query(tmp_path, monkeypatch):
    loaded, _, rows, contexts, source, _, _ = packet(tmp_path)
    module, cls, _ = consumer.FAMILIES["DAILY_5TD"]
    def forbidden(**_):
        pytest.fail("model query ran before whole-batch validation")
    monkeypatch.setitem(consumer.FAMILIES, "DAILY_5TD", (module, cls, forbidden))
    for broken_source, broken_rows, broken_context in (
        ({**source, "feature_visible_through": "2026-06-11"}, rows, contexts),
        (source, pd.concat([rows, rows], ignore_index=True), contexts),
        (source, rows.assign(ret_1=np.inf), contexts),
        (source, rows, {"000001.SZ": {**contexts["000001.SZ"], "visible_through": "2026-06-11"}}),
    ):
        with pytest.raises(AdvisoryModelFirstError):
            consumer.project_generic_price_sets_v1(loaded=loaded, candidate_rows=broken_rows,
                price_contexts=broken_context, source_context=broken_source)


def test_manifest_weights_missing_identity_and_snapshot_tamper_fail_closed(tmp_path):
    loaded, _, _, _, _, _, ref = packet(tmp_path)
    with pytest.raises(AdvisoryModelFirstError):
        consumer.load_generic_price_set_model_v1(model_family="MINUTE_5TD", trained_manifest_ref=ref)
    path = tmp_path / "DAILY_5TD" / "trained" / "model.json"
    path.write_bytes(path.read_bytes()+b" ")
    with pytest.raises(AdvisoryModelFirstError):
        consumer.load_generic_price_set_model_v1(model_family="DAILY_5TD", trained_manifest_ref=ref)
    path.unlink()
    with pytest.raises(AdvisoryModelFirstError) as error:
        consumer.load_generic_price_set_model_v1(model_family="DAILY_5TD", trained_manifest_ref=ref)
    assert error.value.reason_code == "ADVISORY_GENERIC_MODEL_NOT_TRAINED"
    with pytest.raises(AdvisoryModelFirstError):
        consumer.project_generic_price_sets_v1(loaded=replace(loaded, trained_manifest_sha256="c"*64),
            candidate_rows=pd.DataFrame(), price_contexts={}, source_context={})


def test_deadline_never_returns_a_partial_success(tmp_path):
    loaded, _, rows, contexts, source, _, _ = packet(tmp_path)
    clock = iter([0., 0., 31.])
    with pytest.raises(AdvisoryModelFirstError, match="partial batch"):
        consumer.project_generic_price_sets_v1(loaded=loaded, candidate_rows=rows, price_contexts=contexts,
            source_context=source, monotonic=lambda: next(clock))


def test_unknown_support_holes_are_not_bridged_or_confused_with_known_empty(tmp_path):
    loaded, fitted, rows, contexts, source, _, _ = packet(tmp_path)
    known_empty = {"000001.SZ": {**contexts["000001.SZ"], "legal_low_cny": 104., "legal_high_cny": 105.}}
    result = consumer.project_generic_price_sets_v1(loaded=loaded, candidate_rows=rows,
        price_contexts=known_empty, source_context=source)
    assert result["advice"][0]["status"] == "NO_ACCEPTABLE_PRICE" and not result["advice"][0]["intervals_cny"]
    fitted = replace(fitted, intervals_bps=((-100., -100.), (100., 100.)))
    fitted = replace(fitted, model_sha256=consumer.daily.fitted_identity(fitted))
    stage = publish_stage(study_root=tmp_path / "hole", stage="trained", plan_sha256="a"*64, parent_sha256="b"*64,
        artifacts={"model.json": json.dumps(asdict(fitted), allow_nan=False).encode()})
    hole = consumer.load_generic_price_set_model_v1(model_family="DAILY_5TD",
        trained_manifest_ref=evidence_reference_for_file(stage / "manifest.json", role="hole_manifest"))
    result = consumer.project_generic_price_sets_v1(loaded=hole, candidate_rows=rows, price_contexts=contexts, source_context=source)
    assert result["advice"][0]["intervals_cny"] == ((99., 99.), (101., 101.))
    assert result["advice"][0]["unknown_node_count"] == 2


def test_bounded_json_rejects_duplicate_and_nonfinite_values_without_changing_shared_hash():
    for body in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}', b'[]'):
        with pytest.raises(AdvisoryModelFirstError):
            consumer._object(body)
