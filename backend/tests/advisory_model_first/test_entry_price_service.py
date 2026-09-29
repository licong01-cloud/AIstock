from dataclasses import replace
from datetime import date

import pandas as pd
import pytest

from backend.services.advisory_model_first.entry_price_service import score_entry_price_bundle
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.price_range_inference import score_price_range_bundle
from backend.tests.advisory_model_first.test_price_range_inference import (
    _calibrated_bundle, _context, _features, _outcome,
)

D, T = date(2026, 7, 20), date(2026, 7, 21)
POLICY = {"stop_loss_bps": 800, "take_profit_bps": 1800, "trailing_stop_bps": 700, "take_profit_mode": "trailing"}


def entry_inputs():
    features = _features().assign(decision_as_of_trade_date=D, target_trade_date=T)
    return {
        "bundle": _calibrated_bundle(), "features": features,
        "contexts": {symbol: _context(symbol) for symbol in features.instrument},
        "context_unavailable": (), "decision_as_of_trade_date": D, "target_trade_date": T,
    }


def test_independent_entry_matches_legacy_projection_without_any_m3_input():
    inputs = entry_inputs()
    scored = score_entry_price_bundle(**inputs)
    legacy = score_price_range_bundle(
        inputs["bundle"], inputs["features"], contexts=inputs["contexts"], context_unavailable=(),
        outcome_candidates=[_outcome(symbol) for symbol in inputs["features"].instrument],
        review_policy=POLICY, review_policy_sha256="a" * 64, target_trade_date=T,
    )
    for item, old in zip(scored, legacy, strict=True):
        row = item.candidate.model_dump(mode="json")
        assert row["entry_price"]["raw_range"] == old["entry_price_range"]
        assert row["entry_price"]["calibrated_range"] == old["calibrated_entry_price_range"]
        assert row["entry_price"]["calibration"] == old["entry_gap_calibration"]
        assert row["take_profit"]["status"] == row["protective"]["status"] == row["stop_loss"]["status"] == "UNAVAILABLE"
        assert item.raw_gaps[0] < item.raw_gaps[2]
        assert item.calibrated_gaps[0] < item.raw_gaps[0]


def test_missing_context_preserves_roster_and_last_close_can_precede_decision():
    inputs = entry_inputs()
    first = "000001.SZ"
    inputs["contexts"] = {first: replace(inputs["contexts"][first], decision_price_trade_date=date(2026, 7, 17))}
    inputs["context_unavailable"] = ({"symbol": "000002.SZ", "reason_code": "NO_PRICE", "message": "no historical close"},)
    result = score_entry_price_bundle(**inputs)
    assert [row.candidate.symbol for row in result] == ["000001.SZ", "000002.SZ"]
    assert result[0].candidate.decision_price_trade_date == date(2026, 7, 17)
    assert result[1].candidate.entry_price.status == "UNAVAILABLE"
    assert result[1].candidate.entry_price.reason_code == "NO_PRICE"
    assert result[1].candidate.entry_price.raw_range is None


@pytest.mark.parametrize("cash,stock", [(0.0, 0.0), (0.5, 0.2)])
def test_adjusted_training_label_and_pit_raw_projection_share_the_same_coordinate(cash, stock):
    from backend.services.advisory_model_first.price_range_labels import build_daily_price_envelope_labels
    from backend.services.advisory_model_first.price_range_inference import _entry_band
    from backend.services.advisory_model_first.price_range_regulatory import resolve_regulatory_price_range
    from backend.services.advisory_model_first.realtime_feature_source import _target_raw_price_multiplier
    symbol, close, raw_open = "000001.SZ", 10.0, 8.0 if stock else 10.2
    action = (symbol, D, T, None, stock, stock, 0.0, cash, cash, D)
    multiplier, _ = _target_raw_price_multiplier(
        symbol=symbol,
        decision_raw_close=close,
        decision_adjustment_factor=1.7,
        rows=[action] if cash or stock else [],
        decision_as_of_trade_date=D,
    )
    # The Qlib adjusted coordinate is raw * factor; ex-date factor ratio is 1 / multiplier.
    factor_d, factor_t = 1.7, 1.7 / multiplier
    daily = pd.DataFrame({"open": [close * factor_d, raw_open * factor_t],
        "close": [close * factor_d, raw_open * factor_t]},
        index=pd.MultiIndex.from_tuples([(pd.Timestamp(D), symbol), (pd.Timestamp(T), symbol)], names=["datetime", "instrument"]))
    labels = build_daily_price_envelope_labels(
        candidates=pd.DataFrame([dict(instrument=symbol, decision_as_of_trade_date=D, target_trade_date=T)]),
        daily=daily, suspend_rows=pd.DataFrame(columns=["trade_date", "instrument"]), trading_calendar=pd.to_datetime([D, T]))
    gap = labels.labels.iloc[0].entry_gap_return
    assert gap == pytest.approx(raw_open / (close * multiplier) - 1)
    context = replace(_context(symbol), target_raw_price_multiplier=multiplier)
    regulatory = resolve_regulatory_price_range(context, target_trade_date=T)
    band = _entry_band(symbol=symbol, context=context, regulatory=regulatory, entry_gaps=(gap, gap, gap))
    assert band[1] == pytest.approx(raw_open)


@pytest.mark.parametrize("invalid", ["duplicate", "future_feature", "future_price", "foreign_context", "conflicting_error"])
def test_identity_and_pit_violations_fail_closed(invalid):
    inputs = entry_inputs()
    if invalid == "duplicate":
        inputs["features"].loc[1, "instrument"] = "000001.SZ"
    elif invalid == "future_feature":
        inputs["features"].loc[0, "decision_as_of_trade_date"] = T
    elif invalid == "future_price":
        inputs["contexts"]["000001.SZ"] = replace(inputs["contexts"]["000001.SZ"], decision_price_trade_date=T)
    elif invalid == "foreign_context":
        inputs["contexts"]["000003.SZ"] = _context("000003.SZ")
    else:
        inputs["context_unavailable"] = ({"symbol": "000001.SZ", "reason_code": "FAILED", "message": "conflict"},)
    with pytest.raises(AdvisoryModelFirstError, match="entry"):
        score_entry_price_bundle(**inputs)


def test_future_market_columns_never_enter_frozen_model_matrix():
    inputs = entry_inputs()
    expected = score_entry_price_bundle(**inputs)
    inputs["features"]["future_target_open"] = [float("inf"), -1000000]
    assert score_entry_price_bundle(**inputs) == expected


def test_empty_group_does_not_invoke_model_or_invent_recommendations():
    inputs = entry_inputs()
    inputs["features"] = pd.DataFrame(columns=["instrument", "decision_as_of_trade_date", "target_trade_date"])
    inputs["contexts"] = {}
    assert score_entry_price_bundle(**inputs) == []


def integrated_service():
    from types import SimpleNamespace
    from backend.services.advisory_model_first.entry_price_contracts import EntryPriceScope
    from backend.services.advisory_model_first.entry_price_service import AdvisoryEntryPriceService
    from backend.tests.advisory_model_first.test_model_inference import (
        _ProgramService, _SelectionService, _ReviewSource, _FeatureSource,
        _feature_inputs, _bundle, PROGRAM_ID, BINDING_VERSION_ID, LSTM_LEG_ID, FUND_LEG_ID,
    )

    universe = {"mode": "stock_universe", "pool_ids": []}
    class Programs(_ProgramService):
        def recommendation_list_version_detail(self, list_version_id):
            detail = super().recommendation_list_version_detail(list_version_id)
            detail["list_version"]["summary_json"] = {"advisory_universe_receipt": {"universe_selection": universe}}
            return detail
    programs = Programs(target_count=20)
    programs.binding["universe_selection"] = universe
    programs.calendar_provider = SimpleNamespace(list_trading_days=lambda *_args: [D, T])
    parent = _bundle()
    class ForbiddenRanking:
        def predict(self, *_args, **_kwargs):
            raise AssertionError("entry must never execute the parent ranking head")
    parent = replace(parent, booster=ForbiddenRanking())
    scope = EntryPriceScope(
        package_id=parent.manifest["package_id"], package_manifest_sha256=parent.manifest["manifest_sha256"],
        style_profile_id=parent.manifest["style_profile_id"], style_profile_hash=parent.manifest["style_profile_hash"],
        selection_runtime_semantics_hash=parent.manifest["selection_runtime_semantics_hash"],
        feature_schema_sha256=parent.manifest["feature_schema_hash"],
        parent_bundle_id="d" * 64, parent_bundle_manifest_sha256=parent.manifest_file_sha256,
        outcome_bundle_id="e" * 64, price_range_bundle_id="f" * 64,
        price_range_bundle_manifest_sha256="a" * 64, review_policy_sha256=programs.program.review_policy_sha256,
        component_roles={"lstm": LSTM_LEG_ID, "fund": FUND_LEG_ID}, universe_selection=universe,
    )
    source = _FeatureSource(replace(
        _feature_inputs(), price_range_contexts={s: _context(s) for s in ("000001.SZ", "000002.SZ")},
    ))
    selection = _SelectionService()
    selection.run.run_id = "selection-1"
    service = AdvisoryEntryPriceService(
        program_service=programs, selection_service=selection, review_source=_ReviewSource(),
        feature_source=source, parent_loader=lambda **_kwargs: parent,
        price_loader=lambda **_kwargs: _calibrated_bundle(),
    )
    return service, source, dict(
        model_root="/model", program_id=PROGRAM_ID, binding_version_id=BINDING_VERSION_ID,
        target_trade_date=T, scope=scope, role_binding_sha256="b" * 64,
    )


def test_service_reads_real_feature_builder_without_ranking_or_outcome_execution():
    service, source, request = integrated_service()
    envelope = service.evaluate(**request)
    assert envelope.available_count == envelope.candidate_count == 2
    assert envelope.auxiliary_availability == "UNAVAILABLE"
    assert source.last_kwargs["decision_as_of_trade_date"] == D
    assert envelope.training_lineage.parent_bundle_id == request["scope"].parent_bundle_id


def test_verified_empty_selection_is_no_candidates_and_never_runs_features():
    service, source, args = integrated_service()
    service._reader._selection_service.run.aggregate_results = []
    original = service._programs.recommendation_list_version_detail
    def detail(identity):
        value = original(identity)
        value["items"] = []
        return value
    service._programs.recommendation_list_version_detail = detail
    def forbidden(**_kwargs):
        raise AssertionError("empty candidates cannot execute features or models")
    source.load = forbidden
    result = service.evaluate_day(**args)
    assert result.envelope.candidate_count == 0 and result.envelope.reason_code == "NO_CANDIDATES"
    assert result.scored == () and result.envelope.availability_status == "UNAVAILABLE"


@pytest.mark.parametrize("keep_symbols", [("000001.SZ",), ()])
def test_service_missing_history_retains_unavailable_candidate(keep_symbols):
    service, source, request = integrated_service()
    daily = source.inputs.candidate_daily
    source.inputs = replace(source.inputs, candidate_daily=daily[daily.index.get_level_values("instrument").isin(keep_symbols)])
    envelope = service.evaluate(**request)
    assert envelope.candidate_count == 2
    assert envelope.availability_status == ("PARTIAL" if keep_symbols else "UNAVAILABLE")
    assert envelope.unavailable_count == 2 - len(keep_symbols)
    assert envelope.candidates[1].entry_price.reason_code == "ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING"


def test_service_rejects_universe_drift_before_model_loading():
    service, source, request = integrated_service()
    service._programs.binding["universe_selection"] = None
    with pytest.raises(AdvisoryModelFirstError, match="universe"):
        service.evaluate(**request)
    assert source.calls == 0
