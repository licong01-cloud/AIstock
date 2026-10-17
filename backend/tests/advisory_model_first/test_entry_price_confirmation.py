import numpy as np
import pytest
from datetime import date

from backend.services.advisory_model_first.entry_price_confirmation import (
    _interval_metrics, evaluate_entry_price_confirmation, moving_block_interval,
)
from backend.services.advisory_model_first.entry_price_confirmation_contracts import (
    EntryPriceConfirmationPredictionDay, EntryPriceConfirmationSettlementDay,
    build_entry_price_confirmation_request,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_entry_price_confirmation_contracts import request_values
from backend.tests.advisory_model_first.test_entry_price_service import integrated_service


def experiment(tmp_path, *, formal=True, dates=20):
    request = build_entry_price_confirmation_request(**request_values(tmp_path, formal=formal, dates=dates))
    service, _, args = integrated_service()
    base = service.evaluate(**args).as_payload()
    template = base["candidates"][0]
    predictions, outcomes = [], []
    for day in request.days:
        rows, candidates, settled = [], [], []
        for i, symbol in enumerate(day.candidate_symbols):
            candidate = dict(template, symbol=symbol, decision_price_trade_date=day.decision_as_of_trade_date,
                             decision_reference_price=10.0, target_raw_price_multiplier=1.0)
            band = dict(template["entry_price"]["calibrated_range"], low=9.8, mid=10.0, high=10.2)
            candidate["entry_price"] = dict(template["entry_price"], raw_range=band, calibrated_range=band,
                                            calibration=dict(template["entry_price"]["calibration"], delta=0.0))
            candidates.append(candidate)
            rows.append(dict(symbol=symbol, raw_gaps=(-0.02, 0, 0.02), calibrated_gaps=(-0.02, 0, 0.02),
                             control_range=dict(band, low=9.6, mid=10, high=10.4)))
            settled.append(dict(symbol=symbol, market_status="AVAILABLE", raw_open=10.0 if i < 16 else 10.3))
        envelope = dict(base, program_id=request.program_id, binding_version_id=request.binding_version_id,
                        role_binding_sha256=request.request_sha256, candidates=candidates, candidate_count=20,
                        available_count=20, unavailable_count=0, decision_as_of_trade_date=day.decision_as_of_trade_date,
                        target_trade_date=day.target_trade_date)
        predictions.append(EntryPriceConfirmationPredictionDay(
            envelope=envelope, rows=rows, candidate_source_sha256=day.candidate_source_sha256, feature_values_sha256="b" * 64,
        ))
        outcomes.append(EntryPriceConfirmationSettlementDay(
            target_trade_date=day.target_trade_date, outcomes=settled, source_sha256="d" * 64,
        ))
    return request, predictions, outcomes


def test_manual_interval_score_and_calendar_block_bootstrap():
    assert _interval_metrics((-0.02, 0, 0.02), 0.03)["interval_score"] == pytest.approx(0.14)
    values = np.arange(20) / 1000
    first = moving_block_interval(values, request_hash="a" * 64, block_days=5, samples=5000)
    assert first == moving_block_interval(values, request_hash="a" * 64, block_days=5, samples=5000)
    assert first[0] < values.mean() < first[1]
    assert moving_block_interval([-0.02] * 20, request_hash="b" * 64, block_days=5, samples=5000) == pytest.approx([-0.02, -0.02])


def test_replay_capacity_not_qe_history_controls_execution(monkeypatch, tmp_path):
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace
    from backend.services.advisory_model_first.entry_price_confirmation import require_entry_replay_execution
    from backend.services.advisory_model_first import entry_price_daily_service
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    request = SimpleNamespace(request_sha256="a" * 64, registry_path=tmp_path / "registry.jsonl")
    slot = dict(authorization_ref="QE-window-test-only", request_sha256=request.request_sha256,
                starts_at=now - timedelta(minutes=1), expires_at=now + timedelta(hours=1))
    monkeypatch.setattr(entry_price_daily_service.QEEntryResourceGuard, "check",
                        lambda *_a: pytest.fail("replay must not scan QE history or require idle QE"))
    calls = []
    def capacity(path):
        calls.append(path)
        return {"status": "REPLAY_CAPACITY_AVAILABLE"}
    for invalid in (dict(slot, request_sha256="b" * 64), dict(slot, expires_at=now)):
        with pytest.raises(AdvisoryModelFirstError) as error:
            require_entry_replay_execution(request, invalid, now=now, capacity_probe=capacity)
        assert error.value.reason_code == "ADVISORY_ENTRY_RESOURCE_WAITING"
    assert not calls
    require_entry_replay_execution(request, None, now=now, capacity_probe=capacity)
    require_entry_replay_execution(request, slot, now=now, capacity_probe=capacity)
    require_entry_replay_execution(request, None, capacity_probe=capacity, output_root=tmp_path / "other-volume")
    with pytest.raises(AdvisoryModelFirstError):
        require_entry_replay_execution(request, None, capacity_probe=lambda _p: {"status": "WAITING_RESOURCE"})
    assert calls == [tmp_path, tmp_path, tmp_path / "other-volume"]


@pytest.mark.parametrize("violation", [None, "quantile", "duplicate"])
def test_input_verifier_reads_only_original_validation_labels_and_bound_candidates(tmp_path, monkeypatch, violation):
    import json
    import pandas as pd
    from types import SimpleNamespace
    from backend.services.advisory_model_first import entry_price_confirmation as module
    from backend.services.advisory_model_first.research_control import evidence_reference_for_file
    from backend.services.advisory_model_first.entry_price_service import _frame_sha256
    values = request_values(tmp_path, formal=False, dates=2)
    evidence = tmp_path / "review.json"
    evidence.write_text("{}", encoding="utf-8")
    for name in ("vintage_evidence", "candidate_provenance", "consumption_review"):
        values["data_identity"][name] = evidence_reference_for_file(evidence, role=name)
    source = tmp_path / "price_range_runs" / "original-run" / "daily_price_envelope_labels.parquet"
    source.parent.mkdir(parents=True)
    rows = [dict(decision_as_of_trade_date=d, target_trade_date=pd.Timestamp(d) + pd.Timedelta(days=1),
                 instrument=symbol, entry_gap_return=gap, entry_gap_label_status="AVAILABLE", gap_modelable=True, split="validation")
            for d in values["control"]["validation_dates"] for symbol, gap in (("000001.SZ", -0.05), ("000002.SZ", 0.05))]
    if violation == "duplicate":
        rows.append(dict(rows[0]))
    rows.append(dict(rows[0], decision_as_of_trade_date="2026-01-01", entry_gap_return=999, split="test"))
    pd.DataFrame(rows).to_parquet(source, index=False)
    values["control"].update(validation_labels=evidence_reference_for_file(source, role="labels"),
                             q10=-0.04 if violation == "quantile" else -0.05, q90=0.05)
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "split.json").write_text(json.dumps({"validation": values["control"]["validation_dates"]}), encoding="utf-8")
    monkeypatch.setattr("backend.services.advisory_model_first.price_range_runtime_bundle.load_frozen_price_range_bundle",
        lambda **_kw: SimpleNamespace(bundle_path=bundle, manifest={"parent_price_range_request_id": "original-run"}))
    monkeypatch.setattr("backend.services.advisory_model_first.model_bundle.load_frozen_research_bundle",
        lambda **_kw: SimpleNamespace(manifest_file_sha256=values["scope"].parent_bundle_manifest_sha256))
    prepared = {}
    for day in values["days"]:
        frame = pd.DataFrame({"instrument": day["candidate_symbols"]})
        day["candidate_source_sha256"] = _frame_sha256(frame)
        prepared[day["target_trade_date"]] = SimpleNamespace(decision_date=day["decision_as_of_trade_date"], candidates=frame)
    def metadata_reader(*, metadata_only=False):
        assert metadata_only, "input verification must not load inference predictors"
        return SimpleNamespace(prepare_day=lambda **kw: prepared[kw["target_trade_date"]])
    monkeypatch.setattr(module, "_cached_day_service", metadata_reader)
    request = build_entry_price_confirmation_request(**values)
    if violation:
        with pytest.raises(AdvisoryModelFirstError, match="control"):
            module.verify_confirmation_inputs(request, model_root=tmp_path)
    else:
        module.verify_confirmation_inputs(request, model_root=tmp_path)


@pytest.mark.parametrize("metadata_only", [True, False])
def test_day_loader_metadata_mode_does_not_hide_inference_dependency_errors(monkeypatch, metadata_only):
    from types import SimpleNamespace
    from backend.services.advisory_model_first.entry_price_confirmation import _cached_day_service
    from backend.services.advisory_model_first import model_bundle, price_range_runtime_bundle, entry_price_service
    from backend.services.advisory_model_first import entry_price_replay_runtime
    from backend.services import advisory_program

    calls = []
    def loader(*, booster_factory=None, **kwargs):
        if booster_factory is None:
            raise AdvisoryModelFirstError("real inference dependency unavailable", reason_code="ADVISORY_MODEL_BUNDLE_INVALID")
        booster = booster_factory("unused-model-path")
        calls.append(kwargs["bundle_id"])
        return SimpleNamespace(booster=booster)
    def unavailable_predictor(_path):
        raise AdvisoryModelFirstError("real inference dependency unavailable", reason_code="ADVISORY_MODEL_BUNDLE_INVALID")
    monkeypatch.setattr(entry_price_replay_runtime, "load_replay_booster", unavailable_predictor)
    monkeypatch.setattr(model_bundle, "load_frozen_research_bundle", loader)
    monkeypatch.setattr(price_range_runtime_bundle, "load_frozen_price_range_bundle", loader)
    monkeypatch.setattr(advisory_program, "AdvisoryProgramService", lambda **kwargs: object())
    monkeypatch.setattr(entry_price_service, "AdvisoryEntryPriceService", lambda **kwargs: SimpleNamespace(**kwargs))
    reader = _cached_day_service(metadata_only=metadata_only)
    for kind, factory in (("parent", reader.parent_loader), ("price", reader.price_loader)):
        if metadata_only:
            assert factory(bundle_id=kind).booster is None
        else:
            with pytest.raises(AdvisoryModelFirstError, match="real inference dependency"):
                factory(bundle_id=kind)
    assert calls == (["parent", "price"] if metadata_only else [])


def test_continuous_projection_and_calibration_cannot_diverge_from_displayed_prices(tmp_path):
    from pydantic import ValidationError
    _, predictions, _ = experiment(tmp_path)
    for field, value in (("raw_gaps", [-0.03, 0, 0.02]), ("calibrated_gaps", [-0.03, 0, 0.02])):
        payload = predictions[0].model_dump(mode="json")
        payload["rows"][0][field] = value
        with pytest.raises(ValidationError, match="calibration"):
            EntryPriceConfirmationPredictionDay.model_validate(payload)
    payload = predictions[0].model_dump(mode="json")
    payload["envelope"]["candidates"][0]["entry_price"]["calibrated_range"]["low"] = 9.7
    with pytest.raises(ValidationError, match="projection"):
        EntryPriceConfirmationPredictionDay.model_validate(payload)


def test_full_support_confirmed_price_distribution_not_profit(tmp_path):
    request, predictions, outcomes = experiment(tmp_path)
    result = evaluate_entry_price_confirmation(request, predictions, outcomes)
    assert result["status"] == "CONFIRMED_PRICE_DISTRIBUTION"
    assert result["valid_rows"] == 400
    assert result["date_weighted"]["continuous_coverage"] == pytest.approx(0.8)
    assert result["interval_score_difference_ci95"] == pytest.approx([-0.02, -0.02])
    assert result["business_width_ratio"] == pytest.approx(0.5)
    assert not result["profitability_confirmed"] and not result["binding_activated"]


def test_old_window_or_short_sample_cannot_confirm(tmp_path):
    for formal, dates in ((False, 20), (True, 19)):
        result = evaluate_entry_price_confirmation(*experiment(tmp_path, formal=formal, dates=dates))
        assert result["status"] == "INCONCLUSIVE"
        assert result["decision_use"] == "NAVIGATION_ONLY"


def test_market_absence_is_not_an_economic_failure(tmp_path):
    request, predictions, outcomes = experiment(tmp_path)
    payload = outcomes[0].model_dump(mode="json")
    payload["outcomes"][0].update(market_status="UNAVAILABLE", raw_open=None, reason_code="UNKNOWN")
    outcomes[0] = EntryPriceConfirmationSettlementDay.model_validate(payload)
    result = evaluate_entry_price_confirmation(request, predictions, outcomes)
    assert result["status"] == "INPUT_INCOMPLETE"
    assert result["counts"]["total_candidates"] == 400
    assert result["counts"]["market_unknown"] == 1


def test_suspension_does_not_hide_quantile_crossing(tmp_path):
    request, predictions, outcomes = experiment(tmp_path)
    prediction = predictions[0].model_dump(mode="json")
    prediction["rows"][0]["raw_gaps"] = [0, -0.02, 0.02]
    predictions[0] = EntryPriceConfirmationPredictionDay.model_validate(prediction)
    outcome = outcomes[0].model_dump(mode="json")
    outcome["outcomes"][0].update(market_status="NOT_APPLICABLE", raw_open=None, reason_code="AUTHORITATIVE_SUSPENSION")
    outcomes[0] = EntryPriceConfirmationSettlementDay.model_validate(outcome)
    result = evaluate_entry_price_confirmation(request, predictions, outcomes)
    assert result["status"] == "NOT_CONFIRMED" and result["counts"]["prediction_crossings"] == 1
    assert result["counts"]["suspended"] == 1 and result["counts"]["total_candidates"] == 400


def test_future_or_foreign_prediction_rejected_not_omitted(tmp_path):
    request, predictions, outcomes = experiment(tmp_path)
    with pytest.raises(AdvisoryModelFirstError, match="calendar"):
        evaluate_entry_price_confirmation(request, predictions[:-1], outcomes)
    payload = predictions[0].model_dump(mode="json")
    payload["candidate_source_sha256"] = "e" * 64
    predictions[0] = EntryPriceConfirmationPredictionDay.model_validate(payload)
    with pytest.raises(AdvisoryModelFirstError, match="scope"):
        evaluate_entry_price_confirmation(request, predictions, outcomes)


@pytest.mark.parametrize("failure", ["capacity", "dependency"])
def test_replay_preflight_failure_does_not_consume_window(tmp_path, monkeypatch, failure):
    from backend.services.advisory_model_first import entry_price_confirmation as module
    from backend.services.advisory_model_first import entry_price_replay_runtime as runtime

    def reject(*_args, **_kwargs):
        raise AdvisoryModelFirstError("preflight unavailable", reason_code=(
            "ADVISORY_ENTRY_RESOURCE_WAITING" if failure == "capacity"
            else "ADVISORY_ENTRY_REPLAY_DEPENDENCY_UNAVAILABLE"))

    service = module.AdvisoryEntryPriceConfirmationService(
        input_verifier=lambda *_a, **_kw: None,
        execution_guard=reject if failure == "capacity" else lambda *_a: None)
    monkeypatch.setattr(runtime, "validate_replay_dependencies", reject)
    monkeypatch.setattr(module, "_authorize_or_resume", lambda *_a: pytest.fail("no window consumption"))
    values = request_values(tmp_path, formal=False, dates=2)
    request_path = service.prepare(spec=values, model_root=tmp_path, output_root=tmp_path)
    registry = tmp_path / "registry.jsonl"
    registered_before = registry.read_bytes()
    with pytest.raises(AdvisoryModelFirstError):
        service.predict(request_path=request_path, model_root=tmp_path, output_root=tmp_path)
    assert not (request_path.parent / "consuming.json").exists()
    assert registry.read_bytes() == registered_before


def test_pipeline_freezes_all_predictions_before_outcomes_and_exact_retry(tmp_path):
    from dataclasses import replace
    from types import SimpleNamespace
    from backend.services.advisory_model_first.entry_price_confirmation import (
        AdvisoryEntryPriceConfirmationService, read_confirmed_entry_price_artifact,
    )
    from backend.services.advisory_model_first.entry_price_service import ScoredEntryPrice
    from backend.tests.advisory_model_first.test_price_range_inference import _context

    request, predictions, outcomes = experiment(tmp_path)
    events = []
    by_date = {row.envelope.target_trade_date: row for row in predictions}

    class DayService:
        def evaluate_day(self, **kwargs):
            day = kwargs["target_trade_date"]
            events.append(("predict", day))
            prediction = by_date[day]
            assert kwargs["role_binding_sha256"] == request.request_sha256
            return SimpleNamespace(
                envelope=prediction.envelope,
                contexts={row.symbol: replace(_context(row.symbol), decision_raw_close=10.0,
                          decision_price_trade_date=prediction.envelope.decision_as_of_trade_date) for row in prediction.rows},
                scored=tuple(ScoredEntryPrice(row, continuous.raw_gaps, continuous.calibrated_gaps)
                             for row, continuous in zip(prediction.envelope.candidates, prediction.rows)),
                candidate_source_sha256=prediction.candidate_source_sha256,
                feature_values_sha256=prediction.feature_values_sha256,
            )

    class Outcomes:
        def load(self, *, symbols, target_trade_date):
            assert (request_path.parent / "prediction" / "all.json").is_file()
            assert sum(kind == "predict" for kind, _ in events) == 20
            events.append(("settle", target_trade_date))
            return next(row for row in outcomes if row.target_trade_date == target_trade_date)

    service = AdvisoryEntryPriceConfirmationService(day_service=DayService(), outcome_source=Outcomes(),
        input_verifier=lambda *_a, **_kw: None, execution_guard=lambda *_a: None)
    request_path = service.prepare(spec=request.functional_payload(), model_root=tmp_path, output_root=tmp_path)
    args = dict(request_path=request_path, model_root=tmp_path, output_root=tmp_path)
    with pytest.raises(FileNotFoundError):
        service.settle(**args)
    assert events == []
    prediction = service.predict(**args)
    assert service.predict(**args) == prediction
    settlement = service.settle(**args)
    assert service.settle(**args) == settlement
    evaluation = service.evaluate(**args)
    assert service.evaluate(**args) == evaluation
    assert evaluation["evaluation"]["status"] == "CONFIRMED_PRICE_DISTRIBUTION"
    assert len(events) == 40
    assert (request_path.parent / "manifest.json").is_file()
    confirmed, readback = read_confirmed_entry_price_artifact(request_path.parent / "evaluation.json", recompute=True)
    assert confirmed == request and readback == evaluation["evaluation"]
    assert len((tmp_path / "registry.jsonl").read_text(encoding="utf-8").splitlines()) == 2
    # A changed candidate may not reuse a consumed frontier/window, even under a fresh request ID.
    changed = request.functional_payload()
    changed["control"]["q90"] = 0.05
    other_path = service.prepare(spec=changed, model_root=tmp_path, output_root=tmp_path)
    with pytest.raises(AdvisoryModelFirstError):
        service.predict(request_path=other_path, model_root=tmp_path, output_root=tmp_path)
    assert len(events) == 40


def _allow_historical_read_timeout(cursor, monkeypatch):
    original_execute = cursor.execute

    def execute(sql, params):
        if sql == "SET LOCAL statement_timeout = %s":
            assert params == (30_000,)
            cursor.sql.append(sql)
        else:
            original_execute(sql, params)

    monkeypatch.setattr(cursor, "execute", execute)


def test_historical_outcome_source_preserves_unknown_and_suspended_rows(monkeypatch):
    from backend.services.advisory_model_first.entry_price_confirmation import PostgresEntryPriceConfirmationOutcomeSource
    from backend.tests.advisory_model_first.test_price_range_prospective_evaluation_boundaries import _source

    old_source, conn, cursor = _source(opens=[("000001.SZ", 10120)], suspends=[("000002.SZ", "S")])
    _allow_historical_read_timeout(cursor, monkeypatch)
    source = PostgresEntryPriceConfirmationOutcomeSource(connection_context_factory=old_source._connection_context_factory)
    outcome = source.load(symbols=("000001.SZ", "000002.SZ", "000003.SZ"), target_trade_date=date(2026, 9, 15))
    assert outcome.outcomes[0].raw_open == 10.12
    assert [row.market_status for row in outcome.outcomes] == ["AVAILABLE", "NOT_APPLICABLE", "UNAVAILABLE"]
    assert conn.session["readonly"] and conn.rollbacks == 1
    assert cursor.sql[0] == "SET LOCAL statement_timeout = %s"


@pytest.mark.parametrize("opens,suspends", [
    ([("000001.SZ", 10000), ("000001.SZ", 10001)], []),
    ([("000001.SZ", 10000)], [("000001.SZ", "S")]),
    ([("000001.SZ", -1)], []),
    ([], [("000001.SZ", "S"), ("000001.SZ", "R")]),
])
def test_historical_outcome_conflicts_are_not_normal_absences(opens, suspends, monkeypatch):
    from backend.services.advisory_model_first.entry_price_confirmation import PostgresEntryPriceConfirmationOutcomeSource
    from backend.tests.advisory_model_first.test_price_range_prospective_evaluation_boundaries import _source
    old_source, conn, cursor = _source(opens=opens, suspends=suspends)
    _allow_historical_read_timeout(cursor, monkeypatch)
    source = PostgresEntryPriceConfirmationOutcomeSource(connection_context_factory=old_source._connection_context_factory)
    with pytest.raises(AdvisoryModelFirstError):
        source.load(symbols=("000001.SZ",), target_trade_date=date(2026, 9, 15))
    assert conn.rollbacks == 1
