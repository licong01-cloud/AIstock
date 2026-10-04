from __future__ import annotations

import copy
from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import formal_state_effect as subject
from backend.services.hmm_risk import formal_state_model as model_module
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_sha256
from backend.services.hmm_risk.formal_state_model import receipt
from backend.services.hmm_risk.rotation_l2 import _newey_west, predictions_for_calendar


def _calendar():
    holidays = {
        "2024-09-16",
        "2024-09-17",
        "2024-10-01",
        "2024-10-02",
        "2024-10-03",
        "2024-10-04",
        "2024-10-07",
        "2025-01-01",
        "2025-01-28",
        "2025-01-29",
        "2025-01-30",
        "2025-01-31",
        "2025-02-03",
        "2025-02-04",
        "2025-04-04",
        "2025-05-01",
        "2025-05-02",
        "2025-05-05",
        "2025-06-02",
        "2025-10-01",
        "2025-10-02",
        "2025-10-03",
        "2025-10-06",
        "2025-10-07",
        "2025-10-08",
        "2026-01-01",
        "2026-01-02",
        "2026-02-16",
        "2026-02-17",
        "2026-02-18",
        "2026-02-19",
        "2026-02-20",
        "2026-02-23",
    }
    return [
        d.date().isoformat() for d in pd.bdate_range("2023-01-02", "2026-03-31") if d.date().isoformat() not in holidays
    ]


def reseal(payload):
    return receipt({k: v for k, v in payload.items() if k != "receipt_sha256"})


@pytest.fixture(scope="module")
def inputs():
    # Synthetic contract matrix only. No dataset, production model or fitting.
    calendar = _calendar()
    prefix = [d for d in calendar if "2024-07-01" <= d <= "2025-03-31"]
    continuation = [d for d in calendar if "2025-04-01" <= d <= "2026-03-30"]
    codes = sorted([f"801{i:03d}.SI" for i in range(127)] + ["801204.SI", "801207.SI", "801743.SI", "801983.SI"])
    models = {}
    preprocess = {"family": "identity"}
    for i, code in enumerate(codes):
        raw = np.tile(np.asarray([-1.0, 0.0, 1.0])[:, None], (1, 20))
        if code == "801207.SI":
            raw[:, 19] = 0
        _, projection = model_module.project_training(
            raw, preprocess, family="autocycle_all_core", level="L2", sector=code, source_receipt_sha256="a" * 64
        )
        d = projection["likelihood_feature_count"]
        model = {
            "means": np.tile(np.asarray([-2.0, 0.0, 2.0])[:, None], (1, d)).tolist(),
            "covariance": np.ones((3, d)).tolist(),
            "startprob": [0.2, 0.6, 0.2],
            "transmat": [[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]],
        }
        values = np.zeros((len(prefix) - 2, 20))
        prefix_positions = list(range(2, len(prefix)))
        posterior = model_module.causal_filter(
            model_module.restore_model(model),
            prefix_positions,
            model_module.project_validation(values, preprocess, projection),
            len(prefix),
        )
        models[code] = {
            "model": model,
            "model_sha256": canonical_sha256(model),
            "projection": projection,
            "prefix_positions": prefix_positions,
            "prefix_values": values.tolist(),
            "prefix_posterior": posterior.tolist(),
            "mapping": None if code in subject.UNMAPPED else {"0": "fading", "1": "neutral", "2": "trending"},
            "utility_means": {"0": i / 1000 - 0.1, "1": i / 1000, "2": i / 1000 + 0.1},
            "semantic_receipt_sha256": "b" * 64,
        }
    frozen = receipt(
        {
            "schema_version": subject.MODEL_SCHEMA,
            "contract": subject.CONTRACT,
            "original_request_sha256": subject.REQUEST_SHA,
            "original_acceptance_sha256": subject.ACCEPTANCE_SHA,
            "original_repeat_sha256": subject.REPEAT_SHA,
            "research_sha256": subject.RESEARCH_SHA,
            "models": models,
            "preprocess": preprocess,
            "catalog": codes,
            "names": {c: c for c in codes},
            "prefix_calendar": prefix,
            "eligibility": {"000001.SZ": True},
            "eligibility_receipt_sha256": "c" * 64,
            "feature_definition": {"fixture": True},
            "security_identity_sha256": "d" * 64,
            "provider_absence_sha256": "e" * 64,
            "industry_authority": {},
            "source_identity": {},
            "fits": 0,
            "selection_performed": False,
        }
    )
    observations = receipt(
        {
            "schema_version": subject.OBSERVATION_SCHEMA,
            "model_set_sha256": frozen["receipt_sha256"],
            "feature_names": list(ALL_CORE_FEATURES),
            "eligibility_receipt_sha256": frozen["eligibility_receipt_sha256"],
            "calendar": prefix + continuation,
            "source_calendar": calendar,
            "sectors": {
                c: {
                    "positions": list(range(len(prefix), len(prefix) + len(continuation))),
                    "values": np.zeros((len(continuation), 20)).tolist(),
                    "na_reasons": {},
                }
                for c in codes
            },
            "structural_membership": {d: {c: True for c in codes} for d in continuation},
            "source_identity": {},
            "feature_definition_sha256": canonical_sha256(frozen["feature_definition"]),
            "cross_section_lineage_sha256": "f" * 64,
            "tail_accessed": False,
        }
    )
    return frozen, observations


@pytest.fixture(scope="module")
def predicted(inputs):
    return subject.predict(*inputs)


def test_calendar_counts_are_derived_and_tail_never_matures():
    schedule = subject.calendar_contract(_calendar())
    assert len(schedule["decisions"]) == 221
    assert len(schedule["maturity"]["20"]) == 201
    assert schedule["maturity"]["20"][-1] == "2026-03-03"
    assert schedule["as_of"]["2025-05-06"] == "2025-04-30"
    with pytest.raises(model_module.FormalStateError):
        subject.calendar_contract([d for d in _calendar() if d != "2026-03-03"])


def test_prediction_is_full_l2_hard_only_zero_fit(inputs, predicted, monkeypatch):
    monkeypatch.setattr(model_module, "fit_entry", lambda *_a, **_k: pytest.fail("fit reached"))
    monkeypatch.setattr(model_module, "select_restart", lambda *_a, **_k: pytest.fail("D5 reached"))
    monkeypatch.setattr(model_module, "preprocess_fit", lambda *_a, **_k: pytest.fail("preprocess refit reached"))
    repeated = subject.predict(*inputs)
    assert repeated == predicted
    assert len(predicted["predictions"]) == 131 * 221
    for row in predicted["predictions"]:
        if row["sector_code"] in subject.UNMAPPED:
            assert row["rotation_score"] is row["forecast_state"] is None
            assert row["structural_eligible"] and row["reason_code"].endswith("mapping_insufficient")
        else:
            assert row["semantic_state"] == "neutral"
            assert row["raw_score"] == row["feature_contributions"]["frozen_utility_mean"]
    assert {r["daily_rank_group"] for r in predicted["predictions"] if r["availability"] == "available"} == {
        "trending",
        "neutral",
        "fading",
    }


@pytest.mark.parametrize("fault", ["parameter", "preprocess", "mapping", "prefix", "target"])
def test_rehashed_input_cannot_change_frozen_or_causal_contract(inputs, fault):
    frozen, observations = copy.deepcopy(inputs)
    code = frozen["catalog"][0]
    if fault == "parameter":
        frozen["models"][code]["model"]["means"][0][0] += 1
    elif fault == "preprocess":
        frozen["preprocess"]["family"] = "not_approved"
    elif fault == "mapping":
        frozen["models"][code]["mapping"] = None
    elif fault == "prefix":
        frozen["models"][code]["prefix_posterior"][0][0] += 0.1
    else:
        observations["targets"] = {"future": 1}
    frozen = reseal(frozen)
    observations["model_set_sha256"] = frozen["receipt_sha256"]
    with pytest.raises(model_module.FormalStateError):
        subject.predict(frozen, reseal(observations))


def test_future_observation_change_does_not_change_earlier_predictions(inputs, predicted):
    frozen, observations = copy.deepcopy(inputs)
    offset = (
        observations["calendar"].index("2025-10-01")
        if "2025-10-01" in observations["calendar"]
        else observations["calendar"].index("2025-10-09")
    )
    code = frozen["catalog"][0]
    slot = observations["sectors"][code]["positions"].index(offset)
    observations["sectors"][code]["values"][slot] = [2.0] * 20
    changed = subject.predict(frozen, reseal(observations))
    old = [r for r in predicted["predictions"] if r["as_of_date"] < observations["calendar"][offset]]
    new = [r for r in changed["predictions"] if r["as_of_date"] < observations["calendar"][offset]]
    assert old == new


def test_finite_inactive_continuation_is_diagnostic_not_prediction_gate(inputs, predicted, monkeypatch):
    frozen, observations = copy.deepcopy(inputs)
    for name in ("fit_entry", "select_restart", "preprocess_fit"):
        monkeypatch.setattr(model_module, name, lambda *_a, **_kw: pytest.fail("refit/selection reached"))
    code = "801207.SI"
    obs = observations["sectors"][code]
    slot = obs["positions"].index(observations["calendar"].index("2026-03-05"))
    obs["values"][slot][19] = -0.007118292485459976
    observations = reseal(observations)
    before = canonical_sha256([frozen, observations])
    changed = subject.predict(frozen, observations)
    assert changed["predictions"] == predicted["predictions"]
    diagnostics = changed["inactive_dimension_observation_receipts"]
    nonzero = [r for r in diagnostics if r["inactive_feature_observed_non_zero"]]
    assert len(nonzero) == 1
    row = nonzero[0]
    assert row["as_of_date"] == "2026-03-05" and row["trade_date"] == "2026-03-06"
    assert row["raw_value_f64"] == -0.007118292485459976
    assert row["preprocessed_value_f64"] == row["raw_value_f64"]
    assert row["projection_sha256"] == frozen["models"][code]["projection"]["receipt_sha256"]
    subject.verify_receipt(row)
    assert canonical_sha256([frozen, observations]) == before


def test_legitimate_observation_na_does_not_make_transition_only_prediction(inputs):
    frozen, observations = copy.deepcopy(inputs)
    code = frozen["catalog"][0]
    as_of = "2025-04-30"
    p = observations["calendar"].index(as_of)
    slot = observations["sectors"][code]["positions"].index(p)
    observations["sectors"][code]["positions"].pop(slot)
    observations["sectors"][code]["values"].pop(slot)
    observations["sectors"][code]["na_reasons"][as_of] = "hmm_risk_l2_effect_all_members_suspended"
    result = subject.predict(frozen, reseal(observations))
    row = next(r for r in result["predictions"] if r["trade_date"] == "2025-05-06" and r["sector_code"] == code)
    assert row["structural_eligible"] and row["availability"] == "unavailable"
    assert row["reason_code"].endswith("all_members_suspended")


def _labels(frozen, calendar):
    returns = {d: {c: i / 10000 for i, c in enumerate(frozen["catalog"])} for d in calendar}
    return subject.composite_outcomes(calendar, returns, {d: 0.0 for d in calendar}, frozen["catalog"])


def _baseline(frozen, calendar):
    dates = [date.fromisoformat(d) for d in calendar]
    rows = [
        {
            "trade_date": d.isoformat(),
            "sector_code": c,
            "eligible": True,
            "structural_eligible": True,
            "reason_code": None,
            "net_mf_amount_cny": float(i * day),
            "amount_cny": 1.0,
            "expected_contributors": 1,
            "valid_contributors": 1,
            "maximum_member_amount_share": 1.0,
        }
        for day, d in enumerate(dates)
        for i, c in enumerate(frozen["catalog"])
    ]
    return predictions_for_calendar(
        calendar=dates,
        catalog=frozen["catalog"],
        names=frozen["names"],
        daily_rows=rows,
        decision_days=[d for d in dates if subject.START <= d <= subject.END],
    )


def test_composite_label_uses_t_plus_one_and_never_reads_tail(inputs):
    frozen, observations = inputs
    calendar = observations["source_calendar"]
    code = frozen["catalog"][1]
    returns = {d: {c: 0.0 for c in frozen["catalog"]} for d in calendar}
    returns["2025-05-06"][code] = 1000.0  # Decision-day poison must not enter its label.
    for d in calendar[calendar.index("2025-05-06") + 1 : calendar.index("2025-05-06") + 21]:
        returns[d][code] = 0.01
    labels = subject.composite_outcomes(calendar, returns, {d: 0.0 for d in calendar}, frozen["catalog"])
    assert labels["outcomes"]["2025-05-06"][code] == pytest.approx(0.1125)
    assert labels["outcomes"]["2026-03-04"][code] is None
    assert labels["components"]["2026-03-24"][code]["5"] is not None
    assert labels["components"]["2026-03-25"][code]["5"] is None


def test_effect_full_population_same_composite_baseline_and_diagnostic_hac(inputs, predicted):
    frozen, observations = inputs
    labels = _labels(frozen, observations["source_calendar"])
    result = subject.evaluate(predicted, labels, _baseline(frozen, observations["source_calendar"]))
    assert result["effect_status"] == subject.EFFECT_REACHED
    metrics = result["metrics"]
    assert metrics["valid_ic_day_count"] == metrics["mature_day_count"] == 201
    assert metrics["overall"]["prediction_coverage"] == pytest.approx(127 / 131)
    assert metrics["baseline"]["label"] == "same_composite_5_10_20"
    assert metrics["blocks"][0]["decision_count"] == 105
    assert metrics["blocks"][1]["mature_count"] == 96
    assert metrics["horizon_diagnostics"]["5"]["planned_mature_days"] == 216
    assert len(metrics["daily"][0]["population_masks"]["S"]) == 131
    assert len(metrics["daily"][0]["population_masks"]["P"]) == 127
    assert result["predictions"][-1]["outcome_status"] == "outcome_not_mature"


def test_equal_scores_and_zero_denominator_are_insufficient_not_fake_zero(inputs, predicted):
    frozen, observations = inputs
    sealed = copy.deepcopy(predicted)
    for row in sealed["predictions"]:
        if row["raw_score"] is not None:
            row["raw_score"] = 1.0
    result = subject.evaluate(
        reseal(sealed),
        _labels(frozen, observations["source_calendar"]),
        _baseline(frozen, observations["source_calendar"]),
    )
    assert result["effect_status"] == "EVIDENCE_INSUFFICIENT"
    assert result["metrics"]["overall"]["mean_daily_rank_ic"] is None
    for row in sealed["predictions"]:
        row["structural_eligible"] = row["feature_eligible"] = False
        row["raw_score"] = row["rotation_score"] = row["forecast_state"] = row["feature_contributions"] = None
        row["availability"] = "unavailable"
        row["reason_code"] = "hmm_risk_l2_effect_no_resolved_pit_members"
    result = subject.evaluate(
        reseal(sealed),
        _labels(frozen, observations["source_calendar"]),
        _baseline(frozen, observations["source_calendar"]),
    )
    assert result["metrics"]["overall"]["prediction_coverage"] is None
    assert result["effect_status"] == "EVIDENCE_INSUFFICIENT"


def test_hac_keeps_missing_calendar_distance_and_spread_does_not_break_ties():
    days = [date(2025, 1, i) for i in range(1, 6)]
    sparse = {days[0]: 0.1, days[2]: -0.1, days[4]: 0.1}
    assert _newey_west(days, sparse, lag=1) != _newey_west(list(sparse), sparse, lag=1)
    assert subject._spread({"a": 1.0, "b": 1.0, "c": 1.0}, {"a": 1.0, "b": 2.0, "c": 3.0}) is None
    # Missing the extreme label must not promote the next predicted sector.
    assert (
        subject._spread({"a": 0.0, "b": 1.0, "c": 2.0, "d": 3.0, "e": 4.0}, {"b": 1.0, "c": 2.0, "d": 3.0, "e": 4.0})
        is None
    )


@pytest.fixture(scope="module")
def acceptance(inputs, predicted):
    frozen, observations = inputs
    result = subject.evaluate(
        predicted, _labels(frozen, observations["source_calendar"]), _baseline(frozen, observations["source_calendar"])
    )
    params = {c: m["model_sha256"] for c, m in frozen["models"].items()}
    semantics = {c: [m["mapping"], m["utility_means"]] for c, m in frozen["models"].items()}
    identity = {
        "frozen_model_set_sha256": frozen["receipt_sha256"],
        "model_entry_sha256_by_sector": {c: canonical_sha256(m) for c, m in frozen["models"].items()},
        "projection_sha256_by_sector": {c: m["projection"]["receipt_sha256"] for c, m in frozen["models"].items()},
        "observation_sha256": observations["receipt_sha256"],
        "inactive_dimension_observation_receipts": predicted["inactive_dimension_observation_receipts"],
        "source_calendar": observations["source_calendar"],
        "model_parameter_sha256_by_sector": params,
        "model_parameter_set_sha256": canonical_sha256(params),
        "semantic_mapping_and_utility_by_sector": semantics,
        "semantic_mapping_sha256": canonical_sha256(semantics),
        "original_request_sha256": subject.REQUEST_SHA,
        "original_acceptance_sha256": subject.ACCEPTANCE_SHA,
        "research_sha256": subject.RESEARCH_SHA,
        "mapping_sha256": "a" * 64,
        "quote_authority_sha256": "b" * 64,
    }
    repeat = receipt(
        {
            "schema_version": subject.VERSION + "_repeat",
            "contract": subject.CONTRACT,
            "fits": 0,
            "selection_performed": False,
            "tail_accessed": False,
            "result": result,
            "evaluation_input_identity": identity,
            "numeric_environment": {"fixture": True},
        }
    )
    return subject.close_effect_processes(repeat, copy.deepcopy(repeat))


def test_explicit_product_version_preserves_frozen_semantics_and_null_mapping(acceptance):
    from backend.services.hmm_risk.rotation_l2_prediction import rows_from_acceptance

    rows = rows_from_acceptance(acceptance)
    assert len(rows) == 131 * 221
    assert all(row["validation_basis"] == subject.BASIS for row in rows)
    row = next(row for row in rows if row["sector_code"] not in subject.UNMAPPED)
    assert row["forecast_state"] == row["feature_contributions"]["semantic_state"] == "neutral"
    assert row["forecast_state"] != row["feature_contributions"]["daily_rank_group"]
    assert all(
        row["rotation_score"] is None and row["structural_eligible"]
        for row in rows
        if row["sector_code"] in subject.UNMAPPED
    )


@pytest.mark.parametrize("fault", ["missing", "duplicate", "value_hash", "projection", "flag"])
def test_product_rejects_rehashed_inactive_observation_diagnostic_drift(acceptance, fault):
    bad = copy.deepcopy(acceptance)
    rows = bad["evaluation_input_identity"]["inactive_dimension_observation_receipts"]
    if fault == "missing":
        rows.pop()
    elif fault == "duplicate":
        rows.append(copy.deepcopy(rows[-1]))
    else:
        row = rows[0]
        field, value = {
            "value_hash": ("raw_value_float64_sha256", "f" * 64),
            "projection": ("projection_sha256", "f" * 64),
            "flag": ("inactive_feature_observed_non_zero", True),
        }[fault]
        row[field] = value
        rows[0] = reseal(row)
    bad["input_hash"] = canonical_sha256(bad["evaluation_input_identity"])
    bad["acceptance_sha256"] = canonical_sha256({k: v for k, v in bad.items() if k != "acceptance_sha256"})
    with pytest.raises(model_module.FormalStateError, match="inactive observation"):
        subject.validate_acceptance(bad)


@pytest.mark.parametrize(
    "fault", ["semantic", "rank", "utility", "effect", "mapping_pin", "catalog", "metric", "maturity"]
)
def test_product_rejects_rehashed_projection_and_state_drift(acceptance, fault):
    from backend.services.hmm_risk.rotation_l2_prediction import RotationL2PredictionError, rows_from_acceptance

    bad = copy.deepcopy(acceptance)
    row = next(row for row in bad["predictions"] if row["availability"] == "available")
    if fault == "semantic":
        row["forecast_state"] = "trending"
    elif fault == "rank":
        row["rotation_score"] += 0.01
        row["feature_contributions"]["average_rank_score"] = row["rotation_score"]
    elif fault == "utility":
        row["raw_score"] += 0.01
    elif fault == "effect":
        bad["effect_status"] = "BELOW_BINDING_MBE"
    elif fault == "mapping_pin":
        bad["mapping_hash"] = "f" * 64
    elif fault == "metric":
        bad["metrics"]["overall"]["mean_daily_rank_ic"] = 0.5
    elif fault == "maturity":
        row["outcome_status"] = "outcome_not_mature"
    else:
        bad["predictions"].pop()
    bad["acceptance_sha256"] = canonical_sha256({k: v for k, v in bad.items() if k != "acceptance_sha256"})
    with pytest.raises(RotationL2PredictionError, match="effect acceptance"):
        rows_from_acceptance(bad)


def test_hmm_product_writer_readback_and_api_keep_explicit_version(acceptance):
    from backend.tests.hmm_risk.test_rotation_l2_prediction import _Cursor, _Connection
    from backend.services.hmm_risk.rotation_l2_prediction import (
        PREDICTION_COLUMNS,
        RotationL2PredictionRepository,
        rows_from_acceptance,
    )

    class ReadCursor(_Cursor):
        def execute(self, sql, params=None):
            if "WHERE run_id=%s AND trade_date=%s" in sql:
                self._rows = list(self.inserted)
                return
            if "SELECT max(trade_date)" in sql:
                self.maximum = max(row[PREDICTION_COLUMNS.index("trade_date")] for row in self.inserted)
                return
            super().execute(sql, params)

        def fetchone(self):
            return (self.maximum,)

    cursor = ReadCursor()
    repository = RotationL2PredictionRepository(conn_factory=lambda: _Connection(cursor))
    rows = [row for row in rows_from_acceptance(acceptance) if row["trade_date"] == subject.END]
    assert repository.write_rows(rows)["row_count"] == 131
    response = repository.read_date(subject.END, run_id=acceptance["run_id"])
    assert len(response["rows"]) == 131
    assert all(row["model_version"] == subject.VERSION for row in response["rows"])
    row = next(row for row in response["rows"] if row["availability"] == "available")
    assert row["semantic_state"] == "neutral" and row["daily_rank_group"] == "fading"
    overview = repository.overview(run_id=acceptance["run_id"])
    assert overview["model_version"] == subject.VERSION and overview["research_surface_status"] == "NOT_AVAILABLE"


def test_parent_prediction_is_sealed_before_first_label_access(inputs, monkeypatch):
    from backend.services.hmm_risk import formal_state_executor as executor
    from backend.services.hmm_risk import formal_state_input as input_module

    frozen, observations = inputs
    events = []
    original_predict = subject.predict

    def seal(*args):
        result = original_predict(*args)
        events.append("sealed")
        return result

    def outcomes(*_args):
        assert events == ["sealed"]
        events.append("labels")
        return reseal({**_labels(frozen, observations["source_calendar"]), "source_identity": {}})

    monkeypatch.setattr(subject, "predict", seal)
    monkeypatch.setattr(input_module, "prepare_effect_outcomes", outcomes)
    monkeypatch.setattr(
        executor, "numeric_environment", lambda: {"versions": {}, "thread_variables": {}, "thread_pools": []}
    )
    for name in ("fit_entry", "select_restart", "preprocess_fit"):
        monkeypatch.setattr(model_module, name, lambda *_a, **_kw: pytest.fail("training/selection reached"))
    baseline = receipt(
        {
            "predictions": _baseline(frozen, observations["source_calendar"]),
            "mapping_sha256": "a" * 64,
            "quote_authority_sha256": "b" * 64,
        }
    )
    result = executor.effect_repeat(
        receipt(
            {
                "frozen": frozen,
                "observations": observations,
                "baseline": baseline,
                "source": {"producer_commit": "a" * 40},
            }
        )
    )
    assert events == ["sealed", "labels"] and result["fits"] == 0
    assert result["evaluation_input_identity"]["inactive_dimension_observation_receipts"]


def test_model_extraction_authenticates_original_parameters_without_fitting(inputs, monkeypatch):
    frozen, _ = inputs
    codes = frozen["catalog"]
    entries = {
        c: receipt({"accepted": True, "model": m["model"], "projection": m["projection"]})
        for c, m in frozen["models"].items()
    }
    eligibility = receipt({"entries": [{"canonical_ts_code": "000001.SZ", "moneyflow_contributor_eligible": True}]})
    request = receipt(
        {
            "sector_codes": {"L2": codes},
            "validation_calendar": frozen["prefix_calendar"],
            "series": {
                subject.KEY: {
                    c: {
                        "validation": {
                            "observation_available_positions": m["prefix_positions"],
                            "observation_values_f64": m["prefix_values"],
                        }
                    }
                    for c, m in frozen["models"].items()
                }
            },
            "policy": {
                "eligibility_receipt": eligibility,
                "eligibility_receipt_sha256": eligibility["receipt_sha256"],
                "l2_feature_definition": frozen["feature_definition"],
                "security_identity_manifest_sha256": "d" * 64,
                "provider_absence_manifest_sha256": "e" * 64,
            },
            "source_identity": {},
            "industry_authority": {
                "l2_projection": {"rows": [{"canonical_l2_code": c, "canonical_l2_name": c} for c in codes]}
            },
        }
    )
    child = receipt(
        {
            "request_sha256": request["receipt_sha256"],
            "groups": {
                subject.KEY: {"preprocess": frozen["preprocess"], "candidates": [{"seed": 47, "entries": entries}]}
            },
        }
    )
    original = receipt(
        {
            "request_sha256": request["receipt_sha256"],
            "repeat_sha256": child["receipt_sha256"],
            "fresh_process_bitwise_equal": True,
            "selection": {subject.KEY: {"selected_seed": 47, "accepted": True}},
            "semantic": {
                subject.KEY: {
                    c: {"selected_model_parameter_sha256": m["model_sha256"], "posterior": m["prefix_posterior"]}
                    for c, m in frozen["models"].items()
                }
            },
        }
    )
    research = receipt(
        {
            "request_sha256": request["receipt_sha256"],
            "original_acceptance_sha256": original["receipt_sha256"],
            "selected_seed": 47,
            "semantic": {
                c: receipt(
                    {
                        "selected_model_parameter_sha256": m["model_sha256"],
                        "selected_identity": {"family": "autocycle_all_core", "level": "L2", "sector": c, "seed": 47},
                        "mapping": m["mapping"],
                        "semantic_evidence_valid": m["mapping"] is not None,
                        "states": [{"state": int(k), "utility_mean": v} for k, v in m["utility_means"].items()],
                    }
                )
                for c, m in frozen["models"].items()
            },
        }
    )
    for name, payload in (
        ("REQUEST_SHA", request),
        ("ACCEPTANCE_SHA", original),
        ("REPEAT_SHA", child),
        ("RESEARCH_SHA", research),
    ):
        monkeypatch.setattr(subject, name, payload["receipt_sha256"])
    for name in ("fit_entry", "select_restart", "preprocess_fit"):
        monkeypatch.setattr(model_module, name, lambda *_a, **_kw: pytest.fail("fit/D5/preprocess reached"))
    extracted = subject.extract_frozen_models(request, original, research, child)
    assert {c: m["model_sha256"] for c, m in extracted["models"].items()} == {
        c: m["model_sha256"] for c, m in frozen["models"].items()
    }
    bad = copy.deepcopy(child)
    bad["groups"][subject.KEY]["candidates"][0]["entries"][codes[0]]["model"]["means"][0][0] = 999
    with pytest.raises(model_module.FormalStateError, match="identity"):
        subject.extract_frozen_models(request, original, research, reseal(bad))


def test_cli_rejects_effect_prepare_without_original_acceptance(tmp_path, monkeypatch, capsys):
    from scripts.hmm_risk import run_formal_state_model_set as cli

    output = tmp_path / "effect.json"
    monkeypatch.setattr("sys.argv", ["executor", "effect-prepare", "--output", str(output)])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
    assert "effect-prepare requires --original-acceptance" in capsys.readouterr().err
    assert not output.exists() and not output.with_name(output.name + ".failure.json").exists()


def test_cli_rejects_effect_child_without_parent_pin(tmp_path, monkeypatch, capsys):
    from scripts.hmm_risk import run_formal_state_model_set as cli

    monkeypatch.setattr(
        "sys.argv",
        [
            "executor",
            "effect-child",
            "--request",
            str(tmp_path / "request.json"),
            "--output",
            str(tmp_path / "output.json"),
        ],
    )
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2 and "parent's --request-sha256" in capsys.readouterr().err
    assert not (tmp_path / "output.json").exists()


def test_two_processes_fail_closed_on_payload_or_refit_drift():
    repeat = receipt(
        {
            "schema_version": subject.VERSION + "_repeat",
            "contract": subject.CONTRACT,
            "fits": 0,
            "selection_performed": False,
            "tail_accessed": False,
        }
    )
    bad = reseal({**repeat, "fits": 1})
    with pytest.raises(model_module.FormalStateError):
        subject.close_effect_processes(repeat, bad)
    bad = reseal({**repeat, "unexpected_numeric_drift": 1})
    with pytest.raises(model_module.FormalStateError, match="differ"):
        subject.close_effect_processes(repeat, bad)
