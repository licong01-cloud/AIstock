"""Direct policy/source/parent boundary matrix; no production or new fits."""

from copy import deepcopy
from datetime import date
from pathlib import Path
import pandas as pd

import pytest

from backend.services.hmm_risk import risk_l2_value_continuous as c
from backend.services.hmm_risk import formal_state_input as kernel
from backend.services.hmm_risk.formal_state_executor import frozen_release_binding
from backend.services.hmm_risk.formal_state_model import receipt
from backend.tests.hmm_risk.test_risk_l2_value_replay import panel
from backend.tests.hmm_risk.test_rotation_l2_moneyflow_supervised import _calendar
from scripts.hmm_risk import replay_risk_l2_value_continuous as cli


def test_continuous_formula_probability_boundaries_cash_and_two_matched_controls():
    codes, days, signals, _ = panel()
    row = signals[days[0]]
    row[codes[0]].update(probability=0.0, warning=False)
    row[codes[1]].update(probability=1.0, warning=True)
    row[codes[2]].update(probability=None, warning=None, availability="unavailable", reason_code="NORMAL_SUSPENSION")
    target, eligible, warnings = c.budgets(codes, row)
    assert target["C"][codes[0]] == 1 / 131 and target["C"][codes[1]] == 0
    assert all(target[a][codes[2]] == 0 for a in c.ARMS)
    assert len(eligible) == 130 and warnings == [codes[1]]
    assert sum(target["R"].values()) == pytest.approx(sum(target["X_R"].values()))
    assert sum(target["C"].values()) == pytest.approx(sum(target["X_C"].values()))
    assert target["X_C"][codes[0]] != target["C"][codes[0]]


def test_reference_engine_delay_drift_initial_cost_and_opportunity():
    codes, days, signals, returns = panel()
    for day in days:
        signals[day][codes[0]].update(probability=0.9, warning=True)
    returns[days[1]][codes[0]] = -0.5
    result = c.compare(codes, days, signals, returns)
    first = result["daily"][0]
    assert set(first["arms"]) == set(c.ARMS)
    assert first["signal_date"] == days[0] and first["signal_as_of"] == "2026-03-25"
    assert first["arms"]["C"]["gross_return"] > first["arms"]["X_C"]["gross_return"]
    assert first["arms"]["C"]["one_sided_risk_turnover"] == pytest.approx(first["arms"]["C"]["risk_budget"])
    assert first["arms"]["C"]["cost_sensitivity_return"]["20"] == pytest.approx(
        first["arms"]["C"]["gross_return"] - 0.002 * first["arms"]["C"]["risk_budget"]
    )
    assert result["opportunity_cost"]["daily_continuous"][0]["negative_gross_loss_avoided_C"] < 0
    changed = deepcopy(signals)
    changed[days[1]][codes[0]].update(probability=0, warning=False)
    assert c.compare(codes, days, changed, returns)["daily"][0] == first
    assert result["net_value_status"] == "UNASSESSED" and result["forward_confirmed"] is False


def test_equal_probabilities_match_exposure_not_claim_selective_risk_reduction():
    result = c.compare(*panel())
    assert result["status"] == "REFERENCE_RISK_REDUCTION_NOT_OBSERVED"
    for day in result["daily"]:
        assert day["arms"]["C"]["gross_return"] == pytest.approx(day["arms"]["X_C"]["gross_return"])
    assert result["gross_full_path"]["C"]["cumulative_return"] < result["gross_full_path"]["B"]["cumulative_return"]


def test_legal_na_is_paired_and_never_stitched_or_filled():
    codes, days, signals, returns = panel()
    returns[days[1]][codes[0]] = None
    result = c.compare(codes, days, signals, returns)
    assert result["status"] == "INSUFFICIENT_REFERENCE_PATH"
    assert all(result["gross_full_path"][a] is None for a in c.ARMS)
    assert all(result["daily"][0]["arms"][a]["gross_return"] is None for a in c.ARMS)
    assert all(result["daily"][1]["arms"][a]["one_sided_risk_turnover"] is None for a in c.ARMS)
    assert all(result["daily"][1]["arms"][a]["cost_sensitivity_return"]["20"] is None for a in c.ARMS)


@pytest.mark.parametrize("corrupt", ["future", "warning", "nan", "status", "unknown_sector", "missing_date"])
def test_invalid_input_never_silently_reduces_population(corrupt):
    codes, days, signals, returns = panel()
    row = signals[days[0]][codes[0]]
    if corrupt == "future":
        row["as_of_date"] = days[1]
    elif corrupt == "warning":
        row["warning"] = True
    elif corrupt == "nan":
        row["probability"] = float("nan")
    elif corrupt == "status":
        row["availability"] = "unknown"
    elif corrupt == "unknown_sector":
        signals[days[0]]["unknown"] = row
    else:
        del returns[days[1]]
    with pytest.raises((ValueError, RuntimeError)):
        c.compare(codes, days, signals, returns)


@pytest.mark.parametrize("binding", [None, c.BINDING])
def test_source_kernel_preserves_old_default_and_forwards_new_explicit_binding(monkeypatch, tmp_path, binding):
    from backend.services.hmm_risk import rotation_l1_input_bundle as reader

    class StopRead(Exception):
        pass

    def load(root, **kwargs):
        assert root == tmp_path
        assert kwargs["frozen_release_binding"] == (frozen_release_binding() if binding is None else c.BINDING)
        raise StopRead

    monkeypatch.setattr(reader, "load_rotation_l1_direct_v2_source_assets", load)
    source = {
        "candidate_root": str(tmp_path),
        "security_identity_manifest": str(tmp_path / "identity.json"),
        "provider_absence_manifest": str(tmp_path / "absence.json"),
    }
    with pytest.raises(StopRead):
        kernel._bounded_l2_stock_facts(
            {}, source, start=date(2026, 4, 1), end=date(2026, 8, 31), release_binding=binding
        )


def test_wrong_approved_contract_stops_before_original_model_or_numeric_data(monkeypatch):
    monkeypatch.setattr(c.inputs, "_risk_model_source", lambda _: pytest.fail("must not read source"))
    request = receipt({"schema_version": c.VERSION + "_request", "contract": {**c.CONTRACT, "warning_threshold": 0.19}})
    with pytest.raises(RuntimeError, match="approved continuous contract"):
        c.prepare(request, "a" * 40)


def test_cli_failure_is_durable_and_collision_never_overwrites(tmp_path, monkeypatch):
    request = tmp_path / "request.json"
    output = tmp_path / "result.json"
    monkeypatch.setattr(
        cli.study.inputs, "_asset", lambda *a: (_ for _ in ()).throw(RuntimeError("injected identity failure"))
    )
    assert cli.main(["prepare", "--request", str(request), "--request-sha256", "0" * 64, "--output", str(output)]) == 1
    failure = cli.read_json(output.with_name("result.json.failure.json"))
    assert failure["status"] == "FAILED" and failure["fits"] == 0
    first = output.with_name("result.json.failure.json").read_bytes()
    assert cli.main(["prepare", "--request", str(request), "--request-sha256", "0" * 64, "--output", str(output)]) == 1
    assert output.with_name("result.json.failure.json").read_bytes() == first
    assert not output.exists()


def test_release_binding_and_consumed_scope_are_explicit():
    assert c.CONTRACT["release_binding"] == c.BINDING
    assert c.BINDING["generation"] == "20261015-v18-unified-quote1"
    assert c.BINDING["cutoff"] == "2026-08-31"
    assert c.CONTRACT["independence_status"] == "ALREADY_CONSUMED_HISTORY_DEVELOPMENT_ONLY"
    assert c.CONTRACT["fits"] == 0 and c.CONTRACT["product_activation"] is False
    assert c.CONTRACT["model_sha256"] == c.value.APPROVED_PINS["model_hash"]


@pytest.fixture
def formal_case(monkeypatch):
    closed = {"2026-04-06", "2026-05-01", "2026-05-04", "2026-05-05", "2026-06-19"}
    calendar = [d.isoformat() for d in _calendar()] + [
        d.date().isoformat() for d in pd.bdate_range("2026-04-01", "2026-08-31") if d.date().isoformat() not in closed
    ]
    codes = [f"801{i:03d}.SI" for i in range(131)]
    plan = c.history.schedule(calendar)
    parameters = {
        "mean": [0.0] * 20,
        "scale": [1.0] * 20,
        "active": [True] * 20,
        "coef": [[0.1] * 20],
        "intercept": [0.0],
        "classes": [0, 1],
        "iterations": [1],
    }
    model_sha = c.canonical_sha256(parameters)
    monkeypatch.setattr(c, "CONTRACT", {**c.CONTRACT, "model_sha256": model_sha})
    monkeypatch.setitem(c.value.APPROVED_PINS, "model_hash", model_sha)
    rows = {
        d: {code: {"as_of_date": plan["as_of"][d], "features": [1.0] * 20, "reason_code": None} for code in codes}
        for d in plan["days"]
    }
    rows[plan["days"][0]][codes[0]].update(features=None, reason_code="NORMAL_SUSPENSION")
    request = receipt({"schema_version": c.VERSION + "_request", "contract": c.CONTRACT})
    identity = {
        "frozen_release_generation": c.BINDING["generation"],
        "frozen_release_binding_sha256": c.canonical_sha256(c.BINDING),
        "dataset_manifest_sha256": c.BINDING["manifest_sha256"],
        "cutoff": c.BINDING["cutoff"],
    }
    bundle = receipt(
        {
            "schema_version": c.VERSION + "_risk_features",
            "contract": c.CONTRACT,
            "calendar": calendar,
            "catalog": codes,
            "feature_names": list(c.ALL_CORE_FEATURES),
            "parameters": parameters,
            "rows": rows,
            "source_commit": "a" * 40,
            "request_sha256": request["receipt_sha256"],
            "source_identity": {"release_identity": identity},
        }
    )
    monkeypatch.setattr(c, "_source", lambda req: ({"frozen": {"catalog": codes}}, parameters, calendar))
    return request, bundle


def reseal(payload):
    return receipt({k: v for k, v in payload.items() if k != "receipt_sha256"})


def test_fixed_model_inference_uses_new_consumed_contract_no_fit_or_db(formal_case):
    request, bundle = formal_case
    c.validate_bundle(request, bundle)
    with c.history.no_training_or_external_actions():
        sealed = c.infer(bundle)
        signals = c.validate_sealed(bundle, sealed)
    assert sealed["contract"]["independence_status"] == "ALREADY_CONSUMED_HISTORY_DEVELOPMENT_ONLY"
    assert len(sealed["predictions"]) == 104 * 131 and sealed["fits"] == 0
    assert signals["2026-04-01"][bundle["catalog"][0]]["probability"] is None
    assert signals["2026-04-01"][bundle["catalog"][1]]["warning"] is True


@pytest.mark.parametrize("drift", ["catalog", "parameters", "feature_names", "manifest", "request"])
def test_self_hashed_feature_drift_cannot_match_parent_authority(formal_case, drift):
    request, original = formal_case
    bundle = deepcopy(original)
    if drift == "manifest":
        bundle["source_identity"]["release_identity"]["dataset_manifest_sha256"] = "0" * 64
    elif drift == "request":
        bundle["request_sha256"] = "0" * 64
    elif drift == "parameters":
        bundle["parameters"]["intercept"][0] = 0.1
    else:
        bundle[drift].pop()
    with pytest.raises(RuntimeError, match="parent authorities"):
        c.validate_bundle(request, reseal(bundle))


@pytest.mark.parametrize("drift", ["missing", "duplicate", "future", "model", "eligibility"])
def test_self_hashed_child_drift_rejected_before_outcome_read(formal_case, drift):
    _, bundle = formal_case
    sealed = c.infer(bundle)
    if drift == "missing":
        sealed["predictions"].pop()
    elif drift == "duplicate":
        sealed["predictions"][-1] = sealed["predictions"][0]
    elif drift == "future":
        sealed["predictions"][0]["as_of_date"] = "2026-04-02"
    elif drift == "model":
        sealed["model_sha256"] = "0" * 64
    else:
        sealed["predictions"][0].update(probability=0.1, warning=False, availability="available", reason_code=None)
    with pytest.raises((ValueError, RuntimeError)):
        c.validate_sealed(bundle, reseal(sealed))


@pytest.mark.parametrize("fault", [None, "child_drift", "parent_finalization"])
def test_parent_closure_outcome_order_and_durable_finalization(formal_case, tmp_path, monkeypatch, fault):
    from types import SimpleNamespace

    request, bundle = formal_case
    bundle = reseal({**bundle, "numeric_environment": {"fixture": True}})
    sealed = c.infer(bundle)
    facts = receipt(
        {
            "schema_version": c.VERSION + "_outcomes",
            "contract": c.CONTRACT,
            "feature_sha256": bundle["receipt_sha256"],
            "source_identity": bundle["source_identity"],
            "returns": {
                d: {code: 0.001 for code in bundle["catalog"]}
                for d in c.history.schedule(bundle["calendar"])["days"][1:]
            },
        }
    )
    args = SimpleNamespace(
        output=tmp_path / "run", features=tmp_path / "features.json", request=tmp_path / "request.json"
    )
    monkeypatch.setattr(cli, "_head", lambda: bundle["source_commit"])
    outcome_calls = []

    def outcomes(req, actual_bundle):
        assert all((args.output / f"process_{n}.ready.json").is_file() for n in (1, 2))
        outcome_calls.append(True)
        return facts

    monkeypatch.setattr(c, "outcomes", outcomes)
    calls = []

    class Process:
        def __init__(self, command, **kwargs):
            self.output = Path(command[command.index("--output") + 1])
            calls.append(self)
            item = deepcopy(sealed)
            if fault == "child_drift" and len(calls) == 2:
                item["model_sha256"] = "0" * 64
                item = reseal(item)
            cli.write_once(self.output.with_suffix(".sealed.json"), item)
            cli.write_once(self.output.with_suffix(".ready.json"), {"sealed_sha256": item["receipt_sha256"]})

        def poll(self):
            return None

        def wait(self):
            if (args.output / "parent.failure.json").exists():
                return 1
            if self.output.exists():
                return 0
            actual = cli.read_json(args.output / "outcomes.json")
            result = c.evaluate(bundle, sealed, actual)
            cli.write_once(
                self.output,
                receipt(
                    {
                        "schema_version": c.VERSION + "_repeat",
                        "contract": c.CONTRACT,
                        "request_sha256": request["receipt_sha256"],
                        "features_sha256": bundle["receipt_sha256"],
                        "sealed_sha256": sealed["receipt_sha256"],
                        "outcome_sha256": actual["receipt_sha256"],
                        "executor_commit": bundle["source_commit"],
                        "numeric_environment": bundle["numeric_environment"],
                        "fits": 0,
                        "result": result,
                    }
                ),
            )
            return 0

    monkeypatch.setattr(cli.subprocess, "Popen", Process)
    write = cli.write_once

    def guarded_write(path, body):
        if path.name == "acceptance.json" and fault == "parent_finalization":
            raise OSError("injected finalization failure")
        write(path, body)

    monkeypatch.setattr(cli, "write_once", guarded_write)
    if fault:
        with pytest.raises((OSError, RuntimeError)):
            cli.run(args, request, bundle)
        assert not (args.output / "acceptance.json").exists()
        assert cli.read_json(args.output / "parent.failure.json")["status"] == "FAILED"
        assert bool(outcome_calls) is (fault != "child_drift")
    else:
        cli.run(args, request, bundle)
        accepted = cli.read_json(args.output / "acceptance.json")
        assert accepted["two_process_bitwise_equal"] is True and accepted["fits"] == 0
        assert accepted["result"]["planned_return_dates"] == 103


def test_prepare_binds_v18_and_builds_original_20d_only_through_prior_open_date(formal_case, monkeypatch):
    request, template = formal_case
    plan = c.history.schedule(template["calendar"])
    window = tuple(date.fromisoformat(d) for d in template["calendar"] if "2025-01-01" <= d <= "2026-08-28")
    frozen = {"catalog": template["catalog"], "feature_definition": {"fixture": "frozen_20d"}}
    monkeypatch.setattr(
        c,
        "_source",
        lambda _: (
            {"frozen": frozen, "source": {"candidate_root": "explicit-v18"}},
            template["parameters"],
            template["calendar"],
        ),
    )
    calls = []

    def bounded(actual, source, **kwargs):
        calls.append(kwargs)
        assert actual is frozen and source["candidate_root"] == "explicit-v18"
        assert kwargs["release_binding"] == c.BINDING
        assert kwargs["end"] == date(2026, 8, 28) < c.history.END
        return (
            {"benchmark": dict.fromkeys(window, 0.0)},
            window,
            [],
            {**template["source_identity"], "domain_reasons": {}, "structural_membership": {}},
        )

    def panel(*args, **kwargs):
        assert kwargs["expected_sector_count"] == 131 and kwargs["direct_sector_level"] == "L2"
        index = pd.MultiIndex.from_product(
            [pd.to_datetime(list(plan["as_of"].values())), template["catalog"]], names=["trade_date", "l1_code"]
        )
        frame = pd.DataFrame(1.0, index=index, columns=list(c.ALL_CORE_FEATURES))
        return frame, frozen["feature_definition"], {"fixture": True}

    monkeypatch.setattr(c, "_bounded_l2_stock_facts", bounded)
    monkeypatch.setattr(c, "build_c010_feature_domain_panel", panel)
    result = c.prepare(request, "a" * 40)
    assert len(calls) == 1 and len(result["rows"]) == 104
    assert len(result["rows"]["2026-04-01"][template["catalog"][0]]["features"]) == 20
    assert result["rows"]["2026-08-31"][template["catalog"][0]]["as_of_date"] == "2026-08-28"


def test_outcome_constructor_refuses_duplicate_sector_days(formal_case, monkeypatch):
    from types import SimpleNamespace

    request, bundle = formal_case
    source = {"frozen": {}, "source": {}}
    monkeypatch.setattr(c, "_source", lambda _: (source, bundle["parameters"], bundle["calendar"]))
    row = SimpleNamespace(trade_date=date(2026, 4, 2), l1_code=bundle["catalog"][0], l1_return=0.01)

    def bounded(*args, **kwargs):
        assert kwargs["release_binding"] == c.BINDING and kwargs["prewarm_price_history"] is True
        return {}, [], [row, row], bundle["source_identity"]

    monkeypatch.setattr(c, "_bounded_l2_stock_facts", bounded)
    with pytest.raises(RuntimeError, match="duplicate outcome"):
        c.outcomes(request, bundle)
