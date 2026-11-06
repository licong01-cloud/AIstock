"""Direct contracts for label-free inference; no training or real data writes."""

from copy import deepcopy
from datetime import date
import hashlib
import math
from pathlib import Path

import pandas as pd
import pytest

from backend.services.hmm_risk import risk_l2_date_inference as h
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_executor import read_json, write_once
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt
from backend.services.hmm_risk.frozen_l2_history import no_training_or_external_actions
from scripts.hmm_risk import run_risk_l2_date_inference as cli


def reseal(body):
    return receipt({k: v for k, v in body.items() if k != "receipt_sha256"})


@pytest.fixture
def case(tmp_path, monkeypatch):
    root = tmp_path / "release"
    calendar = root / "components/daily_bin_candidate/calendars/day.txt"
    calendar.parent.mkdir(parents=True)
    days = pd.bdate_range(end="2026-08-31", periods=400).strftime("%Y-%m-%d").tolist()
    calendar.write_text("\n".join(days) + "\n", encoding="utf-8")
    catalog = [f"{801000 + n:06}.SI" for n in range(131)]
    params = {
        "mean": [0.1] + [0.0] * 19,
        "scale": [2.0] + [0.0] * 19,
        "active": [True] + [False] * 19,
        "coef": [[1.5]],
        "intercept": [-0.4],
        "classes": [0, 1],
        "iterations": [3],
    }
    frozen = {"catalog": catalog, "feature_definition": {"version": "original"}, "eligibility": {"train_only": True}}
    identity = {"model_sha256": canonical_sha256(params), "training_input_sha256": "a" * 64}
    monkeypatch.setattr(h, "model_source", lambda request: (frozen, params, identity))
    source = {
        "candidate_root": str(root),
        "release_binding": dict(
            generation="test-v1",
            release_id="shared",
            revision="test-r1",
            cutoff="2026-08-31",
            manifest_sha256="b" * 64,
            manifest_file_sha256="c" * 64,
        ),
        "calendar_sha256": hashlib.sha256(calendar.read_bytes()).hexdigest(),
        "security_identity_manifest": str(tmp_path / "security.json"),
        "security_identity_sha256": "d" * 64,
        "provider_absence_manifest": str(tmp_path / "provider.json"),
        "provider_absence_sha256": "e" * 64,
        "industry_authority": {"official": "C-013"},
        "work_parent": str(tmp_path / "spool"),
    }
    request = receipt(
        {
            "schema_version": h.VERSION + "_request",
            "model_assets": {
                key: str(tmp_path / (key + ".json"))
                for key in ("source_request_path", "sealed_path", "acceptance_path")
            },
            "source": source,
            "decision_dates": ["2026-08-28", "2026-08-31"],
            "display_limit": 10,
        }
    )
    as_of = h.date_plan(request)["as_of"]
    bundle = receipt(
        {
            "schema_version": h.VERSION + "_features",
            "request_sha256": request["receipt_sha256"],
            "source_commit": "f" * 40,
            "model_identity": identity,
            "parameters": params,
            "catalog": catalog,
            "feature_names": list(h.ALL_CORE_FEATURES),
            "as_of": as_of,
            "rows": {
                day: {
                    code: {"as_of_date": as_of[day], "features": [n / 100] + [0.0] * 19, "reason_code": None}
                    for n, code in enumerate(catalog)
                }
                for day in as_of
            },
            "inference_identity": {
                "release_identity": {
                    "frozen_release_generation": "test-v1",
                    "frozen_release_binding_sha256": canonical_sha256(source["release_binding"]),
                    "dataset_manifest_sha256": "b" * 64,
                    "cutoff": "2026-08-31",
                },
                "calendar_sha256": source["calendar_sha256"],
                "industry_authority_sha256": canonical_sha256(source["industry_authority"]),
                "security_identity_sha256": source["security_identity_sha256"],
                "provider_absence_sha256": source["provider_absence_sha256"],
            },
        }
    )
    return request, bundle


def test_explicit_calendar_context_and_no_future_or_fitted_training_dates(case):
    request, _ = case
    plan = h.date_plan(request)
    assert plan["as_of"] == {"2026-08-28": "2026-08-27", "2026-08-31": "2026-08-28"}
    calendar = [
        date.fromisoformat(line)
        for line in (Path(request["source"]["candidate_root"]) / "components/daily_bin_candidate/calendars/day.txt")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert len([day for day in calendar if date.fromisoformat(plan["start"]) <= day < date(2026, 8, 28)]) == 264
    for dates in ([], ["2026-08-28"] * 2, ["2026-08-31", "2026-08-28"], ["2024-06-28"], ["2026-09-01"]):
        changed = reseal({**request, "decision_dates": dates})
        with pytest.raises(FormalStateError):
            h.validate_request(changed)
    with pytest.raises(FormalStateError, match="not an open"):
        h.date_plan(reseal({**request, "decision_dates": ["2026-08-30"]}))
    with pytest.raises(FormalStateError, match="context unavailable"):
        h.date_plan(reseal({**request, "decision_dates": [calendar[263].isoformat()]}))


def test_request_rejects_labels_unknown_fields_display_and_hash_drift(case):
    request, _ = case
    for changed in ({**request, "labels": {}}, {**request, "display_limit": 31}, {**request, "display_limit": True}):
        with pytest.raises(FormalStateError):
            h.validate_request(reseal(changed))
    changed = deepcopy(request)
    changed["source"]["calendar_sha256"] = "0" * 64
    with pytest.raises(h.inputs.reader.RotationL2Error):
        h.date_plan(reseal(changed))


def test_explicit_quarterly_bundle_locator_is_not_a_path_hash_or_latest(case, tmp_path):
    request, _ = case
    assets = {
        **request["model_assets"],
        "model_bundle_path": str(tmp_path / "model.json"),
        "model_bundle_sha256": "a" * 64,
    }
    h.validate_request(reseal({**request, "model_assets": assets}))
    for changed in ({**assets, "latest": True}, {k: v for k, v in assets.items() if k != "model_bundle_sha256"}):
        with pytest.raises(FormalStateError):
            h.validate_request(reseal({**request, "model_assets": changed}))


def test_release_locator_and_binding_can_change_without_model_or_source_change(case):
    request, bundle = case
    changed = deepcopy(request)
    changed["source"]["release_binding"].update(
        generation="next-compatible", revision="next-r1", manifest_sha256="f" * 64
    )
    changed = reseal(changed)
    updated = deepcopy(bundle)
    updated["request_sha256"] = changed["receipt_sha256"]
    updated["inference_identity"]["release_identity"].update(
        frozen_release_generation="next-compatible",
        frozen_release_binding_sha256=canonical_sha256(changed["source"]["release_binding"]),
        dataset_manifest_sha256="f" * 64,
    )
    updated = reseal(updated)
    original = h.infer(request, bundle, expected_bundle_sha256=bundle["receipt_sha256"])
    new = h.infer(changed, updated, expected_bundle_sha256=updated["receipt_sha256"])
    assert original["model_identity"] == new["model_identity"]
    assert original["predictions"] == new["predictions"]
    assert original["inference_identity"] != new["inference_identity"]
    # This proves consumer identity independence, not that an unvalidated new
    # dataset is ready. prepare() still uses the official pinned source reader.


def test_indirect_input_and_unexpected_label_bundle_fail_closed(case, tmp_path, monkeypatch):
    request, bundle = case
    changed = deepcopy(request)
    link = tmp_path / "indirect-calendar"
    # Exercise the scanner's indirect-path verdict without requiring Windows
    # administrator/developer-mode privileges just to create a test junction.
    ordinary_check = h.inputs.reader._is_link
    monkeypatch.setattr(h.inputs.reader, "_is_link", lambda path: path == link or ordinary_check(path))
    changed["source"]["candidate_root"] = str(link)
    with pytest.raises(FormalStateError, match="indirect"):
        h.validate_request(reseal(changed))
    changed_bundle = reseal({**bundle, "labels": {}})
    with pytest.raises(FormalStateError, match="fields differ"):
        h.infer(request, changed_bundle, expected_bundle_sha256=changed_bundle["receipt_sha256"])


def test_frozen_sigmoid_no_fit_and_single_date_batch_equivalence(case):
    request, bundle = case
    before = deepcopy(bundle["parameters"])
    with no_training_or_external_actions():
        result = h.infer(request, bundle, expected_bundle_sha256=bundle["receipt_sha256"])
    assert len(result["predictions"]) == 262 and result["fits"] == 0 and result["labels_read"] is False
    assert result["predictions"][0]["probability"] == pytest.approx(1 / (1 + math.exp(0.475)))
    assert bundle["parameters"] == before
    assert len(result["dates"][0]["display_sector_codes"]) == 10
    assert result["dates"][0]["display_sector_codes"][0] == bundle["catalog"][-1]
    day = request["decision_dates"][-1]
    single = reseal({**request, "decision_dates": [day]})
    subset = reseal(
        {
            **bundle,
            "request_sha256": single["receipt_sha256"],
            "as_of": {day: bundle["as_of"][day]},
            "rows": {day: bundle["rows"][day]},
        }
    )
    assert (
        h.infer(single, subset, expected_bundle_sha256=subset["receipt_sha256"])["predictions"]
        == result["predictions"][131:]
    )
    h.validate_prediction(request, bundle, result)


def test_natural_na_keeps_all_catalog_no_neutral_or_all_sector_gate(case):
    request, bundle = case
    for day in request["decision_dates"]:
        for code in bundle["catalog"]:
            bundle["rows"][day][code].update(features=None, reason_code="hmm_risk_c010_price_domain_empty_membership")
    bundle = reseal(bundle)
    result = h.infer(request, bundle, expected_bundle_sha256=bundle["receipt_sha256"])
    assert len(result["predictions"]) == 262 and result["capability_state"] == "NOT_AVAILABLE"
    assert all(row["probability"] is None and row["warning"] is None for row in result["predictions"])
    assert all(day["unavailable_count"] == 131 and day["display_sector_codes"] == [] for day in result["dates"])
    h.validate_prediction(request, bundle, result)


@pytest.mark.parametrize(
    "drift", ["parameters", "feature_order", "missing_sector", "as_of", "boolean", "untyped_na", "release"]
)
def test_parent_pinned_bundle_fails_closed_on_contract_drift(case, drift):
    request, bundle = case
    bundle = deepcopy(bundle)
    row = bundle["rows"][request["decision_dates"][0]][bundle["catalog"][0]]
    if drift == "parameters":
        bundle["parameters"] = {**bundle["parameters"], "intercept": [5]}
    elif drift == "feature_order":
        bundle["feature_names"] = bundle["feature_names"][::-1]
    elif drift == "missing_sector":
        bundle["rows"][request["decision_dates"][0]].pop(bundle["catalog"][-1])
    elif drift == "as_of":
        row["as_of_date"] = request["decision_dates"][0]
    elif drift == "boolean":
        row["features"][0] = True
    elif drift == "untyped_na":
        row["features"] = None
    else:
        bundle["inference_identity"]["release_identity"]["dataset_manifest_sha256"] = "f" * 64
    changed = reseal(bundle)
    with pytest.raises(FormalStateError):
        h.infer(request, changed, expected_bundle_sha256=changed["receipt_sha256"])


def test_prediction_readback_rejects_self_rehashed_wrong_warning_and_grid(case):
    request, bundle = case
    result = h.infer(request, bundle, expected_bundle_sha256=bundle["receipt_sha256"])
    changed = deepcopy(result)
    changed["predictions"][0]["warning"] = not changed["predictions"][0]["warning"]
    with pytest.raises(FormalStateError, match="value/NA/as-of"):
        h.validate_prediction(request, bundle, reseal(changed))
    changed = deepcopy(result)
    changed["predictions"] = changed["predictions"][::-1]
    with pytest.raises(FormalStateError, match="grid"):
        h.validate_prediction(request, bundle, reseal(changed))


def test_prepare_reuses_bounded_kernel_and_original_20d_definition(case, monkeypatch):
    request, fixture = case
    catalog = fixture["catalog"]
    as_of = fixture["as_of"]
    window = tuple(date.fromisoformat(day) for day in as_of.values())
    calls = []

    def bounded(frozen, source, **kwargs):
        calls.append(kwargs)
        assert source is request["source"] and frozen["eligibility"] == {"train_only": True}
        identity = {**fixture["inference_identity"], "domain_reasons": {}}
        return {"benchmark": dict.fromkeys(window, 0.0)}, window, [], identity

    frame = pd.DataFrame(
        0.0,
        index=pd.MultiIndex.from_product(
            [pd.to_datetime(list(as_of.values())), catalog], names=["trade_date", "l1_code"]
        ),
        columns=h.ALL_CORE_FEATURES,
    )
    monkeypatch.setattr(h, "_bounded_l2_stock_facts", bounded)
    monkeypatch.setattr(
        h, "build_c010_feature_domain_panel", lambda *args, **kwargs: (frame, {"version": "original"}, {})
    )
    actual = h.prepare(request, "f" * 40)
    assert calls[0]["end"] == date(2026, 8, 28) and calls[0]["prewarm_price_history"] is True
    assert calls[0]["expected_calendar_sha256"] == request["source"]["calendar_sha256"]
    assert calls[0]["release_binding"] == request["source"]["release_binding"]
    assert len(actual["rows"]["2026-08-31"][catalog[0]]["features"]) == 20
    h.validate_bundle(request, actual, actual["receipt_sha256"])


def test_new_release_calendar_pin_does_not_weaken_existing_default(monkeypatch):
    from backend.services.hmm_risk import formal_state_input as kernel
    from backend.services.hmm_risk.formal_state_effect import CALENDAR_SHA

    assets = {"files": {}, "inventory": {"qlib": {"calendar_sha256": "1" * 64}}, "qlib_root": Path(".")}
    monkeypatch.setattr(h.reader, "load_rotation_l1_direct_v2_source_assets", lambda *a, **k: assets)
    monkeypatch.setattr(h.reader, "_load_qlib_calendar", lambda p: [date(2026, 8, 28)])

    class ReachedAuthority(Exception):
        pass

    def reached(*a, **k):
        raise ReachedAuthority

    monkeypatch.setattr(h.reader, "_industry_adapter", reached)
    kwargs = dict(start=date(2026, 8, 28), end=date(2026, 8, 28), release_binding={})
    source = {"candidate_root": ".", "security_identity_manifest": ".", "provider_absence_manifest": "."}
    with pytest.raises(FormalStateError, match="calendar file changed"):
        kernel._bounded_l2_stock_facts({}, source, **kwargs)
    with pytest.raises(ReachedAuthority):
        kernel._bounded_l2_stock_facts({"industry_authority": {}}, source, expected_calendar_sha256="1" * 64, **kwargs)
    assets["inventory"]["qlib"]["calendar_sha256"] = CALENDAR_SHA
    with pytest.raises(ReachedAuthority):
        kernel._bounded_l2_stock_facts({"industry_authority": {}}, source, **kwargs)


def test_model_bootstrap_needs_no_feature_or_label_asset(tmp_path, monkeypatch):
    # Exercise the real authentication function rather than the inference fixture.
    path = tmp_path / "source.json"
    path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(h.risk, "SOURCE_REQUEST_FILE_SHA", hashlib.sha256(path.read_bytes()).hexdigest())
    frozen = {
        "feature_definition": {"original": True},
        "eligibility": {},
        "industry_authority": {"identity": {"bundle_hash": h.risk.PIT_BUNDLE_SHA}},
    }
    monkeypatch.setattr(h, "load_effect_request", lambda p: {"frozen": frozen})
    identity = {
        "pit_bundle_sha256": h.risk.PIT_BUNDLE_SHA,
        "feature_definition_sha256": canonical_sha256(frozen["feature_definition"]),
    }
    parameters = {"unchanged": True}
    sealed = receipt(
        {
            "contract": h.risk.CONTRACT,
            "feature_sha256": "2" * 64,
            "model_sha256": canonical_sha256(parameters),
            "parameters": parameters,
            "input_identity": identity,
            "predictions": [],
        }
    )
    accepted = receipt(
        {
            "model": {k: v for k, v in sealed.items() if k not in ("predictions", "receipt_sha256")},
            "sealed_prediction_sha256": sealed["receipt_sha256"],
        }
    )
    for name, obj in (("sealed", sealed), ("acceptance", accepted)):
        write_once(tmp_path / (name + ".json"), obj)
    pins = dict(
        sealed_hash=sealed["receipt_sha256"],
        acceptance_hash=accepted["receipt_sha256"],
        features_hash="2" * 64,
        model_hash=canonical_sha256(parameters),
        input_hash=canonical_sha256(identity),
    )
    monkeypatch.setattr(h.value, "APPROVED_PINS", pins)
    request = {
        "model_assets": {
            "source_request_path": str(path),
            "sealed_path": str(tmp_path / "sealed.json"),
            "acceptance_path": str(tmp_path / "acceptance.json"),
        }
    }
    assert h.model_source(request)[1] == parameters
    assert not (tmp_path / "features.json").exists()
    from backend.services.hmm_risk import risk_l2_rolling as rolling

    quarterly = {"parameters": "loaded"}
    monkeypatch.setattr(rolling, "load_model", lambda *args: (quarterly, {"vintage": rolling.VINTAGES[0]}))
    extended = {
        **request,
        "model_assets": {**request["model_assets"], "model_bundle_path": "explicit"},
        "decision_dates": ["2026-04-01"],
    }
    assert h.model_source(extended)[1] == quarterly
    with pytest.raises(FormalStateError, match="effective interval"):
        h.model_source({**extended, "decision_dates": ["2026-07-01"]})
    pins["model_hash"] = "0" * 64
    with pytest.raises(FormalStateError, match="linkage differs"):
        h.model_source(request)


@pytest.mark.parametrize("failure", [None, "prepare", "second_spawn", "readback"])
def test_cli_actual_completion_counts_and_no_overwrite(case, tmp_path, monkeypatch, failure):
    request, bundle = case
    request_path, output = tmp_path / "request.json", tmp_path / "run"
    write_once(request_path, request)
    monkeypatch.setattr(cli, "_head", lambda: "f" * 40)
    monkeypatch.setattr(cli, "_clean", lambda head: None)
    monkeypatch.setattr(cli, "_numeric", lambda kind: {"fixed": True})

    def prepare(*a):
        if failure == "prepare":
            raise h.risk.fail("injected prepare failure")
        return bundle

    monkeypatch.setattr(h, "prepare", prepare)
    processes = []

    class Process:
        def __init__(self, command, **kwargs):
            if failure == "second_spawn" and processes:
                raise OSError("injected spawn failure")
            result = h.infer(request, bundle, expected_bundle_sha256=bundle["receipt_sha256"])
            write_once(
                Path(command[command.index("--output") + 1]), reseal({**result, "numeric_environment": {"fixed": True}})
            )
            processes.append(self)

        def wait(self):
            return 0

    monkeypatch.setattr(cli.subprocess, "Popen", Process)
    if failure == "readback":
        monkeypatch.setattr(
            h, "validate_prediction", lambda *a: (_ for _ in ()).throw(h.risk.fail("injected readback failure"))
        )
    args = [
        "run",
        "--request",
        str(request_path),
        "--request-sha256",
        request["receipt_sha256"],
        "--output",
        str(output),
    ]
    assert cli.main(args) == (0 if failure is None else 2)
    if failure is None:
        acceptance = read_json(output / "acceptance.json")
        before = (output / "acceptance.json").read_bytes()
        assert acceptance["completed_children"] == 2 and acceptance["fits"] == 0
        assert cli.main(args) == 2 and (output / "acceptance.json").read_bytes() == before
    else:
        failed = read_json(output.with_name("run.parent.failure.json"))
        assert failed["completed_children"] == {"prepare": 0, "second_spawn": 1, "readback": 2}[failure]
