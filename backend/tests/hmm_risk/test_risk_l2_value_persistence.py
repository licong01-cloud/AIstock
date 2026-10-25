"""Direct approved persistence consumption contracts, not another training suite."""

from copy import deepcopy
from pathlib import Path

import pytest

from backend.services.hmm_risk import risk_l2_value_persistence as new
from backend.services.hmm_risk.risk_l2_value_replay import ReplayError, replay
from backend.tests.hmm_risk.test_risk_l2_value_replay import panel, warn


def test_two_day_confirmation_and_cold_start_do_not_use_future_warning():
    c, d, s, _ = panel()
    for day in d[:2]:
        warn(s, c[0], day)
    actions = new.actions(c, d, s)
    assert [(actions[x][c[0]]["cash_latch"], actions[x][c[0]]["status"]) for x in d] == [
        (0, "ENTER_CONFIRMATION_PENDING"),
        (1, "CONFIRMED_CASH"),
        (1, "EXIT_CONFIRMATION_PENDING"),
        (0, "BASE_EXPOSED"),
    ]
    changed = deepcopy(s)
    for day in d[1:]:
        warn(changed, c[1], day)
    assert new.actions(c, d, changed)[d[0]] == actions[d[0]]


def test_legal_na_breaks_confirmation_but_retains_policy_memory():
    c, d, s, _ = panel()
    for day in d[:2]:
        warn(s, c[0], day)
    s[d[2]][c[0]].update(probability=None, warning=None, availability="unavailable", reason_code="input_na")
    a = new.actions(c, d, s)
    assert a[d[2]][c[0]] == {
        "cash_latch": 1,
        "warning_streak": 0,
        "clear_streak": 0,
        "status": "INPUT_UNAVAILABLE_CASH",
    }
    assert a[d[3]][c[0]]["cash_latch"] == 1
    assert a[d[3]][c[0]]["clear_streak"] == 1
    assert a[d[3]][c[0]]["status"] == "EXIT_CONFIRMATION_PENDING"


def test_common_valuation_na_and_cost_unknown_do_not_reset_actions_or_make_full_nav():
    c, d, s, r = panel()
    for day in d:
        warn(s, c[0], day)
    r[d[2]][c[0]] = None
    old = replay(c, d, s, r)
    result = new.compare(c, d, s, r, old, [])
    assert result["paired_return_dates"] == 2
    assert result["reference_status"] == "INSUFFICIENT_REFERENCE_PATH"
    assert result["gross_full_path"]["C"] is None
    assert [x["date_count"] for x in result["gross_blocks"]["C"]] == [1, 1]
    final = result["daily"][-1]["arms"]["C"]
    assert final["risk_budget"] == pytest.approx(130 / 131)
    assert final["one_sided_risk_turnover"] is None
    assert final["cost_sensitivity_return"]["0"] is not None
    assert final["cost_sensitivity_return"]["5"] is None
    assert result["net_value_status"] == "UNASSESSED"


def test_all_warning_first_day_exposed_second_cash_and_alternation_memory():
    c, d, s, r = panel()
    for day in d[:2]:
        for code in c:
            warn(s, code, day)
    old = replay(c, d, s, r)
    result = new.compare(c, d, s, r, old, [])
    assert [x["arms"]["C"]["risk_budget"] for x in result["daily"]] == pytest.approx([1, 0, 0])
    assert all(x["arms"]["C"]["risk_budget"] == x["arms"]["X_C"]["risk_budget"] for x in result["daily"])
    assert result["daily"][0]["arms"]["R"] == old["daily"][0]["arms"]["R"]


@pytest.mark.parametrize("drift", ["missing_date", "unknown_code", "future_asof", "warning", "nan"])
def test_illegal_signals_fail_closed_instead_of_entering_na_memory(drift):
    c, d, s, _ = panel()
    if drift == "missing_date":
        s.pop(d[1])
    elif drift == "unknown_code":
        s[d[0]]["999999.SI"] = s[d[0]][c[0]]
    elif drift == "future_asof":
        s[d[1]][c[0]]["as_of_date"] = d[1]
    elif drift == "warning":
        s[d[0]][c[0]]["warning"] = True
    else:
        s[d[0]][c[0]]["probability"] = float("nan")
    with pytest.raises(ReplayError):
        new.actions(c, d, s)


def test_turnover_is_drifted_and_common_dates_not_initial_cash_or_half_turnover():
    c, d, s, r = panel()
    r[d[1]][c[0]] = 1
    for code in c[1:]:
        r[d[1]][code] = 0
    old = replay(c, d, s, r)
    result = new.compare(c, d, s, r, old, [])
    expected = abs(1 / 131 - 2 / 132) + 130 * abs(1 / 131 - 1 / 132)
    assert result["daily"][1]["arms"]["C"]["one_sided_risk_turnover"] == pytest.approx(expected)
    assert result["turnover_comparison"]["C_minus_R"] == 0
    assert result["turnover_comparison"]["date_count"] == 3
    assert result["turnover_assessment"] == "KNOWN_PAIRED_TURNOVER_NOT_LOWER"
    assert result["turnover_comparison"]["exposure_adjusted_difference"] == 0


def test_delay_event_and_exit_opportunity_are_not_original_model_precision():
    c, d, s, r = panel()
    for day in d[:2]:
        warn(s, c[0], day)
    labels = [
        {
            "trade_date": day,
            "sector_code": c[0],
            "status": "AVAILABLE",
            "availability": "available",
            "event": event,
            "warning": s[day][c[0]]["warning"],
            "return": value,
            "drawdown": dd,
        }
        for day, event, value, dd in zip(d[:3], [1, 1, 0], [-0.09, -0.08, 0.1], [-0.12, -0.1, -0.02], strict=True)
    ]
    result = new.compare(c, d, s, r, replay(c, d, s, r), labels)
    tradeoff = result["risk_opportunity_tradeoff"]
    assert tradeoff["raw_event_covered"] == 2 and tradeoff["confirmed_event_covered"] == 1
    assert tradeoff["groups"]["delayed_event"]["return_10d"]["mean"] == -0.09
    assert tradeoff["groups"]["delayed_exit_non_event"]["return_10d"]["mean"] == 0.1
    changed = deepcopy(labels)
    changed[0]["event"] = 0
    assert new.compare(c, d, s, r, replay(c, d, s, r), changed)["action_sha256"] == result["action_sha256"]


@pytest.mark.parametrize("kind", ["wrong_date", "missing_sector", "wrong_count", "old_block"])
def test_old_result_population_and_readback_drift_are_not_recomputed_or_accepted(kind, monkeypatch):
    c, d, s, r = panel()
    baseline = replay(c, d, s, r)
    if kind == "wrong_date":
        baseline["daily"][0]["return_date"] = d[-1]
    elif kind == "missing_sector":
        baseline["daily"][0]["eligible_count"] -= 1
    elif kind == "wrong_count":
        baseline["planned_return_dates"] -= 1
    else:
        baseline["gross_blocks"]["R"][0]["cumulative_return"] += 0.01
    monkeypatch.setattr(new.old, "replay", lambda *a: pytest.fail("old replay must not execute"))
    with pytest.raises(ReplayError, match="old"):
        new.compare(c, d, s, r, baseline, [])


def test_all_unavailable_is_explicit_cash_and_not_success():
    c, d, s, r = panel()
    for rows in s.values():
        for row in rows.values():
            row.update(probability=None, warning=None, availability="unavailable", reason_code="input_na")
    result = new.compare(c, d, s, r, replay(c, d, s, r), [])
    assert result["reference_status"] == "INSUFFICIENT_REFERENCE_PATH"
    assert result["gross_full_path"]["C"] is None
    assert all(x["arms"]["C"]["cash_budget"] == 1 for x in result["daily"])
    assert result["action_count"] == 131 * len(d)


def test_producer_filter_and_active_profile_resolution_are_poisoned():
    from backend.services.hmm_risk import formal_state_model, rotation_l1_input_bundle

    with new.old.zero_compute():
        for call in (
            lambda: formal_state_model.fit_entry(None, None, None),
            lambda: formal_state_model.causal_filter(None, None, None, None),
            rotation_l1_input_bundle.load_active_hmm_dataset_identity,
        ):
            with pytest.raises(ReplayError, match="forbidden"):
                call()


def test_rehashed_request_cannot_change_the_approved_model(tmp_path):
    from backend.services.hmm_risk.contracts import canonical_json_bytes
    from backend.services.hmm_risk.formal_state_model import receipt

    body = receipt(
        {
            "schema_version": new.VERSION + "_request",
            **new.APPROVED_PINS,
            "model_hash": "a" * 64,
            "contract": new.CONTRACT,
            "contract_sha256": new.CONTRACT_SHA256,
        }
    )
    path = tmp_path / "request.json"
    path.write_bytes(canonical_json_bytes(body))
    with pytest.raises(ReplayError) as error:
        new.load_inputs(path, body["receipt_sha256"])
    assert error.value.reason_code == "hmm_risk_l2_value_identity_mismatch"


def test_capital_depletion_is_a_reference_limitation_not_another_model_gate():
    c, d, s, r = panel()
    r[d[1]] = {code: -1 for code in c}
    result = new.compare(c, d, s, r, replay(c, d, s, r), [])
    assert result["reference_capital_depleted"] is True
    assert result["execution_status"] == "COMPLETED"
    assert result["reference_status"] == "INSUFFICIENT_REFERENCE_PATH"
    assert result["gross_full_path"]["C"] is None


@pytest.mark.parametrize("fault", [None, "tail", "wrong_source", "changed_during_read", "symlink"])
def test_fifth_source_receipt_is_closed_without_replaying_or_copying_old_assets(tmp_path, monkeypatch, fault):
    from backend.services.hmm_risk.contracts import canonical_json_bytes
    from backend.services.hmm_risk.formal_state_model import receipt

    c, d, s, r = panel()
    baseline = replay(c, d, s, r)
    request = {
        **new.APPROVED_PINS,
        "contract": new.CONTRACT,
        "contract_sha256": new.CONTRACT_SHA256,
        **{k + "_path": str(tmp_path / (k + ".json")) for k in new.SOURCES},
    }
    baseline.update(source_pins=new.old.APPROVED_PINS, source_paths={k: request[k + "_path"] for k in new.SOURCES[:-1]})
    if fault == "wrong_source":
        baseline["source_pins"] = {**new.old.APPROVED_PINS, "facts_hash": "a" * 64}
    value = receipt(
        {
            "schema_version": new.old.VERSION + "_acceptance",
            "execution_status": "COMPLETED",
            "fresh_process_bitwise_equal": True,
            "numeric_tolerance_used": False,
            "planned_fits": 0,
            "completed_fits": 0,
            "database_access": False,
            "tail_accessed": fault == "tail",
            "runtime_action": False,
            "result": receipt({k: v for k, v in baseline.items() if k != "receipt_sha256"}),
        }
    )
    labels = [{"trade_date": day, "sector_code": code} for day in d for code in c]
    risk = receipt({"result": {"predictions": labels}})
    monkeypatch.setitem(new.APPROVED_PINS, "value_hash", value["receipt_sha256"])
    monkeypatch.setitem(new.APPROVED_PINS, "acceptance_hash", risk["receipt_sha256"])
    for name, payload in (("value", value), ("acceptance", risk)):
        (tmp_path / (name + ".json")).write_bytes(canonical_json_bytes(payload))
    # First-four source contracts have their own production validator; isolate the added fifth boundary.
    monkeypatch.setattr(new.old, "load_inputs", lambda *a, **kw: (request, c, d, s, r))
    if fault == "changed_during_read":
        stable = new.old.original._stable

        def change(path, stamp):
            if path.name == "value.json":
                path.write_bytes(path.read_bytes() + b" ")
            stable(path, stamp)

        monkeypatch.setattr(new.old.original, "_stable", change)
    elif fault == "symlink":
        # Exercise the reader's indirect-path rejection without requiring Windows symlink privileges.
        import os
        from types import SimpleNamespace

        real = os.stat

        def lstat(path, *args, **kwargs):
            actual = real(path, *args, **kwargs)
            if str(path).endswith("value.json"):
                return SimpleNamespace(st_mode=actual.st_mode, st_file_attributes=0x400)
            return actual

        monkeypatch.setattr(Path, "lstat", lstat)
    if fault is None:
        assert new.load_inputs(tmp_path / "request.json", "a" * 64)[5] == value["result"]
    else:
        with pytest.raises(ReplayError):
            new.load_inputs(tmp_path / "request.json", "a" * 64)
