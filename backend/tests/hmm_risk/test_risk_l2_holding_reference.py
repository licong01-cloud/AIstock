"""Holding-reference causal boundaries and parent authority, without new inference."""

from contextlib import nullcontext
from copy import deepcopy
from datetime import date
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import risk_l2_holding_reference as h
from backend.services.hmm_risk.formal_state_model import receipt
from scripts.hmm_risk import run_risk_l2_holding_reference as cli


CALENDAR = [date(2026, 3, 30), date(2026, 3, 31), date(2026, 4, 1), date(2026, 4, 2)]
LEDGER = [{"q": "2026-03-31", "p": "2026-04-01", "u": "2026-04-02"}]
CODE = "801783.SI"
SYMBOL = "000001.SZ"


def cohort(symbol=SYMBOL, **changes):
    return {
        **LEDGER[0],
        "sector_code": CODE,
        "symbol": symbol,
        "circ_mv": 100.0,
        "circ_source_date": "2026-03-31",
        "circ_reason": None,
        "weight": 1.0,
        "pit_start": "2020-07-30",
        "pit_end": "2026-08-31",
        "security_row_sha256": "a" * 64,
        "projection_sha256": "b" * 64,
        **changes,
    }


def prices(symbol=SYMBOL):
    # Official prev_close, not a made-up bridge from a different day's close.
    return {
        (symbol, "2026-04-01"): {"close": 100.0, "prev_close": 99.0},
        (symbol, "2026-04-02"): {"close": 55.0, "prev_close": 50.0},
    }


def spans(symbol=SYMBOL, end=CALENDAR[-1]):
    return {symbol: [(date(2020, 7, 30), end)]}


def freeze(monkeypatch, intervals, history, mapped=None):
    def industry(symbol, day):
        return SimpleNamespace(status="resolved", l2_code=CODE if mapped is None else mapped[symbol])

    projection = SimpleNamespace(resolve=industry)
    security = SimpleNamespace(resolve=lambda s, d, dataset: SimpleNamespace(source_ts_code=s, row_hash="a" * 64))
    monkeypatch.setattr(h.reader, "_industry_projection_identity", lambda value: (value.status, value.l2_code))
    return h.freeze_cohorts(LEDGER, [CODE], intervals, projection, security, history, CALENDAR)


def test_new_and_reentering_members_never_change_prior_cohort(monkeypatch):
    intervals = {
        **spans(),
        "000002.SZ": [(CALENDAR[-1], date(2026, 8, 31))],
        "000003.SZ": [(CALENDAR[0], CALENDAR[0]), (CALENDAR[2], CALENDAR[-1])],
    }
    history = {SYMBOL: [(CALENDAR[1], 100.0, "available", None)]}
    frozen = freeze(monkeypatch, intervals, history)
    assert [r["symbol"] for r in frozen] == [SYMBOL]
    # Unknown prices or cap of a future entrant are irrelevant, not filled.
    actual, _, _ = h.reference_returns(LEDGER, [CODE], frozen, prices(), set(), intervals)
    assert actual["2026-04-02"][CODE] == pytest.approx(0.1)
    audit = h.boundary_audit(LEDGER, intervals)[0]
    assert audit["q_member_count"] == 1 and audit["later_entrants_not_held"] == ["000002.SZ", "000003.SZ"]


def test_q_member_is_included_even_if_entered_q_and_exited_before_return(monkeypatch):
    intervals = {SYMBOL: [(CALENDAR[1], CALENDAR[1])]}
    history = {SYMBOL: [(CALENDAR[0], 100.0, "available", None), (CALENDAR[2], 999.0, "available", None)]}
    frozen = freeze(monkeypatch, intervals, history)
    assert frozen[0]["circ_source_date"] == "2026-03-30"
    assert frozen[0]["circ_mv"] == 100.0 and frozen[0]["weight"] == 1.0
    actual, _, _ = h.reference_returns(LEDGER, [CODE], frozen, prices(), set(), intervals)
    assert actual["2026-04-02"][CODE] == pytest.approx(0.1)


def test_latest_invalid_cap_is_not_replaced_by_older_finite_fact_or_renormalized(monkeypatch):
    intervals = {**spans(), **spans("000002.SZ")}
    history = {
        SYMBOL: [(CALENDAR[0], 100.0, "available", None), (CALENDAR[1], None, "missing", "source_na")],
        "000002.SZ": [(CALENDAR[1], 100.0, "available", None)],
    }
    frozen = freeze(monkeypatch, intervals, history)
    assert len(frozen) == 2 and all(r["weight"] is None for r in frozen)
    actual, status, members = h.reference_returns(LEDGER, [CODE], frozen, prices(), set(), intervals)
    assert actual["2026-04-02"][CODE] is None
    assert status[0]["reason_codes"] == ["REFERENCE_WEIGHT_UNAVAILABLE"]
    assert all(r["return"] is None for r in members)


def test_source_alias_is_resolved_without_private_industry_ids(monkeypatch):
    monkeypatch.setattr(h.reader, "_industry_projection_identity", lambda value: value.l2_code)
    security = SimpleNamespace(resolve=lambda *args: SimpleNamespace(source_ts_code="000022.SZ", row_hash="c" * 64))
    projection = SimpleNamespace(resolve=lambda *args: SimpleNamespace(status="resolved", l2_code=CODE))
    rows = h.freeze_cohorts(
        LEDGER,
        [CODE],
        spans(),
        projection,
        security,
        {"000022.SZ": [(CALENDAR[1], 200.0, "available", None)]},
        CALENDAR,
    )
    assert rows[0]["sector_code"] == CODE and rows[0]["circ_mv"] == 200.0


def test_only_unresolved_cap_keys_seek_real_prior_context(monkeypatch):
    calls = []

    def read(_path, **kwargs):
        calls.append((kwargs["start"], kwargs["end"]))
        if kwargs["start"].month == 4:
            rows = [(date(2026, 4, 2), "alias", 900.0), (date(2026, 4, 1), "new", 100.0)]
        elif kwargs["start"].month == 3:
            rows = [(date(2026, 3, 31), "alias", None)]
        else:
            pytest.fail("must not backtrack past the latest invalid prior fact")
        return pd.DataFrame(
            {"circ_mv": [r[2] for r in rows]}, index=pd.MultiIndex.from_tuples([(r[0], r[1]) for r in rows])
        )

    monkeypatch.setattr(h.reader, "_load_fixed_h5_window", read)
    monkeypatch.setattr(
        h.reader,
        "_daily_basic_lookup",
        lambda f: (
            {},
            {
                d: [
                    (s, value, "available" if pd.notna(value) else "missing", None)
                    for (day, s), value in f["circ_mv"].items()
                    if day == d
                ]
                for d in set(f.index.get_level_values(0))
            },
        ),
    )
    result = h._basic_history(
        {"files": {"daily_basic": "not-opened"}},
        {"alias": date(2026, 4, 1), "new": date(2026, 4, 1)},
        date(2026, 4, 1),
        date(2026, 4, 2),
    )
    assert len(calls) == 2 and result["alias"][0][0] == date(2026, 3, 31)
    assert result["alias"][0][1] is None


@pytest.mark.parametrize(
    "missing, suspended, exited, expected",
    [
        ("start", False, False, "REFERENCE_START_VALUATION_UNAVAILABLE"),
        ("end", True, False, "REFERENCE_SUSPENSION_VALUATION_UNAVAILABLE"),
        ("end", False, True, "REFERENCE_EXIT_VALUATION_UNAVAILABLE"),
        ("end", False, False, "REFERENCE_QUOTE_UNAVAILABLE"),
    ],
)
def test_missing_valuation_is_typed_na_not_zero_or_member_deletion(missing, suspended, exited, expected):
    quotes = prices()
    quotes.pop((SYMBOL, "2026-04-01" if missing == "start" else "2026-04-02"))
    actual, status, members = h.reference_returns(
        LEDGER,
        [CODE],
        [cohort()],
        quotes,
        {(CALENDAR[-1], SYMBOL)} if suspended else set(),
        spans(end=CALENDAR[1] if exited else CALENDAR[-1]),
    )
    assert actual["2026-04-02"][CODE] is None
    assert status[0]["members"] == 1 and status[0]["reason_codes"] == [expected]
    assert members[0]["reason_code"] == expected


def test_real_suspension_and_singleton_extreme_returns_are_not_quality_gates():
    for end in (50.0, 0.5, 500.0):
        quotes = prices()
        quotes[(SYMBOL, "2026-04-02")]["close"] = end
        actual, status, members = h.reference_returns(
            LEDGER, [CODE], [cohort()], quotes, {(CALENDAR[-1], SYMBOL)}, spans()
        )
        assert actual["2026-04-02"][CODE] == end / 50.0 - 1
        assert status[0]["reason_codes"] == [] and members[0]["reason_code"] is None
        assert members[0]["suspended"] is True


@pytest.mark.parametrize("drift", ["future", "duplicate", "weight", "partial"])
def test_illegal_cohort_cannot_self_authorize_or_silently_reduce_denominator(drift):
    rows = [cohort()]
    if drift == "future":
        rows[0]["circ_source_date"] = "2026-04-01"
    elif drift == "duplicate":
        rows.append(deepcopy(rows[0]))
    elif drift == "weight":
        rows[0]["weight"] = 0.5
    else:
        rows[0]["circ_mv"] = None
    with pytest.raises(RuntimeError):
        h.reference_returns(LEDGER, [CODE], rows, prices(), set(), spans())


def test_qlib_reader_preserves_factor_semantics_and_reads_held_stock_after_exit(tmp_path):
    root = tmp_path / "features" / SYMBOL.lower()
    root.mkdir(parents=True)
    for field in h.reader.QLIB_STOCK_FIELDS:
        values = {"close": [200.0, 110.0], "factor": [2.0, 2.0], "prev_close": [99.0, 50.0]}.get(
            field, [float("nan"), float("nan")]
        )
        (root / f"{field}.day.bin").write_bytes(np.array([2.0, *values], dtype="<f4").tobytes())
    actual, inventory = h._quotes({"qlib_root": tmp_path}, [cohort(pit_end="2026-03-31")], CALENDAR)
    assert actual == prices()
    assert len(inventory) == 64


@pytest.fixture
def arithmetic_case(monkeypatch):
    codes = [f"801{i:03d}.SI" for i in range(131)]
    days = ["2026-04-01", "2026-04-02"]
    monkeypatch.setattr(h, "schedule", lambda bundle: LEDGER)
    monkeypatch.setattr(h.continuous.history, "schedule", lambda calendar: {"days": days})
    signals = {
        day: {
            code: {
                "trade_date": day,
                "as_of_date": "2026-03-31" if day == days[0] else days[0],
                "availability": "available",
                "probability": (i % 10) / 10,
                "warning": (i % 10) >= 2,
                "reason_code": None,
            }
            for i, code in enumerate(codes)
        }
        for day in days
    }
    rows = [cohort(sector_code=code) for code in codes]
    returns, status, members = h.reference_returns(LEDGER, codes, rows, prices(), set(), spans())
    request = receipt({"unit_request": True})
    identity = {"dataset_manifest_sha256": h.continuous.BINDING["manifest_sha256"]}
    original = {"features": {"catalog": codes, "calendar": [], "source_identity": {"release_identity": identity}}}
    facts = receipt(
        {
            "schema_version": h.VERSION + "_facts",
            "contract": h.CONTRACT,
            "request_sha256": request["receipt_sha256"],
            "original_pins": h.PINS,
            "catalog": codes,
            "ledger": LEDGER,
            "source_identity": {"release_identity": identity},
            "cohorts": rows,
            "cohort_sha256": h.canonical_sha256(rows),
            "quotes": [{"symbol": s, "day": d, **v} for (s, d), v in sorted(prices().items())],
            "suspension_keys": [],
            "valuation_spans": {SYMBOL: [["2020-07-30", "2026-08-31"]]},
            "natural_boundary_audit": h.boundary_audit(LEDGER, spans()),
            "returns": returns,
            "industry_status": status,
            "member_values": members,
        }
    )
    return request, original, signals, facts


def test_five_arms_four_costs_readback_does_not_promote_value(arithmetic_case):
    request, original, signals, facts = arithmetic_case
    with h.zero_compute():
        result = h.evaluate(request, original, signals, facts, facts["receipt_sha256"])
    assert result["status"] == "REFERENCE_PATH_COMPLETE" and result["fits"] == 0
    assert set(result["gross_full_path"]) == {"B", "R", "X_R", "C", "X_C"}
    assert set(result["cost_comparisons"]) == {"C_minus_B", "C_minus_R", "C_minus_X_C", "R_minus_X_R"}
    assert all(set(r) == {"0", "5", "10", "20"} for r in result["cost_comparisons"].values())
    assert result["product_activation"] is False and result["forward_confirmed"] is False
    assert result["net_value_status"] == "UNASSESSED" and result["new_inference_calls"] == 0


@pytest.mark.parametrize("drift", ["parent_pin", "returns", "model_lineage", "duplicate_quote"])
def test_self_hashed_facts_cannot_replace_parent_or_business_readback(arithmetic_case, drift):
    request, original, signals, facts = arithmetic_case
    changed = deepcopy(facts)
    if drift == "returns":
        changed["returns"]["2026-04-02"][original["features"]["catalog"][0]] = 100.0
    elif drift == "model_lineage":
        changed["original_pins"]["sealed_1"] = "0" * 64
    else:
        changed["quotes"].append(changed["quotes"][0])
    changed = receipt({k: v for k, v in changed.items() if k != "receipt_sha256"})
    expected = facts["receipt_sha256"] if drift == "parent_pin" else changed["receipt_sha256"]
    with pytest.raises((ValueError, RuntimeError)):
        h.evaluate(request, original, signals, changed, expected)


def test_wrong_contract_stops_before_any_original_asset_read(monkeypatch):
    monkeypatch.setattr(h.continuous.inputs, "_asset", lambda *args: pytest.fail("must not read assets"))
    request = receipt(
        {
            "schema_version": h.VERSION + "_request",
            "contract": {**h.CONTRACT, "fits": 1},
            "assets": dict.fromkeys(h.PINS, "not-opened"),
            "asset_locator_rebind": {},
        }
    )
    with pytest.raises(RuntimeError, match="approved authority"):
        h.load_inputs(request)


def test_fit_filter_predict_active_profile_and_db_are_poisoned():
    from sklearn.linear_model import LogisticRegression
    from backend.db import pg_pool
    from backend.services.hmm_risk import formal_state_model

    with h.zero_compute():
        for function in (
            LogisticRegression().predict,
            h.continuous.history.risk.fit_predict,
            formal_state_model.causal_filter,
            h.reader.load_active_hmm_dataset_identity,
            pg_pool.get_conn,
        ):
            with pytest.raises((RuntimeError, AssertionError), match="forbidden"):
                function(None)


def test_cli_success_and_finalization_failure_have_real_child_counts(tmp_path, monkeypatch, arithmetic_case):
    request, original, signals, facts = arithmetic_case
    output = tmp_path / "run"
    monkeypatch.setattr(cli, "_head", lambda: "a" * 40)
    monkeypatch.setattr(cli.subprocess, "check_output", lambda *args, **kwargs: "")
    monkeypatch.setattr(h, "build_facts", lambda *args: facts)

    class Process:
        def __init__(self, command, **kwargs):
            path = command[command.index("--output") + 1]
            cli.write_once(cli.Path(path), cli.repeat(request, original, signals, facts, "a" * 40, {}))

        def wait(self):
            return 0

    monkeypatch.setattr(cli.subprocess, "Popen", Process)
    args = SimpleNamespace(request=tmp_path / "request.json", output=output)
    cli.run(args, request, original, {}, signals, "a" * 40, {})
    accepted = cli.read_json(output / "acceptance.json")
    assert accepted["completed_children"] == 2 and accepted["two_process_bitwise_equal"] is True
    assert (output / "process_1.json").read_bytes() == (output / "process_2.json").read_bytes()
    monkeypatch.setattr(cli, "_numeric", lambda *args: {})
    monkeypatch.setattr(h, "zero_compute", nullcontext)
    monkeypatch.setattr(h.continuous.inputs, "_asset", lambda *args: request)
    monkeypatch.setattr(h, "load_inputs", lambda *args: (original, {}, signals))
    original_write = cli.write_once

    def fail_write(path, value):
        if path.name == "acceptance.json":
            raise OSError("injected finalization write error")
        original_write(path, value)

    monkeypatch.setattr(cli, "write_once", fail_write)
    failed = tmp_path / "failed"
    argv = [
        "run",
        "--request",
        str(args.request),
        "--request-sha256",
        request["receipt_sha256"],
        "--output",
        str(failed),
    ]
    assert cli.main(argv) == 1
    failure = cli.read_json(tmp_path / "failed.parent.failure.json")
    assert failure["completed_children"] == 2 and failure["fits"] == 0
    first = (tmp_path / "failed.parent.failure.json").read_bytes()
    assert cli.main(argv) == 1
    assert (tmp_path / "failed.parent.failure.json").read_bytes() == first


@pytest.mark.parametrize("fault, expected", [("facts", 0), ("second_spawn", 1), ("child_identity", 2)])
def test_parent_failure_counts_include_preparation_partial_spawn_and_self_hashed_child(
    tmp_path, monkeypatch, arithmetic_case, fault, expected
):
    request, original, signals, facts = arithmetic_case
    monkeypatch.setattr(cli, "_head", lambda: "a" * 40)
    monkeypatch.setattr(cli, "_numeric", lambda *args: {})
    monkeypatch.setattr(cli.subprocess, "check_output", lambda *args, **kwargs: "")
    monkeypatch.setattr(h.continuous.inputs, "_asset", lambda *args: request)
    monkeypatch.setattr(h, "load_inputs", lambda *args: (original, {}, signals))

    def build(*args):
        if fault == "facts":
            raise RuntimeError("injected facts failure")
        return facts

    monkeypatch.setattr(h, "build_facts", build)
    spawned = []

    class Process:
        def __init__(self, command, **kwargs):
            if fault == "second_spawn" and spawned:
                raise OSError("injected second spawn failure")
            item = cli.repeat(request, original, signals, facts, "a" * 40, {})
            if fault == "child_identity":
                item = receipt({**{k: v for k, v in item.items() if k != "receipt_sha256"}, "facts_sha256": "0" * 64})
            cli.write_once(cli.Path(command[command.index("--output") + 1]), item)
            spawned.append(self)

        def wait(self):
            return 0

    monkeypatch.setattr(cli.subprocess, "Popen", Process)
    output = tmp_path / "run"
    assert (
        cli.main(
            ["run", "--request", str(tmp_path / "request.json"), "--request-sha256", "0" * 64, "--output", str(output)]
        )
        == 1
    )
    failed = cli.read_json(tmp_path / "run.parent.failure.json")
    assert failed["completed_children"] == expected
    assert not (output / "acceptance.json").exists()
