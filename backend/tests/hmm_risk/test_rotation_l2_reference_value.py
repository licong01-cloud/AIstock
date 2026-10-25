"""Direct zero-fit value invariants; synthetic prices do not prove model value."""

from copy import deepcopy
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts.hmm_risk import rotation_l2_reference_value as subject
from backend.tests.hmm_risk.test_rotation_l2_moneyflow_supervised import _calendar
from scripts.hmm_risk import run_rotation_l2_reference_value as cli


@pytest.fixture
def ledger():
    days = [(date(2025, 4, 16) + timedelta(days=i)).isoformat() for i in range(25)]
    decisions = days[:-10]
    groups = {d: ["A"] for d in decisions}
    quotes = {(d, c): 0.0 for d in days for c in ("A", "B", "Z")}
    return days, decisions, groups, quotes


@pytest.mark.parametrize("bps", subject.COSTS)
def test_cost_formula_complete_liquidation_and_self_financing(ledger, bps):
    days, decisions, groups, quotes = ledger
    rows = subject.replay_path(days, decisions, groups, quotes, cost_bps=bps)
    ratio = (1 - bps / 10000) / (1 + bps / 10000)
    assert rows[-1]["nav"] == pytest.approx(0.5 * ratio**2 + 0.5 * ratio, abs=1e-14)
    assert rows[0]["buy_notional"] == pytest.approx(0.1 / (1 + bps / 10000))
    assert rows[10]["sell_notional"] == pytest.approx(0.1 / (1 + bps / 10000))
    assert rows[10]["buy_notional"] == pytest.approx(0.1 * ratio / (1 + bps / 10000))
    assert rows[-1]["exposure"] == 0 and rows[-1]["cash"] == rows[-1]["nav"]
    assert all(r["entry_count"] == 0 for r in rows[len(decisions) :])
    assert [r["entry_cohort"] for r in rows[:15]] == list(range(10)) + list(range(5))


def test_first_return_is_after_entry_and_tenth_return_precedes_exit(ledger):
    days, decisions, groups, quotes = ledger
    groups = {d: ["A"] if d == days[0] else [] for d in decisions}
    for d in days:
        quotes[d, "A"] = 0.01
    quotes[days[0], "A"] = 3.0  # Already realized before reference entry: must not be earned.
    rows = subject.replay_path(days, decisions, groups, quotes, cost_bps=0)
    assert rows[0]["nav"] == 1
    assert rows[10]["nav"] == pytest.approx(0.9 + 0.1 * 1.01**10)
    assert rows[-1]["nav"] == rows[10]["nav"]


def test_fixed_shares_are_not_daily_equal_weight_rebalanced(ledger):
    days, decisions, _, quotes = ledger
    groups = {d: ["A", "B"] if d == days[0] else [] for d in decisions}
    for d in days:
        quotes[d, "A"], quotes[d, "B"] = 0.1, -0.1
    rows = subject.replay_path(days, decisions, groups, quotes, cost_bps=0)
    assert rows[1]["nav"] == pytest.approx(1)
    assert rows[2]["nav"] == pytest.approx(1.001)
    assert rows[10]["cash"] == pytest.approx(0.9 + 0.05 * (1.1**10 + 0.9**10))


def test_unheld_legal_na_does_not_block_while_held_na_is_never_stitched(ledger):
    days, decisions, groups, quotes = ledger
    for d in days:
        quotes[d, "Z"] = None
    valid = subject.replay_path(days, decisions, groups, quotes, cost_bps=0)
    assert all(r["valuation_status"] == "AVAILABLE" for r in valid)
    quotes[days[3], "A"] = None
    unavailable = subject.replay_path(days, decisions, groups, quotes, cost_bps=0)
    assert all(r["nav"] is None for r in unavailable[3:])
    assert unavailable[3]["legal_held_quote_na"]
    summary = subject.summarize(unavailable)
    assert summary["status"] == "INSUFFICIENT_REFERENCE_PATH"
    assert summary["cumulative_return"] is summary["max_drawdown"] is None
    assert summary["unavailable_dates"] == 22
    assert summary["continuous_valued_blocks"] == [
        {
            "start": days[0],
            "end": days[2],
            "dates": 3,
            "nav_start": 1.0,
            "nav_end": 1.0,
        }
    ]


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), True, -1.0, -1.1])
def test_held_illegal_quote_explicitly_fails(ledger, bad):
    days, decisions, groups, quotes = ledger
    quotes[days[1], "A"] = bad
    with pytest.raises(subject.baseline.RotationL2Error, match="nonfinite/impossible"):
        subject.replay_path(days, decisions, groups, quotes, cost_bps=0)


def test_quote_missing_calendar_drift_cost_search_and_duplicate_group_fail(ledger):
    days, decisions, groups, quotes = ledger
    invalid = deepcopy(quotes)
    del invalid[days[1], "A"]
    with pytest.raises(subject.baseline.RotationL2Error, match="absent"):
        subject.replay_path(days, decisions, groups, invalid, cost_bps=0)
    with pytest.raises(subject.baseline.RotationL2Error, match="calendar"):
        subject.replay_path(list(reversed(days)), decisions, groups, quotes, cost_bps=0)
    with pytest.raises(subject.baseline.RotationL2Error, match="unapproved cost"):
        subject.replay_path(days, decisions, groups, quotes, cost_bps=3)
    groups[days[0]] = ["A", "A"]
    with pytest.raises(subject.baseline.RotationL2Error, match="canonical unique"):
        subject.replay_path(days, decisions, groups, quotes, cost_bps=0)


def test_paired_hac_keeps_calendar_gaps_and_fixed_blocks(ledger):
    days, decisions, groups, quotes = ledger
    first = subject.replay_path(days, decisions, groups, quotes, cost_bps=0)
    second = subject.replay_path(days, decisions, groups, quotes, cost_bps=5)
    first[3]["daily_return"] = None
    result = subject.paired(days, first, second)
    calendar = [date.fromisoformat(d) for d in days]
    differences = {
        calendar[i]: a["daily_return"] - b["daily_return"]
        for i, (a, b) in enumerate(zip(first, second, strict=True))
        if a["daily_return"] is not None
    }
    assert result["hac"] == subject.baseline._newey_west(calendar, differences, lag=9)
    assert result["paired_dates"] == 24 and result["unavailable_dates"] == 1
    assert set(result["blocks"]) == {n for n, _, _ in subject.ridge.BLOCKS}


@pytest.fixture
def frozen_reader(monkeypatch):
    days = [d.isoformat() for d in _calendar() if d >= subject.ridge.PREDICTION_START]
    codes = [f"801{i:03d}.SI" for i in range(131)]
    identity = {"mapping_hash": "m", "quote_authority_hash": "q"}
    original = {
        "input_hash": subject.return_model.ORIGINAL_INPUT_HASH,
        "source": {
            "identity": identity,
            "sector_returns": [
                {"trade_date": d, "sector_code": c, "quote_available": i > 0, "pct_change": 1.0 if i else None}
                for d in days
                for i, c in enumerate(codes)
            ],
        },
    }
    rows = [
        {
            "trade_date": d,
            "sector_code": c,
            "availability": "available" if i else "unavailable",
            "forecast_state": "trending" if i > 113 else "neutral",
            "rotation_score": 0.1 if i else None,
            "outcome_status": "outcome_available",
        }
        for d in days
        for i, c in enumerate(codes)
    ]
    rank = {
        "predictions": rows,
        "input_identity": identity,
        "mapping_hash": "m",
        "quote_authority_hash": "q",
        "baseline_metrics": {"test": "sealed"},
    }
    candidate = deepcopy(rank)
    files = {"input.json": original, "rank.json": rank, "return.json": candidate}
    monkeypatch.setattr(subject, "_sealed_file", lambda p, expected_byte=None: files[p.name])
    monkeypatch.setattr(
        subject.rank_model,
        "validate_input",
        lambda b: {"identity": identity, "catalog_codes": codes, "calendar": [date.fromisoformat(d) for d in days]},
    )
    monkeypatch.setattr(subject.rank_model, "_reference", lambda *a, **kw: None)
    monkeypatch.setattr(
        subject.ridge, "feature_rows", lambda b: (deepcopy(rows), [], [date.fromisoformat(d) for d in days])
    )
    monkeypatch.setattr(subject.return_model, "_evaluate", lambda *a: {"metrics": rank["baseline_metrics"]})

    def quote_facts(bundle):
        return subject.ridge.seal({"sector_returns": bundle["source"]["sector_returns"]}, "outcome_sha256")

    monkeypatch.setattr(subject.rank_model, "read_evaluation_facts", quote_facts)
    monkeypatch.setitem(subject.RANK_PINS, "outcome_sha256", quote_facts(original)["outcome_sha256"])
    return files, days, codes


def load():
    return subject.load_frozen(Path("input.json"), Path("rank.json"), Path("return.json"))


def test_frozen_reader_full_denominator_native_group_and_pct_units(frozen_reader):
    _, days, _ = frozen_reader
    pins, calendar, groups, quotes = load()
    assert calendar == days and len(days) == 232 and pins["sector_count"] == 131
    assert len(groups["no_order"]) == 222
    assert len(groups["no_order"][days[0]]) == 130
    assert len(groups["rank"][days[0]]) == 17  # Preserve original group, never UI top 10.
    assert quotes[days[-1], "801001.SI"] == 0.01  # Label quote view includes final valuation.
    assert quotes[days[-1], "801000.SI"] is None


@pytest.mark.parametrize(
    "drift",
    ["input", "mapping", "quote_authority", "duplicate", "unknown_sector", "missing", "outcome", "nonfinite", "tail"],
)
def test_reader_drift_fails_closed(frozen_reader, drift):
    files, days, codes = frozen_reader
    if drift == "input":
        files["input.json"]["input_hash"] = "wrong"
    elif drift in {"mapping", "quote_authority"}:
        files["return.json"][drift + "_hash"] = "wrong"
    elif drift == "duplicate":
        files["rank.json"]["predictions"].append(files["rank.json"]["predictions"][1])
        files["return.json"]["predictions"].append(files["return.json"]["predictions"][1])
    elif drift == "unknown_sector":
        for key in ("rank.json", "return.json"):
            files[key]["predictions"][1]["sector_code"] = "L1_NOT_L2"
    elif drift == "missing":
        files["input.json"]["source"]["sector_returns"].pop()
    elif drift == "outcome":
        files["return.json"]["predictions"][1]["relative_return_10d"] = 123
    elif drift == "nonfinite":
        files["input.json"]["source"]["sector_returns"][1]["pct_change"] = float("nan")
    elif drift == "tail":
        files["input.json"]["source"]["sector_returns"].append({"trade_date": "2026-04-01", "sector_code": codes[1]})
    with pytest.raises(subject.baseline.RotationL2Error):
        load()


def test_all_four_costs_and_arms_without_fit_predict_filter(frozen_reader, monkeypatch):
    for api in (subject.ridge, subject.rank_model, subject.return_model):
        for name in ("run_process", "training_matrix", "predictions_from_parameters"):
            monkeypatch.setattr(api, name, cli.forbidden)
    result = subject.execute(
        input_path=Path("input.json"),
        rank_path=Path("rank.json"),
        return_path=Path("return.json"),
        executor_commit="a" * 40,
    )
    assert result["new_fits"] == result["new_predict_calls"] == result["new_filter_calls"] == 0
    assert set(result["cost_paths"]) == {"0", "5", "10", "20"}
    for cost in result["cost_paths"].values():
        assert set(cost["arms"]) == set(subject.ARMS) and len(cost["paired"]) == 5
    assert result["qe_net_value_status"] == "UNASSESSED" and result["production_adoption"] is False


def test_sealed_file_byte_drift_and_relative_path_rejected(tmp_path):
    path = tmp_path / "input.json"
    path.write_text('{"x":1}', encoding="utf8")
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    assert subject._sealed_file(path, expected) == {"x": 1}
    path.write_text('{"x":2}', encoding="utf8")
    with pytest.raises(subject.baseline.RotationL2Error, match="byte SHA"):
        subject._sealed_file(path, expected)
    with pytest.raises(subject.baseline.RotationL2Error, match="ordinary absolute"):
        subject._sealed_file(Path("relative.json"))


def test_fresh_process_import_guards_do_not_allow_estimator_or_database():
    root = Path(__file__).resolve().parents[3]
    code = "from scripts.hmm_risk.run_rotation_l2_reference_value import install_zero_compute_guards; install_zero_compute_guards(); import sklearn"
    result = subprocess.run([sys.executable, "-c", code], cwd=root, capture_output=True, text=True)
    assert result.returncode != 0 and "zero-fit/file-only guard: sklearn" in result.stderr
    result = subprocess.run(
        [sys.executable, "-c", code.replace("import sklearn", "from backend.db.pg_pool import get_conn; get_conn()")],
        cwd=root,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0 and "fit/filter/predict/database/network is forbidden" in result.stderr


def test_cli_child_failure_is_durable_without_duplicating_success(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli, "source_head", lambda: "a" * 40)
    monkeypatch.setattr(cli, "require_clean_source", lambda: None)
    monkeypatch.setattr(
        cli.subprocess, "run", lambda *a, **kw: subprocess.CompletedProcess([], 1, b"", b"quote failure")
    )
    output = tmp_path / "new_run"
    code = cli.main(
        [
            "run",
            "--input",
            str(tmp_path / "i"),
            "--rank-acceptance",
            str(tmp_path / "r"),
            "--return-acceptance",
            str(tmp_path / "c"),
            "--executor-commit",
            "a" * 40,
            "--output",
            str(output),
        ]
    )
    assert code == 1 and not output.exists()
    failure = json.loads(output.with_name(output.name + ".failure.json").read_text(encoding="utf8"))
    assert failure["execution_status"] == "FAILED" and failure["reason_code"].endswith("execution_failed")
    assert "quote failure" in capsys.readouterr().err


@pytest.mark.parametrize(
    "problem", ["success", "repeat_mismatch", "write_failure", "readback_failure", "incomplete_child"]
)
def test_parent_finalization_failures_never_report_success(frozen_reader, tmp_path, monkeypatch, capsys, problem):
    result = subject.execute(
        input_path=Path("input.json"),
        rank_path=Path("rank.json"),
        return_path=Path("return.json"),
        executor_commit="a" * 40,
    )
    result = subject.ridge.seal(
        {**{k: v for k, v in result.items() if k != "result_sha256"}, "zero_compute_poison_active": True},
        "result_sha256",
    )
    monkeypatch.setattr(cli, "source_head", lambda: "a" * 40)
    monkeypatch.setattr(cli, "require_clean_source", lambda: None)
    calls = []

    def child(*args, **kwargs):
        payload = deepcopy(result)
        if problem == "repeat_mismatch" and calls:
            payload["limitations"].append("changed")
        if problem == "incomplete_child":
            del payload["cost_paths"]["20"]
        payload = subject.ridge.seal({k: v for k, v in payload.items() if k != "result_sha256"}, "result_sha256")
        calls.append(1)
        return subprocess.CompletedProcess([], 0, cli.canonical_json_bytes(payload), b"")

    monkeypatch.setattr(cli.subprocess, "run", child)
    original_write = cli.write_once

    def write(path, body):
        if problem == "write_failure" and path.name == "acceptance.json":
            raise OSError("cannot write result")
        original_write(path, body)

    monkeypatch.setattr(cli, "write_once", write)
    if problem == "readback_failure":
        monkeypatch.setattr(cli, "read_json", lambda p: {"wrong": True})
    output = tmp_path / "run"
    exit_code = cli.main(
        [
            "run",
            "--input",
            str(tmp_path / "i"),
            "--rank-acceptance",
            str(tmp_path / "r"),
            "--return-acceptance",
            str(tmp_path / "c"),
            "--executor-commit",
            "a" * 40,
            "--output",
            str(output),
        ]
    )
    if problem == "success":
        assert exit_code == 0 and len(calls) == 2
        acceptance = json.loads((output / "acceptance.json").read_text(encoding="utf8"))
        subject.ridge.verify(acceptance, "acceptance_sha256")
        assert acceptance["fresh_process_bitwise_equal"] is True
        assert list(output.iterdir()) == [output / "acceptance.json"]
        return
    assert exit_code == 1
    failure = json.loads(output.with_name("run.failure.json").read_text(encoding="utf8"))
    assert failure["execution_status"] == "FAILED"
    subject.ridge.verify(failure, "failure_sha256")
    assert not capsys.readouterr().out


def test_two_real_fresh_process_synthetic_reference_paths_are_bitwise_equal():
    root = Path(__file__).resolve().parents[3]
    code = """
from datetime import date,timedelta
from scripts.hmm_risk.run_rotation_l2_reference_value import install_zero_compute_guards
from scripts.hmm_risk.rotation_l2_reference_value import replay_path
from backend.services.hmm_risk.contracts import canonical_json_bytes
import sys
install_zero_compute_guards()
days=[(date(2025,4,16)+timedelta(days=i)).isoformat() for i in range(25)]
groups={d:['A','B'] for d in days[:-10]}
quotes={(d,c):0.01 if c=='A' else -0.005 for d in days for c in ['A','B']}
sys.stdout.buffer.write(canonical_json_bytes(replay_path(days,days[:-10],groups,quotes,cost_bps=10)))
"""
    children = [
        subprocess.run([sys.executable, "-c", code], cwd=root, capture_output=True, check=True) for _ in range(2)
    ]
    assert children[0].stdout == children[1].stdout
    assert len(json.loads(children[0].stdout)) == 25
