import pandas as pd
import pytest

from backend.services.dataset_release.daily_basic_history_repair import (
    _collect_circ_mv_facts, causal_coverage, merge_missing_facts,
)


def _frame(days, values):
    return pd.DataFrame({"db_circ_mv": pd.Series(values, dtype="float32").to_numpy()},
                        index=pd.MultiIndex.from_product([pd.to_datetime(days), ["000001.SZ"]],
                                                         names=["datetime", "instrument"]))


def test_repair_never_overwrites_existing_values():
    old = _frame(["2024-07-01"], [12])
    source = _frame(["2024-06-28", "2024-07-01"], [10, 999])
    merged, added = merge_missing_facts(old, source)
    assert merged.db_circ_mv.tolist() == [10, 12]
    assert len(added) == 1
    with pytest.raises(ValueError, match="duplicate"):
        merge_missing_facts(old, pd.concat([source, source]))


def test_consumer_causal_windows_include_full_source_product_and_qe_history():
    from backend.services.dataset_release.daily_basic_history_repair import consumer_causal_windows

    windows = consumer_causal_windows("2026-08-31")
    assert windows == {
        "source": ("2020-07-30", "2025-04-30"),
        "train": ("2022-01-01", "2024-06-30"),
        "validation": ("2024-07-01", "2025-03-31"),
        "product_history": ("2020-07-30", "2026-08-31"),
        "qe_coefficients": ("2024-07-01", "2026-08-28"),
    }
    with pytest.raises(ValueError):
        consumer_causal_windows("2025-04-30")


def test_ready_requires_all_declared_windows_not_just_executor_source():
    from backend.services.dataset_release.daily_basic_history_repair import causal_audit_status, consumer_causal_windows

    audit = {name: {"after": {"expected_keys": 1, "strict_prior_resolved": 1, "unresolved_count": 0}}
             for name in consumer_causal_windows("2026-08-31")}
    assert causal_audit_status(audit) == "CANDIDATE_READY"
    audit["product_history"]["after"]["unresolved_count"] = 1
    audit["product_history"]["after"]["strict_prior_resolved"] = 0
    assert causal_audit_status(audit) == "BLOCKED"
    del audit["product_history"]
    with pytest.raises(ValueError):
        causal_audit_status(audit)


@pytest.mark.parametrize("count", [-1, False, None, "0"])
def test_causal_readiness_rejects_invalid_counts_and_empty_denominators(count):
    from backend.services.dataset_release.daily_basic_history_repair import causal_audit_status, consumer_causal_windows

    audit = {name: {"after": {"expected_keys": 1, "strict_prior_resolved": 1, "unresolved_count": count}}
             for name in consumer_causal_windows("2026-08-31")}
    with pytest.raises(ValueError):
        causal_audit_status(audit)
    for item in audit.values():
        item["after"] = {"expected_keys": 0, "strict_prior_resolved": 0, "unresolved_count": 0}
    with pytest.raises(ValueError):
        causal_audit_status(audit)


def test_causal_audit_rejects_same_day_and_does_not_fill_halts():
    spans = [("000001.SZ", "2024-07-01", "2024-07-03")]
    calendar = ["2024-07-01", "2024-07-02", "2024-07-03"]
    facts = {"000001.SZ": [(pd.Timestamp("2024-07-01").value, True)]}
    audit = causal_coverage(spans, calendar, facts, start=calendar[0], end=calendar[-1])
    assert audit["expected_keys"] == 3
    assert audit["strict_prior_resolved"] == 2
    assert audit["unresolved"] == [{"symbol": "000001.SZ", "trade_date": "2024-07-01"}]


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), 0, -1])
def test_latest_invalid_fact_cannot_fall_back_to_older_positive_cap(invalid):
    facts = {}
    _collect_circ_mv_facts(
        _frame(["2024-06-28", "2024-07-01", "2024-07-02"], [10, invalid, 12]),
        facts, window_start="2024-06-28",
    )
    audit = causal_coverage(
        [("000001.SZ", "2024-07-01", "2024-07-03")],
        ["2024-07-01", "2024-07-02", "2024-07-03"],
        facts, start="2024-07-01", end="2024-07-03",
    )
    assert audit["strict_prior_resolved"] == 2
    assert audit["unresolved"] == [{"symbol": "000001.SZ", "trade_date": "2024-07-02"}]


def test_cli_passes_independent_connection_factory_not_live_connection(monkeypatch, tmp_path):
    from scripts import repair_qe_daily_basic_history as cli
    diagnostic = tmp_path / "diagnostic.json"
    diagnostic.write_text("{}")
    args = ["repair"]
    for key in ["baseline", "candidate", "manifest", "generation", "revision", "dotenv"]:
        args.extend([f"--{key}", "unused"])
    args.extend(["--diagnostic", str(diagnostic)])
    monkeypatch.setattr(cli.sys, "argv", args)
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    def factory():
        return object()
    monkeypatch.setattr(cli, "independent_postgres_connection_factory", factory)
    def repair(**kwargs):
        assert kwargs["connection_factory"] is factory
        return {"status": "PASS"}
    monkeypatch.setattr(cli, "repair_daily_basic_history", repair)
    cli.main()
