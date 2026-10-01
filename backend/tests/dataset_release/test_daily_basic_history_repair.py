import pandas as pd
import pytest

from backend.services.dataset_release.daily_basic_history_repair import causal_coverage, merge_missing_facts


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


def test_causal_audit_rejects_same_day_and_does_not_fill_halts():
    spans = [("000001.SZ", "2024-07-01", "2024-07-03")]
    calendar = ["2024-07-01", "2024-07-02", "2024-07-03"]
    facts = {"000001.SZ": [pd.Timestamp("2024-07-01").value]}
    audit = causal_coverage(spans, calendar, facts, start=calendar[0], end=calendar[-1])
    assert audit["expected_keys"] == 3
    assert audit["strict_prior_resolved"] == 2
    assert audit["unresolved"] == [{"symbol": "000001.SZ", "trade_date": "2024-07-01"}]


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
