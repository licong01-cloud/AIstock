from datetime import date

import pytest

from scripts.repair_local_data_daily_sync import readback_audit_contract, run_plan


def test_completed_etf_repair_has_source_bound_publication_readback():
    row = {"ts_code": "510300.SH", "trade_date": date(2026, 10, 13),
           "total_share": 10, "total_size": 20, "nav": 2, "close": 2}
    result = readback_audit_contract("etf_share_size", [row], {row["ts_code"]: row}, "a" * 64)
    assert result["source_sha256"] == "a" * 64
    assert result["readback_row_count"] == 1
    assert result["publication_quality"]["total_size_finite_count"] == 1


@pytest.mark.parametrize("problem", ["missing_key", "fillable_null", "unpublished"])
def test_incomplete_repair_cannot_publish_success(problem):
    source = {"ts_code": "510300.SH", "trade_date": date(2026, 10, 13),
              "total_share": 10, "total_size": 20, "nav": 2, "close": 2}
    actual = dict(source)
    if problem == "missing_key":
        readback = {}
    else:
        actual["total_size"] = None
        if problem == "unpublished":
            source["total_size"] = None
        readback = {actual["ts_code"]: actual}
    with pytest.raises(ValueError):
        readback_audit_contract("etf_share_size", [source], readback, "a" * 64)


def test_real_source_nulls_remain_null_and_existing_finite_values_are_preserved():
    source = {"ts_code": "510300.SH", "trade_date": date(2026, 10, 13),
              "total_share": 10, "total_size": 20, "nav": None, "close": 2}
    actual = {**source, "close": 3}
    result = readback_audit_contract("etf_share_size", [source], {source["ts_code"]: actual}, "a" * 64)
    assert result["readback_row_count"] == 1
    assert actual["nav"] is None and actual["close"] == 3


def test_margin_readback_binds_all_provider_keys_without_changing_old_jobs():
    row = {"ts_code": "000001.SZ", "trade_date": date(2026, 10, 13), "rzye": 10}
    result = readback_audit_contract("margin_detail", [row], {row["ts_code"]: row}, "b" * 64)
    assert result["provider_publication"]["row_count"] == 1
    assert "job_id" not in result


def test_margin_keeps_raw_provider_hash_separate_from_typed_date_readback():
    row = {"ts_code": "000001.SZ", "trade_date": date(2026, 10, 13), "rzye": 10}
    result = readback_audit_contract("margin_detail", [row], {row["ts_code"]: row},
                                    "b" * 64, provider_rows_sha256="c" * 64)
    assert result["provider_publication"]["source_rows_sha256"] == "c" * 64
    assert result["source_rowset_sha256"] != "c" * 64


def test_partial_raw_source_does_not_certify_whole_market():
    assert readback_audit_contract("kline_daily_raw", [], {}, "a" * 64) is None


@pytest.mark.parametrize("problem", ["date", "hash", "duplicate", "wrong_key", "unknown"])
def test_conflicting_source_identity_is_rejected(problem):
    row = {"ts_code": "000001.SZ", "trade_date": date(2026, 10, 13), "rzye": 10}
    rows, actual, table, identity = [row], {row["ts_code"]: row}, "margin_detail", "a" * 64
    if problem == "date":
        rows = [row, {**row, "ts_code": "000002.SZ", "trade_date": date(2026, 10, 12)}]
    elif problem == "hash":
        identity = "z" * 64
    elif problem == "duplicate":
        rows = [row, row]
    elif problem == "wrong_key":
        actual = {row["ts_code"]: {**row, "trade_date": date(2026, 10, 12)}}
    else:
        table = "sector_data"
    with pytest.raises(ValueError):
        readback_audit_contract(table, rows, actual, identity)


def test_cli_still_has_no_production_apply_route():
    with pytest.raises(ValueError, match="does not authorize production"):
        run_plan("unused", "production", {}, apply=True)


@pytest.mark.parametrize("audit_error", [False, True])
def test_dev_repair_commits_fact_and_audit_together_or_neither(monkeypatch, audit_error):
    from scripts import repair_local_data_daily_sync as repair

    row = {"ts_code": "510300.SH", "trade_date": date(2026, 10, 13),
           "total_share": 10, "total_size": 20, "nav": None, "close": 2}
    calls = []

    class Connection:
        def cursor(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, statement):
            calls.append(statement)
            if audit_error:
                raise ValueError("audit readback changed")

        def commit(self):
            calls.append("commit")

        def close(self):
            calls.append("close")

    monkeypatch.setattr(repair, "connect", lambda *_args, **_kwargs: Connection())
    monkeypatch.setattr(repair, "source_groups", lambda _s: iter([("etf_share_size", tuple(row), [row])]))
    monkeypatch.setattr(repair, "existing_rows", lambda *_args: {row["ts_code"]: row})
    monkeypatch.setattr(repair, "guarded_audit_statements", lambda *_args, **_kwargs:
                        (["exact-readback-guard", "audit-upsert"], {"verified": True}))
    source = {"batches": [{"api": "etf_share_size", "request_date": "20261013", "rows_sha256": "a" * 64}]}
    if audit_error:
        with pytest.raises(ValueError, match="audit readback"):
            repair.run_plan("unused", "dev", source, apply=True)
        assert "commit" not in calls
    else:
        result = repair.run_plan("unused", "dev", source, apply=True)
        assert result["results"][0]["refresh_audit_written"]
        assert calls.index("audit-upsert") < calls.index("commit")
    assert calls[-1] == "close"
