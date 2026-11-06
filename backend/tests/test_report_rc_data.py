import pytest

from backend.services.report_rc_data import (
    CONTRACT,
    collect_day,
    normalize_rows,
    digest,
    validate_page,
)


def row(eps=1.0, quarter="2025Q4", **kw):
    value = {name: None for name in CONTRACT["fields"]}
    value.update(
        ts_code="000001.SZ",
        org_name="测试机构",
        report_date="20240704",
        report_title="报告",
        quarter=quarter,
        eps=eps,
        create_time="2024-07-05 12:00:00",
    )
    value.update(kw)
    return value


def test_schema_and_errors_do_not_become_empty():
    with pytest.raises(ValueError):
        validate_page({"code": 1, "data": None})
    with pytest.raises(ValueError):
        validate_page({"code": 0, "data": {"fields": ["eps"], "items": [[1]]}})
    assert validate_page({"code": 0, "data": {"fields": list(CONTRACT["fields"]), "items": []}}) == []


def test_full_page_continue_and_resume_without_refetch(tmp_path):
    calls = []

    def query(params):
        calls.append(params["offset"])
        return [row(eps=i + 1) for i in range(3)] if params["offset"] == 0 else [row(5)]

    result = collect_day(tmp_path, "batch", "2024-07-04", query, page_size=3)
    assert calls == [0, 3] and result["rows"] == 4
    collect_day(tmp_path, "batch", "2024-07-04", lambda _: pytest.fail("refetch"), page_size=3)


def test_repeated_full_page_fails_closed(tmp_path):
    with pytest.raises(ValueError, match="repeated"):
        collect_day(tmp_path, "batch", "2024-07-04", lambda _: [row()], page_size=1)
    assert not (tmp_path / "days" / "2024-07-04.json").exists()


def test_failed_page_resume_and_empty_distinction(tmp_path):
    with pytest.raises(RuntimeError):
        collect_day(
            tmp_path, "batch", "2024-07-04", lambda _: (_ for _ in ()).throw(RuntimeError("offline")), retries=1
        )
    assert not (tmp_path / "days" / "2024-07-04.json").exists()
    result = collect_day(tmp_path, "batch", "2024-07-04", lambda _: [], retries=1)
    assert result["status"] == "SUCCESS_EMPTY" and result["rows"] == 0


def test_raw_duplicates_quarters_conflicts_preserved():
    data = [row(), row(), row(2), row(3, "2026Q4")]
    records = []
    for n, r in enumerate(data):
        records.append(
            {
                "row": r,
                "capture_id": "cap",
                "row_ordinal": n,
                "captured_at": "2026-10-23T10:00:00+00:00",
                "batch_id": "batch",
            }
        )
    panel = normalize_rows(records)
    assert len(panel) == 4
    assert panel["quarter"].tolist() == ["2025Q4"] * 3 + ["2026Q4"]
    assert panel["provider_report_id"].isna().all()
    assert set(panel["vintage_status"]) == {"NON_VINTAGE"}
    assert panel["publication_available_at"].isna().all()
    assert panel["duplicate_source_row"].sum() == 2
    assert panel["same_report_quarter_conflict"].sum() == 3
    assert panel["eps_basis_status"].eq("UNKNOWN").all()


def test_invalid_eps_not_zero_and_hash_stable():
    records = [
        {
            "row": row(float("nan")),
            "capture_id": "cap",
            "row_ordinal": 0,
            "captured_at": "2026-10-23T10:00:00+00:00",
            "batch_id": "batch",
        }
    ]
    panel = normalize_rows(records)
    assert panel["eps"].isna().all()
    assert panel["eps_quality"].eq("NON_FINITE_OR_MISSING").all()
    assert digest({"b": 1, "a": 2}) == digest({"a": 2, "b": 1})


def test_wrong_report_date_does_not_complete_day(tmp_path):
    with pytest.raises(ValueError, match="date"):
        collect_day(tmp_path, "batch", "2024-07-04", lambda _: [row(report_date="20240705")])
    assert not (tmp_path / "days" / "2024-07-04.json").exists()


def test_report_date_pair_never_crosses_quarter_or_claims_asof():
    from backend.services.report_rc_data_delivery import add_prior_context

    data = [row(1), row(2, report_date="20240705"), row(3, "2026Q4", report_date="20240705")]
    records = [
        {
            "row": r,
            "capture_id": "cap",
            "row_ordinal": n,
            "captured_at": "2026-10-23T10:00:00+00:00",
            "batch_id": "batch",
        }
        for n, r in enumerate(data)
    ]
    panel = add_prior_context(normalize_rows(records))
    assert panel.loc[1, "prior_eps"] == 1
    assert panel.loc[1, "within_snapshot_pair_status"] == "CHANGED_VALUE_BASIS_UNKNOWN"
    assert panel.loc[2, "within_snapshot_pair_status"] == "NO_PRIOR_IN_CAPTURE_WINDOW"
    assert panel["pair_time_semantics"].eq("REPORT_DATE_SEQUENCE_NOT_ASOF_VALIDATED").all()


def test_collection_resume_detects_hash_or_contract_drift(tmp_path):
    collect_day(tmp_path, "batch", "2024-07-04", lambda _: [row()])
    with pytest.raises(ValueError, match="contract"):
        collect_day(tmp_path, "other", "2024-07-04", lambda _: [])
    page = next((tmp_path / "pages").rglob("*.json"))
    page.write_bytes(b"{}")
    with pytest.raises(ValueError, match="hash"):
        collect_day(tmp_path, "batch", "2024-07-04", lambda _: [])


def test_exclusive_artifact_creation(tmp_path):
    from backend.services.report_rc_data import create_json

    p = tmp_path / "artifact.json"
    create_json(p, {"a": 1})
    with pytest.raises(FileExistsError):
        create_json(p, {"a": 2})
    assert p.read_text().strip() == '{"a": 1}'


def test_conflicting_previous_date_not_arbitrarily_selected():
    from backend.services.report_rc_data_delivery import add_prior_context

    data = [row(1), row(2), row(3, report_date="20240705")]
    records = [
        {
            "row": r,
            "capture_id": "cap",
            "row_ordinal": n,
            "captured_at": "2026-10-23T10:00:00+00:00",
            "batch_id": "batch",
        }
        for n, r in enumerate(data)
    ]
    panel = add_prior_context(normalize_rows(records))
    assert panel.iloc[2]["within_snapshot_pair_status"] == "PRIOR_DATE_CONFLICT"
    assert panel.iloc[2]["prior_eps"] != panel.iloc[2]["prior_eps"]


def test_real_file_export_and_identity_projection(tmp_path, monkeypatch):
    import pandas as pd
    from backend.services.report_rc_data_delivery import export_delivery, verify_delivery

    monkeypatch.setitem(CONTRACT, "start_date", "2024-07-04")
    monkeypatch.setitem(CONTRACT, "end_date", "2024-07-05")
    source = tmp_path / "capture"
    collect_day(source, "batch", "2024-07-04", lambda _: [row()])
    collect_day(source, "batch", "2024-07-05", lambda _: [row(2, report_date="20240705")])
    identity = tmp_path / "identity.parquet"
    pd.DataFrame(
        [
            dict(
                package_id="pkg",
                manifest_sha256="sha",
                run_id="run",
                decision_as_of_trade_date="2024-07-05",
                target_trade_date="2024-07-08",
                instrument="SZ000001",
                ret_5="DO_NOT_READ",
            )
        ]
    ).to_parquet(identity, index=False)
    destination = tmp_path / "delivery"
    result = export_delivery(source, destination, population_identity=identity)
    assert result["quality"]["population_input_coverage"]["outcome_columns_read"] is False
    assert verify_delivery(destination)["panel_rows"] == 2
    population = pd.read_parquet(destination / "original_population_input_coverage.parquet")
    assert "ret_5" not in population and population["has_any_report_in_capture"].all()
    (destination / "quality.json").write_bytes(b"{}")
    with pytest.raises(ValueError, match="identity"):
        verify_delivery(destination)


def test_dev_writer_rejects_production_before_dml(tmp_path):
    from backend.services.report_rc_data import persist_dev

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, sql):
            assert sql == "SELECT current_database()"

        def fetchone(self):
            return ("aistock",)

    class Conn:
        def cursor(self):
            return Cursor()

    with pytest.raises(ValueError, match="target"):
        persist_dev(Conn(), tmp_path)


def test_missing_and_nonstandard_quarter_stay_unknown_not_normal_pairs():
    from backend.services.report_rc_data_delivery import add_prior_context

    records = [
        {
            "row": row(1, quarter),
            "capture_id": "cap",
            "row_ordinal": n,
            "captured_at": "2026-10-23T10:00:00+00:00",
            "batch_id": "batch",
        }
        for n, quarter in enumerate([None, "2024Q5"])
    ]
    panel = add_prior_context(normalize_rows(records))
    assert len(panel) == 2
    assert panel["forecast_period_quality"].eq("UNKNOWN_MISSING_OR_NONSTANDARD").all()
    assert panel["within_snapshot_pair_status"].eq("FORECAST_PERIOD_UNKNOWN").all()
    assert panel["prior_eps"].isna().all()
