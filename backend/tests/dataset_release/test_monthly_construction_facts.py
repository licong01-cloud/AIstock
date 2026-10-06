"""Month-only payloads and real QFQ construction facts, not historical QA."""
from datetime import date
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.source_authority import MonthlySourceAuthority, PRODUCTION_QUERY_SPECS
from backend.services.dataset_release.monthly_source_progress import MonthlyObservedSourceAuthority


@pytest.mark.parametrize("cancel_at_tail", [False, True])
def test_construction_progress_uses_real_durable_source_contract(tmp_path, monkeypatch, cancel_at_tail):
    import hashlib
    import json
    import pandas as pd
    from backend.services.dataset_release import monthly_construction_facts as subject
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseCancelled
    from backend.tests.dataset_release.test_monthly_unified_v2 import _service, _request, Pipeline

    root = tmp_path / "prefix"
    catalog = root / "day/instruments/all.txt"
    catalog.parent.mkdir(parents=True)
    catalog.write_text("000001.SZ\t2018-08-01\t2026-08-31\n", encoding="utf-8")
    factor_path = root / "factor/daily_pv.h5"
    factor_path.parent.mkdir()
    frame = pd.DataFrame({"close": [1.0]}, index=pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2026-08-31"), "000001.SZ")], names=["datetime", "instrument"],
    ))
    frame.to_hdf(factor_path, key="data", format="table", data_columns=True)
    inventory = root / "inventory.json"
    inventory.write_text(json.dumps({"files": [{"path": "factor/daily_pv.h5", "size": factor_path.stat().st_size}]}), encoding="utf-8")
    prefix = SimpleNamespace(root=root, cutoff=date(2026, 8, 31), manifest={"components": {
        "day_meta_export": {"path": "day/meta.json"}, "factor_meta": {"path": "factor/meta.json"},
        "factor_content_manifest": {"path": "inventory.json", "size": inventory.stat().st_size,
                                    "sha256": hashlib.sha256(inventory.read_bytes()).hexdigest()},
    }})
    monkeypatch.setattr(subject, "read_legacy_qfq_anchors", lambda *args, **kwargs: {
        "000001.SZ": {"trade_date": "2026-08-28"},
    })
    service = _service(tmp_path, Pipeline())
    operation = service.submit(_request())["operation_id"]
    service.store.update_state(operation, attempt=1, current_stage="SOURCE", status="CHECKING_SOURCE")
    checkpoint, durable_progress = service._stage_control(operation, attempt=1, stage="SOURCE")
    durable_progress({"phase": "SOURCE_ROWS", "rows_validated": 105498, "rows_sealed": 5498, "partitions_sealed": 3})
    pulses = []

    def progress(value):
        pulses.append(value)
        durable_progress(value)
        if cancel_at_tail and value.get("partition_key") == "daily_pv":
            service.cancel(operation)

    if cancel_at_tail:
        with pytest.raises(MonthlyReleaseCancelled):
            subject.collect_qfq_construction_anchors(prefix, month_start=date(2026, 9, 1),
                instruments=("000001.SZ",), checkpoint=checkpoint, progress=progress)
    else:
        anchors = subject.collect_qfq_construction_anchors(prefix, month_start=date(2026, 9, 1),
            instruments=("000001.SZ",), checkpoint=checkpoint, progress=progress)
        assert anchors == (("000001.SZ", date(2026, 8, 28)), ("000001.SZ", date(2026, 8, 31)))
    assert {pulse["partition_key"] for pulse in pulses} == {"daily_bin", "minute_bin", "daily_pv"}
    assert all(set(pulse) == {"phase", "query_id", "partition_key"} for pulse in pulses)
    assert all(pulse["query_id"] == "adj_factor_construction" for pulse in pulses)
    state = service.status(operation)
    assert state["stage_progress"]["rows_validated"] == 105498
    assert state["stage_progress"]["rows_sealed"] == 5498
    assert state["stage_progress"]["partitions_sealed"] == 3
    assert not any(state["checkpoints"].values())


def _authority(*, monthly, month_start=date(2026, 9, 1)):
    profile = SimpleNamespace(
        profile="fixture", start_date=date(2018, 8, 1), minute_start_date=date(2024, 7, 1),
        source_date_chunk_months=3, minute_code_bucket_count=128, minute_code_bucket_capacity=100,
        minute_partition_schema_version="fixture", index_codes=("000300.SH",),
        indices=(SimpleNamespace(daily_code="000300.SH", required_from=date(2018, 8, 1)),),
    )
    if monthly:
        return MonthlyObservedSourceAuthority(profile, None, progress=lambda _: None,
            month_start=month_start, construction_anchors=(("000001.SZ", month_start - date.resolution),))
    return MonthlySourceAuthority(profile, None)


def test_month_source_requests_never_iterate_old_quote_partitions():
    pit = SimpleNamespace(spans=[SimpleNamespace(ts_code="000001.SZ")])
    authority = _authority(monthly=True)
    assert authority._refresh_audit_start(date(2026, 9, 30)) == date(2026, 9, 1)
    assert _authority(monthly=False)._refresh_audit_start(date(2026, 9, 30)) == date(2018, 8, 1)
    for name in ("kline_daily_raw", "kline_minute_raw", "adj_factor", "daily_basic", "index_daily"):
        requests = list(authority._partition_requests(PRODUCTION_QUERY_SPECS[name], date(2026, 9, 30), pit_snapshot=pit))
        assert requests
        assert all(params["start"] == date(2026, 9, 1) and params["end"] == date(2026, 9, 30) for _, params in requests)
    # PIT is independent metadata, not a fabricated current snapshot.
    _, params = next(authority._partition_requests(PRODUCTION_QUERY_SPECS["index_membership_pit"], date(2026, 9, 30), pit_snapshot=pit))
    assert params["start"] == date(2018, 8, 1)


def test_construction_query_is_bounded_real_maximum_and_exact_anchors_not_default_full_export():
    query = PRODUCTION_QUERY_SPECS["adj_factor_construction"]
    assert query not in _authority(monthly=False)._database_query_specs()
    authority = _authority(monthly=True)
    assert query in authority._database_query_specs()
    pit = SimpleNamespace(spans=[SimpleNamespace(ts_code="000001.SZ")])
    key, params = next(authority._partition_requests(query, date(2026, 9, 30), pit_snapshot=pit))
    assert key == "construction-2026-09-30"
    assert params["cutoff"] == date(2026, 9, 30)
    assert params["codes"] == ["000001.SZ"]
    assert params["anchors_json"] == '[{"trade_date":"2026-08-31","ts_code":"000001.SZ"}]'
    assert "ORDER BY fact.adj_factor DESC NULLS LAST, fact.trade_date DESC LIMIT 1" in query.sql
    assert "fact.trade_date <= %(cutoff)s" in query.sql
    assert "jsonb_to_recordset(%(anchors_json)s::jsonb)" in query.sql
    assert "UNION" in query.sql


def test_month_moneyflow_resolves_shared_source_alias_without_expanding_stock_pool():
    pit = SimpleNamespace(spans=[SimpleNamespace(ts_code="302132.SZ")])
    authority = _authority(monthly=True, month_start=date(2024, 9, 1))
    requests = list(authority._partition_requests(PRODUCTION_QUERY_SPECS["moneyflow_ts"], date(2024, 9, 30), pit_snapshot=pit))
    assert requests[0][1]["codes"] == ["300114.SZ", "302132.SZ"]
    daily = list(authority._partition_requests(PRODUCTION_QUERY_SPECS["daily_basic"], date(2024, 9, 30), pit_snapshot=pit))
    assert daily[0][1]["codes"] == ["302132.SZ"]
    assert [span.ts_code for span in pit.spans] == ["302132.SZ"]


def test_build_reader_selects_real_boundary_facts_and_keeps_factor_month_backing_distinct():
    from backend.services.dataset_release.artifact_ready_build_source import ArtifactReadyBuildSource
    from backend.services.dataset_release.contracts import Component

    entries = [{"identity": f"{dataset}:{key}", "dataset": dataset, "partition_key": key,
                "role": "sealed_database_source", "row_count": 1,
                "content_digest": "a" * 64, "schema_digest": "b" * 64, "rows_ref": {}}
               for dataset, key in (("adj_factor", "2026-09-01_2026-09-30"),
                                    ("adj_factor_construction", "construction-2026-09-30"))]
    rows = {"adj_factor": [{"ts_code": "000001.SZ", "trade_date": "2026-09-01", "adj_factor": 3.0}],
            "adj_factor_construction": [{"ts_code": "000001.SZ", "trade_date": "2026-08-31", "adj_factor": 2.0}]}
    visited = []

    class Reader:
        def iter_rows(self, dataset, key, **kwargs):
            visited.append(dataset)
            return iter(rows[dataset])

    source = ArtifactReadyBuildSource.__new__(ArtifactReadyBuildSource)
    source.component_manifests = {Component.FACTOR_H5_STATIC: {"partitions": entries}}
    source._descriptors = {entry["identity"]: entry for entry in entries}
    source._reader = Reader()
    source._effective_adj_rows = lambda component, descriptor, values: values
    boundary = source.ordered_partitions(Component.FACTOR_H5_STATIC, "adj_factor",
        date_ranges=((date(2026, 8, 31), date(2026, 8, 31)),), instruments=("000001.SZ",))
    assert [row for partition in boundary for row in partition.rows] == rows["adj_factor_construction"]
    assert visited == ["adj_factor_construction"]
    month = source.ordered_partitions(Component.FACTOR_H5_STATIC, "adj_factor",
        date_ranges=((date(2026, 9, 1), date(2026, 9, 30)),), instruments=("000001.SZ",))
    assert [row for partition in month for row in partition.rows] == rows["adj_factor"]


@pytest.mark.parametrize("defect", [None, "different_max", "duplicate", "missing_dead_code"])
def test_month_qfq_uses_real_cutoff_max_and_boundary_without_old_daily_prices(tmp_path, defect):
    from backend.services.dataset_release.artifact_ready_source import ArtifactReadySourceBuilder, ArtifactReadyCoverageIncomplete
    from backend.services.dataset_release.cas_store import CASStore
    from backend.services.dataset_release.control_store import ControlStore
    from backend.services.dataset_release.canonical_stock_transformer import qfq_denominator_authority_from_mapping

    month_key = "2026-09-01_2026-09-30"
    rows = {
        "adj_factor": [{"ts_code": "000001.SZ", "trade_date": "2026-09-01", "adj_factor": 3.0}],
        "adj_factor_construction": [
            {"ts_code": "000001.SZ", "trade_date": "2026-08-28", "adj_factor": 2.0},
            {"ts_code": "000001.SZ", "trade_date": "2026-09-01", "adj_factor": 3.0},
            {"ts_code": "000002.SZ", "trade_date": "2025-06-01", "adj_factor": 5.0},
        ],
        "kline_daily_raw": [{"ts_code": "000001.SZ", "trade_date": "2026-09-01"}],
        "trading_calendar": [{"cal_date": "2026-08-31", "is_trading": True}, {"cal_date": "2026-09-01", "is_trading": True}],
    }
    if defect == "different_max":
        rows["adj_factor_construction"][1]["adj_factor"] = 4.0
    elif defect == "duplicate":
        rows["adj_factor_construction"].append(dict(rows["adj_factor_construction"][0]))
    elif defect == "missing_dead_code":
        rows["adj_factor_construction"].pop()

    class View:
        def descriptors(self, dataset):
            return [{"dataset": dataset, "partition_key": month_key if dataset != "adj_factor_construction" else "construction-2026-09-30",
                     "content_digest": "a" * 64, "schema_digest": "b" * 64}]

        def iter_partition_rows(self, descriptor):
            return iter(rows[descriptor["dataset"]])

    profile = SimpleNamespace(start_date=date(2018, 8, 1))
    cas = CASStore(ControlStore.initialize(tmp_path / "control").root)
    builder = ArtifactReadySourceBuilder(profile, cas, month_start=date(2026, 9, 1))
    snapshot = SimpleNamespace(official_cutoff=date(2026, 9, 30), pit_snapshot_digest="c" * 64,
                               pit_snapshot=SimpleNamespace(spans=[SimpleNamespace(ts_code=code) for code in ("000001.SZ", "000002.SZ")]))
    if defect:
        with pytest.raises(ArtifactReadyCoverageIncomplete):
            builder._adj_factor_entries(View(), snapshot=snapshot, checkpoint=lambda: None)
        return
    _, provider, _, summary, ref = builder._adj_factor_entries(View(), snapshot=snapshot, checkpoint=lambda: None)
    authority = qfq_denominator_authority_from_mapping(cas.get_json(ref), expected_cutoff=snapshot.official_cutoff,
                                                      expected_pit_spans_sha256=snapshot.pit_snapshot_digest)
    assert authority.by_code == {"000001.SZ": 3.0, "000002.SZ": 5.0}
    assert authority.source_row_count == 3  # Genuine selected facts, not claimed historical row count.
    assert summary["source_rows_scope"] == "month_and_authoritative_max_boundary_facts_v1"
    assert summary["historical_business_audit_performed"] is False
    assert provider == ()
    assert builder._trading_dates(View(), snapshot.official_cutoff) == (date(2026, 9, 1),)
