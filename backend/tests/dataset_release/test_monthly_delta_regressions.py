"""Monthly bounds/diagnostics: no real datasets, database or worker process."""

from types import SimpleNamespace

import pytest

from backend.services.dataset_release.monthly_error_diagnostics import monthly_error_diagnostics, monthly_error_message
from backend.services.dataset_release.monthly_snapshot import MonthlySnapshotCoordinator, MonthlySnapshotError
from backend.services.dataset_release.monthly_source_progress import MonthlyObservedSourceAuthority, MonthlySourcePartitionError
from backend.services.dataset_release.monthly_unified import MonthlyReleaseCancelled
from backend.services.dataset_release.monthly_unified import SourceChange, classify_component_actions
from backend.services.dataset_release.source_authority import MonthlySourceAuthority
from backend.tests.dataset_release.test_monthly_snapshot import Connection


@pytest.mark.parametrize("repair_first", [False, True])
def test_historical_basic_repair_does_not_rebuild_unrelated_monthly_tails(repair_first):
    from datetime import date

    tails = [
        SourceChange(dataset, (), (), date(2026, 9, 1), date(2026, 9, 30), "TAIL_APPEND", "a" * 64)
        for dataset in ("kline_daily_raw", "kline_minute_raw", "daily_basic", "index_daily")
    ]
    repair = SourceChange("daily_basic", (), (), date(2026, 6, 17), date(2026, 6, 17), "HISTORICAL_REPAIR", "a" * 64)
    actions = classify_component_actions([repair, *tails] if repair_first else [*tails, repair])
    assert actions["factor"] == "SELECTIVE_REBUILD"
    assert {name: actions[name] for name in ("day", "minute", "index", "benchmark")} == {
        name: "INCREMENTAL" for name in ("day", "minute", "index", "benchmark")
    }
    assert actions["suspend"] == actions["stock_pools"] == actions["sector_context"] == "REUSE"


@pytest.mark.parametrize("dataset,affected", [
    ("adj_factor", {"day", "minute", "factor", "benchmark"}),
    ("suspend_d", {"minute", "suspend"}),
    ("moneyflow", {"factor"}),
    ("index_daily", {"index"}),
    ("stock_universe_pit", {"day", "minute", "factor", "suspend", "stock_pools", "sector_context"}),
])
def test_repair_actions_are_scoped_and_order_independent(dataset, affected):
    from datetime import date

    repairs = [
        SourceChange(source, (), (), date(2026, 9, 1), date(2026, 9, 30), "TAIL_APPEND", "a" * 64)
        for source in ("kline_daily_raw", "kline_minute_raw", "daily_basic", "index_daily")
    ]
    change = SourceChange(dataset, (), (), date(2026, 8, 3), date(2026, 8, 3), "HISTORICAL_REPAIR", "b" * 64)
    forward = classify_component_actions([*repairs, change])
    assert forward == classify_component_actions([change, *repairs])
    assert {key for key, action in forward.items() if action == "SELECTIVE_REBUILD"} == affected


def test_schema_rebuild_is_not_downgraded_by_a_later_repair():
    from datetime import date

    changes = [
        SourceChange("daily_basic", (), (), date(2026, 9, 1), date(2026, 9, 30), kind, "a" * 64)
        for kind in ("SCHEMA_CHANGE", "HISTORICAL_REPAIR")
    ]
    assert classify_component_actions(changes)["factor"] == "COMPONENT_REBUILD"


def test_normal_source_progress_tracks_rows_and_completed_partitions(dataset_profile, monkeypatch):
    progress = []
    def seal(_self, _session, **kwargs):
        kwargs["payload_observer"]({})
        kwargs["checkpoint"]()
        kwargs["payload_observer"]({})
        return SimpleNamespace(summary=SimpleNamespace(row_count=2))
    monkeypatch.setattr(MonthlySourceAuthority, "_seal_query_partition", seal)
    authority = MonthlyObservedSourceAuthority(dataset_profile, None, progress=progress.append)
    for partition in ("one", "two"):
        authority._seal_query_partition(None, query=SimpleNamespace(query_id="daily"), partition_key=partition)
    assert progress[-1]["rows_validated"] == progress[-1]["rows_sealed"] == 4
    assert progress[-1]["partitions_sealed"] == 2
    assert [item["rows_validated"] for item in progress] == sorted(item["rows_validated"] for item in progress)


def test_preparation_retains_its_own_progress_and_observer(dataset_profile, monkeypatch):
    def observer(_row):
        pass
    seen = []
    monkeypatch.setattr(MonthlySourceAuthority, "_seal_query_partition", lambda *_a, **kw: seen.append(kw["payload_observer"]))
    authority = MonthlyObservedSourceAuthority(dataset_profile, None, progress=lambda _: pytest.fail("second progress stream"))
    authority._seal_query_partition(None, query=SimpleNamespace(query_id="daily"), partition_key="one", payload_observer=observer)
    assert seen == [observer]


def test_snapshot_cancel_is_not_wrapped_as_failed_transport():
    with MonthlySnapshotCoordinator(Connection, repair_watermark_reader=lambda _: "watermark", overlapping_repair_reader=lambda *_: ()) as snapshot:
        with pytest.raises(MonthlyReleaseCancelled):
            snapshot.read(lambda *_: (_ for _ in ()).throw(MonthlyReleaseCancelled("cancel")))


def test_original_failure_type_and_sqlstate_survive_snapshot_wrapper_without_secrets():
    class DriverError(RuntimeError):
        pgcode = "23505"
    with MonthlySnapshotCoordinator(Connection, repair_watermark_reader=lambda _: "watermark", overlapping_repair_reader=lambda *_: ()) as snapshot:
        with pytest.raises(MonthlySnapshotError) as caught:
            snapshot.read(lambda *_: (_ for _ in ()).throw(DriverError("password=TOP_SECRET SQL bind")))
    chain = monthly_error_diagnostics(caught.value)
    assert [item["type"] for item in chain] == ["MonthlySnapshotError", "DriverError"]
    assert chain[-1]["pgcode"] == "23505"
    assert "TOP_SECRET" not in str(chain)
    assert chain[0]["locations"][-1]["function"] == "read"


def test_exception_chain_is_bounded_and_cycles_do_not_hang():
    error = RuntimeError("private")
    error.__cause__ = error
    assert monthly_error_diagnostics(error) == [{"type": "RuntimeError", "locations": []}]


def test_partition_failure_retains_identity_not_driver_credentials(dataset_profile, monkeypatch):
    class DriverError(RuntimeError):
        sqlstate = "57014"
        diag = SimpleNamespace(table_name="kline_minute_raw", message_detail="private SQL")
    def fail(*_args, **_kwargs):
        raise DriverError("token=SECRET_BIND")
    monkeypatch.setattr(MonthlySourceAuthority, "_seal_query_partition", fail)
    authority = MonthlyObservedSourceAuthority(dataset_profile, None, progress=lambda _: None)
    with pytest.raises(MonthlySourcePartitionError) as caught:
        authority._seal_query_partition(None, query=SimpleNamespace(query_id="minute"), partition_key="2026-09-01_2026-09-30")
    chain = monthly_error_diagnostics(caught.value)
    assert chain[0]["context"] == {"query_id": "minute", "partition_key": "2026-09-01_2026-09-30"}
    assert chain[-1]["sqlstate"] == "57014"
    assert chain[-1]["table_name"] == "kline_minute_raw"
    assert "SECRET_BIND" not in str(chain)
    assert "private SQL" not in str(chain)
    assert "SECRET_BIND" not in monthly_error_message(caught.value.__cause__)


def test_private_partition_failure_retains_identity_and_preserves_owned_observer(dataset_profile, monkeypatch):
    seen = []

    def observer(_row):
        pass

    def fail(*_args, **kwargs):
        seen.append(kwargs["payload_observer"])
        raise OSError(5, "private connection detail")

    monkeypatch.setattr(MonthlySourceAuthority, "_seal_query_partition", fail)
    authority = MonthlyObservedSourceAuthority(dataset_profile, None, progress=lambda _: pytest.fail("private counters reset"))
    with pytest.raises(MonthlySourcePartitionError) as caught:
        authority._seal_query_partition(
            None, query=SimpleNamespace(query_id="minute"), partition_key="2026-09-01_2026-09-30", payload_observer=observer,
        )
    assert seen == [observer]
    chain = monthly_error_diagnostics(caught.value)
    assert chain[0]["context"]["query_id"] == "minute"
    assert chain[-1]["errno"] == 5
    assert "private connection detail" not in str(chain)


_SYNTHETIC_CREDENTIAL = "synthetic test marker"
_SYNTHETIC_BEARER = "synthetic.test.marker"


@pytest.mark.parametrize("style", ["quoted", "json", "bearer"])
def test_owned_error_message_redacts_quoted_and_url_credentials(style):
    import json
    credential = {
        "quoted": "password" + "=" + json.dumps(_SYNTHETIC_CREDENTIAL),
        "json": json.dumps({"password": _SYNTHETIC_CREDENTIAL}),
        "bearer": "authorization" + "=" + "Bearer " + _SYNTHETIC_BEARER,
    }[style]
    error = MonthlySnapshotError(f'{credential} postgresql://user:another@localhost/db')
    message = monthly_error_message(error)
    assert _SYNTHETIC_CREDENTIAL not in message
    assert _SYNTHETIC_BEARER not in message
    assert "another" not in message
    assert "[REDACTED]" in message


def test_partition_cancel_never_becomes_transport_failure(dataset_profile, monkeypatch):
    def cancel(*_args, **_kwargs):
        raise MonthlyReleaseCancelled("cancel")
    monkeypatch.setattr(MonthlySourceAuthority, "_seal_query_partition", cancel)
    authority = MonthlyObservedSourceAuthority(dataset_profile, None, progress=lambda _: None)
    with pytest.raises(MonthlyReleaseCancelled):
        authority._seal_query_partition(None, query=SimpleNamespace(query_id="minute"), partition_key="one")
