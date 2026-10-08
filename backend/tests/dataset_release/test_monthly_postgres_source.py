from __future__ import annotations

from datetime import date, timedelta, timezone
from contextlib import contextmanager
import json
from types import SimpleNamespace
import pytest

from backend.services.dataset_release.cas_store import CASRef
from backend.services.dataset_release.monthly_frozen_source_audit import (
    GateCounter,
    MinuteSummary,
    audit_month_rows,
    audit_margin_publication,
    audit_frozen_source,
)
from backend.data_service.security_source_identity import load_default_security_source_identity_manifest
from backend.services.dataset_release.monthly_source_audit import cn_a_share_minute_labels
from backend.services.dataset_release.monthly_postgres_source import (
    FROZEN_SOURCE_BUNDLE_SCHEMA,
    PostgresMonthlySourceAdapter,
    _source_diffs,
)
from backend.services.dataset_release.monthly_snapshot import MonthlySnapshotIdentity


def _partition(
    dataset: str,
    key: str,
    *,
    content: str,
    schema: str = "schema-v1",
    rows: int = 1,
) -> dict[str, object]:
    return {
        "dataset": dataset,
        "partition_key": key,
        "schema_digest": schema,
        "content_digest": content,
        "row_count": rows,
    }


def test_source_diff_classifies_tail_history_schema_removal_and_pit() -> None:
    baseline = {
        "partitions": [
            _partition("kline_daily_raw", "2026-08-01_2026-08-31", content="old"),
            _partition("adj_factor", "2026-08-01_2026-08-31", content="same"),
            _partition("daily_basic", "2026-08-01_2026-08-31", content="gone"),
        ]
    }
    current = {
        "partitions": [
            _partition("kline_daily_raw", "2026-08-01_2026-08-31", content="repaired"),
            _partition("kline_daily_raw", "2026-09-01_2026-09-30", content="tail"),
            _partition(
                "adj_factor",
                "2026-08-01_2026-08-31",
                content="same",
                schema="schema-v2",
            ),
        ]
    }

    observed = _source_diffs(
        baseline=baseline,
        current=current,
        predecessor_cutoff=date(2026, 8, 31),
        target_cutoff=date(2026, 9, 30),
        pit_changed=True,
    )
    identities = {(item.dataset, item.kind, item.start, item.end) for item in observed}
    assert (
        "kline_daily_raw",
        "HISTORICAL_REPAIR",
        date(2026, 8, 1),
        date(2026, 8, 31),
    ) in identities
    assert (
        "kline_daily_raw",
        "TAIL_APPEND",
        date(2026, 9, 1),
        date(2026, 9, 30),
    ) in identities
    assert ("adj_factor", "SCHEMA_CHANGE", date(2026, 8, 1), date(2026, 8, 31)) in identities
    assert ("daily_basic", "SCHEMA_CHANGE", date(2026, 8, 1), date(2026, 8, 31)) in identities
    assert (
        "stock_universe_pit",
        "PIT_REVISION",
        date(2026, 9, 1),
        date(2026, 9, 30),
    ) in identities


def test_missing_source_baseline_does_not_invent_schema_or_historical_repairs() -> None:
    observed = _source_diffs(
        baseline=None,
        current={"partitions": [
            _partition("kline_daily_raw", "2018-08-01_2026-09-30", content="inherited-and-tail"),
            _partition("moneyflow_ts", "2026-08-01_2026-08-31", content="historical"),
        ]},
        predecessor_cutoff=date(2026, 8, 31),
        target_cutoff=date(2026, 9, 30),
        pit_changed=True,
    )
    assert not any(item.kind in {"SCHEMA_CHANGE", "HISTORICAL_REPAIR", "NEW_SECURITY_HISTORY", "PIT_REVISION"} for item in observed)
    assert {(item.dataset, item.kind) for item in observed} == {
        ("kline_daily_raw", "TAIL_APPEND"), ("stock_universe_pit", "TAIL_APPEND"),
    }
    assert all(item.start == date(2026, 9, 1) and item.end == date(2026, 9, 30) for item in observed)


def test_frozen_bundle_pins_formal_source_stage_receipt() -> None:
    digest = "a" * 64
    reference = CASRef(digest, 7, f"cas/sha256/aa/{digest}")
    frozen = SimpleNamespace(
        artifact_ready_contract_ref=reference,
        official_cutoff=date(2026, 9, 30),
        source_content_root=digest,
        source_provenance_root=digest,
        stable_source_provenance_root=digest,
        pit_snapshot_digest=digest,
        source_manifest_ref=reference,
        source_reuse_manifest_ref=reference,
        source_audit_ref=reference,
        source_provenance_ref=reference,
        pit_snapshot_ref=reference,
        artifact_ready_content_root=digest,
        artifact_ready_provenance_root=digest,
        provider_receipt_refs=(),
        derived_source_receipt_refs=(),
        artifact_ready_derived_source_receipt_refs=(),
        source_cas_usage={"predicted_remaining_new_bytes": 1},
    )
    identity = MonthlySnapshotIdentity(
        snapshot_id="1-ABC-1",
        source_as_of="2026-10-01T00:00:00+00:00",
        initial_repair_watermark="repair-1",
    )
    adapter = SimpleNamespace(profile=SimpleNamespace(profile="qe_hmm_full_v2"))

    bundle = PostgresMonthlySourceAdapter._bundle(
        adapter,
        frozen,
        identity=identity,
        predecessor_cutoff=date(2026, 8, 31),
        baseline_row=None,
        source_stage_ref=reference,
    )

    assert bundle["schema_version"] == FROZEN_SOURCE_BUNDLE_SCHEMA
    assert bundle["source_stage_receipt_ref"] == reference.as_dict()


def test_blocked_actual_audit_prevents_materialization_and_seal(tmp_path, monkeypatch):
    from backend.services.dataset_release import monthly_postgres_source as source
    from backend.services.dataset_release.monthly_source_audit import SourceGateEvidence, MonthlySourceAuditError
    from backend.services.dataset_release.monthly_unified import SOURCE_GATES

    frozen = SimpleNamespace(source_reuse_manifest_ref="ref", source_content_root="a" * 64)
    source_policies = []

    def authority_factory(*_args, **kwargs):
        source_policies.append(kwargs.get("sector_source_policy"))
        return SimpleNamespace(freeze=lambda **_kwargs: frozen)

    monkeypatch.setattr(source, "MonthlyObservedSourceAuthority", authority_factory)
    monkeypatch.setattr(source, "_preflight_refresh_readiness", lambda *_args, **_kwargs: None)
    seen = []
    monkeypatch.setattr(source, "ArtifactReadySourceBuilder", lambda *_args: seen.append("build"))
    monkeypatch.setattr(source, "seal_source_stage_receipt", lambda *_args, **_kwargs: seen.append("seal"))
    gates = tuple(
        SourceGateEvidence(
            name,
            "postgres:1-AA-1",
            f"{name}-expectation.json",
            f"{name}-readback.json",
            1,
            0 if name == "daily_price" else 1,
            unexplained_missing_count=1 if name == "daily_price" else 0,
        )
        for name in SOURCE_GATES
    )
    monkeypatch.setattr(source, "audit_frozen_source", lambda **_kwargs: (gates, ()))
    adapter = PostgresMonthlySourceAdapter(
        profile=SimpleNamespace(profile="fixture"),
        cas=SimpleNamespace(
            root=tmp_path,
            get_json_bounded=lambda *_args, **_kwargs: {
                "partitions": [_partition("kline_daily_raw", "2026-09-01_2026-09-30", content="frozen", rows=1000)]
            },
        ),
        artifact_root=tmp_path,
        source_catalog=SimpleNamespace(root=tmp_path, latest_source_snapshot=lambda **_kwargs: None),
    )
    context = SimpleNamespace(
        operation_id="dmr_test",
        attempt=1,
        plan={"predecessor": {"cutoff": "2026-08-31"}, "target_cutoff": "2026-09-30"},
    )
    identity = MonthlySnapshotIdentity("1-AA-1", "2026-10-01T00:00:00+00:00", "repair")
    with pytest.raises(MonthlySourceAuditError, match="blocking counts"):
        adapter.read(None, identity, context)
    assert seen == []
    assert source_policies == ["classification_published_snapshot_v1"]


def test_monthly_adapter_registry_identity_pins_sector_publication_policy(tmp_path):
    from backend.services.dataset_release import monthly_postgres_source as source
    from backend.services.dataset_release.canonical import digest_named_fields
    from backend.services.dataset_release.monthly_unified import SOURCE_GATES

    profile = SimpleNamespace(profile="qe_hmm_full_v2", semantic_profile_digest="a" * 64)
    adapter = PostgresMonthlySourceAdapter(
        profile=profile,
        cas=SimpleNamespace(root=tmp_path),
        artifact_root=tmp_path,
        source_catalog=SimpleNamespace(root=tmp_path),
    )
    old_fields = {
        "profile": profile.profile,
        "semantic_profile_digest": profile.semantic_profile_digest,
        "source_authority_policy": "dataset_release_source_authority_v1",
        "artifact_ready_contract": "dataset_release_artifact_ready_contract_v1",
        "snapshot_policy": "postgres_exported_repeatable_read_read_only_v1",
        "pit_readiness_policy": "same_snapshot_pre_materialization_v1",
        "mvcc_partition_reuse": False,
        "gates": list(SOURCE_GATES),
        "source_audit_contract": source.AUDIT_SCHEMA,
    }
    old_identity = digest_named_fields("aistock_monthly_postgres_source_adapter_v1", old_fields)
    assert adapter.adapter_version == "9"
    assert adapter.contract_sha256 != old_identity
    assert adapter.contract_sha256 == digest_named_fields(
        "aistock_monthly_postgres_source_adapter_v1",
        {
            **old_fields,
            "sector_source_policy": "classification_published_snapshot_v1",
            "refresh_audit_readiness_policy": source.REFRESH_READINESS_POLICY,
            "business_validation_scope": "target_calendar_month_only_v1",
            "payload_scope": "target_month_and_exact_qfq_construction_facts_v1",
            "sector_mapping_policy": "immutable_predecessor_shared_ids_v1",
            "source_quality_policy": "operation_exact_finite_parity_warnings_v1",
            "component_preparation_dependency_digest": digest_named_fields(
                "aistock_monthly_component_dependency_v1",
                source.component_dependencies(),
            ),
        },
    )


DAY = date(2026, 9, 30)
SYMBOL = "000001.SZ"


@pytest.mark.parametrize(
    "state",
    [
        None,
        (date(2018, 8, 1), date(2026, 8, 31), "ready", False),
        (date(2020, 1, 1), DAY, "ready", False),
        (date(2018, 8, 1), DAY, "building", False),
        (date(2018, 8, 1), DAY, "ready", True),
        (date(2018, 8, 1), "2026-09-30", "ready", False),
        (date(2018, 8, 1), DAY, "ready", None),
    ],
)
def test_canonical_pit_readiness_blocks_before_freeze(tmp_path, monkeypatch, state):
    from backend.services.dataset_release import monthly_postgres_source as source
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseSourceBlocked

    class Connection:
        statements = []

        def cursor(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def execute(self, sql, params):
            self.statements.append((sql, params))

        def fetchone(self):
            return state

    monkeypatch.setattr(source, "MonthlyObservedSourceAuthority", lambda *_args, **_kwargs: pytest.fail("must not freeze"))
    adapter = PostgresMonthlySourceAdapter(
        profile=SimpleNamespace(
            profile="qe_hmm_full_v2", universe_key="aistock_equity_pit_canonical_v2", start_date=date(2018, 8, 1)
        ),
        cas=SimpleNamespace(root=tmp_path),
        artifact_root=tmp_path,
        source_catalog=SimpleNamespace(
            root=tmp_path, latest_source_snapshot=lambda **_kwargs: pytest.fail("no baseline read")
        ),
    )
    context = SimpleNamespace(operation_id="dmr_" + "1" * 32,
        plan={"predecessor": {"cutoff": "2026-08-31"}, "target_cutoff": DAY.isoformat()})
    connection = Connection()
    with pytest.raises(MonthlyReleaseSourceBlocked) as caught:
        adapter.read(connection, MonthlySnapshotIdentity("1-AA-1", "2026-10-02T00:00:00+00:00", "watermark"), context)
    assert caught.value.context["reason_code"] == "BLOCKED_PIT_STATE_NOT_READY"
    assert caught.value.context["requested_cutoff"] == DAY.isoformat()
    assert caught.value.context["operator_script"] == "scripts/prepare_canonical_pit_monthly.py"
    assert len(connection.statements) == 1
    assert connection.statements[0][0].lstrip().startswith("SELECT")
    assert list(tmp_path.iterdir()) == []


def test_canonical_pit_precheck_accepts_exact_ready_scope():
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def execute(self, sql, params):
            assert sql.lstrip().startswith("SELECT")
            assert params == ("aistock_equity_pit_canonical_v2",)

        def fetchone(self):
            return date(2018, 8, 1), DAY, "ready", False

    adapter = SimpleNamespace(
        profile=SimpleNamespace(
            universe_key="aistock_equity_pit_canonical_v2",
            start_date=date(2018, 8, 1),
        )
    )
    PostgresMonthlySourceAdapter._require_pit_coverage(adapter, SimpleNamespace(cursor=Cursor), DAY)


def test_refresh_readiness_aggregates_missing_and_ineligible_dates_without_payload_reads():
    from backend.services.dataset_release import monthly_postgres_source as source
    from backend.services.dataset_release.source_authority import SourceRefreshAuditLedger
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseSourceBlocked

    days = (date(2026, 9, 29), DAY)
    ledger = SourceRefreshAuditLedger(
        rows={
            ("margin_detail", days[0]): (
                {
                    "data_source": "tushare",
                    "status": "success",
                    "quality_status": "valid",
                    "error_present": False,
                    "audit_payload_sha256": "a" * 64,
                },
            ),
            ("margin_detail", DAY): (
                {
                    "data_source": "tushare",
                    "status": "success",
                    "quality_status": "low_coverage",
                    "error_present": False,
                    "audit_payload_sha256": "b" * 64,
                },
            ),
        },
        trading_dates=days,
        eligible_sources={name: ("tushare",) for name in ("daily_basic", "margin_detail")},
        eligible_quality_statuses={name: ("valid",) for name in ("daily_basic", "margin_detail")},
    )
    calls = []

    @contextmanager
    def factory(policy):
        calls.append(("session", policy))
        yield "imported-readonly-snapshot"

    def read_audit(session, **kwargs):
        assert session == "imported-readonly-snapshot"
        assert kwargs["cutoff"] == DAY
        calls.append("audit")
        return ledger

    authority = SimpleNamespace(
        _freeze_refresh_audit=read_audit,
        _database_query_specs=lambda: [
            SimpleNamespace(date_expression="date", start_policy="daily", audit_dataset=name)
            for name in ("daily_basic", "margin_detail", "margin_detail")
        ],
    )
    profile = SimpleNamespace(start_date=days[0], minute_start_date=DAY, resource_policy="policy")
    with pytest.raises(MonthlyReleaseSourceBlocked) as caught:
        source._preflight_refresh_readiness(authority, factory, profile=profile, cutoff=DAY)
    context = caught.value.context
    assert context["reason_code"] == "BLOCKED_SOURCE_REFRESH_AUDIT_INCOMPLETE"
    assert context["blocker_count"] == 2
    assert context["blockers"][0]["dataset"] == "daily_basic"
    assert context["blockers"][0]["missing_count"] == 2
    assert context["blockers"][1]["unusable_sample"] == [DAY.isoformat()]
    assert context["database_write_performed"] is False
    assert calls == [("session", "policy"), "audit"]


def test_refresh_readiness_pass_preserves_per_partition_fact_validation():
    from backend.services.dataset_release import monthly_postgres_source as source

    checked = []
    ledger = SimpleNamespace(partition_digest=lambda *args: checked.append(args) or "a" * 64)

    @contextmanager
    def factory(_policy):
        yield None

    authority = SimpleNamespace(
        _freeze_refresh_audit=lambda *_args, **_kwargs: ledger,
        _database_query_specs=lambda: [
            SimpleNamespace(date_expression=None, start_policy="timeless", audit_dataset=None),
            SimpleNamespace(date_expression="date", start_policy="minute", audit_dataset="kline_minute_raw"),
        ],
    )
    source._preflight_refresh_readiness(
        authority,
        factory,
        profile=SimpleNamespace(start_date=date(2018, 8, 1), minute_start_date=date(2020, 1, 1), resource_policy=None),
        cutoff=DAY,
    )
    assert checked == [("kline_minute_raw", date(2026, 9, 1), DAY)]


def test_explicit_financing_deferral_only_changes_actual_tail_query_and_audit_end(dataset_profile):
    from backend.services.dataset_release.monthly_source_progress import MonthlyObservedSourceAuthority
    from backend.services.dataset_release.source_authority import PRODUCTION_QUERY_SPECS
    from backend.services.dataset_release import monthly_postgres_source as source
    authority = MonthlyObservedSourceAuthority(dataset_profile, None, progress=lambda _: None,
        month_start=date(2026, 9, 1), deferred_margin_cutoff=DAY)
    pit = SimpleNamespace(spans=[SimpleNamespace(ts_code="000001.SZ")])
    margin = list(authority._partition_requests(PRODUCTION_QUERY_SPECS["margin_detail"], DAY, pit_snapshot=pit))
    basic = list(authority._partition_requests(PRODUCTION_QUERY_SPECS["daily_basic"], DAY, pit_snapshot=pit))
    assert margin[0][0] == basic[0][0] == "2026-09-01_2026-09-30"
    assert margin[0][1]["end"] == date(2026, 9, 29)
    assert margin[0][1]["user_deferred_trade_date"] == DAY
    assert basic[0][1]["end"] == DAY
    checked = []
    ledger = SimpleNamespace(partition_digest=lambda *args: checked.append(args))
    audit_authority = SimpleNamespace(_freeze_refresh_audit=lambda *_a, **_kw: ledger,
        _database_query_specs=lambda: [SimpleNamespace(date_expression="date", start_policy="daily", audit_dataset=name)
            for name in ("daily_basic", "margin_detail")])
    @contextmanager
    def factory(_policy):
        yield None
    source._preflight_refresh_readiness(audit_authority, factory, profile=dataset_profile, cutoff=DAY, deferred_margin_cutoff=DAY)
    assert checked == [("daily_basic", date(2026, 9, 1), DAY), ("margin_detail", date(2026, 9, 1), date(2026, 9, 29))]


def test_adapter_readiness_failure_precedes_freeze_and_input_directory(tmp_path, monkeypatch):
    from backend.services.dataset_release import monthly_postgres_source as source
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseSourceBlocked

    def fail_readiness(*_args, **_kwargs):
        raise MonthlyReleaseSourceBlocked(
            "readiness incomplete",
            context={
                "reason_code": "BLOCKED_SOURCE_REFRESH_AUDIT_INCOMPLETE",
                "blockers": [{"dataset": "margin_detail", "unusable_sample": [DAY.isoformat()]}],
            },
        )

    monkeypatch.setattr(source, "_preflight_refresh_readiness", fail_readiness)
    monkeypatch.setattr(
        source,
        "MonthlyObservedSourceAuthority",
        lambda *_args, **_kwargs: SimpleNamespace(freeze=lambda **_kw: pytest.fail("payload freeze must not start")),
    )
    adapter = PostgresMonthlySourceAdapter(
        profile=SimpleNamespace(profile="fixture"),
        cas=SimpleNamespace(root=tmp_path),
        artifact_root=tmp_path,
        source_catalog=SimpleNamespace(root=tmp_path, latest_source_snapshot=lambda **_kwargs: None),
    )
    context = SimpleNamespace(
        operation_id="dmr_test",
        attempt=1,
        plan={
            "predecessor": {"cutoff": "2026-08-31"},
            "target_cutoff": DAY.isoformat(),
        },
    )
    with pytest.raises(MonthlyReleaseSourceBlocked) as caught:
        adapter.read(None, MonthlySnapshotIdentity("1-AA-1", "2026-10-03T00:00:00+00:00", "repair"), context)
    assert caught.value.context["blockers"][0]["dataset"] == "margin_detail"
    assert list(tmp_path.iterdir()) == []


def daily():
    return {
        "ts_code": SYMBOL,
        "trade_date": DAY,
        "open_li": 10000,
        "high_li": 10000,
        "low_li": 10000,
        "close_li": 10000,
        "volume_hand": 240,
        "amount_li": 2400000,
    }


def test_frozen_source_audit_checks_cancel_inside_stream_before_gate_or_receipt(tmp_path, monkeypatch):
    from backend.services.dataset_release import monthly_frozen_source_audit as audit
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseCancelled

    rows_read = checks = 0

    class Reader:
        @contextmanager
        def iter_rows(self, *_args):
            def rows():
                nonlocal rows_read
                for _ in range(200_000):
                    rows_read += 1
                    yield {"cal_date": "2026-09-30"}
            yield rows()

    def checkpoint():
        nonlocal checks
        checks += 1
        if checks == 2:
            raise MonthlyReleaseCancelled("cancelled during read")

    monkeypatch.setattr(audit, "CASSealedPartitionReader", lambda *_args, **_kwargs: Reader())
    frozen = SimpleNamespace(partitions=(SimpleNamespace(as_build_input=lambda: {
        "dataset": "trading_calendar", "partition_key": "2026-09-01_2026-09-30",
    }),))
    with pytest.raises(MonthlyReleaseCancelled):
        audit.audit_frozen_source(
            cas=None, frozen=frozen, profile=None, input_root=tmp_path, artifact_root=tmp_path,
            snapshot_group_id="fixture", changes=(), predecessor_cutoff=date(2026, 8, 31),
            checkpoint=checkpoint,
        )
    assert rows_read == 100_000
    assert list(tmp_path.iterdir()) == []


def minutes():
    return [
        {**daily(), "trade_time": stamp, "volume_hand": 1, "amount_li": 10000}
        for stamp in cn_a_share_minute_labels(DAY)
    ]


def rows():
    return {
        "kline_daily_raw": [daily()],
        "kline_minute_raw": minutes(),
        "adj_factor": [{"ts_code": SYMBOL, "trade_date": DAY, "adj_factor": 1.0}],
        "daily_basic": [
            {
                "ts_code": SYMBOL,
                "trade_date": DAY,
                "turnover_rate": 1.0,
                "turnover_rate_f": 1.0,
                "volume_ratio": 1.0,
                "total_mv": 100.0,
                "circ_mv": 80.0,
            }
        ],
        "suspend_d": [],
        "stk_limit": [{"ts_code": SYMBOL, "trade_date": DAY, "pre_close": 10, "up_limit": 11, "down_limit": 9}],
    }


def run(data):
    gates = {
        name: GateCounter(name)
        for name in (
            "daily_price",
            "minute_price",
            "adj_factor_history",
            "daily_basic_required_fields",
            "suspend_limit",
            "pit_stock_pools",
        )
    }
    audit_month_rows(
        data,
        sessions=(DAY,),
        pools={
            name: {DAY: {SYMBOL}} for name in ("stock_universe", "csi300", "csi500", "csi1000", "star50", "star100")
        },
        gates=gates,
        authority_sha256="a" * 64,
        minute_start=DAY,
    )
    return gates


def test_missing_date_is_not_replaced_by_observed_count():
    data = rows()
    data["kline_daily_raw"] = []
    result = run(data)["daily_price"]
    assert (result.expected_count, result.observed_count, result.missing_count) == (1, 0, 1)
    assert result.status == "BLOCKED"


@pytest.mark.parametrize("field", ["turnover_rate", "turnover_rate_f", "volume_ratio"])
def test_null_required_field_blocks_present_row(field):
    data = rows()
    data["daily_basic"][0][field] = None
    result = run(data)["daily_basic_required_fields"]
    assert result.observed_count == result.expected_count == 1
    assert result.invalid_count == 1
    assert result.status == "BLOCKED"


def test_complete_month_closes_real_counts():
    assert all(gate.status == "PASS" for gate in run(rows()).values())


def test_price_parity_blocks_complete_240_bars():
    data = rows()
    data["kline_minute_raw"][0]["open_li"] = 10100
    data["kline_minute_raw"][0]["high_li"] = 10100
    result = run(data)["minute_price"]
    assert result.invalid_count == 1
    assert result.status == "BLOCKED"


@pytest.mark.parametrize("mutation", ["duplicate", "missing", "forbidden", "nonfinite"])
def test_minute_session_defects_fail_closed(mutation):
    data = rows()
    if mutation == "duplicate":
        data["kline_minute_raw"].append(data["kline_minute_raw"][0])
    elif mutation == "missing":
        data["kline_minute_raw"].pop()
    elif mutation == "forbidden":
        row = dict(data["kline_minute_raw"][120])
        row["trade_time"] = row["trade_time"].replace(hour=13, minute=0)
        data["kline_minute_raw"].append(row)
    else:
        data["kline_minute_raw"][0]["amount_li"] = float("nan")
    result = run(data)["minute_price"]
    assert result.status == "BLOCKED"


def test_full_day_suspend_explains_missing_prices_not_positive_trades():
    data = rows()
    data["kline_daily_raw"] = []
    data["kline_minute_raw"] = []
    data["suspend_d"] = [{"ts_code": SYMBOL, "trade_date": DAY, "suspend_type": "S", "suspend_timing": None}]
    result = run(data)
    assert result["daily_price"].explained_count == 1
    assert result["minute_price"].explained_count == 1
    assert all(item.status == "PASS" for item in result.values())
    data["kline_daily_raw"] = [daily()]
    assert run(data)["suspend_limit"].status == "BLOCKED"


def test_minute_summary_does_not_store_240_rows():
    value = MinuteSummary(DAY)
    for row in minutes():
        value.add(row)
    assert value.count == 240
    assert value.labels.bit_count() == 240
    assert not hasattr(value, "rows")


def auction_data():
    data = rows()
    auction = {**data["kline_minute_raw"][0],
               "trade_time": data["kline_minute_raw"][0]["trade_time"].replace(minute=30),
               "open_li": 9900, "high_li": 9900, "low_li": 9900, "close_li": 9900,
               "volume_hand": 2, "amount_li": 20000}
    data["kline_minute_raw"].insert(0, auction)
    data["kline_daily_raw"][0].update(open_li=9900, low_li=9900, volume_hand=242, amount_li=2420000)
    return data


@pytest.mark.parametrize("placement", ["first", "last", "utc"])
def test_minute_source_auction_is_not_a_canonical_bar(placement):
    data = auction_data()
    if placement == "last":
        data["kline_minute_raw"].append(data["kline_minute_raw"].pop(0))
    elif placement == "utc":
        auction = data["kline_minute_raw"][0]
        auction["trade_time"] = auction["trade_time"].replace(tzinfo=timezone(timedelta(hours=8))).astimezone(timezone.utc)
    summary = MinuteSummary(DAY)
    for row in data["kline_minute_raw"]:
        summary.add(row)
    assert (summary.count, summary.labels.bit_count(), summary.invalid) == (240, 240, False)
    assert summary.aggregates()["open"] == 9.9
    assert summary.aggregates()["vol"] == 24200
    assert all(gate.status == "PASS" for gate in run(data).values())


@pytest.mark.parametrize("defect", ["duplicate", "second", "nonfinite", "missing", "only_auction",
                                    "price_drift", "volume_drift", "cash_drift", "out_of_session", "auction_only_suspended"])
def test_minute_auction_preserves_fail_closed(defect):
    data = auction_data()
    minute = data["kline_minute_raw"]
    auction = minute[0]
    if defect == "duplicate":
        minute.append(dict(auction))
    elif defect == "second":
        auction["trade_time"] += timedelta(seconds=1)
    elif defect == "nonfinite":
        auction["amount_li"] = float("nan")
    elif defect == "missing":
        minute.pop()
    elif defect == "only_auction":
        data["kline_minute_raw"] = [auction]
    elif defect == "price_drift":
        auction.update(open_li=9800, high_li=9800, low_li=9800, close_li=9800)
    elif defect == "volume_drift":
        auction["volume_hand"] *= 2
    elif defect == "cash_drift":
        auction["amount_li"] *= 3
    elif defect == "out_of_session":
        auction["trade_time"] = auction["trade_time"].replace(minute=0)
    else:
        data["kline_minute_raw"] = [auction]
        data["kline_daily_raw"] = []
        data["suspend_d"] = [{"ts_code": SYMBOL, "trade_date": DAY, "suspend_type": "S", "suspend_timing": None}]
    result = run(data)
    assert result["suspend_limit" if defect == "auction_only_suspended" else "minute_price"].status == "BLOCKED"
    if defect == "duplicate":
        assert result["minute_price"].duplicate_count == 1


def test_real_shared_source_resolutions_are_used_as_codes():
    data = rows()
    gates = run(data)
    audit_month_rows(
        data,
        sessions=(DAY,),
        pools={
            name: {DAY: {SYMBOL}} for name in ("stock_universe", "csi300", "csi500", "csi1000", "star50", "star100")
        },
        gates=gates,
        authority_sha256="a" * 64,
        minute_start=DAY,
        aliases=load_default_security_source_identity_manifest(),
    )
    assert all(gate.status == "PASS" for gate in gates.values())


def test_expected_key_identity_does_not_depend_on_observed_rows():
    complete = run(rows())["daily_price"]
    data = rows()
    data["kline_daily_raw"] = []
    missing = run(data)["daily_price"]
    assert complete.expected_keys.hexdigest() == missing.expected_keys.hexdigest()


@pytest.mark.parametrize("defect", [None, "missing", "partial", "unknown", "null", "duplicate", "ambiguous"])
def test_margin_uses_independent_provider_bound_not_all_stock_count(defect):
    from backend.services.dataset_release.source_authority import _MARGIN_DETAIL_VALUES

    values = [{"ts_code": f"{i:06d}.SZ", **dict.fromkeys(_MARGIN_DETAIL_VALUES, 1.0)} for i in range(2)]
    source = {
        "data_source": "tushare",
        "status": "success",
        "error_present": False,
        "quality_status": "complete",
        "expected_rows": 2,
    }
    receipt = {
        "eligible_sources": {"margin_detail": ["tushare"]},
        "eligible_quality_statuses": {"margin_detail": ["complete"]},
        "rows": [{"dataset": "margin_detail", "trade_date": DAY.isoformat(), "sources": [source]}],
    }
    if defect == "missing":
        values = []
    elif defect == "partial":
        values.pop()
    elif defect == "unknown":
        source["expected_rows"] = None
    elif defect == "null":
        values[0]["rzye"] = None
    elif defect == "duplicate":
        values[1] = dict(values[0])
    elif defect == "ambiguous":
        receipt["rows"][0]["sources"].append({**source, "expected_rows": 3})
    gate = GateCounter("financial_moneyflow")
    audit_margin_publication(day=DAY, rows=values, receipt=receipt, gate=gate)
    assert gate.expected_count == 1  # A provider publication, not every equity.
    assert gate.status == ("PASS" if defect is None else "BLOCKED")


@pytest.mark.parametrize("defect", [None, "resume_and_suspend_zero", "no_suspend", "intraday", "resume", "duplicate_suspend",
    "positive_volume", "positive_amount", "missing_daily", "duplicate_daily", "other_null", "nan", "no_hash", "partial"])
def test_margin_suspended_quote_nulls_require_independent_zero_turnover(defect):
    from backend.services.dataset_release.source_authority import _MARGIN_DETAIL_VALUES
    values = [{"ts_code": SYMBOL, **dict.fromkeys(_MARGIN_DETAIL_VALUES, 1.0)}]
    values[0].update(rqye=None, rzrqye=None)
    source = {"data_source":"tushare", "status":"success", "error_present":False,
              "quality_status":"complete", "expected_rows":1}
    receipt = {"eligible_sources":{"margin_detail":["tushare"]},
               "eligible_quality_statuses":{"margin_detail":["complete"]},
               "rows":[{"dataset":"margin_detail", "trade_date":DAY.isoformat(), "sources":[source]}]}
    suspension = [{"ts_code":SYMBOL, "trade_date":DAY, "suspend_type":"S", "suspend_timing":None}]
    daily = [{"ts_code":SYMBOL, "trade_date":DAY, "open_li":1000, "high_li":1000,
              "low_li":1000, "close_li":1000, "volume_hand":0, "amount_li":0}]
    authority = "a" * 64
    if defect == "no_suspend":
        suspension = []
    elif defect == "resume_and_suspend_zero":
        suspension.append({**suspension[0], "suspend_type":"R"})
    elif defect == "intraday":
        suspension[0]["suspend_timing"] = "09:30-10:00"
    elif defect == "resume":
        suspension[0]["suspend_type"] = "R"
    elif defect == "duplicate_suspend":
        suspension.append(dict(suspension[0]))
    elif defect in {"positive_volume","positive_amount"}:
        daily[0]["volume_hand" if defect == "positive_volume" else "amount_li"] = 1
    elif defect == "missing_daily":
        daily = []
    elif defect == "duplicate_daily":
        daily.append(dict(daily[0]))
    elif defect == "other_null":
        values[0]["rzye"] = None
    elif defect == "nan":
        values[0]["rqye"] = float("nan")
    elif defect == "no_hash":
        authority = None
    elif defect == "partial":
        source["expected_rows"] = 2
    gate = GateCounter("financial_moneyflow")
    audit_margin_publication(day=DAY, rows=values, receipt=receipt, gate=gate,
                            suspend_rows=suspension, daily_rows=daily, suspend_authority_sha256=authority)
    assert gate.status == ("PASS" if defect in {None,"resume_and_suspend_zero"} else "BLOCKED")
    assert values[0]["rzrqye"] is None  # Never synthesize the published value.
    if defect in {None,"resume_and_suspend_zero"}:
        assert gate.expected_count == 2 and gate.observed_count == 1 and gate.explained_count == 1
        assert gate.exceptions[0].field == "rqye,rzrqye"
        assert gate.exceptions[0].symbol == SYMBOL
        assert gate.exceptions[0].reason_code == "SUSPEND_FULL_DAY"


@pytest.mark.parametrize("missing", [False, True])
@pytest.mark.parametrize("old_partition", [False, True])
def test_frozen_wrapper_persists_all_nine_real_gates(tmp_path, monkeypatch, missing, old_partition):
    from backend.services.dataset_release import monthly_frozen_source_audit as audit
    from backend.services.dataset_release import sw_l2_quote_policy as policy
    from backend.services.core_index_catalog import P0_POOL_IDS, POOL_DEFINITIONS
    from backend.services.dataset_release.cas_store import CASRef
    from backend.services.dataset_release.monthly_unified import SOURCE_GATES
    from backend.services.dataset_release.source_authority import (
        _MONEYFLOW_VALUES,
        _CYQ_VALUES,
        _BAK_BASIC_VALUES,
        _MARGIN_DETAIL_VALUES,
        SOURCE_REFRESH_AUDIT_RECEIPT_SCHEMA,
    )

    codes = [f"801{i:03d}.SI" for i in range(131)]
    monkeypatch.setattr(policy, "_NEVER_QUOTED", frozenset({codes[0]}))
    monkeypatch.setattr(policy, "_LAST_PUBLISHED", {})
    monkeypatch.setattr(policy, "SW2021_L2_TAXONOMY_DIGEST", policy.taxonomy_digest(codes))
    data = rows()
    data.update(
        {
            "trading_calendar": [{"cal_date": DAY, "is_trading": True}],
            "stock_basic": [{"ts_code": SYMBOL, "list_date": date(1991, 1, 1)}],
            "index_membership_pit": [
                {
                    "pool_id": pool,
                    "index_code": POOL_DEFINITIONS[pool].index_code,
                    "source_provider": POOL_DEFINITIONS[pool].source_provider,
                    "ts_code": SYMBOL,
                    "effective_from": DAY,
                    "effective_to_exclusive": None,
                    "source_reference": "fixture",
                    "updated_at": "2026-10-01",
                }
                for pool in P0_POOL_IDS
            ],
            "sw_index_classify": [{"index_code": code, "level": "L2"} for code in codes],
            "sw_index_member": [
                {
                    "ts_code": SYMBOL if i == 1 else f"{10000 + i:06d}.SZ",
                    "in_date": DAY,
                    "out_date": None,
                    "l2_code": code,
                }
                for i, code in enumerate(codes)
            ],
            "sector_data": [
                {
                    "ts_code": SYMBOL,
                    "trade_date": DAY,
                    "l2_code_id": 1,
                    **dict.fromkeys(
                        (
                            "sw2_pct_change",
                            "sw2_vol",
                            "sw2_amount",
                            "sw2_mf_net_amt",
                            "sw2_mf_buy_elg_amt",
                            "sw2_mf_sell_elg_amt",
                        ),
                        1.0,
                    ),
                }
            ],
        }
    )
    for name, fields in (
        ("moneyflow_ts", _MONEYFLOW_VALUES),
        ("cyq_perf", _CYQ_VALUES),
        ("bak_basic", _BAK_BASIC_VALUES),
        ("margin_detail", _MARGIN_DETAIL_VALUES),
    ):
        data[name] = [{"ts_code": SYMBOL, "trade_date": DAY, **dict.fromkeys(fields, 1.0)}]
    if missing:
        data["kline_daily_raw"] = []
    key = "2026-09-01_2026-09-30"
    visited = []

    class Reader:
        def __init__(self, *_args, **_kwargs):
            pass

        @contextmanager
        def iter_rows(self, dataset, partition_key):
            if old_partition and partition_key == "2026-08-01_2026-08-31":
                pytest.fail("monthly business audit reopened historical facts")
            visited.append((dataset, partition_key))
            yield iter(data[dataset])

    monkeypatch.setattr(audit, "CASSealedPartitionReader", Reader)
    reference = CASRef("a" * 64, 1, "cas/test")
    frozen = SimpleNamespace(
        partitions=[
            SimpleNamespace(as_build_input=lambda name=name: {"dataset": name, "partition_key": key}) for name in data
        ] + ([SimpleNamespace(as_build_input=lambda: {
            "dataset": "kline_minute_raw", "partition_key": "2026-08-01_2026-08-31",
        })] if old_partition else []),
        official_cutoff=DAY,
        pit_snapshot=SimpleNamespace(spans=[SimpleNamespace(ts_code=SYMBOL, eligible_start=DAY, eligible_end=DAY)]),
        source_manifest_ref=reference,
        source_audit_ref=reference,
        pit_snapshot_digest="b" * 64,
    )
    receipt = {
        "schema_version": SOURCE_REFRESH_AUDIT_RECEIPT_SCHEMA,
        "profile": "fixture",
        "cutoff": DAY.isoformat(),
        "eligible_sources": {"margin_detail": ["tushare"]},
        "eligible_quality_statuses": {"margin_detail": ["complete"]},
        "rows": [
            {
                "dataset": "margin_detail",
                "trade_date": DAY.isoformat(),
                "sources": [
                    {
                        "data_source": "tushare",
                        "status": "success",
                        "error_present": False,
                        "quality_status": "complete",
                        "expected_rows": 1,
                    }
                ],
            }
        ],
    }
    from dataclasses import replace
    from backend.services import tushare_dataset_specs

    monkeypatch.setattr(
        tushare_dataset_specs, "MARGIN_DETAIL", replace(tushare_dataset_specs.MARGIN_DETAIL, min_expected_rows=1)
    )
    target = tmp_path / "input"
    target.mkdir()
    gates, artifacts = audit_frozen_source(
        cas=SimpleNamespace(get_json_bounded=lambda *_args, **_kwargs: receipt),
        frozen=frozen,
        profile=SimpleNamespace(profile="fixture", start_date=DAY, minute_start_date=DAY),
        input_root=target,
        artifact_root=tmp_path,
        snapshot_group_id="postgres:1-AA-1",
        changes=(SimpleNamespace(start=date(2018, 8, 1), end=DAY),) if old_partition else (),
        predecessor_cutoff=date(2026, 8, 31),
        audit_causal_history=not old_partition,
    )
    assert {gate.gate for gate in gates} == set(SOURCE_GATES)
    assert all(gate.payload()["status"] == "PASS" for gate in gates) is not missing
    daily_gate = next(gate for gate in gates if gate.gate == "daily_price")
    assert daily_gate.expected_count == 1
    assert daily_gate.unexplained_missing_count == int(missing)
    assert len(artifacts) == (21 if old_partition else 22)
    readback = json.loads((tmp_path / daily_gate.readback_ref).read_text())
    assert readback["unexplained_missing_count"] == int(missing)
    assert readback["database_write_performed"] is False
    assert visited.count(("kline_minute_raw", key)) == 1


def test_minute_session_failure_names_missing_and_invalid_labels():
    data = rows()
    bar = data["kline_minute_raw"][119]
    bar["trade_time"] = bar["trade_time"].replace(hour=13, minute=0)
    issues = []
    from backend.services.dataset_release.monthly_frozen_source_audit import audit_month_rows
    gates = run(rows())
    for gate in gates.values():
        gate.emit = issues.append
    audit_month_rows(data, sessions=(DAY,), pools={"stock_universe": {DAY: {SYMBOL}}},
                     gates=gates, authority_sha256="a" * 64, minute_start=DAY)
    issue = next(item for item in issues if item["gate"] == "minute_price")
    assert issue["reason"] == "minute_session_invalid"
    assert issue["details"]["missing_labels"] == ["11:30:00"]
    assert issue["details"]["invalid_labels"] == ["13:00:00"]
    assert issue["details"]["bar_count"] == 240


@pytest.mark.parametrize('daily_present', [False, True])
def test_frozen_full_suspend_zero_placeholder_is_typed_gap(daily_present):
    data = rows()
    data['suspend_d'] = [{'ts_code': SYMBOL, 'trade_date': DAY, 'suspend_type': 'S', 'suspend_timing': None}]
    for row in data['kline_minute_raw'] + data['kline_daily_raw']:
        row.update(volume_hand=0, amount_li=0)
    data['kline_minute_raw'][119]['trade_time'] = data['kline_minute_raw'][119]['trade_time'].replace(hour=13, minute=0)
    if not daily_present:
        data['kline_daily_raw'] = []
    gate = run(data)['minute_price']
    assert gate.status == 'PASS'
    assert gate.explained_count == 1 and gate.observed_count == 0
    assert gate.exceptions[0].reason_code == 'SUSPEND_FULL_DAY'


@pytest.mark.parametrize('conflict', ['no_suspend', 'partial_suspend', 'daily_volume', 'daily_amount',
                                     'minute_volume', 'minute_amount', 'minute_null', 'duplicate', 'other_label'])
def test_suspended_placeholder_conflicts_remain_blocked(conflict):
    data = rows()
    data['suspend_d'] = [{'ts_code': SYMBOL, 'trade_date': DAY, 'suspend_type': 'S', 'suspend_timing': None}]
    for row in data['kline_minute_raw'] + data['kline_daily_raw']:
        row.update(volume_hand=0, amount_li=0)
    data['kline_minute_raw'][119]['trade_time'] = data['kline_minute_raw'][119]['trade_time'].replace(hour=13, minute=0)
    if conflict == 'no_suspend':
        data['suspend_d'] = []
    elif conflict == 'partial_suspend':
        data['suspend_d'][0]['suspend_timing'] = '13:00-14:00'
    elif conflict.startswith('daily_'):
        data['kline_daily_raw'][0]['volume_hand' if conflict == 'daily_volume' else 'amount_li'] = 1
    elif conflict.startswith('minute_'):
        # The invalid 13:00 row must contribute to no-trade proof as well.
        data['kline_minute_raw'][119]['volume_hand' if conflict != 'minute_amount' else 'amount_li'] = None if conflict == 'minute_null' else 1
    elif conflict == 'duplicate':
        data['kline_minute_raw'].append(dict(data['kline_minute_raw'][0]))
    elif conflict == 'other_label':
        data['kline_minute_raw'][1]['trade_time'] = data['kline_minute_raw'][1]['trade_time'].replace(hour=9, minute=0)
    assert run(data)['minute_price'].status == 'BLOCKED'


@pytest.mark.parametrize('files, expected', [
    (['monthly_frozen_source_audit.py'], 'monthly_release_worker_process'),
    (['monthly_frozen_source_audit.py', 'canonical_stock_transformer.py', 'build_stage.py'], 'monthly_release_worker_process'),
    (['canonical_stock_transformer.py', 'build_stage.py'], 'dataset_release_worker_heartbeat'),
])
def test_monthly_suspend_fix_selects_real_worker_probe(files, expected):
    from pathlib import Path
    import yaml
    from scripts.aistock_issue_workflow import _select_runtime_probe_route
    root = Path(__file__).resolve().parents[3]
    catalog = yaml.safe_load((root / 'docs/standards/aistock_runtime_targets_v1.yaml').read_text(encoding='utf-8'))
    selected, error = _select_runtime_probe_route(catalog['targets']['worker-scheduler'],
        runtime_files=[f'backend/services/dataset_release/{name}' for name in files])
    assert error is None
    assert selected['probe_route_id'] == expected


@pytest.mark.parametrize('case', ['missing_daily', 'zero_daily', 'no_daily_proof', 'positive_daily', 'null_daily',
                                 'no_suspend', 'positive_minute', 'duplicate', 'other_label'])
def test_monthly_build_suspended_1300_uses_proof_or_fails_closed(case):
    from backend.tests.dataset_release.test_canonical_stock_transformer import (
        DAY2, _minute_day, _daily, _spec, _adj, _limits, _full_day_suspend,
    )
    from backend.services.dataset_release.canonical_stock_transformer import (
        CanonicalStockTransformer, CanonicalStockTransformError, CanonicalStockTransformMetrics,
    )
    raw = _minute_day(DAY2)
    for row in raw:
        row.update(volume_hand=0, amount_li=0)
    raw[119]['trade_time'] = f'{DAY2} 13:00:00+08:00'
    day = _daily(DAY2)
    day.update(volume_hand=0, amount_li=0)
    if case == 'positive_daily':
        day['amount_li'] = 1
    elif case == 'null_daily':
        day['volume_hand'] = None
    elif case == 'positive_minute':
        raw[119]['amount_li'] = 1
    elif case == 'duplicate':
        raw.append(dict(raw[0]))
    elif case == 'other_label':
        raw[1]['trade_time'] = f'{DAY2} 09:00:00+08:00'
    raw.sort(key=lambda row: row['trade_time'])
    metrics = CanonicalStockTransformMetrics('minute_bin')
    output = CanonicalStockTransformer().transform_minute(
        _spec(start=DAY2), minute_rows=raw, adj_factor_rows=_adj(), stk_limit_rows=_limits(DAY2),
        suspend_rows=[] if case == 'no_suspend' else _full_day_suspend(DAY2),
        daily_rows=None if case == 'no_daily_proof' else [] if case == 'missing_daily' else [day], metrics=metrics,
    )
    if case not in ('missing_daily', 'zero_daily'):
        with pytest.raises(CanonicalStockTransformError):
            list(output)
        return
    result = list(output)
    assert len(result) == 240 and metrics.synthesized_stock_days == 1
    assert all(row['volume'] == row['amount'] == 0 for row in result)
    assert result[119]['datetime'].endswith('11:30:00')
    assert not any(row['datetime'].endswith('13:00:00') for row in result)


def test_margin_missing_evidence_is_not_reported_as_invalid_moneyflow():
    issues = []
    gate = GateCounter("financial_moneyflow", emit=issues.append)
    audit_margin_publication(day=date(2026, 9, 1), rows=[{"ts_code": SYMBOL}], receipt={}, gate=gate)
    assert gate.status == "BLOCKED"
    assert issues[0]["dataset"] == "margin_detail"
    assert issues[0]["reason"] == "provider_denominator_unproven"


def test_limit_failure_names_exact_missing_field():
    from backend.services.dataset_release.monthly_frozen_source_audit import audit_month_rows
    data = rows()
    data["stk_limit"][0]["pre_close"] = None
    gates = run(rows())
    issues = []
    for gate in gates.values():
        gate.emit = issues.append
    audit_month_rows(data, sessions=(DAY,), pools={"stock_universe": {DAY: {SYMBOL}}},
                     gates=gates, authority_sha256="a" * 64, minute_start=DAY)
    issue = next(item for item in issues if item["gate"] == "suspend_limit")
    assert issue["dataset"] == "stk_limit"
    assert issue["details"]["invalid_fields"] == ["pre_close"]


def test_only_rule_preparable_limits_can_reach_materialization(tmp_path):
    from backend.services.dataset_release.monthly_postgres_source import _require_preparable_source
    from backend.services.dataset_release.monthly_source_audit import MonthlySourceAuditError, SourceGateEvidence
    from backend.services.dataset_release.monthly_unified import SOURCE_GATES
    import pytest
    import json
    gates = [SourceGateEvidence(name, "snapshot", "expect.json", "read.json", 1, 1,
                                invalid_value_count=int(name == "suspend_limit")) for name in SOURCE_GATES]
    issues = tmp_path / "source-issues.ndjson"
    issues.write_text(json.dumps({"gate": "suspend_limit", "dataset": "stk_limit", "reason": "invalid_fact"}) + "\n")
    _require_preparable_source(gates=gates, issues_path=issues, cutoff=date(2026, 9, 30),
                               predecessor_cutoff=date(2026, 8, 31), changes=())
    issues.write_text(json.dumps({"gate": "suspend_limit", "dataset": "suspend_d", "reason": "suspend_conflicts_with_trade"}) + "\n")
    with pytest.raises(MonthlySourceAuditError):
        _require_preparable_source(gates=gates, issues_path=issues, cutoff=date(2026, 9, 30),
                                   predecessor_cutoff=date(2026, 8, 31), changes=())


@pytest.mark.parametrize("defect", [None, "null", "duplicate", "denominator"])
def test_prepared_limits_close_actual_month_rows_not_raw_nulls(tmp_path, monkeypatch, defect):
    import json
    from types import SimpleNamespace
    from backend.services.dataset_release import artifact_ready_build_source as build
    from backend.services.dataset_release import monthly_shared_components as shared
    from backend.services.dataset_release.monthly_frozen_source_audit import audit_prepared_limit_source
    from backend.services.dataset_release.monthly_source_audit import SourceGateEvidence, MonthlySourceAuditError

    days = (date(2026, 9, 1), date(2026, 9, 2))
    codes = ("000001.SZ", "000002.SZ")
    raw = GateCounter("suspend_limit")
    for code in codes:
        for day in days:
            raw.check((code, day), [{"pre_close": None}], valid=lambda _: False, dataset="stk_limit")
    (tmp_path / "raw-expect.json").write_text(json.dumps({"expected_keys_sha256": raw.expected_keys.hexdigest()}))
    gate = SourceGateEvidence("suspend_limit", "snapshot", "raw-expect.json", "raw-read.json", 4, 4, invalid_value_count=4)
    values = [{"ts_code": code, "trade_date": day, "pre_close": "10.00", "up_limit": "11.00", "down_limit": "9.00"}
              for day in days for code in codes]
    if defect == "null":
        values[0]["pre_close"] = None
    elif defect == "duplicate":
        values.append(dict(values[0]))
    elif defect == "denominator":
        (tmp_path / "raw-expect.json").write_text(json.dumps({"expected_keys_sha256": "f" * 64}))
    seen = []

    class Source:
        def __init__(self, **kwargs):
            pass
        def ordered_partitions(self, component, dataset, **kwargs):
            seen.append((dataset, kwargs))
            payload = [{"cal_date": day} for day in days] if dataset == "trading_calendar" else values
            return [SimpleNamespace(rows=payload)]

    monkeypatch.setattr(build, "ArtifactReadyBuildSource", Source)
    monkeypatch.setattr(shared, "_pit_intervals", lambda *_: [(code, days[0], days[-1]) for code in codes])
    frozen = SimpleNamespace(official_cutoff=date(2026, 9, 30), pit_snapshot=None, source_content_root="a" * 64,
                             partitions=(), artifact_ready_contract_ref=SimpleNamespace(as_dict=lambda: {"sha256": "b" * 64}))
    args = dict(cas=None, frozen=frozen, profile=None, raw_gate=gate, input_root=tmp_path, artifact_root=tmp_path)
    if defect == "denominator":
        with pytest.raises(MonthlySourceAuditError, match="denominator differs"):
            audit_prepared_limit_source(**args)
    else:
        result, artifacts = audit_prepared_limit_source(**args)
        assert result.expected_count == 4
        assert result.payload()["status"] == ("PASS" if defect is None else "BLOCKED")
        assert len(artifacts) == 3
        assert result.readback_ref != gate.readback_ref
    assert {dataset for dataset, _ in seen} == {"trading_calendar", "stk_limit"}
    assert all(kwargs["date_ranges"] == ((date(2026, 9, 1), date(2026, 9, 30)),) for _, kwargs in seen)


@pytest.mark.parametrize("defect", [None, "hash", "count"])
def test_provider_publication_identity_survives_source_freeze(defect):
    from backend.services.dataset_release.source_authority import _sanitize_refresh_audit_row, SourceAuditIncomplete
    from datetime import datetime, timezone
    proof = {"source": "tushare_response", "row_count": 3, "source_rows_sha256": "b" * 64}
    raw = {"data_source": "tushare", "status": "success", "quality_status": "ok", "row_count": 3,
           "written_rows": 3, "expected_rows": 3, "refreshed_at": datetime(2026, 10, 6, tzinfo=timezone.utc),
           "metadata": {"provider_publication": proof}}
    if defect == "hash":
        proof["source_rows_sha256"] = "invalid"
    elif defect == "count":
        raw["expected_rows"] = 2
    if defect:
        with pytest.raises(SourceAuditIncomplete, match="publication proof differs"):
            _sanitize_refresh_audit_row(raw)
    else:
        assert _sanitize_refresh_audit_row(raw)["provider_publication"] == proof
