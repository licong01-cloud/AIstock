"""Direct regression contracts for BUG-1946, without a database or services."""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.outcome_source import (
    HistoricalRangeOutcomeSourceError,
    HistoricalRangeSymbolPathRequestV1,
)
from backend.services.advisory_historical_range.outcome_file_source import (
    FileHistoricalRangeOutcomeSourceProvider,
    freeze_outcome_file_binding,
)


def _dataset(root: Path) -> Path:
    daily = root / "components/daily_bin_candidate"
    (daily / "calendars").mkdir(parents=True)
    (daily / "instruments").mkdir()
    (daily / "features/000001.sz").mkdir(parents=True)
    (daily / "calendars/day.txt").write_text("2026-01-19\n2026-01-20\n2026-01-21\n", encoding="utf-8")
    (daily / "instruments/stock_universe.txt").write_text("000001.SZ\t2026-01-19\t2026-01-21\n", encoding="utf-8")
    (daily / "meta_export.json").write_text("{}", encoding="utf-8")
    factor = root / "components/factor_h5_static_candidate_v2"
    factor.mkdir()
    (factor / "meta.json").write_text('{"schema_version":"qe_direct_factor_h5_static_v2"}', encoding="utf-8")
    index = pd.MultiIndex.from_product([pd.to_datetime(["2026-01-19", "2026-01-21"]), ["000001.SZ"]], names=["datetime", "instrument"])
    frame = pd.DataFrame({"open": [5., 6.], "high": [5.5, 6.5], "low": [4.5, 5.5], "close": [5.25, 6.25], "factor": [.5, .5]}, index=index)
    frame.to_hdf(factor / "daily_pv.h5", key="data", format="table", data_columns=True)
    for name, values in (("up_limit_price", [11., np.nan, 14.]), ("down_limit_price", [9., np.nan, 10.])):
        np.asarray([0, *values], dtype="<f4").tofile(daily / f"features/000001.sz/{name}.day.bin")
    suspend = root / "components/suspend_d_daily_candidate_v2"
    suspend.mkdir()
    pd.DataFrame({"trade_date": ["2026-01-20"], "ts_code": ["000001.SZ"], "suspend_type": ["S"], "suspend_timing": [None]}).to_parquet(suspend / "suspend_d.parquet")
    sector = root / "components/sector_context_candidate_v1"
    sector.mkdir()
    pd.DataFrame({"instrument": ["000001.SZ"], "start_date": ["2026-01-19"], "end_date": ["2026-01-21"], "l2_code_id": [1]}).to_parquet(sector / "sector_membership_spans.parquet")
    (sector / "sector_code_map.json").write_text('{"entries":[{"l2_code_id":1,"canonical_l2_code":"L2-A"}]}', encoding="utf-8")
    components = {}
    for key, relative in {
        "day_calendar": "components/daily_bin_candidate/calendars/day.txt",
        "day_meta_export": "components/daily_bin_candidate/meta_export.json",
        "day_selection_universe": "components/daily_bin_candidate/instruments/stock_universe.txt",
        "factor_meta": "components/factor_h5_static_candidate_v2/meta.json",
        "suspend_data": "components/suspend_d_daily_candidate_v2/suspend_d.parquet",
        "sector_membership_spans": "components/sector_context_candidate_v1/sector_membership_spans.parquet",
        "sector_code_map": "components/sector_context_candidate_v1/sector_code_map.json",
    }.items():
        components[key] = {"path": relative, "sha256": hashlib.sha256((root / relative).read_bytes()).hexdigest()}
    manifest = root / "qe_dataset_manifest.json"
    manifest.write_text(json.dumps({"release_id": "test-release", "components": components}), encoding="utf-8")
    profile = root / "active.json"
    profile.write_text(json.dumps({"schema_version": "aistock_active_dataset_profile_v3", "generation": "test-generation", "release_id": "test-release", "cutoff": "2026-01-21", "controller_paths": {"candidate_root": str(root)}, "components": {"dataset_manifest_file_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest()}}), encoding="utf-8")
    return profile


def _path(provider, *, end=date(2026, 1, 21)):
    observed = datetime(2026, 1, 21, 23, 59, tzinfo=UTC)
    bundle = provider.resolve_source_revision_bundle(symbol="000001.SZ", start_trade_date=date(2026, 1, 19), end_trade_date=end, label_as_of_ts=observed)
    return provider.load_symbol_path(HistoricalRangeSymbolPathRequestV1(symbol="000001.SZ", start_trade_date=date(2026, 1, 19), end_trade_date=end, label_as_of_trade_date=observed.date(), source_available_at=observed, price_source=bundle.price_source, adjustment_source=bundle.adjustment_source, tradability_source=bundle.tradability_source, expected_source_revision_set_hash=bundle.source_revision_set.source_revision_set_hash))


def test_file_batch_reuses_window_and_preserves_raw_prices_and_suspension(tmp_path, monkeypatch):
    import psycopg2
    monkeypatch.setattr(psycopg2, "connect", lambda **_: pytest.fail("file computation opened DB"))
    binding = freeze_outcome_file_binding(_dataset(tmp_path))
    provider = FileHistoricalRangeOutcomeSourceProvider(binding=binding, start_trade_date=date(2026, 1, 19), label_as_of_trade_date=date(2026, 1, 21))
    first = _path(provider)
    second = _path(provider, end=date(2026, 1, 20))
    assert provider.batch_read_count == 1
    assert first.price_path.bars[0].open_li == Decimal("10000")
    assert first.price_path.bars[0].normalized(Decimal("10000")) == Decimal("5")
    suspended = first.price_path.bars[1]
    assert suspended.open_li is None and not suspended.entry_executable and not suspended.sell_executable
    assert len(second.price_path.bars) == 2 and not first.missing_trade_dates
    assert canonical_json_sha256(binding) != first.source_revision_set_hash


def test_file_binding_drift_and_undeclared_missing_prices_fail_closed(tmp_path):
    profile = _dataset(tmp_path)
    binding = freeze_outcome_file_binding(profile)
    parquet = tmp_path / "components/suspend_d_daily_candidate_v2/suspend_d.parquet"
    parquet.write_bytes(parquet.read_bytes() + b"drift")
    with pytest.raises(HistoricalRangeOutcomeSourceError, match="hash"):
        FileHistoricalRangeOutcomeSourceProvider(binding=binding, start_trade_date=date(2026, 1, 19), label_as_of_trade_date=date(2026, 1, 21))


def test_file_path_rejects_future_label_access(tmp_path):
    binding = freeze_outcome_file_binding(_dataset(tmp_path))
    provider = FileHistoricalRangeOutcomeSourceProvider(binding=binding, start_trade_date=date(2026, 1, 19), label_as_of_trade_date=date(2026, 1, 20))
    with pytest.raises(HistoricalRangeOutcomeSourceError, match="window"):
        _path(provider)


def test_file_path_refuses_a_substituted_revision_set_hash(tmp_path):
    provider = FileHistoricalRangeOutcomeSourceProvider(binding=freeze_outcome_file_binding(_dataset(tmp_path)),
        start_trade_date=date(2026, 1, 19), label_as_of_trade_date=date(2026, 1, 21))
    captured = []
    original = provider.load_symbol_path
    def capture(request):
        captured.append(request)
        return original(request)
    provider.load_symbol_path = capture
    _path(provider)
    with pytest.raises(HistoricalRangeOutcomeSourceError, match="revision"):
        original(captured[0].model_copy(update={"expected_source_revision_set_hash": "f" * 64}))


def test_old_request_hash_is_unchanged_and_file_source_is_in_new_identity(tmp_path):
    from backend.services.advisory_historical_range.models import HistoricalRangeOutcomeRefreshRequestV1
    from backend.tests.advisory_historical_range.test_r4_outcome_service import _request_contract
    old = _request_contract()
    payload = old.model_dump(mode="json", exclude={"request_hash", "file_source_binding"})
    assert old.request_hash == canonical_json_sha256(payload)
    assert HistoricalRangeOutcomeRefreshRequestV1.model_validate({**payload, "request_hash": old.request_hash}) == old
    binding = freeze_outcome_file_binding(_dataset(tmp_path))
    new = HistoricalRangeOutcomeRefreshRequestV1.model_validate({**payload, "file_source_binding": binding})
    assert new.request_hash != old.request_hash
    assert HistoricalRangeOutcomeRefreshRequestV1.model_validate(new.model_dump(mode="json")) == new


def test_ordinary_missing_price_without_full_day_suspend_is_not_filled(tmp_path):
    profile = _dataset(tmp_path)
    suspend = tmp_path / "components/suspend_d_daily_candidate_v2/suspend_d.parquet"
    frame = pd.read_parquet(suspend)
    frame["suspend_timing"] = "09:30-10:00"
    frame.to_parquet(suspend)
    manifest_path = tmp_path / "qe_dataset_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["components"]["suspend_data"]["sha256"] = hashlib.sha256(suspend.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest))
    contents = json.loads(profile.read_text())
    contents["components"]["dataset_manifest_file_sha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    profile.write_text(json.dumps(contents))
    provider = FileHistoricalRangeOutcomeSourceProvider(binding=freeze_outcome_file_binding(profile), start_trade_date=date(2026, 1, 19), label_as_of_trade_date=date(2026, 1, 21))
    with pytest.raises(HistoricalRangeOutcomeSourceError, match="ordinary tradable"):
        _path(provider)


def test_file_industry_and_pit_pool_are_versioned_and_point_in_time(tmp_path):
    provider = FileHistoricalRangeOutcomeSourceProvider(binding=freeze_outcome_file_binding(_dataset(tmp_path)), start_trade_date=date(2026, 1, 19), label_as_of_trade_date=date(2026, 1, 21))
    assert provider.load_industry_at_t(symbol="000001.SZ", decision_trade_date=date(2026, 1, 19))[0] == "SW_L2_FILE_V1:L2-A"
    assert provider.load_pit_eligible_symbols(universe_key="aistock_equity_pit_canonical_v2", decision_trade_date=date(2026, 1, 19))[0] == ("000001.SZ",)
    with pytest.raises(HistoricalRangeOutcomeSourceError, match="missing or conflicting"):
        provider.load_industry_at_t(symbol="000001.SZ", decision_trade_date=date(2026, 1, 18))


def test_validated_artifact_cache_reuses_reads_without_sharing_mutable_payload(tmp_path, monkeypatch):
    from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
    from backend.services.advisory_historical_range.models import HistoricalRangeArtifactKind, HistoricalRangeContractError
    from backend.services.advisory_historical_range.outcome_file_inputs import HistoricalRangeOutcomeReadCache

    store = HistoricalRangeOutcomeReadCache(root=tmp_path)
    ref = store.publish_payload(artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="test_v1", payload_schema_version="test_v1",
        resolved_request_hash="a" * 64, payload={"nested": {"value": 1}}).ref
    original = HistoricalRangeArtifactStore.load
    calls = []
    def counted(self, ref):
        calls.append(ref)
        return original(self, ref)
    monkeypatch.setattr(HistoricalRangeArtifactStore, "load", counted)
    store.load(ref).payload["nested"]["value"] = 2
    assert store.load(ref).payload["nested"]["value"] == 1
    assert len(calls) == 1
    forged = ref.model_copy(update={"payload_sha256": "f" * 64})
    with pytest.raises(HistoricalRangeContractError):
        store.load(forged)
    path = tmp_path / ref.relative_path
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="changed"):
        store.load(ref)


def test_frozen_command_exact_retry_does_not_resolve_new_profile(tmp_path):
    from types import SimpleNamespace
    from backend.services.advisory_historical_range.api_models import HistoricalRangeRefreshOutcomesRequest
    from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
    from backend.services.advisory_historical_range.runtime_factories import HistoricalRangeR5OutcomeRequestFactory
    from backend.tests.advisory_historical_range.test_r4_outcome_service import _request_contract

    old = _request_contract()
    calls = []
    class Registry:
        def resolve(self, **kwargs):
            calls.append("resolve")
            return ((old.range_run_ids[0], SimpleNamespace(bundle_ref=old.policy_bundle_ref,
                bundle=SimpleNamespace(horizons=(1,), policy_bundle_hash=old.policy_bundle_hash))),)
        def register_frozen_policy_refs(self, refs):
            calls.append("restore")
    binding = freeze_outcome_file_binding(_dataset(tmp_path / "dataset"))
    def freeze():
        calls.append("freeze")
        return binding
    factory = HistoricalRangeR5OutcomeRequestFactory(policy_registry=Registry(),
        identities=SimpleNamespace(outcome_producer_hash="e" * 64), file_binding_factory=freeze)
    store = HistoricalRangeArtifactStore(root=tmp_path / "artifacts")
    api = HistoricalRangeRefreshOutcomesRequest(operation_idempotency_key="frozen-command",
        expected_row_version=1, label_as_of_trade_date=date(2026, 1, 21),
        range_run_ids=list(old.range_run_ids), horizons=[1])
    first = factory.build_frozen(old.batch_id, api, artifact_store=store, resolved_request_hash="a" * 64)
    # The active profile could change or vanish after command registration.
    factory._file_binding_factory = lambda: pytest.fail("exact retry rebound active profile")
    second = factory.build_frozen(old.batch_id, api, artifact_store=store, resolved_request_hash="a" * 64)
    assert first == second and calls == ["resolve", "freeze", "restore"]
    with pytest.raises(ValueError, match="API request"):
        factory.build_frozen(old.batch_id, api.model_copy(update={"horizons": [2]}),
            artifact_store=store, resolved_request_hash="a" * 64)
    with pytest.raises(ValueError, match="identity"):
        factory.build_frozen(old.batch_id, api, artifact_store=store, resolved_request_hash="b" * 64)


def test_file_and_native_row_adapters_preserve_identical_price_paths(tmp_path):
    from backend.services.advisory_historical_range.outcome_source import HistoricalRangeOutcomeSourceProvider

    provider = FileHistoricalRangeOutcomeSourceProvider(binding=freeze_outcome_file_binding(_dataset(tmp_path)),
        start_trade_date=date(2026, 1, 19), label_as_of_trade_date=date(2026, 1, 21))
    file_path = _path(provider).price_path
    # Same raw observations in the existing DB row contract; no alternative math.
    rows = provider._load_symbol_rows(symbol="000001.SZ", start_trade_date=date(2026, 1, 19), end_trade_date=date(2026, 1, 21))
    class NativeRows(HistoricalRangeOutcomeSourceProvider):
        def _load_symbol_rows(self, **kwargs):
            return rows
        def _trading_dates(self, *_args):
            return tuple(row["trade_date"] for row in rows)
    native_path = _path(NativeRows()).price_path
    assert [(bar.trade_date, bar.open_li, bar.close_li, bar.adj_factor, bar.entry_executable, bar.sell_executable)
            for bar in file_path.bars] == [(bar.trade_date, bar.open_li, bar.close_li, bar.adj_factor, bar.entry_executable, bar.sell_executable)
            for bar in native_path.bars]


def test_parent_stops_heartbeat_if_unexpected_failure_escapes(monkeypatch):
    from types import SimpleNamespace
    from backend.services.advisory_historical_range import service as module

    events = []
    claim = {"status": "PENDING", "request_payload_sha256": "a" * 64}
    runtime = SimpleNamespace(artifact_store=object(), repository=object(),
        query=SimpleNamespace(get_operation_internal=lambda _id: claim))
    application = module.ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: runtime)
    class Heartbeat:
        def __init__(self, **_kwargs):
            pass
        def start(self):
            events.append("start")
        def stop(self):
            events.append("stop")
    class Interrupted(BaseException):
        pass
    monkeypatch.setattr(module, "_OutcomeParentLeaseHeartbeatSupervisor", Heartbeat)
    monkeypatch.setattr(application, "_claim_outcome_parent", lambda **_kwargs: claim)
    def interrupted(**_kwargs):
        raise Interrupted()
    monkeypatch.setattr(application, "_execute_claimed_outcome_plan", interrupted)
    with pytest.raises(Interrupted):
        application._run_outcome_plan(runtime=runtime, plan=SimpleNamespace(request_hash="a" * 64),
            operation_id="op", batch_id="batch", worker_id="worker")
    assert events == ["start", "stop"]


def test_parent_finalization_validates_while_heartbeat_remains_active(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
    from backend.services.advisory_historical_range.models import HistoricalRangeOperationStatus, HistoricalRangeOperationType
    from backend.services.advisory_historical_range.repository import PostgresHistoricalRangeRepository

    events = []
    class BoundaryReached(Exception):
        pass
    class Connection:
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            pass
        def cursor(self, **_kwargs):
            return self
    repo = PostgresHistoricalRangeRepository(conn_factory=Connection,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path))
    monkeypatch.setattr(repo, "_get_operation_artifact_identity", lambda _id: "a" * 64)
    monkeypatch.setattr(repo, "_get_operation_type", lambda _id: HistoricalRangeOperationType.REFRESH_OUTCOMES)
    monkeypatch.setattr(repo, "_validate_operation_attempt_artifacts", lambda **_kwargs: events.append("validate_attempt"))
    monkeypatch.setattr(repo, "_validate_operation_result_artifact", lambda **_kwargs: events.append("validate_result"))
    monkeypatch.setattr(repo, "_lock_row", lambda *_args, **_kwargs: {})
    def check_version(_row, version, **_kwargs):
        assert version == 9
        events.append("cas")
        raise BoundaryReached()
    monkeypatch.setattr(repo, "_require_row_version", check_version)
    def finalize():
        events.append("stop")
        return {"operation_id": "op", "attempt_no": 1, "fencing_token": 2,
            "worker_id": "worker", "lease_token": "token", "row_version": 9}
    attempt = SimpleNamespace(worker_id="worker", lease_token="token")
    with pytest.raises(BoundaryReached):
        repo.transition_operation(operation_id="op", expected_row_version=1,
            target_status=HistoricalRangeOperationStatus.COMPLETED, attempt_no=1,
            fencing_token=2, attempt=attempt, result_ref=object(), finalize_lease=finalize)
    assert events == ["validate_attempt", "validate_result", "stop", "cas"]
