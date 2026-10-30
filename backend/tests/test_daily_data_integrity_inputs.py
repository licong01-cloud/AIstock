from contextlib import contextmanager
from datetime import date

import pandas as pd
import pytest

from backend.services.advisory_historical_range.inference_inputs import InferenceInputSession
from backend.services.data_integrity.release import pin_payload, release_day
from backend.services.data_integrity.writes import fact_day


def scoped_missing_example(dataset="bak_basic", missing="002731.SZ", members=("000001.SZ",)):
    from backend.services.data_integrity.advisory_scope import scope_identity
    from backend.services.data_integrity.catalog import RULE_VERSION, SCOPE_VERSION
    from backend.services.data_integrity.consumers import ConsumerDependencies, project_readiness
    day = date(2026, 10, 29)
    validation = {"state": "INCOMPLETE", "checks": {"coverage_basis": "independent_dated_traded_stock_keys"},
        "counts": [{"unit": "security_date", "expected": 3, "valid": 2, "legal_na": 0,
                    "missing": 1, "invalid": 0, "unverified": 0}],
        "gaps": [{"classification": "MISSING", "symbols": [missing]}]}
    row = {"dataset": dataset, "trade_date": str(day), "data_revision": 3, "validated_revision": 3,
           "validation_status": "INCOMPLETE", "validation_id": "audited-gap-v3",
           "scope_version": SCOPE_VERSION, "rule_version": RULE_VERSION, "metadata": {"validation": validation}}
    scope = scope_identity(members, day)
    deps = ConsumerDependencies((dataset,), ("cp_winner_rate",), 1, "dependency")
    def project():
        return project_readiness(deps, [day], {(day, dataset): row}, pit_binding={"universe_key": "canonical"},
                                 input_scope=scope, members=set(members))
    return row, scope, project


def test_advisory_outside_decision_pit_does_not_green_source_or_other_consumers():
    from copy import deepcopy
    from backend.services.data_integrity.consumers import ConsumerDependencies, project_readiness
    row, scope, project = scoped_missing_example()
    before = deepcopy(row)
    result = project()
    assert result["prediction_ready"] is True and row == before
    assert result["partition_refs"][0]["consumer_projection"]["outside_scope"] == [
        {"symbol": "002731.SZ", "classification": "MISSING"}]
    day = date.fromisoformat(row["trade_date"])
    ordinary = project_readiness(ConsumerDependencies(("bak_basic",), (), 1, "dep"), [day],
        {(day, "bak_basic"): row}, pit_binding={"universe_key": "canonical"})
    assert ordinary["prediction_ready"] is False
    assert result["input_scope"] == scope


def test_restored_decision_member_keeps_its_full_historical_input_obligation():
    # Eligibility on the fact date must never remove history actually consumed
    # by a member that is eligible again on the prediction decision date.
    _, _, project = scoped_missing_example(members=("002731.SZ",))
    assert project()["prediction_ready"] is False


@pytest.mark.parametrize("damage", ["dirty", "old_rule", "unknown", "error", "truncated", "duplicate",
                                   "unverified", "wrong_basis", "no_metadata", "aggregate"])
def test_advisory_scope_never_exempts_unproven_or_aggregate_source(damage):
    row, _, project = scoped_missing_example()
    validation = row["metadata"]["validation"]
    if damage == "dirty":
        row["validated_revision"] = 2
    if damage == "old_rule":
        row["rule_version"] = "old"
    if damage == "unknown":
        row["validation_status"] = "UNVERIFIED"
    if damage == "error":
        row["validation_status"] = "ERROR"
    if damage == "truncated":
        validation["gaps"] = []
    if damage == "duplicate":
        validation["gaps"] *= 2
    if damage == "unverified":
        validation["counts"][0].update(valid=1, unverified=1)
    if damage == "wrong_basis":
        validation["checks"] = {}
    if damage == "no_metadata":
        row["metadata"] = {}
    if damage == "aggregate":
        validation["counts"] *= 2
    assert project()["prediction_ready"] is False


def test_validated_chip_gap_is_visible_missing_input_but_invalid_chip_still_blocks():
    row, _, project = scoped_missing_example("cyq_perf", "000001.SZ")
    result = project()
    proof = result["partition_refs"][0]["consumer_projection"]
    assert result["prediction_ready"] and proof["missing_as_nan"] == ["000001.SZ"]
    assert proof["source_state"] == "INCOMPLETE" and not proof["outside_scope"]
    row["metadata"]["validation"]["gaps"][0]["classification"] = "INVALID"
    row["metadata"]["validation"]["counts"][0].update(missing=0, invalid=1)
    assert not project()["prediction_ready"]


@pytest.mark.parametrize("damage", [None, "universe", "revision", "validation", "source_gap"])
def test_projected_capture_binds_scope_and_exact_missing_evidence(damage):
    from backend.services.data_integrity.inputs import InputReadinessError, ready_binding, verify_capture
    row, scope, project = scoped_missing_example("cyq_perf", "000001.SZ")
    ready = project()
    ready.update(binding_id="pkg", manifest_sha256="a"*64, data_date=row["trade_date"])
    binding = ready_binding(ready)
    capture = {"start": row["trade_date"], "end": row["trade_date"], "ledger_available": True,
               "partition_refs": [row], "snapshot_kind": "REPEATABLE_READ_READ_ONLY",
               "external_fallback_allowed": False, "pit_identity": ready["global_pit"],
               "universe_sha256": scope["universe_sha256"]}
    if damage == "universe":
        capture["universe_sha256"] = "wrong"
    if damage == "revision":
        row.update(data_revision=4, validated_revision=4)
    if damage == "validation":
        row["validation_id"] = "new"
    if damage == "source_gap":
        row["metadata"]["validation"]["gaps"][0]["symbols"] = ["000002.SZ"]
    if damage:
        with pytest.raises(InputReadinessError):
            verify_capture(binding, capture)
    else:
        verify_capture(binding, capture)


@pytest.mark.parametrize("value", [float("nan"), 0.0, 12.0])
def test_missing_chip_raw_input_is_never_filled(value):
    from backend.services.data_integrity.advisory_scope import verify_missing_chip_values
    _, _, project = scoped_missing_example("cyq_perf", "000001.SZ")
    raw = pd.DataFrame({"cp_winner_rate": [value], "cp_weight_avg": [value]},
        index=pd.MultiIndex.from_tuples([(pd.Timestamp("2026-10-29"), "SZ000001")],
                                       names=["datetime", "instrument"]))
    if pd.isna(value):
        verify_missing_chip_values(project(), raw)
        assert raw.isna().all().all()
    else:
        with pytest.raises(ValueError, match="filled or omitted"):
            verify_missing_chip_values(project(), raw)


def test_scoped_gaps_use_shared_dated_source_aliases(monkeypatch):
    from types import SimpleNamespace
    from backend.data_service import security_source_identity as identity
    row, _, project = scoped_missing_example(missing="000999.SZ")
    alias = SimpleNamespace(source_dataset="market.bak_basic", source_ts_code="000999.SZ",
        canonical_ts_code="000001.SZ", effective_start=date(2026, 1, 1), effective_end=date(2026, 12, 31))
    monkeypatch.setattr(identity, "load_default_security_source_identity_manifest", lambda: SimpleNamespace(rows=[alias]))
    assert not project()["prediction_ready"]  # alias belongs to an actual member
    alias.effective_end = date(2026, 10, 28)
    assert project()["prediction_ready"]  # expired alias cannot rewrite identity


@pytest.mark.parametrize("changed", [False, True])
def test_publish_recheck_independently_recomputes_scope(monkeypatch, changed):
    from dataclasses import dataclass
    from backend.services import canonical_equity_pit as pit_module
    from backend.services.data_integrity import advisory_scope, inputs
    row, scope, project = scoped_missing_example()
    ready = project()
    ready.update(binding_id="pkg", manifest_sha256="a"*64, data_date=row["trade_date"])
    binding = inputs.ready_binding(ready)

    @dataclass
    class Pit:
        universe_key: str = "canonical"

    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def execute(self, *_): pass
        def fetchone(self): return "a"*64, "ACTIVE"

    class Connection:
        def cursor(self): return Cursor()

    monkeypatch.setattr(pit_module.CanonicalPitAuthorityResolver, "resolve_live_binding", lambda _: Pit())
    monkeypatch.setattr(inputs, "partition_pins", lambda *a, **kw: [row])
    # Even if a supplied hash claims no change, reloaded members must prove the
    # exemption again; this also covers an erroneously classified source alias.
    monkeypatch.setattr(advisory_scope, "resolve_scope", lambda *a: (
        scope, {"000001.SZ", "002731.SZ"} if changed else {"000001.SZ"}))
    if changed:
        with pytest.raises(inputs.InputReadinessError, match="缺失投影"):
            inputs.verify_current(Connection(), {"pkg": binding})
    else:
        inputs.verify_current(Connection(), {"pkg": binding})


def test_partition_capture_keeps_validation_and_adapts_dataset_sequence(monkeypatch):
    from backend.services.data_integrity import inputs
    row, _, _ = scoped_missing_example()
    row["trade_date"] = date.fromisoformat(row["trade_date"])
    seen = []

    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def execute(self, query, params): seen.append((query, params))
        def fetchall(self): return [row]

    class Connection:
        def cursor(self, **_): return Cursor()

    monkeypatch.setattr(inputs, "schema_installed", lambda _: True)
    actual = inputs.partition_pins(Connection(), row["trade_date"], row["trade_date"], ("bak_basic",))
    assert "metadata->'validation'" in seen[0][0]
    assert seen[0][1][-1] == ["bak_basic"]
    assert actual[0]["metadata"] == row["metadata"]


def frame(days):
    return pd.DataFrame({"close": list(range(1, len(days)+1))},
        index=pd.MultiIndex.from_product([pd.to_datetime(days), ["000001.SZ"]], names=["datetime", "instrument"]))


def pin(day, revision=1):
    return {"dataset": "kline_daily_raw", "trade_date": day, "data_revision": revision}


def test_capture_reuses_prefix_but_not_a_revised_old_date():
    session = InferenceInputSession(consumer_scope="ADVISORY", run_id="test", source_identity="db", max_bytes=10000)
    current = [pin("2026-09-01"), pin("2026-09-02")]

    @contextmanager
    def capture(start, end):
        yield {"partition_refs": list(current)}

    session.capture_factory = capture
    calls = []

    def read(codes, start, end):
        calls.append((start, end))
        return frame(pd.date_range(start, end).date)

    def capture_read(end):
        with session.capture(date(2026, 9, 1), end):
            return session.read(dataset="market.kline_daily_raw", members=["000001.SZ"],
                start=date(2026, 9, 1), end=end, loader=read)

    capture_read(date(2026, 9, 2))
    current.append(pin("2026-09-03"))
    capture_read(date(2026, 9, 3))
    assert calls == [(date(2026, 9, 1), date(2026, 9, 2)), (date(2026, 9, 3), date(2026, 9, 3))]
    current[1] = pin("2026-09-02", 2)
    capture_read(date(2026, 9, 3))
    assert calls[-1] == (date(2026, 9, 2), date(2026, 9, 3))
    assert session.retained_bytes > 0


def test_failed_capture_does_not_reuse_partial_input():
    session = InferenceInputSession(consumer_scope="LOCALSIM", run_id="test", source_identity="db", max_bytes=10000)
    with pytest.raises(RuntimeError):
        with session.capture(date(2026, 9, 1), date(2026, 9, 1)):
            session.read(dataset="price", members=["000001.SZ"], start=date(2026, 9, 1),
                end=date(2026, 9, 1), loader=lambda *_: frame(["2026-09-01"]))
            raise RuntimeError("failure after one reader")
    assert session.retained_bytes == 0


def test_release_inclusion_is_not_inferred_from_cutoff_or_source_green():
    reference = {"dataset": "daily_basic", "fact_date": "2026-09-01", "data_revision": 2,
                 "validation_id": "v", "scope_version": "s", "rule_version": "r"}
    day = {"fact_date": "2026-09-01", "partitions": [reference], "all_sync_complete": True}
    assert release_day(day, cutoff=date(2026, 9, 30), pins=None, identity={})["release_contains_revision"] is None
    assert release_day(day, cutoff=date(2026, 9, 30), pins=[reference], identity={})["release_contains_revision"] is True
    assert release_day(day, cutoff=date(2026, 9, 30), pins=[{**reference, "data_revision": 1}], identity={})["release_contains_revision"] is False
    with_event = {**day, "partitions": [reference, {**reference, "dataset": "anns_metadata"}]}
    # Unknown/unexported event pins do not conceal a proven stale daily_basic.
    assert release_day(with_event, cutoff=date(2026, 9, 30), pins=[{**reference, "data_revision": 1}], identity={})["release_contains_revision"] is False
    assert release_day(day, cutoff=date(2026, 8, 31), pins=None, identity={})["release_coverage_state"] == "OUTSIDE_CUTOFF"
    assert pin_payload([reference], source_bundle_sha256="a"*64)["partition_refs"] == [reference]
    with pytest.raises(ValueError, match="duplicate"):
        pin_payload([reference, reference], source_bundle_sha256="a"*64)


@pytest.mark.parametrize("value", ["2026-09-01", "20260901", "2026-09-01T09:31:00+08:00"])
def test_writer_normalizes_actual_fact_day(value):
    assert fact_day(value) == date(2026, 9, 1)


def test_monthly_provider_green_cannot_override_dirty_canonical_revision():
    from backend.services.data_integrity.catalog import RULE_VERSION, SCOPE_VERSION
    from backend.services.dataset_release.source_authority import SourceAuditIncomplete, SourceRefreshAuditLedger
    canonical = {"data_source": "canonical_db", "status": "success", "quality_status": "ok",
        "error_present": False, "audit_payload_sha256": "c"*64, "integrity_revision": {
            "data_revision": 2, "validated_revision": 1, "validation_status": "VALID", "validation_id": "v1",
            "scope_version": SCOPE_VERSION, "rule_version": RULE_VERSION}}
    provider = {"data_source": "tushare", "status": "success", "quality_status": "ok",
                "error_present": False, "audit_payload_sha256": "b"*64}
    day = date(2026, 9, 1)
    ledger = SourceRefreshAuditLedger({("daily_basic", day): (provider, canonical)}, (day,),
        {"daily_basic": ("tushare",)}, {"daily_basic": ("ok",)})
    with pytest.raises(SourceAuditIncomplete, match="successful authority"):
        ledger.partition_digest("daily_basic", day, day)
    canonical["integrity_revision"]["validated_revision"] = 2
    assert len(ledger.partition_digest("daily_basic", day, day)) == 64
    pins = ledger.as_receipt(profile="shared", cutoff=day)["integrity_revision_pins"]
    assert pins[0]["data_revision"] == 2 and pins[0]["fact_date"] == str(day)


def test_newly_tracked_old_partition_cannot_reuse_untracked_cached_values():
    session = InferenceInputSession(consumer_scope="LOCALSIM", run_id="test", source_identity="db", max_bytes=10000)
    day = date(2026, 9, 1)
    session.read(dataset="market.kline_daily_raw", members=["000001.SZ"], start=day, end=day, loader=lambda *_: frame([day]))
    assert session.retained_bytes
    session._reconcile_revisions([pin(str(day))])
    assert not session.retained_bytes


def test_optional_monthly_worker_configuration_failure_does_not_abort_api_startup(tmp_path):
    from backend.services.dataset_release.monthly_worker_supervisor import start_monthly_worker_nonfatal

    def invalid(**_kwargs):
        raise ValueError("invalid producer configuration")
    supervisor, status = start_monthly_worker_nonfatal(project_root=tmp_path, factory=invalid)
    assert supervisor is None and status["status"] == "ERROR"
    assert status["reason_code"] == "MONTHLY_WORKER_STARTUP_FAILED"


@pytest.mark.parametrize("dynamic", [False, True])
def test_dependencies_use_frozen_factor_bytes_and_actual_window(tmp_path, dynamic):
    from backend.services.data_integrity.consumers import DependencyUnknown, package_dependencies
    from backend.services.strategy_package.models import AlphaMode, FactorAsset
    from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
    from backend.tests.strategy_package.test_advisory_input_projection import _manifest, _component, _runtime

    store = LocalPackageAssetStore(tmp_path / "assets")
    code = b"raise RuntimeError('must never execute')\ndef compute(data):\n    return data['db_volume_ratio']\n"
    if dynamic:
        code += b"def hidden_field(df, field):\n    return df[field]\n"
    blob = store.put(code, kind="factor_code")
    manifest = _manifest(alpha_mode=AlphaMode.SINGLE_ALPHA, components=[_component("single", ["factor_a"])],
        factors=[FactorAsset(factor_id="factor_a", factor_name="Momentum_120D", asset_ref=blob.uri, sha256=blob.sha256)],
        runtime_assets=_runtime())
    manifest.model_asset[0].model_type = "LGBModel"
    if dynamic:
        with pytest.raises(DependencyUnknown, match="deterministically"):
            package_dependencies(manifest, store)
        return
    result = package_dependencies(manifest, store)
    assert "daily_basic" in result.datasets and "margin_detail" not in result.datasets
    assert result.window_trading_days >= 125
    manifest.model_asset[0].model_type = "GeneralPTNN"
    with pytest.raises(DependencyUnknown, match="sequence window"):
        package_dependencies(manifest, store)


def ready_example(package="pkg_test"):
    from backend.services.data_integrity.consumers import ConsumerDependencies, project_readiness
    from backend.services.data_integrity.catalog import RULE_VERSION, SCOPE_VERSION
    from backend.services.data_integrity.inputs import ready_binding
    days = [date(2026, 8, 13), date(2026, 8, 14)]
    rows = {(d, "kline_daily_raw"): {"dataset": "kline_daily_raw", "trade_date": str(d),
        "data_revision": 2, "validated_revision": 2, "validation_status": "VALID", "validation_id": str(d),
        "scope_version": SCOPE_VERSION, "rule_version": RULE_VERSION} for d in days}
    ready = project_readiness(ConsumerDependencies(("kline_daily_raw",), ("close",), 2, "dependency"), days,
        rows, pit_binding={"universe_key": "canonical", "rule_version": "v2"})
    ready.update(binding_id=package, manifest_sha256="a"*64, data_date=str(days[-1]), all_sync_complete=False)
    binding = ready_binding(ready)
    capture = dict(start=str(days[0]), end=str(days[-1]), ledger_available=True, partition_refs=list(rows.values()),
        snapshot_kind="REPEATABLE_READ_READ_ONLY", external_fallback_allowed=False,
        pit_identity=ready["global_pit"], readiness_binding_sha256=binding["binding_sha256"])
    return ready, binding, capture


def test_binding_ignores_global_completion_and_clock_but_changes_artifact_key_on_revision():
    from copy import deepcopy
    from backend.services.data_integrity.inputs import ready_binding, verify_capture
    from backend.services.strategy_package.selection_artifact import selection_artifact_runtime_hash_v2
    ready, binding, capture = ready_example()
    verify_capture(binding, capture)
    assert ready_binding({**ready, "as_of": "another observation", "all_sync_complete": True}) == binding
    changed = deepcopy(ready)
    changed["partition_refs"][0]["data_revision"] += 1
    second = ready_binding(changed)
    def key(b): return selection_artifact_runtime_hash_v2({"selection_artifact_config": {"input_readiness_by_package": {"pkg_test": b}}})
    assert key(binding) != key(second)


@pytest.mark.parametrize("kind", ["missing", "dirty", "revision", "validation", "scope", "fallback", "window", "pit", "untracked"])
def test_actual_capture_rejects_source_drift(kind):
    from backend.services.data_integrity.inputs import InputReadinessError, verify_capture
    _, binding, capture = ready_example()
    if kind == "missing":
        capture["partition_refs"].pop()
    if kind == "dirty":
        capture["partition_refs"][0]["validated_revision"] = 1
    if kind == "revision":
        capture["partition_refs"][0].update(data_revision=3, validated_revision=3)
    if kind == "validation":
        capture["partition_refs"][0]["validation_status"] = "ERROR"
    if kind == "scope":
        capture["partition_refs"][0]["scope_version"] = "foreign"
    if kind == "fallback":
        capture["external_fallback_allowed"] = True
    if kind == "window":
        capture["start"] = "2026-08-14"
    if kind == "pit":
        capture["pit_identity"] = {"universe_key": "legacy"}
    if kind == "untracked":
        capture["ledger_available"] = False
    with pytest.raises(InputReadinessError):
        verify_capture(binding, capture)


def test_static_alias_join_and_chained_window_without_executing_code():
    from backend.services.data_integrity.dependency_source import factor_fields, factor_window, field_sources
    source = '''
raise RuntimeError("no execution")
a = pd.read_hdf("daily_pv.h5", key="data")
b = pd.read_parquet("static_factors.parquet", columns=["db_volume_ratio"])
joined = a.join(b)
c = joined["close"].rolling(250).max().shift(3)
result = c.rolling(20).mean() / joined["db_volume_ratio"]
'''
    assert factor_fields(source, field_sources()) == {"close", "db_volume_ratio"}
    assert factor_window(source) == 272


@pytest.mark.parametrize("expression", ["df[field]", "getattr(df, field)", "df.loc[:, field]", "df.values", 'df.eval("hidden_field + close")'])
def test_partial_known_source_cannot_conceal_dynamic_dependency(expression):
    from backend.services.data_integrity.dependency_source import DependencyUnknown, factor_fields, field_sources
    with pytest.raises(DependencyUnknown):
        factor_fields(f'df = pd.read_hdf("daily_pv.h5")\nx = df["close"]\ny = {expression}', field_sources())


def test_dynamic_file_root_cannot_be_certified_from_its_basename():
    from backend.services.data_integrity.dependency_source import DependencyUnknown, factor_fields, field_sources
    with pytest.raises(DependencyUnknown, match="input file"):
        factor_fields('df = pd.read_hdf(external_root / "daily_pv.h5")\nx = df["close"]', field_sources())
    assert factor_fields('from pathlib import Path\nDATA_DIR = Path(__file__).resolve().parent.parent\n'
                         'df = pd.read_hdf(DATA_DIR / "daily_pv.h5")\nx = df["close"]', field_sources()) == {"close"}


def test_dynamic_window_reassignment_cannot_reuse_an_old_literal():
    from backend.services.data_integrity.dependency_source import DependencyUnknown, factor_window
    with pytest.raises(DependencyUnknown, match="dynamic"):
        factor_window('window = 20\nwindow = choose_window()\nresult = data["close"].rolling(window).mean()')


def _dataset_pickle(length=20, *, extra=b"", root="TSDatasetH"):
    import pickle
    return (b"\x80\x02cqlib.data.dataset\n" + root.encode() + b"\n)\x81}(X\x08\x00\x00\x00step_len" +
            pickle.dumps(length, protocol=2)[2:-1] + extra + b"ub.")


def test_inert_metadata_never_calls_pickle_globals(monkeypatch):
    import builtins
    import pickle
    from backend.services.data_integrity.frozen_metadata import dataset_sequence_length
    monkeypatch.setattr(pickle, "loads", lambda *_: pytest.fail("unpickler invoked"))
    monkeypatch.setattr(builtins, "eval", lambda *_: pytest.fail("pickle callback invoked"))
    extra = b"X\x06\x00\x00\x00opaque" + b"cbuiltins\neval\nX\x03\x00\x00\x001/0\x85R"
    assert dataset_sequence_length(_dataset_pickle(extra=extra)) == 20


@pytest.mark.parametrize("raw", [_dataset_pickle(0), _dataset_pickle(True), _dataset_pickle(-1),
    _dataset_pickle(root="OtherDataset"), _dataset_pickle()+b"extra",
    b"\x80\x02cqlib.data.dataset\nTSDatasetH\n)\x81}b."])
def test_inert_metadata_rejects_ambiguous_or_unbound_length(raw):
    from backend.services.data_integrity.frozen_metadata import dataset_sequence_length
    with pytest.raises(ValueError):
        dataset_sequence_length(raw)
