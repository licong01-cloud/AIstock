"""Only the consumed close slice may certify the LSTM benchmark, with revision pins."""
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import lstm_spo_shadow_source_v1 as source
from backend.services.data_integrity.catalog import RULE_VERSION, SCOPE_VERSION
from backend.services.data_integrity.inputs import InputReadinessError
from backend.services.data_integrity.contracts import digest


@pytest.fixture
def db(monkeypatch):
    days = list(pd.bdate_range("2026-09-01", periods=20).date)

    class Connection:
        calendar = days
        quotes = [(day, source.BENCHMARK, 4000. + i) for i, day in enumerate(days)]

        @contextmanager
        def cursor(self):
            yield self

        def execute(self, sql, params):
            if "trading_calendar" in sql:
                assert params == (str(days[-1]),)
                self.result = [(d,) for d in reversed(self.calendar)]
            else:
                assert "SELECT trade_date,ts_code,close" in sql and "LIMIT 21" in sql
                assert params == (source.BENCHMARK, list(map(str, days)))
                self.result = [r for r in self.quotes if r[1] == source.BENCHMARK]

        def fetchall(self):
            return self.result

    conn = Connection()
    rows = [dict(dataset=ds, trade_date=str(day), data_revision=7, validated_revision=7,
                 validation_status="INCOMPLETE" if ds == "index_daily" else "VALID",
                 validation_id="v7", scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)
            for day in days for ds in source.EXTRA_DATASETS]
    monkeypatch.setattr(source, "partition_pins", lambda *args: deepcopy(rows))
    return SimpleNamespace(conn=conn, days=days, pins=rows)


def test_other_index_failures_preserved_while_consumed_close_is_valid(db):
    db.conn.quotes = [*db.conn.quotes, (db.days[0], "999999.SH", None)]
    before = deepcopy(db.pins)
    receipt = source.extra_input_capture(db.conn, db.days)
    assert db.pins == before
    benchmark = receipt["benchmark"]
    assert benchmark["valid"] == benchmark["expected"] == 20
    assert benchmark["instrument"] == "000300.SH" and benchmark["fields"] == ["close"]
    assert benchmark["whole_index_partition_certified"] is False
    assert all(r["global_validation_status"] == "INCOMPLETE" for r in benchmark["partition_refs"])
    assert all(r["data_revision"] == 7 for r in benchmark["partition_refs"])
    assert len(receipt["partition_refs"]) == 40


@pytest.mark.parametrize("bad", [None, float("nan"), float("inf"), float("-inf"), 0., -1., True, "4000"])
def test_consumed_close_must_be_finite_positive_number(db, bad):
    day, code, _ = db.conn.quotes[0]
    db.conn.quotes = [(day, code, bad), *db.conn.quotes[1:]]
    with pytest.raises(InputReadinessError, match="缺行、重复或无效"):
        source.extra_input_capture(db.conn, db.days)


@pytest.mark.parametrize("change", ["missing", "duplicate", "duplicate_replaces_day", "foreign"])
def test_consumed_keys_must_match_every_session_once(db, change):
    quotes = list(db.conn.quotes)
    if change == "missing":
        quotes.pop()
    elif change == "duplicate":
        quotes.append(quotes[0])
    elif change == "duplicate_replaces_day":
        quotes[-1] = quotes[0]
    else:
        quotes[-1] = (db.days[-1] + timedelta(days=1), source.BENCHMARK, 4000.)
    db.conn.quotes = quotes
    with pytest.raises(InputReadinessError):
        source.extra_input_capture(db.conn, db.days)


@pytest.mark.parametrize("change", ["missing_pin", "duplicate_pin", "revision_unknown", "old_rule", "stock_invalid"])
def test_current_revision_and_stock_contracts_are_required(db, change):
    index = next(r for r in db.pins if r["dataset"] == "index_daily")
    if change == "missing_pin":
        db.pins.remove(index)
    elif change == "duplicate_pin":
        db.pins.append(dict(index))
    elif change == "revision_unknown":
        index["data_revision"] = None
    elif change == "old_rule":
        index["rule_version"] = "obsolete"
    else:
        db.pins[0]["validation_status"] = "UNVERIFIED"
    with pytest.raises(InputReadinessError):
        source.extra_input_capture(db.conn, db.days)


def test_window_is_the_authoritative_twenty_sessions_ending_on_d(db):
    db.conn.calendar = [db.days[0] - timedelta(days=1), *db.days[1:]]
    with pytest.raises(InputReadinessError, match="D对应"):
        source.extra_input_capture(db.conn, db.days)


@pytest.mark.parametrize("change", ["none", "revision", "validation", "value", "missing", "tampered_window"])
def test_post_compute_rechecks_same_scope_values_and_revision(db, monkeypatch, change):
    extra = source.extra_input_capture(db.conn, db.days)
    baseline_checks = []
    class Session:
        @contextmanager
        def connection(self):
            yield db.conn

        def close(self):
            pass

    reader = source.LstmSpoShadowSource(session_factory=Session)
    monkeypatch.setattr(reader, "_verify_original", lambda conn, pub, native: baseline_checks.append(native))
    if change == "revision":
        next(r for r in db.pins if r["dataset"] == "index_daily")["data_revision"] += 1
    elif change == "validation":
        next(r for r in db.pins if r["dataset"] == "index_daily")["validation_id"] = "new"
    elif change == "value":
        day, code, close = db.conn.quotes[0]
        db.conn.quotes = [(day, code, close + 1), *db.conn.quotes[1:]]
    elif change == "missing":
        db.conn.quotes = db.conn.quotes[:-1]
    elif change == "tampered_window":
        extra["session_dates"][0] = "2000-01-01"
    bindings = {"original": "retained"}
    native = {"publication": {"list_version_id": "frozen"}, "baseline_bindings_sha256": source.sha(bindings)}
    receipt = {"price": {"input_readiness": extra}, "baseline_input_bindings": bindings,
               "native_source_reference": native}
    if change == "none":
        reader.verify_capture(receipt)
    else:
        with pytest.raises(InputReadinessError):
            reader.verify_capture(receipt)
    assert baseline_checks == [native]


@pytest.fixture
def archive(monkeypatch):
    from backend.services.advisory_program import AdvisoryProgramPGRepository
    from backend.services.selection_center.repository import SelectionCenterRepository
    from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactRepository

    pkg = source.TARGET["package_id"]
    d, t = "2026-10-30", "2026-11-02"
    stamp = datetime(2026, 10, 30, 14, tzinfo=timezone.utc)
    pin = dict(dataset="cyq_perf", fact_date="2026-05-20", data_revision=2, validation_id="v2",
               scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)
    binding = dict(binding_id=pkg, manifest_sha256=source.TARGET["manifest_sha256"], data_date="2026-05-20",
                   dependencies={"datasets": ["cyq_perf"]}, partition_refs=[pin], global_pit={"id": "frozen"})
    binding["binding_sha256"] = digest(binding)
    bindings = {pkg: binding}
    capture = dict(ledger_available=True, start="2026-05-20", end="2026-05-20",
        snapshot_kind="REPEATABLE_READ_READ_ONLY", external_fallback_allowed=False, pit_identity=binding["global_pit"],
        readiness_binding_sha256=binding["binding_sha256"], partition_refs=[{**pin, "trade_date": pin["fact_date"],
            "validated_revision": 2, "validation_status": "VALID", "metadata": {"validation": {
                "checks": {"coverage_basis": "independent_dated_traded_stock_keys"}}}}])
    config = {"selection_artifact_config": {"input_readiness_by_package": bindings},
              "qe_backtest_runtime_contract": {"source": {k: source.TARGET[k] for k in ("source_id", "run_id")}}}
    scores = [dict(symbol=f"{600000+i}.SH", rank=i+1, score=float(100-i)) for i in range(60)]
    run = SimpleNamespace(runtime_config={"package_runtime_configs": {pkg: config}, "universe_selection": None},
        trade_date=date.fromisoformat(t), data_source="DB_HISTORICAL", completed_at=stamp,
        status="SUCCEEDED", run_id="sel_frozen", package_ids=[pkg],
        manifest_sha256_by_package={pkg: source.TARGET["manifest_sha256"]}, package_results={pkg: [SimpleNamespace(
            symbol=r["symbol"], score=r["score"], component_scores={"raw_rank": r["rank"]}) for r in scores[:40]]})
    artifact = SimpleNamespace(artifact_id="asset_frozen", created_at=stamp, trade_date=run.trade_date,
        package_id=pkg, manifest_sha256=source.TARGET["manifest_sha256"], data_source=run.data_source,
        status="SUCCEEDED", score_count=60, scores_json=scores, artifact_sha256=source.sha(scores),
        artifact_payload_sha256="payload_frozen", metadata={"score_trade_date": d, "cutoff_date": d,
            "source_id": source.TARGET["source_id"], "artifact_input_context": {"input_capture": capture}})
    version = SimpleNamespace(program_id=source.PROGRAM, binding_version_id=source.BINDING,
        version_status="PUBLISHED", created_at=stamp, summary_json={"selection_run_id": run.run_id,
            "advisory_date_context": {"decision_as_of_trade_date": d, "target_trade_date": t},
            "input_readiness": {pkg: {"binding_sha256": binding["binding_sha256"]}}})
    publication = dict(program_id=source.PROGRAM, binding_version_id=source.BINDING,
        list_version_id="published_frozen", selection_run_id=run.run_id,
        decision_date=d, target_date=t, created_at=stamp.isoformat())
    monkeypatch.setattr(AdvisoryProgramPGRepository, "get_list_version", lambda *args: version)
    monkeypatch.setattr(SelectionCenterRepository, "get_run", lambda *args: run)
    monkeypatch.setattr(StrategyPackageSelectionArtifactRepository, "get", lambda *args, **kw: artifact)
    monkeypatch.setattr(source, "verify_native_liveness", lambda *args: None)
    monkeypatch.setattr(source, "partition_pins", lambda *args: pytest.fail("frozen scores must not reread unconsumed history"))
    reader = source.LstmSpoShadowSource(now=lambda: stamp + timedelta(hours=1))
    return SimpleNamespace(reader=reader, publication=publication, artifact=artifact, version=version,
                           bindings=bindings, capture=capture, run=run)


def test_original_frozen_score_qualification_survives_unconsumed_history_revision(archive):
    before = deepcopy(archive.bindings)
    original = archive.reader._original_inputs(archive.publication, object())
    reference = archive.reader._native_reference(archive.publication, original)
    archive.reader._verify_original(object(), archive.publication, reference)
    assert len(original[-1]) == 50 and reference["original_score_count"] == 60
    assert original[-1].instrument.iloc[0] == "600000.SH"
    assert archive.bindings == before and original[4] == before
    assert before[source.TARGET["package_id"]]["partition_refs"][0]["data_revision"] == 2


@pytest.mark.parametrize("change", ["payload", "score", "capture", "binding", "publication"])
def test_original_archive_recheck_rejects_mutated_source(archive, change):
    original = archive.reader._original_inputs(archive.publication, object())
    reference = archive.reader._native_reference(archive.publication, original)
    if change == "payload":
        archive.artifact.artifact_payload_sha256 = "changed"
    elif change == "score":
        archive.artifact.scores_json[0]["score"] += 1
    elif change == "capture":
        archive.capture["readiness_binding_sha256"] = "changed"
    elif change == "binding":
        archive.bindings[source.TARGET["package_id"]]["partition_refs"][0]["data_revision"] = 3
    else:
        archive.version.summary_json["selection_run_id"] = "changed"
    with pytest.raises((InputReadinessError, ValueError)):
        archive.reader._verify_original(object(), archive.publication, reference)


@pytest.mark.parametrize("change", ["none", "disabled", "binding", "pit", "retired", "manifest"])
def test_live_native_identity_guards_remain_required(monkeypatch, change):
    from backend.services.canonical_equity_pit import CanonicalPitAuthorityResolver
    @dataclass
    class Pit:
        authority: str = "frozen"
    monkeypatch.setattr(CanonicalPitAuthorityResolver, "resolve_live_binding",
                        lambda *args: Pit("changed" if change == "pit" else "frozen"))
    pkg, manifest = source.TARGET["package_id"], source.TARGET["manifest_sha256"]
    repo = SimpleNamespace(get_program=lambda *_: SimpleNamespace(status="DISABLED" if change == "disabled" else "ENABLED", package_ids=[pkg]),
        get_active_binding_version=lambda *_: SimpleNamespace(binding_version_id="changed" if change == "binding" else source.BINDING))
    binding = dict(binding_id=pkg, manifest_sha256=manifest, global_pit={"authority": "frozen"})
    binding["binding_sha256"] = digest(binding)
    class Connection:
        @contextmanager
        def cursor(self):
            yield self

        def execute(self, sql, args):
            assert sql.startswith("SELECT") and args == (pkg,)

        def fetchone(self):
            return ("changed" if change == "manifest" else manifest, "RETIRED" if change == "retired" else "READY")

    if change == "none":
        source.verify_native_liveness(Connection(), repo, {pkg: binding})
    else:
        with pytest.raises(InputReadinessError):
            source.verify_native_liveness(Connection(), repo, {pkg: binding})
