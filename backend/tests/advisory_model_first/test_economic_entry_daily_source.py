import json
import io
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_daily_source import (
    EconomicEntryReadonlyPriceContextSourceV1, prepare_frozen_economic_research_day_v1,
    build_economic_D_features_v1,
    EconomicEntryReadonlyDSourceV1,
    project_economic_frozen_candidate_roster_v1,
    EconomicEntryReadonlyCandidateSourceV1,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_entry_daily_inference import _daily

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def test_historical_price_component_read_is_not_live_PIT_activation(study, monkeypatch):
    from backend.services import canonical_equity_pit as pit
    from backend.services.advisory_model_first.realtime_feature_source import PostgresRealtimeFeatureSource
    args = _daily(study)
    D, T = args["prediction_input"].decision_date, args["prediction_input"].target_date
    rows = [pit.CANONICAL_PIT_RULE_VERSION, "canonical_all_listed", "ready", False, D, T, "a" * 64]
    snapshots, quotes = [], []
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params): assert params == (pit.CANONICAL_PIT_UNIVERSE_KEY,)
        def fetchone(self): return tuple(rows)
    class Session:
        budget = SimpleNamespace(check=lambda: None)
        @contextmanager
        def connection(self):
            snapshots.append("readonly")
            yield SimpleNamespace(cursor=lambda: Cursor())
    monkeypatch.setattr(pit.CanonicalPitAuthorityResolver, "resolve_live_binding", lambda self: SimpleNamespace(
        authority_status=pit.PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION,
        universe_key=pit.LEGACY_PIT_UNIVERSE_KEY, activation_generation=0))
    def read(cursor, **kwargs):
        assert kwargs["pit_universe_key"] == pit.CANONICAL_PIT_UNIVERSE_KEY
        quotes.append("D-only")
        return {"000001.SZ": args["context"]}, ()
    monkeypatch.setattr(PostgresRealtimeFeatureSource, "_price_range_contexts", staticmethod(read))
    source = EconomicEntryReadonlyPriceContextSourceV1(read_session=Session(), pit_universe_key=pit.CANONICAL_PIT_UNIVERSE_KEY)
    assert source.load(symbols=["000001.SZ"], decision_date=D, target_date=T)[0] == {"000001.SZ": args["context"]}
    assert snapshots == ["readonly"] and quotes == ["D-only"]
    assert source.last_receipt["component_is_live"] is False and source.last_receipt["source_evidence"] == "RECOVERED_LIMITED"
    assert source.last_receipt["legacy_fallback"] is False and source.last_receipt["data_activation"] is False
    rows[3] = True
    with pytest.raises(AdvisoryModelFirstError, match="exact ready canonical"):
        source.load(symbols=["000001.SZ"], decision_date=D, target_date=T)
    assert source.last_receipt is None and quotes == ["D-only"]
    count = len(snapshots)
    assert source.load(symbols=[], decision_date=D, target_date=T) == ({}, ()) and len(snapshots) == count
    assert source.last_receipt["query_count"] == 0


def _loaded(study, *, missing_feature=False):
    args = _daily(study)
    rankings = pd.DataFrame([{**{key: value for key, value in row.items() if key in (
        "decision_as_of_trade_date", "target_trade_date", "instrument")},
        "selection_effective_rank": int(row["instrument"][:6]), "is_candidate_decision": True}
        for row in study["features"].to_dict("records")])
    features = study["features"].copy()
    if missing_feature:
        features = features.loc[~(features.decision_as_of_trade_date.eq(pd.Timestamp(args["prediction_input"].decision_date)) & features.instrument.eq("000001.SZ"))]
    files = {"identity.json": study["identity"].model_dump_json().encode(),
        "calendar.json": json.dumps([day.date().isoformat() for day in pd.bdate_range("2025-01-02", periods=24)]).encode(),
        "frozen_rankings.parquet": rankings.to_parquet(index=False), "features.parquet": features.to_parquet(index=False)}
    return SimpleNamespace(original_input_files=files,
        source_plan=SimpleNamespace(training_request=args["fitted"].request, experiment_id="unit-only"),
        manifest=SimpleNamespace(original_input_identity_sha256=study["identity"].identity_sha256,
            scope=args["model_scope"], evidence_limitations=study["identity"].evidence_limitations)), args


def test_frozen_source_keeps_missing_candidates_without_inventing_native_references(study):
    loaded, args = _loaded(study, missing_feature=True)
    class Source:
        def load(self, *, symbols, decision_date, target_date):
            assert len(symbols) == 6 and decision_date < target_date
            return {}, tuple({"symbol": symbol, "reason_code": "NORMAL_UNAVAILABLE"} for symbol in symbols)
    prepared = prepare_frozen_economic_research_day_v1(loaded=loaded,
        decision_date=args["prediction_input"].decision_date, context_source=Source())
    assert len(prepared.inputs) == 6 and prepared.decision_features[0]["ret_1"] is None
    assert all(value.run_id is None and value.list_id is None and value.restored_cohort_sha256 for value in prepared.inputs)
    assert all(value.source_evidence == "RECOVERED_LIMITED" for value in prepared.inputs)
    assert prepared.source_receipt["outcomes_read"] is False


def test_identity_clock_and_incomplete_price_partition_fail_before_prediction(study):
    loaded, args = _loaded(study)
    class Incomplete:
        calls = 0
        def load(self, **kwargs):
            self.calls += 1
            return {}, ()
    source = Incomplete()
    with pytest.raises(AdvisoryModelFirstError, match="outside the registered consumed"):
        prepare_frozen_economic_research_day_v1(loaded=loaded, decision_date="2026-08-31", context_source=source)
    assert source.calls == 0
    with pytest.raises(AdvisoryModelFirstError, match="partition the exact"):
        prepare_frozen_economic_research_day_v1(loaded=loaded, decision_date=args["prediction_input"].decision_date, context_source=source)
    rankings = pd.read_parquet(io.BytesIO(loaded.original_input_files["frozen_rankings.parquet"]))
    index = rankings.index[pd.to_datetime(rankings.decision_as_of_trade_date).eq(pd.Timestamp(args["prediction_input"].decision_date))][0]
    rankings.loc[index, "selection_effective_rank"] = float("nan")
    loaded.original_input_files["frozen_rankings.parquet"] = rankings.to_parquet(index=False)
    with pytest.raises(AdvisoryModelFirstError, match="silently dropped"):
        prepare_frozen_economic_research_day_v1(loaded=loaded, decision_date=args["prediction_input"].decision_date, context_source=source)
    loaded.manifest.original_input_identity_sha256 = "0" * 64
    with pytest.raises(AdvisoryModelFirstError, match="original program identity differs"):
        prepare_frozen_economic_research_day_v1(loaded=loaded, decision_date=args["prediction_input"].decision_date, context_source=source)
    assert source.calls == 1


def test_explicit_PIT_key_and_empty_roster_never_open_an_input_transaction():
    class NoConnection:
        def connection(self):
            pytest.fail("empty economic roster must not query the database")
    with pytest.raises(AdvisoryModelFirstError, match="explicit canonical PIT component key"):
        EconomicEntryReadonlyPriceContextSourceV1(read_session=NoConnection(), pit_universe_key=None)
    source = EconomicEntryReadonlyPriceContextSourceV1(read_session=NoConnection(), pit_universe_key="aistock_equity_pit_canonical_v2")
    assert source.load(symbols=[], decision_date=pd.Timestamp("2025-01-28").date(), target_date=pd.Timestamp("2025-01-29").date()) == ({}, ())


def test_frozen_leg_projection_needs_no_ranking_parent_and_rejects_fake_scores():
    roles, weights = {"lstm": "unit-leg-a", "fund": "unit-leg-b"}, {"unit-leg-a": .4, "unit-leg-b": .6}
    row = SimpleNamespace(symbol="000001.SZ", rank=1, score=.32,
        component_scores={"unit-leg-a": {"normalized_score": .2, "weight": .4},
                          "unit-leg-b": {"normalized_score": .4, "weight": .6}})
    parameters = dict(rows=[row], decision_date="2025-01-28", target_date="2025-01-29",
        component_roles=roles, terminal_weights=weights, candidate_group_size=1)
    frame = project_economic_frozen_candidate_roster_v1(**parameters)
    assert frame.instrument.tolist() == [row.symbol] and frame.combined_score.tolist() == [.32]
    for modified in ({"score": True}, {"rank": True}, {"component_scores": {}}, {"score": .33}):
        with pytest.raises(AdvisoryModelFirstError):
            project_economic_frozen_candidate_roster_v1(**{**parameters, "rows": [SimpleNamespace(**{**vars(row), **modified})]})
    assert project_economic_frozen_candidate_roster_v1(**{**parameters, "rows": [], "candidate_group_size": 0}).empty


@pytest.mark.parametrize("suspended", [False, True])
def test_eight_D_adapter_matches_shared_training_features_without_HMM_or_M4(suspended):
    from backend.tests.advisory_model_first.test_shared_feature_builder import _feature_inputs
    from backend.services.advisory_model_first.shared_feature_builder import build_advisory_feature_matrix
    from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID
    inputs = _feature_inputs()
    calendar = inputs["candidate_daily"].index.get_level_values("datetime").unique()[-80:]
    for field in ("candidate_daily", "candidate_static", "market_daily", "benchmark_daily"):
        inputs[field] = inputs[field].loc[inputs[field].index.get_level_values("datetime") >= calendar[0]]
    if suspended:
        inputs["candidate_daily"] = inputs["candidate_daily"].drop(index=(calendar[-1], "000001.SZ"))
        inputs["suspend_rows"] = pd.DataFrame({"trade_date": [calendar[-1]], "instrument": ["000001.SZ"], "suspend_type": ["S"]})
    roles = {"lstm": LSTM_LEG_ID, "fund": FUND_LEG_ID}
    shared = build_advisory_feature_matrix(**inputs, incomplete_candidate_policy="preserve_exact",
        feature_schema_version="advisory_feature_schema_v2_suspension_aware", trading_calendar=calendar).features
    adapter = {field: inputs[field] for field in ("candidates", "candidate_daily", "market_daily", "benchmark_daily", "suspend_rows")}
    eight, receipt = build_economic_D_features_v1(**adapter, trading_calendar=calendar,
        decision_date=calendar[-1].date(), component_roles=roles)
    assert len(eight) == 2 and receipt["hmm_loaded"] is False and receipt["m4_loaded"] is False
    names = [field for field in eight if field not in ("decision_as_of_trade_date", "target_trade_date", "instrument")]
    pd.testing.assert_frame_equal(eight.set_index("instrument")[names], shared.set_index("instrument")[names], check_dtype=False)
    future = inputs["candidate_daily"].iloc[[0]].copy()
    future.index = pd.MultiIndex.from_tuples([(calendar[-1] + pd.offsets.BDay(), "000001.SZ")], names=future.index.names)
    with pytest.raises(AdvisoryModelFirstError, match="foreign-time"):
        build_economic_D_features_v1(**{**adapter, "candidate_daily": pd.concat([inputs["candidate_daily"], future])},
            trading_calendar=calendar, decision_date=calendar[-1].date(), component_roles=roles)


def test_readonly_D_source_has_one_snapshot_exact_cutoff_and_no_HMM_queries(monkeypatch):
    from backend.tests.advisory_model_first.test_shared_feature_builder import _feature_inputs
    from backend.services.advisory_model_first import realtime_feature_source as existing
    from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID
    inputs = _feature_inputs()
    calendar = inputs["candidate_daily"].index.get_level_values("datetime").unique()[-20:]
    D, T = calendar[-1].date(), (calendar[-1] + pd.offsets.BDay()).date()
    sessions, queries = [], []
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
    class Session:
        budget = SimpleNamespace(check=lambda: None)
        @contextmanager
        def connection(self):
            sessions.append("readonly")
            yield SimpleNamespace(cursor=lambda: Cursor())
    daily = inputs["candidate_daily"].loc[inputs["candidate_daily"].index.get_level_values("datetime") >= calendar[0]]
    market = inputs["market_daily"].loc[inputs["market_daily"].index.get_level_values("datetime") >= calendar[-2]].reset_index()
    breadth_raw = market.rename(columns={"datetime": "trade_date", "instrument": "ts_code", "close": "close_li"})
    breadth_raw["close_li"] *= 1000.
    breadth_raw["adj_factor"] = 1.
    benchmark = inputs["benchmark_daily"].loc[inputs["benchmark_daily"].index.get_level_values("datetime") >= calendar[0]].reset_index().rename(
        columns={"datetime": "trade_date", "instrument": "ts_code"})
    def read_frame(cursor, sql, parameters):
        queries.append((sql, parameters))
        assert "hmm" not in sql.lower() and "moneyflow" not in sql.lower()
        assert (parameters["end_date"] if isinstance(parameters, dict) else parameters[-1]) == D
        if "WITH base_adj" in sql:
            return pd.DataFrame({"unit_only_marker": [1]})
        if "market.index_daily" in sql:
            return benchmark
        return breadth_raw
    monkeypatch.setattr(existing, "_read_frame", read_frame)
    monkeypatch.setattr(existing, "_market_frame", lambda *args, **kwargs: daily)
    monkeypatch.setattr(existing.PostgresRealtimeFeatureSource, "_recent_trading_dates", staticmethod(lambda *args, **kwargs: calendar))
    monkeypatch.setattr(existing.PostgresRealtimeFeatureSource, "_trading_calendar", staticmethod(lambda *args, **kwargs: pd.DatetimeIndex([D, T])))
    monkeypatch.setattr(existing.PostgresRealtimeFeatureSource, "_suspend_rows", staticmethod(lambda *args, **kwargs: inputs["suspend_rows"]))
    monkeypatch.setattr(existing.PostgresRealtimeFeatureSource, "_price_range_contexts", staticmethod(lambda cursor, **kwargs:
        ({}, tuple({"symbol": symbol, "reason_code": "UNIT_ONLY_ATTRIBUTE_UNAVAILABLE"} for symbol in kwargs["symbols"]))))
    source = EconomicEntryReadonlyDSourceV1(read_session=Session(), pit_universe_key="unit-only-explicit-key")
    result = source.load_day(candidates=inputs["candidates"], decision_date=D, target_date=T,
        component_roles={"lstm": LSTM_LEG_ID, "fund": FUND_LEG_ID})
    assert sessions == ["readonly"] and len(queries) == 3 and len(result["features"]) == 2
    assert result["source_receipt"]["outcomes_read"] is False and result["source_receipt"]["new_native_receipt"] is False
    assert result["source_receipt"]["source_window_contract"]["training_temporal_parity"] == "UNPROVEN"
    assert all(value["last_existing_price_trade_date"] == D.isoformat() for value in result["price_context_unavailable"])
    for invalid_roles, invalid_candidates in (({}, inputs["candidates"]),
            ({"lstm": LSTM_LEG_ID, "fund": FUND_LEG_ID}, inputs["candidates"].assign(selection_effective_rank=True))):
        with pytest.raises(AdvisoryModelFirstError):
            source.load_day(candidates=invalid_candidates, decision_date=D, target_date=T, component_roles=invalid_roles)
        assert sessions == ["readonly"]
    empty = source.load_day(candidates=inputs["candidates"].iloc[:0], decision_date=D, target_date=T,
        component_roles={"lstm": LSTM_LEG_ID, "fund": FUND_LEG_ID})
    assert empty["source_receipt"]["query_count"] == 0 and sessions == ["readonly"]


@pytest.mark.parametrize("empty", [False, True])
def test_native_reader_preserves_original_archive_and_one_readonly_snapshot(tmp_path, study, monkeypatch, empty):
    from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryCandidateProjectionV1
    from backend.services.advisory_model_first.research_control import evidence_reference_for_file
    from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
    from backend.services.selection_center.models import SelectionRun, SelectionCandidate
    from backend.services import advisory_program as programs_module
    from backend.services.selection_center import repository as selections_module, canonical_pit_runtime as pit
    from backend.services.advisory_model_first import realtime_feature_source as realtime
    from backend.services.canonical_equity_pit import PitAuthorityStatus
    from backend.tests.advisory_model_first.test_economic_entry_daily_inference import _daily
    scope = _daily(study)["model_scope"]
    projection = EconomicEntryCandidateProjectionV1(scope=scope, component_roles={"lstm": "unit-a", "fund": "unit-b"},
        terminal_weights={"unit-a": .4, "unit-b": .6}, universe_selection={"mode": "stock_universe", "pool_ids": []},
        review_policy_sha256="a" * 64)
    D, T = pd.Timestamp("2025-01-28").date(), pd.Timestamp("2025-01-29").date()
    members = ["000001.SZ", "000002.SZ"]
    selection = SelectionRun(run_id="sel_" + "1" * 32, mode="single_package", trade_date=T, data_source="UNIT_ONLY_FROZEN",
        package_ids=[scope.package_id], status="VALID_NO_CANDIDATE" if empty else "SUCCEEDED",
        valid_no_candidate=empty, no_candidate_reason="UNIT_ONLY_NO_CANDIDATES" if empty else None,
        manifest_sha256_by_package={scope.package_id: scope.manifest_sha256},
        aggregate_results=[] if empty else [SelectionCandidate(symbol=members[0], rank=1, score=.32,
            selection_entry_price_time=D.isoformat(), component_scores={"unit-a": {"normalized_score": .2, "weight": .4},
                "unit-b": {"normalized_score": .4, "weight": .6}})])
    universe = projection.universe_selection.model_dump(mode="json")
    universe_receipt = {"schema_version": "advisory_universe_admission_receipt_v1", "universe_selection": universe,
        "trade_date": T.isoformat(), "universe_as_of_trade_date": D.isoformat(),
        "admission_stage": "AFTER_SELECTION_BEFORE_ADVISORY_RANKING", "input_candidate_count": 0 if empty else 1,
        "output_candidate_count": 0 if empty else 1, "excluded_candidate_count": 0}
    selection.runtime_config = {"advisory_universe_receipt": universe_receipt}
    context = {"cutoff_date": D.isoformat(), "score_trade_date": D.isoformat(), "requested_trade_date": T.isoformat(),
               "universe_input_hash": sha(members)}
    artifact = {"package_id": scope.package_id, "manifest_sha256": scope.manifest_sha256,
        "trade_date": T.isoformat(), "data_source": selection.data_source, "status": "SUCCEEDED", "universe_count": 2,
        "metadata": {"artifact_input_context": context}, "artifact_input_context_hash": sha(context)}
    payload = {"schema_version": "advisory_selection_input_archive_v1", "selection_run_id": selection.run_id,
        "program_id": "unit-program", "binding_version_id": "unit-binding", "review_policy_sha256": projection.review_policy_sha256,
        "target_trade_date": T.isoformat(), "decision_as_of_trade_date": D.isoformat(),
        "observed_at": "2025-01-28T08:00:00+00:00", "historical_capture_backfilled": False, "historical_vintage_proven": False,
        "manifest_sha256_by_package": selection.manifest_sha256_by_package, "runtime_config": dict(selection.runtime_config),
        "frozen_candidates": [value.model_dump(mode="json") for value in selection.aggregate_results],
        "full_source_universe_members": members, "source_universe_members_sha256": sha(members),
        "package_inputs": {scope.package_id: {"artifact": artifact, "artifact_content_sha256": sha(artifact)}}}
    archive_file = tmp_path / "native_unit_archive.json"
    archive_file.write_text(json.dumps(payload), encoding="utf-8")
    reference = {"selection_run_id": selection.run_id, "reference": evidence_reference_for_file(archive_file, role="SELECTION_EXECUTION_INPUTS").model_dump(mode="json"),
                 "package_artifact_hashes": {scope.package_id: sha(artifact)}}
    selection.runtime_config["advisory_frozen_input_archive"] = reference
    version = {"program_id": "unit-program", "binding_version_id": "unit-binding", "list_version_id": "unit-list",
        "review_run_id": "unit-review", "target_trade_date": T.isoformat(), "version_status": "PUBLISHED",
        "selection_as_of_trade_date": D.isoformat(), "created_at": "2025-01-28T08:01:00+00:00",
        "summary_json": {"advisory_universe_receipt": universe_receipt, "advisory_frozen_input_archive": reference}}
    items = [] if empty else [{"symbol": members[0], "rank": 1, "action": "WATCH", "evidence_json": {"review_policy_sha256": projection.review_policy_sha256}}]
    events, sessions, quote_loads = [], [], []
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params):
            assert "SELECT" in sql and "advisory_review_run" in sql
            events.append("review_select")
        def fetchone(self): return ("unit-review", "unit-program", "unit-binding", T, selection.run_id, [selection.run_id])
    class Session:
        budget = SimpleNamespace(check=lambda: None)
        @contextmanager
        def connection(self):
            sessions.append("snapshot")
            yield SimpleNamespace(cursor=lambda: Cursor())
    class Programs:
        def __init__(self, conn_factory): self.connection = conn_factory
        def get_program(self, value): return SimpleNamespace(program_id="unit-program", package_ids=[scope.package_id], review_policy_sha256=projection.review_policy_sha256, target_count=20)
        def get_active_binding_version(self, value): return SimpleNamespace(program_id="unit-program", activation_status="ACTIVE", binding_version_id="unit-binding", package_ids=[scope.package_id],
            effective_from_trade_date=D, effective_to_trade_date=None, runtime_config_json={"universe_selection": universe})
        def list_version_for_date(self, *args, **kwargs): return version
        def list_version_items(self, *args): return items
    class Selections:
        def __init__(self, conn_factory): self.connection = conn_factory
        def get_run(self, value): return selection
    monkeypatch.setattr(programs_module, "AdvisoryProgramPGRepository", Programs)
    monkeypatch.setattr(programs_module, "list_version_to_dict", lambda value: value)
    monkeypatch.setattr(programs_module, "list_item_to_dict", lambda value: value)
    monkeypatch.setattr(selections_module, "SelectionCenterRepository", Selections)
    monkeypatch.setattr(pit, "require_canonical_pit_runtime_binding", lambda *args, **kwargs:
        SimpleNamespace(universe_key="unit-explicit-PIT", rule_version="unit-native-rule", authority_status=PitAuthorityStatus.ACTIVE_CANONICAL))
    monkeypatch.setattr(pit, "require_canonical_pit_generation_current", lambda *args, **kwargs: events.append("generation_check"))
    monkeypatch.setattr(realtime.PostgresRealtimeFeatureSource, "_trading_calendar", staticmethod(lambda *args, **kwargs: pd.DatetimeIndex([D, T])))
    def quotes(self, **kwargs):
        with self._session.connection():
            quote_loads.append(len(kwargs["candidates"]))
        return {"features": pd.DataFrame(), "price_contexts": {}, "source_receipt": {"outcomes_read": False},
                "trading_calendar": () if empty else (D, T)}
    monkeypatch.setattr(EconomicEntryReadonlyDSourceV1, "load_day", quotes)
    source = EconomicEntryReadonlyCandidateSourceV1(read_session=Session(), pit_universe_key="unit-explicit-PIT",
        now=lambda: datetime(2025, 1, 28, 8, 2, tzinfo=timezone.utc))
    result = source.load_day(program_id="unit-program", binding_version_id="unit-binding", target_date=T, projection=projection)
    assert sessions == ["snapshot"] and quote_loads == [0 if empty else 1] and events.count("generation_check") == 1
    assert result["candidate_receipt"]["pit_generation_checked_at"] == "READONLY_SNAPSHOT_NOT_FRESH_POST_READ"
    assert result["source_members"] == members and result["selection_run_id"] == selection.run_id
    assert result["candidate_receipt"]["model_scope_qualified"] is False and result["candidate_receipt"]["native_receipt_created"] is False
    archive_file.write_text(json.dumps({**payload, "full_source_universe_members": [members[0]]}), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError, match="source identity changed"):
        source.load_day(program_id="unit-program", binding_version_id="unit-binding", target_date=T, projection=projection)
    assert quote_loads == [0 if empty else 1]


@pytest.mark.parametrize("mode,pools", [("single_index", ["csi300"]), ("index_union", ["csi300", "csi500"])])
def test_index_projection_requires_complete_original_hash_and_ignores_old_holding_ranks(mode, pools):
    from backend.services.advisory_model_first.economic_entry_daily_source import _economic_native_pool_rows_v1
    from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
    D, T = pd.Timestamp("2025-01-28").date(), pd.Timestamp("2025-01-29").date()
    universe, members = {"mode": mode, "pool_ids": pools}, ["000001.SZ"]
    selection = SimpleNamespace(aggregate_results=[SimpleNamespace(symbol="000001.SZ", rank=1), SimpleNamespace(symbol="000002.SZ", rank=2)], runtime_config={})
    receipt = {"schema_version": "advisory_universe_admission_receipt_v1", "universe_selection": universe,
        "trade_date": T.isoformat(), "universe_as_of_trade_date": D.isoformat(),
        "admission_stage": "AFTER_SELECTION_BEFORE_ADVISORY_RANKING", "input_candidate_count": 2,
        "output_candidate_count": 1, "excluded_candidate_count": 1, "symbol_set_sha256": sha(members)}
    snapshot = {"universe_selection": universe, "decision_date": D.isoformat(), "members": members,
                "pit_universe_key": "unit-explicit-key", "pit_rule_version": "unit-native-rule", "complete": True}
    items = [{"symbol": "000001.SZ", "rank": 1, "action": "WATCH"},
             {"symbol": "000002.SZ", "rank": 2, "action": "HOLD"}, {"symbol": "000003.SZ", "rank": 2, "action": "HOLD"}]
    arguments = dict(selection=selection, items=items, version={"summary_json": {"advisory_universe_receipt": receipt}}, archive={},
        universe=universe, decision=D, target=T, pinned_connection=lambda: None, pit_universe_key="unit-explicit-key", pit_rule_version="unit-native-rule")
    rows, actual, limitations = _economic_native_pool_rows_v1(**arguments, index_reader=lambda **kwargs: snapshot)
    assert [row.symbol for row in rows] == actual == members and limitations
    for changes in ({"complete": False}, {"pit_universe_key": "wrong-key"}, {"pit_rule_version": "wrong-rule"}, {"members": ["000002.SZ"]}):
        with pytest.raises(AdvisoryModelFirstError, match="index members differ"):
            _economic_native_pool_rows_v1(**arguments, index_reader=lambda **kwargs: {**snapshot, **changes})
    with pytest.raises(AdvisoryModelFirstError, match="no legacy fallback"):
        _economic_native_pool_rows_v1(**arguments, index_reader=None)


def test_index_repository_uses_only_explicit_lease_key_and_rule(monkeypatch):
    from backend.services.advisory_model_first.economic_entry_daily_source import read_economic_index_members_v1
    from backend.services import core_index_membership as core
    D, queries = pd.Timestamp("2025-01-28").date(), []
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params):
            assert "SELECT" in sql and "universe_key = %s" in sql and "rule_version = %s" in sql
            queries.append(params)
        def fetchall(self): return [("000001.SZ", D, D)]
    @contextmanager
    def connection():
        yield SimpleNamespace(cursor=lambda: Cursor())
    def resolve(selection, start, end, *, repository):
        assert selection.pool_ids == ("csi300", "csi500") and start == end == D
        return SimpleNamespace(intervals=repository.fetch_canonical_intervals(start, end), membership_revision="unit-revision")
    monkeypatch.setattr(core, "resolve_universe", resolve)
    arguments = dict(universe_selection={"mode": "index_union", "pool_ids": ["csi300", "csi500"]},
        decision_date=D, connection_factory=connection, pit_universe_key="unit-explicit-active-key", pit_rule_version="unit-native-rule")
    snapshot = read_economic_index_members_v1(**arguments)
    assert snapshot["members"] == ["000001.SZ"] and snapshot["complete"] is True
    assert queries == [("unit-explicit-active-key", "unit-native-rule", D, D)]
    with pytest.raises(AdvisoryModelFirstError, match="explicit native key/rule"):
        read_economic_index_members_v1(**{**arguments, "pit_rule_version": None})
    assert len(queries) == 1
