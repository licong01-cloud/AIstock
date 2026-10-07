"""Full orchestration contracts with hand model; not runtime/economic evidence."""
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import json
from types import SimpleNamespace as NS

import pytest

from backend.services.advisory_model_first import generic_daily_price_api_service_v1 as api
from backend.services.advisory_model_first.generic_daily_price_input_v1 import FEATURES, ROSTER, _records
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_generic_daily_price_context_v1 import context
from backend.tests.advisory_model_first.test_generic_price_set_consumer_v1 import packet


@pytest.fixture
def chain(tmp_path):
    loaded, _fitted, rows, _coordinates, _source, _price_set, ref = packet(tmp_path)
    state = NS(events=[], closed=False, pinned=None, frames=[], corrupt=False, fail=False, empty=False, model_loads=0)
    config = tmp_path/"config.json"
    config.write_text(json.dumps(dict(schema_version="generic_daily_price_api_config_v1", model_family="DAILY_5TD",
        trained_manifest_ref=ref.model_dump(), policy_sha256=api.POLICY_SHA256)), encoding="utf-8")
    class Cursor:
        def execute(self, sql, params=None):
            assert not state.closed
            state.events.append(sql)
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            self.close()
    class Session:
        def __init__(self, budget):
            self.budget = budget
        @contextmanager
        def connection(self):
            assert not state.closed
            state.events.append("BEGIN_READ_ONLY_REPEATABLE_READ")
            try:
                yield NS(cursor=Cursor)
            finally:
                state.events.append("ROLLBACK")
        def close(self):
            state.closed = True
            state.events.append("CLOSE")
    def source(pinned):
        state.pinned = pinned
        def load(**query):
            with pinned.connection() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT original_list")
            t = query["target_date"] or date(2026, 6, 11)
            d = t-timedelta(days=1)
            frame = rows.copy(deep=True).loc[:, ROSTER]
            frame[ROSTER[0]], frame[ROSTER[1]], frame["candidate_group_size"] = d.isoformat(), t.isoformat(), 1
            if state.empty:
                frame = frame.iloc[:0]
            metadata = dict(package_id="original", run_id=f"r{t}", list_version_id=f"l{t}", universe_identity=None)
            identity = api.sha(_records(frame))
            receipt = dict(decision_date=d.isoformat(), target_date=t.isoformat(), original_items=[{"action": "HOLD"}],
                unmodeled_items=[{"action": "EXIT"}], candidate_roster_sha256=identity, **metadata)
            return dict(packet=dict(decision_date=d, target_date=t, candidates=frame, metadata=metadata), candidate_receipt=receipt)
        return NS(load_day=load)
    def input_reader(pinned, now):
        assert pinned is state.pinned
        def load_batch(*, packets):
            with pinned.connection() as connection, connection.cursor() as cursor:
                for name in ("calendar", "raw", "benchmark"):
                    cursor.execute(f"/* bounded */ SELECT {name}")
            pinned.close()
            results = []
            for p in packets:
                frame = p["candidates"].copy(deep=True)
                for feature in FEATURES:
                    frame[feature] = rows.iloc[0][feature]
                identity = api.sha(_records(frame.loc[:, ROSTER]))
                if state.corrupt and not frame.empty:
                    frame["instrument"] = "000002.SZ"
                state.frames.append(frame)
                results.append((frame, dict(query_count=3, context={"source_visible_through": p["decision_date"]},
                    calendar_verified=True, status="NO_CANDIDATES" if frame.empty else "READ", candidate_roster_sha256=identity)))
            return results
        return NS(load_batch=load_batch)
    def context_reader(pinned, now):
        assert pinned is state.pinned and not state.closed
        def load_batch(*, packets):
            if state.fail:
                raise AdvisoryModelFirstError("query unavailable", reason_code="ADVISORY_GENERIC_DAILY_DB_INPUT_UNAVAILABLE")
            with pinned.connection() as connection, connection.cursor() as cursor:
                for name in ("base", "st", "actions"):
                    cursor.execute(f"SELECT legal_{name}")
            return [dict(contexts={symbol: {**context(), "visible_through": p["decision_date"].isoformat()}
                for symbol in p["candidates"].instrument}, receipt=dict(query_count=3)) for p in packets]
        return NS(load_batch=load_batch)
    def model_loader(**_kwargs):
        state.model_loads += 1
        return loaded
    state.service = api.GenericDailyPriceAPIServiceV1(config_path=config, model_loader=model_loader,
        session_factory=Session, list_source_factory=source, input_factory=input_reader, context_factory=context_reader,
        now=lambda: datetime(2026, 7, 1, tzinfo=timezone.utc))
    state.config, state.loaded = config, loaded
    return state


def test_single_and_batch_same_kernel_one_snapshot_and_model(chain):
    result = chain.service.read_batch(program_id="p", requests=[{"target_trade_date": date(2026, 6, 11)},
                                                            {"target_trade_date": date(2026, 6, 12)}])
    assert result["status"] == "COMPUTED" and result["complete"] and len(result["days"]) == 2
    assert chain.model_loads == 1 and chain.events.count("BEGIN_READ_ONLY_REPEATABLE_READ") == 1
    assert chain.events[-2:] == ["ROLLBACK", "CLOSE"] and result["query_counts"]["total_selects"] == 8
    assert result["query_counts"]["shared_nine_field_selects"] == 3 and result["query_counts"]["original_list_and_identity_selects"] == 2
    first = result["days"][0]
    assert first["advice"][0]["status"] == "ACCEPTABLE_PRICE_SET" and first["unmodeled_items"] == [{"action": "EXIT"}]
    assert first["source_context"]["universe_identity"] is None
    chain.closed = False
    single = chain.service.read_price(program_id="p", target_date=date(2026, 6, 11))
    assert single["days"][0]["advice"] == first["advice"] and single["days"][0]["input_sha256"] == first["input_sha256"]
    assert not any(result[name] for name in ("economic_confirmation", "deployable", "database_write", "model_activation"))


def test_not_configured_is_zero_db_and_not_empty_candidates(monkeypatch):
    monkeypatch.delenv("AISTOCK_ADVISORY_GENERIC_PRICE_CONFIG", raising=False)
    def forbidden(*_args, **_kwargs):
        pytest.fail("unconfigured API must not touch DB or model")
    service = api.GenericDailyPriceAPIServiceV1(model_loader=forbidden, session_factory=forbidden)
    result = service.read_price(program_id="p")
    assert result["status"] == "NOT_CONFIGURED" and not result["database_read"] and not result["complete"]


@pytest.mark.parametrize("case", ["config", "query_failure", "foreign_roster"])
def test_error_does_not_publish_partial_or_leak_connection(chain, case):
    if case == "config":
        chain.config.write_text('{"schema_version":"bad"}', encoding="utf-8")
    else:
        chain.fail, chain.corrupt = case == "query_failure", case == "foreign_roster"
    with pytest.raises(AdvisoryModelFirstError) as failure:
        chain.service.read_price(program_id="p")
    if case == "config":
        assert failure.value.reason_code == "ADVISORY_GENERIC_DAILY_INVALID_CONFIGURATION" and not chain.events
    else:
        assert chain.closed and chain.events[-2:] == ["ROLLBACK", "CLOSE"]


def test_empty_original_list_is_not_configuration_or_unknown(chain):
    chain.empty = True
    result = chain.service.read_price(program_id="p")
    assert result["status"] == "NO_CANDIDATES" and result["days"][0]["advice"] == []
    assert result["days"][0]["original_items"] == [{"action": "HOLD"}]


def test_budget_defers_atomically_after_snapshot_release(chain, monkeypatch):
    def expired(**kwargs):
        assert chain.closed
        raise AdvisoryModelFirstError("bounded", reason_code="ADVISORY_ENTRY_PRICE_DEFERRED_BUDGET")
    monkeypatch.setattr(api, "project_generic_raw_price_sets_v1", expired)
    result = chain.service.read_price(program_id="p")
    assert result["status"] == "DEFERRED_BUDGET" and not result["days"] and not result["complete"]
    assert len(result["original_lists"]) == 1


@pytest.mark.parametrize("requests", [[{"target_trade_date": "2099-01-01"}], [{}]*2, [{}]*21])
def test_invalid_or_future_original_queries_do_not_open_snapshot(chain, requests):
    with pytest.raises(AdvisoryModelFirstError):
        chain.service.read_batch(program_id="p", requests=requests)
    assert not chain.events and not chain.model_loads


def test_get_batch_routes_validation_and_no_config_does_not_open_database(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.routers import advisory
    monkeypatch.delenv("AISTOCK_ADVISORY_GENERIC_PRICE_CONFIG", raising=False)
    app = FastAPI()
    app.include_router(advisory.router, prefix="/api/v1")
    with TestClient(app) as client:
        path = "/api/v1/advisory/programs/p/generic-entry-price"
        result = client.get(path)
        assert result.status_code == 200 and result.json()["status"] == "NOT_CONFIGURED"
        result = client.post(path+"/batch", json={"requests": [{"target_trade_date": "2026-06-11"}]})
        assert result.status_code == 200 and result.json()["status"] == "NOT_CONFIGURED"
        assert client.post(path+"/batch", json={"requests": []}).status_code == 422
        assert client.post(path+"/batch", json={"requests": [{"future_data": True}]}).status_code == 422
