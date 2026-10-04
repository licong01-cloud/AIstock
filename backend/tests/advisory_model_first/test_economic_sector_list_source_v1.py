"""Published-roster contracts; no model fit, candidate generation or real DB writes."""

from contextlib import contextmanager
from copy import deepcopy
from datetime import date
from types import SimpleNamespace as NS

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_sector_list_source_v1 as m

D, T = date(2026, 8, 27), date(2026, 8, 28)
ROLES, WEIGHTS = {"lstm": "a", "fund": "b"}, {"a": 0.6, "b": 0.4}
SCOPE = dict(
    program_id="p",
    package_id="pkg",
    manifest_sha256="a" * 64,
    parent_policy_identity="b" * 64,
    policy_identity="c" * 64,
    universe_selection={"mode": "stock_universe", "pool_ids": []},
)


@pytest.fixture
def original(monkeypatch):
    rows = [
        NS(
            symbol=f"{i:06}.SH",
            rank=i,
            score=1.0,
            component_scores={key: {"normalized_score": 1.0, "weight": weight} for key, weight in WEIGHTS.items()},
            selection_entry_price_time="2026-08-27T17:00:00+08:00",
        )
        for i in range(1, 31)
    ]
    version = dict(
        list_version_id="l",
        program_id="p",
        binding_version_id="b",
        review_run_id="r",
        target_trade_date=T.isoformat(),
        selection_as_of_trade_date=D.isoformat(),
        version_status="PUBLISHED",
        summary_json={},
    )
    items = [
        dict(
            list_version_id="l",
            program_id="p",
            binding_version_id="b",
            symbol=row.symbol,
            rank=row.rank,
            score=row.score,
            component_scores_json=deepcopy(row.component_scores),
            action="WAITING",
            evidence_json={"source_run_id": "s", "review_policy_sha256": "d" * 64},
        )
        for row in rows
    ]
    run = NS(
        run_id="s",
        trade_date=T,
        package_ids=["pkg"],
        manifest_sha256_by_package={"pkg": "a" * 64},
        status="SUCCEEDED",
        valid_no_candidate=False,
        no_candidate_reason=None,
        aggregate_results=rows,
        runtime_config={},
    )
    state = NS(
        version=version,
        items=items,
        run=run,
        review=["r", "p", "b", T, "s", ["s"], {}],
        binding=["p", "b", ["pkg"], {}],
        queries=[],
        rollbacks=0,
        price_calls=0,
    )

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, sql, params):
            assert sql.lstrip().startswith("SELECT") and isinstance(params, tuple)
            state.queries.append((sql, params))
            self.row = state.review if "advisory_review_run" in sql else state.binding

        def fetchone(self):
            return self.row

    @contextmanager
    def connection():
        try:
            yield NS(cursor=lambda: Cursor())
        finally:
            state.rollbacks += 1

    repo = NS(
        get_list_version=lambda _: state.version,
        list_version_for_date=lambda *a, **k: state.version,
        latest_list_version=lambda *a, **k: state.version,
        list_version_items=lambda _: state.items,
    )
    monkeypatch.setattr(m, "AdvisoryProgramPGRepository", lambda **_: repo)
    monkeypatch.setattr(m, "SelectionCenterRepository", lambda **_: NS(get_run=lambda _: state.run))
    monkeypatch.setattr(m, "list_version_to_dict", lambda value: value)
    monkeypatch.setattr(m, "list_item_to_dict", lambda value: value)
    monkeypatch.setattr(
        m.PostgresRealtimeFeatureSource, "_recent_trading_dates", lambda *a, **k: pd.bdate_range(end=D, periods=21)
    )
    monkeypatch.setattr(m.PostgresRealtimeFeatureSource, "_trading_calendar", lambda *a, **k: pd.DatetimeIndex([D, T]))

    def prices(*a, symbols, **k):
        state.price_calls += 1
        return {}, tuple(dict(symbol=symbol, reason_code="NORMAL_MISSING_PRICE") for symbol in symbols)

    monkeypatch.setattr(m.PostgresRealtimeFeatureSource, "_price_range_contexts", prices)
    state.source = m.EconomicSectorPublishedListSourceV1(read_session=NS(connection=connection))
    return state


def load(original, **kwargs):
    return original.source.load_day(
        program_id="p",
        target_date=T,
        component_roles=ROLES,
        terminal_weights=WEIGHTS,
        model_scope=deepcopy(SCOPE),
        **kwargs,
    )


def test_published_top20_preserves_watch_extras_policy_and_missing_prices(original):
    for item in original.items[20:]:
        item["action"] = "WATCH"
    original.items.extend(
        {**deepcopy(original.items[-1]), "symbol": f"{i:06}.SZ", "rank": None, "action": "HOLD"}
        for i in range(100, 117)
    )
    result = load(original)
    receipt = result["candidate_receipt"]
    assert result["candidates"].instrument.tolist() == [row.symbol for row in original.run.aggregate_results[:20]]
    assert receipt["original_list_item_count"] == 47 and len(receipt["unmodeled_items"]) == 27
    assert len(receipt["price_context_unavailable"]) == 20 and result["price_contexts"] == {}
    assert receipt["source_review_policy_sha256"] == "d" * 64 != SCOPE["parent_policy_identity"]
    assert (
        receipt["package_qualification_rechecked"] is receipt["database_written"] is receipt["outcomes_read"] is False
    )
    assert len(result["calendar"]) == 22 and original.rollbacks == 1 and len(original.queries) == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("program_id", "other"),
        ("target_trade_date", "2026-08-31"),
        ("version_status", "DRAFT"),
        ("selection_as_of_trade_date", "2026-08-26"),
    ],
)
def test_wrong_list_identity_and_clock_fail(original, field, value):
    original.version[field] = value
    if field == "selection_as_of_trade_date":
        original.run.runtime_config["advisory_date_context"] = {"selection_as_of_trade_date": D.isoformat()}
    with pytest.raises(m.AdvisoryModelFirstError):
        load(original)
    assert original.rollbacks == 1


@pytest.mark.parametrize(
    "kind",
    ["review", "binding", "manifest", "duplicate", "rank_gap", "score", "leg", "future", "policy", "missing_tail"],
)
def test_actual_conflicts_fail_closed(original, kind):
    if kind == "review":
        original.review[4] = "foreign"
    if kind == "binding":
        original.binding[1] = "foreign"
    if kind == "manifest":
        original.run.manifest_sha256_by_package["pkg"] = "e" * 64
    if kind == "duplicate":
        original.items[1]["symbol"] = original.items[0]["symbol"]
    if kind == "rank_gap":
        original.items[1]["rank"] = 99
    if kind == "score":
        original.items[0]["score"] = 2.0
    if kind == "leg":
        original.items[0]["component_scores_json"]["a"]["weight"] = 0.5
    if kind == "future":
        original.run.aggregate_results[0].selection_entry_price_time = "2026-08-28T09:30:00+08:00"
    if kind == "policy":
        original.items[0]["evidence_json"]["review_policy_sha256"] = "f" * 64
    if kind == "missing_tail":
        del original.items[18:20]
    with pytest.raises(m.AdvisoryModelFirstError):
        load(original)
    assert original.rollbacks == 1


def test_missing_policy_stale_suspended_quote_and_display_enrichment_are_not_gates(original):
    for item in original.items:
        item["evidence_json"].pop("review_policy_sha256")
    original.run.aggregate_results[0].selection_entry_price_time = "2026-08-20T17:00:00+08:00"
    original.items[0]["component_scores_json"]["selection_price_guidance"] = {"display_only": True}
    result = load(original)
    assert len(result["candidates"]) == 20 and result["candidate_receipt"]["source_policy_state"] == "UNKNOWN_POLICY"


@pytest.mark.parametrize(
    "universe",
    [{"mode": "single_index", "pool_ids": ["csi300"]}, {"mode": "index_union", "pool_ids": ["csi300", "csi500"]}],
)
def test_index_pool_uses_original_published_ranks_without_current_member_resolution(original, universe):
    original.binding[3]["universe_selection"] = universe
    scope = {**SCOPE, "universe_selection": universe}
    original.run.aggregate_results[0].rank = 50
    result = original.source.load_day(
        program_id="p", target_date=T, component_roles=ROLES, terminal_weights=WEIGHTS, model_scope=scope
    )
    assert result["candidates"].selection_effective_rank.tolist() == list(range(1, 21))
    assert result["scope"]["universe_selection"] == universe


def test_legitimate_no_candidate_does_not_call_price_reader(original):
    original.items.clear()
    original.run.aggregate_results.clear()
    original.run.status = "VALID_NO_CANDIDATE"
    original.run.valid_no_candidate = True
    original.run.no_candidate_reason = "ORIGINAL_EMPTY"
    result = load(original)
    assert result["candidates"].empty and original.price_calls == 0


def test_original_zero_index_admission_is_empty_not_a_new_selection(original):
    original.items.clear()
    universe = {"mode": "single_index", "pool_ids": ["csi300"]}
    original.binding[3]["universe_selection"] = universe
    original.version["summary_json"]["advisory_universe_receipt"] = {"output_candidate_count": 0}

    def read():
        return original.source.load_day(
            program_id="p",
            target_date=T,
            component_roles=ROLES,
            terminal_weights=WEIGHTS,
            model_scope={**SCOPE, "universe_selection": universe},
        )

    result = read()
    assert result["candidates"].empty and original.price_calls == 0
    del original.version["summary_json"]["advisory_universe_receipt"]
    with pytest.raises(m.AdvisoryModelFirstError, match="nonempty original"):
        read()


def test_missing_published_list_is_not_successful_empty(original):
    original.version = None
    with pytest.raises(m.AdvisoryModelFirstError, match="not available") as exc:
        load(original)
    assert exc.value.reason_code == "ADVISORY_SECTOR_FROZEN_LIST_NOT_READY"
