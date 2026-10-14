"""Reference-price availability is not a score or tradability policy."""

from contextlib import contextmanager
from datetime import date, timedelta

import pandas as pd
import pytest

from backend.services.selection_center.models import SelectionCandidate, SelectionMode, TargetPosition
from backend.services.selection_center.prospective_evidence import (
    CandidateStageName,
    StageReceiptStatus,
    build_stage_receipt,
)
from backend.services.selection_center.risk_policy import StockRiskPolicyService
from backend.services.selection_center.runtime_profile import parse_selection_runtime_profile
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.models import AlphaMode
from backend.services.strategy_package.multi_alpha_live import MultiAlphaLivePredictionProvider
from backend.services.strategy_package.reference_prices import reference_price_observations_from_rows
from backend.services.strategy_package.selection_artifact import (
    StrategyPackageSelectionArtifactService,
    build_reference_price_source_receipt,
)
from backend.services.strategy_package.selection_computation import (
    PreparedPackageComponentLineageV1,
    PreparedPackageSignalV1,
    SelectionArtifactHeaderV1,
    StrategyPackageSelectionComputation,
    StrategyPackageSelectionComputationRequestV1,
    StrategyPackageSelectionReadOnlyProvidersV1,
    selection_runtime_profile_sha256,
)
from backend.services.trading_core.errors import DataUnavailableError


C = date(2026, 1, 19)
P = date(2026, 1, 16)
SYMBOL = "301362.SZ"
NORMAL = "000001.SZ"


class _Market:
    def __init__(self, *, quotes=None, suspensions=None, calendar=None):
        self.quotes = quotes or {}
        self.suspensions = suspensions or {}
        self.calendar = calendar
        self.reads = []
        self.rows = []

    @contextmanager
    def connection(self):
        yield self

    @contextmanager
    def cursor(self):
        yield self

    def execute(self, sql, params):
        self.reads.append((sql, params))
        if "market.kline_daily_raw" in sql:
            cutoff, symbols = params
            if "DISTINCT ON" in sql:
                self.rows = []
                for symbol in symbols:
                    matches = [
                        (day, price) for (code, day), price in self.quotes.items() if code == symbol and day < cutoff
                    ]
                    if matches:
                        day, price = max(matches)
                        self.rows.append((symbol, day, price))
            else:
                self.rows = [
                    (symbol, self.quotes[(symbol, cutoff)]) for symbol in symbols if (symbol, cutoff) in self.quotes
                ]
        elif "market.suspend_d" in sql:
            symbols, start, end = params
            self.rows = [
                (symbol, day, kind, timing)
                for (symbol, day), (kind, timing) in self.suspensions.items()
                if symbol in symbols and start <= day <= end
            ]
        elif "market.trading_calendar" in sql:
            start, end = params
            self.rows = []
            while start <= end:
                if self.calendar is None or start in self.calendar:
                    self.rows.append((start, start.weekday() < 5 if self.calendar is None else self.calendar[start]))
                start += timedelta(days=1)
        else:
            raise AssertionError("Only bounded market reads are permitted")

    def fetchall(self):
        return self.rows


def _scores(market, rows=None, *, cutoff=C, topk=1, required=True):
    service = StrategyPackageSelectionArtifactService(conn_factory=market.connection)
    return service._scores_from_live_result(
        rows or [{"symbol": SYMBOL, "score": 0.5, "rank": 1}],
        package_id="regression-package",
        manifest_sha256="a" * 64,
        trade_date=cutoff,
        topk=topk,
        include_reference_price=required,
        universe_count=2,
    )


def test_legal_full_day_suspension_preserves_last_real_quote_and_score():
    market = _Market(quotes={(SYMBOL, P): 44980}, suspensions={(SYMBOL, C): ("S", None)})
    rows = _scores(market)
    assert (rows[0]["symbol"], rows[0]["score"], rows[0]["rank"], rows[0]["reference_price"]) == (SYMBOL, 0.5, 1, 44.98)
    availability = rows[0]["component_scores"]["reference_price_availability"]
    assert availability["state"] == "SUSPENSION_LAST_OBSERVED_CLOSE"
    assert availability["quote_date"] == P.isoformat()
    assert availability["as_of_date"] == C.isoformat()


def test_tradable_stock_missing_close_is_still_an_explicit_error():
    with pytest.raises(DataUnavailableError, match="reference prices are missing"):
        _scores(_Market(quotes={(SYMBOL, P): 44980}))


@pytest.mark.parametrize("case", ["partial", "resume", "calendar_gap", "no_prior", "invalid_current", "invalid_prior"])
def test_missing_or_conflicting_proof_must_not_forward_fill(case):
    quotes = {(SYMBOL, P): 44980}
    suspensions = {(SYMBOL, C): ("S", None)}
    calendar = None
    if case == "partial":
        suspensions[(SYMBOL, C)] = ("S", "09:30-10:30")
    elif case == "resume":
        suspensions[(SYMBOL, C)] = ("R", None)
    elif case == "calendar_gap":
        calendar = {C: True}
    elif case == "no_prior":
        quotes = {(SYMBOL, C + timedelta(days=1)): 44980}
    elif case == "invalid_current":
        quotes[(SYMBOL, C)] = 0
    elif case == "invalid_prior":
        quotes[(SYMBOL, P)] = 0
    with pytest.raises(DataUnavailableError, match="reference prices are missing"):
        _scores(_Market(quotes=quotes, suspensions=suspensions, calendar=calendar))


def test_each_intervening_open_day_requires_suspension_and_future_is_not_consumed():
    prior = date(2026, 1, 15)
    market = _Market(
        quotes={(SYMBOL, prior): 44000, (SYMBOL, C + timedelta(days=1)): 55000},
        suspensions={(SYMBOL, C): ("S", None), (SYMBOL, C + timedelta(days=1)): ("S", None)},
    )
    with pytest.raises(DataUnavailableError):
        _scores(market)
    market.suspensions[(SYMBOL, P)] = ("S", None)
    rows = _scores(market)
    assert rows[0]["reference_price"] == 44.0
    assert rows[0]["component_scores"]["reference_price_availability"]["suspension_dates"] == [
        P.isoformat(),
        C.isoformat(),
    ]
    assert all(value <= C for _sql, params in market.reads for value in params if isinstance(value, date))


@pytest.mark.parametrize("cutoff", [P, C, C + timedelta(days=1)])
def test_normal_quotes_are_unchanged_across_dates(cutoff):
    rows = _scores(_Market(quotes={(SYMBOL, cutoff): 44980}), cutoff=cutoff)
    assert rows[0]["reference_price"] == 44.98
    assert rows[0]["component_scores"]["reference_price_availability"]["state"] == "EXACT_CLOSE"
    TargetPosition.model_validate(
        {**{key: value for key, value in rows[0].items() if key != "component_scores"}, "target_quantity": 100}
    )


def _compute(rows, *, suspended=True, exclude=True):
    package_id = "regression-package"
    candidates = tuple(SelectionCandidate.model_validate(row) for row in rows)
    profile = parse_selection_runtime_profile(
        {
            "runtime_profile": {
                "selection": {"top_k": 1},
                "tradability": {"exclude_suspended": exclude},
                "risk_policy": {"enabled": False},
                "hmm": {"enabled": False},
            }
        }
    )
    hmm_metadata = {"enabled": False, "status": "NOT_APPLICABLE", "generation_mode": "NOT_APPLICABLE"}
    prepared = PreparedPackageSignalV1(
        package_id=package_id,
        package_version="1.0.0",
        manifest_sha256="a" * 64,
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        component_lineage=(PreparedPackageComponentLineageV1("alpha", 1.0, ("factor",), "rank"),),
        alpha_raw_candidates=candidates,
        hmm_adjusted_candidates=candidates,
        hmm_receipt=build_stage_receipt(
            stage=CandidateStageName.HMM_ADJUSTED,
            status=StageReceiptStatus.NOT_APPLICABLE,
            input_count=len(candidates),
            candidates=[],
            semantic_payload={"enabled": False, "generation_mode": "NOT_APPLICABLE"},
        ),
        hmm_metadata=hmm_metadata,
        artifact_header=SelectionArtifactHeaderV1(
            "artifact", "b" * 64, package_id, "a" * 64, C + timedelta(days=1), "DB_HISTORICAL", "c" * 64
        ),
    )
    request = StrategyPackageSelectionComputationRequestV1(
        trade_date=C + timedelta(days=1),
        data_source="DB_HISTORICAL",
        selection_mode=SelectionMode.SINGLE_PACKAGE,
        ordered_package_ids=(package_id,),
        package_runtime_profiles={package_id: profile},
        package_runtime_profile_hashes={package_id: selection_runtime_profile_sha256(profile)},
        package_top_k={package_id: 1},
    )

    class _Suspensions:
        def get_suspended_symbols(self, symbols, trade_date):
            assert trade_date == C + timedelta(days=1)
            return (
                {SYMBOL: {"source": "market.suspend_d", "suspend_type": "S", "suspend_timing": None}}
                if suspended
                else {}
            )

    return StrategyPackageSelectionComputation().compute(
        request=request,
        prepared_signals={package_id: prepared},
        providers=StrategyPackageSelectionReadOnlyProvidersV1(
            StockRiskPolicyService(), TradabilityFilter(suspend_provider=_Suspensions())
        ),
    )


@pytest.mark.parametrize(
    "exclude,suspended,selected", [(True, True, NORMAL), (False, True, SYMBOL), (True, False, SYMBOL)]
)
def test_original_filter_policy_and_topk_are_preserved(exclude, suspended, selected):
    market = _Market(quotes={(SYMBOL, P): 44980, (NORMAL, C): 12000}, suspensions={(SYMBOL, C): ("S", None)})
    rows = _scores(market, [{"symbol": SYMBOL, "score": 0.5, "rank": 1}, {"symbol": NORMAL, "score": 0.3, "rank": 2}])
    result = _compute(rows, suspended=suspended, exclude=exclude)
    assert [row.symbol for row in result.aggregate_results] == [selected]
    assert result.stage_trace_by_package["regression-package"].alpha_raw.output_count == 2
    if selected == NORMAL:
        assert result.excluded_results["regression-package"][0].reason == "suspended_by_suspend_d"
    assert [(row["symbol"], row["score"], row["rank"]) for row in rows] == [(SYMBOL, 0.5, 1), (NORMAL, 0.3, 2)]


def test_promoted_candidate_missing_price_fails_before_target_quantities():
    rows = _scores(
        _Market(quotes={(SYMBOL, C): 44980}),
        [{"symbol": SYMBOL, "score": 0.5, "rank": 1}, {"symbol": NORMAL, "score": 0.3, "rank": 2}],
    )
    with pytest.raises(DataUnavailableError, match="effective selection candidates"):
        _compute(rows)


def test_unpriced_advisory_raw_path_does_not_read_or_add_price_requirements():
    market = _Market()
    rows = _scores(market, required=False)
    assert market.reads == []
    assert "reference_price_required" not in rows[0]["component_scores"]
    assert _compute(rows, suspended=False).aggregate_results[0].reference_price is None


def test_multi_alpha_uses_identical_price_availability_without_changing_combination():
    service = StrategyPackageSelectionArtifactService(
        conn_factory=_Market(
            quotes={(SYMBOL, P): 44980},
            suspensions={(SYMBOL, C): ("S", None)},
        ).connection
    )
    provider = MultiAlphaLivePredictionProvider(
        package_repository=None,
        artifact_repository=None,
        runtime_asset_resolver=None,
        live_inference_provider=None,
        reference_price_loader=service._load_reference_prices,
    )
    frame = pd.DataFrame(
        [
            {
                "instrument": SYMBOL,
                "combined_score": 0.5,
                "raw__a": 0.4,
                "norm__a": 0.6,
                "rank__a": 1,
                "raw__b": 0.6,
                "norm__b": 0.4,
                "rank__b": 2,
            }
        ]
    )
    rows = provider._artifact_rows(
        frame, leg_ids=["a", "b"], weights={"a": 0.5, "b": 0.5}, trade_date=C, topk=1, include_reference_price=True
    )
    assert rows[0]["reference_price"] == 44.98
    assert rows[0]["score"] == 0.5
    assert rows[0]["component_scores"]["a"]["raw_score"] == 0.4
    observations = reference_price_observations_from_rows(rows)
    receipt = build_reference_price_source_receipt(
        symbols=[SYMBOL], trade_date=C, price_by_symbol={SYMBOL: 44.98}, observations=observations
    )
    assert receipt["query_template_version"] == "v2"
    assert date.fromisoformat(receipt["first_observed_at"][:10]) >= C
    changed = {SYMBOL: {**observations[SYMBOL], "quote_date": "2026-01-15"}}
    assert (
        receipt["content_hash"]
        != build_reference_price_source_receipt(
            symbols=[SYMBOL],
            trade_date=C,
            price_by_symbol={SYMBOL: 44.98},
            observations=changed,
        )["content_hash"]
    )
