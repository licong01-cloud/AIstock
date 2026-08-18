from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from backend.services.advisory_model_first import model_inference
from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.model_bundle import LoadedAdvisoryModelBundle
from backend.services.advisory_model_first.model_binding_resolution import (
    AdvisoryModelBindingResolutionV1,
)
from backend.services.advisory_model_first.model_inference import AdvisoryModelShadowService, _score
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.realtime_feature_source import (
    PersistedAdvisoryReviewIdentity,
    RealtimeFeatureInputs,
)
from backend.services.advisory_model_first.target_binding import (
    BINDING_VERSION_ID,
    FUND_LEG_ID,
    LSTM_LEG_ID,
    MANIFEST_SHA256,
    PACKAGE_ID,
    PROGRAM_ID,
    RUNTIME_SEMANTICS_HASH,
    RUNTIME_SEMANTICS_PAYLOAD,
    STYLE_PROFILE_ID,
)
from backend.services.selection_center.models import SelectionRunStatus
from backend.services.trading_core.errors import DataUnavailableError

REVIEW_POLICY_HASH = "a" * 64
REVIEW_POLICY = {
    "stop_loss_bps": 800,
    "take_profit_bps": 1800,
    "trailing_stop_bps": 700,
    "take_profit_mode": "trailing",
}


class _ProgramService:
    def __init__(self, *, target_count: int = 2) -> None:
        self.program = SimpleNamespace(
            program_id=PROGRAM_ID,
            package_ids=[PACKAGE_ID],
            target_count=target_count,
            review_policy=REVIEW_POLICY,
            review_policy_sha256=REVIEW_POLICY_HASH,
        )
        self.binding = {
            "binding_version_id": BINDING_VERSION_ID,
            "package_ids": [PACKAGE_ID],
        }

    def get_program(self, program_id: str):
        assert program_id == PROGRAM_ID
        return self.program

    def active_binding(self, program_id: str):
        assert program_id == PROGRAM_ID
        return self.binding

    def recommendation_list_versions(self, program_id: str, *, limit: int, offset: int):
        assert (program_id, limit, offset) == (PROGRAM_ID, 500, 0)
        return [
            {
                "list_version_id": "list-1",
                "review_run_id": "review-1",
                "binding_version_id": BINDING_VERSION_ID,
                "trade_date": "2026-07-21",
                "target_trade_date": "2026-07-21",
                "selection_as_of_trade_date": "2026-07-20",
            }
        ]

    def recommendation_list_version_detail(self, list_version_id: str):
        assert list_version_id == "list-1"
        return {
            "list_version": self.recommendation_list_versions(PROGRAM_ID, limit=500, offset=0)[0],
            "items": [
                {
                    "symbol": "000001.SZ",
                    "evidence_json": {
                        "source_run_id": "old-selection-run",
                        "reference_price_trade_date": "2026-07-20",
                        "review_policy_sha256": REVIEW_POLICY_HASH,
                    }
                },
                {
                    "symbol": "000002.SZ",
                    "evidence_json": {
                        "source_run_id": "selection-1",
                        "reference_price_trade_date": "2026-07-20",
                        "review_policy_sha256": REVIEW_POLICY_HASH,
                    }
                },
            ],
        }

class _ReviewSource:
    def get(self, review_run_id: str):
        assert review_run_id == "review-1"
        return PersistedAdvisoryReviewIdentity(
            review_run_id=review_run_id,
            program_id=PROGRAM_ID,
            binding_version_id=BINDING_VERSION_ID,
            trade_date=pd.Timestamp("2026-07-21").date(),
            selection_run_id="selection-1",
            selection_run_ids=("selection-1",),
        )


class _SelectionService:
    def __init__(self) -> None:
        self.run = SimpleNamespace(
            status=SelectionRunStatus.SUCCEEDED,
            trade_date=pd.Timestamp("2026-07-21").date(),
            package_ids=[PACKAGE_ID],
            runtime_config={},
            manifest_sha256_by_package={PACKAGE_ID: MANIFEST_SHA256},
            aggregate_results=[
                _candidate("000001.SZ", rank=1, score=0.8),
                _candidate("000002.SZ", rank=2, score=0.2),
            ],
        )

    def get_run(self, run_id: str):
        assert run_id == "selection-1"
        return self.run


class _OtherProgramService:
    def get_program(self, program_id: str):
        return SimpleNamespace(program_id=program_id, package_ids=["pkg_single_alpha"], target_count=20)

    def active_binding(self, program_id: str):
        return {
            "binding_version_id": "advb_single_alpha",
            "package_ids": ["pkg_single_alpha"],
        }


class _Booster:
    def feature_name(self) -> list[str]:
        return list(MODEL_FEATURE_COLUMNS)

    def predict(self, matrix: pd.DataFrame, pred_contrib: bool = False):
        if pred_contrib:
            output = np.zeros((len(matrix), len(MODEL_FEATURE_COLUMNS) + 1), dtype=float)
            output[:, 0] = [0.1, -0.2]
            output[:, 1] = [0.4, 0.3]
            return output
        return np.asarray([0.1, 0.9], dtype=float)


class _BindingResolver:
    def is_configured(self, **kwargs: object) -> bool:
        return kwargs.get("program_id") == PROGRAM_ID and kwargs.get("binding_version_id") == BINDING_VERSION_ID

    def resolve(self, **_: object) -> AdvisoryModelBindingResolutionV1:
        return AdvisoryModelBindingResolutionV1(
            program_id=PROGRAM_ID,
            binding_version_id=BINDING_VERSION_ID,
            package_id=PACKAGE_ID,
            manifest_sha256=MANIFEST_SHA256,
            style_profile_id=STYLE_PROFILE_ID,
            style_profile_hash="8e8226885af25dbf1830403ea2ba768ec4a135a35680f827ad30994c0369904b",
            selection_runtime_semantics_hash=RUNTIME_SEMANTICS_HASH,
            feature_schema_version="advisory_feature_schema_v1",
            feature_schema_hash="e56adb47d444df26e35eb327d3aacacd273477edf67c4c1db201ea5b4c3bd49c",
            bundle_id="bundle-1",
            bundle_manifest_sha256="c" * 64,
            component_roles={"lstm": LSTM_LEG_ID, "fund": FUND_LEG_ID},
            descriptor_sha256="d" * 64,
        )


class _InspectingBooster(_Booster):
    def __init__(self) -> None:
        self.seen: pd.DataFrame | None = None

    def predict(self, matrix: pd.DataFrame, pred_contrib: bool = False):
        self.seen = matrix.copy()
        return super().predict(matrix, pred_contrib=pred_contrib)


class _FeatureSource:
    def __init__(self, inputs: RealtimeFeatureInputs) -> None:
        self.inputs = inputs
        self.calls = 0
        self.last_kwargs: dict[str, object] = {}

    def load(self, **kwargs: object) -> RealtimeFeatureInputs:
        self.calls += 1
        self.last_kwargs = dict(kwargs)
        return self.inputs


def _candidate(symbol: str, *, rank: int, score: float):
    component_scores = {
        "raw_rank": rank,
        LSTM_LEG_ID: {
            "raw_score": score,
            "normalized_score": score,
            "leg_rank": rank,
            "weight": 0.6966591521,
        },
        FUND_LEG_ID: {
            "raw_score": score,
            "normalized_score": score,
            "leg_rank": rank,
            "weight": 0.3033408479,
        },
    }
    return SimpleNamespace(
        symbol=symbol,
        rank=rank,
        score=score,
        component_scores=component_scores,
        selection_entry_price_time="2026-07-20",
    )


def _bundle() -> LoadedAdvisoryModelBundle:
    return LoadedAdvisoryModelBundle(
        bundle_id="bundle-1",
        bundle_path=Path("/model/bundle-1"),
        manifest={
            "package_id": PACKAGE_ID,
            "manifest_sha256": MANIFEST_SHA256,
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": "UNCALIBRATED",
            "selection_runtime_semantics_hash": RUNTIME_SEMANTICS_HASH,
            "selection_runtime_semantics": RUNTIME_SEMANTICS_PAYLOAD,
            "style_profile_id": STYLE_PROFILE_ID,
            "style_profile_hash": "8e8226885af25dbf1830403ea2ba768ec4a135a35680f827ad30994c0369904b",
            "terminal_weights": {LSTM_LEG_ID: 0.6966591521, FUND_LEG_ID: 0.3033408479},
            "continuation_cutoff": "2026-03-10",
            "request_id": "request-1",
            "feature_schema_version": "advisory_feature_schema_v1",
            "feature_schema_hash": "e56adb47d444df26e35eb327d3aacacd273477edf67c4c1db201ea5b4c3bd49c",
        },
        feature_schema={
            "trained_feature_names": list(MODEL_FEATURE_COLUMNS),
            "categorical_vocabulary": {"l2_code_id": [1, 2]},
        },
        hmm_models={},
        baselines={"model_top5": {"mean_excess_return_5": -0.01}},
        booster=_Booster(),
        manifest_file_sha256="c" * 64,
    )


def _feature_inputs() -> RealtimeFeatureInputs:
    dates = pd.bdate_range(end="2026-07-20", periods=90)
    symbols = ["000001.SZ", "000002.SZ"]
    index = pd.MultiIndex.from_product([dates, symbols], names=["datetime", "instrument"])
    day = np.repeat(np.arange(len(dates), dtype=float), len(symbols))
    daily = pd.DataFrame(index=index)
    daily["close"] = 10.0 + day * 0.01
    daily["open"] = daily["close"] - 0.01
    daily["high"] = daily["close"] + 0.1
    daily["low"] = daily["close"] - 0.1
    daily["volume"] = 1000.0 + day
    daily["amount"] = daily["volume"] * daily["close"]
    daily["factor"] = 1.0
    daily["up_limit_price"] = daily["close"] * 1.1
    daily["down_limit_price"] = daily["close"] * 0.9
    daily["prev_close"] = daily.groupby(level="instrument")["close"].shift(1)
    daily["limit_up"] = 0.0
    daily["limit_down"] = 0.0

    static = pd.DataFrame(index=index)
    static["db_turnover_rate"] = 1.0
    static["db_volume_ratio"] = 1.0
    static["db_pe_ttm"] = 10.0
    static["db_pb"] = 1.5
    static["db_circ_mv"] = 1000.0
    static["mf_lg_buy_amt"] = 60.0
    static["mf_lg_sell_amt"] = 40.0
    static["mf_elg_buy_amt"] = 60.0
    static["mf_elg_sell_amt"] = 40.0
    static["bb_rev_yoy"] = 5.0
    static["bb_profit_yoy"] = 4.0
    static["bb_gpr"] = 20.0
    static["bb_npr"] = 10.0
    static["cp_cost_5pct"] = 8.0
    static["cp_cost_50pct"] = 10.0
    static["cp_cost_95pct"] = 12.0
    static["cp_winner_rate"] = 0.5
    static["md_rzye"] = 100.0 + day
    static["l2_code_id"] = np.tile([1, 2], len(dates))
    static["sw2_close"] = 100.0 + day * 0.02
    static["sw2_amount"] = 1_000_000.0 + day
    static["sw2_mf_net_amt"] = 1000.0

    market_symbols = [f"{index:06d}.SZ" for index in range(100, 205)]
    market_index = pd.MultiIndex.from_product([dates, market_symbols], names=["datetime", "instrument"])
    market = pd.DataFrame(index=market_index)
    market["close"] = np.repeat(np.linspace(10.0, 11.0, len(dates)), len(market_symbols))
    market["limit_up"] = 0.0
    benchmark = pd.DataFrame(
        {"close": np.linspace(100.0, 111.0, len(dates))},
        index=pd.MultiIndex.from_product([dates, ["000300.SH"]], names=["datetime", "instrument"]),
    )
    return RealtimeFeatureInputs(
        candidate_daily=daily,
        candidate_static=static,
        market_daily=market,
        benchmark_daily=benchmark,
        suspend_rows=pd.DataFrame(columns=["trade_date", "instrument", "suspend_type"]),
        hmm_states=pd.DataFrame(),
        hmm_unavailable=(),
        trading_calendar=dates,
    )


def _shadow_service(**kwargs: object) -> AdvisoryModelShadowService:
    kwargs.setdefault("binding_resolver", _BindingResolver())
    return AdvisoryModelShadowService(**kwargs)


def test_model_shadow_keeps_rule_path_available_when_model_root_is_missing() -> None:
    feature_source = _FeatureSource(_feature_inputs())
    service = _shadow_service(
        program_service=_ProgramService(),
        selection_service=_SelectionService(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "",
    )
    result = service.model_shadow(program_id=PROGRAM_ID, target_trade_date=pd.Timestamp("2026-07-21").date())
    assert result["status"] == "MODEL_UNAVAILABLE"
    assert result["reason_code"] == "ADVISORY_MODEL_ROOT_NOT_CONFIGURED"
    assert feature_source.calls == 0


def test_forward_model_shadow_uses_frozen_program_binding_without_reading_current_binding() -> None:
    service = _shadow_service(
        program_service=SimpleNamespace(
            get_program=lambda _program_id: (_ for _ in ()).throw(AssertionError("current Program read")),
            active_binding=lambda _program_id: (_ for _ in ()).throw(AssertionError("current binding read")),
            recommendation_list_versions=lambda *_args, **_kwargs: [],
        ),
        model_root_provider=lambda: "",
    )
    frozen_program = SimpleNamespace(
        program_id=PROGRAM_ID,
        package_ids=[PACKAGE_ID],
        target_count=20,
    )

    result = service.model_shadow_for_forward(
        program=frozen_program,
        binding_version_id=BINDING_VERSION_ID,
        target_trade_date=date(2026, 7, 21),
        list_version_id="list-frozen",
        review_run_id="review-frozen",
        selection_run_id="selection-frozen",
    )

    assert result["status"] == "MODEL_UNAVAILABLE"
    assert result["reason_code"] == "ADVISORY_MODEL_ROOT_NOT_CONFIGURED"


def test_forward_selection_context_uses_exact_list_id_without_date_scan() -> None:
    service = _shadow_service(
        program_service=SimpleNamespace(
            recommendation_list_versions=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("forward inference scanned list versions")
            ),
            recommendation_list_version_detail=lambda list_version_id: {
                "list_version": {
                    "list_version_id": list_version_id,
                    "program_id": PROGRAM_ID,
                    "binding_version_id": BINDING_VERSION_ID,
                    "review_run_id": "review-frozen",
                    "trade_date": "2026-07-21",
                    "target_trade_date": "2026-07-21",
                },
                "items": [{"symbol": "000001.SZ"}],
            },
        )
    )

    version, items = service._selection_list_context(
        program_id=PROGRAM_ID,
        target_trade_date=date(2026, 7, 21),
        list_version_id="list-frozen",
    )

    assert version["list_version_id"] == "list-frozen"
    assert items == [{"symbol": "000001.SZ"}]


def test_model_shadow_never_applies_parent_bundle_to_another_program() -> None:
    feature_source = _FeatureSource(_feature_inputs())
    bundle_loads = 0

    def bundle_loader(**_: object) -> LoadedAdvisoryModelBundle:
        nonlocal bundle_loads
        bundle_loads += 1
        return _bundle()

    service = _shadow_service(
        program_service=_OtherProgramService(),
        selection_service=_SelectionService(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model",
        bundle_loader=bundle_loader,
    )
    result = service.model_shadow(
        program_id="advp_single_alpha",
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "MODEL_UNAVAILABLE"
    assert result["reason_code"] == "ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE"
    assert feature_source.calls == 0
    assert bundle_loads == 0


def test_model_shadow_scores_complete_persisted_candidate_group() -> None:
    feature_source = _FeatureSource(_feature_inputs())
    service = _shadow_service(
        program_service=_ProgramService(),
        selection_service=_SelectionService(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model",
        bundle_loader=lambda **_: _bundle(),
    )
    result = service.model_shadow(program_id=PROGRAM_ID, target_trade_date=pd.Timestamp("2026-07-21").date())
    assert result["status"] == "EXPERIMENTAL_SHADOW"
    assert result["candidate_count"] == 2
    assert result["shortlist_count"] == 2
    assert [item["symbol"] for item in result["candidates"]] == ["000002.SZ", "000001.SZ"]
    assert [item["advisory_model_rank"] for item in result["candidates"]] == [1, 2]
    assert all(item["is_top5"] for item in result["candidates"])
    assert feature_source.calls == 1


def test_model_shadow_propagates_validated_canonical_runtime_universe(monkeypatch) -> None:
    selection_service = _SelectionService()
    selection_service.run.runtime_config = {"canonical_pit_authority_profile": {"enabled": True}}
    feature_source = _FeatureSource(_feature_inputs())
    lease = SimpleNamespace(universe_key="aistock_equity_pit_canonical_v2")
    monkeypatch.setattr(model_inference, "has_canonical_pit_runtime_profile", lambda _config: True)
    monkeypatch.setattr(
        model_inference,
        "require_canonical_pit_runtime_binding",
        lambda _config, *, trade_date: lease,
    )
    monkeypatch.setattr(
        model_inference,
        "require_canonical_pit_generation_current",
        lambda _config: lease,
    )
    service = _shadow_service(
        program_service=_ProgramService(),
        selection_service=selection_service,
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model",
        bundle_loader=lambda **_: _bundle(),
    )

    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "EXPERIMENTAL_SHADOW"
    assert feature_source.last_kwargs["pit_universe_key"] == lease.universe_key


def test_model_shadow_attaches_outcome_predictions_from_same_feature_matrix() -> None:
    feature_source = _FeatureSource(_feature_inputs())
    outcome_loads: list[dict[str, object]] = []

    def outcome_loader(**kwargs):
        outcome_loads.append(kwargs)
        return SimpleNamespace(
            outcome_bundle_id="outcome-1",
            manifest={"request_id": "advoutreq_runtime", "horizons": [1, 3, 5, 10, 20]},
        )

    def outcome_scorer(_bundle, features):
        return [
            {"symbol": str(symbol), "horizons": [], "holding_period": {}}
            for symbol in features["instrument"]
        ]

    service = _shadow_service(
        program_service=_ProgramService(),
        selection_service=_SelectionService(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model",
        bundle_loader=lambda **_: _bundle(),
        outcome_bundle_loader=outcome_loader,
        outcome_scorer=outcome_scorer,
    )
    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "EXPERIMENTAL_SHADOW"
    assert result["outcome"]["status"] == "EXPERIMENTAL_SHADOW"
    assert [item["symbol"] for item in result["outcome"]["candidates"]] == [
        "000002.SZ",
        "000001.SZ",
    ]
    assert outcome_loads[0]["parent_bundle_id"] == "bundle-1"
    assert feature_source.calls == 1


def test_model_shadow_attaches_exact_price_range_without_changing_m2_order() -> None:
    price_loads: list[dict[str, object]] = []
    feature_source = _FeatureSource(_feature_inputs())

    def outcome_scorer(_bundle, features):
        return [
            {
                "symbol": str(symbol),
                "horizons": [],
                "holding_period": {},
            }
            for symbol in features["instrument"]
        ]

    def price_loader(**kwargs):
        price_loads.append(kwargs)
        return SimpleNamespace(
            price_range_bundle_id="price-1",
            manifest={"request_id": "advprreq-runtime", "calibration_state": "UNCALIBRATED"},
        )

    def price_scorer(_bundle, features, **_kwargs):
        return [
            {
                "symbol": str(symbol),
                "status": "EXPERIMENTAL_SHADOW",
                "reason_code": None,
                "message": None,
            }
            for symbol in features["instrument"]
        ]

    service = _shadow_service(
        program_service=_ProgramService(),
        selection_service=_SelectionService(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model",
        bundle_loader=lambda **_: _bundle(),
        outcome_bundle_loader=lambda **_: SimpleNamespace(
            outcome_bundle_id="e" * 64,
            manifest={"request_id": "advoutreq-runtime", "horizons": [1, 3, 5, 10, 20]},
        ),
        outcome_scorer=outcome_scorer,
        price_range_bundle_loader=price_loader,
        price_range_scorer=price_scorer,
    )
    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )
    assert result["price_range"]["status"] == "EXPERIMENTAL_SHADOW"
    assert [item["symbol"] for item in result["price_range"]["candidates"]] == [
        item["symbol"] for item in result["candidates"]
    ]
    assert price_loads[0]["parent_bundle_id"] == "bundle-1"
    assert price_loads[0]["outcome_bundle_id"] == "e" * 64
    assert feature_source.calls == 1


def test_calibrated_outcome_keeps_price_range_bound_to_parent_m3_bundle() -> None:
    price_loads: list[dict[str, object]] = []
    parent_m3 = "3" * 64

    service = _shadow_service(
        program_service=_ProgramService(),
        selection_service=_SelectionService(),
        review_source=_ReviewSource(),
        feature_source=_FeatureSource(_feature_inputs()),
        model_root_provider=lambda: "/model",
        bundle_loader=lambda **_: _bundle(),
        outcome_bundle_loader=lambda **_: SimpleNamespace(
            outcome_bundle_id="4" * 64,
            manifest={
                "request_id": "advoutcal_runtime",
                "horizons": [1, 3, 5, 10, 20],
                "calibration_state": "PARTIAL",
                "parent_outcome_bundle_id": parent_m3,
                "binary_calibration_state": "CALIBRATED",
                "return_interval_calibration_state": "CALIBRATED",
                "path_upper_calibration_state": "CALIBRATED",
                "holding_calibration_state": "UNCALIBRATED",
            },
        ),
        outcome_scorer=lambda _bundle, features: [
            {"symbol": str(symbol), "horizons": [], "holding_period": {}}
            for symbol in features["instrument"]
        ],
        price_range_bundle_loader=lambda **kwargs: (
            price_loads.append(kwargs)
            or SimpleNamespace(
                price_range_bundle_id="price-v1-m3",
                manifest={"request_id": "advprreq-runtime", "calibration_state": "UNCALIBRATED"},
            )
        ),
        price_range_scorer=lambda _bundle, features, **_kwargs: [
            {
                "symbol": str(symbol),
                "status": "EXPERIMENTAL_SHADOW",
                "reason_code": None,
                "message": None,
            }
            for symbol in features["instrument"]
        ],
    )

    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["outcome"]["outcome_bundle_id"] == "4" * 64
    assert result["outcome"]["parent_outcome_bundle_id"] == parent_m3
    assert result["price_range"]["outcome_bundle_id"] == parent_m3
    assert price_loads[0]["outcome_bundle_id"] == parent_m3


def test_outcome_unavailable_does_not_remove_m2_ranking() -> None:
    feature_source = _FeatureSource(_feature_inputs())

    def unavailable_loader(**_kwargs):
        raise AdvisoryModelFirstError(
            "no outcome binding",
            reason_code="ADVISORY_OUTCOME_BUNDLE_NOT_AVAILABLE",
        )

    service = _shadow_service(
        program_service=_ProgramService(),
        selection_service=_SelectionService(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model",
        bundle_loader=lambda **_: _bundle(),
        outcome_bundle_loader=unavailable_loader,
    )
    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "EXPERIMENTAL_SHADOW"
    assert len(result["candidates"]) == 2
    assert result["outcome"]["status"] == "OUTCOME_UNAVAILABLE"
    assert result["outcome"]["reason_code"] == "ADVISORY_OUTCOME_BUNDLE_NOT_AVAILABLE"


def test_duplicate_outcome_symbol_is_typed_unavailable_without_changing_m2() -> None:
    feature_source = _FeatureSource(_feature_inputs())
    outcome_bundle = SimpleNamespace(
        outcome_bundle_id="outcome-1",
        manifest={"request_id": "advoutreq_runtime", "horizons": [1, 3, 5, 10, 20]},
    )
    service = _shadow_service(
        program_service=_ProgramService(),
        selection_service=_SelectionService(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model",
        bundle_loader=lambda **_: _bundle(),
        outcome_bundle_loader=lambda **_: outcome_bundle,
        outcome_scorer=lambda _bundle, _features: [
            {"symbol": "000001.SZ"},
            {"symbol": "000001.SZ"},
        ],
    )
    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert len(result["candidates"]) == 2
    assert result["outcome"]["status"] == "OUTCOME_UNAVAILABLE"
    assert result["outcome"]["reason_code"] == "ADVISORY_OUTCOME_INFERENCE_FAILED"


def test_unexpected_outcome_error_is_logged_and_visible_without_changing_m2(caplog) -> None:
    feature_source = _FeatureSource(_feature_inputs())
    outcome_bundle = SimpleNamespace(
        outcome_bundle_id="outcome-1",
        manifest={"request_id": "advoutreq_runtime", "horizons": [1, 3, 5, 10, 20]},
    )

    def broken_scorer(_bundle, _features):
        raise RuntimeError("broken outcome scorer")

    service = _shadow_service(
        program_service=_ProgramService(),
        selection_service=_SelectionService(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model",
        bundle_loader=lambda **_: _bundle(),
        outcome_bundle_loader=lambda **_: outcome_bundle,
        outcome_scorer=broken_scorer,
    )
    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert len(result["candidates"]) == 2
    assert result["outcome"]["status"] == "OUTCOME_UNAVAILABLE"
    assert result["outcome"]["reason_code"] == "ADVISORY_OUTCOME_INFERENCE_FAILED"
    assert result["outcome"]["message"] == "unexpected outcome inference failure: RuntimeError"
    assert "advisory outcome shadow failed unexpectedly" in caplog.text


def test_model_shadow_accepts_legal_shallow_candidate_group() -> None:
    feature_source = _FeatureSource(_feature_inputs())
    service = _shadow_service(
        program_service=_ProgramService(target_count=20),
        selection_service=_SelectionService(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model",
        bundle_loader=lambda **_: _bundle(),
    )

    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "EXPERIMENTAL_SHADOW"
    assert result["candidate_count"] == 2
    assert result["shortlist_count"] == 2


def test_model_shadow_rejects_parent_score_that_differs_from_frozen_leg_sum() -> None:
    selection_service = _SelectionService()
    selection_service.run.aggregate_results[0].score = 0.7
    feature_source = _FeatureSource(_feature_inputs())
    service = _shadow_service(
        program_service=_ProgramService(),
        selection_service=selection_service,
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model",
        bundle_loader=lambda **_: _bundle(),
    )

    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "MODEL_UNAVAILABLE"
    assert result["reason_code"] == "ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH"
    assert feature_source.calls == 0


def test_model_shadow_exposes_missing_review_run_as_typed_unavailable() -> None:
    class _MissingReviewSource:
        def get(self, _: str):
            raise DataUnavailableError("advisory review run does not exist", context={"review_run_id": "review-1"})

    feature_source = _FeatureSource(_feature_inputs())
    service = _shadow_service(
        program_service=_ProgramService(),
        selection_service=_SelectionService(),
        review_source=_MissingReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model",
        bundle_loader=lambda **_: _bundle(),
    )

    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "MODEL_UNAVAILABLE"
    assert result["reason_code"] == "ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE"
    assert result["message"] == "persisted Advisory or Selection input is unavailable"
    assert feature_source.calls == 0


def test_model_shadow_rejects_non_numeric_terminal_weight_as_typed_mismatch() -> None:
    bundle = _bundle()
    bundle.manifest["terminal_weights"] = {LSTM_LEG_ID: "invalid", FUND_LEG_ID: 0.3033408479}
    feature_source = _FeatureSource(_feature_inputs())
    service = _shadow_service(
        program_service=_ProgramService(),
        selection_service=_SelectionService(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model",
        bundle_loader=lambda **_: bundle,
    )

    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "MODEL_UNAVAILABLE"
    assert result["reason_code"] == "ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH"
    assert feature_source.calls == 0


def test_score_reuses_strict_numeric_contract_and_marks_unseen_sector_missing() -> None:
    features = pd.DataFrame(0.0, index=range(2), columns=MODEL_FEATURE_COLUMNS)
    features["instrument"] = ["000001.SZ", "000002.SZ"]
    features["selection_effective_rank"] = [1, 2]
    features["parent_combined_score"] = [0.8, 0.2]
    features["market_up_ratio"] = [Decimal("0.5"), Decimal("0.75")]
    features["l2_code_id"] = [999, 999]
    features["l2_code_id__missing"] = [0, 0]
    booster = _InspectingBooster()

    scored = _score(replace(_bundle(), booster=booster), features)

    assert len(scored) == 2
    assert booster.seen is not None
    assert pd.api.types.is_float_dtype(booster.seen["market_up_ratio"])
    assert booster.seen["l2_code_id"].isna().all()
    assert booster.seen["l2_code_id__missing"].tolist() == [1, 1]
