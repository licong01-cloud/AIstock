from __future__ import annotations

from copy import deepcopy

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import strategy_packages as router_module
from backend.services.multi_alpha.combine_backtest import InMemoryCombineBacktestRepository
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import AlphaCombinationPolicy, AlphaMode, PackageStatus
from backend.services.strategy_package.multi_alpha_promotion import (
    MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION,
    MULTI_ALPHA_PAPER_ADMISSION_BLOCKER,
    MultiAlphaPackagePromotionService,
)
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError
from backend.tests.strategy_package.test_multi_alpha_base_schema import _single_manifest


RUN_ID = "macb_target_two_leg_20260627"
A1_LEG = "a1_plus3_LSTM_h20"
FUND_LEG = "new_FUNDGROWTH_h20"
A1_SEED = "qear_a1_seed_42"
FUND_SEED = "qear_fundgrowth_seed_42"
PRED_SHA = "a" * 64


def _seed_child(repo: InMemoryStrategyPackageRepository, name: str, leg_id: str, seed_run_id: str):
    manifest = _single_manifest(name, run_id=seed_run_id)
    component = manifest.alpha_components[0].model_copy(
        update={
            "alpha_id": leg_id,
            "alpha_name": leg_id,
            "holding_period": "20d",
            "rebalance_frequency": "1day",
        }
    )
    manifest = manifest.model_copy(
        update={
            "alpha_components": [component],
            "alpha_combination_policy": AlphaCombinationPolicy(method="identity", weights={leg_id: 1.0}),
            "source_evidence": {"seed_run_ids": [seed_run_id]},
            "manifest_sha256": None,
        }
    )
    return repo.save_manifest(freeze_manifest(manifest))


def _seed_repos():
    combine_repo = InMemoryCombineBacktestRepository()
    package_repo = InMemoryStrategyPackageRepository()
    child_a1 = _seed_child(package_repo, "a1", A1_LEG, A1_SEED)
    child_fund = _seed_child(package_repo, "fund", FUND_LEG, FUND_SEED)
    combine_repo.runs[RUN_ID] = {
        "id": RUN_ID,
        "roster_hash": "roster_hash",
        "roster_json": [
            {"leg_id": A1_LEG, "seed_run_ids": [A1_SEED], "metadata": {"family": "plus3"}},
            {"leg_id": FUND_LEG, "seed_run_ids": [FUND_SEED], "metadata": {"family": "fundgrowth"}},
        ],
        "oos_start": "2024-07-02",
        "oos_end": "2026-03-10",
        "normalize_method": "zscore",
        "walk_forward_json": {"enabled": True, "window": 60, "min_periods": 2},
        "backtest_config_json": {
            "stock_pool": "V25_1_SMALL_CAP",
            "filtered_pool": "filtered_pool_20260428",
            "label_horizon": 20,
            "execution_algo": "V25_1_SMALL_CAP",
            "n_drop": 2,
            "topk": 50,
        },
        "baseline_leg_id": A1_LEG,
        "status": "succeeded",
        "reason": None,
        "created_at": "2026-06-27T00:00:00+00:00",
        "updated_at": "2026-06-27T00:00:00+00:00",
    }
    combine_repo.scheme_results.append(
        {
            "id": "scheme_icw_1",
            "run_id": RUN_ID,
            "weighting_scheme": "ic_weighted",
            "weights_json": {
                A1_LEG: 0.61,
                FUND_LEG: 0.39,
                "combined_prediction_ref": {
                    "uri": f"aistock-prediction-store://multi-alpha/{RUN_ID}/combined_prediction.pkl",
                    "sha256": PRED_SHA,
                },
            },
            "per_window_weights_json": [
                {"window_start": "2025-01-01", "window_end": "2025-03-31", A1_LEG: 0.61, FUND_LEG: 0.39}
            ],
            "cagr": 1.0715,
            "max_drawdown": -0.1651,
            "sharpe": 2.845,
            "calmar": 6.4886,
            "topk_return_20": 0.0631,
            "topk_hit_rate_20": 0.6471,
            "turnover": 19.2,
            "vs_baseline_sharpe_delta": 0.1,
            "vs_baseline_calmar_delta": 0.2,
            "pred_persisted": True,
            "skipped": False,
            "skipped_reason": None,
        }
    )
    return combine_repo, package_repo, child_a1, child_fund


def _service(combine_repo, package_repo):
    return MultiAlphaPackagePromotionService(combine_repository=combine_repo, package_repository=package_repo)


def _request(child_a1, child_fund):
    return {
        "combine_backtest_run_id": RUN_ID,
        "weighting_scheme": "ic_weighted",
        "scheme_result_id": "scheme_icw_1",
        "topk": 50,
        "secondary_topk": [25],
        "package_name": "MA2_a1_plus3_LSTM_new_FUNDGROWTH_icw_h20",
        "component_package_ids": {A1_LEG: child_a1.package_id, FUND_LEG: child_fund.package_id},
        "weight_policy": {
            "mode": "frozen_backtest_terminal_weights",
            "metric": "rank_ic",
            "lookback_trading_days": 252,
            "min_periods": 60,
            "label_horizon": 20,
            "label_maturity_lag_days": 20,
            "clip_negative_to_zero": True,
        },
        "confirmation": MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION,
    }


def _promote(service, child_a1, child_fund, **overrides):
    payload = _request(child_a1, child_fund)
    payload.update(overrides)
    return service.promote_from_combine_run(**payload)


def _reason_code(exc: BaseException) -> str | None:
    return getattr(exc, "context", {}).get("reason_code")


def test_promote_target_two_leg_run_freezes_deterministic_multi_alpha_package() -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    service = _service(combine_repo, package_repo)

    first = _promote(service, child_a1, child_fund)
    second = _promote(service, child_a1, child_fund)

    assert first.package.package_id == second.package.package_id
    assert first.package.manifest_sha256 == second.package.manifest_sha256
    assert first.package.alpha_mode == AlphaMode.MULTI_ALPHA
    assert first.package.package_status == PackageStatus.ASSET_VALIDATED
    assert first.package.package_status != PackageStatus.PAPER_ENABLED
    assert first.paper_admission == {"eligible": False, "blocking": [MULTI_ALPHA_PAPER_ADMISSION_BLOCKER]}
    assert first.package.prediction_ref_uri == f"aistock-prediction-store://multi-alpha/{RUN_ID}/combined_prediction.pkl"
    assert first.package.prediction_ref_sha256 == PRED_SHA
    manifest = first.package.manifest
    assert manifest.alpha_combination_policy.method == "ic_weighted"
    assert manifest.source_evidence["multi_alpha"]["combine_backtest_run_id"] == RUN_ID
    assert manifest.source_evidence["multi_alpha"]["paper_admission"]["blocking"] == [MULTI_ALPHA_PAPER_ADMISSION_BLOCKER]
    assert manifest.backtest_context["daily_strategy"]["topk"] == 50
    assert manifest.backtest_context["daily_strategy"]["secondary_topk"] == [25]
    assert sorted(component.child_package_id for component in first.components) == sorted(
        [child_a1.package_id, child_fund.package_id]
    )
    eligibility = service.package_repository.get(first.package.package_id)
    assert eligibility.package_status == PackageStatus.ASSET_VALIDATED


@pytest.mark.parametrize(
    ("mutator", "expected_reason_code"),
    [
        (lambda repos, children: repos[1].records.pop(children[0].package_id), "multi_alpha_child_package_missing"),
        (
            lambda repos, children: repos[1].records.__setitem__(
                children[0].package_id,
                repos[1].records[children[0].package_id].model_copy(update={"manifest_sha256": ""}),
            ),
            "multi_alpha_child_package_not_frozen",
        ),
        (lambda repos, children: repos[0].scheme_results.clear(), "multi_alpha_scheme_not_succeeded"),
        (
            lambda repos, children: repos[0].runs[RUN_ID].__setitem__(
                "roster_json",
                [{"leg_id": A1_LEG, "seed_run_ids": [A1_SEED]}, {"leg_id": "unexpected_leg", "seed_run_ids": ["seed"]}],
            ),
            "multi_alpha_roster_mismatch",
        ),
        (
            lambda repos, children: repos[1].records.__setitem__(
                children[0].package_id,
                repos[1].records[children[0].package_id].model_copy(
                    update={
                        "run_id": "different_seed",
                        "manifest": repos[1]
                        .records[children[0].package_id]
                        .manifest.model_copy(update={"source_evidence": {}, "backtest_context": {}}),
                    }
                ),
            ),
            "multi_alpha_roster_mismatch",
        ),
        (
            lambda repos, children: repos[0].scheme_results[0].__setitem__(
                "weights_json",
                {A1_LEG: 0.61, FUND_LEG: 0.39},
            ),
            "multi_alpha_prediction_ref_missing",
        ),
        (
            lambda repos, children: repos[0].scheme_results[0].__setitem__("sharpe", 0.1),
            "multi_alpha_metrics_below_gate",
        ),
    ],
)
def test_promote_fails_loud_with_reason_codes(mutator, expected_reason_code) -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    mutator((combine_repo, package_repo), (child_a1, child_fund))

    with pytest.raises((StrategyPackageValidationError, DataUnavailableError)) as excinfo:
        _promote(
            _service(combine_repo, package_repo),
            child_a1,
            child_fund,
            promotion_gate={"min_sharpe": 2.0},
        )

    assert _reason_code(excinfo.value) == expected_reason_code


def test_promote_rejects_live_rolling_weight_policy_in_p0() -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()

    with pytest.raises(StrategyPackageValidationError) as excinfo:
        _promote(
            _service(combine_repo, package_repo),
            child_a1,
            child_fund,
            weight_policy={"mode": "live_rolling_ic_weighted", "metric": "rank_ic"},
        )

    assert _reason_code(excinfo.value) == "multi_alpha_manifest_incomplete"
    assert excinfo.value.context["weight_policy_mode"] == "live_rolling_ic_weighted"


def test_asset_eligibility_blocks_multi_alpha_until_dry_run() -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    service = _service(combine_repo, package_repo)
    result = _promote(service, child_a1, child_fund)

    eligibility = service.component_service.repository.get(result.package.package_id)
    summary = router_module.StrategyPackageAssetEligibilityService().summarize(eligibility)

    assert summary.eligible is False
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER in summary.blockers


def test_router_endpoint_promotes_and_maps_loud_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()

    def _factory(*args, **kwargs):  # noqa: ANN001
        return MultiAlphaPackagePromotionService(combine_repository=combine_repo, package_repository=package_repo)

    monkeypatch.setattr(router_module, "MultiAlphaPackagePromotionService", _factory)
    app = FastAPI()
    app.include_router(router_module.router)
    client = TestClient(app)

    response = client.post("/strategy-packages/from-multi-alpha-combine-run", json=_request(child_a1, child_fund))

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ok"] is True
    assert payload["alpha_mode"] == AlphaMode.MULTI_ALPHA.value
    assert payload["paper_admission"]["blocking"] == [MULTI_ALPHA_PAPER_ADMISSION_BLOCKER]

    bad_request = deepcopy(_request(child_a1, child_fund))
    bad_request["combine_backtest_run_id"] = "missing_run"
    failure = client.post("/strategy-packages/from-multi-alpha-combine-run", json=bad_request)

    assert failure.status_code == 404, failure.text
    detail = failure.json()["detail"]
    assert detail["context"]["reason_code"] == "multi_alpha_combine_run_missing"


@pytest.mark.parametrize(
    ("mutator", "expected_reason_code", "expected_status"),
    [
        (lambda repos, children: repos[1].records.pop(children[0].package_id), "multi_alpha_child_package_missing", 404),
        (lambda repos, children: repos[0].scheme_results.clear(), "multi_alpha_scheme_not_succeeded", 400),
        (
            lambda repos, children: repos[0].runs[RUN_ID].__setitem__(
                "roster_json",
                [{"leg_id": A1_LEG, "seed_run_ids": [A1_SEED]}, {"leg_id": "unexpected_leg", "seed_run_ids": ["seed"]}],
            ),
            "multi_alpha_roster_mismatch",
            400,
        ),
        (
            lambda repos, children: repos[1].records.__setitem__(
                children[0].package_id,
                repos[1].records[children[0].package_id].model_copy(
                    update={
                        "run_id": "different_seed",
                        "manifest": repos[1]
                        .records[children[0].package_id]
                        .manifest.model_copy(update={"source_evidence": {}, "backtest_context": {}}),
                    }
                ),
            ),
            "multi_alpha_roster_mismatch",
            400,
        ),
    ],
)
def test_router_endpoint_negative_paths_are_loud(
    monkeypatch: pytest.MonkeyPatch,
    mutator,
    expected_reason_code: str,
    expected_status: int,
) -> None:
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    mutator((combine_repo, package_repo), (child_a1, child_fund))

    def _factory(*args, **kwargs):  # noqa: ANN001
        return MultiAlphaPackagePromotionService(combine_repository=combine_repo, package_repository=package_repo)

    monkeypatch.setattr(router_module, "MultiAlphaPackagePromotionService", _factory)
    app = FastAPI()
    app.include_router(router_module.router)

    response = TestClient(app).post("/strategy-packages/from-multi-alpha-combine-run", json=_request(child_a1, child_fund))

    assert response.status_code == expected_status, response.text
    detail = response.json()["detail"]
    assert detail["context"]["reason_code"] == expected_reason_code
