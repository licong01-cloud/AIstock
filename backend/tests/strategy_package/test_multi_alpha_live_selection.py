from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    DIAGNOSTIC_BACKTEST_SCOPE,
    DIAGNOSTIC_BACKTEST_SOURCE_TYPE,
    LiveInferenceResult,
)
from backend.services.strategy_package.multi_alpha_live import (
    LIVE_MULTI_ALPHA_SELECTION_SOURCE_TYPE,
    MULTI_ALPHA_LIVE_PROVIDER_VERSION,
    REASON_COMPONENT_COVERAGE_LOW,
    REASON_DEADLINE_EXCEEDED,
    REASON_LABEL_WINDOW_INSUFFICIENT,
    REASON_PARENT_ALPHA158_SCHEMA_MISSING,
    REASON_PARENT_LEG_FACTOR_REFS_MISSING,
    REASON_PARENT_LEG_MODEL_ASSET_MISSING,
    REASON_PREDICTION_NOT_AUTHORITATIVE,
    REASON_RUNTIME_NOT_ENABLED,
    REASON_SEED_PREDICTION_MISSING,
    REASON_TOPK_RUNTIME_MISMATCH,
    REASON_WEIGHT_ALL_NON_POSITIVE,
    REASON_WEIGHT_UNAVAILABLE,
    MultiAlphaLivePredictionProvider,
    MultiAlphaWeightService,
)
from backend.services.strategy_package.models import RuntimeAssetManifest
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    StrategyPackageSelectionArtifactService,
)
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError, UnsupportedFeatureError
from backend.tests.strategy_package.test_multi_alpha_promotion import (
    A1_LEG,
    A1_SEED,
    FUND_LEG,
    FUND_SEED,
    _promote,
    _seed_repos,
    _service,
)
from backend.tests.strategy_package.test_manifest_v1 import admit_manifest_for_test


TRADE_DATE = date(2024, 7, 2)
TRADING_DAYS = [date(2024, 5, 1) + timedelta(days=offset) for offset in range(63)]


def _sha256(payload) -> str:  # noqa: ANN001
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _calendar_context(
    *,
    trade_date: date,
    window_start_date: str,
    required_window: int,
    calendar_version: str = "market.trading_calendar.v1",
) -> dict:
    calendar_source = "market.trading_calendar"
    calendar_identity_hash = _sha256(
        {
            "dataset_id": calendar_source,
            "effective_trade_date": trade_date.isoformat(),
            "calendar_version": calendar_version,
            "calendar_source": calendar_source,
        }
    )
    window_context = {
        "window_start_date": window_start_date,
        "required_window": required_window,
        "window_resolution": "model_required_window",
    }
    window_lineage_hash = _sha256({"calendar_identity_hash": calendar_identity_hash, **window_context})
    return {
        "calendar_version": calendar_version,
        "calendar_identity_hash": calendar_identity_hash,
        "calendar_hash": window_lineage_hash,
        "calendar_source": calendar_source,
        **window_context,
        "window_lineage_hash": window_lineage_hash,
    }


class FakeResolver:
    def __init__(self) -> None:
        self.load_calls: list[dict] = []
        self.legacy_load_calls: list[dict] = []
        self.prepare_calls: list[dict] = []

    def load_source_for_strategy_package(self, **kwargs):  # noqa: ANN001, ANN201
        self.legacy_load_calls.append(kwargs)
        raise AssertionError("multi-alpha parent runtime must not call legacy child/package source loader")

    def load_source_for_strategy_package_leg(self, **kwargs):  # noqa: ANN001, ANN201
        self.load_calls.append(kwargs)
        return SimpleNamespace(
            experiment_id=f"{kwargs['package_id']}:{kwargs['leg_id']}",
            model_params_origin="package_asset",
            source_workspace_type="strategy_package_asset_store",
            leg_id=kwargs["leg_id"],
        )

    def prepare_workspace(self, **kwargs):  # noqa: ANN001, ANN201
        self.prepare_calls.append(kwargs)
        artifact_config = kwargs["runtime_config"]["selection_artifact_config"]
        return SimpleNamespace(
            workspace_path=f"workspace/{kwargs['package_id']}/{artifact_config['multi_alpha_leg_id']}",
            seed_run_id=artifact_config["multi_alpha_seed_run_id"],
            leg_id=artifact_config["multi_alpha_leg_id"],
        )


class FakeProvider:
    backend_name = "fake_live"

    def __init__(
        self,
        scores_by_leg: dict[str, list[dict]],
        *,
        universe_count_by_leg: dict[str, int] | None = None,
        universe_input_hash_by_leg: dict[str, str] | None = None,
        input_context_overrides_by_leg: dict[str, dict] | None = None,
    ) -> None:
        self.scores_by_leg = scores_by_leg
        self.universe_count_by_leg = universe_count_by_leg or {}
        self.universe_input_hash_by_leg = universe_input_hash_by_leg or {}
        self.input_context_overrides_by_leg = input_context_overrides_by_leg or {}
        self.calls: list[dict] = []

    def run(self, **kwargs):  # noqa: ANN001, ANN201
        self.calls.append(kwargs)
        leg_id = kwargs["workspace"].leg_id
        trade_date = kwargs["cutoff_date"] or kwargs["trade_date"]
        observed_at = datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc)
        scores = deepcopy(self.scores_by_leg.get(leg_id, []))
        calendar_context = _calendar_context(
            trade_date=trade_date,
            window_start_date="2024-03-01",
            required_window=60,
        )
        input_context = {
            "requested_trade_date": trade_date.isoformat(),
            "effective_trade_date": trade_date.isoformat(),
            "score_trade_date": trade_date.isoformat(),
            "pit_mode": "stock_universe_pit_v1",
            **calendar_context,
            "universe_input_hash": self.universe_input_hash_by_leg.get(leg_id, "5" * 64),
        }
        input_context.update(self.input_context_overrides_by_leg.get(leg_id, {}))
        return LiveInferenceResult(
            scores=scores,
            metadata={"leg_id": leg_id, "seed_run_id": kwargs["workspace"].seed_run_id},
            universe_count=self.universe_count_by_leg.get(leg_id, max(80, len(scores))),
            source_read_receipts=[
                {
                    "source_role": "pit_universe",
                    "dataset_id": "market.stock_universe_pit",
                    "row_count": 80,
                    "content_hash": "1" * 64,
                    "first_observed_at": observed_at,
                },
                {
                    "source_role": "market_history",
                    "dataset_id": "market.kline_daily_raw",
                    "row_count": 1600,
                    "content_hash": "2" * 64,
                    "first_observed_at": observed_at,
                },
                {
                    "source_role": "fundamental_moneyflow",
                    "dataset_id": "timescaledb.fundamental_moneyflow",
                    "row_count": 1600,
                    "content_hash": "3" * 64,
                    "first_observed_at": observed_at,
                },
                {
                    "source_role": "trading_calendar",
                    "dataset_id": "market.trading_calendar",
                    "row_count": 2,
                    "content_hash": input_context["calendar_hash"],
                    "first_observed_at": observed_at,
                },
            ],
            input_context=input_context,
        )


class ChildFailingRepository:
    def __init__(self, delegate) -> None:  # noqa: ANN001
        self.delegate = delegate
        self.get_calls: list[str] = []

    def get(self, package_id: str):  # noqa: ANN201
        self.get_calls.append(package_id)
        if str(package_id).startswith("pkg_mac"):
            raise AssertionError("multi-alpha parent runtime attempted to read legacy child package")
        return self.delegate.get(package_id)

    def __getattr__(self, name: str):  # noqa: ANN204
        return getattr(self.delegate, name)


def _score_rows(*, reverse: bool = False, count: int = 60) -> list[dict]:
    rows = []
    for idx in range(count):
        symbol = f"{idx + 1:06d}.SZ"
        score = float(count - idx if not reverse else idx + 1)
        rows.append({"symbol": symbol, "score": score, "rank": idx + 1})
    return rows


def _runtime_config(*, top_k: int = 25, extra_artifact: dict | None = None) -> dict:
    artifact = {
        "multi_alpha_live_inference_enabled": True,
        "component_coverage_threshold": top_k,
        "trading_days": [item.isoformat() for item in TRADING_DAYS],
    }
    artifact.update(extra_artifact or {})
    return {
        "runtime_profile": {"selection": {"top_k": top_k}},
        "selection_artifact_config": artifact,
    }


def _live_weight_history(*, a1_value: float = 0.12, fund_value: float = 0.04, samples: int = 3) -> list[dict]:
    label_dates = [date(2024, 5, 3), date(2024, 5, 4), date(2024, 5, 5), date(2024, 5, 6)]
    rows = []
    for label_date in label_dates[:samples]:
        rows.append({"leg_id": A1_LEG, "label_date": label_date.isoformat(), "rank_ic": a1_value})
        rows.append({"leg_id": FUND_LEG, "label_date": label_date.isoformat(), "rank_ic": fund_value})
    return rows


def _make_parent(*, live_weight_policy: bool = True):
    _combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    parent = _promote(_service(_combine_repo, package_repo), child_a1, child_fund).package
    if not live_weight_policy:
        return package_repo, parent

    manifest = parent.manifest
    source_evidence = deepcopy(manifest.source_evidence)
    live_policy = {
        "mode": "live_rolling_ic_weighted",
        "metric": "rank_ic",
        "lookback_trading_days": 60,
        "min_periods": 2,
        "label_horizon": 20,
        "settlement_lag_trading_days": 1,
        "clip_negative_to_zero": True,
    }
    source_evidence["multi_alpha"]["weight_policy"] = live_policy
    backtest_context = deepcopy(manifest.backtest_context)
    backtest_context["weight_policy"] = live_policy
    updated = admit_manifest_for_test(
        manifest.model_copy(
            update={
                "package_id": f"{parent.package_id}_live",
                "package_name": f"{parent.package_name}_live",
                "source": manifest.source.model_copy(update={"source_id": f"{manifest.source.source_id}:live"}),
                "source_evidence": source_evidence,
                "backtest_context": backtest_context,
                "manifest_sha256": None,
            }
        )
    )
    live_parent = package_repo.save_manifest(updated)
    return package_repo, live_parent


def _artifact_service(
    package_repo,
    provider_scores: dict[str, list[dict]] | None = None,
    *,
    universe_count_by_leg: dict[str, int] | None = None,
    universe_input_hash_by_leg: dict[str, str] | None = None,
    input_context_overrides_by_leg: dict[str, dict] | None = None,
):  # noqa: ANN001
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    resolver = FakeResolver()
    provider = FakeProvider(
        provider_scores
        or {
            A1_LEG: _score_rows(reverse=False),
            FUND_LEG: _score_rows(reverse=True),
        },
        universe_count_by_leg=universe_count_by_leg,
        universe_input_hash_by_leg=universe_input_hash_by_leg,
        input_context_overrides_by_leg=input_context_overrides_by_leg,
    )
    service = StrategyPackageSelectionArtifactService(
        package_repository=package_repo,
        artifact_repository=artifact_repo,
        runtime_asset_resolver=resolver,
        live_inference_provider=provider,
    )
    return service, artifact_repo, resolver, provider


def _reason(exc: BaseException) -> str | None:
    return getattr(exc, "context", {}).get("reason_code")


def _generate(service, parent, runtime_config: dict):  # noqa: ANN001
    return service.generate_from_live_inference(
        package_id=parent.package_id,
        trade_date=TRADE_DATE,
        runtime_config=runtime_config,
        include_reference_price=False,
    )


def test_multi_alpha_live_selection_artifact_is_authoritative_and_deterministic() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    runtime_config = _runtime_config(extra_artifact={"multi_alpha_weight_history": _live_weight_history()})
    service, artifact_repo, resolver, provider = _artifact_service(package_repo)

    first = _generate(service, parent, runtime_config)
    second = _generate(service, parent, runtime_config)

    assert first.metadata["source_type"] == LIVE_MULTI_ALPHA_SELECTION_SOURCE_TYPE
    assert first.metadata["authority_scope"] == AUTHORITATIVE_SELECTION_SCOPE
    assert first.metadata["combine_backtest_run_id"]
    assert first.metadata["component_score_artifact_ids"].keys() == {A1_LEG, FUND_LEG}
    assert first.metadata["weight_artifact_id"].startswith("maw_")
    assert first.metadata["component_manifest_sha256"][A1_LEG]
    assert first.metadata["runtime_source"] == "parent_package_asset"
    assert first.metadata["runtime_package_id"] == parent.package_id
    assert first.metadata["model_params_origin"] == "package_asset"
    assert first.metadata["component_artifacts"][A1_LEG]["runtime_source"] == "parent_package_asset"
    assert first.metadata["component_artifacts"][FUND_LEG]["model_params_origin"] == "package_asset"
    assert first.artifact_contract_version == "selection_score_artifact_v2"
    assert first.universe_count == 80
    assert first.universe_count > first.score_count
    assert first.metadata["multi_alpha_parent_parity_hash"]
    assert {item["leg_id"] for item in first.metadata["source_read_receipts"] if item.get("leg_id")} == {A1_LEG, FUND_LEG}
    assert first.metadata["component_artifacts"][A1_LEG]["child_package_id"] is None
    assert first.metadata["seed_run_ids"] == {A1_LEG: [A1_SEED], FUND_LEG: [FUND_SEED]}
    assert first.metadata["normalization_method"] == "zscore"
    assert first.metadata["provider_version"] == "multi_alpha_live_selection_provider_v3"
    assert MULTI_ALPHA_LIVE_PROVIDER_VERSION == "multi_alpha_live_selection_provider_v3"
    assert first.metadata["final_topk"] == 25
    assert first.metadata["component_candidate_universe_size"] == 60
    assert first.metadata["coverage_threshold"] == 25
    assert first.artifact_sha256 == second.artifact_sha256
    assert first.scores_json == second.scores_json
    assert len(provider.calls) == 4
    assert resolver.legacy_load_calls == []
    assert {call["leg_id"] for call in resolver.load_calls} == {A1_LEG, FUND_LEG}
    assert all(call["package_id"] == parent.package_id for call in resolver.load_calls)
    assert all(call["model_asset"].asset_ref for call in resolver.load_calls)
    assert all(call["factor_set"] for call in resolver.load_calls)
    assert all(str(call["cache_namespace"]).startswith("leg_") for call in resolver.prepare_calls)

    snapshot = StrategyPackageRuntime(artifact_repository=artifact_repo).build_signal_snapshot(
        manifest=parent.current_manifest(),
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        runtime_config=runtime_config,
    )
    assert snapshot.candidates[0].rank == 1
    assert snapshot.candidates[0].component_scores[A1_LEG]["weight"] > snapshot.candidates[0].component_scores[FUND_LEG]["weight"]


def test_component_leg_rank_uses_full_leg_universe_before_inner_alignment() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    runtime_config = _runtime_config(
        extra_artifact={
            "component_coverage_threshold": 2,
            "multi_alpha_weight_history": _live_weight_history(),
        }
    )
    service, _artifact_repo, _resolver, _provider = _artifact_service(
        package_repo,
        {
            A1_LEG: _score_rows(count=26),
            FUND_LEG: _score_rows(count=26)[1:],
        },
    )

    artifact = _generate(service, parent, runtime_config)
    row_by_symbol = {row["symbol"]: row for row in artifact.scores_json}

    assert row_by_symbol["000002.SZ"]["component_scores"][A1_LEG]["leg_rank"] == 2
    assert row_by_symbol["000002.SZ"]["component_scores"][FUND_LEG]["leg_rank"] == 1


def test_multi_alpha_runtime_rejects_non_authoritative_artifact() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config()
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=parent.package_id,
            manifest_sha256=parent.manifest_sha256,
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            runtime_config_hash=first_multi_alpha_hash(parent.current_manifest(), runtime_config),
            scores_json=[{"symbol": "000001.SZ", "score": 1.0, "rank": 1, "target_weight": 0.04}],
            score_count=1,
            universe_count=1,
            top_score_symbol="000001.SZ",
            metadata={"source_type": DIAGNOSTIC_BACKTEST_SOURCE_TYPE, "authority_scope": DIAGNOSTIC_BACKTEST_SCOPE},
        )
    )

    with pytest.raises(DataUnavailableError) as excinfo:
        StrategyPackageRuntime(artifact_repository=artifact_repo).build_signal_snapshot(
            manifest=parent.current_manifest(),
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            runtime_config=runtime_config,
        )

    assert _reason(excinfo.value) == REASON_PREDICTION_NOT_AUTHORITATIVE


def first_multi_alpha_hash(manifest, runtime_config):  # noqa: ANN001, ANN201
    from backend.services.strategy_package.multi_alpha_live import multi_alpha_selection_artifact_runtime_hash

    return multi_alpha_selection_artifact_runtime_hash(manifest, runtime_config)


@pytest.mark.parametrize(
    ("mutator", "expected_reason"),
    [
        (
            lambda _package_repo, parent, _runtime_config: _remove_first_leg_model_asset(parent),
            REASON_PARENT_LEG_MODEL_ASSET_MISSING,
        ),
        (
            lambda _package_repo, parent, _runtime_config: _clear_first_leg_factor_refs(parent),
            REASON_PARENT_LEG_FACTOR_REFS_MISSING,
        ),
        (
            lambda _package_repo, parent, _runtime_config: _remove_parent_alpha158_schema(parent),
            REASON_PARENT_ALPHA158_SCHEMA_MISSING,
        ),
        (
            lambda _package_repo, _parent, runtime_config: runtime_config["selection_artifact_config"].__setitem__(
                "component_coverage_threshold",
                100,
            ),
            REASON_COMPONENT_COVERAGE_LOW,
        ),
    ],
)
def test_multi_alpha_live_selection_negative_manifest_and_coverage(mutator, expected_reason) -> None:  # noqa: ANN001
    package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config()
    service, _artifact_repo, _resolver, _provider = _artifact_service(package_repo)
    mutator(package_repo, parent, runtime_config)

    with pytest.raises((DataUnavailableError, RuntimeConfigInvalidError)) as excinfo:
        _generate(service, parent, runtime_config)

    assert _reason(excinfo.value) == expected_reason


def test_multi_alpha_live_selection_records_natural_raw_empty_when_a_leg_has_no_scores() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config()
    service, artifact_repo, _resolver, _provider = _artifact_service(package_repo, provider_scores={A1_LEG: _score_rows()})

    artifact = _generate(service, parent, runtime_config)

    assert artifact.status.value == "SUCCEEDED"
    assert artifact.scores_json == []
    assert artifact.score_count == 0
    assert artifact.universe_count == 80
    assert artifact.metadata["candidate_outcome"] == "VALID_NO_CANDIDATE"
    assert artifact.metadata["empty_stage"] == "alpha_raw"
    snapshot = StrategyPackageRuntime(artifact_repository=artifact_repo).build_signal_snapshot(
        manifest=parent.current_manifest(),
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        runtime_config=runtime_config,
    )
    assert snapshot.valid_no_candidate is True
    assert snapshot.candidates == []


def test_multi_alpha_live_selection_fails_loud_when_empty_leg_has_no_input_universe() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config()
    service, _artifact_repo, _resolver, _provider = _artifact_service(
        package_repo,
        provider_scores={A1_LEG: _score_rows()},
        universe_count_by_leg={FUND_LEG: 0},
    )

    with pytest.raises(DataUnavailableError) as excinfo:
        _generate(service, parent, runtime_config)

    assert _reason(excinfo.value) == REASON_SEED_PREDICTION_MISSING


def test_multi_alpha_live_selection_fails_loud_when_leg_input_universes_differ() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config()
    service, _artifact_repo, _resolver, _provider = _artifact_service(
        package_repo,
        universe_count_by_leg={A1_LEG: 80, FUND_LEG: 79},
    )

    with pytest.raises(DataUnavailableError) as excinfo:
        _generate(service, parent, runtime_config)

    assert _reason(excinfo.value) == "ADVISORY_PHASE0A2C_LINEAGE_MISMATCH"


def test_multi_alpha_live_selection_fails_loud_when_leg_input_universe_hashes_differ() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config()
    service, _artifact_repo, _resolver, _provider = _artifact_service(
        package_repo,
        universe_input_hash_by_leg={A1_LEG: "5" * 64, FUND_LEG: "6" * 64},
    )

    with pytest.raises(DataUnavailableError) as excinfo:
        _generate(service, parent, runtime_config)

    assert _reason(excinfo.value) == "ADVISORY_PHASE0A2C_LINEAGE_MISMATCH"


def test_multi_alpha_live_selection_allows_distinct_per_leg_history_windows() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config()
    service, _artifact_repo, _resolver, _provider = _artifact_service(
        package_repo,
        input_context_overrides_by_leg={
            A1_LEG: _calendar_context(trade_date=TRADE_DATE, window_start_date="2024-03-01", required_window=60),
            FUND_LEG: _calendar_context(trade_date=TRADE_DATE, window_start_date="2023-07-01", required_window=250),
        },
    )

    artifact = _generate(service, parent, runtime_config)

    assert artifact.status.value == "SUCCEEDED"
    windows = artifact.metadata["per_leg_window_lineage"]
    assert windows[A1_LEG]["required_window"] == 60
    assert windows[FUND_LEG]["required_window"] == 250
    assert windows[A1_LEG]["window_lineage_hash"] != windows[FUND_LEG]["window_lineage_hash"]


def test_multi_alpha_live_selection_fails_when_shared_calendar_identity_differs() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config()
    mismatched_context = _calendar_context(
        trade_date=TRADE_DATE,
        window_start_date="2023-07-01",
        required_window=250,
        calendar_version="market.trading_calendar.v2",
    )
    service, _artifact_repo, _resolver, _provider = _artifact_service(
        package_repo,
        input_context_overrides_by_leg={FUND_LEG: mismatched_context},
    )

    with pytest.raises(DataUnavailableError) as excinfo:
        _generate(service, parent, runtime_config)

    assert _reason(excinfo.value) == "ADVISORY_PHASE0A2C_LINEAGE_MISMATCH"


def test_multi_alpha_live_selection_fails_when_leg_requested_trade_date_differs() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config()
    service, _artifact_repo, _resolver, _provider = _artifact_service(
        package_repo,
        input_context_overrides_by_leg={
            FUND_LEG: {"requested_trade_date": (TRADE_DATE - timedelta(days=1)).isoformat()},
        },
    )

    with pytest.raises(DataUnavailableError) as excinfo:
        _generate(service, parent, runtime_config)

    assert _reason(excinfo.value) == "ADVISORY_PHASE0A2C_LINEAGE_MISMATCH"


@pytest.mark.parametrize(
    ("history", "expected_reason"),
    [
        ([], REASON_WEIGHT_UNAVAILABLE),
        (_live_weight_history(samples=1), REASON_LABEL_WINDOW_INSUFFICIENT),
        (_live_weight_history(a1_value=-0.1, fund_value=-0.2), REASON_WEIGHT_ALL_NON_POSITIVE),
    ],
)
def test_multi_alpha_live_rolling_weight_failures(history, expected_reason) -> None:  # noqa: ANN001
    package_repo, parent = _make_parent(live_weight_policy=True)
    runtime_config = _runtime_config(extra_artifact={"multi_alpha_weight_history": history})
    service, _artifact_repo, _resolver, _provider = _artifact_service(package_repo)

    with pytest.raises(DataUnavailableError) as excinfo:
        _generate(service, parent, runtime_config)

    assert _reason(excinfo.value) == expected_reason


def test_multi_alpha_topk_mismatch_fails_loud() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    service, _artifact_repo, _resolver, _provider = _artifact_service(package_repo)

    with pytest.raises(RuntimeConfigInvalidError) as excinfo:
        _generate(service, parent, _runtime_config(top_k=10))

    assert _reason(excinfo.value) == REASON_TOPK_RUNTIME_MISMATCH


def test_multi_alpha_deadline_gate_fails_before_inference() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    resolver = FakeResolver()
    provider = FakeProvider({A1_LEG: _score_rows(), FUND_LEG: _score_rows(reverse=True)})
    gate = MultiAlphaLivePredictionProvider(
        package_repository=package_repo,
        artifact_repository=artifact_repo,
        runtime_asset_resolver=resolver,
        live_inference_provider=provider,
        weight_service=MultiAlphaWeightService(),
        clock=lambda: datetime(2024, 7, 2, 9, 31, tzinfo=timezone.utc),
    )

    with pytest.raises(RuntimeConfigInvalidError) as excinfo:
        gate.generate_artifacts(
            package_id=parent.package_id,
            trade_dates=[TRADE_DATE],
            data_source="DB_HISTORICAL",
            runtime_config=_runtime_config(extra_artifact={"multi_alpha_deadline_at": "2024-07-02T09:30:00+00:00"}),
            include_reference_price=False,
            inference_backend="fake_live",
        )

    assert _reason(excinfo.value) == REASON_DEADLINE_EXCEEDED
    assert provider.calls == []


def test_multi_alpha_runtime_disabled_fails_loud() -> None:
    _package_repo, parent = _make_parent(live_weight_policy=False)
    runtime_config = _runtime_config(extra_artifact={"multi_alpha_live_inference_enabled": False})

    with pytest.raises(UnsupportedFeatureError) as excinfo:
        StrategyPackageRuntime(artifact_repository=InMemorySelectionScoreArtifactRepository()).build_signal_snapshot(
            manifest=parent.current_manifest(),
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            runtime_config=runtime_config,
        )

    assert _reason(excinfo.value) == REASON_RUNTIME_NOT_ENABLED


def test_multi_alpha_legacy_child_ref_is_ignored_and_child_repository_is_not_read() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    manifest = parent.manifest
    first = manifest.alpha_components[0]
    updated_component = first.model_copy(
        update={
            "lineage": first.lineage.model_copy(update={"model_artifact_ref": "child_package:pkg_mac_legacy"})
        }
    )
    parent.manifest = manifest.model_copy(update={"alpha_components": [updated_component, *manifest.alpha_components[1:]]})
    sentinel_repo = ChildFailingRepository(package_repo)
    service, _artifact_repo, resolver, provider = _artifact_service(sentinel_repo)

    artifact = _generate(service, parent, _runtime_config())

    assert artifact.status.value == "SUCCEEDED"
    assert sentinel_repo.get_calls == [parent.package_id, parent.package_id]
    assert resolver.legacy_load_calls == []
    assert artifact.metadata["legacy_child_ref_ignored"][A1_LEG] is True
    assert artifact.metadata["component_artifacts"][A1_LEG]["legacy_child_ref_ignored"] is True
    assert artifact.metadata["runtime_source"] == "parent_package_asset"
    assert len(provider.calls) == 2


def test_multi_alpha_runtime_uses_per_leg_alpha158_disabled_mapping() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    manifest = parent.manifest
    source_evidence = deepcopy(manifest.source_evidence)
    for leg in source_evidence["multi_alpha"]["legs"]:
        if leg["leg_id"] == FUND_LEG:
            leg["runtime_assets"] = RuntimeAssetManifest().model_dump(mode="json")
    parent.manifest = manifest.model_copy(update={"source_evidence": source_evidence})
    service, _artifact_repo, resolver, _provider = _artifact_service(package_repo)

    artifact = _generate(service, parent, _runtime_config())

    by_leg = {call["leg_id"]: call["runtime_assets"] for call in resolver.load_calls}
    assert by_leg[A1_LEG].alpha158.enabled is True
    assert by_leg[FUND_LEG].alpha158.enabled is False
    assert artifact.metadata["component_artifacts"][FUND_LEG]["alpha158_schema_sha256"] is None


def _remove_first_leg_model_asset(parent) -> None:  # noqa: ANN001
    manifest = parent.manifest
    first_model_id = manifest.alpha_components[0].model_id
    models = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    parent.manifest = manifest.model_copy(update={"model_asset": [model for model in models if model.model_id != first_model_id]})


def _clear_first_leg_factor_refs(parent) -> None:  # noqa: ANN001
    manifest = parent.manifest
    first = manifest.alpha_components[0]
    updated = first.model_copy(update={"lineage": first.lineage.model_copy(update={"factor_artifact_refs": []})})
    parent.manifest = manifest.model_copy(update={"alpha_components": [updated, *manifest.alpha_components[1:]]})


def _remove_parent_alpha158_schema(parent) -> None:  # noqa: ANN001
    manifest = parent.manifest
    parent.manifest = manifest.model_copy(update={"runtime_assets": None})
