"""Shared multi-alpha live-selection contract fixtures.

The behavioral contracts live in the focused signal-preparation and repository
test modules.  Keeping these builders in one place avoids duplicating the same
promotion snapshot in every consumer test.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

from backend.services.strategy_package.live_inference import LiveInferenceResult
from backend.tests.strategy_package.test_manifest_v1 import admit_manifest_for_test
from backend.tests.strategy_package.test_multi_alpha_promotion import (
    A1_LEG,
    FUND_LEG,
    _promote,
    _seed_repos,
    _service,
)


TRADE_DATE = date(2024, 7, 2)
TRADING_DAYS = [date(2024, 5, 1) + timedelta(days=offset) for offset in range(63)]


def _sha256(payload: object) -> str:
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
    window_lineage_hash = _sha256(
        {"calendar_identity_hash": calendar_identity_hash, **window_context}
    )
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
        raise AssertionError(
            "multi-alpha parent runtime must not call legacy child/package source loader"
        )

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
        artifact = kwargs["runtime_config"]["selection_artifact_config"]
        return SimpleNamespace(
            workspace_path=(
                f"workspace/{kwargs['package_id']}/{artifact['multi_alpha_leg_id']}"
            ),
            seed_run_id=artifact["multi_alpha_seed_run_id"],
            leg_id=artifact["multi_alpha_leg_id"],
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
        input_context = {
            "requested_trade_date": trade_date.isoformat(),
            "effective_trade_date": trade_date.isoformat(),
            "score_trade_date": trade_date.isoformat(),
            "pit_mode": "stock_universe_pit_v1",
            **_calendar_context(
                trade_date=trade_date,
                window_start_date="2024-03-01",
                required_window=60,
            ),
            "universe_input_hash": self.universe_input_hash_by_leg.get(
                leg_id, "5" * 64
            ),
        }
        input_context.update(self.input_context_overrides_by_leg.get(leg_id, {}))
        receipts = [
            ("pit_universe", "market.stock_universe_pit", 80, "1" * 64),
            ("market_history", "market.kline_daily_raw", 1600, "2" * 64),
            (
                "fundamental_moneyflow",
                "timescaledb.fundamental_moneyflow",
                1600,
                "3" * 64,
            ),
            (
                "trading_calendar",
                "market.trading_calendar",
                2,
                input_context["calendar_hash"],
            ),
        ]
        return LiveInferenceResult(
            scores=scores,
            metadata={
                "leg_id": leg_id,
                "seed_run_id": kwargs["workspace"].seed_run_id,
            },
            universe_count=self.universe_count_by_leg.get(
                leg_id, max(80, len(scores))
            ),
            source_read_receipts=[
                {
                    "source_role": role,
                    "dataset_id": dataset,
                    "row_count": count,
                    "content_hash": digest,
                    "first_observed_at": observed_at,
                }
                for role, dataset, count, digest in receipts
            ],
            input_context=input_context,
        )


def _score_rows(*, reverse: bool = False, count: int = 60) -> list[dict]:
    return [
        {
            "symbol": f"{idx + 1:06d}.SZ",
            "score": float(idx + 1 if reverse else count - idx),
            "rank": idx + 1,
        }
        for idx in range(count)
    ]


def _runtime_config(*, top_k: int = 25, extra_artifact: dict | None = None) -> dict:
    artifact = {
        "multi_alpha_live_inference_enabled": True,
        "component_coverage_threshold": top_k,
        "trading_days": [item.isoformat() for item in TRADING_DAYS],
        **(extra_artifact or {}),
    }
    return {
        "runtime_profile": {"selection": {"top_k": top_k}},
        "selection_artifact_config": artifact,
    }


def _live_weight_history(
    *,
    a1_value: float = 0.12,
    fund_value: float = 0.04,
    samples: int = 3,
) -> list[dict]:
    label_dates = [date(2024, 5, day) for day in range(3, 7)]
    return [
        {"leg_id": leg_id, "label_date": label_date.isoformat(), "rank_ic": value}
        for label_date in label_dates[:samples]
        for leg_id, value in ((A1_LEG, a1_value), (FUND_LEG, fund_value))
    ]


def _make_parent(*, live_weight_policy: bool = True):  # noqa: ANN202
    combine_repo, package_repo, child_a1, child_fund = _seed_repos()
    parent = _promote(
        _service(combine_repo, package_repo), child_a1, child_fund
    ).package
    if not live_weight_policy:
        return package_repo, parent

    manifest = parent.manifest
    live_policy = {
        "mode": "live_rolling_ic_weighted",
        "metric": "rank_ic",
        "lookback_trading_days": 60,
        "min_periods": 2,
        "label_horizon": 20,
        "settlement_lag_trading_days": 1,
        "clip_negative_to_zero": True,
    }
    source_evidence = deepcopy(manifest.source_evidence)
    source_evidence["multi_alpha"]["weight_policy"] = live_policy
    backtest_context = deepcopy(manifest.backtest_context)
    backtest_context["weight_policy"] = live_policy
    updated = admit_manifest_for_test(
        manifest.model_copy(
            update={
                "package_id": f"{parent.package_id}_live",
                "package_name": f"{parent.package_name}_live",
                "source": manifest.source.model_copy(
                    update={"source_id": f"{manifest.source.source_id}:live"}
                ),
                "source_evidence": source_evidence,
                "backtest_context": backtest_context,
                "manifest_sha256": None,
            }
        )
    )
    return package_repo, package_repo.save_manifest(updated)


def test_live_input_contract_is_pit_bound_and_reproducible() -> None:
    provider = FakeProvider({A1_LEG: _score_rows(count=3)})
    workspace = SimpleNamespace(leg_id=A1_LEG, seed_run_id="qe_seed")

    first = provider.run(
        workspace=workspace, cutoff_date=TRADE_DATE, trade_date=TRADE_DATE
    )
    second = provider.run(
        workspace=workspace, cutoff_date=TRADE_DATE, trade_date=TRADE_DATE
    )

    assert first.scores == second.scores
    assert first.input_context["pit_mode"] == "stock_universe_pit_v1"
    assert first.input_context["calendar_identity_hash"]
    assert first.input_context["universe_input_hash"]
    assert {item["source_role"] for item in first.source_read_receipts} == {
        "pit_universe",
        "market_history",
        "fundamental_moneyflow",
        "trading_calendar",
    }
