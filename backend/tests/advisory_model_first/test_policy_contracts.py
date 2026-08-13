from __future__ import annotations

from backend.services.advisory_model_first.contracts import PredictionArtifactDescriptor
from backend.services.advisory_model_first.policy_contracts import (
    AdvisoryPolicyCostV1,
    AdvisoryPolicySplitV1,
    build_frozen_policy_dataset_request,
)


def _request(**overrides):
    policy = {
        "target_count": 5,
        "rank_enter_threshold": 5,
        "rank_exit_threshold": 40,
        "rank_exit_confirm_days": 2,
        "daily_replacement_budget": 5,
        "stop_loss_bps": 800,
        "take_profit_bps": 1800,
        "trailing_stop_bps": 700,
        "time_stop_days": 20,
        "take_profit_mode": "trailing",
        "entry_price_basis": "next_open_executable",
        "exit_price_basis": "next_open_executable",
    }
    values = {
        "program_id": "advp_test",
        "binding_version_id": "advb_test",
        "package_id": "pkg_test",
        "manifest_sha256": "1" * 64,
        "package_asset_closure_hash": "2" * 64,
        "style_profile_id": "short_rebound_v1",
        "style_profile_hash": "3" * 64,
        "selection_runtime_semantics_id": "semantics_v1",
        "selection_runtime_semantics_hash": "4" * 64,
        "selection_runtime_semantics": {"raw_top_k": 40},
        "representative_seed_run_ids": {"leg_a": "run_a", "leg_b": "run_b"},
        "representative_model_asset_sha256": {"leg_a": "5" * 64, "leg_b": "6" * 64},
        "prediction_artifacts": {
            run_id: PredictionArtifactDescriptor(
                run_id=run_id,
                run_key=run_id,
                artifact_uri=f"runs/{run_id}/pred.pkl",
                artifact_sha256=digest * 64,
                size_bytes=1,
                row_count=40,
                date_start="2026-01-01",
                date_end="2026-06-30",
            )
            for run_id, digest in (("run_a", "a"), ("run_b", "b"))
        },
        "terminal_weights": {"leg_a": 0.6, "leg_b": 0.4},
        "qlib_daily_root": "/data/qlib",
        "suspend_data_root": "/data/suspend",
        "prediction_store_root": "/data/predictions",
        "repository_root": "/repo",
        "repository_commit": "7" * 40,
        "decision_date_start": "2026-01-01",
        "decision_date_end": "2026-05-31",
        "data_cutoff": "2026-06-30",
        "baseline_policy": {**policy, "target_count": 20, "rank_enter_threshold": 20},
        "shadow_policy": policy,
        "cost_policy": AdvisoryPolicyCostV1(buy_cost_bps=0.95, sell_cost_bps=5.95),
        "split_policy": AdvisoryPolicySplitV1(),
        "output_root": "/output/one",
    }
    values.update(overrides)
    return build_frozen_policy_dataset_request(**values)


def test_policy_request_identity_ignores_creation_time_and_output_root() -> None:
    first = _request(created_at="2026-08-13T00:00:00+00:00")
    second = _request(output_root="/output/two", created_at="2026-08-13T01:00:00+00:00")
    assert first.request_id == second.request_id
    assert first.request_sha256 == second.request_sha256


def test_policy_request_identity_changes_with_cost_or_policy() -> None:
    first = _request()
    second = _request(cost_policy=AdvisoryPolicyCostV1(buy_cost_bps=1.0, sell_cost_bps=5.95))
    assert first.request_sha256 != second.request_sha256
