from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.model_bundle import LoadedAdvisoryModelBundle
from backend.services.advisory_model_first.quality_contracts import (
    ENSEMBLE_SCORE_POLICY,
    QUALITY_SEEDS,
    SELECTION_PRIOR_POLICY,
)
from backend.services.advisory_model_first.target_binding import (
    FUND_LEG_ID,
    LSTM_LEG_ID,
    RUNTIME_SEMANTICS_HASH,
    TERMINAL_WEIGHTS,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.model_challenger import (
    HistoricalModelChallengerArtifactV1,
    HistoricalModelChallenger,
    REASON_MODEL_OUTPUT_INVALID,
    REASON_PARENT_MISMATCH,
)
from backend.services.advisory_historical_range.fullstack_comparison import (
    HistoricalComparisonArtifactStore,
    HistoricalComparisonLifecycleDayV1,
    compare_day_ranks,
    replay_matched_lifecycle,
    summarize_paired_daily_delta,
    summarize_return_records,
)
from backend.services.advisory_list_transition import (
    AdvisoryTransitionCandidateV1,
    AdvisoryTransitionPolicyV1,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeCandidateArtifactPayloadV2,
    HistoricalRangeCandidateFactV1,
    HistoricalRangeSourceRevisionRefV1,
    derive_prefixed_id,
)
from backend.tests.advisory_historical_range.conftest import digest


PACKAGE_ID = "pkg_ma_8ec5e389fa2c5e484a1ac7e9"
MANIFEST_SHA256 = "f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016"
BUNDLE_ID = "1" * 64


def test_historical_model_challenger_scores_raw_parent_without_mutation() -> None:
    parent = _parent()
    before = parent.model_dump(mode="json")
    challenger = HistoricalModelChallenger(
        feature_source=_FeatureSource(),
        bundle_loader=lambda **_: _bundle(),
        feature_builder=_feature_builder,
        scorer=_scorer,
    )

    artifact = challenger.score_day(
        parent=parent,
        parent_candidate_artifact_hash=digest("parent-artifact"),
        target_trade_date=date(2026, 5, 18),
        model_root="/research-model-root",
        bundle_id=BUNDLE_ID,
        expected_selection_runtime_semantics_hash=RUNTIME_SEMANTICS_HASH,
    )

    assert parent.model_dump(mode="json") == before
    assert artifact.candidate_count == 20
    assert artifact.shortlist_count == 5
    assert [item.model_rank for item in artifact.candidates] == list(range(1, 21))
    assert [item.selection_rank for item in artifact.candidates] == list(range(20, 0, -1))
    assert artifact.artifact_hash == canonical_json_sha256(
        artifact.model_dump(mode="json", exclude={"artifact_hash"})
    )


def test_historical_model_challenger_rejects_non_raw_control_parent() -> None:
    parent = _parent()
    first = parent.candidates[0].model_copy(update={"hmm_adjusted_rank": 2})
    altered = parent.model_copy(update={"candidates": (first, *parent.candidates[1:])})
    challenger = HistoricalModelChallenger(bundle_loader=lambda **_: _bundle())

    with pytest.raises(AdvisoryModelFirstError) as raised:
        challenger.score_day(
            parent=altered,
            parent_candidate_artifact_hash=digest("parent-artifact"),
            target_trade_date=date(2026, 5, 18),
            model_root="/research-model-root",
            bundle_id=BUNDLE_ID,
            expected_selection_runtime_semantics_hash=RUNTIME_SEMANTICS_HASH,
        )
    assert raised.value.reason_code == REASON_PARENT_MISMATCH


@pytest.mark.parametrize("malformation", ["missing", "duplicate", "bad_top5"])
def test_historical_model_challenger_rejects_malformed_scorer_output(
    malformation: str,
) -> None:
    def malformed_scorer(
        bundle: LoadedAdvisoryModelBundle,
        features: pd.DataFrame,
    ) -> list[dict[str, object]]:
        scored = _scorer(bundle, features)
        if malformation == "missing":
            scored.pop()
        elif malformation == "duplicate":
            scored[-1] = {**scored[-1], "symbol": scored[0]["symbol"]}
        else:
            scored[5] = {**scored[5], "is_top5": True}
        return scored

    challenger = HistoricalModelChallenger(
        feature_source=_FeatureSource(),
        bundle_loader=lambda **_: _bundle(),
        feature_builder=_feature_builder,
        scorer=malformed_scorer,
    )

    with pytest.raises(AdvisoryModelFirstError) as raised:
        challenger.score_day(
            parent=_parent(),
            parent_candidate_artifact_hash=digest("parent-artifact"),
            target_trade_date=date(2026, 5, 18),
            model_root="/research-model-root",
            bundle_id=BUNDLE_ID,
            expected_selection_runtime_semantics_hash=RUNTIME_SEMANTICS_HASH,
        )

    assert raised.value.reason_code == REASON_MODEL_OUTPUT_INVALID


def test_challenger_artifact_store_is_immutable_and_exact_retryable(tmp_path: Path) -> None:
    parent = _parent()
    challenger = HistoricalModelChallenger(
        feature_source=_FeatureSource(),
        bundle_loader=lambda **_: _bundle(),
        feature_builder=_feature_builder,
        scorer=_scorer,
    )
    artifact = challenger.score_day(
        parent=parent,
        parent_candidate_artifact_hash=digest("parent-artifact"),
        target_trade_date=date(2026, 5, 18),
        model_root="/research-model-root",
        bundle_id=BUNDLE_ID,
        expected_selection_runtime_semantics_hash=RUNTIME_SEMANTICS_HASH,
    )
    store = HistoricalComparisonArtifactStore(root=(tmp_path / "comparison").resolve())

    first = store.publish_challenger(artifact)
    second = store.publish_challenger(artifact)

    assert first == second
    assert store.load_challenger(first) == artifact
    altered_payload = artifact.model_dump(mode="json", exclude={"artifact_hash"})
    altered_payload["feature_matrix_hash"] = digest("different-features")
    altered = HistoricalModelChallengerArtifactV1.model_validate(altered_payload)
    assert altered.artifact_hash != artifact.artifact_hash


def test_compare_day_ranks_and_return_summary_use_matched_capacity() -> None:
    parent = _parent()
    challenger = HistoricalModelChallenger(
        feature_source=_FeatureSource(),
        bundle_loader=lambda **_: _bundle(),
        feature_builder=_feature_builder,
        scorer=_scorer,
    ).score_day(
        parent=parent,
        parent_candidate_artifact_hash=digest("parent-artifact"),
        target_trade_date=date(2026, 5, 18),
        model_root="/research-model-root",
        bundle_id=BUNDLE_ID,
        expected_selection_runtime_semantics_hash=RUNTIME_SEMANTICS_HASH,
    )

    ranks = compare_day_ranks(control=parent, enhanced=parent, challenger=challenger)
    summary = summarize_return_records(
        [
            {"trade_date": "2026-05-15", "value": 0.10},
            {"trade_date": "2026-05-15", "value": -0.02},
            {"trade_date": "2026-06-01", "value": 0.03},
        ]
    )

    assert ranks["a20_b20_changed"] == 0
    assert ranks["a5_c5_overlap"] == 0
    assert ranks["model_rank_changed_count"] == 20
    assert summary["sample_count"] == 3
    assert summary["win_rate"] == pytest.approx(2 / 3)
    assert summary["daily_observation_count"] == 2


def test_paired_daily_delta_uses_only_shared_sample_days() -> None:
    baseline = summarize_return_records(
        [
            {"trade_date": "2026-05-15", "value": 0.01},
            {"trade_date": "2026-05-16", "value": -0.02},
        ]
    )
    current = summarize_return_records(
        [
            {"trade_date": "2026-05-15", "value": 0.03},
            {"trade_date": "2026-05-17", "value": 0.04},
        ]
    )

    delta = summarize_paired_daily_delta(baseline, current)

    assert delta["paired_day_count"] == 1
    assert delta["mean_return_difference"] == pytest.approx(0.02)
    assert delta["mean_return_difference_ci95"] == pytest.approx((0.02, 0.02))
    assert delta["win_rate_difference"] == 0.0


def test_matched_lifecycle_uses_model_rank_for_entry_and_selection_rank_for_exit() -> None:
    def candidate(symbol: str, rank: int, mark: float) -> AdvisoryTransitionCandidateV1:
        return AdvisoryTransitionCandidateV1(
            symbol=symbol,
            rank=rank,
            score=float(10 - rank),
            entry_mark=mark,
            exit_mark=mark,
        )

    first_date = date(2026, 5, 15)
    replay = replay_matched_lifecycle(
        group_name="C5",
        policy=AdvisoryTransitionPolicyV1(
            target_count=1,
            rank_enter_threshold=20,
            rank_exit_threshold=40,
            rank_exit_confirm_days=1,
            daily_replacement_budget=1,
            stop_loss_bps=5000,
            take_profit_bps=5000,
            trailing_stop_bps=5000,
            time_stop_days=20,
        ),
        days=(
            HistoricalComparisonLifecycleDayV1(
                decision_trade_date=first_date,
                next_trade_date=first_date + timedelta(days=1),
                entry_candidates=(
                    candidate("000001.SZ", 1, 10.0),
                    candidate("000002.SZ", 2, 8.0),
                ),
                review_rank_by_symbol={"000001.SZ": 10, "000002.SZ": 1},
                exit_mark_by_symbol={"000001.SZ": 10.0, "000002.SZ": 8.0},
                exit_mark_available_by_symbol={"000001.SZ": True, "000002.SZ": True},
                observed_max_selection_rank=50,
            ),
            HistoricalComparisonLifecycleDayV1(
                decision_trade_date=first_date + timedelta(days=1),
                next_trade_date=first_date + timedelta(days=2),
                entry_candidates=(
                    candidate("000001.SZ", 1, 9.0),
                    candidate("000002.SZ", 2, 8.0),
                ),
                review_rank_by_symbol={"000001.SZ": 50, "000002.SZ": 1},
                exit_mark_by_symbol={"000001.SZ": 9.0, "000002.SZ": 8.0},
                exit_mark_available_by_symbol={"000001.SZ": True, "000002.SZ": True},
                observed_max_selection_rank=50,
            ),
        ),
    )

    assert replay["daily"][0]["active_symbols"] == ["000001.SZ"]
    assert replay["daily"][1]["active_symbols"] == ["000002.SZ"]
    assert replay["action_counts"]["EXIT"] == 1
    assert replay["completed_episodes"][0]["symbol"] == "000001.SZ"
    assert replay["completed_episodes"][0]["gross_return"] == pytest.approx(-0.1)
    assert replay["episode_win_rate"] == 0.0


def _parent() -> HistoricalRangeCandidateArtifactPayloadV2:
    candidates = []
    for rank in range(1, 21):
        symbol = f"{rank:06d}.SZ"
        score = Decimal(21 - rank)
        lineage = {
            "component_scores": {
                LSTM_LEG_ID: {
                    "raw_score": str(score),
                    "normalized_score": str(score),
                    "leg_rank": rank,
                    "weight": str(TERMINAL_WEIGHTS[LSTM_LEG_ID]),
                },
                FUND_LEG_ID: {
                    "raw_score": str(score),
                    "normalized_score": str(score),
                    "leg_rank": rank,
                    "weight": str(TERMINAL_WEIGHTS[FUND_LEG_ID]),
                },
            }
        }
        candidates.append(
            HistoricalRangeCandidateFactV1(
                candidate_id=derive_prefixed_id("ahc", {"day_run_id": "day_1", "symbol": symbol}),
                day_run_id="day_1",
                symbol=symbol,
                membership_status="INCLUDED",
                alpha_raw_rank=rank,
                alpha_raw_score=score,
                hmm_adjusted_rank=rank,
                hmm_adjusted_score=score,
                risk_policy_adjusted_rank=rank,
                risk_policy_adjusted_score=score,
                selection_effective_rank=rank,
                selection_effective_score=score,
                component_lineage_json=lineage,
                component_lineage_hash=canonical_json_sha256(lineage),
            )
        )
    header = {
        "runtime_profile_hash": digest("runtime"),
        "selection_semantics_hash": digest("selection"),
        "code_release_hash": digest("release"),
        "calendar_identity_hash": digest("calendar"),
        "universe_identity_hash": digest("universe"),
    }
    stage_trace = {
        name: {
            "stage": name,
            "status": "COMPLETE",
            "input_count": 20,
            "output_count": 20,
            "excluded_count": 0,
        }
        for name in ("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective")
    }
    return HistoricalRangeCandidateArtifactPayloadV2(
        range_run_id="range_1",
        day_run_id="day_1",
        research_program_id="program_1",
        decision_trade_date=date(2026, 5, 15),
        candidate_input_hash=digest("candidate-input"),
        package_id=PACKAGE_ID,
        package_version="1.0.0",
        manifest_sha256=MANIFEST_SHA256,
        alpha_mode="multi_alpha",
        runtime_profile_hash=header["runtime_profile_hash"],
        selection_semantics_hash=header["selection_semantics_hash"],
        code_release_hash=header["code_release_hash"],
        calendar_identity_hash=header["calendar_identity_hash"],
        universe_identity_hash=header["universe_identity_hash"],
        universe_count=5000,
        raw_signal_identity_hash=canonical_json_sha256(header),
        raw_signal_semantic_header=header,
        raw_inference_receipt={"status": "COMPLETE", "score_count": 20},
        source_read_receipt_hashes=(digest("source-read"),),
        stage_trace=stage_trace,
        candidate_outcome="CANDIDATES_AVAILABLE",
        source_revision_refs=(
            HistoricalRangeSourceRevisionRefV1(
                revision_id="revision-1",
                revision_hash=digest("revision-1"),
            ),
        ),
        candidates=tuple(candidates),
    )


def _bundle() -> LoadedAdvisoryModelBundle:
    manifest = {
        "schema_version": "advisory_model_bundle_v2",
        "status": "EXPERIMENTAL_SHADOW",
        "calibration_state": "NOT_APPLICABLE_RANKING_SCORE",
        "program_id": "advp_test",
        "binding_version_id": "advb_test",
        "package_id": PACKAGE_ID,
        "manifest_sha256": MANIFEST_SHA256,
        "style_profile_id": "style-test",
        "style_profile_hash": digest("style"),
        "selection_runtime_semantics_hash": RUNTIME_SEMANTICS_HASH,
        "feature_schema_version": "advisory_feature_schema_v1",
        "feature_schema_hash": digest("feature-schema"),
        "terminal_weights": TERMINAL_WEIGHTS,
        "ensemble_score_policy": ENSEMBLE_SCORE_POLICY,
        "selection_prior_policy": SELECTION_PRIOR_POLICY,
        "explanation_policy": "MODEL_MEMBER_RAW_CONTRIBUTION_MEAN_V1",
        "seeds": list(QUALITY_SEEDS),
        "model_weight": 0.75,
        "continuation_cutoff": "2026-03-10",
    }
    return LoadedAdvisoryModelBundle(
        bundle_id=BUNDLE_ID,
        bundle_path=Path("/bundle"),
        manifest=manifest,
        feature_schema={"trained_feature_names": list(MODEL_FEATURE_COLUMNS)},
        hmm_models={},
        baselines={},
        booster=None,
        boosters=tuple(object() for _ in QUALITY_SEEDS),
        manifest_file_sha256=digest("bundle-manifest"),
    )


class _FeatureSource:
    def load(self, **_: object) -> SimpleNamespace:
        return SimpleNamespace(
            candidate_daily=None,
            candidate_static=None,
            market_daily=None,
            benchmark_daily=None,
            suspend_rows=None,
            hmm_states=None,
            hmm_unavailable=(),
        )


def _feature_builder(**kwargs: object) -> SimpleNamespace:
    candidates = kwargs["candidates"]
    assert isinstance(candidates, pd.DataFrame)
    return SimpleNamespace(
        coverage=pd.DataFrame([{"status": "available", "required_missing_columns": []}]),
        features=pd.DataFrame(
            {
                "instrument": candidates["instrument"].tolist(),
                "selection_effective_rank": candidates["selection_effective_rank"].tolist(),
                "parent_combined_score": candidates["combined_score"].tolist(),
            }
        ),
    )


def _scorer(_bundle: LoadedAdvisoryModelBundle, features: pd.DataFrame) -> list[dict[str, object]]:
    result = []
    for row in features.itertuples(index=False):
        model_rank = 21 - int(row.selection_effective_rank)
        result.append(
            {
                "symbol": row.instrument,
                "selection_effective_rank": int(row.selection_effective_rank),
                "selection_score": float(row.parent_combined_score),
                "advisory_model_rank": model_rank,
                "advisory_model_score": float(model_rank),
                "is_top5": model_rank <= 5,
                "score_components": {
                    "ensemble_score": float(model_rank),
                    "selection_prior": 0.0,
                    "model_weight": 0.75,
                },
                "top_feature_contributions": [],
            }
        )
    return sorted(result, key=lambda item: int(item["advisory_model_rank"]))
