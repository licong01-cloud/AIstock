from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.action_value_contracts import (
    AdvisoryActionRole,
    AdvisoryActionValueStatus,
    AdvisoryEvidenceLevel,
    build_incremental_value_label,
)
from backend.services.advisory_model_first.entry_exit_formal_contracts import (
    build_n2_action_request,
)
from backend.services.advisory_model_first.entry_exit_formal_pipeline import (
    _build_entry_daily,
    _entry_overlap,
    _exit_episode_best,
    _exit_summary,
    _repository_commit,
    _verify_exit_baseline_parity,
    _verify_m4_n1_candidate_identity,
)
from backend.services.advisory_model_first.entry_guard_decision import (
    EntryGuardAction,
    EntryGuardMode,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.exit_label_oracle import ExitLabelOracleResult
from backend.services.advisory_model_first.research_control_contracts import DecisionUse
from backend.tests.advisory_model_first.test_entry_exit_formal_contracts import (
    HASH_A,
    HASH_B,
    HASH_C,
    _request_values,
)


def _entry_fixture():
    decisions = pd.bdate_range("2025-11-07", periods=60)
    rows = []
    actions = {}
    action_by_mode = {
        EntryGuardMode.NO_GUARD: lambda rank: EntryGuardAction.ACCEPT,
        EntryGuardMode.FIXED_GAP_3: lambda rank: EntryGuardAction.SKIP if rank == 1 else EntryGuardAction.ACCEPT,
        EntryGuardMode.FIXED_GAP_5: lambda rank: EntryGuardAction.REDUCE if rank == 1 else EntryGuardAction.ACCEPT,
        EntryGuardMode.FROZEN_DYNAMIC: lambda rank: EntryGuardAction.WAITING if rank == 1 else EntryGuardAction.ACCEPT,
    }
    for day_index, decision in enumerate(decisions):
        target = decision + pd.offsets.BDay(1)
        for rank in range(1, 21):
            instrument = f"{rank:06d}.SZ"
            rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "target_trade_date": target,
                    "instrument": instrument,
                    "selection_rank": rank,
                    "label_status": ("CENSORED_RIGHT_BOUNDARY" if day_index == 59 and rank == 7 else "MATURED"),
                    "net_return_bps": -100.0 if rank == 1 else float(rank * 10),
                }
            )
            for mode, action_builder in action_by_mode.items():
                actions[(decision.date(), target.date(), instrument, mode)] = SimpleNamespace(
                    action=action_builder(rank)
                )
    return pd.DataFrame(rows), actions


def test_entry_daily_preserves_cash_reduce_and_rank_only_replacement_semantics() -> None:
    request = build_n2_action_request(**_request_values())
    overlap, actions = _entry_fixture()

    daily = _build_entry_daily(request=request, overlap=overlap, actions=actions)
    first = daily[daily["decision_date"].eq(daily["decision_date"].min())].set_index("arm_id")

    assert first.loc["NO_GUARD_BASELINE", "daily_net_return_bps"] == pytest.approx(8.0)
    assert first.loc["FIXED_3_CASH", "cash_slot_count"] == 1
    assert first.loc["FIXED_3_REPLACE", "selected_ranks"] == [2, 3, 4, 5, 6]
    assert first.loc["FIXED_3_REPLACE", "replacement_count"] == 1
    assert first.loc["FIXED_5_CASH", "reduce_count"] == 1
    assert first.loc["FIXED_5_CASH", "cash_slot_count"] == 0
    assert first.loc["DYNAMIC_Q90_REPLACE", "selected_ranks"] == [2, 3, 4, 5, 6]
    assert first.loc["PERFECT_SKIP_CASH_ORACLE", "daily_net_return_bps"] == pytest.approx(28.0)
    assert first.loc["PERFECT_SKIP_REPLACE_ORACLE", "selected_ranks"] == [2, 3, 4, 5, 6]
    assert daily.groupby("arm_id").size().eq(60).all()
    # The rank-7 right-boundary row is irrelevant after five positive rank-ordered fills.
    last_oracle = daily[
        daily["decision_date"].eq(daily["decision_date"].max()) & daily["arm_id"].eq("PERFECT_SKIP_REPLACE_ORACLE")
    ].iloc[0]
    assert last_oracle["status"] == "AVAILABLE"


def _exit_label(*, episode: str, decision: date, value: float):
    return build_incremental_value_label(
        role=AdvisoryActionRole.EXIT,
        decision_use=DecisionUse.NAVIGATION_ONLY,
        evidence_level=AdvisoryEvidenceLevel.HISTORICAL_REPLAY,
        sealed_holdout_accessed=False,
        decision_date=decision,
        target_action_date=pd.Timestamp(decision + pd.offsets.BDay(1)).date(),
        effective_action_date=pd.Timestamp(decision + pd.offsets.BDay(1)).date(),
        instrument="000001.SZ" if episode == "episode-a" else "000002.SZ",
        episode_id=episode,
        baseline_action="CONTINUE_BASELINE",
        intervention_action="EXIT_NEXT_OPEN",
        status=AdvisoryActionValueStatus.AVAILABLE,
        baseline_net_value_bps=10.0,
        action_net_value_bps=10.0 + value,
        incremental_net_value_bps=value,
        baseline_policy_sha256=HASH_A,
        intervention_policy_sha256=HASH_B,
        cost_policy_sha256=HASH_C,
        shadow_simulator_sha256="d" * 64,
        information_start=decision,
        information_end=date(2026, 2, 2),
        reason_code="TEST",
    )


def _exit_result() -> ExitLabelOracleResult:
    labels = (
        _exit_label(episode="episode-a", decision=date(2025, 1, 2), value=-10.0),
        _exit_label(episode="episode-a", decision=date(2025, 1, 3), value=20.0),
        _exit_label(episode="episode-b", decision=date(2025, 1, 2), value=-5.0),
        _exit_label(episode="episode-b", decision=date(2025, 1, 3), value=-2.0),
    )
    label_frame = pd.DataFrame([item.model_dump(mode="python") for item in labels])
    baseline = pd.DataFrame(
        {
            "episode_label_id": ["episode-a", "episode-b"],
            "entry_trade_date": pd.to_datetime(["2024-12-30", "2024-12-31"]),
            "effective_exit_date": pd.to_datetime(["2025-01-20", "2025-01-21"]),
            "selection_rank": [1, 2],
            "net_return_bps": [10.0, 10.0],
        }
    )
    decisions = pd.DataFrame(
        {
            "execution_state": [
                "EXECUTED_NEXT_OPEN",
                "DEFERRED_TO_FIRST_EXECUTABLE",
                "BASELINE_CONTINUE",
                "BASELINE_CONTINUE",
            ],
            "deferred_trading_days": [0, 2, 0, 0],
        }
    )
    return ExitLabelOracleResult(
        baseline=SimpleNamespace(labels=baseline),
        labels=labels,
        decisions=(),
        label_frame=label_frame,
        decision_frame=decisions,
        coverage=pd.DataFrame(),
    )


def test_exit_best_chooses_one_positive_intervention_or_holds_per_episode() -> None:
    result = _exit_result()
    best, selected = _exit_episode_best(result)
    summary = _exit_summary(best=best, result=result)

    by_episode = best.set_index("episode_id")
    assert by_episode.loc["episode-a", "oracle_action"] == "EXIT_NEXT_OPEN"
    assert by_episode.loc["episode-a", "realized_oracle_lift_bps"] == 20.0
    assert by_episode.loc["episode-b", "oracle_action"] == "HOLD"
    assert by_episode.loc["episode-b", "realized_oracle_lift_bps"] == 0.0
    assert len(selected) == 2
    assert summary["positive_intervention_count"] == 1
    assert summary["deferred_decision_count"] == 1
    assert summary["mean_deferred_trading_days"] == 2.0


def test_exit_baseline_parity_rejects_numeric_drift() -> None:
    frame = pd.DataFrame(
        {
            "decision_as_of_trade_date": pd.to_datetime(["2025-01-02"]),
            "target_trade_date": pd.to_datetime(["2025-01-03"]),
            "instrument": ["000001.SZ"],
            "selection_rank": [1],
            "label_status": ["MATURED"],
            "entry_price": [10.0],
            "exit_price": [11.0],
            "net_return_bps": [900.0],
            "entry_trade_date": pd.to_datetime(["2025-01-03"]),
            "effective_exit_date": pd.to_datetime(["2025-01-20"]),
        }
    )
    assert _verify_exit_baseline_parity(actual=frame, expected=frame.copy())["status"] == "EXACT"

    changed = frame.copy()
    changed.loc[0, "exit_price"] = 11.1
    with pytest.raises(AdvisoryModelFirstError) as captured:
        _verify_exit_baseline_parity(actual=changed, expected=frame)
    assert captured.value.reason_code == "ADVISORY_N2_EXIT_BASELINE_PARITY_FAILED"


def test_m4_and_n1_candidate_identity_uses_package_not_unrelated_request_hashes() -> None:
    shared = {"package_id": "pkg_parent", "manifest_sha256": HASH_A}
    _verify_m4_n1_candidate_identity(m4_manifest=shared, policy_manifest=shared)

    with pytest.raises(AdvisoryModelFirstError):
        _verify_m4_n1_candidate_identity(
            m4_manifest=shared,
            policy_manifest={**shared, "package_id": "pkg_other"},
        )


def test_cross_os_git_failure_is_remapped_to_the_n2_error_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.services.advisory_model_first import entry_exit_formal_pipeline as pipeline

    def fail(_root):  # noqa: ANN001, ANN202
        raise AdvisoryModelFirstError(
            "source failure",
            reason_code="ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
        )

    monkeypatch.setattr(pipeline, "_cross_os_git_commit", fail)
    with pytest.raises(AdvisoryModelFirstError) as captured:
        _repository_commit(Path("/repo"))
    assert captured.value.reason_code == "ADVISORY_N2_ACTION_REQUEST_INVALID"


def test_entry_overlap_schema_failure_is_typed(tmp_path: Path) -> None:
    keys = {
        "decision_as_of_trade_date": [pd.Timestamp("2025-11-07")],
        "target_trade_date": [pd.Timestamp("2025-11-10")],
        "instrument": ["000001.SZ"],
    }
    m4_path = tmp_path / "m4.parquet"
    baseline_path = tmp_path / "baseline.parquet"
    pd.DataFrame({**keys, "selection_effective_rank": [1]}).to_parquet(m4_path)
    pd.DataFrame(
        {
            **keys,
            "episode_label_id": ["episode"],
            "label_status": ["MATURED"],
            "net_return_bps": [1.0],
            "shadow_policy_sha256": [HASH_B],
            "cost_policy_sha256": [HASH_C],
        }
    ).to_parquet(baseline_path)

    with pytest.raises(AdvisoryModelFirstError) as captured:
        _entry_overlap(m4_path, baseline_path)
    assert captured.value.reason_code == "ADVISORY_N2_ENTRY_KEY_OVERLAP_INVALID"
