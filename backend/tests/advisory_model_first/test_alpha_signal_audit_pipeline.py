from __future__ import annotations

import json
import subprocess

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.alpha_signal_audit_contracts import (
    ARM_IDS,
    FUNDGROWTH_LEG_ID,
    LSTM_LEG_ID,
    PARENT_ARM_ID,
    PARENT_TERMINAL_WEIGHTS,
    build_three_arm_alpha_audit_request,
)
from backend.services.advisory_model_first.alpha_signal_audit_pipeline import (
    AlphaAuditMetricResult,
    _git_command_for_worktree,
    _git_dirty_paths,
    _publish_bundle,
    _read_bundle,
    build_common_signal_panel,
    build_three_arm_alpha_metrics,
    verify_parent_ranking_parity,
)
from backend.services.advisory_model_first.contracts import PredictionArtifactDescriptor
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.advisory_model_first.tier1_oracle_pipeline import build_tier1_full_universe_outcomes
from backend.services.strategy_package.runtime_variant import canonical_json_sha256
from backend.tests.advisory_model_first.test_oracle_mini_contract import (
    HASH_A,
    HASH_B,
    HASH_C,
    _request,
)
from backend.tests.advisory_model_first.test_tier1_oracle_pipeline import _fixture_frames


def _n1_request(**overrides):
    runs = {LSTM_LEG_ID: "run_lstm", FUNDGROWTH_LEG_ID: "run_fund"}
    prediction_artifacts = {
        run_id: PredictionArtifactDescriptor(
            run_id=run_id,
            run_key=run_id,
            artifact_uri=f"/predictions/{run_id}.pkl",
            artifact_sha256=digest,
            size_bytes=1,
            row_count=100,
            date_start="2024-07-04",
            date_end="2026-03-10",
        )
        for run_id, digest in (("run_lstm", HASH_A), ("run_fund", HASH_B))
    }
    return _request(
        representative_seed_run_ids=runs,
        prediction_artifacts=prediction_artifacts,
        terminal_weights=PARENT_TERMINAL_WEIGHTS,
        **overrides,
    )


def _synthetic_frames():
    decisions = pd.bdate_range("2025-01-02", periods=8)
    calendar = pd.bdate_range("2024-12-02", periods=40)
    symbols = [f"{index:06d}.SZ" for index in range(1, 61)]
    leg_rows = {LSTM_LEG_ID: [], FUNDGROWTH_LEG_ID: []}
    outcome_rows = []
    for day_index, decision in enumerate(decisions):
        for symbol_index, symbol in enumerate(symbols, start=1):
            leg_rows[LSTM_LEG_ID].append({"trade_date": decision, "instrument": symbol, "score": float(symbol_index)})
            leg_rows[FUNDGROWTH_LEG_ID].append(
                {
                    "trade_date": decision,
                    "instrument": symbol,
                    "score": float(np.sin(symbol_index * 1.7 + day_index * 0.3)),
                }
            )
            value = float(symbol_index * 10 + day_index)
            status = "NOT_ENTERED_SUSPENDED" if symbol_index == 1 else "MATURED"
            outcome_rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "instrument": symbol,
                    "target_trade_date": decision + pd.offsets.BDay(1),
                    "planned_exit_trade_date": decision + pd.offsets.BDay(20),
                    "effective_exit_trade_date": decision + pd.offsets.BDay(20),
                    "outcome_status": status,
                    "entry_price": 100.0 if status == "MATURED" else np.nan,
                    "exit_price": 101.0 if status == "MATURED" else np.nan,
                    "gross_excess_return_bps": value if status == "MATURED" else np.nan,
                    "economic_net_excess_bps": value if status == "MATURED" else np.nan,
                    "outcome_known": True,
                    "slot_return_bps": value if status == "MATURED" else 0.0,
                }
            )
    outcomes = pd.DataFrame(outcome_rows)
    coverage = pd.DataFrame(
        {
            "decision_as_of_trade_date": decisions,
            "pit_member_count": 60,
            "known_outcome_count": 60,
            "matured_outcome_count": 59,
            "not_entered_count": 1,
            "unknown_outcome_count": 0,
            "known_outcome_fraction": 1.0,
            "status": "AVAILABLE",
        }
    )
    benchmark = pd.DataFrame(
        {
            "datetime": calendar,
            "instrument": "000300.SH",
            "open": np.linspace(100.0, 101.0, len(calendar)),
            "close": np.linspace(100.0, 102.0, len(calendar)),
        }
    ).set_index(["datetime", "instrument"])
    return (
        {key: pd.DataFrame(value) for key, value in leg_rows.items()},
        outcomes,
        coverage,
        benchmark,
        decisions,
        calendar,
    )


def test_three_arm_metrics_compare_same_window_and_do_not_leak_future_outcomes() -> None:
    leg_frames, outcomes, coverage, benchmark, decisions, calendar = _synthetic_frames()
    request = _n1_request()

    first = build_three_arm_alpha_metrics(
        leg_frames=leg_frames,
        outcomes=outcomes,
        outcome_coverage=coverage,
        benchmark_daily=benchmark,
        decision_dates=decisions,
        trading_calendar=calendar,
        n1_request=request,
    )
    poisoned = outcomes.copy()
    poisoned["economic_net_excess_bps"] *= -1.0
    poisoned["slot_return_bps"] *= -1.0
    second = build_three_arm_alpha_metrics(
        leg_frames=leg_frames,
        outcomes=poisoned,
        outcome_coverage=coverage,
        benchmark_daily=benchmark,
        decision_dates=decisions,
        trading_calendar=calendar,
        n1_request=request,
    )

    ranking_columns = ["arm_id", "decision_as_of_trade_date", "instrument", "selection_effective_rank"]
    pd.testing.assert_frame_equal(first.rankings_top50[ranking_columns], second.rankings_top50[ranking_columns])
    assert set(first.rankings_top50["arm_id"]) == set(ARM_IDS)
    assert first.coverage_daily["common_prediction_count"].eq(60).all()
    assert len(first.full_signal_outcomes) == len(decisions) * 60
    assert set(
        first.full_signal_outcomes.loc[first.full_signal_outcomes["instrument"].eq("000001.SZ"), "outcome_status"]
    ) == {"NOT_ENTERED_SUSPENDED"}
    summaries = first.arm_summary["arms"]
    assert summaries["LSTM_ONLY"]["metrics"]["matured_rank_ic"]["mean"] > 0.99
    assert summaries["LSTM_ONLY"]["metrics"]["top20_winner_recall"]["mean"] == 1.0
    assert summaries["FUNDGROWTH_ONLY"]["metrics"]["matured_rank_ic"]["mean"] < 0.2
    assert len(summaries["LSTM_ONLY"]["bucket_returns"]["5_bucket"]) == 5
    assert len(summaries["LSTM_ONLY"]["bucket_returns"]["10_bucket"]) == 10
    lstm_recall = first.recall_daily[first.recall_daily["arm_id"].eq("LSTM_ONLY")]
    assert np.allclose(lstm_recall["top20_random_expected_recall"], 1.0 / 3.0)
    assert set(first.pairwise_summary["pairs"]) == {
        "IC_WEIGHTED_PARENT_MINUS_LSTM_ONLY",
        "IC_WEIGHTED_PARENT_MINUS_FUNDGROWTH_ONLY",
        "LSTM_ONLY_MINUS_FUNDGROWTH_ONLY",
    }
    assert first.pairwise_summary["arm_churn"]["LSTM_ONLY"]["top20_mean_churn"] == 0.0
    assert set(first.regime_quarter_summary["period_type"]) == {"REGIME", "QUARTER"}


def test_common_signal_panel_reports_own_and_intersection_coverage() -> None:
    leg_frames, outcomes, _, _, decisions, _ = _synthetic_frames()
    leg_frames[FUNDGROWTH_LEG_ID] = leg_frames[FUNDGROWTH_LEG_ID][
        ~leg_frames[FUNDGROWTH_LEG_ID]["instrument"].isin(["000059.SZ", "000060.SZ"])
    ]

    panel, coverage = build_common_signal_panel(
        leg_frames=leg_frames,
        outcomes=outcomes,
        decision_dates=decisions,
        parent_terminal_weights=PARENT_TERMINAL_WEIGHTS,
    )

    assert coverage[f"prediction_count__{LSTM_LEG_ID}"].eq(60).all()
    assert coverage[f"prediction_count__{FUNDGROWTH_LEG_ID}"].eq(58).all()
    assert coverage["common_prediction_count"].eq(58).all()
    assert len(panel) == len(decisions) * 58


def test_parent_ranking_parity_is_exact_and_fails_on_score_drift(tmp_path) -> None:
    leg_frames, outcomes, coverage, benchmark, decisions, calendar = _synthetic_frames()
    result = build_three_arm_alpha_metrics(
        leg_frames=leg_frames,
        outcomes=outcomes,
        outcome_coverage=coverage,
        benchmark_daily=benchmark,
        decision_dates=decisions,
        trading_calendar=calendar,
        n1_request=_n1_request(),
    )
    parent = result.rankings_top50[result.rankings_top50["arm_id"].eq(PARENT_ARM_ID)].drop(columns=["arm_id"])
    expected_path = tmp_path / "parent.parquet"
    parent.to_parquet(expected_path, index=False)
    verify_parent_ranking_parity(parent, expected_path)

    changed = parent.copy()
    changed.loc[changed.index[0], "combined_score"] += 1e-6
    with pytest.raises(AdvisoryModelFirstError) as captured:
        verify_parent_ranking_parity(changed, expected_path)
    assert captured.value.reason_code == "ADVISORY_ALPHA_AUDIT_PARENT_PARITY_FAILED"


def test_public_full_universe_helper_preserves_n1_cash_and_missing_semantics() -> None:
    _, daily, benchmark, suspend, snapshot, calendar, decisions, symbols = _fixture_frames()

    result = build_tier1_full_universe_outcomes(
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        pit_snapshot=snapshot,
        trading_calendar=calendar,
        decision_dates=decisions,
        request=_request(),
    )

    assert len(result.outcomes) == len(decisions) * len(symbols)
    suspended = result.outcomes[result.outcomes["instrument"].eq(symbols[0])]
    assert set(suspended["outcome_status"]) == {"NOT_ENTERED_SUSPENDED"}
    assert suspended["slot_return_bps"].eq(0.0).all()
    missing = result.outcomes[result.outcomes["instrument"].eq(symbols[-1])]
    assert set(missing["outcome_status"]) == {"DATA_UNAVAILABLE_ENTRY"}
    assert missing["slot_return_bps"].isna().all()


def _audit_request(tmp_path):
    runs = {LSTM_LEG_ID: "run_lstm", FUNDGROWTH_LEG_ID: "run_fund"}
    descriptors = {
        run_id: PredictionArtifactDescriptor(
            run_id=run_id,
            run_key=run_id,
            artifact_uri=f"/predictions/{run_id}.pkl",
            artifact_sha256=digest,
            size_bytes=1,
            row_count=100,
            date_start="2024-07-04",
            date_end="2026-03-10",
        )
        for run_id, digest in (("run_lstm", HASH_A), ("run_fund", HASH_B))
    }
    return build_three_arm_alpha_audit_request(
        n0_completion_ref=EvidenceReferenceV1(
            role="n0_completion", artifact_uri="/n0/completion.json", sha256=HASH_A, size_bytes=1
        ),
        n0_completion_receipt_sha256=HASH_B,
        research_window_contract_ref=EvidenceReferenceV1(
            role="research_window", artifact_uri="/n0/window.json", sha256=HASH_B, size_bytes=1
        ),
        research_window_contract_sha256=HASH_C,
        n1_request_ref=EvidenceReferenceV1(
            role="n1_frozen_request", artifact_uri="/n1/request.json", sha256=HASH_A, size_bytes=1
        ),
        n1_request_sha256=HASH_B,
        n1_bundle_path="/n1/bundle",
        n1_bundle_manifest_ref=EvidenceReferenceV1(
            role="n1_formal_bundle_manifest",
            artifact_uri="/n1/bundle/manifest.json",
            sha256=HASH_B,
            size_bytes=1,
        ),
        n1_bundle_id=HASH_C,
        registry_path=str(tmp_path / "registry.jsonl"),
        program_id="program",
        binding_version_id="binding",
        package_id="package",
        manifest_sha256=HASH_A,
        selection_runtime_semantics_hash=HASH_B,
        baseline_policy_sha256=HASH_A,
        shadow_policy_sha256=HASH_B,
        cost_policy_sha256=HASH_C,
        split_policy_sha256="d" * 64,
        pit_spans_sha256="e" * 64,
        feature_schema_hash="f" * 64,
        representative_seed_run_ids=runs,
        prediction_artifacts=descriptors,
        parent_terminal_weights=PARENT_TERMINAL_WEIGHTS,
        repository_root="/repo",
        repository_commit="7" * 40,
        output_root=str(tmp_path),
        created_at="2026-08-31T00:00:00Z",
    )


def test_bundle_is_immutable_has_zero_trials_and_exact_retry(tmp_path) -> None:
    request = _audit_request(tmp_path)
    metrics = AlphaAuditMetricResult(
        coverage_daily=pd.DataFrame({"decision_as_of_trade_date": [pd.Timestamp("2024-07-04")]}),
        full_signal_outcomes=pd.DataFrame({"instrument": ["000001.SZ"]}),
        rankings_top50=pd.DataFrame({"arm_id": ARM_IDS}),
        recall_daily=pd.DataFrame({"arm_id": ARM_IDS, "status": "AVAILABLE"}),
        top5_daily=pd.DataFrame({"arm_id": ARM_IDS, "status": "AVAILABLE"}),
        oracle_daily=pd.DataFrame({"intervened": [True]}),
        signal_metrics_daily=pd.DataFrame({"matured_rank_ic": [0.1]}),
        arm_summary={"schema_version": "test"},
        pairwise_summary={"schema_version": "test"},
        regime_quarter_summary=pd.DataFrame({"period": ["2024Q3"]}),
    )
    source = {"sealed_holdout_accessed": False}
    source["source_identity_sha256"] = canonical_json_sha256(source)
    arguments = {
        "request": request,
        "environment": {"python": "test"},
        "source_receipt": source,
        "metrics": metrics,
        "resource_report": {"peak_rss_bytes": 1, "stages": []},
    }

    first = _publish_bundle(**arguments)
    second = _publish_bundle(**arguments)
    loaded = _read_bundle(first)

    assert first == second
    assert loaded["record"].planned_trial_count == 0
    assert loaded["record"].decision_use.value == "NAVIGATION_ONLY"
    assert loaded["manifest"]["sealed_holdout_accessed"] is False
    assert loaded["manifest"]["files"]["full_universe_signal_outcomes.parquet"]["row_count"] == 1
    assert json.loads((first / "registry_record.json").read_text(encoding="utf-8"))["planned_trial_count"] == 0


def test_run_authorizes_window_before_any_market_or_prediction_loader(tmp_path, monkeypatch) -> None:
    from backend.services.advisory_model_first import alpha_signal_audit_pipeline as pipeline

    request = _audit_request(tmp_path)
    request_path = tmp_path / "request.json"
    request_path.write_text(request.model_dump_json(), encoding="utf-8")
    events: list[str] = []
    monkeypatch.setattr(pipeline, "_load_and_verify_bound_n1", lambda _: _n1_request())

    def deny(_):
        events.append("authorize")
        raise AdvisoryModelFirstError("denied", reason_code="ADVISORY_N1_SEALED_HOLDOUT_ACCESS_DENIED")

    monkeypatch.setattr(pipeline, "authorize_n1_development_access", deny)

    class ForbiddenPredictionLoader:
        def __init__(self, *_args, **_kwargs):
            events.append("prediction_loader")
            raise AssertionError("prediction loader must not run")

    monkeypatch.setattr(pipeline, "ExactPredictionSource", ForbiddenPredictionLoader)

    with pytest.raises(AdvisoryModelFirstError) as captured:
        pipeline.run_three_arm_alpha_audit(request_path)

    assert captured.value.reason_code == "ADVISORY_N1_SEALED_HOLDOUT_ACCESS_DENIED"
    assert events == ["authorize"]


def test_compute_identity_detects_uncommitted_files(tmp_path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=tmp_path, check=True)
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "test"], cwd=tmp_path, check=True)
    assert _git_dirty_paths(tmp_path) == []

    tracked.write_text("dirty\n", encoding="utf-8")
    assert _git_dirty_paths(tmp_path) == ["tracked.txt"]


def test_git_command_translates_windows_linked_worktree_for_wsl(tmp_path, monkeypatch) -> None:
    drive = "X"
    relative_git_dir = "unit-repo/.git/worktrees/advisory-n2"
    windows_git_dir = f"{drive}:/{relative_git_dir}"
    translated_git_dir = f"/mnt/{drive.lower()}/{relative_git_dir}"
    (tmp_path / ".git").write_text(
        f"gitdir: {windows_git_dir}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "backend.services.advisory_model_first.alpha_signal_audit_pipeline._running_on_posix",
        lambda: True,
    )

    def fake_run(command, **kwargs):
        assert command == [
            "wslpath",
            "-u",
            windows_git_dir,
        ]
        assert kwargs == {"check": True, "capture_output": True, "text": True}
        return subprocess.CompletedProcess(command, 0, stdout=f"{translated_git_dir}\n")

    monkeypatch.setattr(
        "backend.services.advisory_model_first.alpha_signal_audit_pipeline.subprocess.run",
        fake_run,
    )

    command, root = _git_command_for_worktree(tmp_path)

    assert root == tmp_path.resolve()
    assert command == [
        "git",
        "-c",
        "core.fileMode=false",
        "-c",
        "core.autocrlf=true",
        f"--git-dir={translated_git_dir}",
        f"--work-tree={tmp_path.resolve()}",
    ]
