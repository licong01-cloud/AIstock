from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from backend.services.advisory_forward.service import (
    AdvisoryForwardService,
    _active_episode_state_hash,
    _published_publication_matches,
    _settlement_payload,
)
from backend.services.advisory_forward.models import AdvisoryForwardRunV1
from backend.services.advisory_program import (
    AdvisoryCandidate,
    AdvisoryEpisode,
    AdvisoryProgram,
    AdvisoryReviewDecision,
    AdvisoryReviewResult,
    program_to_dict,
    _forward_episode_id,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def _program() -> AdvisoryProgram:
    return AdvisoryProgram(
        program_id="advp_test",
        program_name="test",
        status="ENABLED",
        version=3,
        target_count=20,
        package_mode="single_package",
        package_ids=["pkg_test"],
        package_weights={"pkg_test": 1.0},
        fusion_method=None,
        package_set_hash="a" * 64,
        fusion_policy_sha256=None,
        review_policy={
            "rank_enter_threshold": 20,
            "rank_exit_threshold": 40,
            "rank_exit_confirm_days": 2,
            "daily_replacement_budget": 5,
            "stop_loss_bps": 800,
            "take_profit_bps": 1800,
            "trailing_stop_bps": 700,
            "time_stop_days": 20,
            "take_profit_mode": "trailing",
        },
        review_policy_sha256="b" * 64,
        entry_price_basis="next_open_executable",
        exit_price_basis="next_open_executable",
        review_schedule={"frequency": "daily_after_close"},
    )


def _episode() -> AdvisoryEpisode:
    return AdvisoryEpisode(
        episode_id="episode-current",
        program_id="advp_test",
        program_version=3,
        symbol="000001.SZ",
        status="ACTIVE",
        signal_date=date(2026, 8, 14),
        effective_entry_date=date(2026, 8, 17),
        entry_price=10.0,
        entry_price_basis="next_open_executable",
        entry_rank=1,
    )


class _RecoveryRepository:
    def __init__(self, persisted: dict[str, object]) -> None:
        self.persisted = persisted
        self.saved_observation = None
        self.settlement_kwargs = None

    def get(self, _forward_run_id: str):
        return {"forward_run": self.persisted, "model_observation": None}

    def save_observation(self, observation):
        self.saved_observation = observation
        return observation.payload()

    def commit_settlement(self, **kwargs):
        self.settlement_kwargs = kwargs
        return {**self.persisted, "settlement_status": "NOT_ENTERED"}


class _RecoveryPrograms:
    def __init__(self, program: AdvisoryProgram, episode: AdvisoryEpisode) -> None:
        self.program = program
        self.episode = episode
        candidate = AdvisoryCandidate(symbol=episode.symbol, rank=1, score=0.9)
        self.selection_service = SimpleNamespace(
            get_run=lambda _run_id: SimpleNamespace(
                run_id="sel-test",
                trade_date=date(2026, 8, 17),
                runtime_config={},
                aggregate_results=[
                    SimpleNamespace(
                        symbol=candidate.symbol,
                        rank=candidate.rank,
                        score=candidate.score,
                        reference_price=None,
                        previous_close=None,
                        selection_entry_price=None,
                        current_price=None,
                        component_scores={},
                        stock_name=None,
                        selection_entry_price_time=None,
                    )
                ],
            )
        )

    def get_program(self, _program_id: str):
        return self.program

    def active_episode_objects(self, _program_id: str):
        return [self.episode]

    def load_forward_market_marks(self, **_kwargs):
        return {self.episode.symbol: SimpleNamespace(symbol=self.episode.symbol)}

    def evaluate_forward_settlement(self, **_kwargs):
        return AdvisoryReviewResult(
            program=self.program,
            trade_date=date(2026, 8, 17),
            review_status="SUCCEEDED",
            decisions=[],
            active_pool=[self.episode],
            metrics={},
        )


def _persisted(program: AdvisoryProgram) -> dict[str, object]:
    return {
        "forward_run_id": "advfwd-test",
        "program_id": program.program_id,
        "binding_version_id": "advb-test",
        "decision_as_of_trade_date": date(2026, 8, 14),
        "target_trade_date": date(2026, 8, 17),
        "selection_run_id": "sel-test",
        "review_run_id": "review-test",
        "list_version_id": "list-test",
        "publication_status": "PUBLISHED",
        "settlement_status": "NOT_DUE",
        "active_episode_state_hash": "publication-time-state",
        "model_resolution_json": {
            "status": "UNAVAILABLE",
            "reason_code": "ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
        },
        "run_payload_json": {"program_snapshot": program_to_dict(program)},
    }


def test_published_run_recovers_missing_model_observation_without_republishing() -> None:
    program = _program()
    persisted = _persisted(program)
    repository = _RecoveryRepository(persisted)
    service = AdvisoryForwardService(
        repository=repository,
        program_service=_RecoveryPrograms(program, _episode()),
        model_service=SimpleNamespace(),
        calendar=SimpleNamespace(),
    )

    result = service._resume_published_observation(persisted)

    assert result["status"] == "IDEMPOTENT_REPLAY"
    assert result["model_status"] == "UNAVAILABLE"
    assert repository.saved_observation is not None
    assert repository.saved_observation.observation_id.startswith("advobs_")


def test_settlement_reloads_legal_active_state_change_and_protects_commit_with_current_hash() -> None:
    program = _program()
    episode = _episode()
    persisted = _persisted(program)
    repository = _RecoveryRepository(persisted)
    service = AdvisoryForwardService(
        repository=repository,
        program_service=_RecoveryPrograms(program, episode),
        model_service=SimpleNamespace(),
        calendar=SimpleNamespace(),
    )

    result = service._settle(persisted)

    assert result["status"] == "NOT_ENTERED"
    assert repository.settlement_kwargs["expected_active_episode_state_hash"] == _active_episode_state_hash([episode])
    assert repository.settlement_kwargs["expected_program_version"] == program.version
    assert repository.settlement_kwargs["expected_program_status"] == program.status


def test_publish_rejects_reused_target_row_with_mixed_decision_identity() -> None:
    program = _program()
    persisted = _persisted(program)
    persisted["decision_as_of_trade_date"] = date(2026, 8, 13)

    class _AttemptRepository(_RecoveryRepository):
        def begin_attempt(self, _run: AdvisoryForwardRunV1):
            return self.persisted

    service = AdvisoryForwardService(
        repository=_AttemptRepository(persisted),
        program_service=SimpleNamespace(
            get_program=lambda _program_id: program,
            active_binding=lambda _program_id: {"binding_version_id": "advb-test"},
        ),
        model_service=SimpleNamespace(),
        calendar=SimpleNamespace(),
    )

    with pytest.raises(RuntimeError, match="attempt identity differs"):
        service._publish(
            program.program_id,
            decision_date=date(2026, 8, 14),
            target_date=date(2026, 8, 17),
        )


def test_forward_episode_identity_is_deterministic_per_program_target_and_symbol() -> None:
    first = _forward_episode_id(
        program_id="advp_test",
        target_trade_date=date(2026, 8, 17),
        symbol="000001.sz",
    )
    repeated = _forward_episode_id(
        program_id="advp_test",
        target_trade_date=date(2026, 8, 17),
        symbol="000001.SZ",
    )

    assert first == repeated
    assert first.startswith("advep_")


def test_uncertain_publication_commit_recovers_only_exact_payload_hash() -> None:
    payload = {
        "schema_version": "advisory_forward_publication_v1",
        "program_id": "advp_test",
        "target_trade_date": "2026-08-17",
    }
    persisted = {
        "publication_status": "PUBLISHED",
        "publication_payload_sha256": canonical_json_sha256(payload),
    }

    assert _published_publication_matches(persisted, payload) is True
    assert _published_publication_matches(
        persisted,
        {**payload, "target_trade_date": "2026-08-18"},
    ) is False
    assert _published_publication_matches(persisted, None) is False


def test_settlement_payload_hash_tracks_economic_changes_but_not_runtime_timestamps() -> None:
    episode = _episode()
    decision = AdvisoryReviewDecision(
        program_id="advp_test",
        program_version=3,
        trade_date=date(2026, 8, 17),
        symbol=episode.symbol,
        action="HOLD",
        reason_code="RANK_HELD",
        review_status="SUCCEEDED",
        episode_id=episode.episode_id,
        rank=1,
        score=0.9,
        created_at=datetime(2026, 8, 17, 1, tzinfo=UTC),
    )
    first = _settlement_payload(
        program_id="advp_test",
        target_trade_date=date(2026, 8, 17),
        review_status="SUCCEEDED",
        decisions=[decision],
        active_pool=[episode],
    )
    timestamp_only = _settlement_payload(
        program_id="advp_test",
        target_trade_date=date(2026, 8, 17),
        review_status="SUCCEEDED",
        decisions=[replace(decision, created_at=datetime(2026, 8, 17, 2, tzinfo=UTC))],
        active_pool=[
            replace(
                episode,
                created_at=datetime(2026, 8, 17, 2, tzinfo=UTC),
                updated_at=datetime(2026, 8, 17, 2, tzinfo=UTC),
            )
        ],
    )
    changed_price = _settlement_payload(
        program_id="advp_test",
        target_trade_date=date(2026, 8, 17),
        review_status="SUCCEEDED",
        decisions=[replace(decision, exit_price=11.0)],
        active_pool=[episode],
    )

    assert canonical_json_sha256(first) == canonical_json_sha256(timestamp_only)
    assert canonical_json_sha256(first) != canonical_json_sha256(changed_price)
