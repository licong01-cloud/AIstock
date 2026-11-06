from __future__ import annotations

from contextlib import nullcontext

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
from backend.services.advisory_forward.repository import publication_timing, LATE_PUBLICATION_REASON
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


class _CrossDayRepository:
    def __init__(self, rows, *, acquired=True):
        self.rows = {r["forward_run_id"]: dict(r) for r in rows}
        self.acquired = acquired

    def cycle_lock(self):
        return nullcontext(self.acquired)

    def pending_settlements(self, **_kwargs):
        return []

    def pending_publications(self, *, before, program_ids=None):
        return sorted((r for r in self.rows.values() if r["publication_status"] != "PUBLISHED"
                       and r["decision_as_of_trade_date"] < before
                       and (program_ids is None or r["program_id"] in program_ids)),
                      key=lambda r: (r["target_trade_date"], r["program_id"]))

    def get(self, rid):
        return {"forward_run": self.rows[rid]}

    def mark_failure(self, **kwargs):
        row = self.rows[kwargs["forward_run_id"]]
        row["publication_status"] = "WAITING_DATA" if kwargs["waiting_data"] else "FAILED"
        return row


def _cross_day_service(*, failed_program=None, acquired=True, clock=None):
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    from backend.services.trading_core.errors import DataUnavailableError
    rows = [dict(forward_run_id=f"{pid}-{day}", program_id=pid, program_version=3,
                 binding_version_id="binding", decision_as_of_trade_date=date(2026, 10, day),
                 target_trade_date=date(2026, 10, day + 1), publication_status="WAITING_DATA",
                 settlement_status="NOT_DUE", newer_published=False)
            for day in (28, 29) for pid in ("a", "b")]
    repo = _CrossDayRepository(rows, acquired=acquired)
    service = AdvisoryForwardService(repository=repo,
        program_service=SimpleNamespace(get_program=lambda pid: replace(_program(), program_id=pid),
                                        active_binding=lambda pid: {"binding_version_id": "binding"}),
        model_service=SimpleNamespace(),
        calendar=SimpleNamespace(next_trading_day=lambda d, **kwargs: d + timedelta(days=1)),
        now_provider=lambda: clock or datetime(2026, 10, 30, 10, tzinfo=ZoneInfo("Asia/Shanghai")))
    calls = []
    def publish(pid, *, decision_date, target_date):
        calls.append((pid, decision_date))
        if pid == failed_program:
            raise DataUnavailableError("frozen seed asset unavailable")
        row = repo.rows[f"{pid}-{decision_date.day}"]
        row.update(publication_status="PUBLISHED", settlement_status="NOT_ENTERED")
        return {"status": "PUBLISHED", "program_id": pid, "forward_run_id": row["forward_run_id"]}
    service._publish = publish
    return service, repo, calls


def test_cross_day_recovery_orders_dates_and_is_idempotent():
    service, _, calls = _cross_day_service()
    result = service.recover_pending_publications(through=date(2026, 10, 29), program_ids=["a", "b"])
    assert result["status"] == "COMPLETE"
    assert calls == [("a", date(2026, 10, 28)), ("b", date(2026, 10, 28)),
                     ("a", date(2026, 10, 29)), ("b", date(2026, 10, 29))]
    service.recover_pending_publications(through=date(2026, 10, 29), program_ids=["a", "b"])
    assert len(calls) == 4


def test_cross_day_failed_program_does_not_block_other_programs_or_skip_its_own_date():
    service, _, calls = _cross_day_service(failed_program="a")
    result = service.recover_pending_publications(through=date(2026, 10, 29), program_ids=["a", "b"])
    assert result["blocked_program_ids"] == ["a"]
    assert calls == [("a", date(2026, 10, 28)), ("b", date(2026, 10, 28)), ("b", date(2026, 10, 29))]
    assert any(r["status"] == "SKIPPED_PREVIOUS_PUBLICATION_PENDING" for r in result["results"])


@pytest.mark.parametrize("field,value", [("program_version", 2),
                                        ("binding_version_id", "old"), ("target_trade_date", date(2026, 11, 2))])
def test_cross_day_recovery_preserves_dated_identity(field, value):
    service, repo, calls = _cross_day_service()
    repo.rows["a-28"][field] = value
    result = service.recover_pending_publications(through=date(2026, 10, 29), program_ids=["a"])
    assert result["status"] == "BLOCKED" and calls == []


def test_cross_day_superseded_failure_is_preserved_without_blocking_current_backlog():
    service, repo, calls = _cross_day_service()
    repo.rows["a-28"]["newer_published"] = True
    result = service.recover_pending_publications(through=date(2026, 10, 29), program_ids=["a"])
    assert result["status"] == "COMPLETE"
    assert result["results"][0]["status"] == "SKIPPED_SUPERSEDED_BY_PUBLISHED"
    assert repo.rows["a-28"]["publication_status"] == "WAITING_DATA"
    assert calls == [("a", date(2026, 10, 29))]


def test_operator_cli_rejects_unbound_apply_before_loading_database(monkeypatch):
    import sys
    from scripts import advisory_forward_recover as cli
    monkeypatch.setattr(sys, "argv", ["recover", "--through", "2020-01-01", "--program-id", "a", "--apply"])
    monkeypatch.setattr(cli.subprocess, "check_output", lambda args, **kwargs: "a" * 40 if "rev-parse" in args else "")
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2


def test_cross_day_recovery_exact_program_and_end_bound():
    service, _, calls = _cross_day_service()
    service.recover_pending_publications(through=date(2026, 10, 28), program_ids=["b"])
    assert calls == [("b", date(2026, 10, 28))]


def test_cross_day_recovery_busy_does_not_write_or_predict():
    service, _, calls = _cross_day_service(acquired=False)
    assert service.recover_pending_publications(through=date(2026, 10, 29), program_ids=["a"])["status"] == "ALREADY_RUNNING"
    assert calls == []


@pytest.mark.parametrize("day,hour,is_trading", [(30, 8, True), (31, 10, False)])
def test_scheduler_recovers_known_pending_rows_before_close_and_on_weekend(day, hour, is_trading):
    from zoneinfo import ZoneInfo
    service, _, calls = _cross_day_service(clock=datetime(2026, 10, day, hour, tzinfo=ZoneInfo("Asia/Shanghai")))
    service.calendar.is_trading_day = lambda d: is_trading
    service._evaluate_mature_model_observations = lambda *a, **k: None
    service._retry_one_model_observation = lambda *a: None
    service._entry_price_after_baseline = lambda: {}
    service._entry_value_after_baseline = lambda: {}
    service._lstm_shadow_after_baseline = lambda: {}
    result = service.run_once()
    assert not result["publication_due"] and len(calls) == 4
    assert all(r["stage"] == "CROSS_DAY_RECOVERY" for r in result["results"])


def test_cross_day_recovery_does_not_allow_future_or_duplicate_scope():
    service, _, calls = _cross_day_service()
    for through, ids in [(date(2026, 10, 30), ["a"]), (date(2026, 10, 29), []), (date(2026, 10, 29), ["a", "a"])]:
        with pytest.raises(ValueError):
            service.recover_pending_publications(through=through, program_ids=ids)
    assert calls == []


@pytest.mark.parametrize("hour,minute,eligible", [(1, 29, True), (1, 30, False), (2, 0, False)])
def test_publication_open_boundary_uses_real_aware_time(hour, minute, eligible):
    timing = publication_timing(datetime(2026, 10, 30, hour, minute, tzinfo=UTC), date(2026, 10, 30))
    assert timing["execution_eligible"] is eligible
    assert timing["reason_code"] == (None if eligible else LATE_PUBLICATION_REASON)
    with pytest.raises(ValueError):
        publication_timing(datetime(2026, 10, 30, hour, minute), date(2026, 10, 30))


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
    def cycle_lock(self):
        return nullcontext(True)

    def pending_publications(self, **_kwargs):
        return []

    def __init__(
        self,
        persisted: dict[str, object],
        *,
        model_observation: dict[str, object] | None = None,
        retryable_observations: list[dict[str, object]] | None = None,
    ) -> None:
        self.persisted = persisted
        self.model_observation = model_observation
        self.retryable_observations = retryable_observations or []
        self.saved_observation = None
        self.settlement_kwargs = None

    def pending_settlements(self, **_kwargs):
        return []

    def pending_mature_model_observations(self, **_kwargs):
        return []

    def retryable_model_observations(self, *, limit: int):
        assert limit == 1
        return self.retryable_observations[:limit]

    def get(self, _forward_run_id: str):
        return {
            "forward_run": self.persisted,
            "model_observation": self.model_observation,
        }

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


def test_published_run_recovers_legacy_transient_unavailable_observation() -> None:
    program = _program()
    descriptor_sha256 = "c" * 64
    bundle_id = "d" * 64
    persisted = {
        **_persisted(program),
        "model_resolution_json": {
            "status": "CONFIGURED",
            "descriptor_sha256": descriptor_sha256,
            "bundle_id": bundle_id,
            "package_id": "pkg_test",
        },
    }
    repository = _RecoveryRepository(
        persisted,
        model_observation={
            "status": "UNAVAILABLE",
            "reason_code": "ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
        },
    )
    model_service = SimpleNamespace(
        model_shadow_for_forward=lambda **_kwargs: {
            "status": "EXPERIMENTAL_SHADOW",
            "model_descriptor_sha256": descriptor_sha256,
            "bundle_id": bundle_id,
            "candidate_count": 20,
            "shortlist_count": 5,
        }
    )
    service = AdvisoryForwardService(
        repository=repository,
        program_service=_RecoveryPrograms(program, _episode()),
        model_service=model_service,
        calendar=SimpleNamespace(),
    )

    result = service._resume_published_observation(persisted)

    assert result["status"] == "IDEMPOTENT_REPLAY"
    assert result["model_status"] == "EXPERIMENTAL_SHADOW"
    assert repository.saved_observation.status == "EXPERIMENTAL_SHADOW"
    assert repository.saved_observation.model_descriptor_sha256 == descriptor_sha256


def test_transient_realtime_gap_is_persisted_as_retryable_failure() -> None:
    program = _program()
    descriptor_sha256 = "c" * 64
    bundle_id = "d" * 64
    service = AdvisoryForwardService(
        repository=SimpleNamespace(),
        program_service=_RecoveryPrograms(program, _episode()),
        model_service=SimpleNamespace(
            model_shadow_for_forward=lambda **_kwargs: {
                "status": "UNAVAILABLE",
                "reason_code": "ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
                "message": "realtime market breadth query returned no rows",
                "model_descriptor_sha256": descriptor_sha256,
                "bundle_id": bundle_id,
            }
        ),
        calendar=SimpleNamespace(),
    )

    observation = service._model_observation(
        forward_run_id="advfwd-test",
        program=program,
        binding_version_id="advb-test",
        decision_date=date(2026, 8, 14),
        target_date=date(2026, 8, 17),
        frozen_resolution={
            "status": "CONFIGURED",
            "descriptor_sha256": descriptor_sha256,
            "bundle_id": bundle_id,
        },
        selection_run_id="sel-test",
        review_run_id="review-test",
        list_version_id="list-test",
    )

    assert observation.status == "FAILED"
    assert observation.reason_code == "ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE"


def test_run_once_recovers_transient_observation_after_publication_date_has_advanced() -> None:
    program = _program()
    descriptor_sha256 = "c" * 64
    bundle_id = "d" * 64
    persisted = {
        **_persisted(program),
        "model_resolution_json": {
            "status": "CONFIGURED",
            "descriptor_sha256": descriptor_sha256,
            "bundle_id": bundle_id,
            "package_id": "pkg_test",
        },
    }
    repository = _RecoveryRepository(
        persisted,
        model_observation={
            "status": "UNAVAILABLE",
            "reason_code": "ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
        },
        retryable_observations=[persisted],
    )
    service = AdvisoryForwardService(
        repository=repository,
        program_service=_RecoveryPrograms(program, _episode()),
        model_service=SimpleNamespace(
            model_shadow_for_forward=lambda **_kwargs: {
                "status": "EXPERIMENTAL_SHADOW",
                "model_descriptor_sha256": descriptor_sha256,
                "bundle_id": bundle_id,
                "candidate_count": 20,
                "shortlist_count": 5,
            }
        ),
        calendar=SimpleNamespace(is_trading_day=lambda _value: True),
        now_provider=lambda: datetime(2026, 8, 21, 2, 30, tzinfo=UTC),
    )

    result = service.run_once()

    assert result["publication_due"] is False
    assert result["results"] == [
        {
            "program_id": program.program_id,
            "forward_run_id": "advfwd-test",
            "status": "IDEMPOTENT_REPLAY",
            "target_trade_date": "2026-08-17",
            "model_status": "EXPERIMENTAL_SHADOW",
            "model_reason_code": None,
            "stage": "MODEL_OBSERVATION_RETRY",
        }
    ]
    assert repository.saved_observation.status == "EXPERIMENTAL_SHADOW"


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


def test_stale_pending_settlement_returns_authoritative_terminal_without_re_evaluation() -> None:
    program = _program()
    persisted = {**_persisted(program), "settlement_status": "SETTLED"}
    repository = _RecoveryRepository(persisted)

    class _MustNotEvaluate(_RecoveryPrograms):
        def get_program(self, _program_id: str):
            raise AssertionError("terminal settlement must not reload Program or evaluate transition")

    service = AdvisoryForwardService(
        repository=repository,
        program_service=_MustNotEvaluate(program, _episode()),
        model_service=SimpleNamespace(),
        calendar=SimpleNamespace(),
    )

    result = service._settle({**persisted, "settlement_status": "NOT_DUE"})

    assert result == {
        "program_id": program.program_id,
        "forward_run_id": "advfwd-test",
        "status": "SETTLED",
        "target_trade_date": "2026-08-17",
        "idempotent_replay": True,
    }
    assert repository.settlement_kwargs is None


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
