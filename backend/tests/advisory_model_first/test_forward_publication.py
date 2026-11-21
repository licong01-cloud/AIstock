from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

from backend.services.advisory_forward.service import (
    ACTION_WATCH,
    AdvisoryForwardService,
    _build_publication_list,
)
from backend.services.advisory_program import ACTION_HOLD, ACTION_WAITING, AdvisoryCandidate, AdvisoryProgram


def test_formal_forward_entry_uses_real_selection_risk_and_evidence_with_d_coverage(monkeypatch):
    from dataclasses import replace
    from backend.services.advisory_program import (
        AdvisoryProgramService, AdvisoryStrategyBindingVersion, InMemoryAdvisoryProgramRepository,
    )
    from backend.services.selection_center.service import SelectionCenterService
    from backend.services.selection_center.repository import InMemorySelectionCenterRepository
    from backend.tests.advisory_model_first.test_forward_boundaries import _Ready, _Repository, _program
    from backend.tests.simulation_runtime.test_strategy_package_selection_service import (
        _advisory_pit_clock_case, _seed_artifact,
    )
    case = _advisory_pit_clock_case()
    program = replace(_program("advp_pit_clock"), package_ids=[case.manifest.package_id],
        package_weights={case.manifest.package_id: 1.}, target_count=1)
    binding = AdvisoryStrategyBindingVersion(binding_version_id="advb-pit-clock", program_id=program.program_id,
        program_version=program.version, package_mode=program.package_mode, package_ids=program.package_ids,
        package_weights=program.package_weights, fusion_method=None, package_set_hash=program.package_set_hash,
        fusion_policy_sha256=None, runtime_config_json=case.config, effective_from_trade_date=case.d)
    programs = InMemoryAdvisoryProgramRepository()
    programs.create_program_with_binding(program, binding)
    center = SelectionCenterService(repository=InMemorySelectionCenterRepository(), package_repository=case.packages,
        strategy_selection_service=case.service,
        result_enrichment_service=SimpleNamespace(enrich_candidates=lambda candidates, **_: candidates))
    run_packages = center.run_packages
    def seeded_run(**kwargs):
        _seed_artifact(case.artifacts, case.manifest,
            [{"symbol": "000001.SZ", "score": .9, "rank": 1, "reference_price": 10.},
             {"symbol": "000002.SZ", "score": .8, "rank": 2, "reference_price": 11.}],
            trade_date=case.t, runtime_config=kwargs["runtime_config"])
        return run_packages(**kwargs)
    monkeypatch.setattr(center, "run_packages", seeded_run)
    advisory = AdvisoryProgramService(repository=programs, selection_service=center)
    monkeypatch.setattr(advisory, "apply_universe_admission",
        lambda candidates, **kwargs: (candidates, kwargs["runtime_config"]))
    repository = _Repository()
    service = AdvisoryForwardService(repository=repository, program_service=advisory, input_readiness=_Ready(),
        model_service=SimpleNamespace(model_root=lambda: None), calendar=_DailyCalendar())
    result = service._publish(program.program_id, decision_date=case.d, target_date=case.t)
    assert result["status"] == "PUBLISHED"
    assert repository.run["publication_status"] == "PUBLISHED"
    assert repository.run["decision_as_of_trade_date"] == case.d
    assert repository.run["target_trade_date"] == case.t
    assert case.reads == [case.d]
    assert case.resolver.calls == 2


def test_inference_failure_still_diagnoses_changed_inputs():
    import pytest
    from backend.services.data_integrity.inputs import InputReadinessError
    from backend.tests.advisory_model_first.test_forward_boundaries import (
        _Programs, _Ready, _Repository, _program as boundary_program,
    )

    programs = _Programs(boundary_program("advp_failure"))
    def inference_failure(*_args, **_kwargs):
        raise RuntimeError("inference failed")
    programs.prepare_forward_selection = inference_failure
    calls = []
    class Readiness(_Ready):
        def recheck(self, bindings):
            calls.append(bindings)
            raise InputReadinessError("input changed during inference")
    repo = _Repository()
    service = AdvisoryForwardService(repository=repo, program_service=programs,
                                    input_readiness=Readiness(), model_service=SimpleNamespace())
    with pytest.raises(InputReadinessError, match="input changed"):
        service._publish(programs.program.program_id, decision_date=date(2026, 8, 14),
                         target_date=date(2026, 8, 17))
    assert len(calls) == 1 and repo.failure["waiting_data"] is True
    assert repo.run["publication_status"] == "PENDING"


class _DailyCalendar:
    def next_trading_day(self, value: date, *, inclusive: bool = False) -> date:
        assert not inclusive
        return value + timedelta(days=1)


def _program() -> AdvisoryProgram:
    return AdvisoryProgram(
        program_id="advp_test",
        program_name="test",
        status="ENABLED",
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


def test_after_close_publication_uses_watch_not_fake_enter_and_preserves_active_hold() -> None:
    candidates = [
        AdvisoryCandidate(symbol="000001.SZ", rank=1, score=1.0, next_open_executable=99.0),
        AdvisoryCandidate(symbol="000002.SZ", rank=2, score=0.9, next_open_executable=88.0),
    ]
    active = [SimpleNamespace(symbol="000002.SZ", episode_id="ep-2")]

    version, items = _build_publication_list(
        program=_program(),
        binding_version_id="advb_test",
        review_run_id="review-test",
        target_trade_date=date(2026, 8, 17),
        decision_as_of_trade_date=date(2026, 8, 14),
        selection_run_id="sel-test",
        candidates=candidates,
        active_episodes=active,
        previous_list=None,
        previous_items=[],
    )

    assert version.trade_date == date(2026, 8, 17)
    assert version.entered_count == 0
    assert [item.action for item in items] == [ACTION_WATCH, ACTION_HOLD]
    assert all(item.entry_price is None for item in items)
    assert all(item.effective_trade_date == date(2026, 8, 17) for item in items)
    assert items[0].evidence_json["decision_as_of_trade_date"] == "2026-08-14"


def test_after_close_publication_keeps_active_episode_missing_from_selection_as_waiting() -> None:
    candidates = [AdvisoryCandidate(symbol="000001.SZ", rank=1, score=1.0)]
    active = [SimpleNamespace(
        symbol="000099.SZ",
        episode_id="ep-missing",
        stock_name="missing active",
        current_rank=35,
        current_score=0.2,
    )]

    version, items = _build_publication_list(
        program=_program(),
        binding_version_id="advb_test",
        review_run_id="review-test",
        target_trade_date=date(2026, 8, 17),
        decision_as_of_trade_date=date(2026, 8, 14),
        selection_run_id="sel-test",
        candidates=candidates,
        active_episodes=active,
        previous_list=None,
        previous_items=[],
    )

    waiting = next(item for item in items if item.symbol == "000099.SZ")
    assert waiting.action == ACTION_WAITING
    assert waiting.episode_id == "ep-missing"
    assert waiting.rank is None
    assert waiting.entry_price is None
    assert version.waiting_count == 1


def test_forward_freezes_meta_label_role_policy_and_projection_identity() -> None:
    resolution = SimpleNamespace(
        program_id="advp_test",
        binding_version_id="advb_test",
        package_id="pkg_test",
        manifest_sha256="a" * 64,
        style_profile_id="style-test",
        style_profile_hash="b" * 64,
        bundle_id="c" * 64,
        bundle_manifest_sha256="d" * 64,
        model_role="meta_label_take_skip_confidence",
        shadow_policy_sha256="e" * 64,
        selection_runtime_semantics_hash="f" * 64,
        feature_schema_version="advisory_feature_schema_v1",
        feature_schema_hash="1" * 64,
        component_roles={"lstm": "alpha_lstm", "fund": "alpha_fund"},
        terminal_weights={"alpha_lstm": 0.7, "alpha_fund": 0.3},
        descriptor_sha256="2" * 64,
    )
    service = object.__new__(AdvisoryForwardService)
    service.model_service = SimpleNamespace(model_root=lambda: "/model-root")
    service.model_resolver = SimpleNamespace(
        is_configured=lambda **_: True,
        resolve=lambda **_: resolution,
    )

    frozen = service._freeze_model_resolution(
        program=_program(),
        active_binding={"binding_version_id": "advb_test", "package_ids": ["pkg_test"]},
        selection_run=SimpleNamespace(),
    )

    assert frozen["status"] == "CONFIGURED"
    assert frozen["model_role"] == "meta_label_take_skip_confidence"
    assert frozen["shadow_policy_sha256"] == "e" * 64
    assert frozen["terminal_weights"] == {"alpha_lstm": 0.7, "alpha_fund": 0.3}


def test_meta_label_forward_observation_uses_frozen_policy_maturity() -> None:
    descriptor_sha256 = "c" * 64
    bundle_id = "d" * 64
    policy_sha256 = "e" * 64
    service = AdvisoryForwardService(
        repository=SimpleNamespace(),
        program_service=SimpleNamespace(),
        model_service=SimpleNamespace(
            model_shadow_for_forward=lambda **_: {
                "status": "EXPERIMENTAL_SHADOW",
                "model_role": "meta_label_take_skip_confidence",
                "model_descriptor_sha256": descriptor_sha256,
                "bundle_id": bundle_id,
                "shadow_policy_sha256": policy_sha256,
                "shadow_policy_maturity_horizon_days": 20,
                "candidate_count": 20,
                "shortlist_count": 5,
                "outcome": {"status": "OUTCOME_UNAVAILABLE"},
                "price_range": {"status": "PRICE_RANGE_UNAVAILABLE"},
            }
        ),
        calendar=_DailyCalendar(),
    )

    observation = service._model_observation(
        forward_run_id="advfwd-test",
        program=_program(),
        binding_version_id="advb-test",
        decision_date=date(2026, 8, 14),
        target_date=date(2026, 8, 17),
        frozen_resolution={
            "status": "CONFIGURED",
            "descriptor_sha256": descriptor_sha256,
            "bundle_id": bundle_id,
            "model_role": "meta_label_take_skip_confidence",
            "shadow_policy_sha256": policy_sha256,
        },
        selection_run_id="sel-test",
        review_run_id="review-test",
        list_version_id="list-test",
    )

    assert observation.status == "EXPERIMENTAL_SHADOW"
    assert observation.maturity_trade_date == date(2026, 9, 6)


def test_legacy_forward_observation_keeps_outcome_horizon_maturity() -> None:
    descriptor_sha256 = "c" * 64
    bundle_id = "d" * 64
    service = AdvisoryForwardService(
        repository=SimpleNamespace(),
        program_service=SimpleNamespace(),
        model_service=SimpleNamespace(
            model_shadow_for_forward=lambda **_: {
                "status": "EXPERIMENTAL_SHADOW",
                "model_role": "quality_reranker",
                "model_descriptor_sha256": descriptor_sha256,
                "bundle_id": bundle_id,
                "candidate_count": 20,
                "shortlist_count": 5,
                "outcome": {"status": "EXPERIMENTAL_SHADOW", "horizons": [2]},
                "price_range": {"status": "PRICE_RANGE_UNAVAILABLE"},
            }
        ),
        calendar=_DailyCalendar(),
    )

    observation = service._model_observation(
        forward_run_id="advfwd-test",
        program=_program(),
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

    assert observation.maturity_trade_date == date(2026, 8, 19)


def test_meta_label_forward_observation_rejects_missing_runtime_role() -> None:
    service = AdvisoryForwardService(
        repository=SimpleNamespace(),
        program_service=SimpleNamespace(),
        model_service=SimpleNamespace(
            model_shadow_for_forward=lambda **_: {
                "status": "EXPERIMENTAL_SHADOW",
                "model_descriptor_sha256": "c" * 64,
                "bundle_id": "d" * 64,
                "candidate_count": 20,
                "shortlist_count": 5,
            }
        ),
        calendar=_DailyCalendar(),
    )

    try:
        service._model_observation(
            forward_run_id="advfwd-test",
            program=_program(),
            binding_version_id="advb-test",
            decision_date=date(2026, 8, 14),
            target_date=date(2026, 8, 17),
            frozen_resolution={
                "status": "CONFIGURED",
                "descriptor_sha256": "c" * 64,
                "bundle_id": "d" * 64,
                "model_role": "meta_label_take_skip_confidence",
                "shadow_policy_sha256": "e" * 64,
            },
            selection_run_id="sel-test",
            review_run_id="review-test",
            list_version_id="list-test",
        )
    except RuntimeError as exc:
        assert str(exc) == "model inference role differs from the publication-frozen descriptor"
    else:
        raise AssertionError("missing runtime model role must fail closed")
