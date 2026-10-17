from __future__ import annotations

import subprocess
from datetime import date, timedelta
from pathlib import Path

from backend.services.advisory_historical_range.calendar_resolver import HistoricalRangeCalendarResolver
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.requirement_planner import HistoricalRangeSourceRequirementPlanner
from backend.services.advisory_historical_range.code_release import HistoricalRangeCodeReleaseResolver
from backend.services.advisory_historical_range.models import (
    HistoricalRangeAlphaMode,
    HistoricalRangeResearchBatchRequestV1,
    ResearchProgramSpecV1,
)
from backend.services.advisory_historical_range.request_resolver import (
    HistoricalRangeAdmittedPackageResolver,
    HistoricalRangeProgramResolver,
)
from backend.tests.advisory_historical_range.conftest import date_plan, digest, frozen_program, research_spec
from backend.tests.strategy_package.test_multi_alpha_live_selection import _make_parent


def _git(repo: Path, *args: str) -> None:
    result = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


def test_code_release_hashes_dirty_executed_bytes_without_clean_gate(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "AIstock Test")
    source = repo / "service.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    _git(repo, "add", "service.py")
    _git(repo, "commit", "-m", "seed")
    resolver = HistoricalRangeCodeReleaseResolver(repository_root=repo, closure_paths=("service.py",))
    clean = resolver.resolve()

    source.write_text("VALUE = 2\n", encoding="utf-8")
    dirty = resolver.resolve()

    assert dirty.git_commit == clean.git_commit
    assert dirty.code_release_hash != clean.code_release_hash
    assert dirty.file_content_hashes[0][0] == "service.py"
    assert dirty.code_release_hash == canonical_json_sha256(dirty.semantic_payload())


def test_program_resolver_freezes_complete_target_specific_review_policies() -> None:
    package_repository, parent = _make_parent(live_weight_policy=False)
    specs = tuple(
        ResearchProgramSpecV1(
            program_name=f"history-{target_count}",
            package_id=parent.package_id,
            target_count=target_count,
            review_policy={},
            runtime_config={"runtime_profile": {"selection": {"top_k": target_count}}},
            entry_price_basis="next_open_executable",
            exit_price_basis="next_open_executable",
        )
        for target_count in (5, 20)
    )
    request = HistoricalRangeResearchBatchRequestV1(
        request_id="request-review-policy",
        client_idempotency_key="request-review-policy-key",
        program_specs=specs,
        start_trade_date=date(2026, 7, 1),
        end_trade_date=date(2026, 7, 1),
    )
    resolver = HistoricalRangeProgramResolver(
        package_resolver=HistoricalRangeAdmittedPackageResolver(
            package_reader=package_repository,
        )
    )

    programs = resolver.freeze_programs(
        request=request,
        code_release_id="git-test",
        code_release_hash=digest("code-release"),
        selection_semantics_version="selection-v1",
        selection_semantics_hash=digest("selection"),
        list_semantics_version="list-v1",
        list_semantics_hash=digest("list"),
    )

    programs_by_target = {
        int(program.program_config["target_count"]): program for program in programs
    }
    assert set(programs_by_target) == {5, 20}
    assert programs_by_target[5].review_policy["rank_enter_threshold"] == 5
    assert programs_by_target[5].review_policy["rank_exit_threshold"] == 10
    assert programs_by_target[20].review_policy["rank_enter_threshold"] == 20
    assert programs_by_target[20].review_policy["rank_exit_threshold"] == 40
    for program in programs_by_target.values():
        assert program.program_config["review_policy"] == program.review_policy
        assert program.review_policy_hash == canonical_json_sha256(program.review_policy)
        assert set(program.review_policy) == {
            "rank_enter_threshold",
            "rank_exit_threshold",
            "rank_exit_confirm_days",
            "daily_replacement_budget",
            "stop_loss_bps",
            "take_profit_bps",
            "trailing_stop_bps",
            "time_stop_days",
            "take_profit_mode",
        }


class _Cursor:
    def __init__(self) -> None:
        self._rows = []

    def __enter__(self):  # noqa: ANN204
        return self

    def __exit__(self, *_args):  # noqa: ANN002, ANN204
        return False

    def execute(self, query, params=None):  # noqa: ANN001, ANN201
        normalized = " ".join(str(query).split())
        if "MAX(trade_date)" in normalized:
            self._rows = [{"completed_trade_date": params[0]}]
        elif "BETWEEN" in normalized:
            start, end = params
            self._rows = [
                {"cal_date": start + timedelta(days=offset)}
                for offset in range((end - start).days + 1)
            ]
        else:
            end, limit = params
            self._rows = [
                {"cal_date": end - timedelta(days=offset)} for offset in range(limit - 1, -1, -1)
            ]

    def fetchall(self):  # noqa: ANN201
        return list(self._rows)

    def fetchone(self):  # noqa: ANN201
        return self._rows[0] if self._rows else None


class _Connection:
    def __init__(self) -> None:
        self.readonly = False
        self.rolled_back = False

    def __enter__(self):  # noqa: ANN204
        return self

    def __exit__(self, *_args):  # noqa: ANN002, ANN204
        return False

    def set_session(self, *, isolation_level, readonly, autocommit):  # noqa: ANN001, ANN201
        assert isolation_level == "REPEATABLE READ"
        assert autocommit is False
        self.readonly = readonly

    def cursor(self, **_kwargs):  # noqa: ANN003, ANN201
        return _Cursor()

    def rollback(self) -> None:
        self.rolled_back = True


def test_calendar_resolver_accepts_non_latest_history_and_freezes_leg_warmup() -> None:
    spec = research_spec(package_id="pkg-calendar")
    program = frozen_program(spec, alpha_mode=HistoricalRangeAlphaMode.MULTI_ALPHA)
    request = HistoricalRangeResearchBatchRequestV1(
        request_id="request-calendar",
        client_idempotency_key="calendar-key",
        program_specs=(spec,),
        start_trade_date=date(2026, 6, 1),
        end_trade_date=date(2026, 6, 3),
    )
    connection = _Connection()

    plan, identity_hash = HistoricalRangeCalendarResolver(conn_factory=lambda: connection).resolve(
        request=request,
        frozen_programs=(program,),
    )

    assert plan.ordered_trade_dates == (date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 3))
    assert plan.end_trade_date == plan.completed_trade_date_watermark
    assert all(
        component.warmup_start_trade_date <= plan.start_trade_date
        for component in plan.per_program_input_warmup_ranges[program.research_program_id].components
    )
    for component in plan.per_program_input_warmup_ranges[program.research_program_id].components:
        starts = [item.window_start_trade_date for item in component.day_windows]
        assert starts == sorted(starts)
        assert len(set(starts)) == len(plan.ordered_trade_dates)
    requirements = HistoricalRangeSourceRequirementPlanner().build(
        request=request,
        date_plan=plan,
        frozen_programs=(program,),
        calendar_identity_hash=identity_hash,
        code_release_hash=program.code_release_hash,
    )
    market_starts = {
        (item.component_id, item.decision_trade_date): item.parameter_template["start_date"]
        for item in requirements.requirements
        if item.source_role == "market_history"
    }
    assert market_starts[("leg_a", date(2026, 6, 1))] != market_starts[("leg_a", date(2026, 6, 2))]
    first_calendar = next(
        item
        for item in requirements.requirements
        if item.source_role == "trading_calendar" and item.decision_trade_date == date(2026, 6, 1)
    )
    assert first_calendar.parameter_template["range_start"] < request.start_trade_date.isoformat()
    assert len(identity_hash) == 64
    assert connection.readonly is True
    assert connection.rolled_back is True


def test_requirement_planner_emits_one_resumable_hmm_bundle_per_program_day() -> None:
    spec = research_spec(package_id="pkg-hmm-requirement")
    base = frozen_program(spec, alpha_mode=HistoricalRangeAlphaMode.MULTI_ALPHA)
    runtime_config = {
        "runtime_profile": {
            "selection": {"top_k": 5},
            "hmm": {
                "enabled": True,
                "model_config_id": "hmm-config-1",
                "signal_preset": "sector_trend_v1",
            },
        }
    }
    payload = base.model_dump(mode="json")
    payload.update(
        {
            "runtime_config": runtime_config,
            "runtime_config_hash": digest(runtime_config),
            "frozen_program_hash": None,
        }
    )
    program = type(base).model_validate(payload)
    request = HistoricalRangeResearchBatchRequestV1(
        request_id="request-hmm-requirement",
        client_idempotency_key="request-hmm-requirement-key",
        program_specs=(spec,),
        start_trade_date=date(2026, 6, 2),
        end_trade_date=date(2026, 6, 2),
    )
    plan = date_plan(
        trade_dates=(date(2026, 6, 2),),
        research_program_ids=(program.research_program_id,),
    )

    requirements = HistoricalRangeSourceRequirementPlanner().build(
        request=request,
        date_plan=plan,
        frozen_programs=(program,),
        calendar_identity_hash=digest("calendar"),
        code_release_hash=program.code_release_hash,
    )

    hmm_requirements = [item for item in requirements.requirements if item.source_role == "hmm_frozen_evidence"]
    assert len(hmm_requirements) == 1
    requirement = hmm_requirements[0]
    assert requirement.query_template_id == "historical_hmm_frozen_evidence_bundle"
    assert requirement.parameter_template["phase0a_hmm_metadata"] is None
    assert requirement.parameter_template["selector"] == {
        "schema_version": "advisory_hmm_frozen_evidence_selector_v1",
        "research_program_id": program.research_program_id,
        "package_id": program.package_id,
        "decision_trade_date": "2026-06-02",
        "model_config_id": "hmm-config-1",
        "signal_preset": "sector_trend_v1",
    }
    assert not any(item.source_role in {"hmm_snapshot", "hmm_coefficients"} for item in requirements.requirements)
