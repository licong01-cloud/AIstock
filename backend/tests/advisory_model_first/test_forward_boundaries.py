from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from backend.services.advisory_forward.service import AdvisoryForwardService
from backend.services.advisory_program import AdvisoryProgram


def _program(program_id: str) -> AdvisoryProgram:
    return AdvisoryProgram(
        program_id=program_id,
        program_name=program_id,
        status="ENABLED",
        version=1,
        target_count=20,
        package_mode="single_package",
        package_ids=[f"pkg_{program_id}"],
        package_weights={f"pkg_{program_id}": 1.0},
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


class _Calendar:
    def is_trading_day(self, value: date) -> bool:
        return value == date(2026, 8, 14)

    def next_trading_day(self, value: date, *, inclusive: bool = False) -> date:
        assert value == date(2026, 8, 14)
        assert not inclusive
        return date(2026, 8, 17)


class _Repository:
    def __init__(self) -> None:
        self.run: dict[str, object] | None = None
        self.observation = None

    def pending_settlements(self, **_kwargs):
        return []

    def list_runs(self, **_kwargs):
        return []

    def begin_attempt(self, run):
        self.run = {
            **run.__dict__,
            "publication_status": "PENDING",
            "run_payload_json": {},
            "model_resolution_json": {},
        }
        return dict(self.run)

    def commit_publication(self, **kwargs):
        self.run = {
            **self.run,
            "publication_status": "PUBLISHED",
            "settlement_status": "NOT_DUE",
            "selection_run_id": kwargs["review_run"].selection_run_id,
            "review_run_id": kwargs["review_run"].review_run_id,
            "list_version_id": kwargs["list_version"].list_version_id,
            "run_payload_json": kwargs["publication_payload"],
            "model_resolution_json": kwargs["model_resolution"],
        }
        return dict(self.run)

    def save_observation(self, observation):
        self.observation = observation
        return observation.payload()

    def mark_failure(self, **_kwargs):
        return dict(self.run or {})


class _Programs:
    def __init__(self, program: AdvisoryProgram) -> None:
        self.program = program
        self.repository = SimpleNamespace(list_versions=lambda *_args, **_kwargs: [])

    def list_programs(self, **_kwargs):
        return [self.program]

    def get_program(self, _program_id: str):
        return self.program

    def active_binding(self, _program_id: str):
        return {"binding_version_id": "advb-test", "package_ids": self.program.package_ids}

    def prepare_forward_selection(self, *_args, **_kwargs):
        binding = SimpleNamespace(binding_version_id="advb-test", package_ids=self.program.package_ids)
        candidate = SimpleNamespace(
            symbol="000001.SZ",
            rank=1,
            score=0.9,
            reference_price=None,
            previous_close=None,
            selection_entry_price=None,
            current_price=None,
            component_scores={},
            stock_name=None,
            selection_entry_price_time=None,
        )
        run = SimpleNamespace(
            run_id="sel-test",
            data_source="DB_HISTORICAL",
            trade_date=date(2026, 8, 17),
            runtime_config={},
            aggregate_results=[candidate],
        )
        return self.program, binding, run, {}

    def active_episode_objects(self, _program_id: str):
        return []

    def load_forward_market_marks(self, **_kwargs):
        raise AssertionError("publication must not read target-date market data")


def test_publication_does_not_read_target_market_and_model_unavailable_does_not_block_baseline() -> None:
    repository = _Repository()
    service = AdvisoryForwardService(
        repository=repository,
        program_service=_Programs(_program("advp_test")),
        model_service=SimpleNamespace(model_root=lambda: ""),
        calendar=_Calendar(),
        now_provider=lambda: datetime(2026, 8, 14, 16, 31, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    result = service.run_once()

    assert result["results"][0]["status"] == "PUBLISHED"
    assert result["results"][0]["model_status"] == "UNAVAILABLE"
    assert repository.observation.status == "UNAVAILABLE"


def test_one_program_failure_does_not_block_another_program() -> None:
    first = _program("advp_first")
    second = _program("advp_second")

    class _ProgramList:
        def list_programs(self, **_kwargs):
            return [first, second]

    class _Service(AdvisoryForwardService):
        def _publish(self, program_id: str, **_kwargs):
            if program_id == first.program_id:
                raise RuntimeError("first program failed")
            return {"program_id": program_id, "status": "PUBLISHED"}

    service = _Service(
        repository=SimpleNamespace(pending_settlements=lambda **_kwargs: [], list_runs=lambda **_kwargs: []),
        program_service=_ProgramList(),
        model_service=SimpleNamespace(),
        calendar=_Calendar(),
        now_provider=lambda: datetime(2026, 8, 14, 16, 31, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    result = service.run_once()

    assert [row["status"] for row in result["results"]] == ["FAILED", "PUBLISHED"]
