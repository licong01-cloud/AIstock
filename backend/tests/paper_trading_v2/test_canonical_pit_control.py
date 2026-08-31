from __future__ import annotations

from datetime import date

import pytest

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SCOPE,
    LEGACY_PIT_RULE_VERSION,
)
from backend.services.paper_trading_v2.canonical_pit_control import (
    PIT_PROFILE_CANONICAL,
    PIT_PROFILE_INVALID,
    PIT_PROFILE_LEGACY,
    PaperCanonicalPitControlError,
    PaperCanonicalPitControlService,
    classify_paper_runtime_config,
    plan_paper_runtime_profile_migration,
)
from backend.services.paper_trading_v2.models import PaperRuntimeProfileVersion
from backend.services.selection_center.canonical_pit_runtime import (
    CANONICAL_PIT_RUNTIME_PROFILE_KEY,
    migrate_runtime_config_to_canonical_pointer,
)
from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository
from backend.tests.paper_trading_v2.test_runtime_profile import _portfolio_fixture
from backend.routers import paper_trading_v2 as paper_router


def _legacy_config() -> dict:
    return {
        "runtime_profile": {
            "risk_policy": {
                "enabled": True,
                "providers": ["st_pit"],
                "st_universe_key": "shsz_st_pit_active_v1",
            }
        }
    }


def test_migration_plan_is_deterministic_and_never_rewrites_source() -> None:
    version = PaperRuntimeProfileVersion(
        profile_id="profile_1",
        version_no=1,
        config_json=_legacy_config(),
    )
    source = version.model_copy(deep=True)

    plan = plan_paper_runtime_profile_migration(version)

    assert plan["action"] == "CREATE_NEW_CANONICAL_VERSION"
    assert plan["source_immutable"] is True
    assert plan["in_place_update_allowed"] is False
    assert plan["source_config_sha256"] != plan["target_config_sha256"]
    assert CANONICAL_PIT_RUNTIME_PROFILE_KEY in plan["target_config_json"]
    assert version == source

    canonical = version.model_copy(
        update={"config_json": migrate_runtime_config_to_canonical_pointer(version.config_json)}
    )
    assert plan_paper_runtime_profile_migration(canonical)["action"] == "NO_OP_ALREADY_CANONICAL"


def test_profile_classification_is_fail_closed() -> None:
    assert classify_paper_runtime_config(_legacy_config())["classification"] == PIT_PROFILE_LEGACY
    canonical = migrate_runtime_config_to_canonical_pointer(_legacy_config())
    assert classify_paper_runtime_config(canonical)["classification"] == PIT_PROFILE_CANONICAL
    canonical[CANONICAL_PIT_RUNTIME_PROFILE_KEY]["latest"] = True
    assert classify_paper_runtime_config(canonical)["classification"] == PIT_PROFILE_INVALID


def test_inventory_and_readiness_require_current_active_profile_to_be_canonical() -> None:
    _package_repo, paper_repo, service, _manifest, portfolio = _portfolio_fixture()
    _profile, legacy_version = service.create_runtime_profile(
        portfolio_id=portfolio.portfolio_id,
        profile_name="legacy profile",
        config_json=_legacy_config(),
    )
    control = PaperCanonicalPitControlService(
        repository=paper_repo,
        simulation_repository=InMemorySimulationRuntimeRepository(),
    )

    before = control.activation_readiness(portfolio_id=portfolio.portfolio_id)
    assert before["ready"] is False
    assert {item["reason_code"] for item in before["blockers"]} == {
        "PAPER_ACTIVE_PROFILE_NOT_CANONICAL"
    }

    canonical_version = service.migrate_runtime_profile_version_to_canonical_pit(
        portfolio_id=portfolio.portfolio_id,
        profile_version_id=legacy_version.profile_version_id,
    )
    after = control.activation_readiness(portfolio_id=portfolio.portfolio_id)
    inventory = control.inventory(portfolio_id=portfolio.portfolio_id)

    assert canonical_version.profile_version_id != legacy_version.profile_version_id
    assert paper_repo.get_runtime_profile_version(legacy_version.profile_version_id) == legacy_version
    assert after["ready"] is True
    assert inventory["counts"]["legacy_profile_version_count"] == 1
    assert inventory["counts"]["canonical_profile_version_count"] == 1


def test_inventory_fails_closed_instead_of_truncating_or_growing_unbounded() -> None:
    class _OverflowRepository:
        @staticmethod
        def list_portfolios(*, limit: int) -> list[object]:
            return [object()] * limit

    control = PaperCanonicalPitControlService(
        repository=_OverflowRepository(),
        simulation_repository=InMemorySimulationRuntimeRepository(),
    )

    with pytest.raises(PaperCanonicalPitControlError, match="bounded metadata limit") as exc_info:
        control.inventory()

    assert exc_info.value.context == {
        "scope": "portfolios",
        "current_count": 0,
        "row_limit": 10_000,
    }


class _ShadowCursor:
    def __init__(self, *, state: dict, members: list[dict], executed: list[tuple[str, tuple]]) -> None:
        self.state = state
        self.members = members
        self.executed = executed
        self.execute_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params: tuple) -> None:
        self.execute_count += 1
        self.executed.append((" ".join(sql.split()), params))

    def fetchone(self) -> dict:
        return self.state

    def fetchall(self) -> list[dict]:
        return self.members


class _ShadowConnection:
    def __init__(self, *, state: dict, members: list[dict], executed: list[tuple[str, tuple]]) -> None:
        self.state = state
        self.members = members
        self.executed = executed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self, **_kwargs) -> _ShadowCursor:
        return _ShadowCursor(state=self.state, members=self.members, executed=self.executed)


def test_shadow_compare_is_bounded_exact_identity_and_read_only() -> None:
    executed: list[tuple[str, tuple]] = []
    states = iter(
        [
            (
                {
                    "rule_version": LEGACY_PIT_RULE_VERSION,
                    "scope": "st_only_active",
                    "status": "ready",
                    "dirty": False,
                    "start_date": date(2018, 8, 1),
                    "end_date": date(2026, 7, 31),
                },
                [{"ts_code": "000001.SZ"}],
            ),
            (
                {
                    "rule_version": CANONICAL_PIT_RULE_VERSION,
                    "scope": CANONICAL_PIT_SCOPE,
                    "status": "ready",
                    "dirty": False,
                    "start_date": date(2018, 8, 1),
                    "end_date": date(2026, 7, 31),
                },
                [{"ts_code": "000002.SZ"}],
            ),
        ]
    )

    def factory() -> _ShadowConnection:
        state, members = next(states)
        return _ShadowConnection(state=state, members=members, executed=executed)

    result = PaperCanonicalPitControlService(connection_factory=factory).shadow_compare(
        trade_date=date(2026, 7, 31),
        symbols=["000001.sz", "000002.SZ", "000002.sz"],
    )

    assert result["symbol_count"] == 2
    assert result["counts"] == {"both": 0, "v1_only": 1, "v2_only": 1, "neither": 0}
    assert [row["difference_reason_code"] for row in result["differences"]] == [
        "MEMBER_ONLY_UNDER_LEGACY_RULE",
        "MEMBER_ONLY_UNDER_CANONICAL_RULE",
    ]
    assert all(
        any(span and "entry_reason" in span for span in (row["v1_span"], row["v2_span"]))
        for row in result["differences"]
    )
    assert result["read_only"] is True
    assert result["orders_submitted"] is False
    assert result["official_selection_written"] is False
    assert len(executed) == 4
    assert all(statement.startswith("SELECT ") for statement, _params in executed)

    with pytest.raises(PaperCanonicalPitControlError, match="500 symbols"):
        PaperCanonicalPitControlService().shadow_compare(
            trade_date=date(2026, 7, 31),
            symbols=[f"{index:06d}.SZ" for index in range(501)],
        )


def test_router_migration_defaults_to_plan_only(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class _Service:
        def plan_runtime_profile_canonical_pit_migration(self, **_kwargs) -> dict:
            calls.append("plan")
            return {"action": "CREATE_NEW_CANONICAL_VERSION"}

        def migrate_runtime_profile_version_to_canonical_pit(self, **_kwargs):
            calls.append("apply")
            raise AssertionError("default migration request must not apply")

    monkeypatch.setattr(paper_router, "PaperTradingV2PortfolioService", _Service)

    result = paper_router.migrate_portfolio_runtime_profile_to_canonical_pit(
        "portfolio_1",
        "version_1",
        paper_router.CanonicalPitMigrationRequest(),
    )

    assert result == {
        "ok": True,
        "applied": False,
        "plan": {"action": "CREATE_NEW_CANONICAL_VERSION"},
    }
    assert calls == ["plan"]


def test_router_shadow_delegates_bounded_read_only_request(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Control:
        def shadow_compare(self, *, trade_date: date, symbols: list[str]) -> dict:
            return {
                "trade_date": trade_date.isoformat(),
                "symbols": symbols,
                "read_only": True,
                "orders_submitted": False,
            }

    monkeypatch.setattr(paper_router, "PaperCanonicalPitControlService", _Control)
    result = paper_router.compare_canonical_pit_shadow(
        paper_router.CanonicalPitShadowRequest(
            trade_date=date(2026, 7, 31),
            symbols=["000001.SZ"],
        )
    )

    assert result["shadow"]["read_only"] is True
    assert result["shadow"]["orders_submitted"] is False
