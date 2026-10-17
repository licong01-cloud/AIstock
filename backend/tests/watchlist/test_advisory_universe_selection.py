from __future__ import annotations

from datetime import date, timedelta

import pytest

from backend.services.advisory_program import (
    PACKAGE_MODE_SINGLE,
    REASON_ADVISORY_UNIVERSE_BINDING_MISMATCH,
    AdvisoryProgramService,
    InMemoryAdvisoryProgramRepository,
)
from backend.services.advisory_universe import (
    ADVISORY_PIT_SOURCE_LIVE_ROLLING,
    AdvisoryLiveRollingPitRepository,
    AdvisoryUniverseContractError,
    AdvisoryUniverseSnapshot,
    CoreIndexAdvisoryUniverseResolver,
    advisory_universe_catalog,
    normalize_advisory_universe_selection,
)
from backend.services.core_index_membership import (
    CoreIndexMembershipUnavailable,
    ResolvedUniverse,
    UniverseInterval,
    UniverseUnavailableReason,
)
from backend.services.trading_core.errors import RuntimeConfigInvalidError


class Calendar:
    def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
        days: list[date] = []
        current = start_date
        while current <= end_date:
            days.append(current)
            current += timedelta(days=1)
        return days

    def next_trading_day(self, anchor_date: date, *, inclusive: bool = False) -> date:
        return anchor_date if inclusive else anchor_date + timedelta(days=1)


class Resolver:
    def __init__(
        self,
        symbols: set[str],
        *,
        pit_source: str = "FROZEN_CANONICAL_PIT",
        pit_universe_key: str = "all_a_ex_st_bj_ipo_ge_1y_v1",
        pit_rule_version: str = "shsz_a_252td_st_delist_asof_v2",
        pit_revision: str | None = None,
    ) -> None:
        self.symbols = frozenset(symbols)
        self.calls: list[tuple[dict, date]] = []
        self.pit_source = pit_source
        self.pit_universe_key = pit_universe_key
        self.pit_rule_version = pit_rule_version
        self.pit_revision = pit_revision

    def resolve(self, selection: dict, trade_date: date) -> AdvisoryUniverseSnapshot:
        self.calls.append((selection, trade_date))
        return AdvisoryUniverseSnapshot(
            selection=dict(selection),
            trade_date=trade_date,
            membership_revision="2026-09-12T00:00:00+00:00",
            eligible_symbols=self.symbols,
            source_pool_ids_by_symbol={symbol: tuple(selection["pool_ids"]) for symbol in self.symbols},
            pit_source=self.pit_source,
            pit_universe_key=self.pit_universe_key,
            pit_rule_version=self.pit_rule_version,
            pit_revision=self.pit_revision,
        )


def candidate(symbol: str, rank: int) -> dict:
    return {
        "symbol": symbol,
        "rank": rank,
        "score": 100 - rank,
        "reference_price": 10,
        "next_open_executable": 10,
        "component_scores": {"package_ranks": {"pkg_a": rank}},
    }


def service_with_resolver(resolver: Resolver) -> tuple[AdvisoryProgramService, InMemoryAdvisoryProgramRepository]:
    repository = InMemoryAdvisoryProgramRepository()
    service = AdvisoryProgramService(
        repository=repository,
        calendar_provider=Calendar(),
        universe_resolver=resolver,
    )
    return service, repository


def test_universe_contract_matches_qe_modes_and_p0_pool_ids() -> None:
    catalog = advisory_universe_catalog()

    assert catalog["modes"] == ["stock_universe", "single_index", "index_union"]
    assert [row["pool_id"] for row in catalog["pools"]] == [
        "csi300",
        "csi500",
        "csi1000",
        "star50",
        "star100",
    ]
    assert normalize_advisory_universe_selection(
        {"mode": "index_union", "pool_ids": ["csi500", "csi300", "csi500"]}
    ) == {"mode": "index_union", "pool_ids": ["csi300", "csi500"]}


def test_resolver_uses_live_selection_pit_after_frozen_canonical_cutoff() -> None:
    class LiveRepository:
        pit_revision = "a" * 64

    live_repository = LiveRepository()
    calls: list[object] = []

    def resolve_fn(selection, start_date, end_date, *, repository=None):
        calls.append(repository)
        if repository is None:
            raise CoreIndexMembershipUnavailable(
                UniverseUnavailableReason.CANONICAL_EQUITY_PIT_UNAVAILABLE,
                "frozen canonical cutoff",
            )
        assert repository is live_repository
        return ResolvedUniverse(
            mode=selection.mode,
            pool_ids=selection.pool_ids,
            benchmark_code=selection.benchmark_code,
            membership_revision="index-revision",
            intervals=(UniverseInterval("000001.SZ", start_date, end_date),),
            source_pool_ids_by_symbol={"000001.SZ": ("csi300",)},
        )

    snapshot = CoreIndexAdvisoryUniverseResolver(
        resolve_fn=resolve_fn,
        live_repository_factory=lambda: live_repository,  # type: ignore[arg-type]
    ).resolve({"mode": "single_index", "pool_ids": ["csi300"]}, date(2026, 9, 11))

    assert calls == [None, live_repository]
    assert snapshot.eligible_symbols == frozenset({"000001.SZ"})
    assert snapshot.pit_source == ADVISORY_PIT_SOURCE_LIVE_ROLLING
    assert snapshot.pit_universe_key == "shsz_st_pit_active_v1"
    assert snapshot.pit_rule_version == "st_pub_next_trade_restore_active_l_v1"
    assert snapshot.pit_revision == "a" * 64


def test_resolver_does_not_hide_index_membership_failure_with_live_pit() -> None:
    def resolve_fn(_selection, _start_date, _end_date, *, repository=None):
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.MEMBERSHIP_HISTORY_UNAVAILABLE,
            "index history unavailable",
        )

    with pytest.raises(AdvisoryUniverseContractError) as captured:
        CoreIndexAdvisoryUniverseResolver(
            resolve_fn=resolve_fn,
            live_repository_factory=lambda: pytest.fail("live fallback must not run"),
        ).resolve({"mode": "single_index", "pool_ids": ["csi300"]}, date(2026, 9, 11))

    assert str(captured.value) == "index history unavailable"


def test_live_selection_pit_repository_fails_closed_when_state_is_dirty() -> None:
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args):
            return None

        def fetchone(self):
            return {
                "rule_version": "st_pub_next_trade_restore_active_l_v1",
                "status": "ready",
                "dirty": True,
                "start_date": date(2018, 8, 1),
                "end_date": date(2026, 9, 11),
                "source_fingerprint_sha256": "a" * 64,
                "updated_at": None,
            }

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, **_kwargs):
            return Cursor()

    repository = AdvisoryLiveRollingPitRepository(lambda: Connection())

    with pytest.raises(CoreIndexMembershipUnavailable) as captured:
        repository.fetch_canonical_intervals(date(2026, 9, 11), date(2026, 9, 11))

    assert captured.value.reason is UniverseUnavailableReason.CANONICAL_EQUITY_PIT_UNAVAILABLE


def test_live_selection_pit_repository_fails_closed_when_revision_is_not_sha256() -> None:
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args):
            return None

        def fetchone(self):
            return {
                "rule_version": "st_pub_next_trade_restore_active_l_v1",
                "status": "ready",
                "dirty": False,
                "start_date": date(2018, 8, 1),
                "end_date": date(2026, 9, 11),
                "source_fingerprint_sha256": "not-a-sha256",
                "updated_at": None,
            }

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, **_kwargs):
            return Cursor()

    with pytest.raises(CoreIndexMembershipUnavailable) as captured:
        AdvisoryLiveRollingPitRepository(lambda: Connection()).fetch_canonical_intervals(
            date(2026, 9, 11), date(2026, 9, 11)
        )

    assert captured.value.reason is UniverseUnavailableReason.CANONICAL_EQUITY_PIT_UNAVAILABLE


@pytest.mark.parametrize(
    ("selection", "reason_code"),
    [
        (["csi300"], "ADVISORY_UNIVERSE_SELECTION_INVALID"),
        ({"mode": "single_index", "pool_ids": "csi300"}, "ADVISORY_UNIVERSE_POOL_IDS_INVALID"),
        ({"mode": "single_index", "pool_ids": ["not_a_pool"]}, "ADVISORY_UNIVERSE_SELECTION_INVALID"),
        (
            {"mode": "single_index", "pool_ids": ["csi300"], "benchmark_code": "000300.SH"},
            "ADVISORY_UNIVERSE_FIELDS_INVALID",
        ),
    ],
)
def test_universe_contract_rejects_non_qe_shapes_and_unknown_pools(selection: object, reason_code: str) -> None:
    with pytest.raises(AdvisoryUniverseContractError) as captured:
        normalize_advisory_universe_selection(selection)  # type: ignore[arg-type]

    assert captured.value.reason_code == reason_code


def test_create_binding_persists_first_class_universe_selection_without_ddl() -> None:
    service, _repository = service_with_resolver(Resolver({"000001.SZ"}))

    program = service.create_program(
        program_name="CSI union advisory",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
        universe_selection={"mode": "index_union", "pool_ids": ["csi500", "csi300"]},
    )

    binding = service.active_binding(program.program_id)
    assert binding["universe_selection"] == {"mode": "index_union", "pool_ids": ["csi300", "csi500"]}
    assert binding["runtime_config_json"]["universe_selection"] == binding["universe_selection"]


def test_review_filters_to_pit_index_union_reranks_and_records_receipt() -> None:
    resolver = Resolver(
        {"000002.SZ", "000003.SZ"},
        pit_source=ADVISORY_PIT_SOURCE_LIVE_ROLLING,
        pit_universe_key="shsz_st_pit_active_v1",
        pit_rule_version="st_pub_next_trade_restore_active_l_v1",
        pit_revision="b" * 64,
    )
    service, repository = service_with_resolver(resolver)
    program = service.create_program(
        program_name="CSI union advisory",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
        target_count=2,
        status="ENABLED",
        universe_selection={"mode": "index_union", "pool_ids": ["csi300", "csi500"]},
    )

    result = service.run_review(
        program.program_id,
        trade_date=date(2026, 9, 11),
        candidates=[candidate("000001.SZ", 1), candidate("000002.SZ", 2), candidate("000003.SZ", 5)],
        preview=True,
    )

    assert resolver.calls == [({"mode": "index_union", "pool_ids": ["csi300", "csi500"]}, date(2026, 9, 11))]
    entered = [row for row in result.decisions if row.action == "ENTER"]
    assert [(row.symbol, row.rank) for row in entered] == [("000002.SZ", 1), ("000003.SZ", 2)]
    receipt = repository.review_runs[-1].runtime_config_json["advisory_universe_receipt"]
    assert receipt["input_candidate_count"] == 3
    assert receipt["output_candidate_count"] == 2
    assert receipt["excluded_candidate_count"] == 1
    assert receipt["admission_stage"] == "AFTER_SELECTION_BEFORE_ADVISORY_RANKING"
    assert len(receipt["symbol_set_sha256"]) == 64
    assert receipt["pit_source"] == ADVISORY_PIT_SOURCE_LIVE_ROLLING
    assert receipt["pit_universe_key"] == "shsz_st_pit_active_v1"
    assert receipt["pit_rule_version"] == "st_pub_next_trade_restore_active_l_v1"
    assert receipt["pit_revision"] == "b" * 64


def test_forward_target_uses_selection_cutoff_for_universe_without_future_pit() -> None:
    resolver = Resolver({"000001.SZ"})
    service, repository = service_with_resolver(resolver)
    program = service.create_program(
        program_name="CSI300 next-day advisory",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
    )

    service.run_review(
        program.program_id,
        trade_date=date(2026, 9, 14),
        candidates=[candidate("000001.SZ", 1)],
        runtime_config={
            "advisory_date_context": {
                "target_trade_date": "2026-09-14",
                "selection_as_of_trade_date": "2026-09-11",
            }
        },
        preview=True,
    )

    assert resolver.calls == [({"mode": "single_index", "pool_ids": ["csi300"]}, date(2026, 9, 11))]
    receipt = repository.review_runs[-1].runtime_config_json["advisory_universe_receipt"]
    assert receipt["trade_date"] == "2026-09-14"
    assert receipt["universe_as_of_trade_date"] == "2026-09-11"


def test_universe_rejects_selection_cutoff_on_or_after_target_date() -> None:
    service, _repository = service_with_resolver(Resolver({"000001.SZ"}))
    program = service.create_program(
        program_name="CSI300 invalid cutoff advisory",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
    )

    with pytest.raises(RuntimeConfigInvalidError, match="must be before target_trade_date"):
        service.run_review(
            program.program_id,
            trade_date=date(2026, 9, 14),
            candidates=[candidate("000001.SZ", 1)],
            runtime_config={
                "advisory_date_context": {
                    "target_trade_date": "2026-09-14",
                    "selection_as_of_trade_date": "2026-09-14",
                }
            },
            preview=True,
        )


def test_review_rejects_runtime_universe_override_that_differs_from_binding() -> None:
    service, _repository = service_with_resolver(Resolver({"000001.SZ"}))
    program = service.create_program(
        program_name="CSI300 advisory",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
    )

    with pytest.raises(RuntimeConfigInvalidError) as captured:
        service.run_review(
            program.program_id,
            trade_date=date(2026, 9, 11),
            candidates=[candidate("000001.SZ", 1)],
            runtime_config={"universe_selection": {"mode": "single_index", "pool_ids": ["csi500"]}},
            preview=True,
        )

    assert captured.value.context["reason_code"] == REASON_ADVISORY_UNIVERSE_BINDING_MISMATCH


def test_stock_universe_keeps_legacy_candidates_without_membership_lookup() -> None:
    resolver = Resolver(set())
    service, repository = service_with_resolver(resolver)
    program = service.create_program(
        program_name="Full market advisory",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
        target_count=1,
    )

    result = service.run_review(
        program.program_id,
        trade_date=date(2026, 9, 11),
        candidates=[candidate("000001.SZ", 1)],
        preview=True,
    )

    assert resolver.calls == []
    assert [row.symbol for row in result.decisions if row.action == "ENTER"] == ["000001.SZ"]
    receipt = repository.review_runs[-1].runtime_config_json["advisory_universe_receipt"]
    assert receipt["universe_selection"] == {"mode": "stock_universe", "pool_ids": []}
    assert receipt["excluded_candidate_count"] == 0


def test_index_universe_zero_match_is_valid_no_recommendation_without_fallback() -> None:
    resolver = Resolver({"600000.SH"})
    service, repository = service_with_resolver(resolver)
    program = service.create_program(
        program_name="CSI300 no match advisory",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
        target_count=1,
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
    )

    result = service.run_review(
        program.program_id,
        trade_date=date(2026, 9, 11),
        candidates=[candidate("000001.SZ", 1)],
        preview=True,
    )

    assert [row for row in result.decisions if row.action == "ENTER"] == []
    receipt = repository.review_runs[-1].runtime_config_json["advisory_universe_receipt"]
    assert receipt["input_candidate_count"] == 1
    assert receipt["output_candidate_count"] == 0
    assert receipt["excluded_candidate_count"] == 1
    assert resolver.calls == [({"mode": "single_index", "pool_ids": ["csi300"]}, date(2026, 9, 11))]


def test_replay_uses_same_index_universe_admission_for_each_trade_date() -> None:
    resolver = Resolver({"000002.SZ"})
    service, repository = service_with_resolver(resolver)
    program = service.create_program(
        program_name="CSI300 replay advisory",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
        target_count=1,
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
    )

    replay = service.run_replay(
        program.program_id,
        start_date=date(2026, 9, 10),
        end_date=date(2026, 9, 11),
        candidates_by_date={
            "2026-09-10": [candidate("000001.SZ", 1), candidate("000002.SZ", 2)],
            "2026-09-11": [candidate("000001.SZ", 1), candidate("000002.SZ", 2)],
        },
        market_by_date={
            "2026-09-10": {"000002.SZ": {"next_open_executable": 10}},
            "2026-09-11": {"000002.SZ": {"next_open_executable": 10, "mark_price": 10}},
        },
    )

    assert len(replay["daily_reviews"]) == 2
    assert all(
        [row["symbol"] for row in day["decisions"] if row["action"] == "ENTER"] in (["000002.SZ"], [])
        for day in replay["daily_reviews"]
    )
    assert resolver.calls == [
        ({"mode": "single_index", "pool_ids": ["csi300"]}, date(2026, 9, 10)),
        ({"mode": "single_index", "pool_ids": ["csi300"]}, date(2026, 9, 11)),
    ]
    replay_receipts = [
        row.runtime_config_json["advisory_universe_receipt"]
        for row in repository.review_runs
        if row.run_type == "REPLAY"
    ]
    assert [row["output_candidate_count"] for row in replay_receipts] == [1, 1]


def test_binding_universe_change_preserves_existing_episode_for_exit_policy() -> None:
    service, repository = service_with_resolver(Resolver({"000001.SZ"}))
    program = service.create_program(
        program_name="CSI300 active advisory",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
        target_count=1,
        status="ENABLED",
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
    )
    service.run_review(
        program.program_id,
        trade_date=date(2026, 9, 11),
        candidates=[candidate("000001.SZ", 1)],
    )
    active_episode_ids = [row.episode_id for row in repository.active_episodes(program.program_id)]
    defaults = service.binding_defaults(program.program_id)

    applied = service.apply_binding(
        program.program_id,
        binding={"universe_selection": {"mode": "single_index", "pool_ids": ["csi500"]}},
        activation_reason="switch new-entry index pool",
        expected_program_version=defaults["expected_program_version"],
        expected_binding_version_id=defaults["expected_binding_version_id"],
        effective_from_trade_date=date.fromisoformat(defaults["effective_from_trade_date"]),
    )

    assert applied["binding"]["universe_selection"] == {"mode": "single_index", "pool_ids": ["csi500"]}
    assert [row.episode_id for row in repository.active_episodes(program.program_id)] == active_episode_ids


def test_binding_runtime_replacement_preserves_bound_index_when_universe_is_omitted() -> None:
    service, _repository = service_with_resolver(Resolver({"000001.SZ"}))
    program = service.create_program(
        program_name="CSI300 runtime replacement",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
    )
    defaults = service.binding_defaults(program.program_id)

    applied = service.apply_binding(
        program.program_id,
        binding={"runtime_config_json": {"hmm": {"enabled": True}}},
        activation_reason="replace non-universe runtime settings",
        expected_program_version=defaults["expected_program_version"],
        expected_binding_version_id=defaults["expected_binding_version_id"],
        effective_from_trade_date=date.fromisoformat(defaults["effective_from_trade_date"]),
    )

    assert applied["binding"]["universe_selection"] == {"mode": "single_index", "pool_ids": ["csi300"]}
    assert applied["binding"]["runtime_config_json"] == {
        "hmm": {"enabled": True},
        "universe_selection": {"mode": "single_index", "pool_ids": ["csi300"]},
    }
