from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.decision_mark_provider import (
    HistoricalRangeDecisionMarkProvider,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeAdmittedPackageProjectionV1,
    HistoricalRangeArtifactKind,
    HistoricalRangeEpisodeMarkV2,
    HistoricalRangeFrozenProgramV1,
    HistoricalRangeRevisionAdmissibility,
    HistoricalRangeSourceRevisionCatalogV1,
    HistoricalRangeSourceRevisionMemberV1,
    HistoricalRangeSourceRevisionRefV1,
)
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    canonical_rule_parameters_digest,
)
from backend.services.strategy_package.models import StrategyPackageCanonicalPitBindingV2
from backend.services.advisory_historical_range.source_roles import DECISION_MARK_SOURCE_ROLES_V1
from backend.tests.advisory_historical_range.conftest import artifact_ref, digest, frozen_program, research_spec
from backend.tests.advisory_historical_range.test_task_input_sources import authority as authority, _task


def _member(
    role: str, *, universe_key: str | None = None, parameter_date: str = "2026-06-03",
) -> HistoricalRangeSourceRevisionMemberV1:
    parameters = {"trade_date": parameter_date, "role": role}
    if universe_key is not None:
        parameters["universe_key"] = universe_key
    return HistoricalRangeSourceRevisionMemberV1(
        requirement_id=f"requirement_{role}",
        source_role=role,
        dataset_id=f"market.{role}",
        partition_ref=f"partition:{role}",
        decision_trade_date=date(2026, 6, 3),
        query_template_id=f"query_{role}",
        query_template_version="v1",
        query_template_hash=digest(f"query:{role}"),
        bound_parameters=parameters,
        parameter_hash=canonical_json_sha256(parameters),
        row_count=1,
        content_hash=digest(f"content:{role}"),
        admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
        observed_at=datetime(2026, 7, 22, tzinfo=UTC),
    )


def _catalog(
    *, universe_key: str | None = None, state_parameter_date: str = "2026-06-03",
) -> HistoricalRangeSourceRevisionCatalogV1:
    return HistoricalRangeSourceRevisionCatalogV1(
        requirement_plan_hash=digest("plan"),
        catalog_generation=1,
        query_contract_hash=digest("query-contract"),
        calendar_identity_hash=digest("calendar"),
        members=(
            _member("pit_universe"),
            _member("market_history"),
            _member("decision_mark_daily_market"),
            _member("decision_mark_market_state", universe_key=universe_key, parameter_date=state_parameter_date),
        ),
    )


class _Reader:
    def read(self, *, decision_trade_date: date, universe_key: str):
        assert decision_trade_date == date(2026, 6, 3)
        assert universe_key == "shsz_st_pit_active_v1"
        return (
            {"000001.SZ": {"ts_code": "000001.SZ", "close_li": 10000, "adj_factor": "1.2"}},
            {"000001.SZ": {"ts_code": "000001.SZ", "suspended": False, "pit_eligible": True}},
            datetime(2026, 7, 22, tzinfo=UTC),
        )


class _CanonicalReader(_Reader):
    def read(self, *, decision_trade_date: date, universe_key: str):
        assert universe_key == "aistock_equity_pit_snapshot_qe_hmm_full_v2_20260731"
        assert decision_trade_date == date(2026, 6, 3)
        return (
            {"000001.SZ": {"ts_code": "000001.SZ", "close_li": 10000, "adj_factor": "1.2"}},
            {"000001.SZ": {"ts_code": "000001.SZ", "suspended": False, "pit_eligible": True}},
            datetime(2026, 7, 22, tzinfo=UTC),
        )


def _canonical_program() -> HistoricalRangeFrozenProgramV1:
    program = frozen_program(research_spec())
    binding = StrategyPackageCanonicalPitBindingV2(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(),
        release_id="qe_hmm_full_v2_20260731",
        release_cutoff="2026-07-31",
        frozen_snapshot_digest="a" * 64,
        release_manifest_digest="b" * 64,
        qualification_method="REVALIDATED",
        qualification_evidence_digest="c" * 64,
    )
    projection_payload = program.admitted_package_projection.model_dump(mode="json")
    projection_payload["canonical_pit_binding"] = binding.model_dump(mode="json")
    projection = HistoricalRangeAdmittedPackageProjectionV1.model_validate(projection_payload)
    payload = program.model_dump(mode="json")
    payload.update(
        {
            "admitted_package_projection": projection.model_dump(mode="json"),
            "admitted_package_projection_hash": digest(projection.model_dump(mode="json")),
            "frozen_program_hash": None,
        }
    )
    return HistoricalRangeFrozenProgramV1.model_validate(payload)


class _Verifier:
    def __init__(self, catalog: HistoricalRangeSourceRevisionCatalogV1) -> None:
        self._catalog = catalog
        self.calls = 0

    def verify_program_day(self, *, source_roles, **kwargs):
        assert source_roles == DECISION_MARK_SOURCE_ROLES_V1
        self.calls += 1
        return tuple(
            sorted(
                (
                    HistoricalRangeSourceRevisionRefV1(revision_id=item.revision_id, revision_hash=item.revision_hash)
                    for item in self._catalog.members
                    if item.source_role in source_roles
                ),
                key=lambda item: (item.revision_id, item.revision_hash),
            )
        )


def test_decision_mark_provider_seals_unfiltered_market_mark_with_exact_source_refs(tmp_path: Path) -> None:
    catalog = _catalog()
    verifier = _Verifier(catalog)
    provider = HistoricalRangeDecisionMarkProvider(
        reader=_Reader(),
        source_verifier=verifier,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path / "artifacts"),
    )
    program = frozen_program(research_spec())
    request_ref = artifact_ref(HistoricalRangeArtifactKind.REQUEST, "request")

    result = provider.produce(
        resolved_request_hash=digest("resolved-request"),
        catalog=catalog,
        program=program,
        range_run_id="range_1",
        day_run_id="day_1",
        decision_trade_date=date(2026, 6, 3),
        request_ref=request_ref,
        included_symbols={"000001.SZ"},
        previous_marks_by_symbol={},
        predecessor_day_receipt_ref=None,
        decision_cutoff=datetime(2026, 6, 3, 15, tzinfo=UTC),
    )

    mark = result.mark_set.marks[0]
    assert mark.raw_reference_yuan == Decimal("10")
    assert mark.normalized_reference_mark == Decimal("12.0")
    assert mark.availability == "AVAILABLE"
    assert verifier.calls == 2
    assert result.artifact_ref.artifact_kind is HistoricalRangeArtifactKind.DECISION_MARK_SET


def test_decision_mark_provider_uses_v2_package_frozen_universe_key(tmp_path: Path) -> None:
    catalog = _catalog()
    provider = HistoricalRangeDecisionMarkProvider(
        reader=_CanonicalReader(),
        source_verifier=_Verifier(catalog),
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path / "artifacts"),
    )

    result = provider.produce(
        resolved_request_hash=digest("resolved-request-v2"),
        catalog=catalog,
        program=_canonical_program(),
        range_run_id="range_v2",
        day_run_id="day_v2",
        decision_trade_date=date(2026, 6, 3),
        request_ref=artifact_ref(HistoricalRangeArtifactKind.REQUEST, "request-v2"),
        included_symbols={"000001.SZ"},
        previous_marks_by_symbol={},
        predecessor_day_receipt_ref=None,
        decision_cutoff=datetime(2026, 6, 3, 15, tzinfo=UTC),
    )

    assert result.mark_set.marks[0].availability == "AVAILABLE"


def test_decision_mark_provider_closes_second_day_upstream_lineage_in_canonical_order(tmp_path: Path) -> None:
    catalog = _catalog()
    store = HistoricalRangeArtifactStore(root=tmp_path / "artifacts")
    provider = HistoricalRangeDecisionMarkProvider(
        reader=_Reader(),
        source_verifier=_Verifier(catalog),
        artifact_store=store,
    )
    request_ref = artifact_ref(HistoricalRangeArtifactKind.REQUEST, "request")
    predecessor_ref = artifact_ref(HistoricalRangeArtifactKind.DAY_RECEIPT, "predecessor")

    result = provider.produce(
        resolved_request_hash=digest("resolved-request"),
        catalog=catalog,
        program=frozen_program(research_spec()),
        range_run_id="range_1",
        day_run_id="day_2",
        decision_trade_date=date(2026, 6, 3),
        request_ref=request_ref,
        included_symbols={"000001.SZ"},
        previous_marks_by_symbol={},
        predecessor_day_receipt_ref=predecessor_ref,
        decision_cutoff=datetime(2026, 6, 3, 15, tzinfo=UTC),
    )

    envelope = store.load(result.artifact_ref)
    assert envelope.upstream_refs == tuple(
        sorted(
            (request_ref, predecessor_ref),
            key=lambda ref: (ref.artifact_kind.value, ref.semantic_content_hash, ref.relative_path),
        )
    )


def test_legal_no_quote_uses_previous_raw_mark_with_current_adjustment_factor() -> None:
    source_ref = HistoricalRangeSourceRevisionRefV1(revision_id="revision_1", revision_hash=digest("revision"))
    previous = HistoricalRangeEpisodeMarkV2(
        recommendation_anchor=Decimal("10"),
        current_raw_reference_yuan=Decimal("10"),
        current_adjustment_factor=Decimal("1"),
        current_normalized_mark=Decimal("10"),
        holding_trading_days=1,
        runup_bps=Decimal("0"),
        drawdown_bps=Decimal("0"),
        rank_classification="RETAINED",
        review_rank=1,
        review_score=Decimal("1"),
        weak_rank_confirmation_count=0,
        decision_cutoff=datetime(2026, 6, 2, 15, tzinfo=UTC),
        tradability_status="TRADABLE",
        mark_quality="T_CLOSE",
        source_evidence_hash=digest("previous"),
    )

    mark = HistoricalRangeDecisionMarkProvider._build_mark(
        symbol="000001.SZ",
        decision_trade_date=date(2026, 6, 3),
        market_row={"adj_factor": "1.3"},
        state_row={"suspended": True, "pit_eligible": True},
        previous_mark=previous,
        source_refs=(source_ref,),
        observed_at=datetime(2026, 7, 22, tzinfo=UTC),
        decision_cutoff=datetime(2026, 6, 3, 15, tzinfo=UTC),
    )

    assert mark.availability == "MARKET_STATE_NO_QUOTE"
    assert mark.mark_quality == "SUSPENDED_CARRY_FORWARD"
    assert mark.normalized_reference_mark == Decimal("13.0")


def _planned_mark_program() -> HistoricalRangeFrozenProgramV1:
    from backend.services.advisory_historical_range.task_input_sources import freeze_program_input_sources

    request, program, dates = _task("DATABASE_ONLY", dates=("2026-06-03",))
    result, = freeze_program_input_sources(request=request, date_plan=dates, frozen_programs=(program,))
    return result


def _produce_planned_marks(provider, program, catalog):
    return provider.produce(
        resolved_request_hash=digest("planned-request"), catalog=catalog, program=program,
        range_run_id="planned_range", day_run_id="planned_day",
        decision_trade_date=date(2026, 6, 3),
        request_ref=artifact_ref(HistoricalRangeArtifactKind.REQUEST, "planned-request"),
        included_symbols={"000001.SZ", "000002.SZ"}, previous_marks_by_symbol={},
        predecessor_day_receipt_ref=None, decision_cutoff=datetime(2026, 6, 3, 15, tzinfo=UTC),
    )


def test_planned_marks_use_frozen_native_pit_and_keep_outside_quotes(tmp_path, monkeypatch, authority):
    from backend.services.canonical_equity_pit import CanonicalPitAuthorityResolver

    program = _planned_mark_program()
    before = program.model_dump(mode="json")
    key = program.selection_input_source_plan["native_policy"]["universe_key"]
    catalog = _catalog(universe_key=key)
    verifier = _Verifier(catalog)

    class Reader:
        def read(self, *, decision_trade_date, universe_key):
            assert decision_trade_date == date(2026, 6, 3)
            assert universe_key == key == "aistock_equity_pit_canonical_v2"
            return (
                {symbol: {"ts_code": symbol, "close_li": 10000, "adj_factor": "1.2"}
                 for symbol in ("000001.SZ", "000002.SZ")},
                {"000001.SZ": {"pit_eligible": True}, "000002.SZ": {"pit_eligible": False}},
                datetime(2026, 7, 22, tzinfo=UTC),
            )

    monkeypatch.setattr(CanonicalPitAuthorityResolver, "resolve_live_binding",
                        lambda _: pytest.fail("mark restore resolved the active PIT pointer"))
    provider = HistoricalRangeDecisionMarkProvider(
        reader=Reader(), source_verifier=verifier,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path / "marks"),
    )
    result = _produce_planned_marks(provider, program, catalog)
    marks = {mark.symbol: mark for mark in result.mark_set.marks}
    assert marks["000001.SZ"].tradability_status == "TRADABLE"
    assert marks["000002.SZ"].tradability_status == "OUTSIDE_PIT_UNIVERSE_WITH_QUOTE"
    assert all(mark.normalized_reference_mark == Decimal("12.0") for mark in marks.values())
    assert verifier.calls == 2
    assert program.model_dump(mode="json") == before


@pytest.mark.parametrize("sealed_key,sealed_date", [
    (None, "2026-06-03"),
    ("shsz_st_pit_active_v1", "2026-06-03"),
    ("aistock_equity_pit_canonical_v2", "2026-06-02"),
])
def test_planned_marks_reject_wrong_sealed_pit_or_date_before_read_or_write(
    tmp_path, authority, sealed_key, sealed_date,
):
    program = _planned_mark_program()
    catalog = _catalog(universe_key=sealed_key, state_parameter_date=sealed_date)
    before = catalog.model_dump(mode="json")

    class NoReader:
        def read(self, **kwargs):
            pytest.fail("a mismatched frozen catalog reached the mark reader")

    root = tmp_path / "marks"
    provider = HistoricalRangeDecisionMarkProvider(
        reader=NoReader(), source_verifier=_Verifier(catalog),
        artifact_store=HistoricalRangeArtifactStore(root=root),
    )
    with pytest.raises(ValueError, match="differs from the sealed market-state source"):
        _produce_planned_marks(provider, program, catalog)
    assert not list(root.rglob("*.json"))
    assert catalog.model_dump(mode="json") == before
