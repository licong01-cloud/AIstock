from __future__ import annotations

from datetime import date

import pytest

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_UNIVERSE_KEY,
    LEGACY_PIT_RULE_VERSION,
    LEGACY_PIT_UNIVERSE_KEY,
    PitAuthorityStatus,
    PitConsumerBinding,
    canonical_rule_parameters_digest,
    legacy_rule_parameters_digest,
)
from backend.services.hmm_risk.canonical_stock_fact_source import (
    CanonicalRollingStockFactSource,
    require_same_stock_fact_lease,
)
from backend.services.hmm_risk.state_model_set import StateModelSetError


def _binding(*, generation: int = 7, canonical: bool = True) -> PitConsumerBinding:
    return PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=(
            PitAuthorityStatus.ACTIVE_CANONICAL
            if canonical
            else PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION
        ),
        universe_key=CANONICAL_PIT_UNIVERSE_KEY if canonical else LEGACY_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION if canonical else LEGACY_PIT_RULE_VERSION,
        rule_parameters_digest=(
            canonical_rule_parameters_digest() if canonical else legacy_rule_parameters_digest()
        ),
        activation_generation=generation,
        activation_envelope_digest="a" * 64,
        expected_source_commit="b" * 40,
        coverage_start=date(2018, 8, 1),
        coverage_end=date(2026, 7, 31),
    )


class _Resolver:
    def __init__(self, binding: PitConsumerBinding) -> None:
        self.binding = binding

    def resolve_live_binding(self) -> PitConsumerBinding:
        return self.binding


class _Reader:
    def __init__(self, _conn, spec, **_kwargs) -> None:
        self.spec = spec

    def validate_source(self):
        return {"universe_key": self.spec.universe_key}

    def iter_stock_fact_rows(self):
        yield {"symbol": "600000.SH", "trade_date": date(2026, 7, 31)}

    def iter_missing_price_rows(self):
        return iter(())

    def iter_mapping_source_rows(self):
        yield {"symbol": "600000.SH", "trade_date": date(2026, 7, 31)}


def _source(resolver: _Resolver) -> CanonicalRollingStockFactSource:
    return CanonicalRollingStockFactSource(
        object(),
        source_start=date(2026, 7, 1),
        source_end=date(2026, 7, 31),
        security_identity_manifest=object(),
        provider_absence_manifest=object(),
        authority_resolver=resolver,
        reader_factory=_Reader,
    )


def test_online_source_derives_reader_identity_only_from_canonical_resolver() -> None:
    resolver = _Resolver(_binding())
    source = _source(resolver)

    state = source.validate_source()
    row = next(source.iter_stock_fact_rows())

    assert state["universe_key"] == CANONICAL_PIT_UNIVERSE_KEY
    assert state["canonical_pit_authority"] == source.lease.as_dict()
    assert row["canonical_pit_authority"]["activation_generation"] == 7
    assert require_same_stock_fact_lease(source.lease, row["canonical_pit_authority"]) == source.lease


def test_online_source_fails_closed_when_activation_generation_drifts() -> None:
    resolver = _Resolver(_binding(generation=7))
    source = _source(resolver)
    resolver.binding = _binding(generation=8)

    with pytest.raises(StateModelSetError, match="generation_drift"):
        source.validate_source()
    with pytest.raises(StateModelSetError, match="generation_drift"):
        next(source.iter_stock_fact_rows())


def test_online_source_fails_closed_when_generation_drifts_during_stream() -> None:
    resolver = _Resolver(_binding(generation=7))
    source = _source(resolver)
    rows = source.iter_stock_fact_rows()

    assert next(rows)["canonical_pit_authority"]["activation_generation"] == 7
    resolver.binding = _binding(generation=8)

    with pytest.raises(StateModelSetError, match="generation_drift"):
        next(rows)


def test_online_source_rejects_legacy_migration_pointer_as_canonical() -> None:
    with pytest.raises(StateModelSetError, match="binding_invalid"):
        _source(_Resolver(_binding(canonical=False)))


def test_online_source_wraps_authority_transport_failure_as_typed_unavailable() -> None:
    class _FailingResolver:
        @staticmethod
        def resolve_live_binding():
            raise ConnectionError("database unavailable")

    with pytest.raises(StateModelSetError, match="authority_unavailable.*ConnectionError"):
        _source(_FailingResolver())
