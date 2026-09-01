from __future__ import annotations

from datetime import date

import pytest

from backend.services.selection_center.models import SelectionCandidate
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_UNIVERSE_KEY,
    PitAuthorityStatus,
    PitConsumerBinding,
    canonical_rule_parameters_digest,
)
from backend.services.selection_center.canonical_pit_runtime import SelectionPitRuntimeLease
from backend.services.selection_center.risk_policy import (
    RiskDecision,
    StPitRiskDecisionProvider,
    StockRiskPolicyService,
)
from backend.services.selection_center.runtime_profile import RuntimeRiskPolicyProfile
from backend.services.stock_universe_pit_service import StockUniversePitError
from backend.services.trading_core.models import PositionLot


def _candidate(symbol: str, rank: int, score: float = 1.0) -> SelectionCandidate:
    return SelectionCandidate(
        symbol=symbol,
        score=score,
        rank=rank,
        target_weight=0.03,
        reference_price=10.0,
    )


def _position(symbol: str) -> PositionLot:
    return PositionLot(
        portfolio_id="paper_test",
        symbol=symbol,
        quantity=300,
        available_quantity=300,
        avg_cost=10.0,
        trade_date=date(2024, 1, 2),
    )


def test_risk_policy_candidate_contract_blocks_buys_and_backfills() -> None:
    service = StockRiskPolicyService(providers={})
    decisions = {
        "000001.SZ": RiskDecision(
            symbol="000001.SZ",
            can_buy=False,
            reason_codes=["st_pit_not_eligible"],
        )
    }

    selected, excluded = service.apply_to_candidates(
        candidates=[
            _candidate("000001.SZ", 1, 0.9),
            _candidate("000002.SZ", 2, 0.8),
            _candidate("000003.SZ", 3, 0.7),
        ],
        decisions=decisions,
        trade_date=date(2024, 1, 3),
        package_id="pkg_test",
        manifest_sha256="sha_test",
        top_k=2,
    )

    assert [item.symbol for item in selected[:2]] == ["000002.SZ", "000003.SZ"]
    assert excluded[0].symbol == "000001.SZ"
    assert excluded[0].reason == "risk_policy_block_buy"


def test_risk_policy_forced_exit_target_uses_same_decision_contract() -> None:
    service = StockRiskPolicyService(providers={})
    decisions = {
        "000001.SZ": RiskDecision(
            symbol="000001.SZ",
            can_buy=False,
            force_exit=True,
            position_target_override=0,
            reason_codes=["st_pit_not_eligible"],
        )
    }

    targets = service.forced_exit_targets(
        decisions=decisions,
        current_positions={"000001.SZ": _position("000001.SZ")},
        trade_date=date(2024, 1, 3),
        package_id="pkg_test",
        manifest_sha256="sha_test",
        existing_target_symbols=set(),
    )

    assert len(targets) == 1
    assert targets[0].symbol == "000001.SZ"
    assert targets[0].target_quantity == 0
    assert targets[0].reason == "risk_policy_forced_exit"
    assert targets[0].metadata["risk_policy"]["force_exit"] is True


def test_runtime_risk_policy_profile_accepts_future_score_overlay_shape() -> None:
    profile = RuntimeRiskPolicyProfile.model_validate(
        {
            "enabled": True,
            "providers": ["st_pit"],
            "hard_actions": ["block_buy", "force_exit"],
            "score_overlay": {
                "enabled": True,
                "negative_multiplier_floor": 0.8,
                "positive_multiplier_cap": 1.05,
            },
        }
    )

    assert profile.enabled is True
    assert profile.providers == ["st_pit"]
    assert profile.score_overlay.enabled is True


def test_runtime_risk_policy_profile_rejects_qe_backtest_pit_namespace() -> None:
    with pytest.raises(ValueError, match="live Selection/Paper/simulation"):
        RuntimeRiskPolicyProfile.model_validate(
            {
                "enabled": True,
                "providers": ["st_pit"],
                "st_universe_key": "shsz_st_pit_qe_dataset_contract_v1",
            }
        )


def test_st_pit_provider_rejects_qe_namespace_even_if_profile_validation_is_bypassed() -> None:
    profile = RuntimeRiskPolicyProfile.model_construct(
        enabled=True,
        providers=["st_pit"],
        st_universe_key="shsz_st_pit_qe_dataset_contract_v1",
        strict_data_ready=False,
        hard_actions=["block_buy", "force_exit"],
    )

    with pytest.raises(StockUniversePitError, match="authoritative rolling universe"):
        StPitRiskDecisionProvider().evaluate(
            symbols=["000001.SZ"],
            trade_date=date(2026, 7, 17),
            profile=profile,
        )


class _Cursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params = ()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return [
            (
                "000001.SZ",
                date(2025, 1, 1),
                date(2026, 7, 31),
                "IPO_WARMUP_COMPLETE",
                None,
                CANONICAL_PIT_RULE_VERSION,
                {},
            )
        ]


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self._cursor


def test_st_pit_provider_uses_frozen_canonical_generation_and_rule() -> None:
    cursor = _Cursor()
    lease = SelectionPitRuntimeLease.from_binding(
        PitConsumerBinding(
            authority_id=CANONICAL_PIT_AUTHORITY_ID,
            authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
            universe_key=CANONICAL_PIT_UNIVERSE_KEY,
            rule_version=CANONICAL_PIT_RULE_VERSION,
            rule_parameters_digest=canonical_rule_parameters_digest(),
            activation_generation=3,
            activation_envelope_digest="b" * 64,
            expected_source_commit="source-commit",
            state_source_digest="a" * 64,
            coverage_start=date(2018, 8, 1),
            coverage_end=date(2026, 7, 31),
        )
    )
    profile = RuntimeRiskPolicyProfile.model_validate(
        {
            "enabled": True,
            "providers": ["st_pit"],
            "canonical_pit_runtime_lease": lease.as_dict(),
        }
    )

    decisions = StPitRiskDecisionProvider(
        conn_factory=lambda: _Connection(cursor),
    ).evaluate(
        symbols=["000001.SZ", "000002.SZ"],
        trade_date=date(2026, 7, 31),
        profile=profile,
        current_positions={"000002.SZ": _position("000002.SZ")},
    )

    assert "rule_version = %s" in cursor.sql
    assert cursor.params[-1] == CANONICAL_PIT_RULE_VERSION
    assert cursor.params[0] == CANONICAL_PIT_UNIVERSE_KEY
    assert decisions["000001.SZ"].can_buy is True
    assert decisions["000001.SZ"].source_events[0]["activation_generation"] == 3
    assert decisions["000002.SZ"].can_buy is False
    assert decisions["000002.SZ"].force_exit is True
    assert decisions["000002.SZ"].source_events[0]["activation_generation"] == 3
