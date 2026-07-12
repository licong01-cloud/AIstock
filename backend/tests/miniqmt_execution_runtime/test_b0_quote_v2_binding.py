from __future__ import annotations

from datetime import date

import pytest

from backend.execution_algos.adaptive_is.contracts import ControlRevision
from backend.execution_algos.adaptive_is.reasons import QuoteContractError
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import (
    QUOTE_CONTROL_BINDING_KEY,
    QUOTE_CONTROL_BINDING_SCHEMA_VERSION,
    B0QuoteV2RevisionV1,
    ParentQuoteControlAssignmentV1,
    QuoteControlBindingV1,
)
from backend.services.qmt_strategy_ledger.tca_models import canonical_json_sha256
from backend.services.simulation_runtime.models import SimulationBrokerBackend
from backend.tests.simulation_runtime.test_target_rebalance_shared import _compiled_plan_for_bridge


def _sha(token: str) -> str:
    return token * 64


def _execution_policy() -> dict[str, object]:
    return {
        "quote_contract": {
            "schema_version": "miniqmt_quote_contract_policy_v2",
            "control_revision": "B0_QUOTE_V2",
            "required_capabilities": [
                "FIVE_LEVEL_DEPTH",
                "EXCHANGE_TIMESTAMP",
                "RAW_PRICE_BASIS",
                "DEPTH_UNIT_SHARES",
                "TRADABILITY",
                "CALENDAR",
            ],
            "max_receive_age_ms": 1000,
            "max_source_lag_ms": 1000,
            "max_exchange_age_ms": 1000,
            "max_negative_skew_ms": 100,
            "max_clock_age_divergence_ms": 100,
            "max_dependency_group_skew_ms": 1000,
            "auction_mode": "OBSERVE_ONLY",
        }
    }


def _revision() -> B0QuoteV2RevisionV1:
    return B0QuoteV2RevisionV1.build(
        execution_policy=_execution_policy(),
        execution_policy_version_id="policy-v1",
        execution_policy_sha256=_sha("a"),
        adapter_version="adapter-v1",
        adapter_sha256=_sha("b"),
        code_revision="code-v1",
        code_sha256=_sha("c"),
        evidence_schema_version="evidence-v1",
        evidence_schema_sha256=_sha("d"),
        benchmark_policy_version="benchmark-v1",
        mark_policy_version="mark-v1",
        markout_max_lag_ms=5000,
    )


def test_binding_exact_schema_selects_one_revision_without_copying_policy_or_approval() -> None:
    parsed = QuoteControlBindingV1.from_binding_config(
        {
            QUOTE_CONTROL_BINDING_KEY: {
                "schema_version": QUOTE_CONTROL_BINDING_SCHEMA_VERSION,
                "control_revision": "B0_QUOTE_V2",
            }
        }
    )

    assert parsed.control_revision is ControlRevision.B0_QUOTE_V2
    assert parsed.canonical_payload() == {
        "schema_version": QUOTE_CONTROL_BINDING_SCHEMA_VERSION,
        "control_revision": "B0_QUOTE_V2",
    }

    with pytest.raises(QuoteContractError):
        QuoteControlBindingV1.from_binding_config(
            {
                QUOTE_CONTROL_BINDING_KEY: {
                    "schema_version": QUOTE_CONTROL_BINDING_SCHEMA_VERSION,
                    "control_revision": "B0_QUOTE_V2",
                    "policy_sha256": _sha("a"),
                }
            }
        )


def test_legacy_binding_without_quote_control_is_legacy_only_and_invalid_explicit_value_never_falls_back() -> None:
    assert QuoteControlBindingV1.from_binding_config({}).control_revision is ControlRevision.LEGACY_B0

    with pytest.raises(QuoteContractError):
        QuoteControlBindingV1.from_binding_config(
            {
                QUOTE_CONTROL_BINDING_KEY: {
                    "schema_version": QUOTE_CONTROL_BINDING_SCHEMA_VERSION,
                    "control_revision": "",
                }
            }
        )


def test_revision_and_parent_assignment_hashes_are_deterministic_and_conflicts_fail_loud() -> None:
    first = _revision()
    second = _revision()
    assert first == second
    assert first.revision_id.startswith("b0qrev_")

    assignment = ParentQuoteControlAssignmentV1.build(
        binding_id="binding-1",
        binding_hash=_sha("e"),
        trade_date=date(2026, 7, 13),
        parent_intent_id="parent-1",
        control_revision=ControlRevision.B0_QUOTE_V2,
        revision=first,
    )
    assert assignment.canonical_payload()["assignment_sha256"]

    with pytest.raises(QuoteContractError):
        ParentQuoteControlAssignmentV1.build(
            binding_id="binding-1",
            binding_hash=_sha("e"),
            trade_date=date(2026, 7, 13),
            parent_intent_id="parent-1",
            control_revision=ControlRevision.B0_QUOTE_V2,
            revision=None,
        )


def test_plan_runtime_and_algo_readback_preserve_the_same_assignment() -> None:
    revision = _revision()
    assignment = ParentQuoteControlAssignmentV1.build(
        binding_id="binding-1",
        binding_hash=_sha("e"),
        trade_date=date(2026, 7, 13),
        parent_intent_id="parent-1",
        control_revision=ControlRevision.B0_QUOTE_V2,
        revision=revision,
    )
    plan_quote_control = {
        "binding": {
            "schema_version": QUOTE_CONTROL_BINDING_SCHEMA_VERSION,
            "control_revision": "B0_QUOTE_V2",
        },
        "revision": revision.canonical_payload(),
        "assignments": [assignment.canonical_payload()],
    }

    parsed = ParentQuoteControlAssignmentV1.from_plan_payload(
        plan_quote_control["assignments"][0],
        revision=revision,
    )

    assert parsed == assignment
    assert parsed.canonical_payload() == plan_quote_control["assignments"][0]


def test_historical_binding_without_quote_control_preserves_legacy_plan_identity() -> None:
    _release, _binding, plan = _compiled_plan_for_bridge(backend=SimulationBrokerBackend.MINIQMT_SIM)

    assert "quote_control" not in plan.plan_payload_json
    assert all("quote_control_assignment" not in intent.metadata for intent in plan.intents)
    assert canonical_json_sha256(plan.plan_payload_json) == plan.plan_hash
