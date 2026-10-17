from __future__ import annotations

import pytest

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_phase0b.audit_service import Phase0BMetricEngine, SignalContext
from backend.services.advisory_phase0b.errors import (
    Phase0BAuditError,
    REASON_METRIC_REGISTRY_CONFLICT,
)
from backend.services.advisory_phase0b.snapshot_reader import Phase0BTargetProgramBindingV1
from backend.services.advisory_phase1.label_policy import Projection
from backend.tests.advisory_phase0b.test_contracts import _request


def _candidate_context(
    *,
    signal_id: str,
    symbol: str,
    rank: int,
    stage_evidence_id: str,
    universe_policy_hash: str = "c" * 64,
) -> SignalContext:
    return SignalContext(
        snapshot_id="snapshot-1",
        signal_id=signal_id,
        canonical_signal_scope_hash=canonical_json_sha256(signal_id),
        universe_policy_hash=universe_policy_hash,
        market_regime_at_t=None,
        market_regime_evidence_hash=None,
        candidates_by_stage={
            "alpha_raw": (
                {
                    "stage_evidence_id": stage_evidence_id,
                    "membership_status": "INCLUDED",
                    "symbol": symbol,
                    "rank": rank,
                },
            )
        },
        stage_capability_by_stage={"alpha_raw": "FULL"},
        outcomes_by_stage_symbol={
            (stage_evidence_id, symbol, "RETURN_NET_ABSOLUTE", 1): {
                "label_version_id": f"label-{signal_id}",
                "projection_value_decimal": "0.01",
                "maturity_status": "MATURED",
                "outcome_event_status": "TERMINAL",
            }
        },
        universe_outcomes=(),
    )


def _merge(*contexts: SignalContext) -> SignalContext:
    return Phase0BMetricEngine._merge_date_contexts(
        decision_date="2026-07-01",
        contexts=contexts,
    )[0]


def test_date_context_merges_candidates_before_topk_without_collapsing_rank_gaps() -> None:
    merged = _merge(
        _candidate_context(
            signal_id="signal-3",
            symbol="000003.SZ",
            rank=3,
            stage_evidence_id="stage-3",
        ),
        _candidate_context(
            signal_id="signal-1",
            symbol="000001.SZ",
            rank=1,
            stage_evidence_id="stage-1",
        ),
    )

    outcomes = Phase0BMetricEngine._candidate_outcomes(
        context=merged,
        stage="alpha_raw",
        projection=Projection.RETURN_NET_ABSOLUTE,
        horizon=1,
    )

    assert [(item.symbol, item.rank) for item in outcomes] == [
        ("000001.SZ", 1),
        ("000003.SZ", 3),
    ]


def test_date_context_rejects_conflicting_universe_identity() -> None:
    with pytest.raises(Phase0BAuditError, match="conflicting snapshot or universe"):
        _merge(
            _candidate_context(
                signal_id="signal-1",
                symbol="000001.SZ",
                rank=1,
                stage_evidence_id="stage-1",
            ),
            _candidate_context(
                signal_id="signal-2",
                symbol="000002.SZ",
                rank=2,
                stage_evidence_id="stage-2",
                universe_policy_hash="d" * 64,
            ),
        )


def test_metric_engine_fails_closed_before_reading_an_incomplete_registry() -> None:
    request = _request()

    with pytest.raises(Phase0BAuditError) as captured:
        Phase0BMetricEngine().evaluate_target(
            request=request,
            target=request.audit_targets[0],
            program_binding=Phase0BTargetProgramBindingV1(
                target_hash=str(request.audit_targets[0].target_hash),
                range_program_hash="9" * 64,
            ),
            spool=object(),  # type: ignore[arg-type]
        )

    assert captured.value.reason_code == REASON_METRIC_REGISTRY_CONFLICT
