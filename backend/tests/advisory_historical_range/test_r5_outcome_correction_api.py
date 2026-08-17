from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range.api_models import (
    HistoricalRangeRefreshOutcomesRequest,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeOutcomeRevisionReason,
)
from backend.services.advisory_historical_range.runtime_factories import (
    HistoricalRangeR5DerivedIdentities,
    HistoricalRangeR5OutcomeRequestFactory,
)


def _ref(char: str) -> HistoricalRangeArtifactRefV1:
    digest = char * 64
    return HistoricalRangeArtifactRefV1(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        relative_path=f"requests/{digest}.json",
        producer_contract_version="test_v1",
        payload_schema_version="test_v1",
        semantic_content_hash=digest,
        payload_sha256=digest,
        file_sha256=digest,
    )


def _factory() -> HistoricalRangeR5OutcomeRequestFactory:
    resolution = SimpleNamespace(
        bundle=SimpleNamespace(horizons=(1, 3), policy_bundle_hash="a" * 64),
        bundle_ref=_ref("a"),
    )
    registry = SimpleNamespace(resolve=lambda **_kwargs: (("run-1", resolution),))
    identities = HistoricalRangeR5DerivedIdentities(
        outcome_producer_hash="b" * 64,
        summary_producer_hash="c" * 64,
        bridge_builder_hash="d" * 64,
        bridge_writer_hash="e" * 64,
        code_commit="f" * 40,
    )
    return HistoricalRangeR5OutcomeRequestFactory(
        policy_registry=registry,
        identities=identities,
    )


def test_r5_outcome_factory_passes_exact_calculation_correction_scope() -> None:
    evidence_ref = _ref("f")
    request = HistoricalRangeRefreshOutcomesRequest(
        operation_idempotency_key="correction-key",
        expected_row_version=7,
        label_as_of_trade_date="2026-08-14",
        range_run_ids=["run-1"],
        horizons=[1, 3],
        requested_outcome_logical_ids=["logical-a", "logical-b"],
        correction_reason=HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION,
        correction_evidence_ref=evidence_ref,
    )

    domain = _factory().build("batch-1", request).requests[0]

    assert domain.requested_outcome_logical_ids == ("logical-a", "logical-b")
    assert domain.correction_reason is HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION
    assert domain.correction_evidence_ref == evidence_ref


def test_r5_outcome_api_rejects_correction_without_immutable_evidence() -> None:
    with pytest.raises(ValueError, match="correction reason/evidence"):
        HistoricalRangeRefreshOutcomesRequest(
            operation_idempotency_key="correction-key",
            expected_row_version=7,
            label_as_of_trade_date="2026-08-14",
            range_run_ids=["run-1"],
            horizons=[1],
            correction_reason=HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION,
        )
