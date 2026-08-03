from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from backend.services.advisory_historical_range.api_models import ExistingProgramInput
from backend.services.advisory_historical_range.canonical import canonical_json_sha256, canonicalize
from backend.services.advisory_modeling.base_snapshot import _resolve_multi_alpha_legs
from backend.services.advisory_modeling.batch_b import (
    BatchBDatasetMaterializationRequestV1,
    BatchBHistoricalRangeDriver,
)
from backend.services.advisory_modeling.errors import AdvisoryModelingError
from backend.services.advisory_modeling.feature_builder import frozen_formula_registry_v1
from backend.services.advisory_modeling.feature_schema import (
    REQUIRED_FORMULA_IDS,
    REQUIRED_QUERY_TEMPLATE_IDS,
    frozen_feature_schema_v1,
)
from backend.services.advisory_modeling.feature_sources import frozen_feature_query_registry_v1
from backend.services.advisory_modeling.label_policy import RankingLabelPolicyV1
from backend.services.advisory_modeling.market_regime import MarketRegimePolicyTemplateV1
from backend.services.advisory_modeling.style_profile import (
    SHORT_REBOUND_TARGET_PACKAGE_ID,
    StrategyStyleProfileV1,
)
from backend.services.advisory_modeling.training_view import DatasetBuildIntentV1


_COMMIT = "f20cd062285230a1e24829afd7386203891a2897"


def _profile() -> StrategyStyleProfileV1:
    return StrategyStyleProfileV1(
        profile_id="short-rebound-target-package-v1",
        profile_version="1",
        package_id=SHORT_REBOUND_TARGET_PACKAGE_ID,
        package_manifest_sha256="a" * 64,
        package_asset_closure_hash="b" * 64,
        selection_runtime_semantics_hash="c" * 64,
        effective_package_oos_cutoff=date(2026, 7, 1),
    )


def _intent() -> DatasetBuildIntentV1:
    profile = _profile()
    schema = frozen_feature_schema_v1()
    formulas = frozen_formula_registry_v1()
    queries = frozen_feature_query_registry_v1(repository_commit=_COMMIT)
    regime = MarketRegimePolicyTemplateV1()
    label = RankingLabelPolicyV1()
    return DatasetBuildIntentV1(
        style_profile_id=profile.profile_id,
        style_profile_hash=str(profile.profile_payload_sha256),
        package_id=profile.package_id,
        package_manifest_sha256=profile.package_manifest_sha256,
        package_asset_closure_hash=profile.package_asset_closure_hash,
        selection_runtime_semantics_hash=profile.selection_runtime_semantics_hash,
        multi_alpha_parent_contract_version="advisory_historical_range_candidate_component_lineage_v1",
        multi_alpha_component_identity_set_hash="d" * 64,
        decision_date_start=date(2020, 1, 1),
        decision_date_end=date(2026, 7, 1),
        feature_schema_id=schema.feature_schema_id,
        feature_schema_hash=str(schema.feature_schema_hash),
        feature_formula_registry_hash=str(formulas.registry_hash),
        feature_query_registry_hash=str(queries.registry_hash),
        market_regime_policy_template_id=regime.policy_template_id,
        market_regime_policy_template_hash=str(regime.policy_template_hash),
        label_policy_id=label.label_policy_id,
        label_policy_hash=str(label.label_policy_hash),
        calendar_version="cn-trading-v1",
        calendar_hash="e" * 64,
        repository_commit=_COMMIT,
        final_fit_as_of=datetime(2026, 7, 31, 23, 59, tzinfo=UTC),
    )


def test_batch_b_request_requires_exact_existing_program_and_profile_closure() -> None:
    request = BatchBDatasetMaterializationRequestV1(
        dataset_intent=_intent(),
        style_profile=_profile(),
        existing_program=ExistingProgramInput(
            source_kind="EXISTING_PROGRAM",
            program_id="advprog_short_rebound",
            expected_program_version=7,
            expected_binding_version_id="advbind_short_rebound_v7",
        ),
    )

    assert request.request_hash
    assert request.existing_program.expected_program_version == 7

    payload = request.model_dump(mode="python")
    payload["dataset_intent"] = {
        **payload["dataset_intent"],
        "package_manifest_sha256": "9" * 64,
        "intent_semantic_hash": None,
    }
    payload["request_hash"] = None
    with pytest.raises(ValidationError, match="differs from style profile"):
        BatchBDatasetMaterializationRequestV1.model_validate(payload)


def test_dataset_intent_derives_dynamic_source_and_universe_authority_after_seal() -> None:
    intent = _intent()
    request = intent.finalize(
        source_revision_set_id="advsrs_composite_v1",
        source_revision_set_hash="1" * 64,
        universe_policy_set_id="advups_composite_v1",
        universe_policy_set_hash="2" * 64,
    )

    assert request.source_revision_set_hash == "1" * 64
    assert request.universe_policy_set_hash == "2" * 64
    assert request.request_semantic_hash

    guarded = intent.model_copy(update={"expected_source_revision_set_hash": "3" * 64})
    with pytest.raises(ValueError, match="derived source revision"):
        guarded.finalize(
            source_revision_set_id="advsrs_composite_v1",
            source_revision_set_hash="1" * 64,
            universe_policy_set_id="advups_composite_v1",
            universe_policy_set_hash="2" * 64,
        )


def test_feature_contracts_close_all_frozen_formulas_queries_and_regime_fields() -> None:
    formulas = frozen_formula_registry_v1()
    schema = frozen_feature_schema_v1()
    queries = frozen_feature_query_registry_v1(repository_commit=_COMMIT)
    names = {item.name for item in schema.definitions}

    assert tuple(item.formula_id for item in formulas.formulas) == REQUIRED_FORMULA_IDS
    assert tuple(item.query_template_id for item in queries.templates) == REQUIRED_QUERY_TEMPLATE_IDS
    assert MarketRegimePolicyTemplateV1().return_feature_id in names
    assert MarketRegimePolicyTemplateV1().breadth_feature_id in names
    assert all(item.formula_id in REQUIRED_FORMULA_IDS for item in schema.definitions)

    market_sql = next(
        item.sql_bytes_base64
        for item in queries.templates
        if item.query_template_id == "historical_market_history_window"
    )
    import base64

    decoded = base64.b64decode(market_sql).decode("utf-8")
    assert "span.eligible_start <= price.trade_date" in decoded
    assert "span.eligible_end >= price.trade_date" in decoded
    state_sql = base64.b64decode(
        next(
            item.sql_bytes_base64
            for item in queries.templates
            if item.query_template_id == "historical_decision_mark_market_state"
        )
    ).decode("utf-8")
    assert "basic.list_status" not in state_sql
    assert "basic.delist_date <= cal.cal_date" in state_sql


class _HistoricalService:
    def __init__(self) -> None:
        self.create_request = None
        self.refresh_request = None
        self.bridge_request = None

    def create_batch(self, request, **_kwargs):
        self.create_request = request
        return {"data": {"batch": {"batch_id": "batch-1"}}}

    @staticmethod
    def get_batch(_batch_id):
        return {"batch_id": "batch-1", "status": "COMPLETED", "row_version": 11}

    @staticmethod
    def list_runs(_batch_id, **_kwargs):
        return {"items": [{"range_run_id": "run-1", "status": "COMPLETED"}]}

    @staticmethod
    def list_operations(_batch_id, **_kwargs):
        return {"items": []}

    def refresh_outcomes(self, _batch_id, request, **_kwargs):
        self.refresh_request = request
        return {"data": {"operation_id": "outcome-op"}}

    def build_dataset_bridge(self, _batch_id, request, **_kwargs):
        self.bridge_request = request
        return {"data": {"operation_id": "bridge-op"}}

    @staticmethod
    def get_operation(operation_id):
        if operation_id == "bridge-op":
            return {
                "operation_id": operation_id,
                "status": "COMPLETED",
                "bridge_receipt": {"sealed_snapshot_id": "snapshot-1"},
                "snapshot": {"snapshot_id": "snapshot-1", "status": "SEALED"},
            }
        return {"operation_id": operation_id, "status": "COMPLETED"}


def test_historical_driver_uses_exact_program_and_full_outcome_bridge_scope() -> None:
    service = _HistoricalService()
    request = BatchBDatasetMaterializationRequestV1(
        dataset_intent=_intent(),
        style_profile=_profile(),
        existing_program=ExistingProgramInput(
            source_kind="EXISTING_PROGRAM",
            program_id="advprog_short_rebound",
            expected_program_version=7,
            expected_binding_version_id="advbind_short_rebound_v7",
        ),
    )

    result = BatchBHistoricalRangeDriver(service=service).ensure_sealed_base(request=request)

    assert result.sealed_snapshot_id == "snapshot-1"
    assert service.create_request.program_specs == [request.existing_program]
    assert service.refresh_request.horizons == [1, 3, 5, 10, 20]
    assert service.refresh_request.expected_row_version == 11
    assert service.bridge_request.requested_horizons == [1, 3, 5, 10, 20]
    assert service.bridge_request.requested_maturity_statuses == ["COMPLETE", "TERMINAL"]


def test_historical_driver_reuses_completed_durable_operations_on_exact_retry() -> None:
    service = _HistoricalService()
    operations = (
        {
            "operation_id": "outcome-op",
            "operation_type": "REFRESH_OUTCOMES",
            "operation_idempotency_key": "unused",
            "status": "COMPLETED",
        },
        {
            "operation_id": "bridge-op",
            "operation_type": "BUILD_DATASET_BRIDGE",
            "operation_idempotency_key": "unused",
            "status": "COMPLETED",
        },
    )
    request = BatchBDatasetMaterializationRequestV1(
        dataset_intent=_intent(),
        style_profile=_profile(),
        existing_program=ExistingProgramInput(
            source_kind="EXISTING_PROGRAM",
            program_id="advprog_short_rebound",
            expected_program_version=7,
            expected_binding_version_id="advbind_short_rebound_v7",
        ),
    )
    key_prefix = f"adv-reranker-batch-b-{str(request.request_hash)[:32]}"
    operations[0]["operation_idempotency_key"] = f"{key_prefix}-outcomes"
    operations[1]["operation_idempotency_key"] = f"{key_prefix}-bridge"
    service.list_operations = lambda *_args, **_kwargs: {"items": list(operations)}
    service.refresh_outcomes = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("completed outcome operation must be reused")
    )
    service.build_dataset_bridge = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("completed bridge operation must be reused")
    )

    result = BatchBHistoricalRangeDriver(service=service).ensure_sealed_base(request=request)

    assert result.bridge_operation_id == "bridge-op"


def test_multi_alpha_leg_resolution_closes_order_membership_and_weight() -> None:
    components = [
        {"component_id": "leg-b", "weight": "0.4", "required_window": 20},
        {"component_id": "leg-a", "weight": "0.6", "required_window": 60},
    ]
    component_set_hash = canonical_json_sha256(
        tuple(
            canonicalize(item)
            for item in sorted(components, key=lambda value: value["component_id"])
        )
    )
    scores = {
        "leg-a": {"normalized_score": "0.8", "weight": "0.6"},
        "leg-b": {"normalized_score": "0.3", "weight": "0.4"},
    }

    legs = _resolve_multi_alpha_legs(
        components=components,
        raw_component_scores=scores,
        expected_component_set_hash=component_set_hash,
        symbol="000001.SZ",
    )

    assert tuple(item.component_id for item in legs) == ("leg-a", "leg-b")
    with pytest.raises(AdvisoryModelingError, match="does not close every admitted component"):
        _resolve_multi_alpha_legs(
            components=components,
            raw_component_scores={"leg-a": scores["leg-a"]},
            expected_component_set_hash=component_set_hash,
            symbol="000001.SZ",
        )
    with pytest.raises(AdvisoryModelingError, match="differs from the admitted component weight"):
        _resolve_multi_alpha_legs(
            components=components,
            raw_component_scores={
                **scores,
                "leg-b": {"normalized_score": "0.3", "weight": "0.5"},
            },
            expected_component_set_hash=component_set_hash,
            symbol="000001.SZ",
        )
