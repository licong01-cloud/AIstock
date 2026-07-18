from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    AdvisorySourceMappingRegistry,
    AdvisorySourceObservationScopeRequest,
    AdvisoryStrategyPackageInputProjectionV1,
    AlphaMode,
    ExpectedLogicalInput,
    O4ArtifactKind,
    O4_ARTIFACT_STORE_POLICY_HASH,
    PartitionGranularity,
    REASON_SOURCE_MAPPING_CONFLICT,
    RealDevOnboardingError,
    SourceBindParameter,
    SourceMappingEntry,
    SourcePhysicalRequirementMapping,
)
from backend.services.advisory_dev_input_onboarding.phase1e_inputs import Phase1EInputArtifactStore
from backend.services.advisory_dev_input_onboarding.phase1e_input_builder import (
    PersistedDseSourceReadReceipt,
    ProgramWindowLineage,
    build_pre_observation_scope,
    reconcile_dse_and_build_requirement_set,
)
from backend.services.advisory_dev_input_onboarding.phase1e_source_mapping import (
    compiled_o4_source_mapping_registry,
    expected_selection_logical_input_identities,
)
from backend.services.advisory_phase1.source_observer import SOURCE_QUERY_TEMPLATES
from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.strategy_package.advisory_input_projection import (
    SELECTION_QUERY_CONTRACT_HASH,
    StrategyPackageAdvisoryInputLegV1,
    StrategyPackageAdvisoryInputProjectionV1,
)
from backend.services.strategy_package.models import AlphaMode as PackageAlphaMode


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def _ref(kind: str, digest: str) -> AdvisoryImmutableArtifactRef:
    return AdvisoryImmutableArtifactRef(
        artifact_kind=kind,
        store_policy_hash=O4_ARTIFACT_STORE_POLICY_HASH,
        relative_path=f"advisory/phase1e/inputs/{kind}/{digest[:2]}/{digest}.json",
        semantic_hash=digest,
        file_sha256=digest,
    )


def _physical(dataset: str = "market.kline_daily_raw") -> SourcePhysicalRequirementMapping:
    return SourcePhysicalRequirementMapping(
        source_role="market_history",
        dataset_name=dataset,
        observer_query_template_id="market_kline_daily_raw_window_v2",
        observer_query_template_version="2",
        observer_query_template_hash=SHA_A,
        audit_evidence_policy_id="dataset_refresh_audit_success_v1",
        audit_evidence_policy_version="1",
        audit_evidence_policy_hash=SHA_B,
        partition_mapper_id="inclusive_trading_day_window_v1",
        partition_mapper_version="1",
        partition_mapper_hash=SHA_C,
        partition_granularity=PartitionGranularity.DAILY,
        bind_parameter_schema=(
            SourceBindParameter(name="window_start_trade_date", data_type="date"),
            SourceBindParameter(name="effective_trade_date", data_type="date"),
        ),
        canonical_sort_columns=("trade_date", "ts_code"),
        capacity_date_column="trade_date",
        business_window_derivation="inclusive_trading_calendar_window_v1",
        availability_requirement="successful_refresh_audit_at_or_before_cutoff_v1",
        cutoff_predicate_id="formal_available_at_lte_decision_cutoff_v1",
        cutoff_predicate_version="1",
        cutoff_predicate_hash=SHA_A,
    )


def _registry() -> AdvisorySourceMappingRegistry:
    return AdvisorySourceMappingRegistry(
        registry_id="advisory_real_dev_source_mapping",
        registry_version="1",
        entries=(
            SourceMappingEntry(
                dse_source_role="market_history",
                dse_dataset_id="market.kline_daily_raw",
                dse_query_template_id="strategy_package_live_inference_v2",
                dse_query_template_version="2",
                physical_requirements=(_physical(),),
            ),
        ),
    )


def test_mapping_registry_and_observation_scope_are_hash_closed_and_leg_specific() -> None:
    registry = _registry()
    registry_ref = _ref(O4ArtifactKind.SOURCE_MAPPING_REGISTRY.value, str(registry.registry_hash))
    projection_ref = _ref(O4ArtifactKind.STRATEGY_PACKAGE_INPUT_PROJECTION.value, SHA_A)
    query_ref = _ref("source_query_registry", SHA_B)
    window_ref = _ref("window_policy", SHA_C)
    scope = AdvisorySourceObservationScopeRequest(
        target_database_identity_hash=SHA_A,
        program_id="program_multi",
        decision_trade_date=date(2026, 7, 18),
        pit_universe_key="shsz_st_pit_active_v1",
        package_id="pkg_multi",
        manifest_sha256=SHA_B,
        alpha_mode=AlphaMode.MULTI,
        style_family="oversold_rebound",
        binding_version_id="binding_1",
        binding_payload_hash=SHA_C,
        selection_normalized_config_hash=SHA_A,
        strategy_package_input_projection_ref=projection_ref,
        strategy_package_input_projection_hash=SHA_A,
        source_mapping_registry_ref=registry_ref,
        source_mapping_registry_hash=str(registry.registry_hash),
        source_query_registry_ref=query_ref,
        source_query_registry_hash=SHA_B,
        window_policy_ref=window_ref,
        window_policy_hash=SHA_C,
        decision_cutoff_ts=datetime(2026, 7, 18, 7, 0, tzinfo=timezone.utc),
        expected_logical_inputs=(
            ExpectedLogicalInput(
                alpha_component_id="fundamental_leg",
                source_role="market_history",
                dataset_id="market.kline_daily_raw",
                query_template_id="strategy_package_live_inference_v2",
                query_template_version="2",
                expected_window_start_date=date(2026, 6, 1),
                effective_trade_date=date(2026, 7, 18),
                required_window=35,
                window_resolution="trading_day",
                expected_window_lineage_hash=SHA_A,
                physical_requirement_templates=(_physical(),),
            ),
            ExpectedLogicalInput(
                alpha_component_id="lstm_leg",
                source_role="market_history",
                dataset_id="market.kline_daily_raw",
                query_template_id="strategy_package_live_inference_v2",
                query_template_version="2",
                expected_window_start_date=date(2026, 4, 1),
                effective_trade_date=date(2026, 7, 18),
                required_window=75,
                window_resolution="trading_day",
                expected_window_lineage_hash=SHA_B,
                physical_requirement_templates=(_physical(),),
            ),
        ),
    )

    assert scope.observation_scope_hash is not None
    assert [item.alpha_component_id for item in scope.expected_logical_inputs] == ["fundamental_leg", "lstm_leg"]
    assert [item.required_window for item in scope.expected_logical_inputs] == [35, 75]


def test_observation_scope_rejects_ref_hash_drift() -> None:
    registry = _registry()
    with pytest.raises(ValidationError, match="source_mapping_registry ref semantic hash"):
        AdvisorySourceObservationScopeRequest(
            target_database_identity_hash=SHA_A,
            program_id="program_single",
            decision_trade_date=date(2026, 7, 18),
            pit_universe_key="shsz_st_pit_active_v1",
            package_id="pkg_single",
            manifest_sha256=SHA_B,
            alpha_mode=AlphaMode.SINGLE,
            style_family="trend",
            binding_version_id="binding_1",
            binding_payload_hash=SHA_C,
            selection_normalized_config_hash=SHA_A,
            strategy_package_input_projection_ref=_ref(
                O4ArtifactKind.STRATEGY_PACKAGE_INPUT_PROJECTION.value, SHA_A
            ),
            strategy_package_input_projection_hash=SHA_A,
            source_mapping_registry_ref=_ref(O4ArtifactKind.SOURCE_MAPPING_REGISTRY.value, SHA_C),
            source_mapping_registry_hash=str(registry.registry_hash),
            source_query_registry_ref=_ref("source_query_registry", SHA_B),
            source_query_registry_hash=SHA_B,
            window_policy_ref=_ref("window_policy", SHA_C),
            window_policy_hash=SHA_C,
            decision_cutoff_ts=datetime(2026, 7, 18, 7, 0, tzinfo=timezone.utc),
            expected_logical_inputs=(
                ExpectedLogicalInput(
                    source_role="market_history",
                    dataset_id="market.kline_daily_raw",
                    query_template_id="strategy_package_live_inference_v2",
                    query_template_version="2",
                    expected_window_start_date=date(2026, 6, 1),
                    effective_trade_date=date(2026, 7, 18),
                    required_window=35,
                    window_resolution="trading_day",
                    expected_window_lineage_hash=SHA_A,
                    physical_requirement_templates=(_physical(),),
                ),
            ),
        )


def test_o4_store_publishes_and_fully_reads_back_typed_registry(tmp_path) -> None:
    registry = _registry()
    store = Phase1EInputArtifactStore(root=tmp_path)
    first = store.publish(
        artifact_kind=O4ArtifactKind.SOURCE_MAPPING_REGISTRY,
        model=registry,
        semantic_hash=str(registry.registry_hash),
    )
    second = store.publish(
        artifact_kind=O4ArtifactKind.SOURCE_MAPPING_REGISTRY,
        model=registry,
        semantic_hash=str(registry.registry_hash),
    )

    assert first == second
    assert first.relative_path.startswith("advisory/phase1e/inputs/source_mapping_registry/")
    assert store.load(ref=first, model_type=AdvisorySourceMappingRegistry) == registry


def test_compiled_o4_registry_exactly_covers_all_five_selection_logical_inputs() -> None:
    registry = compiled_o4_source_mapping_registry()
    actual = {
        (
            item.dse_source_role,
            item.dse_dataset_id,
            item.dse_query_template_id,
            item.dse_query_template_version,
        )
        for item in registry.entries
    }

    assert actual == set(expected_selection_logical_input_identities())
    assert len(registry.entries) == 5
    assert all(item.physical_requirements for item in registry.entries)
    assert all("generic" not in item.observer_query_template_id.lower() for entry in registry.entries for item in entry.physical_requirements)


def test_compiled_o4_physical_templates_are_fixed_typed_parameter_selects() -> None:
    registry = compiled_o4_source_mapping_registry()
    template_ids = {
        item.observer_query_template_id
        for entry in registry.entries
        for item in entry.physical_requirements
    }

    for template_id in template_ids:
        template = SOURCE_QUERY_TEMPLATES[template_id]
        normalized = " ".join(template.sql.split()).upper()
        assert normalized.startswith("SELECT ")
        expected_parameter_count = 2 if "stock_universe_pit_" in template_id else 1
        assert template.sql.count("%s") == expected_parameter_count
        assert all(token not in normalized for token in (" INSERT ", " UPDATE ", " DELETE ", " TRUNCATE ", " FOR UPDATE"))


def _projection(*component_ids: str) -> AdvisoryStrategyPackageInputProjectionV1:
    legs = tuple(
        StrategyPackageAdvisoryInputLegV1(
            alpha_component_id=component_id,
            factor_order=("Factor_20D",),
            factor_order_hash=canonical_json_sha256(["Factor_20D"]),
            required_window=20,
            alpha158_alias_set_hash=canonical_json_sha256([]),
            dynamic_factor_ref_set_hash=canonical_json_sha256([f"factor_{component_id}"]),
        )
        for component_id in component_ids
    )
    shared = StrategyPackageAdvisoryInputProjectionV1(
        package_id="pkg_builder",
        manifest_sha256=SHA_A,
        alpha_mode=(PackageAlphaMode.MULTI_ALPHA if len(legs) > 1 else PackageAlphaMode.SINGLE_ALPHA),
        selection_query_contract_hash=SELECTION_QUERY_CONTRACT_HASH,
        legs=legs,
    )
    return AdvisoryStrategyPackageInputProjectionV1.model_validate(shared.model_dump(mode="json"))


def _lineage(component_id: str | None, *, end: date = date(2026, 7, 20)) -> ProgramWindowLineage:
    dates = tuple(end - timedelta(days=offset) for offset in reversed(range(20)))
    payload = {
        "window_start_date": dates[0],
        "required_window": 20,
        "window_resolution": "trading_day",
    }
    return ProgramWindowLineage(
        alpha_component_id=component_id,
        window_start_date=dates[0],
        effective_trade_date=end,
        required_window=20,
        window_resolution="trading_day",
        window_lineage_hash=canonical_json_sha256(payload),
        trading_dates=dates,
    )


def _built_scope(
    projection: AdvisoryStrategyPackageInputProjectionV1,
    lineages: tuple[ProgramWindowLineage, ...],
) -> AdvisorySourceObservationScopeRequest:
    registry = compiled_o4_source_mapping_registry()
    return build_pre_observation_scope(
        target_database_identity_hash=SHA_A,
        program_id="program_builder",
        decision_trade_date=date(2026, 7, 20),
        pit_universe_key="shsz_st_pit_active_v1",
        style_family="trend",
        binding_version_id="binding_builder",
        binding_payload_hash=SHA_B,
        selection_normalized_config_hash=SHA_C,
        projection=projection,
        projection_ref=_ref(O4ArtifactKind.STRATEGY_PACKAGE_INPUT_PROJECTION.value, str(projection.projection_hash)),
        mapping_registry=registry,
        mapping_registry_ref=_ref(O4ArtifactKind.SOURCE_MAPPING_REGISTRY.value, str(registry.registry_hash)),
        source_query_registry_ref=_ref("source_query_registry", SHA_A),
        source_query_registry_hash=SHA_A,
        window_policy_ref=_ref("window_policy", SHA_B),
        window_policy_hash=SHA_B,
        decision_cutoff_ts=datetime(2026, 7, 20, 8, 0, tzinfo=timezone.utc),
        window_lineages=lineages,
    )


def test_pre_observation_builder_uses_single_and_native_multi_leg_cardinality() -> None:
    single = _built_scope(_projection("single"), (_lineage(None),))
    multi = _built_scope(
        _projection("leg_a", "leg_b"),
        (_lineage("leg_a"), _lineage("leg_b")),
    )

    assert len(single.expected_logical_inputs) == 5
    assert len(multi.expected_logical_inputs) == 9
    assert sum(item.source_role == "reference_price" for item in multi.expected_logical_inputs) == 1
    assert {
        item.alpha_component_id
        for item in multi.expected_logical_inputs
        if item.source_role != "reference_price"
    } == {"leg_a", "leg_b"}


def test_pre_observation_rejects_non_authoritative_pit_universe_key() -> None:
    registry = compiled_o4_source_mapping_registry()
    projection = _projection("single")
    with pytest.raises(RealDevOnboardingError, match="differs from the frozen Selection provider contract") as error:
        build_pre_observation_scope(
            target_database_identity_hash=SHA_A,
            program_id="program_builder",
            decision_trade_date=date(2026, 7, 20),
            pit_universe_key="qe_backtest_universe",
            style_family="trend",
            binding_version_id="binding_builder",
            binding_payload_hash=SHA_B,
            selection_normalized_config_hash=SHA_C,
            projection=projection,
            projection_ref=_ref(
                O4ArtifactKind.STRATEGY_PACKAGE_INPUT_PROJECTION.value,
                str(projection.projection_hash),
            ),
            mapping_registry=registry,
            mapping_registry_ref=_ref(
                O4ArtifactKind.SOURCE_MAPPING_REGISTRY.value,
                str(registry.registry_hash),
            ),
            source_query_registry_ref=_ref("source_query_registry", SHA_A),
            source_query_registry_hash=SHA_A,
            window_policy_ref=_ref("window_policy", SHA_B),
            window_policy_hash=SHA_B,
            decision_cutoff_ts=datetime(2026, 7, 20, 8, 0, tzinfo=timezone.utc),
            window_lineages=(_lineage(None),),
        )
    assert error.value.reason_code == REASON_SOURCE_MAPPING_CONFLICT

def test_dse_reconciliation_expands_exact_daily_and_as_of_physical_requirements() -> None:
    lineage = _lineage(None)
    scope = _built_scope(_projection("single"), (lineage,))
    receipts = tuple(
        PersistedDseSourceReadReceipt(
            source_role=item.source_role,
            dataset_id=item.dataset_id,
            partition_ref=(
                f"{item.expected_window_start_date.isoformat()}:{item.effective_trade_date.isoformat()}"
                if item.source_role in {"market_history", "fundamental_moneyflow", "trading_calendar"}
                else item.effective_trade_date.isoformat()
            ),
            query_template_id=item.query_template_id,
            query_template_version=item.query_template_version,
            parameter_hash=(
                canonical_json_sha256(
                    {"trade_date": item.effective_trade_date.isoformat(), "ensure": True}
                )
                if item.source_role == "pit_universe"
                else SHA_B
            ),
            row_count=1,
            content_hash=SHA_A,
            first_observed_at=datetime(2026, 7, 20, 7, 0, tzinfo=timezone.utc),
            admissibility="PROSPECTIVE_FIRST_OBSERVED",
            leg_id=item.alpha_component_id,
        )
        for item in scope.expected_logical_inputs
    )
    scope_ref = _ref(O4ArtifactKind.SOURCE_OBSERVATION_SCOPE_REQUEST.value, str(scope.observation_scope_hash))

    requirements = reconcile_dse_and_build_requirement_set(
        observation_scope=scope,
        observation_scope_ref=scope_ref,
        dse_evidence_hash=SHA_B,
        selection_artifact_hash=SHA_C,
        source_receipts=receipts,
        window_lineages=(lineage,),
    )

    pit_requirements = tuple(
        item
        for item in requirements.physical_requirements
        if item.source_role in {"pit_universe", "pit_universe_build_state"}
    )
    assert len(pit_requirements) == 2
    assert all(item.partition_key["universe_key"] == "shsz_st_pit_active_v1" for item in pit_requirements)
    assert all(item.partition_key["as_of_date"] == "2026-07-20" for item in pit_requirements)

    assert len(requirements.physical_requirements) == 166
    assert {item.partition_granularity for item in requirements.physical_requirements} == {
        PartitionGranularity.DAILY,
        PartitionGranularity.AS_OF_SNAPSHOT,
    }


def test_dse_reconciliation_rejects_query_drift_without_backfill() -> None:
    lineage = _lineage(None)
    scope = _built_scope(_projection("single"), (lineage,))
    item = scope.expected_logical_inputs[0]
    receipt = PersistedDseSourceReadReceipt(
        source_role=item.source_role,
        dataset_id=item.dataset_id,
        partition_ref=item.effective_trade_date.isoformat(),
        query_template_id="unexpected_query",
        query_template_version=item.query_template_version,
        parameter_hash=SHA_B,
        row_count=1,
        content_hash=SHA_A,
        first_observed_at=datetime(2026, 7, 20, 7, 0, tzinfo=timezone.utc),
        admissibility="PROSPECTIVE_FIRST_OBSERVED",
        leg_id=item.alpha_component_id,
    )

    with pytest.raises(RealDevOnboardingError, match="differ from the pre-observation logical scope") as error:
        reconcile_dse_and_build_requirement_set(
            observation_scope=scope,
            observation_scope_ref=_ref(
                O4ArtifactKind.SOURCE_OBSERVATION_SCOPE_REQUEST.value,
                str(scope.observation_scope_hash),
            ),
            dse_evidence_hash=SHA_B,
            selection_artifact_hash=SHA_C,
            source_receipts=(receipt,),
            window_lineages=(lineage,),
        )
    assert error.value.reason_code == REASON_SOURCE_MAPPING_CONFLICT


def test_dse_reconciliation_rejects_pit_parameter_hash_drift() -> None:
    lineage = _lineage(None)
    scope = _built_scope(_projection("single"), (lineage,))
    receipts = []
    for item in scope.expected_logical_inputs:
        receipts.append(
            PersistedDseSourceReadReceipt(
                source_role=item.source_role,
                dataset_id=item.dataset_id,
                partition_ref=(
                    f"{item.expected_window_start_date.isoformat()}:{item.effective_trade_date.isoformat()}"
                    if item.source_role in {"market_history", "fundamental_moneyflow", "trading_calendar"}
                    else item.effective_trade_date.isoformat()
                ),
                query_template_id=item.query_template_id,
                query_template_version=item.query_template_version,
                parameter_hash=SHA_C if item.source_role == "pit_universe" else SHA_B,
                row_count=1,
                content_hash=SHA_A,
                first_observed_at=datetime(2026, 7, 20, 7, 0, tzinfo=timezone.utc),
                admissibility="PROSPECTIVE_FIRST_OBSERVED",
                leg_id=item.alpha_component_id,
            )
        )

    with pytest.raises(RealDevOnboardingError, match="PIT parameter hash differs") as error:
        reconcile_dse_and_build_requirement_set(
            observation_scope=scope,
            observation_scope_ref=_ref(
                O4ArtifactKind.SOURCE_OBSERVATION_SCOPE_REQUEST.value,
                str(scope.observation_scope_hash),
            ),
            dse_evidence_hash=SHA_B,
            selection_artifact_hash=SHA_C,
            source_receipts=tuple(receipts),
            window_lineages=(lineage,),
        )
    assert error.value.reason_code == REASON_SOURCE_MAPPING_CONFLICT

