from __future__ import annotations

from datetime import date, datetime, timezone

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    AdvisorySourceMappingRegistry,
    AdvisorySourceObservationScopeRequest,
    AdvisorySourceRequirementRegistry,
    AggregateInputReadiness,
    AlphaMode,
    ExpectedLogicalInput,
    HistoricalProgramStatus,
    O4ArtifactKind,
    O4_ARTIFACT_STORE_POLICY_HASH,
    PartitionGranularity,
    Phase1EProgramDateInput,
    Phase1EProgramInputUnit,
    Phase1ERealInputBuildRequest,
    Phase1ERealInputBundle,
    ProgramCapacityStatus,
    ProgramIdentityReadiness,
    ProgramPlanReadiness,
    ProgramSourceReadiness,
    ProgramSourceRequirementSet,
    SourceBindParameter,
    SourceMappingEntry,
    SourcePartitionRequirement,
    SourcePhysicalRequirementMapping,
)
from backend.services.advisory_dev_input_onboarding.phase1e_inputs import Phase1EInputArtifactStore
from backend.services.advisory_phase1.readiness_plan import (
    Phase1EProgramDateRequest,
    Phase1ERevalidationBatchRequest,
)
from backend.services.advisory_phase1.source_capacity import (
    Phase1ECapacityMeasurementsV2,
    Phase1ECapacityPolicyV1,
    Phase1EProgramCapacityWorkload,
    build_capacity_program_coverage_v1,
    build_capacity_receipt_v2,
    build_capacity_request_v2,
)
from backend.services.strategy_package.advisory_input_projection import project_advisory_inputs
from backend.services.strategy_package.models import (
    Alpha158SchemaAsset,
    AlphaCombinationPolicy,
    AlphaComponent,
    AlphaLineage,
    AlphaMode as PackageAlphaMode,
    BacktestSummary,
    FactorAsset,
    ModelAsset,
    RuntimeAssetManifest,
    SourceType,
    StrategyPackageManifest,
    StrategyPackageSource,
)


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def _external_ref(kind: str, digest: str) -> AdvisoryImmutableArtifactRef:
    return AdvisoryImmutableArtifactRef(
        artifact_kind=kind,
        store_policy_hash=O4_ARTIFACT_STORE_POLICY_HASH,
        relative_path=f"external/{kind}/{digest}.json",
        semantic_hash=digest,
        file_sha256=digest,
    )


def _physical() -> SourcePhysicalRequirementMapping:
    return SourcePhysicalRequirementMapping(
        source_role="market_history",
        dataset_name="market.kline_daily_raw",
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


def _manifest() -> StrategyPackageManifest:
    component = AlphaComponent(
        alpha_id="single",
        alpha_name="single",
        component_weight=1.0,
        factor_ids=["factor_a"],
        holding_period="5day",
        rebalance_frequency="1day",
        score_direction="higher_better",
        lineage=AlphaLineage(factor_artifact_refs=["factor_a"]),
    )
    return StrategyPackageManifest(
        package_id="pkg_o4_closure",
        package_name="o4_closure",
        source=StrategyPackageSource(source_type=SourceType.CANDIDATE_STRATEGY_PACKAGE, source_id="unit"),
        alpha_mode=PackageAlphaMode.SINGLE_ALPHA,
        alpha_components=[component],
        alpha_combination_policy=AlphaCombinationPolicy(method="identity", weights={"single": 1.0}),
        factor_set=[FactorAsset(factor_id="factor_a", factor_name="Trend_120D")],
        model_asset=ModelAsset(model_id="model_single"),
        runtime_assets=RuntimeAssetManifest(
            alpha158=Alpha158SchemaAsset(enabled=True, aliases=["ROC60"], alias_count=1)
        ),
        backtest_summary=BacktestSummary(ic=0.01),
        manifest_sha256=SHA_A,
    )


def _policy() -> Phase1ECapacityPolicyV1:
    return Phase1ECapacityPolicyV1(
        policy_id="phase1e_capacity",
        policy_version="1",
        retained_snapshot_count=3,
        concurrent_build_count=2,
        staging_copy_count=1,
        parquet_target_file_bytes=1024,
        memory_budget_bytes=1_000_000,
        worker_memory_overheads={
            "arrow_builder_bytes": 100,
            "hash_buffer_bytes": 100,
            "verifier_bytes": 100,
        },
        orphan_reserve_bytes=100,
        manifest_overhead_bytes_per_snapshot=50,
        parquet_measurement_snapshot_limit=5,
        parquet_measurement_file_limit=50,
    )


def test_all_fifteen_o4_artifact_kinds_publish_with_exact_program_hierarchy(tmp_path) -> None:
    store = Phase1EInputArtifactStore(root=tmp_path)

    mapping = AdvisorySourceMappingRegistry(
        registry_id="o4_mapping",
        registry_version="1",
        entries=(
            SourceMappingEntry(
                dse_source_role="market_history",
                dse_dataset_id="market.kline_daily_raw",
                dse_query_template_id="get_history_window",
                dse_query_template_version="v1",
                physical_requirements=(_physical(),),
            ),
        ),
    )
    mapping_ref = store.publish(
        artifact_kind=O4ArtifactKind.SOURCE_MAPPING_REGISTRY,
        model=mapping,
        semantic_hash=str(mapping.registry_hash),
    )

    build_request = Phase1ERealInputBuildRequest(
        historical_run_request_ref=_external_ref("historical_run_request", SHA_A),
        historical_run_request_hash=SHA_A,
        historical_run_receipt_ref=_external_ref("historical_run_receipt", SHA_B),
        historical_run_receipt_hash=SHA_B,
        target_database_identity_hash=SHA_C,
        target_package_asset_root_hash=SHA_A,
        program_dates=(
            Phase1EProgramDateInput(
                program_id="program_o4",
                decision_trade_date=date(2026, 7, 18),
                package_id="pkg_o4_closure",
                manifest_sha256=SHA_A,
                alpha_mode=AlphaMode.SINGLE,
                style_family="trend",
                historical_status=HistoricalProgramStatus.COMPLETE,
                historical_program_run_id="run_o4",
                historical_batch_receipt_ref=_external_ref("historical_batch_receipt", SHA_C),
                historical_batch_receipt_hash=SHA_C,
            ),
        ),
        phase0a_policy_registry_ref=_external_ref("phase0a_policy_registry", SHA_A),
        phase0a_policy_registry_hash=SHA_A,
        source_mapping_registry_ref=mapping_ref,
        source_mapping_registry_hash=str(mapping.registry_hash),
        source_query_registry_ref=_external_ref("source_query_registry", SHA_B),
        source_query_registry_hash=SHA_B,
        calendar_registry_ref=_external_ref("calendar_registry", SHA_C),
        calendar_registry_hash=SHA_C,
        label_policy_bundle_ref=_external_ref("label_policy_bundle", SHA_A),
        label_policy_bundle_hash=SHA_A,
        partition_policy_ref=_external_ref("partition_policy", SHA_B),
        partition_policy_hash=SHA_B,
        store_backend_policy_ref=_external_ref("store_backend_policy", SHA_C),
        store_backend_policy_hash=SHA_C,
        capacity_policy_ref=_external_ref(O4ArtifactKind.CAPACITY_POLICY.value, SHA_A),
        capacity_policy_hash=SHA_A,
        phase1e_artifact_store_policy_ref=_external_ref("phase1e_artifact_store_policy", SHA_B),
        phase1e_artifact_store_policy_hash=SHA_B,
        code_release_id="aa77463b",
        code_release_hash=SHA_C,
    )
    build_ref = store.publish(
        artifact_kind=O4ArtifactKind.REAL_INPUT_BUILD_REQUEST,
        model=build_request,
        semantic_hash=str(build_request.build_request_hash),
    )

    projection = project_advisory_inputs(_manifest())
    projection_ref = store.publish(
        artifact_kind=O4ArtifactKind.STRATEGY_PACKAGE_INPUT_PROJECTION,
        model=projection,
        semantic_hash=str(projection.projection_hash),
    )
    observation = AdvisorySourceObservationScopeRequest(
        target_database_identity_hash=SHA_C,
        program_id="program_o4",
        decision_trade_date=date(2026, 7, 18),
        pit_universe_key="shsz_st_pit_active_v1",
        package_id="pkg_o4_closure",
        manifest_sha256=SHA_A,
        alpha_mode=AlphaMode.SINGLE,
        style_family="trend",
        binding_version_id="binding_o4",
        binding_payload_hash=SHA_B,
        selection_normalized_config_hash=SHA_C,
        strategy_package_input_projection_ref=projection_ref,
        strategy_package_input_projection_hash=str(projection.projection_hash),
        source_mapping_registry_ref=mapping_ref,
        source_mapping_registry_hash=str(mapping.registry_hash),
        source_query_registry_ref=_external_ref("source_query_registry", SHA_B),
        source_query_registry_hash=SHA_B,
        window_policy_ref=_external_ref("window_policy", SHA_C),
        window_policy_hash=SHA_C,
        decision_cutoff_ts=datetime(2026, 7, 18, 7, 0, tzinfo=timezone.utc),
        expected_logical_inputs=(
            ExpectedLogicalInput(
                source_role="market_history",
                dataset_id="market.kline_daily_raw",
                query_template_id="get_history_window",
                query_template_version="v1",
                expected_window_start_date=date(2026, 1, 1),
                effective_trade_date=date(2026, 7, 18),
                required_window=120,
                window_resolution="trading_day",
                expected_window_lineage_hash=SHA_A,
                physical_requirement_templates=(_physical(),),
            ),
        ),
    )
    observation_ref = store.publish(
        artifact_kind=O4ArtifactKind.SOURCE_OBSERVATION_SCOPE_REQUEST,
        model=observation,
        semantic_hash=str(observation.observation_scope_hash),
    )

    requirement_set = ProgramSourceRequirementSet(
        program_id="program_o4",
        decision_trade_date=date(2026, 7, 18),
        observation_scope_ref=observation_ref,
        observation_scope_hash=str(observation.observation_scope_hash),
        dse_evidence_hash=SHA_B,
        selection_artifact_hash=SHA_C,
        physical_requirements=(
            SourcePartitionRequirement(
                source_role="market_history",
                dataset_name="market.kline_daily_raw",
                query_template_id="market_kline_daily_raw_window_v2",
                query_template_version="2",
                partition_granularity=PartitionGranularity.DAILY,
                partition_key={"trade_date": "2026-07-18"},
            ),
        ),
    )
    requirement_set_ref = store.publish(
        artifact_kind=O4ArtifactKind.SOURCE_REQUIREMENT_SET,
        model=requirement_set,
        semantic_hash=str(requirement_set.requirement_set_hash),
    )
    requirement_registry = AdvisorySourceRequirementRegistry(
        build_request_hash=str(build_request.build_request_hash),
        source_mapping_registry_hash=str(mapping.registry_hash),
        source_query_registry_hash=SHA_B,
        program_requirement_sets=(requirement_set,),
    )
    requirement_registry_ref = store.publish(
        artifact_kind=O4ArtifactKind.SOURCE_REQUIREMENT_REGISTRY,
        model=requirement_registry,
        semantic_hash=str(requirement_registry.registry_hash),
    )

    policy = _policy()
    policy_ref = store.publish(
        artifact_kind=O4ArtifactKind.CAPACITY_POLICY,
        model=policy,
        semantic_hash=str(policy.policy_hash),
    )
    workload = Phase1EProgramCapacityWorkload(
        program_id="program_o4",
        decision_trade_date=date(2026, 7, 18),
        style_family="trend",
        package_id="pkg_o4_closure",
        manifest_sha256=SHA_A,
        alpha_mode=AlphaMode.SINGLE,
        candidate_depth=5,
        input_universe_count=4200,
        horizons=(5, 10, 20),
        projection_count=3,
        stage_projection_factor=2,
        source_requirement_set_hash=str(requirement_set.requirement_set_hash),
    )
    workload_ref = store.publish(
        artifact_kind=O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD,
        model=workload,
        semantic_hash=str(workload.program_workload_hash),
    )
    capacity_request = build_capacity_request_v2(
        observer_config_ref=_external_ref("observer_config", SHA_A),
        query_registry_ref=_external_ref("source_query_registry", SHA_B),
        capacity_policy_ref=policy_ref,
        capacity_policy=policy,
        as_of_ts=datetime(2026, 7, 18, 8, 0, tzinfo=timezone.utc),
        history_start_trade_date=date(2026, 1, 1),
        history_end_trade_date=date(2026, 7, 18),
        program_workloads=(workload,),
        store_root_ref=_external_ref("store_backend_policy", SHA_C),
    )
    capacity_request_ref = store.publish(
        artifact_kind=O4ArtifactKind.CAPACITY_REQUEST,
        model=capacity_request,
        semantic_hash=str(capacity_request.request_hash),
    )
    capacity_receipt = build_capacity_receipt_v2(
        request=capacity_request,
        request_ref=capacity_request_ref,
        measurements=Phase1ECapacityMeasurementsV2(
            target_database_identity_hash=SHA_C,
            database_observed_at=datetime(2026, 7, 18, 8, 1, tzinfo=timezone.utc),
            database_version="PostgreSQL 16",
            source_coverage_summary={"program_count": 1},
            relation_size_summary={},
            row_distribution_summary={},
            observed_revision_multiplier_p50=1.0,
            observed_revision_multiplier_p95=1.1,
            observed_revision_multiplier_max=1.2,
            role_projection_summary={},
            parquet_measurement_summary={},
            db_transaction_budget_summary={},
            memory_budget_summary={},
            staging_store_summary={},
            durable_store_summary={},
            store_available_bytes=10_000,
            measured_program_workload_hashes=(str(workload.program_workload_hash),),
            missing_measurements_by_program_workload_hash={},
        ),
    )
    capacity_receipt_ref = store.publish(
        artifact_kind=O4ArtifactKind.CAPACITY_RECEIPT,
        model=capacity_receipt,
        semantic_hash=str(capacity_receipt.receipt_hash),
    )
    coverage = build_capacity_program_coverage_v1(
        request=capacity_request,
        request_ref=capacity_request_ref,
        receipt=capacity_receipt,
        receipt_ref=capacity_receipt_ref,
        workload=workload,
        workload_ref=workload_ref,
    )
    coverage_ref = store.publish(
        artifact_kind=O4ArtifactKind.CAPACITY_PROGRAM_COVERAGE,
        model=coverage,
        semantic_hash=str(coverage.coverage_hash),
    )

    program_date_request = Phase1EProgramDateRequest(
        program_id="program_o4",
        decision_trade_date=date(2026, 7, 18),
        expected_package_id="pkg_o4_closure",
        expected_manifest_sha256=SHA_A,
        expected_alpha_mode="single_alpha",
        expected_style_family="trend",
        historical_batch_receipt_ref="historical_batch_receipt_o4",
        label_as_of_ts=datetime(2026, 7, 19, 0, 0, tzinfo=timezone.utc),
    )
    program_date_request_ref = store.publish(
        artifact_kind=O4ArtifactKind.PHASE1E_PROGRAM_DATE_REQUEST,
        model=program_date_request,
        semantic_hash=str(program_date_request.program_date_request_hash),
    )
    program_input = Phase1EProgramInputUnit(
        program_id="program_o4",
        decision_trade_date=date(2026, 7, 18),
        package_id="pkg_o4_closure",
        manifest_sha256=SHA_A,
        alpha_mode=AlphaMode.SINGLE,
        style_family="trend",
        historical_program_run_ref=_external_ref("historical_program_run", SHA_A),
        historical_program_run_hash=SHA_A,
        phase0a_audit_ref=_external_ref("phase0a_audit", SHA_B),
        phase0a_audit_hash=SHA_B,
        handoff_readiness_ref=_external_ref("handoff_readiness", SHA_C),
        handoff_readiness_hash=SHA_C,
        handoff_bundle_ref=_external_ref("handoff_bundle", SHA_A),
        handoff_bundle_hash=SHA_A,
        source_requirement_set_ref=requirement_set_ref,
        source_requirement_set_hash=str(requirement_set.requirement_set_hash),
        source_resolution_receipt_ref=_external_ref("source_resolution_receipt", SHA_B),
        source_resolution_receipt_hash=SHA_B,
        capacity_program_workload_ref=workload_ref,
        capacity_program_workload_hash=str(workload.program_workload_hash),
        capacity_coverage_ref=coverage_ref,
        capacity_coverage_hash=str(coverage.coverage_hash),
        phase1e_program_date_request_ref=program_date_request_ref,
        phase1e_program_date_request_hash=str(program_date_request.program_date_request_hash),
        identity_readiness=ProgramIdentityReadiness.COMPLETE,
        source_readiness=ProgramSourceReadiness.READY,
        capacity_status=ProgramCapacityStatus.MEASURED,
        plan_readiness=ProgramPlanReadiness.FULL_READY,
    )
    program_input_ref = store.publish(
        artifact_kind=O4ArtifactKind.PROGRAM_INPUT,
        model=program_input,
        semantic_hash=str(program_input.program_input_hash),
    )

    batch_request = Phase1ERevalidationBatchRequest(
        program_dates=(program_date_request,),
        phase0a_policy_hash=SHA_A,
        source_requirement_registry_hash=str(requirement_registry.registry_hash),
        query_registry_hash=SHA_B,
        calendar_hash=SHA_C,
        label_policy_bundle_hash=SHA_A,
        dataset_schema_fingerprint="schema-v1",
        partition_policy_hash=SHA_B,
        store_backend_config_hash=SHA_C,
        capacity_request_ref=capacity_request_ref.relative_path,
        capacity_receipt_ref=capacity_receipt_ref.relative_path,
        compiler_version="phase1e-v1",
        serializer_version="canonical-json-v1",
        compiler_source_hash=SHA_A,
        artifact_store_policy_hash=O4_ARTIFACT_STORE_POLICY_HASH,
    )
    batch_request_ref = store.publish(
        artifact_kind=O4ArtifactKind.PHASE1E_BATCH_REQUEST,
        model=batch_request,
        semantic_hash=str(batch_request.invocation_request_hash),
    )
    bundle = Phase1ERealInputBundle(
        build_request_ref=build_ref,
        build_request_hash=str(build_request.build_request_hash),
        target_database_identity_hash=SHA_C,
        phase0a_policy_registry_ref=_external_ref("phase0a_policy_registry", SHA_A),
        phase0a_policy_registry_hash=SHA_A,
        source_query_registry_ref=_external_ref("source_query_registry", SHA_B),
        source_query_registry_hash=SHA_B,
        calendar_registry_ref=_external_ref("calendar_registry", SHA_C),
        calendar_registry_hash=SHA_C,
        label_policy_bundle_ref=_external_ref("label_policy_bundle", SHA_A),
        label_policy_bundle_hash=SHA_A,
        partition_policy_ref=_external_ref("partition_policy", SHA_B),
        partition_policy_hash=SHA_B,
        store_backend_policy_ref=_external_ref("store_backend_policy", SHA_C),
        store_backend_policy_hash=SHA_C,
        capacity_policy_ref=policy_ref,
        capacity_policy_hash=str(policy.policy_hash),
        phase1e_artifact_store_policy_ref=_external_ref("phase1e_artifact_store_policy", SHA_B),
        phase1e_artifact_store_policy_hash=SHA_B,
        source_mapping_registry_ref=mapping_ref,
        source_mapping_registry_hash=str(mapping.registry_hash),
        source_requirement_registry_ref=requirement_registry_ref,
        source_requirement_registry_hash=str(requirement_registry.registry_hash),
        capacity_request_ref=capacity_request_ref,
        capacity_request_hash=str(capacity_request.request_hash),
        capacity_receipt_ref=capacity_receipt_ref,
        capacity_receipt_hash=str(capacity_receipt.receipt_hash),
        phase1e_revalidation_batch_request_ref=batch_request_ref,
        phase1e_revalidation_batch_request_hash=str(batch_request.invocation_request_hash),
        program_inputs=(program_input,),
        counts_by_identity_readiness={"COMPLETE": 1},
        counts_by_source_readiness={"READY": 1},
        counts_by_capacity_status={"MEASURED": 1},
        counts_by_plan_readiness={"FULL_READY": 1},
        aggregate_readiness=AggregateInputReadiness.ALL_FULL_READY,
    )
    bundle_ref = store.publish(
        artifact_kind=O4ArtifactKind.INPUT_BUNDLE,
        model=bundle,
        semantic_hash=str(bundle.input_bundle_hash),
    )

    refs = {
        build_ref,
        projection_ref,
        mapping_ref,
        observation_ref,
        requirement_registry_ref,
        requirement_set_ref,
        policy_ref,
        capacity_request_ref,
        workload_ref,
        capacity_receipt_ref,
        coverage_ref,
        program_input_ref,
        bundle_ref,
        program_date_request_ref,
        batch_request_ref,
    }
    assert {ref.artifact_kind for ref in refs} == {kind.value for kind in O4ArtifactKind}
