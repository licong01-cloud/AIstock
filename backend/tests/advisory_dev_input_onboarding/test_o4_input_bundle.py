from __future__ import annotations

from datetime import date, datetime, timezone

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    AdvisorySourceMappingRegistry,
    AdvisorySourceObservationScopeRequest,
    AdvisorySourceResolutionArtifact,
    AdvisorySourceRequirementRegistry,
    ArtifactStorePolicyArtifact,
    AggregateInputReadiness,
    AlphaMode,
    ExpectedLogicalInput,
    HistoricalProgramStatus,
    CalendarIdentityArtifact,
    ObserverConfigArtifact,
    O4ArtifactKind,
    O4_ARTIFACT_STORE_POLICY_HASH,
    O4_ARTIFACT_STORE_POLICY_PAYLOAD,
    PartitionGranularity,
    PartitionPolicyArtifact,
    Phase0APolicyRegistryArtifact,
    Phase1ECompileAggregateStatus,
    Phase1ECompileProgramResult,
    Phase1ECompileProgramStatus,
    Phase1ECompileReceipt,
    Phase1EProgramCompilerDependency,
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
    SourceQueryRegistryArtifact,
    SourceMappingEntry,
    SourcePartitionRequirement,
    SourcePhysicalRequirementMapping,
    StoreBackendPolicyArtifact,
)
from backend.services.advisory_phase0a.handoff import audit_request_identity_payload
from backend.services.advisory_phase0a.models import (
    AuditDateRange,
    AuditReceipt,
    AuditRequest,
    AuditTarget,
    ExpectedAlphaMode,
    HandoffReadiness,
    HandoffReadinessReport,
)
from backend.services.advisory_phase0a.policy import canonical_json_sha256
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
from backend.services.advisory_phase1.source_ledger import (
    InMemorySourceAvailabilityLedger,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
)
from backend.services.advisory_phase1.source_resolution import (
    FixtureSourceRevisionResolver,
    SourceRequirement,
    SourceRequirementSet,
    build_source_requirement_common_pit_identity_hash,
)
from backend.services.advisory_phase1.source_revision import AvailabilityRequirement, SourceRevisionKind
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


def test_all_twenty_five_o4_artifact_kinds_publish_with_exact_program_hierarchy(tmp_path) -> None:
    store = Phase1EInputArtifactStore(root=tmp_path)

    typed_artifacts = (
        (O4ArtifactKind.PHASE0A_POLICY_REGISTRY, Phase0APolicyRegistryArtifact(payload={"policy": "phase0a-v1"})),
        (O4ArtifactKind.SOURCE_QUERY_REGISTRY, SourceQueryRegistryArtifact(payload={"queries": ["market_history"]})),
        (O4ArtifactKind.OBSERVER_CONFIG, ObserverConfigArtifact(payload={"config_id": "observer-dev-v1"})),
        (O4ArtifactKind.CALENDAR_IDENTITY, CalendarIdentityArtifact(payload={"calendar": "sse-szse-v1"})),
        (O4ArtifactKind.PARTITION_POLICY, PartitionPolicyArtifact(payload={"partition": "trade-date-v1"})),
        (O4ArtifactKind.STORE_BACKEND_POLICY, StoreBackendPolicyArtifact(payload={"store": "advisory-dev-v1"})),
        (
            O4ArtifactKind.ARTIFACT_STORE_POLICY,
            ArtifactStorePolicyArtifact(payload=O4_ARTIFACT_STORE_POLICY_PAYLOAD),
        ),
    )
    typed_refs = {
        kind: store.publish(artifact_kind=kind, model=model, semantic_hash=str(model.content_hash))
        for kind, model in typed_artifacts
    }

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

    audit_target = AuditTarget(
        audit_target_id="target_o4",
        program_id="program_o4",
        package_id="pkg_o4_closure",
        manifest_sha256=SHA_A,
        expected_alpha_mode=ExpectedAlphaMode.SINGLE_ALPHA,
        decision_date_range=AuditDateRange(start_date=date(2026, 7, 18), end_date=date(2026, 7, 18)),
        decision_dates=[date(2026, 7, 18)],
        style_family="trend",
        requested_capabilities=[
            "candidate_authority",
            "hmm_vintage",
            "oos_classification",
            "runtime_semantics",
            "source_availability",
        ],
        audit_policy_version="phase0a-v1",
    )
    audit_request = AuditRequest(
        audit_id="audit_o4",
        policy_registry_id="phase0a-policy",
        audit_policy_version="phase0a-v1",
        policy_registry_content_hash=typed_refs[O4ArtifactKind.PHASE0A_POLICY_REGISTRY].semantic_hash,
        targets=[audit_target],
    )
    audit_request_hash = canonical_json_sha256(audit_request_identity_payload(audit_request))
    compiler_dependency = Phase1EProgramCompilerDependency(
        program_id="program_o4",
        decision_trade_date=date(2026, 7, 18),
        package_id="pkg_o4_closure",
        manifest_sha256=SHA_A,
        alpha_mode=AlphaMode.SINGLE,
        style_family="trend",
        historical_program_run_id="run_o4",
        historical_batch_receipt_ref=_external_ref("historical_batch_receipt", SHA_C),
        historical_batch_receipt_hash=SHA_C,
        phase0a_audit_request=audit_request,
        phase0a_audit_receipt=AuditReceipt(
            audit_id=audit_request.audit_id,
            audit_policy_version=audit_request.audit_policy_version,
            request_hash=audit_request_hash,
            audit_manifest_hash=SHA_B,
            result_hash=SHA_C,
            results=[],
        ),
        handoff_readiness_report=HandoffReadinessReport(
            audit_id=audit_request.audit_id,
            audit_manifest_hash=SHA_B,
            request_hash=audit_request_hash,
            readiness=HandoffReadiness.READY,
            handoff_readiness_hash=SHA_A,
        ),
        phase0a_policy_registry_ref=typed_refs[O4ArtifactKind.PHASE0A_POLICY_REGISTRY],
        phase0a_policy_registry_hash=typed_refs[O4ArtifactKind.PHASE0A_POLICY_REGISTRY].semantic_hash,
        source_query_registry_ref=typed_refs[O4ArtifactKind.SOURCE_QUERY_REGISTRY],
        source_query_registry_hash=typed_refs[O4ArtifactKind.SOURCE_QUERY_REGISTRY].semantic_hash,
        observer_config_ref=typed_refs[O4ArtifactKind.OBSERVER_CONFIG],
        observer_config_hash=typed_refs[O4ArtifactKind.OBSERVER_CONFIG].semantic_hash,
        calendar_identity_ref=typed_refs[O4ArtifactKind.CALENDAR_IDENTITY],
        calendar_identity_hash=typed_refs[O4ArtifactKind.CALENDAR_IDENTITY].semantic_hash,
        dataset_schema_fingerprint="schema-v1",
        partition_policy_ref=typed_refs[O4ArtifactKind.PARTITION_POLICY],
        partition_policy_hash=typed_refs[O4ArtifactKind.PARTITION_POLICY].semantic_hash,
        store_backend_policy_ref=typed_refs[O4ArtifactKind.STORE_BACKEND_POLICY],
        store_backend_policy_hash=typed_refs[O4ArtifactKind.STORE_BACKEND_POLICY].semantic_hash,
        artifact_store_policy_ref=typed_refs[O4ArtifactKind.ARTIFACT_STORE_POLICY],
        artifact_store_policy_hash=typed_refs[O4ArtifactKind.ARTIFACT_STORE_POLICY].semantic_hash,
        compiler_version="phase1e-v1",
        serializer_version="canonical-json-v1",
        compiler_source_hash=SHA_A,
    )
    compiler_dependency_ref = store.publish(
        artifact_kind=O4ArtifactKind.PROGRAM_COMPILER_DEPENDENCY,
        model=compiler_dependency,
        semantic_hash=str(compiler_dependency.dependency_hash),
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
                compiler_dependency_ref=compiler_dependency_ref,
                compiler_dependency_hash=str(compiler_dependency.dependency_hash),
            ),
        ),
        source_mapping_registry_ref=mapping_ref,
        source_mapping_registry_hash=str(mapping.registry_hash),
        capacity_policy_ref=_external_ref(O4ArtifactKind.CAPACITY_POLICY.value, SHA_A),
        capacity_policy_hash=SHA_A,
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
        source_query_registry_ref=typed_refs[O4ArtifactKind.SOURCE_QUERY_REGISTRY],
        source_query_registry_hash=typed_refs[O4ArtifactKind.SOURCE_QUERY_REGISTRY].semantic_hash,
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
        source_query_registry_hash=typed_refs[O4ArtifactKind.SOURCE_QUERY_REGISTRY].semantic_hash,
        program_requirement_sets=(requirement_set,),
    )
    requirement_registry_ref = store.publish(
        artifact_kind=O4ArtifactKind.SOURCE_REQUIREMENT_REGISTRY,
        model=requirement_registry,
        semantic_hash=str(requirement_registry.registry_hash),
    )
    cutoff = datetime(2026, 7, 18, 7, 0, tzinfo=timezone.utc)
    common_pit_hash = build_source_requirement_common_pit_identity_hash(
        admission_scope_id="scope-o4",
        admission_scope_hash=SHA_A,
        handoff_readiness_hash=SHA_B,
        program_id="program_o4",
        binding_version_id="binding_o4",
        package_id="pkg_o4_closure",
        manifest_sha256=SHA_A,
        alpha_mode="single_alpha",
        decision_as_of_trade_date=date(2026, 7, 18),
        requested_source_cutoff=cutoff,
        query_registry_hash=typed_refs[O4ArtifactKind.SOURCE_QUERY_REGISTRY].semantic_hash,
        calendar_hash=typed_refs[O4ArtifactKind.CALENDAR_IDENTITY].semantic_hash,
        universe_policy_hash=SHA_C,
        data_source="DB_HISTORICAL",
        execution_origin="MANUAL_HISTORICAL_RESEARCH",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        research_only=True,
    )
    bound_parameters = {"trade_date": "2026-07-18"}
    phase1_requirement = SourceRequirement(
        consumer_scope_id="scope-o4:single:market_history",
        source_role="market_history",
        dataset_name="market.kline_daily_raw",
        query_template_id="market_kline_daily_raw_window_v2",
        query_template_version="2",
        query_template_hash=SHA_A,
        bound_parameters=bound_parameters,
        bound_parameter_hash=canonical_json_sha256(bound_parameters),
        partition_key=bound_parameters,
        revision_kind=SourceRevisionKind.PARTITION_CONTENT_HASH,
        availability_requirement=AvailabilityRequirement.DECISION_CUTOFF,
        business_min_date=date(2026, 7, 18),
        business_max_date=date(2026, 7, 18),
        requested_cutoff=cutoff,
        enforced_cutoff_predicate_hash=SHA_B,
        common_pit_identity_hash=common_pit_hash,
    )
    phase1_requirement_set = SourceRequirementSet(
        admission_scope_id="scope-o4",
        admission_scope_hash=SHA_A,
        handoff_readiness_hash=SHA_B,
        program_id="program_o4",
        binding_version_id="binding_o4",
        package_id="pkg_o4_closure",
        manifest_sha256=SHA_A,
        alpha_mode="single_alpha",
        decision_as_of_trade_date=date(2026, 7, 18),
        requested_source_cutoff=cutoff,
        label_as_of_ts=cutoff,
        query_registry_hash=typed_refs[O4ArtifactKind.SOURCE_QUERY_REGISTRY].semantic_hash,
        calendar_hash=typed_refs[O4ArtifactKind.CALENDAR_IDENTITY].semantic_hash,
        universe_policy_hash=SHA_C,
        formal_oos_status="RETROSPECTIVE_RESEARCH_ONLY",
        evidence_scope="RETROSPECTIVE_RESEARCH_ONLY",
        requirements=(phase1_requirement,),
    )
    ledger = InMemorySourceAvailabilityLedger(
        now_provider=lambda: datetime(2026, 7, 18, 6, 59, tzinfo=timezone.utc)
    )
    event = ledger.append(
        SourceAvailabilityEventRequest(
            dataset_name="market.kline_daily_raw",
            source_role="market_history",
            partition_key=bound_parameters,
            revision_id="revision-o4",
            event_revision_no=1,
            event_type=SourceAvailabilityEventType.INGESTED,
            schema_fingerprint="schema-v1",
            row_count=100,
            partition_content_hash=SHA_C,
            quality_status="PASS",
            created_by_service_principal="fixture-observer",
        )
    )
    source_resolution = FixtureSourceRevisionResolver().resolve(
        requirement_set=phase1_requirement_set,
        availability_events=(event,),
    )
    source_resolution_receipt = AdvisorySourceResolutionArtifact(
        program_id="program_o4",
        decision_trade_date=date(2026, 7, 18),
        physical_requirement_set_ref=requirement_set_ref,
        physical_requirement_set_hash=str(requirement_set.requirement_set_hash),
        phase1_requirement_set=phase1_requirement_set,
        resolution_receipt=source_resolution.receipt,
    )
    source_resolution_receipt_ref = store.publish(
        artifact_kind=O4ArtifactKind.SOURCE_RESOLUTION_RECEIPT,
        model=source_resolution_receipt,
        semantic_hash=str(source_resolution_receipt.artifact_hash),
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
        workload_scope="SOURCE_CAPTURE_ONLY",
        horizons=(),
        projection_count=0,
        stage_projection_factor=0,
        source_requirement_set_hash=str(requirement_set.requirement_set_hash),
    )
    workload_ref = store.publish(
        artifact_kind=O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD,
        model=workload,
        semantic_hash=str(workload.program_workload_hash),
    )
    capacity_request = build_capacity_request_v2(
        observer_config_ref=typed_refs[O4ArtifactKind.OBSERVER_CONFIG],
        query_registry_ref=typed_refs[O4ArtifactKind.SOURCE_QUERY_REGISTRY],
        capacity_policy_ref=policy_ref,
        capacity_policy=policy,
        as_of_ts=datetime(2026, 7, 18, 8, 0, tzinfo=timezone.utc),
        history_start_trade_date=date(2026, 1, 1),
        history_end_trade_date=date(2026, 7, 18),
        program_workloads=(workload,),
        store_root_ref=typed_refs[O4ArtifactKind.STORE_BACKEND_POLICY],
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
        compiler_dependency_ref=compiler_dependency_ref,
        compiler_dependency_hash=str(compiler_dependency.dependency_hash),
        source_requirement_set_ref=requirement_set_ref,
        source_requirement_set_hash=str(requirement_set.requirement_set_hash),
        source_resolution_receipt_ref=source_resolution_receipt_ref,
        source_resolution_receipt_hash=str(source_resolution_receipt.artifact_hash),
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
        query_registry_hash=compiler_dependency.source_query_registry_hash,
        calendar_hash=compiler_dependency.calendar_identity_hash,
        label_policy_bundle_hash=None,
        dataset_schema_fingerprint="schema-v1",
        partition_policy_hash=compiler_dependency.partition_policy_hash,
        store_backend_config_hash=compiler_dependency.store_backend_policy_hash,
        capacity_request_ref=capacity_request_ref.semantic_hash,
        capacity_receipt_ref=capacity_receipt_ref.semantic_hash,
        capacity_program_workload_hash=str(workload.program_workload_hash),
        capacity_program_coverage_hash=str(coverage.coverage_hash),
        compiler_version="phase1e-v1",
        serializer_version="canonical-json-v1",
        compiler_source_hash=SHA_A,
        artifact_store_policy_hash=compiler_dependency.artifact_store_policy_hash,
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
        capacity_policy_ref=policy_ref,
        capacity_policy_hash=str(policy.policy_hash),
        source_mapping_registry_ref=mapping_ref,
        source_mapping_registry_hash=str(mapping.registry_hash),
        source_requirement_registry_ref=requirement_registry_ref,
        source_requirement_registry_hash=str(requirement_registry.registry_hash),
        capacity_request_ref=capacity_request_ref,
        capacity_request_hash=str(capacity_request.request_hash),
        capacity_receipt_ref=capacity_receipt_ref,
        capacity_receipt_hash=str(capacity_receipt.receipt_hash),
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
    compile_receipt = Phase1ECompileReceipt(
        input_bundle_ref=bundle_ref,
        input_bundle_hash=str(bundle.input_bundle_hash),
        program_results=(
            Phase1ECompileProgramResult(
                program_id="program_o4",
                decision_trade_date=date(2026, 7, 18),
                status=Phase1ECompileProgramStatus.COMPLETE,
                phase1e_batch_request_ref=batch_request_ref,
                phase1e_batch_request_hash=str(batch_request.invocation_request_hash),
                plan_refs=(_external_ref("phase1e_plan", SHA_A),),
                batch_receipt_ref=_external_ref("phase1e_plan_batch_receipt", SHA_B),
                batch_receipt_hash=SHA_B,
            ),
        ),
        aggregate_status=Phase1ECompileAggregateStatus.COMPLETE,
    )
    compile_receipt_ref = store.publish(
        artifact_kind=O4ArtifactKind.PHASE1E_COMPILE_RECEIPT,
        model=compile_receipt,
        semantic_hash=str(compile_receipt.compile_receipt_hash),
    )

    refs = {
        build_ref,
        projection_ref,
        mapping_ref,
        observation_ref,
        requirement_registry_ref,
        requirement_set_ref,
        source_resolution_receipt_ref,
        policy_ref,
        capacity_request_ref,
        workload_ref,
        capacity_receipt_ref,
        coverage_ref,
        program_input_ref,
        bundle_ref,
        program_date_request_ref,
        batch_request_ref,
        compiler_dependency_ref,
        compile_receipt_ref,
        *typed_refs.values(),
    }
    assert {ref.artifact_kind for ref in refs} == {kind.value for kind in O4ArtifactKind}
