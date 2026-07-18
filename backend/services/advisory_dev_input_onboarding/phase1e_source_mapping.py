"""Compiled O4 logical-to-physical source mapping for Advisory Phase 1E."""

from __future__ import annotations

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisorySourceMappingRegistry,
    PartitionGranularity,
    STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_PAYLOAD,
    SourceBindParameter,
    SourceMappingEntry,
    SourcePhysicalRequirementMapping,
)
from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.source_observer import SOURCE_QUERY_TEMPLATES


REGISTRY_ID = "advisory_phase1e_source_mapping"
REGISTRY_VERSION = "v1"

_AUDIT_POLICY = {
    "policy_id": "dataset_refresh_audit_success_v1",
    "policy_version": "v1",
    "eligible_status": "success",
    "availability": "refreshed_at_lte_decision_cutoff",
}
_DERIVED_PIT_POLICY = {
    "policy_id": "st_pit_upstream_audit_and_ready_state_v1",
    "policy_version": "v1",
    "required_upstream": ["stock_basic", "stock_st_events", "trading_calendar"],
    "required_state": {"status": "ready", "dirty": False},
}
_CUTOFF_POLICY = {
    "predicate_id": "formal_available_at_lte_decision_cutoff_v1",
    "predicate_version": "v1",
}


def compiled_o4_source_mapping_registry() -> AdvisorySourceMappingRegistry:
    """Return the complete fixed registry for the five persisted DSE logical inputs."""

    entries = (
        _entry(
            role="pit_universe",
            dataset_id="market.stock_universe_pit",
            query_template_id="StockUniversePitService.get_eligible_codes",
            query_template_version="v1",
            physical=(
                _physical(
                    source_role="pit_universe",
                    template_id="market_stock_universe_pit_spans_as_of_v2",
                    granularity=PartitionGranularity.AS_OF_SNAPSHOT,
                    audit_policy=_DERIVED_PIT_POLICY,
                    business_window="decision_date_interval_membership_v1",
                ),
                _physical(
                    source_role="pit_universe_build_state",
                    template_id="market_stock_universe_pit_state_as_of_v2",
                    granularity=PartitionGranularity.AS_OF_SNAPSHOT,
                    audit_policy=_DERIVED_PIT_POLICY,
                    business_window="ready_state_covering_decision_date_v1",
                ),
                _physical(
                    source_role="pit_universe_stock_basic_upstream",
                    template_id="market_stock_basic_as_of_v2",
                    granularity=PartitionGranularity.AS_OF_SNAPSHOT,
                    audit_policy=_AUDIT_POLICY,
                    business_window="listed_at_or_before_decision_date_v1",
                ),
                _physical(
                    source_role="pit_universe_st_event_upstream",
                    template_id="market_stock_st_events_as_of_v2",
                    granularity=PartitionGranularity.AS_OF_SNAPSHOT,
                    audit_policy=_AUDIT_POLICY,
                    business_window="published_at_or_before_decision_date_v1",
                ),
            ),
        ),
        _entry(
            role="market_history",
            dataset_id="market.kline_daily_raw",
            query_template_id="get_history_window",
            query_template_version="v1",
            physical=(
                _physical(
                    source_role="market_history",
                    template_id="market_kline_daily_raw_trade_date_v2",
                    granularity=PartitionGranularity.DAILY,
                    audit_policy=_AUDIT_POLICY,
                    business_window="inclusive_trading_calendar_window_v1",
                ),
                _physical(
                    source_role="corporate_action",
                    template_id="market_adj_factor_trade_date_v1",
                    granularity=PartitionGranularity.DAILY,
                    audit_policy=_AUDIT_POLICY,
                    business_window="inclusive_trading_calendar_window_v1",
                ),
            ),
        ),
        _entry(
            role="fundamental_moneyflow",
            dataset_id="timescaledb.fundamental_moneyflow",
            query_template_id="timescaledb_adapter.fetch_fundamental_data_ts",
            query_template_version="v1",
            physical=(
                _physical("fundamental_daily_basic", "market_daily_basic_trade_date_v1"),
                _physical("fundamental_moneyflow", "market_moneyflow_ts_trade_date_v2"),
                _physical("fundamental_bak_basic", "market_bak_basic_trade_date_v2"),
                _physical(
                    source_role="fundamental_stock_basic",
                    template_id="market_stock_basic_as_of_v2",
                    granularity=PartitionGranularity.AS_OF_SNAPSHOT,
                    audit_policy=_AUDIT_POLICY,
                    business_window="listed_at_or_before_effective_date_v1",
                ),
                _physical("fundamental_cyq_perf", "market_cyq_perf_trade_date_v2"),
                _physical("fundamental_sector_data", "market_sector_data_trade_date_v2"),
            ),
        ),
        _entry(
            role="trading_calendar",
            dataset_id="market.trading_calendar",
            query_template_id="InferenceEngine.trade_date_and_window_resolution",
            query_template_version="v1",
            physical=(
                _physical(
                    source_role="trading_calendar",
                    template_id="market_trading_calendar_date_v2",
                    granularity=PartitionGranularity.DAILY,
                    audit_policy=_AUDIT_POLICY,
                    business_window="inclusive_trading_calendar_window_v1",
                ),
            ),
        ),
        _entry(
            role="reference_price",
            dataset_id="market.kline_daily_raw",
            query_template_id="SelectionArtifact.reference_price",
            query_template_version="v1",
            physical=(
                _physical(
                    source_role="reference_price",
                    template_id="market_kline_daily_raw_trade_date_v2",
                    granularity=PartitionGranularity.DAILY,
                    audit_policy=_AUDIT_POLICY,
                    business_window="exact_reference_trade_date_v1",
                ),
            ),
        ),
    )
    return AdvisorySourceMappingRegistry(
        registry_id=REGISTRY_ID,
        registry_version=REGISTRY_VERSION,
        entries=entries,
    )


def _entry(
    *,
    role: str,
    dataset_id: str,
    query_template_id: str,
    query_template_version: str,
    physical: tuple[SourcePhysicalRequirementMapping, ...],
) -> SourceMappingEntry:
    return SourceMappingEntry(
        dse_source_role=role,
        dse_dataset_id=dataset_id,
        dse_query_template_id=query_template_id,
        dse_query_template_version=query_template_version,
        physical_requirements=physical,
    )


def _physical(
    source_role: str,
    template_id: str,
    *,
    granularity: PartitionGranularity = PartitionGranularity.DAILY,
    audit_policy: dict | None = None,
    business_window: str = "inclusive_trading_calendar_window_v1",
) -> SourcePhysicalRequirementMapping:
    template = SOURCE_QUERY_TEMPLATES[template_id]
    policy = audit_policy or _AUDIT_POLICY
    partition_policy = {
        "mapper_id": (
            "exact_as_of_snapshot_v1"
            if granularity is PartitionGranularity.AS_OF_SNAPSHOT
            else "inclusive_trading_day_window_v1"
        ),
        "mapper_version": "v1",
        "template_id": template_id,
        "partition_parameter_name": template.partition_parameter_name,
    }
    bind_parameter_schema = (
        (
            SourceBindParameter(name="universe_key", data_type="text"),
            SourceBindParameter(name=template.partition_parameter_name, data_type="date"),
        )
        if str(policy["policy_id"]) == str(_DERIVED_PIT_POLICY["policy_id"])
        else (
            SourceBindParameter(
                name=template.partition_parameter_name,
                data_type="date",
            ),
        )
    )
    return SourcePhysicalRequirementMapping(
        source_role=source_role,
        dataset_name=f"{template.schema_name}.{template.table_name}",
        observer_query_template_id=template.template_id,
        observer_query_template_version=template.template_version,
        observer_query_template_hash=template.template_hash,
        audit_evidence_policy_id=str(policy["policy_id"]),
        audit_evidence_policy_version=str(policy["policy_version"]),
        audit_evidence_policy_hash=canonical_json_sha256(policy),
        partition_mapper_id=str(partition_policy["mapper_id"]),
        partition_mapper_version=str(partition_policy["mapper_version"]),
        partition_mapper_hash=canonical_json_sha256(partition_policy),
        partition_granularity=granularity,
        bind_parameter_schema=bind_parameter_schema,
        canonical_sort_columns=("row_payload",),
        capacity_date_column=(
            None if granularity is PartitionGranularity.AS_OF_SNAPSHOT else template.partition_parameter_name
        ),
        business_window_derivation=business_window,
        availability_requirement=(
            "upstream_audits_and_ready_state_at_or_before_cutoff_v1"
            if str(policy["policy_id"]) == str(_DERIVED_PIT_POLICY["policy_id"])
            else "successful_refresh_audit_at_or_before_cutoff_v1"
        ),
        cutoff_predicate_id=str(_CUTOFF_POLICY["predicate_id"]),
        cutoff_predicate_version=str(_CUTOFF_POLICY["predicate_version"]),
        cutoff_predicate_hash=canonical_json_sha256(_CUTOFF_POLICY),
    )


def expected_selection_logical_input_identities() -> tuple[tuple[str, str, str, str], ...]:
    """Expose the frozen provider contract identities for direct registry parity tests."""

    return tuple(
        (
            str(item["source_role"]),
            str(item["dataset_id"]),
            str(item["query_template_id"]),
            str(item["query_template_version"]),
        )
        for item in STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_PAYLOAD["logical_inputs"]
    )
