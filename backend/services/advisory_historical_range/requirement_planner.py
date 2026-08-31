"""Deterministic source requirement DAG for Phase 1R historical inference."""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from datetime import date
from typing import Any

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeDatePlanV1,
    HistoricalRangeFrozenProgramV1,
    HistoricalRangeRequirementPurpose,
    HistoricalRangeResearchBatchRequestV1,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSourceRequirementV1,
    derive_prefixed_id,
    normalize_hmm_binding_metadata,
)
from backend.services.strategy_package.advisory_input_projection import (
    CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH,
    HISTORICAL_RANGE_QUERY_CONTRACT_HASH,
    SELECTION_PIT_UNIVERSE_KEY,
)
from backend.services.strategy_package.selection_computation import parse_selection_runtime_profile_for_computation


_QUERY_VERSION = "v1"


def _program_pit_universe_key(program: HistoricalRangeFrozenProgramV1) -> str:
    binding = program.admitted_package_projection.canonical_pit_binding
    if binding is None:
        return SELECTION_PIT_UNIVERSE_KEY
    return binding.frozen_universe_key


def _query_contract_hash(frozen_programs: tuple[HistoricalRangeFrozenProgramV1, ...]) -> str:
    canonical_flags = {
        program.admitted_package_projection.canonical_pit_binding is not None
        for program in frozen_programs
    }
    if len(canonical_flags) != 1:
        raise ValueError("historical range batch cannot mix legacy reproduction and canonical v2 packages")
    return (
        CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH
        if True in canonical_flags
        else HISTORICAL_RANGE_QUERY_CONTRACT_HASH
    )


class HistoricalRangeSourceRequirementPlanner:
    def build(
        self,
        *,
        request: HistoricalRangeResearchBatchRequestV1,
        date_plan: HistoricalRangeDatePlanV1,
        frozen_programs: tuple[HistoricalRangeFrozenProgramV1, ...],
        calendar_identity_hash: str,
        code_release_hash: str,
        code_release_manifest: Mapping[str, Any] | None = None,
    ) -> HistoricalRangeSourceRequirementPlanV1:
        requirements: dict[str, HistoricalRangeSourceRequirementV1] = {}
        release_manifest = dict(code_release_manifest or {"code_release_hash": code_release_hash})
        if code_release_manifest is not None and canonical_json_sha256(release_manifest) != code_release_hash:
            raise ValueError("code release manifest does not match code_release_hash")

        code_id = self._add(
            requirements,
            source_role="code_release",
            dataset_id="aistock.source_closure",
            query_template_id="frozen_artifact_identity",
            parameters={
                "content_hash": code_release_hash,
                "row_count": 1,
                "code_release_manifest": release_manifest,
            },
            partition_ref=f"code-release:{code_release_hash}",
            required_for=HistoricalRangeRequirementPurpose.REQUEST_SEAL,
            missing_reason_code="ADVISORY_HR_CODE_RELEASE_UNAVAILABLE",
        )
        package_requirement_ids: dict[str, str] = {}
        for program in frozen_programs:
            package_requirement_ids[program.package_id] = self._add(
                requirements,
                source_role="package_runtime_assets",
                dataset_id="strategy_pkg.package_manifest_assets",
                query_template_id="frozen_artifact_identity",
                parameters={
                    "content_hash": program.target_package_asset_root_hash,
                    "manifest_sha256": program.manifest_sha256,
                    "row_count": 1,
                },
                partition_ref=(
                    f"strategy-package:{program.package_id}:{program.package_version}:{program.manifest_sha256}"
                ),
                required_for=HistoricalRangeRequirementPurpose.REQUEST_SEAL,
                missing_reason_code="ADVISORY_HR_PACKAGE_RUNTIME_ASSET_UNAVAILABLE",
                package_id=program.package_id,
                dependencies=(code_id,),
            )

        for trade_date in date_plan.ordered_trade_dates:
            date_text = trade_date.isoformat()
            calendar_range_start = min(
                window.window_start_trade_date
                for warmup in date_plan.per_program_input_warmup_ranges.values()
                for component in warmup.components
                for window in component.day_windows
                if window.decision_trade_date == trade_date
            )
            calendar_id = self._add(
                requirements,
                source_role="trading_calendar",
                dataset_id="market.trading_calendar",
                query_template_id="historical_trading_calendar_window",
                parameters={
                    "range_start": calendar_range_start.isoformat(),
                    "trade_date": date_text,
                    "calendar_identity_hash": calendar_identity_hash,
                },
                partition_ref=f"trading-calendar:{calendar_range_start.isoformat()}:{date_text}",
                required_for=HistoricalRangeRequirementPurpose.DAY_EXECUTION,
                missing_reason_code="ADVISORY_HR_TRADING_CALENDAR_UNAVAILABLE",
                decision_trade_date=trade_date,
            )
            # Decision marks are one canonical market/state source per date.  They
            # deliberately do not inherit a package/component lookback or the
            # positive PIT universe used by candidate inference.
            self._add(
                requirements,
                source_role="decision_mark_daily_market",
                dataset_id="market.kline_daily_raw",
                query_template_id="historical_decision_mark_daily_market",
                parameters={
                    "trade_date": date_text,
                    "adjustment_basis": "corporate_action_normalized_from_raw",
                    "raw_price_unit": "yuan",
                },
                partition_ref=f"decision-mark-daily-market:{date_text}",
                required_for=HistoricalRangeRequirementPurpose.DAY_EXECUTION,
                missing_reason_code="ADVISORY_HR_DECISION_MARK_MARKET_UNAVAILABLE",
                decision_trade_date=trade_date,
                dependencies=(calendar_id,),
            )
            for program in frozen_programs:
                pit_universe_key = _program_pit_universe_key(program)
                universe_id = self._add(
                    requirements,
                    source_role="pit_universe",
                    dataset_id="market.stock_universe_pit",
                    query_template_id="historical_pit_universe_existing_readonly",
                    parameters={
                        "trade_date": date_text,
                        "universe_key": pit_universe_key,
                        "ensure": False,
                    },
                    partition_ref=f"{pit_universe_key}:{date_text}",
                    required_for=HistoricalRangeRequirementPurpose.DAY_EXECUTION,
                    missing_reason_code="ADVISORY_HR_PIT_UNIVERSE_UNAVAILABLE",
                    decision_trade_date=trade_date,
                )
                self._add(
                    requirements,
                    source_role="decision_mark_market_state",
                    dataset_id="market.decision_mark_market_state",
                    query_template_id="historical_decision_mark_market_state",
                    parameters={
                        "trade_date": date_text,
                        "universe_key": pit_universe_key,
                        "state_fields": ("suspend", "listing", "delist", "st", "pit_universe"),
                    },
                    partition_ref=(
                        f"decision-mark-market-state:{pit_universe_key}:{date_text}"
                    ),
                    required_for=HistoricalRangeRequirementPurpose.DAY_EXECUTION,
                    missing_reason_code="ADVISORY_HR_DECISION_MARK_STATE_UNAVAILABLE",
                    package_id=(
                        program.package_id
                        if program.admitted_package_projection.canonical_pit_binding is not None
                        else None
                    ),
                    decision_trade_date=trade_date,
                    dependencies=(calendar_id, universe_id),
                )
                runtime_profile = parse_selection_runtime_profile_for_computation(program.runtime_config)
                provider_dependencies = (universe_id, calendar_id, package_requirement_ids[program.package_id])
                if runtime_profile.risk_policy.enabled and "st_pit" in runtime_profile.risk_policy.providers:
                    self._add(
                        requirements,
                        source_role="st_risk",
                        dataset_id="market.stock_universe_pit_spans",
                        query_template_id="historical_st_risk_existing_readonly",
                        parameters={
                            "trade_date": date_text,
                            "universe_key": runtime_profile.risk_policy.st_universe_key,
                        },
                        partition_ref=f"st-risk:{runtime_profile.risk_policy.st_universe_key}:{date_text}",
                        required_for=HistoricalRangeRequirementPurpose.DAY_EXECUTION,
                        missing_reason_code="ADVISORY_HR_ST_RISK_UNAVAILABLE",
                        package_id=program.package_id,
                        decision_trade_date=trade_date,
                        dependencies=provider_dependencies,
                    )
                if runtime_profile.tradability.exclude_suspended:
                    self._add(
                        requirements,
                        source_role="suspend",
                        dataset_id="market.suspend_d",
                        query_template_id="historical_suspend_lookup",
                        parameters={
                            "trade_date": date_text,
                            "formal_partition_key": {"trade_date": date_text},
                        },
                        partition_ref=f"suspend:{date_text}",
                        required_for=HistoricalRangeRequirementPurpose.DAY_EXECUTION,
                        missing_reason_code="ADVISORY_HR_SUSPEND_DATA_UNAVAILABLE",
                        package_id=program.package_id,
                        decision_trade_date=trade_date,
                        dependencies=(calendar_id,),
                    )
                if runtime_profile.industry_blacklist:
                    self._add(
                        requirements,
                        source_role="industry",
                        dataset_id="market.sw_index_member",
                        query_template_id="historical_industry_membership",
                        parameters={"trade_date": date_text},
                        partition_ref=f"industry:{date_text}",
                        required_for=HistoricalRangeRequirementPurpose.DAY_EXECUTION,
                        missing_reason_code="ADVISORY_HR_INDUSTRY_DATA_UNAVAILABLE",
                        package_id=program.package_id,
                        decision_trade_date=trade_date,
                        dependencies=(calendar_id,),
                    )
                if runtime_profile.hmm.enabled:
                    metadata = _hmm_metadata_for_day(program.runtime_config, date_text)
                    selector = {
                        "schema_version": "advisory_hmm_frozen_evidence_selector_v1",
                        "research_program_id": program.research_program_id,
                        "package_id": program.package_id,
                        "decision_trade_date": date_text,
                        "model_config_id": runtime_profile.hmm.model_config_id,
                        "model_snapshot_id": runtime_profile.hmm.model_snapshot_id,
                        "signal_preset": runtime_profile.hmm.signal_preset,
                    }
                    selector = {key: value for key, value in selector.items() if value is not None}
                    self._add(
                        requirements,
                        source_role="hmm_frozen_evidence",
                        dataset_id="hmm.frozen_evidence_bundle",
                        query_template_id="historical_hmm_frozen_evidence_bundle",
                        parameters={
                            "selector": selector,
                            "phase0a_hmm_metadata": metadata or None,
                            "formal_partition_selector": {
                                "schema_version": "advisory_hmm_frozen_evidence_partition_v1",
                                "selector": selector,
                            },
                        },
                        partition_ref=(
                            f"hmm-frozen-evidence:{program.package_id}:"
                            f"{canonical_json_sha256(selector)[:24]}:{date_text}"
                        ),
                        required_for=HistoricalRangeRequirementPurpose.DAY_EXECUTION,
                        missing_reason_code="ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE",
                        package_id=program.package_id,
                        decision_trade_date=trade_date,
                        dependencies=provider_dependencies,
                    )
                warmup = date_plan.per_program_input_warmup_ranges[program.research_program_id]
                warmup_by_component = {item.component_id: item for item in warmup.components}
                for component in program.admitted_package_projection.components:
                    component_warmup = warmup_by_component[component.component_id]
                    start_by_date = {
                        item.decision_trade_date: item.window_start_trade_date
                        for item in component_warmup.day_windows
                    }
                    start_date = start_by_date[trade_date]
                    common = {
                        "start_date": start_date.isoformat(),
                        "trade_date": date_text,
                        "universe_key": pit_universe_key,
                        "factor_order_hash": canonical_json_sha256(list(component.factor_order)),
                        "lookback_contract_hash": component.lookback_contract_hash,
                        "required_window": component.required_window,
                        "buffer_trading_days": component.buffer_trading_days,
                        "window_resolution": component.window_resolution,
                    }
                    dependencies = (universe_id, calendar_id, package_requirement_ids[program.package_id])
                    self._add(
                        requirements,
                        source_role="market_history",
                        dataset_id="market.kline_daily_raw",
                        query_template_id="historical_market_history_window",
                        parameters=common,
                        partition_ref=(
                            f"market-history:{program.package_id}:{component.component_id}:"
                            f"{start_date.isoformat()}:{date_text}"
                        ),
                        required_for=HistoricalRangeRequirementPurpose.DAY_EXECUTION,
                        missing_reason_code="ADVISORY_HR_MARKET_HISTORY_UNAVAILABLE",
                        package_id=program.package_id,
                        component_id=component.component_id,
                        decision_trade_date=trade_date,
                        dependencies=dependencies,
                    )
                    self._add(
                        requirements,
                        source_role="fundamental_moneyflow",
                        dataset_id="timescaledb.fundamental_moneyflow",
                        query_template_id="historical_fundamental_moneyflow_window",
                        parameters=common,
                        partition_ref=(
                            f"fundamental-moneyflow:{program.package_id}:{component.component_id}:"
                            f"{start_date.isoformat()}:{date_text}"
                        ),
                        required_for=HistoricalRangeRequirementPurpose.DAY_EXECUTION,
                        missing_reason_code="ADVISORY_HR_FUNDAMENTAL_MONEYFLOW_UNAVAILABLE",
                        package_id=program.package_id,
                        component_id=component.component_id,
                        decision_trade_date=trade_date,
                        dependencies=dependencies,
                    )

        return HistoricalRangeSourceRequirementPlanV1(
            request=request,
            date_plan=date_plan,
            frozen_programs=frozen_programs,
            query_contract_hash=_query_contract_hash(frozen_programs),
            calendar_identity_hash=calendar_identity_hash,
            code_release_hash=code_release_hash,
            requirements=tuple(requirements.values()),
        )
    @staticmethod
    def _add(
        requirements: dict[str, HistoricalRangeSourceRequirementV1],
        *,
        source_role: str,
        dataset_id: str,
        query_template_id: str,
        parameters: dict[str, Any],
        partition_ref: str,
        required_for: HistoricalRangeRequirementPurpose,
        missing_reason_code: str,
        package_id: str | None = None,
        component_id: str | None = None,
        decision_trade_date: Any | None = None,
        dependencies: Iterable[str] = (),
    ) -> str:
        identity = {
            "source_role": source_role,
            "dataset_id": dataset_id,
            "query_template_id": query_template_id,
            "parameters": parameters,
            "partition_ref": partition_ref,
            "package_id": package_id,
            "component_id": component_id,
            "decision_trade_date": decision_trade_date,
        }
        requirement_id = derive_prefixed_id("ahrreq", identity, digest_chars=48)
        requirement = HistoricalRangeSourceRequirementV1(
            requirement_id=requirement_id,
            source_role=source_role,
            dataset_id=dataset_id,
            query_template_id=query_template_id,
            query_template_version=_QUERY_VERSION,
            query_template_hash=canonical_json_sha256(
                {
                    "query_template_id": query_template_id,
                    "query_template_version": _QUERY_VERSION,
                    "dataset_id": dataset_id,
                }
            ),
            parameter_template=parameters,
            partition_ref_template=partition_ref,
            depends_on_requirement_ids=tuple(dependencies),
            package_id=package_id,
            component_id=component_id,
            decision_trade_date=decision_trade_date,
            required_for=required_for,
            missing_reason_code=missing_reason_code,
        )
        existing = requirements.get(requirement_id)
        if existing is not None and existing != requirement:
            raise ValueError("source requirement identity collision")
        requirements[requirement_id] = requirement
        return requirement_id


def _hmm_metadata_for_day(runtime_config: dict[str, Any], trade_date: str) -> dict[str, Any]:
    by_date = runtime_config.get("phase0a_hmm_metadata_by_date")
    if isinstance(by_date, dict) and isinstance(by_date.get(trade_date), dict):
        return dict(by_date[trade_date])
    metadata = runtime_config.get("phase0a_hmm_metadata")
    if isinstance(metadata, dict) and str(metadata.get("as_of_trade_date") or "") == trade_date:
        return dict(metadata)
    return {}


def build_hmm_frozen_evidence_partition(
    *,
    selector: Mapping[str, Any],
    phase0a_hmm_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the exact append-only ledger partition consumed by catalog resume."""

    normalized_selector = dict(selector)
    if normalized_selector.get("schema_version") != "advisory_hmm_frozen_evidence_selector_v1":
        raise ValueError("historical HMM evidence selector has an unsupported schema")
    for key in ("research_program_id", "package_id", "decision_trade_date", "signal_preset"):
        if not str(normalized_selector.get(key) or "").strip():
            raise ValueError(f"historical HMM evidence selector requires {key}")
    decision_trade_date = date.fromisoformat(str(normalized_selector["decision_trade_date"]))
    metadata = normalize_hmm_binding_metadata(
        dict(phase0a_hmm_metadata),
        decision_trade_date=decision_trade_date,
    )
    for key in ("model_config_id", "model_snapshot_id", "signal_preset"):
        expected = normalized_selector.get(key)
        if expected is not None and str(metadata.get(key) or "") != str(expected):
            raise ValueError(f"historical HMM evidence metadata differs from selector field {key}")
    return {
        "schema_version": "advisory_hmm_frozen_evidence_partition_v1",
        "selector": normalized_selector,
        "phase0a_hmm_metadata": metadata,
    }
