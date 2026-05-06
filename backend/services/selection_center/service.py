"""Package-based Unified Selection Center service."""

from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
from math import isfinite
from typing import Any

from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.market_data import TradeCalendarProvider
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.strategy_package.metrics_summary import metrics_summary_from_record
from backend.services.strategy_package.backtest_contract import normalize_runtime_config_with_backtest_contract
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    StrategyPackageSelectionArtifactService,
    selection_artifact_runtime_hash,
)
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services import watchlist_service
from backend.services.trading_core.errors import (
    DataUnavailableError,
    StrategyPackageValidationError,
    TradingCoreError,
    UnsupportedFeatureError,
)

from .models import SelectionCandidate, SelectionMode, SelectionPaperPortfolioLink, SelectionRun, SelectionRunStatus
from .package_health import SelectionPackageHealthService
from .repository import SelectionCenterRepository
from .risk_policy import StockRiskPolicyService
from .runtime_profile import normalize_selection_runtime_config, parse_selection_runtime_profile
from .tradability import TradabilityFilter


class SelectionCenterService:
    def __init__(
        self,
        *,
        package_repository: StrategyPackageRepository | Any | None = None,
        repository: SelectionCenterRepository | Any | None = None,
        runtime: StrategyPackageRuntime | None = None,
        tradability_filter: TradabilityFilter | Any | None = None,
        refresh_audit: DataRefreshAuditRepository | Any | None = None,
        paper_portfolio_service: PaperTradingV2PortfolioService | Any | None = None,
        selection_artifact_service: StrategyPackageSelectionArtifactService | Any | None = None,
        calendar_provider: TradeCalendarProvider | Any | None = None,
        risk_policy_service: StockRiskPolicyService | Any | None = None,
        package_health_service: SelectionPackageHealthService | Any | None = None,
    ) -> None:
        self.package_repository = package_repository or StrategyPackageRepository()
        self.repository = repository or SelectionCenterRepository()
        self.runtime = runtime or StrategyPackageRuntime()
        self.tradability_filter = tradability_filter or TradabilityFilter()
        self.refresh_audit = refresh_audit or DataRefreshAuditRepository()
        self.paper_portfolio_service = paper_portfolio_service or PaperTradingV2PortfolioService(
            package_repository=self.package_repository
        )
        self.selection_artifact_service = selection_artifact_service or StrategyPackageSelectionArtifactService(
            package_repository=self.package_repository,
            artifact_repository=getattr(self.runtime, "artifact_repository", None),
        )
        self.calendar_provider = calendar_provider or TradeCalendarProvider()
        self.risk_policy_service = risk_policy_service or StockRiskPolicyService()
        self.package_health_service = package_health_service or SelectionPackageHealthService(
            artifact_repository=getattr(self.runtime, "artifact_repository", None),
            runtime_source_resolver=getattr(self.selection_artifact_service, "runtime_asset_resolver", None),
        )

    def run_single_package(
        self,
        *,
        package_id: str,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any] | None = None,
    ) -> SelectionRun:
        return self.run_packages(
            package_ids=[package_id],
            mode=SelectionMode.SINGLE_PACKAGE,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config or {},
        )

    def run_packages(
        self,
        *,
        package_ids: list[str],
        mode: SelectionMode,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any] | None = None,
    ) -> SelectionRun:
        config = normalize_selection_runtime_config(runtime_config or {})
        config = self._apply_point_in_time_selection_config(config, trade_date=trade_date)
        if not package_ids:
            raise StrategyPackageValidationError("selection run requires package_ids")
        if len(set(package_ids)) != len(package_ids):
            raise StrategyPackageValidationError("selection run package_ids must be unique")
        if mode == SelectionMode.SINGLE_PACKAGE and len(package_ids) != 1:
            raise StrategyPackageValidationError("single package selection requires exactly one package")
        if mode in {SelectionMode.INTERSECTION, SelectionMode.UNION, SelectionMode.WEIGHTED_FUSION} and len(package_ids) < 2:
            raise StrategyPackageValidationError("package aggregation requires at least two packages")
        weights = self._package_weights(config, package_ids) if mode == SelectionMode.WEIGHTED_FUSION else None
        records_by_id, package_configs, package_health = self._prepare_package_runtime_configs(
            package_ids=package_ids,
            config=config,
            trade_date=trade_date,
            data_source=data_source,
        )
        if package_configs:
            config["package_runtime_configs"] = package_configs
        if package_health:
            config["package_health"] = package_health

        run = SelectionRun(
            mode=mode,
            trade_date=trade_date,
            data_source=data_source,
            package_ids=package_ids,
            runtime_config=config,
        )
        self.repository.create_run(run)
        try:
            global_profile = parse_selection_runtime_profile(config)
            self._require_data_ready(trade_date=trade_date, runtime_config=config)
            package_results: dict[str, list[SelectionCandidate]] = {}
            excluded_results = {}
            manifest_sha: dict[str, str] = {}
            for package_id in package_ids:
                record = records_by_id[package_id]
                manifest = record.current_manifest()
                package_config = package_configs[package_id]
                package_profile = parse_selection_runtime_profile(package_config)
                self._ensure_authoritative_selection_artifact(
                    record=record,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config=package_config,
                )
                snapshot = self.runtime.build_signal_snapshot(
                    manifest=manifest,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config=package_config,
                )
                if snapshot.valid_no_candidate:
                    package_results[package_id] = []
                    excluded_results[package_id] = []
                else:
                    top_k = self._top_k_for_package(manifest, package_profile, global_profile)
                    risk_decisions = self.risk_policy_service.evaluate(
                        symbols=[item.symbol for item in snapshot.candidates],
                        trade_date=trade_date,
                        profile=package_profile.risk_policy,
                    )
                    risk_adjusted, risk_excluded = self.risk_policy_service.apply_to_candidates(
                        candidates=snapshot.candidates,
                        decisions=risk_decisions,
                        trade_date=trade_date,
                        top_k=top_k,
                        package_id=manifest.package_id,
                        manifest_sha256=snapshot.manifest_sha256,
                    )
                    if not (
                        package_profile.tradability.exclude_suspended
                        or package_profile.industry_blacklist
                    ):
                        package_results[package_id] = risk_adjusted[:top_k]
                        excluded_results[package_id] = risk_excluded
                        manifest_sha[package_id] = snapshot.manifest_sha256
                        continue
                    tradable, excluded = self.tradability_filter.filter_candidates(
                        candidates=risk_adjusted,
                        trade_date=trade_date,
                        top_k=top_k,
                        package_id=manifest.package_id,
                        manifest_sha256=snapshot.manifest_sha256,
                        enabled=package_profile.tradability.exclude_suspended,
                        industry_blacklist=package_profile.industry_blacklist,
                    )
                    package_results[package_id] = tradable
                    excluded_results[package_id] = [*risk_excluded, *excluded]
                manifest_sha[package_id] = snapshot.manifest_sha256
            aggregate = self._aggregate(mode=mode, package_results=package_results, package_weights=weights)
            if not aggregate:
                if config.get("valid_no_candidate"):
                    completed = run.model_copy(
                        update={
                            "package_results": package_results,
                            "aggregate_results": [],
                            "excluded_results": excluded_results,
                            "manifest_sha256_by_package": manifest_sha,
                            "valid_no_candidate": True,
                            "no_candidate_reason": str(config.get("no_candidate_reason") or "selection aggregation has no candidates"),
                        }
                    )
                    return self.repository.complete_run(completed)
                raise StrategyPackageValidationError(
                    "selection aggregation produced no candidates",
                    context={"mode": mode.value, "package_ids": package_ids},
                )
            completed = run.model_copy(
                update={
                    "package_results": package_results,
                    "aggregate_results": aggregate,
                    "excluded_results": excluded_results,
                    "manifest_sha256_by_package": manifest_sha,
                }
            )
            return self.repository.complete_run(completed)
        except TradingCoreError as exc:
            self.repository.fail_run(run, exc.to_dict())
            raise
        except Exception as exc:
            error = {"error_code": "SELECTION_CENTER_ERROR", "message": str(exc), "context": {"run_id": run.run_id}}
            self.repository.fail_run(run, error)
            raise

    def _require_data_ready(self, *, trade_date: date, runtime_config: dict[str, Any]) -> None:
        if parse_selection_runtime_profile(runtime_config).tradability.exclude_suspended:
            self.refresh_audit.require_success(dataset="suspend_d", trade_date=trade_date)
        artifact_config = self._selection_artifact_config(runtime_config)
        for dataset in artifact_config.get("required_cutoff_audit_datasets") or []:
            cutoff = artifact_config.get("cutoff_date")
            if not cutoff:
                raise StrategyPackageValidationError(
                    "required_cutoff_audit_datasets requires selection_artifact_config.cutoff_date",
                    context={"required_cutoff_audit_datasets": artifact_config.get("required_cutoff_audit_datasets")},
            )
            self.refresh_audit.require_success(dataset=str(dataset), trade_date=date.fromisoformat(str(cutoff)))

    def _prepare_package_runtime_configs(
        self,
        *,
        package_ids: list[str],
        config: dict[str, Any],
        trade_date: date,
        data_source: str,
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        records_by_id: dict[str, Any] = {}
        package_configs: dict[str, dict[str, Any]] = {}
        package_health: dict[str, dict[str, Any]] = {}
        for package_id in package_ids:
            record = self.package_repository.get(package_id)
            if record.package_status not in {
                PackageStatus.BACKTEST_APPROVED,
                PackageStatus.SELECTION_ENABLED,
                PackageStatus.PAPER_ENABLED,
            }:
                raise StrategyPackageValidationError(
                    "package is not enabled for selection",
                    context={"package_id": package_id, "package_status": record.package_status.value},
                )
            manifest = record.current_manifest()
            raw_package_config = self._package_runtime_config(config, package_id)
            package_config = self._normalize_package_runtime_config(
                manifest=manifest,
                runtime_config=raw_package_config,
                package_id=package_id,
            )
            health = self.package_health_service.require_runnable(
                record,
                runtime_config=package_config,
                trade_date=trade_date,
                data_source=data_source,
            )
            records_by_id[package_id] = record
            package_configs[package_id] = package_config
            package_health[package_id] = health
        return records_by_id, package_configs, package_health

    def _normalize_package_runtime_config(
        self,
        *,
        manifest: Any,
        runtime_config: dict[str, Any],
        package_id: str,
    ) -> dict[str, Any]:
        if not self._st_pit_authoritative(runtime_config):
            return normalize_selection_runtime_config(runtime_config)

        contract_input, display_top_n = self._contract_input_with_display_top_n(runtime_config)
        normalized = normalize_runtime_config_with_backtest_contract(
            manifest,
            contract_input,
            context={"package_id": package_id, "check": "selection_center"},
            include_contract=True,
        )
        normalized["st_pit_authoritative"] = True
        if display_top_n is not None:
            normalized["display_top_n"] = self._validate_display_top_n(display_top_n, package_id=package_id)
        return normalized

    @staticmethod
    def _st_pit_authoritative(runtime_config: dict[str, Any]) -> bool:
        return bool(runtime_config.get("st_pit_authoritative") or runtime_config.get("enforce_st_pit_contract"))

    @staticmethod
    def _contract_input_with_display_top_n(runtime_config: dict[str, Any]) -> tuple[dict[str, Any], Any | None]:
        config = deepcopy(runtime_config)
        display_top_n = config.get("display_top_n")
        if display_top_n is None and "top_k" in config:
            display_top_n = config["top_k"]
        config.pop("top_k", None)

        raw_profile = config.get("runtime_profile")
        if raw_profile is None:
            profile: dict[str, Any] = {}
        elif isinstance(raw_profile, dict):
            profile = dict(raw_profile)
        else:
            raise StrategyPackageValidationError(
                "runtime_config.runtime_profile must be an object",
                context={"runtime_profile_type": type(raw_profile).__name__},
            )

        selection_payload = dict(profile.get("selection") or {})
        if display_top_n is None and "top_k" in selection_payload:
            display_top_n = selection_payload["top_k"]
        selection_payload.pop("top_k", None)
        if selection_payload:
            profile["selection"] = selection_payload
        else:
            profile.pop("selection", None)
        risk_payload = profile.get("risk_policy")
        if isinstance(risk_payload, dict) and risk_payload.get("enabled") is False and "risk_policy" not in config:
            profile.pop("risk_policy", None)
        hmm_payload = profile.get("hmm")
        if isinstance(hmm_payload, dict) and hmm_payload.get("enabled") is False and "hmm" not in config:
            profile.pop("hmm", None)
        config["runtime_profile"] = profile
        return config, display_top_n

    @staticmethod
    def _validate_display_top_n(value: Any, *, package_id: str) -> int:
        try:
            display_top_n = int(value)
        except (TypeError, ValueError) as exc:
            raise StrategyPackageValidationError(
                "selection display_top_n must be an integer",
                context={"package_id": package_id, "display_top_n": value},
            ) from exc
        if display_top_n <= 0 or display_top_n > 50:
            raise StrategyPackageValidationError(
                "selection display_top_n must be between 1 and 50",
                context={"package_id": package_id, "display_top_n": display_top_n, "max_display_top_n": 50},
            )
        return display_top_n

    @staticmethod
    def _package_runtime_config(config: dict[str, Any], package_id: str) -> dict[str, Any]:
        package_config = config.get(package_id)
        if isinstance(package_config, dict):
            merged = dict(config)
            merged.update(package_config)
            return merged
        return config

    @staticmethod
    def _top_k_for_package(manifest: Any, package_profile: Any, global_profile: Any) -> int:
        configured = package_profile.selection.top_k or global_profile.selection.top_k
        top_k = int(configured if configured is not None else manifest.portfolio_policy.topk)
        if top_k > 50:
            raise StrategyPackageValidationError(
                "selection top_k must not exceed 50",
                context={"package_id": manifest.package_id, "top_k": top_k, "max_top_k": 50},
            )
        return top_k

    def _ensure_authoritative_selection_artifact(
        self,
        *,
        record: Any,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
    ) -> None:
        artifact_config = runtime_config.get("selection_artifact_config")
        if artifact_config is None:
            artifact_config = runtime_config.get("selection_artifact")
        if not isinstance(artifact_config, dict) or not bool(artifact_config.get("auto_generate")):
            return

        manifest = record.current_manifest()
        runtime_hash = selection_artifact_runtime_hash(runtime_config)
        cutoff_date = self._parse_selection_cutoff_date(artifact_config, trade_date=trade_date, strict_before=True)
        force_regenerate = bool(artifact_config.get("force_regenerate"))
        artifact_repository = getattr(self.runtime, "artifact_repository", None)
        if artifact_repository is not None and not force_regenerate:
            try:
                artifact = artifact_repository.get(
                    package_id=record.package_id,
                    manifest_sha256=manifest.manifest_sha256 or record.manifest_sha256,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config_hash=runtime_hash,
                )
                metadata = artifact.metadata or {}
                if (
                    artifact.status.value == "SUCCEEDED"
                    and artifact.scores_json
                    and metadata.get("source_type") == AUTHORITATIVE_SELECTION_SOURCE_TYPE
                    and metadata.get("authority_scope") == AUTHORITATIVE_SELECTION_SCOPE
                ):
                    return
            except DataUnavailableError:
                pass

        self.selection_artifact_service.generate_from_live_inference(
            package_id=record.package_id,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
            include_reference_price=bool(artifact_config.get("include_reference_price", True)),
            cutoff_date=cutoff_date,
        )

    def resolve_point_in_time_context(
        self,
        *,
        trade_date: date,
        pit_mode: str,
        explicit_cutoff_date: date | None = None,
    ) -> dict[str, Any]:
        """Resolve the target trade date to the latest allowed as-of date.

        The authoritative historical selection mode is point-in-time: selecting
        stocks for D must use data available no later than the previous trading
        day. This method only reads the trading calendar and never fabricates a
        cutoff when the calendar is missing.
        """

        normalized = self._normalize_pit_mode(pit_mode)
        if normalized == "NONE":
            return {
                "pit_mode": "NONE",
                "trade_date": trade_date.isoformat(),
                "cutoff_date": explicit_cutoff_date.isoformat() if explicit_cutoff_date else None,
                "score_trade_date": explicit_cutoff_date.isoformat() if explicit_cutoff_date else trade_date.isoformat(),
                "reference_price_trade_date": explicit_cutoff_date.isoformat() if explicit_cutoff_date else trade_date.isoformat(),
            }
        self.calendar_provider.ensure_trading_day(trade_date)
        cutoff = explicit_cutoff_date or self._previous_trading_day(trade_date)
        if cutoff >= trade_date:
            raise StrategyPackageValidationError(
                "point-in-time selection cutoff_date must be before trade_date",
                context={"trade_date": trade_date.isoformat(), "cutoff_date": cutoff.isoformat(), "pit_mode": normalized},
            )
        return {
            "pit_mode": normalized,
            "trade_date": trade_date.isoformat(),
            "cutoff_date": cutoff.isoformat(),
            "score_trade_date": cutoff.isoformat(),
            "reference_price_trade_date": cutoff.isoformat(),
            "calendar_source": "market.trading_calendar",
        }

    def _apply_point_in_time_selection_config(self, config: dict[str, Any], *, trade_date: date) -> dict[str, Any]:
        artifact_config = self._selection_artifact_config(config)
        raw_mode = artifact_config.get("pit_mode") or artifact_config.get("cutoff_policy") or config.get("pit_mode")
        if raw_mode is None:
            raw_mode = "NONE"
        pit_mode = self._normalize_pit_mode(str(raw_mode))
        if pit_mode == "NONE":
            cutoff = artifact_config.get("cutoff_date")
            if cutoff:
                self._parse_selection_cutoff_date(artifact_config, trade_date=trade_date, strict_before=True)
            return config
        explicit_cutoff = self._parse_selection_cutoff_date(artifact_config, trade_date=trade_date, strict_before=True)
        context = self.resolve_point_in_time_context(
            trade_date=trade_date,
            pit_mode=pit_mode,
            explicit_cutoff_date=explicit_cutoff,
        )
        updated = dict(config)
        updated_artifact = dict(artifact_config)
        updated_artifact["pit_mode"] = context["pit_mode"]
        updated_artifact["cutoff_date"] = context["cutoff_date"]
        updated["selection_artifact_config"] = updated_artifact
        if "selection_artifact" in updated:
            updated["selection_artifact"] = updated_artifact
        updated["point_in_time_context"] = context
        return updated

    @staticmethod
    def _selection_artifact_config(config: dict[str, Any]) -> dict[str, Any]:
        artifact_config = config.get("selection_artifact_config")
        if artifact_config is None:
            artifact_config = config.get("selection_artifact")
        if artifact_config is None:
            return {}
        if not isinstance(artifact_config, dict):
            raise StrategyPackageValidationError(
                "runtime_config.selection_artifact_config must be an object",
                context={"selection_artifact_config_type": type(artifact_config).__name__},
            )
        return artifact_config

    @staticmethod
    def _normalize_pit_mode(pit_mode: str) -> str:
        text = str(pit_mode or "NONE").strip().upper()
        aliases = {
            "": "NONE",
            "NONE": "NONE",
            "DISABLED": "NONE",
            "PREVIOUS_TRADING_DAY_CUTOFF": "PREVIOUS_TRADING_DAY_CLOSE",
            "PREVIOUS_TRADING_DAY": "PREVIOUS_TRADING_DAY_CLOSE",
            "PREVIOUS_TRADING_DAY_CLOSE": "PREVIOUS_TRADING_DAY_CLOSE",
            "PREV_TRADING_DAY_CLOSE": "PREVIOUS_TRADING_DAY_CLOSE",
        }
        normalized = aliases.get(text)
        if normalized is None:
            raise StrategyPackageValidationError(
                "unsupported point-in-time selection mode",
                context={"pit_mode": pit_mode, "supported": ["NONE", "PREVIOUS_TRADING_DAY_CLOSE"]},
            )
        return normalized

    def _previous_trading_day(self, trade_date: date) -> date:
        lookup_start = trade_date - timedelta(days=31)
        days = self.calendar_provider.list_trading_days(lookup_start, trade_date - timedelta(days=1))
        if not days:
            raise DataUnavailableError(
                "trading calendar has no previous trading day for point-in-time selection",
                context={"trade_date": trade_date.isoformat(), "lookup_start": lookup_start.isoformat()},
            )
        return days[-1]

    @staticmethod
    def _parse_selection_cutoff_date(
        artifact_config: dict[str, Any],
        *,
        trade_date: date,
        strict_before: bool = False,
    ) -> date | None:
        raw = artifact_config.get("cutoff_date")
        if raw is None or raw == "":
            return None
        try:
            parsed = date.fromisoformat(str(raw))
        except ValueError as exc:
            raise StrategyPackageValidationError(
                "selection_artifact_config.cutoff_date must be YYYY-MM-DD",
                context={"cutoff_date": raw},
            ) from exc
        if parsed > trade_date or (strict_before and parsed >= trade_date):
            raise StrategyPackageValidationError(
                "selection_artifact_config.cutoff_date must be before trade_date",
                context={"trade_date": trade_date.isoformat(), "cutoff_date": parsed.isoformat()},
            )
        return parsed

    def get_run(self, run_id: str) -> SelectionRun:
        return self.repository.get_run(run_id)

    def list_runs(self, *, limit: int = 100) -> list[SelectionRun]:
        return self.repository.list_runs(limit=limit)

    def list_selectable_packages(self, *, limit: int = 200) -> list[dict[str, Any]]:
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        eligible_statuses = [
            PackageStatus.BACKTEST_APPROVED,
            PackageStatus.SELECTION_ENABLED,
            PackageStatus.PAPER_ENABLED,
        ]
        records_by_id = {}
        for status in eligible_statuses:
            for record in self.package_repository.list(status=status, limit=limit):
                records_by_id[record.package_id] = record
        records = sorted(records_by_id.values(), key=lambda item: item.created_at, reverse=True)[:limit]
        latest_runs = self._latest_run_by_package(limit=max(limit * 5, 200))
        package_service = StrategyPackageService(repository=self.package_repository)
        items: list[dict[str, Any]] = []
        for record in records:
            manifest = record.current_manifest()
            model_state = package_service.get_model_state(record.package_id)
            latest_run = latest_runs.get(record.package_id)
            health = self.package_health_service.summarize(record)
            items.append(
                {
                    "package_id": record.package_id,
                    "package_name": record.package_name,
                    "package_version": record.package_version,
                    "package_status": record.package_status.value,
                    "source_type": record.source_type,
                    "source_id": record.source_id,
                    "loop_id": record.loop_id,
                    "run_id": record.run_id,
                    "manifest_sha256": record.manifest_sha256,
                    "alpha_mode": manifest.alpha_mode.value,
                    "alpha_count": len(manifest.alpha_components),
                    "portfolio_topk": manifest.portfolio_policy.topk,
                    "created_at": record.created_at.isoformat(),
                    "updated_at": record.updated_at.isoformat(),
                    "metrics_summary": metrics_summary_from_record(record).model_dump(mode="json"),
                    "model_state": model_state.model_dump(mode="json"),
                    "selection_health": health,
                    "latest_selection_run": self._selection_run_summary(latest_run) if latest_run else None,
                }
            )
        return items

    def aggregate_existing_runs(
        self,
        *,
        source_run_ids: list[str],
        mode: SelectionMode,
        runtime_config: dict[str, Any] | None = None,
    ) -> SelectionRun:
        config = dict(runtime_config or {})
        if len(source_run_ids) < 2:
            raise StrategyPackageValidationError("aggregate existing runs requires at least two source_run_ids")
        if len(set(source_run_ids)) != len(source_run_ids):
            raise StrategyPackageValidationError("source_run_ids must be unique")
        if mode == SelectionMode.SINGLE_PACKAGE:
            raise StrategyPackageValidationError("aggregate existing runs requires an aggregation mode")

        source_runs = [self.repository.get_run(run_id) for run_id in source_run_ids]
        trade_dates = {run.trade_date for run in source_runs}
        data_sources = {run.data_source for run in source_runs}
        if len(trade_dates) != 1:
            raise StrategyPackageValidationError(
                "source selection runs must share the same trade_date",
                context={"source_run_ids": source_run_ids, "trade_dates": sorted(item.isoformat() for item in trade_dates)},
            )
        if len(data_sources) != 1:
            raise StrategyPackageValidationError(
                "source selection runs must share the same data_source",
                context={"source_run_ids": source_run_ids, "data_sources": sorted(data_sources)},
            )

        package_ids: list[str] = []
        package_results: dict[str, list[SelectionCandidate]] = {}
        manifest_sha: dict[str, str] = {}
        for source_run in source_runs:
            if source_run.status != SelectionRunStatus.SUCCEEDED:
                raise StrategyPackageValidationError(
                    "source selection run must be succeeded",
                    context={"run_id": source_run.run_id, "status": source_run.status.value},
                )
            if source_run.mode != SelectionMode.SINGLE_PACKAGE or len(source_run.package_ids) != 1:
                raise StrategyPackageValidationError(
                    "source selection run must be a single-package run",
                    context={"run_id": source_run.run_id, "mode": source_run.mode.value, "package_ids": source_run.package_ids},
                )
            if not source_run.aggregate_results:
                raise StrategyPackageValidationError(
                    "source selection run has no aggregate results",
                    context={"run_id": source_run.run_id},
                )
            package_id = source_run.package_ids[0]
            if package_id in package_results:
                raise StrategyPackageValidationError(
                    "source selection runs must reference unique packages",
                    context={"package_id": package_id, "source_run_ids": source_run_ids},
                )
            package_ids.append(package_id)
            manifest_sha256 = source_run.manifest_sha256_by_package.get(package_id)
            if not manifest_sha256:
                raise StrategyPackageValidationError(
                    "source selection run is missing manifest hash",
                    context={"run_id": source_run.run_id, "package_id": package_id},
                )
            manifest_sha[package_id] = manifest_sha256
            package_results[package_id] = [
                self._candidate_from_source_run(candidate, source_run=source_run, package_id=package_id)
                for candidate in source_run.aggregate_results
            ]

        weights = self._package_weights(config, package_ids) if mode == SelectionMode.WEIGHTED_FUSION else None
        run = SelectionRun(
            mode=mode,
            trade_date=source_runs[0].trade_date,
            data_source=source_runs[0].data_source,
            package_ids=package_ids,
            runtime_config={
                **config,
                "aggregation_source": "existing_selection_runs",
                "source_run_ids": source_run_ids,
            },
        )
        self.repository.create_run(run)
        try:
            aggregate = self._aggregate(mode=mode, package_results=package_results, package_weights=weights)
            aggregate = [
                item.model_copy(
                    update={
                        "component_scores": {
                            **item.component_scores,
                            "aggregation_source": "existing_selection_runs",
                            "source_run_ids": source_run_ids,
                        }
                    }
                )
                for item in aggregate
            ]
            if not aggregate:
                if config.get("valid_no_candidate"):
                    completed = run.model_copy(
                        update={
                            "package_results": package_results,
                            "aggregate_results": [],
                            "manifest_sha256_by_package": manifest_sha,
                            "valid_no_candidate": True,
                            "no_candidate_reason": str(config.get("no_candidate_reason") or "existing run aggregation has no candidates"),
                        }
                    )
                    return self.repository.complete_run(completed)
                raise StrategyPackageValidationError(
                    "existing selection run aggregation produced no candidates",
                    context={"mode": mode.value, "source_run_ids": source_run_ids},
                )
            completed = run.model_copy(
                update={
                    "package_results": package_results,
                    "aggregate_results": aggregate,
                    "manifest_sha256_by_package": manifest_sha,
                }
            )
            return self.repository.complete_run(completed)
        except TradingCoreError as exc:
            self.repository.fail_run(run, exc.to_dict())
            raise
        except Exception as exc:
            error = {"error_code": "SELECTION_CENTER_ERROR", "message": str(exc), "context": {"run_id": run.run_id}}
            self.repository.fail_run(run, error)
            raise

    def create_paper_portfolio_from_run(
        self,
        *,
        run_id: str,
        portfolio_name: str,
        initial_cash: float,
        start_date: date,
        data_source: MinuteDataSource,
        fee_policy: dict[str, Any] | None = None,
        risk_policy: dict[str, Any] | None = None,
        execution_policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        run = self.repository.get_run(run_id)
        if run.status != SelectionRunStatus.SUCCEEDED:
            raise StrategyPackageValidationError(
                "only successful selection runs can create paper portfolios",
                context={"run_id": run_id, "status": run.status.value},
            )
        if not run.aggregate_results:
            raise StrategyPackageValidationError(
                "selection run has no aggregate results for paper portfolio creation",
                context={"run_id": run_id},
            )
        if run.mode != SelectionMode.SINGLE_PACKAGE or len(run.package_ids) != 1:
            raise UnsupportedFeatureError(
                "creating a paper portfolio from multi-package selection requires a combined StrategyPackage",
                context={"run_id": run_id, "mode": run.mode.value, "package_ids": run.package_ids},
            )
        package_id = run.package_ids[0]
        record = self.package_repository.get(package_id)
        manifest = record.current_manifest()
        manifest_sha256 = run.manifest_sha256_by_package.get(package_id)
        if manifest_sha256 != manifest.manifest_sha256:
            raise StrategyPackageValidationError(
                "selection run manifest hash does not match current strategy package",
                context={
                    "run_id": run_id,
                    "package_id": package_id,
                    "selection_manifest_sha256": manifest_sha256,
                    "current_manifest_sha256": manifest.manifest_sha256,
                },
            )

        paper_runtime_config = self._paper_runtime_config_from_selection_run(run)
        portfolio = self.paper_portfolio_service.create_portfolio(
            package_id=package_id,
            portfolio_name=portfolio_name,
            initial_cash=initial_cash,
            start_date=start_date,
            data_source=data_source,
            fee_policy=fee_policy,
            risk_policy=risk_policy,
            execution_policy=execution_policy,
        )
        link = self.repository.create_paper_portfolio_link(
            SelectionPaperPortfolioLink(
                run_id=run.run_id,
                portfolio_id=portfolio.portfolio_id,
                package_id=package_id,
                manifest_sha256=manifest_sha256,
                trade_date=run.trade_date,
                data_source=run.data_source,
                start_date=start_date,
                initial_cash=initial_cash,
                runtime_config=paper_runtime_config,
            )
        )
        return {"portfolio": portfolio, "link": link, "paper_runtime_config": paper_runtime_config}

    def list_paper_portfolio_links(self, run_id: str) -> list[SelectionPaperPortfolioLink]:
        self.repository.get_run(run_id)
        return self.repository.list_paper_portfolio_links(run_id)

    def add_run_to_watchlist(
        self,
        *,
        run_id: str,
        category_id: int | None = None,
        category_name: str | None = None,
        top_k: int = 20,
        on_conflict: str = "ignore",
    ) -> dict[str, Any]:
        if top_k <= 0 or top_k > 50:
            raise StrategyPackageValidationError(
                "watchlist import top_k must be between 1 and 50",
                context={"run_id": run_id, "top_k": top_k},
            )
        if on_conflict not in {"ignore", "move"}:
            raise StrategyPackageValidationError(
                "watchlist import on_conflict must be ignore or move",
                context={"run_id": run_id, "on_conflict": on_conflict},
            )
        run = self.repository.get_run(run_id)
        if run.status != SelectionRunStatus.SUCCEEDED:
            raise StrategyPackageValidationError(
                "only successful selection runs can be added to watchlist",
                context={"run_id": run_id, "status": run.status.value},
            )
        if not run.aggregate_results:
            raise StrategyPackageValidationError(
                "selection run has no aggregate results to add to watchlist",
                context={"run_id": run_id},
            )
        selected = sorted(run.aggregate_results, key=lambda item: item.rank)[:top_k]
        missing_prices = [
            item.symbol
            for item in selected
            if item.reference_price is None or float(item.reference_price) <= 0
        ]
        if missing_prices:
            raise StrategyPackageValidationError(
                "selection watchlist import requires reference_price for every selected symbol",
                context={
                    "run_id": run_id,
                    "trade_date": run.trade_date.isoformat(),
                    "missing_price_count": len(missing_prices),
                    "missing_price_examples": missing_prices[:20],
                },
            )
        resolved_category_id = self._resolve_watchlist_category(
            category_id=category_id,
            category_name=category_name,
            run=run,
        )
        source_names = self._watchlist_source_names(run)
        source_label = " + ".join(source_names) if source_names else "StrategyPackage Selection"
        items = [
            {
                "code": candidate.symbol,
                "rank": candidate.rank,
                "entry_price": float(candidate.reference_price or 0),
                "task_id": run.run_id,
                "loop_id": None,
                "as_of": run.trade_date.isoformat(),
                "entry_source": source_label,
                "note": (
                    f"Selection Center {run.mode.value}; trade_date={run.trade_date.isoformat()}; "
                    f"data_source={run.data_source}; package={source_label}"
                ),
            }
            for candidate in selected
        ]
        result = watchlist_service.add_items_bulk_from_task_selection(
            items=items,
            category_id=resolved_category_id,
            on_conflict=on_conflict,
            entry_source=source_label,
        )
        if not result.get("ok") or result.get("errors"):
            raise StrategyPackageValidationError(
                "selection watchlist import failed",
                context={
                    "run_id": run_id,
                    "category_id": resolved_category_id,
                    "result": result,
                },
            )
        return {
            **result,
            "run_id": run_id,
            "category_id": resolved_category_id,
            "entry_source": source_label,
            "entry_as_of": run.trade_date.isoformat(),
            "requested_top_k": top_k,
            "imported_symbols": [item["code"] for item in items],
        }

    def _resolve_watchlist_category(
        self,
        *,
        category_id: int | None,
        category_name: str | None,
        run: SelectionRun,
    ) -> int:
        if category_id is not None:
            if int(category_id) <= 0:
                raise StrategyPackageValidationError(
                    "watchlist category_id must be positive",
                    context={"run_id": run.run_id, "category_id": category_id},
                )
            return int(category_id)
        clean_name = str(category_name or "").strip()
        if not clean_name:
            names = self._watchlist_source_names(run)
            clean_name = f"选股中心-{names[0] if names else run.run_id}-{run.trade_date.isoformat()}"
        for category in watchlist_service.list_categories():
            if str(category.get("name") or "").strip() == clean_name:
                return int(category["id"])
        return int(
            watchlist_service.create_category(
                clean_name,
                f"Selection Center run {run.run_id} on {run.trade_date.isoformat()}",
            )
        )

    def _watchlist_source_names(self, run: SelectionRun) -> list[str]:
        names: list[str] = []
        for package_id in run.package_ids:
            names.append(self.package_repository.get(package_id).package_name)
        return names

    def _latest_run_by_package(self, *, limit: int) -> dict[str, SelectionRun]:
        latest: dict[str, SelectionRun] = {}
        for run in self.repository.list_runs(limit=limit):
            if run.status != SelectionRunStatus.SUCCEEDED:
                continue
            for package_id in run.package_ids:
                latest.setdefault(package_id, run)
        return latest

    @staticmethod
    def _selection_run_summary(run: SelectionRun) -> dict[str, Any]:
        return {
            "run_id": run.run_id,
            "mode": run.mode.value,
            "trade_date": run.trade_date.isoformat(),
            "data_source": run.data_source,
            "status": run.status.value,
            "candidate_count": len(run.aggregate_results),
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        }

    @staticmethod
    def _candidate_from_source_run(
        candidate: SelectionCandidate,
        *,
        source_run: SelectionRun,
        package_id: str,
    ) -> SelectionCandidate:
        return candidate.model_copy(
            update={
                "component_scores": {
                    **candidate.component_scores,
                    "source_selection_run_id": source_run.run_id,
                    "source_selection_mode": source_run.mode.value,
                    "source_package_id": package_id,
                    "source_manifest_sha256": source_run.manifest_sha256_by_package.get(package_id),
                }
            }
        )

    @staticmethod
    def _paper_runtime_config_from_selection_run(run: SelectionRun) -> dict[str, Any]:
        return {
            "selection_source": {
                "run_id": run.run_id,
                "mode": run.mode.value,
                "trade_date": run.trade_date.isoformat(),
                "data_source": run.data_source,
                "package_ids": run.package_ids,
                "manifest_sha256_by_package": run.manifest_sha256_by_package,
                "candidate_count": len(run.aggregate_results),
                "note": "Paper v2 must generate authoritative live selection artifacts for each trading day; "
                "selection run scores are trace-only and are not reused as signal input.",
            },
        }

    def _aggregate(
        self,
        *,
        mode: SelectionMode,
        package_results: dict[str, list[SelectionCandidate]],
        package_weights: dict[str, float] | None = None,
    ) -> list[SelectionCandidate]:
        if mode == SelectionMode.SINGLE_PACKAGE:
            return next(iter(package_results.values()))
        if mode == SelectionMode.WEIGHTED_FUSION:
            if package_weights is None:
                raise StrategyPackageValidationError("weighted package fusion requires package_weights")
            return self._weighted_rank_fusion(package_results=package_results, package_weights=package_weights)
        symbol_sets = [set(candidate.symbol for candidate in rows) for rows in package_results.values()]
        symbols = set.union(*symbol_sets) if mode == SelectionMode.UNION else set.intersection(*symbol_sets)
        rows_by_symbol: dict[str, list[tuple[str, SelectionCandidate]]] = {}
        for package_id, rows in package_results.items():
            for row in rows:
                if row.symbol in symbols:
                    rows_by_symbol.setdefault(row.symbol, []).append((package_id, row))
        aggregate: list[SelectionCandidate] = []
        for symbol, rows in rows_by_symbol.items():
            rows.sort(key=lambda item: item[1].rank)
            best = rows[0][1]
            source_package_ids = [package_id for package_id, _ in rows]
            aggregate.append(
                SelectionCandidate(
                    symbol=symbol,
                    score=sum(row.score for _, row in rows) / len(rows),
                    rank=best.rank,
                    target_weight=best.target_weight,
                    target_quantity=best.target_quantity,
                    reference_price=best.reference_price,
                    component_scores={
                        "source_package_ids": source_package_ids,
                        "package_ranks": {package_id: row.rank for package_id, row in rows},
                    },
                    reason=f"{mode.value}_aggregate",
                )
            )
        aggregate.sort(key=lambda item: (-item.score, item.rank, item.symbol))
        reranked: list[SelectionCandidate] = []
        for idx, item in enumerate(aggregate, start=1):
            reranked.append(item.model_copy(update={"rank": idx}))
        return reranked

    @staticmethod
    def _package_weights(config: dict[str, Any], package_ids: list[str]) -> dict[str, float]:
        raw = config.get("package_weights")
        if not isinstance(raw, dict):
            raise StrategyPackageValidationError(
                "weighted package fusion requires runtime_config.package_weights",
                context={"package_ids": package_ids},
            )
        expected = set(package_ids)
        actual = {str(key) for key in raw}
        if actual != expected:
            raise StrategyPackageValidationError(
                "runtime_config.package_weights must match package_ids exactly",
                context={"package_ids": package_ids, "weight_keys": sorted(actual)},
            )
        weights: dict[str, float] = {}
        for package_id in package_ids:
            value = float(raw[package_id])
            if not isfinite(value) or value <= 0:
                raise StrategyPackageValidationError(
                    "package weights must be positive finite numbers",
                    context={"package_id": package_id, "weight": raw[package_id]},
                )
            weights[package_id] = value
        return weights

    @staticmethod
    def _weighted_rank_fusion(
        *,
        package_results: dict[str, list[SelectionCandidate]],
        package_weights: dict[str, float],
    ) -> list[SelectionCandidate]:
        total_weight = sum(package_weights.values())
        normalized_weights = {package_id: weight / total_weight for package_id, weight in package_weights.items()}
        rows_by_symbol: dict[str, list[tuple[str, SelectionCandidate, float]]] = {}
        for package_id, rows in package_results.items():
            candidate_count = len(rows)
            if candidate_count <= 0:
                continue
            denominator = max(candidate_count - 1, 1)
            for row in rows:
                normalized_rank_score = 1.0 - ((row.rank - 1) / denominator)
                rows_by_symbol.setdefault(row.symbol, []).append((package_id, row, normalized_rank_score))

        aggregate: list[SelectionCandidate] = []
        for symbol, rows in rows_by_symbol.items():
            rows.sort(key=lambda item: item[1].rank)
            best = rows[0][1]
            source_package_ids = [package_id for package_id, _, _ in rows]
            package_scores = {package_id: row.score for package_id, row, _ in rows}
            package_ranks = {package_id: row.rank for package_id, row, _ in rows}
            rank_scores = {package_id: rank_score for package_id, _, rank_score in rows}
            fusion_score = sum(normalized_weights[package_id] * rank_score for package_id, _, rank_score in rows)
            aggregate.append(
                SelectionCandidate(
                    symbol=symbol,
                    score=fusion_score,
                    rank=best.rank,
                    target_weight=best.target_weight,
                    target_quantity=best.target_quantity,
                    reference_price=best.reference_price,
                    component_scores={
                        "fusion_method": "weighted_rank_fusion",
                        "source_package_ids": source_package_ids,
                        "package_ranks": package_ranks,
                        "package_raw_scores": package_scores,
                        "package_rank_scores": rank_scores,
                        "package_weights": package_weights,
                        "normalized_package_weights": normalized_weights,
                        "fusion_score": fusion_score,
                    },
                    reason="weighted_fusion_aggregate",
                )
            )
        aggregate.sort(key=lambda item: (-item.score, item.rank, item.symbol))
        return [item.model_copy(update={"rank": idx}) for idx, item in enumerate(aggregate, start=1)]
