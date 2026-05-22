"""Normalize already-collected QE completion payloads for archive writes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from numbers import Number
from typing import Any, Mapping, Sequence

from .models import (
    AccountSummaryRecord,
    CurveRecord,
    DataContextRecord,
    ExecutionEventRecord,
    MetricRecord,
    QEArchiveRun,
    RawPayloadRecord,
    ReproducibilityManifestRecord,
    RunConfigRecord,
    RunFactorImportanceRecord,
    RunFactorRecord,
    RunSourceRecord,
    SymbolSummaryRecord,
    TradeRecord,
    build_factor_set_hash,
    sha256_json,
)


DEFAULT_CONFIG_SCHEMA_VERSION = "qe_archive_config_v1"
DEFAULT_MANIFEST_SCHEMA_VERSION = "qe_archive_repro_manifest_v1"


@dataclass(frozen=True)
class ExtractedArchivePayload:
    run: QEArchiveRun
    source: RunSourceRecord
    config: RunConfigRecord
    reproducibility_manifest: ReproducibilityManifestRecord
    data_contexts: list[DataContextRecord]
    account_summary: AccountSummaryRecord | None
    metrics: list[MetricRecord]
    curves: list[CurveRecord]
    factors: list[RunFactorRecord]
    factor_importance: list[RunFactorImportanceRecord]
    symbol_summaries: list[SymbolSummaryRecord]
    trades: list[TradeRecord]
    execution_events: list[ExecutionEventRecord]
    raw_payloads: list[RawPayloadRecord]
    stats: dict[str, Any]


class QEArchivePayloadExtractor:
    """Build archive records from DB/API payloads without reading worker files."""

    def extract(
        self,
        payload: Mapping[str, Any],
        *,
        event_type: str | None = None,
        source_system: str | None = None,
        source_id: str | None = None,
        source_sub_id: str | None = None,
    ) -> ExtractedArchivePayload:
        data = _ensure_mapping(payload)
        source_system = str(source_system or data.get("source_system") or "quantevolver")
        source_id = str(
            source_id
            or data.get("source_id")
            or data.get("experiment_id")
            or data.get("task_id")
            or data.get("run_id")
            or sha256_json(data)[:24]
        )
        source_sub_id = _optional_str(source_sub_id or data.get("source_sub_id") or data.get("loop_id"))

        metrics = _ensure_mapping(
            data.get("metrics")
            or data.get("metrics_json")
            or data.get("result_metrics")
            or data.get("results")
            or {}
        )
        enhanced = _ensure_mapping(data.get("enhanced_metrics") or metrics.get("enhanced_metrics") or {})
        summary = _ensure_mapping(metrics.get("summary") or enhanced.get("summary") or {})

        config_source = _ensure_mapping(
            data.get("canonical_config")
            or data.get("config")
            or data.get("config_json")
            or data.get("qlib_config")
            or {}
        )
        raw_config = _ensure_mapping(data.get("raw_config") or data.get("config_raw") or config_source)
        factor_items = _extract_factor_items(data, config_source)
        factor_list = [_factor_display_value(item) for item in factor_items]
        factor_set_hash = build_factor_set_hash(factor_list)

        data_context = self._extract_data_context(data, config_source, metrics, factor_set_hash)
        missing_items = self._missing_items(config_source, factor_list, metrics, enhanced, data_context)
        config_capture_complete = len(missing_items) == 0

        run_type = _run_type(data, event_type)
        run_id = str(
            data.get("run_id")
            or f"qear_run_{sha256_json({'source_system': source_system, 'source_id': source_id, 'source_sub_id': source_sub_id, 'run_type': run_type})[:24]}"
        )
        data_context.run_id = run_id
        logical_experiment_id = str(
            data.get("logical_experiment_id")
            or data.get("experiment_id")
            or (f"{data.get('task_id')}:{data.get('loop_id')}" if data.get("task_id") and data.get("loop_id") else source_id)
        )

        research_valid, invalid_reason, exclusion_tags = _research_validity(data, data_context)
        run = QEArchiveRun(
            run_id=run_id,
            logical_experiment_id=logical_experiment_id,
            source_system=source_system,
            run_type=run_type,
            status=str(data.get("status") or "completed"),
            task_id=_optional_str(data.get("task_id")),
            loop_id=_optional_str(data.get("loop_id")),
            loop_index=_as_int(data.get("loop_index")),
            experiment_id=_optional_str(data.get("experiment_id")),
            node_id=_optional_str(data.get("node_id") or data.get("execution_node_id")),
            model_family=_optional_str(data.get("model_family") or _first_value(config_source, ("model_family",))),
            model_type=_optional_str(data.get("model_type") or data.get("model_name") or _first_value(config_source, ("model_type", "model_name"))),
            factor_set_hash=factor_set_hash,
            factor_count=len(factor_list),
            freq=data_context.freq,
            label_horizon=data_context.label_horizon,
            research_valid=research_valid,
            invalid_reason=invalid_reason,
            exclusion_tags=exclusion_tags,
            score_total=_as_float(data.get("score_total") or metrics.get("score_total") or summary.get("score_total")),
            score_version=_optional_str(data.get("score_version") or metrics.get("score_version")),
            priority_rank=_as_int(data.get("priority_rank")),
            started_at=_as_datetime(data.get("started_at") or data.get("source_created_at")),
            completed_at=_as_datetime(data.get("completed_at") or data.get("finished_at") or data.get("source_updated_at")),
            source_created_at=_as_datetime(data.get("source_created_at") or data.get("created_at")),
            source_updated_at=_as_datetime(data.get("source_updated_at") or data.get("updated_at")),
        )

        config = RunConfigRecord(
            run_id=run_id,
            config_schema_version=str(data.get("config_schema_version") or DEFAULT_CONFIG_SCHEMA_VERSION),
            canonical_config=config_source,
            raw_config=raw_config,
            factor_list=factor_list,
            factor_set_hash=factor_set_hash,
            model_config=_section(data, config_source, "model_config", "model"),
            model_params=_section(data, config_source, "model_params", "params", "hyperparams", "hyperparameters"),
            strategy_config=_section(data, config_source, "strategy_config", "strategy"),
            backtest_config=_section(data, config_source, "backtest_config", "backtest"),
            data_split=_section(data, config_source, "data_split", "split"),
            execution_config=_section(data, config_source, "execution_config", "executor", "execution"),
            runtime_flags=_section(data, config_source, "runtime_flags", "flags"),
            agent_context=_section(data, config_source, "agent_context", "agent"),
            config_capture_complete=config_capture_complete,
            config_provenance={
                "source_system": source_system,
                "source_id": source_id,
                "source_sub_id": source_sub_id,
                "event_type": event_type,
                "source_keys": sorted(str(k) for k in data.keys()),
            },
            missing_config_items=missing_items,
        )

        account_summary = self._extract_account_summary(run_id, data, metrics, enhanced, summary)
        metric_records = self._extract_metrics(run_id, metrics, enhanced)
        curves = self._extract_curves(run_id, enhanced)
        factor_records = self._extract_factors(run_id, factor_items)
        factor_importance = self._extract_factor_importance(run_id, data, metrics, enhanced)
        symbol_summaries = self._extract_symbol_summaries(run_id, enhanced, metrics)
        trades = self._extract_trades(run_id, enhanced, metrics)
        execution_events = self._extract_execution_events(
            run_id,
            data,
            metrics,
            enhanced,
            symbol_summary_count=len(symbol_summaries),
            trade_count=len(trades),
        )
        raw_payloads = self._raw_payloads(run_id, source_system, source_id, data, metrics, enhanced)

        random_seed = _extract_random_seed(data, config)
        seed_policy = "fixed" if random_seed is not None else "unset_legacy"
        reproducibility_level = (
            "audit_only"
            if random_seed is None
            else ("full" if config_capture_complete else "partial")
        )
        verification_status = "not_reproducible" if random_seed is None else "not_verified"
        runtime_metadata = _extract_reproducibility_runtime_metadata(
            data,
            config,
            random_seed=random_seed,
            seed_policy=seed_policy,
        )

        manifest_json = {
            "source_system": source_system,
            "source_id": source_id,
            "source_sub_id": source_sub_id,
            "run_id": run_id,
            "run_type": run_type,
            "reproducibility_level": reproducibility_level,
            "verification_status": verification_status,
            "seed_policy": seed_policy,
            "random_seed": random_seed,
            "config_capture_complete": config_capture_complete,
            "missing_items": missing_items,
            "factor_count": len(factor_list),
            "metric_count": len(metric_records),
            "curve_count": len(curves),
            "factor_importance_count": len(factor_importance),
            "symbol_summary_count": len(symbol_summaries),
            "trade_count": len(trades),
            "execution_event_count": len(execution_events),
            "raw_payload_sha256": sha256_json(data),
            "package_versions": runtime_metadata["package_versions"],
            "deterministic_flags": runtime_metadata["deterministic_flags"],
            "git_commit": runtime_metadata["git_commit"],
            "runner_script": runtime_metadata["runner_script"],
            "source_config_paths": runtime_metadata["source_config_paths"],
            "python_version": runtime_metadata["python_version"],
            "qlib_version": runtime_metadata["qlib_version"],
            "mlflow_version": runtime_metadata["mlflow_version"],
            "torch_version": runtime_metadata["torch_version"],
        }
        manifest = ReproducibilityManifestRecord(
            run_id=run_id,
            manifest_schema_version=DEFAULT_MANIFEST_SCHEMA_VERSION,
            reproducibility_level=reproducibility_level,
            verification_status=verification_status,
            config_sha256=config.config_sha256,
            canonical_config_sha256=sha256_json(config.canonical_config),
            raw_config_sha256=sha256_json(config.raw_config),
            factor_set_hash=factor_set_hash,
            model_params_sha256=sha256_json(config.model_params),
            strategy_config_sha256=sha256_json(config.strategy_config),
            data_context_sha256=sha256_json(data_context),
            metrics_payload_sha256=sha256_json(metrics) if metrics else None,
            enhanced_metrics_sha256=sha256_json(enhanced) if enhanced else None,
            artifact_manifest_sha256=sha256_json(data.get("artifact_manifest")) if data.get("artifact_manifest") else None,
            git_commit=runtime_metadata["git_commit"],
            git_dirty=runtime_metadata["git_dirty"],
            runner_script=runtime_metadata["runner_script"],
            runner_script_sha256=runtime_metadata["runner_script_sha256"],
            python_version=runtime_metadata["python_version"],
            qlib_version=runtime_metadata["qlib_version"],
            mlflow_version=runtime_metadata["mlflow_version"],
            torch_version=runtime_metadata["torch_version"],
            package_versions=runtime_metadata["package_versions"],
            random_seed=random_seed,
            deterministic_flags=runtime_metadata["deterministic_flags"],
            source_config_paths=runtime_metadata["source_config_paths"],
            missing_items=missing_items,
            manifest_json=manifest_json,
        )

        source = RunSourceRecord(
            run_id=run_id,
            source_system=source_system,
            source_type="evolution_loop_completion" if run_type == "evolution_loop" else "experiment_completion",
            source_id=source_id,
            source_sub_id=source_sub_id,
            source_status=run.status,
            source_uri=_optional_str(data.get("source_uri")),
            recorder_experiment_id=_optional_str(data.get("recorder_experiment_id")),
            recorder_id=_optional_str(data.get("recorder_id")),
            mlflow_tracking_uri=_optional_str(data.get("mlflow_tracking_uri")),
            mlflow_artifact_uri=_optional_str(data.get("mlflow_artifact_uri")),
            qlib_recorder_name=_optional_str(data.get("qlib_recorder_name")),
            node_api_base_url=_optional_str(data.get("node_api_base_url")),
            metadata={"event_type": event_type, "source_keys": sorted(str(k) for k in data.keys())},
        )

        return ExtractedArchivePayload(
            run=run,
            source=source,
            config=config,
            reproducibility_manifest=manifest,
            data_contexts=[data_context],
            account_summary=account_summary,
            metrics=metric_records,
            curves=curves,
            factors=factor_records,
            factor_importance=factor_importance,
            symbol_summaries=symbol_summaries,
            trades=trades,
            execution_events=execution_events,
            raw_payloads=raw_payloads,
            stats={
                "missing_items": missing_items,
                "factor_count": len(factor_records),
                "metric_count": len(metric_records),
                "curve_count": len(curves),
                "factor_importance_count": len(factor_importance),
                "symbol_summary_count": len(symbol_summaries),
                "trade_count": len(trades),
                "execution_event_count": len(execution_events),
                "raw_payload_count": len(raw_payloads),
                "research_valid": research_valid,
            },
        )

    def _extract_data_context(
        self,
        data: Mapping[str, Any],
        config: Mapping[str, Any],
        metrics: Mapping[str, Any],
        factor_set_hash: str,
    ) -> DataContextRecord:
        context = _ensure_mapping(data.get("data_context") or config.get("data_context") or {})
        backtest = _section(data, config, "backtest_config", "backtest")
        split = _section(data, config, "data_split", "split")
        execution = _section(data, config, "execution_config", "execution", "executor")
        enhanced = _ensure_mapping(metrics.get("enhanced_metrics") or {})
        return_curves = _ensure_mapping(enhanced.get("return_curves") or {})
        curve_dates = _as_list(return_curves.get("dates") or enhanced.get("return_dates") or enhanced.get("dates"))
        inferred_backtest_start = _as_date(curve_dates[0]) if curve_dates else None
        inferred_backtest_end = _as_date(curve_dates[-1]) if curve_dates else None
        explicit_limit_suspend = _as_bool(context.get("limit_suspend_authoritative"))
        if explicit_limit_suspend is None:
            explicit_limit_suspend = _as_bool(data.get("limit_suspend_authoritative"))
        inferred_limit_suspend = (
            str(context.get("limit_handling") or execution.get("limit_handling") or "").lower() in {"authoritative", "strict"}
            and str(context.get("suspend_handling") or execution.get("suspend_handling") or "").lower() in {"authoritative", "strict"}
        )

        return DataContextRecord(
            run_id="__pending__",
            context_type=str(context.get("context_type") or "primary"),
            freq=_optional_str(_first_value(context, ("freq", "frequency")) or data.get("freq") or config.get("freq") or metrics.get("freq")),
            market=_optional_str(_first_value(context, ("market",)) or data.get("market") or config.get("market")),
            universe=_optional_str(_first_value(context, ("universe", "stock_pool")) or data.get("universe") or config.get("universe")),
            benchmark=_optional_str(_first_value(context, ("benchmark", "bench")) or data.get("benchmark") or config.get("benchmark")),
            train_start=_as_date(_first_value(split, ("train_start", "train_begin")) or data.get("train_start")),
            train_end=_as_date(_first_value(split, ("train_end",)) or data.get("train_end")),
            valid_start=_as_date(_first_value(split, ("valid_start", "validation_start")) or data.get("valid_start")),
            valid_end=_as_date(_first_value(split, ("valid_end", "validation_end")) or data.get("valid_end")),
            test_start=_as_date(_first_value(split, ("test_start",)) or data.get("test_start")),
            test_end=_as_date(_first_value(split, ("test_end",)) or data.get("test_end")),
            backtest_start=_as_date(_first_value(context, ("backtest_start", "start_time", "start_date")) or _first_value(backtest, ("start_time", "start_date", "backtest_start")) or data.get("backtest_start")) or inferred_backtest_start,
            backtest_end=_as_date(_first_value(context, ("backtest_end", "end_time", "end_date")) or _first_value(backtest, ("end_time", "end_date", "backtest_end")) or data.get("backtest_end")) or inferred_backtest_end,
            label_horizon=_as_int(_first_value(context, ("label_horizon", "horizon")) or data.get("label_horizon") or config.get("label_horizon")),
            qlib_provider_uri=_optional_str(context.get("qlib_provider_uri") or config.get("provider_uri")),
            qlib_dataset_version=_optional_str(context.get("qlib_dataset_version") or data.get("qlib_dataset_version")),
            dataset_snapshot_id=_optional_str(context.get("dataset_snapshot_id") or data.get("dataset_snapshot_id")),
            feature_snapshot_id=_optional_str(context.get("feature_snapshot_id") or data.get("feature_snapshot_id")),
            factor_cache_snapshot_id=_optional_str(context.get("factor_cache_snapshot_id") or factor_set_hash),
            data_version_hash=_optional_str(context.get("data_version_hash") or data.get("data_version_hash")),
            pit_cutoff_date=_as_date(context.get("pit_cutoff_date") or data.get("pit_cutoff_date")),
            limit_handling=_optional_str(context.get("limit_handling") or execution.get("limit_handling")),
            suspend_handling=_optional_str(context.get("suspend_handling") or execution.get("suspend_handling")),
            limit_suspend_authoritative=explicit_limit_suspend if explicit_limit_suspend is not None else inferred_limit_suspend,
            cost_config=_ensure_mapping(context.get("cost_config") or execution.get("cost_config") or {}),
            stock_pool_config=_ensure_mapping(context.get("stock_pool_config") or config.get("stock_pool_config") or {}),
            data_quality_flags=_ensure_mapping(context.get("data_quality_flags") or {}),
        )

    def _missing_items(
        self,
        config: Mapping[str, Any],
        factor_list: Sequence[Any],
        metrics: Mapping[str, Any],
        enhanced: Mapping[str, Any],
        data_context: DataContextRecord,
    ) -> list[str]:
        missing = []
        if not config:
            missing.append("canonical_config")
        if not factor_list:
            missing.append("factor_list")
        if not metrics:
            missing.append("metrics")
        if not enhanced:
            missing.append("enhanced_metrics")
        if not data_context.backtest_start or not data_context.backtest_end:
            missing.append("backtest_window")
        return missing

    def _extract_account_summary(
        self,
        run_id: str,
        data: Mapping[str, Any],
        metrics: Mapping[str, Any],
        enhanced: Mapping[str, Any],
        summary: Mapping[str, Any],
    ) -> AccountSummaryRecord | None:
        account = _ensure_mapping(data.get("account_summary") or metrics.get("account_summary") or {})
        absolute = _ensure_mapping(enhanced.get("absolute_returns") or metrics.get("absolute_returns") or {})
        position = _ensure_mapping(enhanced.get("position_summary") or enhanced.get("holding_audit") or {})
        sources = [account, absolute, summary, metrics, data]
        record = AccountSummaryRecord(
            run_id=run_id,
            initial_capital=_pick_float(sources, "initial_capital", "start_value", "initial_cash"),
            final_total_value=_pick_float(sources, "final_total_value", "final_value", "final_account"),
            final_account_value=_pick_float(sources, "final_account_value", "final_account"),
            final_nav_value=_pick_float(sources, "final_nav_value", "final_nav", "nav"),
            total_return=_pick_float(sources, "total_return", "absolute_return", "return"),
            cagr=_pick_float(sources, "cagr", "annualized_return", "excess_return_with_cost_annualized", "1day.excess_return_with_cost.annualized_return"),
            max_drawdown=_pick_float(sources, "max_drawdown", "excess_return_with_cost_max_drawdown", "1day.excess_return_with_cost.max_drawdown"),
            max_drawdown_date=_pick_date(sources, "max_drawdown_date", "mdd_date"),
            sharpe=_pick_float(sources, "sharpe", "sharpe_ratio"),
            annualized_volatility=_pick_float(sources, "annualized_volatility", "volatility"),
            avg_cash_ratio=_pick_float([absolute, position, account], "avg_cash_ratio", "cash_ratio_avg"),
            final_cash=_pick_float(sources, "final_cash", "cash"),
            final_stock_value=_pick_float(sources, "final_stock_value", "stock_value"),
            final_stock_count=_pick_int([absolute, position, account], "final_stock_count", "stock_count"),
            final_cash_ratio=_pick_float(sources, "final_cash_ratio"),
            n_trading_days=_pick_int(sources, "n_trading_days", "trading_days"),
            position_count_min=_pick_float([position, absolute, account], "position_count_min"),
            position_count_avg=_pick_float([position, absolute, account], "position_count_avg"),
            position_count_max=_pick_float([position, absolute, account], "position_count_max"),
            position_count_p95=_pick_float([position, absolute, account], "position_count_p95"),
            source_payload_path="metrics.enhanced_metrics.absolute_returns",
            metadata={"sources": ["account_summary", "absolute_returns", "summary", "metrics"]},
        )
        values = [v for k, v in record.__dict__.items() if k not in {"run_id", "metadata", "source_payload_path"}]
        return record if any(v is not None for v in values) else None

    def _extract_metrics(
        self,
        run_id: str,
        metrics: Mapping[str, Any],
        enhanced: Mapping[str, Any],
    ) -> list[MetricRecord]:
        records: list[MetricRecord] = []
        for key, value in metrics.items():
            if key == "enhanced_metrics":
                continue
            if _is_metric_value(value):
                records.append(_metric_record(run_id, key, value, "run", f"metrics.{key}"))

        for scope, obj, prefix in (
            ("summary", _ensure_mapping(metrics.get("summary") or enhanced.get("summary") or {}), "summary"),
            ("account", _ensure_mapping(enhanced.get("absolute_returns") or {}), "absolute_returns"),
            ("position", _ensure_mapping(enhanced.get("position_summary") or enhanced.get("holding_audit") or {}), "position_summary"),
            ("training", _ensure_mapping(enhanced.get("training_diagnostics") or {}), "training_diagnostics"),
        ):
            for key, value in obj.items():
                if _is_metric_value(value):
                    records.append(_metric_record(run_id, key, value, scope, f"enhanced_metrics.{prefix}.{key}"))

        deduped: dict[tuple[str, str, str | None], MetricRecord] = {}
        for record in records:
            deduped[(record.metric_key, record.metric_scope, record.source_key)] = record
        return list(deduped.values())

    def _extract_curves(self, run_id: str, enhanced: Mapping[str, Any]) -> list[CurveRecord]:
        records: list[CurveRecord] = []
        ic = _ensure_mapping(enhanced.get("ic_diagnostics") or {})
        ic_dates = _as_list(ic.get("ic_dates") or ic.get("dates") or enhanced.get("dates"))
        for key in ("ic_series", "rank_ic_series", "ic_rolling_30d_mean", "ic_rolling_30d_std", "ic_positive_ratio"):
            records.extend(_date_curve_records(run_id, key, ic.get(key), ic_dates, f"enhanced_metrics.ic_diagnostics.{key}"))

        rc = _ensure_mapping(enhanced.get("return_curves") or {})
        return_dates = _as_list(rc.get("dates") or enhanced.get("return_dates") or enhanced.get("dates"))
        for key in ("cumulative_excess_no_cost", "cumulative_excess_with_cost", "cumulative_benchmark", "drawdown_series"):
            records.extend(_date_curve_records(run_id, key, rc.get(key) or enhanced.get(key), return_dates, f"enhanced_metrics.return_curves.{key}"))

        training = _ensure_mapping(enhanced.get("training_diagnostics") or {})
        for key, split in (("train_loss_curve", "train"), ("val_loss_curve", "validation")):
            values = _as_list(training.get(key) or enhanced.get(key))
            for step, value in enumerate(values):
                if _as_float(value) is None:
                    continue
                records.append(
                    CurveRecord(
                        run_id=run_id,
                        curve_key=key,
                        step=step,
                        split_name=split,
                        value_num=_as_float(value),
                        source_key=f"enhanced_metrics.training_diagnostics.{key}",
                    )
                )
        return records

    def _extract_factors(self, run_id: str, factor_items: Sequence[Any]) -> list[RunFactorRecord]:
        records: list[RunFactorRecord] = []
        for idx, item in enumerate(factor_items):
            if isinstance(item, Mapping):
                name = _optional_str(item.get("factor_name") or item.get("name") or item.get("field"))
                if not name:
                    continue
                records.append(
                    RunFactorRecord(
                        run_id=run_id,
                        factor_name=name,
                        factor_catalog_id=_as_int(item.get("factor_catalog_id") or item.get("catalog_id")),
                        factor_source=_optional_str(item.get("factor_source") or item.get("source")),
                        factor_version=_optional_str(item.get("factor_version") or item.get("version")),
                        factor_order=_as_int(item.get("factor_order")) if item.get("factor_order") is not None else idx,
                        factor_group=_optional_str(item.get("factor_group") or item.get("group")),
                        factor_classification=_ensure_mapping(item.get("factor_classification") or item.get("classification") or {}),
                        factor_expression_hash=_optional_str(item.get("factor_expression_hash")),
                        factor_asset_hash=_optional_str(item.get("factor_asset_hash")),
                        inclusion_reason=_optional_str(item.get("inclusion_reason")),
                        inclusion_source=_optional_str(item.get("inclusion_source")),
                        is_alpha158=bool(_as_bool(item.get("is_alpha158")) or False),
                        independent_metrics_snapshot=_ensure_mapping(item.get("independent_metrics_snapshot") or item.get("metrics") or {}),
                        official_rating_snapshot=_ensure_mapping(item.get("official_rating_snapshot") or item.get("rating") or {}),
                        correlation_cluster=_optional_str(item.get("correlation_cluster")),
                    )
                )
            else:
                name = _optional_str(item)
                if name:
                    records.append(RunFactorRecord(run_id=run_id, factor_name=name, factor_order=idx))
        return records

    def _extract_factor_importance(
        self,
        run_id: str,
        data: Mapping[str, Any],
        metrics: Mapping[str, Any],
        enhanced: Mapping[str, Any],
    ) -> list[RunFactorImportanceRecord]:
        sources: list[tuple[str, Any, str]] = []
        for path, value, default_method in (
            ("enhanced_metrics.factor_analysis.feature_importance", _ensure_mapping(enhanced.get("factor_analysis")).get("feature_importance"), "feature_importance"),
            ("enhanced_metrics.feature_importance", enhanced.get("feature_importance"), "feature_importance"),
            ("metrics.factor_importance", metrics.get("factor_importance"), "factor_importance"),
            ("data.factor_importance", data.get("factor_importance"), "factor_importance"),
            ("enhanced_metrics.pytorch_correlation", enhanced.get("pytorch_correlation"), "pytorch_correlation"),
            ("metrics.pytorch_correlation", metrics.get("pytorch_correlation"), "pytorch_correlation"),
        ):
            if value not in (None, {}, []):
                sources.append((path, value, default_method))

        raw_records: list[dict[str, Any]] = []
        for source_path, value, default_method in sources:
            for row_idx, row in enumerate(_iter_importance_rows(value)):
                factor_name = _optional_str(
                    row.get("factor_name")
                    or row.get("name")
                    or row.get("factor")
                    or row.get("feature")
                    or row.get("feature_name")
                )
                raw_value = _as_float(
                    row.get("importance_value")
                    if row.get("importance_value") is not None
                    else row.get("importance")
                    if row.get("importance") is not None
                    else row.get("gain")
                    if row.get("gain") is not None
                    else row.get("value")
                    if row.get("value") is not None
                    else row.get("weight_pct")
                    if row.get("weight_pct") is not None
                    else row.get("gain_pct")
                    if row.get("gain_pct") is not None
                    else row.get("normalized_value")
                )
                if not factor_name or raw_value is None:
                    continue
                normalized = _as_float(row.get("normalized_value"))
                if normalized is None:
                    normalized = _as_float(row.get("weight_pct") if row.get("weight_pct") is not None else row.get("gain_pct"))
                    if normalized is not None and normalized > 1:
                        normalized = normalized / 100.0
                raw_records.append(
                    {
                        "run_id": run_id,
                        "factor_catalog_id": _as_int(row.get("factor_catalog_id") or row.get("catalog_id")),
                        "factor_name": factor_name,
                        "feature_name": _optional_str(row.get("feature_name") or row.get("feature")),
                        "feature_index": _as_int(row.get("feature_index") or row.get("index")),
                        "model_family": _optional_str(row.get("model_family") or data.get("model_family")),
                        "model_type": _optional_str(row.get("model_type") or data.get("model_type") or data.get("model_name")),
                        "method": _optional_str(row.get("method")) or default_method,
                        "method_version": _optional_str(row.get("method_version")),
                        "split_name": _optional_str(row.get("split_name") or row.get("split")),
                        "time_bucket": _optional_str(row.get("time_bucket")),
                        "epoch": _as_int(row.get("epoch")),
                        "step": _as_int(row.get("step")),
                        "importance_value": raw_value,
                        "normalized_value": normalized,
                        "signed_value": _as_float(row.get("signed_value") or row.get("raw_value")),
                        "rank_in_run": _as_int(row.get("rank_in_run") or row.get("rank")),
                        "sample_count": _as_int(row.get("sample_count") or row.get("n")),
                        "reliability": _optional_str(row.get("reliability")) or "unknown",
                        "metadata": {
                            "source_payload_path": source_path,
                            "source_row_index": row_idx,
                            "source_keys": sorted(str(key) for key in row.keys()),
                        },
                    }
                )

        if not raw_records:
            return []

        total_abs_by_method: dict[str, float] = {}
        for record in raw_records:
            if record.get("normalized_value") is None:
                total_abs_by_method[record["method"]] = total_abs_by_method.get(record["method"], 0.0) + abs(float(record["importance_value"]))
        for record in raw_records:
            if record.get("normalized_value") is None:
                total = total_abs_by_method.get(record["method"], 0.0)
                record["normalized_value"] = abs(float(record["importance_value"])) / total if total else None

        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in raw_records:
            grouped.setdefault(str(record["method"]), []).append(record)
        for rows in grouped.values():
            rows.sort(key=lambda item: (item.get("normalized_value") is None, -(item.get("normalized_value") or 0.0), -abs(float(item["importance_value"]))))
            for idx, record in enumerate(rows, start=1):
                record.setdefault("rank_in_run", idx)
                if record.get("rank_in_run") is None:
                    record["rank_in_run"] = idx

        return [RunFactorImportanceRecord(**record) for record in raw_records]

    def _extract_symbol_summaries(
        self,
        run_id: str,
        enhanced: Mapping[str, Any],
        metrics: Mapping[str, Any],
    ) -> list[SymbolSummaryRecord]:
        enhanced_metrics = _ensure_mapping(metrics.get("enhanced_metrics") or {})
        sources = [enhanced, enhanced_metrics, metrics]
        records: list[SymbolSummaryRecord] = []
        for source_list in ("all_stocks", "top_stocks", "bottom_stocks"):
            value = _first_non_empty_value(sources, source_list)
            for idx, symbol, item in _iter_symbol_rows(value):
                records.append(
                    SymbolSummaryRecord(
                        run_id=run_id,
                        symbol=symbol,
                        source_list=source_list,
                        profit=_as_float(_first_value(item, ("profit", "pnl", "total_profit", "return_value"))),
                        profit_pct=_as_float(_first_value(item, ("profit_pct", "return_pct", "return", "pct"))),
                        avg_cost=_as_float(_first_value(item, ("avg_cost", "average_cost", "cost"))),
                        last_price=_as_float(_first_value(item, ("last_price", "price", "close"))),
                        holding_days=_as_int(_first_value(item, ("holding_days", "hold_days", "days"))),
                        first_date=_as_date(_first_value(item, ("first_date", "start_date", "buy_date", "entry_date"))),
                        last_date=_as_date(_first_value(item, ("last_date", "end_date", "sell_date", "exit_date"))),
                        rank_in_list=idx + 1,
                        metadata={
                            "source_payload_path": f"enhanced_metrics.{source_list}[{idx}]",
                            "source_keys": sorted(str(key) for key in item.keys()),
                            "raw": dict(item),
                        },
                    )
                )
        deduped: dict[tuple[str, str], SymbolSummaryRecord] = {}
        for record in records:
            deduped.setdefault((record.source_list, record.symbol), record)
        return list(deduped.values())

    def _extract_trades(
        self,
        run_id: str,
        enhanced: Mapping[str, Any],
        metrics: Mapping[str, Any],
    ) -> list[TradeRecord]:
        enhanced_metrics = _ensure_mapping(metrics.get("enhanced_metrics") or {})
        sources = [enhanced, enhanced_metrics, metrics]
        source_key, value = _first_named_non_empty_value(
            sources,
            ("stock_trades", "trades", "trade_records", "trade_details"),
        )
        if not source_key:
            return []

        records: list[TradeRecord] = []
        for idx, symbol, item, path_suffix in _iter_trade_rows(value):
            source_payload_path = f"enhanced_metrics.{source_key}{path_suffix}"
            quantity = _as_float(_first_value(item, ("quantity", "qty", "shares", "volume", "vol")))
            amount = _as_float(_first_value(item, ("amount", "trade_amount", "value", "cash", "money")))
            raw_uid = _optional_str(_first_value(item, ("trade_uid", "trade_id", "id", "fill_id")))
            records.append(
                TradeRecord(
                    run_id=run_id,
                    trade_uid=raw_uid
                    or f"qear_trd_{sha256_json({'run_id': run_id, 'source_payload_path': source_payload_path, 'idx': idx, 'record': item})[:24]}",
                    order_uid=_optional_str(_first_value(item, ("order_uid", "order_id"))),
                    trade_date=_as_date(_first_value(item, ("trade_date", "date", "datetime", "ts", "time"))),
                    ts=_as_datetime(_first_value(item, ("ts", "datetime", "time"))),
                    symbol=symbol,
                    side=_normalize_side(_first_value(item, ("side", "type", "action", "direction"))),
                    price=_as_float(_first_value(item, ("price", "trade_price", "fill_price", "deal_price"))),
                    quantity=quantity,
                    amount=amount,
                    commission=_as_float(_first_value(item, ("commission", "fee", "cost"))),
                    tax=_as_float(_first_value(item, ("tax", "stamp_tax"))),
                    slippage=_as_float(_first_value(item, ("slippage", "slippage_cost"))),
                    pnl=_as_float(_first_value(item, ("pnl", "profit", "realized_pnl"))),
                    source_payload_path=source_payload_path,
                    metadata={
                        "source_keys": sorted(str(key) for key in item.keys()),
                        "amount_semantics": "source_reported_amount_without_quantity"
                        if amount is not None and quantity is None
                        else "source_reported",
                        "raw": dict(item),
                    },
                )
            )
        return records

    def _extract_execution_events(
        self,
        run_id: str,
        data: Mapping[str, Any],
        metrics: Mapping[str, Any],
        enhanced: Mapping[str, Any],
        *,
        symbol_summary_count: int,
        trade_count: int,
    ) -> list[ExecutionEventRecord]:
        event_ts = (
            _as_datetime(data.get("completed_at"))
            or _as_datetime(data.get("finished_at"))
            or _as_datetime(data.get("source_updated_at"))
            or datetime.now(timezone.utc)
        )
        records: list[ExecutionEventRecord] = []
        if symbol_summary_count or trade_count:
            records.append(
                ExecutionEventRecord(
                    run_id=run_id,
                    event_ts=event_ts,
                    event_type="archive_parser.structured_payload_extracted",
                    severity="info",
                    message="Structured symbol and trade data extracted from already archived QE payloads.",
                    metadata={
                        "symbol_summary_count": symbol_summary_count,
                        "trade_count": trade_count,
                        "source_fields": ["all_stocks", "top_stocks", "bottom_stocks", "stock_trades"],
                    },
                )
            )

        trade_diagnostics = _ensure_mapping(enhanced.get("trade_diagnostics") or metrics.get("trade_diagnostics") or {})
        if trade_diagnostics:
            records.append(
                ExecutionEventRecord(
                    run_id=run_id,
                    event_ts=event_ts,
                    event_type="source.trade_diagnostics",
                    severity="info",
                    message="Trade diagnostics captured from enhanced metrics payload.",
                    metadata=dict(trade_diagnostics),
                )
            )

        execution_trace = _ensure_mapping(metrics.get("execution_trace") or data.get("execution_trace") or {})
        if execution_trace:
            records.append(
                ExecutionEventRecord(
                    run_id=run_id,
                    event_ts=event_ts,
                    event_type="source.execution_trace",
                    severity="info",
                    message="Execution trace metadata captured from QE metrics payload.",
                    metadata=dict(execution_trace),
                )
            )
        return records

    def _raw_payloads(
        self,
        run_id: str,
        source_system: str,
        source_id: str,
        payload: Mapping[str, Any],
        metrics: Mapping[str, Any],
        enhanced: Mapping[str, Any],
    ) -> list[RawPayloadRecord]:
        raw = [
            RawPayloadRecord(
                run_id=run_id,
                payload_type="qe_completion_payload",
                source_system=source_system,
                source_id=source_id,
                payload_json=dict(payload),
                provenance_level="direct",
            )
        ]
        if metrics:
            raw.append(
                RawPayloadRecord(
                    run_id=run_id,
                    payload_type="qe_metrics_payload",
                    source_system=source_system,
                    source_id=source_id,
                    payload_json=dict(metrics),
                    provenance_level="direct",
                )
            )
        if enhanced:
            raw.append(
                RawPayloadRecord(
                    run_id=run_id,
                    payload_type="qe_enhanced_metrics_payload",
                    source_system=source_system,
                    source_id=source_id,
                    payload_json=dict(enhanced),
                    provenance_level="direct",
                )
            )
        return raw



def _iter_importance_rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, Mapping):
        rows: list[dict[str, Any]] = []
        for key, val in value.items():
            if isinstance(val, Mapping):
                row = dict(val)
                row.setdefault("factor_name", key)
                rows.append(row)
            elif isinstance(val, Number):
                rows.append({"factor_name": key, "importance_value": val})
            elif isinstance(val, (list, tuple)) and val and isinstance(val[0], Number):
                rows.append({"factor_name": key, "importance_value": val[0]})
        return rows
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        rows = []
        for item in value:
            if isinstance(item, Mapping):
                rows.append(dict(item))
            elif isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)) and len(item) >= 2:
                rows.append({"factor_name": item[0], "importance_value": item[1]})
        return rows
    return []


def _extract_random_seed(data: Mapping[str, Any], config: Mapping[str, Any] | RunConfigRecord) -> int | None:
    config_sources: list[Mapping[str, Any]] = []
    if isinstance(config, RunConfigRecord):
        canonical_config = _ensure_mapping(config.canonical_config)
        raw_config = _ensure_mapping(config.raw_config)
        config_sources.extend(
            [
                canonical_config,
                raw_config,
                config.model_params or {},
                config.strategy_config or {},
                _ensure_mapping(canonical_config.get("runtime_flags")),
                _ensure_mapping(canonical_config.get("model_params")),
                _ensure_mapping(canonical_config.get("params")),
                _ensure_mapping(canonical_config.get("custom_params")),
                _ensure_mapping(raw_config.get("runtime_flags")),
                _ensure_mapping(raw_config.get("model_params")),
                _ensure_mapping(raw_config.get("params")),
                _ensure_mapping(raw_config.get("custom_params")),
            ]
        )
    else:
        config_map = _ensure_mapping(config)
        config_sources.extend(
            [
                config_map,
                _ensure_mapping(config_map.get("runtime_flags")),
                _ensure_mapping(config_map.get("model_params")),
                _ensure_mapping(config_map.get("params")),
                _ensure_mapping(config_map.get("custom_params")),
            ]
        )

    data_config = _ensure_mapping(data.get("config"))
    execution_manifest = _ensure_mapping(data.get("execution_manifest"))
    requested_manifest = _ensure_mapping(execution_manifest.get("requested"))
    for source in (
        data,
        _ensure_mapping(data.get("runtime_flags")),
        _ensure_mapping(data.get("model_params")),
        _ensure_mapping(data.get("params")),
        data_config,
        _ensure_mapping(data_config.get("runtime_flags")),
        _ensure_mapping(data_config.get("model_params")),
        _ensure_mapping(data_config.get("params")),
        _ensure_mapping(data_config.get("custom_params")),
        execution_manifest,
        requested_manifest,
        _ensure_mapping(requested_manifest.get("runtime_flags")),
        _ensure_mapping(requested_manifest.get("model_params")),
        *config_sources,
    ):
        for key in ("random_seed", "seed", "loop_seed", "random_state"):
            value = source.get(key) if isinstance(source, Mapping) else None
            if value not in (None, ""):
                parsed = _as_int(value)
                if parsed is not None:
                    return parsed
    return None


def _extract_reproducibility_runtime_metadata(
    data: Mapping[str, Any],
    config: RunConfigRecord,
    *,
    random_seed: int | None,
    seed_policy: str,
) -> dict[str, Any]:
    canonical_config = _ensure_mapping(config.canonical_config)
    raw_config = _ensure_mapping(config.raw_config)
    execution_manifest = _ensure_mapping(data.get("execution_manifest") or canonical_config.get("execution_manifest"))
    artifact_manifest = _ensure_mapping(data.get("artifact_manifest") or canonical_config.get("artifact_manifest"))
    runtime_flags = _merge_mappings(
        canonical_config.get("runtime_flags"),
        raw_config.get("runtime_flags"),
        data.get("runtime_flags"),
        execution_manifest.get("runtime_flags"),
        _ensure_mapping(execution_manifest.get("requested")).get("runtime_flags"),
    )
    environment = _merge_mappings(
        data.get("environment"),
        data.get("env"),
        data.get("runtime_environment"),
        canonical_config.get("environment"),
        artifact_manifest.get("environment"),
        execution_manifest.get("environment"),
    )
    package_versions = _merge_mappings(
        data.get("package_versions"),
        environment.get("package_versions"),
        artifact_manifest.get("package_versions"),
        execution_manifest.get("package_versions"),
    )
    for key, package_key in (("qlib_version", "qlib"), ("mlflow_version", "mlflow"), ("torch_version", "torch")):
        value = _first_non_empty_value([data, environment, package_versions], key) or package_versions.get(package_key)
        if value not in (None, "", [], {}):
            package_versions.setdefault(package_key, value)

    deterministic_flags = _merge_mappings(
        data.get("deterministic_flags"),
        data.get("nondeterministic_flags"),
        environment.get("deterministic_flags"),
        runtime_flags.get("deterministic_flags"),
        runtime_flags.get("nondeterministic_flags"),
        execution_manifest.get("deterministic_flags"),
    )
    deterministic_flags.setdefault("seed_policy", seed_policy)
    if random_seed is not None:
        deterministic_flags.setdefault("random_seed", random_seed)
    if "torch_deterministic" not in deterministic_flags:
        torch_deterministic = _first_non_empty_value([runtime_flags, environment], "torch_deterministic")
        if torch_deterministic not in (None, "", [], {}):
            deterministic_flags["torch_deterministic"] = torch_deterministic
    if "cudnn_deterministic" not in deterministic_flags:
        cudnn_deterministic = _first_non_empty_value([runtime_flags, environment], "cudnn_deterministic")
        if cudnn_deterministic not in (None, "", [], {}):
            deterministic_flags["cudnn_deterministic"] = cudnn_deterministic

    source_config_paths = _merge_mappings(
        data.get("source_config_paths"),
        data.get("config_paths"),
        artifact_manifest.get("source_config_paths"),
        execution_manifest.get("source_config_paths"),
    )
    for key in ("config_path", "conf_yaml_path", "qlib_config_path", "run_log_path"):
        value = _first_non_empty_value([data, artifact_manifest, execution_manifest], key)
        if value not in (None, "", [], {}):
            source_config_paths.setdefault(key, value)

    git_dirty_value = _first_non_empty_value([data, environment, artifact_manifest, execution_manifest], "git_dirty")
    return {
        "git_commit": _optional_str(
            _first_non_empty_value(
                [data, environment, artifact_manifest, execution_manifest],
                "git_commit",
            )
            or _first_non_empty_value([data, environment, artifact_manifest, execution_manifest], "commit_sha")
        ),
        "git_dirty": _as_bool(git_dirty_value),
        "runner_script": _optional_str(
            _first_non_empty_value([data, artifact_manifest, execution_manifest], "runner_script")
            or _first_non_empty_value([data, artifact_manifest, execution_manifest], "entrypoint")
        ),
        "runner_script_sha256": _optional_str(
            _first_non_empty_value([data, artifact_manifest, execution_manifest], "runner_script_sha256")
        ),
        "python_version": _optional_str(_first_non_empty_value([data, environment, package_versions], "python_version")),
        "qlib_version": _optional_str(_first_non_empty_value([data, environment, package_versions], "qlib_version") or package_versions.get("qlib")),
        "mlflow_version": _optional_str(_first_non_empty_value([data, environment, package_versions], "mlflow_version") or package_versions.get("mlflow")),
        "torch_version": _optional_str(_first_non_empty_value([data, environment, package_versions], "torch_version") or package_versions.get("torch")),
        "package_versions": package_versions,
        "deterministic_flags": deterministic_flags,
        "source_config_paths": source_config_paths,
    }


def _ensure_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _merge_mappings(*values: Any) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for value in values:
        mapping = _ensure_mapping(value)
        for key, item in mapping.items():
            if item not in (None, "", [], {}):
                merged[str(key)] = item
    return merged


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Number):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Number):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except ValueError:
            return None
    return None


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Number):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
    return None


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _first_value(mapping: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, "", [], {}):
            return value
    return None


def _first_non_empty_value(sources: Sequence[Mapping[str, Any]], key: str) -> Any:
    for source in sources:
        value = source.get(key)
        if value not in (None, "", [], {}):
            return value
    return None


def _first_named_non_empty_value(
    sources: Sequence[Mapping[str, Any]],
    keys: Sequence[str],
) -> tuple[str | None, Any]:
    for key in keys:
        value = _first_non_empty_value(sources, key)
        if value not in (None, "", [], {}):
            return key, value
    return None, None


def _section(data: Mapping[str, Any], config: Mapping[str, Any], *keys: str) -> dict[str, Any]:
    for source in (data, config):
        for key in keys:
            value = source.get(key)
            if isinstance(value, Mapping):
                return dict(value)
    return {}


def _extract_factor_items(data: Mapping[str, Any], config: Mapping[str, Any]) -> list[Any]:
    for source in (data, config):
        for key in ("factor_list", "factors", "factor_names", "features", "feature_names"):
            value = source.get(key)
            if isinstance(value, Mapping):
                return list(value.values())
            if isinstance(value, (list, tuple)):
                return list(value)
    model = _ensure_mapping(config.get("model") or {})
    for key in ("features", "feature_names"):
        value = model.get(key)
        if isinstance(value, (list, tuple)):
            return list(value)
    return []


def _iter_symbol_rows(value: Any) -> list[tuple[int, str, dict[str, Any]]]:
    rows: list[tuple[int, str, dict[str, Any]]] = []
    if isinstance(value, Mapping):
        iterable = list(value.items())
        for idx, (symbol_key, item) in enumerate(iterable):
            if isinstance(item, Mapping):
                record = dict(item)
            else:
                record = {"value": item}
            symbol = _symbol_from_record(record, fallback=symbol_key)
            if symbol:
                rows.append((idx, symbol, record))
        return rows
    for idx, item in enumerate(_as_list(value)):
        if not isinstance(item, Mapping):
            continue
        record = dict(item)
        symbol = _symbol_from_record(record)
        if symbol:
            rows.append((idx, symbol, record))
    return rows


def _iter_trade_rows(value: Any) -> list[tuple[int, str, dict[str, Any], str]]:
    rows: list[tuple[int, str, dict[str, Any], str]] = []
    if isinstance(value, Mapping):
        ordinal = 0
        for symbol_key, trades in value.items():
            symbol = _optional_str(symbol_key)
            if isinstance(trades, Mapping):
                trades_iter = [trades]
            else:
                trades_iter = _as_list(trades)
            for idx, item in enumerate(trades_iter):
                if not isinstance(item, Mapping):
                    continue
                record = dict(item)
                trade_symbol = _symbol_from_record(record, fallback=symbol)
                if not trade_symbol:
                    continue
                rows.append((ordinal, trade_symbol, record, f".{trade_symbol}[{idx}]"))
                ordinal += 1
        return rows
    for idx, item in enumerate(_as_list(value)):
        if not isinstance(item, Mapping):
            continue
        record = dict(item)
        symbol = _symbol_from_record(record)
        if symbol:
            rows.append((idx, symbol, record, f"[{idx}]"))
    return rows


def _symbol_from_record(record: Mapping[str, Any], fallback: Any = None) -> str | None:
    return _optional_str(
        _first_value(record, ("symbol", "code", "stock", "stock_code", "instrument", "ticker"))
        or fallback
    )


def _normalize_side(value: Any) -> str | None:
    text = _optional_str(value)
    if not text:
        return None
    lowered = text.lower()
    aliases = {
        "buy": "buy",
        "b": "buy",
        "long": "buy",
        "open_long": "buy",
        "买": "buy",
        "买入": "buy",
        "sell": "sell",
        "s": "sell",
        "short": "sell",
        "close": "sell",
        "close_long": "sell",
        "卖": "sell",
        "卖出": "sell",
    }
    return aliases.get(lowered, lowered)


def _factor_display_value(item: Any) -> Any:
    if isinstance(item, Mapping):
        return {
            "name": item.get("factor_name") or item.get("name") or item.get("field"),
            "source": item.get("factor_source") or item.get("source"),
            "version": item.get("factor_version") or item.get("version"),
        }
    return item


def _run_type(data: Mapping[str, Any], event_type: str | None) -> str:
    if data.get("run_type"):
        return str(data["run_type"])
    if event_type and "loop" in event_type:
        return "evolution_loop"
    if data.get("loop_id") is not None or data.get("loop_index") is not None:
        return "evolution_loop"
    return "single_experiment"


def _research_validity(data: Mapping[str, Any], context: DataContextRecord) -> tuple[bool, str | None, list[str]]:
    explicit = data.get("research_valid")
    if explicit is False:
        return False, _optional_str(data.get("invalid_reason")) or "source_marked_invalid", list(data.get("exclusion_tags") or [])
    freq = (context.freq or "").lower()
    if freq in {"day", "daily", "1day", "1d"} and not context.limit_suspend_authoritative:
        return False, "daily_backtest_without_authoritative_limit_suspend", ["daily_no_limit_suspend_authoritative"]
    return True, None, list(data.get("exclusion_tags") or [])


def _is_metric_value(value: Any) -> bool:
    if value is None or isinstance(value, bool):
        return False
    return isinstance(value, (Number, str))


def _metric_key(key: str) -> str:
    aliases = {
        "IC": "ic",
        "ICIR": "icir",
        "Rank IC": "rank_ic",
        "Rank_IC": "rank_ic",
        "Rank ICIR": "rank_icir",
        "Rank_ICIR": "rank_icir",
        "excess_return_with_cost_annualized": "annualized_return",
        "1day.excess_return_with_cost.annualized_return": "annualized_return",
        "excess_return_with_cost_max_drawdown": "max_drawdown",
        "1day.excess_return_with_cost.max_drawdown": "max_drawdown",
        "excess_return_with_cost_IR": "information_ratio",
        "1day.excess_return_with_cost.information_ratio": "information_ratio",
    }
    return aliases.get(str(key), str(key))


def _metric_record(run_id: str, key: str, value: Any, scope: str, source_key: str) -> MetricRecord:
    num = _as_float(value)
    return MetricRecord(
        run_id=run_id,
        metric_key=_metric_key(key),
        metric_scope=scope,
        value_num=num,
        value_text=None if num is not None else str(value),
        source_key=source_key,
    )


def _date_curve_records(
    run_id: str,
    curve_key: str,
    values: Any,
    dates: Sequence[Any],
    source_key: str,
) -> list[CurveRecord]:
    items = _as_list(values)
    if not items:
        return []
    records = []
    for idx, value in enumerate(items):
        num = _as_float(value)
        if num is None:
            continue
        trade_date = _as_date(dates[idx]) if idx < len(dates) else None
        records.append(
            CurveRecord(
                run_id=run_id,
                curve_key=curve_key,
                trade_date=trade_date,
                step=None if trade_date else idx,
                value_num=num,
                source_key=source_key,
            )
        )
    return records


def _pick_float(sources: Sequence[Mapping[str, Any]], *keys: str) -> float | None:
    for source in sources:
        for key in keys:
            value = _as_float(source.get(key))
            if value is not None:
                return value
    return None


def _pick_int(sources: Sequence[Mapping[str, Any]], *keys: str) -> int | None:
    for source in sources:
        for key in keys:
            value = _as_int(source.get(key))
            if value is not None:
                return value
    return None


def _pick_date(sources: Sequence[Mapping[str, Any]], *keys: str) -> date | None:
    for source in sources:
        for key in keys:
            value = _as_date(source.get(key))
            if value is not None:
                return value
    return None
