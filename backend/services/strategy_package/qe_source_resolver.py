"""Read-only resolver from QE experiment records to Strategy Package manifests."""

from __future__ import annotations

import json
import re
from contextlib import contextmanager
from typing import Any, Callable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.quantevolver.runtime_contract import (
    merge_qe_minute_runtime_contract,
    parse_json_mapping,
    runtime_contract_missing,
)
from backend.services.trading_core.errors import (
    DataUnavailableError,
    StrategyPackageValidationError,
    UnsupportedFeatureError,
)

from .manifest import freeze_manifest
from .model_asset_resolver import ModelAssetResolver
from .models import (
    AlphaCombinationPolicy,
    AlphaComponent,
    AlphaLineage,
    AlphaMode,
    AssetCheck,
    BacktestSummary,
    FactorAsset,
    MetricsSnapshot,
    ModelAsset,
    PackageStatus,
    SourceType,
    StrategyPackageManifest,
    StrategyPackageSource,
)

ConnFactory = Callable[[], Iterator[Any]]


def _parse_jsonish(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        if not value.strip():
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise StrategyPackageValidationError(
                "invalid JSON payload in QE experiment record",
                context={"value_preview": value[:200]},
            ) from exc
    return value


def _normalize_alpha_mode(value: Any) -> AlphaMode:
    normalized = str(value or "single").strip().lower()
    if normalized in {"single", "single_alpha"}:
        return AlphaMode.SINGLE_ALPHA
    if normalized in {"multi", "multi_alpha"}:
        return AlphaMode.MULTI_ALPHA
    raise UnsupportedFeatureError(
        "unsupported QE alpha mode",
        context={"alpha_mode": value},
    )


def _metric_float(metrics: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = metrics.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _metric_int(metrics: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = metrics.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _loop_index_from_record(record: dict[str, Any]) -> int | None:
    raw = record.get("loop_index")
    if raw not in (None, ""):
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    for value in (record.get("qe_loop_id"), record.get("experiment_id")):
        match = re.search(r"(?:Loop|_L)(\d+)$", str(value or ""), flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


class QEExperimentSourceResolver:
    """Build a strategy package from a completed QE experiment without writes."""

    def __init__(
        self,
        conn_factory: ConnFactory | None = None,
        model_asset_resolver: ModelAssetResolver | None = None,
    ) -> None:
        self._conn_factory = conn_factory or get_conn
        self._model_asset_resolver = model_asset_resolver or ModelAssetResolver()

    def build_from_experiment(
        self,
        experiment_id: str,
        *,
        resolve_runtime_assets: bool = False,
    ) -> StrategyPackageManifest:
        record = self._load_experiment(experiment_id)
        manifest = self._build_manifest(record)
        if resolve_runtime_assets:
            raise StrategyPackageValidationError(
                "alpha-core StrategyPackage does not resolve execution runtime assets into the frozen manifest",
                context={"experiment_id": experiment_id},
            )
        frozen = freeze_manifest(manifest)
        return frozen

    def build_from_evolution_loop(
        self,
        *,
        qe_task_id: str,
        qe_loop_id: str,
        resolve_runtime_assets: bool = False,
    ) -> StrategyPackageManifest:
        """Build a package from one explicit QE evolution loop without writes."""

        record = self._load_evolution_loop(qe_task_id=qe_task_id, qe_loop_id=qe_loop_id)
        manifest = self._build_manifest(
            record,
            source_type=SourceType.QE_EVOLUTION_LOOP,
            source_id=qe_task_id,
            loop_id=qe_loop_id,
            run_id=record.get("experiment_id"),
        )
        if resolve_runtime_assets:
            raise StrategyPackageValidationError(
                "alpha-core StrategyPackage does not resolve execution runtime assets into the frozen manifest",
                context={"qe_task_id": qe_task_id, "qe_loop_id": qe_loop_id},
            )
        return freeze_manifest(manifest)

    def _load_experiment(self, experiment_id: str) -> dict[str, Any]:
        if not experiment_id or not experiment_id.strip():
            raise StrategyPackageValidationError("experiment_id is required")

        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT experiment_id, experiment_name, status, alpha_mode,
                           qe_task_id, qe_loop_id, factor_names, model_id,
                           strategy_id, data_split, custom_params, result_metrics,
                           created_at, completed_at, is_evolution_loop
                    FROM qe_experiments
                    WHERE experiment_id = %s
                    """,
                    (experiment_id,),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "QE experiment does not exist",
                context={"experiment_id": experiment_id},
            )
        return dict(row)

    def _load_evolution_loop(self, *, qe_task_id: str, qe_loop_id: str) -> dict[str, Any]:
        qe_task_id = str(qe_task_id or "").strip()
        qe_loop_id = str(qe_loop_id or "").strip()
        if not qe_task_id:
            raise StrategyPackageValidationError("qe_task_id is required")
        if not qe_loop_id:
            raise StrategyPackageValidationError("qe_loop_id is required")

        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT experiment_id, experiment_name, status, alpha_mode,
                           qe_task_id, qe_loop_id, factor_names, model_id,
                           strategy_id, data_split, custom_params, result_metrics,
                           created_at, completed_at, is_evolution_loop
                    FROM qe_experiments
                    WHERE qe_task_id = %s
                      AND qe_loop_id = %s
                    ORDER BY completed_at DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (qe_task_id, qe_loop_id),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "QE evolution loop does not exist",
                context={"qe_task_id": qe_task_id, "qe_loop_id": qe_loop_id},
            )
        return dict(row)

    def _load_loop_runtime_config(self, record: dict[str, Any]) -> dict[str, Any]:
        """Load full loop config for legacy qe_experiments rows missing runtime fields."""

        task_id = str(record.get("qe_task_id") or "").strip()
        qe_loop_id = str(record.get("qe_loop_id") or "").strip()
        experiment_id = str(record.get("experiment_id") or "").strip()
        if not task_id and not experiment_id:
            return {}

        loop_index = _loop_index_from_record(record)
        task_prefixed_loop_id = f"{task_id}_{qe_loop_id}" if task_id and qe_loop_id else qe_loop_id
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT l.config_json,
                           t.execution_algo AS task_execution_algo,
                           t.execution_algo_params AS task_execution_algo_params
                    FROM qe_evolution_loops l
                    LEFT JOIN qe_evolution_tasks t ON t.task_id = l.task_id
                    WHERE l.experiment_id = %s
                       OR (
                           l.task_id = %s
                           AND (
                               l.loop_id = %s
                               OR l.loop_id = %s
                               OR (%s IS NOT NULL AND l.loop_index = %s)
                           )
                       )
                    ORDER BY l.updated_at DESC NULLS LAST, l.created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (
                        experiment_id,
                        task_id,
                        qe_loop_id,
                        task_prefixed_loop_id,
                        loop_index,
                        loop_index,
                    ),
                )
                row = cur.fetchone()
        if not row:
            return {}
        config = parse_json_mapping(row.get("config_json"))
        if row.get("task_execution_algo") and not config.get("execution_algo"):
            config["execution_algo"] = row.get("task_execution_algo")
        task_params = parse_json_mapping(row.get("task_execution_algo_params"))
        if task_params and not config.get("execution_algo_params"):
            config["execution_algo_params"] = task_params
        return config

    def _effective_custom_params(
        self,
        record: dict[str, Any],
        custom_params: dict[str, Any],
    ) -> dict[str, Any]:
        """Merge old qe_experiments rows with explicit loop runtime evidence."""

        if not runtime_contract_missing(custom_params):
            return custom_params
        loop_config = self._load_loop_runtime_config(record)
        if not loop_config:
            return custom_params
        return merge_qe_minute_runtime_contract(
            custom_params,
            config=loop_config,
            source="strategy_package_loop_config",
            allow_default_execution_algo=False,
        )

    def _build_manifest(
        self,
        record: dict[str, Any],
        *,
        source_type: SourceType = SourceType.QE_EXPERIMENT,
        source_id: str | None = None,
        loop_id: str | None = None,
        run_id: str | None = None,
    ) -> StrategyPackageManifest:
        experiment_id = str(record["experiment_id"])
        status = str(record.get("status") or "").lower()
        if status != "completed":
            raise StrategyPackageValidationError(
                "QE experiment is not completed",
                context={"experiment_id": experiment_id, "status": record.get("status")},
            )

        factor_names = _parse_jsonish(record.get("factor_names")) or []
        if not isinstance(factor_names, list) or not factor_names:
            raise StrategyPackageValidationError(
                "QE experiment has no factor_names",
                context={"experiment_id": experiment_id},
            )

        custom_params = _parse_jsonish(record.get("custom_params")) or {}
        if not isinstance(custom_params, dict):
            raise StrategyPackageValidationError(
                "QE experiment custom_params must be an object",
                context={"experiment_id": experiment_id},
            )
        custom_params = self._effective_custom_params(record, custom_params)

        data_split = _parse_jsonish(record.get("data_split")) or {}
        if not isinstance(data_split, dict):
            raise StrategyPackageValidationError(
                "QE experiment data_split must be an object",
                context={"experiment_id": experiment_id},
            )

        metrics = _parse_jsonish(record.get("result_metrics")) or {}
        if not isinstance(metrics, dict) or not metrics:
            raise StrategyPackageValidationError(
                "QE experiment has no backtest metrics",
                context={"experiment_id": experiment_id},
            )

        model_id = str(record.get("model_id") or "").strip()
        strategy_id = str(record.get("strategy_id") or "").strip()
        if not model_id:
            raise StrategyPackageValidationError(
                "QE experiment has no model_id",
                context={"experiment_id": experiment_id},
            )
        if not strategy_id:
            raise StrategyPackageValidationError(
                "QE experiment has no strategy_id",
                context={"experiment_id": experiment_id},
            )

        alpha_mode = _normalize_alpha_mode(record.get("alpha_mode"))
        components = self._build_alpha_components(
            alpha_mode=alpha_mode,
            experiment_id=experiment_id,
            factor_names=factor_names,
            model_id=model_id,
            metrics=metrics,
        )
        combination_policy = self._build_combination_policy(alpha_mode, components)
        backtest_context = self._build_backtest_context(custom_params, data_split, experiment_id, strategy_id=strategy_id)
        source_evidence = self._build_source_evidence(
            record=record,
            custom_params=custom_params,
            data_split=data_split,
            backtest_context=backtest_context,
        )
        asset_checks = self._build_asset_checks(record, factor_names)
        package_status = (
            PackageStatus.BACKTEST_APPROVED
            if all(check.passed for check in asset_checks)
            else PackageStatus.DRAFT
        )

        return StrategyPackageManifest(
            manifest_version="alpha_core_v1",
            package_name=str(record.get("experiment_name") or experiment_id),
            source=StrategyPackageSource(
                source_type=source_type,
                source_id=source_id or experiment_id,
                loop_id=loop_id if loop_id is not None else record.get("qe_loop_id"),
                run_id=run_id if run_id is not None else record.get("qe_task_id"),
                created_at=record.get("created_at") or record.get("completed_at"),
            ),
            alpha_mode=alpha_mode,
            alpha_components=components,
            alpha_combination_policy=combination_policy,
            factor_set=[
                FactorAsset(factor_id=str(name), factor_name=str(name)) for name in factor_names
            ],
            model_asset=ModelAsset(model_id=model_id),
            source_evidence=source_evidence,
            backtest_context=backtest_context,
            backtest_summary=self._build_backtest_summary(metrics, data_split),
            asset_checks=asset_checks,
            package_status=package_status,
        )

    def _build_alpha_components(
        self,
        *,
        alpha_mode: AlphaMode,
        experiment_id: str,
        factor_names: list[Any],
        model_id: Any,
        metrics: dict[str, Any],
    ) -> list[AlphaComponent]:
        factor_ids = [str(name) for name in factor_names]
        model_ref = str(model_id) if model_id else None
        snapshot = MetricsSnapshot(
            ic=_metric_float(metrics, "IC"),
            rank_ic=_metric_float(metrics, "Rank IC", "rank_ic"),
            annual_return=_metric_float(
                metrics,
                "annualized_return",
                "1day.excess_return_with_cost.annualized_return",
                "cagr",
            ),
            max_drawdown=_metric_float(
                metrics,
                "max_drawdown",
                "1day.excess_return_with_cost.max_drawdown",
            ),
            turnover=_metric_float(metrics, "turnover"),
        )

        if alpha_mode == AlphaMode.SINGLE_ALPHA:
            return [
                AlphaComponent(
                    alpha_id="alpha_001",
                    alpha_name="single_alpha",
                    component_weight=1.0,
                    factor_ids=factor_ids,
                    model_id=model_ref,
                    model_ref=model_ref,
                    holding_period="1day",
                    rebalance_frequency="1day",
                    score_direction="higher_better",
                    score_normalization="rank",
                    risk_tags=[],
                    metrics_snapshot=snapshot,
                    lineage=AlphaLineage(
                        qe_artifact_id=experiment_id,
                        factor_artifact_refs=factor_ids,
                        model_artifact_ref=model_ref,
                    ),
                )
            ]

        multi_detail = metrics.get("multi_alpha_detail") or {}
        group_results = multi_detail.get("group_results") or []
        if not isinstance(group_results, list) or len(group_results) < 2:
            raise UnsupportedFeatureError(
                "multi_alpha QE package preprocessing requires group_results",
                context={"experiment_id": experiment_id},
            )

        components: list[AlphaComponent] = []
        default_weight = 1.0 / len(group_results)
        for idx, group in enumerate(group_results, start=1):
            if not isinstance(group, dict):
                raise StrategyPackageValidationError(
                    "multi_alpha group result must be an object",
                    context={"experiment_id": experiment_id, "group_index": idx},
                )
            group_name = str(group.get("group_name") or f"group_{idx}")
            group_factors = group.get("factor_names") or factor_ids
            if not isinstance(group_factors, list) or not group_factors:
                raise StrategyPackageValidationError(
                    "multi_alpha group has no factors",
                    context={"experiment_id": experiment_id, "group_name": group_name},
                )
            weight = float(group.get("meta_weight") or group.get("weight") or default_weight)
            components.append(
                AlphaComponent(
                    alpha_id=f"alpha_{idx:03d}",
                    alpha_name=group_name,
                    component_weight=weight,
                    factor_ids=[str(name) for name in group_factors],
                    model_id=str(group.get("model_id") or model_id or ""),
                    model_ref=str(group.get("model_id") or model_id or ""),
                    holding_period="1day",
                    rebalance_frequency="1day",
                    score_direction="higher_better",
                    score_normalization="rank",
                    risk_tags=[],
                    metrics_snapshot=MetricsSnapshot(
                        ic=_metric_float(group, "ic", "IC", "group_ic"),
                        rank_ic=_metric_float(group, "rank_ic", "Rank IC", "group_rank_ic"),
                        annual_return=_metric_float(group, "annual_return", "annualized_return"),
                        max_drawdown=_metric_float(group, "max_drawdown"),
                    ),
                    lineage=AlphaLineage(
                        qe_artifact_id=experiment_id,
                        factor_artifact_refs=[str(name) for name in group_factors],
                        model_artifact_ref=str(group.get("model_id") or model_id or ""),
                    ),
                )
            )
        return components

    def _build_combination_policy(
        self,
        alpha_mode: AlphaMode,
        components: list[AlphaComponent],
    ) -> AlphaCombinationPolicy:
        if alpha_mode == AlphaMode.SINGLE_ALPHA:
            component = components[0]
            return AlphaCombinationPolicy(
                method="identity",
                weights={component.alpha_id: 1.0},
                conflict_resolution="highest_score",
            )
        weights = {component.alpha_id: component.component_weight for component in components}
        return AlphaCombinationPolicy(method="weighted_score", weights=weights)

    def _build_source_evidence(
        self,
        *,
        record: dict[str, Any],
        custom_params: dict[str, Any],
        data_split: dict[str, Any],
        backtest_context: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "schema_version": "strategy_package_source_evidence_v1",
            "source_kind": "qe_experiment",
            "experiment_id": str(record.get("experiment_id") or ""),
            "strategy_id": str(record.get("strategy_id") or ""),
            "qe_task_id": str(record.get("qe_task_id") or ""),
            "qe_loop_id": str(record.get("qe_loop_id") or ""),
            "workspace_path": str(record.get("workspace_path") or ""),
            "custom_params": custom_params,
            "data_split": data_split,
            "backtest_context_ref": backtest_context.get("schema_version"),
            "authority": "audit_only_not_runtime_authority",
        }

    def _build_backtest_context(
        self,
        custom_params: dict[str, Any],
        data_split: dict[str, Any],
        experiment_id: str,
        *,
        strategy_id: str,
    ) -> dict[str, Any]:
        execution_algo_params = custom_params.get("execution_algo_params") or {}
        if execution_algo_params is not None and not isinstance(execution_algo_params, dict):
            raise StrategyPackageValidationError(
                "execution_algo_params must be an object",
                context={"experiment_id": experiment_id},
            )
        backtest_freq = self._normalize_backtest_freq(custom_params, experiment_id, required=False)
        execution_algo = str(custom_params.get("execution_algo") or "").strip().upper() or None
        return {
            "schema_version": "qe_backtest_context_v1",
            "authority": "source_evidence_not_runtime_authority",
            "daily_strategy": {
                "strategy_id": str(custom_params.get("strategy_id") or custom_params.get("strategy_class") or strategy_id or "").strip() or None,
                "topk": _optional_int(custom_params.get("topk")),
                "n_drop": _optional_int(custom_params.get("n_drop")),
                "stock_pool": str(custom_params.get("stock_pool") or "").strip() or None,
                "custom_params": custom_params,
            },
            "execution": {
                "backtest_freq": backtest_freq,
                "execution_algo": execution_algo,
                "execution_algo_params": execution_algo_params or {},
            },
            "data_split": data_split,
        }

    def _normalize_backtest_freq(
        self,
        custom_params: dict[str, Any],
        experiment_id: str,
        *,
        required: bool = True,
    ) -> str | None:
        freq = str(custom_params.get("backtest_freq") or "").strip().lower()
        if freq in {"1min", "1m", "minute"}:
            return "1min"
        if freq in {"5min", "5m"}:
            return "5min"
        if freq == "day":
            raise UnsupportedFeatureError(
                "daily backtest frequency is not allowed for Strategy Package alpha-core evidence",
                context={"experiment_id": experiment_id, "backtest_freq": freq},
            )
        if not required:
            return None
        raise StrategyPackageValidationError(
            "QE experiment must declare minute backtest_freq",
            context={"experiment_id": experiment_id, "backtest_freq": freq},
        )

    def _build_backtest_summary(
        self,
        metrics: dict[str, Any],
        data_split: dict[str, Any],
    ) -> BacktestSummary:
        return BacktestSummary(
            ic=_metric_float(metrics, "IC"),
            rank_ic=_metric_float(metrics, "Rank IC", "rank_ic"),
            icir=_metric_float(metrics, "ICIR", "Rank ICIR"),
            annual_return=_metric_float(
                metrics,
                "annualized_return",
                "1day.excess_return_with_cost.annualized_return",
                "cagr",
            ),
            max_drawdown=_metric_float(
                metrics,
                "max_drawdown",
                "1day.excess_return_with_cost.max_drawdown",
            ),
            final_nav=_metric_float(metrics, "final_nav"),
            n_trading_days=_metric_int(metrics, "n_trading_days"),
            raw_metrics=metrics,
            sample_start=data_split.get("test_start"),
            sample_end=data_split.get("backtest_end") or data_split.get("test_end"),
        )

    def _build_asset_checks(
        self,
        record: dict[str, Any],
        factor_names: list[Any],
    ) -> list[AssetCheck]:
        qe_task_id = str(record.get("qe_task_id") or "").strip()
        qe_loop_id = str(record.get("qe_loop_id") or "").strip()
        metrics = _parse_jsonish(record.get("result_metrics")) or {}
        checks = [
            AssetCheck(
                check_name="factor_names_present",
                passed=bool(factor_names),
                message="factor_names must be present",
                context={"factor_count": len(factor_names)},
            ),
            AssetCheck(
                check_name="qe_task_loop_present",
                passed=bool(qe_task_id and qe_loop_id),
                message="qe_task_id and qe_loop_id are required to resolve runtime assets through the node API",
                context={"qe_task_id": qe_task_id or None, "qe_loop_id": qe_loop_id or None},
            ),
            AssetCheck(
                check_name="backtest_metrics_present",
                passed=bool(metrics),
                message="result_metrics must be present",
                context={"metric_keys": sorted(str(key) for key in metrics.keys())[:30] if isinstance(metrics, dict) else []},
            ),
            AssetCheck(
                check_name="runtime_assets_api_only",
                passed=bool(qe_task_id and qe_loop_id),
                message="QE runtime assets are resolved through the execution-node API/cache; workspace_path is not inspected",
                context={"workspace_path_inspected": False},
            )
        ]
        return checks


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


@contextmanager
def dict_record_conn(record: dict[str, Any]) -> Iterator[Any]:
    """Test helper: expose one dict record through a connection-like object."""

    class _Cursor:
        description = None

        def __enter__(self) -> "_Cursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, *_args: object, **_kwargs: object) -> None:
            return None

        def fetchone(self) -> dict[str, Any]:
            return record

    class _Conn:
        def __enter__(self) -> "_Conn":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self, *args: object, **kwargs: object) -> _Cursor:
            return _Cursor()

    yield _Conn()
