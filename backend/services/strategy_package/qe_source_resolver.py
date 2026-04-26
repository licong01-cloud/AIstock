"""Read-only resolver from QE experiment records to Strategy Package manifests."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
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
    ExecutionPolicy,
    FactorAsset,
    MetricsSnapshot,
    MinuteExecutionPolicy,
    ModelAsset,
    PackageStatus,
    PortfolioPolicy,
    SourceType,
    StrategyPackageManifest,
    StrategyPackageSource,
    UniversePolicy,
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
            manifest = self._model_asset_resolver.resolve_manifest_assets(
                manifest,
                copy_missing=True,
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
            manifest = self._model_asset_resolver.resolve_manifest_assets(
                manifest,
                copy_missing=True,
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
                           workspace_path, created_at, completed_at
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
                           workspace_path, created_at, completed_at
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

        alpha_mode = _normalize_alpha_mode(record.get("alpha_mode"))
        components = self._build_alpha_components(
            alpha_mode=alpha_mode,
            experiment_id=experiment_id,
            factor_names=factor_names,
            model_id=record.get("model_id"),
            metrics=metrics,
        )
        combination_policy = self._build_combination_policy(alpha_mode, components)
        minute_policy = self._build_minute_execution_policy(custom_params, experiment_id)
        asset_checks = self._build_asset_checks(record, factor_names)
        package_status = (
            PackageStatus.BACKTEST_APPROVED
            if all(check.passed for check in asset_checks)
            else PackageStatus.DRAFT
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

        topk = int(custom_params.get("topk") or 50)
        n_drop = int(custom_params.get("n_drop") or 0)
        stock_pool = str(custom_params.get("stock_pool") or "unknown")

        return StrategyPackageManifest(
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
            strategy_config={
                "strategy_id": strategy_id,
                "custom_params": custom_params,
                "data_split": data_split,
            },
            universe_policy=UniversePolicy(stock_pool=stock_pool),
            portfolio_policy=PortfolioPolicy(topk=topk, n_drop=n_drop),
            execution_policy=ExecutionPolicy(
                backtest_freq=self._normalize_backtest_freq(custom_params, experiment_id)
            ),
            minute_execution_policy=minute_policy,
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

    def _normalize_backtest_freq(self, custom_params: dict[str, Any], experiment_id: str) -> str:
        freq = str(custom_params.get("backtest_freq") or "").strip().lower()
        if freq in {"1min", "1m", "minute"}:
            return "1min"
        if freq in {"5min", "5m"}:
            return "5min"
        if freq == "day":
            raise UnsupportedFeatureError(
                "daily backtest frequency is not allowed for Strategy Package v1",
                context={"experiment_id": experiment_id, "backtest_freq": freq},
            )
        raise StrategyPackageValidationError(
            "QE experiment must declare minute backtest_freq",
            context={"experiment_id": experiment_id, "backtest_freq": freq},
        )

    def _build_minute_execution_policy(
        self,
        custom_params: dict[str, Any],
        experiment_id: str,
    ) -> MinuteExecutionPolicy:
        self._normalize_backtest_freq(custom_params, experiment_id)
        algo_code = str(custom_params.get("execution_algo") or "").strip().upper()
        if not algo_code:
            raise StrategyPackageValidationError(
                "QE experiment must declare execution_algo",
                context={"experiment_id": experiment_id},
            )
        algo_config = custom_params.get("execution_algo_params") or {}
        if not isinstance(algo_config, dict):
            raise StrategyPackageValidationError(
                "execution_algo_params must be an object",
                context={"experiment_id": experiment_id},
            )
        return MinuteExecutionPolicy(
            bar_freq="1m",
            algo_code=algo_code,
            algo_config=algo_config,
            fallback_algo_code=None,
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
        workspace_path = record.get("workspace_path")
        checks = [
            AssetCheck(
                check_name="factor_names_present",
                passed=bool(factor_names),
                message="factor_names must be present",
                context={"factor_count": len(factor_names)},
            )
        ]
        if workspace_path:
            path = Path(str(workspace_path))
            checks.append(
                AssetCheck(
                    check_name="workspace_exists",
                    passed=path.exists(),
                    message="workspace_path must exist",
                    context={"workspace_path": str(path)},
                )
            )
            minute_runner = path / "qrun_limit_minute.py"
            checks.append(
                AssetCheck(
                    check_name="minute_runner_exists",
                    passed=minute_runner.exists(),
                    message="qrun_limit_minute.py must exist in QE workspace",
                    context={"path": str(minute_runner)},
                )
            )
        else:
            checks.append(
                AssetCheck(
                    check_name="workspace_exists",
                    passed=False,
                    message="workspace_path is required for package validation",
                )
            )
        return checks


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
