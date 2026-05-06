"""Selection score artifacts for StrategyPackage runtime.

Selection artifacts are deliberately separate from minute execution policies:
they store the ranked model score universe needed by Selection Center, without
requiring V24/V25 execution algorithms or their runtime dependencies.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator
from uuid import uuid4

import pandas as pd
import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError

from .live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
    DIAGNOSTIC_BACKTEST_SCOPE,
    DIAGNOSTIC_BACKTEST_SOURCE_TYPE,
    LocalStrategyPackageInferenceProvider,
    QEExperimentRuntimeAssetResolver,
    WslStrategyPackageInferenceProvider,
    win_to_wsl_path,
)
from .models import SelectionScoreArtifactStatus
from .repository import StrategyPackageRepository
from .workspace_policy import ensure_not_forbidden_worker_workspace_path

ConnFactory = Callable[[], Iterator[Any]]


def _canonical_json_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def selection_artifact_runtime_hash(runtime_config: dict[str, Any] | None = None) -> str:
    """Hash only the score-production config, not mutable selection filters.

    Runtime profile choices such as HMM, industry blacklist, suspension filtering
    or top-k are applied after raw scores are loaded, so they must not change the
    artifact lookup hash.
    """

    config = runtime_config or {}
    payload = config.get("selection_artifact_config")
    if payload is None:
        payload = config.get("selection_artifact")
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise StrategyPackageValidationError(
            "runtime_config.selection_artifact_config must be an object",
            context={"selection_artifact_config_type": type(payload).__name__},
        )
    payload = dict(payload)
    # Orchestration switches decide whether to generate/reuse an artifact, but
    # they do not change model scores and must not fragment the artifact key.
    payload.pop("auto_generate", None)
    payload.pop("force_regenerate", None)
    return _canonical_json_sha256(payload)


class SelectionScoreArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifact_id: str = Field(default_factory=lambda: f"ssa_{uuid4().hex}")
    package_id: str
    manifest_sha256: str
    trade_date: date
    data_source: str
    runtime_config_hash: str
    scores_json: list[dict[str, Any]]
    artifact_sha256: str | None = None
    score_count: int = Field(ge=0)
    universe_count: int = Field(ge=0)
    top_score_symbol: str | None = None
    status: SelectionScoreArtifactStatus = SelectionScoreArtifactStatus.SUCCEEDED
    error_json: dict[str, Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("data_source")
    @classmethod
    def _data_source_required(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("data_source is required")
        return value

    @field_validator("scores_json")
    @classmethod
    def _scores_required_for_success(cls, value: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return value


class StrategyPackageSelectionArtifactRepository:
    """PostgreSQL-backed repository for selection score artifacts."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def save(self, artifact: SelectionScoreArtifact) -> SelectionScoreArtifact:
        artifact = self._with_digest(artifact)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.selection_score_artifact (
                        artifact_id, package_id, manifest_sha256, trade_date,
                        data_source, runtime_config_hash, scores_json,
                        artifact_sha256, score_count, universe_count,
                        top_score_symbol, status, error_json, metadata, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (package_id, manifest_sha256, trade_date, data_source, runtime_config_hash)
                    DO UPDATE SET
                        artifact_id = EXCLUDED.artifact_id,
                        scores_json = EXCLUDED.scores_json,
                        artifact_sha256 = EXCLUDED.artifact_sha256,
                        score_count = EXCLUDED.score_count,
                        universe_count = EXCLUDED.universe_count,
                        top_score_symbol = EXCLUDED.top_score_symbol,
                        status = EXCLUDED.status,
                        error_json = EXCLUDED.error_json,
                        metadata = EXCLUDED.metadata,
                        created_at = EXCLUDED.created_at
                    """,
                    (
                        artifact.artifact_id,
                        artifact.package_id,
                        artifact.manifest_sha256,
                        artifact.trade_date,
                        artifact.data_source,
                        artifact.runtime_config_hash,
                        psycopg2.extras.Json(artifact.scores_json),
                        artifact.artifact_sha256,
                        artifact.score_count,
                        artifact.universe_count,
                        artifact.top_score_symbol,
                        artifact.status.value,
                        psycopg2.extras.Json(artifact.error_json) if artifact.error_json else None,
                        psycopg2.extras.Json(artifact.metadata),
                        artifact.created_at,
                    ),
                )
        return self.get(
            package_id=artifact.package_id,
            manifest_sha256=artifact.manifest_sha256,
            trade_date=artifact.trade_date,
            data_source=artifact.data_source,
            runtime_config_hash=artifact.runtime_config_hash,
        )

    def get(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        trade_date: date,
        data_source: str,
        runtime_config_hash: str,
    ) -> SelectionScoreArtifact:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.selection_score_artifact
                    WHERE package_id = %s
                      AND manifest_sha256 = %s
                      AND trade_date = %s
                      AND data_source = %s
                      AND runtime_config_hash = %s
                    """,
                    (package_id, manifest_sha256, trade_date, data_source, runtime_config_hash),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "selection score artifact is missing; generate selection artifact first",
                context={
                    "package_id": package_id,
                    "manifest_sha256": manifest_sha256,
                    "trade_date": trade_date.isoformat(),
                    "data_source": data_source,
                    "runtime_config_hash": runtime_config_hash,
                },
            )
        return self._from_row(dict(row))

    def list(
        self,
        *,
        package_id: str,
        manifest_sha256: str | None = None,
        limit: int = 100,
    ) -> list[SelectionScoreArtifact]:
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        sql = """
            SELECT *
            FROM strategy_pkg.selection_score_artifact
            WHERE package_id = %s
        """
        params: list[Any] = [package_id]
        if manifest_sha256 is not None:
            sql += " AND manifest_sha256 = %s"
            params.append(manifest_sha256)
        sql += " ORDER BY trade_date DESC, created_at DESC LIMIT %s"
        params.append(limit)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
        return [self._from_row(dict(row)) for row in rows]

    @staticmethod
    def _with_digest(artifact: SelectionScoreArtifact) -> SelectionScoreArtifact:
        digest = _canonical_json_sha256(artifact.scores_json)
        return artifact.model_copy(update={"artifact_sha256": digest})

    @staticmethod
    def _from_row(row: dict[str, Any]) -> SelectionScoreArtifact:
        return SelectionScoreArtifact(
            artifact_id=row["artifact_id"],
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            trade_date=row["trade_date"],
            data_source=row["data_source"],
            runtime_config_hash=row["runtime_config_hash"],
            scores_json=row["scores_json"] or [],
            artifact_sha256=row["artifact_sha256"],
            score_count=int(row["score_count"] or 0),
            universe_count=int(row["universe_count"] or 0),
            top_score_symbol=row["top_score_symbol"],
            status=SelectionScoreArtifactStatus(row["status"]),
            error_json=row["error_json"],
            metadata=row["metadata"] or {},
            created_at=row["created_at"],
        )


class InMemorySelectionScoreArtifactRepository:
    def __init__(self) -> None:
        self.artifacts: dict[tuple[str, str, date, str, str], SelectionScoreArtifact] = {}

    def save(self, artifact: SelectionScoreArtifact) -> SelectionScoreArtifact:
        digest = _canonical_json_sha256(artifact.scores_json)
        stored = artifact.model_copy(update={"artifact_sha256": digest})
        key = (
            stored.package_id,
            stored.manifest_sha256,
            stored.trade_date,
            stored.data_source,
            stored.runtime_config_hash,
        )
        self.artifacts[key] = stored
        return stored

    def get(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        trade_date: date,
        data_source: str,
        runtime_config_hash: str,
    ) -> SelectionScoreArtifact:
        key = (package_id, manifest_sha256, trade_date, data_source, runtime_config_hash)
        artifact = self.artifacts.get(key)
        if artifact is None:
            raise DataUnavailableError(
                "selection score artifact is missing; generate selection artifact first",
                context={
                    "package_id": package_id,
                    "trade_date": trade_date.isoformat(),
                    "data_source": data_source,
                    "runtime_config_hash": runtime_config_hash,
                },
            )
        return artifact

    def list(
        self,
        *,
        package_id: str,
        manifest_sha256: str | None = None,
        limit: int = 100,
    ) -> list[SelectionScoreArtifact]:
        rows = [
            item
            for item in self.artifacts.values()
            if item.package_id == package_id
            and (manifest_sha256 is None or item.manifest_sha256 == manifest_sha256)
        ]
        rows.sort(key=lambda item: (item.trade_date, item.created_at), reverse=True)
        return rows[:limit]


class StrategyPackageSelectionArtifactService:
    """Generate and list StrategyPackage selection score artifacts.

    Authoritative artifacts must be produced by live/latest-data inference.
    QE backtest ``pred.pkl`` conversion remains available only as an explicit
    diagnostic path and is rejected by the authoritative runtime by default.
    """

    def __init__(
        self,
        *,
        package_repository: StrategyPackageRepository | Any | None = None,
        artifact_repository: StrategyPackageSelectionArtifactRepository | Any | None = None,
        runtime_asset_resolver: QEExperimentRuntimeAssetResolver | Any | None = None,
        live_inference_provider: Any | None = None,
        conn_factory: ConnFactory | None = None,
    ) -> None:
        self.package_repository = package_repository or StrategyPackageRepository(conn_factory=conn_factory)
        self.artifact_repository = artifact_repository or StrategyPackageSelectionArtifactRepository(conn_factory=conn_factory)
        self.runtime_asset_resolver = runtime_asset_resolver or QEExperimentRuntimeAssetResolver(conn_factory=conn_factory)
        self.live_inference_provider = live_inference_provider
        self._conn_factory = conn_factory or get_conn

    def generate_from_live_inference(
        self,
        *,
        package_id: str,
        trade_date: date,
        data_source: str = "DB_HISTORICAL",
        runtime_config: dict[str, Any] | None = None,
        include_reference_price: bool = True,
        cutoff_date: date | None = None,
    ) -> SelectionScoreArtifact:
        return self.generate_from_live_inference_dates(
            package_id=package_id,
            trade_dates=[trade_date],
            data_source=data_source,
            runtime_config=runtime_config,
            include_reference_price=include_reference_price,
            cutoff_date=cutoff_date,
        )[0]

    def generate_from_live_inference_dates(
        self,
        *,
        package_id: str,
        trade_dates: list[date],
        data_source: str = "DB_HISTORICAL",
        runtime_config: dict[str, Any] | None = None,
        include_reference_price: bool = True,
        cutoff_date: date | None = None,
    ) -> list[SelectionScoreArtifact]:
        if data_source != "DB_HISTORICAL":
            raise DataUnavailableError(
                "live StrategyPackage factor inference currently requires DB_HISTORICAL daily data",
                context={"package_id": package_id, "data_source": data_source},
            )
        if not trade_dates:
            raise StrategyPackageValidationError("live selection artifact generation requires trade_dates")
        unique_dates = sorted(set(trade_dates))
        record = self.package_repository.get(package_id)
        manifest = record.current_manifest()
        if not manifest.manifest_sha256:
            raise StrategyPackageValidationError(
                "strategy package manifest must be frozen before generating live selection artifacts",
                context={"package_id": package_id},
            )
        runtime_hash = selection_artifact_runtime_hash(runtime_config)
        source_loader = getattr(self.runtime_asset_resolver, "load_source_for_strategy_package", None)
        if callable(source_loader):
            source = source_loader(
                source_type=record.source_type,
                source_id=record.source_id,
                loop_id=record.loop_id,
                run_id=record.run_id,
            )
        else:
            source = self.runtime_asset_resolver.load_source(record.source_id)
        provider, inference_backend = self._resolve_live_provider(runtime_config)
        prepared = self.runtime_asset_resolver.prepare_workspace(
            package_id=package_id,
            manifest_sha256=manifest.manifest_sha256,
            source=source,
            runtime_config=runtime_config,
            path_converter=win_to_wsl_path if inference_backend == "wsl" else None,
        )

        artifacts: list[SelectionScoreArtifact] = []
        for current_date in unique_dates:
            score_trade_date = cutoff_date or current_date
            result = provider.run(
                workspace=prepared,
                trade_date=current_date,
                cutoff_date=cutoff_date,
            )
            scores = self._scores_from_live_result(
                result.scores,
                package_id=package_id,
                manifest_sha256=manifest.manifest_sha256,
                trade_date=score_trade_date,
                topk=int(manifest.portfolio_policy.topk),
                include_reference_price=include_reference_price,
            )
            artifact = SelectionScoreArtifact(
                package_id=package_id,
                manifest_sha256=manifest.manifest_sha256,
                trade_date=current_date,
                data_source=data_source,
                runtime_config_hash=runtime_hash,
                scores_json=scores,
                score_count=len(scores),
                universe_count=len(scores),
                top_score_symbol=scores[0]["symbol"] if scores else None,
                status=SelectionScoreArtifactStatus.SUCCEEDED,
                metadata={
                    "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                    "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
                    "source_id": record.source_id,
                    "inference_backend": inference_backend,
                    "runtime_workspace": str(prepared.workspace_path),
                    "factor_order_path": str(prepared.factor_order_path),
                    "factor_entry_path": str(prepared.factor_entry_path),
                    "model_params_path": str(prepared.model_params_path),
                    "model_source_path": str(prepared.model_source_path),
                    "model_candidate_count": prepared.model_candidate_count,
                    "factor_source_dir": str(prepared.factor_source_dir),
                    "factor_count": len(prepared.factor_order),
                    "alpha158_count": len(prepared.alpha158_factors),
                    "dynamic_factor_count": len(prepared.dynamic_factors),
                    "score_direction": manifest.alpha_components[0].score_direction
                    if manifest.alpha_components
                    else "higher_better",
                    "target_weight_policy": "equal_weight_topk",
                    "topk": int(manifest.portfolio_policy.topk),
                    "trade_date_requested": current_date.isoformat(),
                    "cutoff_date": cutoff_date.isoformat() if cutoff_date else None,
                    "score_trade_date": score_trade_date.isoformat(),
                    "reference_price_trade_date": score_trade_date.isoformat() if include_reference_price else None,
                    "provider_metadata": result.metadata,
                },
            )
            artifacts.append(self.artifact_repository.save(artifact))
        return artifacts

    def generate_from_qe_prediction(
        self,
        *,
        package_id: str,
        trade_date: date,
        data_source: str = "DB_HISTORICAL",
        runtime_config: dict[str, Any] | None = None,
        source_path: str | None = None,
        include_reference_price: bool = False,
    ) -> SelectionScoreArtifact:
        return self.generate_from_qe_prediction_dates(
            package_id=package_id,
            trade_dates=[trade_date],
            data_source=data_source,
            runtime_config=runtime_config,
            source_path=source_path,
            include_reference_price=include_reference_price,
        )[0]

    def generate_from_qe_prediction_dates(
        self,
        *,
        package_id: str,
        trade_dates: list[date],
        data_source: str = "DB_HISTORICAL",
        runtime_config: dict[str, Any] | None = None,
        source_path: str | None = None,
        include_reference_price: bool = False,
    ) -> list[SelectionScoreArtifact]:
        """Generate diagnostic-only artifacts from QE backtest predictions.

        This path is intentionally not authoritative. It exists for explicit
        replay diagnostics and must not be used by Selection Center/Paper v2
        runtime unless a caller opts into diagnostic behavior outside the
        authoritative trading path.
        """

        if not trade_dates:
            raise StrategyPackageValidationError("selection artifact generation requires trade_dates")
        unique_dates = sorted(set(trade_dates))
        record = self.package_repository.get(package_id)
        manifest = record.current_manifest()
        if not manifest.manifest_sha256:
            raise StrategyPackageValidationError(
                "strategy package manifest must be frozen before generating selection artifacts",
                context={"package_id": package_id},
            )
        pred_path = self._resolve_prediction_path(record.source_id, source_path=source_path)
        pred = self._load_prediction_frame(pred_path)
        available_dates = self._available_dates(pred)
        missing_dates = [item for item in unique_dates if item not in available_dates]
        if missing_dates:
            raise DataUnavailableError(
                "QE prediction artifact does not contain requested trade_date",
                context={
                    "package_id": package_id,
                    "source_id": record.source_id,
                    "prediction_path": str(pred_path),
                    "missing_trade_dates": [item.isoformat() for item in missing_dates],
                    "available_start": min(available_dates).isoformat() if available_dates else None,
                    "available_end": max(available_dates).isoformat() if available_dates else None,
                },
            )

        runtime_hash = selection_artifact_runtime_hash(runtime_config)
        artifacts: list[SelectionScoreArtifact] = []
        for current_date in unique_dates:
            scores = self._scores_for_date(
                pred,
                package_id=package_id,
                manifest_sha256=manifest.manifest_sha256,
                trade_date=current_date,
                topk=int(manifest.portfolio_policy.topk),
                include_reference_price=include_reference_price,
            )
            artifact = SelectionScoreArtifact(
                package_id=package_id,
                manifest_sha256=manifest.manifest_sha256,
                trade_date=current_date,
                data_source=data_source,
                runtime_config_hash=runtime_hash,
                scores_json=scores,
                score_count=len(scores),
                universe_count=len(scores),
                top_score_symbol=scores[0]["symbol"] if scores else None,
                status=SelectionScoreArtifactStatus.SUCCEEDED,
                metadata={
                    "source_type": DIAGNOSTIC_BACKTEST_SOURCE_TYPE,
                    "authority_scope": DIAGNOSTIC_BACKTEST_SCOPE,
                    "source_id": record.source_id,
                    "prediction_path": str(pred_path),
                    "score_direction": manifest.alpha_components[0].score_direction
                    if manifest.alpha_components
                    else "higher_better",
                    "target_weight_policy": "equal_weight_topk",
                    "topk": int(manifest.portfolio_policy.topk),
                },
            )
            artifacts.append(self.artifact_repository.save(artifact))
        return artifacts

    def list_artifacts(self, package_id: str, *, limit: int = 100) -> list[SelectionScoreArtifact]:
        record = self.package_repository.get(package_id)
        return self.artifact_repository.list(
            package_id=package_id,
            manifest_sha256=record.manifest_sha256,
            limit=limit,
        )

    def _resolve_prediction_path(self, experiment_id: str, *, source_path: str | None) -> Path:
        if source_path:
            path = Path(source_path)
            ensure_not_forbidden_worker_workspace_path(path, purpose="diagnostic QE prediction source_path")
            if not path.exists() or not path.is_file():
                raise DataUnavailableError(
                    "selection artifact source_path does not exist",
                    context={"experiment_id": experiment_id, "source_path": str(path)},
                )
            return path
        raise DataUnavailableError(
            "diagnostic QE pred.pkl generation requires an explicit AIstock-local source_path; "
            "automatic worker workspace scanning is disabled",
            context={"experiment_id": experiment_id, "source_path_required": True},
        )

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @classmethod
    def _prediction_frame_sha256(cls, path: Path) -> str:
        frame = cls._load_prediction_frame(path)
        frame = frame.sort_index()
        payload = pd.util.hash_pandas_object(frame, index=True).values.tobytes()
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _load_prediction_frame(path: Path) -> pd.DataFrame:
        try:
            pred = pd.read_pickle(path)
        except Exception as exc:
            raise DataUnavailableError(
                "failed to read QE prediction artifact",
                context={"prediction_path": str(path), "error": str(exc)},
            ) from exc
        if isinstance(pred, pd.Series):
            pred = pred.to_frame(name="score")
        if not isinstance(pred, pd.DataFrame):
            raise StrategyPackageValidationError(
                "QE prediction artifact must be a pandas DataFrame or Series",
                context={"prediction_path": str(path), "artifact_type": type(pred).__name__},
            )
        if "score" not in pred.columns:
            if len(pred.columns) == 1:
                pred = pred.rename(columns={pred.columns[0]: "score"})
            else:
                raise StrategyPackageValidationError(
                    "QE prediction artifact is missing score column",
                    context={"prediction_path": str(path), "columns": [str(col) for col in pred.columns]},
                )
        if not isinstance(pred.index, pd.MultiIndex):
            raise StrategyPackageValidationError(
                "QE prediction artifact index must be MultiIndex(datetime, instrument)",
                context={"prediction_path": str(path), "index_type": type(pred.index).__name__},
            )
        names = list(pred.index.names)
        if "datetime" not in names or "instrument" not in names:
            raise StrategyPackageValidationError(
                "QE prediction artifact index must contain datetime and instrument levels",
                context={"prediction_path": str(path), "index_names": names},
            )
        return pred[["score"]].copy()

    @staticmethod
    def _available_dates(pred: pd.DataFrame) -> set[date]:
        values = pd.to_datetime(pred.index.get_level_values("datetime")).date
        return set(values)

    def _scores_for_date(
        self,
        pred: pd.DataFrame,
        *,
        package_id: str,
        manifest_sha256: str,
        trade_date: date,
        topk: int,
        include_reference_price: bool,
    ) -> list[dict[str, Any]]:
        date_level = pd.to_datetime(pred.index.get_level_values("datetime")).date
        day = pred[date_level == trade_date].copy()
        if day.empty:
            raise DataUnavailableError(
                "QE prediction artifact has no rows for trade_date",
                context={"package_id": package_id, "trade_date": trade_date.isoformat()},
            )
        day = day.reset_index()
        day["symbol"] = day["instrument"].astype(str)
        day["score"] = pd.to_numeric(day["score"], errors="coerce")
        invalid_count = int((~day["score"].map(lambda value: math.isfinite(float(value)) if pd.notna(value) else False)).sum())
        if invalid_count:
            raise StrategyPackageValidationError(
                "QE prediction artifact contains invalid scores",
                context={"package_id": package_id, "trade_date": trade_date.isoformat(), "invalid_score_count": invalid_count},
            )
        day = day.sort_values(["score", "symbol"], ascending=[False, True]).reset_index(drop=True)
        day["rank"] = day.index + 1
        reference_prices = self._load_reference_prices(day["symbol"].tolist(), trade_date) if include_reference_price else {}
        target_weight = 1.0 / float(topk)
        rows: list[dict[str, Any]] = []
        missing_prices: list[str] = []
        for item in day.itertuples(index=False):
            symbol = str(item.symbol)
            reference_price = reference_prices.get(symbol)
            if include_reference_price and reference_price is None:
                missing_prices.append(symbol)
            rows.append(
                {
                    "symbol": symbol,
                    "score": float(item.score),
                    "rank": int(item.rank),
                    "target_weight": target_weight,
                    "reference_price": reference_price,
                    "component_scores": {
                        "artifact_source": DIAGNOSTIC_BACKTEST_SOURCE_TYPE,
                        "raw_rank": int(item.rank),
                        "manifest_sha256": manifest_sha256,
                    },
                    "reason": "qe_prediction_score_artifact",
                }
            )
        if missing_prices:
            raise DataUnavailableError(
                "reference prices are missing for selection artifact rows",
                context={
                    "package_id": package_id,
                    "trade_date": trade_date.isoformat(),
                    "missing_price_count": len(missing_prices),
                    "missing_price_examples": missing_prices[:20],
                },
            )
        return rows

    def _resolve_live_provider(self, runtime_config: dict[str, Any] | None) -> tuple[Any, str]:
        if self.live_inference_provider is not None:
            return self.live_inference_provider, str(getattr(self.live_inference_provider, "backend_name", "injected"))
        config = runtime_config or {}
        artifact_config = config.get("selection_artifact_config") or config.get("selection_artifact") or {}
        if artifact_config and not isinstance(artifact_config, dict):
            raise StrategyPackageValidationError("selection_artifact_config must be an object")
        backend = str(
            (artifact_config or {}).get("inference_backend")
            or os.getenv("STRATEGY_PACKAGE_SELECTION_INFERENCE_BACKEND")
            or ("wsl" if os.name == "nt" else "local")
        ).strip().lower()
        if backend == "wsl":
            return WslStrategyPackageInferenceProvider(), "wsl"
        if backend == "local":
            return LocalStrategyPackageInferenceProvider(), "local"
        raise StrategyPackageValidationError(
            "unsupported live selection inference_backend",
            context={"inference_backend": backend, "supported": ["wsl", "local"]},
        )

    def _scores_from_live_result(
        self,
        rows: list[dict[str, Any]],
        *,
        package_id: str,
        manifest_sha256: str,
        trade_date: date,
        topk: int,
        include_reference_price: bool,
    ) -> list[dict[str, Any]]:
        if not rows:
            raise DataUnavailableError(
                "live inference returned no score rows",
                context={"package_id": package_id, "trade_date": trade_date.isoformat()},
            )
        normalized: list[dict[str, Any]] = []
        for row in rows:
            missing = [key for key in ("symbol", "score", "rank") if row.get(key) is None]
            if missing:
                raise StrategyPackageValidationError(
                    "live inference score row is missing required fields",
                    context={"package_id": package_id, "missing": missing, "row": row},
                )
            score = float(row["score"])
            if not math.isfinite(score):
                raise StrategyPackageValidationError(
                    "live inference score row contains non-finite score",
                    context={"package_id": package_id, "row": row},
                )
            normalized.append({"symbol": str(row["symbol"]), "score": score, "rank": int(row["rank"])})
        normalized.sort(key=lambda item: (item["rank"], -item["score"], item["symbol"]))
        reference_prices = self._load_reference_prices([row["symbol"] for row in normalized], trade_date) if include_reference_price else {}
        target_weight = 1.0 / float(topk)
        output: list[dict[str, Any]] = []
        missing_prices: list[str] = []
        for row in normalized:
            reference_price = reference_prices.get(row["symbol"])
            if include_reference_price and reference_price is None and row["rank"] <= topk:
                missing_prices.append(row["symbol"])
            output.append(
                {
                    "symbol": row["symbol"],
                    "score": row["score"],
                    "rank": row["rank"],
                    "target_weight": target_weight,
                    "reference_price": reference_price,
                    "component_scores": {
                        "artifact_source": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                        "raw_rank": row["rank"],
                        "manifest_sha256": manifest_sha256,
                        "reference_price_missing": reference_price is None,
                        "reference_price_trade_date": trade_date.isoformat(),
                    },
                    "reason": "live_qe_model_inference_score",
                }
            )
        if missing_prices:
            raise DataUnavailableError(
                "reference prices are missing for live selection artifact rows",
                context={
                    "package_id": package_id,
                    "trade_date": trade_date.isoformat(),
                    "missing_price_count": len(missing_prices),
                    "missing_price_examples": missing_prices[:20],
                },
            )
        return output

    def _load_reference_prices(self, symbols: list[str], trade_date: date) -> dict[str, float]:
        if not symbols:
            return {}
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ts_code, close_li
                    FROM market.kline_daily_raw
                    WHERE trade_date = %s
                      AND ts_code = ANY(%s)
                      AND close_li IS NOT NULL
                      AND close_li > 0
                    """,
                    (trade_date, symbols),
                )
                rows = cur.fetchall()
        return {str(symbol): float(close_li) / 1000.0 for symbol, close_li in rows}
