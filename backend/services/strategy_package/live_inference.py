"""Authoritative live/latest-data inference for StrategyPackage selection.

This module builds a transient inference workspace from a frozen StrategyPackage
and its QE source assets. It never reads QE backtest ``pred.pkl`` as a current
selection signal; scores must be produced by recomputing factors from the
current DB data window and applying the saved QE model.
"""

from __future__ import annotations

import ast
import asyncio
import hashlib
import io
import json
import logging
import math
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from threading import Thread
from typing import Any, Callable, Iterator, Literal

import pandas as pd
import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.infra.wsl_qlib_runner import win_to_wsl_path
from backend.services.quantevolver.node_execution import resolve_default_qe_node_id
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceClient,
    QEWorkspaceFileNotFound,
)
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    DataUnavailableError,
    PackageAssetInvalidError,
    RuntimeConfigInvalidError,
    TradingCoreError,
)

from .models import AlphaMode, FactorAsset, ModelAsset, ModelCodeAsset, RuntimeAssetManifest, StrategyPackageManifest
from .package_asset_freeze import manifest_has_frozen_runtime_assets, pickled_model_code_references_from_params_bytes
from .package_asset_store import LocalPackageAssetStore, PackageAssetStore
from .runtime_schema import (
    ALPHA158_SCHEMA_VERSION,
    extract_alpha158_aliases,
    load_conf_yaml_file,
    minimal_conf_with_alpha158,
)
from .workspace_policy import ensure_not_forbidden_worker_workspace_path

logger = logging.getLogger(__name__)

ModelParamsOrigin = Literal["node", "cache", "package_asset", "unavailable"]
ConnFactory = Callable[[], Iterator[Any]]

AUTHORITATIVE_SELECTION_SOURCE_TYPE = "live_qe_model_inference_v1"
AUTHORITATIVE_SELECTION_SCOPE = "authoritative_selection"
DIAGNOSTIC_BACKTEST_SOURCE_TYPE = "qe_mlruns_pred_pkl_v1"
DIAGNOSTIC_BACKTEST_SCOPE = "diagnostic_backtest_only"


@dataclass(frozen=True)
class QEExperimentRuntimeSource:
    experiment_id: str
    db_workspace_path: Path
    asset_workspace_path: Path
    factor_names: list[str]
    custom_params: dict[str, Any]
    data_split: dict[str, Any]
    qe_task_id: str | None = None
    qe_loop_id: str | None = None
    execution_node_id: str | None = None
    # Provenance of the params.pkl materialized into asset_workspace_path.
    # Set by _materialize_runtime_source_from_node based on whether the
    # mlruns archive came from the QE node API or the local cache fallback.
    model_params_origin: ModelParamsOrigin = "node"
    source_workspace_type: str = "aistock_node_api_cache"
    package_id: str | None = None
    manifest_sha256: str | None = None


@dataclass(frozen=True)
class StaticLoaderFeatureResolution:
    factors: list[str]
    configs: list[str]
    missing_configs: list[str]
    unreadable_configs: list[dict[str, str]]


@dataclass(frozen=True)
class FactorOrderResolution:
    alpha158_factors: list[str]
    dynamic_factors: list[str]
    factor_order: list[str]
    dynamic_factor_source: str
    static_loader_schema_available: bool
    static_loader_configs: list[str]
    static_loader_missing_configs: list[str]
    static_loader_unreadable_configs: list[dict[str, str]]
    warnings: list[str]


@dataclass(frozen=True)
class PreparedInferenceWorkspace:
    workspace_path: Path
    manifest_path: Path
    factor_order_path: Path
    factor_entry_path: Path
    model_params_path: Path
    source_workspace_path: Path
    factor_source_dir: Path
    factor_order: list[str]
    alpha158_factors: list[str]
    dynamic_factors: list[str]
    model_source_path: Path
    model_candidate_count: int
    dataset_processor_path: Path | None = None
    # Provenance of the params.pkl used for this workspace. 'node' = downloaded
    # from the QE node API (the only origin allowed by default for unfrozen
    # packages); 'cache' = local StrategyPackage cache fallback (requires
    # explicit allow_cache_fallback=True at the materialization call site);
    # 'package_asset' = package-owned immutable asset blob; 'unavailable' is
    # reserved for failed runs written from upstream error handlers.
    model_params_origin: ModelParamsOrigin = "node"


@dataclass(frozen=True)
class LiveInferenceResult:
    scores: list[dict[str, Any]]
    metadata: dict[str, Any]
    # These are produced by the same inference invocation as ``scores``. They
    # are intentionally separate from diagnostic metadata so artifact v2 can
    # validate factual source and input provenance without changing score logic.
    universe_count: int | None = None
    source_read_receipts: list[dict[str, Any]] | None = None
    input_context: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Live inference cold-start preflight (P0-F / Codex doc P0-4)
# ---------------------------------------------------------------------------

PREFLIGHT_STATUS_PASS = "PASS"
PREFLIGHT_STATUS_BLOCKED = "BLOCKED"

PREFLIGHT_CHECK_QE_SOURCE = "qe_source"
PREFLIGHT_CHECK_QE_NODE = "qe_node"
PREFLIGHT_CHECK_CONF_YAML = "conf_yaml"
PREFLIGHT_CHECK_FACTOR_SOURCE = "factor_source"
PREFLIGHT_CHECK_MODEL_PARAMS = "model_params"

PREFLIGHT_CHECK_NAMES = (
    PREFLIGHT_CHECK_QE_SOURCE,
    PREFLIGHT_CHECK_QE_NODE,
    PREFLIGHT_CHECK_CONF_YAML,
    PREFLIGHT_CHECK_FACTOR_SOURCE,
    PREFLIGHT_CHECK_MODEL_PARAMS,
)


@dataclass(frozen=True)
class LiveInferencePreflightCheck:
    """Per-check result for live inference cold-start preflight."""

    name: str
    status: str
    message: str
    suggestion: str | None = None
    context: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "message": self.message,
            "suggestion": self.suggestion,
            "context": dict(self.context or {}),
        }


@dataclass(frozen=True)
class LiveInferencePreflightResult:
    """Aggregate preflight outcome.

    ``checks`` always contains exactly 5 entries (one per
    ``PREFLIGHT_CHECK_NAMES``). The first BLOCKED entry stops further checks
    so we never run expensive node downloads or factor reads after a known
    failure (cold-start root-cause for the 30+ historical failures).
    """

    passed: bool
    checks: list[LiveInferencePreflightCheck]

    @property
    def blocked_check(self) -> LiveInferencePreflightCheck | None:
        for check in self.checks:
            if check.status == PREFLIGHT_STATUS_BLOCKED:
                return check
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "checks": [check.to_dict() for check in self.checks],
        }


class LiveInferencePreflightError(ArtifactGenerationFailedError):
    """Live inference cold-start preflight failed.

    Surfaced fail-fast before Selection Center commits to a heavy
    ``generate_from_live_inference`` invocation. Replaces the historical
    behaviour of letting the run timeout deep inside ``prepare_workspace``.
    """

    error_code = "LIVE_INFERENCE_PREFLIGHT_FAILED"


def _remaining_skipped_checks(*, after: str) -> list[LiveInferencePreflightCheck]:
    """Return SKIPPED entries for every check that follows ``after``.

    ``after`` itself is NOT emitted (the caller is responsible for emitting
    the BLOCKED entry that triggered the short-circuit). This keeps the
    ``checks`` list always 5 items long for the UI.
    """

    try:
        idx = PREFLIGHT_CHECK_NAMES.index(after)
    except ValueError:
        return []
    skipped: list[LiveInferencePreflightCheck] = []
    for name in PREFLIGHT_CHECK_NAMES[idx + 1 :]:
        skipped.append(
            LiveInferencePreflightCheck(
                name=name,
                status=PREFLIGHT_STATUS_BLOCKED,
                message=f"check skipped because {after} failed",
                suggestion=None,
                context={"skipped_due_to": after},
            )
        )
    return skipped


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
            raise PackageAssetInvalidError(
                "invalid JSON payload in QE experiment runtime source",
                context={"value_preview": value[:200]},
            ) from exc
    return value


def _load_qe_conf_yaml(conf_path: Path, *, purpose: str) -> dict[str, Any]:
    try:
        return load_conf_yaml_file(conf_path, purpose=purpose)
    except PackageAssetInvalidError as exc:
        exc.context.setdefault("conf_path", str(conf_path))
        exc.context.setdefault("purpose", purpose)
        raise


def _sanitize_unresolved_jinja_for_yaml(text: str) -> tuple[str, bool]:
    changed = False
    sanitized_lines: list[str] = []
    for line in text.splitlines(keepends=True):
        stripped = line.lstrip()
        if stripped.startswith(("{%", "{#")):
            indent = line[: len(line) - len(stripped)]
            newline = "\r\n" if line.endswith("\r\n") else "\n" if line.endswith("\n") else ""
            sanitized_lines.append(f"{indent}# {stripped.rstrip()}{newline}")
            changed = True
            continue
        sanitized, line_changed = _replace_unquoted_jinja_expressions(line)
        sanitized_lines.append(sanitized)
        changed = changed or line_changed
    return "".join(sanitized_lines), changed


def _replace_unquoted_jinja_expressions(line: str) -> tuple[str, bool]:
    result: list[str] = []
    changed = False
    in_single = False
    in_double = False
    i = 0
    while i < len(line):
        if not in_single and not in_double and line.startswith("{{", i):
            end = line.find("}}", i + 2)
            if end != -1:
                expr = line[i + 2 : end].strip()
                safe_expr = re.sub(r"[^0-9A-Za-z_]+", "_", expr).strip("_")[:64] or "expr"
                result.append(json.dumps(f"__AISTOCK_UNRESOLVED_JINJA_{safe_expr}__"))
                i = end + 2
                changed = True
                continue

        ch = line[i]
        result.append(ch)
        if ch == "'" and not in_double:
            if in_single and i + 1 < len(line) and line[i + 1] == "'":
                result.append(line[i + 1])
                i += 2
                continue
            in_single = not in_single
        elif ch == '"' and not in_single:
            backslashes = 0
            j = len(result) - 2
            while j >= 0 and result[j] == "\\":
                backslashes += 1
                j -= 1
            if backslashes % 2 == 0:
                in_double = not in_double
        i += 1
    return "".join(result), changed


def _safe_name(value: str) -> str:
    text = re.sub(r"\W+", "_", value.strip())
    if not text or text[0].isdigit():
        text = f"factor_{text}"
    return text


def _date_to_datetime(value: date) -> datetime:
    return datetime.combine(value, datetime.min.time())


def _run_async_blocking(factory: Callable[[], Any]) -> Any:
    """Run a coroutine factory from sync service code, including event-loop threads."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())

    result: dict[str, Any] = {}

    def _target() -> None:
        try:
            result["value"] = asyncio.run(factory())
        except BaseException as exc:  # pragma: no cover - re-raised below
            result["error"] = exc

    thread = Thread(target=_target, daemon=True)
    thread.start()
    thread.join()
    if "error" in result:
        raise result["error"]
    return result.get("value")


def _safe_cache_component(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
    return text or "unknown"


def _single_model_asset_for_runtime(manifest: StrategyPackageManifest) -> ModelAsset:
    model_asset = manifest.model_asset
    models = model_asset if isinstance(model_asset, list) else [model_asset]
    if len(models) != 1:
        raise PackageAssetInvalidError(
            "single-alpha runtime package asset path requires exactly one model asset",
            context={
                "reason_code": "strategy_package_runtime_model_asset_ambiguous",
                "package_id": manifest.package_id,
                "model_asset_count": len(models),
            },
        )
    model = models[0]
    if not isinstance(model, ModelAsset):
        raise PackageAssetInvalidError(
            "strategy package runtime model asset is invalid",
            context={"reason_code": "strategy_package_runtime_assets_incomplete", "package_id": manifest.package_id},
        )
    return model


def _runtime_factor_name(factor: FactorAsset, *, package_id: str) -> str:
    name = str(factor.factor_name or factor.factor_id or "").strip()
    if not name:
        raise PackageAssetInvalidError(
            "strategy package runtime factor asset is missing factor_name",
            context={"reason_code": "strategy_package_runtime_assets_incomplete", "package_id": package_id},
        )
    if name in {".", ".."} or any(sep in name for sep in ("/", "\\", ":")):
        raise PackageAssetInvalidError(
            "strategy package runtime factor_name must be a safe file stem",
            context={
                "reason_code": "strategy_package_runtime_factor_name_invalid",
                "package_id": package_id,
                "factor_name": name,
            },
        )
    return name


def _safe_model_code_relpath(asset: ModelCodeAsset) -> Path:
    pure = PurePosixPath(str(asset.relative_path or "").replace("\\", "/"))
    if (
        not str(pure)
        or pure.is_absolute()
        or any(part in {"", ".", ".."} for part in pure.parts)
        or pure.suffix != ".py"
    ):
        raise PackageAssetInvalidError(
            "frozen StrategyPackage model code asset path is invalid",
            context={
                "reason_code": "strategy_package_model_code_path_invalid",
                "relative_path": asset.relative_path,
                "module_name": asset.module_name,
            },
        )
    return Path(*pure.parts)


_LOCAL_PICKLED_MODEL_MODULES = frozenset({"model"})


def _model_code_module_exists(module_name: str, roots: list[Path]) -> bool:
    relative_path = Path(*module_name.split(".")) if "." in module_name else Path(f"{module_name}.py")
    if "." in module_name:
        relative_path = relative_path.with_suffix(".py")
    for root in roots:
        if (root / relative_path).exists() and (root / relative_path).is_file():
            return True
    return False


def _require_model_code_for_pickled_local_modules(
    *,
    model_params_path: Path,
    model_code_roots: list[Path],
    package_id: str | None,
    experiment_id: str | None,
    source_workspace_type: str,
    phase: str,
) -> list[str]:
    if not model_params_path.exists() or not model_params_path.is_file():
        return []
    referenced_refs = pickled_model_code_references_from_params_bytes(
        model_params_path.read_bytes(),
        _LOCAL_PICKLED_MODEL_MODULES,
    )
    referenced = {ref.module_name for ref in referenced_refs}
    if not referenced:
        return []
    missing = [module for module in sorted(referenced) if not _model_code_module_exists(module, model_code_roots)]
    if missing:
        missing_set = set(missing)
        raise DataUnavailableError(
            "StrategyPackage model params.pkl references local model code that is missing from the runtime workspace",
            context={
                "reason_code": "strategy_package_model_code_missing",
                "package_id": package_id,
                "experiment_id": experiment_id,
                "model_params_path": str(model_params_path),
                "missing_modules": missing,
                "missing_relative_paths": [f"{module}.py" for module in missing],
                "referenced_classes": [ref.qualified_name for ref in referenced_refs],
                "missing_referenced_classes": [
                    ref.qualified_name for ref in referenced_refs if ref.module_name in missing_set
                ],
                "model_code_roots": [str(path) for path in model_code_roots],
                "source_workspace_type": source_workspace_type,
                "phase": phase,
            },
        )
    return sorted(referenced)


def _write_inside_runtime_source(target_path: Path, data: bytes, *, source_dir: Path, package_id: str) -> None:
    source_root = source_dir.resolve(strict=False)
    target = target_path.resolve(strict=False)
    if source_root not in target.parents:
        raise ArtifactGenerationFailedError(
            "refusing to materialize StrategyPackage asset outside the runtime source cache",
            context={
                "reason_code": "strategy_package_runtime_asset_path_invalid",
                "package_id": package_id,
                "target_path": str(target_path),
                "source_dir": str(source_dir),
            },
        )
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(data)


def _manifest_source_text(source_evidence: dict[str, Any], key: str) -> str | None:
    value = source_evidence.get(key)
    text = str(value or "").strip()
    return text or None


def _manifest_runtime_experiment_id(manifest: StrategyPackageManifest) -> str:
    source_evidence = manifest.source_evidence if isinstance(manifest.source_evidence, dict) else {}
    for value in (
        source_evidence.get("experiment_id"),
        manifest.source.source_id,
        manifest.source.run_id,
        manifest.package_id,
    ):
        text = str(value or "").strip()
        if text:
            return text
    return manifest.package_id


def _manifest_runtime_custom_params(
    manifest: StrategyPackageManifest,
    *,
    runtime_assets_override: RuntimeAssetManifest | None = None,
) -> dict[str, Any]:
    source_evidence = manifest.source_evidence if isinstance(manifest.source_evidence, dict) else {}
    backtest_context = manifest.backtest_context if isinstance(manifest.backtest_context, dict) else {}
    custom = source_evidence.get("custom_params")
    if not isinstance(custom, dict):
        daily_strategy = backtest_context.get("daily_strategy")
        if isinstance(daily_strategy, dict) and isinstance(daily_strategy.get("custom_params"), dict):
            custom = daily_strategy["custom_params"]
        else:
            custom = {}
    runtime_custom = dict(custom)
    runtime_assets = runtime_assets_override if runtime_assets_override is not None else manifest.runtime_assets
    if runtime_assets is None:
        runtime_custom["disable_alpha158"] = True
        runtime_custom["runtime_contract_source"] = "strategy_package_package_assets_legacy"
        return runtime_custom
    alpha158 = runtime_assets.alpha158
    if alpha158.enabled and not (alpha158.asset_ref and alpha158.sha256 and alpha158.aliases):
        raise PackageAssetInvalidError(
            "package-owned runtime Alpha158 schema is incomplete",
            context={
                "reason_code": "strategy_package_alpha158_schema_missing",
                "package_id": manifest.package_id,
                "asset_ref": alpha158.asset_ref,
                "sha256": alpha158.sha256,
                "alias_count": len(alpha158.aliases),
            },
        )
    runtime_custom["disable_alpha158"] = not alpha158.enabled
    runtime_custom["runtime_contract_source"] = "strategy_package_package_assets_v2"
    return runtime_custom


def _manifest_runtime_data_split(source_evidence: dict[str, Any], backtest_context: dict[str, Any]) -> dict[str, Any]:
    data_split = source_evidence.get("data_split")
    if isinstance(data_split, dict):
        return dict(data_split)
    data_split = backtest_context.get("data_split")
    return dict(data_split) if isinstance(data_split, dict) else {}


def _remote_relpath(value: str) -> str:
    text = str(value or "").strip().replace("\\", "/")
    if not text:
        raise RuntimeConfigInvalidError("remote workspace file path is empty")
    if ":" in text:
        raise RuntimeConfigInvalidError(
            "absolute or drive-qualified QE workspace paths are not allowed",
            context={"path": value},
        )
    pure = PurePosixPath(text)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        raise RuntimeConfigInvalidError(
            "QE workspace file path must be a safe relative path",
            context={"path": value},
        )
    return str(pure)


def _score_rows_from_frame(df_scores: pd.DataFrame, expected_date: date) -> list[dict[str, Any]]:
    if df_scores is None or df_scores.empty:
        raise DataUnavailableError(
            "live QE model inference produced no score rows",
            context={"expected_trade_date": expected_date.isoformat()},
        )
    if isinstance(df_scores, pd.Series):
        df_scores = df_scores.to_frame(name="score")
    if "score" not in df_scores.columns:
        if len(df_scores.columns) == 1:
            df_scores = df_scores.rename(columns={df_scores.columns[0]: "score"})
        else:
            raise ArtifactGenerationFailedError(
                "live QE model inference output is missing score column",
                context={"columns": [str(item) for item in df_scores.columns]},
            )
    if not isinstance(df_scores.index, pd.MultiIndex):
        raise ArtifactGenerationFailedError(
            "live QE model inference output must use MultiIndex(datetime, instrument)",
            context={"index_type": type(df_scores.index).__name__},
        )
    names = list(df_scores.index.names)
    if "datetime" not in names or "instrument" not in names:
        raise ArtifactGenerationFailedError(
            "live QE model inference output index must contain datetime and instrument",
            context={"index_names": names},
        )

    actual_dates = sorted(set(pd.to_datetime(df_scores.index.get_level_values("datetime")).date))
    if actual_dates != [expected_date]:
        raise DataUnavailableError(
            "live QE model inference did not score the requested trade_date exactly",
            context={
                "expected_trade_date": expected_date.isoformat(),
                "actual_dates": [item.isoformat() for item in actual_dates],
            },
        )

    day = df_scores.reset_index()
    day["symbol"] = day["instrument"].astype(str)
    day["score"] = pd.to_numeric(day["score"], errors="coerce")
    if not day["score"].map(lambda value: pd.notna(value) and math.isfinite(float(value))).all():
        raise ArtifactGenerationFailedError(
            "live QE model inference output contains invalid scores",
            context={"row_count": int(len(day))},
        )
    day = day.sort_values(["score", "symbol"], ascending=[False, True]).reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    for rank, item in enumerate(day.itertuples(index=False), start=1):
        rows.append({"symbol": str(item.symbol), "score": float(item.score), "rank": rank})
    if not rows:
        raise DataUnavailableError(
            "live QE model inference produced an empty ranked universe",
            context={"expected_trade_date": expected_date.isoformat()},
        )
    return rows


class QEExperimentRuntimeAssetResolver:
    """Resolve QE source assets required for live model inference."""

    def __init__(
        self,
        *,
        conn_factory: ConnFactory | None = None,
        cache_root: Path | str | None = None,
        asset_store: PackageAssetStore | None = None,
    ) -> None:
        self._conn_factory = conn_factory or get_conn
        self.cache_root = Path(cache_root or Path("rdagent_assets") / "strategy_package_runtime")
        self.asset_store = asset_store or LocalPackageAssetStore()

    def load_source(self, experiment_id: str) -> QEExperimentRuntimeSource:
        experiment_id = str(experiment_id or "").strip()
        if not experiment_id:
            raise RuntimeConfigInvalidError("QE experiment_id is required for live inference")
        row = self._load_experiment_row_by_id(experiment_id)
        return self._source_from_experiment_row(row, source_lookup={"experiment_id": experiment_id})

    def load_source_for_strategy_package(
        self,
        *,
        source_type: str,
        source_id: str,
        loop_id: str | None = None,
        run_id: str | None = None,
        manifest: StrategyPackageManifest | None = None,
        package_id: str | None = None,
    ) -> QEExperimentRuntimeSource:
        """Resolve runtime source using the frozen StrategyPackage source identity."""

        if manifest is not None and (
            manifest_has_frozen_runtime_assets(manifest) or manifest.runtime_assets is not None
        ):
            return self._source_from_package_assets(manifest, package_id=package_id)

        normalized_type = str(source_type or "").strip()
        normalized_source_id = str(source_id or "").strip()
        normalized_loop_id = str(loop_id or "").strip()
        normalized_run_id = str(run_id or "").strip()
        if normalized_type == "qe_evolution_loop":
            if not normalized_source_id or not normalized_loop_id:
                raise DataUnavailableError(
                    "QE evolution loop package is missing qe_task_id/qe_loop_id for live inference",
                    context={
                        "source_type": normalized_type,
                        "source_id": normalized_source_id or None,
                        "loop_id": normalized_loop_id or None,
                        "run_id": normalized_run_id or None,
                    },
                )
            row = self._load_experiment_row_by_task_loop(
                qe_task_id=normalized_source_id,
                qe_loop_id=normalized_loop_id,
            )
            return self._source_from_experiment_row(
                row,
                source_lookup={
                    "source_type": normalized_type,
                    "source_id": normalized_source_id,
                    "loop_id": normalized_loop_id,
                    "run_id": normalized_run_id or None,
                },
            )

        if normalized_type == "qe_experiment":
            if not normalized_source_id:
                raise RuntimeConfigInvalidError("QE experiment source_id is required for live inference")
            return self.load_source(normalized_source_id)

        if normalized_type == "candidate_strategy_package":
            if not normalized_source_id:
                raise RuntimeConfigInvalidError("Candidate StrategyPackage source_id is required for live inference")
            candidate = self._load_candidate_strategy_package_source(normalized_source_id)
            candidate_source_type = str(candidate.get("source_type") or "").strip()
            if candidate_source_type == "qe_evolution_loop":
                qe_task_id = str(candidate.get("source_task_id") or "").strip()
                qe_loop_id = self._normalize_candidate_qe_loop_id(
                    qe_task_id=qe_task_id,
                    source_loop_id=str(candidate.get("source_loop_id") or "").strip(),
                    source_id=str(candidate.get("source_id") or "").strip(),
                )
                if not qe_task_id or not qe_loop_id:
                    raise DataUnavailableError(
                        "Candidate StrategyPackage is missing qe_task_id/qe_loop_id for live inference",
                        context={"candidate_id": normalized_source_id, "candidate_source": candidate},
                    )
                row = self._load_experiment_row_by_task_loop(qe_task_id=qe_task_id, qe_loop_id=qe_loop_id)
                return self._source_from_experiment_row(
                    row,
                    source_lookup={
                        "source_type": normalized_type,
                        "source_id": normalized_source_id,
                        "candidate_source_type": candidate_source_type,
                        "qe_task_id": qe_task_id,
                        "qe_loop_id": qe_loop_id,
                        "run_id": str(candidate.get("source_experiment_id") or "") or normalized_run_id or None,
                    },
                )
            if candidate_source_type == "qe_experiment":
                experiment_id = str(candidate.get("source_experiment_id") or candidate.get("source_id") or "").strip()
                if not experiment_id:
                    raise DataUnavailableError(
                        "Candidate StrategyPackage is missing experiment_id for live inference",
                        context={"candidate_id": normalized_source_id, "candidate_source": candidate},
                    )
                return self.load_source(experiment_id)
            raise PackageAssetInvalidError(
                "unsupported Candidate StrategyPackage source_type for live inference",
                context={
                    "candidate_id": normalized_source_id,
                    "candidate_source_type": candidate_source_type,
                    "supported": ["qe_experiment", "qe_evolution_loop"],
                },
            )

        raise PackageAssetInvalidError(
            "unsupported StrategyPackage source_type for live inference",
            context={
                "source_type": normalized_type,
                "supported": ["qe_experiment", "qe_evolution_loop", "candidate_strategy_package"],
            },
            )

    def load_source_for_strategy_package_leg(
        self,
        *,
        manifest: StrategyPackageManifest,
        package_id: str,
        leg_id: str,
        model_asset: ModelAsset,
        factor_set: list[FactorAsset],
        runtime_assets: RuntimeAssetManifest | None,
    ) -> QEExperimentRuntimeSource:
        """Resolve one MULTI_ALPHA leg strictly from parent package-owned assets."""

        return self._source_from_package_assets(
            manifest,
            package_id=package_id,
            model_asset_override=model_asset,
            factor_set_override=factor_set,
            runtime_assets_override=runtime_assets,
            cache_namespace=f"leg_{_safe_cache_component(leg_id)}",
        )

    def _source_from_package_assets(
        self,
        manifest: StrategyPackageManifest,
        *,
        package_id: str | None,
        model_asset_override: ModelAsset | None = None,
        factor_set_override: list[FactorAsset] | None = None,
        runtime_assets_override: RuntimeAssetManifest | None = None,
        cache_namespace: str | None = None,
    ) -> QEExperimentRuntimeSource:
        package_key = str(package_id or manifest.package_id or "").strip()
        manifest_sha = str(manifest.manifest_sha256 or "").strip().lower()
        if not package_key or not manifest_sha:
            raise PackageAssetInvalidError(
                "frozen StrategyPackage manifest identity is required for package-owned runtime assets",
                context={
                    "reason_code": "strategy_package_runtime_manifest_identity_missing",
                    "package_id": package_key or None,
                    "manifest_sha256": manifest_sha or None,
                },
            )

        model_asset = model_asset_override or _single_model_asset_for_runtime(manifest)
        namespace = _safe_cache_component(cache_namespace or "")
        source_dir = (
            self.cache_root
            / "_package_asset_sources"
            / _safe_cache_component(package_key)
            / _safe_cache_component(manifest_sha[:16])
        )
        if namespace:
            source_dir = source_dir / namespace
        self._reset_cache_dir(source_dir)
        factors_dir = source_dir / "factors"
        model_dir = source_dir / "mlruns" / "package_asset" / "artifacts"
        factors_dir.mkdir(parents=True, exist_ok=True)
        model_dir.mkdir(parents=True, exist_ok=True)

        factor_names: list[str] = []
        factors = list(factor_set_override) if factor_set_override is not None else list(manifest.factor_set)
        for factor in factors:
            factor_name = _runtime_factor_name(factor, package_id=package_key)
            factor_names.append(factor_name)
            payload = self._read_package_asset_bytes(
                asset_ref=factor.asset_ref,
                expected_sha256=factor.sha256,
                package_id=package_key,
                asset_kind="factor_code",
                logical_name=factor_name,
            )
            _write_inside_runtime_source(
                factors_dir / f"{factor_name}.py",
                payload,
                source_dir=source_dir,
                package_id=package_key,
            )

        model_payload = self._read_package_asset_bytes(
            asset_ref=model_asset.asset_ref,
            expected_sha256=model_asset.sha256,
            package_id=package_key,
            asset_kind="model_weight",
            logical_name=str(model_asset.model_id),
        )
        _write_inside_runtime_source(
            model_dir / "params.pkl",
            model_payload,
            source_dir=source_dir,
            package_id=package_key,
        )
        self._materialize_model_code_assets(
            model_asset,
            model_dir=model_dir,
            source_dir=source_dir,
            package_id=package_key,
        )
        self._materialize_alpha158_conf(
            manifest,
            source_dir=source_dir,
            package_id=package_key,
            runtime_assets_override=runtime_assets_override,
        )

        source_evidence = manifest.source_evidence if isinstance(manifest.source_evidence, dict) else {}
        backtest_context = manifest.backtest_context if isinstance(manifest.backtest_context, dict) else {}
        custom_params = _manifest_runtime_custom_params(manifest, runtime_assets_override=runtime_assets_override)
        data_split = _manifest_runtime_data_split(source_evidence, backtest_context)
        experiment_id = _manifest_runtime_experiment_id(manifest)
        return QEExperimentRuntimeSource(
            experiment_id=experiment_id,
            db_workspace_path=Path(),
            asset_workspace_path=source_dir,
            factor_names=factor_names,
            custom_params=custom_params,
            data_split=data_split,
            qe_task_id=_manifest_source_text(source_evidence, "qe_task_id"),
            qe_loop_id=_manifest_source_text(source_evidence, "qe_loop_id"),
            execution_node_id=None,
            model_params_origin="package_asset",
            source_workspace_type="strategy_package_asset_store",
            package_id=package_key,
            manifest_sha256=manifest_sha,
        )

    def _materialize_alpha158_conf(
        self,
        manifest: StrategyPackageManifest,
        *,
        source_dir: Path,
        package_id: str,
        runtime_assets_override: RuntimeAssetManifest | None = None,
    ) -> None:
        runtime_assets = runtime_assets_override if runtime_assets_override is not None else manifest.runtime_assets
        if runtime_assets is None or not runtime_assets.alpha158.enabled:
            (source_dir / "conf.yaml").write_text("task: {}\n", encoding="utf-8")
            return
        alpha158 = runtime_assets.alpha158
        try:
            payload = self._read_package_asset_bytes(
                asset_ref=alpha158.asset_ref,
                expected_sha256=alpha158.sha256,
                package_id=package_id,
                asset_kind="factor_schema",
                logical_name="alpha158_schema",
            )
        except PackageAssetInvalidError as exc:
            reason_code = (exc.context or {}).get("reason_code")
            mapped_reason = (
                "strategy_package_alpha158_schema_sha_mismatch"
                if reason_code == "strategy_package_asset_sha_mismatch"
                else "strategy_package_alpha158_schema_missing"
            )
            raise PackageAssetInvalidError(
                "frozen Alpha158 schema asset is unavailable or invalid",
                context={**(exc.context or {}), "reason_code": mapped_reason, "package_id": package_id},
            ) from exc
        try:
            schema = json.loads(payload.decode("utf-8"))
        except Exception as exc:
            raise PackageAssetInvalidError(
                "frozen Alpha158 schema asset is not valid JSON",
                context={"reason_code": "strategy_package_alpha158_schema_invalid", "package_id": package_id, "error": str(exc)},
            ) from exc
        if schema.get("schema_version") != ALPHA158_SCHEMA_VERSION:
            raise PackageAssetInvalidError(
                "frozen Alpha158 schema asset version is unsupported",
                context={
                    "reason_code": "strategy_package_alpha158_schema_invalid",
                    "package_id": package_id,
                    "schema_version": schema.get("schema_version"),
                },
            )
        aliases = extract_alpha158_aliases(schema.get("loader_node"))
        if aliases != list(alpha158.aliases):
            raise PackageAssetInvalidError(
                "frozen Alpha158 schema aliases do not match manifest",
                context={
                    "reason_code": "strategy_package_alpha158_schema_alias_mismatch",
                    "package_id": package_id,
                    "manifest_aliases": list(alpha158.aliases),
                    "schema_aliases": aliases,
                },
            )
        (source_dir / "conf.yaml").write_text(minimal_conf_with_alpha158(schema), encoding="utf-8")

    def _materialize_model_code_assets(
        self,
        model_asset: ModelAsset,
        *,
        model_dir: Path,
        source_dir: Path,
        package_id: str,
    ) -> None:
        assets = list(model_asset.model_code_assets or [])
        if model_asset.model_code_required and not assets:
            raise PackageAssetInvalidError(
                "frozen StrategyPackage model code asset is required but missing",
                context={
                    "reason_code": "strategy_package_model_code_missing",
                    "package_id": package_id,
                    "model_id": model_asset.model_id,
                },
            )
        for asset in assets:
            payload = self._read_package_asset_bytes(
                asset_ref=asset.asset_ref,
                expected_sha256=asset.sha256,
                package_id=package_id,
                asset_kind="model_code",
                logical_name=asset.relative_path,
            )
            target = model_dir / _safe_model_code_relpath(asset)
            _write_inside_runtime_source(target, payload, source_dir=source_dir, package_id=package_id)

    def _read_package_asset_bytes(
        self,
        *,
        asset_ref: str | None,
        expected_sha256: str | None,
        package_id: str,
        asset_kind: str,
        logical_name: str,
    ) -> bytes:
        if not asset_ref or not expected_sha256:
            raise PackageAssetInvalidError(
                "frozen StrategyPackage runtime asset is missing asset_ref or sha256",
                context={
                    "reason_code": "strategy_package_runtime_assets_incomplete",
                    "package_id": package_id,
                    "asset_kind": asset_kind,
                    "logical_name": logical_name,
                    "asset_ref": asset_ref,
                    "expected_sha256": expected_sha256,
                },
            )
        try:
            data = self.asset_store.get(asset_ref)
        except PackageAssetInvalidError as exc:
            raise PackageAssetInvalidError(
                exc.message,
                context={
                    **(exc.context or {}),
                    "package_id": package_id,
                    "asset_kind": asset_kind,
                    "logical_name": logical_name,
                },
            ) from exc
        actual = hashlib.sha256(data).hexdigest()
        expected = str(expected_sha256).strip().lower()
        if actual != expected:
            raise PackageAssetInvalidError(
                "strategy package runtime asset sha256 mismatch",
                context={
                    "reason_code": "strategy_package_asset_sha_mismatch",
                    "package_id": package_id,
                    "asset_kind": asset_kind,
                    "logical_name": logical_name,
                    "asset_ref": asset_ref,
                    "expected_sha256": expected,
                    "actual_sha256": actual,
                },
            )
        return data

    def preflight_for_strategy_package(
        self,
        *,
        source_type: str,
        source_id: str,
        loop_id: str | None = None,
        run_id: str | None = None,
        runtime_config: dict[str, Any] | None = None,
        manifest: StrategyPackageManifest | None = None,
        package_id: str | None = None,
    ) -> LiveInferencePreflightResult:
        """Cold-start preflight for live inference (P0-F / Codex doc P0-4).

        Runs five fail-fast checks before any heavy downstream work:

        1. ``qe_source``     - QE experiment row resolves
        2. ``qe_node``       - execution_node_id resolves and is non-empty
        3. ``conf_yaml``     - QE conf.yaml exists in the materialized
                               asset workspace
        4. ``factor_source`` - QE factors directory exists; declared factor
                               files are present
        5. ``model_params``  - QE model params.pkl is locatable (explicit
                               override or via mlruns artifact glob)

        Each check produces a structured ``LiveInferencePreflightCheck`` with
        ``status`` (``PASS`` / ``BLOCKED``), an operator-facing ``message``
        and ``suggestion``, plus a ``context`` payload for the UI. The first
        BLOCKED check short-circuits further checks (downstream work would
        always fail, and we want fast rejection — not 30-minute hangs as in
        the cold-start incident history).

        This method NEVER mutates assets (no DB writes, no fresh downloads
        beyond what ``load_source_for_strategy_package`` already performs).
        """

        config = runtime_config or {}
        artifact_config = config.get("selection_artifact_config")
        if artifact_config is None:
            artifact_config = config.get("selection_artifact") or {}
        if artifact_config and not isinstance(artifact_config, dict):
            return LiveInferencePreflightResult(
                passed=False,
                checks=[
                    LiveInferencePreflightCheck(
                        name=PREFLIGHT_CHECK_QE_SOURCE,
                        status=PREFLIGHT_STATUS_BLOCKED,
                        message="selection_artifact_config must be an object for live inference preflight",
                        suggestion=(
                            "ensure runtime_config.selection_artifact_config is a JSON object, "
                            "not a string or list"
                        ),
                        context={"actual_type": type(artifact_config).__name__},
                    ),
                    *_remaining_skipped_checks(after=PREFLIGHT_CHECK_QE_SOURCE),
                ],
            )

        checks: list[LiveInferencePreflightCheck] = []

        if manifest is not None and manifest.alpha_mode == AlphaMode.MULTI_ALPHA:
            return self._preflight_for_multi_alpha_parent_package(
                source_type=source_type,
                source_id=source_id,
                loop_id=loop_id,
                run_id=run_id,
                artifact_config=artifact_config,
                manifest=manifest,
                package_id=package_id,
            )

        # ---- Check 1: qe_source ----
        try:
            source_kwargs: dict[str, Any] = {
                "source_type": source_type,
                "source_id": source_id,
                "loop_id": loop_id,
                "run_id": run_id,
            }
            if manifest is not None:
                source_kwargs["manifest"] = manifest
            if package_id is not None:
                source_kwargs["package_id"] = package_id
            source = self.load_source_for_strategy_package(**source_kwargs)
        except TradingCoreError as exc:
            checks.append(
                LiveInferencePreflightCheck(
                    name=PREFLIGHT_CHECK_QE_SOURCE,
                    status=PREFLIGHT_STATUS_BLOCKED,
                    message=exc.message,
                    suggestion=(
                        "verify the StrategyPackage source identity (source_type / source_id"
                        " / loop_id) points to a completed QE experiment"
                    ),
                    context={
                        "source_type": source_type,
                        "source_id": source_id,
                        "loop_id": loop_id,
                        "run_id": run_id,
                        **(exc.context or {}),
                    },
                )
            )
            checks.extend(_remaining_skipped_checks(after=PREFLIGHT_CHECK_QE_SOURCE))
            return LiveInferencePreflightResult(passed=False, checks=checks)

        checks.append(
            LiveInferencePreflightCheck(
                name=PREFLIGHT_CHECK_QE_SOURCE,
                status=PREFLIGHT_STATUS_PASS,
                message=(
                    "StrategyPackage frozen runtime assets resolved"
                    if source.source_workspace_type == "strategy_package_asset_store"
                    else "QE experiment source resolved"
                ),
                context={
                    "experiment_id": source.experiment_id,
                    "qe_task_id": source.qe_task_id,
                    "qe_loop_id": source.qe_loop_id,
                    "package_id": source.package_id,
                    "source_workspace_type": source.source_workspace_type,
                },
            )
        )

        # ---- Check 2: qe_node ----
        execution_node_id = (source.execution_node_id or "").strip()
        if source.source_workspace_type == "strategy_package_asset_store":
            checks.append(
                LiveInferencePreflightCheck(
                    name=PREFLIGHT_CHECK_QE_NODE,
                    status=PREFLIGHT_STATUS_PASS,
                    message="package-owned runtime assets do not require QE node access",
                    context={
                        "package_id": source.package_id,
                        "manifest_sha256": source.manifest_sha256,
                        "asset_workspace_path": str(source.asset_workspace_path),
                    },
                )
            )
        elif not execution_node_id:
            checks.append(
                LiveInferencePreflightCheck(
                    name=PREFLIGHT_CHECK_QE_NODE,
                    status=PREFLIGHT_STATUS_BLOCKED,
                    message="QE execution_node_id is missing for live inference",
                    suggestion=(
                        "set execution_node_id in qe_experiments.custom_params.execution_node_id"
                        " or ensure resolve_default_qe_node_id() returns a non-empty value"
                    ),
                    context={"experiment_id": source.experiment_id},
                )
            )
            checks.extend(_remaining_skipped_checks(after=PREFLIGHT_CHECK_QE_NODE))
            return LiveInferencePreflightResult(passed=False, checks=checks)
        else:
            checks.append(
                LiveInferencePreflightCheck(
                    name=PREFLIGHT_CHECK_QE_NODE,
                    status=PREFLIGHT_STATUS_PASS,
                    message="QE execution node resolved",
                    context={
                        "execution_node_id": execution_node_id,
                        "asset_workspace_path": str(source.asset_workspace_path),
                    },
                )
            )

        # ---- Check 3: conf.yaml ----
        try:
            conf_path = self._resolve_conf_path(source)
        except DataUnavailableError as exc:
            checks.append(
                LiveInferencePreflightCheck(
                    name=PREFLIGHT_CHECK_CONF_YAML,
                    status=PREFLIGHT_STATUS_BLOCKED,
                    message=exc.message,
                    suggestion=(
                        "ensure the QE node downloaded conf.yaml into"
                        " asset_workspace_path; rerun the QE workspace export if missing"
                    ),
                    context={"experiment_id": source.experiment_id, **(exc.context or {})},
                )
            )
            checks.extend(_remaining_skipped_checks(after=PREFLIGHT_CHECK_CONF_YAML))
            return LiveInferencePreflightResult(passed=False, checks=checks)

        checks.append(
            LiveInferencePreflightCheck(
                name=PREFLIGHT_CHECK_CONF_YAML,
                status=PREFLIGHT_STATUS_PASS,
                message="QE conf.yaml is present",
                context={"conf_path": str(conf_path)},
            )
        )

        # ---- Check 4: factor_source ----
        try:
            factor_source_dir = self._resolve_factor_source_dir(source)
        except DataUnavailableError as exc:
            checks.append(
                LiveInferencePreflightCheck(
                    name=PREFLIGHT_CHECK_FACTOR_SOURCE,
                    status=PREFLIGHT_STATUS_BLOCKED,
                    message=exc.message,
                    suggestion=(
                        "verify the QE workspace export includes the factors/ directory;"
                        " rerun export if needed"
                    ),
                    context={"experiment_id": source.experiment_id, **(exc.context or {})},
                )
            )
            checks.extend(_remaining_skipped_checks(after=PREFLIGHT_CHECK_FACTOR_SOURCE))
            return LiveInferencePreflightResult(passed=False, checks=checks)

        # cheap declared-factor presence check: just look for any one declared
        # factor file under the source dir. Full per-factor verification is
        # done later inside prepare_workspace; here we only want fast rejection
        # when factors/ exists but is empty / missing the expected factor set.
        missing_factor_samples: list[str] = []
        sample_factors = list(source.factor_names[:3])  # at most 3 samples
        for factor_name in sample_factors:
            candidate = factor_source_dir / f"{factor_name}.py"
            if not candidate.exists() or not candidate.is_file():
                missing_factor_samples.append(factor_name)
        if missing_factor_samples and len(missing_factor_samples) == len(sample_factors):
            checks.append(
                LiveInferencePreflightCheck(
                    name=PREFLIGHT_CHECK_FACTOR_SOURCE,
                    status=PREFLIGHT_STATUS_BLOCKED,
                    message="QE factor source files are missing for declared factors",
                    suggestion=(
                        "rerun the QE workspace export to refresh factors/, or check"
                        " qe_experiments.factor_names matches the workspace contents"
                    ),
                    context={
                        "experiment_id": source.experiment_id,
                        "factor_source_dir": str(factor_source_dir),
                        "missing_factor_samples": missing_factor_samples,
                        "factor_names_count": len(source.factor_names),
                    },
                )
            )
            checks.extend(_remaining_skipped_checks(after=PREFLIGHT_CHECK_FACTOR_SOURCE))
            return LiveInferencePreflightResult(passed=False, checks=checks)

        checks.append(
            LiveInferencePreflightCheck(
                name=PREFLIGHT_CHECK_FACTOR_SOURCE,
                status=PREFLIGHT_STATUS_PASS,
                message="QE factor source directory and sampled factor files are present",
                context={
                    "factor_source_dir": str(factor_source_dir),
                    "factor_names_count": len(source.factor_names),
                    "sampled_factors": sample_factors,
                },
            )
        )

        # ---- Check 5: model_params ----
        try:
            model_params_path, candidate_count = self._resolve_model_params_path(
                source, artifact_config
            )
            referenced_model_modules = _require_model_code_for_pickled_local_modules(
                model_params_path=model_params_path,
                model_code_roots=[source.asset_workspace_path, model_params_path.parent],
                package_id=source.package_id,
                experiment_id=source.experiment_id,
                source_workspace_type=source.source_workspace_type,
                phase="preflight",
            )
        except DataUnavailableError as exc:
            checks.append(
                LiveInferencePreflightCheck(
                    name=PREFLIGHT_CHECK_MODEL_PARAMS,
                    status=PREFLIGHT_STATUS_BLOCKED,
                    message=exc.message,
                    suggestion=(
                        "ensure mlruns/<run>/artifacts/params.pkl exists in the QE"
                        " workspace, or pass an explicit selection_artifact_config.model_params_path"
                    ),
                    context={"experiment_id": source.experiment_id, **(exc.context or {})},
                )
            )
            return LiveInferencePreflightResult(passed=False, checks=checks)

        checks.append(
            LiveInferencePreflightCheck(
                name=PREFLIGHT_CHECK_MODEL_PARAMS,
                status=PREFLIGHT_STATUS_PASS,
                message="QE model params.pkl is locatable",
                context={
                    "model_params_path": str(model_params_path),
                    "candidate_count": candidate_count,
                    "referenced_model_modules": referenced_model_modules,
                },
            )
        )

        return LiveInferencePreflightResult(passed=True, checks=checks)

    def _preflight_for_multi_alpha_parent_package(
        self,
        *,
        source_type: str,
        source_id: str,
        loop_id: str | None,
        run_id: str | None,
        artifact_config: dict[str, Any],
        manifest: StrategyPackageManifest,
        package_id: str | None,
    ) -> LiveInferencePreflightResult:
        """Run cold-start preflight for MULTI_ALPHA parent-owned leg assets."""

        package_key = str(package_id or manifest.package_id or "").strip()
        checks: list[LiveInferencePreflightCheck] = []
        try:
            from .multi_alpha_live import _multi_alpha_evidence, _parent_leg_runtime_slices

            evidence = _multi_alpha_evidence(manifest)
            leg_slices = _parent_leg_runtime_slices(manifest, evidence=evidence, package_id=package_key)
        except TradingCoreError as exc:
            checks.append(
                LiveInferencePreflightCheck(
                    name=PREFLIGHT_CHECK_QE_SOURCE,
                    status=PREFLIGHT_STATUS_BLOCKED,
                    message=exc.message,
                    suggestion=(
                        "verify the MULTI_ALPHA parent package embeds every leg model, "
                        "factor source, and Alpha158 runtime schema asset"
                    ),
                    context={
                        "source_type": source_type,
                        "source_id": source_id,
                        "loop_id": loop_id,
                        "run_id": run_id,
                        "package_id": package_key,
                        "alpha_mode": AlphaMode.MULTI_ALPHA.value,
                        **(exc.context or {}),
                    },
                )
            )
            checks.extend(_remaining_skipped_checks(after=PREFLIGHT_CHECK_QE_SOURCE))
            return LiveInferencePreflightResult(passed=False, checks=checks)

        leg_sources: list[tuple[Any, QEExperimentRuntimeSource]] = []
        for leg_slice in leg_slices:
            try:
                source = self.load_source_for_strategy_package_leg(
                    manifest=manifest,
                    package_id=package_key,
                    leg_id=leg_slice.leg_id,
                    model_asset=leg_slice.model_asset,
                    factor_set=list(leg_slice.factor_set),
                    runtime_assets=leg_slice.runtime_assets,
                )
            except TradingCoreError as exc:
                checks.append(
                    LiveInferencePreflightCheck(
                        name=PREFLIGHT_CHECK_QE_SOURCE,
                        status=PREFLIGHT_STATUS_BLOCKED,
                        message=exc.message,
                        suggestion=(
                            "verify the MULTI_ALPHA parent package embeds the missing or mismatched "
                            "asset for this leg"
                        ),
                        context={
                            "source_type": source_type,
                            "source_id": source_id,
                            "loop_id": loop_id,
                            "run_id": run_id,
                            "package_id": package_key,
                            "alpha_mode": AlphaMode.MULTI_ALPHA.value,
                            "leg_id": leg_slice.leg_id,
                            "model_id": leg_slice.component.model_id,
                            **(exc.context or {}),
                        },
                    )
                )
                checks.extend(_remaining_skipped_checks(after=PREFLIGHT_CHECK_QE_SOURCE))
                return LiveInferencePreflightResult(passed=False, checks=checks)
            leg_sources.append((leg_slice, source))

        leg_context = [
            {
                "leg_id": leg_slice.leg_id,
                "model_id": leg_slice.component.model_id,
                "factor_count": len(leg_slice.factor_set),
                "source_workspace_type": source.source_workspace_type,
                "model_params_origin": source.model_params_origin,
            }
            for leg_slice, source in leg_sources
        ]
        checks.append(
            LiveInferencePreflightCheck(
                name=PREFLIGHT_CHECK_QE_SOURCE,
                status=PREFLIGHT_STATUS_PASS,
                message="MULTI_ALPHA parent package leg runtime assets resolved",
                context={
                    "package_id": package_key,
                    "alpha_mode": AlphaMode.MULTI_ALPHA.value,
                    "leg_count": len(leg_sources),
                    "legs": leg_context,
                },
            )
        )

        checks.append(
            LiveInferencePreflightCheck(
                name=PREFLIGHT_CHECK_QE_NODE,
                status=PREFLIGHT_STATUS_PASS,
                message="MULTI_ALPHA parent package assets do not require QE node access",
                context={
                    "package_id": package_key,
                    "manifest_sha256": manifest.manifest_sha256,
                    "leg_count": len(leg_sources),
                },
            )
        )

        conf_paths: list[dict[str, Any]] = []
        active_leg_context: dict[str, Any] = {}
        try:
            for leg_slice, source in leg_sources:
                active_leg_context = {
                    "leg_id": leg_slice.leg_id,
                    "model_id": leg_slice.component.model_id,
                }
                conf_paths.append(
                    {
                        **active_leg_context,
                        "conf_path": str(self._resolve_conf_path(source)),
                    }
                )
        except DataUnavailableError as exc:
            checks.append(
                LiveInferencePreflightCheck(
                    name=PREFLIGHT_CHECK_CONF_YAML,
                    status=PREFLIGHT_STATUS_BLOCKED,
                    message=exc.message,
                    suggestion="verify the parent package Alpha158 schema asset can materialize conf.yaml for each leg",
                    context={
                        "package_id": package_key,
                        "alpha_mode": AlphaMode.MULTI_ALPHA.value,
                        **active_leg_context,
                        **(exc.context or {}),
                    },
                )
            )
            checks.extend(_remaining_skipped_checks(after=PREFLIGHT_CHECK_CONF_YAML))
            return LiveInferencePreflightResult(passed=False, checks=checks)

        checks.append(
            LiveInferencePreflightCheck(
                name=PREFLIGHT_CHECK_CONF_YAML,
                status=PREFLIGHT_STATUS_PASS,
                message="MULTI_ALPHA leg conf.yaml files are present",
                context={"package_id": package_key, "leg_count": len(conf_paths), "legs": conf_paths},
            )
        )

        factor_summaries: list[dict[str, Any]] = []
        active_leg_context = {}
        try:
            for leg_slice, source in leg_sources:
                active_leg_context = {
                    "leg_id": leg_slice.leg_id,
                    "model_id": leg_slice.component.model_id,
                }
                factor_source_dir = self._resolve_factor_source_dir(source)
                missing_factor_samples: list[str] = []
                sample_factors = list(source.factor_names[:3])
                for factor_name in sample_factors:
                    candidate = factor_source_dir / f"{factor_name}.py"
                    if not candidate.exists() or not candidate.is_file():
                        missing_factor_samples.append(factor_name)
                if missing_factor_samples and len(missing_factor_samples) == len(sample_factors):
                    checks.append(
                        LiveInferencePreflightCheck(
                            name=PREFLIGHT_CHECK_FACTOR_SOURCE,
                            status=PREFLIGHT_STATUS_BLOCKED,
                            message="MULTI_ALPHA leg factor source files are missing for declared factors",
                            suggestion="verify the parent package factor_set contains the leg factor source assets",
                            context={
                                "package_id": package_key,
                                "alpha_mode": AlphaMode.MULTI_ALPHA.value,
                                **active_leg_context,
                                "factor_source_dir": str(factor_source_dir),
                                "missing_factor_samples": missing_factor_samples,
                                "factor_names_count": len(source.factor_names),
                            },
                        )
                    )
                    checks.extend(_remaining_skipped_checks(after=PREFLIGHT_CHECK_FACTOR_SOURCE))
                    return LiveInferencePreflightResult(passed=False, checks=checks)
                factor_summaries.append(
                    {
                        "leg_id": leg_slice.leg_id,
                        "model_id": leg_slice.component.model_id,
                        "factor_source_dir": str(factor_source_dir),
                        "factor_names_count": len(source.factor_names),
                        "sampled_factors": sample_factors,
                    }
                )
        except DataUnavailableError as exc:
            checks.append(
                LiveInferencePreflightCheck(
                    name=PREFLIGHT_CHECK_FACTOR_SOURCE,
                    status=PREFLIGHT_STATUS_BLOCKED,
                    message=exc.message,
                    suggestion="verify the parent package factor_set contains every leg factor source asset",
                    context={
                        "package_id": package_key,
                        "alpha_mode": AlphaMode.MULTI_ALPHA.value,
                        **active_leg_context,
                        **(exc.context or {}),
                    },
                )
            )
            checks.extend(_remaining_skipped_checks(after=PREFLIGHT_CHECK_FACTOR_SOURCE))
            return LiveInferencePreflightResult(passed=False, checks=checks)

        checks.append(
            LiveInferencePreflightCheck(
                name=PREFLIGHT_CHECK_FACTOR_SOURCE,
                status=PREFLIGHT_STATUS_PASS,
                message="MULTI_ALPHA leg factor source directories and sampled factors are present",
                context={"package_id": package_key, "leg_count": len(factor_summaries), "legs": factor_summaries},
            )
        )

        model_summaries: list[dict[str, Any]] = []
        active_leg_context = {}
        try:
            for leg_slice, source in leg_sources:
                active_leg_context = {
                    "leg_id": leg_slice.leg_id,
                    "model_id": leg_slice.component.model_id,
                }
                model_params_path, candidate_count = self._resolve_model_params_path(source, artifact_config)
                referenced_model_modules = _require_model_code_for_pickled_local_modules(
                    model_params_path=model_params_path,
                    model_code_roots=[source.asset_workspace_path, model_params_path.parent],
                    package_id=source.package_id,
                    experiment_id=source.experiment_id,
                    source_workspace_type=source.source_workspace_type,
                    phase="preflight",
                )
                model_summaries.append(
                    {
                        **active_leg_context,
                        "model_params_path": str(model_params_path),
                        "candidate_count": candidate_count,
                        "referenced_model_modules": referenced_model_modules,
                    }
                )
        except TradingCoreError as exc:
            checks.append(
                LiveInferencePreflightCheck(
                    name=PREFLIGHT_CHECK_MODEL_PARAMS,
                    status=PREFLIGHT_STATUS_BLOCKED,
                    message=exc.message,
                    suggestion="verify every MULTI_ALPHA leg has a parent-owned model weight and required model code assets",
                    context={
                        "package_id": package_key,
                        "alpha_mode": AlphaMode.MULTI_ALPHA.value,
                        **active_leg_context,
                        **(exc.context or {}),
                    },
                )
            )
            return LiveInferencePreflightResult(passed=False, checks=checks)

        checks.append(
            LiveInferencePreflightCheck(
                name=PREFLIGHT_CHECK_MODEL_PARAMS,
                status=PREFLIGHT_STATUS_PASS,
                message="MULTI_ALPHA leg model params.pkl files are locatable",
                context={"package_id": package_key, "leg_count": len(model_summaries), "legs": model_summaries},
            )
        )

        return LiveInferencePreflightResult(passed=True, checks=checks)

    def require_preflight_or_raise(
        self,
        *,
        source_type: str,
        source_id: str,
        loop_id: str | None = None,
        run_id: str | None = None,
        runtime_config: dict[str, Any] | None = None,
        manifest: StrategyPackageManifest | None = None,
        package_id: str | None = None,
    ) -> LiveInferencePreflightResult:
        """Run preflight and raise ``LiveInferencePreflightError`` on failure.

        Selection Center calls this before any heavy ``generate_from_live_inference``
        invocation so failures surface fast (not after 30+ minutes inside
        ``prepare_workspace``). The raised error carries the full
        ``LiveInferencePreflightResult`` payload in its context for UI surfacing.
        """

        result = self.preflight_for_strategy_package(
            source_type=source_type,
            source_id=source_id,
            loop_id=loop_id,
            run_id=run_id,
            runtime_config=runtime_config,
            manifest=manifest,
            package_id=package_id,
        )
        if result.passed:
            return result
        blocked = result.blocked_check
        message = (
            blocked.message
            if blocked is not None
            else "live inference preflight failed without a specific blocked check"
        )
        raise LiveInferencePreflightError(
            f"live inference cold-start preflight failed: {message}",
            context={
                "source_type": source_type,
                "source_id": source_id,
                "loop_id": loop_id,
                "run_id": run_id,
                "package_id": package_id,
                "preflight": result.to_dict(),
                "blocked_check": blocked.name if blocked is not None else None,
            },
        )

    def _load_experiment_row_by_id(self, experiment_id: str) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT experiment_id, status, qe_task_id, qe_loop_id,
                           factor_names, custom_params, data_split, result_metrics
                    FROM qe_experiments
                    WHERE experiment_id = %s
                    """,
                    (experiment_id,),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "QE experiment does not exist for live inference",
                context={"experiment_id": experiment_id},
            )
        return dict(row)

    def _load_experiment_row_by_task_loop(self, *, qe_task_id: str, qe_loop_id: str) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT experiment_id, status, qe_task_id, qe_loop_id,
                           factor_names, custom_params, data_split, result_metrics
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
                "QE evolution loop does not exist for live inference",
                context={"qe_task_id": qe_task_id, "qe_loop_id": qe_loop_id},
            )
        return dict(row)

    def _load_candidate_strategy_package_source(self, candidate_id: str) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT candidate_id, source_type, source_id, source_task_id,
                           source_loop_id, source_experiment_id, status
                    FROM strategy_pkg.candidate_strategy_package
                    WHERE candidate_id = %s
                    """,
                    (candidate_id,),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "Candidate StrategyPackage does not exist for live inference",
                context={"candidate_id": candidate_id},
            )
        if str(row.get("status") or "").upper() != "ACTIVE":
            raise PackageAssetInvalidError(
                "Candidate StrategyPackage must be ACTIVE for live inference",
                context={"candidate_id": candidate_id, "status": row.get("status")},
            )
        return dict(row)

    @staticmethod
    def _normalize_candidate_qe_loop_id(*, qe_task_id: str, source_loop_id: str, source_id: str) -> str:
        for value in (source_loop_id, source_id):
            if not value:
                continue
            if qe_task_id and value.startswith(f"{qe_task_id}_"):
                return value[len(qe_task_id) + 1 :]
            if value.startswith("Loop"):
                return value
        return source_loop_id

    def _source_from_experiment_row(
        self,
        row: dict[str, Any],
        *,
        source_lookup: dict[str, Any],
    ) -> QEExperimentRuntimeSource:
        experiment_id = str(row["experiment_id"])
        if str(row["status"]).lower() != "completed":
            raise PackageAssetInvalidError(
                "QE experiment must be completed before live inference",
                context={"experiment_id": experiment_id, "status": row["status"]},
            )
        factor_names = _parse_jsonish(row.get("factor_names")) or []
        if not isinstance(factor_names, list) or not factor_names:
            raise PackageAssetInvalidError(
                "QE experiment has no factor_names for live inference",
                context={"experiment_id": experiment_id},
            )
        custom_params = _parse_jsonish(row.get("custom_params")) or {}
        data_split = _parse_jsonish(row.get("data_split")) or {}
        if not isinstance(custom_params, dict):
            raise PackageAssetInvalidError("QE experiment custom_params must be an object")
        if not isinstance(data_split, dict):
            raise PackageAssetInvalidError("QE experiment data_split must be an object")
        result_metrics = _parse_jsonish(row.get("result_metrics")) or {}
        if not isinstance(result_metrics, dict):
            result_metrics = {}
        execution_trace = result_metrics.get("execution_trace")
        if not isinstance(execution_trace, dict):
            execution_trace = {}

        qe_task_id = str(row.get("qe_task_id") or experiment_id).strip()
        qe_loop_id = str(row.get("qe_loop_id") or "").strip()
        if not qe_task_id or not qe_loop_id:
            raise DataUnavailableError(
                "QE experiment is missing qe_task_id/qe_loop_id for node API runtime asset resolution",
                context={
                    "experiment_id": experiment_id,
                    "qe_task_id": qe_task_id or None,
                    "qe_loop_id": qe_loop_id or None,
                    **source_lookup,
                },
            )
        execution_node_id = str(
            custom_params.get("execution_node_id")
            or custom_params.get("node_id")
            or result_metrics.get("execution_node_id")
            or execution_trace.get("node_id")
            or resolve_default_qe_node_id()
        ).strip()
        # allow_cache_fallback is intentionally False here. Live inference must
        # surface the node fetch failure rather than silently substitute a
        # cached params.pkl (feedback_no_silent_errors). Callers that need
        # cache fallback must construct the source through a dedicated path
        # that flips this flag and records origin='cache' downstream.
        asset_workspace, model_params_origin = self._materialize_runtime_source_from_node(
            experiment_id=experiment_id,
            qe_task_id=qe_task_id,
            qe_loop_id=qe_loop_id,
            execution_node_id=execution_node_id,
            factor_names=[str(item) for item in factor_names],
            custom_params=custom_params,
            data_split=data_split,
            allow_cache_fallback=False,
        )
        return QEExperimentRuntimeSource(
            experiment_id=experiment_id,
            db_workspace_path=Path(),
            asset_workspace_path=asset_workspace,
            factor_names=[str(item) for item in factor_names],
            custom_params=custom_params,
            data_split=data_split,
            model_params_origin=model_params_origin,
            qe_task_id=qe_task_id,
            qe_loop_id=qe_loop_id,
            execution_node_id=execution_node_id,
        )

    def prepare_workspace(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        source: QEExperimentRuntimeSource,
        runtime_config: dict[str, Any] | None = None,
        path_converter: Callable[[str], str] | None = None,
        cache_namespace: str | None = None,
        verify_model_code_contract: bool = True,
    ) -> PreparedInferenceWorkspace:
        config = runtime_config or {}
        artifact_config = config.get("selection_artifact_config") or config.get("selection_artifact") or {}
        if artifact_config and not isinstance(artifact_config, dict):
            raise RuntimeConfigInvalidError("selection_artifact_config must be an object")

        source_conf = self._resolve_conf_path(source)
        factor_source_dir = self._resolve_factor_source_dir(source)
        factor_order_resolution = self._build_factor_order(
            source=source,
            conf_path=source_conf,
        )
        factor_files = self._resolve_factor_files(
            factor_source_dir,
            factor_order_resolution.dynamic_factors,
        )
        model_source_path, model_candidate_count = self._resolve_model_params_path(source, artifact_config)

        cache_key = manifest_sha256[:16] if manifest_sha256 else "unfrozen_manifest"
        namespace = _safe_cache_component(cache_namespace or "")
        if namespace:
            cache_key = f"{cache_key}__{namespace}"
        workspace_path = self.cache_root / package_id / cache_key
        self._reset_cache_dir(workspace_path)
        (workspace_path / "model").mkdir(parents=True, exist_ok=True)

        model_dest = workspace_path / "model" / "params.pkl"
        shutil.copy2(model_source_path, model_dest)
        model_code_source = source.asset_workspace_path / "model.py"
        if model_code_source.exists() and model_code_source.is_file():
            shutil.copy2(model_code_source, workspace_path / "model" / "model.py")
        self._copy_model_code_siblings(
            model_source_path=model_source_path,
            model_dest_dir=workspace_path / "model",
        )
        referenced_model_modules = (
            _require_model_code_for_pickled_local_modules(
                model_params_path=model_dest,
                model_code_roots=[workspace_path / "model"],
                package_id=source.package_id or package_id,
                experiment_id=source.experiment_id,
                source_workspace_type=source.source_workspace_type,
                phase="runtime_asset_admission",
            )
            if verify_model_code_contract
            else []
        )
        dataset_processor_source = self._resolve_dataset_processor_path(
            source=source,
            model_source_path=model_source_path,
        )
        dataset_processor_relpath: str | None = None
        dataset_processor_dest: Path | None = None
        if dataset_processor_source is not None:
            dataset_processor_dest = workspace_path / "model" / "dataset"
            shutil.copy2(dataset_processor_source, dataset_processor_dest)
            dataset_processor_relpath = "model/dataset"

        factor_order_path = workspace_path / "factor_order.json"
        factor_order_path.write_text(
            json.dumps(
                {
                    "package_id": package_id,
                    "source_experiment_id": source.experiment_id,
                    "total_factors": len(factor_order_resolution.factor_order),
                    "alpha158_count": len(factor_order_resolution.alpha158_factors),
                    "dynamic_count": len(factor_order_resolution.dynamic_factors),
                    "factor_order": factor_order_resolution.factor_order,
                    "alpha158_factors": factor_order_resolution.alpha158_factors,
                    "dynamic_factors": factor_order_resolution.dynamic_factors,
                    "dynamic_factor_source": factor_order_resolution.dynamic_factor_source,
                    "qe_experiment_factor_name_count": len(source.factor_names),
                    "static_loader_schema_available": factor_order_resolution.static_loader_schema_available,
                    "static_loader_configs": factor_order_resolution.static_loader_configs,
                    "static_loader_missing_configs": factor_order_resolution.static_loader_missing_configs,
                    "static_loader_unreadable_configs": factor_order_resolution.static_loader_unreadable_configs,
                    "schema_alignment_basis": factor_order_resolution.dynamic_factor_source,
                    "warnings": factor_order_resolution.warnings,
                    "source": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                    "is_aligned": True,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        factor_entry_path = workspace_path / "strategy_package_factor_entry.py"
        factor_entry_path.write_text(
            self._build_factor_entry_source(
                factor_files=factor_files,
                path_converter=path_converter,
            ),
            encoding="utf-8",
        )

        manifest_path = workspace_path / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "task_id": package_id,
                    "source": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                    "primary_assets": {
                        "model_weight_relpath": "model/params.pkl",
                        "factor_entry_relpath": "strategy_package_factor_entry.py",
                        "dataset_processor_relpath": dataset_processor_relpath,
                    },
                    "assets": {
                        "model_weight": "model/params.pkl",
                        "factor_entry": "strategy_package_factor_entry.py",
                        "dataset_processor": dataset_processor_relpath,
                        "factor_order": "factor_order.json",
                        "factors_count": len(factor_order_resolution.factor_order),
                    },
                    "diagnostics": {
                        "qe_experiment_id": source.experiment_id,
                        "source_workspace_path": str(source.asset_workspace_path),
                        "source_workspace_type": source.source_workspace_type,
                        "package_id": source.package_id,
                        "package_manifest_sha256": source.manifest_sha256,
                        "qe_task_id": source.qe_task_id,
                        "qe_loop_id": source.qe_loop_id,
                        "execution_node_id": source.execution_node_id,
                        "factor_source_dir": str(factor_source_dir),
                        "model_source_path": str(model_source_path),
                        "model_candidate_count": model_candidate_count,
                        "model_params_origin": source.model_params_origin,
                        "referenced_model_modules": referenced_model_modules,
                        "dataset_processor_source_path": str(dataset_processor_source) if dataset_processor_source else None,
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        return PreparedInferenceWorkspace(
            workspace_path=workspace_path,
            manifest_path=manifest_path,
            factor_order_path=factor_order_path,
            factor_entry_path=factor_entry_path,
            model_params_path=model_dest,
            source_workspace_path=source.asset_workspace_path,
            factor_source_dir=factor_source_dir,
            factor_order=factor_order_resolution.factor_order,
            alpha158_factors=factor_order_resolution.alpha158_factors,
            dynamic_factors=factor_order_resolution.dynamic_factors,
            model_source_path=model_source_path,
            model_candidate_count=model_candidate_count,
            dataset_processor_path=dataset_processor_dest,
            model_params_origin=source.model_params_origin,
        )

    def _materialize_runtime_source_from_node(
        self,
        *,
        experiment_id: str,
        qe_task_id: str,
        qe_loop_id: str,
        execution_node_id: str,
        factor_names: list[str],
        custom_params: dict[str, Any],
        data_split: dict[str, Any],
        allow_cache_fallback: bool = False,
    ) -> tuple[Path, ModelParamsOrigin]:
        """Materialize a QE runtime source workspace from the node API.

        Returns the (source_dir, origin) tuple. ``origin`` is ``'node'`` when
        ``download_mlruns_params`` succeeded; ``'cache'`` only when both
        ``allow_cache_fallback=True`` AND the node fetch failed but a local
        cache hit replaced the params.

        Per feedback_no_silent_errors: a node fetch failure with
        ``allow_cache_fallback=False`` (the default) propagates the original
        exception. Callers that want to opt into cache fallback must pass
        ``allow_cache_fallback=True`` and record the resulting origin.
        """

        source_dir = (
            self.cache_root
            / "_qe_node_sources"
            / _safe_cache_component(experiment_id)
            / _safe_cache_component(execution_node_id)
            / _safe_cache_component(qe_task_id)
            / _safe_cache_component(qe_loop_id)
        )
        self._reset_cache_dir(source_dir)

        # Mutable origin holder threaded into the inner async closure so the
        # return value reflects the actual provenance of params.pkl.
        origin_holder: dict[str, ModelParamsOrigin] = {"origin": "node"}

        async def _download() -> Path:
            async with QEWorkspaceClient.for_node(execution_node_id) as client:
                await self._download_workspace_file(client, qe_task_id, qe_loop_id, "conf.yaml", source_dir / "conf.yaml")

                temp_source = QEExperimentRuntimeSource(
                    experiment_id=experiment_id,
                    db_workspace_path=Path(),
                    asset_workspace_path=source_dir,
                    factor_names=factor_names,
                    custom_params=custom_params,
                    data_split=data_split,
                    qe_task_id=qe_task_id,
                    qe_loop_id=qe_loop_id,
                    execution_node_id=execution_node_id,
                )
                conf_path = source_dir / "conf.yaml"
                static_paths = self._find_static_loader_configs(
                    _load_qe_conf_yaml(conf_path, purpose="node static asset discovery")
                )
                seen_static_relpaths: set[str] = set()
                for raw_path in static_paths:
                    if isinstance(raw_path, str) and raw_path.strip():
                        rel_path = _remote_relpath(raw_path)
                        if rel_path in seen_static_relpaths:
                            continue
                        seen_static_relpaths.add(rel_path)
                        try:
                            await self._download_workspace_file(
                                client,
                                qe_task_id,
                                qe_loop_id,
                                rel_path,
                                source_dir / rel_path,
                            )
                        except QEWorkspaceFileNotFound:
                            # Historical QE workspaces may keep conf.yaml and factors but not the schema parquet.
                            # Factor order can still be recovered from qe_experiments.factor_names below.
                            continue

                static_loader = self._extract_static_loader_feature_names(
                    source=temp_source,
                    conf_path=conf_path,
                )
                if static_loader.unreadable_configs:
                    raise DataUnavailableError(
                        "QE StaticDataLoader feature-order artifact is unreadable for live inference",
                        context={
                            "experiment_id": experiment_id,
                            "qe_task_id": qe_task_id,
                            "qe_loop_id": qe_loop_id,
                            "unreadable_configs": static_loader.unreadable_configs,
                        },
                    )
                required_factor_names = sorted(set(factor_names) | set(static_loader.factors))
                for factor_name in required_factor_names:
                    rel_path = _remote_relpath(f"factors/{factor_name}.py")
                    await self._download_workspace_file(
                        client,
                        qe_task_id,
                        qe_loop_id,
                        rel_path,
                        source_dir / rel_path,
                    )

                try:
                    await self._download_workspace_file(
                        client,
                        qe_task_id,
                        qe_loop_id,
                        "model.py",
                        source_dir / "model.py",
                    )
                except QEWorkspaceFileNotFound:
                    pass

                params_tar: bytes | None = None
                try:
                    params_tar = await client.download_mlruns_params(qe_task_id, qe_loop_id)
                except Exception as fetch_exc:
                    if not allow_cache_fallback:
                        # Per feedback_no_silent_errors: do NOT silently fall
                        # back to a locally cached params.pkl. Reproducibility
                        # requires the caller to opt in to cache fallback.
                        raise
                    cache_path = self._copy_cached_mlruns_params(
                        experiment_id=experiment_id,
                        source_dir=source_dir,
                    )
                    if cache_path is None:
                        raise
                    logger.warning(
                        "live_inference.mlruns_params_cache_fallback experiment_id=%s "
                        "qe_task_id=%s qe_loop_id=%s cache_path=%s reason=%s",
                        experiment_id,
                        qe_task_id,
                        qe_loop_id,
                        str(cache_path),
                        repr(fetch_exc),
                    )
                    origin_holder["origin"] = "cache"
                if params_tar:
                    self._extract_mlruns_params_archive(params_tar, source_dir)
                elif not list(source_dir.glob("**/artifacts/params.pkl")):
                    # Node returned an empty archive (or fetch was bypassed via
                    # cache fallback above without producing a params.pkl). Cache
                    # fallback here is only allowed when the caller opted in.
                    if not allow_cache_fallback:
                        raise DataUnavailableError(
                            "QE node API returned an empty mlruns params archive and cache fallback is disabled",
                            context={
                                "experiment_id": experiment_id,
                                "qe_task_id": qe_task_id,
                                "qe_loop_id": qe_loop_id,
                            },
                        )
                    cache_path = self._copy_cached_mlruns_params(
                        experiment_id=experiment_id,
                        source_dir=source_dir,
                    )
                    if cache_path is None:
                        raise DataUnavailableError(
                            "QE node API returned an empty mlruns params archive and no local StrategyPackage cache was available",
                            context={"experiment_id": experiment_id, "qe_task_id": qe_task_id, "qe_loop_id": qe_loop_id},
                        )
                    logger.warning(
                        "live_inference.mlruns_params_cache_fallback_empty_archive experiment_id=%s "
                        "qe_task_id=%s qe_loop_id=%s cache_path=%s",
                        experiment_id,
                        qe_task_id,
                        qe_loop_id,
                        str(cache_path),
                    )
                    origin_holder["origin"] = "cache"
            return source_dir

        try:
            resolved = _run_async_blocking(_download)
            return resolved, origin_holder["origin"]
        except TradingCoreError:
            raise
        except DataUnavailableError:
            raise
        except Exception as exc:
            raise DataUnavailableError(
                "failed to materialize QE runtime assets through the node API",
                context={
                    "experiment_id": experiment_id,
                    "qe_task_id": qe_task_id,
                    "qe_loop_id": qe_loop_id,
                    "execution_node_id": execution_node_id,
                    "cache_dir": str(source_dir),
                    "error": str(exc),
                },
            ) from exc

    def _reset_cache_dir(self, path: Path) -> None:
        cache_root = self.cache_root.resolve(strict=False)
        target = path.resolve(strict=False)
        if target == cache_root or cache_root not in target.parents:
            raise ArtifactGenerationFailedError(
                "refusing to reset a path outside the StrategyPackage runtime cache",
                context={"path": str(path), "cache_root": str(self.cache_root)},
            )
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    async def _download_workspace_file(
        self,
        client: QEWorkspaceClient,
        task_id: str,
        loop_id: str,
        rel_path: str,
        target_path: Path,
    ) -> None:
        rel_path = _remote_relpath(rel_path)
        target = target_path.resolve(strict=False)
        cache_root = self.cache_root.resolve(strict=False)
        if cache_root not in target.parents:
            raise ArtifactGenerationFailedError(
                "refusing to write QE runtime asset outside the StrategyPackage runtime cache",
                context={"target_path": str(target_path), "cache_root": str(self.cache_root)},
            )
        data = await client.download_workspace_file_bytes(task_id, loop_id, rel_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(data)

    def _extract_mlruns_params_archive(self, payload: bytes, dest_dir: Path) -> None:
        dest_root = dest_dir.resolve(strict=False)
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
            members = archive.getmembers()
            for member in members:
                if member.issym() or member.islnk():
                    raise ArtifactGenerationFailedError(
                        "QE mlruns params archive must not contain links",
                        context={"member": member.name},
                    )
                target = (dest_dir / member.name).resolve(strict=False)
                if target != dest_root and dest_root not in target.parents:
                    raise ArtifactGenerationFailedError(
                        "QE mlruns params archive contains an unsafe path",
                        context={"member": member.name},
                    )
            try:
                archive.extractall(dest_dir, filter="data")
            except TypeError:  # pragma: no cover - compatibility with older Python/tarfile.
                archive.extractall(dest_dir)
        if not any(dest_dir.glob("**/artifacts/params.pkl")):
            raise DataUnavailableError(
                "QE mlruns params archive does not contain artifacts/params.pkl",
                context={"dest_dir": str(dest_dir)},
            )

    def _copy_cached_mlruns_params(
        self, *, experiment_id: str, source_dir: Path
    ) -> Path | None:
        """Reuse an AIstock-local StrategyPackage model cache when the node lacks mlruns params.

        Returns the destination ``params.pkl`` Path on cache hit (so the caller
        can record the cache path used), or ``None`` when no cache candidate
        was found. The caller is responsible for deciding whether a cache hit
        is acceptable (origin='cache') or whether the absence of a cache hit
        should propagate the original node fetch error.
        """

        cache_root = self.cache_root.resolve(strict=False)
        candidates: list[Path] = []
        for manifest_path in self.cache_root.glob("*/*/manifest.json"):
            resolved_manifest = manifest_path.resolve(strict=False)
            if cache_root not in resolved_manifest.parents:
                continue
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            diagnostics = manifest.get("diagnostics") if isinstance(manifest, dict) else {}
            if not isinstance(diagnostics, dict) or str(diagnostics.get("qe_experiment_id") or "") != experiment_id:
                continue
            params_path = manifest_path.parent / "model" / "params.pkl"
            resolved_params = params_path.resolve(strict=False)
            if cache_root not in resolved_params.parents:
                continue
            if params_path.exists() and params_path.is_file():
                candidates.append(params_path)

        if not candidates:
            return None
        candidates.sort(key=lambda item: (item.stat().st_mtime, str(item).lower()), reverse=True)
        dest = source_dir / "mlruns" / "cached_strategy_package" / "artifacts" / "params.pkl"
        resolved_dest = dest.resolve(strict=False)
        resolved_source = source_dir.resolve(strict=False)
        if resolved_source not in resolved_dest.parents:
            raise ArtifactGenerationFailedError(
                "refusing to copy cached QE params outside the materialized source cache",
                context={"dest": str(dest), "source_dir": str(source_dir)},
            )
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(candidates[0], dest)
        return dest

    def _resolve_conf_path(self, source: QEExperimentRuntimeSource) -> Path:
        candidates = [source.asset_workspace_path / "conf.yaml"]
        for path in candidates:
            if path.exists() and path.is_file():
                return path
        raise DataUnavailableError(
            "QE conf.yaml is missing for live inference",
            context={"experiment_id": source.experiment_id, "checked_paths": [str(item) for item in candidates]},
        )

    def _build_factor_order(
        self,
        *,
        source: QEExperimentRuntimeSource,
        conf_path: Path,
    ) -> FactorOrderResolution:
        disable_alpha158 = bool(source.custom_params.get("disable_alpha158"))
        alpha158_factors = [] if disable_alpha158 else self._extract_alpha158_aliases(conf_path)
        static_loader = self._extract_static_loader_feature_names(
            source=source,
            conf_path=conf_path,
        )
        warnings: list[str] = []
        if static_loader.unreadable_configs:
            raise DataUnavailableError(
                "QE StaticDataLoader feature-order artifact is unreadable for live inference",
                context={
                    "experiment_id": source.experiment_id,
                    "conf_path": str(conf_path),
                    "unreadable_configs": static_loader.unreadable_configs,
                },
            )

        if static_loader.missing_configs:
            if not source.factor_names:
                raise DataUnavailableError(
                    "QE StaticDataLoader feature-order artifact is unavailable for live inference",
                    context={
                        "experiment_id": source.experiment_id,
                        "conf_path": str(conf_path),
                        "missing_configs": static_loader.missing_configs,
                    },
                )
            dynamic_factors = list(source.factor_names)
            dynamic_factor_source = "qe_experiments.factor_names_after_missing_static_loader"
            warnings.append(
                "StaticDataLoader schema artifact is missing; recovered dynamic factor order from qe_experiments.factor_names."
            )
        elif static_loader.factors:
            dynamic_factors = static_loader.factors
            dynamic_factor_source = "qe_static_dataloader"
        elif static_loader.configs:
            dynamic_factors = list(source.factor_names)
            dynamic_factor_source = "qe_experiments.factor_names_after_empty_static_loader"
            warnings.append(
                "StaticDataLoader schema artifact exposed no feature columns; recovered dynamic factor order from qe_experiments.factor_names."
            )
        else:
            dynamic_factors = list(source.factor_names)
            dynamic_factor_source = (
                "strategy_package_manifest.factor_set"
                if source.source_workspace_type == "strategy_package_asset_store"
                else "qe_experiments.factor_names"
            )
        factor_order = [*alpha158_factors, *dynamic_factors]
        if not factor_order:
            raise ArtifactGenerationFailedError(
                "live inference factor_order is empty",
                context={"experiment_id": source.experiment_id},
            )
        duplicates = sorted({item for item in factor_order if factor_order.count(item) > 1})
        if duplicates:
            raise ArtifactGenerationFailedError(
                "live inference factor_order contains duplicates",
                context={"experiment_id": source.experiment_id, "duplicates": duplicates},
            )
        return FactorOrderResolution(
            alpha158_factors=alpha158_factors,
            dynamic_factors=dynamic_factors,
            factor_order=factor_order,
            dynamic_factor_source=dynamic_factor_source,
            static_loader_schema_available=bool(static_loader.configs)
            and not static_loader.missing_configs
            and not static_loader.unreadable_configs
            and bool(static_loader.factors),
            static_loader_configs=static_loader.configs,
            static_loader_missing_configs=static_loader.missing_configs,
            static_loader_unreadable_configs=static_loader.unreadable_configs,
            warnings=warnings,
        )

    def _extract_static_loader_feature_names(
        self,
        *,
        source: QEExperimentRuntimeSource,
        conf_path: Path,
    ) -> StaticLoaderFeatureResolution:
        conf = _load_qe_conf_yaml(conf_path, purpose="static factor order")

        configs = self._find_static_loader_configs(conf)
        if not configs:
            return StaticLoaderFeatureResolution(
                factors=[],
                configs=[],
                missing_configs=[],
                unreadable_configs=[],
            )

        factors: list[str] = []
        config_paths: list[str] = []
        seen_config_paths: set[str] = set()
        missing_paths: list[str] = []
        unreadable_paths: list[dict[str, str]] = []
        for config in configs:
            if not isinstance(config, str) or not config.strip():
                continue
            config_text = config.strip()
            if config_text in seen_config_paths:
                continue
            seen_config_paths.add(config_text)
            config_paths.append(config_text)
            path = Path(config_text)
            candidates = [path] if path.is_absolute() else [
                source.asset_workspace_path / path,
                conf_path.parent / path,
            ]
            for candidate in candidates:
                ensure_not_forbidden_worker_workspace_path(
                    candidate,
                    purpose="live inference StaticDataLoader config",
                )
            resolved = next((candidate for candidate in candidates if candidate.exists() and candidate.is_file()), None)
            if resolved is None:
                missing_paths.append(config_text)
                continue
            try:
                factors.extend(self._read_static_feature_columns(resolved))
            except Exception as exc:
                unreadable_paths.append({"path": str(resolved), "error": str(exc)})

        if not factors:
            return StaticLoaderFeatureResolution(
                factors=[],
                configs=config_paths,
                missing_configs=missing_paths,
                unreadable_configs=unreadable_paths,
            )
        unique: list[str] = []
        seen: set[str] = set()
        for factor in factors:
            if factor not in seen:
                unique.append(factor)
                seen.add(factor)
        return StaticLoaderFeatureResolution(
            factors=unique,
            configs=config_paths,
            missing_configs=missing_paths,
            unreadable_configs=unreadable_paths,
        )

    def _find_static_loader_configs(self, node: Any) -> list[Any]:
        configs: list[Any] = []
        if isinstance(node, dict):
            if node.get("class") == "qlib.data.dataset.loader.StaticDataLoader":
                configs.append((node.get("kwargs") or {}).get("config"))
            for value in node.values():
                configs.extend(self._find_static_loader_configs(value))
        elif isinstance(node, list):
            for value in node:
                configs.extend(self._find_static_loader_configs(value))
        return configs

    def _read_static_feature_columns(self, path: Path) -> list[str]:
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            try:
                import pyarrow.parquet as pq  # type: ignore

                names = list(pq.ParquetFile(path).schema_arrow.names)
            except Exception as exc:
                raise ArtifactGenerationFailedError(
                    "failed to read parquet feature schema for live inference",
                    context={"path": str(path), "error": str(exc)},
                ) from exc
            factors: list[str] = []
            for name in names:
                parsed: Any = name
                if isinstance(name, str) and name.startswith("("):
                    try:
                        parsed = ast.literal_eval(name)
                    except (SyntaxError, ValueError):
                        parsed = name
                if isinstance(parsed, tuple) and len(parsed) >= 2 and parsed[0] == "feature":
                    factors.append(str(parsed[1]))
                elif isinstance(parsed, str) and parsed not in {"datetime", "instrument"}:
                    factors.append(parsed)
            return factors

        raise ArtifactGenerationFailedError(
            "unsupported StaticDataLoader feature-order artifact format for live inference",
            context={"path": str(path), "suffix": suffix},
        )

    def _copy_model_code_siblings(self, *, model_source_path: Path, model_dest_dir: Path) -> None:
        source_dir = model_source_path.parent.resolve(strict=False)
        dest_root = model_dest_dir.resolve(strict=False)
        if not source_dir.exists() or not source_dir.is_dir():
            return
        for source_path in sorted(source_dir.rglob("*.py")):
            resolved_source = source_path.resolve(strict=False)
            if source_dir not in resolved_source.parents:
                raise ArtifactGenerationFailedError(
                    "refusing to copy model code outside the materialized model source directory",
                    context={"source_path": str(source_path), "model_source_dir": str(source_dir)},
                )
            rel_path = source_path.relative_to(source_dir)
            if any(part in {"", ".", ".."} for part in rel_path.parts):
                raise ArtifactGenerationFailedError(
                    "model code relative path is unsafe for live inference workspace",
                    context={"source_path": str(source_path), "relative_path": str(rel_path)},
                )
            dest_path = model_dest_dir / rel_path
            resolved_dest = dest_path.resolve(strict=False)
            if resolved_dest != dest_root and dest_root not in resolved_dest.parents:
                raise ArtifactGenerationFailedError(
                    "refusing to copy model code outside the inference workspace",
                    context={
                        "source_path": str(source_path),
                        "dest_path": str(dest_path),
                        "model_dest_dir": str(model_dest_dir),
                    },
                )
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, dest_path)

    def _extract_alpha158_aliases(self, conf_path: Path) -> list[str]:
        conf = _load_qe_conf_yaml(conf_path, purpose="alpha158 factors")

        aliases = self._find_alpha158_aliases(conf)
        if not aliases:
            raise DataUnavailableError(
                "QE conf.yaml does not expose Alpha158 aliases required by live inference",
                context={"conf_path": str(conf_path)},
            )
        return aliases

    def _find_alpha158_aliases(self, node: Any) -> list[str]:
        return extract_alpha158_aliases(node)

    def _resolve_factor_source_dir(self, source: QEExperimentRuntimeSource) -> Path:
        candidates = [source.asset_workspace_path / "factors"]
        for candidate in candidates:
            if candidate.exists() and candidate.is_dir():
                return candidate
        raise DataUnavailableError(
            "QE factor source directory is missing for live inference",
            context={"experiment_id": source.experiment_id, "checked_paths": [str(item) for item in candidates]},
        )

    def _resolve_factor_files(self, factor_source_dir: Path, factor_names: list[str]) -> dict[str, Path]:
        files: dict[str, Path] = {}
        missing: list[str] = []
        for factor_name in factor_names:
            path = factor_source_dir / f"{factor_name}.py"
            if not path.exists() or not path.is_file():
                missing.append(factor_name)
            else:
                files[factor_name] = path
        if missing:
            raise DataUnavailableError(
                "QE factor source files are missing for live inference",
                context={
                    "factor_source_dir": str(factor_source_dir),
                    "missing_factors": missing,
                    "missing_count": len(missing),
                },
            )
        return files

    def _resolve_model_params_path(
        self,
        source: QEExperimentRuntimeSource,
        artifact_config: dict[str, Any],
    ) -> tuple[Path, int]:
        explicit = artifact_config.get("model_params_path")
        if explicit:
            if source.source_workspace_type == "strategy_package_asset_store":
                raise RuntimeConfigInvalidError(
                    "frozen StrategyPackage runtime must use package-owned model assets",
                    context={
                        "reason_code": "strategy_package_runtime_model_override_forbidden",
                        "package_id": source.package_id,
                        "model_params_path": str(explicit),
                    },
                )
            path = Path(str(explicit))
            ensure_not_forbidden_worker_workspace_path(path, purpose="live inference explicit model_params_path")
            if not path.exists() or not path.is_file():
                raise DataUnavailableError(
                    "explicit model_params_path does not exist for live inference",
                    context={"model_params_path": str(path), "experiment_id": source.experiment_id},
                )
            return path, 1

        candidates: list[Path] = []
        root = source.asset_workspace_path
        if root.exists():
            candidates.extend(root.glob("**/artifacts/params.pkl"))
        unique: dict[str, Path] = {}
        for candidate in candidates:
            if candidate.exists() and candidate.is_file():
                unique[str(candidate).lower()] = candidate
        candidates = list(unique.values())
        if not candidates:
            raise DataUnavailableError(
                "QE model params.pkl is missing for live inference",
                context={
                    "experiment_id": source.experiment_id,
                    "asset_workspace_path": str(source.asset_workspace_path),
                },
            )
        candidates.sort(key=lambda item: (item.stat().st_mtime, str(item).lower()), reverse=True)
        return candidates[0], len(candidates)

    def _resolve_dataset_processor_path(
        self,
        *,
        source: QEExperimentRuntimeSource,
        model_source_path: Path,
    ) -> Path | None:
        """Locate the fitted Qlib dataset artifact that owns infer processors."""
        sibling = model_source_path.parent / "dataset"
        if sibling.exists() and sibling.is_file():
            return sibling
        root = source.asset_workspace_path
        if not root.exists():
            return None
        candidates = [path for path in root.glob("**/artifacts/dataset") if path.exists() and path.is_file()]
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item.stat().st_mtime, str(item).lower()), reverse=True)
        return candidates[0]

    def _build_factor_entry_source(
        self,
        *,
        factor_files: dict[str, Path],
        path_converter: Callable[[str], str] | None,
    ) -> str:
        entries = {}
        for factor_name, path in factor_files.items():
            resolved = str(path.resolve(strict=False))
            entries[factor_name] = path_converter(resolved) if path_converter else resolved
        lines = [
            "from __future__ import annotations",
            "",
            "import os",
            "import runpy",
            "import tempfile",
            "from pathlib import Path",
            "",
            "import pandas as pd",
            "",
            f"_FACTOR_FILES = {json.dumps(entries, ensure_ascii=False, indent=2)}",
            "_DATA_FILES = (",
            "    'daily_pv.h5', 'static_factors.parquet', 'daily_basic.h5',",
            "    'moneyflow.h5', 'sector_data.h5', 'bak_basic.h5',",
            "    'cyq_perf.h5', 'margin_detail.h5',",
            ")",
            "",
            "def _link_data_file(src: Path, dst: Path) -> None:",
            "    if dst.exists() or dst.is_symlink():",
            "        dst.unlink()",
            "    try:",
            "        os.link(src, dst)",
            "        return",
            "    except OSError:",
            "        pass",
            "    try:",
            "        os.symlink(src, dst)",
            "        return",
            "    except OSError:",
            "        raise RuntimeError(f'failed to link inference data file {src} -> {dst}')",
            "",
            "def _ensure_h5_aliases(base_dir: Path) -> None:",
            "    pv_path = base_dir / 'daily_pv.h5'",
            "    clean_pv_path = base_dir / 'daily_pv_clean.h5'",
            "    if pv_path.exists() and not clean_pv_path.exists():",
            "        pv = pd.read_hdf(pv_path)",
            "        dollar_cols = [col for col in pv.columns if str(col).startswith('$')]",
            "        if dollar_cols:",
            "            pv = pv[[col for col in pv.columns if col not in dollar_cols]]",
            "            pv.to_hdf(clean_pv_path, key='data', mode='w')",
            "    static_path = base_dir / 'static_factors.parquet'",
            "    if not static_path.exists():",
            "        return",
            "    aliases = [",
            "        'daily_basic.h5', 'moneyflow.h5', 'sector_data.h5',",
            "        'bak_basic.h5', 'cyq_perf.h5', 'margin_detail.h5',",
            "    ]",
            "    if all((base_dir / name).exists() for name in aliases):",
            "        return",
            "    df = pd.read_parquet(static_path)",
            "    for name in aliases:",
            "        out = base_dir / name",
            "        if not out.exists():",
            "            df.to_hdf(out, key='data', mode='w')",
            "",
            "def _run_factor(factor_name: str):",
            "    outer_dir = Path.cwd()",
            "    _ensure_h5_aliases(outer_dir)",
            "    source = Path(_FACTOR_FILES[factor_name])",
            "    if not source.exists():",
            "        raise FileNotFoundError(f'factor source missing: {source}')",
            "    with tempfile.TemporaryDirectory(prefix=f'sp_factor_{factor_name}_') as tmp:",
            "        work_root = Path(tmp)",
            "        factor_dir = work_root / f'_factor_{factor_name}'",
            "        factor_dir.mkdir(parents=True, exist_ok=True)",
            "        factor_py = factor_dir / 'factor.py'",
            "        factor_py.write_text(source.read_text(encoding='utf-8'), encoding='utf-8')",
            "        for filename in _DATA_FILES:",
            "            src = outer_dir / filename",
            "            if filename == 'daily_pv.h5' and (outer_dir / 'daily_pv_clean.h5').exists():",
            "                src = outer_dir / 'daily_pv_clean.h5'",
            "            if not src.exists():",
            "                continue",
            "            _link_data_file(src, work_root / filename)",
            "            _link_data_file(src, factor_dir / filename)",
            "        old_cwd = os.getcwd()",
            "        try:",
            "            os.chdir(factor_dir)",
            "            runpy.run_path(str(factor_py), run_name='__main__')",
            "            result_path = factor_dir / 'result.h5'",
            "            if not result_path.exists():",
            "                raise FileNotFoundError(f'factor result.h5 missing for {factor_name}')",
            "            result = pd.read_hdf(result_path)",
            "        finally:",
            "            os.chdir(old_cwd)",
            "    if isinstance(result, pd.Series):",
            "        result = result.to_frame(name=factor_name)",
            "    if not isinstance(result, pd.DataFrame):",
            "        raise TypeError(f'factor {factor_name} returned {type(result).__name__}, expected DataFrame')",
            "    if len(result.columns) == 1 and factor_name not in result.columns:",
            "        result = result.rename(columns={result.columns[0]: factor_name})",
            "    if result.empty:",
            "        raise ValueError(f'factor {factor_name} returned empty result')",
            "    return result",
            "",
        ]
        for idx, factor_name in enumerate(factor_files.keys(), start=1):
            lines.extend(
                [
                    f"def calculate_{idx:03d}_{_safe_name(factor_name)}():",
                    f"    return _run_factor({factor_name!r})",
                    "",
                ]
            )
        return "\n".join(lines)


class LocalStrategyPackageInferenceProvider:
    """Run the live inference engine in the current Python process."""

    def run(
        self,
        *,
        workspace: PreparedInferenceWorkspace,
        trade_date: date,
        cutoff_date: date | None = None,
    ) -> LiveInferenceResult:
        try:
            from backend.inference_engine import InferenceEngine
        except Exception as exc:
            raise DataUnavailableError(
                "local live inference engine is unavailable",
                context={"error": str(exc)},
            ) from exc
        old_strict = os.environ.get("AISTOCK_STRICT_INFERENCE")
        os.environ["AISTOCK_STRICT_INFERENCE"] = "1"
        try:
            engine = InferenceEngine()
            df_scores = engine.run_inference(
                strategy_id="",
                version_tag="strategy_package_live",
                trade_date=_date_to_datetime(trade_date),
                cutoff_date=_date_to_datetime(cutoff_date) if cutoff_date else None,
                experiment_id="strategy_package_live",
                workspace_path=str(workspace.workspace_path),
            )
        except Exception as exc:
            raise DataUnavailableError(
                "local live QE model inference failed",
                context={"workspace_path": str(workspace.workspace_path), "error": str(exc)},
            ) from exc
        finally:
            if old_strict is None:
                os.environ.pop("AISTOCK_STRICT_INFERENCE", None)
            else:
                os.environ["AISTOCK_STRICT_INFERENCE"] = old_strict
        receipt = engine.last_inference_receipt
        if not isinstance(receipt, dict):
            raise DataUnavailableError(
                "local live inference did not return an execution receipt",
                context={"reason_code": "ADVISORY_PHASE0A2C_SOURCE_RECEIPT_INCOMPLETE"},
            )
        return LiveInferenceResult(
            scores=_score_rows_from_frame(df_scores, cutoff_date or trade_date),
            metadata={"inference_backend": "local"},
            universe_count=_required_receipt_universe_count(receipt),
            source_read_receipts=_required_receipt_rows(receipt, "source_read_receipts"),
            input_context=_required_receipt_object(receipt, "input_context"),
        )


_MALFORMED_TS_CODE_WITH_DATE_RE = re.compile(
    r"\b\d{6}\.[A-Z]{1,4}\d{4}-\d{2}-\d{2}T[^\s,;\"'\]\)}]+"
)


def _extract_malformed_ts_code_samples(text: str, *, limit: int = 10) -> list[str]:
    if not text:
        return []
    samples = []
    seen = set()
    for match in _MALFORMED_TS_CODE_WITH_DATE_RE.finditer(text):
        value = match.group(0)
        if value not in seen:
            samples.append(value)
            seen.add(value)
        if len(samples) >= limit:
            break
    return samples


class WslStrategyPackageInferenceProvider:
    """Run live inference inside the WSL Qlib environment."""

    def __init__(
        self,
        *,
        distro: str | None = None,
        conda_sh: str | None = None,
        conda_env: str | None = None,
        repo_root: Path | str | None = None,
        timeout_seconds: int = 3600,
    ) -> None:
        self.distro = distro or os.getenv("QLIB_WSL_DISTRO") or "Ubuntu"
        self.conda_sh = conda_sh or os.getenv("QLIB_WSL_CONDA_SH") or "~/miniconda3/etc/profile.d/conda.sh"
        self.conda_env = conda_env or os.getenv("QLIB_WSL_CONDA_ENV") or "rdagent-gpu"
        self.repo_root = Path(repo_root or Path.cwd()).resolve()
        self.timeout_seconds = timeout_seconds

    def run(
        self,
        *,
        workspace: PreparedInferenceWorkspace,
        trade_date: date,
        cutoff_date: date | None = None,
    ) -> LiveInferenceResult:
        with tempfile.TemporaryDirectory(prefix="sp_live_inference_") as tmp:
            output_path = Path(tmp) / "scores.json"
            args = [
                "scripts/strategy_package_live_inference.py",
                "--runtime-workspace",
                win_to_wsl_path(str(workspace.workspace_path)),
                "--trade-date",
                trade_date.isoformat(),
                "--output-path",
                win_to_wsl_path(str(output_path)),
            ]
            if cutoff_date:
                args.extend(["--cutoff-date", cutoff_date.isoformat()])
            env_exports = self._build_env_exports()
            command = (
                f"source {self.conda_sh} && "
                f"conda activate {self.conda_env} && "
                f"cd {win_to_wsl_path(str(self.repo_root))} && "
                f"{env_exports} "
                + "python "
                + " ".join(self._quote(arg) for arg in args)
            )
            completed = subprocess.run(
                ["wsl", "-d", self.distro, "bash", "-lc", command],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.timeout_seconds,
                check=False,
            )
            if completed.returncode != 0:
                combined_output = completed.stdout + "\n" + completed.stderr
                raise DataUnavailableError(
                    "WSL live QE model inference failed",
                    context={
                        "returncode": completed.returncode,
                        "stdout_tail": completed.stdout[-4000:],
                        "stderr_tail": completed.stderr[-4000:],
                        "workspace_path": str(workspace.workspace_path),
                        "trade_date": trade_date.isoformat(),
                        "cutoff_date": cutoff_date.isoformat() if cutoff_date else None,
                        "runner_args": args,
                        "malformed_ts_code_samples": _extract_malformed_ts_code_samples(combined_output),
                    },
                )
            if not output_path.exists():
                combined_output = completed.stdout + "\n" + completed.stderr
                raise DataUnavailableError(
                    "WSL live QE model inference did not write output JSON",
                    context={
                        "stdout_tail": completed.stdout[-4000:],
                        "stderr_tail": completed.stderr[-4000:],
                        "output_path": str(output_path),
                        "trade_date": trade_date.isoformat(),
                        "cutoff_date": cutoff_date.isoformat() if cutoff_date else None,
                        "runner_args": args,
                        "malformed_ts_code_samples": _extract_malformed_ts_code_samples(combined_output),
                    },
                )
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        scores = payload.get("scores")
        if not isinstance(scores, list) or not scores:
            raise DataUnavailableError(
                "WSL live QE model inference output contains no scores",
                context={"payload_keys": sorted(payload.keys())},
            )
        metadata = dict(payload.get("metadata") or {})
        metadata.update({"inference_backend": "wsl", "wsl_distro": self.distro, "wsl_conda_env": self.conda_env})
        return LiveInferenceResult(
            scores=scores,
            metadata=metadata,
            universe_count=_required_receipt_universe_count(payload),
            source_read_receipts=_required_receipt_rows(payload, "source_read_receipts"),
            input_context=_required_receipt_object(payload, "input_context"),
        )

    def _build_env_exports(self) -> str:
        keys = [
            "TDX_DB_HOST",
            "TDX_DB_PORT",
            "TDX_DB_NAME",
            "TDX_DB_USER",
            "TDX_DB_PASSWORD",
            "AISTOCK_PG_STATEMENT_TIMEOUT_MS",
        ]
        exports = ["PYTHONIOENCODING=utf-8", "PYTHONDONTWRITEBYTECODE=1", "AISTOCK_STRICT_INFERENCE=1"]
        for key in keys:
            value = os.getenv(key)
            if value is not None:
                exports.append(f"{key}={self._quote(value)}")
        return " ".join(exports)

    @staticmethod
    def _quote(value: str) -> str:
        return "'" + str(value).replace("'", "'\"'\"'") + "'"


def _required_receipt_universe_count(payload: dict[str, Any]) -> int:
    value = payload.get("universe_count")
    if isinstance(value, bool):
        value = None
    try:
        count = int(value)
    except (TypeError, ValueError) as exc:
        raise DataUnavailableError(
            "live inference receipt is missing an actual universe_count",
            context={"reason_code": "ADVISORY_PHASE0A2C_UNIVERSE_RECEIPT_INCOMPLETE"},
        ) from exc
    if count < 0:
        raise DataUnavailableError(
            "live inference receipt has an invalid universe_count",
            context={"reason_code": "ADVISORY_PHASE0A2C_UNIVERSE_RECEIPT_INCOMPLETE", "universe_count": count},
        )
    return count


def _required_receipt_rows(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise DataUnavailableError(
            "live inference receipt is missing source-read rows",
            context={"reason_code": "ADVISORY_PHASE0A2C_SOURCE_RECEIPT_INCOMPLETE", "field": key},
        )
    return [dict(item) for item in value]


def _required_receipt_object(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise DataUnavailableError(
            "live inference receipt is missing input context",
            context={"reason_code": "ADVISORY_PHASE0A2C_SOURCE_RECEIPT_INCOMPLETE", "field": key},
        )
    return dict(value)
