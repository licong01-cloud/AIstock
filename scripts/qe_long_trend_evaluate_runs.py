#!/usr/bin/env python3
"""Evaluate archived QE recorder runs with the F-014 long-trend engine.

This command is intentionally QE-only. It reads immutable QE workspaces and the
versioned QE factor-data snapshot, then writes deterministic research artifacts
under ``rdagent_assets/long_trend_evaluations``. It does not connect to the
database, Selection, Advisory, Paper, simulated trading, QMT, or live services.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from backend.services.quantevolver.long_trend_data_reader import (
    QELongTrendDatasetReader,
    inspect_qe_snapshot_identity,
    verify_outcome_snapshot_extension,
)
from backend.services.quantevolver.long_trend_evaluation import (
    ExecutionEvidenceBundle,
    LongTrendEvaluationResult,
    QELongTrendEvaluationEngine,
)
from backend.services.quantevolver.long_trend_evaluation_contract import (
    QELongTrendEvaluationContext,
    QE_LONG_TREND_PROFILE_V1,
    canonical_sha256,
)
from backend.services.quantevolver.qe_dataset_contract import QE_DATASET_CONTRACT_ID


_RECORDER_FILE = "qe_current_recorder.json"
_CONFIG_FILE = "config.json"
_RUN_ID_RE = re.compile(r"^qe_\d{8}_\d{6}_[0-9a-f]+_L\d+$")
_DERIVATION_VERSION = "qlib_indicator_and_position_resolver_v1"


@dataclass(frozen=True)
class RecorderArtifacts:
    workspace: Path
    run_id: str
    label_horizon: int
    strategy_topk: int
    test_start: str
    recorder_manifest: Path
    config_path: Path
    artifact_root: Path
    prediction_path: Path
    label_path: Path | None
    position_path: Path | None
    portfolio_report_path: Path | None
    indicator_object_path: Path | None
    input_hashes: Mapping[str, str | None]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid JSON artifact {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON artifact must contain an object: {path}")
    return value


def _optional_file(path: Path) -> Path | None:
    return path if path.is_file() else None


def _nested(config: Mapping[str, Any], *path: str) -> Any:
    current: Any = config
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _first_present(config: Mapping[str, Any], paths: Iterable[tuple[str, ...]]) -> Any:
    for path in paths:
        value = _nested(config, *path)
        if value not in (None, ""):
            return value
    return None


def _require_positive_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"{field} must be a positive integer, got {value!r}")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{field} must be a positive integer, got {value!r}") from exc
    if parsed <= 0 or (isinstance(value, float) and not value.is_integer()):
        raise RuntimeError(f"{field} must be a positive integer, got {value!r}")
    return parsed


def resolve_recorder_artifacts(workspace_value: str | os.PathLike[str]) -> RecorderArtifacts:
    workspace = Path(workspace_value).expanduser().resolve()
    if not workspace.is_dir():
        raise RuntimeError(f"QE workspace does not exist: {workspace}")

    recorder_path = workspace / _RECORDER_FILE
    config_path = workspace / _CONFIG_FILE
    if not recorder_path.is_file() or not config_path.is_file():
        raise RuntimeError(
            f"QE workspace requires {_RECORDER_FILE} and {_CONFIG_FILE}: {workspace}"
        )
    recorder = _read_json_object(recorder_path)
    config = _read_json_object(config_path)
    if recorder.get("schema_version") != 1:
        raise RuntimeError(
            f"unsupported recorder schema_version={recorder.get('schema_version')!r}: {recorder_path}"
        )

    recorded_cwd = Path(str(recorder.get("cwd") or "")).expanduser().resolve()
    if recorded_cwd != workspace:
        raise RuntimeError(
            f"recorder cwd does not match workspace: recorded={recorded_cwd}, actual={workspace}"
        )
    mlruns_root = (workspace / "mlruns").resolve()
    recorded_mlruns = Path(str(recorder.get("target_mlruns_realpath") or "")).expanduser().resolve()
    if recorded_mlruns != mlruns_root:
        raise RuntimeError(
            f"recorder mlruns root does not match workspace: recorded={recorded_mlruns}, actual={mlruns_root}"
        )

    experiment_id = str(recorder.get("experiment_id") or "").strip()
    recorder_id = str(recorder.get("recorder_id") or "").strip()
    if not experiment_id.isdigit() or not re.fullmatch(r"[0-9a-f]{16,64}", recorder_id):
        raise RuntimeError(f"invalid recorder identity in {recorder_path}")
    artifact_root = (mlruns_root / experiment_id / recorder_id / "artifacts").resolve()
    if mlruns_root not in artifact_root.parents or not artifact_root.is_dir():
        raise RuntimeError(f"recorder artifact root is missing or escaped mlruns: {artifact_root}")

    prediction_path = artifact_root / "pred.pkl"
    if not prediction_path.is_file():
        raise RuntimeError(f"required prediction artifact is missing: {prediction_path}")
    label_path = _optional_file(artifact_root / "label.pkl")
    position_path = _optional_file(artifact_root / "portfolio_analysis" / "positions_normal_1day.pkl")
    portfolio_report_path = _optional_file(
        artifact_root / "portfolio_analysis" / "report_normal_1day.pkl"
    )
    indicator_object_path = _optional_file(
        artifact_root / "portfolio_analysis" / "indicators_normal_1day_obj.pkl"
    )

    task_id = str(
        config.get("task_id")
        or _nested(config, "requested", "task_id")
        or _nested(config, "execution_manifest", "requested", "task_id")
        or ""
    ).strip()
    loop_index = _first_present(
        config,
        (
            ("loop_index",),
            ("requested", "loop_index"),
            ("execution_manifest", "requested", "loop_index"),
            ("runtime_flags", "loop_index"),
        ),
    )
    loop_index = _require_positive_int(loop_index, field="loop_index")
    run_id = f"{task_id}_L{loop_index}"
    if not _RUN_ID_RE.fullmatch(run_id):
        raise RuntimeError(f"derived QE run_id is invalid: {run_id!r}")

    label_horizon = _require_positive_int(
        _first_present(
            config,
            (
                ("label_horizon",),
                ("model_params", "label_horizon"),
                ("requested", "label_horizon"),
                ("requested", "custom_params", "label_horizon"),
                ("execution_manifest", "requested", "label_horizon"),
                ("execution_manifest", "requested", "custom_params", "label_horizon"),
                ("runtime_flags", "label_horizon"),
            ),
        ),
        field="label_horizon",
    )
    strategy_topk = _require_positive_int(
        _first_present(
            config,
            (
                ("execution_manifest", "artifact", "strategy", "audit_subset", "topk"),
                ("execution_manifest", "artifact", "strategy", "kwargs", "topk"),
                ("strategy_audit_subset", "topk"),
                ("model_params", "topk"),
                ("requested", "strategy_params", "topk"),
                ("requested", "custom_params", "topk"),
                ("execution_manifest", "requested", "strategy_audit_subset", "topk"),
                ("execution_manifest", "requested", "strategy_params", "topk"),
                ("execution_manifest", "requested", "custom_params", "topk"),
                ("runtime_flags", "strategy_topk"),
            ),
        ),
        field="strategy_topk",
    )
    test_start_value = _first_present(
        config,
        (
            ("data_split", "test_start"),
            ("requested", "data_split", "test_start"),
            ("execution_manifest", "requested", "data_split", "test_start"),
        ),
    )
    try:
        test_start = pd.Timestamp(test_start_value).date().isoformat()
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"invalid test_start={test_start_value!r} in {config_path}") from exc

    paths = {
        "prediction_sha256": prediction_path,
        "label_sha256": label_path,
        "position_sha256": position_path,
        "portfolio_report_sha256": portfolio_report_path,
        "indicator_object_sha256": indicator_object_path,
    }
    input_hashes: dict[str, str | None] = {
        key: _sha256_file(path) if path is not None else None for key, path in paths.items()
    }
    input_hashes.update(
        {
            "trade_sha256": None,
            "order_sha256": None,
            "recorder_manifest_sha256": _sha256_file(recorder_path),
            "config_sha256": _sha256_file(config_path),
            "execution_derivation_version": _DERIVATION_VERSION,
        }
    )
    return RecorderArtifacts(
        workspace=workspace,
        run_id=run_id,
        label_horizon=label_horizon,
        strategy_topk=strategy_topk,
        test_start=test_start,
        recorder_manifest=recorder_path,
        config_path=config_path,
        artifact_root=artifact_root,
        prediction_path=prediction_path,
        label_path=label_path,
        position_path=position_path,
        portfolio_report_path=portfolio_report_path,
        indicator_object_path=indicator_object_path,
        input_hashes=input_hashes,
    )


def normalize_qlib_position_artifact(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy(deep=False)
    if not isinstance(value, Mapping) or not value:
        raise RuntimeError("Qlib position artifact must be a non-empty mapping or DataFrame")

    rows: dict[pd.Timestamp, dict[str, float]] = {}
    for raw_date, snapshot in value.items():
        try:
            date = pd.Timestamp(raw_date).normalize()
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"invalid Qlib position snapshot date {raw_date!r}") from exc
        if isinstance(snapshot, Mapping):
            position = snapshot.get("position")
        else:
            position = getattr(snapshot, "position", None)
        if not isinstance(position, Mapping):
            raise RuntimeError(f"position snapshot {date.date()} lacks position mapping")
        holdings: dict[str, float] = {}
        for instrument, payload in position.items():
            if instrument in {"cash", "now_account_value"}:
                continue
            if not isinstance(payload, Mapping) or "amount" not in payload:
                raise RuntimeError(
                    f"position payload for {instrument!r} on {date.date()} lacks amount"
                )
            amount = float(payload["amount"])
            if not np.isfinite(amount) or amount < 0:
                raise RuntimeError(
                    f"invalid position amount for {instrument!r} on {date.date()}: {amount!r}"
                )
            holdings[str(instrument)] = amount
        rows[date] = holdings

    snapshot_dates = pd.DatetimeIndex(sorted(rows))
    wide = (
        pd.DataFrame.from_dict(rows, orient="index")
        .reindex(snapshot_dates)
        .sort_index()
        .fillna(0.0)
    )
    if wide.empty or len(wide.columns) == 0:
        return pd.DataFrame(columns=["datetime", "instrument", "amount"])
    wide.index.name = "datetime"
    long = wide.stack(future_stack=True).rename("amount").reset_index()
    long.columns = ["datetime", "instrument", "amount"]
    return long


def _single_data_series(value: Any, *, field: str) -> pd.Series:
    if isinstance(value, pd.Series):
        return value.copy(deep=False)
    data = getattr(value, "data", None)
    indices = getattr(value, "indices", None)
    index_obj = indices[0] if isinstance(indices, list) and len(indices) == 1 else getattr(value, "index", None)
    index_values = getattr(index_obj, "idx_list", None)
    if data is None or index_values is None:
        raise RuntimeError(f"unsupported Qlib indicator field representation for {field!r}")
    array = np.asarray(data)
    index_array = np.asarray(index_values)
    if array.ndim != 1 or len(array) != len(index_array):
        raise RuntimeError(
            f"Qlib indicator field {field!r} has incompatible data/index lengths"
        )
    return pd.Series(array, index=index_array, name=field)


def normalize_qlib_indicator_object(value: Any) -> tuple[pd.DataFrame, pd.DataFrame]:
    history = getattr(value, "order_indicator_his", None)
    if not isinstance(history, Mapping):
        raise RuntimeError("Qlib indicator object lacks order_indicator_his mapping")
    frames: list[pd.DataFrame] = []
    for raw_date, raw_indicator in history.items():
        data = getattr(raw_indicator, "data", raw_indicator)
        if not isinstance(data, Mapping):
            raise RuntimeError(f"unsupported Qlib order indicator at {raw_date!r}")
        series = {
            str(field): _single_data_series(field_value, field=str(field))
            for field, field_value in data.items()
        }
        if not series:
            continue
        frame = pd.concat(series, axis=1, sort=True)
        frame.index.name = "instrument"
        frame = frame.reset_index()
        frame.insert(0, "datetime", pd.Timestamp(raw_date).normalize())
        frames.append(frame)
    if not frames:
        empty = pd.DataFrame(
            columns=["datetime", "instrument", "amount", "deal_amount", "ffr", "side"]
        )
        return empty, empty.copy()

    indicator = pd.concat(frames, ignore_index=True, sort=False)
    if "trade_dir" not in indicator or "deal_amount" not in indicator or "amount" not in indicator:
        raise RuntimeError(
            "Qlib order indicator requires trade_dir, amount, and deal_amount fields"
        )
    direction = pd.to_numeric(indicator["trade_dir"], errors="coerce")
    invalid_direction = direction.notna() & ~direction.isin([0.0, 1.0])
    if bool(invalid_direction.any()):
        raise RuntimeError(
            "Qlib order indicator contains unsupported trade_dir values: "
            f"{sorted(direction.loc[invalid_direction].unique().tolist())}"
        )
    indicator["side"] = direction.map({1.0: "buy", 0.0: "sell"})
    for field in ("amount", "deal_amount", "ffr", "trade_price", "trade_cost"):
        if field in indicator:
            indicator[field] = pd.to_numeric(indicator[field], errors="coerce")

    trades = indicator.loc[indicator["deal_amount"].fillna(0.0) > 0.0].copy()
    trades["quantity"] = trades["deal_amount"].abs()
    if "trade_price" in trades:
        trades["price"] = trades["trade_price"]
    if "trade_cost" in trades:
        trades["fees"] = trades["trade_cost"]
    trade_columns = [
        column
        for column in ("datetime", "instrument", "side", "quantity", "price", "fees")
        if column in trades
    ]
    return indicator, trades.loc[:, trade_columns]


def _load_optional_pickle(path: Path | None) -> Any:
    return pd.read_pickle(path) if path is not None else None


def _evaluator_source_sha256() -> str:
    source_root = Path(__file__).resolve().parents[1]
    files = (
        Path(__file__).resolve(),
        source_root / "backend/services/quantevolver/long_trend_evaluation.py",
        source_root / "backend/services/quantevolver/long_trend_data_reader.py",
        source_root / "backend/services/quantevolver/long_trend_evaluation_contract.py",
    )
    payload = {str(path.relative_to(source_root)): _sha256_file(path) for path in files}
    return canonical_sha256(payload)


def _json_default(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, set):
        return sorted(value)
    if pd.isna(value):
        return None
    raise TypeError(f"value is not JSON serializable: {type(value).__name__}")


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, default=_json_default),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _parquet_safe(frame: pd.DataFrame) -> pd.DataFrame:
    safe = frame.copy(deep=False)
    for column in safe.select_dtypes(include=["object"]).columns:
        sample = safe[column].dropna().head(100)
        if any(isinstance(value, (dict, list, tuple, set)) for value in sample):
            safe[column] = safe[column].map(
                lambda value: json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    default=_json_default,
                )
                if isinstance(value, (dict, list, tuple, set))
                else value
            )
    return safe


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.stem + ".tmp.parquet")
    _parquet_safe(frame).to_parquet(temporary, index=False)
    os.replace(temporary, path)


def persist_result(
    *,
    output_root: Path,
    artifacts: RecorderArtifacts,
    result: LongTrendEvaluationResult,
) -> Path:
    output_dir = output_root / artifacts.run_id / result.evaluation_id
    summary_path = output_dir / "summary.json"
    if summary_path.is_file():
        existing = _read_json_object(summary_path)
        if existing.get("evaluation_id") != result.evaluation_id:
            raise RuntimeError(f"immutable F-014 output identity conflict: {summary_path}")
        return output_dir

    output_dir.mkdir(parents=True, exist_ok=True)
    observations_path = output_dir / "signal_observations.parquet"
    episodes_path = output_dir / "holding_episodes.parquet"
    _atomic_parquet(observations_path, result.signal_observations)
    _atomic_parquet(episodes_path, result.holding_episodes)

    summary = {
        "schema_version": "qe_long_trend_evaluation_artifact_v1",
        "evaluation_id": result.evaluation_id,
        "run_id": artifacts.run_id,
        "profile_id": result.profile_id,
        "profile_sha256": result.profile_sha256,
        "evaluator_version": result.evaluator_version,
        "evaluation_asof": result.evaluation_asof,
        "label_horizon": artifacts.label_horizon,
        "strategy_topk": artifacts.strategy_topk,
        "family_status": {
            key: value.as_dict() for key, value in result.family_status.items()
        },
        "metrics": result.metrics,
        "receipt": result.receipt,
        "source_workspace": str(artifacts.workspace),
        "source_artifact_root": str(artifacts.artifact_root),
        "outputs": {
            "signal_observations": {
                "path": str(observations_path),
                "rows": int(len(result.signal_observations)),
                "sha256": _sha256_file(observations_path),
            },
            "holding_episodes": {
                "path": str(episodes_path),
                "rows": int(len(result.holding_episodes)),
                "sha256": _sha256_file(episodes_path),
            },
        },
    }
    _atomic_json(summary_path, summary)
    return output_dir


def evaluate_run(
    *,
    artifacts: RecorderArtifacts,
    prices: pd.DataFrame,
    sectors: pd.DataFrame | None,
    snapshot_identity: Any,
    overlap_receipt: Any,
    evaluator_source_sha256: str,
    output_root: Path,
) -> dict[str, Any]:
    loaded_source_sha256 = _evaluator_source_sha256()
    if loaded_source_sha256 != evaluator_source_sha256:
        raise RuntimeError(
            "F-014 evaluator source changed after process identity was captured; "
            f"expected={evaluator_source_sha256} actual={loaded_source_sha256}"
        )
    predictions = pd.read_pickle(artifacts.prediction_path)
    labels = _load_optional_pickle(artifacts.label_path)
    raw_positions = _load_optional_pickle(artifacts.position_path)
    positions = (
        normalize_qlib_position_artifact(raw_positions) if raw_positions is not None else None
    )
    portfolio_report = _load_optional_pickle(artifacts.portfolio_report_path)
    raw_indicator = _load_optional_pickle(artifacts.indicator_object_path)
    execution_evidence = None
    if raw_indicator is not None:
        indicator, trades = normalize_qlib_indicator_object(raw_indicator)
        execution_evidence = ExecutionEvidenceBundle(indicator=indicator, trades=trades)

    context = QELongTrendEvaluationContext(
        run_id=artifacts.run_id,
        evaluator_source_sha256=evaluator_source_sha256,
        feature_snapshot=snapshot_identity,
        outcome_snapshot=snapshot_identity,
        overlap_receipt=overlap_receipt,
        input_artifact_hashes=artifacts.input_hashes,
    )
    engine = QELongTrendEvaluationEngine(QE_LONG_TREND_PROFILE_V1)
    result = engine.evaluate(
        context=context,
        predictions=predictions,
        prices=prices,
        sectors=sectors,
        labels=labels,
        label_horizon=artifacts.label_horizon,
        positions=positions,
        portfolio_report=portfolio_report,
        execution_evidence=execution_evidence,
        strategy_topk=artifacts.strategy_topk,
    )
    persisted_source_sha256 = _evaluator_source_sha256()
    if persisted_source_sha256 != evaluator_source_sha256:
        raise RuntimeError(
            "F-014 evaluator source changed during evaluation; refusing to persist a mismatched receipt; "
            f"expected={evaluator_source_sha256} actual={persisted_source_sha256}"
        )
    output_dir = persist_result(output_root=output_root, artifacts=artifacts, result=result)
    return {
        "run_id": artifacts.run_id,
        "evaluation_id": result.evaluation_id,
        "output_dir": str(output_dir),
        "signal_rows": int(len(result.signal_observations)),
        "episode_rows": int(len(result.holding_episodes)),
        "metric_count": int(len(result.metrics)),
        "family_status": {
            name: status.status.value for name, status in result.family_status.items()
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workspace",
        action="append",
        required=True,
        help="QE loop workspace; repeat for a batch",
    )
    parser.add_argument(
        "--factor-data-dir",
        default=os.getenv("QE_FACTOR_DATA_DIR") or os.getenv("RDAGENT_FACTOR_DATA_WSL"),
        help="immutable QE factor-data snapshot root; or QE_FACTOR_DATA_DIR/RDAGENT_FACTOR_DATA_WSL",
    )
    parser.add_argument(
        "--output-root",
        default=os.getenv("QE_LONG_TREND_ARTIFACT_STORE_ROOT"),
        help="QE-only result root; or QE_LONG_TREND_ARTIFACT_STORE_ROOT",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.factor_data_dir:
        raise RuntimeError(
            "--factor-data-dir or QE_FACTOR_DATA_DIR/RDAGENT_FACTOR_DATA_WSL is required"
        )
    if not args.output_root:
        raise RuntimeError(
            "--output-root or QE_LONG_TREND_ARTIFACT_STORE_ROOT is required"
        )
    evaluator_sha = _evaluator_source_sha256()
    artifacts = [resolve_recorder_artifacts(value) for value in args.workspace]
    run_ids = [item.run_id for item in artifacts]
    if len(run_ids) != len(set(run_ids)):
        raise RuntimeError(f"duplicate QE run ids in batch: {run_ids}")

    snapshot_identity = inspect_qe_snapshot_identity(args.factor_data_dir)
    for item in artifacts:
        QELongTrendDatasetReader(
            factor_data_dir=args.factor_data_dir,
            qe_workspace_root=item.workspace,
            qe_dataset_contract_id=QE_DATASET_CONTRACT_ID,
            snapshot_identity=snapshot_identity,
        ).verify_workspace_binding()

    reader = QELongTrendDatasetReader(
        factor_data_dir=args.factor_data_dir,
        qe_workspace_root=artifacts[0].workspace,
        qe_dataset_contract_id=QE_DATASET_CONTRACT_ID,
        snapshot_identity=snapshot_identity,
    )
    print(
        json.dumps(
            {
                "event": "f014_snapshot_load_started",
                "snapshot_id": snapshot_identity.snapshot_id,
                "run_count": len(artifacts),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    full_prices = reader.load_prices(
        start_date=snapshot_identity.start_date,
        end_date=snapshot_identity.end_date,
    )
    overlap_receipt = verify_outcome_snapshot_extension(
        feature_identity=snapshot_identity,
        outcome_identity=snapshot_identity,
        feature_prices=full_prices,
        outcome_prices=full_prices,
    )
    del full_prices
    gc.collect()

    evaluation_start = min(item.test_start for item in artifacts)
    frames = reader.load(
        start_date=evaluation_start,
        end_date=snapshot_identity.end_date,
        include_sector=True,
    )
    output_root = Path(args.output_root).expanduser().resolve()
    results: list[dict[str, Any]] = []
    for index, item in enumerate(artifacts, start=1):
        print(
            json.dumps(
                {
                    "event": "f014_run_started",
                    "run_id": item.run_id,
                    "batch_index": index,
                    "batch_size": len(artifacts),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        result = evaluate_run(
            artifacts=item,
            prices=frames.prices,
            sectors=frames.sectors,
            snapshot_identity=snapshot_identity,
            overlap_receipt=overlap_receipt,
            evaluator_source_sha256=evaluator_sha,
            output_root=output_root,
        )
        results.append(result)
        print(json.dumps({"event": "f014_run_completed", **result}, ensure_ascii=False), flush=True)
        gc.collect()

    batch_summary = {
        "schema_version": "qe_long_trend_evaluation_batch_v1",
        "snapshot_identity": {
            "snapshot_id": snapshot_identity.snapshot_id,
            "manifest_sha256": snapshot_identity.manifest_sha256,
            "start_date": snapshot_identity.start_date,
            "end_date": snapshot_identity.end_date,
        },
        "profile_id": QE_LONG_TREND_PROFILE_V1.profile_id,
        "profile_sha256": QE_LONG_TREND_PROFILE_V1.profile_sha256,
        "evaluator_source_sha256": evaluator_sha,
        "results": results,
    }
    batch_id = canonical_sha256(
        {
            "run_ids": run_ids,
            "snapshot_manifest_sha256": snapshot_identity.manifest_sha256,
            "profile_sha256": QE_LONG_TREND_PROFILE_V1.profile_sha256,
            "evaluator_source_sha256": evaluator_sha,
        }
    )
    batch_path = output_root / "batches" / f"{batch_id}.json"
    _atomic_json(batch_path, batch_summary)
    print(
        json.dumps(
            {"event": "f014_batch_completed", "batch_path": str(batch_path), "results": results},
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
