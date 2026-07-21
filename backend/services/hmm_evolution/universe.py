"""Resolve the exact source-loop stock pool and immutable QE ST-PIT universe."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass, replace
from datetime import date
from typing import Any, Mapping, Protocol

import numpy as np
import pandas as pd
from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.hmm_data_source.legacy_qe_artifact_manifests import (
    LegacyQESTPITCompatibilityReceipt,
    find_legacy_qe_artifact_manifest,
)
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient
from backend.services.quantevolver.stock_pool_sync import (
    StockPoolInterval,
    StockPoolSnapshot,
    read_stock_pool_snapshot,
)
from backend.services.stock_universe_pit_service import DEFAULT_ST_PIT_RULE_VERSION

from .errors import InvalidSpecError
from .models import EvaluationSpec


SOURCE_LOOP_UNIVERSE_TYPE = "source_loop_stock_pool_st_pit"
SOURCE_RISK_POLICY_ARTIFACT = "qe_event_risk_policy.json"
LEGACY_QE_ST_PIT_UNIVERSE_KEY = "shsz_st_pit_active_v1"
DATASET_QE_ST_PIT_PREFIX = "shsz_st_pit_qe_dataset_"
RISK_POLICY_CONTRACT = "stock_event_risk_policy_v1"
RISK_POLICY_VISIBLE_TIME_MODE = "next_trading_session"
MAX_RISK_POLICY_ARTIFACT_BYTES = 16 * 1024 * 1024
_LOOP_RE = re.compile(r"^Loop(?P<index>[1-9][0-9]*)$")
_SYMBOL_RE = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class SourceLoopUniverseContract:
    task_id: str
    loop_name: str
    stock_pool: str
    risk_policy: Mapping[str, Any]
    risk_policy_origin: str = "persisted_source_loop"
    st_pit_compatibility: LegacyQESTPITCompatibilityReceipt | None = None


@dataclass(frozen=True)
class ResolvedEvaluationUniverse:
    predictions: pd.DataFrame
    labels: pd.DataFrame
    evidence: Mapping[str, Any]


@dataclass(frozen=True)
class SourceLoopRiskPolicySnapshot:
    snapshot: StockPoolSnapshot
    artifact_sha256: str
    dataset_contract_id: str | None
    universe_key: str
    binding_mode: str
    rule_version: str
    scope: str
    source_fingerprint_sha256: str
    start_date: date
    end_date: date
    artifact_size_bytes: int
    artifact_source_task_id: str
    artifact_source_loop_name: str


class SourceLoopUniverseRepository(Protocol):
    def load(self, base_loop_ref: str) -> SourceLoopUniverseContract: ...


class StockPoolSnapshotLoader(Protocol):
    def __call__(self, stock_pool: str) -> StockPoolSnapshot: ...


class SourceRiskPolicySnapshotLoader(Protocol):
    def __call__(self, task_id: str, loop_name: str) -> SourceLoopRiskPolicySnapshot: ...


class QELoopUniverseRepository:
    """Read the source loop's persisted selection contract without modifying QE state."""

    def load(self, base_loop_ref: str) -> SourceLoopUniverseContract:
        task_id, loop_name, loop_index = _parse_loop_ref(base_loop_ref)
        with get_conn(autocommit=False, manage_transaction=True) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                cursor.execute(
                    """
                    SELECT config_json
                    FROM qe_evolution_loops
                    WHERE task_id = %s AND loop_index = %s AND status = 'completed'
                    """,
                    (task_id, loop_index),
                )
                row = cursor.fetchone()
        if row is None:
            raise InvalidSpecError(
                "source QE loop has no completed persisted selection contract",
                context={"base_loop_ref": base_loop_ref},
            )
        config = dict(row.get("config_json") or {})
        config_sections = [
            config,
            *(
                dict(config.get(key) or {})
                for key in ("model_params", "strategy_params")
                if isinstance(config.get(key), Mapping)
            ),
        ]
        stock_pools = {
            str(section.get("stock_pool") or "").strip()
            for section in config_sections
            if str(section.get("stock_pool") or "").strip()
        }
        if not stock_pools:
            raise InvalidSpecError(
                "source QE loop does not declare an explicit stock_pool",
                context={"base_loop_ref": base_loop_ref},
            )
        if len(stock_pools) != 1:
            raise InvalidSpecError(
                "source QE loop contains conflicting stock_pool declarations",
                context={"base_loop_ref": base_loop_ref},
            )
        stock_pool = next(iter(stock_pools))
        risk_candidates = [
            dict(section.get("risk_policy") or {})
            for section in config_sections
            if isinstance(section.get("risk_policy"), Mapping)
        ]
        compatibility: LegacyQESTPITCompatibilityReceipt | None = None
        risk_policy_origin = "persisted_source_loop"
        if not risk_candidates:
            legacy_manifest = find_legacy_qe_artifact_manifest(base_loop_ref)
            compatibility = (
                legacy_manifest.st_pit_compatibility if legacy_manifest is not None else None
            )
            if compatibility is None:
                raise InvalidSpecError(
                    "source QE loop does not declare an ST-PIT risk policy",
                    context={"base_loop_ref": base_loop_ref},
                )
            actual_config_sha256 = _canonical_json_sha256(config)
            if actual_config_sha256 != compatibility.source_config_sha256:
                raise InvalidSpecError(
                    "legacy QE source config differs from its allowlisted ST-PIT compatibility receipt",
                    context={
                        "base_loop_ref": base_loop_ref,
                        "expected_sha256": compatibility.source_config_sha256,
                        "actual_sha256": actual_config_sha256,
                    },
                )
            risk_policy_origin = compatibility.binding_mode
            risk_candidates = [
                {
                    "enabled": True,
                    "providers": ["st_pit"],
                    "hard_actions": ["block_buy", "force_exit"],
                    "policy_version": RISK_POLICY_CONTRACT,
                    "strict_data_ready": True,
                    "st_universe_key": compatibility.universe_key,
                    "visible_time_mode": RISK_POLICY_VISIBLE_TIME_MODE,
                }
            ]
        if any(candidate != risk_candidates[0] for candidate in risk_candidates[1:]):
            raise InvalidSpecError(
                "source QE loop contains conflicting risk policy declarations",
                context={"base_loop_ref": base_loop_ref},
            )
        risk_policy = risk_candidates[0]
        if (
            risk_policy.get("enabled") is not True
            or "st_pit" not in set(risk_policy.get("providers") or ())
            or risk_policy.get("strict_data_ready") is not True
        ):
            raise InvalidSpecError(
                "source QE loop ST-PIT policy is not enabled in strict mode",
                context={"base_loop_ref": base_loop_ref},
            )
        configured_universe_key = str(risk_policy.get("st_universe_key") or "").strip()
        if not configured_universe_key:
            raise InvalidSpecError(
                "source QE loop does not persist its ST-PIT universe key",
                context={"base_loop_ref": base_loop_ref},
            )
        return SourceLoopUniverseContract(
            task_id=task_id,
            loop_name=loop_name,
            stock_pool=stock_pool,
            risk_policy=risk_policy,
            risk_policy_origin=risk_policy_origin,
            st_pit_compatibility=compatibility,
        )


async def _download_source_risk_policy_artifact(task_id: str, loop_name: str) -> bytes:
    client = QEWorkspaceClient.for_task_loop(task_id, loop_name)
    async with client:
        return await client.download_workspace_file_bytes(
            task_id,
            loop_name,
            SOURCE_RISK_POLICY_ARTIFACT,
        )


def load_source_risk_policy_snapshot(
    task_id: str,
    loop_name: str,
) -> SourceLoopRiskPolicySnapshot:
    """Load and validate the exact ST-PIT artifact consumed by the source loop."""

    try:
        raw = asyncio.run(_download_source_risk_policy_artifact(task_id, loop_name))
    except Exception as exc:
        raise InvalidSpecError(
            "source QE runtime ST-PIT artifact is unavailable",
            context={
                "task_id": task_id,
                "loop_name": loop_name,
                "artifact": SOURCE_RISK_POLICY_ARTIFACT,
                "error_type": type(exc).__name__,
            },
        ) from exc
    return _parse_source_risk_policy_snapshot(
        raw,
        task_id=task_id,
        loop_name=loop_name,
    )


def _parse_source_risk_policy_snapshot(
    raw: bytes,
    *,
    task_id: str,
    loop_name: str,
) -> SourceLoopRiskPolicySnapshot:
    if not raw or len(raw) > MAX_RISK_POLICY_ARTIFACT_BYTES:
        raise InvalidSpecError(
            "source QE runtime ST-PIT artifact has an invalid size",
            context={
                "task_id": task_id,
                "loop_name": loop_name,
                "size_bytes": len(raw),
                "max_bytes": MAX_RISK_POLICY_ARTIFACT_BYTES,
            },
        )
    artifact_sha256 = hashlib.sha256(raw).hexdigest()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidSpecError(
            "source QE runtime ST-PIT artifact is not valid UTF-8 JSON",
            context={"task_id": task_id, "loop_name": loop_name},
        ) from exc
    if not isinstance(payload, Mapping):
        raise InvalidSpecError("source QE runtime ST-PIT artifact must be a JSON object")

    providers = tuple(str(item).strip() for item in payload.get("providers") or ())
    hard_actions = tuple(str(item).strip() for item in payload.get("hard_actions") or ())
    if (
        payload.get("enabled") is not True
        or payload.get("strict_data_ready") is not True
        or "st_pit" not in providers
        or "block_buy" not in hard_actions
        or str(payload.get("contract") or "").strip() != RISK_POLICY_CONTRACT
        or str(payload.get("visible_time_mode") or "").strip()
        != RISK_POLICY_VISIBLE_TIME_MODE
    ):
        raise InvalidSpecError(
            "source QE runtime ST-PIT artifact does not declare the required strict policy",
            context={"task_id": task_id, "loop_name": loop_name},
        )

    state = payload.get("state")
    if not isinstance(state, Mapping):
        raise InvalidSpecError("source QE runtime ST-PIT artifact is missing state metadata")
    universe_key = str(payload.get("st_universe_key") or "").strip()
    state_universe_key = str(state.get("universe_key") or "").strip()
    dataset_contract_id = str(payload.get("dataset_contract_id") or "").strip() or None
    if not universe_key or state_universe_key != universe_key:
        raise InvalidSpecError(
            "source QE runtime ST-PIT artifact has inconsistent universe identity",
            context={
                "task_id": task_id,
                "loop_name": loop_name,
                "declared_universe_key": universe_key or None,
                "state_universe_key": state_universe_key or None,
            },
        )
    if dataset_contract_id is None:
        if universe_key != LEGACY_QE_ST_PIT_UNIVERSE_KEY:
            raise InvalidSpecError(
                "legacy source QE runtime ST-PIT artifact uses an unknown universe key",
                context={"task_id": task_id, "loop_name": loop_name, "universe_key": universe_key},
            )
        binding_mode = "legacy_frozen_runtime_artifact_v1"
    else:
        expected_key = f"{DATASET_QE_ST_PIT_PREFIX}{dataset_contract_id}"
        if universe_key != expected_key:
            raise InvalidSpecError(
                "source QE runtime ST-PIT dataset identity is inconsistent",
                context={
                    "task_id": task_id,
                    "loop_name": loop_name,
                    "dataset_contract_id": dataset_contract_id,
                    "expected_universe_key": expected_key,
                    "actual_universe_key": universe_key,
                },
            )
        binding_mode = "immutable_dataset_runtime_artifact_v1"

    rule_version = str(state.get("rule_version") or "").strip()
    scope = str(state.get("scope") or "").strip()
    fingerprint = str(state.get("source_fingerprint_sha256") or "").strip().lower()
    if (
        state.get("status") != "ready"
        or state.get("dirty") is not False
        or rule_version != DEFAULT_ST_PIT_RULE_VERSION
        or not scope
        or not _SHA256_RE.fullmatch(fingerprint)
    ):
        raise InvalidSpecError(
            "source QE runtime ST-PIT artifact state is not immutable and ready",
            context={
                "task_id": task_id,
                "loop_name": loop_name,
                "status": state.get("status"),
                "dirty": state.get("dirty"),
                "rule_version": rule_version or None,
                "scope": scope or None,
            },
        )

    try:
        start_date = date.fromisoformat(str(payload.get("start_date") or ""))
        end_date = date.fromisoformat(str(payload.get("end_date") or ""))
    except ValueError as exc:
        raise InvalidSpecError("source QE runtime ST-PIT artifact has invalid coverage dates") from exc
    if end_date < start_date:
        raise InvalidSpecError("source QE runtime ST-PIT artifact coverage ends before it starts")

    raw_spans = payload.get("active_spans")
    if not isinstance(raw_spans, list) or not raw_spans:
        raise InvalidSpecError("source QE runtime ST-PIT artifact contains no active spans")
    if payload.get("span_count") != len(raw_spans):
        raise InvalidSpecError(
            "source QE runtime ST-PIT artifact span count is inconsistent",
            context={"declared": payload.get("span_count"), "actual": len(raw_spans)},
        )
    intervals: list[StockPoolInterval] = []
    last_end_by_symbol: dict[str, date] = {}
    for index, item in enumerate(raw_spans):
        if not isinstance(item, Mapping):
            raise InvalidSpecError(
                "source QE runtime ST-PIT artifact contains a non-object span",
                context={"span_index": index},
            )
        symbol = str(item.get("ts_code") or "").strip().upper()
        try:
            eligible_start = date.fromisoformat(str(item.get("eligible_start") or ""))
            eligible_end = date.fromisoformat(str(item.get("eligible_end") or ""))
        except ValueError as exc:
            raise InvalidSpecError(
                "source QE runtime ST-PIT artifact contains invalid span dates",
                context={"span_index": index, "symbol": symbol or None},
            ) from exc
        if (
            not _SYMBOL_RE.fullmatch(symbol)
            or eligible_end < eligible_start
            or str(item.get("rule_version") or "").strip() != rule_version
        ):
            raise InvalidSpecError(
                "source QE runtime ST-PIT artifact contains an invalid span",
                context={"span_index": index, "symbol": symbol or None},
            )
        previous_end = last_end_by_symbol.get(symbol)
        if previous_end is not None and eligible_start <= previous_end:
            raise InvalidSpecError(
                "source QE runtime ST-PIT artifact contains overlapping or unsorted spans",
                context={"span_index": index, "symbol": symbol},
            )
        last_end_by_symbol[symbol] = eligible_end
        intervals.append(StockPoolInterval(symbol, eligible_start, eligible_end))

    return SourceLoopRiskPolicySnapshot(
        snapshot=StockPoolSnapshot(
            filename=SOURCE_RISK_POLICY_ARTIFACT,
            instrument_name=universe_key,
            sha256=artifact_sha256,
            intervals=tuple(intervals),
        ),
        artifact_sha256=artifact_sha256,
        dataset_contract_id=dataset_contract_id,
        universe_key=universe_key,
        binding_mode=binding_mode,
        rule_version=rule_version,
        scope=scope,
        source_fingerprint_sha256=fingerprint,
        start_date=start_date,
        end_date=end_date,
        artifact_size_bytes=len(raw),
        artifact_source_task_id=task_id,
        artifact_source_loop_name=loop_name,
    )


def _canonical_json_sha256(value: Mapping[str, Any]) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _verify_legacy_st_pit_compatibility_receipt(
    *,
    base_loop_ref: str,
    receipt: LegacyQESTPITCompatibilityReceipt,
    runtime_snapshot: SourceLoopRiskPolicySnapshot,
) -> None:
    actual = {
        "artifact_source_task_id": runtime_snapshot.artifact_source_task_id,
        "artifact_source_loop_name": runtime_snapshot.artifact_source_loop_name,
        "sha256": runtime_snapshot.artifact_sha256,
        "size_bytes": runtime_snapshot.artifact_size_bytes,
        "universe_key": runtime_snapshot.universe_key,
        "rule_version": runtime_snapshot.rule_version,
        "scope": runtime_snapshot.scope,
        "source_fingerprint_sha256": runtime_snapshot.source_fingerprint_sha256,
        "start_date": runtime_snapshot.start_date,
        "end_date": runtime_snapshot.end_date,
        "span_count": len(runtime_snapshot.snapshot.intervals),
    }
    expected = {
        "artifact_source_task_id": receipt.artifact_source_task_id,
        "artifact_source_loop_name": receipt.artifact_source_loop_name,
        "sha256": receipt.sha256,
        "size_bytes": receipt.size_bytes,
        "universe_key": receipt.universe_key,
        "rule_version": receipt.rule_version,
        "scope": receipt.scope,
        "source_fingerprint_sha256": receipt.source_fingerprint_sha256,
        "start_date": receipt.start_date,
        "end_date": receipt.end_date,
        "span_count": receipt.span_count,
    }
    mismatches = {
        key: {"expected": expected[key], "actual": actual[key]}
        for key in expected
        if expected[key] != actual[key]
    }
    if mismatches:
        raise InvalidSpecError(
            "legacy QE ST-PIT compatibility artifact differs from its allowlisted receipt",
            context={"base_loop_ref": base_loop_ref, "mismatches": mismatches},
        )


def _verify_persisted_policy_matches_runtime_artifact(
    *,
    base_loop_ref: str,
    persisted_policy: Mapping[str, Any],
    runtime_snapshot: SourceLoopRiskPolicySnapshot,
) -> None:
    mismatches: dict[str, Any] = {}
    configured_key = str(persisted_policy.get("st_universe_key") or "").strip()
    if configured_key != runtime_snapshot.universe_key:
        mismatches["st_universe_key"] = {
            "persisted": configured_key or None,
            "runtime_artifact": runtime_snapshot.universe_key,
        }
    expected_values = {
        "policy_version": RISK_POLICY_CONTRACT,
        "visible_time_mode": RISK_POLICY_VISIBLE_TIME_MODE,
    }
    for key, expected in expected_values.items():
        actual = str(persisted_policy.get(key) or "").strip()
        if actual != expected:
            mismatches[key] = {"persisted": actual or None, "required": expected}
    if "block_buy" not in set(persisted_policy.get("hard_actions") or ()):
        mismatches["hard_actions"] = {
            "persisted": list(persisted_policy.get("hard_actions") or ()),
            "required": "contains block_buy",
        }
    if mismatches:
        raise InvalidSpecError(
            "source QE persisted risk policy differs from its frozen runtime artifact",
            context={"base_loop_ref": base_loop_ref, "mismatches": mismatches},
        )


class QEExecutionUniverseResolver:
    """Intersect the source pool with the exact frozen ST-PIT runtime artifact."""

    def __init__(
        self,
        *,
        loop_repository: SourceLoopUniverseRepository | None = None,
        stock_pool_loader: StockPoolSnapshotLoader = read_stock_pool_snapshot,
        risk_policy_loader: SourceRiskPolicySnapshotLoader = load_source_risk_policy_snapshot,
    ) -> None:
        self._loop_repository = loop_repository or QELoopUniverseRepository()
        self._stock_pool_loader = stock_pool_loader
        self._risk_policy_loader = risk_policy_loader

    def resolve(
        self,
        *,
        evaluation_spec: EvaluationSpec,
        predictions: pd.DataFrame,
        labels: pd.DataFrame,
    ) -> ResolvedEvaluationUniverse:
        if evaluation_spec.schema_version != "hmm_evaluation_spec_v2":
            raise InvalidSpecError("new HMM evaluations require hmm_evaluation_spec_v2")
        if evaluation_spec.universe != {"type": SOURCE_LOOP_UNIVERSE_TYPE}:
            raise InvalidSpecError(
                "HMM evaluation universe must be resolved from the source loop stock pool and ST-PIT contract"
            )
        contract = self._loop_repository.load(evaluation_spec.base_loop_ref)
        try:
            pool_snapshot = self._stock_pool_loader(contract.stock_pool)
        except (OSError, RuntimeError, ValueError) as exc:
            raise InvalidSpecError(
                "source QE stock_pool cannot be read and verified",
                context={"stock_pool": contract.stock_pool, "error_type": type(exc).__name__},
            ) from exc
        compatibility = contract.st_pit_compatibility
        if compatibility is not None and pool_snapshot.sha256 != compatibility.stock_pool_sha256:
            raise InvalidSpecError(
                "legacy QE stock_pool differs from its allowlisted ST-PIT compatibility receipt",
                context={
                    "base_loop_ref": evaluation_spec.base_loop_ref,
                    "stock_pool": contract.stock_pool,
                    "expected_sha256": compatibility.stock_pool_sha256,
                    "actual_sha256": pool_snapshot.sha256,
                },
            )
        artifact_task_id = (
            compatibility.artifact_source_task_id if compatibility is not None else contract.task_id
        )
        artifact_loop_name = (
            compatibility.artifact_source_loop_name if compatibility is not None else contract.loop_name
        )
        try:
            risk_snapshot = self._risk_policy_loader(artifact_task_id, artifact_loop_name)
        except InvalidSpecError:
            raise
        except Exception as exc:
            raise InvalidSpecError(
                "source QE runtime ST-PIT artifact cannot be read and verified",
                context={
                    "base_loop_ref": evaluation_spec.base_loop_ref,
                    "artifact": SOURCE_RISK_POLICY_ARTIFACT,
                    "error_type": type(exc).__name__,
                },
            ) from exc
        if compatibility is not None:
            _verify_legacy_st_pit_compatibility_receipt(
                base_loop_ref=evaluation_spec.base_loop_ref,
                receipt=compatibility,
                runtime_snapshot=risk_snapshot,
            )
            risk_snapshot = replace(
                risk_snapshot,
                binding_mode=compatibility.binding_mode,
            )
        _verify_persisted_policy_matches_runtime_artifact(
            base_loop_ref=evaluation_spec.base_loop_ref,
            persisted_policy=contract.risk_policy,
            runtime_snapshot=risk_snapshot,
        )
        if (
            risk_snapshot.start_date > evaluation_spec.window_start
            or risk_snapshot.end_date < evaluation_spec.window_end
        ):
            raise InvalidSpecError(
                "source QE runtime ST-PIT artifact does not cover the evaluation window",
                context={
                    "base_loop_ref": evaluation_spec.base_loop_ref,
                    "artifact_start_date": risk_snapshot.start_date.isoformat(),
                    "artifact_end_date": risk_snapshot.end_date.isoformat(),
                    "window_start": evaluation_spec.window_start.isoformat(),
                    "window_end": evaluation_spec.window_end.isoformat(),
                },
            )

        normalized_predictions = _window_pairs(
            _normalize_pairs(predictions, required_value="score"),
            start=evaluation_spec.window_start,
            end=evaluation_spec.window_end,
        )
        normalized_labels = _window_pairs(
            _normalize_pairs(labels, required_value="future_return"),
            start=evaluation_spec.window_start,
            end=evaluation_spec.window_end,
        )
        dates = tuple(sorted(normalized_predictions["trade_date"].unique()))
        symbols = tuple(sorted(normalized_predictions["symbol"].unique()))
        if not dates or not symbols:
            raise InvalidSpecError("source prediction artifact has no universe rows")

        date_index = pd.DatetimeIndex(dates)
        base_mask = _build_pool_mask(date_index, symbols, pool_snapshot)
        pit_mask = _build_pool_mask(date_index, symbols, risk_snapshot.snapshot)
        if pit_mask.shape != base_mask.shape:
            raise InvalidSpecError("QE ST-PIT mask shape does not match the source prediction universe")
        combined_mask = base_mask & pit_mask
        filtered_predictions = _apply_mask(normalized_predictions, date_index, symbols, combined_mask)
        per_day = (
            filtered_predictions.groupby("trade_date", sort=True)["symbol"]
            .nunique()
            .reindex(dates, fill_value=0)
        )
        insufficient = per_day.loc[per_day < evaluation_spec.topk]
        if not insufficient.empty:
            first_date = insufficient.index[0]
            raise InvalidSpecError(
                "source-loop eligible universe is smaller than TopK",
                context={
                    "date": first_date.isoformat(),
                    "eligible_count": int(insufficient.iloc[0]),
                    "topk": evaluation_spec.topk,
                },
            )
        eligible_pairs = filtered_predictions[["trade_date", "symbol"]].drop_duplicates()
        filtered_labels = normalized_labels.merge(
            eligible_pairs,
            on=["trade_date", "symbol"],
            how="inner",
            validate="many_to_one",
        )
        evidence = {
            "type": SOURCE_LOOP_UNIVERSE_TYPE,
            "universe_id": f"{pool_snapshot.instrument_name}:{risk_snapshot.universe_key}",
            "universe_hash": _eligible_pair_hash(eligible_pairs),
            "symbol_count": int(filtered_predictions["symbol"].nunique()),
            "eligible_pair_count": int(len(eligible_pairs)),
            "prediction_row_count_before": int(len(normalized_predictions)),
            "prediction_row_count_after": int(len(filtered_predictions)),
            "excluded_prediction_row_count": int(len(normalized_predictions) - len(filtered_predictions)),
            "source_loop": {"task_id": contract.task_id, "loop_name": contract.loop_name},
            "stock_pool": {
                "name": pool_snapshot.instrument_name,
                "filename": pool_snapshot.filename,
                "sha256": pool_snapshot.sha256,
                "interval_count": len(pool_snapshot.intervals),
            },
            "st_pit": {
                "artifact_path": SOURCE_RISK_POLICY_ARTIFACT,
                "artifact_sha256": risk_snapshot.artifact_sha256,
                "binding_mode": risk_snapshot.binding_mode,
                "dataset_contract_id": risk_snapshot.dataset_contract_id,
                "universe_key": risk_snapshot.universe_key,
                "rule_version": risk_snapshot.rule_version,
                "scope": risk_snapshot.scope,
                "source_fingerprint_sha256": risk_snapshot.source_fingerprint_sha256,
                "artifact_start_date": risk_snapshot.start_date.isoformat(),
                "artifact_end_date": risk_snapshot.end_date.isoformat(),
                "span_count": len(risk_snapshot.snapshot.intervals),
                "index_policy": "source_loop_runtime_active_spans_v1",
                "coverage_semantics": (
                    "allowlisted_cross_loop_immutable_artifact_v1"
                    if compatibility is not None
                    else "exact_source_loop_runtime_artifact_v1"
                ),
            },
        }
        if compatibility is not None:
            evidence["st_pit"]["compatibility_receipt"] = {
                "source_loop_config_sha256": compatibility.source_config_sha256,
                "source_loop_stock_pool_sha256": compatibility.stock_pool_sha256,
                "artifact_source": {
                    "task_id": compatibility.artifact_source_task_id,
                    "loop_name": compatibility.artifact_source_loop_name,
                    "path": compatibility.workspace_path,
                },
                "artifact_sha256": compatibility.sha256,
                "artifact_size_bytes": compatibility.size_bytes,
            }
        return ResolvedEvaluationUniverse(
            predictions=filtered_predictions,
            labels=filtered_labels,
            evidence=evidence,
        )


def _parse_loop_ref(base_loop_ref: str) -> tuple[str, str, int]:
    parts = str(base_loop_ref or "").split("/")
    if len(parts) != 2 or not parts[0]:
        raise InvalidSpecError("base_loop_ref must use '<task_id>/LoopN'")
    match = _LOOP_RE.fullmatch(parts[1])
    if match is None:
        raise InvalidSpecError("base_loop_ref must use '<task_id>/LoopN'")
    return parts[0], parts[1], int(match.group("index"))


def _normalize_pairs(frame: pd.DataFrame, *, required_value: str) -> pd.DataFrame:
    required = {"trade_date", "symbol", required_value}
    if not required.issubset(frame.columns):
        raise InvalidSpecError(
            "universe input frame is missing required columns",
            context={"required": sorted(required), "actual": sorted(map(str, frame.columns))},
        )
    result = frame.copy()
    result["trade_date"] = pd.to_datetime(result["trade_date"], errors="raise").dt.date
    result["symbol"] = result["symbol"].astype(str).str.strip().str.upper()
    return result


def _window_pairs(frame: pd.DataFrame, *, start: date, end: date) -> pd.DataFrame:
    return frame.loc[
        frame["trade_date"].between(start, end, inclusive="both")
    ].reset_index(drop=True)


def _build_pool_mask(
    date_index: pd.DatetimeIndex,
    symbols: tuple[str, ...],
    snapshot: StockPoolSnapshot,
) -> np.ndarray:
    mask = np.zeros((len(date_index), len(symbols)), dtype=bool)
    column_by_symbol = {symbol: index for index, symbol in enumerate(symbols)}
    for interval in snapshot.intervals:
        column = column_by_symbol.get(interval.ts_code)
        if column is None:
            continue
        active = (date_index >= pd.Timestamp(interval.eligible_start)) & (
            date_index <= pd.Timestamp(interval.eligible_end)
        )
        if active.any():
            mask[active, column] = True
    return mask


def _apply_mask(
    frame: pd.DataFrame,
    dates: pd.DatetimeIndex,
    symbols: tuple[str, ...],
    mask: np.ndarray,
) -> pd.DataFrame:
    row_by_date = {item.date(): index for index, item in enumerate(dates)}
    column_by_symbol = {symbol: index for index, symbol in enumerate(symbols)}
    keep = np.fromiter(
        (
            mask[row_by_date[trade_date], column_by_symbol[symbol]]
            for trade_date, symbol in frame[["trade_date", "symbol"]].itertuples(index=False, name=None)
        ),
        dtype=bool,
        count=len(frame),
    )
    return frame.loc[keep].reset_index(drop=True)


def _eligible_pair_hash(pairs: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    ordered = pairs.sort_values(["trade_date", "symbol"], kind="mergesort")
    for trade_date, symbol in ordered.itertuples(index=False, name=None):
        digest.update(f"{trade_date.isoformat()}\t{symbol}\n".encode("utf-8"))
    return digest.hexdigest()
