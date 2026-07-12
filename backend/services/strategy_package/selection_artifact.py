"""Selection score artifacts for StrategyPackage runtime.

Selection artifacts are deliberately separate from minute execution policies:
they store the ranked model score universe needed by Selection Center, without
requiring V24/V25 execution algorithms or their runtime dependencies.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence
from uuid import uuid4

import pandas as pd
import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.db.pg_pool import get_conn
from backend.services.market_data.instrument_validator import (
    DEFAULT_SQL_CHUNK_SIZE,
    load_chunks_with_logging,
    normalize_and_validate_ts_codes,
)
from backend.services.selection_center.runtime_profile import parse_selection_runtime_profile
from backend.services.selection_center.prospective_evidence import (
    REASON_ASSET_CLOSURE_INCOMPLETE,
    REASON_SOURCE_RECEIPT_INCOMPLETE,
    REASON_UNIVERSE_RECEIPT_INCOMPLETE,
    REASON_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE,
    SourceReadReceipt,
    canonical_evidence_json_sha256,
)
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    DataUnavailableError,
    InvalidStateTransitionError,
    PackageAssetInvalidError,
    RuntimeConfigInvalidError,
)

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
from .models import AlphaMode, SelectionScoreArtifactStatus
from .repository import StrategyPackageRepository
from .workspace_policy import ensure_not_forbidden_worker_workspace_path

ConnFactory = Callable[[], Iterator[Any]]
logger = logging.getLogger("aistock.strategy_package.selection_artifact")

SELECTION_SCORE_ARTIFACT_CONTRACT_V2 = "selection_score_artifact_v2"
SELECTION_ARTIFACT_V2_PROVIDER_SEMANTICS = "strategy_package_live_inference_v2"


@dataclass(frozen=True)
class SelectionArtifactV2Provenance:
    """Canonical raw-inference facts that are independent of Advisory capture."""

    universe_count: int
    artifact_input_context: dict[str, Any]
    source_read_receipts: list[dict[str, Any]]
    asset_closure: list[dict[str, Any]]
    asset_closure_status: str
    reason_codes: list[str]
    artifact_input_context_hash: str
    source_revision_set_hash: str
    asset_closure_hash: str
    provider_semantics_id: str
    provider_semantics_hash: str


def _is_sha256_hex(value: str | None) -> bool:
    return bool(value) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _callable_accepts_keyword(func: Callable[..., Any], keyword: str) -> bool:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return True
    return keyword in signature.parameters


def _canonical_json_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _required_v2_hash(value: Any, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _is_sha256_hex(normalized):
        raise DataUnavailableError(
            "live inference provenance contains an invalid SHA256 field",
            context={
                "reason_code": REASON_SOURCE_RECEIPT_INCOMPLETE,
                "field": field_name,
                "value": str(value)[:80],
            },
        )
    return normalized


def _date_text(value: Any, *, field_name: str) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    normalized = str(value or "").strip()
    try:
        return date.fromisoformat(normalized[:10]).isoformat()
    except ValueError as exc:
        raise DataUnavailableError(
            "live inference provenance contains an invalid trade-date field",
            context={"reason_code": REASON_SOURCE_RECEIPT_INCOMPLETE, "field": field_name, "value": normalized},
        ) from exc


def _canonical_source_receipts(receipts: Any) -> list[dict[str, Any]]:
    if not isinstance(receipts, Sequence) or isinstance(receipts, (str, bytes)):
        raise DataUnavailableError(
            "live inference did not provide source read receipts",
            context={"reason_code": REASON_SOURCE_RECEIPT_INCOMPLETE},
        )
    parsed: list[dict[str, Any]] = []
    for index, raw in enumerate(receipts):
        try:
            receipt = SourceReadReceipt.model_validate(raw)
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "live inference source read receipt is invalid",
                context={"reason_code": REASON_SOURCE_RECEIPT_INCOMPLETE, "receipt_index": index, "error": str(exc)},
            ) from exc
        parsed.append(receipt.model_dump(mode="json"))
    if not parsed:
        raise DataUnavailableError(
            "live inference did not provide source read receipts",
            context={"reason_code": REASON_SOURCE_RECEIPT_INCOMPLETE},
        )
    required_roles = {"pit_universe", "market_history", "fundamental_moneyflow", "trading_calendar"}
    present_roles = {str(item["source_role"]) for item in parsed}
    missing_roles = sorted(required_roles - present_roles)
    if missing_roles:
        raise DataUnavailableError(
            "live inference source read receipts are incomplete",
            context={"reason_code": REASON_SOURCE_RECEIPT_INCOMPLETE, "missing_source_roles": missing_roles},
        )
    return sorted(
        parsed,
        key=lambda item: (
            str(item["source_role"]),
            str(item["dataset_id"]),
            str(item.get("partition_ref") or ""),
            str(item.get("leg_id") or ""),
            str(item.get("content_hash") or ""),
        ),
    )


def _live_input_context(
    *,
    result: Any,
    requested_trade_date: date,
    cutoff_date: date | None,
    include_reference_price: bool,
) -> dict[str, Any]:
    raw_context = getattr(result, "input_context", None)
    if not isinstance(raw_context, Mapping):
        raise DataUnavailableError(
            "live inference did not provide an input context",
            context={"reason_code": REASON_SOURCE_RECEIPT_INCOMPLETE, "field": "input_context"},
        )
    provided_requested = raw_context.get("requested_trade_date")
    if provided_requested is not None and _date_text(provided_requested, field_name="requested_trade_date") != requested_trade_date.isoformat():
        raise DataUnavailableError(
            "live inference input context does not match requested trade date",
            context={
                "reason_code": REASON_SOURCE_RECEIPT_INCOMPLETE,
                "requested_trade_date": requested_trade_date.isoformat(),
                "provider_requested_trade_date": str(provided_requested),
            },
        )
    effective_trade_date = _date_text(raw_context.get("effective_trade_date"), field_name="effective_trade_date")
    score_trade_date = _date_text(
        raw_context.get("score_trade_date") or effective_trade_date,
        field_name="score_trade_date",
    )
    pit_mode = str(raw_context.get("pit_mode") or "").strip()
    calendar_version = str(raw_context.get("calendar_version") or "").strip()
    calendar_source = str(raw_context.get("calendar_source") or "").strip()
    if not pit_mode or not calendar_version or not calendar_source:
        raise DataUnavailableError(
            "live inference input context is incomplete",
            context={
                "reason_code": REASON_SOURCE_RECEIPT_INCOMPLETE,
                "missing_fields": [
                    name
                    for name, value in (
                        ("pit_mode", pit_mode),
                        ("calendar_version", calendar_version),
                        ("calendar_source", calendar_source),
                    )
                    if not value
                ],
            },
        )
    return {
        "requested_trade_date": requested_trade_date.isoformat(),
        "effective_trade_date": effective_trade_date,
        "cutoff_date": cutoff_date.isoformat() if cutoff_date else None,
        "score_trade_date": score_trade_date,
        "reference_price_trade_date": score_trade_date if include_reference_price else None,
        "pit_mode": pit_mode,
        "calendar_version": calendar_version,
        "calendar_hash": _required_v2_hash(raw_context.get("calendar_hash"), field_name="calendar_hash"),
        "calendar_source": calendar_source,
        "universe_input_hash": _required_v2_hash(raw_context.get("universe_input_hash"), field_name="universe_input_hash"),
    }


def build_manifest_asset_closure(
    manifest: Any,
    *,
    observed_at: datetime | None = None,
    extra_entries: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], str, list[str]]:
    """Freeze manifest-owned assets without putting local workspace paths in the closure."""

    seen_at = observed_at or datetime.now(timezone.utc)
    if seen_at.tzinfo is None:
        raise ValueError("asset closure observed_at must be timezone-aware")
    entries: list[dict[str, Any]] = [
        {
            "asset_role": "strategy_package_manifest",
            "asset_id": str(getattr(manifest, "package_id", "")),
            "asset_ref": None,
            "sha256": str(getattr(manifest, "manifest_sha256", "") or "").lower() or None,
            "first_observed_at": seen_at.isoformat(),
            "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
        }
    ]
    incomplete = not _is_sha256_hex(entries[0]["sha256"])
    model_assets = getattr(manifest, "model_asset", [])
    if not isinstance(model_assets, list):
        model_assets = [model_assets]
    for model in model_assets:
        model_id = str(getattr(model, "model_id", "") or "")
        asset_ref = getattr(model, "asset_ref", None)
        sha256 = str(getattr(model, "sha256", "") or "").lower() or None
        entries.append(
            {
                "asset_role": "model_weight",
                "asset_id": model_id,
                "asset_ref": asset_ref,
                "sha256": sha256,
                "first_observed_at": seen_at.isoformat(),
                "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
            }
        )
        incomplete = incomplete or not bool(asset_ref) or not _is_sha256_hex(sha256)
        for code_asset in getattr(model, "model_code_assets", []) or []:
            code_ref = getattr(code_asset, "asset_ref", None)
            code_sha = str(getattr(code_asset, "sha256", "") or "").lower() or None
            entries.append(
                {
                    "asset_role": "model_code",
                    "asset_id": str(getattr(code_asset, "module_name", "") or getattr(code_asset, "relative_path", "")),
                    "asset_ref": code_ref,
                    "sha256": code_sha,
                    "first_observed_at": seen_at.isoformat(),
                    "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
                }
            )
            incomplete = incomplete or not bool(code_ref) or not _is_sha256_hex(code_sha)
        if bool(getattr(model, "model_code_required", False)) and not list(getattr(model, "model_code_assets", []) or []):
            incomplete = True

    for factor in getattr(manifest, "factor_set", []) or []:
        asset_ref = getattr(factor, "asset_ref", None)
        sha256 = str(getattr(factor, "sha256", "") or "").lower() or None
        required = bool(getattr(factor, "required", True))
        entries.append(
            {
                "asset_role": "factor_code",
                "asset_id": str(getattr(factor, "factor_id", "") or getattr(factor, "factor_name", "")),
                "asset_ref": asset_ref,
                "sha256": sha256,
                "required": required,
                "first_observed_at": seen_at.isoformat(),
                "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
            }
        )
        if required and (not asset_ref or not _is_sha256_hex(sha256)):
            incomplete = True

    runtime_assets = getattr(manifest, "runtime_assets", None)
    alpha158 = getattr(runtime_assets, "alpha158", None) if runtime_assets is not None else None
    if alpha158 is not None and bool(getattr(alpha158, "enabled", False)):
        asset_ref = getattr(alpha158, "asset_ref", None)
        sha256 = str(getattr(alpha158, "sha256", "") or "").lower() or None
        entries.append(
            {
                "asset_role": "alpha158_schema",
                "asset_id": "alpha158_schema",
                "asset_ref": asset_ref,
                "sha256": sha256,
                "first_observed_at": seen_at.isoformat(),
                "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
            }
        )
        incomplete = incomplete or not bool(asset_ref) or not _is_sha256_hex(sha256)

    for raw in extra_entries or []:
        entries.append(dict(raw))
    ordered = sorted(
        entries,
        key=lambda item: (
            str(item.get("asset_role") or ""),
            str(item.get("asset_id") or ""),
            str(item.get("asset_ref") or ""),
        ),
    )
    return ordered, ("INCOMPLETE" if incomplete else "COMPLETE"), ([REASON_ASSET_CLOSURE_INCOMPLETE] if incomplete else [])


def build_selection_artifact_v2_provenance(
    *,
    result: Any,
    requested_trade_date: date,
    cutoff_date: date | None,
    include_reference_price: bool,
    asset_closure: Sequence[Mapping[str, Any]],
    asset_closure_status: str,
    asset_reason_codes: Sequence[str],
    provider_semantics: Mapping[str, Any],
    additional_source_receipts: Sequence[Mapping[str, Any]] | None = None,
) -> SelectionArtifactV2Provenance:
    """Build v2 hashes from one completed provider result without a second read."""

    raw_universe_count = getattr(result, "universe_count", None)
    if isinstance(raw_universe_count, bool):
        raw_universe_count = None
    try:
        universe_count = int(raw_universe_count)
    except (TypeError, ValueError) as exc:
        raise DataUnavailableError(
            "live inference did not provide an actual input universe count",
            context={"reason_code": REASON_UNIVERSE_RECEIPT_INCOMPLETE},
        ) from exc
    score_row_count = len(getattr(result, "scores", []) or [])
    if universe_count < score_row_count:
        raise DataUnavailableError(
            "live inference universe count is smaller than scored rows",
            context={
                "reason_code": REASON_UNIVERSE_RECEIPT_INCOMPLETE,
                "universe_count": universe_count,
                "score_row_count": score_row_count,
            },
        )
    receipts = _canonical_source_receipts(
        [*(getattr(result, "source_read_receipts", None) or []), *(additional_source_receipts or [])]
    )
    input_context = _live_input_context(
        result=result,
        requested_trade_date=requested_trade_date,
        cutoff_date=cutoff_date,
        include_reference_price=include_reference_price,
    )
    semantics = dict(provider_semantics)
    semantics_id = str(semantics.get("provider_semantics_id") or "").strip()
    if not semantics_id:
        raise ValueError("provider_semantics_id is required")
    return SelectionArtifactV2Provenance(
        universe_count=universe_count,
        artifact_input_context=input_context,
        source_read_receipts=receipts,
        asset_closure=[dict(item) for item in asset_closure],
        asset_closure_status=str(asset_closure_status),
        reason_codes=sorted(set(asset_reason_codes)),
        artifact_input_context_hash=canonical_evidence_json_sha256(input_context),
        source_revision_set_hash=canonical_evidence_json_sha256(_semantic_source_receipt_entries(receipts)),
        asset_closure_hash=canonical_evidence_json_sha256(_semantic_asset_closure_entries(asset_closure)),
        provider_semantics_id=semantics_id,
        provider_semantics_hash=canonical_evidence_json_sha256(semantics),
    )


def _semantic_asset_closure_entries(entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Exclude observation timestamps from the immutable asset identity hash.

    ``first_observed_at`` records when this producer saw an asset and remains in
    metadata for Phase 1 maturity analysis. It is not an asset revision and
    cannot make an otherwise exact idempotent retry conflict.
    """

    return [
        {key: value for key, value in dict(entry).items() if key != "first_observed_at"}
        for entry in entries
    ]


def _semantic_source_receipt_entries(entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Exclude local observation timestamps from an immutable source revision identity.

    A source revision is defined by its dataset/query/content/availability
    identity. ``first_observed_at`` is retained in metadata for later source
    maturity analysis, but a retry must not become a conflicting artifact only
    because this producer observed the same immutable source again.
    """

    return [
        {key: value for key, value in dict(entry).items() if key != "first_observed_at"}
        for entry in entries
    ]


def build_reference_price_source_receipt(
    *,
    symbols: Sequence[str],
    trade_date: date,
    price_by_symbol: Mapping[str, float],
) -> dict[str, Any]:
    """Record the reference-price query already used while enriching raw scores."""

    observed_at = datetime.now(timezone.utc)
    normalized_symbols = sorted({str(item) for item in symbols})
    normalized_prices = {str(symbol): float(price) for symbol, price in sorted(price_by_symbol.items())}
    return SourceReadReceipt(
        source_role="reference_price",
        dataset_id="market.kline_daily_raw",
        partition_ref=trade_date.isoformat(),
        query_template_id="SelectionArtifact.reference_price",
        query_template_version="v1",
        parameter_hash=canonical_evidence_json_sha256(
            {"trade_date": trade_date.isoformat(), "symbols": normalized_symbols}
        ),
        row_count=len(normalized_prices),
        content_hash=canonical_evidence_json_sha256(normalized_prices),
        first_observed_at=observed_at,
        admissibility="PROSPECTIVE_FIRST_OBSERVED",
    ).model_dump(mode="json")


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
        raise RuntimeConfigInvalidError(
            "runtime_config.selection_artifact_config must be an object",
            context={"selection_artifact_config_type": type(payload).__name__},
        )
    payload = dict(payload)
    # Orchestration switches decide whether to generate/reuse an artifact, but
    # they do not change model scores and must not fragment the artifact key.
    payload.pop("auto_generate", None)
    payload.pop("force_regenerate", None)
    payload.pop("signal_data_source", None)
    return _canonical_json_sha256(payload)


def selection_artifact_runtime_hash_v2(runtime_config: dict[str, Any] | None = None) -> str:
    """Return the v2 raw-artifact key without changing candidate semantics.

    The contract marker intentionally separates immutable v2 rows from legacy
    rows with the same scoring configuration. Capture mode and Advisory
    context are not inputs to this hash.
    """

    config = runtime_config or {}
    payload = config.get("selection_artifact_config")
    if payload is None:
        payload = config.get("selection_artifact")
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise RuntimeConfigInvalidError(
            "runtime_config.selection_artifact_config must be an object",
            context={"selection_artifact_config_type": type(payload).__name__},
        )
    normalized = dict(payload)
    normalized.pop("auto_generate", None)
    normalized.pop("force_regenerate", None)
    normalized.pop("signal_data_source", None)
    normalized["artifact_contract_version"] = SELECTION_SCORE_ARTIFACT_CONTRACT_V2
    return _canonical_json_sha256(normalized)


def selection_artifact_runtime_hash_v2_for_manifest(manifest: Any, runtime_config: dict[str, Any] | None = None) -> str:
    """Return the v2 raw-artifact key matching the package's actual alpha mode."""

    if getattr(manifest, "alpha_mode", None) == AlphaMode.MULTI_ALPHA:
        from .multi_alpha_live import multi_alpha_selection_artifact_runtime_hash_v2

        return multi_alpha_selection_artifact_runtime_hash_v2(manifest, runtime_config)
    return selection_artifact_runtime_hash_v2(runtime_config)


def selection_artifact_runtime_hash_for_manifest(manifest: Any, runtime_config: dict[str, Any] | None = None) -> str:
    """Return the legacy lookup key only for previously persisted operational rows."""

    if getattr(manifest, "alpha_mode", None) == AlphaMode.MULTI_ALPHA:
        from .multi_alpha_live import multi_alpha_selection_artifact_runtime_hash

        return multi_alpha_selection_artifact_runtime_hash(manifest, runtime_config)
    return selection_artifact_runtime_hash(runtime_config)


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
    artifact_contract_version: str | None = None
    artifact_payload_sha256: str | None = None
    artifact_input_context_hash: str | None = None
    source_revision_set_hash: str | None = None
    asset_closure_hash: str | None = None
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

    @model_validator(mode="after")
    def _v2_contract_fields_are_complete(self) -> "SelectionScoreArtifact":
        if self.score_count != len(self.scores_json):
            raise ValueError("score_count must match scores_json")
        if self.universe_count < self.score_count:
            raise ValueError("universe_count cannot be smaller than score_count")
        if self.artifact_contract_version is None:
            if any(
                value is not None
                for value in (
                    self.artifact_payload_sha256,
                    self.artifact_input_context_hash,
                    self.source_revision_set_hash,
                    self.asset_closure_hash,
                )
            ):
                raise ValueError("v2 artifact identity fields require artifact_contract_version")
            return self
        if self.artifact_contract_version != SELECTION_SCORE_ARTIFACT_CONTRACT_V2:
            raise ValueError("unsupported selection score artifact contract version")
        missing = [
            name
            for name, value in (
                ("artifact_input_context_hash", self.artifact_input_context_hash),
                ("source_revision_set_hash", self.source_revision_set_hash),
                ("asset_closure_hash", self.asset_closure_hash),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"v2 artifact is missing required fields: {missing}")
        metadata = self.metadata or {}
        required_metadata = (
            "authority_scope",
            "candidate_outcome",
            "provider_semantics_id",
            "provider_semantics_hash",
        )
        missing_metadata = [name for name in required_metadata if not str(metadata.get(name) or "").strip()]
        if missing_metadata:
            raise ValueError(f"v2 artifact is missing semantic metadata: {missing_metadata}")
        candidate_outcome = str(metadata.get("candidate_outcome") or "").strip()
        if candidate_outcome not in {"CANDIDATES_PRESENT", "VALID_NO_CANDIDATE"}:
            raise ValueError("v2 artifact candidate_outcome is unsupported")
        if candidate_outcome == "CANDIDATES_PRESENT" and self.score_count == 0:
            raise ValueError("CANDIDATES_PRESENT v2 artifact requires score rows")
        if candidate_outcome == "VALID_NO_CANDIDATE":
            if self.status != SelectionScoreArtifactStatus.SUCCEEDED:
                raise ValueError("VALID_NO_CANDIDATE v2 artifact requires SUCCEEDED status")
            if self.score_count != 0 or self.scores_json:
                raise ValueError("VALID_NO_CANDIDATE v2 artifact cannot contain score rows")
            if self.universe_count <= 0:
                raise ValueError("VALID_NO_CANDIDATE v2 artifact requires a positive input universe")
            if metadata.get("empty_stage") != "alpha_raw":
                raise ValueError("VALID_NO_CANDIDATE v2 artifact requires empty_stage=alpha_raw")
            if self.top_score_symbol is not None:
                raise ValueError("VALID_NO_CANDIDATE v2 artifact cannot have top_score_symbol")
        hash_fields = {
            "manifest_sha256": self.manifest_sha256,
            "runtime_config_hash": self.runtime_config_hash,
            "artifact_input_context_hash": self.artifact_input_context_hash,
            "source_revision_set_hash": self.source_revision_set_hash,
            "asset_closure_hash": self.asset_closure_hash,
            "provider_semantics_hash": metadata.get("provider_semantics_hash"),
        }
        if self.artifact_sha256 is not None:
            hash_fields["artifact_sha256"] = self.artifact_sha256
        if self.artifact_payload_sha256 is not None:
            hash_fields["artifact_payload_sha256"] = self.artifact_payload_sha256
        if metadata.get("multi_alpha_parent_parity_hash") is not None:
            hash_fields["multi_alpha_parent_parity_hash"] = metadata["multi_alpha_parent_parity_hash"]
        invalid_hashes = [name for name, value in hash_fields.items() if not _is_sha256_hex(str(value or ""))]
        if invalid_hashes:
            raise ValueError(f"v2 artifact has invalid SHA256 fields: {invalid_hashes}")
        return self

    def canonical_v2_header(self, *, score_hash: str | None = None) -> dict[str, Any]:
        if self.artifact_contract_version != SELECTION_SCORE_ARTIFACT_CONTRACT_V2:
            raise ValueError("canonical v2 header requires selection_score_artifact_v2")
        return {
            "schema_version": SELECTION_SCORE_ARTIFACT_CONTRACT_V2,
            "package_id": self.package_id,
            "manifest_sha256": self.manifest_sha256,
            "trade_date": self.trade_date,
            "data_source": self.data_source,
            "runtime_config_hash": self.runtime_config_hash,
            "artifact_sha256": score_hash or self.artifact_sha256 or _canonical_json_sha256(self.scores_json),
            "score_count": self.score_count,
            "universe_count": self.universe_count,
            "top_score_symbol": self.top_score_symbol,
            "status": self.status.value,
            "authority_scope": (self.metadata or {}).get("authority_scope"),
            "candidate_outcome": (self.metadata or {}).get("candidate_outcome"),
            "artifact_input_context_hash": self.artifact_input_context_hash,
            "source_revision_set_hash": self.source_revision_set_hash,
            "asset_closure_hash": self.asset_closure_hash,
            "provider_semantics_id": (self.metadata or {}).get("provider_semantics_id"),
            "provider_semantics_hash": (self.metadata or {}).get("provider_semantics_hash"),
            "multi_alpha_parent_parity_hash": (self.metadata or {}).get("multi_alpha_parent_parity_hash"),
        }


class StrategyPackageSelectionArtifactRepository:
    """PostgreSQL-backed repository for selection score artifacts."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def save(self, artifact: SelectionScoreArtifact) -> SelectionScoreArtifact:
        artifact = self._with_digest(artifact)
        if artifact.artifact_contract_version == SELECTION_SCORE_ARTIFACT_CONTRACT_V2:
            return self._save_v2(artifact)
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

    def _save_v2(self, artifact: SelectionScoreArtifact) -> SelectionScoreArtifact:
        """Insert immutable v2 rows or prove an exact retry is identical."""

        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.selection_score_artifact (
                        artifact_id, package_id, manifest_sha256, trade_date,
                        data_source, runtime_config_hash, scores_json,
                        artifact_sha256, score_count, universe_count,
                        top_score_symbol, status, error_json, metadata,
                        artifact_contract_version, artifact_payload_sha256,
                        artifact_input_context_hash, source_revision_set_hash,
                        asset_closure_hash, created_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    self._insert_params(artifact),
                )
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
                    (
                        artifact.package_id,
                        artifact.manifest_sha256,
                        artifact.trade_date,
                        artifact.data_source,
                        artifact.runtime_config_hash,
                    ),
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute(
                        "SELECT * FROM strategy_pkg.selection_score_artifact WHERE artifact_payload_sha256 = %s",
                        (artifact.artifact_payload_sha256,),
                    )
                    payload_row = cur.fetchone()
                    if payload_row is not None:
                        raise InvalidStateTransitionError(
                            "selection score artifact payload hash conflicts with another business identity",
                            context={
                                "reason_code": "ADVISORY_PHASE0A2C_ARTIFACT_IDEMPOTENCY_CONFLICT",
                                "artifact_payload_sha256": artifact.artifact_payload_sha256,
                            },
                        )
                    raise InvalidStateTransitionError(
                        "selection score artifact insert did not produce a readable row",
                        context={"reason_code": "ADVISORY_PHASE0A2C_ARTIFACT_IDEMPOTENCY_CONFLICT"},
                    )
                stored = self._from_row(dict(row))
                self._assert_same_v2(stored, artifact)
                return stored

    @staticmethod
    def _insert_params(artifact: SelectionScoreArtifact) -> tuple[Any, ...]:
        return (
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
            artifact.artifact_contract_version,
            artifact.artifact_payload_sha256,
            artifact.artifact_input_context_hash,
            artifact.source_revision_set_hash,
            artifact.asset_closure_hash,
            artifact.created_at,
        )

    @staticmethod
    def _assert_same_v2(stored: SelectionScoreArtifact, requested: SelectionScoreArtifact) -> None:
        if (
            stored.artifact_contract_version != SELECTION_SCORE_ARTIFACT_CONTRACT_V2
            or stored.artifact_payload_sha256 != requested.artifact_payload_sha256
            or canonical_evidence_json_sha256(stored.scores_json) != canonical_evidence_json_sha256(requested.scores_json)
            or canonical_evidence_json_sha256(stored.canonical_v2_header())
            != canonical_evidence_json_sha256(requested.canonical_v2_header())
        ):
            raise InvalidStateTransitionError(
                "selection score artifact conflicts with immutable v2 identity",
                context={
                    "reason_code": "ADVISORY_PHASE0A2C_ARTIFACT_IDEMPOTENCY_CONFLICT",
                    "package_id": requested.package_id,
                    "manifest_sha256": requested.manifest_sha256,
                    "trade_date": requested.trade_date.isoformat(),
                    "runtime_config_hash": requested.runtime_config_hash,
                },
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
            raise RuntimeConfigInvalidError("limit must be positive")
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
        update: dict[str, Any] = {"artifact_sha256": digest}
        if artifact.artifact_contract_version == SELECTION_SCORE_ARTIFACT_CONTRACT_V2:
            payload_hash = canonical_evidence_json_sha256(artifact.canonical_v2_header(score_hash=digest))
            if artifact.artifact_payload_sha256 and artifact.artifact_payload_sha256 != payload_hash:
                raise ValueError("artifact_payload_sha256 does not match canonical v2 header")
            update["artifact_payload_sha256"] = payload_hash
        return artifact.model_copy(update=update)

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
            artifact_contract_version=row.get("artifact_contract_version"),
            artifact_payload_sha256=row.get("artifact_payload_sha256"),
            artifact_input_context_hash=row.get("artifact_input_context_hash"),
            source_revision_set_hash=row.get("source_revision_set_hash"),
            asset_closure_hash=row.get("asset_closure_hash"),
            created_at=row["created_at"],
        )


class InMemorySelectionScoreArtifactRepository:
    def __init__(self) -> None:
        self.artifacts: dict[tuple[str, str, date, str, str], SelectionScoreArtifact] = {}

    def save(self, artifact: SelectionScoreArtifact) -> SelectionScoreArtifact:
        stored = StrategyPackageSelectionArtifactRepository._with_digest(artifact)
        key = (
            stored.package_id,
            stored.manifest_sha256,
            stored.trade_date,
            stored.data_source,
            stored.runtime_config_hash,
        )
        existing = self.artifacts.get(key)
        if stored.artifact_contract_version == SELECTION_SCORE_ARTIFACT_CONTRACT_V2 and existing is not None:
            StrategyPackageSelectionArtifactRepository._assert_same_v2(existing, stored)
            return existing
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
            raise RuntimeConfigInvalidError("live selection artifact generation requires trade_dates")
        self._validate_v2_generation_mode(runtime_config)
        unique_dates = sorted(set(trade_dates))
        record = self.package_repository.get(package_id)
        manifest = record.current_manifest()
        if not manifest.manifest_sha256:
            raise PackageAssetInvalidError(
                "strategy package manifest must be frozen before generating live selection artifacts",
                context={"package_id": package_id},
            )
        if manifest.alpha_mode == AlphaMode.MULTI_ALPHA:
            provider, inference_backend = self._resolve_live_provider(runtime_config)
            from .multi_alpha_live import MultiAlphaLivePredictionProvider

            return MultiAlphaLivePredictionProvider(
                package_repository=self.package_repository,
                artifact_repository=self.artifact_repository,
                runtime_asset_resolver=self.runtime_asset_resolver,
                live_inference_provider=provider,
                reference_price_loader=self._load_reference_prices,
            ).generate_artifacts(
                package_id=package_id,
                trade_dates=unique_dates,
                data_source=data_source,
                runtime_config=runtime_config,
                include_reference_price=include_reference_price,
                cutoff_date=cutoff_date,
                inference_backend=inference_backend,
            )
        runtime_hash = selection_artifact_runtime_hash_v2(runtime_config)
        topk = self._runtime_top_k(manifest, runtime_config)
        source_loader = getattr(self.runtime_asset_resolver, "load_source_for_strategy_package", None)
        if callable(source_loader):
            source_kwargs: dict[str, Any] = {
                "source_type": record.source_type,
                "source_id": record.source_id,
                "loop_id": record.loop_id,
                "run_id": record.run_id,
            }
            if _callable_accepts_keyword(source_loader, "manifest"):
                source_kwargs["manifest"] = manifest
                source_kwargs["package_id"] = record.package_id
            source = source_loader(**source_kwargs)
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
                topk=topk,
                include_reference_price=include_reference_price,
                universe_count=result.universe_count,
            )
            reference_price_by_symbol = {
                str(row["symbol"]): float(row["reference_price"])
                for row in scores
                if row.get("reference_price") is not None
            }
            asset_closure, asset_closure_status, asset_reason_codes = build_manifest_asset_closure(manifest)
            provider_semantics = {
                "provider_semantics_id": SELECTION_ARTIFACT_V2_PROVIDER_SEMANTICS,
                "provider_version": "v2",
                "inference_backend": inference_backend,
                "inference_provider": f"{type(provider).__module__}.{type(provider).__qualname__}",
                "source_type": record.source_type,
                "source_id": record.source_id,
                "source_experiment_id": getattr(source, "experiment_id", None),
                "qe_task_id": getattr(source, "qe_task_id", None),
                "qe_loop_id": getattr(source, "qe_loop_id", None),
                "model_params_origin": getattr(prepared, "model_params_origin", None),
                "score_order": "score_desc_symbol_asc",
                "strict_inference": True,
            }
            provenance = build_selection_artifact_v2_provenance(
                result=result,
                requested_trade_date=current_date,
                cutoff_date=cutoff_date,
                include_reference_price=include_reference_price,
                asset_closure=asset_closure,
                asset_closure_status=asset_closure_status,
                asset_reason_codes=asset_reason_codes,
                provider_semantics=provider_semantics,
                additional_source_receipts=(
                    [
                        build_reference_price_source_receipt(
                            symbols=[str(row["symbol"]) for row in scores],
                            trade_date=score_trade_date,
                            price_by_symbol=reference_price_by_symbol,
                        )
                    ]
                    if include_reference_price
                    else []
                ),
            )
            artifact = SelectionScoreArtifact(
                package_id=package_id,
                manifest_sha256=manifest.manifest_sha256,
                trade_date=current_date,
                data_source=data_source,
                runtime_config_hash=runtime_hash,
                scores_json=scores,
                score_count=len(scores),
                universe_count=provenance.universe_count,
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
                    "topk": topk,
                    "candidate_outcome": "CANDIDATES_PRESENT" if scores else "VALID_NO_CANDIDATE",
                    "empty_stage": "alpha_raw" if not scores else None,
                    "provider_semantics_id": provenance.provider_semantics_id,
                    "provider_semantics_hash": provenance.provider_semantics_hash,
                    "provider_semantics": provider_semantics,
                    "artifact_input_context": provenance.artifact_input_context,
                    "source_read_receipts": provenance.source_read_receipts,
                    "asset_closure": provenance.asset_closure,
                    "asset_closure_status": provenance.asset_closure_status,
                    "capture_prerequisite_reason_codes": provenance.reason_codes,
                    "trade_date_requested": current_date.isoformat(),
                    "cutoff_date": cutoff_date.isoformat() if cutoff_date else None,
                    "score_trade_date": score_trade_date.isoformat(),
                    "reference_price_trade_date": score_trade_date.isoformat() if include_reference_price else None,
                    "provider_metadata": result.metadata,
                },
                artifact_contract_version=SELECTION_SCORE_ARTIFACT_CONTRACT_V2,
                artifact_input_context_hash=provenance.artifact_input_context_hash,
                source_revision_set_hash=provenance.source_revision_set_hash,
                asset_closure_hash=provenance.asset_closure_hash,
            )
            artifacts.append(self.artifact_repository.save(artifact))
        return artifacts

    @staticmethod
    def _validate_v2_generation_mode(runtime_config: dict[str, Any] | None) -> None:
        """Prevent force-regenerate from overwriting an immutable authoritative v2 key."""

        config = runtime_config or {}
        artifact_config = config.get("selection_artifact_config")
        if artifact_config is None:
            artifact_config = config.get("selection_artifact")
        if artifact_config is None:
            return
        if not isinstance(artifact_config, Mapping):
            raise RuntimeConfigInvalidError("selection_artifact_config must be an object")
        if not bool(artifact_config.get("force_regenerate")):
            return
        execution_origin = str(artifact_config.get("execution_origin") or "").strip().upper()
        diagnostic_key = str(artifact_config.get("diagnostic_run_id") or "").strip()
        if execution_origin not in {"PREVIEW", "REPLAY"} or not diagnostic_key:
            raise RuntimeConfigInvalidError(
                "force_regenerate is forbidden for authoritative v2 selection artifacts; "
                "PREVIEW/REPLAY requires execution_origin and diagnostic_run_id",
                context={
                    "reason_code": "ADVISORY_PHASE0A2C_ARTIFACT_IDEMPOTENCY_CONFLICT",
                    "execution_origin": execution_origin or None,
                    "diagnostic_run_id": diagnostic_key or None,
                },
            )

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
            raise RuntimeConfigInvalidError("selection artifact generation requires trade_dates")
        unique_dates = sorted(set(trade_dates))
        record = self.package_repository.get(package_id)
        manifest = record.current_manifest()
        if not manifest.manifest_sha256:
            raise PackageAssetInvalidError(
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
        topk = self._runtime_top_k(manifest, runtime_config)
        artifacts: list[SelectionScoreArtifact] = []
        for current_date in unique_dates:
            scores = self._scores_for_date(
                pred,
                package_id=package_id,
                manifest_sha256=manifest.manifest_sha256,
                trade_date=current_date,
                topk=topk,
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
                    "topk": topk,
                },
            )
            artifacts.append(self.artifact_repository.save(artifact))
        return artifacts

    @staticmethod
    def _runtime_top_k(manifest: Any, runtime_config: dict[str, Any] | None) -> int:
        profile = parse_selection_runtime_profile(runtime_config or {})
        if profile.selection.top_k is not None:
            return int(profile.selection.top_k)
        daily_strategy = (manifest.backtest_context or {}).get("daily_strategy")
        if isinstance(daily_strategy, dict) and daily_strategy.get("topk") is not None:
            return int(daily_strategy["topk"])
        if getattr(manifest, "is_legacy_runtime_manifest", False) and manifest.portfolio_policy is not None:
            return int(manifest.portfolio_policy.topk)
        raise RuntimeConfigInvalidError(
            "selection artifact generation requires runtime_profile.selection.top_k; StrategyPackage manifest cannot provide runtime top_k",
            context={"package_id": manifest.package_id, "manifest_version": getattr(manifest, "manifest_version", None)},
        )

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
            raise ArtifactGenerationFailedError(
                "QE prediction artifact must be a pandas DataFrame or Series",
                context={"prediction_path": str(path), "artifact_type": type(pred).__name__},
            )
        if "score" not in pred.columns:
            if len(pred.columns) == 1:
                pred = pred.rename(columns={pred.columns[0]: "score"})
            else:
                raise ArtifactGenerationFailedError(
                    "QE prediction artifact is missing score column",
                    context={"prediction_path": str(path), "columns": [str(col) for col in pred.columns]},
                )
        if not isinstance(pred.index, pd.MultiIndex):
            raise ArtifactGenerationFailedError(
                "QE prediction artifact index must be MultiIndex(datetime, instrument)",
                context={"prediction_path": str(path), "index_type": type(pred.index).__name__},
            )
        names = list(pred.index.names)
        if "datetime" not in names or "instrument" not in names:
            raise ArtifactGenerationFailedError(
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
            raise ArtifactGenerationFailedError(
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
            raise RuntimeConfigInvalidError("selection_artifact_config must be an object")
        backend = str(
            (artifact_config or {}).get("inference_backend")
            or os.getenv("STRATEGY_PACKAGE_SELECTION_INFERENCE_BACKEND")
            or ("wsl" if os.name == "nt" else "local")
        ).strip().lower()
        if backend == "wsl":
            return WslStrategyPackageInferenceProvider(), "wsl"
        if backend == "local":
            return LocalStrategyPackageInferenceProvider(), "local"
        raise RuntimeConfigInvalidError(
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
        universe_count: int | None,
    ) -> list[dict[str, Any]]:
        if rows is None:
            raise DataUnavailableError(
                "live inference did not return a score payload",
                context={
                    "package_id": package_id,
                    "trade_date": trade_date.isoformat(),
                    "reason_code": REASON_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE,
                },
            )
        if not isinstance(rows, list):
            raise ArtifactGenerationFailedError(
                "live inference score payload must be a list",
                context={"package_id": package_id, "score_payload_type": type(rows).__name__},
            )
        if not rows:
            if isinstance(universe_count, bool) or not isinstance(universe_count, int) or universe_count <= 0:
                raise DataUnavailableError(
                    "empty live inference scores require a positive actual input universe",
                    context={
                        "package_id": package_id,
                        "trade_date": trade_date.isoformat(),
                        "universe_count": universe_count,
                        "reason_code": REASON_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE,
                    },
                )
            return []
        normalized: list[dict[str, Any]] = []
        for row in rows:
            missing = [key for key in ("symbol", "score", "rank") if row.get(key) is None]
            if missing:
                raise ArtifactGenerationFailedError(
                    "live inference score row is missing required fields",
                    context={"package_id": package_id, "missing": missing, "row": row},
                )
            score = float(row["score"])
            if not math.isfinite(score):
                raise ArtifactGenerationFailedError(
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
        ts_codes = normalize_and_validate_ts_codes(
            symbols,
            source="StrategyPackageSelectionArtifactService._load_reference_prices",
            start_date=trade_date,
            end_date=trade_date,
        )
        if not ts_codes:
            return {}

        def _load_reference_chunk(chunk: list[str], _chunk_index: int, correlation_id: str) -> list[tuple[Any, Any]]:
            try:
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
                            (trade_date, chunk),
                        )
                        return list(cur.fetchall())
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "selection artifact reference price chunk failed: correlation_id=%s symbols=%s trade_date=%s error=%s",
                    correlation_id,
                    len(chunk),
                    trade_date,
                    exc,
                )
                raise

        row_chunks = load_chunks_with_logging(
            ts_codes=ts_codes,
            source="SelectionArtifact.reference_price",
            start_date=trade_date,
            end_date=trade_date,
            chunk_size=DEFAULT_SQL_CHUNK_SIZE,
            loader=_load_reference_chunk,
        )
        rows = [row for chunk in row_chunks for row in chunk]
        return {str(symbol): float(close_li) / 1000.0 for symbol, close_li in rows}
