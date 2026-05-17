"""Typed records and deterministic hashing helpers for the QE archive."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import PurePath
from typing import Any, Mapping, MutableMapping, Sequence
from uuid import UUID, uuid4


JsonMap = dict[str, Any]


def normalize_json(value: Any) -> Any:
    """Normalize common Python values into deterministic JSON-compatible data."""

    if is_dataclass(value):
        return normalize_json(asdict(value))
    if isinstance(value, Mapping):
        return {str(k): normalize_json(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, tuple):
        return [normalize_json(v) for v in value]
    if isinstance(value, list):
        return [normalize_json(v) for v in value]
    if isinstance(value, set):
        return sorted(normalize_json(v) for v in value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, PurePath):
        return value.as_posix()
    return value


def canonical_json_dumps(value: Any) -> str:
    """Dump JSON with stable key order and separators for reproducible hashes."""

    return json.dumps(
        normalize_json(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_text(canonical_json_dumps(value))


def build_config_sha256(canonical_config: Mapping[str, Any]) -> str:
    return sha256_json(canonical_config)


def build_factor_set_hash(factor_list: Sequence[Any]) -> str:
    """Hash the exact factor sequence; order is part of the model feature schema."""

    return sha256_json(list(factor_list))


def _json_map(value: Mapping[str, Any] | None) -> JsonMap:
    return dict(value or {})


def _json_list(value: Sequence[Any] | None) -> list[Any]:
    return list(value or [])


def to_record_dict(record: Any) -> dict[str, Any]:
    if is_dataclass(record):
        return asdict(record)
    if isinstance(record, Mapping):
        return dict(record)
    raise TypeError(f"unsupported QE archive record type: {type(record)!r}")


@dataclass(frozen=True)
class QEArchiveRun:
    run_id: str
    logical_experiment_id: str
    source_system: str
    run_type: str
    status: str
    attempt_no: int = 1
    is_latest_attempt: bool = True
    task_id: str | None = None
    loop_id: str | None = None
    loop_index: int | None = None
    experiment_id: str | None = None
    node_id: str | None = None
    model_catalog_id: int | None = None
    model_family: str | None = None
    model_type: str | None = None
    factor_set_hash: str | None = None
    factor_count: int | None = None
    freq: str | None = None
    label_horizon: int | None = None
    research_valid: bool = True
    invalid_reason: str | None = None
    exclusion_tags: list[str] = field(default_factory=list)
    score_total: float | None = None
    score_version: str | None = None
    priority_rank: int | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    archived_at: datetime | None = None
    source_created_at: datetime | None = None
    source_updated_at: datetime | None = None


@dataclass
class RunSourceRecord:
    run_id: str
    source_system: str
    source_type: str
    source_id: str
    source_sub_id: str | None = None
    source_status: str | None = None
    source_uri: str | None = None
    recorder_experiment_id: str | None = None
    recorder_id: str | None = None
    mlflow_tracking_uri: str | None = None
    mlflow_artifact_uri: str | None = None
    qlib_recorder_name: str | None = None
    node_api_base_url: str | None = None
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        self.metadata = _json_map(self.metadata)


@dataclass
class RunConfigRecord:
    run_id: str
    config_schema_version: str
    canonical_config: Mapping[str, Any]
    config_sha256: str | None = None
    raw_config: Mapping[str, Any] | None = None
    factor_list: Sequence[Any] | None = None
    factor_set_hash: str | None = None
    model_config: Mapping[str, Any] | None = None
    model_params: Mapping[str, Any] | None = None
    strategy_config: Mapping[str, Any] | None = None
    backtest_config: Mapping[str, Any] | None = None
    data_split: Mapping[str, Any] | None = None
    execution_config: Mapping[str, Any] | None = None
    runtime_flags: Mapping[str, Any] | None = None
    agent_context: Mapping[str, Any] | None = None
    config_capture_complete: bool = False
    config_provenance: Mapping[str, Any] | None = None
    missing_config_items: Sequence[Any] | None = None

    def __post_init__(self) -> None:
        canonical = _json_map(self.canonical_config)
        self.canonical_config = canonical
        self.raw_config = _json_map(self.raw_config)
        self.factor_list = _json_list(self.factor_list)
        self.model_config = _json_map(self.model_config)
        self.model_params = _json_map(self.model_params)
        self.strategy_config = _json_map(self.strategy_config)
        self.backtest_config = _json_map(self.backtest_config)
        self.data_split = _json_map(self.data_split)
        self.execution_config = _json_map(self.execution_config)
        self.runtime_flags = _json_map(self.runtime_flags)
        self.agent_context = _json_map(self.agent_context)
        self.config_provenance = _json_map(self.config_provenance)
        self.missing_config_items = _json_list(self.missing_config_items)
        if not self.config_sha256:
            self.config_sha256 = build_config_sha256(canonical)
        if not self.factor_set_hash:
            self.factor_set_hash = build_factor_set_hash(self.factor_list)


@dataclass
class DataContextRecord:
    run_id: str
    context_type: str = "primary"
    freq: str | None = None
    market: str | None = None
    universe: str | None = None
    benchmark: str | None = None
    train_start: date | None = None
    train_end: date | None = None
    valid_start: date | None = None
    valid_end: date | None = None
    test_start: date | None = None
    test_end: date | None = None
    backtest_start: date | None = None
    backtest_end: date | None = None
    label_horizon: int | None = None
    qlib_provider_uri: str | None = None
    qlib_dataset_version: str | None = None
    dataset_snapshot_id: str | None = None
    feature_snapshot_id: str | None = None
    factor_cache_snapshot_id: str | None = None
    data_version_hash: str | None = None
    pit_cutoff_date: date | None = None
    limit_handling: str | None = None
    suspend_handling: str | None = None
    limit_suspend_authoritative: bool = False
    cost_config: Mapping[str, Any] | None = None
    stock_pool_config: Mapping[str, Any] | None = None
    data_quality_flags: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        self.cost_config = _json_map(self.cost_config)
        self.stock_pool_config = _json_map(self.stock_pool_config)
        self.data_quality_flags = _json_map(self.data_quality_flags)


@dataclass
class AccountSummaryRecord:
    run_id: str
    initial_capital: float | None = None
    final_total_value: float | None = None
    final_account_value: float | None = None
    final_nav_value: float | None = None
    total_return: float | None = None
    cagr: float | None = None
    max_drawdown: float | None = None
    max_drawdown_date: date | None = None
    sharpe: float | None = None
    annualized_volatility: float | None = None
    avg_cash_ratio: float | None = None
    final_cash: float | None = None
    final_stock_value: float | None = None
    final_stock_count: int | None = None
    final_cash_ratio: float | None = None
    n_trading_days: int | None = None
    position_count_min: float | None = None
    position_count_avg: float | None = None
    position_count_max: float | None = None
    position_count_p95: float | None = None
    source_payload_path: str | None = None
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        self.metadata = _json_map(self.metadata)


@dataclass
class ReproducibilityManifestRecord:
    run_id: str
    manifest_schema_version: str
    reproducibility_level: str
    manifest_json: Mapping[str, Any]
    verification_status: str = "not_verified"
    config_sha256: str | None = None
    canonical_config_sha256: str | None = None
    raw_config_sha256: str | None = None
    factor_set_hash: str | None = None
    qlib_config_sha256: str | None = None
    model_params_sha256: str | None = None
    strategy_config_sha256: str | None = None
    data_context_sha256: str | None = None
    metrics_payload_sha256: str | None = None
    enhanced_metrics_sha256: str | None = None
    artifact_manifest_sha256: str | None = None
    git_commit: str | None = None
    git_dirty: bool | None = None
    runner_script: str | None = None
    runner_script_sha256: str | None = None
    python_version: str | None = None
    qlib_version: str | None = None
    mlflow_version: str | None = None
    torch_version: str | None = None
    package_versions: Mapping[str, Any] | None = None
    random_seed: int | None = None
    deterministic_flags: Mapping[str, Any] | None = None
    source_config_paths: Mapping[str, Any] | None = None
    required_artifact_types: Sequence[str] | None = None
    missing_items: Sequence[Any] | None = None

    def __post_init__(self) -> None:
        self.manifest_json = _json_map(self.manifest_json)
        self.package_versions = _json_map(self.package_versions)
        self.deterministic_flags = _json_map(self.deterministic_flags)
        self.source_config_paths = _json_map(self.source_config_paths)
        self.required_artifact_types = list(self.required_artifact_types or [])
        self.missing_items = _json_list(self.missing_items)


@dataclass(frozen=True)
class RawPayloadRecord:
    payload_type: str
    source_system: str
    run_id: str | None = None
    source_id: str | None = None
    payload_json: Mapping[str, Any] | Sequence[Any] | None = None
    payload_text: str | None = None
    payload_sha256: str | None = None
    provenance_level: str = "direct"


@dataclass(frozen=True)
class MetricRecord:
    run_id: str
    metric_key: str
    metric_scope: str = "run"
    period_start: date | None = None
    period_end: date | None = None
    horizon: int | None = None
    freq: str | None = None
    value_num: float | None = None
    value_text: str | None = None
    value_json: Mapping[str, Any] | Sequence[Any] | None = None
    unit: str | None = None
    direction: str | None = None
    source_key: str | None = None
    source_payload_path: str | None = None
    quality_flag: str = "ok"


@dataclass(frozen=True)
class CurveRecord:
    run_id: str
    curve_key: str
    ts: datetime | None = None
    trade_date: date | None = None
    step: int | None = None
    epoch: int | None = None
    split_name: str | None = None
    value_num: float | None = None
    value_json: Mapping[str, Any] | Sequence[Any] | None = None
    source_key: str | None = None


@dataclass
class SymbolSummaryRecord:
    run_id: str
    symbol: str
    source_list: str = "all_stocks"
    profit: float | None = None
    profit_pct: float | None = None
    avg_cost: float | None = None
    last_price: float | None = None
    holding_days: int | None = None
    first_date: date | None = None
    last_date: date | None = None
    rank_in_list: int | None = None
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        self.metadata = _json_map(self.metadata)


@dataclass
class TradeRecord:
    run_id: str
    symbol: str
    trade_uid: str | None = None
    order_uid: str | None = None
    trade_date: date | None = None
    ts: datetime | None = None
    side: str | None = None
    price: float | None = None
    quantity: float | None = None
    amount: float | None = None
    commission: float | None = None
    tax: float | None = None
    slippage: float | None = None
    pnl: float | None = None
    source_payload_path: str | None = None
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        self.metadata = _json_map(self.metadata)
        if not self.trade_uid:
            fingerprint = {
                "run_id": self.run_id,
                "symbol": self.symbol,
                "trade_date": self.trade_date,
                "ts": self.ts,
                "side": self.side,
                "price": self.price,
                "quantity": self.quantity,
                "amount": self.amount,
                "source_payload_path": self.source_payload_path,
            }
            self.trade_uid = f"qear_trd_{sha256_json(fingerprint)[:24]}"


@dataclass
class ExecutionEventRecord:
    run_id: str
    event_type: str
    event_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    trade_date: date | None = None
    symbol: str | None = None
    severity: str = "info"
    message: str | None = None
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        self.metadata = _json_map(self.metadata)


@dataclass
class RunFactorRecord:
    run_id: str
    factor_name: str
    factor_catalog_id: int | None = None
    factor_source: str | None = None
    factor_version: str | None = None
    factor_order: int | None = None
    factor_group: str | None = None
    factor_classification: Mapping[str, Any] | None = None
    factor_expression_hash: str | None = None
    factor_asset_hash: str | None = None
    inclusion_reason: str | None = None
    inclusion_source: str | None = None
    is_alpha158: bool = False
    independent_metrics_snapshot: Mapping[str, Any] | None = None
    official_rating_snapshot: Mapping[str, Any] | None = None
    correlation_cluster: str | None = None

    def __post_init__(self) -> None:
        self.factor_classification = _json_map(self.factor_classification)
        self.independent_metrics_snapshot = _json_map(self.independent_metrics_snapshot)
        self.official_rating_snapshot = _json_map(self.official_rating_snapshot)


@dataclass
class OutboxEventRecord:
    event_type: str
    source_system: str
    source_id: str
    payload: Mapping[str, Any] | None = None
    event_id: str | None = None
    source_sub_id: str | None = None
    status: str = "pending"

    def __post_init__(self) -> None:
        payload = _json_map(self.payload)
        self.payload = payload
        if not self.event_id:
            fingerprint = {
                "event_type": self.event_type,
                "source_system": self.source_system,
                "source_id": self.source_id,
                "source_sub_id": self.source_sub_id,
            }
            self.event_id = f"qear_evt_{sha256_json(fingerprint)[:24]}"


@dataclass(frozen=True)
class ArchivePolicyDecision:
    source_system: str
    source_type: str
    source_id: str
    source_sub_id: str | None = None
    archive_policy: str = "AUTO"
    archive_policy_source: str = "default"
    reason: str = "default_auto"
    allow_override: bool = False
    runtime_config: Mapping[str, Any] = field(default_factory=dict)
    payload_sha256: str | None = None
    runtime_config_sha256: str | None = None

    @property
    def should_archive(self) -> bool:
        return self.archive_policy == "AUTO"

    @property
    def is_manual_only(self) -> bool:
        return self.archive_policy == "MANUAL_ONLY"

    @property
    def is_skipped(self) -> bool:
        return self.archive_policy in {"SKIP", "MANUAL_ONLY"}


@dataclass
class SkipRegistryRecord:
    source_system: str
    source_type: str
    source_id: str
    archive_policy: str
    archive_policy_source: str
    skip_reason: str
    trigger_reason: str
    source_sub_id: str | None = None
    event_type: str | None = None
    skip_id: str | None = None
    allow_override: bool = False
    override_required_token: str | None = None
    payload_sha256: str | None = None
    runtime_config_sha256: str | None = None
    created_by: str | None = None
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        self.metadata = _json_map(self.metadata)
        if not self.skip_id:
            fingerprint = {
                "source_system": self.source_system,
                "source_type": self.source_type,
                "source_id": self.source_id,
                "source_sub_id": self.source_sub_id,
            }
            self.skip_id = f"qear_skip_{sha256_json(fingerprint)[:24]}"


@dataclass
class IngestHistoryRecord:
    source_system: str
    source_type: str
    source_id: str
    trigger_reason: str
    ingest_status: str
    history_id: str | None = None
    run_id: str | None = None
    logical_experiment_id: str | None = None
    event_id: str | None = None
    job_id: str | None = None
    backfill_run_id: str | None = None
    source_sub_id: str | None = None
    archive_policy: str | None = None
    attempt_no: int = 1
    payload_sha256: str | None = None
    runtime_config_sha256: str | None = None
    result_fingerprint: str | None = None
    anomaly: bool = False
    anomaly_reason: str | None = None
    stats: Mapping[str, Any] | None = None
    error_message: str | None = None
    created_by: str | None = None

    def __post_init__(self) -> None:
        self.stats = _json_map(self.stats)
        if not self.history_id:
            fingerprint = {
                "source_system": self.source_system,
                "source_type": self.source_type,
                "source_id": self.source_id,
                "source_sub_id": self.source_sub_id,
                "trigger_reason": self.trigger_reason,
                "ingest_status": self.ingest_status,
                "attempt_no": self.attempt_no,
                "payload_sha256": self.payload_sha256,
                "runtime_config_sha256": self.runtime_config_sha256,
                "result_fingerprint": self.result_fingerprint,
                "seed": uuid4().hex,
            }
            self.history_id = f"qear_hist_{sha256_json(fingerprint)[:24]}"


@dataclass
class BackfillRunRecord:
    source_mode: str
    mode: str
    status: str = "pending"
    backfill_run_id: str | None = None
    request_payload: Mapping[str, Any] | None = None
    force_rebackfill: bool = False
    confirm_token_used: bool = False
    requested_by: str | None = None
    candidate_count: int = 0
    processed_count: int = 0
    ingested_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    last_cursor: Mapping[str, Any] | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        self.request_payload = _json_map(self.request_payload)
        self.last_cursor = _json_map(self.last_cursor)
        if not self.backfill_run_id:
            self.backfill_run_id = f"qear_bf_{uuid4().hex}"


@dataclass
class BackfillRunItemRecord:
    backfill_run_id: str
    source_system: str
    source_type: str
    source_id: str
    status: str = "candidate"
    item_id: str | None = None
    source_sub_id: str | None = None
    archive_policy: str | None = None
    run_id: str | None = None
    skip_id: str | None = None
    error_message: str | None = None
    stats: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        self.stats = _json_map(self.stats)
        if not self.item_id:
            fingerprint = {
                "backfill_run_id": self.backfill_run_id,
                "source_system": self.source_system,
                "source_type": self.source_type,
                "source_id": self.source_id,
                "source_sub_id": self.source_sub_id,
            }
            self.item_id = f"qear_bfi_{sha256_json(fingerprint)[:24]}"


@dataclass
class BootstrapMarkerRecord:
    source_type: str
    mode: str
    backfill_run_id: str
    status: str = "running"
    operator: str | None = None
    ingested_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    stats: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        self.stats = _json_map(self.stats)


@dataclass(frozen=True)
class ClaimedOutboxEvent:
    event_id: str
    event_type: str
    source_system: str
    source_id: str
    source_sub_id: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    retry_count: int = 0


@dataclass
class ArchiveJobRecord:
    event_id: str
    job_type: str
    job_id: str | None = None
    run_id: str | None = None
    status: str = "running"
    level: str = "A"
    stats: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        self.stats = _json_map(self.stats)
        if not self.job_id:
            self.job_id = f"qear_job_{uuid4().hex}"
