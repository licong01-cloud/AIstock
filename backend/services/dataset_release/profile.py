from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date
import re
from pathlib import Path, PureWindowsPath
from typing import Any, Mapping

import yaml

from .canonical import digest_named_fields
from .contracts import Component
from .errors import IndexContractError, ProfileValidationError
from .index_contract import (
    HMM_BENCHMARK_CODE,
    INDEX_SCHEMA_VERSION,
    INDEX_UNIVERSE_VERSION,
    IndexDefinition,
    index_contract_payload,
    parse_index_definitions,
    validate_index_definitions,
)
from .static_schema import (
    STATIC_DEFAULT_NUMERIC_DTYPE,
    STATIC_ORDERED_COLUMNS,
    STATIC_SCHEMA_VERSION,
    static_schema_digest,
)
from .stock_schema import (
    QLIB_STOCK_FIELDS,
    QLIB_STOCK_SCHEMA_VERSION,
    qlib_stock_schema_digest,
)


GIB = 2**30
PROFILE_SCHEMA_VERSION = "qe_backtest_monthly_profile_v2"
PROFILE_ID = "qe_hmm_full_v1"


@dataclass(frozen=True)
class ResourcePolicy:
    heavy_full_concurrency: int = 1
    aggregate_private_commit_bytes: int = 12 * GIB
    windows_job_commit_bytes: int = 8 * GIB
    hybrid_job_commit_bytes: int = 4 * GIB
    wsl_memory_high_bytes: int = 6 * GIB
    wsl_memory_max_bytes: int = 8 * GIB
    wsl_swap_max_bytes: int = 0
    host_start_available_bytes: int = 16 * GIB
    host_emergency_available_bytes: int = 8 * GIB
    host_start_commit_headroom_bytes: int = 16 * GIB
    host_emergency_commit_headroom_bytes: int = 8 * GIB
    wsl_start_available_bytes: int = 12 * GIB
    wsl_emergency_available_bytes: int = 6 * GIB
    db_pool_size: int = 4
    row_query_concurrency: int = 1
    db_statement_timeout_seconds: int = 300
    provider_request_concurrency: int = 1
    qlib_dump_workers: int = 8
    minute_code_batch_size: int = 20
    date_chunk_months: int = 3
    h5_load_batch_size: int = 100
    parquet_row_group_rows: int = 100_000
    validation_read_chunk_rows: int = 100_000
    enforcement_sample_seconds: float = 1.0
    receipt_rollup_seconds: float = 5.0
    wait_deadline_seconds: int = 3_600
    candidate_free_space_floor_bytes: int = 32 * GIB
    predicted_new_bytes_multiplier: float = 1.25

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ResourcePolicy":
        fields = cls.__dataclass_fields__
        unknown = sorted(set(value).difference(fields))
        if unknown:
            raise ProfileValidationError(f"unknown resource policy keys: {unknown}")
        try:
            return cls(**{name: value[name] for name in value})
        except TypeError as exc:
            raise ProfileValidationError(f"invalid resource policy: {exc}") from exc

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def digest(self) -> str:
        return digest_named_fields("dataset_release_resource_policy_v1", self.as_dict())


@dataclass(frozen=True, slots=True)
class QlibToolchainProfile:
    schema_version: str
    distro: str
    conda_sh_wsl: str
    conda_env: str
    dump_script_windows: PureWindowsPath
    dump_script_wsl: str
    dump_script_sha256: str
    guardian_python_wsl: str
    guardian_script_repo_relative: str
    runner_python_wsl: str
    runner_script_repo_relative: str

    @property
    def digest(self) -> str:
        return digest_named_fields(
            "dataset_release_qlib_toolchain_profile_v1",
            {
                **asdict(self),
                "dump_script_windows": str(self.dump_script_windows),
            },
        )

    def build_verified(self, project_root: Path):
        """Resolve repo helpers and verify every executable script by content."""

        from .daily_minute_materializer import QlibDumpToolchain
        from .streaming_artifacts import sha256_file

        root = Path(project_root).resolve(strict=True)
        dump_windows = Path(str(self.dump_script_windows)).resolve(strict=True)
        guardian_windows = (root / Path(self.guardian_script_repo_relative)).resolve(strict=True)
        runner_windows = (root / Path(self.runner_script_repo_relative)).resolve(strict=True)
        if root not in guardian_windows.parents or root not in runner_windows.parents:
            raise ProfileValidationError("Qlib repo helper path escapes project root")
        if _windows_path_to_wsl(dump_windows) != self.dump_script_wsl:
            raise ProfileValidationError("Qlib dump Windows/WSL path mapping differs")
        return QlibDumpToolchain(
            distro=self.distro,
            conda_sh=self.conda_sh_wsl,
            conda_env=self.conda_env,
            dump_script_wsl=self.dump_script_wsl,
            dump_script_windows=dump_windows,
            dump_script_sha256=self.dump_script_sha256,
            guardian_python=self.guardian_python_wsl,
            guardian_script_wsl=_windows_path_to_wsl(guardian_windows),
            guardian_script_windows=guardian_windows,
            guardian_script_sha256=sha256_file(guardian_windows),
            heartbeat_path_wsl="/dynamic/attempt-fenced-heartbeat.json",
            runner_python_wsl=self.runner_python_wsl,
            runner_script_wsl=_windows_path_to_wsl(runner_windows),
            runner_script_windows=runner_windows,
            runner_script_sha256=sha256_file(runner_windows),
        )


HARD_RESOURCE_BOUNDARIES = ResourcePolicy()

_MAXIMUM_FIELDS = {
    "heavy_full_concurrency",
    "aggregate_private_commit_bytes",
    "windows_job_commit_bytes",
    "hybrid_job_commit_bytes",
    "wsl_memory_high_bytes",
    "wsl_memory_max_bytes",
    "wsl_swap_max_bytes",
    "db_pool_size",
    "row_query_concurrency",
    "db_statement_timeout_seconds",
    "provider_request_concurrency",
    "qlib_dump_workers",
    "minute_code_batch_size",
    "date_chunk_months",
    "h5_load_batch_size",
    "parquet_row_group_rows",
    "validation_read_chunk_rows",
    "enforcement_sample_seconds",
    "receipt_rollup_seconds",
    "wait_deadline_seconds",
}
_MINIMUM_FIELDS = {
    "host_start_available_bytes",
    "host_emergency_available_bytes",
    "host_start_commit_headroom_bytes",
    "host_emergency_commit_headroom_bytes",
    "wsl_start_available_bytes",
    "wsl_emergency_available_bytes",
    "candidate_free_space_floor_bytes",
    "predicted_new_bytes_multiplier",
}
_REAL_FIELDS = {
    "enforcement_sample_seconds",
    "receipt_rollup_seconds",
    "predicted_new_bytes_multiplier",
}
_INTEGER_FIELDS = set(ResourcePolicy.__dataclass_fields__).difference(_REAL_FIELDS)


def validate_resource_policy(policy: ResourcePolicy) -> ResourcePolicy:
    if not isinstance(policy, ResourcePolicy):
        raise ProfileValidationError("resource policy must be a profile.ResourcePolicy")
    actual = policy.as_dict()
    invalid_integer_fields = sorted(field for field in _INTEGER_FIELDS if type(actual[field]) is not int)
    invalid_real_fields = sorted(
        field
        for field in _REAL_FIELDS
        if isinstance(actual[field], bool) or not isinstance(actual[field], (int, float))
    )
    if invalid_integer_fields or invalid_real_fields:
        raise ProfileValidationError(
            "resource policy contains non-numeric or non-integral values: "
            f"{invalid_integer_fields + invalid_real_fields}"
        )
    hard = HARD_RESOURCE_BOUNDARIES.as_dict()
    for field in _MAXIMUM_FIELDS:
        if actual[field] < 0 or actual[field] > hard[field]:
            raise ProfileValidationError(f"resource {field}={actual[field]} exceeds hard maximum {hard[field]}")
    for field in _MINIMUM_FIELDS:
        if actual[field] < hard[field]:
            raise ProfileValidationError(
                f"resource reserve {field}={actual[field]} is below hard minimum {hard[field]}"
            )
    if policy.wsl_swap_max_bytes != 0:
        raise ProfileValidationError("WSL swap.max must remain exactly zero")
    if policy.receipt_rollup_seconds <= 0:
        raise ProfileValidationError("receipt_rollup_seconds must be positive")
    if policy.enforcement_sample_seconds <= 0:
        raise ProfileValidationError("enforcement_sample_seconds must be positive")
    if not (
        policy.host_start_available_bytes > policy.host_emergency_available_bytes
        and policy.host_start_commit_headroom_bytes > policy.host_emergency_commit_headroom_bytes
        and policy.wsl_start_available_bytes > policy.wsl_emergency_available_bytes
    ):
        raise ProfileValidationError("resource start reserves must exceed emergency reserves")
    if (
        min(
            policy.heavy_full_concurrency,
            policy.aggregate_private_commit_bytes,
            policy.windows_job_commit_bytes,
            policy.hybrid_job_commit_bytes,
            policy.wsl_memory_high_bytes,
            policy.wsl_memory_max_bytes,
            policy.db_pool_size,
            policy.row_query_concurrency,
            policy.db_statement_timeout_seconds,
            policy.provider_request_concurrency,
            policy.qlib_dump_workers,
            policy.minute_code_batch_size,
            policy.date_chunk_months,
            policy.h5_load_batch_size,
            policy.parquet_row_group_rows,
            policy.validation_read_chunk_rows,
            policy.wait_deadline_seconds,
        )
        <= 0
    ):
        raise ProfileValidationError("positive resource limits must be greater than zero")
    if policy.wsl_memory_high_bytes >= policy.wsl_memory_max_bytes:
        raise ProfileValidationError("WSL memory.high must remain below memory.max")
    if policy.hybrid_job_commit_bytes > policy.windows_job_commit_bytes:
        raise ProfileValidationError("hybrid Job commit cap cannot exceed Windows-only cap")
    if (
        policy.windows_job_commit_bytes > policy.aggregate_private_commit_bytes
        or policy.hybrid_job_commit_bytes + policy.wsl_memory_max_bytes > policy.aggregate_private_commit_bytes
    ):
        raise ProfileValidationError("per-runtime memory caps exceed aggregate private commit cap")
    if policy.predicted_new_bytes_multiplier <= 0:
        raise ProfileValidationError("predicted_new_bytes_multiplier must be positive")
    return policy


def apply_resource_overrides(
    policy: ResourcePolicy,
    overrides: Mapping[str, Any] | None,
    *,
    source: str,
) -> ResourcePolicy:
    """Apply CLI/env/profile values only when they make execution equally or more restrictive."""

    if not overrides:
        return validate_resource_policy(policy)
    unknown = sorted(set(overrides).difference(policy.__dataclass_fields__))
    if unknown:
        raise ProfileValidationError(f"unknown {source} resource overrides: {unknown}")
    current = policy.as_dict()
    try:
        weakened_maximum = sorted(
            name for name, value in overrides.items() if name in _MAXIMUM_FIELDS and value > current[name]
        )
        weakened_minimum = sorted(
            name for name, value in overrides.items() if name in _MINIMUM_FIELDS and value < current[name]
        )
    except TypeError as exc:
        raise ProfileValidationError(f"unsafe {source} resource override: values must be numeric") from exc
    weakened = weakened_maximum + weakened_minimum
    if weakened:
        raise ProfileValidationError(f"unsafe {source} resource override weakens effective policy: {weakened}")
    candidate = replace(policy, **dict(overrides))
    try:
        return validate_resource_policy(candidate)
    except ProfileValidationError as exc:
        raise ProfileValidationError(f"unsafe {source} resource override: {exc}") from exc


@dataclass(frozen=True)
class DatasetProfile:
    path: Path
    profile: str
    start_date: date
    minute_start_date: date
    cutoff_policy: str
    source_content_probe_ttl_seconds: int
    reconcile_catchup_months: int
    reconcile_lease_ttl_seconds: int
    worker_heartbeat_ttl_seconds: int
    source_audit_reuse_policy: str
    stage_timeouts_seconds: Mapping[str, int]
    candidate_root: PureWindowsPath
    control_root: PureWindowsPath
    candidate_root_id: str
    components: tuple[Component, ...]
    universe_key: str
    universe_rule_version: str
    moneyflow_contract: str
    static_column_count: int
    static_schema_version: str
    static_schema_digest: str
    static_ordered_columns: tuple[str, ...]
    static_default_numeric_dtype: str
    qlib_stock_schema_version: str
    qlib_stock_schema_digest: str
    qlib_stock_fields: tuple[str, ...]
    l2_code_id_dtype: str
    l2_code_id_missing: int
    minute_source_policy: str
    source_partition_schema_version: str
    source_date_chunk_months: int
    minute_partition_schema_version: str
    minute_code_bucket_count: int
    minute_code_bucket_capacity: int
    indices: tuple[IndexDefinition, ...]
    resource_policy: ResourcePolicy
    pressure_ladder: Mapping[str, tuple[int, ...]]
    qlib_toolchain: QlibToolchainProfile
    raw: Mapping[str, Any]
    config_digest: str
    semantic_profile_digest: str

    @property
    def index_codes(self) -> tuple[str, ...]:
        return tuple(item.daily_code for item in self.indices)

    @property
    def resource_policy_digest(self) -> str:
        return self.resource_policy.digest

    def with_resource_overrides(
        self,
        overrides: Mapping[str, Any],
        *,
        source: str,
    ) -> "DatasetProfile":
        return replace(
            self,
            resource_policy=apply_resource_overrides(
                self.resource_policy,
                overrides,
                source=source,
            ),
        )


def _windows_x_path(value: Any, *, field: str) -> PureWindowsPath:
    path = PureWindowsPath(str(value))
    if not path.is_absolute() or path.drive.upper() != "X:":
        raise ProfileValidationError(f"{field} must be an absolute X: path")
    return path


def _windows_path_to_wsl(path: Path) -> str:
    text = str(path).replace("\\", "/")
    if len(text) < 3 or text[1] != ":" or not text[0].isalpha():
        raise ProfileValidationError("Qlib Windows path cannot be mapped into WSL")
    return f"/mnt/{text[0].lower()}{text[2:]}"


def _qlib_toolchain_profile(value: Mapping[str, Any]) -> QlibToolchainProfile:
    expected = {
        "schema_version",
        "distro",
        "conda_sh_wsl",
        "conda_env",
        "dump_script_windows",
        "dump_script_wsl",
        "dump_script_sha256",
        "guardian_python_wsl",
        "guardian_script_repo_relative",
        "runner_python_wsl",
        "runner_script_repo_relative",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ProfileValidationError("versioned Qlib toolchain fields differ")
    text = {key: str(value[key]).strip() for key in expected}
    if (
        text["schema_version"] != "qe_qlib_toolchain_v1"
        or any(not item for item in text.values())
        or re.fullmatch(r"[0-9a-f]{64}", text["dump_script_sha256"]) is None
        or any(
            not text[key].startswith("/")
            for key in (
                "conda_sh_wsl",
                "dump_script_wsl",
                "guardian_python_wsl",
                "runner_python_wsl",
            )
        )
    ):
        raise ProfileValidationError("versioned Qlib toolchain values are invalid")
    dump_windows = PureWindowsPath(text["dump_script_windows"])
    if not dump_windows.is_absolute():
        raise ProfileValidationError("Qlib dump Windows path must be absolute")
    for key in (
        "guardian_script_repo_relative",
        "runner_script_repo_relative",
    ):
        path = Path(text[key])
        if (
            path.is_absolute()
            or not path.parts
            or any(part in {"", ".", ".."} for part in path.parts)
            or any(character in text[key] for character in ("*", "?", "[", "]"))
        ):
            raise ProfileValidationError(f"Qlib repo helper path is unsafe: {key}")
    return QlibToolchainProfile(
        schema_version=text["schema_version"],
        distro=text["distro"],
        conda_sh_wsl=text["conda_sh_wsl"],
        conda_env=text["conda_env"],
        dump_script_windows=dump_windows,
        dump_script_wsl=text["dump_script_wsl"],
        dump_script_sha256=text["dump_script_sha256"],
        guardian_python_wsl=text["guardian_python_wsl"],
        guardian_script_repo_relative=text["guardian_script_repo_relative"],
        runner_python_wsl=text["runner_python_wsl"],
        runner_script_repo_relative=text["runner_script_repo_relative"],
    )


def _pressure_ladder(value: Mapping[str, Any]) -> dict[str, tuple[int, ...]]:
    expected = {
        "h5_batch": (100, 50, 20),
        "minute_batch": (20, 10, 5),
        "date_chunk_months": (3, 1),
        "row_group_rows": (100_000, 50_000),
        "dump_workers": (8, 4, 2),
    }
    actual = {name: tuple(int(item) for item in items) for name, items in value.items()}
    if actual != expected:
        raise ProfileValidationError(f"pressure ladder must exactly match v1: {expected}")
    return actual


def load_dataset_profile(
    path: str | Path,
    *,
    resource_overrides: Mapping[str, Any] | None = None,
    override_source: str = "caller",
) -> DatasetProfile:
    """Load one profile and normalize parser/shape failures to a typed error."""

    try:
        return _load_dataset_profile(
            path,
            resource_overrides=resource_overrides,
            override_source=override_source,
        )
    except ProfileValidationError:
        raise
    except (KeyError, OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        raise ProfileValidationError("dataset release profile could not be parsed or validated") from exc


def _load_dataset_profile(
    path: str | Path,
    *,
    resource_overrides: Mapping[str, Any] | None = None,
    override_source: str = "caller",
) -> DatasetProfile:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise ProfileValidationError(f"profile not found: {resolved}")
    raw = yaml.safe_load(resolved.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ProfileValidationError("profile root must be a mapping")
    if raw.get("schema_version") != PROFILE_SCHEMA_VERSION or raw.get("profile") != PROFILE_ID:
        raise ProfileValidationError(f"profile must be {PROFILE_SCHEMA_VERSION}/{PROFILE_ID}")

    semantic = raw.get("semantic") or {}
    storage = raw.get("storage") or {}
    resources = raw.get("resources") or {}
    runtime = raw.get("runtime") or {}
    components_raw = raw.get("components") or {}
    try:
        start_date = date.fromisoformat(str(raw.get("start_date")))
        minute_start_date = date.fromisoformat(str(raw.get("minute_start_date")))
    except ValueError as exc:
        raise ProfileValidationError("start_date/minute_start_date must be ISO dates") from exc
    if minute_start_date < start_date:
        raise ProfileValidationError("minute_start_date cannot precede start_date")
    probe_ttl = raw.get("source_content_probe_ttl_seconds")
    if type(probe_ttl) is not int or not 60 <= probe_ttl <= 7 * 24 * 60 * 60:
        raise ProfileValidationError("source_content_probe_ttl_seconds must be an integer in 60..604800")
    reconcile_catchup_months = raw.get("reconcile_catchup_months")
    if type(reconcile_catchup_months) is not int or not 1 <= reconcile_catchup_months <= 24:
        raise ProfileValidationError("reconcile_catchup_months must be an integer in 1..24")
    reconcile_lease_ttl_seconds = raw.get("reconcile_lease_ttl_seconds")
    if type(reconcile_lease_ttl_seconds) is not int or not 60 <= reconcile_lease_ttl_seconds <= 3_600:
        raise ProfileValidationError("reconcile_lease_ttl_seconds must be an integer in 60..3600")
    worker_heartbeat_ttl_seconds = raw.get("worker_heartbeat_ttl_seconds")
    if type(worker_heartbeat_ttl_seconds) is not int or not 15 <= worker_heartbeat_ttl_seconds <= 300:
        raise ProfileValidationError("worker_heartbeat_ttl_seconds must be an integer in 15..300")
    source_audit_reuse_policy = str(raw.get("source_audit_reuse_policy", ""))
    if source_audit_reuse_policy != "require_complete_for_reuse_v1":
        raise ProfileValidationError("source_audit_reuse_policy must be require_complete_for_reuse_v1")
    timeout_value = raw.get("stage_timeouts_seconds") or {}
    expected_timeout_keys = {
        "source_freeze",
        "full_build",
        "qlib_dump",
        "consumer",
    }
    if (
        not isinstance(timeout_value, Mapping)
        or set(timeout_value) != expected_timeout_keys
        or any(type(value) is not int for value in timeout_value.values())
        or not 3_600 <= int(timeout_value["consumer"]) <= 172_800
        or not 3_600 <= int(timeout_value["qlib_dump"]) <= 172_800
        or not 14_400 <= int(timeout_value["source_freeze"]) <= 172_800
        or not 14_400 <= int(timeout_value["full_build"]) <= 172_800
    ):
        raise ProfileValidationError("versioned stage timeout contract is invalid")
    stage_timeouts = {key: int(timeout_value[key]) for key in sorted(expected_timeout_keys)}
    required = tuple(
        Component(name)
        for name, definition in components_raw.items()
        if isinstance(definition, Mapping) and definition.get("required") is True
    )
    if required != tuple(Component):
        raise ProfileValidationError(
            f"required components must be ordered exactly as {[item.value for item in Component]}"
        )

    index_value = semantic.get("index_context") or {}
    if (
        index_value.get("schema_version") != INDEX_SCHEMA_VERSION
        or index_value.get("universe_version") != INDEX_UNIVERSE_VERSION
        or index_value.get("benchmark_code") != HMM_BENCHMARK_CODE
        or index_value.get("index_weight_consumed") is not False
    ):
        raise ProfileValidationError("index context version/benchmark/weight contract drift")
    try:
        indices = validate_index_definitions(parse_index_definitions(index_value.get("codes") or []))
    except IndexContractError as exc:
        raise ProfileValidationError(str(exc), context=exc.context) from exc

    if semantic.get("moneyflow_contract") != "tushare_moneyflow_shares_yuan_v1":
        raise ProfileValidationError("moneyflow share/CNY contract drift")
    static = semantic.get("static_authority") or {}
    if (
        int(static.get("column_count", 0)) != 121
        or static.get("schema_version") != STATIC_SCHEMA_VERSION
        or static.get("ordered_columns_digest") != static_schema_digest()
        or static.get("default_numeric_dtype") != STATIC_DEFAULT_NUMERIC_DTYPE
        or static.get("l2_code_id_dtype") != "int16"
        or int(static.get("l2_code_id_missing", 0)) != -1
    ):
        raise ProfileValidationError("121-column static/l2_code_id contract drift")
    stock = semantic.get("qlib_stock_authority") or {}
    if (
        stock.get("schema_version") != QLIB_STOCK_SCHEMA_VERSION
        or stock.get("ordered_fields_digest") != qlib_stock_schema_digest()
        or tuple(stock.get("daily_fields") or ()) != QLIB_STOCK_FIELDS
        or tuple(stock.get("minute_fields") or ()) != QLIB_STOCK_FIELDS
        or stock.get("dtype") != "float32"
    ):
        raise ProfileValidationError("12-field Qlib stock contract drift")
    minute_policy = str(semantic.get("minute_source_policy", ""))
    if minute_policy != "tdx_then_tushare_missing_keys_conflict_fail_v1":
        raise ProfileValidationError("minute TDX/Tushare precedence contract drift")
    source_partitioning = semantic.get("source_partitioning") or {}
    if (
        source_partitioning.get("schema_version") != "qe_source_stable_partitioning_v1"
        or source_partitioning.get("date_chunk_months") != 3
        or source_partitioning.get("minute_bucket_schema_version") != "qe_minute_sha256_bucket_v1"
        or type(source_partitioning.get("minute_bucket_count")) is not int
        or type(source_partitioning.get("minute_bucket_capacity")) is not int
        or not 256 <= source_partitioning["minute_bucket_count"] <= 4096
        or source_partitioning["minute_bucket_count"] & (source_partitioning["minute_bucket_count"] - 1)
        or source_partitioning["minute_bucket_capacity"] != 20
    ):
        raise ProfileValidationError("source stable partition contract drift")

    profile_policy = ResourcePolicy.from_mapping(resources.get("defaults") or {})
    validate_resource_policy(profile_policy)
    declared_hard = ResourcePolicy.from_mapping(resources.get("hard_boundaries") or {})
    if declared_hard != HARD_RESOURCE_BOUNDARIES:
        raise ProfileValidationError("profile hard resource boundaries differ from code authority")
    effective_policy = apply_resource_overrides(
        profile_policy,
        resource_overrides,
        source=override_source,
    )
    ladder = _pressure_ladder(resources.get("pressure_ladder") or {})
    toolchain = _qlib_toolchain_profile(runtime.get("qlib_toolchain") or {})

    candidate_root = _windows_x_path(storage.get("candidate_root"), field="candidate_root")
    control_root = _windows_x_path(storage.get("control_root"), field="control_root")
    if candidate_root == control_root or "candidate" not in str(candidate_root).lower():
        raise ProfileValidationError("candidate and control roots must be separate candidate-named paths")
    root_id = str(storage.get("candidate_root_id", "")).strip()
    if not root_id:
        raise ProfileValidationError("candidate_root_id is required")

    semantic_payload = {
        "profile": PROFILE_ID,
        "start_date": start_date,
        "minute_start_date": minute_start_date,
        "components": [item.value for item in required],
        "universe_key": str(semantic.get("universe_key")),
        "universe_rule_version": str(semantic.get("universe_rule_version")),
        "moneyflow_contract": semantic.get("moneyflow_contract"),
        "static_authority": static,
        "qlib_stock_authority": stock,
        "minute_source_policy": minute_policy,
        "source_partitioning": source_partitioning,
        "index_context": index_contract_payload(),
    }
    config_digest = digest_named_fields("dataset_release_profile_file_v2", raw)
    return DatasetProfile(
        path=resolved,
        profile=PROFILE_ID,
        start_date=start_date,
        minute_start_date=minute_start_date,
        cutoff_policy=str(raw.get("cutoff_policy")),
        source_content_probe_ttl_seconds=probe_ttl,
        reconcile_catchup_months=reconcile_catchup_months,
        reconcile_lease_ttl_seconds=reconcile_lease_ttl_seconds,
        worker_heartbeat_ttl_seconds=worker_heartbeat_ttl_seconds,
        source_audit_reuse_policy=source_audit_reuse_policy,
        stage_timeouts_seconds=stage_timeouts,
        candidate_root=candidate_root,
        control_root=control_root,
        candidate_root_id=root_id,
        components=required,
        universe_key=str(semantic.get("universe_key")),
        universe_rule_version=str(semantic.get("universe_rule_version")),
        moneyflow_contract=str(semantic.get("moneyflow_contract")),
        static_column_count=int(static.get("column_count")),
        static_schema_version=STATIC_SCHEMA_VERSION,
        static_schema_digest=static_schema_digest(),
        static_ordered_columns=STATIC_ORDERED_COLUMNS,
        static_default_numeric_dtype=STATIC_DEFAULT_NUMERIC_DTYPE,
        qlib_stock_schema_version=QLIB_STOCK_SCHEMA_VERSION,
        qlib_stock_schema_digest=qlib_stock_schema_digest(),
        qlib_stock_fields=QLIB_STOCK_FIELDS,
        l2_code_id_dtype=str(static.get("l2_code_id_dtype")),
        l2_code_id_missing=int(static.get("l2_code_id_missing")),
        minute_source_policy=minute_policy,
        source_partition_schema_version=str(source_partitioning["schema_version"]),
        source_date_chunk_months=int(source_partitioning["date_chunk_months"]),
        minute_partition_schema_version=str(source_partitioning["minute_bucket_schema_version"]),
        minute_code_bucket_count=int(source_partitioning["minute_bucket_count"]),
        minute_code_bucket_capacity=int(source_partitioning["minute_bucket_capacity"]),
        indices=indices,
        resource_policy=effective_policy,
        pressure_ladder=ladder,
        qlib_toolchain=toolchain,
        raw=raw,
        config_digest=config_digest,
        semantic_profile_digest=digest_named_fields("dataset_release_semantic_profile_v1", semantic_payload),
    )
