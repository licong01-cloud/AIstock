"""Plan and atomically persist one existing-package B0_QUOTE_V2 SIM pilot.

Dry-run is the default.  Apply mode performs production DML only when the
operator flag and environment confirmation token are both present.  The tool
creates a new immutable runtime release and binding; it never creates or
mutates a StrategyPackage and never calls a broker.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterator

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.config_manager_compat import ConfigManager  # noqa: E402
from backend.miniqmt_quote_contract_config import QuoteContractPolicy  # noqa: E402
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import (  # noqa: E402
    B0QuoteV2RevisionV1,
    QuoteControlBindingV1,
    quote_evidence_policy,
    source_build_manifest,
)
from backend.services.simulation_runtime.models import (  # noqa: E402
    SimulationBrokerBackend,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    canonical_json_sha256,
)
from backend.services.simulation_runtime.repository import (  # noqa: E402
    InMemorySimulationRuntimeRepository,
    SimulationRuntimeRepository,
)
from backend.services.simulation_runtime.service import StrategyRuntimeReleaseService  # noqa: E402
from backend.services.simulation_runtime.tca_capture import TcaBenchmarkPolicy  # noqa: E402
from backend.services.trading_core.errors import TradingCoreError  # noqa: E402

APPLY_CONFIRM_ENV = "MINIQMT_B0_QUOTE_V2_PILOT_APPLY"
APPLY_CONFIRM_VALUE = "I_UNDERSTAND_PRODUCTION_DML"
TARGET_PROD = "prod"
TARGET_DEV = "dev"
QUOTE_CONTROL = {
    "schema_version": "miniqmt_quote_control_binding_v1",
    "control_revision": "B0_QUOTE_V2",
}


class PilotActivationError(RuntimeError):
    """Loud, structured pilot planning or apply failure."""

    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.context = context or {}


@dataclass(frozen=True)
class PilotArtifacts:
    source_release: StrategyRuntimeRelease
    source_binding: SimulationReleaseBinding
    release: StrategyRuntimeRelease
    binding: SimulationReleaseBinding
    execution_policy_json: dict[str, Any]
    quote_policy_sha256: str
    revision: B0QuoteV2RevisionV1
    observation: dict[str, Any]


def _load_env_file(path: Path | None) -> None:
    if path is None or not path.exists():
        return
    for key, value in ConfigManager(path.resolve()).read_env().items():
        os.environ.setdefault(key, value)


def _db_config(*, target_db: str) -> dict[str, Any]:
    prefix = "TDX_DB_DEV_" if target_db == TARGET_DEV else "TDX_DB_"
    names = {
        "host": f"{prefix}HOST",
        "port": f"{prefix}PORT",
        "dbname": f"{prefix}NAME",
        "user": f"{prefix}USER",
        "password": f"{prefix}PASSWORD",
    }
    missing = [env_name for env_name in names.values() if not os.environ.get(env_name)]
    if missing:
        raise PilotActivationError("database configuration is incomplete", context={"missing": missing})
    config = {
        key: int(os.environ[env_name]) if key == "port" else os.environ[env_name]
        for key, env_name in names.items()
    }
    if target_db == TARGET_DEV:
        host = str(config["host"]).lower()
        dbname = str(config["dbname"]).lower()
        if host not in {"127.0.0.1", "localhost"} or not any(
            marker in dbname for marker in ("dev", "scratch", "test")
        ):
            raise PilotActivationError(
                "refusing a dev target that is not a local scratch/dev database",
                context={"host": config["host"], "dbname": config["dbname"]},
            )
    return config


@contextmanager
def _connection(*, env_file: Path | None, target_db: str, readonly: bool) -> Iterator[Any]:
    _load_env_file(env_file)
    conn = psycopg2.connect(**_db_config(target_db=target_db))
    conn.set_session(readonly=readonly, autocommit=readonly)
    try:
        yield conn
    finally:
        conn.close()


def _repo(conn: Any) -> SimulationRuntimeRepository:
    return SimulationRuntimeRepository(conn_factory=lambda: nullcontext(conn))


def _strict_non_negative(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise PilotActivationError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise PilotActivationError(f"{field_name} must be an integer") from exc
    if parsed < 0:
        raise PilotActivationError(f"{field_name} must be non-negative")
    return parsed


def _strict_positive(value: Any, *, field_name: str) -> int:
    parsed = _strict_non_negative(value, field_name=field_name)
    if parsed <= 0:
        raise PilotActivationError(f"{field_name} must be positive")
    return parsed


def _put_exact(container: dict[str, Any], key: str, value: dict[str, Any]) -> None:
    existing = container.get(key)
    if existing is not None and existing != value:
        raise PilotActivationError(
            f"source execution policy already has a conflicting {key}",
            context={"key": key, "existing": existing, "requested": value},
        )
    container[key] = deepcopy(value)


def _build_execution_policy(source_release: StrategyRuntimeRelease, args: argparse.Namespace) -> tuple[dict[str, Any], str]:
    release_config = source_release.release_config_json if isinstance(source_release.release_config_json, dict) else {}
    envelope = release_config.get("execution_policy")
    policy = envelope.get("policy_json") if isinstance(envelope, dict) else None
    if not isinstance(policy, dict) or not policy:
        raise PilotActivationError(
            "source runtime release lacks a complete immutable execution policy snapshot",
            context={"source_release_id": source_release.release_id},
        )
    source_policy_sha256 = canonical_json_sha256(policy)
    if source_policy_sha256 != source_release.execution_policy_sha256:
        raise PilotActivationError(
            "source execution policy snapshot hash differs from its release identity",
            context={
                "source_release_id": source_release.release_id,
                "expected": source_release.execution_policy_sha256,
                "actual": source_policy_sha256,
            },
        )
    result = deepcopy(policy)
    if str(result.get("algo_code") or "").strip().upper() != "SNIPER_MINIQMT":
        raise PilotActivationError(
            "multi-alpha B0 pilot requires the existing SNIPER_MINIQMT business policy",
            context={"algo_code": result.get("algo_code")},
        )

    quote_contract = {
        "schema_version": "miniqmt_quote_contract_policy_v2",
        "control_revision": "B0_QUOTE_V2",
        "required_capabilities": [
            "CALENDAR",
            "DEPTH_UNIT_SHARES",
            "EXCHANGE_TIMESTAMP",
            "FIVE_LEVEL_DEPTH",
            "RAW_PRICE_BASIS",
            "TRADABILITY",
        ],
        "max_receive_age_ms": _strict_positive(args.max_receive_age_ms, field_name="max_receive_age_ms"),
        "max_source_lag_ms": _strict_positive(args.max_source_lag_ms, field_name="max_source_lag_ms"),
        "max_exchange_age_ms": _strict_positive(args.max_exchange_age_ms, field_name="max_exchange_age_ms"),
        "max_negative_skew_ms": _strict_non_negative(args.max_negative_skew_ms, field_name="max_negative_skew_ms"),
        "max_clock_age_divergence_ms": _strict_positive(
            args.max_clock_age_divergence_ms,
            field_name="max_clock_age_divergence_ms",
        ),
        "max_dependency_group_skew_ms": _strict_positive(
            args.max_dependency_group_skew_ms,
            field_name="max_dependency_group_skew_ms",
        ),
        "auction_mode": "OBSERVE_ONLY",
    }
    benchmark_policy = {
        "benchmark_max_age_ms": _strict_positive(
            args.benchmark_max_age_ms,
            field_name="benchmark_max_age_ms",
        ),
        "arrival_forward_window_ms": _strict_non_negative(
            args.arrival_forward_window_ms,
            field_name="arrival_forward_window_ms",
        ),
        "clock_skew_tolerance_ms": _strict_non_negative(
            args.clock_skew_tolerance_ms,
            field_name="clock_skew_tolerance_ms",
        ),
        "benchmark_max_transport_latency_ms": _strict_positive(
            args.benchmark_max_transport_latency_ms,
            field_name="benchmark_max_transport_latency_ms",
        ),
        "policy_version": str(args.benchmark_policy_version).strip(),
    }
    if not benchmark_policy["policy_version"]:
        raise PilotActivationError("benchmark_policy_version is required")
    quote_evidence = {
        "schema_version": "miniqmt_quote_evidence_policy_v1",
        "benchmark_policy_version": benchmark_policy["policy_version"],
        "mark_policy_version": str(args.mark_policy_version).strip(),
        "markout_max_lag_ms": _strict_positive(args.markout_max_lag_ms, field_name="markout_max_lag_ms"),
    }
    if not quote_evidence["mark_policy_version"]:
        raise PilotActivationError("mark_policy_version is required")

    algo_config = result.get("algo_config")
    if not isinstance(algo_config, dict):
        raise PilotActivationError("source execution policy algo_config must be an object")
    algo_config = deepcopy(algo_config)
    tca = algo_config.get("tca")
    if tca is None:
        tca = {}
    if not isinstance(tca, dict):
        raise PilotActivationError("source execution policy algo_config.tca must be an object")
    tca = deepcopy(tca)
    _put_exact(tca, "benchmark_policy", benchmark_policy)
    algo_config["tca"] = tca
    result["algo_config"] = algo_config
    _put_exact(result, "quote_contract", quote_contract)
    _put_exact(result, "quote_evidence", quote_evidence)

    quote_policy = QuoteContractPolicy.from_execution_policy(result)
    validated_benchmark = TcaBenchmarkPolicy.model_validate(benchmark_policy)
    benchmark_version, mark_version, markout_lag = quote_evidence_policy(result)
    if validated_benchmark.policy_version != benchmark_version:
        raise PilotActivationError("benchmark policy version differs between TCA and quote evidence")
    if mark_version != quote_evidence["mark_policy_version"] or markout_lag != quote_evidence["markout_max_lag_ms"]:
        raise PilotActivationError("quote evidence policy readback differs from requested values")
    return result, quote_policy.policy_sha256


def build_pilot_artifacts(
    *,
    source_release: StrategyRuntimeRelease,
    source_binding: SimulationReleaseBinding,
    args: argparse.Namespace,
) -> PilotArtifacts:
    if source_binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
        raise PilotActivationError("source binding must use the MiniQMT SIM backend")
    if source_binding.release_id != source_release.release_id:
        raise PilotActivationError("source binding and release identities differ")
    identity_mismatches = {
        key: {"binding": binding_value, "release": release_value}
        for key, binding_value, release_value in (
            ("package_id", source_binding.package_id, source_release.package_id),
            ("manifest_sha256", source_binding.manifest_sha256, source_release.manifest_sha256),
            ("release_hash", source_binding.release_hash, source_release.release_hash),
        )
        if binding_value != release_value
    }
    if identity_mismatches:
        raise PilotActivationError(
            "source binding identity differs from its immutable release",
            context={"mismatches": identity_mismatches},
        )
    if source_binding.effective_to is None or args.trade_date <= source_binding.effective_to:
        raise PilotActivationError(
            "pilot trade date must follow the immutable source binding window",
            context={"source_effective_to": str(source_binding.effective_to), "trade_date": str(args.trade_date)},
        )
    parsed_source_control = QuoteControlBindingV1.from_binding_config(source_binding.binding_config_json)
    if parsed_source_control.explicitly_configured:
        raise PilotActivationError(
            "pilot source must be a historical omitted LEGACY_B0 binding",
            context={"control_revision": parsed_source_control.control_revision.value},
        )

    policy_json, quote_policy_sha256 = _build_execution_policy(source_release, args)
    execution_policy_sha256 = canonical_json_sha256(policy_json)
    execution_policy_version_id = str(args.execution_policy_version_id).strip()
    if not execution_policy_version_id:
        raise PilotActivationError("execution_policy_version_id is required")
    if quote_policy_sha256 == execution_policy_sha256:
        raise PilotActivationError("quote policy hash must remain distinct from the full execution policy hash")
    observation = {
        "schema_version": "miniqmt_b0_quote_v2_pilot_preregistration_v1",
        "source_runtime_id": str(args.observation_runtime_id).strip(),
        "tick_count": _strict_positive(args.observation_tick_count, field_name="observation_tick_count"),
        "transport_lag_p99_ms": float(args.observation_transport_lag_p99_ms),
        "transport_lag_max_ms": float(args.observation_transport_lag_max_ms),
        "registered_before_trade_date": args.trade_date.isoformat(),
        "no_strategy_package_created": True,
    }
    if not observation["source_runtime_id"]:
        raise PilotActivationError("observation_runtime_id is required")
    if observation["transport_lag_p99_ms"] < 0 or observation["transport_lag_max_ms"] < 0:
        raise PilotActivationError("observation lag values must be non-negative")
    if observation["transport_lag_p99_ms"] > observation["transport_lag_max_ms"]:
        raise PilotActivationError("observation p99 cannot exceed its maximum")

    source_metadata = source_release.release_config_json.get("metadata") or {}
    metadata = {
        "source": "miniqmt_b0_quote_v2_pilot",
        "purpose": "B0_QUOTE_V2_SIM_PILOT",
        "route": source_metadata.get("route"),
        "selection_runtime_config": deepcopy(source_metadata.get("selection_runtime_config")),
        "source_release_id": source_release.release_id,
        "source_binding_id": source_binding.binding_id,
        "target_trade_date": args.trade_date.isoformat(),
        "policy_preregistration": observation,
    }
    validation_evidence = {
        "design": "docs/architecture/miniqmt_adaptive_is_phase1_quote_contract_design.md",
        "feature_tier": "F2",
        "source_release_id": source_release.release_id,
        "source_binding_id": source_binding.binding_id,
        "target_trade_date": args.trade_date.isoformat(),
        "policy_preregistration": observation,
        "business_policy_preserved": {
            "algo_code": policy_json["algo_code"],
            "fallback_algo_code": policy_json.get("fallback_algo_code"),
            "unfilled_handler": policy_json.get("unfilled_handler"),
        },
    }
    memory_repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=memory_repo)
    release = service.create_release(
        package_id=source_release.package_id,
        manifest_sha256=source_release.manifest_sha256,
        runtime_profile_id=source_release.runtime_profile_id,
        runtime_profile_version_id=source_release.runtime_profile_version_id,
        runtime_profile_sha256=source_release.runtime_profile_sha256,
        daily_strategy_profile_version_id=source_release.daily_strategy_profile_version_id,
        execution_policy_version_id=execution_policy_version_id,
        execution_policy_sha256=execution_policy_sha256,
        tail_policy_version_id=source_release.tail_policy_version_id,
        tail_policy_sha256=source_release.tail_policy_sha256,
        execution_policy_json=policy_json,
        base_release_id=source_release.release_id,
        validation_state=source_release.validation_state,
        validation_evidence=validation_evidence,
        release_metadata=metadata,
        effective_from=args.trade_date,
        effective_to=args.trade_date,
        created_by=args.operator,
        created_reason="B0_QUOTE_V2 existing-package SIM pilot",
    )
    binding = service.create_binding(
        strategy_id=source_binding.strategy_id,
        release=release,
        broker_backend=source_binding.broker_backend,
        capital_allocation=source_binding.capital_allocation,
        broker_account_id=source_binding.broker_account_id,
        account_group_id=source_binding.account_group_id,
        strategy_slot_id=source_binding.strategy_slot_id,
        strategy_name=source_binding.strategy_name,
        order_remark_prefix=source_binding.order_remark_prefix,
        approval_state=source_binding.approval_state,
        binding_metadata={
            "source": "miniqmt_b0_quote_v2_pilot",
            "purpose": "B0_QUOTE_V2_SIM_PILOT",
            "source_release_id": source_release.release_id,
            "source_binding_id": source_binding.binding_id,
            "target_trade_date": args.trade_date.isoformat(),
            "no_strategy_package_created": True,
        },
        miniqmt_quote_control=QUOTE_CONTROL,
        effective_from=args.trade_date,
        effective_to=args.trade_date,
        created_by=args.operator,
        created_reason="B0_QUOTE_V2 existing-package SIM pilot",
    )
    build_manifest = source_build_manifest()
    benchmark_version, mark_version, markout_lag = quote_evidence_policy(policy_json)
    revision = B0QuoteV2RevisionV1.build(
        execution_policy=policy_json,
        execution_policy_version_id=release.execution_policy_version_id,
        execution_policy_sha256=release.execution_policy_sha256,
        adapter_version=build_manifest.adapter_version,
        adapter_sha256=build_manifest.adapter_sha256,
        code_revision=build_manifest.code_revision,
        code_sha256=build_manifest.code_sha256,
        evidence_schema_version=build_manifest.evidence_schema_version,
        evidence_schema_sha256=build_manifest.evidence_schema_sha256,
        benchmark_policy_version=benchmark_version,
        mark_policy_version=mark_version,
        markout_max_lag_ms=markout_lag,
    )
    return PilotArtifacts(
        source_release=source_release,
        source_binding=source_binding,
        release=release,
        binding=binding,
        execution_policy_json=policy_json,
        quote_policy_sha256=quote_policy_sha256,
        revision=revision,
        observation=observation,
    )


def _active_conflicts(repo: SimulationRuntimeRepository, artifacts: PilotArtifacts, trade_date: date) -> list[dict[str, Any]]:
    active = repo.list_simulation_release_bindings(
        strategy_id=artifacts.binding.strategy_id,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        active_on=trade_date,
        limit=100,
    )
    conflicts = []
    for item in active:
        if item.binding_id == artifacts.binding.binding_id and item.binding_hash == artifacts.binding.binding_hash:
            continue
        conflicts.append(
            {
                "binding_id": item.binding_id,
                "binding_hash": item.binding_hash,
                "release_id": item.release_id,
                "effective_from": item.effective_from.isoformat() if item.effective_from else None,
                "effective_to": item.effective_to.isoformat() if item.effective_to else None,
                "approval_state": item.approval_state.value,
            }
        )
    return conflicts


def _artifact_report(artifacts: PilotArtifacts) -> dict[str, Any]:
    return {
        "source": {
            "package_id": artifacts.source_release.package_id,
            "manifest_sha256": artifacts.source_release.manifest_sha256,
            "release_id": artifacts.source_release.release_id,
            "binding_id": artifacts.source_binding.binding_id,
            "control_revision": "LEGACY_B0_OMITTED",
        },
        "pilot": {
            "trade_date": artifacts.binding.effective_from.isoformat() if artifacts.binding.effective_from else None,
            "package_id": artifacts.release.package_id,
            "manifest_sha256": artifacts.release.manifest_sha256,
            "release_id": artifacts.release.release_id,
            "release_hash": artifacts.release.release_hash,
            "binding_id": artifacts.binding.binding_id,
            "binding_hash": artifacts.binding.binding_hash,
            "execution_policy_version_id": artifacts.release.execution_policy_version_id,
            "execution_policy_sha256": artifacts.release.execution_policy_sha256,
            "quote_policy_sha256": artifacts.quote_policy_sha256,
            "quote_control": QUOTE_CONTROL,
            "revision": artifacts.revision.canonical_payload(),
            "execution_policy_json": artifacts.execution_policy_json,
            "observation": artifacts.observation,
        },
        "invariants": {
            "same_strategy_package": artifacts.release.package_id == artifacts.source_release.package_id,
            "same_manifest": artifacts.release.manifest_sha256 == artifacts.source_release.manifest_sha256,
            "same_strategy_id": artifacts.binding.strategy_id == artifacts.source_binding.strategy_id,
            "same_account_group_id": artifacts.binding.account_group_id == artifacts.source_binding.account_group_id,
            "same_strategy_slot_id": artifacts.binding.strategy_slot_id == artifacts.source_binding.strategy_slot_id,
            "algo_code": artifacts.execution_policy_json.get("algo_code"),
            "fallback_algo_code": artifacts.execution_policy_json.get("fallback_algo_code"),
            "strategy_package_created": False,
            "broker_called": False,
        },
    }


def _read_artifacts(repo: SimulationRuntimeRepository, source_binding_id: str, args: argparse.Namespace) -> PilotArtifacts:
    source_binding = repo.get_simulation_release_binding(source_binding_id)
    source_release = repo.get_strategy_runtime_release(source_binding.release_id)
    return build_pilot_artifacts(source_release=source_release, source_binding=source_binding, args=args)


def _readback(env_file: Path | None, target_db: str, expected: PilotArtifacts, trade_date: date) -> dict[str, Any]:
    with _connection(env_file=env_file, target_db=target_db, readonly=True) as conn:
        repo = _repo(conn)
        release = repo.get_strategy_runtime_release(expected.release.release_id)
        binding = repo.get_simulation_release_binding(expected.binding.binding_id)
        active = repo.list_simulation_release_bindings(
            strategy_id=binding.strategy_id,
            broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
            active_on=trade_date,
            limit=100,
        )
    mismatches = {}
    expected_values = {
        "release_hash": expected.release.release_hash,
        "execution_policy_sha256": expected.release.execution_policy_sha256,
        "binding_hash": expected.binding.binding_hash,
        "binding_config_json": expected.binding.binding_config_json,
        "effective_from": expected.binding.effective_from,
        "effective_to": expected.binding.effective_to,
    }
    actual_values = {
        "release_hash": release.release_hash,
        "execution_policy_sha256": release.execution_policy_sha256,
        "binding_hash": binding.binding_hash,
        "binding_config_json": binding.binding_config_json,
        "effective_from": binding.effective_from,
        "effective_to": binding.effective_to,
    }
    for key, value in expected_values.items():
        if actual_values[key] != value:
            mismatches[key] = {"expected": value, "actual": actual_values[key]}
    active_ids = sorted(item.binding_id for item in active)
    if active_ids != [binding.binding_id]:
        mismatches["active_binding_ids"] = {"expected": [binding.binding_id], "actual": active_ids}
    readback_policy = release.release_config_json.get("execution_policy", {}).get("policy_json")
    if not isinstance(readback_policy, dict):
        mismatches["policy_json"] = {"expected": "object", "actual": type(readback_policy).__name__}
    else:
        readback_quote = QuoteContractPolicy.from_execution_policy(readback_policy)
        if readback_quote.policy_sha256 != expected.quote_policy_sha256:
            mismatches["quote_policy_sha256"] = {
                "expected": expected.quote_policy_sha256,
                "actual": readback_quote.policy_sha256,
            }
    if mismatches:
        raise PilotActivationError("production pilot readback differs from the immutable plan", context=mismatches)
    return {
        "status": "applied_and_verified",
        "release_id": release.release_id,
        "release_hash": release.release_hash,
        "binding_id": binding.binding_id,
        "binding_hash": binding.binding_hash,
        "active_binding_ids": active_ids,
        "quote_control": binding.binding_config_json.get("miniqmt_quote_control"),
        "broker_called": False,
        "production_runtime_restart_required_after_binding": False,
    }


def _validate_apply_gate(args: argparse.Namespace) -> None:
    if not args.apply:
        return
    if args.target_db == TARGET_PROD:
        if not args.confirm_production_dml:
            raise PilotActivationError("--apply on prod requires --confirm-production-dml")
        if os.environ.get(APPLY_CONFIRM_ENV) != APPLY_CONFIRM_VALUE:
            raise PilotActivationError(f"--apply on prod requires {APPLY_CONFIRM_ENV}={APPLY_CONFIRM_VALUE}")
    elif not args.confirm_scratch_dml:
        raise PilotActivationError("--apply --target-db dev requires --confirm-scratch-dml")


def run(args: argparse.Namespace) -> dict[str, Any]:
    _load_env_file(args.env_file)
    _validate_apply_gate(args)
    with _connection(env_file=args.env_file, target_db=args.target_db, readonly=True) as conn:
        readonly_repo = _repo(conn)
        artifacts = _read_artifacts(readonly_repo, args.source_binding_id, args)
        conflicts = _active_conflicts(readonly_repo, artifacts, args.trade_date)
    if conflicts:
        raise PilotActivationError(
            "target strategy/date already has a different active binding",
            context={"trade_date": args.trade_date.isoformat(), "conflicts": conflicts},
        )
    report = {
        "schema_version": "miniqmt_b0_quote_v2_pilot_plan_v1",
        "mode": "apply" if args.apply else "dry_run",
        "status": "ready_for_apply" if not args.apply else "applying",
        "target_db": args.target_db,
        **_artifact_report(artifacts),
        "conflicts": conflicts,
        "db_writes_executed": False,
    }
    if not args.apply:
        return report

    db_writes_executed = False
    with _connection(env_file=args.env_file, target_db=args.target_db, readonly=False) as conn:
        try:
            repo = _repo(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (f"miniqmt_b0_quote_v2_pilot:{artifacts.binding.strategy_id}:{args.trade_date.isoformat()}",),
                )
            current_source = repo.get_simulation_release_binding(args.source_binding_id)
            if current_source.binding_hash != artifacts.source_binding.binding_hash:
                raise PilotActivationError("source binding changed after dry-run", context={"binding_id": args.source_binding_id})
            transactional_artifacts = _read_artifacts(repo, args.source_binding_id, args)
            conflicts = _active_conflicts(repo, transactional_artifacts, args.trade_date)
            if conflicts:
                raise PilotActivationError(
                    "target strategy/date acquired a conflicting binding during apply",
                    context={"conflicts": conflicts},
                )
            existing_release = repo.get_strategy_runtime_release_by_hash(transactional_artifacts.release.release_hash or "")
            existing_binding = repo.get_simulation_release_binding_by_hash(transactional_artifacts.binding.binding_hash or "")
            saved_release = existing_release or repo.save_strategy_runtime_release(transactional_artifacts.release)
            if saved_release.release_hash != transactional_artifacts.release.release_hash:
                raise PilotActivationError("saved release hash differs from the immutable plan")
            saved_binding = existing_binding or repo.save_simulation_release_binding(transactional_artifacts.binding)
            if saved_binding.binding_hash != transactional_artifacts.binding.binding_hash:
                raise PilotActivationError("saved binding hash differs from the immutable plan")
            db_writes_executed = existing_release is None or existing_binding is None
            conn.commit()
            artifacts = transactional_artifacts
        except Exception:
            conn.rollback()
            raise
    report["status"] = "applied" if db_writes_executed else "already_current"
    report["db_writes_executed"] = db_writes_executed
    report["readback"] = _readback(args.env_file, args.target_db, artifacts, args.trade_date)
    return report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan or apply one existing-package B0_QUOTE_V2 MiniQMT SIM pilot.")
    parser.add_argument("--env-file", type=Path, default=Path(os.environ.get("AISTOCK_ENV_FILE", ".env")))
    parser.add_argument("--target-db", choices=(TARGET_PROD, TARGET_DEV), default=TARGET_PROD)
    parser.add_argument("--source-binding-id", required=True)
    parser.add_argument("--trade-date", type=date.fromisoformat, required=True)
    parser.add_argument("--execution-policy-version-id", required=True)
    parser.add_argument("--max-receive-age-ms", type=int, required=True)
    parser.add_argument("--max-source-lag-ms", type=int, required=True)
    parser.add_argument("--max-exchange-age-ms", type=int, required=True)
    parser.add_argument("--max-negative-skew-ms", type=int, required=True)
    parser.add_argument("--max-clock-age-divergence-ms", type=int, required=True)
    parser.add_argument("--max-dependency-group-skew-ms", type=int, required=True)
    parser.add_argument("--benchmark-max-age-ms", type=int, required=True)
    parser.add_argument("--arrival-forward-window-ms", type=int, required=True)
    parser.add_argument("--clock-skew-tolerance-ms", type=int, required=True)
    parser.add_argument("--benchmark-max-transport-latency-ms", type=int, required=True)
    parser.add_argument("--benchmark-policy-version", required=True)
    parser.add_argument("--mark-policy-version", required=True)
    parser.add_argument("--markout-max-lag-ms", type=int, required=True)
    parser.add_argument("--observation-runtime-id", required=True)
    parser.add_argument("--observation-tick-count", type=int, required=True)
    parser.add_argument("--observation-transport-lag-p99-ms", type=float, required=True)
    parser.add_argument("--observation-transport-lag-max-ms", type=float, required=True)
    parser.add_argument("--operator", default="miniqmt_b0_quote_v2_pilot")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-production-dml", action="store_true")
    parser.add_argument("--confirm-scratch-dml", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    report = run(args)
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (PilotActivationError, TradingCoreError, ValueError) as exc:
        print(
            json.dumps(
                {"ok": False, "error": str(exc), "context": getattr(exc, "context", {})},
                ensure_ascii=False,
                default=str,
            )
        )
        raise SystemExit(1)
