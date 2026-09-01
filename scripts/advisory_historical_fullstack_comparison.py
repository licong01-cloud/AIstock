#!/usr/bin/env python3
"""Start or inspect the strict A/B historical comparison as a resumable long task."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import date
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import BackgroundTasks
import psycopg2
from psycopg2.pool import ThreadedConnectionPool


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.advisory_historical_range.api_models import (  # noqa: E402
    HistoricalRangeCreateRequest,
    HistoricalRangeRefreshOutcomesRequest,
    ResearchProgramInput,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256  # noqa: E402
from backend.services.advisory_historical_range.composition import (  # noqa: E402
    build_explicit_historical_range_r5_runtime_factory,
    build_historical_range_r5_application_service,
    explicit_historical_range_connection_factory,
)
from backend.services.hmm_training_service import HMMTrainingService  # noqa: E402


PACKAGE_ID = "pkg_ma_8ec5e389fa2c5e484a1ac7e9"
SNAPSHOT_ID = "bbec3863-fb67-445f-938e-66f092d18696"
MODEL_CONFIG_ID = "b99c907b-873a-4173-a4ee-5eab266f8c49"
SIGNAL_PRESET = "preset_A"
START_DATE = date(2026, 5, 15)
END_DATE = date(2026, 7, 16)
ORCHESTRATION_REVISION = 6
DATABASE_MAX_PARALLEL_WORKERS_PER_GATHER = 0
DATABASE_WORK_MEM = "64MB"
CANDIDATE_PREFETCH_PER_PROGRAM = 1
DEFAULT_STATE_ROOT = Path("F:/Dev/AIstock_model_artifacts/advisory_fullstack_comparison")
DEFAULT_COEFFICIENT_PATH = (
    DEFAULT_STATE_ROOT / "hmm" / "coefficients_preset_A_2026-05-15_2026-07-16_pit_v3.json"
)
MODEL_BUNDLE_ID = "1757b24b854cf8b5bfee8874bd442491091ea979c86522fbeef15a02930f8ecb"
MODEL_RUNTIME_SEMANTICS_HASH = "83fc0475964df75a9a23db597567af5bf31543f6980170f9d924c650ea3eb692"
REVIEW_POLICY = {
    "rank_enter_threshold": 20,
    "rank_exit_threshold": 40,
    "rank_exit_confirm_days": 2,
    "daily_replacement_budget": 5,
    "stop_loss_bps": 800,
    "take_profit_bps": 1800,
    "trailing_stop_bps": 700,
    "time_stop_days": 20,
    "take_profit_mode": "trailing",
}


class FrozenSnapshotProvider:
    def __init__(self, *, evidence: dict[str, Any]) -> None:
        self._evidence = dict(evidence)
        self._delegate = HMMTrainingService()

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        if snapshot_id != SNAPSHOT_ID:
            return None
        snapshot = self._delegate.get_snapshot(snapshot_id)
        if snapshot is None:
            return None
        if (
            str(snapshot.get("config_id")) != MODEL_CONFIG_ID
            or _sha256_file(Path(str(snapshot.get("model_path"))))
            != self._evidence["model_artifact_sha256"]
        ):
            raise RuntimeError("frozen HMM snapshot readback differs from the comparison contract")
        return {
            **snapshot,
            "available_at": self._evidence["available_at"],
            "training_information_cutoff": self._evidence["training_information_cutoff"],
            "input_data_max_dates": self._evidence["input_data_max_dates"],
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=(
            "start",
            "status",
            "refresh-outcomes",
            "correct-outcomes",
            "run-c",
            "analyze",
        ),
    )
    parser.add_argument(
        "--env-file",
        default=os.environ.get("AISTOCK_ENV_FILE", str(REPOSITORY_ROOT / ".env")),
    )
    parser.add_argument("--state-root", default=str(DEFAULT_STATE_ROOT))
    parser.add_argument("--coefficients-path", default=str(DEFAULT_COEFFICIENT_PATH))
    args = parser.parse_args()

    load_dotenv(args.env_file, override=False)
    state_root = Path(args.state_root).resolve()
    state_root.mkdir(parents=True, exist_ok=True)
    state_path = _state_path(state_root)
    coefficient_path = Path(args.coefficients_path).resolve()
    current_contract = _comparison_contract(coefficient_path)
    contract = (
        current_contract
        if args.command == "start"
        else _load_frozen_contract(state_root=state_root, current_contract=current_contract)
    )
    evidence = contract["hmm_evidence"]
    service = _service(state_root=state_root, snapshot_provider=FrozenSnapshotProvider(evidence=evidence))

    if args.command == "run-c":
        _run_challenger(state_root=state_root, service=service, contract=contract)
        return
    if args.command == "refresh-outcomes":
        _refresh_outcomes(state_root=state_root, service=service, contract=contract)
        return
    if args.command == "correct-outcomes":
        _correct_outcomes(state_root=state_root, service=service, contract=contract)
        return
    if args.command == "analyze":
        _analyze(state_root=state_root, service=service, contract=contract)
        return

    if args.command == "status":
        state = _read_state(state_path)
        if not state:
            print(json.dumps({"status": "NOT_STARTED", "contract_hash": contract["contract_hash"]}))
            return
        batch = service.get_batch(str(state["batch_id"]))
        runs = service.list_runs(str(state["batch_id"]), limit=20)["items"]
        outcome_operation = None
        if state.get("outcome_operation_id"):
            outcome_operation = service.get_operation(str(state["outcome_operation_id"]))
        correction_operation = None
        if state.get("outcome_correction_operation_id"):
            correction_operation = service.get_operation(
                str(state["outcome_correction_operation_id"])
            )
        _write_state(
            state_path,
            {
                **state,
                "batch_status": batch["status"],
                "batch_row_version": batch["row_version"],
                "runs": _run_state(runs),
                "outcome_child_operations": _outcome_child_operations(
                    str(state["batch_id"])
                ),
                **(
                    {
                        "outcome_operation_status": outcome_operation["status"],
                        "outcome_result_status": outcome_operation.get("result_status"),
                        "outcome_operation_updated_at": str(
                            outcome_operation.get("updated_at") or ""
                        ),
                        "outcome_operation_lease_expires_at": str(
                            outcome_operation.get("lease_expires_at") or ""
                        ),
                        "outcome_operation_lease_expired": bool(
                            outcome_operation.get("lease_expired")
                        ),
                    }
                    if outcome_operation is not None
                    else {}
                ),
                **(
                    {
                        "outcome_correction_operation_status": correction_operation["status"],
                        "outcome_correction_result_status": correction_operation.get(
                            "result_status"
                        ),
                        "outcome_correction_operation_updated_at": str(
                            correction_operation.get("updated_at") or ""
                        ),
                        "outcome_correction_operation_lease_expires_at": str(
                            correction_operation.get("lease_expires_at") or ""
                        ),
                        "outcome_correction_operation_lease_expired": bool(
                            correction_operation.get("lease_expired")
                        ),
                    }
                    if correction_operation is not None
                    else {}
                ),
            },
        )
        print(json.dumps(_read_state(state_path), ensure_ascii=False, sort_keys=True))
        return

    state = _read_state(state_path)
    if state and state.get("contract_hash") != contract["contract_hash"]:
        raise RuntimeError("ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT: state contract differs")
    if state and state.get("batch_id"):
        batch = service.get_batch(str(state["batch_id"]))
        print(
            json.dumps(
                {
                    "status": "ALREADY_STARTED",
                    "batch_id": state["batch_id"],
                    "batch_status": batch["status"],
                    "contract_hash": contract["contract_hash"],
                },
                sort_keys=True,
            )
        )
        return

    request = HistoricalRangeCreateRequest(
        program_specs=[
            ResearchProgramInput(
                source_kind="RESEARCH_PROGRAM_SPEC",
                program_name="advisory-fullstack-A-raw-selection-20260815",
                package_id=PACKAGE_ID,
                target_count=20,
                review_policy=REVIEW_POLICY,
                runtime_config=contract["arms"]["A"]["runtime_config"],
            ),
            ResearchProgramInput(
                source_kind="RESEARCH_PROGRAM_SPEC",
                program_name="advisory-fullstack-B-hmm-risk-20260815",
                package_id=PACKAGE_ID,
                target_count=20,
                review_policy=REVIEW_POLICY,
                runtime_config=contract["arms"]["B"]["runtime_config"],
            ),
        ],
        start_trade_date=START_DATE,
        end_trade_date=END_DATE,
    )
    background = BackgroundTasks()
    response = service.create_batch(
        request,
        idempotency_key=f"advisory-fullstack-ab-{contract['contract_hash'][:32]}",
        background_tasks=background,
        requested_by="codex-advisory-fullstack-comparison",
    )
    response_data = response["data"]
    batch = response_data["batch"]
    _write_state(
        state_path,
        {
            "schema_version": "advisory_fullstack_comparison_long_task_state_v1",
            "contract_hash": contract["contract_hash"],
            "contract_path": str(state_root / f"comparison_contract_v{ORCHESTRATION_REVISION}.json"),
            "batch_id": batch["batch_id"],
            "batch_status": batch["status"],
            "batch_row_version": batch["row_version"],
            "catalog_operation_id": response_data["operation"]["operation_id"],
            "runs": [],
        },
    )
    _write_json(state_root / f"comparison_contract_v{ORCHESTRATION_REVISION}.json", contract)
    print(
        json.dumps(
            {
                "status": "STARTED",
                "batch_id": batch["batch_id"],
                "catalog_operation_id": response_data["operation"]["operation_id"],
                "contract_hash": contract["contract_hash"],
            },
            sort_keys=True,
        ),
        flush=True,
    )
    asyncio.run(background())
    final_batch = service.get_batch(batch["batch_id"])
    runs = service.list_runs(batch["batch_id"], limit=20)["items"]
    final_state = {
        **_read_state(state_path),
        "batch_status": final_batch["status"],
        "batch_row_version": final_batch["row_version"],
        "runs": _run_state(runs),
    }
    _write_state(state_path, final_state)
    print(json.dumps(final_state, ensure_ascii=False, sort_keys=True), flush=True)


def _comparison_contract(coefficient_path: Path) -> dict[str, Any]:
    if not coefficient_path.is_file():
        raise RuntimeError(f"HMM coefficient artifact is missing: {coefficient_path}")
    coefficient_payload = json.loads(coefficient_path.read_text(encoding="utf-8"))
    dates = sorted(str(value) for value in coefficient_payload.get("daily_coefficients") or {})
    expected_dates = _database_trade_dates()
    if dates != expected_dates:
        raise RuntimeError(
            f"ADVISORY_COMPARISON_HMM_EVIDENCE_INCOMPLETE: expected {len(expected_dates)} dates, got {len(dates)}"
        )
    maps_by_date = coefficient_payload.get("stock_sector_map_by_date")
    if not isinstance(maps_by_date, dict) or sorted(maps_by_date) != expected_dates:
        raise RuntimeError("ADVISORY_COMPARISON_HMM_EVIDENCE_INCOMPLETE: PIT sector maps do not close")
    snapshot = _snapshot_evidence()
    coefficient_sha256 = _sha256_file(coefficient_path)
    input_dates_by_date = coefficient_payload.get("input_data_max_dates_by_date")
    if not isinstance(input_dates_by_date, dict) or sorted(input_dates_by_date) != expected_dates:
        raise RuntimeError("ADVISORY_COMPARISON_HMM_EVIDENCE_INCOMPLETE: input watermarks do not close")
    metadata_by_date = {
        trade_date: {
            "model_config_id": MODEL_CONFIG_ID,
            "model_snapshot_id": SNAPSHOT_ID,
            "signal_preset": SIGNAL_PRESET,
            "model_artifact_sha256": snapshot["model_artifact_sha256"],
            "coefficient_sha256": coefficient_sha256,
            "snapshot_trained_at": snapshot["snapshot_trained_at"],
            "available_at": snapshot["available_at"],
            "training_information_cutoff": snapshot["training_information_cutoff"],
            "as_of_trade_date": trade_date,
            "effective_trade_date": trade_date,
            "generation_mode": "EXACT_SNAPSHOT",
            "input_data_max_dates": {
                **snapshot["input_data_max_dates"],
                **input_dates_by_date[trade_date],
            },
            "coefficients_path": str(coefficient_path),
        }
        for trade_date in expected_dates
    }
    for metadata in metadata_by_date.values():
        metadata["input_data_max_dates_hash"] = canonical_json_sha256(
            metadata["input_data_max_dates"]
        )
    a_runtime = {
        "runtime_profile": {
            "hmm": {"enabled": False},
            "risk_policy": {"enabled": False},
            "tradability": {"exclude_suspended": True},
            "selection": {"top_k": 50},
            "industry_blacklist": [],
        }
    }
    b_runtime = {
        "phase0a_hmm_metadata_by_date": metadata_by_date,
        "runtime_profile": {
            "hmm": {
                "enabled": True,
                "model_config_id": MODEL_CONFIG_ID,
                "model_snapshot_id": SNAPSHOT_ID,
                "signal_preset": SIGNAL_PRESET,
                "coefficients_path": str(coefficient_path),
                "auto_compute": False,
                "manual_snapshot_required": True,
                "missing_sector_policy": "exclude_candidate",
            },
            "risk_policy": {
                "enabled": True,
                "providers": ["st_pit"],
                "hard_actions": ["block_buy", "force_exit"],
                "strict_data_ready": True,
                "score_overlay": {"enabled": False},
            },
            "tradability": {"exclude_suspended": True},
            "selection": {"top_k": 50},
            "industry_blacklist": [],
        },
    }
    payload = {
        "schema_version": "advisory_historical_fullstack_comparison_contract_v1",
        "orchestration_revision": ORCHESTRATION_REVISION,
        "start_trade_date": START_DATE.isoformat(),
        "end_trade_date": END_DATE.isoformat(),
        "ordered_trade_dates": expected_dates,
        "package_id": PACKAGE_ID,
        "target_count": 20,
        "review_policy": REVIEW_POLICY,
        "hmm_evidence": {
            **snapshot,
            "snapshot_id": SNAPSHOT_ID,
            "model_config_id": MODEL_CONFIG_ID,
            "signal_preset": SIGNAL_PRESET,
            "coefficient_path": str(coefficient_path),
            "coefficient_sha256": coefficient_sha256,
            "coefficient_payload_sha256": canonical_json_sha256(coefficient_payload),
        },
        "arms": {
            "A": {"runtime_config": a_runtime},
            "B": {"runtime_config": b_runtime},
        },
        "runtime_activation": "NOOP",
        "service_restart": "NOOP_USER_OWNED",
        "production_ddl": "NOOP",
        "database_session": {
            "max_parallel_workers_per_gather": DATABASE_MAX_PARALLEL_WORKERS_PER_GATHER,
            "work_mem": DATABASE_WORK_MEM,
            "candidate_prefetch_per_program": CANDIDATE_PREFETCH_PER_PROGRAM,
        },
        "implementation_source_hashes": {
            str(path.relative_to(REPOSITORY_ROOT)).replace("\\", "/"): _sha256_file(path)
            for path in (
                REPOSITORY_ROOT / "backend/services/selection_center/hmm_runtime.py",
                REPOSITORY_ROOT
                / "backend/services/advisory_historical_range/candidate_producer.py",
                REPOSITORY_ROOT / "backend/services/advisory_historical_range/model_challenger.py",
                REPOSITORY_ROOT / "backend/services/advisory_model_first/model_bundle.py",
                REPOSITORY_ROOT / "backend/services/advisory_model_first/model_inference.py",
                REPOSITORY_ROOT / "scripts/precompute_hmm_coefficients.py",
            )
        },
    }
    payload["contract_hash"] = canonical_json_sha256(payload)
    return payload


def _load_frozen_contract(
    *, state_root: Path, current_contract: dict[str, Any]
) -> dict[str, Any]:
    """Load the immutable A/B contract for read/resume commands.

    Source changes made after A/B completion must not rewrite the experiment
    identity.  Challenger code has its own fingerprint in challenger state.
    """

    state = _read_state(_state_path(state_root))
    if not state:
        return current_contract
    contract_path = Path(str(state.get("contract_path") or ""))
    if not contract_path.is_file():
        raise RuntimeError("ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT: frozen contract is missing")
    frozen = _read_state(contract_path)
    frozen_hash = str(frozen.get("contract_hash") or "")
    unsigned = {key: value for key, value in frozen.items() if key != "contract_hash"}
    if (
        not frozen_hash
        or canonical_json_sha256(unsigned) != frozen_hash
        or state.get("contract_hash") != frozen_hash
    ):
        raise RuntimeError("ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT: frozen contract is invalid")
    return frozen


def _challenger_implementation_hash() -> str:
    paths = (
        REPOSITORY_ROOT / "backend/services/advisory_historical_range/model_challenger.py",
        REPOSITORY_ROOT / "backend/services/advisory_historical_range/wsl_model_scorer.py",
        REPOSITORY_ROOT / "backend/services/advisory_model_first/model_bundle.py",
        REPOSITORY_ROOT / "backend/services/advisory_model_first/model_inference.py",
        REPOSITORY_ROOT / "scripts/wsl/advisory_historical_model_predict.py",
        REPOSITORY_ROOT / "scripts/advisory_historical_fullstack_comparison.py",
    )
    return canonical_json_sha256(
        {
            str(path.relative_to(REPOSITORY_ROOT)).replace("\\", "/"): _sha256_file(path)
            for path in paths
        }
    )


def _run_challenger(*, state_root: Path, service: Any, contract: dict[str, Any]) -> None:
    from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
    from backend.services.advisory_historical_range.fullstack_comparison import (
        HistoricalComparisonArtifactRefV1,
        HistoricalComparisonArtifactStore,
    )
    from backend.services.advisory_historical_range.model_challenger import HistoricalModelChallenger
    from backend.services.advisory_historical_range.wsl_model_scorer import (
        WslFrozenFeatureMatrixScorer,
        load_deferred_frozen_research_bundle,
    )
    from backend.services.advisory_historical_range.models import (
        HistoricalRangeArtifactRefV1,
        HistoricalRangeCandidateArtifactPayloadV2,
    )

    state_path = _state_path(state_root)
    state = _read_state(state_path)
    if not state or state.get("contract_hash") != contract["contract_hash"]:
        raise RuntimeError("ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT: A/B state is unavailable")
    batch = service.get_batch(str(state["batch_id"]))
    if batch["status"] != "COMPLETED":
        raise RuntimeError(f"ADVISORY_COMPARISON_PARENT_NOT_READY: batch_status={batch['status']}")
    runs = service.list_runs(str(state["batch_id"]), limit=20)["items"]
    controls = [
        run
        for run in runs
        if not bool(
            (((run.get("frozen_program_json") or {}).get("runtime_config") or {}).get("runtime_profile") or {})
            .get("hmm", {})
            .get("enabled")
        )
        and not bool(
            (((run.get("frozen_program_json") or {}).get("runtime_config") or {}).get("runtime_profile") or {})
            .get("risk_policy", {})
            .get("enabled")
        )
    ]
    if len(controls) != 1 or controls[0].get("status") != "COMPLETED":
        raise RuntimeError("ADVISORY_COMPARISON_PARENT_CANDIDATE_MISMATCH: raw control run is ambiguous")
    parent_run_id = str(controls[0]["range_run_id"])
    day_page = service.list_days(parent_run_id, limit=500)
    days = sorted(day_page["items"], key=lambda item: (item["ordinal"], item["day_run_id"]))
    if day_page["page"]["has_more"] or len(days) != len(contract["ordered_trade_dates"]):
        raise RuntimeError("ADVISORY_COMPARISON_PARENT_CANDIDATE_MISMATCH: parent day set is incomplete")
    historical_store = HistoricalRangeArtifactStore(
        root=Path(os.environ["AISTOCK_ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT"])
    )
    comparison_store = HistoricalComparisonArtifactStore(root=state_root / "comparison-artifacts")
    model_root = str(os.getenv("AISTOCK_ADVISORY_MODEL_ROOT") or "").strip()
    if not model_root:
        raise RuntimeError("ADVISORY_MODEL_ROOT_NOT_CONFIGURED")
    c_state_path = state_root / f"challenger_state_v{ORCHESTRATION_REVISION}.json"
    challenger_implementation_hash = _challenger_implementation_hash()
    c_state = _read_state(c_state_path) or {
        "schema_version": "advisory_historical_model_challenger_state_v2",
        "contract_hash": contract["contract_hash"],
        "parent_range_run_id": parent_run_id,
        "bundle_id": MODEL_BUNDLE_ID,
        "challenger_implementation_hash": challenger_implementation_hash,
        "days": {},
    }
    if not c_state.get("challenger_implementation_hash"):
        if any(
            isinstance(day, dict) and day.get("status") == "COMPLETE"
            for day in (c_state.get("days") or {}).values()
        ):
            raise RuntimeError(
                "ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT: completed challenger state lacks fingerprint"
            )
        c_state["schema_version"] = "advisory_historical_model_challenger_state_v2"
        c_state["challenger_implementation_hash"] = challenger_implementation_hash
    elif c_state.get("challenger_implementation_hash") != challenger_implementation_hash:
        if any(
            isinstance(day, dict) and day.get("status") == "COMPLETE"
            for day in (c_state.get("days") or {}).values()
        ):
            raise RuntimeError(
                "ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT: completed challenger fingerprint differs"
            )
        c_state["challenger_implementation_hash"] = challenger_implementation_hash
        c_state["days"] = {}
    if (
        c_state.get("contract_hash") != contract["contract_hash"]
        or c_state.get("parent_range_run_id") != parent_run_id
        or c_state.get("bundle_id") != MODEL_BUNDLE_ID
        or c_state.get("challenger_implementation_hash") != challenger_implementation_hash
    ):
        raise RuntimeError("ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT: challenger state differs")
    challenger = HistoricalModelChallenger(
        bundle_loader=load_deferred_frozen_research_bundle,
        scorer=WslFrozenFeatureMatrixScorer(repo_root=REPOSITORY_ROOT),
    )
    for day in days:
        trade_date = str(day["decision_trade_date"])
        parent_ref = HistoricalRangeArtifactRefV1.model_validate(day["candidate_artifact_ref"])
        existing = (c_state.get("days") or {}).get(trade_date)
        if isinstance(existing, dict) and existing.get("status") == "COMPLETE":
            ref = HistoricalComparisonArtifactRefV1(**existing["artifact_ref"])
            loaded = comparison_store.load_challenger(ref)
            if loaded.parent_candidate_artifact_hash != parent_ref.semantic_content_hash:
                raise RuntimeError("ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT: challenger parent differs")
            continue
        started = time.monotonic()
        try:
            envelope = historical_store.load(parent_ref)
            parent = HistoricalRangeCandidateArtifactPayloadV2.model_validate(envelope.payload)
            target_trade_date = _next_trade_date(parent.decision_trade_date)
            artifact = challenger.score_day(
                parent=parent,
                parent_candidate_artifact_hash=parent_ref.semantic_content_hash,
                target_trade_date=target_trade_date,
                model_root=model_root,
                bundle_id=MODEL_BUNDLE_ID,
                expected_selection_runtime_semantics_hash=MODEL_RUNTIME_SEMANTICS_HASH,
            )
            artifact_ref = comparison_store.publish_challenger(artifact)
            readback = comparison_store.load_challenger(artifact_ref)
            if readback.artifact_hash != artifact.artifact_hash:
                raise RuntimeError("ADVISORY_COMPARISON_ARTIFACT_READBACK_MISMATCH")
            c_state["days"][trade_date] = {
                "status": "COMPLETE",
                "target_trade_date": target_trade_date.isoformat(),
                "parent_candidate_artifact_hash": parent_ref.semantic_content_hash,
                "artifact_ref": artifact_ref.__dict__,
                "duration_seconds": round(time.monotonic() - started, 3),
                "candidate_count": artifact.candidate_count,
                "shortlist_count": artifact.shortlist_count,
                "hmm_unavailable_count": len(artifact.hmm_unavailable),
            }
            _write_state(c_state_path, c_state)
            print(
                json.dumps(
                    {
                        "arm": "C",
                        "trade_date": trade_date,
                        "status": "COMPLETE",
                        "artifact_hash": artifact.artifact_hash,
                        "duration_seconds": c_state["days"][trade_date]["duration_seconds"],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        except Exception as exc:
            error_context = getattr(exc, "context", None)
            c_state["days"][trade_date] = {
                "status": "FAILED",
                "parent_candidate_artifact_hash": parent_ref.semantic_content_hash,
                "error_type": type(exc).__name__,
                "reason_code": str(
                    getattr(
                        exc,
                        "reason_code",
                        "ADVISORY_COMPARISON_MODEL_CHALLENGER_FAILED",
                    )
                ),
                "error_context": dict(error_context) if isinstance(error_context, dict) else {},
            }
            _write_state(c_state_path, c_state)
            raise
    c_state["status"] = "COMPLETED"
    c_state["completed_day_count"] = len(c_state["days"])
    _write_state(c_state_path, c_state)
    print(
        json.dumps(
            {
                "arm": "C",
                "status": "COMPLETED",
                "parent_range_run_id": parent_run_id,
                "completed_day_count": len(c_state["days"]),
            },
            sort_keys=True,
        ),
        flush=True,
    )


def _refresh_outcomes(*, state_root: Path, service: Any, contract: dict[str, Any]) -> None:
    state_path = _state_path(state_root)
    state = _read_state(state_path)
    if not state or state.get("contract_hash") != contract["contract_hash"]:
        raise RuntimeError("ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT: A/B state is unavailable")
    batch_id = str(state["batch_id"])
    batch = service.get_batch(batch_id)
    if batch["status"] != "COMPLETED":
        raise RuntimeError(f"ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: batch_status={batch['status']}")
    runs = service.list_runs(batch_id, limit=20)["items"]
    run_ids = sorted(str(run["range_run_id"]) for run in runs)
    if len(run_ids) != 2 or any(run["status"] != "COMPLETED" for run in runs):
        raise RuntimeError("ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: A/B runs are not complete")
    retry_generation = int(state.get("outcome_retry_generation") or 0)
    if state.get("outcome_operation_id"):
        previous = service.get_operation(str(state["outcome_operation_id"]))
        if previous["status"] in {"QUEUED", "RUNNING"}:
            raise RuntimeError(
                "ADVISORY_COMPARISON_OUTCOME_IN_PROGRESS: existing outcome operation is active"
            )
        if previous["status"] == "COMPLETED":
            state["outcome_operation_status"] = previous["status"]
            state["outcome_result_status"] = previous.get("result_status")
            _write_state(state_path, state)
            print(json.dumps(state, ensure_ascii=False, sort_keys=True), flush=True)
            return
        retry_generation += 1
    request = HistoricalRangeRefreshOutcomesRequest(
        operation_idempotency_key=(
            f"advisory-fullstack-outcomes-{contract['contract_hash'][:32]}-r{retry_generation}"
        ),
        expected_row_version=int(batch["row_version"]),
        label_as_of_trade_date=_label_as_of_trade_date(),
        range_run_ids=run_ids,
        horizons=[1, 3, 5, 10, 20],
    )
    background = BackgroundTasks()
    response = service.refresh_outcomes(batch_id, request, background_tasks=background)["data"]
    state["outcome_operation_id"] = response["operation"]["operation_id"]
    state["outcome_retry_generation"] = retry_generation
    state["outcome_operation_status"] = response["operation"]["status"]
    state["outcome_label_as_of_trade_date"] = request.label_as_of_trade_date.isoformat()
    _write_state(state_path, state)
    print(
        json.dumps(
            {
                "status": "OUTCOME_REFRESH_STARTED",
                "batch_id": batch_id,
                "operation_id": state["outcome_operation_id"],
                "label_as_of_trade_date": state["outcome_label_as_of_trade_date"],
            },
            sort_keys=True,
        ),
        flush=True,
    )
    asyncio.run(background())
    operation = service.get_operation(str(state["outcome_operation_id"]))
    state["outcome_operation_status"] = operation["status"]
    state["outcome_result_status"] = operation.get("result_status")
    _write_state(state_path, state)
    print(json.dumps(state, ensure_ascii=False, sort_keys=True), flush=True)


def _label_as_of_trade_date() -> date:
    conn_factory = _comparison_connection_factory()
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT LEAST(
                    (SELECT MAX(trade_date) FROM market.kline_daily_raw),
                    (SELECT MAX(trade_date) FROM market.index_daily),
                    (SELECT MAX(cal_date) FROM market.trading_calendar WHERE is_trading=TRUE)
                )
                """
            )
            row = cur.fetchone()
        conn.rollback()
    if row is None or row[0] is None or row[0] <= END_DATE:
        raise RuntimeError("ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: label source watermark is insufficient")
    return row[0]


def _correct_outcomes(*, state_root: Path, service: Any, contract: dict[str, Any]) -> None:
    from backend.services.advisory_historical_range.artifact_store import (
        HistoricalRangeArtifactStore,
    )
    from backend.services.advisory_historical_range.models import (
        HistoricalRangeArtifactKind,
        HistoricalRangeOutcomeRevisionReason,
    )
    from backend.services.advisory_historical_range.runtime_factories import (
        HistoricalRangeR5DerivedIdentities,
    )

    state_path = _state_path(state_root)
    state = _read_state(state_path)
    if not state or state.get("contract_hash") != contract["contract_hash"]:
        raise RuntimeError("ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT: A/B state is unavailable")
    if not state.get("outcome_operation_id"):
        raise RuntimeError("ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: base outcome operation is absent")
    base_operation = service.get_operation(str(state["outcome_operation_id"]))
    if base_operation["status"] in {"QUEUED", "RUNNING"}:
        raise RuntimeError("ADVISORY_COMPARISON_OUTCOME_IN_PROGRESS: base outcome operation is active")
    if base_operation["status"] != "COMPLETED":
        raise RuntimeError(
            f"ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: base_status={base_operation['status']}"
        )
    batch_id = str(state["batch_id"])
    batch = service.get_batch(batch_id)
    runs = service.list_runs(batch_id, limit=20)["items"]
    run_ids = sorted(str(item["range_run_id"]) for item in runs)
    logical_ids, prior_hashes, affected_run_ids = _failed_episode_outcome_scope(run_ids)
    if not logical_ids:
        state["outcome_correction_status"] = "NO_FAILED_EPISODE_OUTCOMES"
        _write_state(state_path, state)
        print(json.dumps(state, ensure_ascii=False, sort_keys=True), flush=True)
        return
    identities = HistoricalRangeR5DerivedIdentities.from_repository(REPOSITORY_ROOT)
    corrected_hash = identities.outcome_producer_hash
    if corrected_hash in prior_hashes or len(prior_hashes) != 1:
        raise RuntimeError(
            "ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT: correction producer identity is not a unique advance"
        )
    correction_retry_generation = int(
        state.get("outcome_correction_retry_generation") or 0
    )
    if state.get("outcome_correction_operation_id"):
        previous = service.get_operation(str(state["outcome_correction_operation_id"]))
        if previous["status"] in {"QUEUED", "RUNNING"}:
            raise RuntimeError("ADVISORY_COMPARISON_OUTCOME_IN_PROGRESS: correction is active")
        if previous["status"] == "COMPLETED":
            state["outcome_correction_operation_status"] = previous["status"]
            state["outcome_correction_result_status"] = previous.get("result_status")
            _write_state(state_path, state)
            print(json.dumps(state, ensure_ascii=False, sort_keys=True), flush=True)
            return
        correction_retry_generation += 1
    resolved_request_hash = str(batch.get("request_payload_sha256") or "")
    if len(resolved_request_hash) != 64:
        raise RuntimeError("ADVISORY_COMPARISON_CONTRACT_MISMATCH: batch request hash is absent")
    evidence_payload = {
        "schema_version": "advisory_fullstack_outcome_calculation_correction_evidence_v1",
        "batch_id": batch_id,
        "range_run_ids": affected_run_ids,
        "contract_hash": contract["contract_hash"],
        "target_outcome_logical_ids": logical_ids,
        "prior_producer_code_hashes": sorted(prior_hashes),
        "corrected_producer_code_hash": corrected_hash,
        "reason_codes": [
            "BUG_1110_CLOSED_EPISODE_T1_TIMELINE",
            "BUG_1111_OUTCOME_PLANNER_PRODUCER_IDENTITY",
        ],
        "bug_refs": ["BUG-1110", "BUG-1111"],
        "pr_refs": [
            "https://github.com/licong01-cloud/AIstock/pull/3539",
            "https://github.com/licong01-cloud/AIstock/pull/3543",
        ],
    }
    evidence_ref = HistoricalRangeArtifactStore(
        root=Path(os.environ["AISTOCK_ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT"])
    ).publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="advisory_fullstack_outcome_calculation_correction_v1",
        payload_schema_version=str(evidence_payload["schema_version"]),
        resolved_request_hash=resolved_request_hash,
        payload=evidence_payload,
    ).ref
    request = HistoricalRangeRefreshOutcomesRequest(
        operation_idempotency_key=(
            f"advisory-fullstack-correction-{contract['contract_hash'][:24]}-"
            f"{evidence_ref.semantic_content_hash[:16]}-r{correction_retry_generation}"
        ),
        expected_row_version=int(batch["row_version"]),
        label_as_of_trade_date=_label_as_of_trade_date(),
        range_run_ids=affected_run_ids,
        horizons=[1, 3, 5, 10, 20],
        requested_outcome_logical_ids=logical_ids,
        correction_reason=HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION,
        correction_evidence_ref=evidence_ref,
    )
    background = BackgroundTasks()
    response = service.refresh_outcomes(batch_id, request, background_tasks=background)["data"]
    state["outcome_correction_operation_id"] = response["operation"]["operation_id"]
    state["outcome_correction_retry_generation"] = correction_retry_generation
    state["outcome_correction_operation_status"] = response["operation"]["status"]
    state["outcome_correction_evidence_ref"] = evidence_ref.model_dump(mode="json")
    state["outcome_correction_logical_id_count"] = len(logical_ids)
    _write_state(state_path, state)
    print(
        json.dumps(
            {
                "status": "OUTCOME_CORRECTION_STARTED",
                "operation_id": state["outcome_correction_operation_id"],
                "target_count": len(logical_ids),
                "evidence_hash": evidence_ref.semantic_content_hash,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    asyncio.run(background())
    operation = service.get_operation(str(state["outcome_correction_operation_id"]))
    state["outcome_correction_operation_status"] = operation["status"]
    state["outcome_correction_result_status"] = operation.get("result_status")
    _write_state(state_path, state)
    print(json.dumps(state, ensure_ascii=False, sort_keys=True), flush=True)


def _failed_episode_outcome_scope(
    run_ids: list[str],
) -> tuple[list[str], set[str], list[str]]:
    conn_factory = _comparison_connection_factory()
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH scope AS (
                    SELECT DISTINCT episode_id, range_run_id
                      FROM app.advisory_historical_range_episode_snapshot
                     WHERE range_run_id=ANY(%s)
                ), latest AS (
                    SELECT DISTINCT ON (outcome.outcome_logical_id)
                           outcome.outcome_logical_id, outcome.maturity_status,
                           outcome.producer_code_hash, scope.range_run_id
                      FROM app.advisory_historical_range_outcome outcome
                      JOIN scope ON scope.episode_id=outcome.subject_id
                     WHERE outcome.subject_type='EPISODE'
                     ORDER BY outcome.outcome_logical_id, outcome.outcome_version DESC
                )
                SELECT outcome_logical_id, producer_code_hash, range_run_id
                  FROM latest
                 WHERE maturity_status='FAILED'
                 ORDER BY outcome_logical_id
                """,
                (run_ids,),
            )
            rows = cur.fetchall()
        conn.rollback()
    return (
        [str(row[0]) for row in rows],
        {str(row[1]) for row in rows},
        sorted({str(row[2]) for row in rows}),
    )


def _outcome_child_operations(batch_id: str) -> list[dict[str, Any]]:
    conn_factory = _comparison_connection_factory()
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT operation_id, status, row_version, updated_at,
                       lease_expires_at, result_status
                  FROM app.advisory_historical_range_operation
                 WHERE batch_id=%s AND operation_type='REFRESH_OUTCOMES_RUN'
                 ORDER BY created_at DESC
                 LIMIT 4
                """,
                (batch_id,),
            )
            rows = cur.fetchall()
        conn.rollback()
    return [
        {
            "operation_id": str(row[0]),
            "status": str(row[1]),
            "row_version": int(row[2]),
            "updated_at": str(row[3] or ""),
            "lease_expires_at": str(row[4] or ""),
            "result_status": row[5],
        }
        for row in rows
    ]


def _analyze(*, state_root: Path, service: Any, contract: dict[str, Any]) -> None:
    from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
    from backend.services.advisory_historical_range.fullstack_comparison import (
        HistoricalComparisonArtifactRefV1,
        HistoricalComparisonArtifactStore,
        HistoricalComparisonLifecycleDayV1,
        compare_day_ranks,
        replay_matched_lifecycle,
        summarize_paired_daily_delta,
        summarize_return_records,
    )
    from backend.services.advisory_historical_range.models import (
        HistoricalRangeArtifactRefV1,
        HistoricalRangeCandidateArtifactPayloadV2,
        HistoricalRangeDayReceiptPayloadV2,
        HistoricalRangeDecisionMarkSetV1,
    )
    from backend.services.advisory_list_transition import AdvisoryTransitionPolicyV1

    state = _read_state(_state_path(state_root))
    c_state = _read_state(state_root / f"challenger_state_v{ORCHESTRATION_REVISION}.json")
    if (
        not state
        or state.get("contract_hash") != contract["contract_hash"]
        or c_state.get("status") != "COMPLETED"
        or c_state.get("contract_hash") != contract["contract_hash"]
        or c_state.get("challenger_implementation_hash")
        != _challenger_implementation_hash()
    ):
        raise RuntimeError("ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: A/B/C state is incomplete")
    c_days = c_state.get("days")
    if (
        not isinstance(c_days, dict)
        or sorted(c_days) != contract["ordered_trade_dates"]
        or int(c_state.get("completed_day_count") or 0) != len(c_days)
    ):
        raise RuntimeError(
            "ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: C day identity is incomplete"
        )
    batch = service.get_batch(str(state["batch_id"]))
    runs = service.list_runs(str(state["batch_id"]), limit=20)["items"]
    if batch["status"] != "COMPLETED" or len(runs) != 2:
        raise RuntimeError("ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: A/B runs are incomplete")
    if not state.get("outcome_operation_id"):
        raise RuntimeError("ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: outcome operation is absent")
    outcome_operation = service.get_operation(str(state["outcome_operation_id"]))
    if outcome_operation["status"] != "COMPLETED":
        raise RuntimeError(
            f"ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: outcome_status={outcome_operation['status']}"
        )
    run_ids = sorted(str(item["range_run_id"]) for item in runs)
    failed_logical_ids, _failed_hashes, _failed_run_ids = _failed_episode_outcome_scope(
        run_ids
    )
    if failed_logical_ids:
        raise RuntimeError(
            "ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: failed episode outcomes require correction"
        )
    if state.get("outcome_correction_operation_id"):
        correction_operation = service.get_operation(
            str(state["outcome_correction_operation_id"])
        )
        if correction_operation["status"] != "COMPLETED":
            raise RuntimeError(
                "ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: correction operation is incomplete"
            )
    a_run, b_run = _identify_ab_runs(runs)
    a_days = service.list_days(str(a_run["range_run_id"]), limit=500)["items"]
    b_days = service.list_days(str(b_run["range_run_id"]), limit=500)["items"]
    a_by_date = {str(item["decision_trade_date"]): item for item in a_days}
    b_by_date = {str(item["decision_trade_date"]): item for item in b_days}
    if sorted(a_by_date) != contract["ordered_trade_dates"] or sorted(b_by_date) != contract["ordered_trade_dates"]:
        raise RuntimeError("ADVISORY_COMPARISON_PARENT_CANDIDATE_MISMATCH: A/B dates differ")
    historical_store = HistoricalRangeArtifactStore(
        root=Path(os.environ["AISTOCK_ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT"])
    )
    comparison_store = HistoricalComparisonArtifactStore(root=state_root / "comparison-artifacts")
    daily_effects = []
    matched_inputs: dict[str, list[HistoricalComparisonLifecycleDayV1]] = {
        name: [] for name in ("A5", "B5", "C5")
    }
    next_trade_dates = _next_trade_dates(contract["ordered_trade_dates"])
    direct_marks_by_date = _direct_normalized_marks(contract["ordered_trade_dates"])
    direct_mark_source = _direct_mark_source_evidence(direct_marks_by_date)
    for trade_date in contract["ordered_trade_dates"]:
        a_ref = HistoricalRangeArtifactRefV1.model_validate(a_by_date[trade_date]["candidate_artifact_ref"])
        b_ref = HistoricalRangeArtifactRefV1.model_validate(b_by_date[trade_date]["candidate_artifact_ref"])
        a_payload = HistoricalRangeCandidateArtifactPayloadV2.model_validate(
            historical_store.load(a_ref).payload
        )
        b_payload = HistoricalRangeCandidateArtifactPayloadV2.model_validate(
            historical_store.load(b_ref).payload
        )
        c_day = c_state["days"].get(trade_date)
        if not isinstance(c_day, dict) or c_day.get("status") != "COMPLETE":
            raise RuntimeError(f"ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: C missing {trade_date}")
        c_ref = HistoricalComparisonArtifactRefV1(**c_day["artifact_ref"])
        c_payload = comparison_store.load_challenger(c_ref)
        effects = compare_day_ranks(
            control=a_payload,
            enhanced=b_payload,
            challenger=c_payload,
        )
        daily_effects.append(effects)
        a_day_receipt = HistoricalRangeDayReceiptPayloadV2.model_validate(
            historical_store.load(
                HistoricalRangeArtifactRefV1.model_validate(
                    a_by_date[trade_date]["day_receipt_ref"]
                )
            ).payload
        )
        b_day_receipt = HistoricalRangeDayReceiptPayloadV2.model_validate(
            historical_store.load(
                HistoricalRangeArtifactRefV1.model_validate(
                    b_by_date[trade_date]["day_receipt_ref"]
                )
            ).payload
        )
        a_marks = HistoricalRangeDecisionMarkSetV1.model_validate(
            historical_store.load(a_day_receipt.decision_mark_set_ref).payload
        )
        b_marks = HistoricalRangeDecisionMarkSetV1.model_validate(
            historical_store.load(b_day_receipt.decision_mark_set_ref).payload
        )
        next_trade_date = next_trade_dates[trade_date]
        matched_inputs["A5"].append(
            _matched_lifecycle_day(
                candidate_payload=a_payload,
                mark_set=a_marks,
                next_trade_date=next_trade_date,
                supplemental_exit_marks=direct_marks_by_date[trade_date],
            )
        )
        matched_inputs["B5"].append(
            _matched_lifecycle_day(
                candidate_payload=b_payload,
                mark_set=b_marks,
                next_trade_date=next_trade_date,
                supplemental_exit_marks=direct_marks_by_date[trade_date],
            )
        )
        matched_inputs["C5"].append(
            _matched_lifecycle_day(
                candidate_payload=a_payload,
                mark_set=a_marks,
                next_trade_date=next_trade_date,
                entry_rank_by_symbol={item.symbol: item.model_rank for item in c_payload.candidates},
                entry_score_by_symbol={item.symbol: float(item.model_score) for item in c_payload.candidates},
                supplemental_exit_marks=direct_marks_by_date[trade_date],
            )
        )
    matched_policy = AdvisoryTransitionPolicyV1(
        target_count=5,
        rank_enter_threshold=20,
        rank_exit_threshold=REVIEW_POLICY["rank_exit_threshold"],
        rank_exit_confirm_days=REVIEW_POLICY["rank_exit_confirm_days"],
        daily_replacement_budget=REVIEW_POLICY["daily_replacement_budget"],
        stop_loss_bps=REVIEW_POLICY["stop_loss_bps"],
        take_profit_bps=REVIEW_POLICY["take_profit_bps"],
        trailing_stop_bps=REVIEW_POLICY["trailing_stop_bps"],
        time_stop_days=REVIEW_POLICY["time_stop_days"],
        take_profit_mode=REVIEW_POLICY["take_profit_mode"],
    )
    authoritative_lifecycle = {
        "A20": _lifecycle_summary(str(a_run["range_run_id"])),
        "B20": _lifecycle_summary(str(b_run["range_run_id"])),
    }
    matched_lifecycle = {
        name: replay_matched_lifecycle(
            group_name=name,
            days=days,
            policy=matched_policy,
        )
        for name, days in matched_inputs.items()
    }
    groups = {
        "A20": _entry_sets(authoritative_lifecycle["A20"]),
        "B20": _entry_sets(authoritative_lifecycle["B20"]),
        **{name: _entry_sets(payload) for name, payload in matched_lifecycle.items()},
    }
    calculation_projections = (
        "RETURN_NET_ABSOLUTE",
        "RETURN_GROSS",
        "RETURN_NET_EXCESS",
    )
    outcome_maps = {
        arm: {
            projection: _calculation_return_map(
                str(run["range_run_id"]), calculation_projection=projection
            )
            for projection in calculation_projections
        }
        for arm, run in (("A", a_run), ("B", b_run))
    }
    horizons = (1, 3, 5, 10, 20)
    performance_by_projection: dict[str, Any] = {}
    for calculation_projection in calculation_projections:
        projection_performance: dict[str, Any] = {}
        for group_name, by_date in groups.items():
            source = outcome_maps["B" if group_name.startswith("B") else "A"][
                calculation_projection
            ]
            projection_performance[group_name] = {}
            for horizon in horizons:
                expected = sum(len(symbols) for symbols in by_date.values())
                records = [
                    {
                        "trade_date": trade_date,
                        "value": source.get((trade_date, symbol, horizon)),
                    }
                    for trade_date, symbols in sorted(by_date.items())
                    for symbol in sorted(symbols)
                ]
                summary = summarize_return_records(records)
                summary["expected_sample_count"] = expected
                summary["missing_sample_count"] = expected - int(
                    summary.get("sample_count", 0)
                )
                projection_performance[group_name][str(horizon)] = summary
        performance_by_projection[calculation_projection] = projection_performance
    net_performance = performance_by_projection["RETURN_NET_ABSOLUTE"]
    matched_deltas = {
        group_name: {
            str(horizon): summarize_paired_daily_delta(
                net_performance["A5"][str(horizon)],
                net_performance[group_name][str(horizon)],
            )
            for horizon in horizons
        }
        for group_name in ("B5", "C5")
    }
    result = {
        "schema_version": "advisory_historical_fullstack_comparison_result_v2",
        "contract_hash": contract["contract_hash"],
        "batch_id": state["batch_id"],
        "a_range_run_id": a_run["range_run_id"],
        "b_range_run_id": b_run["range_run_id"],
        "c_parent_range_run_id": c_state["parent_range_run_id"],
        "bundle_id": MODEL_BUNDLE_ID,
        "challenger_evidence": {
            "implementation_hash": c_state["challenger_implementation_hash"],
            "completed_day_count": c_state.get("completed_day_count"),
            "artifact_refs_by_date": {
                trade_date: c_state["days"][trade_date]["artifact_ref"]
                for trade_date in contract["ordered_trade_dates"]
            },
        },
        "daily_effects": daily_effects,
        "rank_summary": {
            "day_count": len(daily_effects),
            "hmm_changed_day_count": sum(item["hmm_rank_changed_count"] > 0 for item in daily_effects),
            "risk_changed_day_count": sum(item["risk_rank_changed_count"] > 0 for item in daily_effects),
            "selection_changed_day_count": sum(
                item["selection_rank_changed_count"] > 0 for item in daily_effects
            ),
            "model_changed_day_count": sum(item["model_rank_changed_count"] > 0 for item in daily_effects),
            "total_b_excluded_count": sum(
                item["excluded_count"] for item in daily_effects
            ),
            "mean_b_excluded_count": sum(
                item["excluded_count"] for item in daily_effects
            )
            / len(daily_effects),
            "mean_a20_b20_changed": sum(item["a20_b20_changed"] for item in daily_effects)
            / len(daily_effects),
            "mean_a5_b5_overlap": sum(item["a5_b5_overlap"] for item in daily_effects)
            / len(daily_effects),
            "mean_a5_c5_overlap": sum(item["a5_c5_overlap"] for item in daily_effects)
            / len(daily_effects),
        },
        "performance_by_projection": performance_by_projection,
        "matched_deltas_net_absolute": matched_deltas,
        "lifecycle": {**authoritative_lifecycle, **matched_lifecycle},
        "market_context": _market_context(),
        "outcome_source": {
            "projection": "EXECUTABLE",
            "calculation_projection": "RETURN_GROSS",
            "required_calculation_maturity": "MATURED",
            "sample_identity": "LIFECYCLE_ENTER_ACTION",
            "calculation_projections": list(calculation_projections),
            "counts_by_arm_and_projection": {
                arm: {
                    projection: len(values)
                    for projection, values in by_projection.items()
                }
                for arm, by_projection in outcome_maps.items()
            },
        },
        "direct_mark_source": direct_mark_source,
        "outcome_correction": {
            "operation_id": state.get("outcome_correction_operation_id"),
            "operation_status": state.get("outcome_correction_operation_status"),
            "logical_id_count": state.get("outcome_correction_logical_id_count", 0),
            "evidence_ref": state.get("outcome_correction_evidence_ref"),
        },
    }
    result["result_hash"] = canonical_json_sha256(result)
    result_path = state_root / f"comparison_result_v{ORCHESTRATION_REVISION}.json"
    report_path = (
        REPOSITORY_ROOT
        / "docs/analysis/advisory_historical_fullstack_comparison_result_20260817.md"
    )
    _write_json(result_path, result)
    _write_text(report_path, _render_comparison_report(result))
    state.update(
        {
            "comparison_result_path": str(result_path),
            "comparison_result_hash": result["result_hash"],
            "comparison_report_path": str(report_path),
        }
    )
    _write_state(_state_path(state_root), state)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)


def _identify_ab_runs(runs: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    a = []
    b = []
    for run in runs:
        profile = (
            (((run.get("frozen_program_json") or {}).get("runtime_config") or {}).get("runtime_profile") or {})
        )
        if bool((profile.get("hmm") or {}).get("enabled")) and bool(
            (profile.get("risk_policy") or {}).get("enabled")
        ):
            b.append(run)
        elif not bool((profile.get("hmm") or {}).get("enabled")) and not bool(
            (profile.get("risk_policy") or {}).get("enabled")
        ):
            a.append(run)
    if len(a) != 1 or len(b) != 1:
        raise RuntimeError("ADVISORY_COMPARISON_CONTRACT_MISMATCH: A/B runs are ambiguous")
    return a[0], b[0]


def _calculation_return_map(
    range_run_id: str,
    *,
    calculation_projection: str,
) -> dict[tuple[str, str, int], float]:
    conn_factory = _comparison_connection_factory()
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH scope AS (
                    SELECT d.decision_trade_date, c.symbol, c.candidate_id
                      FROM app.advisory_historical_range_candidate c
                      JOIN app.advisory_historical_range_day_run d ON d.day_run_id=c.day_run_id
                     WHERE d.range_run_id=%s
                ), latest AS (
                    SELECT DISTINCT ON (o.outcome_logical_id)
                           o.outcome_logical_id, o.subject_id, o.horizon_trade_days, o.outcome_json
                      FROM app.advisory_historical_range_outcome o
                      JOIN scope s ON s.candidate_id=o.subject_id
                     WHERE o.subject_type='CANDIDATE' AND o.projection='EXECUTABLE'
                     ORDER BY o.outcome_logical_id, o.outcome_version DESC
                )
                SELECT s.decision_trade_date, s.symbol, latest.horizon_trade_days,
                       (calc.item->>'projection_value_decimal')::double precision
                  FROM latest
                  JOIN scope s ON s.candidate_id=latest.subject_id
                  CROSS JOIN LATERAL jsonb_array_elements(latest.outcome_json->'calculation_results') calc(item)
                 WHERE calc.item->>'projection'=%s
                   AND calc.item->>'maturity_status'='MATURED'
                   AND calc.item->>'projection_value_decimal' IS NOT NULL
                 ORDER BY s.decision_trade_date, s.symbol, latest.horizon_trade_days
                """,
                (range_run_id, calculation_projection),
            )
            rows = cur.fetchall()
        conn.rollback()
    result = {
        (row[0].isoformat(), str(row[1]), int(row[2])): float(row[3])
        for row in rows
    }
    if len(result) != len(rows):
        raise RuntimeError("ADVISORY_COMPARISON_OUTCOME_INCOMPLETE: duplicate return facts")
    return result


def _lifecycle_summary(range_run_id: str) -> dict[str, Any]:
    conn_factory = _comparison_connection_factory()
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT item.action, count(*)
                  FROM app.advisory_historical_range_list_item item
                  JOIN app.advisory_historical_range_list_version version
                    ON version.list_version_id=item.list_version_id
                 WHERE version.range_run_id=%s
                 GROUP BY item.action ORDER BY item.action
                """,
                (range_run_id,),
            )
            actions = {str(row[0]): int(row[1]) for row in cur.fetchall()}
            cur.execute(
                """
                SELECT day.decision_trade_date, item.symbol, item.action,
                       item.reason_codes_json, item.episode_id
                  FROM app.advisory_historical_range_list_item item
                  JOIN app.advisory_historical_range_list_version version
                    ON version.list_version_id=item.list_version_id
                  JOIN app.advisory_historical_range_day_run day
                    ON day.day_run_id=version.day_run_id
                 WHERE version.range_run_id=%s
                 ORDER BY day.decision_trade_date, item.symbol, item.action
                """,
                (range_run_id,),
            )
            daily_rows: dict[str, dict[str, Any]] = {}
            for trade_date, symbol, action, reason_codes, episode_id in cur.fetchall():
                key = trade_date.isoformat()
                row = daily_rows.setdefault(key, {"active_symbols": [], "actions": []})
                if action in {"ENTER", "HOLD"}:
                    row["active_symbols"].append(str(symbol))
                row["actions"].append(
                    {
                        "symbol": str(symbol),
                        "action": str(action),
                        "reason_codes": list(reason_codes or []),
                        "episode_id": episode_id,
                    }
                )
            cur.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (episode_id)
                           episode_id, recommendation_state, mark_json
                      FROM app.advisory_historical_range_episode_snapshot
                     WHERE range_run_id=%s
                     ORDER BY episode_id, decision_trade_date DESC
                )
                SELECT count(*),
                       count(*) FILTER (
                           WHERE recommendation_state IN ('ACTIVE', 'ACTIVE_AT_RANGE_END')
                       ),
                       count(*) FILTER (WHERE recommendation_state='EXITED'),
                       avg((mark_json->>'holding_trading_days')::double precision),
                       avg((mark_json->>'runup_bps')::double precision),
                       avg((mark_json->>'drawdown_bps')::double precision)
                  FROM latest
                """,
                (range_run_id,),
            )
            episode_row = cur.fetchone()
        conn.rollback()
    exit_reason_counts: dict[str, int] = {}
    for day in daily_rows.values():
        for action in day["actions"]:
            if action["action"] != "EXIT":
                continue
            reasons = action["reason_codes"] or ["UNSPECIFIED"]
            for reason in reasons:
                key = str(reason)
                exit_reason_counts[key] = exit_reason_counts.get(key, 0) + 1
    episode_performance_by_projection = {
        projection: _episode_return_summary(
            range_run_id,
            calculation_projection=projection,
        )
        for projection in ("RETURN_NET_ABSOLUTE", "RETURN_GROSS")
    }
    return {
        "actions": actions,
        "daily": [
            {
                "decision_trade_date": trade_date,
                "active_symbols": sorted(row["active_symbols"]),
                "actions": row["actions"],
            }
            for trade_date, row in sorted(daily_rows.items())
        ],
        "distinct_episode_count": int(episode_row[0]),
        "active_at_end_count": int(episode_row[1]),
        "exited_episode_count": int(episode_row[2]),
        "mean_holding_trading_days": (
            float(episode_row[3]) if episode_row[3] is not None else None
        ),
        "mean_max_runup_bps": (
            float(episode_row[4]) if episode_row[4] is not None else None
        ),
        "mean_max_drawdown_bps": (
            float(episode_row[5]) if episode_row[5] is not None else None
        ),
        "episode_performance_by_projection": episode_performance_by_projection,
        "exit_reason_counts": dict(sorted(exit_reason_counts.items())),
    }


def _matched_lifecycle_day(
    *,
    candidate_payload: Any,
    mark_set: Any,
    next_trade_date: date,
    entry_rank_by_symbol: dict[str, int] | None = None,
    entry_score_by_symbol: dict[str, float] | None = None,
    supplemental_exit_marks: dict[str, float] | None = None,
) -> Any:
    from backend.services.advisory_historical_range.fullstack_comparison import (
        HistoricalComparisonLifecycleDayV1,
    )
    from backend.services.advisory_list_transition import AdvisoryTransitionCandidateV1

    marks = {item.symbol: item for item in mark_set.marks}
    facts = {item.symbol: item for item in candidate_payload.candidates}
    selected_symbols = (
        set(entry_rank_by_symbol)
        if entry_rank_by_symbol is not None
        else {
            symbol
            for symbol, fact in facts.items()
            if fact.membership_status == "INCLUDED"
        }
    )
    if not selected_symbols <= set(facts):
        raise RuntimeError("ADVISORY_COMPARISON_PARENT_CANDIDATE_MISMATCH: lifecycle symbol missing")
    candidates = []
    for symbol in sorted(selected_symbols):
        fact = facts[symbol]
        mark = marks.get(symbol)
        rank = (
            entry_rank_by_symbol[symbol]
            if entry_rank_by_symbol is not None
            else fact.selection_effective_rank
        )
        if rank is None or fact.membership_status != "INCLUDED":
            raise RuntimeError("ADVISORY_COMPARISON_LIFECYCLE_INPUT_INCOMPLETE: entry rank missing")
        score = (
            entry_score_by_symbol[symbol]
            if entry_score_by_symbol is not None
            else float(fact.selection_effective_score)
            if fact.selection_effective_score is not None
            else None
        )
        candidates.append(
            AdvisoryTransitionCandidateV1(
                symbol=symbol,
                rank=int(rank),
                score=score,
                entry_mark=(
                    float(mark.normalized_reference_mark)
                    if mark is not None and mark.availability == "AVAILABLE"
                    else None
                ),
                exit_mark=(
                    float(mark.normalized_reference_mark)
                    if mark is not None and mark.availability != "DATA_UNAVAILABLE"
                    else None
                ),
                entry_mark_available=bool(mark is not None and mark.availability == "AVAILABLE"),
                exit_mark_available=bool(mark is not None and mark.availability != "DATA_UNAVAILABLE"),
                evidence={"candidate_content_hash": fact.candidate_content_hash},
            )
        )
    review_ranks = {
        symbol: int(fact.selection_effective_rank)
        for symbol, fact in facts.items()
        if fact.selection_effective_rank is not None
    }
    exit_marks = {
        **dict(supplemental_exit_marks or {}),
        **{
        symbol: (
            float(mark.normalized_reference_mark)
            if mark.normalized_reference_mark is not None
            else None
        )
        for symbol, mark in marks.items()
        },
    }
    return HistoricalComparisonLifecycleDayV1(
        decision_trade_date=candidate_payload.decision_trade_date,
        next_trade_date=next_trade_date,
        entry_candidates=tuple(candidates),
        review_rank_by_symbol=review_ranks,
        exit_mark_by_symbol=exit_marks,
        exit_mark_available_by_symbol={
            **{symbol: True for symbol in supplemental_exit_marks or {}},
            **{
                symbol: mark.availability != "DATA_UNAVAILABLE"
                for symbol, mark in marks.items()
            },
        },
        observed_max_selection_rank=max(review_ranks.values(), default=0),
    )


def _entry_sets(lifecycle: dict[str, Any]) -> dict[str, set[str]]:
    return {
        str(day["decision_trade_date"]): {
            str(item["symbol"])
            for item in day.get("actions") or []
            if item.get("action") == "ENTER"
        }
        for day in lifecycle.get("daily") or []
    }


def _episode_return_summary(
    range_run_id: str,
    *,
    calculation_projection: str,
) -> dict[str, Any]:
    from backend.services.advisory_historical_range.fullstack_comparison import (
        summarize_return_records,
    )

    conn_factory = _comparison_connection_factory()
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH scope AS (
                    SELECT episode_id, MAX(exit_decision_trade_date) AS exit_trade_date
                      FROM app.advisory_historical_range_episode_snapshot
                     WHERE range_run_id=%s
                     GROUP BY episode_id
                ), latest AS (
                    SELECT DISTINCT ON (outcome.outcome_logical_id)
                           outcome.subject_id, outcome.outcome_json
                      FROM app.advisory_historical_range_outcome outcome
                      JOIN scope ON scope.episode_id=outcome.subject_id
                     WHERE outcome.subject_type='EPISODE'
                       AND outcome.projection='EXECUTABLE'
                       AND outcome.horizon_trade_days=0
                     ORDER BY outcome.outcome_logical_id, outcome.outcome_version DESC
                )
                SELECT scope.exit_trade_date,
                       (calc.item->>'projection_value_decimal')::double precision
                  FROM latest
                  JOIN scope ON scope.episode_id=latest.subject_id
                  CROSS JOIN LATERAL jsonb_array_elements(latest.outcome_json->'calculation_results') calc(item)
                 WHERE calc.item->>'projection'=%s
                   AND calc.item->>'maturity_status'='MATURED'
                   AND calc.item->>'projection_value_decimal' IS NOT NULL
                 ORDER BY scope.exit_trade_date, latest.subject_id
                """,
                (range_run_id, calculation_projection),
            )
            records = [
                {"trade_date": row[0], "value": float(row[1])}
                for row in cur.fetchall()
                if row[0] is not None
            ]
        conn.rollback()
    return summarize_return_records(records)


def _next_trade_dates(trade_dates: list[str]) -> dict[str, date]:
    conn_factory = _comparison_connection_factory()
    result: dict[str, date] = {}
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            for value in trade_dates:
                current = date.fromisoformat(value)
                cur.execute(
                    """SELECT MIN(cal_date) FROM market.trading_calendar WHERE is_trading=TRUE AND cal_date > %s""",
                    (current,),
                )
                row = cur.fetchone()
                if row is None or row[0] is None:
                    raise RuntimeError(
                        "ADVISORY_COMPARISON_FEATURE_CUTOFF_VIOLATION: next trade date unavailable"
                    )
                result[value] = row[0]
        conn.rollback()
    return result


def _render_comparison_report(result: dict[str, Any]) -> str:
    lines = [
        "# 荐股全栈历史三臂对比结果",
        "",
        f"- 结果哈希：`{result['result_hash']}`",
        f"- 冻结合同：`{result['contract_hash']}`",
        f"- A run：`{result['a_range_run_id']}`",
        f"- B run：`{result['b_range_run_id']}`",
        f"- C parent run：`{result['c_parent_range_run_id']}`",
        f"- M5A bundle：`{result['bundle_id']}`",
        "- C 组实现指纹："
        f"`{result['challenger_evidence']['implementation_hash']}`；"
        f"逐日 artifact `{result['challenger_evidence']['completed_day_count']}` 个",
        "",
        "## 排名与覆盖变化",
        "",
    ]
    rank = result["rank_summary"]
    lines.extend(
        [
            f"- 交易日：{rank['day_count']}",
            f"- HMM 改变排名的交易日：{rank['hmm_changed_day_count']}",
            f"- 风险层改变排名的交易日：{rank['risk_changed_day_count']}",
            f"- Selection 改变排名的交易日：{rank['selection_changed_day_count']}",
            f"- M5A 改变排名的交易日：{rank['model_changed_day_count']}",
            f"- B 组累计显式排除：{rank['total_b_excluded_count']}；日均 {rank['mean_b_excluded_count']:.3f}",
            f"- A5/B5 平均重合数：{rank['mean_a5_b5_overlap']:.3f}",
            f"- A5/C5 平均重合数：{rank['mean_a5_c5_overlap']:.3f}",
        ]
    )
    projection_titles = {
        "RETURN_NET_ABSOLUTE": "净绝对收益（主口径）",
        "RETURN_GROSS": "毛收益（审计对照）",
        "RETURN_NET_EXCESS": "净超额收益（benchmark 可用时）",
    }
    for projection, title in projection_titles.items():
        lines.extend(
            [
                "",
                f"## ENTER 推荐固定期限胜率：{title}",
                "",
                "| 组别 | 期限 | 样本 | 缺失 | 胜率 | 平均收益 | 中位收益 |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for group_name, horizons in result["performance_by_projection"][
            projection
        ].items():
            for horizon, summary in horizons.items():
                lines.append(
                    "| {group} | {horizon}日 | {samples} | {missing} | {win} | {mean_value} | {median_value} |".format(
                        group=group_name,
                        horizon=horizon,
                        samples=summary.get("sample_count", 0),
                        missing=summary.get("missing_sample_count", 0),
                        win=_format_metric(summary.get("win_rate"), percent=True),
                        mean_value=_format_metric(
                            summary.get("mean_return"), percent=True
                        ),
                        median_value=_format_metric(
                            summary.get("median_return"), percent=True
                        ),
                    )
                )
    lines.extend(
        [
            "",
            "## 相对 A5 的净绝对收益增量",
            "",
            "| 组别 | 期限 | 配对交易日 | 日均胜率差 | 胜率差 95% CI | 日均收益差 | 收益差 95% CI |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for group_name in ("B5", "C5"):
        for horizon, delta in result["matched_deltas_net_absolute"][
            group_name
        ].items():
            lines.append(
                "| {group}-A5 | {horizon}日 | {days} | {win} | {win_ci} | {mean_value} | {mean_ci} |".format(
                    group=group_name,
                    horizon=horizon,
                    days=delta.get("paired_day_count", 0),
                    win=_format_metric(
                        delta.get("win_rate_difference"), percent=True
                    ),
                    win_ci=_format_ci95(
                        delta.get("win_rate_difference_ci95"), percent=True
                    ),
                    mean_value=_format_metric(
                        delta.get("mean_return_difference"), percent=True
                    ),
                    mean_ci=_format_ci95(
                        delta.get("mean_return_difference_ci95"), percent=True
                    ),
                )
            )
    lines.extend(
        [
            "",
            "## 净绝对收益月度切片",
            "",
            "| 组别 | 期限 | 月份 | 样本 | 胜率 | 平均收益 |",
            "|---|---:|---|---:|---:|---:|",
        ]
    )
    for group_name, horizons in result["performance_by_projection"][
        "RETURN_NET_ABSOLUTE"
    ].items():
        for horizon, summary in horizons.items():
            for month, monthly in (summary.get("monthly") or {}).items():
                lines.append(
                    f"| {group_name} | {horizon}日 | {month} | "
                    f"{monthly['sample_count']} | "
                    f"{_format_metric(monthly['win_rate'], percent=True)} | "
                    f"{_format_metric(monthly['mean_return'], percent=True)} |"
                )
    lines.extend(
        [
            "",
            "## 名单生命周期与 Episode",
            "",
            "| 组别 | ENTER | HOLD | EXIT | WATCH | 容量换手率 | 已闭合 Episode | Episode 胜率 | Episode 口径 | 期末活跃 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|---:|",
        ]
    )
    for group_name, lifecycle in result["lifecycle"].items():
        actions = lifecycle.get("actions") or lifecycle.get("action_counts") or {}
        authoritative_episode = lifecycle.get("episode_performance_by_projection")
        if authoritative_episode:
            episode = authoritative_episode["RETURN_NET_ABSOLUTE"]
            episode_basis = "EXECUTABLE_NET_ABSOLUTE"
        else:
            episode = lifecycle
            episode_basis = lifecycle.get("episode_return_basis", "UNKNOWN")
        day_count = len(lifecycle.get("daily") or [])
        capacity = 20 if group_name in {"A20", "B20"} else 5
        turnover = (
            float(actions.get("ENTER", 0)) / (capacity * day_count)
            if day_count
            else None
        )
        lines.append(
            "| {group} | {enter} | {hold} | {exit_count} | {watch} | {turnover} | {closed} | {win} | {basis} | {active} |".format(
                group=group_name,
                enter=actions.get("ENTER", 0),
                hold=actions.get("HOLD", 0),
                exit_count=actions.get("EXIT", 0),
                watch=actions.get("WATCH", 0),
                turnover=_format_metric(turnover, percent=True),
                closed=episode.get("sample_count", lifecycle.get("completed_episode_count", 0)),
                win=_format_metric(
                    episode.get("win_rate", lifecycle.get("episode_win_rate")),
                    percent=True,
                ),
                basis=episode_basis,
                active=lifecycle.get("active_at_end_count", 0),
            )
        )
    lines.extend(
        [
            "",
            "### Episode 路径统计",
            "",
            "| 组别 | 平均持有交易日 | 平均 MFE | 平均 MAE | 最大连续亏损 |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for group_name, lifecycle in result["lifecycle"].items():
        episode = (
            lifecycle["episode_performance_by_projection"]["RETURN_NET_ABSOLUTE"]
            if lifecycle.get("episode_performance_by_projection")
            else lifecycle
        )
        lines.append(
            f"| {group_name} | "
            f"{_format_metric(lifecycle.get('mean_holding_trading_days'))} | "
            f"{_format_bps(lifecycle.get('mean_max_runup_bps'))} | "
            f"{_format_bps(lifecycle.get('mean_max_drawdown_bps'))} | "
            f"{episode.get('max_consecutive_losses', 0)} |"
        )
    lines.extend(
        [
            "",
            "### 退出原因",
            "",
            "| 组别 | 原因 | 次数 |",
            "|---|---|---:|",
        ]
    )
    for group_name, lifecycle in result["lifecycle"].items():
        for reason, count in (lifecycle.get("exit_reason_counts") or {}).items():
            lines.append(f"| {group_name} | `{reason}` | {count} |")
    lines.extend(
        [
            "",
            "## 市场环境",
            "",
            "| 区间 | 市场阶段 | 沪深300复合收益 | 交易日 |",
            "|---|---|---:|---:|",
        ]
    )
    for name, context in result["market_context"].items():
        lines.append(
            f"| {name} | {context.get('regime')} | "
            f"{_format_metric(context.get('csi300_compounded_return'), percent=True)} | "
            f"{context.get('trade_date_count', 0)} |"
        )
    lines.extend(
        [
            "",
            "## 口径与限制",
            "",
            "- 固定期限分母仅包含名单生命周期产生的真实 `ENTER` 动作，不把每日重复 Top5 或 HOLD 重复计样。",
            "- 主胜率口径使用 `EXECUTABLE / RETURN_NET_ABSOLUTE / MATURED`；毛收益和净超额收益分别展示，不互相替代。",
            "- A20/B20 使用权威 Historical Range 名单；A5/B5/C5 使用同一中立生命周期引擎重放。C 仅用模型排名决定进入，退出继续使用原 Selection rank。",
            "- A20/B20 Episode 使用成熟的可执行净绝对收益；A5/B5/C5 反事实 Episode 仅报告同日 decision-mark 毛收益，不伪造未执行的成本现金流。",
            "- 相对 A5 的 95% CI 以共同有样本交易日的日均指标差计算，方法为 `PAIRED_DAILY_NORMAL_95`。",
            "- 所有计算修正均为追加版本，未删除或覆盖历史 outcome。",
            "- 反事实生命周期补充标记只读取同一交易日 `T_CLOSE`；来源摘要："
            f"`{result['direct_mark_source']['records_content_hash']}`，"
            f"记录数 `{result['direct_mark_source']['record_count']}`。",
            "",
        ]
    )
    return "\n".join(lines)


def _format_metric(value: Any, *, percent: bool = False) -> str:
    if value is None:
        return "N/A"
    number = float(value)
    return f"{number * 100:.2f}%" if percent else f"{number:.6f}"


def _format_bps(value: Any) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):.2f} bps"


def _format_ci95(value: Any, *, percent: bool = False) -> str:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        return "N/A"
    return (
        f"[{_format_metric(value[0], percent=percent)}, "
        f"{_format_metric(value[1], percent=percent)}]"
    )


def _direct_normalized_marks(trade_dates: list[str]) -> dict[str, dict[str, float]]:
    requested = [date.fromisoformat(value) for value in trade_dates]
    result: dict[str, dict[str, float]] = {value: {} for value in trade_dates}
    conn_factory = _comparison_connection_factory()
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(price.trade_date, adj.trade_date) AS trade_date,
                       COALESCE(price.ts_code, adj.ts_code) AS ts_code,
                       price.close_li, adj.adj_factor
                  FROM market.kline_daily_raw AS price
                  FULL OUTER JOIN market.adj_factor AS adj
                    ON adj.ts_code=price.ts_code AND adj.trade_date=price.trade_date
                 WHERE COALESCE(price.trade_date, adj.trade_date)=ANY(%s)
                   AND price.close_li IS NOT NULL
                   AND adj.adj_factor IS NOT NULL
                 ORDER BY trade_date, ts_code
                """,
                (requested,),
            )
            for trade_date, symbol, close_li, adj_factor in cur.fetchall():
                result[trade_date.isoformat()][str(symbol)] = (
                    float(close_li) / 1000.0 * float(adj_factor)
                )
        conn.rollback()
    return result


def _direct_mark_source_evidence(
    marks_by_date: dict[str, dict[str, float]],
) -> dict[str, Any]:
    """Close the exact same-day T_CLOSE supplement with a deterministic digest."""

    ordered_records = [
        {
            "trade_date": trade_date,
            "symbol": symbol,
            "normalized_reference_mark": value,
        }
        for trade_date, marks in sorted(marks_by_date.items())
        for symbol, value in sorted(marks.items())
    ]
    return {
        "schema_version": "advisory_historical_comparison_direct_mark_source_v1",
        "source_tables": ["market.kline_daily_raw", "market.adj_factor"],
        "price_formula": "close_li / 1000 * adj_factor",
        "revision_admissibility": "RETROSPECTIVE_DB_CONTENT_HASH",
        "trade_dates": sorted(marks_by_date),
        "record_count": len(ordered_records),
        "records_content_hash": canonical_json_sha256(ordered_records),
        "day_record_counts": {
            trade_date: len(marks)
            for trade_date, marks in sorted(marks_by_date.items())
        },
    }


def _market_context() -> dict[str, Any]:
    conn_factory = _comparison_connection_factory()
    periods = {
        "full_window": (START_DATE, END_DATE),
        "2026-05": (date(2026, 5, 15), date(2026, 5, 31)),
        "2026-06": (date(2026, 6, 1), date(2026, 6, 30)),
        "2026-07": (date(2026, 7, 1), date(2026, 7, 16)),
    }
    output = {}
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            for name, (start, end) in periods.items():
                cur.execute(
                    """SELECT trade_date,pct_chg FROM market.index_daily WHERE ts_code='000300.SH' AND trade_date BETWEEN %s AND %s ORDER BY trade_date""",
                    (start, end),
                )
                rows = cur.fetchall()
                compounded = 1.0
                for row in rows:
                    compounded *= 1.0 + float(row[1] or 0.0) / 100.0
                compounded_return = compounded - 1.0
                output[name] = {
                    "trade_date_count": len(rows),
                    "csi300_compounded_return": compounded_return,
                    "regime": (
                        "UP"
                        if compounded_return > 0.01
                        else "DOWN"
                        if compounded_return < -0.01
                        else "RANGE_BOUND"
                    ),
                    "regime_threshold": "ABS_RETURN_1_PERCENT",
                }
        conn.rollback()
    return output


def _next_trade_date(decision_trade_date: date) -> date:
    conn_factory = _comparison_connection_factory()
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT MIN(cal_date) FROM market.trading_calendar WHERE is_trading=TRUE AND cal_date > %s""",
                (decision_trade_date,),
            )
            row = cur.fetchone()
        conn.rollback()
    if row is None or row[0] is None:
        raise RuntimeError("ADVISORY_COMPARISON_FEATURE_CUTOFF_VIOLATION: next trade date unavailable")
    return row[0]


def _snapshot_evidence() -> dict[str, Any]:
    conn_factory = _comparison_connection_factory()
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.config_id, s.trained_at, s.model_path, s.status, c.config_json,
                       j.completed_at
                  FROM public.model_train_snapshots s
                  JOIN public.model_train_configs c ON c.config_id = s.config_id
                  LEFT JOIN LATERAL (
                      SELECT completed_at FROM public.model_train_jobs j
                       WHERE j.snapshot_id = s.snapshot_id AND j.status = 'completed'
                       ORDER BY completed_at DESC LIMIT 1
                  ) j ON TRUE
                 WHERE s.snapshot_id = %s
                """,
                (SNAPSHOT_ID,),
            )
            row = cur.fetchone()
        conn.rollback()
    if row is None or str(row[0]) != MODEL_CONFIG_ID or str(row[3]).lower() != "completed" or row[5] is None:
        raise RuntimeError("frozen HMM snapshot training receipt is unavailable")
    config = dict(row[4] or {})
    cutoff = str(config.get("val_end") or config.get("train_end") or "")[:10]
    if not cutoff or date.fromisoformat(cutoff) >= START_DATE:
        raise RuntimeError("frozen HMM snapshot information cutoff is invalid")
    model_path = Path(str(row[2])).resolve()
    return {
        "snapshot_trained_at": row[1].isoformat(),
        "available_at": row[5].isoformat(),
        "training_information_cutoff": cutoff,
        "input_data_max_dates": {"training_panel": cutoff},
        "model_path": str(model_path),
        "model_artifact_sha256": _sha256_file(model_path),
    }


def _database_trade_dates() -> list[str]:
    conn_factory = _comparison_connection_factory()
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT cal_date FROM market.trading_calendar WHERE is_trading=TRUE AND cal_date BETWEEN %s AND %s ORDER BY cal_date""",
                (START_DATE, END_DATE),
            )
            rows = [row[0].isoformat() for row in cur.fetchall()]
        conn.rollback()
    return rows


def _service(*, state_root: Path, snapshot_provider: FrozenSnapshotProvider):
    required = {
        "artifact_root": "AISTOCK_ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT",
        "package_asset_root": "AISTOCK_PACKAGE_ASSET_STORE_ROOT",
        "policy_component_root": "AISTOCK_ADVISORY_HISTORICAL_RANGE_POLICY_COMPONENT_ROOT",
        "calculation_evidence_root": "AISTOCK_ADVISORY_CALCULATION_EVIDENCE_ROOT",
        "dataset_store_root": "AISTOCK_ADVISORY_DATASET_STORE_ROOT",
    }
    missing = [env_key for env_key in required.values() if not str(os.getenv(env_key) or "").strip()]
    if missing:
        raise RuntimeError(f"historical comparison environment is incomplete: {missing}")
    conn_factory = _comparison_connection_factory()
    task_runtime_root = state_root / "runtime"
    task_runtime_root.mkdir(parents=True, exist_ok=True)
    factory = build_explicit_historical_range_r5_runtime_factory(
        conn_factory=conn_factory,
        artifact_root=Path(os.environ[required["artifact_root"]]),
        task_runtime_root=task_runtime_root,
        package_asset_root=Path(os.environ[required["package_asset_root"]]),
        repository_root=REPOSITORY_ROOT,
        policy_component_root=Path(os.environ[required["policy_component_root"]]),
        calculation_evidence_root=Path(os.environ[required["calculation_evidence_root"]]),
        dataset_store_root=Path(os.environ[required["dataset_store_root"]]),
        hmm_snapshot_provider=snapshot_provider,
    )
    return build_historical_range_r5_application_service(
        query_runtime_factory=factory,
        mutation_runtime_factory=factory,
        candidate_prefetch_per_program=CANDIDATE_PREFETCH_PER_PROGRAM,
    )


def _comparison_connection_factory():
    base_factory = explicit_historical_range_connection_factory()
    base_dsn = str(getattr(base_factory, "_aistock_process_worker_dsn", "") or "").strip()
    if not base_dsn:
        raise RuntimeError("historical comparison connection factory has no worker DSN")
    config = psycopg2.extensions.parse_dsn(base_dsn)
    base_options = str(config.get("options") or "").strip()
    config["options"] = " ".join(
        item
        for item in (
            base_options,
            f"-c max_parallel_workers_per_gather={DATABASE_MAX_PARALLEL_WORKERS_PER_GATHER}",
            f"-c work_mem={DATABASE_WORK_MEM}",
        )
        if item
    )
    task_dsn = psycopg2.extensions.make_dsn(**config)
    pool = ThreadedConnectionPool(minconn=1, maxconn=8, dsn=task_dsn)

    @contextmanager
    def connect():
        conn = pool.getconn()
        discard = False
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()
        finally:
            try:
                if conn.closed:
                    discard = True
                else:
                    conn.reset()
            except Exception:
                discard = True
            pool.putconn(conn, close=discard)

    setattr(connect, "_aistock_process_worker_dsn", task_dsn)
    return connect


def _run_state(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            key: row.get(key)
            for key in (
                "range_run_id",
                "research_program_id",
                "status",
                "successful_day_count",
                "terminal_failed_day_count",
                "completed_day_count",
                "row_version",
            )
        }
        for row in rows
    ]


def _read_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("long-task state is invalid")
    return payload


def _state_path(state_root: Path) -> Path:
    return state_root / f"long_task_state_v{ORCHESTRATION_REVISION}.json"


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    _write_json(path, payload)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temporary = Path(temp_name)
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temporary = Path(temp_name)
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


if __name__ == "__main__":
    main()
