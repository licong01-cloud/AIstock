"""Immutable historical replay for PT-NEXT-019.

The pipeline consumes an explicit r4 candidate and the source authorities
already frozen by PT-NEXT-018.  It never reads the parent result receipt and
has no serving, database, scheduler, card, alert, registry, or order side
effects.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Mapping, Sequence

import pandas as pd

from .action_value import ActionValueError, TZ, cutoff_on
from .action_value_corporate_actions import CorporateActionBook
from .action_value_data import DailyCandidate, file_reference
from .action_value_pipeline import _clean_repository_commit
from .action_value_suspensions import SuspensionSnapshotBook
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import canonical_json_bytes, canonical_sha256
from .pattern_research import (
    FACTOR_ACTION_COVERAGE_POLICY,
    FACTOR_ACTION_COVERAGE_POLICY_SHA256,
    _effect_evidence,
    _load_request as load_pattern_request,
    apply_pattern_corporate_action_policy,
    audit_pattern_factor_action_coverage,
    mean_interval,
    replay_full_policy_symbol,
)
from .pattern_rights_issue import (
    RIGHTS_ISSUE_PARTICIPATION_POLICY,
    RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256,
    combined_corporate_action_source_snapshot,
    freeze_rights_issue_participation_policy,
    open_rights_issue_authority,
    rights_issue_application_audit,
)
from .policy import COST_POLICY_SHA256
from .volatility_contraction_breakout import (
    RULE_SPEC,
    RULE_SPEC_SHA256,
    STRATEGY_ID,
    observe_volatility_contraction_breakout,
    volatility_contraction_breakout_features,
)


PIPELINE_ID = "POSITION_TIMING_VOLATILITY_CONTRACTION_BREAKOUT_V1"
ARTIFACT_FOLDER = "volatility_contraction_breakout_v1"
REQUEST_SCHEMA = "position_timing_volatility_contraction_breakout_request_v1"
RECEIPT_SCHEMA = "position_timing_volatility_contraction_breakout_receipt_v1"
BUNDLE_SCHEMA = "position_timing_volatility_contraction_breakout_bundle_v1"
PARENT_PATTERN_REQUEST_SHA256 = (
    "2a8cdf4d74dd023a1c1cba415c968a089cfc5759d016eebdcfb38617d550cb6f"
)
EXPECTED_CANDIDATE_MANIFEST_SHA256 = (
    "ed8375696030ca95b4a1f30167c2ac956e69b8276ba981babd301682dcea78de"
)
EXPECTED_CANDIDATE_DATASET_SHA256 = (
    "1db13b2129409c2ee4aabd8bc83c3f5e5eee1fde2a859a3cb2a722d885dd5c49"
)
EXPECTED_RIGHTS_AUTHORITY_CANONICAL_SHA256 = (
    "4a7cdb79e968f33a000f2e9b81196986349cff26100688794b87f6a1f454f10c"
)
RESULT_CLASS = "EXPLORATORY_PREDECLARED_MECHANISM_REUSED_EVALUATION_POPULATION"
INITIAL_HISTORY_SESSIONS = 756
BOOTSTRAP_SAMPLES = 5000
BLOCK_SESSIONS = 25
INFERENCE_SEED = 20260914
FORMAL_COMPARISON = "VCB_V1_MINUS_BUY_AND_HOLD"
PARENT_COUNTS: tuple[int, ...] = (1, 2, 3)
FALSE_WRITE_FLAGS: tuple[str, ...] = (
    "registry_write",
    "current_write",
    "serving_model_artifact_write",
    "card_write",
    "alert_write",
    "order_write",
    "database_write",
    "runtime_write",
)
SOURCE_CODE_KEYS: frozenset[str] = frozenset(
    {
        "strategy_source",
        "research_source",
        "pattern_research_source",
        "pattern_strategy_source",
        "action_value_source",
        "corporate_action_source",
        "rights_issue_source",
        "policy_source",
    }
)


def _without_path(reference: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in reference.items() if key != "path"}


def _same_canonical_identity(left: Any, right: Any) -> bool:
    """Compare JSON identities without tuple/list round-trip false negatives."""

    return canonical_sha256(left) == canonical_sha256(right)


def _assert_file_reference(reference: Mapping[str, Any], *, code: str) -> Path:
    try:
        path = Path(str(reference["path"])).resolve()
    except (KeyError, TypeError, ValueError) as exc:
        raise ActionValueError(code) from exc
    if file_reference(path) != reference:
        raise ActionValueError(code, path=path.as_posix())
    return path


def _calendar_ordinals(
    candidate: DailyCandidate,
    *,
    start: date,
    end: date,
) -> tuple[int, int]:
    calendar = tuple(item.date() for item in candidate.calendar)
    by_day = {day: ordinal for ordinal, day in enumerate(calendar)}
    start_ordinal = by_day.get(start)
    end_ordinal = by_day.get(end)
    if start_ordinal is None or end_ordinal is None:
        raise ActionValueError("VCB_CALENDAR_SCOPE_MISMATCH")
    evaluation_start = start_ordinal + INITIAL_HISTORY_SESSIONS
    if evaluation_start >= end_ordinal:
        raise ActionValueError("VCB_EVALUATION_RANGE_EMPTY")
    return evaluation_start, end_ordinal


def _source_contract(
    parent: Mapping[str, Any],
    *,
    candidate: DailyCandidate,
    symbols: Sequence[str],
    start: date,
    end: date,
) -> Mapping[str, Any]:
    corporate_path = _assert_file_reference(
        parent["corporate_action_snapshot"],
        code="VCB_CORPORATE_ACTION_REFERENCE_MISMATCH",
    )
    suspension_path = _assert_file_reference(
        parent["suspension_snapshot"],
        code="VCB_SUSPENSION_REFERENCE_MISMATCH",
    )
    corporate_book = CorporateActionBook.open(corporate_path)
    suspensions = SuspensionSnapshotBook.open(suspension_path)
    if (
        corporate_book.snapshot_sha256 != parent["corporate_action_snapshot_sha256"]
        or suspensions.snapshot_sha256 != parent["suspension_snapshot_sha256"]
    ):
        raise ActionValueError("VCB_SOURCE_SNAPSHOT_IDENTITY_MISMATCH")

    rights = open_rights_issue_authority(
        candidate_root=candidate.root,
        expected_candidate_manifest_sha256=EXPECTED_CANDIDATE_MANIFEST_SHA256,
        expected_authority_canonical_sha256=EXPECTED_RIGHTS_AUTHORITY_CANONICAL_SHA256,
    )
    if (
        rights.candidate_dataset_manifest_sha256
        != EXPECTED_CANDIDATE_DATASET_SHA256
        or parent["rights_issue_authority_canonical_sha256"]
        != rights.authority_canonical_sha256
        or parent["rights_issue_participation_policy"]
        != RIGHTS_ISSUE_PARTICIPATION_POLICY
        or parent["rights_issue_participation_policy_sha256"]
        != RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256
    ):
        raise ActionValueError("VCB_RIGHTS_ISSUE_CONTRACT_MISMATCH")

    coverage = candidate.coverage(symbols)
    applied_book, application_audit = apply_pattern_corporate_action_policy(
        candidate,
        symbols=symbols,
        corporate_actions=corporate_book,
        start=start,
        end=end,
        candidate_source_sha256=coverage["source_sha256"],
    )
    combined_snapshot = combined_corporate_action_source_snapshot(
        dividend_snapshot_sha256=corporate_book.snapshot_sha256,
        authority=rights,
    )
    rights_scope_symbols = tuple(
        sorted(set(symbols).union(event.symbol for event in rights.events))
    )
    rights_audit = rights_issue_application_audit(
        rights,
        symbols=rights_scope_symbols,
        start=start,
        end=end,
    )
    factor_audit = audit_pattern_factor_action_coverage(
        candidate,
        symbols=symbols,
        corporate_actions=applied_book,
        start=start,
        end=end,
        candidate_source_sha256=coverage["source_sha256"],
        rights_issues=rights,
        combined_corporate_action_snapshot_sha256=combined_snapshot["snapshot_sha256"],
        rights_issue_application_sha256=rights_audit["application_sha256"],
    )
    if (
        not factor_audit["coverage_complete"]
        or factor_audit["unbound_material_factor_change_count"] != 0
        or factor_audit["insufficient_factor_symbol_count"] != 0
    ):
        raise ActionValueError(
            "VCB_FACTOR_ACTION_COVERAGE_INCOMPLETE",
            unbound=factor_audit["unbound_material_factor_change_count"],
            insufficient=factor_audit["insufficient_factor_symbol_count"],
        )
    return {
        "candidate_source_identity": coverage,
        "corporate_action_snapshot": file_reference(corporate_path),
        "corporate_action_snapshot_sha256": corporate_book.snapshot_sha256,
        "suspension_snapshot": file_reference(suspension_path),
        "suspension_snapshot_sha256": suspensions.snapshot_sha256,
        "corporate_action_application_audit": application_audit,
        "corporate_action_application_sha256": application_audit["application_sha256"],
        "corporate_action_source_snapshot": combined_snapshot,
        "corporate_action_source_snapshot_sha256": combined_snapshot["snapshot_sha256"],
        "rights_issue_authority": rights.authority_reference,
        "rights_issue_authority_canonical_sha256": rights.authority_canonical_sha256,
        "rights_issue_source_documents_sha256": rights.source_documents_sha256,
        "rights_issue_application_audit": rights_audit,
        "rights_issue_application_sha256": rights_audit["application_sha256"],
        "factor_action_coverage_policy": FACTOR_ACTION_COVERAGE_POLICY,
        "factor_action_coverage_policy_sha256": FACTOR_ACTION_COVERAGE_POLICY_SHA256,
        "factor_action_coverage_audit": factor_audit,
        "factor_action_coverage_audit_sha256": factor_audit["audit_sha256"],
        "applied_corporate_actions": applied_book,
    }


def prepare_request(
    *,
    timing_root: Path,
    repository_root: Path,
    parent_pattern_request: Path,
) -> Path:
    repository_root = repository_root.resolve()
    source_commit = _clean_repository_commit(repository_root)
    parent_path = parent_pattern_request.resolve()
    parent = load_pattern_request(parent_path)
    if parent["request_sha256"] != PARENT_PATTERN_REQUEST_SHA256:
        raise ActionValueError("VCB_PARENT_REQUEST_UNSUPPORTED")
    if parent.get("result_class") is None:
        raise ActionValueError("VCB_PARENT_REQUEST_IDENTITY_INCOMPLETE")

    candidate_root = Path(parent["candidate_root"]).resolve()
    candidate = DailyCandidate.open(candidate_root)
    symbols = tuple(str(item).upper() for item in parent["evaluation_symbols"])
    if len(symbols) != 64 or len(set(symbols)) != len(symbols):
        raise ActionValueError("VCB_EVALUATION_POPULATION_INVALID")
    population = parent["population_spec"]
    start = date.fromisoformat(population["start"])
    end = date.fromisoformat(population["end"])
    evaluation_start_ordinal, terminal_ordinal = _calendar_ordinals(
        candidate,
        start=start,
        end=end,
    )
    source = _source_contract(
        parent,
        candidate=candidate,
        symbols=symbols,
        start=start,
        end=end,
    )
    rights_policy_path = freeze_rights_issue_participation_policy(
        timing_root=timing_root.resolve()
    )
    source_code_paths = {
        "strategy_source": Path(__file__).with_name(
            "volatility_contraction_breakout.py"
        ),
        "research_source": Path(__file__),
        "pattern_research_source": Path(__file__).with_name("pattern_research.py"),
        "pattern_strategy_source": Path(__file__).with_name("pattern_strategy.py"),
        "action_value_source": Path(__file__).with_name("action_value.py"),
        "corporate_action_source": Path(__file__).with_name(
            "action_value_corporate_actions.py"
        ),
        "rights_issue_source": Path(__file__).with_name("pattern_rights_issue.py"),
        "policy_source": Path(__file__).with_name("policy.py"),
    }
    request: dict[str, Any] = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "created_at": datetime.now(TZ).isoformat(),
        "repository_root": repository_root.as_posix(),
        "repository_commit": source_commit,
        "timing_root": timing_root.resolve().as_posix(),
        "parent_pattern_request": file_reference(parent_path),
        "parent_pattern_request_sha256": parent["request_sha256"],
        "parent_result_read": False,
        "candidate_root": candidate.root.as_posix(),
        "candidate_manifest_sha256": EXPECTED_CANDIDATE_MANIFEST_SHA256,
        "candidate_dataset_manifest_sha256": EXPECTED_CANDIDATE_DATASET_SHA256,
        "candidate_source_identity": source["candidate_source_identity"],
        "evaluation_symbols": symbols,
        "evaluation_symbol_count": len(symbols),
        "population_reuse_disclosure": RESULT_CLASS,
        "population_spec": {
            "source_start": start.isoformat(),
            "source_end": end.isoformat(),
            "initial_history_sessions": INITIAL_HISTORY_SESSIONS,
            "evaluation_start": candidate.calendar[evaluation_start_ordinal].date().isoformat(),
            "evaluation_end": candidate.calendar[terminal_ordinal].date().isoformat(),
        },
        "rule_spec": RULE_SPEC,
        "rule_spec_sha256": RULE_SPEC_SHA256,
        "formal_comparison": FORMAL_COMPARISON,
        "formal_hypothesis_count": 1,
        "bootstrap_samples": BOOTSTRAP_SAMPLES,
        "block_sessions": BLOCK_SESSIONS,
        "inference_seed": INFERENCE_SEED,
        "economic_threshold_bps": 0.0,
        "parent_order_scenarios": PARENT_COUNTS,
        "cost_policy_sha256": COST_POLICY_SHA256,
        "corporate_action_snapshot": source["corporate_action_snapshot"],
        "corporate_action_snapshot_sha256": source[
            "corporate_action_snapshot_sha256"
        ],
        "suspension_snapshot": source["suspension_snapshot"],
        "suspension_snapshot_sha256": source["suspension_snapshot_sha256"],
        "corporate_action_application_audit": source[
            "corporate_action_application_audit"
        ],
        "corporate_action_application_sha256": source[
            "corporate_action_application_sha256"
        ],
        "corporate_action_source_snapshot": source[
            "corporate_action_source_snapshot"
        ],
        "corporate_action_source_snapshot_sha256": source[
            "corporate_action_source_snapshot_sha256"
        ],
        "rights_issue_authority": source["rights_issue_authority"],
        "rights_issue_authority_canonical_sha256": source[
            "rights_issue_authority_canonical_sha256"
        ],
        "rights_issue_source_documents_sha256": source[
            "rights_issue_source_documents_sha256"
        ],
        "rights_issue_participation_policy": RIGHTS_ISSUE_PARTICIPATION_POLICY,
        "rights_issue_participation_policy_sha256": (
            RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256
        ),
        "rights_issue_participation_policy_artifact": file_reference(
            rights_policy_path
        ),
        "rights_issue_application_audit": source["rights_issue_application_audit"],
        "rights_issue_application_sha256": source[
            "rights_issue_application_sha256"
        ],
        "factor_action_coverage_policy": source[
            "factor_action_coverage_policy"
        ],
        "factor_action_coverage_policy_sha256": source[
            "factor_action_coverage_policy_sha256"
        ],
        "factor_action_coverage_audit": source[
            "factor_action_coverage_audit"
        ],
        "factor_action_coverage_audit_sha256": source[
            "factor_action_coverage_audit_sha256"
        ],
        "source_code": {
            key: file_reference(path) for key, path in source_code_paths.items()
        },
        "result_class": RESULT_CLASS,
        **{flag: False for flag in FALSE_WRITE_FLAGS},
    }
    request["request_sha256"] = canonical_sha256(request)
    path = (
        timing_root.resolve()
        / "research"
        / ARTIFACT_FOLDER
        / "requests"
        / f"{request['request_sha256']}.json"
    )
    PositionTimingArtifactStore._publish_immutable(
        path, canonical_json_bytes(request)
    )
    return path


def _validate_request_external_references(request: Mapping[str, Any]) -> None:
    _assert_file_reference(
        request["parent_pattern_request"], code="VCB_PARENT_REFERENCE_MISMATCH"
    )
    _assert_file_reference(
        request["rights_issue_participation_policy_artifact"],
        code="VCB_RIGHTS_POLICY_REFERENCE_MISMATCH",
    )
    for reference in request["source_code"].values():
        _assert_file_reference(reference, code="VCB_SOURCE_CODE_IDENTITY_MISMATCH")


def _load_request(
    path: Path, *, verify_external_references: bool = False
) -> dict[str, Any]:
    try:
        request = json.loads(path.resolve().read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("VCB_REQUEST_UNAVAILABLE") from exc
    identity = {key: value for key, value in request.items() if key != "request_sha256"}
    factor_audit = request.get("factor_action_coverage_audit")
    factor_identity = (
        {key: value for key, value in factor_audit.items() if key != "audit_sha256"}
        if isinstance(factor_audit, Mapping)
        else {}
    )
    application = request.get("corporate_action_application_audit")
    application_identity = (
        {key: value for key, value in application.items() if key != "application_sha256"}
        if isinstance(application, Mapping)
        else {}
    )
    rights_application = request.get("rights_issue_application_audit")
    rights_application_identity = (
        {
            key: value
            for key, value in rights_application.items()
            if key != "application_sha256"
        }
        if isinstance(rights_application, Mapping)
        else {}
    )
    population = request.get("population_spec")
    source_code = request.get("source_code")
    if (
        request.get("schema_version") != REQUEST_SCHEMA
        or request.get("pipeline_id") != PIPELINE_ID
        or request.get("request_sha256") != canonical_sha256(identity)
        or request.get("parent_pattern_request_sha256")
        != PARENT_PATTERN_REQUEST_SHA256
        or request.get("parent_result_read") is not False
        or request.get("candidate_manifest_sha256")
        != EXPECTED_CANDIDATE_MANIFEST_SHA256
        or request.get("candidate_dataset_manifest_sha256")
        != EXPECTED_CANDIDATE_DATASET_SHA256
        or request.get("rule_spec") != RULE_SPEC
        or request.get("rule_spec_sha256") != RULE_SPEC_SHA256
        or request.get("formal_comparison") != FORMAL_COMPARISON
        or request.get("formal_hypothesis_count") != 1
        or request.get("bootstrap_samples") != BOOTSTRAP_SAMPLES
        or request.get("block_sessions") != BLOCK_SESSIONS
        or request.get("inference_seed") != INFERENCE_SEED
        or request.get("economic_threshold_bps") != 0.0
        or tuple(request.get("parent_order_scenarios") or ()) != PARENT_COUNTS
        or request.get("cost_policy_sha256") != COST_POLICY_SHA256
        or request.get("result_class") != RESULT_CLASS
        or request.get("population_reuse_disclosure") != RESULT_CLASS
        or request.get("evaluation_symbol_count") != 64
        or len(set(request.get("evaluation_symbols") or ())) != 64
        or not isinstance(population, Mapping)
        or population.get("source_start") != "2018-08-01"
        or population.get("source_end") != "2026-08-31"
        or population.get("initial_history_sessions") != INITIAL_HISTORY_SESSIONS
        or not isinstance(source_code, Mapping)
        or set(source_code) != SOURCE_CODE_KEYS
        or request.get("rights_issue_participation_policy")
        != RIGHTS_ISSUE_PARTICIPATION_POLICY
        or request.get("rights_issue_participation_policy_sha256")
        != RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256
        or request.get("rights_issue_authority_canonical_sha256")
        != EXPECTED_RIGHTS_AUTHORITY_CANONICAL_SHA256
        or request.get("factor_action_coverage_policy")
        != FACTOR_ACTION_COVERAGE_POLICY
        or request.get("factor_action_coverage_policy_sha256")
        != FACTOR_ACTION_COVERAGE_POLICY_SHA256
        or not isinstance(factor_audit, Mapping)
        or factor_audit.get("coverage_complete") is not True
        or factor_audit.get("unbound_material_factor_change_count")
        != 0
        or factor_audit.get("insufficient_factor_symbol_count") != 0
        or factor_audit.get("audit_sha256") != canonical_sha256(factor_identity)
        or request.get("factor_action_coverage_audit_sha256")
        != factor_audit.get("audit_sha256")
        or not isinstance(application, Mapping)
        or application.get("application_sha256")
        != canonical_sha256(application_identity)
        or request.get("corporate_action_application_sha256")
        != application.get("application_sha256")
        or not isinstance(rights_application, Mapping)
        or rights_application.get("application_sha256")
        != canonical_sha256(rights_application_identity)
        or request.get("rights_issue_application_sha256")
        != rights_application.get("application_sha256")
        or request.get("corporate_action_source_snapshot_sha256")
        != request.get("corporate_action_source_snapshot", {}).get(
            "snapshot_sha256"
        )
        or any(request.get(flag) is not False for flag in FALSE_WRITE_FLAGS)
    ):
        raise ActionValueError("VCB_REQUEST_IDENTITY_MISMATCH")
    if verify_external_references:
        _validate_request_external_references(request)
    return request


def _signal_rows(
    *,
    symbol: str,
    features: pd.DataFrame,
    calendar_dates: Sequence[date],
    start_ordinal: int,
    terminal_ordinal: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ordinal in range(start_ordinal, terminal_ordinal):
        observation = observe_volatility_contraction_breakout(features, ordinal)
        rows.append(
            {
                "strategy_id": STRATEGY_ID,
                "rule_spec_sha256": RULE_SPEC_SHA256,
                "symbol": symbol,
                "decision_trade_date": calendar_dates[ordinal],
                "decision_as_of": cutoff_on(calendar_dates[ordinal]),
                "feature_available_at": cutoff_on(calendar_dates[ordinal]),
                "status": observation.status,
                "reason": observation.reason,
                "matched": observation.matched,
                **observation.values,
            }
        )
    return rows


def _comparison(
    sleeve_days: pd.DataFrame,
    *,
    coverage_complete: bool,
    seed: int,
) -> Mapping[str, Any]:
    subset = sleeve_days.loc[
        sleeve_days["comparison"].eq("P_MINUS_BUY_AND_HOLD")
    ]
    if subset.empty:
        inference: Mapping[str, Any] = {
            "point_bps": None,
            "lower_bps": None,
            "upper_bps": None,
            "valid_resample_fraction": 0.0,
            "reason_code": "NO_EVALUABLE_DAYS",
        }
        gross_point = None
        cumulative = None
        days = 0
    else:
        by_day = subset.groupby("valuation_date", sort=True).agg(
            incremental_net_value_bps=("incremental_net_value_bps", "mean"),
            incremental_gross_value_bps=("incremental_gross_value_bps", "mean"),
        )
        inference = mean_interval(
            by_day["incremental_net_value_bps"].to_numpy(float),
            block_sessions=BLOCK_SESSIONS,
            samples=BOOTSTRAP_SAMPLES,
            seed=seed,
            confidence_level=0.95,
        )
        gross_point = float(by_day["incremental_gross_value_bps"].mean())
        cumulative = float(by_day["incremental_net_value_bps"].sum())
        days = int(len(by_day))
    return {
        "comparison": FORMAL_COMPARISON,
        "unit": "DAILY_CROSS_SYMBOL_MEAN_INCREMENTAL_BPS",
        "economic_threshold_bps": 0.0,
        "nominal_interval_level": 0.95,
        "familywise_interval_level": 0.95,
        "inference": inference,
        "gross_daily_mean_incremental_bps": gross_point,
        "period_cumulative_incremental_bps": cumulative,
        "effective_trading_days": days,
        "effect_evidence": _effect_evidence(
            inference, coverage_complete=coverage_complete
        ),
        "power_status": "NOT_COMPUTABLE",
        "power_reason_code": "NO_PREREGISTERED_SAME_ESTIMAND_EFFECT_SCALE",
    }


def _scenario_coverage_complete(
    parent_count: int,
    *,
    expected_symbols: int,
    evaluated_symbols: Mapping[str, set[str]],
    feature_errors: Sequence[Mapping[str, Any]],
    path_errors: Sequence[Mapping[str, Any]],
    source_coverage_complete: bool,
) -> bool:
    return bool(
        source_coverage_complete
        and not feature_errors
        and len(evaluated_symbols[str(parent_count)]) == expected_symbols
        and not any(
            int(item.get("parent_order_count", -1)) == parent_count
            for item in path_errors
        )
    )


def _manifest(root: Path, receipt: Mapping[str, Any]) -> Mapping[str, Any]:
    names = tuple(
        path.relative_to(root).as_posix()
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.relative_to(root).as_posix() != "manifest.json"
    )
    payload = {
        "schema_version": BUNDLE_SCHEMA,
        "request_sha256": receipt["request_sha256"],
        "receipt_sha256": receipt["receipt_sha256"],
        "files": {
            name: _without_path(file_reference(root / name)) for name in names
        },
    }
    return {**payload, "manifest_sha256": canonical_sha256(payload)}


def _publish_bundle(
    bundle: Path,
    *,
    request: Mapping[str, Any],
    receipt: Mapping[str, Any],
    coverage: Mapping[str, Any],
    signal_observations: pd.DataFrame,
    sleeve_days: pd.DataFrame,
    fills: pd.DataFrame,
) -> None:
    bundle.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle.name}.", dir=bundle.parent))
    try:
        PositionTimingArtifactStore._publish_immutable(
            staging / "request.json", canonical_json_bytes(request)
        )
        PositionTimingArtifactStore._publish_immutable(
            staging / "coverage.json", canonical_json_bytes(coverage)
        )
        signal_observations.to_parquet(
            staging / "signal_observations.parquet", index=False
        )
        sleeve_days.to_parquet(staging / "sleeve_days.parquet", index=False)
        fills.to_parquet(staging / "fills.parquet", index=False)
        PositionTimingArtifactStore._publish_immutable(
            staging / "receipt.json", canonical_json_bytes(receipt)
        )
        PositionTimingArtifactStore._publish_immutable(
            staging / "manifest.json", canonical_json_bytes(_manifest(staging, receipt))
        )
        lock = bundle.parents[3] / "locks" / f"vcb-{bundle.name}.lock"
        with _exclusive_file_lock(lock):
            if bundle.exists():
                inspect_bundle(bundle)
            else:
                os.replace(staging, bundle)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def inspect_bundle(bundle: Path) -> Mapping[str, Any]:
    bundle = bundle.resolve()
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        receipt = json.loads((bundle / "receipt.json").read_text(encoding="utf-8"))
        coverage = json.loads((bundle / "coverage.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("VCB_BUNDLE_UNAVAILABLE") from exc
    request = _load_request(bundle / "request.json")
    manifest_identity = {
        key: value for key, value in manifest.items() if key != "manifest_sha256"
    }
    receipt_identity = {
        key: value for key, value in receipt.items() if key != "receipt_sha256"
    }
    actual_files = {
        path.relative_to(bundle).as_posix()
        for path in bundle.rglob("*")
        if path.is_file() and path.relative_to(bundle).as_posix() != "manifest.json"
    }
    if (
        manifest.get("schema_version") != BUNDLE_SCHEMA
        or manifest.get("manifest_sha256") != canonical_sha256(manifest_identity)
        or receipt.get("schema_version") != RECEIPT_SCHEMA
        or receipt.get("pipeline_id") != PIPELINE_ID
        or receipt.get("receipt_sha256") != canonical_sha256(receipt_identity)
        or receipt.get("request_sha256") != request["request_sha256"]
        or manifest.get("request_sha256") != request["request_sha256"]
        or manifest.get("receipt_sha256") != receipt["receipt_sha256"]
        or set(manifest.get("files", {})) != actual_files
        or coverage.get("coverage_sha256")
        != canonical_sha256(
            {key: value for key, value in coverage.items() if key != "coverage_sha256"}
        )
        or receipt.get("repository_commit") != request["repository_commit"]
        or receipt.get("parent_pattern_request_sha256")
        != request["parent_pattern_request_sha256"]
        or receipt.get("candidate_manifest_sha256")
        != request["candidate_manifest_sha256"]
        or receipt.get("candidate_dataset_manifest_sha256")
        != request["candidate_dataset_manifest_sha256"]
        or receipt.get("rule_spec_sha256") != request["rule_spec_sha256"]
        or receipt.get("cost_policy_sha256") != request["cost_policy_sha256"]
        or receipt.get("coverage_sha256") != coverage.get("coverage_sha256")
        or receipt.get("result_class") != RESULT_CLASS
        or receipt.get("formal_hypothesis_count") != 1
        or receipt.get("formal_comparison") != FORMAL_COMPARISON
        or receipt.get("selected_trial_count") not in {0, 1}
        or any(receipt.get(flag.replace("_write", "_written")) is not False for flag in FALSE_WRITE_FLAGS)
    ):
        raise ActionValueError("VCB_BUNDLE_IDENTITY_MISMATCH")
    for name, reference in manifest["files"].items():
        if _without_path(file_reference(bundle / name)) != reference:
            raise ActionValueError("VCB_BUNDLE_FILE_IDENTITY_MISMATCH", file=name)
    return {
        "status": "BUNDLE_VALID",
        "bundle": bundle.as_posix(),
        "manifest": manifest,
        "receipt": receipt,
        "coverage": coverage,
    }


def run_request(request_path: Path) -> Mapping[str, Any]:
    request = _load_request(request_path, verify_external_references=True)
    repository_root = Path(request["repository_root"])
    if _clean_repository_commit(repository_root) != request["repository_commit"]:
        raise ActionValueError("VCB_REPOSITORY_COMMIT_MISMATCH")
    bundle = (
        Path(request["timing_root"])
        / "research"
        / ARTIFACT_FOLDER
        / "bundles"
        / request["request_sha256"]
    )
    if bundle.exists():
        inspected = inspect_bundle(bundle)
        return {
            "status": "ALREADY_MATERIALIZED",
            "bundle": bundle.as_posix(),
            "manifest_sha256": inspected["manifest"]["manifest_sha256"],
            "receipt_sha256": inspected["receipt"]["receipt_sha256"],
        }

    parent = load_pattern_request(
        Path(request["parent_pattern_request"]["path"])
    )
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    population = request["population_spec"]
    source_start = date.fromisoformat(population["source_start"])
    source_end = date.fromisoformat(population["source_end"])
    symbols = tuple(request["evaluation_symbols"])
    source = _source_contract(
        parent,
        candidate=candidate,
        symbols=symbols,
        start=source_start,
        end=source_end,
    )
    identity_pairs = tuple(
        (key, source[key])
        for key in (
            "candidate_source_identity",
            "corporate_action_snapshot",
            "corporate_action_snapshot_sha256",
            "suspension_snapshot",
            "suspension_snapshot_sha256",
            "corporate_action_application_audit",
            "corporate_action_application_sha256",
            "corporate_action_source_snapshot",
            "corporate_action_source_snapshot_sha256",
            "rights_issue_authority",
            "rights_issue_authority_canonical_sha256",
            "rights_issue_source_documents_sha256",
            "rights_issue_application_audit",
            "rights_issue_application_sha256",
            "factor_action_coverage_policy",
            "factor_action_coverage_policy_sha256",
            "factor_action_coverage_audit",
            "factor_action_coverage_audit_sha256",
        )
    )
    if any(
        not _same_canonical_identity(request[key], value)
        for key, value in identity_pairs
    ):
        raise ActionValueError("VCB_SOURCE_CONTRACT_DRIFT")
    candidate = SuspensionSnapshotBook.open(
        Path(request["suspension_snapshot"]["path"])
    ).apply(candidate, snapshot_path=Path(request["suspension_snapshot"]["path"]))
    corporate_actions = source["applied_corporate_actions"]
    start_ordinal, terminal_ordinal = _calendar_ordinals(
        candidate, start=source_start, end=source_end
    )
    calendar_dates = tuple(item.date() for item in candidate.calendar)

    features_by_symbol: dict[str, pd.DataFrame] = {}
    signal_rows: list[dict[str, Any]] = []
    feature_errors: list[dict[str, Any]] = []
    for symbol in symbols:
        try:
            features = volatility_contraction_breakout_features(
                candidate.bars(symbol),
                symbol=symbol,
                corporate_actions=corporate_actions,
            )
            features_by_symbol[symbol] = features
            signal_rows.extend(
                _signal_rows(
                    symbol=symbol,
                    features=features,
                    calendar_dates=calendar_dates,
                    start_ordinal=start_ordinal,
                    terminal_ordinal=terminal_ordinal,
                )
            )
        except ActionValueError as exc:
            feature_errors.append(
                {"symbol": symbol, "error_code": exc.code, "details": exc.details}
            )

    scenario_rows: list[dict[str, Any]] = []
    fill_rows: list[dict[str, Any]] = []
    path_errors: list[dict[str, Any]] = []
    scenario_counts: dict[str, Mapping[str, int]] = {}
    evaluated_symbols: dict[str, set[str]] = {str(item): set() for item in PARENT_COUNTS}
    for parent_count in PARENT_COUNTS:
        counts: Counter[str] = Counter()
        for symbol in symbols:
            features = features_by_symbol.get(symbol)
            if features is None:
                continue
            try:
                rows, fills, replay_counts = replay_full_policy_symbol(
                    symbol=symbol,
                    bars=candidate.bars(symbol),
                    features=features,
                    calendar_dates=calendar_dates,
                    corporate_actions=corporate_actions,
                    start_ordinal=start_ordinal,
                    terminal_ordinal=terminal_ordinal,
                    entry_observer=lambda frame, ordinal: (
                        observe_volatility_contraction_breakout(frame, ordinal).matched
                    ),
                    entry_selector=lambda **_kwargs: {
                        "choice": "E0",
                        "authority": "VOLATILITY_CONTRACTION_BREAKOUT_OPEN",
                    },
                    supplemental_exit_enabled=False,
                    parent_count=parent_count,
                    additional_friction_bps=Decimal(0),
                )
                for row in rows:
                    row["strategy_id"] = STRATEGY_ID
                    row["rule_spec_sha256"] = RULE_SPEC_SHA256
                    row["parent_order_count"] = parent_count
                for fill in fills:
                    fill["strategy_id"] = STRATEGY_ID
                    fill["rule_spec_sha256"] = RULE_SPEC_SHA256
                    fill["parent_order_count"] = parent_count
                scenario_rows.extend(rows)
                fill_rows.extend(fills)
                replay_counts["VCB_SIGNAL_OBSERVED"] = replay_counts.pop(
                    "BREAKOUT_OBSERVED", 0
                )
                counts.update(replay_counts)
                evaluated_symbols[str(parent_count)].add(symbol)
            except ActionValueError as exc:
                path_errors.append(
                    {
                        "parent_order_count": parent_count,
                        "symbol": symbol,
                        "error_code": exc.code,
                        "details": exc.details,
                    }
                )
        scenario_counts[str(parent_count)] = dict(sorted(counts.items()))

    signal_frame = pd.DataFrame(signal_rows)
    sleeve_days = pd.DataFrame(scenario_rows)
    fills = pd.DataFrame(fill_rows)
    signal_counts = (
        signal_frame["status"].value_counts().sort_index().to_dict()
        if not signal_frame.empty
        else {}
    )
    scenario_coverage = {
        str(parent_count): _scenario_coverage_complete(
            parent_count,
            expected_symbols=len(symbols),
            evaluated_symbols=evaluated_symbols,
            feature_errors=feature_errors,
            path_errors=path_errors,
            source_coverage_complete=(
                source["factor_action_coverage_audit"]["coverage_complete"] is True
            ),
        )
        for parent_count in PARENT_COUNTS
    }
    primary_coverage_complete = scenario_coverage["1"]
    diagnostic_coverage_complete = all(
        scenario_coverage[str(parent_count)] for parent_count in (2, 3)
    )
    coverage_identity = {
        "schema_version": "position_timing_volatility_contraction_breakout_coverage_v1",
        "expected_symbol_count": len(symbols),
        "evaluated_symbol_count_by_parent_order": {
            key: len(value) for key, value in evaluated_symbols.items()
        },
        "scenario_coverage_complete": scenario_coverage,
        "primary_coverage_complete": primary_coverage_complete,
        "diagnostic_cost_sensitivity_coverage_complete": (
            diagnostic_coverage_complete
        ),
        "feature_errors": feature_errors,
        "path_errors": path_errors,
        "signal_status_counts": signal_counts,
        "market_signal_match_count": int(signal_counts.get("AVAILABLE_MATCH", 0)),
        "scenario_counts": scenario_counts,
        "factor_action_coverage_audit_sha256": source[
            "factor_action_coverage_audit_sha256"
        ],
        "unbound_material_factor_change_count": source[
            "factor_action_coverage_audit"
        ]["unbound_material_factor_change_count"],
        "insufficient_factor_symbol_count": source[
            "factor_action_coverage_audit"
        ]["insufficient_factor_symbol_count"],
        "coverage_complete": primary_coverage_complete,
    }
    coverage = {
        **coverage_identity,
        "coverage_sha256": canonical_sha256(coverage_identity),
    }
    comparisons: dict[str, Any] = {}
    for offset, parent_count in enumerate(PARENT_COUNTS):
        scenario = (
            sleeve_days.loc[sleeve_days["parent_order_count"].eq(parent_count)]
            if not sleeve_days.empty
            else pd.DataFrame(columns=["comparison"])
        )
        comparisons[str(parent_count)] = _comparison(
            scenario,
            coverage_complete=scenario_coverage[str(parent_count)],
            seed=INFERENCE_SEED + offset,
        )
    main_evidence = comparisons["1"]["effect_evidence"]
    cost_sensitive = bool(
        main_evidence == "SUPPORTED"
        and diagnostic_coverage_complete
        and any(
            comparisons[str(item)]["effect_evidence"] != "SUPPORTED"
            for item in (2, 3)
        )
    )
    receipt_identity = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "completed_at": datetime.now(TZ).isoformat(),
        "repository_commit": request["repository_commit"],
        "request_sha256": request["request_sha256"],
        "parent_pattern_request_sha256": PARENT_PATTERN_REQUEST_SHA256,
        "candidate_manifest_sha256": EXPECTED_CANDIDATE_MANIFEST_SHA256,
        "candidate_dataset_manifest_sha256": EXPECTED_CANDIDATE_DATASET_SHA256,
        "rule_spec_sha256": RULE_SPEC_SHA256,
        "cost_policy_sha256": COST_POLICY_SHA256,
        "corporate_action_snapshot_sha256": request[
            "corporate_action_snapshot_sha256"
        ],
        "suspension_snapshot_sha256": request["suspension_snapshot_sha256"],
        "rights_issue_authority_canonical_sha256": request[
            "rights_issue_authority_canonical_sha256"
        ],
        "rights_issue_participation_policy_sha256": (
            RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256
        ),
        "factor_action_coverage_audit_sha256": source[
            "factor_action_coverage_audit_sha256"
        ],
        "result_class": RESULT_CLASS,
        "formal_hypothesis_count": 1,
        "formal_comparison": FORMAL_COMPARISON,
        "comparisons_by_parent_order_count": comparisons,
        "effect_evidence": main_evidence,
        "power_status": comparisons["1"]["power_status"],
        "cost_assumption_sensitive": cost_sensitive,
        "evidence_reason_codes": [
            *(["COST_ASSUMPTION_SENSITIVE"] if cost_sensitive else []),
            *(
                ["COST_SENSITIVITY_INCOMPLETE"]
                if not diagnostic_coverage_complete
                else []
            ),
        ],
        "selected_trial_count": 1 if main_evidence == "SUPPORTED" else 0,
        "coverage_sha256": coverage["coverage_sha256"],
        "registry_written": False,
        "current_written": False,
        "serving_model_artifact_written": False,
        "card_written": False,
        "alert_written": False,
        "order_written": False,
        "database_written": False,
        "runtime_written": False,
    }
    receipt = {
        **receipt_identity,
        "receipt_sha256": canonical_sha256(receipt_identity),
    }
    _publish_bundle(
        bundle,
        request=request,
        receipt=receipt,
        coverage=coverage,
        signal_observations=signal_frame,
        sleeve_days=sleeve_days,
        fills=fills,
    )
    inspected = inspect_bundle(bundle)
    return {
        "status": "MATERIALIZED",
        "bundle": bundle.as_posix(),
        "manifest_sha256": inspected["manifest"]["manifest_sha256"],
        "receipt_sha256": inspected["receipt"]["receipt_sha256"],
        "effect_evidence": main_evidence,
        "selected_trial_count": receipt["selected_trial_count"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--timing-root", required=True, type=Path)
    prepare.add_argument("--repository-root", required=True, type=Path)
    prepare.add_argument("--parent-pattern-request", required=True, type=Path)
    run = commands.add_parser("run")
    run.add_argument("--request", required=True, type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "prepare":
        result: Mapping[str, Any] = {
            "status": "REQUEST_PREPARED",
            "request": prepare_request(
                timing_root=args.timing_root,
                repository_root=args.repository_root,
                parent_pattern_request=args.parent_pattern_request,
            ).as_posix(),
        }
    elif args.command == "run":
        result = run_request(args.request)
    else:
        result = inspect_bundle(args.bundle)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
