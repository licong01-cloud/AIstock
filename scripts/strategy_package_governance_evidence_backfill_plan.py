"""Plan QE governance evidence backfill for exactly four StrategyPackages.

Default mode validates an explicit JSON evidence bundle and emits planned rows.
It never opens a database connection and never calls StrategyPackage services.
The output is a reviewable plan for the later R6 production window.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "aistock_qe_governance_evidence_backfill_plan_v1"
EXPECTED_PACKAGE_COUNT = 4
ALLOWED_PACKAGE_STATUSES = {"BACKTEST_APPROVED", "SELECTION_ENABLED", "PAPER_ENABLED"}
NO_RETRAIN_TYPES = {"original_fixed_weight", "latest_fixed_weight", "runtime_variant_backtest"}
RETRAIN_TYPES = {"original_retrain", "latest_retrain", "walk_forward_rolling"}


class GovernanceEvidenceBackfillPlanError(RuntimeError):
    """Raised when the evidence bundle cannot be turned into a safe plan."""


@dataclass(frozen=True)
class PlannedRow:
    table: str
    natural_key: dict[str, Any]
    action: str
    columns: dict[str, Any]


@dataclass(frozen=True)
class PackagePlan:
    package_id: str
    manifest_sha256: str
    package_status: str
    required_gates: dict[str, bool]
    blockers: list[str]
    rows: list[PlannedRow]


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise GovernanceEvidenceBackfillPlanError(message)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise GovernanceEvidenceBackfillPlanError(f"invalid JSON evidence bundle: {exc}") from exc
    if not isinstance(payload, dict):
        raise GovernanceEvidenceBackfillPlanError("evidence bundle must be a JSON object")
    return payload


def _as_list(value: Any, *, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise GovernanceEvidenceBackfillPlanError(f"{field} must be a list")
    return value


def _text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GovernanceEvidenceBackfillPlanError("required text field is empty")
    return value.strip()


def _non_empty_dict(value: Any) -> bool:
    return isinstance(value, dict) and bool(value)


def _has_metric(payload: Any, metric_key: str) -> bool:
    if not isinstance(payload, dict):
        return False
    value: Any = payload
    for part in metric_key.split("."):
        if not isinstance(value, dict) or part not in value:
            return False
        value = value[part]
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _requested_package_ids(raw_ids: list[str] | None) -> list[str] | None:
    if not raw_ids:
        return None
    requested = [item.strip() for item in raw_ids if item.strip()]
    _require(len(requested) == EXPECTED_PACKAGE_COUNT, f"--package-id must be repeated exactly {EXPECTED_PACKAGE_COUNT} times")
    _require(len(set(requested)) == EXPECTED_PACKAGE_COUNT, "--package-id values must be unique")
    return requested


def _validate_package_set(packages: list[Any], requested_ids: list[str] | None) -> list[dict[str, Any]]:
    _require(len(packages) == EXPECTED_PACKAGE_COUNT, f"bundle must contain exactly {EXPECTED_PACKAGE_COUNT} packages")
    normalized: list[dict[str, Any]] = []
    package_ids: list[str] = []
    for item in packages:
        _require(isinstance(item, dict), "package entries must be objects")
        package_id = _text(item.get("package_id"))
        package_ids.append(package_id)
        normalized.append(item)
    _require(len(set(package_ids)) == EXPECTED_PACKAGE_COUNT, "package_id values must be unique")
    if requested_ids is not None:
        _require(set(requested_ids) == set(package_ids), "--package-id set must match bundle package_id set")
    return normalized


def _planned_asset_rows(package: dict[str, Any]) -> tuple[list[PlannedRow], bool, list[str]]:
    rows: list[PlannedRow] = []
    blockers: list[str] = []
    assets = _as_list(package.get("assets", []), field="assets")
    protected_count = 0
    for asset in assets:
        _require(isinstance(asset, dict), "assets entries must be objects")
        asset_type = _text(asset.get("asset_type"))
        asset_ref = _text(asset.get("asset_ref"))
        _require("protected_asset" in asset, f"{asset_ref} requires protected_asset")
        _require(isinstance(asset.get("protected_asset"), bool), f"{asset_ref} protected_asset must be boolean")
        protected = asset["protected_asset"]
        if protected:
            protected_count += 1
        else:
            blockers.append(f"unprotected_asset:{asset_ref}")
        rows.append(
            PlannedRow(
                table="strategy_pkg.package_asset",
                natural_key={"package_id": package["package_id"], "asset_type": asset_type, "asset_ref": asset_ref},
                action="insert_or_update_protected_asset_metadata",
                columns={
                    "package_id": package["package_id"],
                    "asset_type": asset_type,
                    "asset_ref": asset_ref,
                    "asset_sha256": asset.get("asset_sha256"),
                    "metadata": asset.get("metadata") or {},
                    "asset_role": asset.get("asset_role") or "governed_asset",
                    "asset_size_bytes": asset.get("asset_size_bytes"),
                    "protected_asset": protected,
                    "source_uri": asset.get("source_uri"),
                },
            )
        )
    if not assets:
        blockers.append("protected_asset_ledger_missing")
    return rows, bool(assets) and protected_count == len(assets), blockers


def _planned_validation_rows(package: dict[str, Any], *, metric_key: str) -> tuple[list[PlannedRow], dict[str, bool], list[str]]:
    rows: list[PlannedRow] = []
    blockers: list[str] = []
    gates = {
        "original_fixed_weight_retest": False,
        "seed_sample_count_present": False,
        "regime_sample_count_present": False,
    }
    seed_values: set[int] = set()
    regime_samples = 0
    validation_runs = _as_list(package.get("validation_runs", []), field="validation_runs")
    for run in validation_runs:
        _require(isinstance(run, dict), "validation_runs entries must be objects")
        run_id = _text(run.get("validation_run_id"))
        validation_type = _text(run.get("validation_type"))
        retrain_mode = _text(run.get("retrain_mode"))
        status = _text(run.get("status"))
        _require(run.get("manifest_sha256") == package["manifest_sha256"], f"{run_id} manifest_sha256 must match package")
        if validation_type in NO_RETRAIN_TYPES:
            _require(retrain_mode == "no_retrain", f"{run_id} fixed-weight/runtime validation requires no_retrain")
        if validation_type in RETRAIN_TYPES:
            _require(retrain_mode != "no_retrain", f"{run_id} retrain validation requires retrain mode")
            _require(isinstance(run.get("seed_policy"), str) and run["seed_policy"].strip(), f"{run_id} requires seed_policy")
        if status == "PASSED":
            _require(_non_empty_dict(run.get("metrics_json")), f"{run_id} PASSED requires metrics_json")
            _require(_non_empty_dict(run.get("artifact_manifest_json")), f"{run_id} PASSED requires artifact_manifest_json")
            _require(isinstance(run.get("completed_at"), str) and run["completed_at"].strip(), f"{run_id} PASSED requires completed_at")
        if validation_type == "original_fixed_weight" and status == "PASSED":
            gates["original_fixed_weight_retest"] = True
        if isinstance(run.get("random_seed"), int) and _has_metric(run.get("metrics_json"), metric_key):
            seed_values.add(int(run["random_seed"]))
        metrics_json = run.get("metrics_json") if isinstance(run.get("metrics_json"), dict) else {}
        evidence_json = run.get("evidence_json") if isinstance(run.get("evidence_json"), dict) else {}
        regime_metrics = evidence_json.get("regime_metrics") or metrics_json.get("regime_metrics")
        if isinstance(regime_metrics, dict):
            regime_samples += sum(1 for value in regime_metrics.values() if _has_metric(value, metric_key))
        rows.append(
            PlannedRow(
                table="strategy_pkg.package_validation_run",
                natural_key={"validation_run_id": run_id},
                action="insert_append_only_validation_evidence",
                columns={
                    "validation_run_id": run_id,
                    "package_id": package["package_id"],
                    "manifest_sha256": package["manifest_sha256"],
                    "runtime_variant_id": run.get("runtime_variant_id"),
                    "runtime_variant_hash": run.get("runtime_variant_hash"),
                    "validation_type": validation_type,
                    "retrain_mode": retrain_mode,
                    "model_version_id": run.get("model_version_id"),
                    "seed_policy": run.get("seed_policy"),
                    "random_seed": run.get("random_seed"),
                    "source_data_version": run.get("source_data_version"),
                    "target_data_version": run.get("target_data_version"),
                    "backtest_start": run.get("backtest_start"),
                    "backtest_end": run.get("backtest_end"),
                    "status": status,
                    "metrics_json": metrics_json,
                    "artifact_manifest_json": run.get("artifact_manifest_json") or {},
                    "evidence_json": evidence_json,
                    "reproducibility_level": run.get("reproducibility_level") or "UNKNOWN",
                    "created_by": run.get("created_by") or "codex_governance_backfill_plan",
                    "completed_at": run.get("completed_at"),
                },
            )
        )
    # These gates prove only sample-count presence; they do not assert stability quality.
    gates["seed_sample_count_present"] = len(seed_values) >= 2
    gates["regime_sample_count_present"] = regime_samples >= 2
    blockers.extend(name for name, passed in gates.items() if not passed)
    return rows, gates, blockers


def _planned_runtime_rows(package: dict[str, Any]) -> tuple[list[PlannedRow], bool, list[str]]:
    variants = package.get("runtime_variants")
    if variants is None and isinstance(package.get("runtime_variant"), dict):
        variants = [package["runtime_variant"]]
    variants = _as_list(variants or [], field="runtime_variants")
    rows: list[PlannedRow] = []
    blockers: list[str] = []
    candidate_ready = False
    for variant in variants:
        _require(isinstance(variant, dict), "runtime_variants entries must be objects")
        variant_id = _text(variant.get("variant_id"))
        _require(variant.get("manifest_sha256") == package["manifest_sha256"], f"{variant_id} manifest_sha256 must match package")
        locked_core_hash = _text(variant.get("locked_core_hash"))
        variant_hash = _text(variant.get("variant_hash"))
        validation_status = _text(variant.get("validation_status"))
        paper_candidate = bool(variant.get("paper_candidate", False))
        if paper_candidate:
            _require(validation_status == "VALIDATION_PASSED", f"{variant_id} paper_candidate requires VALIDATION_PASSED")
            _require(_non_empty_dict(variant.get("validation_evidence")), f"{variant_id} paper_candidate requires validation_evidence")
            candidate_ready = True
        rows.append(
            PlannedRow(
                table="strategy_pkg.package_runtime_variant",
                natural_key={"variant_id": variant_id},
                action="insert_or_update_runtime_candidate_evidence",
                columns={
                    "variant_id": variant_id,
                    "package_id": package["package_id"],
                    "manifest_sha256": package["manifest_sha256"],
                    "locked_core_hash": locked_core_hash,
                    "variant_name": variant.get("variant_name") or "governance backfill runtime candidate",
                    "variant_kind": variant.get("variant_kind") or "risk_policy",
                    "variant_config": variant.get("variant_config") or {},
                    "variant_hash": variant_hash,
                    "validation_status": validation_status,
                    "paper_candidate": paper_candidate,
                    "validation_evidence": variant.get("validation_evidence") or {},
                    "created_by": variant.get("created_by") or "codex_governance_backfill_plan",
                },
            )
        )
    if not candidate_ready:
        blockers.append("runtime_variant_paper_candidate_missing")
    return rows, candidate_ready, blockers


def _planned_seed_fragility_row(package: dict[str, Any]) -> list[PlannedRow]:
    score = package.get("seed_fragility_score")
    if score is None:
        return []
    _require(isinstance(score, dict), "seed_fragility_score must be an object")
    _require(score.get("manifest_sha256") == package["manifest_sha256"], "seed_fragility_score manifest_sha256 must match package")
    return [
        PlannedRow(
            table="strategy_pkg.seed_fragility_score",
            natural_key={"package_id": package["package_id"]},
            action="insert_or_update_same_manifest_seed_fragility_summary",
            columns={
                "package_id": package["package_id"],
                "manifest_sha256": package["manifest_sha256"],
                "seed_policy": score.get("seed_policy") or "fixed",
                "master_seed": score.get("master_seed"),
                "seed_sequence": score.get("seed_sequence") or [],
                "metric_mean_by_seed": score.get("metric_mean_by_seed") or {},
                "metric_std_by_seed": score.get("metric_std_by_seed") or {},
                "worst_seed_metric": score.get("worst_seed_metric") or {},
                "best_seed_metric": score.get("best_seed_metric") or {},
                "seed_sensitivity_score": score.get("seed_sensitivity_score"),
                "rank_stability": score.get("rank_stability"),
                "factor_importance_stability": score.get("factor_importance_stability") or {},
                "selection_overlap_by_seed": score.get("selection_overlap_by_seed") or {},
                "seed_fragile": bool(score.get("seed_fragile", False)),
                "reproducibility_level": score.get("reproducibility_level") or "audit_only",
                "nondeterministic_flags": score.get("nondeterministic_flags") or [],
                "evidence": score.get("evidence") or {},
            },
        )
    ]


def _package_plan(package: dict[str, Any], *, metric_key: str) -> PackagePlan:
    package_id = _text(package.get("package_id"))
    manifest_sha256 = _text(package.get("manifest_sha256"))
    package_status = _text(package.get("package_status"))
    package = dict(package, package_id=package_id, manifest_sha256=manifest_sha256, package_status=package_status)
    rows: list[PlannedRow] = []
    blockers: list[str] = []
    if package_status not in ALLOWED_PACKAGE_STATUSES:
        blockers.append(f"package_status={package_status}")
    asset_rows, asset_gate, asset_blockers = _planned_asset_rows(package)
    validation_rows, validation_gates, validation_blockers = _planned_validation_rows(package, metric_key=metric_key)
    runtime_rows, runtime_gate, runtime_blockers = _planned_runtime_rows(package)
    seed_fragility_rows = _planned_seed_fragility_row(package)
    rows.extend(asset_rows)
    rows.extend(validation_rows)
    rows.extend(runtime_rows)
    rows.extend(seed_fragility_rows)
    blockers.extend(asset_blockers)
    blockers.extend(validation_blockers)
    blockers.extend(runtime_blockers)
    required_gates = {
        "manifest_identity": package_status in ALLOWED_PACKAGE_STATUSES,
        "protected_assets": asset_gate,
        **validation_gates,
        "runtime_variant_candidate": runtime_gate,
    }
    return PackagePlan(
        package_id=package_id,
        manifest_sha256=manifest_sha256,
        package_status=package_status,
        required_gates=required_gates,
        blockers=list(dict.fromkeys(blockers)),
        rows=rows,
    )


def build_plan(payload: dict[str, Any], *, requested_ids: list[str] | None = None, metric_key: str = "annual_return") -> dict[str, Any]:
    _require(payload.get("schema_version") == SCHEMA_VERSION, f"schema_version must be {SCHEMA_VERSION}")
    packages = _validate_package_set(_as_list(payload.get("packages"), field="packages"), requested_ids)
    package_plans = [_package_plan(package, metric_key=metric_key) for package in packages]
    blocking = {plan.package_id: plan.blockers for plan in package_plans if plan.blockers}
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "blocked" if blocking else "passed",
        "mode": "dry_run_plan",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "package_count": len(package_plans),
        "package_ids": [plan.package_id for plan in package_plans],
        "metric_key": metric_key,
        "db_connection_opened": False,
        "db_writes_executed": False,
        "service_calls_executed": False,
        "blocked_packages": blocking,
        "packages": [
            {
                **asdict(plan),
                "row_count": len(plan.rows),
                "tables": sorted({row.table for row in plan.rows}),
            }
            for plan in package_plans
        ],
        "safety_notes": [
            "Planner only: no DB connection, no INSERT/UPDATE/DELETE execution, no service calls.",
            "The later executor, if separately authorized, must still re-check package_status and manifest_sha256 in the target DB.",
            "seed_sample_count_present and regime_sample_count_present prove sample-count presence only, not variance stability.",
            "Do not mutate manifest_json, manifest_sha256, package_status, model artifacts, or Paper runtime state.",
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-bundle", required=True, help="JSON evidence bundle containing exactly four packages.")
    parser.add_argument("--package-id", action="append", help="Allowed package ID; repeat exactly four times.")
    parser.add_argument("--metric-key", default="annual_return", help="Metric key required for stability samples.")
    parser.add_argument("--json", action="store_true", help="Emit JSON report.")
    parser.add_argument("--output", help="Optional path to write the JSON report.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        payload = _load_json(Path(args.evidence_bundle))
        report = build_plan(payload, requested_ids=_requested_package_ids(args.package_id), metric_key=args.metric_key)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as handle:
                json.dump(report, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(f"status={report['status']} mode={report['mode']} package_count={report['package_count']} db_writes_executed=false")
        return 0 if report["status"] == "passed" else 2
    except GovernanceEvidenceBackfillPlanError as exc:
        payload = {"status": "failed", "mode": "dry_run_plan", "error": str(exc)}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(f"status=failed error={exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
