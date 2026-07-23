"""Prepare both approved direct L1/L2 HMM Risk model sets offline."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk.state_model_set import (  # noqa: E402
    ALL_CORE_FEATURES,
    BASE_FEATURES,
    C008_B3_DIAG02_CONTRACT,
    C008_B3_STRUCTURAL_CONTRACT,
    StateModelSetError,
    StateModelSetSpec,
    build_state_model_set,
    canonical_json_bytes,
    canonical_sha256,
    c008_b3_diag02_fixed_numeric_environment,
    diagnostic_runtime_versions,
    diagnose_l1_seed_grid,
    diagnose_l1_seed_grid_b1,
    diagnose_l1_seed_grid_b3_diag02,
    parse_l2_artifact,
    sha256_bytes,
    train_l1_models,
    write_state_model_set,
)
from backend.services.hmm_risk.stock_fact_observation import (  # noqa: E402
    MIN_COVERAGE,
    OBSERVATION_VERSION,
    build_l1_feature_panel,
    build_l1_training_series,
)
from backend.services.hmm_risk.stock_fact_repository import (  # noqa: E402
    PostgresStockFactReader,
    StockFactSourceSpec,
    load_daily_aggregates,
    load_mapping_manifest,
)


REQUEST_SCHEMA = "hmm_risk_state_model_set_preparation_request_v1"


def _read_env_file(path: Path) -> None:
    if not path.is_file():
        raise StateModelSetError(f"env file does not exist: {path}")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _date(value: Any, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise StateModelSetError(f"{field} must be ISO YYYY-MM-DD") from exc


def _load_request(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StateModelSetError(f"preparation request cannot be read: {exc}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != REQUEST_SCHEMA:
        raise StateModelSetError(f"preparation request must use {REQUEST_SCHEMA}")
    if not isinstance(value.get("source"), dict):
        raise StateModelSetError("preparation request source is required")
    families = value.get("families")
    if not isinstance(families, list) or len(families) != 2 or any(not isinstance(item, dict) for item in families):
        raise StateModelSetError("preparation request must contain exactly two family objects")
    return value


def _git_commit() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _resolve_artifact(root: Path, relative_path: str) -> Path:
    normalized = str(relative_path or "").replace("\\", "/")
    parts = normalized.split("/")
    if not normalized or normalized.startswith("/") or any(part in {"", ".", ".."} for part in parts):
        raise StateModelSetError("l2_relative_path must be a normalized relative path")
    target = root.joinpath(*parts).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise StateModelSetError("L2 artifact escapes the configured artifact root") from exc
    if not target.is_file():
        raise StateModelSetError(f"L2 artifact does not exist: {normalized}")
    return target


def _connect_readonly(prefix: str):
    names = {
        name: os.environ.get(f"{prefix}{name}", "").strip() for name in ("HOST", "PORT", "NAME", "USER", "PASSWORD")
    }
    missing = [name for name, value in names.items() if not value]
    if missing:
        raise StateModelSetError(f"database environment is incomplete for prefix {prefix}: {missing}")
    conn = psycopg2.connect(
        host=names["HOST"],
        port=int(names["PORT"]),
        dbname=names["NAME"],
        user=names["USER"],
        password=names["PASSWORD"],
        connect_timeout=10,
    )
    conn.set_session(readonly=True, autocommit=False)
    return conn, {"host": names["HOST"], "port": int(names["PORT"]), "dbname": names["NAME"]}


def _load_calendar_and_benchmark(
    conn: Any, start: date, end: date
) -> tuple[list[date], dict[date, float], dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT c.cal_date::date,i.pct_chg::float8
            FROM market.trading_calendar c
            LEFT JOIN market.index_daily i ON i.trade_date=c.cal_date AND i.ts_code='000300.SH'
            WHERE c.is_trading=true AND c.cal_date BETWEEN %s AND %s
            ORDER BY c.cal_date
            """,
            (start, end),
        )
        rows = cursor.fetchall()
    if not rows or any(item[1] is None for item in rows):
        raise StateModelSetError("calendar/CSI300 benchmark coverage is incomplete")
    calendar = [item[0] for item in rows]
    benchmark = {item[0]: float(item[1]) / 100.0 for item in rows}
    manifest = {
        "schema_version": "hmm_risk_calendar_benchmark_manifest_v1",
        "start": calendar[0].isoformat(),
        "end": calendar[-1].isoformat(),
        "row_count": len(rows),
        "rows": [[item[0].isoformat(), float(item[1]) / 100.0] for item in rows],
    }
    return calendar, benchmark, manifest


def _family_spec(
    item: dict[str, Any],
    *,
    request: dict[str, Any],
    producer_commit: str,
    source_l2_uri: str,
    source_l2_sha256: str,
    dataset_manifest: dict[str, Any],
    mapping_manifest: dict[str, Any],
    feature_definition: dict[str, Any],
) -> StateModelSetSpec:
    candidate_ids = tuple(sorted(str(value) for value in item.get("candidate_ids") or ()))
    return StateModelSetSpec(
        family=str(item.get("family") or ""),
        family_version=str(item.get("family_version") or ""),
        producer_commit=producer_commit,
        created_at=str(request.get("created_at") or ""),
        candidate_ids=candidate_ids,
        parser_contract=str(item.get("parser_contract") or ""),
        source_l2_artifact_uri=source_l2_uri,
        source_l2_artifact_sha256=source_l2_sha256,
        train_start=_date(item.get("train_start"), "train_start"),
        train_end=_date(item.get("train_end"), "train_end"),
        validation_start=_date(item.get("validation_start"), "validation_start"),
        validation_end=_date(item.get("validation_end"), "validation_end"),
        common_data_watermark=_date(request.get("common_data_watermark"), "common_data_watermark"),
        dataset_manifest=dataset_manifest,
        mapping_manifest=mapping_manifest,
        feature_definition=feature_definition,
        observation_version=OBSERVATION_VERSION,
        preprocess_family=str(item.get("preprocess_family") or ""),
        random_seed=int(item.get("random_seed", 42)),
    )


def _load_l1_source_inputs(request: dict[str, Any], *, db_prefix: str) -> dict[str, Any]:
    source = request["source"]
    source_spec = StockFactSourceSpec(
        universe_key=str(source.get("universe_key") or ""),
        universe_rule_version=str(source.get("universe_rule_version") or ""),
        source_start=_date(source.get("source_start"), "source_start"),
        source_end=_date(source.get("source_end"), "source_end"),
    )
    conn, db_identity = _connect_readonly(db_prefix)
    try:
        reader = PostgresStockFactReader(conn, source_spec)
        source_state = reader.validate_source()
        reader.load_classification_lookup()
        reader.validate_fact_uniqueness()
        mapping_manifest, constituents = load_mapping_manifest(reader)
        aggregates, stock_fact_manifest = load_daily_aggregates(reader, min_coverage=MIN_COVERAGE)
        calendar, benchmark, benchmark_manifest = _load_calendar_and_benchmark(
            conn,
            source_spec.source_start,
            source_spec.source_end,
        )
    finally:
        conn.rollback()
        conn.close()
    panel, feature_definition = build_l1_feature_panel(
        aggregates,
        trading_dates=calendar,
        csi300_returns=benchmark,
    )
    dataset_manifest = {
        "schema_version": "hmm_risk_state_model_set_dataset_manifest_v1",
        "source_state": source_state,
        "stock_facts": stock_fact_manifest,
        "calendar_benchmark": benchmark_manifest,
    }
    return {
        "source_spec": source_spec,
        "database": db_identity,
        "mapping_manifest": mapping_manifest,
        "constituents": constituents,
        "panel": panel,
        "feature_definition": feature_definition,
        "dataset_manifest": dataset_manifest,
    }


def prepare(request: dict[str, Any], *, artifact_root: Path, output_root: Path, db_prefix: str) -> dict[str, Any]:
    producer_commit = _git_commit()
    if str(request.get("producer_commit") or "") != producer_commit:
        raise StateModelSetError(
            f"request producer_commit differs from current code expected={request.get('producer_commit')} actual={producer_commit}"
        )
    inputs = _load_l1_source_inputs(request, db_prefix=db_prefix)
    source_spec = inputs["source_spec"]
    db_identity = inputs["database"]
    mapping_manifest = inputs["mapping_manifest"]
    constituents = inputs["constituents"]
    panel = inputs["panel"]
    feature_definition = inputs["feature_definition"]
    dataset_manifest = inputs["dataset_manifest"]
    expected_l2_codes = tuple(sorted({code for item in constituents.values() for code in item["l2_codes"]}))
    receipts = []
    for family in request["families"]:
        feature_names = tuple(str(value) for value in family.get("feature_names") or ())
        if feature_names not in {BASE_FEATURES, ALL_CORE_FEATURES}:
            raise StateModelSetError("family feature_names must exactly match the approved 7/20-dimensional order")
        source_path = _resolve_artifact(artifact_root, str(family.get("l2_relative_path") or ""))
        source_bytes = source_path.read_bytes()
        source_sha256 = str(family.get("l2_artifact_sha256") or "")
        l2 = parse_l2_artifact(
            source_bytes,
            parser_contract=str(family.get("parser_contract") or ""),
            expected_sha256=source_sha256,
            expected_sector_codes=expected_l2_codes,
            expected_features=feature_names,
        )
        spec = _family_spec(
            family,
            request=request,
            producer_commit=producer_commit,
            source_l2_uri=f"configured://{source_path.relative_to(artifact_root).as_posix()}",
            source_l2_sha256=source_sha256,
            dataset_manifest=dataset_manifest,
            mapping_manifest=mapping_manifest,
            feature_definition={**feature_definition, "selected_features": list(feature_names)},
        )
        series = build_l1_training_series(
            panel,
            feature_names=feature_names,
            train_start=spec.train_start,
            train_end=spec.train_end,
            validation_start=spec.validation_start,
            validation_end=spec.validation_end,
            constituent_manifest_by_l1=constituents,
        )
        l1 = train_l1_models(
            series,
            feature_names=feature_names,
            preprocess_family=spec.preprocess_family,
            random_seed=spec.random_seed,
            observation_version=OBSERVATION_VERSION,
        )
        manifest, l1_bytes, l2_bytes = build_state_model_set(spec=spec, l1_artifact=l1, l2_artifact=l2)
        manifest_path = write_state_model_set(
            output_root,
            manifest=manifest,
            l1_bytes=l1_bytes,
            l2_bytes=l2_bytes,
        )
        receipts.append(
            {
                "family": spec.family,
                "state_model_set_id": manifest["state_model_set_id"],
                "state_model_set_hash": manifest["state_model_set_hash"],
                "manifest_path": str(manifest_path),
                "status": "READY",
                "l1_sector_count": 31,
                "l2_sector_count": 131,
            }
        )
    return {
        "schema_version": "hmm_risk_state_model_set_preparation_receipt_v1",
        "producer_commit": producer_commit,
        "database": db_identity,
        "universe_key": source_spec.universe_key,
        "families": receipts,
    }


def _diagnose_c008(request: dict[str, Any], *, db_prefix: str, include_b1_evidence: bool) -> dict[str, Any]:
    """Load one immutable PIT input and run the requested non-selecting C-008 diagnostic."""

    producer_commit = _git_commit()
    if str(request.get("producer_commit") or "") != producer_commit:
        raise StateModelSetError(
            f"request producer_commit differs from current code expected={request.get('producer_commit')} actual={producer_commit}"
        )
    inputs = _load_l1_source_inputs(request, db_prefix=db_prefix)
    source_spec = inputs["source_spec"]
    families: list[dict[str, Any]] = []
    for family in request["families"]:
        feature_names = tuple(str(value) for value in family.get("feature_names") or ())
        if feature_names not in {BASE_FEATURES, ALL_CORE_FEATURES}:
            raise StateModelSetError("family feature_names must exactly match the approved 7/20-dimensional order")
        spec = _family_spec(
            family,
            request=request,
            producer_commit=producer_commit,
            source_l2_uri=f"configured://{str(family.get('l2_relative_path') or '').replace('\\', '/')}",
            source_l2_sha256=str(family.get("l2_artifact_sha256") or ""),
            dataset_manifest=inputs["dataset_manifest"],
            mapping_manifest=inputs["mapping_manifest"],
            feature_definition={**inputs["feature_definition"], "selected_features": list(feature_names)},
        )
        series = build_l1_training_series(
            inputs["panel"],
            feature_names=feature_names,
            train_start=spec.train_start,
            train_end=spec.train_end,
            validation_start=spec.validation_start,
            validation_end=spec.validation_end,
            constituent_manifest_by_l1=inputs["constituents"],
        )
        families.append(
            {
                "family": spec.family,
                "family_version": spec.family_version,
                "candidate_ids": list(spec.candidate_ids),
                "diagnostic": (diagnose_l1_seed_grid_b1 if include_b1_evidence else diagnose_l1_seed_grid)(
                    series,
                    feature_names=feature_names,
                    preprocess_family=spec.preprocess_family,
                ),
            }
        )
    report = {
        "schema_version": (
            "hmm_risk_c008_b1_soft_evidence_report_v1"
            if include_b1_evidence
            else "hmm_risk_c008_seed_diagnostic_report_v1"
        ),
        "status": "diagnostic_complete",
        "diagnostic_contract": "C-008-B1" if include_b1_evidence else "C-008-A",
        "producer_commit": producer_commit,
        "database": inputs["database"],
        "universe_key": source_spec.universe_key,
        "dataset_manifest": inputs["dataset_manifest"],
        "dataset_manifest_hash": canonical_sha256(inputs["dataset_manifest"]),
        "mapping_manifest": inputs["mapping_manifest"],
        "mapping_manifest_hash": canonical_sha256(inputs["mapping_manifest"]),
        "selection_performed": False,
        "ready_artifact_write_performed": False,
        "families": families,
    }
    if include_b1_evidence:
        report["runtime_versions"] = diagnostic_runtime_versions()
        report["formal_acceptance_thresholds_applied"] = False
        report["hard_semantic_authority_changed"] = False
    return report


def diagnose_c008(request: dict[str, Any], *, db_prefix: str) -> dict[str, Any]:
    """Run the user-approved C-008-A grid without selecting or writing models."""

    return _diagnose_c008(request, db_prefix=db_prefix, include_b1_evidence=False)


def diagnose_c008_b1(request: dict[str, Any], *, db_prefix: str) -> dict[str, Any]:
    """Run approved C-008-B1 evidence expansion without changing model semantics."""

    return _diagnose_c008(request, db_prefix=db_prefix, include_b1_evidence=True)


def diagnose_c008_b3_diag02(request: dict[str, Any], *, db_prefix: str) -> dict[str, Any]:
    """Run one fixed-environment structural diagnostic pass without selecting or writing models."""

    producer_commit = _git_commit()
    if str(request.get("producer_commit") or "") != producer_commit:
        raise StateModelSetError(
            f"request producer_commit differs from current code expected={request.get('producer_commit')} actual={producer_commit}"
        )
    c008_b3_diag02_fixed_numeric_environment()
    inputs = _load_l1_source_inputs(request, db_prefix=db_prefix)
    source_spec = inputs["source_spec"]
    families: list[dict[str, Any]] = []
    for family in request["families"]:
        feature_names = tuple(str(value) for value in family.get("feature_names") or ())
        if feature_names not in {BASE_FEATURES, ALL_CORE_FEATURES}:
            raise StateModelSetError("family feature_names must exactly match the approved 7/20-dimensional order")
        spec = _family_spec(
            family,
            request=request,
            producer_commit=producer_commit,
            source_l2_uri=f"configured://{str(family.get('l2_relative_path') or '').replace('\\', '/')}",
            source_l2_sha256=str(family.get("l2_artifact_sha256") or ""),
            dataset_manifest=inputs["dataset_manifest"],
            mapping_manifest=inputs["mapping_manifest"],
            feature_definition={**inputs["feature_definition"], "selected_features": list(feature_names)},
        )
        series = build_l1_training_series(
            inputs["panel"],
            feature_names=feature_names,
            train_start=spec.train_start,
            train_end=spec.train_end,
            validation_start=spec.validation_start,
            validation_end=spec.validation_end,
            constituent_manifest_by_l1=inputs["constituents"],
        )
        families.append(
            {
                "family": spec.family,
                "family_version": spec.family_version,
                "candidate_ids": list(spec.candidate_ids),
                "diagnostic": diagnose_l1_seed_grid_b3_diag02(
                    series,
                    feature_names=feature_names,
                    preprocess_family=spec.preprocess_family,
                ),
            }
        )
    return {
        "schema_version": "hmm_risk_c008_b3_diag02_single_pass_report_v1",
        "status": "diagnostic_complete",
        "diagnostic_contract": C008_B3_DIAG02_CONTRACT,
        "structural_contract": C008_B3_STRUCTURAL_CONTRACT,
        "producer_commit": producer_commit,
        "database": inputs["database"],
        "database_write_performed": False,
        "runtime_action_performed": False,
        "universe_key": source_spec.universe_key,
        "dataset_manifest": inputs["dataset_manifest"],
        "dataset_manifest_hash": canonical_sha256(inputs["dataset_manifest"]),
        "mapping_manifest": inputs["mapping_manifest"],
        "mapping_manifest_hash": canonical_sha256(inputs["mapping_manifest"]),
        "fixed_numeric_environment": c008_b3_diag02_fixed_numeric_environment(),
        "selection_performed": False,
        "formal_acceptance_thresholds_applied": False,
        "hard_semantic_authority_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "d4_exact_contract_approved": False,
        "d5_01_exact_contract_approved": False,
        "d6_exact_contract_approved": False,
        "families": families,
    }


def _c008_b3_diag02_child_command(args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--request",
        str(Path(args.request).resolve()),
        "--artifact-root",
        str(Path(args.artifact_root).resolve()),
        "--output-root",
        str(Path(args.output_root).resolve()),
        "--env-file",
        str(Path(args.env_file).resolve()),
        "--db-env-prefix",
        str(args.db_env_prefix),
        "--_c008-b3-diag02-child",
    ]


def _run_c008_b3_diag02_repeated(args: argparse.Namespace) -> tuple[dict[str, Any], bool]:
    """Execute two fresh child processes and compare their canonical report bytes bit-for-bit."""

    repeat_payloads: list[bytes] = []
    stderr_receipts: list[dict[str, Any]] = []
    for repeat_index in (1, 2):
        completed = subprocess.run(
            _c008_b3_diag02_child_command(args),
            cwd=ROOT,
            env=os.environ.copy(),
            check=False,
            capture_output=True,
        )
        if completed.returncode != 0:
            error = completed.stderr.decode("utf-8", errors="replace").strip()
            raise StateModelSetError(f"C-008-B3-DIAG-02 repeat {repeat_index} failed: {error[-4000:]}")
        try:
            json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise StateModelSetError(f"C-008-B3-DIAG-02 repeat {repeat_index} returned invalid JSON") from exc
        repeat_payloads.append(completed.stdout)
        stderr_receipts.append(
            {
                "repeat_index": repeat_index,
                "stderr_line_count": len(completed.stderr.splitlines()),
                "stderr_sha256": sha256_bytes(completed.stderr),
            }
        )
    repeat_hashes = [sha256_bytes(payload) for payload in repeat_payloads]
    bitwise_equal = repeat_payloads[0] == repeat_payloads[1]
    report = json.loads(repeat_payloads[0])
    report["single_pass_schema_version"] = report["schema_version"]
    report["schema_version"] = "hmm_risk_c008_b3_diag02_repeated_report_v1"
    report["status"] = "diagnostic_complete" if bitwise_equal else "diagnostic_reproducibility_failed"
    report["reproducibility"] = {
        "schema_version": "hmm_risk_c008_b3_diag02_reproducibility_v1",
        "scope": "same_host_same_fixed_numeric_environment_only",
        "fresh_process_repeat_count": 2,
        "canonical_payload_sha256_by_repeat": repeat_hashes,
        "canonical_payload_bitwise_equal": bitwise_equal,
        "numeric_tolerance_used_for_acceptance": False,
        "stderr_diagnostic_receipts": stderr_receipts,
    }
    return report, bitwise_equal


def _write_diagnostic_report(path: Path, report: dict[str, Any]) -> str:
    payload = canonical_json_bytes(report) + b"\n"
    if path.exists():
        if path.read_bytes() != payload:
            raise StateModelSetError(f"diagnostic report collision: {path}")
        return canonical_sha256(report)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise
    return canonical_sha256(report)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True, help="Explicit preparation request JSON.")
    parser.add_argument("--artifact-root", required=True, help="Configured root containing approved L2 artifacts.")
    parser.add_argument("--output-root", required=True, help="Output root for content-addressed READY sets.")
    parser.add_argument("--env-file", required=True, help="Credential-location file; secret values are never printed.")
    parser.add_argument("--db-env-prefix", required=True, help="Environment prefix such as TDX_DB_ or TDX_DB_DEV_.")
    diagnostic_group = parser.add_mutually_exclusive_group()
    diagnostic_group.add_argument(
        "--c008-diagnostic-output",
        help="Reproduce the approved C-008-A seeds 42-49 diagnostic report.",
    )
    diagnostic_group.add_argument(
        "--c008-b1-diagnostic-output",
        help="Run approved C-008-B1 soft/numeric evidence diagnostics without selection or model writes.",
    )
    diagnostic_group.add_argument(
        "--c008-b3-diag02-output",
        help="Run approved C-008-B3-DIAG-02 twice in fresh fixed-environment processes without selection/model writes.",
    )
    parser.add_argument("--_c008-b3-diag02-child", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        _read_env_file(Path(args.env_file).resolve())
        request = _load_request(Path(args.request).resolve())
        if args._c008_b3_diag02_child:
            report = diagnose_c008_b3_diag02(request, db_prefix=str(args.db_env_prefix))
            sys.stdout.buffer.write(canonical_json_bytes(report))
            return 0
        if args.c008_b3_diag02_output:
            report, reproducible = _run_c008_b3_diag02_repeated(args)
            report_path = Path(args.c008_b3_diag02_output).resolve()
            report_sha256 = _write_diagnostic_report(report_path, report)
            receipt = {
                "schema_version": "hmm_risk_c008_b3_diag02_receipt_v1",
                "status": report["status"],
                "diagnostic_contract": C008_B3_DIAG02_CONTRACT,
                "structural_contract": C008_B3_STRUCTURAL_CONTRACT,
                "report_path": str(report_path),
                "report_sha256": report_sha256,
                "fresh_process_repeat_count": 2,
                "canonical_payload_bitwise_equal": reproducible,
                "selection_performed": False,
                "formal_acceptance_thresholds_applied": False,
                "hard_semantic_authority_changed": False,
                "model_write_performed": False,
                "ready_artifact_write_performed": False,
                "family_count": len(report["families"]),
            }
            print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
            return 0 if reproducible else 1
        if args.c008_diagnostic_output or args.c008_b1_diagnostic_output:
            include_b1 = bool(args.c008_b1_diagnostic_output)
            report_path = Path(args.c008_b1_diagnostic_output or args.c008_diagnostic_output).resolve()
            report = (
                diagnose_c008_b1(request, db_prefix=str(args.db_env_prefix))
                if include_b1
                else diagnose_c008(request, db_prefix=str(args.db_env_prefix))
            )
            report_sha256 = _write_diagnostic_report(report_path, report)
            receipt = {
                "schema_version": (
                    "hmm_risk_c008_b1_soft_evidence_receipt_v1"
                    if include_b1
                    else "hmm_risk_c008_seed_diagnostic_receipt_v1"
                ),
                "status": "diagnostic_complete",
                "diagnostic_contract": "C-008-B1" if include_b1 else "C-008-A",
                "report_path": str(report_path),
                "report_sha256": report_sha256,
                "selection_performed": False,
                "ready_artifact_write_performed": False,
                "formal_acceptance_thresholds_applied": False if include_b1 else None,
                "hard_semantic_authority_changed": False if include_b1 else None,
                "family_count": len(report["families"]),
            }
            if not include_b1:
                receipt.pop("formal_acceptance_thresholds_applied")
                receipt.pop("hard_semantic_authority_changed")
        else:
            receipt = prepare(
                request,
                artifact_root=Path(args.artifact_root).resolve(),
                output_root=Path(args.output_root).resolve(),
                db_prefix=str(args.db_env_prefix),
            )
    except Exception as exc:
        error = {
            "schema_version": "hmm_risk_state_model_set_preparation_error_v1",
            "status": "failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        print(json.dumps(error, ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
