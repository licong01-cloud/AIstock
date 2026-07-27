"""Prepare both approved direct L1/L2 HMM Risk model sets offline."""

from __future__ import annotations

import argparse
from copy import deepcopy
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
    C008_B3_DIAG04_CONTRACT,
    C008_B3_STRUCTURAL_CONTRACT,
    StateModelSetError,
    StateModelSetSpec,
    canonical_json_bytes,
    canonical_sha256,
    c008_b3_diag02_fixed_numeric_environment,
    c008_b3_diag04_fixed_numeric_environment,
    diagnostic_runtime_versions,
    diagnose_l1_seed_grid,
    diagnose_l1_seed_grid_b1,
    diagnose_l1_seed_grid_b3_diag02,
    diagnose_l1_seed_grid_b3_diag04,
    sha256_bytes,
)
from backend.services.hmm_risk.b3_acceptance import select_level_restart  # noqa: E402
from backend.services.hmm_risk.b3_training import (  # noqa: E402
    audit_train_only_coverage,
    build_train_only_series,
    build_selected_level_artifact,
    models_from_repeat,
    run_level_repeat,
    write_b3_ready_model_set,
)
from backend.services.hmm_risk.stock_fact_observation import (  # noqa: E402
    MIN_COVERAGE,
    OBSERVATION_VERSION,
    build_l1_feature_panel,
    build_l1_training_series,
)
from backend.services.hmm_risk.security_identity import (  # noqa: E402
    load_security_source_identity_manifest,
)
from backend.services.hmm_risk.provider_absence import load_provider_absence_manifest  # noqa: E402
from backend.services.hmm_risk.stock_fact_repository import (  # noqa: E402
    PostgresStockFactReader,
    StockFactSourceSpec,
    load_direct_daily_aggregates,
    load_mapping_manifest,
)


REQUEST_SCHEMA = "hmm_risk_state_model_set_preparation_request_v1"
B3_PREFLIGHT_SCHEMA = "hmm_risk_b3_formal_preflight_v1"
C009_STOCK_FACT_PREFLIGHT_SCHEMA = "hmm_risk_c009_stock_fact_preflight_v1"
B3_TRAIN_COVERAGE_PREFLIGHT_VERSION = "hmm_risk_b3_train_coverage_preflight_set_v1"
B3_APPROVED_FROZEN_IDENTITIES = {
    "dataset_manifest_hash": "c07177ddd01b324106755e47ee2cfe61a7f2916e08ccf9e888d3abf1115ebd7f",
    "mapping_manifest_hash": "9cdddd98db3cacd9949ac5b7ba007c16eb66de46375e848eea676b0168b58159",
    "l2_stock_fact_manifest_hash": "d4a5cc86f3230a7bbd5704b81e63fa16cf4dc5a074f461f28112d3c9582d1730",
}


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


def _load_request_template(path: Path) -> dict[str, Any]:
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


def _load_request(path: Path) -> dict[str, Any]:
    value = _load_request_template(path)
    for field in ("dataset_manifest_hash", "mapping_manifest_hash", "l2_stock_fact_manifest_hash"):
        identity = str(value.get(field) or "")
        if len(identity) != 64 or any(character not in "0123456789abcdef" for character in identity.lower()):
            raise StateModelSetError(f"preparation request {field} must be a SHA-256 identity")
    return value


def _require_approved_b3_identities(value: dict[str, Any]) -> None:
    mismatches = []
    for field, expected in B3_APPROVED_FROZEN_IDENTITIES.items():
        actual = str(value.get(field) or "")
        if actual != expected:
            mismatches.append(f"{field} expected={expected} actual={actual or '<missing>'}")
    if mismatches:
        raise StateModelSetError("formal B3 frozen identity mismatch: " + "; ".join(mismatches))


def _require_formal_train_coverage_identity(value: dict[str, Any]) -> None:
    identity = str(value.get("train_coverage_receipt_sha256") or "")
    if (
        value.get("train_coverage_contract_version") != B3_TRAIN_COVERAGE_PREFLIGHT_VERSION
        or len(identity) != 64
        or any(character not in "0123456789abcdef" for character in identity.lower())
    ):
        raise StateModelSetError("formal B3 request train coverage identity is missing or invalid")


def _git_commit() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _formal_producer_commit() -> str:
    producer_commit = _git_commit()
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    if status.stdout.strip():
        raise StateModelSetError("formal B3 producer worktree must be clean")
    return producer_commit


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


def _load_security_identity_manifest(source: dict[str, Any]):
    relative_path = str(source.get("security_identity_manifest_path") or "")
    expected_sha256 = str(source.get("security_identity_manifest_sha256") or "").lower()
    normalized = relative_path.replace("\\", "/")
    parts = normalized.split("/")
    if not normalized or normalized.startswith("/") or any(part in {"", ".", ".."} for part in parts):
        raise StateModelSetError("security_identity_manifest_path must be a normalized repository-relative path")
    path = ROOT.joinpath(*parts).resolve()
    try:
        path.relative_to(ROOT)
    except ValueError as exc:
        raise StateModelSetError("security identity manifest escapes the repository root") from exc
    return load_security_source_identity_manifest(path, expected_sha256=expected_sha256)


def _load_provider_absence_manifest(source: dict[str, Any]):
    relative_path = str(source.get("provider_absence_manifest_path") or "")
    expected_sha256 = str(source.get("provider_absence_manifest_sha256") or "").lower()
    normalized = relative_path.replace("\\", "/")
    parts = normalized.split("/")
    if not normalized or normalized.startswith("/") or any(part in {"", ".", ".."} for part in parts):
        raise StateModelSetError("provider_absence_manifest_path must be a normalized repository-relative path")
    path = ROOT.joinpath(*parts).resolve()
    try:
        path.relative_to(ROOT)
    except ValueError as exc:
        raise StateModelSetError("provider absence manifest escapes the repository root") from exc
    return load_provider_absence_manifest(path, expected_sha256=expected_sha256)


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
    security_identity_manifest = _load_security_identity_manifest(source)
    provider_absence_manifest = _load_provider_absence_manifest(source)
    source_spec = StockFactSourceSpec(
        universe_key=str(source.get("universe_key") or ""),
        universe_rule_version=str(source.get("universe_rule_version") or ""),
        source_start=_date(source.get("source_start"), "source_start"),
        source_end=_date(source.get("source_end"), "source_end"),
    )
    conn, db_identity = _connect_readonly(db_prefix)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET LOCAL cursor_tuple_fraction=1.0")
        reader = PostgresStockFactReader(
            conn,
            source_spec,
            security_identity_manifest=security_identity_manifest,
            provider_absence_manifest=provider_absence_manifest,
        )
        source_state = reader.validate_source()
        source_state["query_plan_contract"] = {
            "cursor_tuple_fraction": 1.0,
            "stock_fact_batching": "calendar_month_split_fact_stream_v1",
            "price_mapping_stream": "server_side_cursor",
            "fact_lookup": "date_bounded_exact_key_maps",
            "causal_circ_mv": "python_state_from_authoritative_daily_basic_only",
            "direct_l1_l2_single_stream": True,
        }
        reader.load_classification_lookup()
        reader.validate_fact_uniqueness()
        mapping_manifest, constituents = load_mapping_manifest(reader)
        aggregates, stock_fact_manifest, l2_aggregates, l2_stock_fact_manifest = load_direct_daily_aggregates(
            reader,
            min_coverage=MIN_COVERAGE,
        )
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
    l2_panel, l2_feature_definition = build_l1_feature_panel(
        l2_aggregates,
        trading_dates=calendar,
        csi300_returns=benchmark,
        expected_sector_count=131,
        direct_sector_level="L2",
    )
    dataset_manifest = {
        "schema_version": "hmm_risk_state_model_set_dataset_manifest_v1",
        "source_state": source_state,
        "stock_facts": stock_fact_manifest,
        "calendar_benchmark": benchmark_manifest,
        "security_source_identity": security_identity_manifest.evidence(),
        "provider_absence_authority": provider_absence_manifest.evidence(),
    }
    return {
        "source_spec": source_spec,
        "database": db_identity,
        "mapping_manifest": mapping_manifest,
        "constituents": constituents,
        "panel": panel,
        "l2_panel": l2_panel,
        "feature_definition": feature_definition,
        "l2_feature_definition": l2_feature_definition,
        "l2_stock_fact_manifest": l2_stock_fact_manifest,
        "dataset_manifest": dataset_manifest,
        "security_identity_manifest": security_identity_manifest.evidence(),
        "provider_absence_manifest": provider_absence_manifest.evidence(),
    }


def _manifest_invalid_sector_date_count(manifest: dict[str, Any]) -> int:
    for field in ("invalid_sector_date_count", "invalid_l1_date_count"):
        if field in manifest:
            return int(manifest[field])
    raise StateModelSetError("stock-fact manifest lacks invalid sector-date count")


def _b3_train_coverage_preflight(inputs: dict[str, Any], request_template: dict[str, Any]) -> dict[str, Any]:
    reports: dict[str, Any] = {}
    for family in sorted(request_template["families"], key=lambda item: str(item.get("family") or "")):
        family_name = str(family.get("family") or "")
        features = tuple(str(value) for value in family.get("feature_names") or ())
        train_start = _date(family.get("train_start"), "train_start")
        train_end = _date(family.get("train_end"), "train_end")
        for level, panel, expected_count in (
            ("L1", inputs["panel"], 31),
            ("L2", inputs["l2_panel"], 131),
        ):
            reports[f"{family_name}:{level}"] = audit_train_only_coverage(
                panel,
                feature_names=features,
                train_start=train_start,
                train_end=train_end,
                expected_sector_count=expected_count,
                direct_sector_level=level,
            )
    valid = len(reports) == 4 and all(report["train_coverage_valid"] for report in reports.values())
    body = {
        "schema_version": B3_TRAIN_COVERAGE_PREFLIGHT_VERSION,
        "reports": reports,
        "report_count": len(reports),
        "train_coverage_valid": valid,
        "failure_reason_codes": [] if valid else ["hmm_risk_model_train_observation_coverage_insufficient"],
        "fit_performed": False,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def prepare_b3_preflight_candidate(request_template: dict[str, Any], *, db_prefix: str) -> dict[str, Any]:
    """Freeze current PIT identities without fitting, selecting, or writing model artifacts."""

    producer_commit = _formal_producer_commit()
    inputs = _load_l1_source_inputs(request_template, db_prefix=db_prefix)
    dataset_hash = canonical_sha256(inputs["dataset_manifest"])
    mapping_hash = canonical_sha256(inputs["mapping_manifest"])
    l2_stock_fact_hash = canonical_sha256(inputs["l2_stock_fact_manifest"])
    frozen_identities = {
        "dataset_manifest_hash": dataset_hash,
        "mapping_manifest_hash": mapping_hash,
        "l2_stock_fact_manifest_hash": l2_stock_fact_hash,
    }
    _require_approved_b3_identities(frozen_identities)
    train_coverage = _b3_train_coverage_preflight(inputs, request_template)
    train_coverage_valid = train_coverage["train_coverage_valid"] is True
    request_candidate = deepcopy(request_template)
    request_candidate.update(
        {
            "producer_commit": producer_commit,
            "dataset_manifest_hash": dataset_hash,
            "mapping_manifest_hash": mapping_hash,
            "l2_stock_fact_manifest_hash": l2_stock_fact_hash,
            "train_coverage_contract_version": B3_TRAIN_COVERAGE_PREFLIGHT_VERSION,
            "train_coverage_receipt_sha256": train_coverage["receipt_sha256"],
        }
    )
    l1_stock_facts = inputs["dataset_manifest"]["stock_facts"]
    l2_stock_facts = inputs["l2_stock_fact_manifest"]
    body = {
        "schema_version": B3_PREFLIGHT_SCHEMA,
        "status": "candidate_ready" if train_coverage_valid else "blocked",
        "source_template_producer_commit": str(request_template.get("producer_commit") or ""),
        "producer_commit": producer_commit,
        "database": inputs["database"],
        "approved_frozen_identities": dict(B3_APPROVED_FROZEN_IDENTITIES),
        "approved_frozen_identities_match": True,
        "dataset_manifest_hash": dataset_hash,
        "mapping_manifest_hash": mapping_hash,
        "l2_stock_fact_manifest_hash": l2_stock_fact_hash,
        "request_candidate": request_candidate,
        "request_candidate_sha256": canonical_sha256(request_candidate),
        "l1_sector_count": 31,
        "l1_aggregate_row_count": int(l1_stock_facts["aggregate_row_count"]),
        "l1_invalid_sector_date_count": _manifest_invalid_sector_date_count(l1_stock_facts),
        "l1_panel_row_count": len(inputs["panel"]),
        "l2_sector_count": 131,
        "l2_aggregate_row_count": int(l2_stock_facts["aggregate_row_count"]),
        "l2_invalid_sector_date_count": _manifest_invalid_sector_date_count(l2_stock_facts),
        "l2_panel_row_count": len(inputs["l2_panel"]),
        "train_coverage": train_coverage,
        "train_coverage_valid": train_coverage_valid,
        "failure_reason_codes": (
            [] if train_coverage_valid else ["hmm_risk_model_train_observation_coverage_insufficient"]
        ),
        "fit_performed": False,
        "selection_performed": False,
        "formal_acceptance_thresholds_applied": False,
        "hard_semantic_authority_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    if not train_coverage_valid:
        body["request_candidate"] = None
        body["request_candidate_sha256"] = None
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _c009_train_source_request(request_template: dict[str, Any]) -> dict[str, Any]:
    windows = {
        (
            _date(family.get("train_start"), "train_start"),
            _date(family.get("train_end"), "train_end"),
        )
        for family in request_template["families"]
    }
    if len(windows) != 1:
        raise StateModelSetError("C-009 preflight requires one immutable train window across both families")
    train_start, train_end = next(iter(windows))
    if train_start > train_end:
        raise StateModelSetError("C-009 preflight train window is invalid")
    source_start = _date(request_template["source"].get("source_start"), "source_start")
    source_end = _date(request_template["source"].get("source_end"), "source_end")
    if train_start < source_start or train_end > source_end:
        raise StateModelSetError("C-009 train window escapes the immutable source window")
    request = deepcopy(request_template)
    request["source"]["source_start"] = train_start.isoformat()
    request["source"]["source_end"] = train_end.isoformat()
    return request


def _c009_source_statistics(inputs: dict[str, Any]) -> dict[str, Any]:
    l1 = inputs["dataset_manifest"]["stock_facts"]
    l2 = inputs["l2_stock_fact_manifest"]
    fields = (
        "moneyflow_provider_absence_count",
        "moneyflow_provider_absence_key_sha256",
        "moneyflow_alias_resolution_count",
        "moneyflow_alias_resolution_key_sha256",
        "circ_mv_asof_stale_count",
        "circ_mv_asof_max_staleness_trading_days",
        "circ_mv_asof_stale_key_sha256",
    )
    mismatches = [field for field in fields if l1.get(field) != l2.get(field)]
    if mismatches:
        raise StateModelSetError("C-009 L1/L2 source evidence mismatch: " + ", ".join(sorted(mismatches)))
    return {field: l1.get(field) for field in fields}


def prepare_c009_stock_fact_preflight(request_template: dict[str, Any], *, db_prefix: str) -> dict[str, Any]:
    """Run the approved 601-day C-009 source-only preflight without fitting or selection."""

    producer_commit = _formal_producer_commit()
    train_request = _c009_train_source_request(request_template)
    inputs = _load_l1_source_inputs(train_request, db_prefix=db_prefix)
    train_coverage = _b3_train_coverage_preflight(inputs, train_request)
    source_statistics = _c009_source_statistics(inputs)
    calendar = inputs["dataset_manifest"]["calendar_benchmark"]
    if int(calendar.get("row_count") or 0) != 601:
        raise StateModelSetError(
            f"C-009 frozen train calendar must contain 601 trading dates, got {calendar.get('row_count')}"
        )
    train_coverage_valid = train_coverage["train_coverage_valid"] is True
    body = {
        "schema_version": C009_STOCK_FACT_PREFLIGHT_SCHEMA,
        "status": "preflight_complete" if train_coverage_valid else "blocked",
        "producer_commit": producer_commit,
        "database": inputs["database"],
        "source_start": train_request["source"]["source_start"],
        "source_end": train_request["source"]["source_end"],
        "trading_date_count": int(calendar["row_count"]),
        "calendar_manifest_sha256": canonical_sha256(calendar),
        "dataset_manifest_hash": canonical_sha256(inputs["dataset_manifest"]),
        "mapping_manifest_hash": canonical_sha256(inputs["mapping_manifest"]),
        "l2_stock_fact_manifest_hash": canonical_sha256(inputs["l2_stock_fact_manifest"]),
        "security_source_identity": inputs["security_identity_manifest"],
        "provider_absence_authority": inputs["provider_absence_manifest"],
        "source_statistics": source_statistics,
        "l1_invalid_sector_date_count": _manifest_invalid_sector_date_count(inputs["dataset_manifest"]["stock_facts"]),
        "l2_invalid_sector_date_count": _manifest_invalid_sector_date_count(inputs["l2_stock_fact_manifest"]),
        "train_coverage": train_coverage,
        "train_coverage_valid": train_coverage_valid,
        "failure_reason_codes": list(train_coverage["failure_reason_codes"]),
        "approved_source_coverage_contract_applied": True,
        "fit_performed": False,
        "selection_performed": False,
        "d5_performed": False,
        "d6_performed": False,
        "formal_model_acceptance_thresholds_applied": False,
        "hard_semantic_authority_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def prepare(request: dict[str, Any], *, artifact_root: Path, output_root: Path, db_prefix: str) -> dict[str, Any]:
    del request, artifact_root, output_root, db_prefix
    raise StateModelSetError(
        "legacy fixed-seed preparation is disabled because it cannot satisfy the approved B3 D3-D7 contracts; "
        "use --b3-preparation-output"
    )


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
            frozen_input_identity=_frozen_input_identity(inputs),
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
            frozen_input_identity=_frozen_input_identity(inputs),
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


def diagnose_c008_b3_diag04(request: dict[str, Any], *, db_prefix: str) -> dict[str, Any]:
    """Run one scale-aware DIAG-04 refit pass without selection, formal acceptance, or model writes."""

    producer_commit = _git_commit()
    if str(request.get("producer_commit") or "") != producer_commit:
        raise StateModelSetError(
            f"request producer_commit differs from current code expected={request.get('producer_commit')} actual={producer_commit}"
        )
    c008_b3_diag04_fixed_numeric_environment()
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
            frozen_input_identity=_frozen_input_identity(inputs),
        )
        families.append(
            {
                "family": spec.family,
                "family_version": spec.family_version,
                "candidate_ids": list(spec.candidate_ids),
                "diagnostic": diagnose_l1_seed_grid_b3_diag04(
                    series,
                    feature_names=feature_names,
                    preprocess_family=spec.preprocess_family,
                ),
            }
        )
    return {
        "schema_version": "hmm_risk_c008_b3_diag04_single_pass_report_v1",
        "status": "diagnostic_complete",
        "diagnostic_contract": C008_B3_DIAG04_CONTRACT,
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
        "fixed_numeric_environment": c008_b3_diag04_fixed_numeric_environment(),
        "hmm_refit_performed": True,
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


def _c008_b3_diag04_child_command(args: argparse.Namespace) -> list[str]:
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
        "--_c008-b3-diag04-child",
    ]


def _run_c008_b3_diag04_repeated(args: argparse.Namespace) -> tuple[dict[str, Any], bool]:
    """Execute two fresh DIAG-04 processes and require canonical byte equality without numeric tolerance."""

    repeat_payloads: list[bytes] = []
    stderr_receipts: list[dict[str, Any]] = []
    for repeat_index in (1, 2):
        completed = subprocess.run(
            _c008_b3_diag04_child_command(args),
            cwd=ROOT,
            env=os.environ.copy(),
            check=False,
            capture_output=True,
        )
        if completed.returncode != 0:
            error = completed.stderr.decode("utf-8", errors="replace").strip()
            raise StateModelSetError(f"{C008_B3_DIAG04_CONTRACT} repeat {repeat_index} failed: {error[-4000:]}")
        try:
            json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise StateModelSetError(f"{C008_B3_DIAG04_CONTRACT} repeat {repeat_index} returned invalid JSON") from exc
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
    report["schema_version"] = "hmm_risk_c008_b3_diag04_repeated_report_v1"
    report["status"] = "diagnostic_complete" if bitwise_equal else "diagnostic_reproducibility_failed"
    report["reproducibility"] = {
        "schema_version": "hmm_risk_c008_b3_diag04_reproducibility_v1",
        "scope": "same_host_same_fixed_numeric_environment_only",
        "fresh_process_repeat_count": 2,
        "canonical_payload_sha256_by_repeat": repeat_hashes,
        "canonical_payload_bitwise_equal": bitwise_equal,
        "numeric_tolerance_used_for_acceptance": False,
        "stderr_diagnostic_receipts": stderr_receipts,
    }
    return report, bitwise_equal


def _direct_l2_constituents(inputs: dict[str, Any]) -> dict[str, dict[str, Any]]:
    l2_constituents: dict[str, dict[str, Any]] = {}
    for l1_code, constituent in sorted(inputs["constituents"].items()):
        for l2_code in constituent.get("l2_codes") or ():
            code = str(l2_code)
            if code in l2_constituents:
                raise StateModelSetError(f"canonical L2 code belongs to multiple L1 parents: {code}")
            l2_constituents[code] = {
                "schema_version": "hmm_risk_direct_l2_constituent_identity_v1",
                "l1_code": str(l1_code),
                "l2_codes": [code],
                "mapping_manifest_hash": canonical_sha256(inputs["mapping_manifest"]),
            }
    return l2_constituents


def _frozen_input_identity(inputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset_manifest_hash": canonical_sha256(inputs["dataset_manifest"]),
        "mapping_manifest_hash": canonical_sha256(inputs["mapping_manifest"]),
        "calendar_manifest_hash": canonical_sha256(inputs["dataset_manifest"]["calendar_benchmark"]),
        "l2_stock_fact_manifest_hash": canonical_sha256(inputs["l2_stock_fact_manifest"]),
    }


def _direct_series_for_family(
    inputs: dict[str, Any],
    family: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    features = tuple(str(value) for value in family.get("feature_names") or ())
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("B3 family feature_names must match the approved 7/20-dimensional order")
    train_start = _date(family.get("train_start"), "train_start")
    train_end = _date(family.get("train_end"), "train_end")
    validation_start = _date(family.get("validation_start"), "validation_start")
    validation_end = _date(family.get("validation_end"), "validation_end")
    l1 = build_l1_training_series(
        inputs["panel"],
        feature_names=features,
        train_start=train_start,
        train_end=train_end,
        validation_start=validation_start,
        validation_end=validation_end,
        constituent_manifest_by_l1=inputs["constituents"],
        expected_sector_count=31,
        direct_sector_level="L1",
        frozen_input_identity=_frozen_input_identity(inputs),
    )
    l2 = build_l1_training_series(
        inputs["l2_panel"],
        feature_names=features,
        train_start=train_start,
        train_end=train_end,
        validation_start=validation_start,
        validation_end=validation_end,
        constituent_manifest_by_l1=_direct_l2_constituents(inputs),
        expected_sector_count=131,
        direct_sector_level="L2",
        frozen_input_identity=_frozen_input_identity(inputs),
    )
    return {"L1": l1, "L2": l2}


def _direct_train_series_for_family(
    inputs: dict[str, Any],
    family: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    features = tuple(str(value) for value in family.get("feature_names") or ())
    train_start = _date(family.get("train_start"), "train_start")
    train_end = _date(family.get("train_end"), "train_end")
    return {
        "L1": build_train_only_series(
            inputs["panel"],
            feature_names=features,
            train_start=train_start,
            train_end=train_end,
            constituent_manifest=inputs["constituents"],
            expected_sector_count=31,
            direct_sector_level="L1",
            frozen_input_identity=_frozen_input_identity(inputs),
        ),
        "L2": build_train_only_series(
            inputs["l2_panel"],
            feature_names=features,
            train_start=train_start,
            train_end=train_end,
            constituent_manifest=_direct_l2_constituents(inputs),
            expected_sector_count=131,
            direct_sector_level="L2",
            frozen_input_identity=_frozen_input_identity(inputs),
        ),
    }


def prepare_b3_single_pass(
    request: dict[str, Any],
    *,
    db_prefix: str,
    process_identity: str,
) -> dict[str, Any]:
    """Run one complete train-only B3 pass; selection and D6 are parent-only."""

    producer_commit = _formal_producer_commit()
    _require_approved_b3_identities(request)
    _require_formal_train_coverage_identity(request)
    if str(request.get("producer_commit") or "") != producer_commit:
        raise StateModelSetError("B3 request producer_commit differs from current code")
    inputs = _load_l1_source_inputs(request, db_prefix=db_prefix)
    dataset_hash = canonical_sha256(inputs["dataset_manifest"])
    mapping_hash = canonical_sha256(inputs["mapping_manifest"])
    l2_stock_fact_hash = canonical_sha256(inputs["l2_stock_fact_manifest"])
    if str(request.get("dataset_manifest_hash") or "") != dataset_hash:
        raise StateModelSetError("B3 frozen dataset manifest hash mismatch")
    if str(request.get("mapping_manifest_hash") or "") != mapping_hash:
        raise StateModelSetError("B3 frozen mapping manifest hash mismatch")
    if str(request.get("l2_stock_fact_manifest_hash") or "") != l2_stock_fact_hash:
        raise StateModelSetError("B3 frozen L2 stock-fact manifest hash mismatch")
    train_coverage = _b3_train_coverage_preflight(inputs, request)
    if train_coverage["train_coverage_valid"] is not True:
        raise StateModelSetError("B3 formal train coverage is insufficient")
    if request["train_coverage_receipt_sha256"] != train_coverage["receipt_sha256"]:
        raise StateModelSetError("B3 formal train coverage receipt hash mismatch")
    calendar_manifest = inputs["dataset_manifest"].get("calendar_benchmark")
    if not isinstance(calendar_manifest, dict):
        raise StateModelSetError("B3 frozen calendar manifest is missing")
    calendar_hash = canonical_sha256(calendar_manifest)
    families = list(request.get("families") or ())
    family_names = {str(family.get("family") or "") for family in families}
    if family_names != {"legacy_covfix", "autocycle_all_core"} or len(families) != 2:
        raise StateModelSetError("B3 requires exactly legacy_covfix and autocycle_all_core")
    level_repeats: dict[str, Any] = {}
    for family in sorted(families, key=lambda item: str(item.get("family") or "")):
        family_name = str(family["family"])
        features = tuple(str(value) for value in family.get("feature_names") or ())
        series_by_level = _direct_train_series_for_family(inputs, family)
        for level in ("L1", "L2"):
            repeat, _ = run_level_repeat(
                series_by_level[level],
                family=family_name,
                level=level,
                feature_names=features,
                preprocess_family=str(family.get("preprocess_family") or ""),
                process_identity=process_identity,
            )
            level_repeats[f"{family_name}:{level}"] = repeat
    body = {
        "schema_version": "hmm_risk_b3_single_pass_receipt_v1",
        "producer_commit": producer_commit,
        "process_identity": process_identity,
        "dataset_manifest_hash": dataset_hash,
        "mapping_manifest_hash": mapping_hash,
        "calendar_manifest_hash": calendar_hash,
        "l2_stock_fact_manifest_hash": l2_stock_fact_hash,
        "level_repeats": level_repeats,
        "selection_performed": False,
        "validation_accessed_for_selection": False,
        "future_utility_accessed_for_selection": False,
        "artifact_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "single_pass_receipt_sha256": canonical_sha256(body)}


def _b3_child_command(args: argparse.Namespace, process_identity: str) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--request",
        str(Path(args.request).resolve()),
        "--output-root",
        str(Path(args.output_root).resolve()),
        "--env-file",
        str(Path(args.env_file).resolve()),
        "--db-env-prefix",
        str(args.db_env_prefix),
        "--_b3-child",
        "--b3-process-identity",
        process_identity,
    ]


def _persist_b3_child_failure(
    args: argparse.Namespace,
    *,
    process_identity: str,
    returncode: int,
    stdout: bytes,
    stderr: bytes,
) -> tuple[Path, dict[str, Any]]:
    decoded = stderr.decode("utf-8", errors="replace").strip()
    parsed: dict[str, Any] = {}
    if decoded:
        try:
            value = json.loads(decoded.splitlines()[-1])
            if isinstance(value, dict):
                parsed = value
        except json.JSONDecodeError:
            parsed = {}
    error_type = str(parsed.get("error_type") or "unparsed_child_error")[:256]
    error = str(parsed.get("error") or decoded[-4000:] or "child failed without stderr")[-4000:]
    body = {
        "schema_version": "hmm_risk_b3_child_failure_receipt_v1",
        "status": "failed",
        "process_identity": process_identity,
        "returncode": returncode,
        "error_type": error_type,
        "error": error,
        "child_error_schema_version": parsed.get("schema_version"),
        "stdout_byte_count": len(stdout),
        "stdout_sha256": sha256_bytes(stdout),
        "stderr_byte_count": len(stderr),
        "stderr_sha256": sha256_bytes(stderr),
        "fit_grid_completed": False,
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    report = {**body, "receipt_sha256": canonical_sha256(body)}
    preparation_path = Path(args.b3_preparation_output).resolve()
    failure_path = preparation_path.with_name(f"{preparation_path.stem}.{process_identity}.failure.json")
    _write_diagnostic_report(failure_path, report)
    return failure_path, report


def run_b3_repeated(args: argparse.Namespace, request: dict[str, Any]) -> dict[str, Any]:
    """Run two fresh processes, select train-only identities, then execute D6 once."""

    environment = os.environ.copy()
    for key in (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        environment[key] = "1"
    repeats: list[dict[str, Any]] = []
    for process_identity in ("fresh_process_1", "fresh_process_2"):
        completed = subprocess.run(
            _b3_child_command(args, process_identity),
            check=False,
            capture_output=True,
            env=environment,
            timeout=7200,
        )
        if completed.returncode != 0:
            failure_path, failure = _persist_b3_child_failure(
                args,
                process_identity=process_identity,
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )
            raise StateModelSetError(
                f"formal B3 child failed process={process_identity} returncode={completed.returncode} "
                f"error_type={failure['error_type']} error={failure['error']} "
                f"failure_receipt={failure_path}"
            )
        try:
            repeats.append(json.loads(completed.stdout))
        except json.JSONDecodeError as exc:
            raise StateModelSetError(f"formal B3 child returned invalid JSON: {process_identity}") from exc
    if repeats[0]["dataset_manifest_hash"] != repeats[1]["dataset_manifest_hash"]:
        raise StateModelSetError("formal B3 fresh processes used different dataset manifests")
    if repeats[0]["mapping_manifest_hash"] != repeats[1]["mapping_manifest_hash"]:
        raise StateModelSetError("formal B3 fresh processes used different mapping manifests")
    if repeats[0]["calendar_manifest_hash"] != repeats[1]["calendar_manifest_hash"]:
        raise StateModelSetError("formal B3 fresh processes used different calendar manifests")
    if repeats[0]["l2_stock_fact_manifest_hash"] != repeats[1]["l2_stock_fact_manifest_hash"]:
        raise StateModelSetError("formal B3 fresh processes used different L2 stock-fact manifests")
    inputs = _load_l1_source_inputs(request, db_prefix=str(args.db_env_prefix))
    if canonical_sha256(inputs["dataset_manifest"]) != repeats[0]["dataset_manifest_hash"]:
        raise StateModelSetError("formal B3 D6 reload drifted from the frozen dataset manifest")
    if canonical_sha256(inputs["mapping_manifest"]) != repeats[0]["mapping_manifest_hash"]:
        raise StateModelSetError("formal B3 D6 reload drifted from the frozen mapping manifest")
    if canonical_sha256(inputs["dataset_manifest"]["calendar_benchmark"]) != repeats[0]["calendar_manifest_hash"]:
        raise StateModelSetError("formal B3 D6 reload drifted from the frozen calendar manifest")
    if canonical_sha256(inputs["l2_stock_fact_manifest"]) != repeats[0]["l2_stock_fact_manifest_hash"]:
        raise StateModelSetError("formal B3 D6 reload drifted from the frozen L2 stock-fact manifest")
    selections: dict[tuple[str, str], dict[str, Any]] = {}
    selected_artifacts: dict[tuple[str, str], dict[str, Any]] = {}
    family_map = {str(item["family"]): item for item in request["families"]}
    for family in ("legacy_covfix", "autocycle_all_core"):
        series_by_level = _direct_series_for_family(inputs, family_map[family])
        feature_count = len(tuple(family_map[family]["feature_names"]))
        for level in ("L1", "L2"):
            key = f"{family}:{level}"
            first = repeats[0]["level_repeats"][key]
            second = repeats[1]["level_repeats"][key]
            selection = select_level_restart(
                first,
                second,
                family=family,
                level=level,
                expected_sector_codes=tuple(sorted(series_by_level[level])),
                feature_count=feature_count,
            )
            selections[(family, level)] = selection
            if selection["level_selection_valid"]:
                selected_artifacts[(family, level)] = build_selected_level_artifact(
                    selection,
                    models_from_repeat(first),
                    series_by_level[level],
                    first,
                )
    all_ready = len(selected_artifacts) == 4 and all(
        artifact.get("status") == "accepted" for artifact in selected_artifacts.values()
    )
    family_model_set_statuses = {
        family: (
            "accepted"
            if all(selected_artifacts.get((family, level), {}).get("status") == "accepted" for level in ("L1", "L2"))
            else "blocked"
        )
        for family in ("legacy_covfix", "autocycle_all_core")
    }
    manifest_path = None
    if all_ready:
        manifest_path = write_b3_ready_model_set(
            Path(args.output_root).resolve(),
            selected_artifacts=selected_artifacts,
            selection_receipts=selections,
            dataset_manifest_hash=repeats[0]["dataset_manifest_hash"],
            mapping_manifest_hash=repeats[0]["mapping_manifest_hash"],
            calendar_manifest_hash=repeats[0]["calendar_manifest_hash"],
            l2_stock_fact_manifest_hash=repeats[0]["l2_stock_fact_manifest_hash"],
            producer_commit=_git_commit(),
        )
    body = {
        "schema_version": "hmm_risk_b3_repeated_preparation_receipt_v1",
        "status": "READY" if all_ready else "blocked",
        "producer_commit": _git_commit(),
        "dataset_manifest_hash": repeats[0]["dataset_manifest_hash"],
        "mapping_manifest_hash": repeats[0]["mapping_manifest_hash"],
        "calendar_manifest_hash": repeats[0]["calendar_manifest_hash"],
        "l2_stock_fact_manifest_hash": repeats[0]["l2_stock_fact_manifest_hash"],
        "fresh_process_receipt_hashes": [repeat["single_pass_receipt_sha256"] for repeat in repeats],
        "selections": {f"{family}:{level}": selections[(family, level)] for family, level in sorted(selections)},
        "selected_artifacts": {
            f"{family}:{level}": selected_artifacts[(family, level)] for family, level in sorted(selected_artifacts)
        },
        "family_model_set_statuses": family_model_set_statuses,
        "selection_performed": True,
        "selection_used_validation": False,
        "selection_used_future_utility": False,
        "selection_followed_by_refit": False,
        "ready_manifest_path": None if manifest_path is None else str(manifest_path),
        "ready_artifact_write_performed": manifest_path is not None,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


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
    parser.add_argument("--artifact-root", help="Deprecated legacy L2 artifact root; formal B3 never reads it.")
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
    diagnostic_group.add_argument(
        "--c008-b3-diag04-output",
        help="Run approved C-008-B3 DIAG-04 scale-aware refits twice without selection/formal acceptance/model writes.",
    )
    diagnostic_group.add_argument(
        "--b3-preflight-output",
        help="Freeze current B3 PIT identities without fitting, selection, or model/READY writes.",
    )
    diagnostic_group.add_argument(
        "--c009-stock-fact-preflight-output",
        help="Run the approved 601-day C-009 stock-fact source preflight without HMM fits or writes.",
    )
    diagnostic_group.add_argument(
        "--b3-preparation-output",
        help="Run formal two-process B3 L1/L2 preparation and write its immutable receipt.",
    )
    parser.add_argument(
        "--b3-request-candidate-output",
        help="Immutable request candidate output; required only with --b3-preflight-output.",
    )
    parser.add_argument("--_c008-b3-diag02-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--_c008-b3-diag04-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--_b3-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--b3-process-identity", default="", help=argparse.SUPPRESS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        _read_env_file(Path(args.env_file).resolve())
        request_path = Path(args.request).resolve()
        c009_preflight_output = getattr(args, "c009_stock_fact_preflight_output", None)
        if args.b3_preflight_output or c009_preflight_output:
            if not args.b3_request_candidate_output:
                if args.b3_preflight_output:
                    raise StateModelSetError("--b3-request-candidate-output is required with --b3-preflight-output")
            request = _load_request_template(request_path)
        else:
            if args.b3_request_candidate_output:
                raise StateModelSetError("--b3-request-candidate-output is only valid with --b3-preflight-output")
            request = _load_request(request_path)
        if c009_preflight_output:
            if args.b3_request_candidate_output:
                raise StateModelSetError(
                    "--b3-request-candidate-output is not valid with --c009-stock-fact-preflight-output"
                )
            report = prepare_c009_stock_fact_preflight(request, db_prefix=str(args.db_env_prefix))
            report_path = Path(c009_preflight_output).resolve()
            report_sha256 = _write_diagnostic_report(report_path, report)
            receipt = {
                "schema_version": "hmm_risk_c009_stock_fact_preflight_cli_receipt_v1",
                "status": report["status"],
                "report_path": str(report_path),
                "report_sha256": report_sha256,
                "dataset_manifest_hash": report["dataset_manifest_hash"],
                "mapping_manifest_hash": report["mapping_manifest_hash"],
                "l2_stock_fact_manifest_hash": report["l2_stock_fact_manifest_hash"],
                "security_identity_manifest_sha256": report["security_source_identity"]["manifest_sha256"],
                "provider_absence_manifest_sha256": report["provider_absence_authority"]["manifest_sha256"],
                "train_coverage_valid": report["train_coverage_valid"],
                "failure_reason_codes": report["failure_reason_codes"],
                "fit_performed": False,
                "selection_performed": False,
                "d6_performed": False,
                "model_write_performed": False,
                "ready_artifact_write_performed": False,
                "database_write_performed": False,
                "runtime_action_performed": False,
            }
            print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
            return 0 if report["status"] == "preflight_complete" else 1
        if args.b3_preflight_output:
            report = prepare_b3_preflight_candidate(request, db_prefix=str(args.db_env_prefix))
            request_candidate_path = None
            request_candidate_sha256 = None
            if report["status"] == "candidate_ready":
                request_candidate_path = Path(args.b3_request_candidate_output).resolve()
                request_candidate_sha256 = _write_diagnostic_report(
                    request_candidate_path,
                    report["request_candidate"],
                )
                if request_candidate_sha256 != report["request_candidate_sha256"]:
                    raise StateModelSetError("B3 preflight request candidate hash mismatch")
            elif report["status"] != "blocked":
                raise StateModelSetError("B3 preflight returned an invalid status")
            report_path = Path(args.b3_preflight_output).resolve()
            report_sha256 = _write_diagnostic_report(report_path, report)
            receipt = {
                "schema_version": "hmm_risk_b3_formal_preflight_cli_receipt_v1",
                "status": report["status"],
                "report_path": str(report_path),
                "report_sha256": report_sha256,
                "request_candidate_path": None if request_candidate_path is None else str(request_candidate_path),
                "request_candidate_sha256": request_candidate_sha256,
                "dataset_manifest_hash": report["dataset_manifest_hash"],
                "mapping_manifest_hash": report["mapping_manifest_hash"],
                "l2_stock_fact_manifest_hash": report["l2_stock_fact_manifest_hash"],
                "train_coverage_valid": report["train_coverage_valid"],
                "failure_reason_codes": report["failure_reason_codes"],
                "fit_performed": False,
                "selection_performed": False,
                "formal_acceptance_thresholds_applied": False,
                "model_write_performed": False,
                "ready_artifact_write_performed": False,
                "database_write_performed": False,
                "runtime_action_performed": False,
            }
            print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
            return 0 if report["status"] == "candidate_ready" else 1
        if args._c008_b3_diag02_child:
            report = diagnose_c008_b3_diag02(request, db_prefix=str(args.db_env_prefix))
            sys.stdout.buffer.write(canonical_json_bytes(report))
            return 0
        if args._c008_b3_diag04_child:
            report = diagnose_c008_b3_diag04(request, db_prefix=str(args.db_env_prefix))
            sys.stdout.buffer.write(canonical_json_bytes(report))
            return 0
        if args._b3_child:
            if args.b3_process_identity not in {"fresh_process_1", "fresh_process_2"}:
                raise StateModelSetError("formal B3 child process identity is invalid")
            report = prepare_b3_single_pass(
                request,
                db_prefix=str(args.db_env_prefix),
                process_identity=str(args.b3_process_identity),
            )
            sys.stdout.buffer.write(canonical_json_bytes(report))
            return 0
        if args.b3_preparation_output:
            report = run_b3_repeated(args, request)
            report_path = Path(args.b3_preparation_output).resolve()
            report_sha256 = _write_diagnostic_report(report_path, report)
            receipt = {
                "schema_version": "hmm_risk_b3_preparation_cli_receipt_v1",
                "status": report["status"],
                "report_path": str(report_path),
                "report_sha256": report_sha256,
                "selection_performed": report["selection_performed"],
                "selection_used_validation": False,
                "selection_used_future_utility": False,
                "ready_artifact_write_performed": report["ready_artifact_write_performed"],
                "ready_manifest_path": report["ready_manifest_path"],
                "database_write_performed": False,
                "runtime_action_performed": False,
            }
            print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
            return 0 if report["status"] == "READY" else 1
        if args.c008_b3_diag04_output:
            report, reproducible = _run_c008_b3_diag04_repeated(args)
            report_path = Path(args.c008_b3_diag04_output).resolve()
            report_sha256 = _write_diagnostic_report(report_path, report)
            receipt = {
                "schema_version": "hmm_risk_c008_b3_diag04_receipt_v1",
                "status": report["status"],
                "diagnostic_contract": C008_B3_DIAG04_CONTRACT,
                "structural_contract": C008_B3_STRUCTURAL_CONTRACT,
                "report_path": str(report_path),
                "report_sha256": report_sha256,
                "fresh_process_repeat_count": 2,
                "canonical_payload_bitwise_equal": reproducible,
                "hmm_refit_performed": True,
                "selection_performed": False,
                "formal_acceptance_thresholds_applied": False,
                "hard_semantic_authority_changed": False,
                "model_write_performed": False,
                "ready_artifact_write_performed": False,
                "family_count": len(report["families"]),
            }
            print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
            return 0 if reproducible else 1
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
            raise StateModelSetError(
                "an explicit diagnostic/preflight mode or --b3-preparation-output is required; "
                "legacy preparation is disabled"
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
