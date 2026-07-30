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
from backend.services.hmm_risk.b3_blocker_diagnostic import (  # noqa: E402
    DIAGNOSTIC_VERSION as B3_BLOCKER_DIAGNOSTIC_VERSION,
    FORMAL_AUTHORITY as B3_BLOCKER_FORMAL_AUTHORITY,
    build_matched_comparisons as build_b3_blocker_matched_comparisons,
    derive_target_manifest as derive_b3_blocker_target_manifest,
    replay_selected_d6 as replay_b3_blocker_selected_d6,
    run_targeted_level as run_b3_blocker_targeted_level,
)
from backend.services.hmm_risk.b3_training import (  # noqa: E402
    audit_train_only_coverage,
    build_train_only_series,
    build_selected_level_artifact,
    formal_b3_parameter_profile,
    models_from_repeat,
    run_level_repeat,
    write_b3_ready_model_set,
)
from backend.services.hmm_risk.stock_fact_observation import (  # noqa: E402
    C010_FORMULA_VERSION,
    C010_POLICY_VERSION,
    MIN_COVERAGE,
    OBSERVATION_VERSION,
    build_c010_feature_domain_panel,
    build_l1_feature_panel,
    build_l1_training_series,
    complete_c010_domain_receipts,
    validate_c010_policy_manifest,
)
from backend.services.hmm_risk.observation_eligibility import (  # noqa: E402
    audit_feature_mask_candidates,
    build_train_only_observation_eligibility,
    load_feature_domain_direct_aggregates,
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
C010_FORMAL_PREFLIGHT_SCHEMA = "hmm_risk_c010_formal_preflight_v1"
C009_STOCK_FACT_PREFLIGHT_SCHEMA = "hmm_risk_c009_stock_fact_preflight_v1"
C010_OBSERVATION_ELIGIBILITY_SCHEMA = "hmm_risk_c010_observation_eligibility_diagnostic_v1"
B3_TRAIN_COVERAGE_PREFLIGHT_VERSION = "hmm_risk_b3_train_coverage_preflight_set_v1"
B3_APPROVED_FROZEN_IDENTITIES = {
    "dataset_manifest_hash": "c07177ddd01b324106755e47ee2cfe61a7f2916e08ccf9e888d3abf1115ebd7f",
    "mapping_manifest_hash": "9cdddd98db3cacd9949ac5b7ba007c16eb66de46375e848eea676b0168b58159",
    "l2_stock_fact_manifest_hash": "d4a5cc86f3230a7bbd5704b81e63fa16cf4dc5a074f461f28112d3c9582d1730",
}
B3_APPROVED_WINDOWS = {
    "train_start": "2022-01-01",
    "train_end": "2024-06-30",
    "validation_start": "2024-07-01",
    "validation_end": "2025-03-31",
    "common_data_watermark": "2025-04-30",
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


def _reject_duplicate_json_keys(items: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in items:
        if key in value:
            raise StateModelSetError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _load_json_mapping(path: Path, *, label: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_keys)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StateModelSetError(f"{label} cannot be read: {exc}") from exc
    if not isinstance(value, dict):
        raise StateModelSetError(f"{label} must contain one JSON object")
    return value


def _require_approved_b3_identities(value: dict[str, Any]) -> None:
    mismatches = []
    for field, expected in B3_APPROVED_FROZEN_IDENTITIES.items():
        actual = str(value.get(field) or "")
        if actual != expected:
            mismatches.append(f"{field} expected={expected} actual={actual or '<missing>'}")
    if mismatches:
        raise StateModelSetError("formal B3 frozen identity mismatch: " + "; ".join(mismatches))


def _freeze_approved_b3_windows(request_template: dict[str, Any]) -> dict[str, Any]:
    request = deepcopy(request_template)
    families = list(request.get("families") or ())
    family_names = {str(family.get("family") or "") for family in families}
    if family_names != {"legacy_covfix", "autocycle_all_core"} or len(families) != 2:
        raise StateModelSetError("formal B3 requires exactly the two approved families")
    for family in families:
        family_name = str(family["family"])
        for field in ("train_start", "train_end"):
            actual = _date(family.get(field), field).isoformat()
            expected = B3_APPROVED_WINDOWS[field]
            if actual != expected:
                raise StateModelSetError(
                    f"formal B3 train window mismatch: family={family_name} field={field} "
                    f"expected={expected} actual={actual}"
                )
        for field in ("validation_start", "validation_end"):
            raw = family.get(field)
            expected = B3_APPROVED_WINDOWS[field]
            if raw in (None, ""):
                family[field] = expected
                continue
            actual = _date(raw, field).isoformat()
            if actual != expected:
                raise StateModelSetError(
                    f"formal B3 validation window mismatch: family={family_name} field={field} "
                    f"expected={expected} actual={actual}"
                )
            family[field] = actual
    return request


def _require_approved_b3_windows(request: dict[str, Any]) -> None:
    families = list(request.get("families") or ())
    family_names = {str(family.get("family") or "") for family in families}
    if family_names != {"legacy_covfix", "autocycle_all_core"} or len(families) != 2:
        raise StateModelSetError("formal B3 requires exactly the two approved families")
    for family in families:
        family_name = str(family["family"])
        for field in ("train_start", "train_end", "validation_start", "validation_end"):
            raw = family.get(field)
            if raw in (None, ""):
                window = "validation" if field.startswith("validation_") else "train"
                raise StateModelSetError(f"formal B3 {window} window is missing: family={family_name} field={field}")
            actual = _date(raw, field).isoformat()
            expected = B3_APPROVED_WINDOWS[field]
            if actual != expected:
                window = "validation" if field.startswith("validation_") else "train"
                raise StateModelSetError(
                    f"formal B3 {window} window mismatch: family={family_name} field={field} "
                    f"expected={expected} actual={actual}"
                )


def _require_b3_semantic_source_containment(request: dict[str, Any]) -> None:
    _require_approved_b3_windows(request)
    source = request["source"]
    source_start = _date(source.get("source_start"), "source_start")
    source_end = _date(source.get("source_end"), "source_end")
    semantic_start = _date(B3_APPROVED_WINDOWS["train_start"], "train_start")
    semantic_end = _date(B3_APPROVED_WINDOWS["common_data_watermark"], "common_data_watermark")
    if source_start > semantic_start or source_end < semantic_end:
        raise StateModelSetError("formal B3 semantic window escapes the immutable source window")


def _b3_semantic_source_request(request: dict[str, Any]) -> dict[str, Any]:
    _require_approved_b3_windows(request)
    semantic_request = deepcopy(request)
    source = semantic_request["source"]
    circ_mv_history_start = str(source.get("circ_mv_history_start") or source.get("source_start") or "")
    source["source_start"] = B3_APPROVED_WINDOWS["train_start"]
    source["source_end"] = B3_APPROVED_WINDOWS["common_data_watermark"]
    source["circ_mv_history_start"] = circ_mv_history_start
    return semantic_request


def _require_formal_semantic_identity(request: dict[str, Any]) -> None:
    source = request.get("semantic_source")
    if not isinstance(source, dict):
        raise StateModelSetError("formal B3 semantic source identity is missing")
    expected_source = _b3_semantic_source_request(request)["source"]
    if source != expected_source:
        raise StateModelSetError("formal B3 semantic source identity mismatch")
    for field in (
        "semantic_dataset_manifest_hash",
        "semantic_mapping_manifest_hash",
        "semantic_calendar_manifest_hash",
        "semantic_l2_stock_fact_manifest_hash",
    ):
        identity = str(request.get(field) or "")
        if len(identity) != 64 or any(character not in "0123456789abcdef" for character in identity.lower()):
            raise StateModelSetError(f"formal B3 {field} is missing or invalid")


def _semantic_input_identities(inputs: dict[str, Any]) -> dict[str, str]:
    return {
        "semantic_dataset_manifest_hash": canonical_sha256(inputs["dataset_manifest"]),
        "semantic_mapping_manifest_hash": canonical_sha256(inputs["mapping_manifest"]),
        "semantic_calendar_manifest_hash": canonical_sha256(inputs["dataset_manifest"]["calendar_benchmark"]),
        "semantic_l2_stock_fact_manifest_hash": canonical_sha256(inputs["l2_stock_fact_manifest"]),
    }


def _load_verified_formal_semantic_inputs(request: dict[str, Any], *, db_prefix: str) -> dict[str, Any]:
    _require_formal_semantic_identity(request)
    semantic_request = deepcopy(request)
    semantic_request["source"] = deepcopy(request["semantic_source"])
    inputs = _load_l1_source_inputs(semantic_request, db_prefix=db_prefix, c010_formal=True)
    for field, actual in _semantic_input_identities(inputs).items():
        if request[field] != actual:
            raise StateModelSetError(f"formal B3 semantic input drifted from {field}")
    return inputs


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


def _request_train_window(request: dict[str, Any]) -> tuple[date, date]:
    windows = {
        (_date(family.get("train_start"), "train_start"), _date(family.get("train_end"), "train_end"))
        for family in request["families"]
    }
    if len(windows) != 1:
        raise StateModelSetError("C-010 requires one immutable train window across both families")
    train_start, train_end = next(iter(windows))
    if train_start > train_end:
        raise StateModelSetError("C-010 train window is invalid")
    return train_start, train_end


def _c010_expected_opportunity_dates(
    conn: Any,
    source_spec: StockFactSourceSpec,
    *,
    train_start: date,
    train_end: date,
) -> dict[str, tuple[date, ...]]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT price.ts_code,array_agg(DISTINCT price.trade_date ORDER BY price.trade_date)
            FROM market.kline_daily_raw price
            JOIN market.stock_universe_pit_spans spans
              ON spans.ts_code=price.ts_code AND spans.universe_key=%s
             AND spans.eligible_start<=price.trade_date
             AND (spans.eligible_end IS NULL OR spans.eligible_end>=price.trade_date)
            JOIN market.sw_index_member member
              ON member.ts_code=price.ts_code AND member.in_date<=price.trade_date
             AND (member.out_date IS NULL OR member.out_date>=price.trade_date)
            WHERE price.trade_date BETWEEN %s AND %s
            GROUP BY price.ts_code
            ORDER BY price.ts_code
            """,
            (source_spec.universe_key, train_start, train_end),
        )
        rows = cursor.fetchall()
    result = {str(symbol): tuple(dates) for symbol, dates in rows if dates}
    if not result:
        raise StateModelSetError(
            "hmm_risk_c010_expected_opportunity_missing: "
            "C-010 full-universe expected opportunity query returned no rows"
        )
    return result


def _load_l1_source_inputs(
    request: dict[str, Any],
    *,
    db_prefix: str,
    c010_diagnostic: bool = False,
    c010_formal: bool = False,
) -> dict[str, Any]:
    if c010_diagnostic and c010_formal:
        raise StateModelSetError("C-010 diagnostic and formal policy modes are mutually exclusive")
    source = request["source"]
    security_identity_manifest = _load_security_identity_manifest(source)
    provider_absence_manifest = _load_provider_absence_manifest(source)
    source_start = _date(source.get("source_start"), "source_start")
    source_spec = StockFactSourceSpec(
        universe_key=str(source.get("universe_key") or ""),
        universe_rule_version=str(source.get("universe_rule_version") or ""),
        source_start=source_start,
        source_end=_date(source.get("source_end"), "source_end"),
        circ_mv_history_start=_date(
            source.get("circ_mv_history_start") or source_start.isoformat(),
            "circ_mv_history_start",
        ),
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
        c010_payload = None
        if c010_diagnostic or c010_formal:
            train_start, train_end = _request_train_window(request)
            expected_dates = _c010_expected_opportunity_dates(
                conn,
                source_spec,
                train_start=train_start,
                train_end=train_end,
            )
            eligibility = build_train_only_observation_eligibility(
                provider_absence_manifest.rows,
                expected_opportunity_dates_by_symbol=expected_dates,
                train_start=train_start,
                train_end=train_end,
                minimum_availability_ratio=MIN_COVERAGE,
            )
            diagnostic_l1, diagnostic_l2, aggregate_evidence = load_feature_domain_direct_aggregates(
                reader,
                eligibility,
                min_coverage=MIN_COVERAGE,
                formal_policy=c010_formal,
            )
            c010_payload = {
                "eligibility": eligibility,
                "aggregate_evidence": aggregate_evidence,
                "l1_aggregates": diagnostic_l1,
                "l2_aggregates": diagnostic_l2,
            }
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
    c010_diagnostic_payload = None
    if c010_payload is not None:
        diagnostic_l1_panel, diagnostic_l1_definition, diagnostic_l1_cross_section = build_c010_feature_domain_panel(
            c010_payload["l1_aggregates"],
            trading_dates=calendar,
            csi300_returns=benchmark,
            diagnostic_only=c010_diagnostic,
        )
        diagnostic_l2_panel, diagnostic_l2_definition, diagnostic_l2_cross_section = build_c010_feature_domain_panel(
            c010_payload["l2_aggregates"],
            trading_dates=calendar,
            csi300_returns=benchmark,
            expected_sector_count=131,
            direct_sector_level="L2",
            diagnostic_only=c010_diagnostic,
        )
        complete_aggregate_evidence = complete_c010_domain_receipts(
            c010_payload["aggregate_evidence"],
            trading_dates=calendar,
            l1_sector_codes=diagnostic_l1_cross_section["expected_sector_codes"],
            l2_sector_codes=diagnostic_l2_cross_section["expected_sector_codes"],
        )
        c010_diagnostic_payload = {
            "eligibility": c010_payload["eligibility"].evidence(formal_policy=c010_formal),
            "aggregate_evidence": complete_aggregate_evidence,
            "l1_panel": diagnostic_l1_panel,
            "l2_panel": diagnostic_l2_panel,
            "l1_feature_definition": diagnostic_l1_definition,
            "l2_feature_definition": diagnostic_l2_definition,
            "l1_cross_section_evidence": diagnostic_l1_cross_section,
            "l2_cross_section_evidence": diagnostic_l2_cross_section,
        }
    dataset_manifest = {
        "schema_version": "hmm_risk_state_model_set_dataset_manifest_v1",
        "source_state": source_state,
        "stock_facts": stock_fact_manifest,
        "calendar_benchmark": benchmark_manifest,
        "security_source_identity": security_identity_manifest.evidence(),
        "provider_absence_authority": provider_absence_manifest.evidence(),
    }
    if c010_formal and c010_diagnostic_payload is not None:
        dataset_manifest["c010_feature_domain_inputs"] = {
            "schema_version": "hmm_risk_c010_feature_domain_input_manifest_v1",
            "eligibility_receipt_sha256": c010_diagnostic_payload["eligibility"]["receipt_sha256"],
            "aggregate_receipt_sha256": c010_diagnostic_payload["aggregate_evidence"]["receipt_sha256"],
            "l1_cross_section_receipt_sha256": c010_diagnostic_payload["l1_cross_section_evidence"]["receipt_sha256"],
            "l2_cross_section_receipt_sha256": c010_diagnostic_payload["l2_cross_section_evidence"]["receipt_sha256"],
            "l1_feature_definition_sha256": canonical_sha256(c010_diagnostic_payload["l1_feature_definition"]),
            "l2_feature_definition_sha256": canonical_sha256(c010_diagnostic_payload["l2_feature_definition"]),
        }
        panel = c010_diagnostic_payload["l1_panel"]
        l2_panel = c010_diagnostic_payload["l2_panel"]
        feature_definition = c010_diagnostic_payload["l1_feature_definition"]
        l2_feature_definition = c010_diagnostic_payload["l2_feature_definition"]
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
        "c010_diagnostic": c010_diagnostic_payload,
        "trading_dates": tuple(calendar),
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
        "feature_domain_policy_sha256": inputs.get("feature_domain_policy_sha256"),
        "formula_version": C010_FORMULA_VERSION if inputs.get("feature_domain_policy_sha256") else None,
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


def _require_canonical_receipt(value: dict[str, Any], *, label: str) -> None:
    identity = str(value.get("receipt_sha256") or "")
    body = {key: item for key, item in value.items() if key != "receipt_sha256"}
    if len(identity) != 64 or identity != canonical_sha256(body):
        raise StateModelSetError(f"{label} receipt identity is invalid")


def _require_entry_receipts(values: Any, *, label: str) -> None:
    if not isinstance(values, list):
        raise StateModelSetError(f"{label} entries are missing")
    for index, value in enumerate(values):
        if not isinstance(value, dict):
            raise StateModelSetError(f"{label} entry {index} is invalid")
        identity = str(value.get("entry_sha256") or "")
        body = {key: item for key, item in value.items() if key != "entry_sha256"}
        if len(identity) != 64 or identity != canonical_sha256(body):
            raise StateModelSetError(f"{label} entry {index} identity is invalid")


def _c010_policy_manifest(
    inputs: dict[str, Any],
    request: dict[str, Any],
    *,
    producer_commit: str,
) -> dict[str, Any]:
    c010 = inputs.get("c010_diagnostic")
    if not isinstance(c010, dict):
        raise StateModelSetError("C-010 formal feature-domain payload is missing")
    train_start, train_end = _request_train_window(request)
    eligibility = c010.get("eligibility")
    aggregate = c010.get("aggregate_evidence")
    l1_cross = c010.get("l1_cross_section_evidence")
    l2_cross = c010.get("l2_cross_section_evidence")
    if not all(isinstance(value, dict) for value in (eligibility, aggregate, l1_cross, l2_cross)):
        raise StateModelSetError("C-010 formal feature-domain receipts are incomplete")
    for label, value in (
        ("C-010 eligibility", eligibility),
        ("C-010 aggregate", aggregate),
        ("C-010 L1 cross-section", l1_cross),
        ("C-010 L2 cross-section", l2_cross),
    ):
        _require_canonical_receipt(value, label=label)
    _require_entry_receipts(eligibility.get("entries"), label="C-010 eligibility")
    for field in ("l1_domain_receipts", "l2_domain_receipts", "l1_invalid_price_domain", "l2_invalid_price_domain"):
        _require_entry_receipts(aggregate.get(field), label=f"C-010 aggregate {field}")
    _require_entry_receipts(l1_cross.get("entries"), label="C-010 L1 cross-section")
    _require_entry_receipts(l2_cross.get("entries"), label="C-010 L2 cross-section")
    if (
        eligibility.get("entry_count") != len(eligibility["entries"])
        or eligibility.get("entry_count", 0) <= 0
        or aggregate.get("l1_aggregate_count") != len(aggregate["l1_domain_receipts"])
        or aggregate.get("l2_aggregate_count") != len(aggregate["l2_domain_receipts"])
        or aggregate.get("l1_aggregate_count", 0) <= 0
        or aggregate.get("l2_aggregate_count", 0) <= 0
        or l1_cross.get("entry_count") != len(l1_cross["entries"])
        or l2_cross.get("entry_count") != len(l2_cross["entries"])
        or l1_cross.get("entry_count") != 4 * len(inputs.get("trading_dates") or ())
        or l2_cross.get("entry_count") != 4 * len(inputs.get("trading_dates") or ())
    ):
        raise StateModelSetError("C-010 formal receipt set cardinality is invalid")
    if eligibility.get("formal_policy_activated") is not True or eligibility.get("diagnostic_only") is not False:
        raise StateModelSetError("C-010 formal eligibility receipt is not active")
    feature_definitions = {
        "L1": c010.get("l1_feature_definition"),
        "L2": c010.get("l2_feature_definition"),
    }
    if any(
        not isinstance(value, dict)
        or value.get("schema_version") != C010_FORMULA_VERSION
        or value.get("feature_domain_policy_version") != C010_POLICY_VERSION
        or value.get("diagnostic_only") is not False
        for value in feature_definitions.values()
    ):
        raise StateModelSetError(
            "hmm_risk_c010_feature_identity_drift: C-010 formal feature definition identity is invalid"
        )
    if (
        aggregate.get("formal_policy_activated") is not True
        or l1_cross.get("diagnostic_only") is not False
        or l2_cross.get("diagnostic_only") is not False
        or l1_cross.get("direct_sector_level") != "L1"
        or l1_cross.get("expected_sector_count") != 31
        or l2_cross.get("direct_sector_level") != "L2"
        or l2_cross.get("expected_sector_count") != 131
    ):
        raise StateModelSetError("C-010 formal aggregate/cross-section receipts are not active or complete")
    stock_fact = inputs["dataset_manifest"]["stock_facts"]
    l2_stock_fact = inputs["l2_stock_fact_manifest"]
    circ_mv_identity = {
        "L1": {
            field: stock_fact.get(field)
            for field in (
                "circ_mv_lookback_contract_version",
                "circ_mv_history_start",
                "circ_mv_pit_boundary_crossing_key_sha256",
            )
        },
        "L2": {
            field: l2_stock_fact.get(field)
            for field in (
                "circ_mv_lookback_contract_version",
                "circ_mv_history_start",
                "circ_mv_pit_boundary_crossing_key_sha256",
            )
        },
    }
    if any(not value for level in circ_mv_identity.values() for value in level.values()):
        raise StateModelSetError("C-010 causal circ-mv identity is incomplete")
    feature_order_by_family = {
        "legacy_covfix": list(BASE_FEATURES),
        "autocycle_all_core": list(ALL_CORE_FEATURES),
    }
    receipt_trading_dates = [value.isoformat() for value in inputs.get("trading_dates") or ()]
    body = {
        "schema_version": C010_POLICY_VERSION,
        "formula_version": C010_FORMULA_VERSION,
        "producer_commit": producer_commit,
        "train_start": train_start.isoformat(),
        "train_end": train_end.isoformat(),
        "receipt_trading_dates": receipt_trading_dates,
        "receipt_trading_date_count": len(receipt_trading_dates),
        "receipt_trading_date_sha256": canonical_sha256(receipt_trading_dates),
        "contributor_min_availability": MIN_COVERAGE,
        "domain_min_count_coverage": MIN_COVERAGE,
        "domain_min_weight_coverage": MIN_COVERAGE,
        "feature_cross_section_min_coverage": MIN_COVERAGE,
        "moneyflow_mandatory_fields": list(feature_definitions["L1"]["moneyflow_mandatory_fields"]),
        "eligibility_receipt": eligibility,
        "eligibility_receipt_sha256": eligibility.get("receipt_sha256"),
        "eligibility_entry_count": int(eligibility.get("entry_count") or 0),
        "contributor_ledger": eligibility["entries"],
        "contributor_ledger_sha256": canonical_sha256(eligibility["entries"]),
        "excluded_moneyflow_symbols": list(eligibility.get("excluded_moneyflow_symbols") or ()),
        "excluded_moneyflow_symbol_sha256": canonical_sha256(list(eligibility.get("excluded_moneyflow_symbols") or ())),
        "aggregate_receipt": aggregate,
        "aggregate_receipt_sha256": aggregate.get("receipt_sha256"),
        "l1_cross_section_receipt": l1_cross,
        "l1_cross_section_receipt_sha256": l1_cross.get("receipt_sha256"),
        "l2_cross_section_receipt": l2_cross,
        "l2_cross_section_receipt_sha256": l2_cross.get("receipt_sha256"),
        "l1_feature_definition": feature_definitions["L1"],
        "l1_feature_definition_sha256": canonical_sha256(feature_definitions["L1"]),
        "l2_feature_definition": feature_definitions["L2"],
        "l2_feature_definition_sha256": canonical_sha256(feature_definitions["L2"]),
        "feature_order_by_family": feature_order_by_family,
        "feature_order_sha256": canonical_sha256(feature_order_by_family),
        "dataset_manifest_hash": canonical_sha256(inputs["dataset_manifest"]),
        "mapping_manifest_hash": canonical_sha256(inputs["mapping_manifest"]),
        "l2_stock_fact_manifest_hash": canonical_sha256(inputs["l2_stock_fact_manifest"]),
        "calendar_manifest_hash": canonical_sha256(inputs["dataset_manifest"]["calendar_benchmark"]),
        "security_identity_manifest_sha256": inputs["security_identity_manifest"].get("manifest_sha256"),
        "provider_absence_manifest_sha256": inputs["provider_absence_manifest"].get("manifest_sha256"),
        "causal_circ_mv_identity": circ_mv_identity,
        "causal_circ_mv_identity_sha256": canonical_sha256(circ_mv_identity),
        "pit_universe_changed": False,
        "selection_universe_changed": False,
        "runtime_prediction_eligibility_changed": False,
    }
    if body["eligibility_entry_count"] <= 0:
        raise StateModelSetError("C-010 formal eligibility ledger is empty")
    manifest = {**body, "receipt_sha256": canonical_sha256(body)}
    return validate_c010_policy_manifest(manifest)


def _require_c010_policy_identity(request: dict[str, Any]) -> None:
    manifest = request.get("feature_domain_policy_manifest")
    identity = str(request.get("feature_domain_policy_sha256") or "")
    if not isinstance(manifest, dict):
        raise StateModelSetError(
            "hmm_risk_c010_policy_identity_mismatch: formal B3 request C-010 policy manifest is missing"
        )
    try:
        validated = validate_c010_policy_manifest(manifest)
    except StateModelSetError as exc:
        raise StateModelSetError(f"hmm_risk_c010_policy_identity_mismatch: {exc}") from exc
    if identity != validated.get("receipt_sha256"):
        raise StateModelSetError(
            "hmm_risk_c010_policy_identity_mismatch: formal B3 request C-010 policy identity is invalid"
        )
    if request.get("parent_frozen_identities") != B3_APPROVED_FROZEN_IDENTITIES:
        raise StateModelSetError("formal B3 request parent frozen identity is invalid")


def prepare_b3_preflight_candidate(request_template: dict[str, Any], *, db_prefix: str) -> dict[str, Any]:
    """Freeze the approved C-010 policy and current PIT identities without model actions."""

    producer_commit = _formal_producer_commit()
    _require_approved_b3_identities(request_template)
    approved_request = _freeze_approved_b3_windows(request_template)
    train_request = _c009_train_source_request(approved_request)
    _require_b3_semantic_source_containment(approved_request)
    semantic_request = _b3_semantic_source_request(approved_request)
    inputs = _load_l1_source_inputs(train_request, db_prefix=db_prefix, c010_formal=True)
    semantic_inputs = _load_l1_source_inputs(semantic_request, db_prefix=db_prefix, c010_formal=True)
    dataset_hash = canonical_sha256(inputs["dataset_manifest"])
    mapping_hash = canonical_sha256(inputs["mapping_manifest"])
    l2_stock_fact_hash = canonical_sha256(inputs["l2_stock_fact_manifest"])
    semantic_identities = _semantic_input_identities(semantic_inputs)
    policy_manifest = _c010_policy_manifest(inputs, train_request, producer_commit=producer_commit)
    policy_sha256 = str(policy_manifest["receipt_sha256"])
    inputs["feature_domain_policy_sha256"] = policy_sha256
    train_coverage = _b3_train_coverage_preflight(inputs, train_request)
    _require_canonical_receipt(train_coverage, label="C-010 formal train coverage")
    if (
        train_coverage.get("feature_domain_policy_sha256") != policy_sha256
        or train_coverage.get("formula_version") != C010_FORMULA_VERSION
    ):
        raise StateModelSetError("C-010 formal train coverage policy identity is invalid")
    train_coverage_valid = train_coverage["train_coverage_valid"] is True
    train_start, train_end = _request_train_window(train_request)
    train_trading_dates = tuple(
        value for value in inputs.get("trading_dates") or () if train_start <= value <= train_end
    )
    if len(train_trading_dates) != 601:
        raise StateModelSetError(
            f"C-010 frozen train calendar must contain 601 trading dates, got {len(train_trading_dates)}"
        )
    request_candidate = deepcopy(train_request)
    request_candidate.update(
        {
            "producer_commit": producer_commit,
            "dataset_manifest_hash": dataset_hash,
            "mapping_manifest_hash": mapping_hash,
            "l2_stock_fact_manifest_hash": l2_stock_fact_hash,
            "semantic_source": deepcopy(semantic_request["source"]),
            **semantic_identities,
            "parent_frozen_identities": dict(B3_APPROVED_FROZEN_IDENTITIES),
            "feature_domain_policy_manifest": policy_manifest,
            "feature_domain_policy_sha256": policy_sha256,
            "train_coverage_contract_version": B3_TRAIN_COVERAGE_PREFLIGHT_VERSION,
            "train_coverage_receipt_sha256": train_coverage["receipt_sha256"],
        }
    )
    l1_stock_facts = inputs["dataset_manifest"]["stock_facts"]
    l2_stock_facts = inputs["l2_stock_fact_manifest"]
    body = {
        "schema_version": C010_FORMAL_PREFLIGHT_SCHEMA,
        "status": "candidate_ready" if train_coverage_valid else "blocked",
        "source_template_producer_commit": str(request_template.get("producer_commit") or ""),
        "producer_commit": producer_commit,
        "database": inputs["database"],
        "approved_frozen_identities": dict(B3_APPROVED_FROZEN_IDENTITIES),
        "approved_frozen_identities_match": True,
        "feature_domain_policy_manifest": policy_manifest,
        "feature_domain_policy_sha256": policy_sha256,
        "feature_domain_policy_evidence": {
            "eligibility": inputs["c010_diagnostic"]["eligibility"],
            "aggregate": inputs["c010_diagnostic"]["aggregate_evidence"],
            "L1_cross_section": inputs["c010_diagnostic"]["l1_cross_section_evidence"],
            "L2_cross_section": inputs["c010_diagnostic"]["l2_cross_section_evidence"],
            "L1_feature_definition": inputs["c010_diagnostic"]["l1_feature_definition"],
            "L2_feature_definition": inputs["c010_diagnostic"]["l2_feature_definition"],
        },
        "formula_version": C010_FORMULA_VERSION,
        "train_start": train_start.isoformat(),
        "train_end": train_end.isoformat(),
        "validation_start": B3_APPROVED_WINDOWS["validation_start"],
        "validation_end": B3_APPROVED_WINDOWS["validation_end"],
        "common_data_watermark": B3_APPROVED_WINDOWS["common_data_watermark"],
        "train_trading_date_count": len(train_trading_dates),
        "train_trading_date_sha256": canonical_sha256([value.isoformat() for value in train_trading_dates]),
        "dataset_manifest_hash": dataset_hash,
        "mapping_manifest_hash": mapping_hash,
        "l2_stock_fact_manifest_hash": l2_stock_fact_hash,
        **semantic_identities,
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
    request["source"]["circ_mv_history_start"] = str(
        request_template["source"].get("circ_mv_history_start") or source_start.isoformat()
    )
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
        "circ_mv_lookback_contract_version",
        "circ_mv_history_start",
        "circ_mv_pit_boundary_crossing_count",
        "circ_mv_pit_boundary_crossing_available_count",
        "circ_mv_pit_boundary_crossing_invalid_count",
        "circ_mv_pit_boundary_crossing_key_sha256",
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


def prepare_c010_observation_eligibility_diagnostic(
    request_template: dict[str, Any],
    *,
    db_prefix: str,
) -> dict[str, Any]:
    """Audit feature-domain eligibility/masks without activating model policy."""

    producer_commit = _formal_producer_commit()
    train_request = _c009_train_source_request(request_template)
    inputs = _load_l1_source_inputs(train_request, db_prefix=db_prefix, c010_diagnostic=True)
    c010 = inputs.get("c010_diagnostic")
    if not isinstance(c010, dict):
        raise StateModelSetError("C-010 diagnostic payload is missing")
    baseline_coverage = _b3_train_coverage_preflight(inputs, train_request)
    aggregate_evidence = c010["aggregate_evidence"]
    reports: dict[str, Any] = {}
    for family in sorted(train_request["families"], key=lambda item: str(item.get("family") or "")):
        family_name = str(family.get("family") or "")
        features = tuple(str(value) for value in family.get("feature_names") or ())
        train_start = _date(family.get("train_start"), "train_start")
        train_end = _date(family.get("train_end"), "train_end")
        for level, panel, expected_count, unavailable in (
            ("L1", c010["l1_panel"], 31, aggregate_evidence["impacted_l1_codes"]),
            ("L2", c010["l2_panel"], 131, aggregate_evidence["impacted_l2_codes"]),
        ):
            reports[f"{family_name}:{level}"] = audit_feature_mask_candidates(
                panel,
                family=family_name,
                feature_names=features,
                train_start=train_start,
                train_end=train_end,
                direct_sector_level=level,
                expected_sector_count=expected_count,
                moneyflow_unavailable_sector_codes=unavailable,
            )
    candidate_valid = len(reports) == 4 and all(report["feature_mask_candidate_valid"] for report in reports.values())
    calendar = inputs["dataset_manifest"]["calendar_benchmark"]
    if int(calendar.get("row_count") or 0) != 601:
        raise StateModelSetError(
            f"C-010 frozen train calendar must contain 601 trading dates, got {calendar.get('row_count')}"
        )
    body = {
        "schema_version": C010_OBSERVATION_ELIGIBILITY_SCHEMA,
        "status": "diagnostic_complete",
        "producer_commit": producer_commit,
        "source_start": train_request["source"]["source_start"],
        "source_end": train_request["source"]["source_end"],
        "trading_date_count": int(calendar["row_count"]),
        "dataset_manifest_hash": canonical_sha256(inputs["dataset_manifest"]),
        "mapping_manifest_hash": canonical_sha256(inputs["mapping_manifest"]),
        "l2_stock_fact_manifest_hash": canonical_sha256(inputs["l2_stock_fact_manifest"]),
        "security_source_identity": inputs["security_identity_manifest"],
        "provider_absence_authority": inputs["provider_absence_manifest"],
        "baseline_train_coverage": baseline_coverage,
        "observation_eligibility": c010["eligibility"],
        "feature_domain_aggregate_evidence": aggregate_evidence,
        "l1_feature_definition": c010["l1_feature_definition"],
        "l2_feature_definition": c010["l2_feature_definition"],
        "l1_cross_section_evidence": c010["l1_cross_section_evidence"],
        "l2_cross_section_evidence": c010["l2_cross_section_evidence"],
        "feature_mask_candidate_reports": reports,
        "feature_mask_candidate_valid": candidate_valid,
        "pit_universe_changed": False,
        "selection_universe_changed": False,
        "runtime_prediction_eligibility_changed": False,
        "formal_policy_activated": False,
        "fit_performed": False,
        "selection_performed": False,
        "d6_performed": False,
        "validation_accessed": False,
        "future_utility_accessed": False,
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
    identity = {
        "dataset_manifest_hash": canonical_sha256(inputs["dataset_manifest"]),
        "mapping_manifest_hash": canonical_sha256(inputs["mapping_manifest"]),
        "calendar_manifest_hash": canonical_sha256(inputs["dataset_manifest"]["calendar_benchmark"]),
        "l2_stock_fact_manifest_hash": canonical_sha256(inputs["l2_stock_fact_manifest"]),
    }
    policy_sha256 = str(inputs.get("feature_domain_policy_sha256") or "")
    if policy_sha256:
        identity["feature_domain_policy_sha256"] = policy_sha256
        identity["formula_version"] = C010_FORMULA_VERSION
    return identity


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
    _require_approved_b3_windows(request)
    _require_c010_policy_identity(request)
    _require_formal_train_coverage_identity(request)
    if str(request.get("producer_commit") or "") != producer_commit:
        raise StateModelSetError("B3 request producer_commit differs from current code")
    inputs = _load_l1_source_inputs(request, db_prefix=db_prefix, c010_formal=True)
    dataset_hash = canonical_sha256(inputs["dataset_manifest"])
    mapping_hash = canonical_sha256(inputs["mapping_manifest"])
    l2_stock_fact_hash = canonical_sha256(inputs["l2_stock_fact_manifest"])
    if str(request.get("dataset_manifest_hash") or "") != dataset_hash:
        raise StateModelSetError("B3 frozen dataset manifest hash mismatch")
    if str(request.get("mapping_manifest_hash") or "") != mapping_hash:
        raise StateModelSetError("B3 frozen mapping manifest hash mismatch")
    if str(request.get("l2_stock_fact_manifest_hash") or "") != l2_stock_fact_hash:
        raise StateModelSetError("B3 frozen L2 stock-fact manifest hash mismatch")
    recomputed_policy = _c010_policy_manifest(inputs, request, producer_commit=producer_commit)
    if request["feature_domain_policy_sha256"] != recomputed_policy["receipt_sha256"]:
        raise StateModelSetError("B3 C-010 feature-domain policy hash mismatch")
    if request["feature_domain_policy_manifest"] != recomputed_policy:
        raise StateModelSetError("B3 C-010 feature-domain policy manifest mismatch")
    inputs["feature_domain_policy_sha256"] = recomputed_policy["receipt_sha256"]
    train_coverage = _b3_train_coverage_preflight(inputs, request)
    _require_canonical_receipt(train_coverage, label="B3 formal train coverage")
    if (
        train_coverage.get("feature_domain_policy_sha256") != recomputed_policy["receipt_sha256"]
        or train_coverage.get("formula_version") != C010_FORMULA_VERSION
    ):
        raise StateModelSetError("B3 formal train coverage policy identity mismatch")
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
        "feature_domain_policy_sha256": recomputed_policy["receipt_sha256"],
        "formula_version": C010_FORMULA_VERSION,
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


def _load_b3_blocker_train_inputs(
    request: dict[str, Any],
    *,
    db_prefix: str,
    target_manifest: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    """Reload the frozen formal inputs without requiring the old producer checkout."""

    authority = B3_BLOCKER_FORMAL_AUTHORITY
    _require_approved_b3_windows(request)
    _require_c010_policy_identity(request)
    _require_formal_train_coverage_identity(request)
    for field in (
        "producer_commit",
        "dataset_manifest_hash",
        "mapping_manifest_hash",
        "l2_stock_fact_manifest_hash",
        "semantic_dataset_manifest_hash",
        "semantic_mapping_manifest_hash",
        "semantic_calendar_manifest_hash",
        "semantic_l2_stock_fact_manifest_hash",
        "feature_domain_policy_sha256",
    ):
        if str(request.get(field) or "") != authority[field]:
            raise StateModelSetError(f"blocker diagnostic request {field} differs from formal authority")
    if target_manifest.get("formal_producer_commit") != authority["producer_commit"]:
        raise StateModelSetError("blocker diagnostic target producer identity is invalid")
    parameter_profile_sha256 = canonical_sha256(formal_b3_parameter_profile())
    if parameter_profile_sha256 != target_manifest.get("parameter_profile_sha256"):
        raise StateModelSetError("blocker diagnostic current parameter profile differs from the formal artifact")

    inputs = _load_l1_source_inputs(request, db_prefix=db_prefix, c010_formal=True)
    identities = {
        "dataset_manifest_hash": canonical_sha256(inputs["dataset_manifest"]),
        "mapping_manifest_hash": canonical_sha256(inputs["mapping_manifest"]),
        "calendar_manifest_hash": canonical_sha256(inputs["dataset_manifest"]["calendar_benchmark"]),
        "l2_stock_fact_manifest_hash": canonical_sha256(inputs["l2_stock_fact_manifest"]),
    }
    for field, actual in identities.items():
        if actual != authority[field] or actual != str(request.get(field) or actual):
            raise StateModelSetError(f"blocker diagnostic frozen input drifted from {field}")
    recomputed_policy = _c010_policy_manifest(
        inputs,
        request,
        producer_commit=authority["producer_commit"],
    )
    if (
        recomputed_policy != request.get("feature_domain_policy_manifest")
        or recomputed_policy.get("receipt_sha256") != authority["feature_domain_policy_sha256"]
    ):
        raise StateModelSetError("blocker diagnostic C-010 feature-domain policy identity drifted")
    inputs["feature_domain_policy_sha256"] = str(recomputed_policy["receipt_sha256"])
    train_coverage = _b3_train_coverage_preflight(inputs, request)
    _require_canonical_receipt(train_coverage, label="blocker diagnostic train coverage")
    if train_coverage.get("train_coverage_valid") is not True or train_coverage.get("receipt_sha256") != request.get(
        "train_coverage_receipt_sha256"
    ):
        raise StateModelSetError("blocker diagnostic frozen train coverage identity drifted")
    return inputs, identities


def prepare_b3_blocker_diag01_pass(
    request: dict[str, Any],
    formal_report: dict[str, Any],
    target_manifest: dict[str, Any],
    *,
    db_prefix: str,
) -> dict[str, Any]:
    """Run exactly one approved 174-pair train-only diagnostic pass."""

    inputs, identities = _load_b3_blocker_train_inputs(
        request,
        db_prefix=db_prefix,
        target_manifest=target_manifest,
    )
    if canonical_sha256(formal_report) != target_manifest.get("formal_report_sha256"):
        raise StateModelSetError("blocker diagnostic formal report changed after target derivation")
    families = {str(item.get("family") or ""): item for item in request.get("families") or ()}
    if set(families) != {"legacy_covfix", "autocycle_all_core"}:
        raise StateModelSetError("blocker diagnostic requires exactly the two approved families")
    evidence: list[dict[str, Any]] = []
    train_series_by_family: dict[str, dict[str, dict[str, Any]]] = {}
    for key in ("autocycle_all_core:L1", "autocycle_all_core:L2", "legacy_covfix:L2"):
        family, level = key.split(":", 1)
        family_request = families[family]
        if family not in train_series_by_family:
            train_series_by_family[family] = _direct_train_series_for_family(inputs, family_request)
        series = train_series_by_family[family][level]
        targets = [
            target for target in target_manifest["targets"] if target["family"] == family and target["level"] == level
        ]
        evidence.extend(
            run_b3_blocker_targeted_level(
                series,
                targets,
                family=family,
                level=level,
                feature_names=tuple(str(value) for value in family_request.get("feature_names") or ()),
                preprocess_family=str(family_request.get("preprocess_family") or ""),
            )
        )
    if len(evidence) != 174 or len({item["diagnostic_entry_sha256"] for item in evidence}) != 174:
        raise StateModelSetError("blocker diagnostic pass did not produce 174 unique evidence receipts")
    numeric_environment = c008_b3_diag04_fixed_numeric_environment()
    body = {
        "schema_version": "hmm_risk_c008_b3_formal_blocker_diag01_pass_v1",
        "diagnostic_producer_commit": _formal_producer_commit(),
        "formal_producer_commit": B3_BLOCKER_FORMAL_AUTHORITY["producer_commit"],
        "formal_report_sha256": target_manifest["formal_report_sha256"],
        "target_manifest_sha256": target_manifest["target_manifest_sha256"],
        **identities,
        "feature_domain_policy_sha256": B3_BLOCKER_FORMAL_AUTHORITY["feature_domain_policy_sha256"],
        "formula_version": B3_BLOCKER_FORMAL_AUTHORITY["formula_version"],
        "parameter_profile_sha256": target_manifest["parameter_profile_sha256"],
        "numeric_environment": numeric_environment,
        "numeric_environment_sha256": canonical_sha256(numeric_environment),
        "targeted_evidence": evidence,
        "fit_count": len(evidence),
        "validation_accessed": False,
        "future_utility_accessed": False,
        "selection_performed": False,
        "acceptance_decision_reexecuted": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "pass_receipt_sha256": canonical_sha256(body)}


def _b3_blocker_child_command(args: argparse.Namespace, target_manifest_sha256: str) -> list[str]:
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
        "--b3-formal-report",
        str(Path(args.b3_formal_report).resolve()),
        "--b3-target-manifest-sha256",
        target_manifest_sha256,
        "--_b3-blocker-diag01-child",
    ]


def _parse_canonical_child_payload(payload: bytes, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(payload.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StateModelSetError(f"{label} returned invalid JSON: {exc}") from exc
    if not isinstance(value, dict) or canonical_json_bytes(value) != payload:
        raise StateModelSetError(f"{label} did not return one canonical JSON object")
    identity = str(value.get("pass_receipt_sha256") or "")
    body = {key: item for key, item in value.items() if key != "pass_receipt_sha256"}
    if identity != canonical_sha256(body):
        raise StateModelSetError(f"{label} pass receipt hash is invalid")
    return value


def _validate_b3_blocker_numeric_environment(numeric_environment: dict[str, Any]) -> None:
    expected = c008_b3_diag04_fixed_numeric_environment()
    stable_fields = (
        "schema_version",
        "scope",
        "python_version",
        "python_implementation",
        "python_executable",
        "packages",
        "thread_env",
    )
    if set(numeric_environment) != set(expected) or any(
        numeric_environment.get(field) != expected.get(field) for field in stable_fields
    ):
        raise StateModelSetError("blocker diagnostic child numeric environment identity is invalid")
    pools = numeric_environment.get("thread_pools")
    if not isinstance(pools, list) or any(not isinstance(pool, dict) for pool in pools):
        raise StateModelSetError("blocker diagnostic child thread pool inventory is invalid")
    non_single = [
        {
            "user_api": pool.get("user_api"),
            "internal_api": pool.get("internal_api"),
            "num_threads": pool.get("num_threads"),
        }
        for pool in pools
        if isinstance(pool.get("num_threads"), bool)
        or not isinstance(pool.get("num_threads"), int)
        or pool["num_threads"] != 1
    ]
    if non_single:
        raise StateModelSetError(f"blocker diagnostic child thread pools are not single-threaded: {non_single}")


def _validate_b3_blocker_pass(value: dict[str, Any], target_manifest: dict[str, Any]) -> None:
    authority = B3_BLOCKER_FORMAL_AUTHORITY
    expected_scalars = {
        "schema_version": "hmm_risk_c008_b3_formal_blocker_diag01_pass_v1",
        "formal_producer_commit": authority["producer_commit"],
        "formal_report_sha256": target_manifest["formal_report_sha256"],
        "target_manifest_sha256": target_manifest["target_manifest_sha256"],
        "dataset_manifest_hash": authority["dataset_manifest_hash"],
        "mapping_manifest_hash": authority["mapping_manifest_hash"],
        "calendar_manifest_hash": authority["calendar_manifest_hash"],
        "l2_stock_fact_manifest_hash": authority["l2_stock_fact_manifest_hash"],
        "feature_domain_policy_sha256": authority["feature_domain_policy_sha256"],
        "formula_version": authority["formula_version"],
        "parameter_profile_sha256": target_manifest["parameter_profile_sha256"],
        "fit_count": target_manifest["target_pair_count"],
        "validation_accessed": False,
        "future_utility_accessed": False,
        "selection_performed": False,
        "acceptance_decision_reexecuted": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    for field, expected in expected_scalars.items():
        if value.get(field) != expected:
            raise StateModelSetError(f"blocker diagnostic child {field} closure is invalid")
    producer_commit = str(value.get("diagnostic_producer_commit") or "").lower()
    if len(producer_commit) != 40 or any(character not in "0123456789abcdef" for character in producer_commit):
        raise StateModelSetError("blocker diagnostic child producer commit identity is invalid")
    numeric_environment = value.get("numeric_environment")
    if not isinstance(numeric_environment, dict) or value.get("numeric_environment_sha256") != canonical_sha256(
        numeric_environment
    ):
        raise StateModelSetError("blocker diagnostic child numeric environment identity is invalid")
    _validate_b3_blocker_numeric_environment(numeric_environment)
    evidence = value.get("targeted_evidence")
    if not isinstance(evidence, list) or len(evidence) != target_manifest["target_pair_count"]:
        raise StateModelSetError("blocker diagnostic child evidence count is invalid")
    identities: set[str] = set()
    target_fields = (
        "role",
        "family",
        "level",
        "seed",
        "sector_code",
        "source_entry_receipt_sha256",
        "formal_failed_stages",
    )
    for index, (entry, target) in enumerate(zip(evidence, target_manifest["targets"], strict=True)):
        if not isinstance(entry, dict) or any(entry.get(field) != target.get(field) for field in target_fields):
            raise StateModelSetError(f"blocker diagnostic child target identity differs at index {index}")
        identity = str(entry.get("diagnostic_entry_sha256") or "")
        body = {key: item for key, item in entry.items() if key != "diagnostic_entry_sha256"}
        if identity != canonical_sha256(body) or identity in identities:
            raise StateModelSetError(f"blocker diagnostic child evidence receipt is invalid at index {index}")
        identities.add(identity)
        if entry.get("status") not in {"fit_completed", "fit_failed"}:
            raise StateModelSetError(f"blocker diagnostic child status is invalid at index {index}")
        if entry.get("formal_entry_receipt_reproduced") is not True:
            raise StateModelSetError(f"blocker diagnostic child did not reproduce formal receipt at index {index}")
        for field in ("validation_accessed", "future_utility_accessed", "selection_performed", "model_write_performed"):
            if entry.get(field) is not False:
                raise StateModelSetError(f"blocker diagnostic child {field} is invalid at index {index}")


def run_b3_blocker_diag01_repeated(args: argparse.Namespace, request: dict[str, Any]) -> dict[str, Any]:
    """Run the approved targeted train replay twice, then replay selected D6 without refitting."""

    formal_report = _load_json_mapping(Path(args.b3_formal_report).resolve(), label="formal B3 report")
    target_manifest = derive_b3_blocker_target_manifest(formal_report)
    environment = os.environ.copy()
    for key in (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        environment[key] = "1"
    raw_repeats: list[bytes] = []
    repeats: list[dict[str, Any]] = []
    for index in (1, 2):
        completed = subprocess.run(
            _b3_blocker_child_command(args, target_manifest["target_manifest_sha256"]),
            check=False,
            capture_output=True,
            env=environment,
            timeout=7200,
        )
        if completed.returncode != 0:
            error = completed.stderr.decode("utf-8", errors="replace")[-4000:]
            raise StateModelSetError(f"blocker diagnostic fresh process {index} failed: {error}")
        raw_repeats.append(completed.stdout)
        repeat = _parse_canonical_child_payload(completed.stdout, label=f"fresh process {index}")
        _validate_b3_blocker_pass(repeat, target_manifest)
        repeats.append(repeat)
    if raw_repeats[0] != raw_repeats[1]:
        raise StateModelSetError("blocker diagnostic fresh-process canonical payloads differ")
    if repeats[0].get("target_manifest_sha256") != target_manifest["target_manifest_sha256"]:
        raise StateModelSetError("blocker diagnostic child used a different target manifest")

    semantic_inputs = _load_verified_formal_semantic_inputs(request, db_prefix=str(args.db_env_prefix))
    semantic_identities = _semantic_input_identities(semantic_inputs)
    for field, actual in semantic_identities.items():
        if actual != B3_BLOCKER_FORMAL_AUTHORITY[field]:
            raise StateModelSetError(f"blocker diagnostic D6 semantic input drifted from {field}")
    semantic_inputs["feature_domain_policy_sha256"] = B3_BLOCKER_FORMAL_AUTHORITY["feature_domain_policy_sha256"]
    family = next(item for item in request["families"] if str(item.get("family") or "") == "legacy_covfix")
    semantic_series = _direct_series_for_family(semantic_inputs, family)["L1"]
    d6_replay = replay_b3_blocker_selected_d6(formal_report, semantic_series, target_manifest)
    matched_comparisons = build_b3_blocker_matched_comparisons(repeats[0]["targeted_evidence"])
    body = {
        "schema_version": B3_BLOCKER_DIAGNOSTIC_VERSION,
        "status": "diagnostic_complete",
        "diagnostic_contract": "C-008-B3-FORMAL-BLOCKER-DIAG-01",
        "diagnostic_producer_commit": repeats[0]["diagnostic_producer_commit"],
        "formal_authority": dict(B3_BLOCKER_FORMAL_AUTHORITY),
        "target_manifest": target_manifest,
        "numeric_environment": repeats[0]["numeric_environment"],
        "numeric_environment_sha256": repeats[0]["numeric_environment_sha256"],
        "expected_fresh_process_count": target_manifest["fresh_process_count"],
        "observed_fresh_process_count": len(repeats),
        "expected_fits_per_process": target_manifest["fits_per_process"],
        "observed_fits_per_process": repeats[0]["fit_count"],
        "expected_total_fit_count": target_manifest["total_fit_budget"],
        "observed_total_fit_count": sum(repeat["fit_count"] for repeat in repeats),
        "fresh_process_payload_sha256": [sha256_bytes(payload) for payload in raw_repeats],
        "fresh_process_pass_receipt_sha256": [repeat["pass_receipt_sha256"] for repeat in repeats],
        "canonical_payload_bitwise_equal": True,
        "targeted_evidence": repeats[0]["targeted_evidence"],
        "matched_comparisons": matched_comparisons,
        "d6_replay": d6_replay,
        "d6_replay_count": len(d6_replay),
        "selection_performed": False,
        "selection_reexecuted": False,
        "acceptance_decision_reexecuted": False,
        "formal_thresholds_changed": False,
        "hard_semantic_authority_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


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

    _require_approved_b3_windows(request)
    _require_formal_semantic_identity(request)
    _require_c010_policy_identity(request)
    _require_formal_train_coverage_identity(request)
    _load_verified_formal_semantic_inputs(
        request,
        db_prefix=str(args.db_env_prefix),
    )
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
    if repeats[0]["feature_domain_policy_sha256"] != repeats[1]["feature_domain_policy_sha256"]:
        raise StateModelSetError("formal B3 fresh processes used different C-010 policy identities")
    if repeats[0]["feature_domain_policy_sha256"] != request.get("feature_domain_policy_sha256"):
        raise StateModelSetError("formal B3 fresh-process policy identity differs from request")
    train_inputs = _load_l1_source_inputs(request, db_prefix=str(args.db_env_prefix), c010_formal=True)
    if canonical_sha256(train_inputs["dataset_manifest"]) != repeats[0]["dataset_manifest_hash"]:
        raise StateModelSetError("formal B3 D6 reload drifted from the frozen dataset manifest")
    if canonical_sha256(train_inputs["mapping_manifest"]) != repeats[0]["mapping_manifest_hash"]:
        raise StateModelSetError("formal B3 D6 reload drifted from the frozen mapping manifest")
    if canonical_sha256(train_inputs["dataset_manifest"]["calendar_benchmark"]) != repeats[0]["calendar_manifest_hash"]:
        raise StateModelSetError("formal B3 D6 reload drifted from the frozen calendar manifest")
    if canonical_sha256(train_inputs["l2_stock_fact_manifest"]) != repeats[0]["l2_stock_fact_manifest_hash"]:
        raise StateModelSetError("formal B3 D6 reload drifted from the frozen L2 stock-fact manifest")
    recomputed_policy = _c010_policy_manifest(train_inputs, request, producer_commit=_formal_producer_commit())
    if recomputed_policy["receipt_sha256"] != repeats[0]["feature_domain_policy_sha256"]:
        raise StateModelSetError("formal B3 D6 reload drifted from the C-010 policy identity")
    train_inputs["feature_domain_policy_sha256"] = recomputed_policy["receipt_sha256"]
    semantic_inputs = _load_verified_formal_semantic_inputs(
        request,
        db_prefix=str(args.db_env_prefix),
    )
    semantic_identities = _semantic_input_identities(semantic_inputs)
    semantic_inputs["feature_domain_policy_sha256"] = recomputed_policy["receipt_sha256"]
    selections: dict[tuple[str, str], dict[str, Any]] = {}
    selected_artifacts: dict[tuple[str, str], dict[str, Any]] = {}
    family_map = {str(item["family"]): item for item in request["families"]}
    for family in ("legacy_covfix", "autocycle_all_core"):
        series_by_level = _direct_series_for_family(semantic_inputs, family_map[family])
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
                feature_domain_policy_sha256=repeats[0]["feature_domain_policy_sha256"],
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
            semantic_dataset_manifest_hash=semantic_identities["semantic_dataset_manifest_hash"],
            semantic_mapping_manifest_hash=semantic_identities["semantic_mapping_manifest_hash"],
            semantic_calendar_manifest_hash=semantic_identities["semantic_calendar_manifest_hash"],
            semantic_l2_stock_fact_manifest_hash=semantic_identities["semantic_l2_stock_fact_manifest_hash"],
            feature_domain_policy_sha256=repeats[0]["feature_domain_policy_sha256"],
            feature_domain_policy_manifest=request["feature_domain_policy_manifest"],
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
        **semantic_identities,
        "feature_domain_policy_sha256": repeats[0]["feature_domain_policy_sha256"],
        "formula_version": C010_FORMULA_VERSION,
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
        "--c010-observation-eligibility-output",
        help="Run the approved C-010 feature-domain eligibility diagnostic without activating model policy.",
    )
    diagnostic_group.add_argument(
        "--b3-preparation-output",
        help="Run formal two-process B3 L1/L2 preparation and write its immutable receipt.",
    )
    diagnostic_group.add_argument(
        "--b3-blocker-diagnostic-output",
        help="Run the approved 174-pair x two-process blocker diagnostic and zero-refit D6 replay.",
    )
    parser.add_argument(
        "--b3-request-candidate-output",
        help="Immutable request candidate output; required only with --b3-preflight-output.",
    )
    parser.add_argument("--_c008-b3-diag02-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--_c008-b3-diag04-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--_b3-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--_b3-blocker-diag01-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--b3-process-identity", default="", help=argparse.SUPPRESS)
    parser.add_argument("--b3-formal-report", help="Approved immutable formal B3 preparation report.")
    parser.add_argument("--b3-target-manifest-sha256", default="", help=argparse.SUPPRESS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        _read_env_file(Path(args.env_file).resolve())
        request_path = Path(args.request).resolve()
        c009_preflight_output = getattr(args, "c009_stock_fact_preflight_output", None)
        c010_diagnostic_output = getattr(args, "c010_observation_eligibility_output", None)
        if args.b3_preflight_output or c009_preflight_output or c010_diagnostic_output:
            if not args.b3_request_candidate_output:
                if args.b3_preflight_output:
                    raise StateModelSetError("--b3-request-candidate-output is required with --b3-preflight-output")
            request = _load_request_template(request_path)
        else:
            if args.b3_request_candidate_output:
                raise StateModelSetError("--b3-request-candidate-output is only valid with --b3-preflight-output")
            request = _load_request(request_path)
        blocker_output = getattr(args, "b3_blocker_diagnostic_output", None)
        blocker_formal_report = getattr(args, "b3_formal_report", None)
        blocker_target_sha256 = getattr(args, "b3_target_manifest_sha256", "")
        blocker_parent = bool(blocker_output)
        blocker_child = bool(getattr(args, "_b3_blocker_diag01_child", False))
        if blocker_parent or blocker_child:
            if not blocker_formal_report:
                raise StateModelSetError("--b3-formal-report is required for the blocker diagnostic")
        elif blocker_formal_report or blocker_target_sha256:
            raise StateModelSetError("blocker diagnostic authority arguments require blocker diagnostic mode")
        if blocker_parent and blocker_target_sha256:
            raise StateModelSetError("--b3-target-manifest-sha256 is child-only")
        if blocker_child and not blocker_target_sha256:
            raise StateModelSetError("blocker diagnostic child target manifest identity is required")
        if c010_diagnostic_output:
            if args.b3_request_candidate_output:
                raise StateModelSetError(
                    "--b3-request-candidate-output is not valid with --c010-observation-eligibility-output"
                )
            report = prepare_c010_observation_eligibility_diagnostic(
                request,
                db_prefix=str(args.db_env_prefix),
            )
            report_path = Path(c010_diagnostic_output).resolve()
            report_sha256 = _write_diagnostic_report(report_path, report)
            receipt = {
                "schema_version": "hmm_risk_c010_observation_eligibility_cli_receipt_v1",
                "status": report["status"],
                "report_path": str(report_path),
                "report_sha256": report_sha256,
                "feature_mask_candidate_valid": report["feature_mask_candidate_valid"],
                "excluded_moneyflow_symbols": report["observation_eligibility"]["excluded_moneyflow_symbols"],
                "pit_universe_changed": False,
                "selection_universe_changed": False,
                "formal_policy_activated": False,
                "fit_performed": False,
                "selection_performed": False,
                "d6_performed": False,
                "model_write_performed": False,
                "ready_artifact_write_performed": False,
                "database_write_performed": False,
                "runtime_action_performed": False,
            }
            print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
            return 0
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
                "schema_version": "hmm_risk_c010_formal_preflight_cli_receipt_v1",
                "status": report["status"],
                "report_path": str(report_path),
                "report_sha256": report_sha256,
                "request_candidate_path": None if request_candidate_path is None else str(request_candidate_path),
                "request_candidate_sha256": request_candidate_sha256,
                "dataset_manifest_hash": report["dataset_manifest_hash"],
                "mapping_manifest_hash": report["mapping_manifest_hash"],
                "l2_stock_fact_manifest_hash": report["l2_stock_fact_manifest_hash"],
                "feature_domain_policy_sha256": report["feature_domain_policy_sha256"],
                "formula_version": report["formula_version"],
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
        if blocker_child:
            formal_report = _load_json_mapping(Path(blocker_formal_report).resolve(), label="formal B3 report")
            target_manifest = derive_b3_blocker_target_manifest(formal_report)
            if target_manifest["target_manifest_sha256"] != blocker_target_sha256:
                raise StateModelSetError("blocker diagnostic child target manifest identity mismatch")
            report = prepare_b3_blocker_diag01_pass(
                request,
                formal_report,
                target_manifest,
                db_prefix=str(args.db_env_prefix),
            )
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
        if blocker_parent:
            report_path = Path(blocker_output).resolve()
            try:
                report = run_b3_blocker_diag01_repeated(args, request)
            except Exception as exc:
                failure_body = {
                    "schema_version": B3_BLOCKER_DIAGNOSTIC_VERSION,
                    "status": "failed",
                    "diagnostic_contract": "C-008-B3-FORMAL-BLOCKER-DIAG-01",
                    "failure_reason_code": "hmm_risk_blocker_diagnostic_failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc)[-4000:],
                    "selection_performed": False,
                    "selection_reexecuted": False,
                    "acceptance_decision_reexecuted": False,
                    "formal_thresholds_changed": False,
                    "hard_semantic_authority_changed": False,
                    "model_write_performed": False,
                    "ready_artifact_write_performed": False,
                    "database_write_performed": False,
                    "runtime_action_performed": False,
                }
                failure = {**failure_body, "receipt_sha256": canonical_sha256(failure_body)}
                _write_diagnostic_report(report_path, failure)
                raise
            report_sha256 = _write_diagnostic_report(report_path, report)
            receipt = {
                "schema_version": "hmm_risk_c008_b3_formal_blocker_diag01_cli_receipt_v1",
                "status": report["status"],
                "diagnostic_contract": report["diagnostic_contract"],
                "report_path": str(report_path),
                "report_sha256": report_sha256,
                "target_pair_count": report["target_manifest"]["target_pair_count"],
                "total_fit_count": report["observed_total_fit_count"],
                "fresh_process_count": report["observed_fresh_process_count"],
                "canonical_payload_bitwise_equal": report["canonical_payload_bitwise_equal"],
                "d6_replay_count": report["d6_replay_count"],
                "selection_performed": False,
                "model_write_performed": False,
                "ready_artifact_write_performed": False,
                "database_write_performed": False,
                "runtime_action_performed": False,
            }
            print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
            return 0
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
