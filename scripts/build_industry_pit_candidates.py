#!/usr/bin/env python3
"""Build C-013 dual-authority candidates without production writes.

The PostgreSQL connection is forced read-only and supplies only the frozen
trading calendar/universe denominator plus a legacy conflict inventory.  It is
never an industry-authority source.  Classification facts come from the four
approved local files; index membership resolves only when a separate explicit
evidence JSON is supplied, otherwise every missing boundary remains typed
``membership_boundary_unavailable``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.pg_pool import get_conn  # noqa: E402
from backend.services.dataset_release.canonical import digest_named_fields  # noqa: E402
from backend.services.industry_pit.artifact_store import write_candidate_bundle  # noqa: E402
from backend.services.industry_pit.candidate_builder import (  # noqa: E402
    FrozenDenominator,
    UniverseSpan,
    build_classification_intervals,
    build_index_membership_intervals,
    build_taxonomy_catalog,
    full_denominator_preflight,
)
from backend.services.industry_pit.contracts import (  # noqa: E402
    CLASSIFICATION_CANDIDATE_SCHEMA,
    INDEX_MEMBERSHIP_CANDIDATE_SCHEMA,
    PREFLIGHT_REPORT_SCHEMA,
    AuthorityReceipt,
    AuthorityType,
    IndustryPitContractError,
    KnowledgeTimePolicy,
    ResearchBasis,
)
from backend.services.industry_pit.resolver import IndustryPitResolver  # noqa: E402


EXPECTED_SOURCE_HASHES = {
    "catalog": "923492f4bcf3c7056904385a0769e4dda561904a29ecd9243f942680cef68c81",
    "classification_history": "15979d9cf8a3b83ccc8dadc967de52f35e667b4f4da5e4e4e3dd5a8bb1f17402",
    "latest_snapshot": "b242ab04e0f68357cf90772e3f15367644d3e74c08a767eb9c5edcf21467fcbb",
    "taxonomy_standard": "18fb07fafda072dad39e274371660706e21678045ae8204931958db9906faa1a",
}
EXPECTED_CONFLICT_SYMBOLS = 23
EXPECTED_CONFLICT_OPPORTUNITIES = 23_326
DEFAULT_UNIVERSE_KEY = "aistock_equity_pit_canonical_v2"
DEFAULT_RULE_VERSION = "shsz_a_252td_st_delist_asof_v2"
MANDATORY_REGRESSION_SYMBOLS = (
    "300741.SZ",
    "300858.SZ",
    "603020.SH",
    "605077.SH",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _git_identity() -> Mapping[str, Any]:
    def run(*args: str) -> str:
        result = subprocess.run(
            ["git", *args], cwd=ROOT, text=True, capture_output=True, check=False
        )
        if result.returncode != 0:
            raise IndustryPitContractError(f"git identity command failed: {' '.join(args)}")
        return result.stdout.strip()

    return {
        "commit": run("rev-parse", "HEAD"),
        "tree": run("rev-parse", "HEAD^{tree}"),
        "dirty": bool(run("status", "--porcelain")),
    }


def _require_sources(args: argparse.Namespace) -> Mapping[str, str]:
    paths = {
        "catalog": args.catalog,
        "classification_history": args.classification_history,
        "latest_snapshot": args.latest_snapshot,
        "taxonomy_standard": args.taxonomy_standard,
    }
    observed: dict[str, str] = {}
    for key, path in paths.items():
        if not path.is_file():
            raise IndustryPitContractError(f"required source is missing: {path}")
        observed[key] = _sha256(path)
        if observed[key] != EXPECTED_SOURCE_HASHES[key]:
            raise IndustryPitContractError(
                f"source hash mismatch for {key}: expected={EXPECTED_SOURCE_HASHES[key]} observed={observed[key]}"
            )
    return observed


def _excel_rows(args: argparse.Namespace) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    catalog_frame = pd.read_excel(args.catalog, dtype=object)
    history_frame = pd.read_excel(args.classification_history, dtype=object)
    snapshot_frame = pd.read_excel(args.latest_snapshot, dtype=object)
    if catalog_frame.shape != (511, 4):
        raise IndustryPitContractError(f"catalog shape drifted: {catalog_frame.shape}")
    if history_frame.shape != (11803, 4):
        raise IndustryPitContractError(f"classification history shape drifted: {history_frame.shape}")
    if snapshot_frame.shape[1] != 7:
        raise IndustryPitContractError(f"latest snapshot column count drifted: {snapshot_frame.shape}")
    catalog = [
        {
            "industry_code": row.iloc[0],
            "l1_name": row.iloc[1],
            "l2_name": row.iloc[2],
            "l3_name": row.iloc[3],
        }
        for _, row in catalog_frame.iterrows()
    ]
    history = [
        {
            "stock_code": row.iloc[0],
            "classification_valid_from": row.iloc[1],
            "industry_code": row.iloc[2],
            "source_last_updated_at": row.iloc[3],
        }
        for _, row in history_frame.iterrows()
    ]
    snapshot = [
        {
            "canonical_symbol": row.iloc[2],
            "industry_code": row.iloc[1],
            "l1_name": row.iloc[4],
            "l2_name": row.iloc[5],
            "l3_name": row.iloc[6],
        }
        for _, row in snapshot_frame.iterrows()
    ]
    return catalog, history, snapshot


def _validate_snapshot_crosscheck(
    history: list[dict[str, Any]],
    snapshot: list[dict[str, Any]],
    *,
    mandatory_symbols: tuple[str, ...] = (),
) -> Mapping[str, int]:
    snapshot_by_code: dict[str, set[str]] = {}
    for row in snapshot:
        symbol = str(row["canonical_symbol"] or "").strip().upper()
        if len(symbol) >= 6:
            snapshot_by_code.setdefault(symbol[:6], set()).add(str(row["industry_code"] or "").zfill(6))
    checked = 0
    mandatory_codes = {str(symbol).strip().upper()[:6] for symbol in mandatory_symbols}
    mandatory_checked = 0
    mismatches: list[str] = []
    mandatory_mismatches: list[str] = []
    for row in history:
        valid_text = str(row["classification_valid_from"])
        if not valid_text.startswith("2021-07-30"):
            continue
        numeric = str(row["stock_code"] or "").split(".")[0].zfill(6)
        if numeric not in snapshot_by_code:
            continue
        checked += 1
        code = str(row["industry_code"] or "").zfill(6)
        if code not in snapshot_by_code[numeric]:
            mismatches.append(numeric)
            if numeric in mandatory_codes:
                mandatory_mismatches.append(numeric)
        if numeric in mandatory_codes:
            mandatory_checked += 1
    if mandatory_mismatches:
        raise IndustryPitContractError(
            f"mandatory 2021-07-30 classification snapshot mismatch: {sorted(set(mandatory_mismatches))}"
        )
    if mandatory_codes and mandatory_checked != len(mandatory_codes):
        raise IndustryPitContractError("mandatory 2021-07-30 classification snapshot rows are incomplete")
    return {
        "checked_20210730_rows": checked,
        "current_snapshot_difference_count": len(set(mismatches)),
        "mandatory_checked": mandatory_checked,
        "mandatory_mismatch_count": 0,
    }


def _load_index_evidence(path: Path | None) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    if path is None:
        return [], ()
    if not path.is_file():
        raise IndustryPitContractError(f"index membership evidence is missing: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise IndustryPitContractError(f"index membership evidence JSON is invalid: {exc}") from exc
    if not isinstance(payload, Mapping) or payload.get("schema_version") != "sw_index_membership_evidence_v1":
        raise IndustryPitContractError("index membership evidence schema is invalid")
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise IndustryPitContractError("index membership evidence rows are invalid")
    source_hashes = tuple(sorted({_sha256(path), *(str(row.get("source_sha256") or "") for row in rows)}))
    return [dict(row) for row in rows], source_hashes


def _mandatory_source_regression(
    *,
    mandatory_symbols: tuple[str, ...],
    classification_intervals: tuple[Any, ...],
    index_evidence: list[dict[str, Any]],
) -> Mapping[str, Any]:
    output: dict[str, Any] = {}
    for symbol in mandatory_symbols:
        classification_rows = [
            row
            for row in classification_intervals
            if row.canonical_symbol == symbol
            and row.valid_from.isoformat() == "2021-07-30"
            and row.identity is not None
            and row.identity.leaf_code == "220315"
        ]
        classification_facts = {
            (
                row.valid_from.isoformat(),
                row.known_from.isoformat() if row.known_from else None,
                row.identity.l1_name,
                row.identity.l2_name,
                row.identity.l3_name,
                row.source_last_updated_at,
            )
            for row in classification_rows
        }
        old_index = [
            row
            for row in index_evidence
            if str(row.get("canonical_symbol") or "").upper() == symbol
            and str(row.get("industry_code") or "").zfill(6) == "340404"
            and str(row.get("membership_exit_date_exclusive") or "") == "2021-12-13"
        ]
        new_index = [
            row
            for row in index_evidence
            if str(row.get("canonical_symbol") or "").upper() == symbol
            and str(row.get("industry_code") or "").zfill(6) == "220315"
            and str(row.get("membership_enter_date") or "") == "2021-12-13"
        ]
        if len(classification_facts) != 1 or len(old_index) != 1 or len(new_index) != 1:
            raise IndustryPitContractError(f"mandatory dual-authority regression evidence is incomplete: {symbol}")
        fact = next(iter(classification_facts))
        if fact[:5] != (
            "2021-07-30",
            "2021-08-02",
            "基础化工",
            "化学制品",
            "食品及饲料添加剂",
        ):
            raise IndustryPitContractError(f"mandatory classification regression differs from C-013: {symbol}")
        output[symbol] = {
            "classification_valid_from": fact[0],
            "classification_causal_daily_from": fact[1],
            "classification_leaf_code": "220315",
            "classification_names": list(fact[2:5]),
            "source_last_updated_at_lineage_only": fact[5],
            "index_old_exit_exclusive": "2021-12-13",
            "index_new_enter": "2021-12-13",
            "index_source_hashes": sorted(
                {str(old_index[0]["source_sha256"]), str(new_index[0]["source_sha256"])}
            ),
        }
    return output


def _read_frozen_inputs(
    *,
    universe_key: str,
    rule_version: str,
    window_start: str,
    window_end: str,
) -> tuple[FrozenDenominator, Mapping[str, Mapping[str, Any]], Mapping[str, Any]]:
    with get_conn() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '180s'")
            cur.execute(
                """
                SELECT universe_key,rule_version,scope,start_date,end_date,status,dirty,
                       source_fingerprint_sha256
                FROM market.stock_universe_pit_state WHERE universe_key=%s
                """,
                (universe_key,),
            )
            state_rows = cur.fetchall()
            if len(state_rows) != 1:
                raise IndustryPitContractError("frozen universe state must resolve exactly one row")
            state = state_rows[0]
            if state[1] != rule_version or state[5] != "ready" or bool(state[6]):
                raise IndustryPitContractError("frozen universe state is not ready/clean for the requested rule")
            requested_start = pd.Timestamp(window_start).date()
            requested_end = pd.Timestamp(window_end).date()
            if requested_start < state[3] or requested_end > state[4]:
                raise IndustryPitContractError("requested window escapes frozen universe state coverage")
            cur.execute(
                """
                SELECT cal_date::date FROM market.trading_calendar
                WHERE is_trading=true AND cal_date BETWEEN %s AND %s ORDER BY cal_date
                """,
                (window_start, window_end),
            )
            trading_dates = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                SELECT ts_code,eligible_start,eligible_end
                FROM market.stock_universe_pit_spans
                WHERE universe_key=%s AND eligible_start<=%s
                  AND (eligible_end IS NULL OR eligible_end>=%s)
                ORDER BY ts_code,eligible_start
                """,
                (universe_key, window_end, window_start),
            )
            spans = [UniverseSpan(row[0], row[1], row[2]) for row in cur.fetchall()]
            cur.execute(
                """
                WITH canonical_l1_catalog AS (
                  SELECT DISTINCT l1_code index_code
                  FROM market.sw_index_member WHERE l1_code ~ '^801[0-9]{3}[.]SI$'
                ), l2_catalog AS (
                  SELECT DISTINCT l2_code index_code FROM market.sw_index_member
                ), l2_owner AS (
                  SELECT l2_code,
                         array_agg(DISTINCT l1_code ORDER BY l1_code)
                           FILTER (WHERE l1_code ~ '^801[0-9]{3}[.]SI$') canonical_l1_codes,
                         count(DISTINCT l1_code) FILTER (WHERE l1_code ~ '^801[0-9]{3}[.]SI$') canonical_l1_count
                  FROM market.sw_index_member GROUP BY l2_code
                ), cal AS (
                  SELECT cal_date::date trade_date FROM market.trading_calendar
                  WHERE is_trading=true AND cal_date BETWEEN %s AND %s
                ), opportunity AS (
                  SELECT c.trade_date,s.ts_code FROM cal c JOIN market.stock_universe_pit_spans s
                    ON s.universe_key=%s AND s.eligible_start<=c.trade_date
                   AND (s.eligible_end IS NULL OR s.eligible_end>=c.trade_date)
                ), identity_rows AS (
                  SELECT DISTINCT o.trade_date,o.ts_code,l1.index_code l1_code,l2.index_code l2_code
                  FROM opportunity o JOIN market.sw_index_member m
                    ON m.ts_code=o.ts_code AND m.in_date<=o.trade_date
                   AND (m.out_date IS NULL OR m.out_date>=o.trade_date)
                  JOIN l2_owner owner ON owner.l2_code=m.l2_code AND owner.canonical_l1_count=1
                  JOIN canonical_l1_catalog l1 ON l1.index_code=owner.canonical_l1_codes[1]
                  JOIN l2_catalog l2 ON l2.index_code=m.l2_code
                ), counted AS (
                  SELECT trade_date,ts_code,count(*) identity_count
                  FROM identity_rows GROUP BY trade_date,ts_code
                )
                SELECT ts_code,count(*) conflict_opportunities
                FROM counted WHERE identity_count>1 GROUP BY ts_code ORDER BY ts_code
                """,
                (window_start, window_end, universe_key),
            )
            conflict_rows = cur.fetchall()
    denominator = FrozenDenominator.build(
        window_start=pd.Timestamp(window_start).date(),
        window_end=pd.Timestamp(window_end).date(),
        trading_dates=trading_dates,
        universe_spans=spans,
    )
    conflict_inventory = {
        row[0]: {
            "legacy_conflict_opportunities": int(row[1]),
            "diagnostic_only_not_authority_source": True,
        }
        for row in conflict_rows
    }
    if len(conflict_inventory) != EXPECTED_CONFLICT_SYMBOLS or sum(
        value["legacy_conflict_opportunities"] for value in conflict_inventory.values()
    ) != EXPECTED_CONFLICT_OPPORTUNITIES:
        raise IndustryPitContractError(
            "frozen 23-symbol regression inventory drifted from approved C-013 design"
        )
    state_receipt = {
        "universe_key": state[0],
        "rule_version": state[1],
        "scope": state[2],
        "state_start": state[3].isoformat(),
        "state_end": state[4].isoformat(),
        "status": state[5],
        "dirty": bool(state[6]),
        "source_fingerprint_sha256": state[7],
        "database_access": "read_only",
        "industry_authority_source": False,
    }
    return denominator, conflict_inventory, state_receipt


def build(args: argparse.Namespace) -> Mapping[str, Any]:
    producer = _git_identity()
    source_hashes = _require_sources(args)
    catalog_rows, history_rows, snapshot_rows = _excel_rows(args)
    snapshot_check = _validate_snapshot_crosscheck(
        history_rows,
        snapshot_rows,
        mandatory_symbols=tuple(args.mandatory_symbol),
    )
    index_evidence, index_evidence_hashes = _load_index_evidence(args.index_membership_evidence)
    load_dotenv(args.db_env_file, override=False)
    denominator, conflict_inventory, state_receipt = _read_frozen_inputs(
        universe_key=args.universe_key,
        rule_version=args.rule_version,
        window_start=args.window_start,
        window_end=args.window_end,
    )
    catalog = build_taxonomy_catalog(catalog_rows, source_sha256=source_hashes["catalog"])
    classification_receipt = AuthorityReceipt(
        authority_type=AuthorityType.CLASSIFICATION,
        authority_schema=CLASSIFICATION_CANDIDATE_SCHEMA,
        authority_version="c013_classification_candidate_v1",
        taxonomy_contract_id=catalog.contract_id,
        taxonomy_version=catalog.version,
        knowledge_time_policy=KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE,
        research_basis=ResearchBasis.AS_PUBLISHED_PIT,
        source_ids=(
            "local:SwClassCode_2021.xls",
            "local:StockClassifyUse_stock.xls",
            "local:latest_stock_sw_classification_through_july.xlsx",
            "local:SwClassStd2021.pdf",
        ),
        source_hashes=tuple(source_hashes.values()),
        frozen_denominator=denominator.total_opportunities,
        denominator_digest=denominator.digest,
    )
    index_receipt = AuthorityReceipt(
        authority_type=AuthorityType.INDEX_MEMBERSHIP,
        authority_schema=INDEX_MEMBERSHIP_CANDIDATE_SCHEMA,
        authority_version="c013_index_membership_candidate_v1",
        taxonomy_contract_id=catalog.contract_id,
        taxonomy_version=catalog.version,
        knowledge_time_policy=KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE,
        research_basis=ResearchBasis.AS_PUBLISHED_PIT,
        source_ids=tuple(
            sorted(
                ({
                    "local:SwClassCode_2021.xls",
                    "local:SwClassStd2021.pdf",
                    *( ["task:index_membership_evidence_v1"] if index_evidence else [] ),
                    *(str(row.get("source_url") or "") for row in index_evidence),
                } - {""})
            )
        ),
        source_hashes=tuple(
            sorted(
                {
                    source_hashes["catalog"],
                    source_hashes["taxonomy_standard"],
                    *index_evidence_hashes,
                }
            )
        ),
        frozen_denominator=denominator.total_opportunities,
        denominator_digest=denominator.digest,
    )
    classification, classification_diagnostics = build_classification_intervals(
        history_rows,
        catalog=catalog,
        receipt=classification_receipt,
        denominator=denominator,
        classification_source_hash=source_hashes["classification_history"],
    )
    index_membership, index_diagnostics = build_index_membership_intervals(
        index_evidence,
        catalog=catalog,
        receipt=index_receipt,
        denominator=denominator,
    )
    mandatory_source_regression = _mandatory_source_regression(
        mandatory_symbols=tuple(args.mandatory_symbol),
        classification_intervals=classification,
        index_evidence=index_evidence,
    )
    known = {(catalog.contract_id, catalog.version)}
    classification_resolver = IndustryPitResolver(
        receipt=classification_receipt,
        intervals=classification,
        known_taxonomy_versions=known,
    )
    index_resolver = IndustryPitResolver(
        receipt=index_receipt,
        intervals=index_membership,
        known_taxonomy_versions=known,
    )
    report = dict(
        full_denominator_preflight(
            denominator=denominator,
            classification_resolver=classification_resolver,
            index_membership_resolver=index_resolver,
            conflict_inventory=conflict_inventory,
            mandatory_symbols=args.mandatory_symbol,
        )
    )
    report.pop("canonical_hash", None)
    report["source_diagnostics"] = {
        "classification": dict(classification_diagnostics),
        "index_membership": dict(index_diagnostics),
        "snapshot_crosscheck": dict(snapshot_check),
        "frozen_universe": dict(state_receipt),
        "source_hashes": dict(source_hashes),
        "mandatory_source_regression": mandatory_source_regression,
        "production_database_writes": 0,
    }
    report["canonical_hash"] = digest_named_fields(PREFLIGHT_REPORT_SCHEMA, report)
    if args.dry_run:
        return {
            "status": "PASS_DRY_RUN",
            "artifact_written": False,
            "preflight_canonical_hash": report["canonical_hash"],
            "total_opportunities": report["total_opportunities"],
            "classification": report["classification"],
            "index_membership": report["index_membership"],
            "alignment": report["alignment"],
            "conflict_symbol_count": len(conflict_inventory),
            "closure": report["closure"],
            "classification_receipt_hash": classification_receipt.receipt_hash,
            "index_membership_receipt_hash": index_receipt.receipt_hash,
            "database_writes": 0,
            "producer": producer,
        }
    if producer["dirty"]:
        raise IndustryPitContractError("refusing to write a candidate from a dirty producer worktree")
    readback = write_candidate_bundle(
        artifact_root=args.artifact_root,
        forbidden_roots=(ROOT, ROOT.parent / "AIstock_worktrees"),
        taxonomy_catalog=catalog.as_dict(),
        classification_receipt=classification_receipt,
        index_membership_receipt=index_receipt,
        classification_intervals=classification,
        index_membership_intervals=index_membership,
        preflight_report=report,
        producer_commit=str(producer["commit"]),
        producer_tree=str(producer["tree"]),
    )
    return {
        "status": "PASS_CANDIDATE_ONLY",
        "artifact_written": True,
        "artifact_root": str(readback.artifact_root),
        "bundle_hash": readback.manifest["bundle_hash"],
        "classification_candidate_hash": readback.manifest["classification_candidate_hash"],
        "index_membership_candidate_hash": readback.manifest["index_membership_candidate_hash"],
        "total_opportunities": denominator.total_opportunities,
        "classification": report["classification"],
        "index_membership": report["index_membership"],
        "alignment": report["alignment"],
        "conflict_symbol_count": len(conflict_inventory),
        "closure": report["closure"],
        "database_writes": 0,
        "production_activation": False,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=os.environ.get("AISTOCK_SWCLASS_SOURCE_ROOT"))
    parser.add_argument("--catalog", type=Path)
    parser.add_argument("--classification-history", type=Path)
    parser.add_argument("--latest-snapshot", type=Path)
    parser.add_argument("--taxonomy-standard", type=Path)
    parser.add_argument("--index-membership-evidence", type=Path)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--db-env-file", type=Path, default=os.environ.get("AISTOCK_DB_ENV_FILE"))
    parser.add_argument("--universe-key", default=DEFAULT_UNIVERSE_KEY)
    parser.add_argument("--rule-version", default=DEFAULT_RULE_VERSION)
    parser.add_argument("--window-start", default="2020-07-30")
    parser.add_argument("--window-end", default="2026-03-31")
    parser.add_argument("--mandatory-symbol", action="append", default=[])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.source_root is None:
        parser.error("--source-root or AISTOCK_SWCLASS_SOURCE_ROOT is required")
    if args.db_env_file is None:
        parser.error("--db-env-file or AISTOCK_DB_ENV_FILE is required")
    args.catalog = args.catalog or args.source_root / "SwClassCode_2021.xls"
    args.classification_history = args.classification_history or args.source_root / "StockClassifyUse_stock.xls"
    args.latest_snapshot = args.latest_snapshot or args.source_root / "最新个股申万行业分类(完整版-截至7月末).xlsx"
    args.taxonomy_standard = args.taxonomy_standard or args.source_root / "SwClassStd2021.pdf"
    args.mandatory_symbol = list(
        dict.fromkeys([*MANDATORY_REGRESSION_SYMBOLS, *args.mandatory_symbol])
    )
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = build(args)
    except IndustryPitContractError as exc:
        print(
            json.dumps(
                {"status": "FAILED", "reason_code": exc.code, "error": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
